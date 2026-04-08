use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures::stream::{self, StreamExt};
use tokio::time::{sleep_until, Instant};

use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::DigestManager;
use bookkeeper_protocol::entry::{parse_entry, EntryError};
use bookkeeper_protocol::v2::response::V2Response;
use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;

/// Speculative read policy matching Java's DefaultSpeculativeRequestExecutionPolicy.
/// When enabled, sends a speculative read to the next bookie after a timeout,
/// with exponential backoff between successive speculative attempts.
#[derive(Debug, Clone)]
pub struct SpeculativeReadPolicy {
    pub first_timeout_ms: u64,
    pub max_timeout_ms: u64,
    pub backoff_multiplier: f64,
}

impl SpeculativeReadPolicy {
    /// Create from config values. Returns None if disabled (first_timeout <= 0).
    pub fn from_config(
        speculative_read_timeout: i32,
        first_speculative_read_timeout: i32,
        max_speculative_read_timeout: i32,
        backoff_multiplier: f64,
    ) -> Option<Self> {
        let first = if first_speculative_read_timeout > 0 {
            first_speculative_read_timeout as u64
        } else if speculative_read_timeout > 0 {
            speculative_read_timeout as u64
        } else {
            return None; // disabled
        };
        // Java: getMaxSpeculativeReadTimeout() defaults to getSpeculativeReadTimeout()
        let max = if max_speculative_read_timeout > 0 {
            max_speculative_read_timeout as u64
        } else if speculative_read_timeout > 0 {
            speculative_read_timeout as u64
        } else {
            first
        };
        Some(Self {
            first_timeout_ms: first,
            max_timeout_ms: max,
            backoff_multiplier: if backoff_multiplier > 0.0 { backoff_multiplier } else { 2.0 },
        })
    }

    fn next_timeout(&self, current: u64) -> u64 {
        let next = (current as f64 * self.backoff_multiplier) as u64;
        next.min(self.max_timeout_ms)
    }
}

/// Shared state across entries in a batch read, matching Java's heardFromHostsBitSet.
/// Bit N is set when bookie index N has responded successfully for ANY entry.
/// Used to suppress unnecessary speculative reads.
#[derive(Clone)]
pub struct HeardFromHosts {
    bits: Arc<AtomicU64>,
}

impl HeardFromHosts {
    pub fn new() -> Self {
        Self { bits: Arc::new(AtomicU64::new(0)) }
    }

    /// Mark a bookie index as "heard from" (responded successfully).
    pub fn mark_heard(&self, bookie_idx: usize) {
        if bookie_idx < 64 {
            self.bits.fetch_or(1u64 << bookie_idx, Ordering::Release);
        }
    }

    /// Check if ANY of the given bookie indices have been heard from.
    pub fn any_heard(&self, indices: &[usize]) -> bool {
        let bits = self.bits.load(Ordering::Acquire);
        for &idx in indices {
            if idx < 64 && (bits & (1u64 << idx)) != 0 {
                return true;
            }
        }
        false
    }
}

pub struct ReadHandle {
    ledger_id: i64,
    #[allow(dead_code)]
    ensemble: Vec<String>,
    schedule: RoundRobinDistributionSchedule,
    digest_manager: DigestManager,
    master_key: [u8; MASTER_KEY_LENGTH],
    speculative_policy: Option<SpeculativeReadPolicy>,
}

impl ReadHandle {
    pub fn new(
        ledger_id: i64,
        ensemble: Vec<String>,
        schedule: RoundRobinDistributionSchedule,
        digest_manager: DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Self {
        Self {
            ledger_id, ensemble, schedule, digest_manager, master_key,
            speculative_policy: None,
        }
    }

    /// Enable speculative reads with the given policy.
    pub fn with_speculative_policy(mut self, policy: SpeculativeReadPolicy) -> Self {
        self.speculative_policy = Some(policy);
        self
    }

    pub fn ledger_id(&self) -> i64 {
        self.ledger_id
    }

    /// Read a single entry. With speculative reads enabled, races the primary
    /// read against speculative reads to subsequent bookies after timeouts.
    /// Matches Java's PendingReadOp.SequenceReadRequest.
    pub async fn read_entry(
        &self,
        conns: &[Arc<BookieConnection>],
        entry_id: i64,
    ) -> Result<Vec<u8>, ClientError> {
        let write_set = self.schedule.write_set(entry_id);

        match &self.speculative_policy {
            Some(policy) if !write_set.is_empty() => {
                self.read_entry_speculative(conns, entry_id, &write_set, policy, None).await
            }
            _ => {
                self.read_entry_sequential(conns, entry_id, &write_set).await
            }
        }
    }

    /// Read a single entry with shared heard-from state for batch reads.
    async fn read_entry_with_heard_from(
        &self,
        conns: &[Arc<BookieConnection>],
        entry_id: i64,
        heard_from: &HeardFromHosts,
    ) -> Result<Vec<u8>, ClientError> {
        let write_set = self.schedule.write_set(entry_id);

        match &self.speculative_policy {
            Some(policy) if !write_set.is_empty() => {
                self.read_entry_speculative(conns, entry_id, &write_set, policy, Some(heard_from)).await
            }
            _ => {
                self.read_entry_sequential(conns, entry_id, &write_set).await
            }
        }
    }

    /// Sequential fallback: try bookies one at a time, move to next only on error.
    async fn read_entry_sequential(
        &self,
        conns: &[Arc<BookieConnection>],
        entry_id: i64,
        write_set: &[usize],
    ) -> Result<Vec<u8>, ClientError> {
        let mut last_error: Option<ClientError> = None;

        for &bookie_idx in write_set {
            if bookie_idx >= conns.len() {
                continue;
            }
            match self.try_read_from(conns, bookie_idx, entry_id).await {
                Ok(data) => return Ok(data),
                Err(e) => { last_error = Some(e); continue; }
            }
        }

        Err(last_error.unwrap_or_else(|| ClientError::Protocol(format!(
            "entry not found: ledger={}, entry={}", self.ledger_id, entry_id
        ))))
    }

    /// Speculative read: send to first bookie, then race speculative reads
    /// to subsequent bookies after configurable timeouts.
    /// Matches Java's PendingReadOp + DefaultSpeculativeRequestExecutionPolicy.
    ///
    /// Key Java-compatible behaviors:
    /// - heardFromHosts guard: suppresses speculative if a contacted bookie responded for any entry
    /// - readsOutstanding: only error-fallback when ALL sent reads have errored
    /// - Absolute timer deadlines: timer doesn't reset when errors arrive
    async fn read_entry_speculative(
        &self,
        conns: &[Arc<BookieConnection>],
        entry_id: i64,
        write_set: &[usize],
        policy: &SpeculativeReadPolicy,
        heard_from: Option<&HeardFromHosts>,
    ) -> Result<Vec<u8>, ClientError> {
        let (result_tx, mut result_rx) = tokio::sync::mpsc::channel::<(usize, Result<Vec<u8>, ClientError>)>(write_set.len());
        let completed = Arc::new(AtomicBool::new(false));

        let mut sent_count: usize = 0;
        let mut error_count: usize = 0;
        let mut next_replica: usize = 0;
        // Track which bookie indices we've sent to (for heardFrom guard)
        let mut sent_to: Vec<usize> = Vec::new();

        let spawn_read = |bookie_idx: usize,
                          conns: &[Arc<BookieConnection>],
                          tx: &tokio::sync::mpsc::Sender<(usize, Result<Vec<u8>, ClientError>)>,
                          done: &Arc<AtomicBool>| {
            if bookie_idx >= conns.len() { return; }
            let conn = conns[bookie_idx].clone();
            let dm = self.digest_manager.clone();
            let ledger_id = self.ledger_id;
            let master_key = self.master_key;
            let tx = tx.clone();
            let done = done.clone();
            tokio::spawn(async move {
                if done.load(Ordering::Relaxed) { return; }
                let result = try_read_entry(&conn, &dm, ledger_id, entry_id, master_key).await;
                let _ = tx.send((bookie_idx, result)).await;
            });
        };

        // Send to first bookie immediately
        if let Some(&bookie_idx) = write_set.get(next_replica) {
            spawn_read(bookie_idx, conns, &result_tx, &completed);
            sent_to.push(bookie_idx);
            sent_count += 1;
            next_replica += 1;
        }

        // Use absolute deadlines so timers don't reset when errors arrive
        let mut current_timeout = policy.first_timeout_ms;
        let mut next_deadline = Instant::now() + Duration::from_millis(current_timeout);

        loop {
            tokio::select! {
                result = result_rx.recv() => {
                    match result {
                        Some((bookie_idx, Ok(data))) => {
                            completed.store(true, Ordering::Relaxed);
                            // Mark bookie as heard-from for other entries in the batch
                            if let Some(hf) = heard_from {
                                hf.mark_heard(bookie_idx);
                            }
                            return Ok(data);
                        }
                        Some((_, Err(_))) => {
                            error_count += 1;
                            // Java: only send next when ALL sent reads have errored
                            let reads_outstanding = sent_count > error_count;
                            if !reads_outstanding && next_replica < write_set.len() {
                                let bookie_idx = write_set[next_replica];
                                spawn_read(bookie_idx, conns, &result_tx, &completed);
                                sent_to.push(bookie_idx);
                                sent_count += 1;
                                next_replica += 1;
                            }
                            if error_count >= sent_count && next_replica >= write_set.len() {
                                return Err(ClientError::Protocol(format!(
                                    "all replicas failed: ledger={}, entry={}", self.ledger_id, entry_id
                                )));
                            }
                        }
                        None => {
                            return Err(ClientError::Protocol(format!(
                                "entry not found: ledger={}, entry={}", self.ledger_id, entry_id
                            )));
                        }
                    }
                }
                _ = sleep_until(next_deadline), if next_replica < write_set.len() => {
                    if completed.load(Ordering::Relaxed) { continue; }
                    // Java's heardFromHosts guard: suppress speculative if any
                    // bookie we've contacted has responded for ANY entry.
                    if let Some(hf) = heard_from {
                        if hf.any_heard(&sent_to) {
                            // A contacted bookie is alive — don't send speculative
                            // Reschedule with backoff in case it's still slow
                            current_timeout = policy.next_timeout(current_timeout);
                            next_deadline = Instant::now() + Duration::from_millis(current_timeout);
                            continue;
                        }
                    }
                    let bookie_idx = write_set[next_replica];
                    spawn_read(bookie_idx, conns, &result_tx, &completed);
                    sent_to.push(bookie_idx);
                    sent_count += 1;
                    next_replica += 1;
                    current_timeout = policy.next_timeout(current_timeout);
                    next_deadline = Instant::now() + Duration::from_millis(current_timeout);
                }
            }
        }
    }

    /// Try reading a single entry from a specific bookie, verify digest.
    async fn try_read_from(
        &self,
        conns: &[Arc<BookieConnection>],
        bookie_idx: usize,
        entry_id: i64,
    ) -> Result<Vec<u8>, ClientError> {
        try_read_entry(
            &conns[bookie_idx],
            &self.digest_manager,
            self.ledger_id,
            entry_id,
            self.master_key,
        ).await
    }

    /// Read a range of entries [first_entry..=last_entry] sequentially.
    pub async fn read_entries(
        &self,
        conns: &[Arc<BookieConnection>],
        first_entry: i64,
        last_entry: i64,
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        self.read_entries_parallel(conns, first_entry, last_entry, 1).await
    }

    /// Read a range of entries with bounded parallelism.
    /// Uses shared heardFromHosts state across entries to suppress unnecessary
    /// speculative reads, matching Java's PendingReadOp batch behavior.
    pub async fn read_entries_parallel(
        &self,
        conns: &[Arc<BookieConnection>],
        first_entry: i64,
        last_entry: i64,
        concurrency: usize,
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        let concurrency = concurrency.max(1);
        let heard_from = HeardFromHosts::new();

        let results: Vec<Result<Vec<u8>, ClientError>> = stream::iter(first_entry..=last_entry)
            .map(|eid| {
                let hf = heard_from.clone();
                async move {
                    self.read_entry_with_heard_from(conns, eid, &hf).await
                }
            })
            .buffered(concurrency)
            .collect()
            .await;

        results.into_iter().collect()
    }
}

/// Standalone read+verify function used by both sequential and speculative paths.
async fn try_read_entry(
    conn: &BookieConnection,
    dm: &DigestManager,
    ledger_id: i64,
    entry_id: i64,
    master_key: [u8; MASTER_KEY_LENGTH],
) -> Result<Vec<u8>, ClientError> {
    match conn.send_read_entry(0, ledger_id, entry_id, Some(master_key)).await {
        Ok(V2Response::Read { error_code, data, .. }) => {
            if error_code == V2_EOK {
                if let Some(entry_data) = data {
                    match parse_entry(dm, entry_data) {
                        Ok(entry) => {
                            if entry.ledger_id != ledger_id || entry.entry_id != entry_id {
                                return Err(ClientError::Protocol("wrong entry".into()));
                            }
                            return Ok(entry.payload.to_vec());
                        }
                        Err(EntryError::DigestMismatch) => {
                            return Err(ClientError::Protocol("digest mismatch".into()));
                        }
                        Err(e) => {
                            return Err(ClientError::Protocol(format!("entry parse: {e}")));
                        }
                    }
                }
            }
            Err(ClientError::Protocol(format!("bookie error: {error_code}")))
        }
        Ok(_) => Err(ClientError::Protocol("unexpected response".into())),
        Err(e) => Err(e),
    }
}
