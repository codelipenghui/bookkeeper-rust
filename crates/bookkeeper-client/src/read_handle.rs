use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use futures::future::join_all;
use futures::stream::{self, StreamExt};
use tokio::time::{sleep_until, Instant};

use bookkeeper_common::error::BKError;
use bookkeeper_common::types::LAST_ADD_CONFIRMED;
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

/// Shared state across entries in a batch read, matching Java's
/// heardFromHostsBitSet. Bit N is set when bookie index N has responded
/// successfully for ANY entry. Used to suppress unnecessary speculative
/// reads.
///
/// Backed by a vector of `AtomicU64` words sized at construction so
/// ensembles larger than 64 bookies are tracked correctly. The previous
/// single-`AtomicU64` representation silently dropped every mark/check
/// for bookies at index 64+ — speculative reads then kept firing
/// against bookies that had already responded.
#[derive(Clone)]
pub struct HeardFromHosts {
    /// Each word covers 64 bookie indices. `words[k]` bit `b` <=> bookie
    /// index `k * 64 + b`.
    words: Arc<Vec<AtomicU64>>,
    bookie_count: usize,
}

impl HeardFromHosts {
    pub fn new(bookie_count: usize) -> Self {
        let n_words = (bookie_count + 63) / 64;
        let words = (0..n_words).map(|_| AtomicU64::new(0)).collect();
        Self {
            words: Arc::new(words),
            bookie_count,
        }
    }

    /// Mark a bookie index as "heard from" (responded successfully).
    /// Indices beyond the ensemble size are silently ignored — the
    /// caller bounds-checks against `conns.len()` upstream, so this only
    /// fires on programmer error.
    pub fn mark_heard(&self, bookie_idx: usize) {
        if bookie_idx >= self.bookie_count {
            return;
        }
        let word = bookie_idx / 64;
        let bit = bookie_idx % 64;
        self.words[word].fetch_or(1u64 << bit, Ordering::Release);
    }

    /// Check if ANY of the given bookie indices have been heard from.
    pub fn any_heard(&self, indices: &[usize]) -> bool {
        for &idx in indices {
            if idx >= self.bookie_count {
                continue;
            }
            let word = idx / 64;
            let bit = idx % 64;
            if (self.words[word].load(Ordering::Acquire) & (1u64 << bit)) != 0 {
                return true;
            }
        }
        false
    }
}

pub struct ReadHandle {
    ledger_id: i64,
    /// All ensemble segments: maps firstEntryId to the ensemble active from that
    /// entry.  For a ledger that never underwent an ensemble change this contains
    /// a single entry `{0 -> initial_ensemble}`.  For ledgers with ensemble
    /// changes there will be multiple entries.
    ///
    /// Matches Java's `LedgerMetadata.getEnsembles()`.  Every read resolves the
    /// correct ensemble via [`ensemble_at`] rather than using a single flat list.
    ensemble_segments: std::collections::BTreeMap<i64, Vec<String>>,
    /// Maps bookie address -> index in the `conns` array passed to read methods.
    /// Built from all unique bookie addresses across all ensemble segments.
    /// For single-segment ledgers this is a simple identity map (addr -> 0,1,2...).
    bookie_index: std::collections::HashMap<String, usize>,
    schedule: RoundRobinDistributionSchedule,
    digest_manager: DigestManager,
    master_key: [u8; MASTER_KEY_LENGTH],
    speculative_policy: Option<SpeculativeReadPolicy>,
    /// Client-side LAC, updated from piggybacked read responses.
    /// Matches Java's `LedgerHandle.lastAddConfirmed`.
    last_add_confirmed: AtomicI64,
    /// Enable sticky reads: all reads go to one designated bookie.
    /// Matches Java's `LedgerHandle.stickyBookieIndex`.
    /// -1 means disabled.
    sticky_bookie_index: std::sync::atomic::AtomicIsize,
    /// Bookies that have had read errors — used for reorder_read_sequence.
    /// Matches Java's BookiesHealthInfo.getBookieFailureHistory().
    failed_bookies: std::sync::RwLock<std::collections::HashSet<String>>,
    /// When true, reorder read sequence to put failed bookies last.
    /// Matches Java's `enableReorderReadSequence` config.
    reorder_read_sequence: bool,
}

/// Sentinel value for disabled sticky reads.
const STICKY_READ_DISABLED: isize = -1;

impl ReadHandle {
    /// Build a bookie address -> connection index map from ensemble segments.
    /// For a single-segment ledger the map is `{addr[0] -> 0, addr[1] -> 1, ...}`.
    /// For multi-segment ledgers the map covers all unique bookies across segments.
    fn build_bookie_index(
        segments: &std::collections::BTreeMap<i64, Vec<String>>,
    ) -> std::collections::HashMap<String, usize> {
        let mut index = std::collections::HashMap::new();
        // For backward compatibility with callers that pass conns aligned to the
        // last (or only) ensemble, assign indices 0..N-1 to the last segment first.
        if let Some(last_ens) = segments.values().next_back() {
            for (i, addr) in last_ens.iter().enumerate() {
                index.entry(addr.clone()).or_insert(i);
            }
        }
        // Then assign new indices for bookies only in earlier segments.
        let mut next_idx = index.len();
        for ens in segments.values() {
            for addr in ens {
                if !index.contains_key(addr) {
                    index.insert(addr.clone(), next_idx);
                    next_idx += 1;
                }
            }
        }
        index
    }

    pub fn new(
        ledger_id: i64,
        ensemble: Vec<String>,
        schedule: RoundRobinDistributionSchedule,
        digest_manager: DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Self {
        let mut segments = std::collections::BTreeMap::new();
        segments.insert(0, ensemble);
        let bookie_index = Self::build_bookie_index(&segments);
        Self {
            ledger_id, ensemble_segments: segments, bookie_index,
            schedule, digest_manager, master_key,
            speculative_policy: None,
            last_add_confirmed: AtomicI64::new(-1),
            sticky_bookie_index: std::sync::atomic::AtomicIsize::new(STICKY_READ_DISABLED),
            failed_bookies: std::sync::RwLock::new(std::collections::HashSet::new()),
            reorder_read_sequence: false,
        }
    }

    /// Create a ReadHandle with multiple ensemble segments.
    /// Matches Java's `LedgerMetadata.getEnsembles()` — a sorted map of
    /// `firstEntryId -> ensemble` that allows correct routing when a ledger
    /// underwent ensemble changes during its lifetime.
    pub fn with_ensemble_segments(
        ledger_id: i64,
        ensemble_segments: std::collections::BTreeMap<i64, Vec<String>>,
        schedule: RoundRobinDistributionSchedule,
        digest_manager: DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Self {
        let bookie_index = Self::build_bookie_index(&ensemble_segments);
        Self {
            ledger_id, ensemble_segments, bookie_index,
            schedule, digest_manager, master_key,
            speculative_policy: None,
            last_add_confirmed: AtomicI64::new(-1),
            sticky_bookie_index: std::sync::atomic::AtomicIsize::new(STICKY_READ_DISABLED),
            failed_bookies: std::sync::RwLock::new(std::collections::HashSet::new()),
            reorder_read_sequence: false,
        }
    }

    /// Enable speculative reads with the given policy.
    pub fn with_speculative_policy(mut self, policy: SpeculativeReadPolicy) -> Self {
        self.speculative_policy = Some(policy);
        self
    }

    /// Set the initial LAC for confirmed read enforcement.
    pub fn with_last_add_confirmed(self, lac: i64) -> Self {
        self.last_add_confirmed.store(lac, Ordering::Release);
        self
    }

    /// Enable read sequence reordering: failed bookies are moved to the end.
    /// Matches Java's `enableReorderReadSequence` config.
    pub fn with_reorder_read_sequence(mut self, enabled: bool) -> Self {
        self.reorder_read_sequence = enabled;
        self
    }

    /// Enable sticky reads. Only effective when ensembleSize == writeQuorumSize.
    /// Matches Java: all reads go to one designated bookie; on failure, rotates.
    pub fn with_sticky_reads(self) -> Self {
        if self.schedule.ensemble_size() == self.schedule.write_quorum_size() {
            // Pick a random starting bookie
            let idx = (self.ledger_id.unsigned_abs() as usize) % self.schedule.ensemble_size();
            self.sticky_bookie_index.store(idx as isize, Ordering::Release);
        }
        self
    }

    pub fn ledger_id(&self) -> i64 {
        self.ledger_id
    }

    /// Return the ensemble responsible for the given entry ID.
    /// Resolves by finding the highest segment whose firstEntryId <= entryId.
    /// Matches Java's `LedgerMetadata.getEnsembleAt(entryId)`.
    fn ensemble_at(&self, entry_id: i64) -> &[String] {
        self.ensemble_segments
            .range(..=entry_id)
            .next_back()
            .map(|(_, v)| v.as_slice())
            .unwrap_or_else(|| {
                // Fallback: use the first segment (should never happen for valid IDs)
                self.ensemble_segments.values().next().map(|v| v.as_slice()).unwrap_or(&[])
            })
    }

    /// Return all ensemble segments.
    pub fn ensemble_segments(&self) -> &std::collections::BTreeMap<i64, Vec<String>> {
        &self.ensemble_segments
    }

    /// Resolve the connection index in the `conns` array for the bookie at
    /// position `ensemble_idx` in the ensemble responsible for `entry_id`.
    /// Returns `None` if the bookie address is not in the connection map.
    fn conn_index_for(&self, entry_id: i64, ensemble_idx: usize) -> Option<usize> {
        let ensemble = self.ensemble_at(entry_id);
        ensemble.get(ensemble_idx).and_then(|addr| self.bookie_index.get(addr).copied())
    }

    /// Return the current client-side LAC.
    pub fn last_add_confirmed(&self) -> i64 {
        self.last_add_confirmed.load(Ordering::Acquire)
    }

    /// Update LAC from a piggybacked read response. Only advances, never regresses.
    /// Matches Java's `LedgerHandle.updateLastConfirmed(lac, len)`.
    fn update_last_confirmed(&self, lac: i64) {
        let _ = self.last_add_confirmed.fetch_update(Ordering::Release, Ordering::Acquire, |cur| {
            if lac > cur { Some(lac) } else { None }
        });
    }

    /// Poll the last-add-confirmed value from ALL bookies in the ensemble.
    /// Returns the maximum LAC seen once write-quorum bookies have responded.
    /// Matches Java's `ReadLastConfirmedOp.initiate()`.
    pub async fn read_last_confirmed(
        &self,
        conns: &[Arc<BookieConnection>],
    ) -> Result<i64, ClientError> {
        let write_quorum = self.schedule.write_quorum_size();
        let ledger_id = self.ledger_id;
        let master_key = self.master_key;

        // Send LAST_ADD_CONFIRMED read to ALL bookies in the last ensemble.
        // Use bookie_index to resolve connection positions.
        let last_ensemble = self.ensemble_segments.values().next_back()
            .cloned().unwrap_or_default();
        let conn_indices: Vec<usize> = last_ensemble.iter()
            .filter_map(|addr| self.bookie_index.get(addr).copied())
            .filter(|&idx| idx < conns.len())
            .collect();
        let futures: Vec<_> = conn_indices.iter().map(|&idx| {
            let conn = &*conns[idx];
            async move {
                conn.send_read_entry(0, ledger_id, LAST_ADD_CONFIRMED, Some(master_key)).await
            }
        }).collect();

        let responses = join_all(futures).await;
        let mut max_lac: i64 = -1;
        let mut valid_count = 0;

        for result in responses {
            match result {
                Ok(V2Response::Read { error_code, entry_id, .. }) => {
                    let err = BKError::from_v2_code(error_code);
                    match err {
                        BKError::Ok | BKError::NoEntry => {
                            valid_count += 1;
                            if entry_id > max_lac {
                                max_lac = entry_id;
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        if valid_count >= write_quorum {
            self.update_last_confirmed(max_lac);
            Ok(max_lac)
        } else {
            Err(ClientError::Protocol(format!(
                "LAC quorum not reached: {valid_count}/{write_quorum} valid responses"
            )))
        }
    }

    /// Short-circuit LAC probe: return as soon as the FIRST bookie responds
    /// with a LAC value strictly greater than the caller's known LAC.
    ///
    /// Matches Java's `asyncTryReadLastConfirmed` (Pulsar's tail-read fast
    /// path).  Unlike [`read_last_confirmed`] which waits for write-quorum
    /// responses, this returns as soon as any ensemble bookie confirms
    /// LAC has advanced — used by managed-ledger cursors polling for
    /// new entries.
    ///
    /// If no bookie reports a LAC > `current_lac`, returns
    /// [`BKError::NoEntry`] wrapped in [`ClientError::BookieError`] after
    /// all responses have been collected, matching Java's behavior of
    /// surfacing `NoSuchEntryException` when no progress is visible.
    pub async fn try_read_last_confirmed(
        &self,
        conns: &[Arc<BookieConnection>],
        current_lac: i64,
    ) -> Result<i64, ClientError> {
        use futures::stream::FuturesUnordered;

        let ledger_id = self.ledger_id;
        let master_key = self.master_key;

        let last_ensemble = self
            .ensemble_segments
            .values()
            .next_back()
            .cloned()
            .unwrap_or_default();
        let conn_indices: Vec<usize> = last_ensemble
            .iter()
            .filter_map(|addr| self.bookie_index.get(addr).copied())
            .filter(|&idx| idx < conns.len())
            .collect();

        if conn_indices.is_empty() {
            return Err(ClientError::BookieError { error: BKError::NoEntry });
        }

        let mut futs: FuturesUnordered<_> = conn_indices
            .into_iter()
            .map(|idx| {
                let conn = conns[idx].clone();
                async move {
                    conn.send_read_entry(0, ledger_id, LAST_ADD_CONFIRMED, Some(master_key))
                        .await
                }
            })
            .collect();

        // Track the maximum LAC across replies — Java's
        // ReadLastConfirmedOp returns the highest LAC seen across the
        // ack quorum, not whichever bookie answered first. A single
        // replica with a stale LAC could otherwise win against the
        // up-to-date quorum.
        let ack_quorum = self.schedule.ack_quorum_size();
        let mut best_lac: i64 = current_lac;
        let mut ok_count: usize = 0;
        let mut saw_any = false;
        while let Some(result) = futs.next().await {
            if let Ok(V2Response::Read { error_code, entry_id, .. }) = result {
                let err = BKError::from_v2_code(error_code);
                match err {
                    BKError::Ok | BKError::NoEntry => {
                        saw_any = true;
                        if entry_id > best_lac {
                            best_lac = entry_id;
                        }
                        ok_count += 1;
                        if ok_count >= ack_quorum {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }

        if saw_any {
            if best_lac > current_lac {
                self.update_last_confirmed(best_lac);
                return Ok(best_lac);
            }
            // All responses came back but none exceeded current_lac.
            // Java returns NoSuchEntryException in this case.
            Err(ClientError::BookieError { error: BKError::NoEntry })
        } else {
            Err(ClientError::Protocol(
                "try_read_last_confirmed: no bookie responses".into(),
            ))
        }
    }

    /// Read entries with LAC guard: rejects reads beyond last_add_confirmed.
    /// Matches Java's `asyncReadEntries()`.
    pub async fn read_entry_confirmed(
        &self,
        conns: &[Arc<BookieConnection>],
        entry_id: i64,
    ) -> Result<Vec<u8>, ClientError> {
        let lac = self.last_add_confirmed.load(Ordering::Acquire);
        if entry_id > lac {
            return Err(ClientError::Protocol(format!(
                "read beyond LAC: entry={entry_id}, lac={lac}"
            )));
        }
        self.read_entry(conns, entry_id).await
    }

    /// Read entries without LAC guard: allows reads beyond last_add_confirmed.
    /// Matches Java's `asyncReadUnconfirmedEntries()`.
    pub async fn read_entry_unconfirmed(
        &self,
        conns: &[Arc<BookieConnection>],
        entry_id: i64,
    ) -> Result<Vec<u8>, ClientError> {
        self.read_entry(conns, entry_id).await
    }

    /// Read a single entry. With speculative reads enabled, races the primary
    /// read against speculative reads to subsequent bookies after timeouts.
    /// Matches Java's PendingReadOp.SequenceReadRequest.
    pub async fn read_entry(
        &self,
        conns: &[Arc<BookieConnection>],
        entry_id: i64,
    ) -> Result<Vec<u8>, ClientError> {
        let start = std::time::Instant::now();
        let read_seq = self.read_sequence(entry_id);

        let result = match &self.speculative_policy {
            Some(policy) if !read_seq.is_empty() => {
                self.read_entry_speculative(conns, entry_id, &read_seq, policy, None).await
            }
            _ => {
                self.read_entry_sequential(conns, entry_id, &read_seq).await
            }
        };
        crate::client_metrics::READ_ENTRY.observe(start.elapsed().as_secs_f64());
        result
    }

    /// Read a single entry with shared heard-from state for batch reads.
    async fn read_entry_with_heard_from(
        &self,
        conns: &[Arc<BookieConnection>],
        entry_id: i64,
        heard_from: &HeardFromHosts,
    ) -> Result<Vec<u8>, ClientError> {
        let read_seq = self.read_sequence(entry_id);

        match &self.speculative_policy {
            Some(policy) if !read_seq.is_empty() => {
                self.read_entry_speculative(conns, entry_id, &read_seq, policy, Some(heard_from)).await
            }
            _ => {
                self.read_entry_sequential(conns, entry_id, &read_seq).await
            }
        }
    }

    /// Get the effective read sequence for an entry, applying sticky reads
    /// and read-sequence reordering when enabled.
    /// Matches Java's `ReadOpBase.LedgerEntryRequest` constructor which calls
    /// `reorderReadSequence()` and `getWriteSetForReadOperation()`.
    fn read_sequence(&self, entry_id: i64) -> Vec<usize> {
        let mut write_set = self.schedule.write_set(entry_id);
        let ensemble = self.ensemble_at(entry_id);

        // Apply read sequence reordering: move failed bookies to end
        if self.reorder_read_sequence {
            let failed = self.failed_bookies.read().unwrap();
            if !failed.is_empty() {
                let mut healthy = Vec::new();
                let mut bad = Vec::new();
                for &idx in &write_set {
                    if idx < ensemble.len() && failed.contains(&ensemble[idx]) {
                        bad.push(idx);
                    } else {
                        healthy.push(idx);
                    }
                }
                healthy.extend(bad);
                write_set = healthy;
            }
        }

        // Sticky reads: rotate write set so sticky bookie is first
        let sticky = self.sticky_bookie_index.load(Ordering::Acquire);
        if sticky != STICKY_READ_DISABLED {
            let sticky_idx = sticky as usize;
            if let Some(pos) = write_set.iter().position(|&idx| idx == sticky_idx) {
                let mut reordered = Vec::with_capacity(write_set.len());
                reordered.extend_from_slice(&write_set[pos..]);
                reordered.extend_from_slice(&write_set[..pos]);
                write_set = reordered;
            }
        }

        write_set
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
            let conn_idx = match self.conn_index_for(entry_id, bookie_idx) {
                Some(idx) if idx < conns.len() => idx,
                _ => continue,
            };
            match self.try_read_from_conn(conns, conn_idx, entry_id).await {
                Ok(data) => return Ok(data),
                Err(e) => {
                    // On failure with sticky reads, rotate to next bookie.
                    // Matches Java's LedgerHandle.recordReadErrorOnBookie().
                    //
                    // NOTE: Java's `numBookiesMissingEntry` fast-fail
                    // (`numMissing >= wQ - aQ + 1 → NoSuchEntryException`)
                    // is **guarded by `isRecoveryRead`** in PendingReadOp —
                    // normal reads must try every write-set bookie because
                    // transient NoEntry responses (e.g., a replica still
                    // catching up after ack quorum) would otherwise cause
                    // spurious failures.  See RecoveryHandle for the
                    // recovery-read fast-fail equivalent.
                    self.record_read_error(bookie_idx, entry_id);
                    last_error = Some(e);
                    continue;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| ClientError::Protocol(format!(
            "entry not found: ledger={}, entry={}", self.ledger_id, entry_id
        ))))
    }

    /// Record a read error on a bookie. Tracks the failure for read reordering
    /// and rotates the sticky index if applicable.
    /// Matches Java's `LedgerHandle.recordReadErrorOnBookie()`.
    fn record_read_error(&self, failed_bookie_idx: usize, entry_id: i64) {
        // Track for read reordering
        let ensemble = self.ensemble_at(entry_id);
        if self.reorder_read_sequence && failed_bookie_idx < ensemble.len() {
            let mut failed = self.failed_bookies.write().unwrap();
            failed.insert(ensemble[failed_bookie_idx].clone());
        }
        // Rotate sticky index. Use write_quorum_size for the modulo so the
        // next sticky bookie lands inside any reasonable per-entry write
        // set (the write set for entry e is [e%E .. (e+W-1)%E]).
        // Rotating by ensemble_size could drift the sticky to a bookie
        // that holds no entries for the entry id we're reading, forcing
        // an extra rotation on the next failure. Matches Java's
        // LedgerHandle.recordReadErrorOnBookie which keeps sticky inside
        // the write quorum.
        let sticky = self.sticky_bookie_index.load(Ordering::Acquire);
        if sticky != STICKY_READ_DISABLED && sticky as usize == failed_bookie_idx {
            let wq = self.schedule.write_quorum_size().max(1);
            let next = (sticky as usize + 1) % wq;
            self.sticky_bookie_index.store(next as isize, Ordering::Release);
        }
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
        let (result_tx, mut result_rx) = tokio::sync::mpsc::channel::<(usize, Result<ReadResult, ClientError>)>(write_set.len());
        let completed = Arc::new(AtomicBool::new(false));

        let mut sent_count: usize = 0;
        let mut error_count: usize = 0;
        let mut next_replica: usize = 0;
        // Track which bookie indices we've sent to (for heardFrom guard)
        let mut sent_to: Vec<usize> = Vec::new();

        let spawn_read = |conn_idx: usize,
                          bookie_idx: usize,
                          conns: &[Arc<BookieConnection>],
                          tx: &tokio::sync::mpsc::Sender<(usize, Result<ReadResult, ClientError>)>,
                          done: &Arc<AtomicBool>| {
            if conn_idx >= conns.len() { return; }
            let conn = conns[conn_idx].clone();
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
            if let Some(conn_idx) = self.conn_index_for(entry_id, bookie_idx) {
                spawn_read(conn_idx, bookie_idx, conns, &result_tx, &completed);
                sent_to.push(bookie_idx);
                sent_count += 1;
            }
            next_replica += 1;
        }

        // Use absolute deadlines so timers don't reset when errors arrive
        let mut current_timeout = policy.first_timeout_ms;
        let mut next_deadline = Instant::now() + Duration::from_millis(current_timeout);

        loop {
            tokio::select! {
                result = result_rx.recv() => {
                    match result {
                        Some((bookie_idx, Ok(read_result))) => {
                            completed.store(true, Ordering::Relaxed);
                            // Mark bookie as heard-from for other entries in the batch
                            if let Some(hf) = heard_from {
                                hf.mark_heard(bookie_idx);
                            }
                            // Update piggybacked LAC
                            self.update_last_confirmed(read_result.lac);
                            return Ok(read_result.payload);
                        }
                        Some((_, Err(_))) => {
                            error_count += 1;
                            // Java: only send next when ALL sent reads have errored
                            let reads_outstanding = sent_count > error_count;
                            if !reads_outstanding && next_replica < write_set.len() {
                                let bookie_idx = write_set[next_replica];
                                if let Some(conn_idx) = self.conn_index_for(entry_id, bookie_idx) {
                                    spawn_read(conn_idx, bookie_idx, conns, &result_tx, &completed);
                                    sent_to.push(bookie_idx);
                                    sent_count += 1;
                                }
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
                    if let Some(conn_idx) = self.conn_index_for(entry_id, bookie_idx) {
                        spawn_read(conn_idx, bookie_idx, conns, &result_tx, &completed);
                        sent_to.push(bookie_idx);
                        sent_count += 1;
                    }
                    next_replica += 1;
                    current_timeout = policy.next_timeout(current_timeout);
                    next_deadline = Instant::now() + Duration::from_millis(current_timeout);
                    crate::client_metrics::SPECULATIVE_READ_COUNT.inc();
                }
            }
        }
    }

    /// Try reading a single entry from a specific connection, verify digest.
    /// Updates piggybacked LAC from the response.
    async fn try_read_from_conn(
        &self,
        conns: &[Arc<BookieConnection>],
        conn_idx: usize,
        entry_id: i64,
    ) -> Result<Vec<u8>, ClientError> {
        let result = try_read_entry(
            &conns[conn_idx],
            &self.digest_manager,
            self.ledger_id,
            entry_id,
            self.master_key,
        ).await?;
        // Update piggybacked LAC from read response.
        // Matches Java's PendingReadOp.readEntryComplete():
        //   lh.updateLastConfirmed(rctx.getLastAddConfirmed(), 0L)
        self.update_last_confirmed(result.lac);
        Ok(result.payload)
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
        let heard_from = HeardFromHosts::new(conns.len());

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

    /// Read multiple entries in a single round-trip using the BatchReadEntry protocol.
    /// Matches Java's `BatchedReadOp`. More efficient than `read_entries_parallel`
    /// for sequential reads because it avoids per-entry overhead.
    pub async fn read_entries_batch(
        &self,
        conns: &[Arc<BookieConnection>],
        start_entry_id: i64,
        max_count: i32,
        max_size: i64,
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        let write_set = self.schedule.write_set(start_entry_id);

        for &bookie_idx in &write_set {
            let conn_idx = match self.conn_index_for(start_entry_id, bookie_idx) {
                Some(idx) if idx < conns.len() => idx,
                _ => continue,
            };
            match conns[conn_idx].send_batch_read_entry(
                self.ledger_id, start_entry_id, max_count, max_size,
            ).await {
                Ok(V2Response::BatchRead { error_code, entries, .. }) if error_code == V2_EOK => {
                    let mut result = Vec::with_capacity(entries.len());
                    for (i, entry_data) in entries.into_iter().enumerate() {
                        match parse_entry(&self.digest_manager, entry_data) {
                            Ok(entry) => {
                                if entry.ledger_id != self.ledger_id {
                                    return Err(ClientError::Protocol("wrong ledger in batch".into()));
                                }
                                result.push(entry.payload.to_vec());
                            }
                            Err(_) => {
                                return Err(ClientError::Protocol(format!(
                                    "digest error on batch entry {}", start_entry_id + i as i64
                                )));
                            }
                        }
                    }
                    return Ok(result);
                }
                _ => continue,
            }
        }
        Err(ClientError::Protocol(format!(
            "batch read failed: ledger={}, start={}", self.ledger_id, start_entry_id
        )))
    }

    /// Long-poll read: block server-side until LAC advances past `previous_lac`,
    /// then return the next entry. Matches Java's `ReadLastConfirmedAndEntryOp`.
    ///
    /// This is critical for tailing reads (Pulsar managed ledger cursors):
    /// instead of polling, the client sends a request with `previous_lac` and
    /// `timeout_ms`, and the bookie blocks the response until new data arrives.
    ///
    /// Returns `(new_lac, entry_data)` or error if timeout/no-entry.
    pub async fn read_last_confirmed_and_entry(
        &self,
        conns: &[Arc<BookieConnection>],
        previous_lac: i64,
        timeout_ms: i64,
    ) -> Result<(i64, Vec<u8>), ClientError> {
        self.read_last_confirmed_and_entry_opts(conns, previous_lac, timeout_ms, false)
            .await
    }

    /// Like [`read_last_confirmed_and_entry`] but with `parallel` mode:
    /// when `parallel == true`, sends the long-poll request to ALL bookies
    /// in the write set concurrently and returns the first successful
    /// response.  When false (the default), probes bookies sequentially.
    ///
    /// Matches Java's `ReadLastConfirmedAndEntryOp` with the `parallelRead`
    /// flag, which is used by Pulsar's managed ledger for low-latency tail
    /// reads against multi-replica ensembles.
    pub async fn read_last_confirmed_and_entry_opts(
        &self,
        conns: &[Arc<BookieConnection>],
        previous_lac: i64,
        timeout_ms: i64,
        parallel: bool,
    ) -> Result<(i64, Vec<u8>), ClientError> {
        let start = std::time::Instant::now();
        let entry_id = previous_lac + 1;
        let write_set = self.schedule.write_set(entry_id);

        // Collect connection indices up front; skip bookies without mapping.
        let conn_targets: Vec<usize> = write_set
            .iter()
            .filter_map(|&bidx| self.conn_index_for(entry_id, bidx))
            .filter(|&cidx| cidx < conns.len())
            .collect();

        if parallel {
            use futures::stream::FuturesUnordered;
            let ledger_id = self.ledger_id;
            let mut futs: FuturesUnordered<_> = conn_targets
                .into_iter()
                .map(|cidx| {
                    let conn = conns[cidx].clone();
                    async move {
                        conn.send_long_poll_read(ledger_id, entry_id, previous_lac, timeout_ms)
                            .await
                    }
                })
                .collect();

            while let Some(result) = futs.next().await {
                if let Ok(V2Response::Read { error_code, entry_id: resp_eid, data, .. }) = result {
                    if error_code == V2_EOK {
                        if let Some(entry_data) = data {
                            if let Ok(entry) = parse_entry(&self.digest_manager, entry_data) {
                                crate::client_metrics::READ_LAST_CONFIRMED_AND_ENTRY
                                    .observe(start.elapsed().as_secs_f64());
                                return Ok((resp_eid, entry.payload.to_vec()));
                            }
                        }
                    }
                }
            }
        } else {
            for cidx in conn_targets {
                match conns[cidx]
                    .send_long_poll_read(self.ledger_id, entry_id, previous_lac, timeout_ms)
                    .await
                {
                    Ok(V2Response::Read { error_code, entry_id: resp_eid, data, .. }) => {
                        if error_code == V2_EOK {
                            if let Some(entry_data) = data {
                                if let Ok(entry) = parse_entry(&self.digest_manager, entry_data) {
                                    crate::client_metrics::READ_LAST_CONFIRMED_AND_ENTRY
                                        .observe(start.elapsed().as_secs_f64());
                                    return Ok((resp_eid, entry.payload.to_vec()));
                                }
                            }
                        }
                    }
                    _ => continue,
                }
            }
        }

        crate::client_metrics::READ_LAST_CONFIRMED_AND_ENTRY
            .observe(start.elapsed().as_secs_f64());
        Err(ClientError::BookieError { error: BKError::NoEntry })
    }
}

/// Read result carrying both the payload and piggybacked LAC.
struct ReadResult {
    payload: Vec<u8>,
    /// Piggybacked LAC from the entry metadata.
    lac: i64,
}

/// Standalone read+verify function used by both sequential and speculative paths.
/// Returns typed `ClientError::BookieError` for bookie-level failures, matching
/// Java's `ReadCompletion.handleReadResponse()` which maps StatusCode to
/// typed BKException via `statusCodeToExceptionCode()`.
async fn try_read_entry(
    conn: &BookieConnection,
    dm: &DigestManager,
    ledger_id: i64,
    entry_id: i64,
    master_key: [u8; MASTER_KEY_LENGTH],
) -> Result<ReadResult, ClientError> {
    match conn.send_read_entry(0, ledger_id, entry_id, Some(master_key)).await {
        Ok(V2Response::Read { error_code, data, .. }) => {
            if error_code == V2_EOK {
                if let Some(entry_data) = data {
                    match parse_entry(dm, entry_data) {
                        Ok(entry) => {
                            if entry.ledger_id != ledger_id || entry.entry_id != entry_id {
                                return Err(ClientError::Protocol("wrong entry".into()));
                            }
                            return Ok(ReadResult {
                                payload: entry.payload.to_vec(),
                                lac: entry.lac,
                            });
                        }
                        Err(EntryError::DigestMismatch) => {
                            return Err(ClientError::Protocol("digest mismatch".into()));
                        }
                        Err(e) => {
                            return Err(ClientError::Protocol(format!("entry parse: {e}")));
                        }
                    }
                }
                return Err(ClientError::Protocol("EOK but no data".into()));
            }
            Err(ClientError::BookieError {
                error: BKError::from_v2_code(error_code),
            })
        }
        Ok(_) => Err(ClientError::Protocol("unexpected response".into())),
        Err(e) => Err(e),
    }
}
