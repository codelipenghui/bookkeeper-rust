use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::Bytes;
use futures::future::join_all;
use tokio::sync::oneshot;

use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::DigestManager;
use bookkeeper_protocol::v2::response::V2Response;
use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;

// ---------------------------------------------------------------------------
// LacState — shared, ordered LAC advancement
// ---------------------------------------------------------------------------

struct LacState {
    /// Entries that have reached ack quorum but whose predecessors haven't yet.
    completed: std::sync::Mutex<BTreeSet<i64>>,
}

impl LacState {
    fn new() -> Self {
        Self { completed: std::sync::Mutex::new(BTreeSet::new()) }
    }

    /// Mark `entry_id` as complete and advance LAC as far as possible in order.
    fn advance_lac(&self, entry_id: i64, lac_value: &AtomicI64) {
        let mut completed = self.completed.lock().unwrap();
        completed.insert(entry_id);
        let mut current_lac = lac_value.load(Ordering::Acquire);
        while completed.contains(&(current_lac + 1)) {
            current_lac += 1;
            completed.remove(&current_lac);
        }
        lac_value.store(current_lac, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// PendingAdd — future that resolves when ack quorum is reached
// ---------------------------------------------------------------------------

/// A handle to an in-flight append operation.
/// Resolves to the entry ID when ack quorum is reached.
pub struct PendingAdd {
    rx: oneshot::Receiver<Result<i64, ClientError>>,
}

impl PendingAdd {
    /// Await the result of this append.
    pub async fn wait(self) -> Result<i64, ClientError> {
        self.rx.await.map_err(|_| ClientError::ConnectionClosed)?
    }
}

// ---------------------------------------------------------------------------
// WriteHandle
// ---------------------------------------------------------------------------

pub struct WriteHandle {
    ledger_id: i64,
    ensemble: Vec<String>,
    /// All bookies available in the pool, used to find replacements on failure.
    available_bookies: Vec<String>,
    schedule: RoundRobinDistributionSchedule,
    digest_manager: DigestManager,
    master_key: [u8; MASTER_KEY_LENGTH],
    last_add_pushed: i64,
    current_length: i64,
    /// Lock-free LAC value — updated by spawned ack-collector tasks.
    lac_value: Arc<AtomicI64>,
    /// Completion tracking for out-of-order acks.
    lac_state: Arc<LacState>,
}

impl WriteHandle {
    pub fn new(
        ledger_id: i64,
        ensemble: Vec<String>,
        schedule: RoundRobinDistributionSchedule,
        digest_manager: DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Self {
        Self::with_available_bookies(ledger_id, ensemble.clone(), ensemble, schedule, digest_manager, master_key)
    }

    /// Create a WriteHandle with an explicit pool of available bookies for replacements.
    pub fn with_available_bookies(
        ledger_id: i64,
        ensemble: Vec<String>,
        available_bookies: Vec<String>,
        schedule: RoundRobinDistributionSchedule,
        digest_manager: DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Self {
        Self {
            ledger_id,
            ensemble,
            available_bookies,
            schedule,
            digest_manager,
            master_key,
            last_add_pushed: -1,
            current_length: 0,
            lac_value: Arc::new(AtomicI64::new(-1)),
            lac_state: Arc::new(LacState::new()),
        }
    }

    /// Return the current last-add-confirmed value.
    pub fn last_add_confirmed(&self) -> i64 {
        self.lac_value.load(Ordering::Acquire)
    }

    pub fn ledger_id(&self) -> i64 {
        self.ledger_id
    }

    pub fn ensemble(&self) -> &[String] {
        &self.ensemble
    }

    /// Replace the bookie at `failed_bookie_idx` with one from the available pool
    /// that is not already in the current ensemble. Connects to the replacement and
    /// updates `conns` in-place.
    pub async fn handle_bookie_failure(
        &mut self,
        conns: &mut Vec<Arc<BookieConnection>>,
        failed_bookie_idx: usize,
    ) -> Result<(), ClientError> {
        let current_ensemble: std::collections::HashSet<&str> =
            self.ensemble.iter().map(|s| s.as_str()).collect();

        let replacement = self
            .available_bookies
            .iter()
            .find(|b| !current_ensemble.contains(b.as_str()))
            .ok_or_else(|| ClientError::Protocol("no available replacement bookie".into()))?
            .clone();

        let addr: std::net::SocketAddr = replacement
            .parse()
            .map_err(|e| ClientError::Protocol(format!("bad replacement addr: {e}")))?;
        let new_conn = Arc::new(BookieConnection::connect(addr).await?);

        // Update ensemble and connections.
        if failed_bookie_idx < self.ensemble.len() {
            self.ensemble[failed_bookie_idx] = replacement;
        }
        if failed_bookie_idx < conns.len() {
            conns[failed_bookie_idx] = new_conn;
        } else {
            conns.push(new_conn);
        }

        Ok(())
    }

    /// Non-blocking append. Sends to write quorum and returns immediately.
    /// The returned [`PendingAdd`] resolves when ack quorum is reached.
    /// LAC advances in entry-ID order across all in-flight appends.
    pub fn append_async(
        &mut self,
        conns: &[Arc<BookieConnection>],
        payload: &[u8],
    ) -> Result<PendingAdd, ClientError> {
        let entry_id = self.last_add_pushed + 1;
        self.last_add_pushed = entry_id;
        self.current_length += payload.len() as i64;

        // Build entry bytes (includes digest, lac, etc.)
        let entry_bytes = self.build_entry(entry_id, payload);

        // Compute write set
        let write_set = self.schedule.write_set(entry_id);

        // Validate bookie indices upfront
        for &bookie_idx in &write_set {
            if bookie_idx >= conns.len() {
                return Err(ClientError::Protocol(format!(
                    "bookie index {bookie_idx} out of range (have {} connections)",
                    conns.len()
                )));
            }
        }

        let ack_quorum = self.schedule.ack_quorum_size();
        let mk = self.master_key;
        let lac_value = Arc::clone(&self.lac_value);
        let lac_state = Arc::clone(&self.lac_state);

        // Clone Arc<BookieConnection> handles for the spawned task, keeping the
        // write-set position alongside each handle so we can report which indices failed.
        let conn_handles: Vec<(usize, Arc<BookieConnection>)> = write_set
            .iter()
            .enumerate()
            .map(|(pos, &ensemble_idx)| (ensemble_idx, Arc::clone(&conns[pos])))
            .collect();

        let (tx, rx) = oneshot::channel::<Result<i64, ClientError>>();

        tokio::spawn(async move {
            // Build futures tagged with their ensemble index.
            let futures: Vec<_> = conn_handles
                .into_iter()
                .map(|(ensemble_idx, conn)| {
                    let entry = entry_bytes.clone();
                    async move { (ensemble_idx, conn.send_add_entry(0, mk, entry).await) }
                })
                .collect();

            let results = join_all(futures).await;

            let mut ack_count = 0usize;
            let mut failed_indices: Vec<usize> = Vec::new();
            let write_quorum = results.len();
            let max_failures = write_quorum.saturating_sub(ack_quorum);

            let mut last_err: Option<ClientError> = None;

            for (ensemble_idx, result) in results {
                match result {
                    Ok(V2Response::Add { error_code, .. }) if error_code == V2_EOK => {
                        ack_count += 1;
                        if ack_count >= ack_quorum {
                            break;
                        }
                    }
                    Ok(V2Response::Add { error_code, .. }) => {
                        last_err = Some(ClientError::Protocol(format!(
                            "bookie error_code={error_code}"
                        )));
                        failed_indices.push(ensemble_idx);
                    }
                    Ok(_) => {
                        last_err = Some(ClientError::Protocol(
                            "unexpected response type".into(),
                        ));
                        failed_indices.push(ensemble_idx);
                    }
                    Err(e) => {
                        last_err = Some(e);
                        failed_indices.push(ensemble_idx);
                    }
                }
                if failed_indices.len() > max_failures {
                    break;
                }
            }

            let outcome = if ack_count >= ack_quorum {
                lac_state.advance_lac(entry_id, &lac_value);
                Ok(entry_id)
            } else {
                let message = last_err
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "failed to reach ack quorum".into());
                Err(ClientError::WriteQuorumFailed {
                    message,
                    failed_indices,
                    entry_id,
                })
            };

            let _ = tx.send(outcome);
        });

        Ok(PendingAdd { rx })
    }

    /// Blocking append — fires writes and waits for ack quorum before returning.
    /// Preserved for backward compatibility; internally uses [`append_async`].
    pub async fn append(
        &mut self,
        conns: &[Arc<BookieConnection>],
        payload: &[u8],
    ) -> Result<i64, ClientError> {
        self.append_async(conns, payload)?.wait().await
    }

    /// Append with automatic ensemble change on bookie failure.
    ///
    /// If the write quorum cannot be reached because one or more bookies fail,
    /// this method replaces each failed bookie with a spare from `available_bookies`,
    /// reconnects, and retries the write once. The `conns` slice is updated in-place
    /// to reflect the new ensemble.
    pub async fn append_with_ensemble_change(
        &mut self,
        conns: &mut Vec<Arc<BookieConnection>>,
        payload: &[u8],
    ) -> Result<i64, ClientError> {
        // First attempt.
        let result = self.append_async(conns, payload)?.wait().await;

        match result {
            Ok(entry_id) => return Ok(entry_id),
            Err(ClientError::WriteQuorumFailed { failed_indices, .. }) => {
                // Replace every failed bookie, then retry.
                for idx in failed_indices {
                    self.handle_bookie_failure(conns, idx).await?;
                }
                // Retry: roll back last_add_pushed so append_async assigns the same entry_id.
                self.last_add_pushed -= 1;
                self.current_length -= payload.len() as i64;
                self.append_async(conns, payload)?.wait().await
            }
            Err(e) => Err(e),
        }
    }

    fn build_entry(&self, entry_id: i64, payload: &[u8]) -> Bytes {
        use bookkeeper_protocol::entry::EntryBuilder;
        // LAC embedded in the entry is the current confirmed value at build time.
        let lac = self.lac_value.load(Ordering::Acquire);
        EntryBuilder::new(&self.digest_manager)
            .ledger_id(self.ledger_id)
            .entry_id(entry_id)
            .lac(lac)
            .length(self.current_length)
            .payload(Bytes::copy_from_slice(payload))
            .build()
            .freeze()
    }
}
