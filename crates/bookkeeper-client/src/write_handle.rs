use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::join_all;
use tokio::sync::{oneshot, Mutex};

use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::DigestManager;
use bookkeeper_protocol::v2::response::V2Response;
use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;

// ---------------------------------------------------------------------------
// LacState — shared, ordered LAC advancement
// ---------------------------------------------------------------------------

struct LacState {
    last_add_confirmed: i64,
    /// Entries that have reached ack quorum but whose predecessors haven't yet.
    completed: BTreeSet<i64>,
}

impl LacState {
    fn new() -> Self {
        Self { last_add_confirmed: -1, completed: BTreeSet::new() }
    }

    /// Mark `entry_id` as complete and advance LAC as far as possible in order.
    fn advance_lac(&mut self, entry_id: i64) {
        self.completed.insert(entry_id);
        while self.completed.contains(&(self.last_add_confirmed + 1)) {
            self.last_add_confirmed += 1;
            self.completed.remove(&self.last_add_confirmed);
        }
    }

    fn lac(&self) -> i64 {
        self.last_add_confirmed
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
    schedule: RoundRobinDistributionSchedule,
    digest_manager: DigestManager,
    master_key: [u8; MASTER_KEY_LENGTH],
    last_add_pushed: i64,
    current_length: i64,
    /// Shared LAC state — updated by spawned ack-collector tasks.
    lac_state: Arc<Mutex<LacState>>,
}

impl WriteHandle {
    pub fn new(
        ledger_id: i64,
        ensemble: Vec<String>,
        schedule: RoundRobinDistributionSchedule,
        digest_manager: DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Self {
        Self {
            ledger_id,
            ensemble,
            schedule,
            digest_manager,
            master_key,
            last_add_pushed: -1,
            current_length: 0,
            lac_state: Arc::new(Mutex::new(LacState::new())),
        }
    }

    /// Return the current last-add-confirmed value.
    pub fn last_add_confirmed(&self) -> i64 {
        // We need a sync read. Use `blocking_lock` is not available in async
        // contexts without care, but `try_lock` works fine here since we only
        // read a single integer and contention is transient.
        self.lac_state.try_lock()
            .map(|g| g.lac())
            .unwrap_or_else(|_| {
                // Fall back to a blocking acquire on a new thread is overkill;
                // instead spin briefly — the lock is held for nanoseconds.
                loop {
                    if let Ok(g) = self.lac_state.try_lock() {
                        return g.lac();
                    }
                }
            })
    }

    pub fn ledger_id(&self) -> i64 {
        self.ledger_id
    }

    pub fn ensemble(&self) -> &[String] {
        &self.ensemble
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
        let lac_state = Arc::clone(&self.lac_state);

        // Clone Arc<BookieConnection> handles for the spawned task
        let conn_handles: Vec<Arc<BookieConnection>> = write_set
            .iter()
            .map(|&i| Arc::clone(&conns[i]))
            .collect();

        // Build per-bookie futures (each tagged with its write-set position)
        let futures: Vec<_> = conn_handles
            .into_iter()
            .enumerate()
            .map(|(pos, conn)| {
                let entry = entry_bytes.clone();
                async move { (pos, conn.send_add_entry(0, mk, entry).await) }
            })
            .collect();

        let (tx, rx) = oneshot::channel::<Result<i64, ClientError>>();

        tokio::spawn(async move {
            let results = join_all(futures).await;

            let mut ack_count = 0usize;
            let mut fail_count = 0usize;
            let write_quorum = results.len();
            let max_failures = write_quorum.saturating_sub(ack_quorum);

            let mut last_err: Option<ClientError> = None;

            for (_pos, result) in results {
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
                        fail_count += 1;
                    }
                    Ok(_) => {
                        last_err = Some(ClientError::Protocol(
                            "unexpected response type".into(),
                        ));
                        fail_count += 1;
                    }
                    Err(e) => {
                        last_err = Some(e);
                        fail_count += 1;
                    }
                }
                if fail_count > max_failures {
                    break;
                }
            }

            let outcome = if ack_count >= ack_quorum {
                lac_state.lock().await.advance_lac(entry_id);
                Ok(entry_id)
            } else {
                Err(last_err.unwrap_or_else(|| {
                    ClientError::Protocol("failed to reach ack quorum".into())
                }))
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

    fn build_entry(&self, entry_id: i64, payload: &[u8]) -> Bytes {
        use bookkeeper_protocol::entry::EntryBuilder;
        // LAC embedded in the entry is the current confirmed value at build time.
        let lac = self.lac_state.try_lock().map(|g| g.lac()).unwrap_or(-1);
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
