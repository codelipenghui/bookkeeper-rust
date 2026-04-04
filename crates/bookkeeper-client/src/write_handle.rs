use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use bytes::Bytes;
use futures::future::join_all;
use tokio::sync::oneshot;

use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::DigestManager;
use bookkeeper_protocol::v2::response::V2Response;
use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;

// ---------------------------------------------------------------------------
// EnsembleInfo — ensemble segments, changingEnsemble flag, delayed failures
// ---------------------------------------------------------------------------

/// Tracks ensemble segments (firstEntryId -> ensemble) and serializes
/// concurrent ensemble changes via the `changing_ensemble` flag.
/// Matches Java BookKeeper's ensemble change bookkeeping.
pub struct EnsembleInfo {
    /// Ensemble segments: maps firstEntryId to the ensemble active from that entry.
    /// There is always at least one entry (key 0 = initial ensemble).
    segments: std::sync::Mutex<BTreeMap<i64, Vec<String>>>,
    /// Whether an ensemble change is currently in progress. Gates LAC
    /// advancement and causes new failures to be queued in `delayed_failures`.
    changing_ensemble: AtomicBool,
    /// Failures accumulated while `changing_ensemble` is true.
    /// Each entry is (bookie_index, failed_bookie_addr).
    delayed_failures: std::sync::Mutex<Vec<(usize, String)>>,
}

impl EnsembleInfo {
    fn new(initial_ensemble: Vec<String>) -> Self {
        let mut segments = BTreeMap::new();
        segments.insert(0, initial_ensemble);
        Self {
            segments: std::sync::Mutex::new(segments),
            changing_ensemble: AtomicBool::new(false),
            delayed_failures: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Return the current (latest) ensemble.
    fn current_ensemble(&self) -> Vec<String> {
        let segs = self.segments.lock().unwrap();
        segs.values().next_back().cloned().unwrap_or_default()
    }

    /// Add a new ensemble segment starting at `first_entry_id`.
    fn add_segment(&self, first_entry_id: i64, ensemble: Vec<String>) {
        let mut segs = self.segments.lock().unwrap();
        segs.insert(first_entry_id, ensemble);
    }

    /// Return a snapshot of all segments.
    pub fn segments(&self) -> BTreeMap<i64, Vec<String>> {
        self.segments.lock().unwrap().clone()
    }
}

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
    /// If `changing_ensemble` is true, the entry is recorded as completed but
    /// the atomic LAC value is NOT advanced — matching Java's gating of
    /// `sendAddSuccessCallbacks()` with `!changingEnsemble`.
    fn advance_lac(&self, entry_id: i64, lac_value: &AtomicI64, ensemble_info: &EnsembleInfo) {
        let mut completed = self.completed.lock().unwrap();
        completed.insert(entry_id);

        if ensemble_info.changing_ensemble.load(Ordering::Acquire) {
            // Gate: don't advance LAC during ensemble change
            return;
        }

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

/// Callback invoked when an ensemble change occurs.
/// Arguments: (first_entry_id_of_new_segment, new_ensemble).
/// The caller can use this to perform a metadata CAS update (e.g. in ZooKeeper).
pub type OnEnsembleChange = Box<dyn Fn(i64, &[String]) + Send + Sync>;

pub struct WriteHandle {
    ledger_id: i64,
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
    /// Ensemble segments, changing flag, and delayed failures.
    ensemble_info: Arc<EnsembleInfo>,
    /// Optional callback for metadata CAS update on ensemble change.
    on_ensemble_change: Option<OnEnsembleChange>,
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
            available_bookies,
            schedule,
            digest_manager,
            master_key,
            last_add_pushed: -1,
            current_length: 0,
            lac_value: Arc::new(AtomicI64::new(-1)),
            lac_state: Arc::new(LacState::new()),
            ensemble_info: Arc::new(EnsembleInfo::new(ensemble)),
            on_ensemble_change: None,
        }
    }

    /// Set the callback invoked on ensemble change for metadata CAS integration.
    pub fn set_on_ensemble_change(&mut self, cb: OnEnsembleChange) {
        self.on_ensemble_change = Some(cb);
    }

    /// Return the current last-add-confirmed value.
    pub fn last_add_confirmed(&self) -> i64 {
        self.lac_value.load(Ordering::Acquire)
    }

    pub fn ledger_id(&self) -> i64 {
        self.ledger_id
    }

    pub fn current_length(&self) -> i64 {
        self.current_length
    }

    pub fn last_add_pushed(&self) -> i64 {
        self.last_add_pushed
    }

    /// Return the current (latest) ensemble.
    pub fn ensemble(&self) -> Vec<String> {
        self.ensemble_info.current_ensemble()
    }

    /// Return all ensemble segments (firstEntryId -> ensemble).
    pub fn ensemble_segments(&self) -> BTreeMap<i64, Vec<String>> {
        self.ensemble_info.segments()
    }

    /// Replace the bookie at `failed_bookie_idx` with one from the available pool
    /// that is not already in the current ensemble. Connects to the replacement and
    /// updates `conns` in-place.
    pub async fn handle_bookie_failure(
        &mut self,
        conns: &mut Vec<Arc<BookieConnection>>,
        failed_bookie_idx: usize,
    ) -> Result<(), ClientError> {
        let current_ensemble = self.ensemble_info.current_ensemble();
        let current_set: std::collections::HashSet<&str> =
            current_ensemble.iter().map(|s| s.as_str()).collect();

        let replacement = self
            .available_bookies
            .iter()
            .find(|b| !current_set.contains(b.as_str()))
            .ok_or_else(|| ClientError::Protocol("no available replacement bookie".into()))?
            .clone();

        let addr: std::net::SocketAddr = replacement
            .parse()
            .map_err(|e| ClientError::Protocol(format!("bad replacement addr: {e}")))?;
        let new_conn = Arc::new(BookieConnection::connect(addr).await?);

        // Update ensemble in the current (latest) segment.
        {
            let mut segs = self.ensemble_info.segments.lock().unwrap();
            if let Some((_key, ens)) = segs.iter_mut().next_back() {
                if failed_bookie_idx < ens.len() {
                    ens[failed_bookie_idx] = replacement;
                }
            }
        }

        // Update connections.
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
        let ensemble_info = Arc::clone(&self.ensemble_info);

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

            let mut last_err: Option<ClientError> = None;

            // Process ALL results — don't break on quorum so we can detect
            // post-quorum failures for proactive ensemble change.
            // Matches Java's PendingAddOp.writeComplete() which reports
            // failures via notifyWriteFailed even after quorum is reached.
            for (ensemble_idx, result) in results {
                match result {
                    Ok(V2Response::Add { error_code, .. }) if error_code == V2_EOK => {
                        ack_count += 1;
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
            }

            let outcome = if ack_count >= ack_quorum {
                // Quorum reached — but report post-quorum failures proactively.
                // Matches Java's lh.notifyWriteFailed(bookieIndex, addr) which
                // adds to delayedWriteFailedBookies even when the write succeeded.
                if !failed_indices.is_empty() {
                    let current_ens = ensemble_info.current_ensemble();
                    let mut delayed = ensemble_info.delayed_failures.lock().unwrap();
                    for &idx in &failed_indices {
                        if idx < current_ens.len() {
                            delayed.push((idx, current_ens[idx].clone()));
                        }
                    }
                }
                lac_state.advance_lac(entry_id, &lac_value, &ensemble_info);
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
    /// this method:
    /// 1. Sets the `changing_ensemble` flag (gates LAC advancement)
    /// 2. Replaces each failed bookie with a spare from `available_bookies`
    /// 3. Adds a new ensemble segment at LAC + 1
    /// 4. Invokes the `on_ensemble_change` callback (for metadata CAS)
    /// 5. Clears `changing_ensemble` and flushes queued LAC entries
    /// 6. Processes any delayed failures accumulated during the change
    /// 7. Retries the failed write
    ///
    /// Matches Java BookKeeper's `handleBookieFailure`/`ensembleChangeLoop` behavior.
    pub async fn append_with_ensemble_change(
        &mut self,
        conns: &mut Vec<Arc<BookieConnection>>,
        payload: &[u8],
    ) -> Result<i64, ClientError> {
        // Proactive ensemble change: process any delayed failures from previous
        // writes before attempting the next one.
        // Matches Java's maybeHandleDelayedWriteBookieFailure().
        self.maybe_handle_delayed_failures(conns).await?;

        // First attempt.
        let result = self.append_async(conns, payload)?.wait().await;

        match result {
            Ok(entry_id) => Ok(entry_id),
            Err(ClientError::WriteQuorumFailed { failed_indices, .. }) => {
                // Step 1: Set changing_ensemble flag — gates LAC advancement
                self.ensemble_info.changing_ensemble.store(true, Ordering::Release);

                // Step 2: Replace every failed bookie
                for idx in &failed_indices {
                    self.handle_bookie_failure(conns, *idx).await?;
                }

                // Step 3: Add new ensemble segment at LAC + 1
                let new_segment_start = self.last_add_confirmed() + 1;
                let new_ensemble = self.ensemble_info.current_ensemble();
                self.ensemble_info.add_segment(new_segment_start, new_ensemble.clone());

                // Step 4: Invoke metadata callback if set
                if let Some(ref cb) = self.on_ensemble_change {
                    cb(new_segment_start, &new_ensemble);
                }

                // Step 5: Clear changing_ensemble and flush queued LAC entries
                self.ensemble_info.changing_ensemble.store(false, Ordering::Release);
                self.flush_lac();

                // Step 6: Process any delayed failures accumulated during the change
                let delayed = {
                    let mut df = self.ensemble_info.delayed_failures.lock().unwrap();
                    let d = df.clone();
                    df.clear();
                    d
                };
                if !delayed.is_empty() {
                    // Recursive handling: set flag again and process
                    self.ensemble_info.changing_ensemble.store(true, Ordering::Release);
                    for (idx, _addr) in &delayed {
                        self.handle_bookie_failure(conns, *idx).await?;
                    }
                    let seg_start = self.last_add_confirmed() + 1;
                    let ens = self.ensemble_info.current_ensemble();
                    self.ensemble_info.add_segment(seg_start, ens.clone());
                    if let Some(ref cb) = self.on_ensemble_change {
                        cb(seg_start, &ens);
                    }
                    self.ensemble_info.changing_ensemble.store(false, Ordering::Release);
                    self.flush_lac();
                }

                // Step 7: Retry the failed write
                self.last_add_pushed -= 1;
                self.current_length -= payload.len() as i64;
                self.append_async(conns, payload)?.wait().await
            }
            Err(e) => Err(e),
        }
    }

    /// Process any delayed bookie failures accumulated by post-quorum failure
    /// tracking. If there are failures, trigger a proactive ensemble change
    /// BEFORE the next write attempt.
    /// Matches Java's `maybeHandleDelayedWriteBookieFailure()`.
    async fn maybe_handle_delayed_failures(
        &mut self,
        conns: &mut Vec<Arc<BookieConnection>>,
    ) -> Result<(), ClientError> {
        let delayed = {
            let mut df = self.ensemble_info.delayed_failures.lock().unwrap();
            if df.is_empty() {
                return Ok(());
            }
            let d = df.clone();
            df.clear();
            d
        };

        // Set changing_ensemble flag — gates LAC advancement
        self.ensemble_info.changing_ensemble.store(true, Ordering::Release);

        for (idx, _addr) in &delayed {
            self.handle_bookie_failure(conns, *idx).await?;
        }

        // Add new ensemble segment at LAC + 1
        let new_start = self.last_add_confirmed() + 1;
        let current_ens = self.ensemble_info.current_ensemble();
        self.ensemble_info.add_segment(new_start, current_ens.clone());

        if let Some(ref cb) = self.on_ensemble_change {
            cb(new_start, &current_ens);
        }

        // Clear flag and flush queued LAC entries
        self.ensemble_info.changing_ensemble.store(false, Ordering::Release);
        self.flush_lac();

        Ok(())
    }

    /// Drain the completed set and advance the atomic LAC value.
    /// Called after ensemble change completes to flush entries that were
    /// gated during `changing_ensemble = true`.
    fn flush_lac(&self) {
        let mut completed = self.lac_state.completed.lock().unwrap();
        let mut current_lac = self.lac_value.load(Ordering::Acquire);
        while completed.contains(&(current_lac + 1)) {
            current_lac += 1;
            completed.remove(&current_lac);
        }
        self.lac_value.store(current_lac, Ordering::Release);
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
