use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

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
    /// In-flight entries: maps entry_id -> pre-built entry bytes.
    /// Used by `unset_success_and_resend` after ensemble change to re-send
    /// entries that were targeting the failed bookie.
    /// Matches Java's `PendingAddOp` queue iterated by `unsetSuccessAndSendWriteRequest`.
    in_flight_entries: std::sync::Mutex<BTreeMap<i64, Bytes>>,
    /// Per-entry shared quorum-tracking context. The resend path
    /// looks up the context here so its result feeds the SAME ack
    /// counter the original submission's per-bookie tasks are
    /// updating. Mirrors the `PendingAddOp` instance Java reuses
    /// across `sendWriteRequest` calls.
    pending_acks: std::sync::Mutex<BTreeMap<i64, Arc<PendingAddCtx>>>,
}

impl EnsembleInfo {
    fn new(initial_ensemble: Vec<String>) -> Self {
        let mut segments = BTreeMap::new();
        segments.insert(0, initial_ensemble);
        Self {
            segments: std::sync::Mutex::new(segments),
            changing_ensemble: AtomicBool::new(false),
            delayed_failures: std::sync::Mutex::new(Vec::new()),
            in_flight_entries: std::sync::Mutex::new(BTreeMap::new()),
            pending_acks: std::sync::Mutex::new(BTreeMap::new()),
        }
    }

    fn register_pending_ctx(&self, entry_id: i64, ctx: Arc<PendingAddCtx>) {
        self.pending_acks.lock().unwrap().insert(entry_id, ctx);
    }

    fn remove_pending_ctx(&self, entry_id: i64) {
        self.pending_acks.lock().unwrap().remove(&entry_id);
    }

    fn get_pending_ctx(&self, entry_id: i64) -> Option<Arc<PendingAddCtx>> {
        self.pending_acks.lock().unwrap().get(&entry_id).cloned()
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

    /// Push a delayed bookie failure onto the queue. Test-only helper; the
    /// production code populates this from the per-write completion paths
    /// (post-quorum failures and pre-quorum partial failures).
    #[doc(hidden)]
    pub fn push_delayed_failure_for_test(&self, idx: usize, addr: String) {
        self.delayed_failures.lock().unwrap().push((idx, addr));
    }

    /// Register an in-flight entry for potential re-send after ensemble change.
    fn register_in_flight(&self, entry_id: i64, entry_bytes: Bytes) {
        self.in_flight_entries.lock().unwrap().insert(entry_id, entry_bytes);
    }

    /// Remove a completed entry from the in-flight registry.
    fn remove_in_flight(&self, entry_id: i64) {
        self.in_flight_entries.lock().unwrap().remove(&entry_id);
    }

    /// Return all in-flight entries whose write set includes any of the
    /// failed bookie indices, along with their pre-built entry bytes.
    fn in_flight_for_failed_bookies(
        &self,
        failed_indices: &[usize],
        schedule: &RoundRobinDistributionSchedule,
    ) -> Vec<(i64, Bytes)> {
        let entries = self.in_flight_entries.lock().unwrap();
        entries.iter()
            .filter(|(entry_id, _)| {
                let ws = schedule.write_set(**entry_id);
                failed_indices.iter().any(|fi| ws.contains(fi))
            })
            .map(|(eid, data)| (*eid, data.clone()))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// LacState — shared, ordered LAC advancement + ordered callback signaling
// ---------------------------------------------------------------------------
//
// Java's `LedgerHandle.sendAddSuccessCallbacks` (in `LedgerHandle.java`)
// runs on the OrderedExecutor thread for the ledger.  It walks
// `pendingAddOps` in entry-ID order, advances `lastAddConfirmed`, AND
// invokes the user callback — all under that single thread.  This makes
// the invariant hold trivially: "when the callback for entry N fires,
// LAC ≥ N".
//
// The Rust implementation (Option C) preserves the same invariant without
// a dedicated executor thread: the oneshot `Sender` for each PendingAdd
// is stored inside `LacState` when the add is submitted, and drained in
// LAC-advancement order inside the `advance_lac` critical section.  This
// guarantees that `p.wait().await` only resolves once `lac_value` has
// reached (or exceeded) the entry ID.

/// Per-entry signaling slot — the sender end of a `PendingAdd`'s oneshot.
type PendingTx = oneshot::Sender<Result<i64, ClientError>>;

struct CompletedState {
    /// Entries that have reached ack quorum but whose predecessors haven't yet.
    acked: BTreeSet<i64>,
    /// Pending per-entry signaling slots, keyed by entry ID.  Drained in
    /// LAC-advancement order so the caller sees callbacks fire in the
    /// same order as Java's `sendAddSuccessCallbacks`.
    pending: BTreeMap<i64, PendingTx>,
}

struct LacState {
    state: std::sync::Mutex<CompletedState>,
}

impl LacState {
    fn new() -> Self {
        Self {
            state: std::sync::Mutex::new(CompletedState {
                acked: BTreeSet::new(),
                pending: BTreeMap::new(),
            }),
        }
    }

    /// Register a new in-flight add's signaling slot at submission time.
    /// The returned `PendingAdd` wraps the receiver end.
    fn register(&self, entry_id: i64) -> PendingAdd {
        let (tx, rx) = oneshot::channel();
        self.state.lock().unwrap().pending.insert(entry_id, tx);
        PendingAdd { rx }
    }

    /// Signal success for `entry_id` and advance LAC as far as possible.
    ///
    /// Behavior mirrors Java's `sendAddSuccessCallbacks`:
    /// - `deferred_sync`: signal immediately on ack; LAC advances only on
    ///   `force()` (Java's `WriteFlag.DEFERRED_SYNC` guard on `lastAddConfirmed`).
    /// - `changing_ensemble`: signal nothing; entries are held back until
    ///   `flush_after_ensemble_change` is called post-change (Java's
    ///   `!changingEnsemble` predicate).
    /// - Otherwise: walk acked set; each newly-LAC'd entry's sender fires
    ///   in strict entry-ID order *after* `lac_value` has been updated.
    fn signal_success(
        &self,
        entry_id: i64,
        lac_value: &AtomicI64,
        ensemble_info: &EnsembleInfo,
        deferred_sync: bool,
    ) {
        let to_signal = {
            let mut st = self.state.lock().unwrap();
            st.acked.insert(entry_id);

            if deferred_sync {
                // Deferred sync: fire callback immediately on ack quorum.
                // LAC does NOT advance here — it will catch up when the
                // user calls force().
                st.pending.remove(&entry_id).map(|tx| vec![(entry_id, tx)]).unwrap_or_default()
            } else if ensemble_info.changing_ensemble.load(Ordering::Acquire) {
                // Gated: ensemble change in progress.  The caller will
                // invoke `flush_after_ensemble_change` once the change
                // commits, which drains the held-back callbacks.
                Vec::new()
            } else {
                Self::drain_in_lac_order(&mut st, lac_value)
            }
        };
        for (eid, tx) in to_signal {
            let _ = tx.send(Ok(eid));
        }
    }

    /// Signal failure for a specific entry.  Does NOT touch the acked
    /// set or LAC — the failure is surfaced directly to `PendingAdd::wait`.
    fn signal_error(&self, entry_id: i64, err: ClientError) {
        let tx = self.state.lock().unwrap().pending.remove(&entry_id);
        if let Some(tx) = tx {
            let _ = tx.send(Err(err));
        }
    }

    /// Drain any held-back signals after an ensemble change completes.
    /// Called from `flush_after_ensemble_change`.
    fn flush_after_ensemble_change(&self, lac_value: &AtomicI64) {
        let to_signal = {
            let mut st = self.state.lock().unwrap();
            Self::drain_in_lac_order(&mut st, lac_value)
        };
        for (eid, tx) in to_signal {
            let _ = tx.send(Ok(eid));
        }
    }

    /// Drain all outstanding pending signals with `LedgerClosed`.
    /// Used by `close()` to error out any callers still waiting.
    /// Matches Java's `errorOutPendingAdds(rc)` semantics.
    fn signal_all_closed(&self) {
        let to_signal: Vec<_> = {
            let mut st = self.state.lock().unwrap();
            std::mem::take(&mut st.pending).into_iter().collect()
        };
        for (_, tx) in to_signal {
            let _ = tx.send(Err(ClientError::LedgerClosed));
        }
    }

    /// Walk `st.acked` contiguously from `lac_value + 1` upward, collecting
    /// each pending signal to fire.  Updates `lac_value` under the mutex.
    /// Caller fires the signals AFTER releasing the lock.
    fn drain_in_lac_order(
        st: &mut CompletedState,
        lac_value: &AtomicI64,
    ) -> Vec<(i64, PendingTx)> {
        let mut current_lac = lac_value.load(Ordering::Acquire);
        while st.acked.remove(&(current_lac + 1)) {
            current_lac += 1;
        }
        lac_value.store(current_lac, Ordering::Release);
        // Drain pending entries with id <= current_lac, in ascending order.
        let keep = st.pending.split_off(&(current_lac + 1));
        let to_signal = std::mem::replace(&mut st.pending, keep);
        to_signal.into_iter().collect()
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
// PendingAddCtx — shared per-entry quorum state
// ---------------------------------------------------------------------------
//
// One Arc<PendingAddCtx> per in-flight entry. Every per-bookie write
// task (initial submission AND any resend triggered by an ensemble
// change) records its result into this context. The first writer to
// drive the AckSet over the quorum threshold (or the first writer to
// hit an unrecoverable status) resolves the user's PendingAdd via
// the LacState machinery.
//
// Mirrors Apache BookKeeper's `PendingAddOp`: a per-entry instance
// with an `ackSet` and a `completed` flag, where
// `unsetSuccessAndSendWriteRequest` re-runs `sendWriteRequest`
// against the same instance after an ensemble change so the new
// bookie's ack lands in the same `ackSet`.

struct PendingAddCtx {
    entry_id: i64,
    /// Per-bookie ack/failure tracker. Idempotent for repeated
    /// reports from the same ensemble index, and a fail-then-ack
    /// from the same index correctly retracts the failure (matches
    /// Java's `BitSet ackSet` + `BookieId[] failureMap` semantics).
    ack_set: std::sync::Mutex<crate::distribution::AckSet>,
    /// Failed ensemble indices recorded so far. Drained on success
    /// to populate `EnsembleInfo::delayed_failures` for proactive
    /// ensemble change.
    failed_indices: std::sync::Mutex<Vec<usize>>,
    /// Latched once the user's PendingAdd is signaled — prevents
    /// double-signaling if both a quorum-reached and a
    /// quorum-impossible / timeout event race after the first event.
    completed: AtomicBool,
    // State needed to invoke LacState::signal_success / signal_error.
    lac_state: Arc<LacState>,
    lac_value: Arc<AtomicI64>,
    ensemble_info: Arc<EnsembleInfo>,
    deferred_sync: bool,
    /// Optional caller-supplied hook fired exactly once when the
    /// PendingAdd is signaled (success, failure, timeout, or
    /// unrecoverable). Used by `WriteHandleAdv` to remove the entry
    /// id from its dedup set without spawning a polling task.
    on_completed: std::sync::Mutex<Option<Box<dyn FnOnce() + Send + 'static>>>,
}

impl PendingAddCtx {
    fn new(
        entry_id: i64,
        schedule: &RoundRobinDistributionSchedule,
        lac_state: Arc<LacState>,
        lac_value: Arc<AtomicI64>,
        ensemble_info: Arc<EnsembleInfo>,
        deferred_sync: bool,
    ) -> Self {
        Self::new_with_hook(
            entry_id, schedule, lac_state, lac_value, ensemble_info, deferred_sync, None,
        )
    }

    fn new_with_hook(
        entry_id: i64,
        schedule: &RoundRobinDistributionSchedule,
        lac_state: Arc<LacState>,
        lac_value: Arc<AtomicI64>,
        ensemble_info: Arc<EnsembleInfo>,
        deferred_sync: bool,
        on_completed: Option<Box<dyn FnOnce() + Send + 'static>>,
    ) -> Self {
        Self {
            entry_id,
            ack_set: std::sync::Mutex::new(schedule.new_ack_set()),
            failed_indices: std::sync::Mutex::new(Vec::new()),
            completed: AtomicBool::new(false),
            lac_state,
            lac_value,
            ensemble_info,
            deferred_sync,
            on_completed: std::sync::Mutex::new(on_completed),
        }
    }

    /// Atomically claim the right to signal the user's PendingAdd.
    /// Returns `true` exactly once across the lifetime of this ctx;
    /// subsequent calls return `false`. Also performs registry
    /// cleanup so the in-flight entry / pending-ctx maps don't grow
    /// past the entries the user is actually waiting on.
    fn claim_completion(&self) -> bool {
        let first = !self.completed.swap(true, Ordering::AcqRel);
        if first {
            self.ensemble_info.remove_in_flight(self.entry_id);
            self.ensemble_info.remove_pending_ctx(self.entry_id);
            if let Some(hook) = self.on_completed.lock().unwrap().take() {
                hook();
            }
        }
        first
    }

    /// Record a successful ack from `ensemble_idx`. If this brings
    /// the ack count to quorum, signal LacState (success). Idempotent
    /// for repeated acks from the same index; retracts a prior
    /// failure recorded for the same index.
    fn record_ack(self: &Arc<Self>, ensemble_idx: usize) {
        // Drop the lock before signalling so LacState's mutex
        // ordering doesn't compose with this one across a wait.
        let now_quorum = {
            let mut ack = self.ack_set.lock().unwrap();
            ack.complete_bookie(ensemble_idx)
        };
        // Forget any prior fail-record on this index — the bookie at
        // this slot has now answered Ok, so it shouldn't appear in
        // the post-success "delayed failures" list either.
        self.failed_indices.lock().unwrap().retain(|&i| i != ensemble_idx);
        if now_quorum && self.claim_completion() {
            // Drain post-quorum failures to delayed_failures for
            // proactive ensemble change. Mirrors the behaviour of the
            // pre-refactor ack-collector loop.
            self.publish_delayed_failures();
            self.lac_state.signal_success(
                self.entry_id,
                &self.lac_value,
                &self.ensemble_info,
                self.deferred_sync,
            );
        }
    }

    /// Record a failure from `ensemble_idx` with reason `err`. If
    /// this pushes the failure count past tolerable, signal LacState
    /// with the supplied error. Idempotent for repeated failures from
    /// the same index; retracts a prior ack recorded for the same
    /// index.
    fn record_fail(self: &Arc<Self>, ensemble_idx: usize, err: ClientError) {
        let now_impossible = {
            let mut ack = self.ack_set.lock().unwrap();
            ack.fail_bookie(ensemble_idx)
        };
        {
            let mut failed = self.failed_indices.lock().unwrap();
            if !failed.contains(&ensemble_idx) {
                failed.push(ensemble_idx);
            }
        }
        if now_impossible && self.claim_completion() {
            // Don't publish delayed failures on the failure path —
            // there is no successful pending-add waiting on an
            // ensemble change to re-route, the user's wait() resolves
            // with the supplied error directly.
            self.lac_state.signal_error(
                self.entry_id,
                wrap_quorum_failure(err, self.entry_id, &self.failed_indices.lock().unwrap()),
            );
        }
    }

    /// Record an unrecoverable bookie status (Fenced, Unauthorized).
    /// Resolves the PendingAdd immediately with the typed error,
    /// regardless of how many other bookies might still ack.
    fn record_unrecoverable(self: &Arc<Self>, error: bookkeeper_common::error::BKError) {
        if self.claim_completion() {
            self.lac_state.signal_error(
                self.entry_id,
                ClientError::UnrecoverableWrite { error, entry_id: self.entry_id },
            );
        }
    }

    /// Record the per-write quorum timeout. If still uncompleted,
    /// resolves the PendingAdd with a timeout error.
    fn record_timeout(self: &Arc<Self>, timeout: Duration) {
        if self.claim_completion() {
            self.lac_state.signal_error(
                self.entry_id,
                ClientError::Protocol(format!(
                    "add entry quorum timeout ({timeout:?}) for entry {}", self.entry_id
                )),
            );
        }
    }

    /// Append every failed bookie index recorded against this entry
    /// to `EnsembleInfo::delayed_failures`. Called on the success
    /// path so the next ensemble change can reconcile the failed
    /// slots.
    fn publish_delayed_failures(&self) {
        let failed = self.failed_indices.lock().unwrap();
        if failed.is_empty() {
            return;
        }
        let current_ens = self.ensemble_info.current_ensemble();
        let mut delayed = self.ensemble_info.delayed_failures.lock().unwrap();
        for &idx in failed.iter() {
            if idx < current_ens.len() {
                delayed.push((idx, current_ens[idx].clone()));
            }
        }
    }
}

fn wrap_quorum_failure(
    err: ClientError,
    entry_id: i64,
    failed_indices: &[usize],
) -> ClientError {
    let message = err.to_string();
    ClientError::WriteQuorumFailed {
        message,
        failed_indices: failed_indices.to_vec(),
        entry_id,
    }
}

/// Issue an AddEntry request to one bookie and feed the result into
/// the per-entry [`PendingAddCtx`]. Used for both initial submissions
/// and resends after ensemble change. The task takes ownership of an
/// `Arc<PendingAddCtx>` so it can be `tokio::spawn`-ed independently
/// — the ctx is reference-counted so the user's `PendingAdd` can
/// resolve from any sender's response, including a resend's.
fn spawn_send_to_bookie(
    ctx: Arc<PendingAddCtx>,
    ensemble_idx: usize,
    conn: Arc<BookieConnection>,
    entry: Bytes,
    master_key: [u8; MASTER_KEY_LENGTH],
    deferred_sync: bool,
    closed: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if closed.load(Ordering::Acquire) {
            // Don't write to bookies after the user closed the handle.
            // The PendingAdd's signal will already have come from
            // close()'s drain path.
            return;
        }
        match conn.send_add_entry_with_flags(0, master_key, entry, deferred_sync).await {
            Ok(V2Response::Add { error_code, .. }) if error_code == V2_EOK => {
                ctx.record_ack(ensemble_idx);
            }
            Ok(V2Response::Add { error_code, .. }) => {
                let bk_err = bookkeeper_common::error::BKError::from_v2_code(error_code);
                if matches!(
                    bk_err,
                    bookkeeper_common::error::BKError::Fenced
                        | bookkeeper_common::error::BKError::Unauthorized,
                ) {
                    ctx.record_unrecoverable(bk_err);
                } else {
                    ctx.record_fail(
                        ensemble_idx,
                        ClientError::BookieError { error: bk_err },
                    );
                }
            }
            Ok(_) => ctx.record_fail(
                ensemble_idx,
                ClientError::Protocol("unexpected response type".into()),
            ),
            Err(e) => ctx.record_fail(ensemble_idx, e),
        }
    })
}

// ---------------------------------------------------------------------------
// WriteHandle
// ---------------------------------------------------------------------------

/// Handle for a running explicit LAC flush timer.
/// Drop or call `cancel()` to stop the background task.
pub struct ExplicitLacFlushHandle {
    cancel: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
}

impl ExplicitLacFlushHandle {
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::Relaxed);
    }
}

impl Drop for ExplicitLacFlushHandle {
    fn drop(&mut self) {
        // Signal the loop to stop, then abort in case it's sleeping on the
        // interval — tokio JoinHandles are detached on drop by default, so
        // without this the background task runs until the next tick anyway.
        self.cancel.store(true, Ordering::Relaxed);
        self.handle.abort();
    }
}

/// Callback invoked when an ensemble change occurs.
/// Arguments: (first_entry_id_of_new_segment, new_ensemble).
/// The caller uses this to perform the metadata CAS update (e.g. in ZK
/// or oxia). The returned future completes once the CAS has resolved —
/// the writer awaits it before continuing, so a failed CAS surfaces as
/// a write error instead of letting the writer pile up entries against
/// an in-memory ensemble that ZK doesn't yet know about (split-brain
/// between writer and readers if the writer then crashes).
pub type OnEnsembleChange = Box<
    dyn Fn(i64, &[String]) -> futures::future::BoxFuture<'static, Result<(), String>>
        + Send
        + Sync,
>;

/// Default write quorum timeout (30 seconds).
const DEFAULT_ADD_ENTRY_QUORUM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Default max ensemble changes (0 = unlimited).
const DEFAULT_MAX_ENSEMBLE_CHANGES: usize = 0;

/// Prune completed JoinHandles from `pending_adds` every N appends.
/// Keeps the Vec bounded at O(concurrent_writes) instead of O(total_writes).
const PENDING_ADDS_PRUNE_INTERVAL: usize = 100;

// ---------------------------------------------------------------------------
// Java `OrderedExecutor` equivalence — CORRECTNESS-CRITICAL
// ---------------------------------------------------------------------------
//
// Java's `BookKeeper` uses an `OrderedExecutor` (hash ledger_id → worker
// thread) for response dispatching. Reading Java's source:
//
//   // in PerChannelBookieClient.handleResponse:
//   long orderingKey = completionValue.ledgerId;
//   executor.executeOrdered(orderingKey, new Runnable() { run() { handleV3Response(...); } });
//
// This is **correctness-critical**, not a performance optimization:
//
//   // in LedgerHandle.sendAddSuccessCallbacks — NO `synchronized`:
//   void sendAddSuccessCallbacks() {
//       while ((pendingAddOp = pendingAddOps.peek()) != null && !changingEnsemble) {
//           ...
//           pendingAddsSequenceHead = pendingAddOp.entryId;     // unsynchronized write
//           this.lastAddConfirmed = pendingAddsSequenceHead;    // unsynchronized write
//           pendingAddOp.submitCallback(OK);                     // user callback
//       }
//   }
//
// Without OSE serialization, `pendingAddsSequenceHead` and
// `lastAddConfirmed` would race across concurrent ack responses.
// Java relies on OSE to ensure at most one thread is inside
// `sendAddSuccessCallbacks` for a given ledger at any time.
//
// The Rust implementation achieves the **same correctness** via explicit
// synchronization instead of a dedicated executor thread:
//
// - LAC advancement (`LacState::advance_lac`): holds `Mutex<BTreeSet<i64>>`
//   across the full insert-and-advance critical section — equivalent to
//   OSE serialization of `sendAddSuccessCallbacks`.
// - Ensemble change gate (`EnsembleInfo::changing_ensemble`): AtomicBool
//   checked inside the Mutex, equivalent to Java's `!changingEnsemble`
//   predicate.
// - In-flight entry tracking (`EnsembleInfo::in_flight_entries`):
//   `Mutex<BTreeMap>` — serialized access for resend-on-ensemble-change.
// - Submission path (`WriteHandle::append_async` takes `&mut self`):
//   Rust's borrow checker enforces single-writer access on the handle,
//   matching OSE's "only one thread submits ops for a given ledger".
//
// Observable divergence from Java (acceptable — not a correctness issue):
// - User-visible callback resolution order (`PendingAdd::wait().await`)
//   may not strictly match entry-submission order across concurrent
//   in-flight adds.  The LAC itself still advances in entry-ID order
//   (correctness preserved); only the future-resolution timing differs.
//   Java's OSE resolves callbacks strictly in LAC-advancement order.
// ---------------------------------------------------------------------------

pub struct WriteHandle {
    ledger_id: i64,
    /// All bookies available in the pool, used as a fallback for finding
    /// replacements on failure when no placement policy has been wired in.
    /// When `placement` is `Some`, the policy's `replace_bookie` is the
    /// authoritative source — `available_bookies` is a snapshot from
    /// construction time and may be stale relative to cluster changes.
    available_bookies: Vec<String>,
    /// Optional placement policy for replacement selection on bookie
    /// failure. Wired from the client (`zk_client::open_write_handle`)
    /// so that rack-aware / region-aware deployments preserve their
    /// diversity constraints across ensemble changes; absent only in
    /// tests that bypass the client wiring.
    placement: Option<Arc<parking_lot::Mutex<Box<dyn crate::placement::EnsemblePlacementPolicy>>>>,
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
    /// Tracked pending add futures for drain on close.
    pending_adds: Vec<tokio::task::JoinHandle<()>>,
    /// Count of ensemble changes so far (for limit enforcement).
    ensemble_change_count: usize,
    /// Max ensemble changes before erroring out. 0 = unlimited.
    /// Matches Java's `maxAllowedEnsembleChanges`.
    max_ensemble_changes: usize,
    /// Per-write quorum timeout. Matches Java's `addEntryQuorumTimeoutNanos`.
    add_entry_quorum_timeout: std::time::Duration,
    /// Send DEFERRED_SYNC flag to bookie. Default false.
    deferred_sync: bool,
    /// When true, delay ensemble change until ack quorum is impossible.
    /// Matches Java's `PendingAddOp.delayEnsembleChange`: a single bookie
    /// failure that still allows ack quorum does NOT trigger immediate change.
    delay_ensemble_change: bool,
    /// Shared error slot for asynchronous ensemble change metadata failures.
    /// When a metadata CAS update fails in the background, the error is stored
    /// here. The next `append_with_ensemble_change` checks this slot and fails
    /// the write, matching Java's `handleUnrecoverableErrorDuringAdd`.
    metadata_error: Arc<std::sync::Mutex<Option<String>>>,
    /// Whether this handle has been closed. Once true, subsequent
    /// `append_async`/`append` calls return `ClientError::LedgerClosed`.
    /// Matches Java's `LedgerHandle.isHandleWritable()` which raises
    /// `BKLedgerClosedException` for writes after `asyncClose`.
    ///
    /// Shared via `Arc` so `append_async` can observe the flag from its
    /// spawned ack-collection task as well.
    closed: Arc<AtomicBool>,
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
            placement: None,
            schedule,
            digest_manager,
            master_key,
            last_add_pushed: -1,
            current_length: 0,
            lac_value: Arc::new(AtomicI64::new(-1)),
            lac_state: Arc::new(LacState::new()),
            ensemble_info: Arc::new(EnsembleInfo::new(ensemble)),
            on_ensemble_change: None,
            pending_adds: Vec::new(),
            ensemble_change_count: 0,
            max_ensemble_changes: DEFAULT_MAX_ENSEMBLE_CHANGES,
            add_entry_quorum_timeout: DEFAULT_ADD_ENTRY_QUORUM_TIMEOUT,
            deferred_sync: false,
            delay_ensemble_change: false,
            metadata_error: Arc::new(std::sync::Mutex::new(None)),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Return whether this handle has been closed. Once closed, subsequent
    /// writes will return `ClientError::LedgerClosed`.
    /// Matches Java's `LedgerHandle.isHandleWritable()` — inverted.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Set max allowed ensemble changes. 0 = unlimited.
    pub fn set_max_ensemble_changes(&mut self, max: usize) {
        self.max_ensemble_changes = max;
    }

    /// Set per-write quorum timeout.
    pub fn set_add_entry_quorum_timeout(&mut self, timeout: std::time::Duration) {
        self.add_entry_quorum_timeout = timeout;
    }

    /// Return a handle to the shared metadata error slot.
    /// Used by the ZK integration layer to report async ensemble change failures.
    pub fn metadata_error_slot(&self) -> Arc<std::sync::Mutex<Option<String>>> {
        Arc::clone(&self.metadata_error)
    }

    /// Enable deferred sync: writes will include WRITE_FLAG_DEFERRED_SYNC.
    /// The bookie acks after cache placement without waiting for journal fsync.
    /// Durability is guaranteed by the next ForceLedger or close_ledger call.
    /// Matches Java's `WriteFlag.DEFERRED_SYNC`.
    pub fn set_deferred_sync(&mut self, enabled: bool) {
        self.deferred_sync = enabled;
    }

    /// Enable delayed ensemble change: failures are absorbed as long as ack
    /// quorum is still achievable. Matches Java's `delayEnsembleChange` config.
    pub fn set_delay_ensemble_change(&mut self, enabled: bool) {
        self.delay_ensemble_change = enabled;
    }

    /// Set the callback invoked on ensemble change for metadata CAS integration.
    pub fn set_on_ensemble_change(&mut self, cb: OnEnsembleChange) {
        self.on_ensemble_change = Some(cb);
    }

    /// Inject the cluster's placement policy so `handle_bookie_failure`
    /// can pick rack-aware / region-aware replacements rather than
    /// falling back to "first available bookie not in the current
    /// ensemble" — the latter silently downgrades to no-policy on
    /// every ensemble change.
    pub fn set_placement(
        &mut self,
        placement: Arc<parking_lot::Mutex<Box<dyn crate::placement::EnsemblePlacementPolicy>>>,
    ) {
        self.placement = Some(placement);
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

    /// Return the shared `EnsembleInfo` so tests can inject delayed failures
    /// or inspect the segments map from outside the crate.
    #[doc(hidden)]
    pub fn ensemble_info_for_test(&self) -> Arc<EnsembleInfo> {
        Arc::clone(&self.ensemble_info)
    }

    /// Replace the bookie at `failed_bookie_idx` with one chosen by the
    /// configured placement policy (rack-aware / region-aware /
    /// default), connect to it, and patch `conns` in-place.
    ///
    /// Falls back to "first available bookie not in the current
    /// ensemble" only when no placement policy has been wired in
    /// (`set_placement`) — typical in tests. In production wiring (via
    /// `zk_client::open_write_handle`) the policy is always set, so
    /// rack/region constraints survive ensemble changes.
    ///
    /// The chosen address is resolved via `parse-or-lookup_host`, so
    /// DNS-named bookies work the same as IP-literal ones — mirrors
    /// the auditor / replication-worker fix from commit `cc8def7`.
    pub async fn handle_bookie_failure(
        &mut self,
        conns: &mut Vec<Arc<BookieConnection>>,
        failed_bookie_idx: usize,
    ) -> Result<(), ClientError> {
        let current_ensemble = self.ensemble_info.current_ensemble();
        let to_replace = current_ensemble
            .get(failed_bookie_idx)
            .cloned()
            .ok_or_else(|| ClientError::Protocol(format!(
                "failed_bookie_idx {failed_bookie_idx} out of range \
                 (ensemble size {})",
                current_ensemble.len(),
            )))?;
        let exclude: std::collections::HashSet<String> =
            current_ensemble.iter().cloned().collect();

        let replacement = if let Some(ref policy) = self.placement {
            // ensemble_size / wq / aq are derived from the schedule —
            // the placement policy uses them for adherence checks.
            let es = self.schedule.ensemble_size();
            let wq = self.schedule.write_quorum_size();
            let aq = self.schedule.ack_quorum_size();
            policy
                .lock()
                .replace_bookie(es, wq, aq, &current_ensemble, &to_replace, &exclude)
                .map_err(|e| ClientError::Protocol(format!(
                    "placement policy could not replace {to_replace}: {e}",
                )))?
        } else {
            self.available_bookies
                .iter()
                .find(|b| !exclude.contains(b.as_str()))
                .cloned()
                .ok_or_else(|| ClientError::Protocol(
                    "no available replacement bookie".into(),
                ))?
        };

        let addr = resolve_bookie_address(&replacement).await?;
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
        // Closed-state guard: refuse writes to a closed handle.
        // Matches Java's `LedgerHandle.isHandleWritable()` which raises
        // `BKLedgerClosedException` when the handle is in CLOSED state.
        if self.closed.load(Ordering::Acquire) {
            return Err(ClientError::LedgerClosed);
        }

        let entry_id = self.last_add_pushed + 1;
        self.last_add_pushed = entry_id;
        self.current_length += payload.len() as i64;

        // Build entry bytes (includes digest, lac, etc.)
        let entry_bytes = self.build_entry(entry_id, payload);

        // Register in the in-flight registry so ensemble changes can re-send
        // this entry to a replacement bookie.
        // Matches Java's pendingAddOps queue used by unsetSuccessAndSendWriteRequest.
        self.ensemble_info.register_in_flight(entry_id, entry_bytes.clone());

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

        let mk = self.master_key;
        let use_deferred_sync = self.deferred_sync;

        // Register the PendingAdd's signaling slot BEFORE spawning so the
        // callback order is deterministic relative to LAC advancement.
        // Matches Java's pendingAddOps queue which is populated at submit
        // time and drained in-order by sendAddSuccessCallbacks.
        let pending_add = self.lac_state.register(entry_id);
        let quorum_timeout = self.add_entry_quorum_timeout;

        // Per-entry shared quorum context. Both the per-bookie initial
        // submission tasks AND any later resend triggered by an ensemble
        // change feed their results into this single AckSet, so a
        // resend's Ok lands in the same quorum the original task is
        // waiting on. Mirrors Java's `PendingAddOp` instance reused
        // across `sendWriteRequest` calls.
        let ctx = Arc::new(PendingAddCtx::new(
            entry_id,
            &self.schedule,
            Arc::clone(&self.lac_state),
            Arc::clone(&self.lac_value),
            Arc::clone(&self.ensemble_info),
            use_deferred_sync,
        ));
        self.ensemble_info.register_pending_ctx(entry_id, Arc::clone(&ctx));

        // Per-write quorum timeout. Spawned separately from the
        // bookie-write tasks so it fires regardless of how many of
        // them are still in flight.
        {
            let ctx_for_timeout = Arc::clone(&ctx);
            let closed_for_timeout = Arc::clone(&self.closed);
            let handle = tokio::spawn(async move {
                tokio::time::sleep(quorum_timeout).await;
                if !closed_for_timeout.load(Ordering::Acquire) {
                    ctx_for_timeout.record_timeout(quorum_timeout);
                }
            });
            self.pending_adds.push(handle);
        }

        // Spawn one write task per write-set position. Each task feeds
        // its result back into the shared ctx; the first to push the
        // ack-set over quorum (or hit an unrecoverable status) signals
        // the user's PendingAdd via LacState.
        for (pos, &ensemble_idx) in write_set.iter().enumerate() {
            let conn = Arc::clone(&conns[pos]);
            let entry = entry_bytes.clone();
            let handle = spawn_send_to_bookie(
                Arc::clone(&ctx),
                ensemble_idx,
                conn,
                entry,
                mk,
                use_deferred_sync,
                Arc::clone(&self.closed),
            );
            self.pending_adds.push(handle);
        }

        // Periodically prune completed handles to prevent unbounded growth.
        // Java's PendingAddOp is GC'd naturally; Rust JoinHandles need explicit cleanup.
        if self.pending_adds.len() % PENDING_ADDS_PRUNE_INTERVAL == 0 {
            self.pending_adds.retain(|h| !h.is_finished());
        }
        Ok(pending_add)
    }

    /// Blocking append — fires writes and waits for ack quorum before returning.
    /// Preserved for backward compatibility; internally uses [`append_async`].
    pub async fn append(
        &mut self,
        conns: &[Arc<BookieConnection>],
        payload: &[u8],
    ) -> Result<i64, ClientError> {
        crate::client_metrics::PENDING_ADDS.inc();
        let start = std::time::Instant::now();
        let result = self.append_async(conns, payload)?.wait().await;
        crate::client_metrics::PENDING_ADDS.dec();
        crate::client_metrics::ADD_ENTRY.observe(start.elapsed().as_secs_f64());
        result
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
        // Check for async metadata update failure from a prior ensemble change.
        // Matches Java's handleUnrecoverableErrorDuringAdd which prevents new
        // writes once the metadata is in an inconsistent state.
        {
            let err = self.metadata_error.lock().unwrap();
            if let Some(ref msg) = *err {
                return Err(ClientError::Protocol(format!(
                    "ensemble change metadata update failed: {msg}"
                )));
            }
        }

        // Proactive ensemble change: process any delayed failures from previous
        // writes before attempting the next one.
        // Matches Java's maybeHandleDelayedWriteBookieFailure().
        self.maybe_handle_delayed_failures(conns).await?;

        // First attempt.
        let result = self.append_async(conns, payload)?.wait().await;

        match result {
            Ok(entry_id) => Ok(entry_id),
            // Fenced/Unauthorized: propagate immediately, no ensemble change
            Err(e @ ClientError::UnrecoverableWrite { .. }) => Err(e),
            Err(ClientError::WriteQuorumFailed { failed_indices, .. }) => {
                // Check max ensemble changes limit (matches Java's maxAllowedEnsembleChanges)
                if self.max_ensemble_changes > 0 && self.ensemble_change_count >= self.max_ensemble_changes {
                    return Err(ClientError::Protocol(format!(
                        "max ensemble changes ({}) exceeded", self.max_ensemble_changes
                    )));
                }
                self.ensemble_change_count += 1;
                crate::client_metrics::ENSEMBLE_CHANGE.inc();

                // Step 1: Set changing_ensemble flag — gates LAC advancement
                self.ensemble_info.changing_ensemble.store(true, Ordering::Release);

                // Step 2: Replace every failed bookie. Drain any concurrently
                // accumulated delayed failures into the same change so the
                // segment add and metadata callback fire exactly once at
                // first_entry_id = LAC + 1. Two callbacks at the same
                // first_entry_id would silently overwrite each other in the
                // segments map and on ZK, leaving the writer's local view
                // briefly inconsistent with the persisted metadata.
                let mut all_failed: Vec<usize> = failed_indices.clone();
                for idx in &failed_indices {
                    self.handle_bookie_failure(conns, *idx).await?;
                }
                loop {
                    let delayed = std::mem::take(
                        &mut *self.ensemble_info.delayed_failures.lock().unwrap(),
                    );
                    if delayed.is_empty() {
                        break;
                    }
                    for (idx, _addr) in &delayed {
                        all_failed.push(*idx);
                        self.handle_bookie_failure(conns, *idx).await?;
                    }
                }

                // Step 3: Add ONE ensemble segment at LAC + 1 with the final
                // post-replacement ensemble.
                let new_segment_start = self.last_add_confirmed() + 1;
                let new_ensemble = self.ensemble_info.current_ensemble();
                self.ensemble_info.add_segment(new_segment_start, new_ensemble.clone());

                // Step 4: Invoke the metadata CAS callback and AWAIT its
                // completion before letting more writes flow against the
                // new in-memory ensemble. A failed CAS surfaces here as
                // a write error instead of letting the writer keep
                // committing entries that ZK never recorded. Pre-fix the
                // CAS was a fire-and-forget tokio::spawn — a writer crash
                // before the spawn finished left ZK on the old ensemble
                // while the writer kept emitting entries on the new one.
                if let Some(ref cb) = self.on_ensemble_change {
                    if let Err(msg) = cb(new_segment_start, &new_ensemble).await {
                        *self.metadata_error.lock().unwrap() = Some(msg.clone());
                        return Err(ClientError::Protocol(format!(
                            "ensemble change metadata CAS failed: {msg}"
                        )));
                    }
                }

                // Step 4b: Re-send in-flight entries that were targeting any
                // of the failed bookie indices (immediate or delayed) to
                // their replacements in the new ensemble.
                self.resend_in_flight_to_new_ensemble(conns, &all_failed).await;

                // Step 5: Clear changing_ensemble and flush queued LAC entries
                self.ensemble_info.changing_ensemble.store(false, Ordering::Release);
                self.flush_lac();

                // Step 6: Retry the failed write
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
            std::mem::take(&mut *df)
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
            if let Err(msg) = cb(new_start, &current_ens).await {
                *self.metadata_error.lock().unwrap() = Some(msg.clone());
                // Don't return Err here — we still need to clear the flag
                // and flush LAC so callers don't deadlock waiting for LAC
                // advancement. The metadata_error slot will fail subsequent
                // writes (Java's handleUnrecoverableErrorDuringAdd).
                self.ensemble_info.changing_ensemble.store(false, Ordering::Release);
                self.flush_lac();
                return Err(ClientError::Protocol(format!(
                    "delayed-failure ensemble change CAS failed: {msg}"
                )));
            }
        }

        // Clear flag and flush queued LAC entries
        self.ensemble_info.changing_ensemble.store(false, Ordering::Release);
        self.flush_lac();

        Ok(())
    }

    /// Test-only: trigger the in-flight-resend path for the given
    /// failed bookie indices without going through the `append_async`
    /// → `WriteQuorumFailed` → `handle_bookie_failure` chain. Used by
    /// integration tests that need to deterministically resend
    /// in-flight entries against an already-replaced ensemble slot.
    #[doc(hidden)]
    pub async fn resend_in_flight_for_test(
        &mut self,
        conns: &[Arc<BookieConnection>],
        failed_indices: &[usize],
    ) {
        self.resend_in_flight_to_new_ensemble(conns, failed_indices).await;
    }

    /// Re-send in-flight entries that targeted the failed bookies to the
    /// new replacement bookies. This is the Rust equivalent of Java's
    /// `PendingAddOp.unsetSuccessAndSendWriteRequest()` which iterates
    /// all `pendingAddOps` and re-sends entries affected by the ensemble change.
    ///
    /// For each in-flight entry whose write set includes a failed bookie
    /// index, the resend is dispatched against the replacement bookie at
    /// that ensemble index AND records its result into the same
    /// [`PendingAddCtx`] the original initial-write tasks are using.
    /// The first task — initial OR resend — to drive the per-entry ack
    /// set over quorum resolves the user's `PendingAdd`. Without this
    /// shared-state wiring, the resend would be fire-and-forget and
    /// the original ack-collector would keep waiting on the dead
    /// bookie's response future until the per-write quorum timeout
    /// fires (default 30 seconds), surfacing the entry as a timeout
    /// error even though the data WAS durable on the replacement.
    async fn resend_in_flight_to_new_ensemble(
        &mut self,
        conns: &[Arc<BookieConnection>],
        failed_indices: &[usize],
    ) {
        let entries = self.ensemble_info.in_flight_for_failed_bookies(
            failed_indices, &self.schedule,
        );
        if entries.is_empty() {
            return;
        }
        tracing::info!(
            "resending {} in-flight entries to new ensemble after ensemble change",
            entries.len()
        );
        let mk = self.master_key;
        let ds = self.deferred_sync;
        for (entry_id, entry_bytes) in entries {
            // The pending-ctx may be gone if the entry already
            // resolved (e.g. quorum already reached on the surviving
            // bookies before the ensemble change committed). Skip the
            // resend for such entries — there is nothing to add to.
            let Some(ctx) = self.ensemble_info.get_pending_ctx(entry_id) else {
                continue;
            };
            let write_set = self.schedule.write_set(entry_id);
            for &failed_idx in failed_indices {
                if write_set.contains(&failed_idx) && failed_idx < conns.len() {
                    let conn = Arc::clone(&conns[failed_idx]);
                    let entry = entry_bytes.clone();
                    let handle = spawn_send_to_bookie(
                        Arc::clone(&ctx),
                        failed_idx,
                        conn,
                        entry,
                        mk,
                        ds,
                        Arc::clone(&self.closed),
                    );
                    self.pending_adds.push(handle);
                }
            }
        }
    }

    /// Drain the acked set, advance the atomic LAC value, and signal any
    /// pending callbacks whose entry IDs have now reached LAC.
    ///
    /// Called after:
    /// - an ensemble change completes (unblocks signals gated by
    ///   `changing_ensemble`)
    /// - `force()` for deferred-sync mode (advances LAC after ForceLedger
    ///   acks come back)
    ///
    /// Delegates to `LacState::flush_after_ensemble_change` (both call
    /// sites want the same behavior: walk acked set, advance LAC, fire
    /// any held-back callbacks).
    fn flush_lac(&self) {
        self.lac_state.flush_after_ensemble_change(&self.lac_value);
    }

    /// Force durability of all entries written with DEFERRED_SYNC.
    /// Sends ForceLedger RPC to ALL bookies in the ensemble and advances LAC
    /// once ack quorum is reached.
    /// Matches Java's `LedgerHandle.force()` → `ForceLedgerOp`.
    pub async fn force(
        &self,
        conns: &[Arc<BookieConnection>],
    ) -> Result<(), ClientError> {
        let ack_quorum = self.schedule.ack_quorum_size();
        let ledger_id = self.ledger_id;

        // Send ForceLedger to ALL bookies in the ensemble (not just write quorum).
        // Matches Java's ForceLedgerOp which uses getEnsembleSet (full ensemble).
        let futures: Vec<_> = conns.iter().map(|conn| {
            let conn = Arc::clone(conn);
            async move { conn.send_force_ledger(ledger_id).await }
        }).collect();

        let results = join_all(futures).await;
        let success_count = results.iter().filter(|r| r.is_ok()).count();

        if success_count >= ack_quorum {
            // ForceLedger succeeded — advance LAC for deferred-sync entries.
            self.flush_lac();
            Ok(())
        } else {
            Err(ClientError::Protocol(format!(
                "ForceLedger failed: only {success_count}/{} bookies acked (need {ack_quorum})",
                conns.len()
            )))
        }
    }

    /// Advance LAC for deferred-sync entries without sending ForceLedger RPC.
    /// Used internally; callers should prefer `force()` which also ensures
    /// bookie-side durability.
    pub fn force_lac_flush(&self) {
        self.flush_lac();
    }

    /// Broadcast the current LAC to all bookies in the ensemble via WriteLac.
    /// Matches Java's `ExplicitLacFlushPolicy` which periodically sends WriteLac
    /// to ensure all bookies have up-to-date LAC information for tailing readers.
    pub async fn flush_explicit_lac(
        &self,
        conns: &[Arc<BookieConnection>],
    ) -> Result<(), ClientError> {
        let lac = self.lac_value.load(Ordering::Acquire);
        if lac < 0 {
            return Ok(()); // nothing to flush
        }
        let lac_entry = self.build_lac_body(lac);
        let mk = self.master_key;
        let ledger_id = self.ledger_id;

        let futures: Vec<_> = conns.iter().map(|conn| {
            let body = lac_entry.clone();
            async move {
                conn.send_write_lac(ledger_id, lac, mk, body).await
            }
        }).collect();

        let results = join_all(futures).await;
        let success_count = results.iter().filter(|r| {
            matches!(r, Ok(V2Response::WriteLac { error_code, .. }) if *error_code == V2_EOK)
        }).count();

        if success_count > 0 { Ok(()) } else {
            Err(ClientError::Protocol("explicit LAC flush failed on all bookies".into()))
        }
    }

    fn build_lac_body(&self, lac: i64) -> Bytes {
        build_lac_body_raw(self.ledger_id, lac, &self.digest_manager)
    }

    /// Start a background timer that periodically flushes the explicit LAC
    /// to all bookies via WriteLac. Returns a handle to cancel the timer.
    /// Matches Java's `ExplicitLacFlushPolicy.scheduleExplicitLacFlush()`.
    ///
    /// The timer only sends WriteLac when LAC has advanced since the last flush.
    pub fn start_explicit_lac_flush(
        &self,
        conns: Vec<Arc<BookieConnection>>,
        interval: std::time::Duration,
    ) -> ExplicitLacFlushHandle {
        let lac_value = Arc::clone(&self.lac_value);
        let ledger_id = self.ledger_id;
        let master_key = self.master_key;
        let dm = self.digest_manager.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(async move {
            let mut last_flushed_lac: i64 = -1;
            loop {
                tokio::time::sleep(interval).await;
                if cancel_clone.load(Ordering::Relaxed) {
                    break;
                }
                let current_lac = lac_value.load(Ordering::Acquire);
                if current_lac > last_flushed_lac && current_lac >= 0 {
                    let lac_body = build_lac_body_raw(ledger_id, current_lac, &dm);

                    let mk = master_key;
                    let futs: Vec<_> = conns.iter().map(|conn| {
                        let body = lac_body.clone();
                        async move { conn.send_write_lac(ledger_id, current_lac, mk, body).await }
                    }).collect();
                    let _ = futures::future::join_all(futs).await;
                    last_flushed_lac = current_lac;
                }
            }
        });

        ExplicitLacFlushHandle { cancel, handle }
    }

    /// Drain all pending writes and return final (lastEntryId, length).
    /// Matches Java's `LedgerHandle.asyncCloseInternal()` which drains the
    /// pending add ops queue before updating metadata to CLOSED.
    ///
    /// Sets the `closed` flag so subsequent `append_async`/`append` calls
    /// return `ClientError::LedgerClosed` — matching Java's `BKLedgerClosedException`.
    pub async fn close(&mut self) -> Result<(i64, i64), ClientError> {
        // Mark closed FIRST so any further append calls reject immediately.
        self.closed.store(true, Ordering::Release);
        // Wait for all in-flight add tasks to complete
        let handles = std::mem::take(&mut self.pending_adds);
        for handle in handles {
            let _ = handle.await;
        }
        // Try one last LAC advance — if changing_ensemble was somehow still
        // set it should have been cleared by ensemble-change completion,
        // but an extra flush is cheap and defends against leaks.
        self.flush_lac();
        // Any signals still pending at this point represent entries whose
        // earlier neighbours never ack'd (e.g., entry 0 errored but we
        // successfully wrote entry 1 — LAC can never advance past 0, so
        // entry 1's PendingAdd would hang forever).  Fail them with
        // LedgerClosed so callers unblock, matching Java's
        // `errorOutPendingAdds(BKException.Code.ClientClosedException)`.
        self.lac_state.signal_all_closed();
        Ok((self.last_add_confirmed(), self.current_length))
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
            .payload(payload)
            .build()
            .freeze()
    }
}

/// Build a WriteLac body: `[ledgerId:8][lac:8][digest]`.
/// Free function so it can be called from both `WriteHandle::build_lac_body`
/// and the background timer task without requiring `self`.
fn build_lac_body_raw(ledger_id: i64, lac: i64, dm: &DigestManager) -> Bytes {
    let mut lac_metadata = [0u8; 16];
    lac_metadata[0..8].copy_from_slice(&ledger_id.to_be_bytes());
    lac_metadata[8..16].copy_from_slice(&lac.to_be_bytes());
    let digest = dm.compute_lac(&lac_metadata);
    let mut buf = bytes::BytesMut::with_capacity(16 + digest.len());
    buf.extend_from_slice(&lac_metadata);
    buf.extend_from_slice(digest.as_slice());
    buf.freeze()
}

// ---------------------------------------------------------------------------
// WriteHandleAdv — explicit entry IDs (matches Java's LedgerHandleAdv)
// ---------------------------------------------------------------------------

/// Write handle for advanced mode where the caller supplies explicit entry IDs.
/// Matches Java's `LedgerHandleAdv` / `WriteAdvHandle`.
///
/// Key differences from `WriteHandle`:
/// - Caller provides the entry ID for each append
/// - Out-of-order writes are supported: LAC advances only through contiguous
///   confirmed entries (matching Java's priority-queue reordering)
/// - Duplicate entry IDs are rejected
/// - The server requires no changes — the same wire format carries the entry ID
pub struct WriteHandleAdv {
    inner: WriteHandle,
    /// Tracks entry IDs that have been submitted but not yet confirmed.
    /// Used for duplicate detection (matching Java's `pendingAddOps.contains()`).
    /// Shared with the background task spawned by `append_adv_async` so it
    /// can remove the ID on completion — otherwise long-running writers
    /// would leak entries proportional to total appends.
    pending_entry_ids: Arc<std::sync::Mutex<BTreeSet<i64>>>,
}

impl WriteHandleAdv {
    pub fn new(
        ledger_id: i64,
        ensemble: Vec<String>,
        schedule: RoundRobinDistributionSchedule,
        digest_manager: DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Self {
        Self {
            inner: WriteHandle::new(ledger_id, ensemble, schedule, digest_manager, master_key),
            pending_entry_ids: Arc::new(std::sync::Mutex::new(BTreeSet::new())),
        }
    }

    pub fn with_available_bookies(
        ledger_id: i64,
        ensemble: Vec<String>,
        available_bookies: Vec<String>,
        schedule: RoundRobinDistributionSchedule,
        digest_manager: DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Self {
        Self {
            inner: WriteHandle::with_available_bookies(
                ledger_id, ensemble, available_bookies, schedule, digest_manager, master_key,
            ),
            pending_entry_ids: Arc::new(std::sync::Mutex::new(BTreeSet::new())),
        }
    }

    /// Append an entry with an explicit entry ID.
    /// Returns error if the entry ID is a duplicate (already pending or confirmed).
    /// Matches Java's `LedgerHandleAdv.asyncAddEntry(long entryId, ...)`.
    pub fn append_adv_async(
        &mut self,
        conns: &[Arc<BookieConnection>],
        entry_id: i64,
        payload: &[u8],
    ) -> Result<PendingAdd, ClientError> {
        // Closed-state guard — same as WriteHandle::append_async.
        if self.inner.is_closed() {
            return Err(ClientError::LedgerClosed);
        }
        // Reject duplicates: already confirmed or already pending.
        // Matches Java's `if (entryId <= this.lastAddConfirmed || pendingAddOps.contains(op))`
        let lac = self.inner.last_add_confirmed();
        if entry_id <= lac {
            return Err(ClientError::Protocol(format!(
                "duplicate entry ID {entry_id}: already confirmed (LAC={lac})"
            )));
        }
        {
            let mut pending = self.pending_entry_ids.lock().unwrap();
            if pending.contains(&entry_id) {
                return Err(ClientError::Protocol(format!(
                    "duplicate entry ID {entry_id}: already pending"
                )));
            }
            pending.insert(entry_id);
        }

        // Track last_add_pushed as the maximum entry ID seen,
        // not necessarily sequential. This only matters for the
        // entry data header (LAC field uses current lac_value).
        if entry_id > self.inner.last_add_pushed {
            self.inner.last_add_pushed = entry_id;
        }
        self.inner.current_length += payload.len() as i64;

        // Build and send the entry using the same write path.
        let entry_bytes = self.inner.build_entry(entry_id, payload);
        let write_set = self.inner.schedule.write_set(entry_id);

        let conns_for_write: Vec<Arc<BookieConnection>> = write_set
            .iter()
            .filter_map(|&idx| conns.get(idx).cloned())
            .collect();

        if conns_for_write.len() < self.inner.schedule.ack_quorum_size() {
            return Err(ClientError::Protocol(format!(
                "not enough connections for ack quorum: have {}, need {}",
                conns_for_write.len(),
                self.inner.schedule.ack_quorum_size(),
            )));
        }

        let mk = self.inner.master_key;
        let use_deferred_sync = self.inner.deferred_sync;
        let quorum_timeout = self.inner.add_entry_quorum_timeout;

        // Caller must supply at least `write_quorum` connections (and we
        // already verified `>= ack_quorum` above; the path below requires
        // one connection per write-set position). Falling back to
        // conns_for_write[0] when a slot is missing — as the previous
        // code did — would route multiple ensemble indices to the SAME
        // bookie, and the same bookie's ack would count as multiple
        // distinct acks toward the quorum, letting "ack quorum reached"
        // hold on a single bookie.
        if conns_for_write.len() < write_set.len() {
            return Err(ClientError::Protocol(format!(
                "append_adv_async: not enough connections for write quorum: \
                 have {}, need {}",
                conns_for_write.len(),
                write_set.len(),
            )));
        }

        // Register the PendingAdd's signaling slot in LacState up front
        // — see WriteHandle::append_async for the rationale.
        let pending_add = self.inner.lac_state.register(entry_id);
        let pending_entry_ids = Arc::clone(&self.pending_entry_ids);

        // Per-entry shared quorum context. See `WriteHandle::append_async`
        // for the design rationale: every write task (initial AND any
        // resend after an ensemble change) records into the same ctx so
        // a resend's ack reaches the same quorum the original tasks
        // are tracking. The completion hook drops the entry id from
        // the dedup set so memory stays bounded at the truly-in-flight
        // count.
        let dedup = Arc::clone(&pending_entry_ids);
        let ctx = Arc::new(PendingAddCtx::new_with_hook(
            entry_id,
            &self.inner.schedule,
            Arc::clone(&self.inner.lac_state),
            Arc::clone(&self.inner.lac_value),
            Arc::clone(&self.inner.ensemble_info),
            use_deferred_sync,
            Some(Box::new(move || {
                dedup.lock().unwrap().remove(&entry_id);
            })),
        ));
        self.inner.ensemble_info.register_pending_ctx(entry_id, Arc::clone(&ctx));

        // Per-write quorum timeout.
        {
            let ctx_for_timeout = Arc::clone(&ctx);
            let closed_for_timeout = Arc::clone(&self.inner.closed);
            tokio::spawn(async move {
                tokio::time::sleep(quorum_timeout).await;
                if !closed_for_timeout.load(Ordering::Acquire) {
                    ctx_for_timeout.record_timeout(quorum_timeout);
                }
            });
        }

        // Spawn per-bookie write tasks against the shared ctx.
        for (pos, &ensemble_idx) in write_set.iter().enumerate() {
            let conn = Arc::clone(&conns_for_write[pos]);
            let entry = entry_bytes.clone();
            spawn_send_to_bookie(
                Arc::clone(&ctx),
                ensemble_idx,
                conn,
                entry,
                mk,
                use_deferred_sync,
                Arc::clone(&self.inner.closed),
            );
        }

        Ok(pending_add)
    }

    /// Synchronous append with explicit entry ID.
    pub async fn append_adv(
        &mut self,
        conns: &[Arc<BookieConnection>],
        entry_id: i64,
        payload: &[u8],
    ) -> Result<i64, ClientError> {
        crate::client_metrics::PENDING_ADDS.inc();
        let start = std::time::Instant::now();
        let result = self.append_adv_async(conns, entry_id, payload)?.wait().await;
        crate::client_metrics::PENDING_ADDS.dec();
        crate::client_metrics::ADD_ENTRY.observe(start.elapsed().as_secs_f64());
        result
    }

    pub fn last_add_confirmed(&self) -> i64 {
        self.inner.last_add_confirmed()
    }

    pub fn ledger_id(&self) -> i64 {
        self.inner.ledger_id()
    }

    pub fn set_deferred_sync(&mut self, enabled: bool) {
        self.inner.set_deferred_sync(enabled);
    }

    pub fn set_on_ensemble_change(&mut self, cb: OnEnsembleChange) {
        self.inner.set_on_ensemble_change(cb);
    }
}

/// Resolve a bookie's `host:port` (or `ip:port`) address string to a
/// `SocketAddr`. Mirrors the auditor / replication-worker pattern: try
/// the IP-literal fast path first, fall back to `tokio::net::lookup_host`
/// for DNS names. Without the fallback, deployments with DNS-named
/// bookies cannot recover from a bookie failure on an open ledger.
async fn resolve_bookie_address(addr: &str) -> Result<std::net::SocketAddr, ClientError> {
    if let Ok(sa) = addr.parse::<std::net::SocketAddr>() {
        return Ok(sa);
    }
    let mut iter = tokio::net::lookup_host(addr)
        .await
        .map_err(|e| ClientError::Protocol(format!("resolve {addr}: {e}")))?;
    iter.next()
        .ok_or_else(|| ClientError::Protocol(format!("no addresses for {addr}")))
}
