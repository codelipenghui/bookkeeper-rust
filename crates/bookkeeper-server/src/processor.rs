use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use bytes::Bytes;
use tokio::sync::Semaphore;

use bookkeeper_common::error::BKError;
use bookkeeper_common::types::LAST_ADD_CONFIRMED;
use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::v2::request::V2Request;
use bookkeeper_protocol::v2::response::V2Response;
use bookkeeper_storage::ledger_storage::LedgerStorage;
use tracing::warn;
use crate::state::BookieStateManager;

/// Default concurrency limits matching Java's defaults (0 = unlimited).
const DEFAULT_MAX_ADDS_IN_PROGRESS: usize = 10000;
const DEFAULT_MAX_READS_IN_PROGRESS: usize = 10000;

/// Default backpressure timeout: how long to wait for a semaphore permit
/// before rejecting. Matches Java's `waitTimeoutOnBackpressureMillis` behavior.
/// 0 = immediate rejection (legacy behavior). >0 = wait up to this duration,
/// creating TCP backpressure since the processing task blocks.
const DEFAULT_BACKPRESSURE_TIMEOUT_MS: u64 = 0;

/// Separate limit for high-priority requests (fence reads, recovery adds).
/// Matches Java's `numHighPriorityWorkerThreads` separate pool concept.
const DEFAULT_MAX_HIGH_PRIORITY_IN_PROGRESS: usize = 10000;

use bookkeeper_protocol::entry::peek_ids;

pub struct RequestProcessor {
    storage: Arc<dyn LedgerStorage>,
    state: Arc<BookieStateManager>,
    /// Semaphore gating concurrent add requests.
    /// Matches Java's `BookieRequestProcessor.addsSemaphore`.
    adds_semaphore: Semaphore,
    /// Semaphore gating concurrent read requests.
    /// Matches Java's `BookieRequestProcessor.readsSemaphore`.
    reads_semaphore: Semaphore,
    /// Separate semaphore for high-priority requests (fence reads, recovery adds).
    /// Matches Java's `highPriorityThreadPool` which ensures recovery operations
    /// make progress even when the normal pools are saturated.
    high_priority_semaphore: Semaphore,
    /// Backpressure timeout in milliseconds. When >0, the processor waits
    /// this long for a semaphore permit before rejecting with ETOOMANYREQUESTS.
    /// Creates TCP backpressure since the spawned task blocks.
    /// Matches Java's `acquireUninterruptibly()` behavior.
    backpressure_timeout_ms: u64,
    /// Whether high-priority writes are allowed in read-only mode.
    /// Set by disk monitor based on `minUsableSizeForHighPriorityWrites`.
    /// Matches Java's `availableForHighPriorityWrites` flag.
    /// Shared via Arc so the disk monitor can update it directly.
    available_for_high_priority_writes: Arc<AtomicBool>,
    /// When true, authentication is required (non-disabled provider configured).
    /// Per-connection auth state is tracked in the connection handler;
    /// the processor only needs to know whether to enforce it.
    require_auth: bool,
    /// Max entries per batch read response. Matches Java's entry-count safety limit.
    max_batch_read_count: i32,
    /// Max bytes per batch read response. Matches Java's `maxBatchReadSize`.
    max_batch_read_size_bytes: i64,
}

impl RequestProcessor {
    pub fn new(storage: Arc<dyn LedgerStorage>, state: Arc<BookieStateManager>) -> Self {
        Self::with_limits(storage, state, DEFAULT_MAX_ADDS_IN_PROGRESS, DEFAULT_MAX_READS_IN_PROGRESS)
    }

    pub fn with_limits(
        storage: Arc<dyn LedgerStorage>,
        state: Arc<BookieStateManager>,
        max_adds: usize,
        max_reads: usize,
    ) -> Self {
        Self {
            storage,
            state,
            adds_semaphore: Semaphore::new(max_adds),
            reads_semaphore: Semaphore::new(max_reads),
            high_priority_semaphore: Semaphore::new(DEFAULT_MAX_HIGH_PRIORITY_IN_PROGRESS),
            backpressure_timeout_ms: DEFAULT_BACKPRESSURE_TIMEOUT_MS,
            available_for_high_priority_writes: Arc::new(AtomicBool::new(true)),
            require_auth: false,
            max_batch_read_count: 10_000,
            max_batch_read_size_bytes: 5 * 1024 * 1024, // 5MB
        }
    }

    pub fn set_require_auth(&mut self, require: bool) {
        self.require_auth = require;
    }

    pub fn require_auth(&self) -> bool {
        self.require_auth
    }

    pub fn set_max_batch_read_count(&mut self, count: i32) {
        self.max_batch_read_count = count;
    }

    pub fn set_max_batch_read_size_bytes(&mut self, bytes: i64) {
        self.max_batch_read_size_bytes = bytes;
    }

    /// Set the backpressure timeout. When >0, admission control waits for
    /// a permit instead of rejecting immediately, creating TCP backpressure.
    pub fn set_backpressure_timeout_ms(&mut self, ms: u64) {
        self.backpressure_timeout_ms = ms;
    }

    /// Return a shared reference to the high-priority-writes flag.
    /// Used by the disk monitor to update the flag during state transitions.
    pub fn high_priority_writes_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.available_for_high_priority_writes)
    }

    /// Acquire a semaphore permit with optional backpressure.
    /// Fast path: try_acquire (no timer overhead). Slow path: timeout + acquire.
    async fn acquire_permit<'a>(
        &self,
        semaphore: &'a Semaphore,
    ) -> Option<tokio::sync::SemaphorePermit<'a>> {
        // Always try non-blocking first to avoid timer-wheel overhead
        // on the hot path when permits are available.
        if let Ok(p) = semaphore.try_acquire() {
            return Some(p);
        }
        // Permit not immediately available
        if self.backpressure_timeout_ms == 0 {
            // No backpressure — reject immediately (legacy mode)
            return None;
        }
        // Sentinel "wait forever" — matches Java's acquireUninterruptibly().
        // Using a plain .await avoids tokio timer overhead and sidesteps any
        // Duration arithmetic overflow for extremely large ms values.
        if self.backpressure_timeout_ms >= u64::MAX / 4 {
            return semaphore.acquire().await.ok();
        }
        // Bounded wait — creates TCP backpressure
        tokio::time::timeout(
            std::time::Duration::from_millis(self.backpressure_timeout_ms),
            semaphore.acquire(),
        ).await.ok()?.ok()
    }

    pub fn storage(&self) -> &dyn LedgerStorage {
        &*self.storage
    }

    pub async fn process_request(&self, req: V2Request) -> V2Response {
        match req {
            V2Request::AddEntry { flags, master_key, entry_data, deferred_sync } => {
                // Route high-priority requests (recovery adds) to a separate
                // semaphore, matching Java's highPriorityThreadPool.
                let is_recovery = flags & FLAG_RECOVERY_ADD != 0;
                let is_high_priority = flags & FLAG_HIGH_PRIORITY != 0;
                let semaphore = if is_recovery || is_high_priority {
                    &self.high_priority_semaphore
                } else {
                    &self.adds_semaphore
                };

                let _permit = match self.acquire_permit(semaphore).await {
                    Some(p) => p,
                    None => {
                        crate::metrics::ADD_ENTRY_REJECTED.inc();
                        let (ledger_id, entry_id) = peek_ids(&entry_data).unwrap_or((0, 0));
                        return V2Response::Add { error_code: BKError::TooManyRequests.to_v2_code(), ledger_id, entry_id };
                    }
                };
                let entry_size = entry_data.len();
                crate::metrics::ADD_ENTRY_IN_PROGRESS.inc();
                let timer = std::time::Instant::now();
                let resp = self.handle_add_entry(flags, &master_key, entry_data, deferred_sync).await;
                let elapsed = timer.elapsed().as_secs_f64();
                crate::metrics::ADD_ENTRY_IN_PROGRESS.dec();
                // `BOOKIE_*` histograms capture storage processing time.
                // End-to-end `ADD_ENTRY_REQUEST` is observed by the writer task
                // once the response is flushed (Java-parity with enqueueNanos).
                let success = matches!(resp, V2Response::Add { error_code, .. } if error_code == V2_EOK);
                if success {
                    crate::metrics::BOOKIE_ADD_ENTRY_OK.observe(elapsed);
                    if is_recovery {
                        crate::metrics::BOOKIE_RECOVERY_ADD_ENTRY_OK.observe(elapsed);
                    }
                    crate::metrics::WRITE_BYTES.inc_by(entry_size as u64);
                    crate::metrics::BOOKIE_ADD_ENTRY_BYTES_OK.observe(entry_size as f64);
                } else {
                    crate::metrics::BOOKIE_ADD_ENTRY_FAIL.observe(elapsed);
                    if is_recovery {
                        crate::metrics::BOOKIE_RECOVERY_ADD_ENTRY_FAIL.observe(elapsed);
                    }
                }
                resp
            }
            V2Request::ReadEntry { flags, ledger_id, entry_id, master_key } => {
                // Route fence and high-priority reads to the high-priority semaphore.
                // Matches Java's BookieRequestProcessor which dispatches
                // `isHighPriority(request) || hasFlag(FENCE_LEDGER)` to the
                // highPriorityThreadPool for guaranteed progress.
                let is_fence = flags & FLAG_DO_FENCING != 0;
                let is_high_priority = flags & FLAG_HIGH_PRIORITY != 0;
                let semaphore = if is_fence || is_high_priority {
                    &self.high_priority_semaphore
                } else {
                    &self.reads_semaphore
                };

                let _permit = match self.acquire_permit(semaphore).await {
                    Some(p) => p,
                    None => {
                        crate::metrics::READ_ENTRY_REJECTED.inc();
                        return V2Response::Read { error_code: BKError::TooManyRequests.to_v2_code(), ledger_id, entry_id, data: None };
                    }
                };
                crate::metrics::READ_ENTRY_IN_PROGRESS.inc();
                let timer = std::time::Instant::now();
                let resp = self.handle_read_entry(flags, ledger_id, entry_id, master_key).await;
                let elapsed = timer.elapsed().as_secs_f64();
                crate::metrics::READ_ENTRY_IN_PROGRESS.dec();
                // End-to-end `READ_ENTRY_REQUEST` / `READ_ENTRY_FENCE_REQUEST`
                // are recorded by the writer task once the response is flushed.
                let success = matches!(resp, V2Response::Read { error_code, .. } if error_code == V2_EOK);
                if success {
                    crate::metrics::BOOKIE_READ_ENTRY_OK.observe(elapsed);
                    if let V2Response::Read { data: Some(ref d), .. } = resp {
                        let sz = d.len() as u64;
                        crate::metrics::READ_BYTES.inc_by(sz);
                        crate::metrics::BOOKIE_READ_ENTRY_BYTES_OK.observe(sz as f64);
                    }
                } else {
                    crate::metrics::BOOKIE_READ_ENTRY_FAIL.observe(elapsed);
                }
                resp
            }
            V2Request::WriteLac { ledger_id, lac, master_key, body } => {
                // Validate master key before writing LAC
                if let Err(e) = self.storage.check_master_key(ledger_id, &master_key) {
                    return V2Response::WriteLac {
                        error_code: self.storage_error_to_bk(&e).to_v2_code(),
                        ledger_id,
                        entry_id: 0,
                    };
                }
                // End-to-end `WRITE_LAC_REQUEST` recorded by the writer task.
                self.handle_write_lac(ledger_id, lac, body).await
            }
            V2Request::ReadLac { ledger_id } => {
                // End-to-end `READ_LAC_REQUEST` recorded by the writer task.
                self.handle_read_lac(ledger_id).await
            }
            V2Request::GetBookieInfo { flags: _, requested } => {
                // End-to-end `GET_BOOKIE_INFO_REQUEST` recorded by the writer task.
                self.handle_get_bookie_info(requested)
            }
            V2Request::Auth { payload: _ } => {
                // Auth disabled: return success with empty payload.
                // Matches Java's AuthenticationDisabledAuthProviderFactory behavior.
                V2Response::Auth { error_code: V2_EOK, payload: bytes::Bytes::new() }
            }
            V2Request::BatchReadEntry { flags, ledger_id, start_entry_id, request_id, max_count, max_size, master_key } => {
                // If FLAG_DO_FENCING is set, validate masterKey and fence the
                // ledger before the read.  Matches Java's BatchedReadEntryProcessor
                // which calls fenceLedger() first when the flag is set.
                if flags & FLAG_DO_FENCING != 0 {
                    if let Some(key) = master_key {
                        if let Err(e) = self.storage.check_master_key(ledger_id, &key) {
                            return V2Response::BatchRead {
                                error_code: self.storage_error_to_bk(&e).to_v2_code(),
                                ledger_id,
                                entry_id: start_entry_id,
                                request_id,
                                entries: Vec::new(),
                            };
                        }
                        if let Err(e) = self.storage.set_fenced(ledger_id) {
                            return V2Response::BatchRead {
                                error_code: self.storage_error_to_bk(&e).to_v2_code(),
                                ledger_id,
                                entry_id: start_entry_id,
                                request_id,
                                entries: Vec::new(),
                            };
                        }
                    }
                }
                self.handle_batch_read(flags, ledger_id, start_entry_id, request_id, max_count, max_size).await
            }
        }
    }

    async fn handle_add_entry(&self, flags: u16, master_key: &[u8; MASTER_KEY_LENGTH], entry_data: Bytes, deferred_sync: bool) -> V2Response {
        if entry_data.len() < 16 {
            return self.add_error_response(BKError::BadRequest, 0, 0);
        }
        let (ledger_id, entry_id) = peek_ids(&entry_data).unwrap_or((0, 0));
        let is_recovery = flags & FLAG_RECOVERY_ADD != 0;
        let is_high_priority = flags & FLAG_HIGH_PRIORITY != 0;

        // Allow high-priority recovery adds even when read-only, but only
        // if the disk monitor has flagged space as available for high-priority writes.
        // Matches Java's WriteEntryProcessorV3: if bookie is read-only, only allow
        // high-priority writes when `availableForHighPriorityWrites` is true.
        if !self.state.is_writable() {
            let allow_high_priority = (is_recovery || is_high_priority)
                && self.available_for_high_priority_writes.load(Ordering::Acquire);
            if !allow_high_priority {
                return self.add_error_response(BKError::ReadOnly, ledger_id, entry_id);
            }
        }

        // Single RocksDB read: validate existence + master key + get fenced status
        match self.storage.validate_for_write(ledger_id, master_key) {
            Ok(is_fenced) => {
                if is_fenced && !is_recovery {
                    return self.add_error_response(BKError::Fenced, ledger_id, entry_id);
                }
                if is_recovery {
                    if let Err(e) = self.storage.set_fenced(ledger_id) {
                        // Best-effort fencing for recovery adds — only fail on serious errors
                        if !matches!(e, bookkeeper_storage::StorageError::LedgerNotFound(_)) {
                            return self.add_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id);
                        }
                    }
                }
            }
            Err(e) => return self.add_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id),
        }

        // Matches Java's BookieImpl.addEntryInternal:
        // 1. Storage write (in-memory write cache — fast)
        // 2. Master key journaling (first write per ledger)
        // 3. Data entry journaling (durability)
        match self.storage.add_entry(&entry_data).await {
            Ok(()) => {
                if let Some(journal) = self.storage.journal() {
                    // Journal master key BEFORE data entry (matching Java's
                    // ordering). A send failure here means the channel to
                    // the writer thread is broken — the data entry that
                    // follows would land but the master-key record would
                    // be missing on recovery, leaving the ledger without
                    // its password binding. Surface the error to the
                    // client rather than acking a half-journaled add.
                    if let Err(_e) = journal.maybe_journal_master_key(ledger_id, master_key) {
                        return self.add_error_response(BKError::Io, ledger_id, entry_id);
                    }
                    if deferred_sync {
                        // DEFERRED_SYNC: enqueue to journal without waiting for fsync.
                        // Durability guaranteed by the next ForceLedger call.
                        // Matches Java's ackBeforeSync = writeFlags.contains(DEFERRED_SYNC).
                        if let Err(_e) = journal.log_add_entry_deferred(&entry_data) {
                            return self.add_error_response(BKError::Io, ledger_id, entry_id);
                        }
                    } else {
                        // Normal path: await journal durability before ack
                        if let Err(_e) = journal.log_add_entry(&entry_data).await {
                            return self.add_error_response(BKError::Io, ledger_id, entry_id);
                        }
                    }
                }
                // Fence-then-add re-check. validate_for_write returned the
                // fence flag at the entry of this function, but the BK
                // protocol's "no add confirmed after a fence ack" invariant
                // requires that any fence which landed between that check
                // and the moment of ack reject this add. Recovery adds are
                // exempt — they are the mechanism that closes a fenced
                // ledger.
                if !is_recovery {
                    if let Ok(true) = self.storage.is_fenced(ledger_id) {
                        return self.add_error_response(BKError::Fenced, ledger_id, entry_id);
                    }
                }
                V2Response::Add { error_code: V2_EOK, ledger_id, entry_id }
            }
            Err(e) => self.add_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id),
        }
    }

    async fn handle_read_entry(&self, flags: u16, ledger_id: i64, entry_id: i64, master_key: Option<[u8; MASTER_KEY_LENGTH]>) -> V2Response {
        // Check readable state
        if !self.state.is_readable() {
            return self.read_error_response(BKError::ReadOnly, ledger_id, entry_id);
        }

        // Handle fencing — journal the fence entry before responding.
        // Matches Java's fenceAndLogInJournal() which writes a METAENTRY_ID_FENCE_KEY
        // journal entry and only completes the future after fsync, ensuring durability.
        if flags & FLAG_DO_FENCING != 0 {
            if let Some(key) = master_key {
                // Validate master key before fencing
                if let Err(e) = self.storage.check_master_key(ledger_id, &key) {
                    return self.read_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id);
                }
                if let Err(e) = self.storage.set_fenced(ledger_id) {
                    return self.read_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id);
                }
                // Journal the fence for durability before responding
                if let Some(journal) = self.storage.journal() {
                    if let Err(_e) = journal.journal_fence(ledger_id).await {
                        return self.read_error_response(BKError::Io, ledger_id, entry_id);
                    }
                }
            }
        }

        // Special case: LAST_ADD_CONFIRMED (-1)
        // Java client expects the last entry body so it can verify the digest
        // and extract LAC+length. If we return EOK with an empty body, the
        // client's CRC32CDigestManager tries to validate a digest on 0 bytes
        // and rejects the response with "Mac mismatch" — which blocks topic
        // recovery for any ledger that's newly created but has no committed
        // entries yet (a common state for fresh partitioned topics).
        //
        // The correct response for an empty ledger is NoSuchEntry; the client
        // treats that as "no committed entry, proceed with LAC = -1".
        if entry_id == LAST_ADD_CONFIRMED {
            match self.storage.get_last_add_confirmed(ledger_id) {
                Ok(lac) => {
                    if lac < 0 {
                        return self.read_error_response(
                            BKError::NoEntry, ledger_id, entry_id,
                        );
                    }
                    match self.storage.get_entry(ledger_id, lac).await {
                        Ok(data) => {
                            return V2Response::Read {
                                error_code: V2_EOK,
                                ledger_id,
                                entry_id: lac,
                                data: Some(data),
                            };
                        }
                        Err(e) => {
                            // LAC advertises an entry we can't actually read
                            // (deleted / corrupted). Same problem as above:
                            // an EOK with empty body fails digest validation.
                            warn!(ledger_id, entry_id, error = ?e,
                                  "LAC entry body read failed");
                            return self.read_error_response(
                                BKError::NoEntry, ledger_id, entry_id,
                            );
                        }
                    }
                }
                Err(_) => {
                    return self.read_error_response(BKError::NoLedger, ledger_id, entry_id);
                }
            }
        }

        // Normal read
        match self.storage.get_entry(ledger_id, entry_id).await {
            Ok(data) => V2Response::Read {
                error_code: V2_EOK,
                ledger_id,
                entry_id,
                data: Some(data),
            },
            Err(e) => {
                // `storage_error_to_bk` collapses every variant outside the
                // named ones to `BKError::Io`, which the bookie client sees
                // as opaque EIO. Log the actual variant so it's diagnosable.
                warn!(ledger_id, entry_id, error = ?e, "read_entry failed");
                self.read_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id)
            }
        }
    }

    async fn handle_write_lac(&self, ledger_id: i64, lac: i64, body: Bytes) -> V2Response {
        // Store LAC value
        if let Err(e) = self.storage.set_explicit_lac(ledger_id, lac) {
            return V2Response::WriteLac {
                error_code: self.storage_error_to_bk(&e).to_v2_code(),
                ledger_id,
                entry_id: 0,
            };
        }
        // Store raw LAC body (with digest) for ReadLac to return.
        // Matches Java's TransientLedgerInfo.setExplicitLac(ByteBuf).
        if !body.is_empty() {
            let _ = self.storage.set_explicit_lac_body(ledger_id, &body);
        }
        // Journal the explicit LAC for durability before responding.
        // Matches Java's setExplicitLac() which journals a
        // METAENTRY_ID_LEDGER_EXPLICITLAC entry with ackBeforeSync=false.
        if let Some(journal) = self.storage.journal() {
            if let Err(_e) = journal.journal_explicit_lac(ledger_id, &body).await {
                return V2Response::WriteLac {
                    error_code: BKError::Io.to_v2_code(),
                    ledger_id,
                    entry_id: 0,
                };
            }
        }
        V2Response::WriteLac {
            error_code: V2_EOK,
            ledger_id,
            entry_id: 0,
        }
    }

    /// ReadLac returns two pieces of data matching Java's ReadLacResponse:
    /// 1. lac_body: The explicit LAC from the most recent WriteLac request (with digest)
    /// 2. last_entry_body: The actual last entry on disk (piggybacked LAC)
    /// The client compares both to determine the true LAC.
    async fn handle_read_lac(&self, ledger_id: i64) -> V2Response {
        let lac = match self.storage.get_last_add_confirmed(ledger_id) {
            Ok(l) => l,
            Err(e) => {
                return V2Response::ReadLac {
                    error_code: self.storage_error_to_bk(&e).to_v2_code(),
                    ledger_id,
                    entry_id: 0,
                    lac_body: None,
                    last_entry_body: None,
                };
            }
        };

        // Get explicit LAC body from WriteLac cache
        let lac_body = self
            .storage
            .get_explicit_lac_body(ledger_id)
            .ok()
            .flatten()
            .map(Bytes::from);

        // Get last entry body from disk (piggybacked LAC)
        let last_entry_body = if lac >= 0 {
            self.storage.get_entry(ledger_id, lac).await.ok()
        } else {
            None
        };

        V2Response::ReadLac {
            error_code: V2_EOK,
            ledger_id,
            entry_id: lac,
            lac_body,
            last_entry_body,
        }
    }

    fn handle_get_bookie_info(&self, _requested: i64) -> V2Response {
        let (free, total) = get_disk_space();
        V2Response::GetBookieInfo {
            error_code: V2_EOK,
            ledger_id: 0,
            entry_id: 0,
            free_disk_space: free,
            total_disk_capacity: total,
        }
    }

    async fn handle_batch_read(&self, _flags: u16, ledger_id: i64, start_entry_id: i64, request_id: i64, max_count: i32, max_size: i64) -> V2Response {
        // Check readable state
        if !self.state.is_readable() {
            return V2Response::BatchRead {
                error_code: BKError::ReadOnly.to_v2_code(),
                ledger_id,
                entry_id: start_entry_id,
                request_id,
                entries: vec![],
            };
        }

        // batch_read does not fence: V2 BatchReadEntry codec carries no master key.

        // Cap max_count using the configured limit (default 10K).
        let capped_count = max_count.clamp(0, self.max_batch_read_count);

        let mut entries = Vec::new();
        let mut total_size: i64 = 0;
        // Use the smaller of client-requested max_size and server-configured limit
        let effective_max_size = if max_size > 0 {
            max_size.min(self.max_batch_read_size_bytes)
        } else {
            self.max_batch_read_size_bytes
        };
        for i in 0..capped_count as i64 {
            let eid = start_entry_id + i;
            match self.storage.get_entry(ledger_id, eid).await {
                Ok(data) => {
                    total_size += data.len() as i64;
                    if total_size > effective_max_size && !entries.is_empty() {
                        break;
                    }
                    entries.push(data);
                }
                Err(bookkeeper_storage::StorageError::EntryNotFound { .. }) => {
                    // Normal end-of-batch — Pulsar over-requests and relies
                    // on this sentinel. Not a warning.
                    break;
                }
                Err(e) => {
                    warn!(
                        ledger_id, entry_id = eid, request_id,
                        returned = entries.len(),
                        error = ?e,
                        "batch_read entry failed (loop terminates)",
                    );
                    break;
                }
            }
        }

        V2Response::BatchRead {
            error_code: if entries.is_empty() {
                BKError::NoEntry.to_v2_code()
            } else {
                V2_EOK
            },
            ledger_id,
            entry_id: start_entry_id,
            request_id,
            entries,
        }
    }

    pub async fn force_journal_sync(&self) -> Result<(), bookkeeper_storage::StorageError> {
        self.storage.force_journal_sync().await
    }

    fn add_error_response(&self, err: BKError, ledger_id: i64, entry_id: i64) -> V2Response {
        V2Response::Add {
            error_code: err.to_v2_code(),
            ledger_id,
            entry_id,
        }
    }

    fn read_error_response(&self, err: BKError, ledger_id: i64, entry_id: i64) -> V2Response {
        V2Response::Read { error_code: err.to_v2_code(), ledger_id, entry_id, data: None }
    }

    fn storage_error_to_bk(&self, err: &bookkeeper_storage::StorageError) -> BKError {
        match err {
            bookkeeper_storage::StorageError::EntryNotFound { .. } => BKError::NoEntry,
            bookkeeper_storage::StorageError::LedgerNotFound(_) => BKError::NoLedger,
            bookkeeper_storage::StorageError::Fenced(_) => BKError::Fenced,
            bookkeeper_storage::StorageError::StorageFull => BKError::ReadOnly,
            bookkeeper_storage::StorageError::Unauthorized(_) => BKError::Unauthorized,
            // Matches Java's `OperationRejectedException` → ETOOMANYREQUESTS.
            // Signals the client to back off / retry on another bookie.
            bookkeeper_storage::StorageError::OperationRejected => BKError::TooManyRequests,
            _ => BKError::Io,
        }
    }
}

fn get_disk_space() -> (i64, i64) {
    match bookkeeper_common::disk::statvfs_bytes(std::path::Path::new("/")) {
        Ok((total, available)) => (available as i64, total as i64),
        Err(_) => (0, 0),
    }
}
