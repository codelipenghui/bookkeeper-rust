use std::collections::HashSet;
use std::fs;
use std::io::{self, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel as mpsc;

use super::format::{self, JournalHeader, calculate_padding};
use super::metrics;
use crate::StorageError;

/// Meta-entry ID for master key journaling.
pub const METAENTRY_ID_LEDGER_KEY: i64 = -0x1000;
/// Meta-entry ID for fence journaling. Matches Java's `METAENTRY_ID_FENCE_KEY`.
pub const METAENTRY_ID_FENCE_KEY: i64 = -0x2000;
/// Meta-entry ID for explicit LAC journaling. Matches Java's `METAENTRY_ID_LEDGER_EXPLICITLAC`.
pub const METAENTRY_ID_LEDGER_EXPLICITLAC: i64 = -0x8000;

/// Default journal queue memory in bytes (32MB, matching Java).
const DEFAULT_MAX_QUEUE_MEMORY: usize = 32 * 1024 * 1024;

/// A journal entry to be written.
struct JournalEntry {
    data: Vec<u8>,
    ack_tx: Option<tokio::sync::oneshot::Sender<()>>,
    enqueue_time: Instant,
}

/// Configuration for the journal writer.
#[derive(Clone)]
pub struct JournalConfig {
    pub journal_dir: PathBuf,
    pub max_file_size: u64,
    pub sync_data: bool,
    pub max_group_wait_ms: u64,
    pub buffered_writes_threshold: usize,
    pub buffered_entries_threshold: usize,
    pub alignment_size: usize,
    pub format_version: i32,
    /// Max journal queue memory in bytes. 0 = use default (32MB).
    /// Matches Java's `journalMaxMemorySizeMb`.
    pub max_queue_memory: usize,
    /// When true, flush immediately when the queue is empty after draining
    /// (reduces latency at low throughput). Matches Java's `journalFlushWhenQueueEmpty`.
    pub flush_when_queue_empty: bool,
    /// When true, call posix_fadvise(DONTNEED) after journal writes to evict
    /// written pages from OS page cache. Linux-only. Matches Java's `journalRemoveFromPageCache`.
    pub remove_from_page_cache: bool,
    /// When true, old journal files are renamed and reused instead of deleted
    /// and recreated. Avoids filesystem metadata overhead on rotation.
    /// Matches Java's `journalReuseFiles`.
    pub reuse_files: bool,
    /// Max number of old journal files to keep in the reuse pool.
    /// Matches Java's `maxBackupJournals`.
    pub max_backup_journals: usize,
    /// Test-only: force the std-engine + force-write-thread mode (Mode 2)
    /// even on Linux where io_uring is available. Production code should
    /// leave this `false`; flipping it lets integration tests exercise
    /// the force-write thread's grouping/fsync invariants on hosts where
    /// io_uring would otherwise be selected.
    #[doc(hidden)]
    pub force_std_engine_mode: bool,
    /// Test-only: invoked from the force-write thread (Mode 2) once for
    /// every successful fsync, with the journal log_id whose data was
    /// just durably synced. Lets integration tests verify that every
    /// log_id touched by the writer thread is fsynced — catching bugs
    /// where rotation-spanning groups would skip older log_ids.
    /// Default `None`. Production code should leave it unset; the
    /// closure runs on the force-write thread's hot path.
    #[doc(hidden)]
    pub fsync_observer: Option<Arc<dyn Fn(u64) + Send + Sync>>,
}

/// The journal — write-ahead log for crash recovery.
///
/// Three operating modes depending on platform and config:
///
/// 1. **io_uring + sync** (Linux): Single `journal-uring` thread. Writes
///    entries via `write_all`, submits FSYNC as a non-blocking io_uring SQE,
///    then continues collecting + writing entries while the kernel syncs.
///    ACKs all entries when the CQE completes. Maximum batching — entries
///    written during the fsync are covered by the next fsync.
///
/// 2. **StdEngine + sync**: Two threads. Writer thread does `write_all` and
///    sends to force-write thread. Force-write thread calls blocking
///    `fdatasync()` and ACKs. Matches Java's Journal + ForceWriteThread.
///
/// 3. **No sync** (any engine): Single writer thread. ACKs immediately
///    after `write_all`. No force-write thread.
pub type OnFatalError = Box<dyn Fn(io::Error) + Send + Sync + 'static>;

pub struct Journal {
    tx: mpsc::Sender<JournalEntry>,
    _threads: Vec<std::thread::JoinHandle<()>>,
    master_key_cache: std::sync::Mutex<HashSet<i64>>,
    current_log_id: Arc<std::sync::atomic::AtomicU64>,
    /// Last durably-synced (log_id, byte_offset) pair.
    /// Updated atomically after fsync succeeds. `offset` is the position in
    /// the journal file — including the header — immediately after the last
    /// fsync'd byte. Matches Java's `LastLogMark.curMark` (log_id, offset),
    /// which is advanced in `Journal.ForceWriteThread.syncJournal` via
    /// `lastLogMark.setCurLogMark(logId, lastFlushedPosition)`.
    ///
    /// Used by `newCheckpoint()` so crash-recovery replay skips data already
    /// known to be durable, instead of rescanning the whole journal file.
    last_log_mark: Arc<std::sync::Mutex<(u64, u64)>>,
}

impl Journal {
    pub fn start(
        config: JournalConfig,
        _journal_io: std::sync::Arc<dyn crate::io_engine::JournalIo>,
        on_fatal_error: Option<OnFatalError>,
    ) -> Self {
        let queue_mem = if config.max_queue_memory > 0 {
            config.max_queue_memory
        } else {
            DEFAULT_MAX_QUEUE_MEMORY
        };
        let bound = queue_mem / 1024;
        let (tx, rx) = mpsc::bounded(bound);

        let current_log_id = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let shared_log_id = current_log_id.clone();
        let last_log_mark = Arc::new(std::sync::Mutex::new((0u64, 0u64)));
        let shared_last_mark = last_log_mark.clone();

        let mut threads = Vec::new();

        #[cfg(target_os = "linux")]
        let use_uring = config.sync_data
            && !config.force_std_engine_mode
            && crate::io_engine::uring_available();
        #[cfg(not(target_os = "linux"))]
        let use_uring = false;

        if use_uring {
            // Mode 1: single io_uring thread (write + async fsync)
            #[cfg(target_os = "linux")]
            {
                let uring_config = config.clone();
                let t = std::thread::Builder::new()
                    .name("journal-uring".into())
                    .spawn(move || {
                        journal_uring_loop(uring_config, rx, on_fatal_error, shared_log_id, shared_last_mark)
                    })
                    .expect("failed to spawn journal-uring thread");
                threads.push(t);
            }
        } else if config.sync_data {
            // Mode 2: two threads (writer + blocking force-write)
            let (fw_tx, fw_rx) = mpsc::unbounded::<ForceWriteRequest>();

            // Share the fatal-error callback between both threads so that
            // an fsync failure in the force-write thread also triggers the
            // bookie's transition to read-only mode.
            let shared_cb: Option<Arc<dyn Fn(io::Error) + Send + Sync>> =
                on_fatal_error.map(|cb| Arc::from(cb) as Arc<dyn Fn(io::Error) + Send + Sync>);
            let fw_cb = shared_cb.clone();
            let fw_last_mark = shared_last_mark.clone();
            let fw_observer = config.fsync_observer.clone();

            let sync_thread = std::thread::Builder::new()
                .name("journal-sync".into())
                .spawn(move || force_write_loop(fw_rx, fw_cb, fw_last_mark, fw_observer))
                .expect("failed to spawn journal-sync thread");
            threads.push(sync_thread);

            let writer_config = config.clone();
            let writer_thread = std::thread::Builder::new()
                .name("journal-writer".into())
                .spawn(move || {
                    // Mode 2: writer hands off to force-write; mark is
                    // updated there (after fsync). Pass a dummy Arc
                    // that will never be touched.
                    journal_writer_loop(writer_config, rx, Some(fw_tx), shared_cb, shared_log_id,
                        Arc::new(std::sync::Mutex::new((0u64, 0u64))))
                })
                .expect("failed to spawn journal-writer thread");
            threads.push(writer_thread);
        } else {
            // Mode 3: single writer thread, no sync
            let writer_config = config.clone();
            let cb: Option<Arc<dyn Fn(io::Error) + Send + Sync>> =
                on_fatal_error.map(|cb| Arc::from(cb) as Arc<dyn Fn(io::Error) + Send + Sync>);
            let writer_thread = std::thread::Builder::new()
                .name("journal-writer".into())
                .spawn(move || {
                    // Mode 3 (no-sync): advance mark after write_all since
                    // that's the strongest durability we have.
                    journal_writer_loop(writer_config, rx, None, cb, shared_log_id, shared_last_mark)
                })
                .expect("failed to spawn journal-writer thread");
            threads.push(writer_thread);
        }

        Self {
            tx,
            _threads: threads,
            master_key_cache: std::sync::Mutex::new(HashSet::new()),
            current_log_id,
            last_log_mark,
        }
    }

    pub async fn log_add_entry(&self, data: &[u8]) -> Result<(), StorageError> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let enqueue_time = Instant::now();
        metrics::JOURNAL_QUEUE_SIZE.inc();
        let entry = JournalEntry {
            data: data.to_vec(),
            ack_tx: Some(ack_tx),
            enqueue_time,
        };
        // Fast path: try_send is non-blocking and avoids spawn_blocking overhead.
        // This succeeds >99% of the time since the journal queue rarely fills up.
        // Only fall back to spawn_blocking when the queue is actually full,
        // providing backpressure without stalling the Tokio runtime.
        match self.tx.try_send(entry) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(entry)) => {
                // Queue full — use spawn_blocking for backpressure
                let tx = self.tx.clone();
                tokio::task::spawn_blocking(move || tx.send(entry))
                    .await
                    .map_err(|_| {
                        metrics::JOURNAL_QUEUE_SIZE.dec();
                        StorageError::Io(io::Error::new(
                            io::ErrorKind::BrokenPipe, "journal spawn_blocking failed",
                        ))
                    })?
                    .map_err(|_| {
                        metrics::JOURNAL_QUEUE_SIZE.dec();
                        StorageError::Io(io::Error::new(
                            io::ErrorKind::BrokenPipe, "journal writer closed",
                        ))
                    })?;
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                metrics::JOURNAL_QUEUE_SIZE.dec();
                return Err(StorageError::Io(io::Error::new(
                    io::ErrorKind::BrokenPipe, "journal writer closed",
                )));
            }
        }
        ack_rx.await.map_err(|_| StorageError::Io(io::Error::new(
            io::ErrorKind::BrokenPipe, "journal ack lost",
        )))
    }

    /// Force the journal to fsync all previously written data.
    /// Used by ForceLedger to ensure deferred-sync entries reach disk.
    /// Enqueues a zero-length barrier entry with an ack; when the ack
    /// returns, all preceding entries have been durably synced.
    pub async fn force_sync(&self) -> Result<(), StorageError> {
        // Re-use the same path as log_add_entry: enqueue with an ack
        // and wait for it. The journal thread will write+sync this
        // (empty) entry along with anything queued before it.
        self.log_add_entry(&[]).await
    }

    /// Enqueue an entry to the journal without waiting for fsync.
    /// Used for DEFERRED_SYNC writes where the client will call ForceLedger
    /// later to ensure durability. Matches Java's `ackBeforeSync` behavior.
    ///
    /// Returns an error if the journal queue is full or the writer thread
    /// has shut down, so the caller can reject the write with backpressure
    /// instead of silently losing data.
    pub fn log_add_entry_deferred(&self, data: &[u8]) -> Result<(), StorageError> {
        metrics::JOURNAL_QUEUE_SIZE.inc();
        match self.tx.try_send(JournalEntry {
            data: data.to_vec(),
            ack_tx: None,
            enqueue_time: Instant::now(),
        }) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(_)) => {
                metrics::JOURNAL_QUEUE_SIZE.dec();
                Err(StorageError::Io(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "journal queue full, cannot accept deferred-sync entry",
                )))
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                metrics::JOURNAL_QUEUE_SIZE.dec();
                Err(StorageError::Io(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "journal writer closed",
                )))
            }
        }
    }

    pub fn maybe_journal_master_key(
        &self,
        ledger_id: i64,
        master_key: &[u8],
    ) -> Result<(), StorageError> {
        let mut cache = self.master_key_cache.lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if cache.contains(&ledger_id) {
            return Ok(());
        }
        cache.insert(ledger_id);
        drop(cache);

        let entry_len = 8 + 8 + 4 + master_key.len();
        let mut entry = Vec::with_capacity(entry_len);
        entry.extend_from_slice(&ledger_id.to_be_bytes());
        entry.extend_from_slice(&METAENTRY_ID_LEDGER_KEY.to_be_bytes());
        entry.extend_from_slice(&(master_key.len() as i32).to_be_bytes());
        entry.extend_from_slice(master_key);

        metrics::JOURNAL_QUEUE_SIZE.inc();
        let _ = self.tx.send(JournalEntry {
            data: entry,
            ack_tx: None,
            enqueue_time: Instant::now(),
        });

        Ok(())
    }

    /// Journal a fence operation for `ledger_id`. The entry is written to the
    /// journal and the method returns only after fsync completes.
    /// Matches Java's `fenceAndLogInJournal()` which writes a
    /// `METAENTRY_ID_FENCE_KEY` entry before responding to the client.
    pub async fn journal_fence(&self, ledger_id: i64) -> Result<(), StorageError> {
        // Fence journal entry: [ledger_id:8][METAENTRY_ID_FENCE_KEY:8]
        let mut entry = Vec::with_capacity(16);
        entry.extend_from_slice(&ledger_id.to_be_bytes());
        entry.extend_from_slice(&METAENTRY_ID_FENCE_KEY.to_be_bytes());
        self.log_add_entry(&entry).await
    }

    /// Journal an explicit LAC write. The entry is written to the journal
    /// and the method returns only after fsync completes.
    /// Matches Java's `setExplicitLac()` which journals a
    /// `METAENTRY_ID_LEDGER_EXPLICITLAC` entry for crash recovery.
    pub async fn journal_explicit_lac(
        &self,
        ledger_id: i64,
        lac_body: &[u8],
    ) -> Result<(), StorageError> {
        // LAC journal entry: [ledger_id:8][METAENTRY_ID_LEDGER_EXPLICITLAC:8][body...]
        let mut entry = Vec::with_capacity(16 + lac_body.len());
        entry.extend_from_slice(&ledger_id.to_be_bytes());
        entry.extend_from_slice(&METAENTRY_ID_LEDGER_EXPLICITLAC.to_be_bytes());
        entry.extend_from_slice(lac_body);
        self.log_add_entry(&entry).await
    }

    pub fn current_log_id(&self) -> Option<u64> {
        let id = self.current_log_id.load(std::sync::atomic::Ordering::Relaxed);
        if id == 0 { None } else { Some(id) }
    }

    /// Snapshot the last durably-synced `(log_id, offset)` for use as a
    /// checkpoint. Matches Java's `Journal.newCheckpoint()` →
    /// `lastLogMark.markLog()` which reads `curMark` (updated in
    /// `ForceWriteThread.syncJournal` after each fsync).
    ///
    /// Returns `None` if no data has ever been durably synced yet.
    pub fn current_mark(&self) -> Option<(u64, u64)> {
        let guard = self.last_log_mark.lock().unwrap_or_else(|p| p.into_inner());
        let (log_id, offset) = *guard;
        if log_id == 0 { None } else { Some((log_id, offset)) }
    }
}

// ---------------------------------------------------------------------------
// Force-write request — sent from writer thread to sync thread
// ---------------------------------------------------------------------------

struct ForceWriteRequest {
    /// Owned (dup'd) file handle for the journal file these acks belong to.
    /// Carrying ownership keeps the underlying fd alive across the writer
    /// thread's `state.rotate()` (which drops its own `File` and would
    /// otherwise close the fd before the force-write thread can fsync it).
    /// Each request holds its own fd via `File::try_clone()`.
    file: fs::File,
    acks: Vec<tokio::sync::oneshot::Sender<()>>,
    /// Earliest enqueue time among entries in this batch (for e2e latency).
    oldest_enqueue_time: Instant,
    /// Journal file log_id for this batch — advances the durable lastMark
    /// after fsync. Matches Java's `ForceWriteRequest.logId`. Also serves
    /// as the grouping key in `force_write_loop`: requests with the same
    /// log_id target the same journal file and can share an fsync; a
    /// change in log_id signals a rotation, so the prior file must be
    /// fsynced before continuing — otherwise its writes would be acked
    /// without ever reaching disk.
    log_id: u64,
    /// Journal file position immediately after the bytes just written —
    /// becomes `lastLogMark.offset` after the fsync succeeds.
    /// Matches Java's `ForceWriteRequest.lastFlushedPosition`.
    last_flushed_position: u64,
}

// ---------------------------------------------------------------------------
// Mode 1: io_uring single-thread (write + async fsync)
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn journal_uring_loop(
    config: JournalConfig,
    rx: mpsc::Receiver<JournalEntry>,
    on_fatal_error: Option<OnFatalError>,
    shared_log_id: Arc<std::sync::atomic::AtomicU64>,
    shared_last_mark: Arc<std::sync::Mutex<(u64, u64)>>,
) {
    use io_uring::{opcode, types, IoUring};

    // Helper: complete an in-flight fsync from a CQE, recording metrics.
    // Returns true if the CQE was consumed (fsync completed), false if no CQE available.
    #[inline]
    fn try_complete_fsync(
        ring: &mut IoUring,
        syncing_acks: &mut Vec<tokio::sync::oneshot::Sender<()>>,
        syncing_oldest_enqueue: &mut Option<Instant>,
        syncing_fw_count: &mut u64,
        fsync_start: &mut Option<Instant>,
        fsync_in_flight: &mut bool,
        on_fatal_error: &Option<OnFatalError>,
        syncing_mark: &mut Option<(u64, u64)>,
        shared_last_mark: &Arc<std::sync::Mutex<(u64, u64)>>,
    ) -> bool {
        if let Some(cqe) = ring.completion().next() {
            // Record JOURNAL_SYNC latency
            if let Some(start) = fsync_start.take() {
                metrics::JOURNAL_SYNC.observe(start.elapsed().as_secs_f64());
            }
            if cqe.result() < 0 {
                tracing::error!("FATAL: journal fsync failed: {}",
                    io::Error::from_raw_os_error(-cqe.result()));
                if let Some(ref cb) = on_fatal_error {
                    cb(io::Error::from_raw_os_error(-cqe.result()));
                }
                syncing_acks.clear();
                *syncing_mark = None;
            } else {
                // fsync succeeded: advance the durable lastMark to the
                // (log_id, position) captured when this fsync was submitted.
                if let Some((log_id, offset)) = syncing_mark.take() {
                    let mut guard = shared_last_mark.lock()
                        .unwrap_or_else(|p| p.into_inner());
                    // Only advance forward: a rotation + out-of-order CQE
                    // must not rewind the mark.
                    if (log_id, offset) > *guard {
                        *guard = (log_id, offset);
                    }
                }
                // Record JOURNAL_ADD_ENTRY (e2e) for all acked entries
                if let Some(oldest) = syncing_oldest_enqueue.take() {
                    metrics::JOURNAL_ADD_ENTRY.observe(oldest.elapsed().as_secs_f64());
                }
                // Record JOURNAL_FORCE_WRITE_GROUPING_COUNT
                if *syncing_fw_count > 0 {
                    metrics::JOURNAL_FORCE_WRITE_GROUPING_COUNT.observe(*syncing_fw_count as f64);
                }
                for ack in syncing_acks.drain(..) {
                    let _ = ack.send(());
                }
            }
            *syncing_fw_count = 0;
            *fsync_in_flight = false;
            true
        } else {
            false
        }
    }

    // Helper: if no fsync is currently in flight but there are entries
    // already written to the page cache that no submitted fsync covers
    // (`awaiting_*`), promote them to the syncing slot and submit a fresh
    // fsync. The fresh FSYNC's mark is `state.position` at submit time,
    // so the kernel's vfs_fsync_range — which runs at or after submit —
    // is guaranteed to cover those bytes.
    //
    // An in-flight fsync only durably covers entries written before its
    // SQE was submitted, so any entries written after it must wait for
    // a *new* fsync that the loop submits as soon as the previous one
    // drains.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn chain_fresh_fsync_if_awaiting(
        ring: &mut IoUring,
        state: &JournalFileState,
        syncing_acks: &mut Vec<tokio::sync::oneshot::Sender<()>>,
        syncing_oldest_enqueue: &mut Option<Instant>,
        syncing_fw_count: &mut u64,
        awaiting_acks: &mut Vec<tokio::sync::oneshot::Sender<()>>,
        awaiting_oldest_enqueue: &mut Option<Instant>,
        awaiting_fw_count: &mut u64,
        fsync_in_flight: &mut bool,
        fsync_start: &mut Option<Instant>,
        syncing_mark: &mut Option<(u64, u64)>,
    ) {
        if *fsync_in_flight || awaiting_acks.is_empty() {
            return;
        }
        debug_assert!(syncing_acks.is_empty(),
            "syncing_acks should be empty after CQE drain");
        std::mem::swap(syncing_acks, awaiting_acks);
        *syncing_oldest_enqueue = awaiting_oldest_enqueue.take();
        *syncing_fw_count = std::mem::take(awaiting_fw_count);

        let Some(file) = state.file.as_ref() else {
            // Should be unreachable: awaiting acks imply a previous
            // successful write_all, which requires an open file.
            tracing::error!(
                "journal-uring: awaiting acks but no open file; dropping {} acks",
                syncing_acks.len()
            );
            syncing_acks.clear();
            *syncing_oldest_enqueue = None;
            *syncing_fw_count = 0;
            return;
        };

        let fd = file.as_raw_fd();
        let fsync_sqe = opcode::Fsync::new(types::Fd(fd))
            .build()
            .user_data(1);
        unsafe {
            if ring.submission().push(&fsync_sqe).is_err() {
                tracing::error!("journal-uring: SQ full");
            }
        }
        if let Err(e) = ring.submit() {
            tracing::error!("journal-uring: submit failed: {e}");
        }
        *fsync_in_flight = true;
        *fsync_start = Some(Instant::now());
        *syncing_mark = Some((state.log_id, state.position));
    }

    fs::create_dir_all(&config.journal_dir).ok();

    let mut ring = match IoUring::<io_uring::squeue::Entry>::new(64) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("journal-uring: failed to create io_uring: {e}");
            return;
        }
    };

    let mut state = JournalFileState::new(&config);
    let group_wait = Duration::from_millis(config.max_group_wait_ms.max(1));
    let buffered_entries_threshold = config.buffered_entries_threshold;
    let buffered_writes_threshold = config.buffered_writes_threshold;
    let mut write_buf: Vec<u8> = Vec::with_capacity(buffered_writes_threshold.max(64 * 1024));
    let mut pending_acks: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
    let mut pending_bytes: usize = 0;
    // Track the oldest enqueue time of pending (not-yet-syncing) entries.
    let mut pending_oldest_enqueue: Option<Instant> = None;

    // Acks waiting for the in-flight fsync to complete. These belong to
    // entries whose bytes were written *before* the in-flight fsync's SQE
    // was submitted, so its CQE durably covers them.
    let mut syncing_acks: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
    let mut syncing_oldest_enqueue: Option<Instant> = None;
    let mut syncing_fw_count: u64 = 0; // force-write requests grouped in current fsync
    let mut fsync_in_flight = false;
    let mut fsync_start: Option<Instant> = None;
    // Durable (log_id, offset) snapshot captured when the in-flight fsync
    // was submitted. Applied to shared_last_mark only on CQE success.
    let mut syncing_mark: Option<(u64, u64)> = None;

    // Acks for entries written to the page cache *after* the in-flight
    // fsync's SQE was submitted. They must NOT be drained by the in-flight
    // fsync — io_uring fsync only guarantees coverage of bytes already
    // dirty when vfs_fsync_range runs, and the kernel may execute the
    // fsync inline at submit time, before these later writes. Once the
    // in-flight fsync drains, these are promoted to syncing_* and a fresh
    // fsync is submitted that durably covers them.
    let mut awaiting_acks: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
    let mut awaiting_oldest_enqueue: Option<Instant> = None;
    let mut awaiting_fw_count: u64 = 0;

    loop {
        // If fsync is in flight, don't block -- poll for entries and CQE.
        let first = if fsync_in_flight {
            // Non-blocking: try to get entries while waiting for fsync
            match rx.try_recv() {
                Ok(entry) => Some(entry),
                Err(mpsc::TryRecvError::Empty) => {
                    // No entries available. Check if fsync completed.
                    try_complete_fsync(
                        &mut ring, &mut syncing_acks, &mut syncing_oldest_enqueue,
                        &mut syncing_fw_count, &mut fsync_start, &mut fsync_in_flight,
                        &on_fatal_error,
                        &mut syncing_mark, &shared_last_mark,
                    );
                    chain_fresh_fsync_if_awaiting(
                        &mut ring, &state,
                        &mut syncing_acks, &mut syncing_oldest_enqueue, &mut syncing_fw_count,
                        &mut awaiting_acks, &mut awaiting_oldest_enqueue, &mut awaiting_fw_count,
                        &mut fsync_in_flight, &mut fsync_start, &mut syncing_mark,
                    );
                    // If fsync still in flight and no entries, do a short wait
                    if fsync_in_flight {
                        match rx.recv_timeout(Duration::from_micros(100)) {
                            Ok(entry) => Some(entry),
                            Err(_) => {
                                // Check CQE again
                                try_complete_fsync(
                                    &mut ring, &mut syncing_acks, &mut syncing_oldest_enqueue,
                                    &mut syncing_fw_count, &mut fsync_start, &mut fsync_in_flight,
                                    &on_fatal_error,
                                    &mut syncing_mark, &shared_last_mark,
                                );
                                chain_fresh_fsync_if_awaiting(
                                    &mut ring, &state,
                                    &mut syncing_acks, &mut syncing_oldest_enqueue, &mut syncing_fw_count,
                                    &mut awaiting_acks, &mut awaiting_oldest_enqueue, &mut awaiting_fw_count,
                                    &mut fsync_in_flight, &mut fsync_start, &mut syncing_mark,
                                );
                                continue;
                            }
                        }
                    } else {
                        continue; // loop back, fsync just completed
                    }
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    // Channel closed -- wait for in-flight fsync, then for any
                    // entries that were written but not yet covered by a fsync.
                    if fsync_in_flight {
                        ring.submit_and_wait(1).ok();
                        try_complete_fsync(
                            &mut ring, &mut syncing_acks, &mut syncing_oldest_enqueue,
                            &mut syncing_fw_count, &mut fsync_start, &mut fsync_in_flight,
                            &on_fatal_error,
                            &mut syncing_mark, &shared_last_mark,
                        );
                    }
                    // Promote any awaiting acks and submit a fresh fsync so
                    // their callers see durability before the thread exits.
                    chain_fresh_fsync_if_awaiting(
                        &mut ring, &state,
                        &mut syncing_acks, &mut syncing_oldest_enqueue, &mut syncing_fw_count,
                        &mut awaiting_acks, &mut awaiting_oldest_enqueue, &mut awaiting_fw_count,
                        &mut fsync_in_flight, &mut fsync_start, &mut syncing_mark,
                    );
                    if fsync_in_flight {
                        ring.submit_and_wait(1).ok();
                        try_complete_fsync(
                            &mut ring, &mut syncing_acks, &mut syncing_oldest_enqueue,
                            &mut syncing_fw_count, &mut fsync_start, &mut fsync_in_flight,
                            &on_fatal_error,
                            &mut syncing_mark, &shared_last_mark,
                        );
                    }
                    break;
                }
            }
        } else {
            // No fsync in flight -- block for first entry
            match rx.recv() {
                Ok(entry) => Some(entry),
                Err(_) => break,
            }
        };

        // Process the first entry: record queue latency, track enqueue time
        if let Some(entry) = first {
            let dequeue_time = Instant::now();
            metrics::JOURNAL_QUEUE_LATENCY.observe(entry.enqueue_time.elapsed().as_secs_f64());
            metrics::JOURNAL_QUEUE_SIZE.dec();
            if pending_oldest_enqueue.is_none() {
                pending_oldest_enqueue = Some(entry.enqueue_time);
            }
            frame_into_buf(&mut write_buf, &entry.data);
            pending_bytes += 4 + entry.data.len();
            if let Some(ack) = entry.ack_tx {
                pending_acks.push(ack);
            }
            // Start process time tracking from when first entry was dequeued
            let _ = dequeue_time; // used below via process_start
        }

        // Drain more entries
        while let Ok(entry) = rx.try_recv() {
            metrics::JOURNAL_QUEUE_LATENCY.observe(entry.enqueue_time.elapsed().as_secs_f64());
            metrics::JOURNAL_QUEUE_SIZE.dec();
            if pending_oldest_enqueue.is_none() {
                pending_oldest_enqueue = Some(entry.enqueue_time);
            }
            frame_into_buf(&mut write_buf, &entry.data);
            pending_bytes += 4 + entry.data.len();
            if let Some(ack) = entry.ack_tx {
                pending_acks.push(ack);
            }
        }

        if write_buf.is_empty() {
            continue;
        }

        let process_start = Instant::now();

        // Determine flush trigger reason
        let entries_threshold_met = buffered_entries_threshold > 0
            && pending_acks.len() >= buffered_entries_threshold;
        let bytes_threshold_met = buffered_writes_threshold > 0
            && pending_bytes >= buffered_writes_threshold;

        // Group wait (always wait when sync enabled to batch more per fsync)
        let should_wait = !entries_threshold_met && !bytes_threshold_met;

        if should_wait && !fsync_in_flight {
            let deadline = Instant::now() + group_wait;
            let mut timed_out = false;
            loop {
                if (buffered_entries_threshold > 0
                    && pending_acks.len() >= buffered_entries_threshold)
                    || (buffered_writes_threshold > 0
                        && pending_bytes >= buffered_writes_threshold)
                {
                    break;
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    timed_out = true;
                    break;
                }
                match rx.recv_timeout(remaining) {
                    Ok(entry) => {
                        metrics::JOURNAL_QUEUE_LATENCY.observe(entry.enqueue_time.elapsed().as_secs_f64());
                        metrics::JOURNAL_QUEUE_SIZE.dec();
                        if pending_oldest_enqueue.is_none() {
                            pending_oldest_enqueue = Some(entry.enqueue_time);
                        }
                        frame_into_buf(&mut write_buf, &entry.data);
                        pending_bytes += 4 + entry.data.len();
                        if let Some(ack) = entry.ack_tx {
                            pending_acks.push(ack);
                        }
                    }
                    Err(_) => {
                        timed_out = true;
                        break;
                    }
                }
            }
            // Record flush trigger reason after group wait
            if timed_out {
                metrics::JOURNAL_NUM_FLUSH_MAX_WAIT.inc();
            } else if buffered_writes_threshold > 0
                && pending_bytes >= buffered_writes_threshold
            {
                metrics::JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES.inc();
            }
        } else if bytes_threshold_met {
            metrics::JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES.inc();
        } else if !should_wait {
            // entries threshold met -- still record as max outstanding bytes if applicable
            // (Java counts this as threshold-triggered flush)
        }

        // Write entries to file
        if let Err(e) = state.ensure_open(&config) {
            tracing::warn!("Journal open error: {e}");
            write_buf.clear();
            pending_acks.clear();
            pending_bytes = 0;
            pending_oldest_enqueue = None;
            continue;
        }
        shared_log_id.store(state.log_id, std::sync::atomic::Ordering::Relaxed);

        let batch_bytes = write_buf.len() as u64;
        let batch_entries = pending_acks.len();
        if state.position > 0 && state.position + batch_bytes > config.max_file_size {
            // Rotation must not abandon batches whose bytes are on the
            // file we are about to drop. (1) Drain any in-flight FSYNC —
            // io_uring resolves the fd at execution time, so a FSYNC
            // submitted for the old fd would return EBADF after
            // state.rotate() closes it. (2) If acks are still parked
            // waiting for an fsync covering this file, submit a fresh
            // FSYNC on the *current* file before rotating, so those
            // bytes are durable before the fd disappears. Without this,
            // shared_last_mark advances past the old log_id and replay
            // skips it on restart.
            if fsync_in_flight {
                let _ = ring.submit_and_wait(1);
                try_complete_fsync(
                    &mut ring, &mut syncing_acks, &mut syncing_oldest_enqueue,
                    &mut syncing_fw_count, &mut fsync_start, &mut fsync_in_flight,
                    &on_fatal_error,
                    &mut syncing_mark, &shared_last_mark,
                );
            }
            chain_fresh_fsync_if_awaiting(
                &mut ring, &state,
                &mut syncing_acks, &mut syncing_oldest_enqueue, &mut syncing_fw_count,
                &mut awaiting_acks, &mut awaiting_oldest_enqueue, &mut awaiting_fw_count,
                &mut fsync_in_flight, &mut fsync_start, &mut syncing_mark,
            );
            if fsync_in_flight {
                let _ = ring.submit_and_wait(1);
                try_complete_fsync(
                    &mut ring, &mut syncing_acks, &mut syncing_oldest_enqueue,
                    &mut syncing_fw_count, &mut fsync_start, &mut fsync_in_flight,
                    &on_fatal_error,
                    &mut syncing_mark, &shared_last_mark,
                );
            }
            if let Err(e) = state.rotate(&config) {
                tracing::warn!("Journal rotation error: {e}");
            }
        }

        let padding = if config.format_version >= 5 && config.alignment_size > 0 {
            let p = calculate_padding(state.position, config.alignment_size as u64);
            if !p.is_empty() { Some(p) } else { None }
        } else {
            None
        };

        let file = state.file.as_mut().unwrap();

        // Measure flush (write_all) latency
        let flush_start = Instant::now();
        let write_result = (|| -> io::Result<()> {
            if let Some(ref pad) = padding {
                file.write_all(pad)?;
            }
            file.write_all(&write_buf)?;
            Ok(())
        })();
        let flush_elapsed = flush_start.elapsed();

        match write_result {
            Ok(()) => {
                // Record flush metrics
                metrics::JOURNAL_FLUSH_LATENCY.observe(flush_elapsed.as_secs_f64());
                metrics::JOURNAL_FORCE_WRITE_BATCH_ENTRIES.observe(batch_entries as f64);
                metrics::JOURNAL_FORCE_WRITE_BATCH_BYTES.observe(batch_bytes as f64);
                metrics::JOURNAL_WRITE_BYTES.inc_by(batch_bytes);

                let write_start_pos = state.position;
                if let Some(ref pad) = padding {
                    state.position += pad.len() as u64;
                }
                state.position += batch_bytes;

                if config.remove_from_page_cache {
                    let written_len = state.position - write_start_pos;
                    advise_dontneed(file.as_raw_fd(), write_start_pos as i64, written_len as i64);
                }

                // If a previous fsync completed, drain its CQE first so we
                // can possibly submit a fresh fsync that durably covers the
                // bytes we just wrote.
                if fsync_in_flight {
                    try_complete_fsync(
                        &mut ring, &mut syncing_acks, &mut syncing_oldest_enqueue,
                        &mut syncing_fw_count, &mut fsync_start, &mut fsync_in_flight,
                        &on_fatal_error,
                        &mut syncing_mark, &shared_last_mark,
                    );
                }

                // Record JOURNAL_PROCESS_TIME_LATENCY
                metrics::JOURNAL_PROCESS_TIME_LATENCY.observe(process_start.elapsed().as_secs_f64());

                if fsync_in_flight {
                    // A previous fsync is still in flight. Its SQE was
                    // submitted before this batch's bytes were written, so
                    // it does NOT durably cover them — io_uring fsync only
                    // covers pages dirty when vfs_fsync_range runs, and the
                    // kernel may execute fsync inline at submit time. Park
                    // these acks in `awaiting_*`; a fresh fsync covering
                    // them will be submitted as soon as the in-flight one
                    // drains (see chain_fresh_fsync_if_awaiting and the
                    // !fsync_in_flight branch below).
                    awaiting_acks.append(&mut pending_acks);
                    if awaiting_oldest_enqueue.is_none() {
                        awaiting_oldest_enqueue = pending_oldest_enqueue.take();
                    } else if let Some(pe) = pending_oldest_enqueue.take() {
                        if let Some(ref ae) = awaiting_oldest_enqueue {
                            if pe < *ae {
                                awaiting_oldest_enqueue = Some(pe);
                            }
                        }
                    }
                    awaiting_fw_count += 1;
                } else {
                    // No fsync in flight. The fresh fsync we are about to
                    // submit will cover everything dirty for this fd: this
                    // batch's bytes plus any earlier batch whose acks were
                    // parked in `awaiting_*` while the previous fsync ran.
                    // Promote both into the syncing slot.
                    syncing_acks.append(&mut awaiting_acks);
                    syncing_acks.append(&mut pending_acks);
                    if syncing_oldest_enqueue.is_none() {
                        syncing_oldest_enqueue = awaiting_oldest_enqueue.take();
                    } else if let Some(ae) = awaiting_oldest_enqueue.take() {
                        if let Some(ref se) = syncing_oldest_enqueue {
                            if ae < *se {
                                syncing_oldest_enqueue = Some(ae);
                            }
                        }
                    }
                    if syncing_oldest_enqueue.is_none() {
                        syncing_oldest_enqueue = pending_oldest_enqueue.take();
                    } else if let Some(pe) = pending_oldest_enqueue.take() {
                        if let Some(ref se) = syncing_oldest_enqueue {
                            if pe < *se {
                                syncing_oldest_enqueue = Some(pe);
                            }
                        }
                    }
                    syncing_fw_count += std::mem::take(&mut awaiting_fw_count) + 1;

                    let fd = file.as_raw_fd();
                    let fsync_sqe = opcode::Fsync::new(types::Fd(fd))
                        .build()
                        .user_data(1);
                    unsafe {
                        if ring.submission().push(&fsync_sqe).is_err() {
                            tracing::error!("journal-uring: SQ full");
                        }
                    }
                    if let Err(e) = ring.submit() {
                        tracing::error!("journal-uring: submit failed: {e}");
                    }
                    fsync_in_flight = true;
                    fsync_start = Some(Instant::now());
                    // Capture the durable position at fsync submission time;
                    // applied to shared_last_mark once the CQE reports success.
                    syncing_mark = Some((state.log_id, state.position));
                }
            }
            Err(e) => {
                tracing::error!("FATAL: journal write failed: {e}");
                if let Some(ref cb) = on_fatal_error {
                    cb(io::Error::new(e.kind(), format!("journal write: {e}")));
                }
                pending_acks.clear();
                pending_oldest_enqueue = None;
            }
        }
        write_buf.clear();
        pending_bytes = 0;
    }
}

// ---------------------------------------------------------------------------
// Mode 2/3: StdEngine writer thread
// ---------------------------------------------------------------------------

fn journal_writer_loop(
    config: JournalConfig,
    rx: mpsc::Receiver<JournalEntry>,
    fw_tx: Option<mpsc::Sender<ForceWriteRequest>>,
    on_fatal_error: Option<Arc<dyn Fn(io::Error) + Send + Sync>>,
    shared_log_id: Arc<std::sync::atomic::AtomicU64>,
    shared_last_mark: Arc<std::sync::Mutex<(u64, u64)>>,
) {
    fs::create_dir_all(&config.journal_dir).ok();

    let mut state = JournalFileState::new(&config);
    let group_wait = Duration::from_millis(config.max_group_wait_ms.max(1));
    let buffered_entries_threshold = config.buffered_entries_threshold;
    let buffered_writes_threshold = config.buffered_writes_threshold;
    let mut pending_acks: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
    let mut pending_bytes: usize = 0;
    let mut write_buf: Vec<u8> = Vec::with_capacity(buffered_writes_threshold.max(64 * 1024));
    // Track the oldest enqueue time of pending entries for e2e latency.
    let mut pending_oldest_enqueue: Option<Instant> = None;

    loop {
        let first = match rx.recv() {
            Ok(entry) => entry,
            Err(_) => {
                if !write_buf.is_empty() {
                    flush_write_buf(
                        &config, &mut state, &mut write_buf, &mut pending_acks,
                        &mut pending_oldest_enqueue,
                        &fw_tx, &on_fatal_error, &shared_log_id,
                        &shared_last_mark,
                        FlushTrigger::EmptyQueue,
                    );
                }
                break;
            }
        };

        // Record queue latency for first entry
        metrics::JOURNAL_QUEUE_LATENCY.observe(first.enqueue_time.elapsed().as_secs_f64());
        metrics::JOURNAL_QUEUE_SIZE.dec();
        if pending_oldest_enqueue.is_none() {
            pending_oldest_enqueue = Some(first.enqueue_time);
        }

        frame_into_buf(&mut write_buf, &first.data);
        pending_bytes += 4 + first.data.len();
        if let Some(ack) = first.ack_tx {
            pending_acks.push(ack);
        }

        while let Ok(entry) = rx.try_recv() {
            metrics::JOURNAL_QUEUE_LATENCY.observe(entry.enqueue_time.elapsed().as_secs_f64());
            metrics::JOURNAL_QUEUE_SIZE.dec();
            if pending_oldest_enqueue.is_none() {
                pending_oldest_enqueue = Some(entry.enqueue_time);
            }
            frame_into_buf(&mut write_buf, &entry.data);
            pending_bytes += 4 + entry.data.len();
            if let Some(ack) = entry.ack_tx {
                pending_acks.push(ack);
            }
        }

        // Determine flush trigger before group wait
        let entries_threshold_met = buffered_entries_threshold > 0
            && pending_acks.len() >= buffered_entries_threshold;
        let bytes_threshold_met = buffered_writes_threshold > 0
            && pending_bytes >= buffered_writes_threshold;

        let queue_empty_after_drain = pending_acks.len() <= 1;
        let should_wait = if fw_tx.is_some() {
            !entries_threshold_met && !bytes_threshold_met
                && !(config.flush_when_queue_empty && queue_empty_after_drain)
        } else {
            pending_acks.len() > 1 && !entries_threshold_met && !bytes_threshold_met
        };

        let flush_trigger;
        if should_wait {
            let deadline = Instant::now() + group_wait;
            let mut timed_out = false;
            loop {
                let ent_met = buffered_entries_threshold > 0
                    && pending_acks.len() >= buffered_entries_threshold;
                let bytes_met = buffered_writes_threshold > 0
                    && pending_bytes >= buffered_writes_threshold;
                if ent_met || bytes_met {
                    break;
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    timed_out = true;
                    break;
                }
                match rx.recv_timeout(remaining) {
                    Ok(entry) => {
                        metrics::JOURNAL_QUEUE_LATENCY.observe(entry.enqueue_time.elapsed().as_secs_f64());
                        metrics::JOURNAL_QUEUE_SIZE.dec();
                        if pending_oldest_enqueue.is_none() {
                            pending_oldest_enqueue = Some(entry.enqueue_time);
                        }
                        frame_into_buf(&mut write_buf, &entry.data);
                        pending_bytes += 4 + entry.data.len();
                        if let Some(ack) = entry.ack_tx {
                            pending_acks.push(ack);
                        }
                    }
                    Err(_) => {
                        timed_out = true;
                        break;
                    }
                }
            }
            if timed_out {
                flush_trigger = FlushTrigger::MaxWait;
            } else if buffered_writes_threshold > 0
                && pending_bytes >= buffered_writes_threshold
            {
                flush_trigger = FlushTrigger::MaxOutstandingBytes;
            } else {
                flush_trigger = FlushTrigger::MaxWait;
            }
        } else if fw_tx.is_none() && pending_acks.len() <= 1 {
            // No-sync mode with single entry: empty queue trigger
            flush_trigger = FlushTrigger::EmptyQueue;
        } else if bytes_threshold_met {
            flush_trigger = FlushTrigger::MaxOutstandingBytes;
        } else {
            flush_trigger = FlushTrigger::MaxWait;
        }

        flush_write_buf(
            &config, &mut state, &mut write_buf, &mut pending_acks,
            &mut pending_oldest_enqueue,
            &fw_tx, &on_fatal_error, &shared_log_id,
            &shared_last_mark,
            flush_trigger,
        );
        pending_bytes = 0;
    }
}

/// Frame a journal entry directly into a buffer: `[size(4 BE)][payload]`.
fn frame_into_buf(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
    buf.extend_from_slice(data);
}

/// Flush trigger reason, for recording the appropriate counter metric.
#[derive(Debug, Clone, Copy)]
enum FlushTrigger {
    MaxWait,
    MaxOutstandingBytes,
    EmptyQueue,
}

/// Write buffered entries, then ACK or forward to force-write thread.
fn flush_write_buf(
    config: &JournalConfig,
    state: &mut JournalFileState,
    write_buf: &mut Vec<u8>,
    pending_acks: &mut Vec<tokio::sync::oneshot::Sender<()>>,
    pending_oldest_enqueue: &mut Option<Instant>,
    fw_tx: &Option<mpsc::Sender<ForceWriteRequest>>,
    on_fatal_error: &Option<Arc<dyn Fn(io::Error) + Send + Sync>>,
    shared_log_id: &Arc<std::sync::atomic::AtomicU64>,
    shared_last_mark: &Arc<std::sync::Mutex<(u64, u64)>>,
    flush_trigger: FlushTrigger,
) {
    if write_buf.is_empty() {
        return;
    }

    let process_start = Instant::now();

    // Record flush trigger counter
    match flush_trigger {
        FlushTrigger::MaxWait => metrics::JOURNAL_NUM_FLUSH_MAX_WAIT.inc(),
        FlushTrigger::MaxOutstandingBytes => metrics::JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES.inc(),
        FlushTrigger::EmptyQueue => metrics::JOURNAL_NUM_FLUSH_EMPTY_QUEUE.inc(),
    }

    if let Err(e) = state.ensure_open(config) {
        tracing::warn!("Journal open error: {e}");
        write_buf.clear();
        pending_acks.clear();
        *pending_oldest_enqueue = None;
        return;
    }
    shared_log_id.store(state.log_id, std::sync::atomic::Ordering::Relaxed);

    let batch_bytes = write_buf.len() as u64;
    let batch_entries = pending_acks.len();
    if state.position > 0 && state.position + batch_bytes > config.max_file_size {
        if let Err(e) = state.rotate(config) {
            tracing::warn!("Journal rotation error: {e}");
        }
    }

    let padding = if config.format_version >= 5 && config.alignment_size > 0 {
        let p = calculate_padding(state.position, config.alignment_size as u64);
        if !p.is_empty() { Some(p) } else { None }
    } else {
        None
    };

    let file = state.file.as_mut().unwrap();

    // Measure flush (write_all) latency
    let flush_start = Instant::now();
    let write_result = (|| -> io::Result<()> {
        if let Some(ref pad) = padding {
            file.write_all(pad)?;
        }
        file.write_all(write_buf)?;
        Ok(())
    })();
    let flush_elapsed = flush_start.elapsed();

    match write_result {
        Ok(()) => {
            // Record flush metrics
            metrics::JOURNAL_FLUSH_LATENCY.observe(flush_elapsed.as_secs_f64());
            metrics::JOURNAL_FORCE_WRITE_BATCH_ENTRIES.observe(batch_entries as f64);
            metrics::JOURNAL_FORCE_WRITE_BATCH_BYTES.observe(batch_bytes as f64);
            metrics::JOURNAL_WRITE_BYTES.inc_by(batch_bytes);

            let write_start_pos = state.position;
            if let Some(ref pad) = padding {
                state.position += pad.len() as u64;
            }
            state.position += batch_bytes;

            let fd = file.as_raw_fd();

            // Evict written pages from OS page cache to avoid pollution.
            // Matches Java's journalRemoveFromPageCache.
            if config.remove_from_page_cache {
                let written_len = state.position - write_start_pos;
                advise_dontneed(fd, write_start_pos as i64, written_len as i64);
            }

            let acks = std::mem::take(pending_acks);
            let oldest_enqueue = pending_oldest_enqueue.take().unwrap_or_else(Instant::now);

            // Record JOURNAL_PROCESS_TIME_LATENCY (dequeue to finish processing)
            metrics::JOURNAL_PROCESS_TIME_LATENCY.observe(process_start.elapsed().as_secs_f64());

            if let Some(ref tx) = fw_tx {
                // Mode 2: forward to force-write thread for sync. Dup the
                // fd so the request owns a handle that survives the
                // writer thread's next `state.rotate()` (which drops
                // `state.file` and closes the writer's fd). Without this,
                // the force-write thread can race rotation and fsync a
                // closed-or-reused fd.
                let dup_file = match file.try_clone() {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::error!("FATAL: journal file dup failed: {e}");
                        if let Some(ref callback) = on_fatal_error {
                            callback(io::Error::new(
                                e.kind(),
                                format!("journal dup: {e}"),
                            ));
                        }
                        // Drop acks without sending — callers will see
                        // RecvError ("journal ack lost") and surface
                        // an I/O error rather than a false-durability ack.
                        write_buf.clear();
                        return;
                    }
                };
                metrics::JOURNAL_FORCE_WRITE_QUEUE_SIZE.inc();
                if tx.send(ForceWriteRequest {
                    file: dup_file,
                    acks,
                    oldest_enqueue_time: oldest_enqueue,
                    log_id: state.log_id,
                    last_flushed_position: state.position,
                }).is_err() {
                    tracing::error!("Force-write thread gone");
                    metrics::JOURNAL_FORCE_WRITE_QUEUE_SIZE.dec();
                }
            } else {
                // Mode 3: no sync -- ACK immediately, record e2e latency.
                // Advance lastMark to match the newly-written position.
                // This is the strongest durability point available in no-sync mode.
                {
                    let mut guard = shared_last_mark.lock()
                        .unwrap_or_else(|p| p.into_inner());
                    let new_mark = (state.log_id, state.position);
                    if new_mark > *guard {
                        *guard = new_mark;
                    }
                }
                metrics::JOURNAL_ADD_ENTRY.observe(oldest_enqueue.elapsed().as_secs_f64());
                for ack in acks {
                    let _ = ack.send(());
                }
            }
        }
        Err(e) => {
            tracing::error!("FATAL: journal write failed: {e}");
            if let Some(ref callback) = on_fatal_error {
                callback(io::Error::new(e.kind(), format!("journal write: {e}")));
            }
            pending_acks.clear();
            *pending_oldest_enqueue = None;
        }
    }
    write_buf.clear();
}

// ---------------------------------------------------------------------------
// Force-write thread (StdEngine only)
// ---------------------------------------------------------------------------

/// A run of force-write requests that all target the same journal file
/// (same `log_id`) and can therefore share a single fsync. The owned
/// `file` keeps the underlying fd alive for the duration of the fsync,
/// even if the writer thread has since rotated to a new journal file.
struct FsyncGroup {
    file: fs::File,
    log_id: u64,
    last_position: u64,
    acks: Vec<tokio::sync::oneshot::Sender<()>>,
    oldest_enqueue: Instant,
    request_count: u64,
}

impl FsyncGroup {
    fn from_request(req: ForceWriteRequest) -> Self {
        Self {
            file: req.file,
            log_id: req.log_id,
            last_position: req.last_flushed_position,
            acks: req.acks,
            oldest_enqueue: req.oldest_enqueue_time,
            request_count: 1,
        }
    }

    fn merge(&mut self, req: ForceWriteRequest) {
        // The dup'd file from the merged request is redundant — both
        // dups point at the same open file description, so dropping it
        // (closing one of the two fds) is safe. Keep `self.file`.
        drop(req.file);
        self.acks.extend(req.acks);
        if req.oldest_enqueue_time < self.oldest_enqueue {
            self.oldest_enqueue = req.oldest_enqueue_time;
        }
        // Latest grouped request carries the furthest write position.
        self.last_position = req.last_flushed_position;
        self.request_count += 1;
    }
}

fn force_write_loop(
    rx: mpsc::Receiver<ForceWriteRequest>,
    on_fatal_error: Option<Arc<dyn Fn(io::Error) + Send + Sync>>,
    shared_last_mark: Arc<std::sync::Mutex<(u64, u64)>>,
    fsync_observer: Option<Arc<dyn Fn(u64) + Send + Sync>>,
) {
    loop {
        let first = match rx.recv() {
            Ok(req) => req,
            Err(_) => break,
        };
        metrics::JOURNAL_FORCE_WRITE_QUEUE_SIZE.dec();

        // Build groups in submission order. Requests targeting the same
        // log_id share an fsync; a log_id change marks a journal rotation
        // and forces a new group so the prior file's writes are durably
        // synced before its acks fire.
        let mut groups: Vec<FsyncGroup> = vec![FsyncGroup::from_request(first)];
        while let Ok(req) = rx.try_recv() {
            metrics::JOURNAL_FORCE_WRITE_QUEUE_SIZE.dec();
            // Safe to unwrap: groups starts non-empty and never shrinks.
            let last = groups.last_mut().unwrap();
            if last.log_id == req.log_id {
                last.merge(req);
            } else {
                groups.push(FsyncGroup::from_request(req));
            }
        }

        // Process each group in submission order. Stop on first failure
        // so we don't ack subsequent groups whose data is no longer
        // protected by a successful fsync chain.
        let mut fatal: Option<io::Error> = None;
        for group in groups {
            if fatal.is_some() {
                // Earlier group failed — drop this group's acks too.
                drop(group);
                continue;
            }
            let sync_start = Instant::now();
            if let Err(e) = group.file.sync_data() {
                tracing::error!(
                    "FATAL: journal fsync failed (log_id={:x}): {e}",
                    group.log_id
                );
                fatal = Some(io::Error::new(e.kind(), format!("journal fsync: {e}")));
                continue;
            }
            let sync_elapsed = sync_start.elapsed();

            // fsync succeeded: advance the durable lastMark for this
            // file. Submission order is monotonic, so a later group's
            // (log_id, position) > earlier's; the `>` guard preserves
            // that invariant against rare reorderings.
            {
                let mut guard = shared_last_mark.lock()
                    .unwrap_or_else(|p| p.into_inner());
                let new_mark = (group.log_id, group.last_position);
                if new_mark > *guard {
                    *guard = new_mark;
                }
            }

            metrics::JOURNAL_SYNC.observe(sync_elapsed.as_secs_f64());
            metrics::JOURNAL_ADD_ENTRY.observe(group.oldest_enqueue.elapsed().as_secs_f64());
            metrics::JOURNAL_FORCE_WRITE_GROUPING_COUNT
                .observe(group.request_count as f64);

            if let Some(ref obs) = fsync_observer {
                obs(group.log_id);
            }

            for ack in group.acks {
                let _ = ack.send(());
            }
        }

        if let Some(e) = fatal {
            if let Some(ref cb) = on_fatal_error {
                cb(e);
            }
            // Continue draining the queue rather than exiting — the
            // writer thread may keep pushing requests until the bookie
            // transitions to read-only and shuts the channel down. Acks
            // for those will be dropped by the next iteration's
            // failure-propagation, surfacing as "journal ack lost".
        }
    }
}

/// Advise the OS to evict the given range from the page cache.
/// No-op on non-Linux platforms.
fn advise_dontneed(fd: RawFd, offset: i64, len: i64) {
    #[cfg(target_os = "linux")]
    {
        // POSIX_FADV_DONTNEED = 4
        unsafe { libc::posix_fadvise(fd, offset, len, libc::POSIX_FADV_DONTNEED); }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (fd, offset, len);
    }
}

// ---------------------------------------------------------------------------
// Journal file state
// ---------------------------------------------------------------------------

struct JournalFileState {
    log_id: u64,
    file: Option<fs::File>,
    position: u64,
}

impl JournalFileState {
    fn new(config: &JournalConfig) -> Self {
        let log_id = Self::next_timestamp_log_id(&config.journal_dir);
        Self { log_id, file: None, position: 0 }
    }

    /// Generate the next journal log ID using wall-clock time.
    /// Matches Java's `Journal.getTimestamp()` which uses `System.currentTimeMillis()`.
    /// If an existing file already uses the same timestamp, increment until unique.
    fn next_timestamp_log_id(dir: &Path) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut max_existing = 0u64;
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.ends_with(".txn") {
                    if let Ok(id) = u64::from_str_radix(name.trim_end_matches(".txn"), 16) {
                        if id > max_existing { max_existing = id; }
                    }
                }
            }
        }
        // Ensure monotonically increasing: max(now, max_existing + 1)
        std::cmp::max(now, max_existing + 1)
    }

    fn ensure_open(&mut self, config: &JournalConfig) -> Result<(), StorageError> {
        if self.file.is_some() {
            return Ok(());
        }
        let creation_start = Instant::now();
        let path = config.journal_dir.join(format!("{:x}.txn", self.log_id));
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)?;

        let header = JournalHeader::new(config.format_version);
        let header_bytes = header.to_bytes();
        file.write_all(&header_bytes)?;
        file.flush()?;

        self.position = header_bytes.len() as u64;
        self.file = Some(file);
        metrics::JOURNAL_CREATION_LATENCY.observe(creation_start.elapsed().as_secs_f64());
        Ok(())
    }

    fn rotate(&mut self, config: &JournalConfig) -> Result<(), StorageError> {
        self.file = None;
        // Use wall-clock timestamp for the new journal file, matching Java's
        // Journal.getTimestamp(). Ensure it's strictly greater than the current ID.
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let new_id = std::cmp::max(now, self.log_id + 1);

        // Journal file reuse: if enabled, try to rename an old journal file
        // instead of creating a new one. Avoids filesystem metadata overhead.
        // Matches Java's Journal.journalReuseFiles behavior.
        if config.reuse_files {
            if let Some(old_path) = find_reusable_journal(&config.journal_dir, new_id) {
                let new_path = config.journal_dir.join(format!("{:x}.txn", new_id));
                if fs::rename(&old_path, &new_path).is_ok() {
                    // Truncate the file — ensure_open will write the header
                    let _ = fs::OpenOptions::new().write(true).truncate(true).open(&new_path);
                }
            }
        }

        self.log_id = new_id;
        self.position = 0;
        self.ensure_open(config)
    }
}

/// Find the oldest journal file that can be reused.
/// Matches Java's Journal.journalReuseFiles: picks the oldest .txn file
/// that is not the current one.
fn find_reusable_journal(dir: &Path, current_id: u64) -> Option<PathBuf> {
    let mut oldest: Option<(u64, PathBuf)> = None;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(stem) = name.strip_suffix(".txn") {
                if let Ok(id) = u64::from_str_radix(stem, 16) {
                    if id == current_id {
                        continue;
                    }
                    match &oldest {
                        Some((old_id, _)) if id < *old_id => {
                            oldest = Some((id, entry.path()));
                        }
                        None => {
                            oldest = Some((id, entry.path()));
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    oldest.map(|(_, path)| path)
}

// ---------------------------------------------------------------------------
// Journal replay
// ---------------------------------------------------------------------------

pub fn replay_journal<F>(
    journal_dir: &Path,
    last_mark: Option<format::LastMark>,
    mut callback: F,
) -> Result<usize, StorageError>
where
    F: FnMut(&[u8]),
{
    replay_journal_opt(journal_dir, last_mark, false, &mut callback)
}

/// Replay journal entries from disk, optionally skipping corrupt records.
/// When `skip_invalid_record` is true, scanning continues past corrupt
/// entries instead of stopping — matching Java's `Journal.scanJournal()`
/// with `skipInvalidRecord=true`.
pub fn replay_journal_opt<F>(
    journal_dir: &Path,
    last_mark: Option<format::LastMark>,
    skip_invalid_record: bool,
    callback: &mut F,
) -> Result<usize, StorageError>
where
    F: FnMut(&[u8]),
{
    let mark_log_id = last_mark.as_ref().map(|m| m.log_id()).unwrap_or(0);
    let mark_offset = last_mark.as_ref().map(|m| m.offset()).unwrap_or(0);

    let mut txn_files: Vec<(u64, PathBuf)> = Vec::new();
    if let Ok(entries) = fs::read_dir(journal_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".txn") {
                if let Ok(id) = u64::from_str_radix(name.trim_end_matches(".txn"), 16) {
                    if mark_log_id > 0 && (id as i64) < mark_log_id {
                        continue;
                    }
                    txn_files.push((id, entry.path()));
                }
            }
        }
    }
    txn_files.sort_by_key(|(id, _)| *id);

    let mut total_replayed = 0;
    for (file_id, path) in &txn_files {
        let data = fs::read(path)?;
        if data.len() < 8 {
            continue;
        }
        let header = format::JournalHeader::from_bytes(&data)?;
        let header_size = header.header_size();
        if data.len() <= header_size {
            continue;
        }

        let start_offset = if mark_log_id > 0 && *file_id == mark_log_id as u64 && mark_offset > 0 {
            (mark_offset as usize).saturating_sub(header_size)
        } else {
            0
        };

        // Skip past entries already covered by the lastMark checkpoint.
        // start_offset is a byte offset into the post-header scan region.
        let scan_data = &data[header_size + start_offset..];
        let entries = format::scan_journal_entries_opt(scan_data, header.version(), skip_invalid_record);
        for entry_data in &entries {
            callback(entry_data);
            total_replayed += 1;
        }
    }

    Ok(total_replayed)
}
