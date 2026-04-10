use std::collections::HashSet;
use std::fs;
use std::io::{self, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use super::format::{self, JournalHeader, calculate_padding};
use super::metrics;
use crate::StorageError;

/// Meta-entry ID for master key journaling.
pub const METAENTRY_ID_LEDGER_KEY: i64 = -0x1000;

/// Maximum journal queue memory in bytes (default 32MB, matching Java).
const MAX_QUEUE_MEMORY: usize = 32 * 1024 * 1024;

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
pub type OnFatalError = Box<dyn Fn(io::Error) + Send + 'static>;

pub struct Journal {
    tx: mpsc::SyncSender<JournalEntry>,
    _threads: Vec<std::thread::JoinHandle<()>>,
    master_key_cache: std::sync::Mutex<HashSet<i64>>,
    current_log_id: Arc<std::sync::atomic::AtomicU64>,
}

impl Journal {
    pub fn start(
        config: JournalConfig,
        _journal_io: std::sync::Arc<dyn crate::io_engine::JournalIo>,
        on_fatal_error: Option<OnFatalError>,
    ) -> Self {
        let bound = MAX_QUEUE_MEMORY / 1024;
        let (tx, rx) = mpsc::sync_channel(bound);

        let current_log_id = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let shared_log_id = current_log_id.clone();

        let mut threads = Vec::new();

        #[cfg(target_os = "linux")]
        let use_uring = config.sync_data && crate::io_engine::uring_available();
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
                        journal_uring_loop(uring_config, rx, on_fatal_error, shared_log_id)
                    })
                    .expect("failed to spawn journal-uring thread");
                threads.push(t);
            }
        } else if config.sync_data {
            // Mode 2: two threads (writer + blocking force-write)
            let (fw_tx, fw_rx) = mpsc::channel::<ForceWriteRequest>();

            let sync_thread = std::thread::Builder::new()
                .name("journal-sync".into())
                .spawn(move || force_write_loop(fw_rx))
                .expect("failed to spawn journal-sync thread");
            threads.push(sync_thread);

            let writer_config = config.clone();
            let writer_thread = std::thread::Builder::new()
                .name("journal-writer".into())
                .spawn(move || {
                    journal_writer_loop(writer_config, rx, Some(fw_tx), on_fatal_error, shared_log_id)
                })
                .expect("failed to spawn journal-writer thread");
            threads.push(writer_thread);
        } else {
            // Mode 3: single writer thread, no sync
            let writer_config = config.clone();
            let writer_thread = std::thread::Builder::new()
                .name("journal-writer".into())
                .spawn(move || {
                    journal_writer_loop(writer_config, rx, None, on_fatal_error, shared_log_id)
                })
                .expect("failed to spawn journal-writer thread");
            threads.push(writer_thread);
        }

        Self {
            tx,
            _threads: threads,
            master_key_cache: std::sync::Mutex::new(HashSet::new()),
            current_log_id,
        }
    }

    pub async fn log_add_entry(&self, data: &[u8]) -> Result<(), StorageError> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let enqueue_time = Instant::now();
        metrics::JOURNAL_QUEUE_SIZE.inc();
        self.tx
            .send(JournalEntry {
                data: data.to_vec(),
                ack_tx: Some(ack_tx),
                enqueue_time,
            })
            .map_err(|_| {
                metrics::JOURNAL_QUEUE_SIZE.dec();
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe, "journal writer closed",
                ))
            })?;
        ack_rx.await.map_err(|_| StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe, "journal ack lost",
        )))
    }

    pub fn maybe_journal_master_key(
        &self,
        ledger_id: i64,
        master_key: &[u8],
    ) -> Result<(), StorageError> {
        let mut cache = self.master_key_cache.lock().unwrap();
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

    pub fn current_log_id(&self) -> Option<u64> {
        let id = self.current_log_id.load(std::sync::atomic::Ordering::Relaxed);
        if id == 0 { None } else { Some(id) }
    }
}

// ---------------------------------------------------------------------------
// Force-write request — sent from writer thread to sync thread
// ---------------------------------------------------------------------------

struct ForceWriteRequest {
    fd: RawFd,
    acks: Vec<tokio::sync::oneshot::Sender<()>>,
    /// Earliest enqueue time among entries in this batch (for e2e latency).
    oldest_enqueue_time: Instant,
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
            } else {
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

    // Acks waiting for the in-flight fsync to complete.
    let mut syncing_acks: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
    let mut syncing_oldest_enqueue: Option<Instant> = None;
    let mut syncing_fw_count: u64 = 0; // force-write requests grouped in current fsync
    let mut fsync_in_flight = false;
    let mut fsync_start: Option<Instant> = None;

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
                                );
                                continue;
                            }
                        }
                    } else {
                        continue; // loop back, fsync just completed
                    }
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    // Channel closed -- wait for in-flight fsync then exit
                    if fsync_in_flight {
                        ring.submit_and_wait(1).ok();
                        try_complete_fsync(
                            &mut ring, &mut syncing_acks, &mut syncing_oldest_enqueue,
                            &mut syncing_fw_count, &mut fsync_start, &mut fsync_in_flight,
                            &on_fatal_error,
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

                if let Some(ref pad) = padding {
                    state.position += pad.len() as u64;
                }
                state.position += batch_bytes;

                // If a previous fsync completed, check CQE
                if fsync_in_flight {
                    try_complete_fsync(
                        &mut ring, &mut syncing_acks, &mut syncing_oldest_enqueue,
                        &mut syncing_fw_count, &mut fsync_start, &mut fsync_in_flight,
                        &on_fatal_error,
                    );
                }

                // Move pending acks to syncing (they'll be ACK'd when fsync completes)
                syncing_acks.append(&mut pending_acks);
                if syncing_oldest_enqueue.is_none() {
                    syncing_oldest_enqueue = pending_oldest_enqueue.take();
                } else {
                    // Keep the older of the two
                    if let Some(pe) = pending_oldest_enqueue.take() {
                        if let Some(ref se) = syncing_oldest_enqueue {
                            if pe < *se {
                                syncing_oldest_enqueue = Some(pe);
                            }
                        }
                    }
                }
                syncing_fw_count += 1;

                // Record JOURNAL_PROCESS_TIME_LATENCY
                metrics::JOURNAL_PROCESS_TIME_LATENCY.observe(process_start.elapsed().as_secs_f64());

                // Submit FSYNC if no fsync is in flight
                if !fsync_in_flight {
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
    on_fatal_error: Option<OnFatalError>,
    shared_log_id: Arc<std::sync::atomic::AtomicU64>,
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

        let should_wait = if fw_tx.is_some() {
            !entries_threshold_met && !bytes_threshold_met
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
    on_fatal_error: &Option<OnFatalError>,
    shared_log_id: &Arc<std::sync::atomic::AtomicU64>,
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

            if let Some(ref pad) = padding {
                state.position += pad.len() as u64;
            }
            state.position += batch_bytes;

            let fd = file.as_raw_fd();
            let acks = std::mem::take(pending_acks);
            let oldest_enqueue = pending_oldest_enqueue.take().unwrap_or_else(Instant::now);

            // Record JOURNAL_PROCESS_TIME_LATENCY (dequeue to finish processing)
            metrics::JOURNAL_PROCESS_TIME_LATENCY.observe(process_start.elapsed().as_secs_f64());

            if let Some(ref tx) = fw_tx {
                // Mode 2: forward to force-write thread for sync
                metrics::JOURNAL_FORCE_WRITE_QUEUE_SIZE.inc();
                if tx.send(ForceWriteRequest {
                    fd,
                    acks,
                    oldest_enqueue_time: oldest_enqueue,
                }).is_err() {
                    tracing::error!("Force-write thread gone");
                    metrics::JOURNAL_FORCE_WRITE_QUEUE_SIZE.dec();
                }
            } else {
                // Mode 3: no sync -- ACK immediately, record e2e latency
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

fn force_write_loop(rx: mpsc::Receiver<ForceWriteRequest>) {
    loop {
        let first = match rx.recv() {
            Ok(req) => req,
            Err(_) => break,
        };
        metrics::JOURNAL_FORCE_WRITE_QUEUE_SIZE.dec();

        let mut all_acks = first.acks;
        let mut last_fd = first.fd;
        let mut oldest_enqueue = first.oldest_enqueue_time;
        let mut grouped_count: u64 = 1; // count of ForceWriteRequests grouped

        while let Ok(req) = rx.try_recv() {
            metrics::JOURNAL_FORCE_WRITE_QUEUE_SIZE.dec();
            all_acks.extend(req.acks);
            last_fd = req.fd;
            // Keep the oldest enqueue time across all grouped requests
            if req.oldest_enqueue_time < oldest_enqueue {
                oldest_enqueue = req.oldest_enqueue_time;
            }
            grouped_count += 1;
        }

        let sync_start = Instant::now();
        if let Err(e) = sync_fd(last_fd) {
            tracing::error!("FATAL: journal fsync failed: {e}");
            continue;
        }
        let sync_elapsed = sync_start.elapsed();

        // Record metrics
        metrics::JOURNAL_SYNC.observe(sync_elapsed.as_secs_f64());
        metrics::JOURNAL_ADD_ENTRY.observe(oldest_enqueue.elapsed().as_secs_f64());
        metrics::JOURNAL_FORCE_WRITE_GROUPING_COUNT.observe(grouped_count as f64);

        for ack in all_acks {
            let _ = ack.send(());
        }
    }
}

fn sync_fd(fd: RawFd) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::io::FromRawFd;
        let file = std::mem::ManuallyDrop::new(unsafe { fs::File::from_raw_fd(fd) });
        file.sync_data()
    }
    #[cfg(not(unix))]
    {
        let _ = fd;
        Ok(())
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
        let log_id = Self::find_next_log_id(&config.journal_dir);
        Self { log_id, file: None, position: 0 }
    }

    fn find_next_log_id(dir: &Path) -> u64 {
        let mut max_id = 0u64;
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.ends_with(".txn") {
                    if let Ok(id) = u64::from_str_radix(name.trim_end_matches(".txn"), 16) {
                        if id > max_id { max_id = id; }
                    }
                }
            }
        }
        max_id + 1
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
        self.log_id += 1;
        self.position = 0;
        self.ensure_open(config)
    }
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

        let scan_data = &data[header_size..];
        let entries = format::scan_journal_entries(scan_data, header.version());
        for (idx, entry_data) in entries.iter().enumerate() {
            let _ = start_offset;
            let _ = idx;
            callback(entry_data);
            total_replayed += 1;
        }
    }

    Ok(total_replayed)
}
