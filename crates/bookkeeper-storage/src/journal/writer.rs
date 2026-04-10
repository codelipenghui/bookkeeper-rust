use std::collections::HashSet;
use std::fs;
use std::io::{self, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use super::format::{self, JournalHeader, calculate_padding};
use crate::StorageError;

/// Meta-entry ID for master key journaling.
pub const METAENTRY_ID_LEDGER_KEY: i64 = -0x1000;

/// Maximum journal queue memory in bytes (default 32MB, matching Java).
const MAX_QUEUE_MEMORY: usize = 32 * 1024 * 1024;

/// A journal entry to be written.
struct JournalEntry {
    data: Vec<u8>,
    ack_tx: Option<tokio::sync::oneshot::Sender<()>>,
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
        self.tx
            .send(JournalEntry {
                data: data.to_vec(),
                ack_tx: Some(ack_tx),
            })
            .map_err(|_| StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe, "journal writer closed",
            )))?;
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

        let _ = self.tx.send(JournalEntry {
            data: entry,
            ack_tx: None,
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

    // Acks waiting for the in-flight fsync to complete.
    let mut syncing_acks: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
    let mut fsync_in_flight = false;

    // Instrumentation
    let mut loop_count: u64 = 0;
    let mut total_entries: u64 = 0;
    let mut total_syncs: u64 = 0;
    let mut total_sync_us: u64 = 0;
    let mut total_acked: u64 = 0;

    loop {
        // If fsync is in flight, don't block — poll for entries and CQE.
        let first = if fsync_in_flight {
            // Non-blocking: try to get entries while waiting for fsync
            match rx.try_recv() {
                Ok(entry) => Some(entry),
                Err(mpsc::TryRecvError::Empty) => {
                    // No entries available. Check if fsync completed.
                    if let Some(cqe) = ring.completion().next() {
                        if cqe.result() < 0 {
                            tracing::error!("FATAL: journal fsync failed: {}",
                                io::Error::from_raw_os_error(-cqe.result()));
                            if let Some(ref cb) = on_fatal_error {
                                cb(io::Error::from_raw_os_error(-cqe.result()));
                            }
                            syncing_acks.clear();
                        } else {
                            // Fsync complete — ACK all entries
                            for ack in syncing_acks.drain(..) {
                                let _ = ack.send(());
                            }
                        }
                        fsync_in_flight = false;
                    }
                    // If fsync still in flight and no entries, do a short wait
                    if fsync_in_flight {
                        match rx.recv_timeout(Duration::from_micros(100)) {
                            Ok(entry) => Some(entry),
                            Err(_) => {
                                // Check CQE again
                                if let Some(cqe) = ring.completion().next() {
                                    if cqe.result() < 0 {
                                        tracing::error!("FATAL: journal fsync failed: {}",
                                            io::Error::from_raw_os_error(-cqe.result()));
                                        if let Some(ref cb) = on_fatal_error {
                                            cb(io::Error::from_raw_os_error(-cqe.result()));
                                        }
                                        syncing_acks.clear();
                                    } else {
                                        for ack in syncing_acks.drain(..) {
                                            let _ = ack.send(());
                                        }
                                    }
                                    fsync_in_flight = false;
                                }
                                continue;
                            }
                        }
                    } else {
                        continue; // loop back, fsync just completed
                    }
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    // Channel closed — wait for in-flight fsync then exit
                    if fsync_in_flight {
                        ring.submit_and_wait(1).ok();
                        if let Some(cqe) = ring.completion().next() {
                            if cqe.result() >= 0 {
                                for ack in syncing_acks.drain(..) {
                                    let _ = ack.send(());
                                }
                            }
                        }
                    }
                    break;
                }
            }
        } else {
            // No fsync in flight — block for first entry
            match rx.recv() {
                Ok(entry) => Some(entry),
                Err(_) => break,
            }
        };

        if let Some(entry) = first {
            frame_into_buf(&mut write_buf, &entry.data);
            pending_bytes += 4 + entry.data.len();
            if let Some(ack) = entry.ack_tx {
                pending_acks.push(ack);
            }
        }

        // Drain more entries
        while let Ok(entry) = rx.try_recv() {
            frame_into_buf(&mut write_buf, &entry.data);
            pending_bytes += 4 + entry.data.len();
            if let Some(ack) = entry.ack_tx {
                pending_acks.push(ack);
            }
        }

        if write_buf.is_empty() {
            continue;
        }

        // Group wait (always wait when sync enabled to batch more per fsync)
        let should_wait = (buffered_entries_threshold == 0
                || pending_acks.len() < buffered_entries_threshold)
            && (buffered_writes_threshold == 0
                || pending_bytes < buffered_writes_threshold);

        if should_wait && !fsync_in_flight {
            let deadline = std::time::Instant::now() + group_wait;
            loop {
                if (buffered_entries_threshold > 0
                    && pending_acks.len() >= buffered_entries_threshold)
                    || (buffered_writes_threshold > 0
                        && pending_bytes >= buffered_writes_threshold)
                {
                    break;
                }
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match rx.recv_timeout(remaining) {
                    Ok(entry) => {
                        frame_into_buf(&mut write_buf, &entry.data);
                        pending_bytes += 4 + entry.data.len();
                        if let Some(ack) = entry.ack_tx {
                            pending_acks.push(ack);
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        // Write entries to file
        if let Err(e) = state.ensure_open(&config) {
            tracing::warn!("Journal open error: {e}");
            write_buf.clear();
            pending_acks.clear();
            pending_bytes = 0;
            continue;
        }
        shared_log_id.store(state.log_id, std::sync::atomic::Ordering::Relaxed);

        let batch_size = write_buf.len() as u64;
        if state.position > 0 && state.position + batch_size > config.max_file_size {
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
        let write_result = (|| -> io::Result<()> {
            if let Some(ref pad) = padding {
                file.write_all(pad)?;
            }
            file.write_all(&write_buf)?;
            Ok(())
        })();

        match write_result {
            Ok(()) => {
                if let Some(ref pad) = padding {
                    state.position += pad.len() as u64;
                }
                state.position += batch_size;

                // If a previous fsync completed, check CQE
                if fsync_in_flight {
                    if let Some(cqe) = ring.completion().next() {
                        if cqe.result() < 0 {
                            tracing::error!("FATAL: journal fsync failed: {}",
                                io::Error::from_raw_os_error(-cqe.result()));
                            if let Some(ref cb) = on_fatal_error {
                                cb(io::Error::from_raw_os_error(-cqe.result()));
                            }
                            syncing_acks.clear();
                        } else {
                            for ack in syncing_acks.drain(..) {
                                let _ = ack.send(());
                            }
                        }
                        fsync_in_flight = false;
                    }
                }

                // Move pending acks to syncing (they'll be ACK'd when fsync completes)
                let batch_size = pending_acks.len() as u64;
                total_entries += batch_size;
                total_acked += batch_size;
                syncing_acks.append(&mut pending_acks);

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
                    total_syncs += 1;
                }

                loop_count += 1;
                if loop_count % 1000 == 0 {
                    let avg_batch = if loop_count > 0 { total_entries / loop_count } else { 0 };
                    let avg_acked = if total_syncs > 0 { total_acked / total_syncs } else { 0 };
                    tracing::info!(
                        "journal-uring: loops={loop_count} entries={total_entries} syncs={total_syncs} \
                         avg_batch={avg_batch} avg_acked_per_sync={avg_acked} pending_sync={}", syncing_acks.len()
                    );
                }
            }
            Err(e) => {
                tracing::error!("FATAL: journal write failed: {e}");
                if let Some(ref cb) = on_fatal_error {
                    cb(io::Error::new(e.kind(), format!("journal write: {e}")));
                }
                pending_acks.clear();
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

    let mut loop_count: u64 = 0;
    let mut total_recv_us: u64 = 0;
    let mut total_flush_us: u64 = 0;
    let mut total_entries: u64 = 0;

    loop {
        let recv_start = std::time::Instant::now();
        let first = match rx.recv() {
            Ok(entry) => entry,
            Err(_) => {
                if !write_buf.is_empty() {
                    flush_write_buf(
                        &config, &mut state, &mut write_buf, &mut pending_acks,
                        &fw_tx, &on_fatal_error, &shared_log_id,
                    );
                }
                break;
            }
        };

        frame_into_buf(&mut write_buf, &first.data);
        pending_bytes += 4 + first.data.len();
        if let Some(ack) = first.ack_tx {
            pending_acks.push(ack);
        }

        while let Ok(entry) = rx.try_recv() {
            frame_into_buf(&mut write_buf, &entry.data);
            pending_bytes += 4 + entry.data.len();
            if let Some(ack) = entry.ack_tx {
                pending_acks.push(ack);
            }
        }

        total_recv_us += recv_start.elapsed().as_micros() as u64;

        let should_wait = if fw_tx.is_some() {
            (buffered_entries_threshold == 0
                || pending_acks.len() < buffered_entries_threshold)
                && (buffered_writes_threshold == 0
                    || pending_bytes < buffered_writes_threshold)
        } else {
            pending_acks.len() > 1
                && (buffered_entries_threshold == 0
                    || pending_acks.len() < buffered_entries_threshold)
                && (buffered_writes_threshold == 0
                    || pending_bytes < buffered_writes_threshold)
        };

        if should_wait {
            let deadline = std::time::Instant::now() + group_wait;
            loop {
                if (buffered_entries_threshold > 0
                    && pending_acks.len() >= buffered_entries_threshold)
                    || (buffered_writes_threshold > 0
                        && pending_bytes >= buffered_writes_threshold)
                {
                    break;
                }
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match rx.recv_timeout(remaining) {
                    Ok(entry) => {
                        frame_into_buf(&mut write_buf, &entry.data);
                        pending_bytes += 4 + entry.data.len();
                        if let Some(ack) = entry.ack_tx {
                            pending_acks.push(ack);
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        let flush_start = std::time::Instant::now();
        let batch_entries = pending_acks.len();
        flush_write_buf(
            &config, &mut state, &mut write_buf, &mut pending_acks,
            &fw_tx, &on_fatal_error, &shared_log_id,
        );
        total_flush_us += flush_start.elapsed().as_micros() as u64;
        total_entries += batch_entries as u64;
        loop_count += 1;
        pending_bytes = 0;

        if loop_count % 1000 == 0 {
            let avg_recv = total_recv_us / loop_count;
            let avg_flush = total_flush_us / loop_count;
            let avg_batch = total_entries / loop_count;
            tracing::info!(
                "journal-writer: loops={loop_count} avg_recv={avg_recv}µs avg_flush={avg_flush}µs avg_batch={avg_batch}"
            );
        }
    }
}

/// Frame a journal entry directly into a buffer: `[size(4 BE)][payload]`.
fn frame_into_buf(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
    buf.extend_from_slice(data);
}

/// Write buffered entries, then ACK or forward to force-write thread.
fn flush_write_buf(
    config: &JournalConfig,
    state: &mut JournalFileState,
    write_buf: &mut Vec<u8>,
    pending_acks: &mut Vec<tokio::sync::oneshot::Sender<()>>,
    fw_tx: &Option<mpsc::Sender<ForceWriteRequest>>,
    on_fatal_error: &Option<OnFatalError>,
    shared_log_id: &Arc<std::sync::atomic::AtomicU64>,
) {
    if write_buf.is_empty() {
        return;
    }

    if let Err(e) = state.ensure_open(config) {
        tracing::warn!("Journal open error: {e}");
        write_buf.clear();
        pending_acks.clear();
        return;
    }
    shared_log_id.store(state.log_id, std::sync::atomic::Ordering::Relaxed);

    let batch_size = write_buf.len() as u64;
    if state.position > 0 && state.position + batch_size > config.max_file_size {
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
    let write_result = (|| -> io::Result<()> {
        if let Some(ref pad) = padding {
            file.write_all(pad)?;
        }
        file.write_all(write_buf)?;
        Ok(())
    })();

    match write_result {
        Ok(()) => {
            if let Some(ref pad) = padding {
                state.position += pad.len() as u64;
            }
            state.position += batch_size;

            let fd = file.as_raw_fd();
            let acks = std::mem::take(pending_acks);

            if let Some(ref tx) = fw_tx {
                if tx.send(ForceWriteRequest { fd, acks }).is_err() {
                    tracing::error!("Force-write thread gone");
                }
            } else {
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
        }
    }
    write_buf.clear();
}

// ---------------------------------------------------------------------------
// Force-write thread (StdEngine only)
// ---------------------------------------------------------------------------

fn force_write_loop(rx: mpsc::Receiver<ForceWriteRequest>) {
    let mut sync_count: u64 = 0;
    let mut total_sync_us: u64 = 0;
    let mut max_sync_us: u64 = 0;

    loop {
        let first = match rx.recv() {
            Ok(req) => req,
            Err(_) => break,
        };

        let mut all_acks = first.acks;
        let mut last_fd = first.fd;
        while let Ok(req) = rx.try_recv() {
            all_acks.extend(req.acks);
            last_fd = req.fd;
        }

        let entries_in_batch = all_acks.len();
        let start = std::time::Instant::now();

        if let Err(e) = sync_fd(last_fd) {
            tracing::error!("FATAL: journal fsync failed: {e}");
            continue;
        }

        let elapsed_us = start.elapsed().as_micros() as u64;
        sync_count += 1;
        total_sync_us += elapsed_us;
        if elapsed_us > max_sync_us {
            max_sync_us = elapsed_us;
        }

        if sync_count % 100 == 0 {
            let avg_us = total_sync_us / sync_count;
            tracing::info!(
                "journal-sync: syncs={sync_count} avg={avg_us}µs max={max_sync_us}µs batch={entries_in_batch}"
            );
        }

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
