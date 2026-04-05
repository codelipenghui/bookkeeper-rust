use std::collections::HashSet;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use super::format::{self, JournalHeader, calculate_padding};
use crate::StorageError;

/// Meta-entry ID for master key journaling.
/// Matches Java's BookieImpl.METAENTRY_ID_LEDGER_KEY = -0x1000.
pub const METAENTRY_ID_LEDGER_KEY: i64 = -0x1000;

/// Maximum journal queue memory in bytes (default 32MB, matching Java).
const MAX_QUEUE_MEMORY: usize = 32 * 1024 * 1024;

/// A journal entry to be written.
struct JournalEntry {
    data: Vec<u8>,
    ack_tx: Option<mpsc::Sender<()>>,
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
/// Matches Java's Journal + ForceWriteThread:
/// - Journal writer thread: dequeues entries, writes to BufWriter, flushes, ACKs
/// - ForceWrite thread: batches fsync operations asynchronously
/// - Master key journaling: first write to a ledger journals the master key
/// - V5+ alignment padding between flushes
/// - Memory backpressure: blocks writers when queue exceeds limit
pub struct Journal {
    tx: mpsc::SyncSender<JournalEntry>,
    _writer_thread: Option<std::thread::JoinHandle<()>>,
    _fsync_thread: Option<std::thread::JoinHandle<()>>,
    /// Tracks which ledgers have had master keys journaled (for first-write detection).
    master_key_cache: std::sync::Mutex<HashSet<i64>>,
}

impl Journal {
    /// Start the journal background threads.
    pub fn start(config: JournalConfig) -> Self {
        // Bounded channel for memory backpressure (matching Java's memoryLimitController).
        // Bound = MAX_QUEUE_MEMORY / avg_entry_size. Conservative estimate: 1KB avg.
        let bound = MAX_QUEUE_MEMORY / 1024;
        let (tx, rx) = mpsc::sync_channel(bound);

        // ForceWrite channel: journal writer sends fsync requests to fsync thread
        let (fsync_tx, fsync_rx) = mpsc::channel::<FsyncRequest>();

        let writer_config = config.clone();
        let writer_thread = std::thread::Builder::new()
            .name("journal-writer".into())
            .spawn(move || journal_writer_loop(writer_config, rx, fsync_tx))
            .expect("failed to spawn journal writer thread");

        let fsync_config = config.clone();
        let fsync_thread = std::thread::Builder::new()
            .name("journal-fsync".into())
            .spawn(move || fsync_loop(fsync_config, fsync_rx))
            .expect("failed to spawn journal fsync thread");

        Self {
            tx,
            _writer_thread: Some(writer_thread),
            _fsync_thread: Some(fsync_thread),
            master_key_cache: std::sync::Mutex::new(HashSet::new()),
        }
    }

    /// Write an entry to the journal synchronously.
    /// Blocks until the entry is flushed to OS page cache.
    pub fn log_add_entry(&self, data: &[u8]) -> Result<(), StorageError> {
        let (ack_tx, ack_rx) = mpsc::channel();
        self.tx
            .send(JournalEntry {
                data: data.to_vec(),
                ack_tx: Some(ack_tx),
            })
            .map_err(|_| StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe, "journal writer closed",
            )))?;
        ack_rx.recv().map_err(|_| StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe, "journal ack lost",
        )))
    }

    /// Journal a master key entry for a ledger if not already journaled.
    /// Matches Java's BookieImpl.addEntryInternal which journals
    /// METAENTRY_ID_LEDGER_KEY on first write to each ledger.
    /// Journal a master key entry for a ledger if not already journaled.
    /// Atomic: cache insert happens under lock BEFORE journal send,
    /// matching Java's putIfAbsent + journal pattern.
    pub fn maybe_journal_master_key(
        &self,
        ledger_id: i64,
        master_key: &[u8],
    ) -> Result<(), StorageError> {
        let mut cache = self.master_key_cache.lock().unwrap();
        if cache.contains(&ledger_id) {
            return Ok(()); // already journaled
        }
        // Insert into cache first (under lock) to prevent duplicate journaling.
        // Matches Java's putIfAbsent semantics.
        cache.insert(ledger_id);
        drop(cache); // release lock before blocking on channel send

        // Build master key entry: [ledgerId(8)][METAENTRY_ID(8)][keyLen(4)][key]
        let entry_len = 8 + 8 + 4 + master_key.len();
        let mut entry = Vec::with_capacity(entry_len);
        entry.extend_from_slice(&ledger_id.to_be_bytes());
        entry.extend_from_slice(&METAENTRY_ID_LEDGER_KEY.to_be_bytes());
        entry.extend_from_slice(&(master_key.len() as i32).to_be_bytes());
        entry.extend_from_slice(master_key);

        // Journal it (fire-and-forget, no ACK needed — matches Java's NopWriteCallback)
        let _ = self.tx.send(JournalEntry {
            data: entry,
            ack_tx: None,
        });

        Ok(())
    }
}

/// Request from writer thread to fsync thread.
struct FsyncRequest {
    journal_path: PathBuf,
    /// Pending client ACKs that must wait for fsync (syncData=true).
    /// These are ACKed AFTER fsync, matching Java's ackBeforeSync=false.
    pending_acks: Vec<mpsc::Sender<()>>,
}

/// Background journal writer loop (runs on dedicated OS thread).
fn journal_writer_loop(
    config: JournalConfig,
    rx: mpsc::Receiver<JournalEntry>,
    fsync_tx: mpsc::Sender<FsyncRequest>,
) {
    fs::create_dir_all(&config.journal_dir).ok();

    let mut state = WriterState::new(&config);
    let group_wait = Duration::from_millis(config.max_group_wait_ms.max(1));
    let mut pending_acks: Vec<mpsc::Sender<()>> = Vec::new();
    let mut pending_bytes: usize = 0;
    let mut pending_count: usize = 0;

    loop {
        let timeout = if pending_count > 0 { group_wait } else { Duration::from_secs(3600) };

        match rx.recv_timeout(timeout) {
            Ok(entry) => {
                if let Err(e) = state.write_entry(&config, &entry.data) {
                    tracing::warn!("Journal write error: {e}");
                }
                pending_bytes += 4 + entry.data.len();
                pending_count += 1;
                if let Some(ack) = entry.ack_tx {
                    pending_acks.push(ack);
                }

                // Drain more entries without blocking (batch)
                while let Ok(entry) = rx.try_recv() {
                    if let Err(e) = state.write_entry(&config, &entry.data) {
                        tracing::warn!("Journal write error: {e}");
                    }
                    pending_bytes += 4 + entry.data.len();
                    pending_count += 1;
                    if let Some(ack) = entry.ack_tx {
                        pending_acks.push(ack);
                    }
                }

                // Group commit decision matching Java's Journal.java:1052-1086:
                // 1. Threshold exceeded (entries or bytes)
                // 2. Queue drained (all available entries consumed — flush what we have)
                //
                // Java uses flushWhenQueueEmpty which flushes when no more entries are
                // immediately available. The try_recv() loop above already drained
                // everything available, so we always flush here. The group_wait timeout
                // applies when the FIRST entry arrives and we wait for more before
                // the initial recv_timeout. Once we have entries, we drain and flush.
                let should_flush = pending_count > 0;

                if should_flush {
                    // V5+ padding before flush
                    if config.format_version >= 5 && config.alignment_size > 0 {
                        if let Err(e) = state.write_padding(config.alignment_size) {
                            tracing::warn!("Journal padding error: {e}");
                        }
                    }

                    // Flush to OS page cache
                    if let Err(e) = state.flush(false) {
                        tracing::warn!("Journal flush error: {e}");
                    }

                    if config.sync_data {
                        // syncData=true: ACK AFTER fsync (matching Java's ackBeforeSync=false).
                        // Send pending ACKs to ForceWrite thread; it will ACK after fsync.
                        if let Some(ref path) = state.current_path(&config) {
                            let _ = fsync_tx.send(FsyncRequest {
                                journal_path: path.clone(),
                                pending_acks: pending_acks.drain(..).collect(),
                            });
                        }
                    } else {
                        // syncData=false: ACK immediately after page cache write.
                        for ack in pending_acks.drain(..) {
                            let _ = ack.send(());
                        }
                    }

                    pending_bytes = 0;
                    pending_count = 0;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if pending_count > 0 {
                    if config.format_version >= 5 && config.alignment_size > 0 {
                        let _ = state.write_padding(config.alignment_size);
                    }
                    let _ = state.flush(false);

                    if config.sync_data {
                        if let Some(ref path) = state.current_path(&config) {
                            let _ = fsync_tx.send(FsyncRequest {
                                journal_path: path.clone(),
                                pending_acks: pending_acks.drain(..).collect(),
                            });
                        }
                    } else {
                        for ack in pending_acks.drain(..) {
                            let _ = ack.send(());
                        }
                    }
                    pending_bytes = 0;
                    pending_count = 0;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if pending_count > 0 {
                    let _ = state.flush(config.sync_data);
                    for ack in pending_acks.drain(..) {
                        let _ = ack.send(());
                    }
                }
                break;
            }
        }
    }
}

/// Dedicated fsync thread — batches fsync operations, then ACKs clients.
///
/// Matches Java's ForceWriteThread:
/// 1. Collects all pending FsyncRequests
/// 2. Fsyncs ALL unique files (handles journal rotation across batch)
/// 3. ACKs all clients whose entries are now durable
/// 4. On fsync failure: logs error, does NOT ACK (clients will timeout)
///    Matching Java which crashes the bookie on fsync failure.
fn fsync_loop(_config: JournalConfig, rx: mpsc::Receiver<FsyncRequest>) {
    loop {
        let first = match rx.recv() {
            Ok(req) => req,
            Err(_) => break,
        };

        let mut all_requests = vec![first];
        while let Ok(req) = rx.try_recv() {
            all_requests.push(req);
        }

        // Collect ALL unique file paths (handles rotation across requests)
        let mut unique_paths: Vec<PathBuf> = Vec::new();
        for req in &all_requests {
            if !unique_paths.contains(&req.journal_path) {
                unique_paths.push(req.journal_path.clone());
            }
        }

        // fsync ALL files (each file may have entries from different batches)
        let mut fsync_ok = true;
        for path in &unique_paths {
            match fs::OpenOptions::new().read(true).open(path) {
                Ok(file) => {
                    if let Err(e) = file.sync_data() {
                        tracing::error!("FATAL: journal fsync failed for {:?}: {e}", path);
                        fsync_ok = false;
                    }
                }
                Err(e) => {
                    tracing::error!("FATAL: cannot open journal for fsync {:?}: {e}", path);
                    fsync_ok = false;
                }
            }
        }

        if fsync_ok {
            // ACK all clients — entries are durable on disk
            for req in all_requests {
                for ack in req.pending_acks {
                    let _ = ack.send(());
                }
            }
        } else {
            // fsync failed — do NOT ACK clients. They will timeout.
            // Matching Java's behavior: ForceWriteThread crashes, pending ACKs abandoned.
            tracing::error!("FATAL: journal fsync failed, abandoning {} pending ACKs",
                all_requests.iter().map(|r| r.pending_acks.len()).sum::<usize>());
            // Drop pending_acks without sending — clients will get timeout errors
        }
    }
}

struct WriterState {
    log_id: u64,
    writer: Option<BufWriter<fs::File>>,
    position: u64,
}

impl WriterState {
    fn new(config: &JournalConfig) -> Self {
        let log_id = Self::find_next_log_id(&config.journal_dir);
        Self { log_id, writer: None, position: 0 }
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

    fn current_path(&self, config: &JournalConfig) -> Option<PathBuf> {
        if self.writer.is_some() {
            Some(config.journal_dir.join(format!("{:x}.txn", self.log_id)))
        } else {
            None
        }
    }

    fn ensure_open(&mut self, config: &JournalConfig) -> Result<(), StorageError> {
        if self.writer.is_some() { return Ok(()); }
        let path = config.journal_dir.join(format!("{:x}.txn", self.log_id));
        let file = fs::OpenOptions::new().create(true).write(true).open(&path)?;
        let mut writer = BufWriter::with_capacity(64 * 1024, file);
        let header = JournalHeader::new(config.format_version);
        let header_bytes = header.to_bytes();
        let hdr_size = header_bytes.len();
        writer.write_all(&header_bytes)?;
        self.position = hdr_size as u64;
        self.writer = Some(writer);
        Ok(())
    }

    fn write_entry(&mut self, config: &JournalConfig, data: &[u8]) -> Result<(), StorageError> {
        if self.position > 0 && self.position + (4 + data.len()) as u64 > config.max_file_size {
            self.rotate(config)?;
        }
        self.ensure_open(config)?;
        let writer = self.writer.as_mut().unwrap();
        writer.write_all(&(data.len() as i32).to_be_bytes())?;
        writer.write_all(data)?;
        self.position += (4 + data.len()) as u64;
        Ok(())
    }

    /// Write V5+ alignment padding at current position.
    fn write_padding(&mut self, alignment: usize) -> Result<(), StorageError> {
        if let Some(ref mut writer) = self.writer {
            let padding = calculate_padding(self.position, alignment as u64);
            if !padding.is_empty() {
                writer.write_all(&padding)?;
                self.position += padding.len() as u64;
            }
        }
        Ok(())
    }

    fn flush(&mut self, sync_data: bool) -> Result<(), StorageError> {
        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
            if sync_data { writer.get_ref().sync_data()?; }
        }
        Ok(())
    }

    fn rotate(&mut self, config: &JournalConfig) -> Result<(), StorageError> {
        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
            if config.sync_data { writer.get_ref().sync_data()?; }
        }
        self.writer = None;
        self.log_id += 1;
        self.position = 0;
        Ok(())
    }
}

/// Replay journal files for crash recovery.
pub fn replay_journal(
    journal_dir: &Path,
    mut callback: impl FnMut(&[u8]),
) -> Result<usize, StorageError> {
    let mut journal_files: Vec<(u64, PathBuf)> = Vec::new();
    if let Ok(entries) = fs::read_dir(journal_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".txn") {
                if let Ok(id) = u64::from_str_radix(name_str.trim_end_matches(".txn"), 16) {
                    journal_files.push((id, entry.path()));
                }
            }
        }
    }
    journal_files.sort_by_key(|(id, _)| *id);

    let mut total = 0;
    for (_, path) in &journal_files {
        let data = fs::read(path)?;
        if data.len() < 8 { continue; }
        let header = JournalHeader::from_bytes(&data)?;
        let hdr_size = header.header_size();
        if data.len() < hdr_size { continue; }
        let entries = format::scan_journal_entries(&data[hdr_size..], header.version());
        for entry_data in entries {
            callback(entry_data);
            total += 1;
        }
    }
    Ok(total)
}
