use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use super::format::{self, JournalHeader, calculate_padding};
use crate::io_engine::JournalIo;
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
/// A single `journal-io` thread dequeues entries from a bounded channel,
/// batches them, and calls `JournalIo::write_and_sync()` which performs
/// the write + fsync atomically. When the backend is `UringJournalIo`,
/// this is a single `io_uring_enter()` with linked WRITEV→FSYNC SQEs.
/// When the backend is `StdEngine`, it falls back to `write_all` +
/// `sync_data` on the same thread.
///
/// This replaces the previous 2-thread architecture (writer + fsync)
/// and eliminates the inter-thread hop for the fsync path.
pub struct Journal {
    tx: mpsc::SyncSender<JournalEntry>,
    _io_thread: Option<std::thread::JoinHandle<()>>,
    /// Tracks which ledgers have had master keys journaled.
    master_key_cache: std::sync::Mutex<HashSet<i64>>,
}

impl Journal {
    /// Start the journal I/O thread.
    pub fn start(config: JournalConfig, journal_io: std::sync::Arc<dyn JournalIo>) -> Self {
        let bound = MAX_QUEUE_MEMORY / 1024;
        let (tx, rx) = mpsc::sync_channel(bound);

        let io_thread = std::thread::Builder::new()
            .name("journal-io".into())
            .spawn(move || journal_io_loop(config, rx, journal_io))
            .expect("failed to spawn journal-io thread");

        Self {
            tx,
            _io_thread: Some(io_thread),
            master_key_cache: std::sync::Mutex::new(HashSet::new()),
        }
    }

    /// Write an entry to the journal synchronously.
    /// Blocks until the entry is written and fsynced (data is durable).
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
    /// Atomic: cache insert happens under lock BEFORE journal send,
    /// matching Java's putIfAbsent + journal pattern.
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

        // Build master key entry: [ledgerId(8)][METAENTRY_ID(8)][keyLen(4)][key]
        let entry_len = 8 + 8 + 4 + master_key.len();
        let mut entry = Vec::with_capacity(entry_len);
        entry.extend_from_slice(&ledger_id.to_be_bytes());
        entry.extend_from_slice(&METAENTRY_ID_LEDGER_KEY.to_be_bytes());
        entry.extend_from_slice(&(master_key.len() as i32).to_be_bytes());
        entry.extend_from_slice(master_key);

        // Fire-and-forget (no ACK needed — matches Java's NopWriteCallback)
        let _ = self.tx.send(JournalEntry {
            data: entry,
            ack_tx: None,
        });

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Journal I/O thread — single thread replaces the old writer + fsync pair
// ---------------------------------------------------------------------------

/// Single background thread that owns journal file state and delegates
/// all I/O to the `JournalIo` trait (uring or std).
fn journal_io_loop(
    config: JournalConfig,
    rx: mpsc::Receiver<JournalEntry>,
    journal_io: std::sync::Arc<dyn JournalIo>,
) {
    fs::create_dir_all(&config.journal_dir).ok();

    let mut state = JournalFileState::new(&config);
    let group_wait = Duration::from_millis(config.max_group_wait_ms.max(1));
    let mut pending_acks: Vec<mpsc::Sender<()>> = Vec::new();
    let mut pending_entries: Vec<Vec<u8>> = Vec::new();

    loop {
        let timeout = if !pending_entries.is_empty() {
            group_wait
        } else {
            Duration::from_secs(3600)
        };

        match rx.recv_timeout(timeout) {
            Ok(entry) => {
                // Frame entry: [size(4 BE)][payload]
                let framed = frame_journal_entry(&entry.data);
                pending_entries.push(framed);
                if let Some(ack) = entry.ack_tx {
                    pending_acks.push(ack);
                }

                // Drain more entries without blocking (batch)
                while let Ok(entry) = rx.try_recv() {
                    let framed = frame_journal_entry(&entry.data);
                    pending_entries.push(framed);
                    if let Some(ack) = entry.ack_tx {
                        pending_acks.push(ack);
                    }
                }

                // Flush the batch
                flush_batch(
                    &config,
                    &mut state,
                    &journal_io,
                    &mut pending_entries,
                    &mut pending_acks,
                );
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if !pending_entries.is_empty() {
                    flush_batch(
                        &config,
                        &mut state,
                        &journal_io,
                        &mut pending_entries,
                        &mut pending_acks,
                    );
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if !pending_entries.is_empty() {
                    flush_batch(
                        &config,
                        &mut state,
                        &journal_io,
                        &mut pending_entries,
                        &mut pending_acks,
                    );
                }
                break;
            }
        }
    }
}

/// Flush a batch of framed entries via `JournalIo::write_and_sync`.
fn flush_batch(
    config: &JournalConfig,
    state: &mut JournalFileState,
    journal_io: &std::sync::Arc<dyn JournalIo>,
    pending_entries: &mut Vec<Vec<u8>>,
    pending_acks: &mut Vec<mpsc::Sender<()>>,
) {
    if pending_entries.is_empty() {
        return;
    }

    // Ensure the journal file is open
    if let Err(e) = state.ensure_open(config) {
        tracing::warn!("Journal open error: {e}");
        pending_entries.clear();
        pending_acks.clear();
        return;
    }

    // Check if we need to rotate BEFORE writing (check total batch size)
    let batch_size: u64 = pending_entries.iter().map(|e| e.len() as u64).sum();
    if state.position > 0 && state.position + batch_size > config.max_file_size {
        if let Err(e) = state.rotate(config, journal_io) {
            tracing::warn!("Journal rotation error: {e}");
        }
    }

    // Add V5+ padding if needed
    let padding = if config.format_version >= 5 && config.alignment_size > 0 {
        let p = calculate_padding(state.position, config.alignment_size as u64);
        if !p.is_empty() { Some(p) } else { None }
    } else {
        None
    };

    // Build the slice array for write_and_sync: [padding?] + entries
    let mut slices: Vec<&[u8]> = Vec::with_capacity(pending_entries.len() + 1);
    if let Some(ref pad) = padding {
        slices.push(pad);
    }
    for entry in pending_entries.iter() {
        slices.push(entry);
    }

    // Delegate to the I/O engine (uring: WRITEV→linked FSYNC; std: write_all + sync_data)
    let fd = state.file.as_ref().unwrap().as_raw_fd();
    match journal_io.write_and_sync(fd, &slices) {
        Ok(()) => {
            // Update position tracking
            if let Some(ref pad) = padding {
                state.position += pad.len() as u64;
            }
            for entry in pending_entries.iter() {
                state.position += entry.len() as u64;
            }
            // ACK all clients — data is durable
            for ack in pending_acks.drain(..) {
                let _ = ack.send(());
            }
        }
        Err(e) => {
            tracing::error!("FATAL: journal write+sync failed: {e}");
            // Don't ACK — clients will timeout. Matches Java's crash-on-fsync.
        }
    }
    pending_entries.clear();
}

/// Frame a journal entry: `[size(4 BE)][payload]`.
fn frame_journal_entry(data: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(4 + data.len());
    framed.extend_from_slice(&(data.len() as i32).to_be_bytes());
    framed.extend_from_slice(data);
    framed
}

// ---------------------------------------------------------------------------
// Journal file state — tracks the current .txn file
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

        // Write header
        let header = JournalHeader::new(config.format_version);
        let header_bytes = header.to_bytes();
        file.write_all(&header_bytes)?;
        file.flush()?;

        self.position = header_bytes.len() as u64;
        self.file = Some(file);
        Ok(())
    }

    fn rotate(
        &mut self,
        config: &JournalConfig,
        _journal_io: &std::sync::Arc<dyn JournalIo>,
    ) -> Result<(), StorageError> {
        // Close current file
        self.file = None;
        self.log_id += 1;
        self.position = 0;
        // Next ensure_open will create the new file
        self.ensure_open(config)
    }
}

// ---------------------------------------------------------------------------
// Journal replay (unchanged — reads are always blocking)
// ---------------------------------------------------------------------------

/// Replay journal entries from disk for crash recovery.
pub fn replay_journal<F>(journal_dir: &Path, mut callback: F) -> Result<usize, StorageError>
where
    F: FnMut(&[u8]),
{
    let mut txn_files: Vec<(u64, PathBuf)> = Vec::new();
    if let Ok(entries) = fs::read_dir(journal_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".txn") {
                if let Ok(id) = u64::from_str_radix(name.trim_end_matches(".txn"), 16) {
                    txn_files.push((id, entry.path()));
                }
            }
        }
    }
    txn_files.sort_by_key(|(id, _)| *id);

    let mut total_replayed = 0;
    for (_id, path) in &txn_files {
        let data = fs::read(path)?;
        if data.len() < 8 {
            continue;
        }
        let header = format::JournalHeader::from_bytes(&data)?;
        let header_size = header.header_size();
        if data.len() <= header_size {
            continue;
        }
        let entries = format::scan_journal_entries(&data[header_size..], header.version());
        for entry_data in entries {
            callback(entry_data);
            total_replayed += 1;
        }
    }

    Ok(total_replayed)
}
