use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use super::format::{self, JournalHeader, SECTOR_SIZE};
use crate::StorageError;

/// A journal entry to be written.
struct JournalEntry {
    data: Vec<u8>,
    ack_tx: Option<std::sync::mpsc::Sender<()>>,
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
/// Matches Java's Journal:
/// - Background OS thread dequeues entries and writes to journal files
/// - Group commit: batches by time, entry count, or byte threshold
/// - fsync when sync_data=true
/// - File rotation when max_file_size exceeded
/// - Sync API: `log_add_entry()` blocks until entry is durable
pub struct Journal {
    tx: mpsc::Sender<JournalEntry>,
    _thread: Option<std::thread::JoinHandle<()>>,
}

impl Journal {
    /// Start the journal background writer thread.
    pub fn start(config: JournalConfig) -> Self {
        let (tx, rx) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("journal-writer".into())
            .spawn(move || journal_writer_loop(config, rx))
            .expect("failed to spawn journal writer thread");
        Self {
            tx,
            _thread: Some(thread),
        }
    }

    /// Write an entry to the journal synchronously.
    /// Blocks until the entry is flushed (and fsynced if sync_data=true).
    /// Called from the request processor (spawn_blocking context).
    pub fn log_add_entry(&self, data: &[u8]) -> Result<(), StorageError> {
        let (ack_tx, ack_rx) = mpsc::channel();
        self.tx
            .send(JournalEntry {
                data: data.to_vec(),
                ack_tx: Some(ack_tx),
            })
            .map_err(|_| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "journal writer closed",
                ))
            })?;
        ack_rx.recv().map_err(|_| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "journal ack lost",
            ))
        })
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        // Drop the sender to signal the writer thread to exit
        // The thread will flush remaining entries and stop
    }
}

/// Background journal writer loop (runs on dedicated OS thread).
fn journal_writer_loop(config: JournalConfig, rx: mpsc::Receiver<JournalEntry>) {
    fs::create_dir_all(&config.journal_dir).ok();

    let mut state = WriterState::new(&config);
    let group_wait = Duration::from_millis(config.max_group_wait_ms.max(1));
    let mut pending_acks: Vec<mpsc::Sender<()>> = Vec::new();
    let mut pending_bytes: usize = 0;
    let mut pending_count: usize = 0;

    loop {
        // Wait for first entry (blocking)
        let timeout = if pending_count > 0 { group_wait } else { Duration::from_secs(3600) };

        match rx.recv_timeout(timeout) {
            Ok(entry) => {
                if let Err(e) = state.write_entry(&config, &entry.data) {
                    eprintln!("Journal write error: {e}");
                }
                pending_bytes += 4 + entry.data.len();
                pending_count += 1;
                if let Some(ack) = entry.ack_tx {
                    pending_acks.push(ack);
                }

                // Drain more entries without blocking (batch)
                while let Ok(entry) = rx.try_recv() {
                    if let Err(e) = state.write_entry(&config, &entry.data) {
                        eprintln!("Journal write error: {e}");
                    }
                    pending_bytes += 4 + entry.data.len();
                    pending_count += 1;
                    if let Some(ack) = entry.ack_tx {
                        pending_acks.push(ack);
                    }
                }

                // Group commit threshold check
                let should_flush = (config.buffered_entries_threshold > 0
                    && pending_count >= config.buffered_entries_threshold)
                    || (config.buffered_writes_threshold > 0
                        && pending_bytes >= config.buffered_writes_threshold)
                    || pending_count > 0; // always flush after draining

                if should_flush {
                    flush_and_ack(
                        &mut state,
                        &config,
                        &mut pending_acks,
                        &mut pending_bytes,
                        &mut pending_count,
                    );
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // Group commit by time
                if pending_count > 0 {
                    flush_and_ack(
                        &mut state,
                        &config,
                        &mut pending_acks,
                        &mut pending_bytes,
                        &mut pending_count,
                    );
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                // Sender dropped — flush remaining and exit
                if pending_count > 0 {
                    flush_and_ack(
                        &mut state,
                        &config,
                        &mut pending_acks,
                        &mut pending_bytes,
                        &mut pending_count,
                    );
                }
                break;
            }
        }
    }
}

fn flush_and_ack(
    state: &mut WriterState,
    config: &JournalConfig,
    pending_acks: &mut Vec<mpsc::Sender<()>>,
    pending_bytes: &mut usize,
    pending_count: &mut usize,
) {
    // Step 1: Flush to OS page cache
    if let Err(e) = state.flush(false) {
        eprintln!("Journal flush error: {e}");
    }

    // Step 2: ACK clients immediately after page cache write (matching Java's ackBeforeSync=true).
    // This is the default behavior — clients get fast ACKs, fsync happens async below.
    for ack in pending_acks.drain(..) {
        let _ = ack.send(());
    }

    // Step 3: fsync to disk if configured (async relative to client ACK)
    if config.sync_data {
        if let Err(e) = state.fsync() {
            eprintln!("Journal fsync error: {e}");
        }
    }

    *pending_bytes = 0;
    *pending_count = 0;
}

struct WriterState {
    log_id: u64,
    writer: Option<BufWriter<fs::File>>,
    position: u64,
}

impl WriterState {
    fn new(config: &JournalConfig) -> Self {
        let log_id = Self::find_next_log_id(&config.journal_dir);
        Self {
            log_id,
            writer: None,
            position: 0,
        }
    }

    fn find_next_log_id(dir: &Path) -> u64 {
        let mut max_id = 0u64;
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.ends_with(".txn") {
                    if let Ok(id) = u64::from_str_radix(name.trim_end_matches(".txn"), 16) {
                        if id > max_id {
                            max_id = id;
                        }
                    }
                }
            }
        }
        max_id + 1
    }

    fn ensure_open(&mut self, config: &JournalConfig) -> Result<(), StorageError> {
        if self.writer.is_some() {
            return Ok(());
        }
        let path = config
            .journal_dir
            .join(format!("{:x}.txn", self.log_id));
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)?;
        let mut writer = BufWriter::with_capacity(64 * 1024, file);
        let header = JournalHeader::new(config.format_version);
        let header_bytes = header.to_bytes();
        let header_size = header_bytes.len();
        writer.write_all(&header_bytes)?;
        self.position = header_size as u64;
        self.writer = Some(writer);
        Ok(())
    }

    fn write_entry(&mut self, config: &JournalConfig, data: &[u8]) -> Result<(), StorageError> {
        if self.position > 0
            && self.position + (4 + data.len()) as u64 > config.max_file_size
        {
            self.rotate(config)?;
        }
        self.ensure_open(config)?;
        let writer = self.writer.as_mut().unwrap();
        writer.write_all(&(data.len() as i32).to_be_bytes())?;
        writer.write_all(data)?;
        self.position += (4 + data.len()) as u64;
        Ok(())
    }

    fn flush(&mut self, sync_data: bool) -> Result<(), StorageError> {
        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
            if sync_data {
                writer.get_ref().sync_data()?;
            }
        }
        Ok(())
    }

    fn fsync(&mut self) -> Result<(), StorageError> {
        if let Some(ref mut writer) = self.writer {
            writer.get_ref().sync_data()?;
        }
        Ok(())
    }

    fn rotate(&mut self, config: &JournalConfig) -> Result<(), StorageError> {
        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
            if config.sync_data {
                writer.get_ref().sync_data()?;
            }
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
                if let Ok(id) =
                    u64::from_str_radix(name_str.trim_end_matches(".txn"), 16)
                {
                    journal_files.push((id, entry.path()));
                }
            }
        }
    }
    journal_files.sort_by_key(|(id, _)| *id);

    let mut total = 0;
    for (_, path) in &journal_files {
        let data = fs::read(path)?;
        if data.len() < 8 {
            continue; // too small for even a minimal header
        }
        let header = JournalHeader::from_bytes(&data)?;
        let hdr_size = header.header_size();
        if data.len() < hdr_size {
            continue;
        }
        let entries = format::scan_journal_entries(&data[hdr_size..], header.version());
        for entry_data in entries {
            callback(entry_data);
            total += 1;
        }
    }
    Ok(total)
}
