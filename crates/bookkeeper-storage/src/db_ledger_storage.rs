use std::fs;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use bytes::Bytes;
use parking_lot::RwLock;
use crate::cache::write_cache::WriteCache;
use crate::cache::read_cache::ReadCache;
use crate::entry_log::format::{self as elf, EntryLogHeader, EntryLocation, LOGFILE_HEADER_SIZE};
use crate::index::entry_location::EntryLocationIndex;
use crate::index::ledger_index::LedgerMetadataIndex;
use crate::StorageError;
use crate::ledger_storage::LedgerStorage;

/// Subdirectory under each ledger/journal base dir where all data lives.
/// Matches Java's `BookKeeperConstants.CURRENT_DIR = "current"`.
pub const CURRENT_DIR: &str = "current";

const DEFAULT_WRITE_CACHE_SIZE: usize = 64 * 1024 * 1024; // 64MB
const DEFAULT_READ_CACHE_SIZE: usize = 64 * 1024 * 1024;  // 64MB
const READ_CACHE_SEGMENTS: usize = 4;
const MAX_ENTRY_LOG_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
const MAX_ENTRY_SIZE: usize = 100 * 1024 * 1024; // 100MB sanity limit

struct EntryLogState {
    log_id: u32,
    offset: u64,
    /// Persistent file handle for the current entry log. Avoids open/close per write.
    /// Writes go through `LedgerIo::write_batch` using the raw fd from this handle.
    file: Option<fs::File>,
}

pub struct DbLedgerStorage {
    #[allow(dead_code)]
    base_dir: PathBuf,
    write_cache: RwLock<WriteCache>,
    write_cache_being_flushed: RwLock<WriteCache>,
    read_cache: ReadCache,
    entry_location_index: EntryLocationIndex,
    ledger_index: LedgerMetadataIndex,
    entry_log_dir: PathBuf,
    log_state: Mutex<EntryLogState>,
    flush_mutex: Mutex<()>,
    /// I/O engine for entry log reads/writes.
    ledger_io: Arc<dyn crate::io_engine::LedgerIo>,
    /// Journal for write-ahead logging. None if not initialized (standalone mode).
    journal: Option<crate::journal::writer::Journal>,
    /// Journal directory path (for cleanup after checkpoint).
    journal_dir: Option<PathBuf>,
    /// In-memory cache of explicit LAC bodies (matching Java's TransientLedgerInfo).
    explicit_lac_cache: dashmap::DashMap<i64, Vec<u8>>,
    /// LAC watcher registry for long poll reads.
    lac_watchers: crate::lac_watcher::LacWatcherRegistry,
}

impl DbLedgerStorage {
    pub fn open(base_dir: &Path, ledger_io: Arc<dyn crate::io_engine::LedgerIo>) -> Result<Self, StorageError> {
        // All data lives under `<base_dir>/current/`, matching Java's
        // BookieImpl.getCurrentDirectory(dir). Entry log files (.log) sit
        // directly in `current/` alongside the RocksDB subdirectories —
        // NOT in a separate `entry_logs/` subdir.
        let current_dir = base_dir.join(CURRENT_DIR);
        let locations_dir = current_dir.join("locations");
        let ledgers_dir = current_dir.join("ledgers");
        let entry_log_dir = current_dir.clone();
        fs::create_dir_all(&locations_dir)?;
        fs::create_dir_all(&ledgers_dir)?;

        let entry_location_index = EntryLocationIndex::open(&locations_dir)?;
        let ledger_index = LedgerMetadataIndex::open(&ledgers_dir)?;

        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            write_cache: RwLock::new(WriteCache::new(DEFAULT_WRITE_CACHE_SIZE)),
            write_cache_being_flushed: RwLock::new(WriteCache::new(DEFAULT_WRITE_CACHE_SIZE)),
            read_cache: ReadCache::new(DEFAULT_READ_CACHE_SIZE, READ_CACHE_SEGMENTS),
            entry_location_index,
            ledger_index,
            entry_log_dir,
            log_state: Mutex::new(EntryLogState {
                log_id: 0,
                offset: LOGFILE_HEADER_SIZE as u64,
                file: None,
            }),
            flush_mutex: Mutex::new(()),
            ledger_io,
            journal: None,
            journal_dir: None,
            explicit_lac_cache: dashmap::DashMap::new(),
            lac_watchers: crate::lac_watcher::LacWatcherRegistry::new(),
        })
    }

    /// Initialize the journal for write-ahead logging.
    /// Should be called after open() and before serving writes.
    /// Replays any existing journal entries for crash recovery.
    pub fn init_journal(
        &mut self,
        journal_dir: &Path,
        config: crate::journal::writer::JournalConfig,
        journal_io: std::sync::Arc<dyn crate::io_engine::JournalIo>,
    ) -> Result<usize, StorageError> {
        // Replay existing journal entries (crash recovery)
        let replayed = crate::journal::writer::replay_journal(journal_dir, |entry_data| {
            if entry_data.len() >= 32 {
                // Replay into write cache (same as add_entry, but without re-journaling)
                let ledger_id = Self::extract_ledger_id(entry_data);
                let entry_id = Self::extract_entry_id(entry_data);
                let lac = Self::extract_lac(entry_data);

                if self.ledger_index.get(ledger_id).ok().flatten().is_none() {
                    let _ = self.ledger_index.set(ledger_id, false, None, None);
                }
                if lac >= 0 {
                    let _ = self.ledger_index.update_lac(ledger_id, lac);
                }
                let _ = self.write_cache.read().put(ledger_id, entry_id, entry_data);
            }
        })?;

        if replayed > 0 {
            tracing::info!("Journal: replayed {} entries from crash recovery", replayed);
            // Flush replayed entries to entry logs
            let _ = self.flush();
        }

        // Start journal writer
        self.journal_dir = Some(journal_dir.to_path_buf());
        self.journal = Some(crate::journal::writer::Journal::start(config, journal_io));

        Ok(replayed)
    }

    /// Remove old journal files after successful flush/checkpoint.
    /// All entries have been flushed to entry logs + RocksDB, so journals are no longer needed.
    /// Matches Java's SyncThread journal cleanup after lastMark rollover.
    fn cleanup_journals(&self) {
        if let Some(ref journal_dir) = self.journal_dir {
            if let Ok(entries) = fs::read_dir(journal_dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    if name.to_string_lossy().ends_with(".txn") {
                        let _ = fs::remove_file(entry.path());
                    }
                }
            }
        }
    }

    /// Frame an entry for the entry log: `[size(4 BE)][payload]`.
    fn frame_entry(entry: &[u8]) -> Vec<u8> {
        let mut framed = Vec::with_capacity(4 + entry.len());
        framed.extend_from_slice(&(entry.len() as i32).to_be_bytes());
        framed.extend_from_slice(entry);
        framed
    }

    fn extract_ledger_id(entry: &[u8]) -> i64 {
        i64::from_be_bytes(entry[0..8].try_into().unwrap())
    }

    fn extract_entry_id(entry: &[u8]) -> i64 {
        i64::from_be_bytes(entry[8..16].try_into().unwrap())
    }

    fn extract_lac(entry: &[u8]) -> i64 {
        i64::from_be_bytes(entry[16..24].try_into().unwrap())
    }

    fn entry_log_path(&self, log_id: u32) -> PathBuf {
        self.entry_log_dir.join(format!("{:x}.log", log_id))
    }

    fn ensure_entry_log_exists(&self, log_id: u32) -> Result<(), StorageError> {
        let path = self.entry_log_path(log_id);
        if !path.exists() {
            let header = EntryLogHeader::new(elf::HEADER_CURRENT_VERSION);
            fs::write(&path, header.to_bytes())?;
        }
        Ok(())
    }

    fn read_entry_from_log(&self, location: u64) -> Result<Vec<u8>, StorageError> {
        let loc = EntryLocation::from_u64(location);
        let path = self.entry_log_path(loc.log_id());
        let file = fs::File::open(&path)?;
        let fd = file.as_raw_fd();
        let pos = loc.position() as u64;
        if pos < 4 {
            return Err(StorageError::CorruptData("invalid entry position".into()));
        }
        // Read 4-byte size at (pos - 4)
        let size_buf = self.ledger_io.read_at(fd, pos - 4, 4)?;
        let size = i32::from_be_bytes(size_buf[..4].try_into().unwrap()) as usize;
        if size > MAX_ENTRY_SIZE {
            return Err(StorageError::CorruptData(
                format!("entry size {} exceeds maximum {}", size, MAX_ENTRY_SIZE)));
        }
        // Read entry data at pos
        let entry = self.ledger_io.read_at(fd, pos, size)?;
        Ok(entry)
    }
}

impl LedgerStorage for DbLedgerStorage {
    fn add_entry(&self, entry: &[u8]) -> Result<(), StorageError> {
        if entry.len() < 32 {
            return Err(StorageError::CorruptData("entry too short".into()));
        }
        let ledger_id = Self::extract_ledger_id(entry);
        let entry_id = Self::extract_entry_id(entry);
        let lac = Self::extract_lac(entry);

        // Ensure ledger exists in index (no-op if validate_for_write already created it)
        if self.ledger_index.get(ledger_id)?.is_none() {
            self.ledger_index.set(ledger_id, false, None, None)?;
        }

        // Update LAC if valid
        if lac >= 0 {
            if let Err(e) = self.ledger_index.update_lac(ledger_id, lac) {
                // LAC update is best-effort during add_entry; LAC is also recoverable from entry data
                // Only ignore LedgerNotFound; propagate other errors
                if !matches!(e, StorageError::LedgerNotFound(_)) {
                    return Err(e);
                }
            }
        }

        // Notify long poll watchers that LAC has advanced.
        // Use the LAC value from the entry header (not entry_id).
        // Matches Java's TransientLedgerInfo.setLastAddConfirmed(lac) → notifyWatchers(lac).
        if lac >= 0 {
            self.lac_watchers.notify_lac_advance(ledger_id, lac);
        }

        // Try write cache
        {
            let cache = self.write_cache.read();
            if cache.put(ledger_id, entry_id, entry).is_ok() {
                return Ok(());
            }
        }
        // Cache full — flush and retry
        self.flush()?;
        let cache = self.write_cache.read();
        cache.put(ledger_id, entry_id, entry)
    }

    fn get_entry(&self, ledger_id: i64, entry_id: i64) -> Result<Bytes, StorageError> {
        // 1. Check active write cache (most recent writes)
        if let Some(data) = self.write_cache.read().get(ledger_id, entry_id) {
            return Ok(Bytes::from(data));
        }
        // 2. Check write cache being flushed
        if let Some(data) = self.write_cache_being_flushed.read().get(ledger_id, entry_id) {
            return Ok(Bytes::from(data));
        }
        // 3. Check read cache
        if let Some(data) = self.read_cache.get(ledger_id, entry_id) {
            return Ok(Bytes::from(data));
        }
        // 4. Check RocksDB index → entry log file
        if let Some(location) = self.entry_location_index.get(ledger_id, entry_id)? {
            let data = self.read_entry_from_log(location)?;
            self.read_cache.put(ledger_id, entry_id, &data);
            return Ok(Bytes::from(data));
        }
        Err(StorageError::EntryNotFound { ledger_id, entry_id })
    }

    fn ledger_exists(&self, ledger_id: i64) -> Result<bool, StorageError> {
        Ok(self.ledger_index.get(ledger_id)?.is_some())
    }

    fn set_fenced(&self, ledger_id: i64) -> Result<bool, StorageError> {
        self.ledger_index.set_fenced(ledger_id)?;
        Ok(true)
    }

    fn is_fenced(&self, ledger_id: i64) -> Result<bool, StorageError> {
        Ok(self.ledger_index.get(ledger_id)?.map_or(false, |d| d.fenced))
    }

    fn set_master_key(&self, ledger_id: i64, master_key: &[u8]) -> Result<(), StorageError> {
        match self.ledger_index.get(ledger_id)? {
            Some(existing) => {
                self.ledger_index.set(
                    ledger_id,
                    existing.fenced,
                    Some(master_key),
                    existing.lac,
                )
            }
            None => {
                self.ledger_index.set(ledger_id, false, Some(master_key), None)
            }
        }
    }

    fn check_master_key(&self, ledger_id: i64, master_key: &[u8]) -> Result<(), StorageError> {
        match self.ledger_index.get(ledger_id)? {
            Some(data) => {
                // Ledger exists — check key
                if let Some(stored_key) = &data.master_key {
                    if stored_key != master_key {
                        return Err(StorageError::Unauthorized(ledger_id));
                    }
                }
                // No key stored yet — store it
                if data.master_key.is_none() {
                    self.set_master_key(ledger_id, master_key)?;
                }
                Ok(())
            }
            None => {
                // Ledger doesn't exist — create with key
                self.ledger_index.set(ledger_id, false, Some(master_key), None)?;
                Ok(())
            }
        }
    }

    fn get_last_add_confirmed(&self, ledger_id: i64) -> Result<i64, StorageError> {
        // Return the highest entry_id stored for this ledger. This represents the
        // bookie's view of the last add confirmed: if entry N is durably stored,
        // entries 0..=N are all considered confirmed on this bookie. This is the
        // value returned during fencing reads (FLAG_DO_FENCING with LAST_ADD_CONFIRMED).
        {
            let cache = self.write_cache.read();
            if let Some(entry_id) = cache.last_entry_id(ledger_id) {
                return Ok(entry_id);
            }
        }
        // Check write cache being flushed
        {
            let cache = self.write_cache_being_flushed.read();
            if let Some(entry_id) = cache.last_entry_id(ledger_id) {
                return Ok(entry_id);
            }
        }
        // Fall back to RocksDB ledger index (highest persisted LAC)
        match self.ledger_index.get(ledger_id)? {
            Some(data) => Ok(data.lac.unwrap_or(-1)),
            None => Err(StorageError::LedgerNotFound(ledger_id)),
        }
    }

    fn set_explicit_lac(&self, ledger_id: i64, lac: i64) -> Result<(), StorageError> {
        self.ledger_index.update_lac(ledger_id, lac)
    }

    fn set_explicit_lac_body(&self, ledger_id: i64, body: &[u8]) -> Result<(), StorageError> {
        // In-memory cache for fast reads (matching Java's TransientLedgerInfo)
        self.explicit_lac_cache.insert(ledger_id, body.to_vec());
        // Persist to RocksDB so body survives restart (matching Java's ledgerIndex.setExplicitLac)
        self.ledger_index.set_explicit_lac_body(ledger_id, body)?;
        Ok(())
    }

    fn get_explicit_lac_body(&self, ledger_id: i64) -> Result<Option<Vec<u8>>, StorageError> {
        // Check in-memory cache first (fast path)
        if let Some(body) = self.explicit_lac_cache.get(&ledger_id) {
            return Ok(Some(body.clone()));
        }
        // Fall back to RocksDB (cold path, after restart)
        self.ledger_index.get_explicit_lac_body(ledger_id)
    }

    fn lac_watchers(&self) -> Option<&crate::lac_watcher::LacWatcherRegistry> {
        Some(&self.lac_watchers)
    }

    fn journal(&self) -> Option<&crate::journal::writer::Journal> {
        self.journal.as_ref()
    }

    fn get_active_ledgers(&self) -> Result<Vec<i64>, StorageError> {
        self.ledger_index.list_all_ledgers()
    }

    fn entry_log_dir(&self) -> Option<&Path> {
        Some(&self.entry_log_dir)
    }

    fn current_log_id(&self) -> Option<u32> {
        Some(self.log_state.lock().unwrap().log_id)
    }

    fn compact_entry_locations(&self) -> Result<(), StorageError> {
        self.entry_location_index.compact()
    }

    fn write_entry_to_log(&self, entry: &[u8]) -> Result<u64, StorageError> {
        let mut log_state = self.log_state.lock().unwrap();

        let encoded_size = (4 + entry.len()) as u64;
        if log_state.offset + encoded_size > MAX_ENTRY_LOG_SIZE {
            // Close current file before rotating
            log_state.file = None;
            log_state.log_id += 1;
            log_state.offset = LOGFILE_HEADER_SIZE as u64;
        }

        // Open file if not already open
        if log_state.file.is_none() {
            self.ensure_entry_log_exists(log_state.log_id)?;
            let path = self.entry_log_path(log_state.log_id);
            let file = fs::OpenOptions::new().read(true).write(true).open(&path)?;
            log_state.file = Some(file);
        }

        let framed = Self::frame_entry(entry);

        let fd = log_state.file.as_ref().unwrap().as_raw_fd();
        self.ledger_io.write_batch(fd, log_state.offset, &[&framed])?;

        let position = log_state.offset + 4;
        let location = EntryLocation::new(log_state.log_id, position as u32).to_u64();
        log_state.offset += encoded_size;
        Ok(location)
    }

    fn update_entry_locations(&self, batch: &[(i64, i64, u64)]) -> Result<(), StorageError> {
        // No per-entry flush needed — write_batch already wrote to disk.
        // Just update the RocksDB index.
        self.entry_location_index.put_batch(batch)?;
        self.entry_location_index.flush()
    }

    fn delete_ledger(&self, ledger_id: i64) -> Result<(), StorageError> {
        // Notify long poll watchers that this ledger is gone
        self.lac_watchers.notify_ledger_closed(ledger_id);
        self.ledger_index.delete(ledger_id)?;
        self.entry_location_index.delete_ledger(ledger_id)?;
        Ok(())
    }

    fn flush(&self) -> Result<(), StorageError> {
        let _lock = self.flush_mutex.lock().unwrap();

        // Swap: active → being_flushed, being_flushed (cleared) → active
        {
            let mut active = self.write_cache.write();
            let mut flushing = self.write_cache_being_flushed.write();
            std::mem::swap(&mut *active, &mut *flushing);
        }

        let flushing = self.write_cache_being_flushed.read();
        let entries = flushing.sorted_entries();
        if entries.is_empty() {
            drop(flushing);
            self.write_cache_being_flushed.write().clear();
            return Ok(());
        }

        let mut batch = Vec::with_capacity(entries.len());
        let flushing_data = flushing.data();

        {
            let mut log_state = self.log_state.lock().unwrap();

            // Open file if not already open
            if log_state.file.is_none() {
                self.ensure_entry_log_exists(log_state.log_id)?;
                let path = self.entry_log_path(log_state.log_id);
                let file = fs::OpenOptions::new().read(true).write(true).open(&path)?;
                log_state.file = Some(file);
            }

            // Collect framed entries for the current log segment, flushing
            // the batch via write_batch whenever rotation is needed.
            let mut framed_entries: Vec<Vec<u8>> = Vec::new();
            // Track (ledger_id, entry_id, offset_within_segment) for each
            // framed entry so we can build the final location batch.
            struct PendingEntry {
                ledger_id: i64,
                entry_id: i64,
                log_id: u32,
                position: u64,
            }
            let mut pending: Vec<PendingEntry> = Vec::new();

            for &(ledger_id, entry_id, offset, size) in &entries {
                let entry_data = &flushing_data[offset..offset + size];
                let encoded_size = (4 + entry_data.len()) as u64;

                if log_state.offset + encoded_size > MAX_ENTRY_LOG_SIZE {
                    // Write any accumulated entries to the current log before rotating
                    if !framed_entries.is_empty() {
                        let refs: Vec<&[u8]> = framed_entries.iter().map(|v| v.as_slice()).collect();
                        let fd = log_state.file.as_ref().unwrap().as_raw_fd();
                        // offset for write_batch is the offset of the first pending entry minus 4
                        // (the size prefix), which equals pending[0].position - 4
                        let write_offset = pending.first().unwrap().position - 4;
                        self.ledger_io.write_batch(fd, write_offset, &refs)?;
                        for p in &pending {
                            batch.push((p.ledger_id, p.entry_id,
                                EntryLocation::new(p.log_id, p.position as u32).to_u64()));
                        }
                        framed_entries.clear();
                        pending.clear();
                    }

                    // Rotate: close current, open new
                    log_state.file = None;
                    log_state.log_id += 1;
                    log_state.offset = LOGFILE_HEADER_SIZE as u64;
                    self.ensure_entry_log_exists(log_state.log_id)?;
                    let new_path = self.entry_log_path(log_state.log_id);
                    let file = fs::OpenOptions::new().read(true).write(true).open(&new_path)?;
                    log_state.file = Some(file);
                }

                let framed = Self::frame_entry(entry_data);

                let position = log_state.offset + 4;
                pending.push(PendingEntry {
                    ledger_id,
                    entry_id,
                    log_id: log_state.log_id,
                    position,
                });
                framed_entries.push(framed);
                log_state.offset += encoded_size;
            }

            // Write remaining entries
            if !framed_entries.is_empty() {
                let refs: Vec<&[u8]> = framed_entries.iter().map(|v| v.as_slice()).collect();
                let fd = log_state.file.as_ref().unwrap().as_raw_fd();
                let write_offset = pending.first().unwrap().position - 4;
                self.ledger_io.write_batch(fd, write_offset, &refs)?;
                for p in &pending {
                    batch.push((p.ledger_id, p.entry_id,
                        EntryLocation::new(p.log_id, p.position as u32).to_u64()));
                }
            }
        }

        if !batch.is_empty() {
            self.entry_location_index.put_batch(&batch)?;
        }
        self.entry_location_index.flush()?;
        self.ledger_index.flush()?;
        drop(flushing);
        self.write_cache_being_flushed.write().clear();
        Ok(())
    }

    fn checkpoint(&self) -> Result<(), StorageError> {
        self.flush()?;
        // After flush, all write cache entries are in entry logs + RocksDB index.
        // Old journal files are no longer needed — clean them up.
        self.cleanup_journals();
        Ok(())
    }

    fn validate_for_write(&self, ledger_id: i64, master_key: &[u8]) -> Result<bool, StorageError> {
        match self.ledger_index.get(ledger_id)? {
            Some(data) => {
                // Check master key
                if let Some(stored_key) = &data.master_key {
                    if stored_key != master_key {
                        return Err(StorageError::Unauthorized(ledger_id));
                    }
                } else {
                    // No key stored — store it (preserving existing fenced/lac)
                    self.ledger_index.set(ledger_id, data.fenced, Some(master_key), data.lac)?;
                }
                Ok(data.fenced)
            }
            None => {
                // Create new ledger with key
                self.ledger_index.set(ledger_id, false, Some(master_key), None)?;
                Ok(false)
            }
        }
    }
}
