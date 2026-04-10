use std::fs;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use crate::cache::write_cache::WriteCache;
use crate::cache::read_cache::ReadCache;
use crate::entry_log::format::{self as elf, EntryLogHeader, EntryLocation, LOGFILE_HEADER_SIZE};
use crate::index::entry_location::EntryLocationIndex;
use crate::index::ledger_index::LedgerMetadataIndex;
use crate::StorageError;
use crate::ledger_storage::LedgerStorage;
use crate::storage_metrics;

/// Subdirectory under each ledger/journal base dir where all data lives.
/// Matches Java's `BookKeeperConstants.CURRENT_DIR = "current"`.
pub const CURRENT_DIR: &str = "current";

const DEFAULT_WRITE_CACHE_SIZE: usize = 256 * 1024 * 1024; // 256MB
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
    flush_mutex: tokio::sync::Mutex<()>,
    /// I/O engine for entry log reads/writes.
    ledger_io: Arc<dyn crate::io_engine::LedgerIo>,
    /// Journal for write-ahead logging. None if not initialized (standalone mode).
    journal: Option<crate::journal::writer::Journal>,
    /// Journal directory path (for cleanup after checkpoint).
    journal_dir: Option<PathBuf>,
    /// In-memory LAC cache. Updated on every add_entry (in-memory only).
    /// Persisted to RocksDB during flush/checkpoint. Matches Java's
    /// TransientLedgerInfo which keeps LAC in memory and only writes to
    /// the ledger index during checkpoint.
    lac_cache: dashmap::DashMap<i64, i64>,
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
            flush_mutex: tokio::sync::Mutex::new(()),
            ledger_io,
            journal: None,
            journal_dir: None,
            lac_cache: dashmap::DashMap::new(),
            explicit_lac_cache: dashmap::DashMap::new(),
            lac_watchers: crate::lac_watcher::LacWatcherRegistry::new(),
        })
    }

    /// Initialize the journal for write-ahead logging.
    /// Should be called after open() and before serving writes.
    /// Replays any existing journal entries for crash recovery.
    pub async fn init_journal(
        &mut self,
        journal_dir: &Path,
        config: crate::journal::writer::JournalConfig,
        journal_io: std::sync::Arc<dyn crate::io_engine::JournalIo>,
        on_fatal_error: Option<crate::journal::writer::OnFatalError>,
    ) -> Result<usize, StorageError> {
        // Read lastMark from the ledger directory. This tells us where
        // the last successful checkpoint was, so we only replay journal
        // entries from that point forward. Matches Java's replay logic.
        let mark_path = self.base_dir.join(CURRENT_DIR).join("lastMark");
        let last_mark = crate::checkpoint::read_last_mark(&mark_path).ok();

        // Replay existing journal entries (crash recovery)
        let replayed = crate::journal::writer::replay_journal(journal_dir, last_mark, |entry_data| {
            if entry_data.len() >= 32 {
                // Replay into write cache (same as add_entry, but without re-journaling)
                let ledger_id = Self::extract_ledger_id(entry_data);
                let entry_id = Self::extract_entry_id(entry_data);
                let lac = Self::extract_lac(entry_data);

                if self.ledger_index.get(ledger_id).ok().flatten().is_none() {
                    let _ = self.ledger_index.set(ledger_id, false, None, None);
                }
                if lac >= 0 {
                    self.lac_cache.insert(ledger_id, lac);
                }
                let _ = self.write_cache.read().put(ledger_id, entry_id, entry_data);
            }
        })?;

        if replayed > 0 {
            tracing::info!("Journal: replayed {} entries from crash recovery", replayed);
            // Flush replayed entries to entry logs
            let _ = self.flush().await;
        }

        // Start journal writer
        self.journal_dir = Some(journal_dir.to_path_buf());
        self.journal = Some(crate::journal::writer::Journal::start(config, journal_io, on_fatal_error));

        Ok(replayed)
    }

    /// Remove old journal files after successful flush/checkpoint.
    ///
    /// Only deletes journal files whose ID is strictly less than the
    /// current journal file's ID. The current file (and any newer ones)
    /// are preserved because they may contain entries not yet flushed
    /// to entry logs. This matches Java's SyncThread cleanup which
    /// only removes journals older than the lastMark.
    fn cleanup_journals(&self) {
        let current_id = self.journal.as_ref()
            .and_then(|j| j.current_log_id());
        let Some(current_id) = current_id else { return; };

        if let Some(ref journal_dir) = self.journal_dir {
            if let Ok(entries) = fs::read_dir(journal_dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    if name.ends_with(".txn") {
                        if let Ok(id) = u64::from_str_radix(
                            name.trim_end_matches(".txn"), 16,
                        ) {
                            if id < current_id {
                                let _ = fs::remove_file(entry.path());
                            }
                        }
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

    async fn read_entry_from_log(&self, location: u64) -> Result<Vec<u8>, StorageError> {
        let loc = EntryLocation::from_u64(location);
        let pos = loc.position() as u64;
        if pos < 4 {
            return Err(StorageError::CorruptData("invalid entry position".into()));
        }
        // Single round-trip: read size prefix + data together
        let entry = self.ledger_io.read_framed_entry(
            &self.entry_log_dir, loc.log_id(), pos - 4, MAX_ENTRY_SIZE,
        ).await?;
        Ok(entry)
    }
}

#[async_trait]
impl LedgerStorage for DbLedgerStorage {
    async fn add_entry(&self, entry: &[u8]) -> Result<(), StorageError> {
        let add_start = Instant::now();
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

        // Update LAC in memory only (no RocksDB write per entry).
        // Persisted to RocksDB during flush/checkpoint.
        if lac >= 0 {
            self.lac_cache.insert(ledger_id, lac);
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
                storage_metrics::ADD_ENTRY.observe(add_start.elapsed().as_secs_f64());
                return Ok(());
            }
        }
        // Cache full — flush and retry (throttled write)
        storage_metrics::THROTTLED_WRITE_REQUESTS.inc();
        let throttle_start = Instant::now();
        self.flush().await?;
        storage_metrics::THROTTLED_WRITE.observe(throttle_start.elapsed().as_secs_f64());
        let cache = self.write_cache.read();
        let result = cache.put(ledger_id, entry_id, entry);
        storage_metrics::ADD_ENTRY.observe(add_start.elapsed().as_secs_f64());
        result
    }

    async fn get_entry(&self, ledger_id: i64, entry_id: i64) -> Result<Bytes, StorageError> {
        let read_start = Instant::now();
        // 1. Check active write cache (most recent writes)
        if let Some(data) = self.write_cache.read().get(ledger_id, entry_id) {
            storage_metrics::READ_ENTRY.observe(read_start.elapsed().as_secs_f64());
            return Ok(Bytes::from(data));
        }
        // 2. Check write cache being flushed
        if let Some(data) = self.write_cache_being_flushed.read().get(ledger_id, entry_id) {
            storage_metrics::READ_ENTRY.observe(read_start.elapsed().as_secs_f64());
            return Ok(Bytes::from(data));
        }
        // 3. Check read cache
        if let Some(data) = self.read_cache.get(ledger_id, entry_id) {
            storage_metrics::READ_ENTRY.observe(read_start.elapsed().as_secs_f64());
            return Ok(Bytes::from(data));
        }
        // 4. Check RocksDB index → entry log file
        let index_start = Instant::now();
        let location = self.entry_location_index.get(ledger_id, entry_id)?;
        storage_metrics::READ_LOCATIONS_INDEX_TIME.inc_by(index_start.elapsed().as_nanos() as u64);

        if let Some(location) = location {
            let entrylog_start = Instant::now();
            let data = self.read_entry_from_log(location).await?;
            storage_metrics::READ_ENTRYLOG_TIME.inc_by(entrylog_start.elapsed().as_nanos() as u64);

            self.read_cache.put(ledger_id, entry_id, &data);
            storage_metrics::READ_ENTRY.observe(read_start.elapsed().as_secs_f64());
            return Ok(Bytes::from(data));
        }
        storage_metrics::READ_ENTRY.observe(read_start.elapsed().as_secs_f64());
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
        // 1. Check write cache (max entry_id = effective LAC on this bookie)
        {
            let cache = self.write_cache.read();
            if let Some(entry_id) = cache.last_entry_id(ledger_id) {
                return Ok(entry_id);
            }
        }
        // 2. Check write cache being flushed
        {
            let cache = self.write_cache_being_flushed.read();
            if let Some(entry_id) = cache.last_entry_id(ledger_id) {
                return Ok(entry_id);
            }
        }
        // 3. Check in-memory LAC cache
        if let Some(lac) = self.lac_cache.get(&ledger_id) {
            return Ok(*lac);
        }
        // 4. Fall back to RocksDB ledger index (persisted LAC)
        match self.ledger_index.get(ledger_id)? {
            Some(data) => Ok(data.lac.unwrap_or(-1)),
            None => Err(StorageError::LedgerNotFound(ledger_id)),
        }
    }

    fn set_explicit_lac(&self, ledger_id: i64, lac: i64) -> Result<(), StorageError> {
        self.lac_cache.insert(ledger_id, lac);
        Ok(())
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

    async fn write_entry_to_log(&self, entry: &[u8]) -> Result<u64, StorageError> {
        let (fd, offset, log_id) = {
            let mut log_state = self.log_state.lock().unwrap();

            let encoded_size = (4 + entry.len()) as u64;
            if log_state.offset + encoded_size > MAX_ENTRY_LOG_SIZE {
                log_state.file = None;
                log_state.log_id += 1;
                log_state.offset = LOGFILE_HEADER_SIZE as u64;
            }

            if log_state.file.is_none() {
                self.ensure_entry_log_exists(log_state.log_id)?;
                let path = self.entry_log_path(log_state.log_id);
                let file = fs::OpenOptions::new().read(true).write(true).open(&path)?;
                log_state.file = Some(file);
            }

            let fd = log_state.file.as_ref().unwrap().as_raw_fd();
            let offset = log_state.offset;
            let log_id = log_state.log_id;
            (fd, offset, log_id)
        };

        let framed = Self::frame_entry(entry);
        let encoded_size = framed.len() as u64;
        self.ledger_io.write_batch(fd, offset, vec![framed]).await?;

        // Advance offset only after successful write
        let mut log_state = self.log_state.lock().unwrap();
        log_state.offset = offset + encoded_size;

        let position = offset + 4;
        let location = EntryLocation::new(log_id, position as u32).to_u64();
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

    async fn flush(&self) -> Result<(), StorageError> {
        let flush_start = Instant::now();
        let _lock = self.flush_mutex.lock().await;

        // Swap: active → being_flushed, being_flushed (cleared) → active
        {
            let mut active = self.write_cache.write();
            let mut flushing = self.write_cache_being_flushed.write();
            std::mem::swap(&mut *active, &mut *flushing);
        }

        // Phase 1: Under log_state mutex, collect all framed entries and metadata.
        // We cannot hold non-Send guards (MutexGuard, RwLockReadGuard) across
        // an .await point, so we read from the flushing cache, frame entries,
        // and drop all guards before the async I/O phase.
        struct FlushSegment {
            fd: RawFd,
            write_offset: u64,
            framed_entries: Vec<Vec<u8>>,
            locations: Vec<(i64, i64, u32, u64)>, // (ledger_id, entry_id, log_id, position)
        }
        let segments: Vec<FlushSegment>;
        let entry_count: usize;
        let total_bytes: u64;

        {
            let flushing = self.write_cache_being_flushed.read();
            let entries = flushing.sorted_entries();
            if entries.is_empty() {
                drop(flushing);
                self.write_cache_being_flushed.write().clear();
                storage_metrics::FLUSH.observe(flush_start.elapsed().as_secs_f64());
                return Ok(());
            }

            entry_count = entries.len();
            let flushing_data = flushing.data();

            let mut segs: Vec<FlushSegment> = Vec::new();
            let mut flush_bytes: u64 = 0;

            {
                let mut log_state = self.log_state.lock().unwrap();

                // Open file if not already open
                if log_state.file.is_none() {
                    self.ensure_entry_log_exists(log_state.log_id)?;
                    let path = self.entry_log_path(log_state.log_id);
                    let file = fs::OpenOptions::new().read(true).write(true).open(&path)?;
                    log_state.file = Some(file);
                }

                let mut current = FlushSegment {
                    fd: log_state.file.as_ref().unwrap().as_raw_fd(),
                    write_offset: log_state.offset,
                    framed_entries: Vec::new(),
                    locations: Vec::new(),
                };

                for &(ledger_id, entry_id, offset, size) in &entries {
                    let entry_data = &flushing_data[offset..offset + size];
                    let encoded_size = (4 + entry_data.len()) as u64;

                    if log_state.offset + encoded_size > MAX_ENTRY_LOG_SIZE {
                        // Push the current segment if it has entries
                        if !current.framed_entries.is_empty() {
                            segs.push(current);
                        }

                        // Rotate: close current, open new
                        log_state.file = None;
                        log_state.log_id += 1;
                        log_state.offset = LOGFILE_HEADER_SIZE as u64;
                        self.ensure_entry_log_exists(log_state.log_id)?;
                        let new_path = self.entry_log_path(log_state.log_id);
                        let file = fs::OpenOptions::new().read(true).write(true).open(&new_path)?;
                        log_state.file = Some(file);

                        current = FlushSegment {
                            fd: log_state.file.as_ref().unwrap().as_raw_fd(),
                            write_offset: log_state.offset,
                            framed_entries: Vec::new(),
                            locations: Vec::new(),
                        };
                    }

                    let framed = Self::frame_entry(entry_data);
                    let position = log_state.offset + 4;
                    current.locations.push((ledger_id, entry_id, log_state.log_id, position));
                    current.framed_entries.push(framed);
                    log_state.offset += encoded_size;
                    flush_bytes += encoded_size;
                }

                if !current.framed_entries.is_empty() {
                    segs.push(current);
                }
            } // log_state MutexGuard dropped

            segments = segs;
            total_bytes = flush_bytes;
        } // flushing RwLockReadGuard dropped

        // Phase 2: Async I/O (no non-Send guards held)
        let entrylog_start = Instant::now();
        let mut batch = Vec::with_capacity(entry_count);
        for segment in segments {
            self.ledger_io.write_batch(
                segment.fd,
                segment.write_offset,
                segment.framed_entries,
            ).await?;
            for (ledger_id, entry_id, log_id, position) in segment.locations {
                batch.push((ledger_id, entry_id,
                    EntryLocation::new(log_id, position as u32).to_u64()));
            }
        }
        storage_metrics::FLUSH_ENTRYLOG.observe(entrylog_start.elapsed().as_secs_f64());

        if !batch.is_empty() {
            self.entry_location_index.put_batch(&batch)?;
        }

        let locations_start = Instant::now();
        self.entry_location_index.flush()?;
        storage_metrics::FLUSH_LOCATIONS_INDEX.observe(locations_start.elapsed().as_secs_f64());

        // Persist in-memory LAC cache to RocksDB (batch write, not per-entry).
        // This replaces the per-add_entry RocksDB write with one batch per flush.
        let lac_entries: Vec<(i64, i64)> = self.lac_cache.iter()
            .map(|e| (*e.key(), *e.value()))
            .collect();
        for (ledger_id, lac) in &lac_entries {
            let _ = self.ledger_index.update_lac(*ledger_id, *lac);
        }

        let ledger_start = Instant::now();
        self.ledger_index.flush()?;
        storage_metrics::FLUSH_LEDGER_INDEX.observe(ledger_start.elapsed().as_secs_f64());

        self.write_cache_being_flushed.write().clear();

        storage_metrics::FLUSH_SIZE.observe(total_bytes as f64);
        storage_metrics::FLUSH.observe(flush_start.elapsed().as_secs_f64());
        Ok(())
    }

    async fn checkpoint(&self) -> Result<(), StorageError> {
        self.flush().await?;

        // Write lastMark to each ledger directory. This records the
        // current journal position so that:
        // 1. Crash recovery knows where to start replaying
        // 2. cleanup_journals knows which files are safe to delete
        // 3. A Java bookie restarting on this data dir finds a valid mark
        // Matches Java's checkpointComplete → rollLog → writeLogMark flow.
        if let Some(log_id) = self.journal.as_ref().and_then(|j| j.current_log_id()) {
            let mark = crate::journal::format::LastMark::new(log_id as i64, 0);
            // Write to the ledger directory's current/ subdir.
            // Java writes to ALL ledger dirs; we have one (base_dir).
            let mark_path = self.base_dir.join(CURRENT_DIR).join("lastMark");
            if let Err(e) = crate::checkpoint::write_last_mark(&mark_path, &mark) {
                tracing::warn!("Failed to write lastMark: {e}");
            }
        }

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
