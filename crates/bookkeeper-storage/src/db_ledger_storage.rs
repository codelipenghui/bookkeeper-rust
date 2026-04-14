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

/// Constant-time byte comparison to prevent timing attacks on master key checks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Subdirectory under each ledger/journal base dir where all data lives.
/// Matches Java's `BookKeeperConstants.CURRENT_DIR = "current"`.
pub const CURRENT_DIR: &str = "current";

const DEFAULT_WRITE_CACHE_SIZE: usize = 256 * 1024 * 1024; // 256MB
const DEFAULT_READ_CACHE_SIZE: usize = 64 * 1024 * 1024;  // 64MB
const READ_CACHE_SEGMENTS: usize = 4;
const DEFAULT_MAX_ENTRY_LOG_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
const MAX_ENTRY_SIZE: usize = 100 * 1024 * 1024; // 100MB sanity limit
/// Number of consecutive entries to read ahead on a cache miss.
/// Matches Java's `dbStorage_readAheadCacheBatchSize` (default 100).
const DEFAULT_READ_AHEAD_BATCH_SIZE: usize = 100;
/// Max bytes to read ahead per cache miss.
/// Matches Java's `dbStorage_readAheadCacheBatchBytesSize` (default 256KB).
const DEFAULT_READ_AHEAD_MAX_BYTES: usize = 256 * 1024;

/// Configurable storage parameters. Bridges `ServerConfig` fields to
/// the storage layer so they aren't hardcoded constants.
#[derive(Clone)]
pub struct StorageConfig {
    pub write_cache_max_size: usize,
    pub read_cache_max_size: usize,
    pub max_entry_log_size: u64,
    pub read_ahead_batch_size: usize,
    pub read_ahead_max_bytes: usize,
    /// Max time a single `add_entry` caller will wait for a write-cache
    /// flush before rejecting with `OperationRejected`. Mirrors Java's
    /// `dbStorage_maxThrottleTimeMs`, used by `triggerFlushAndAddEntry`
    /// to bound backpressure instead of blocking Bookie IO threads
    /// indefinitely.
    pub max_throttle_time_ms: u64,
}

/// Matches Java's `DbLedgerStorage.MAX_THROTTLE_TIME_MILLIS` default (10s).
const DEFAULT_MAX_THROTTLE_TIME_MS: u64 = 10_000;

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            write_cache_max_size: DEFAULT_WRITE_CACHE_SIZE,
            read_cache_max_size: DEFAULT_READ_CACHE_SIZE,
            max_entry_log_size: DEFAULT_MAX_ENTRY_LOG_SIZE,
            read_ahead_batch_size: DEFAULT_READ_AHEAD_BATCH_SIZE,
            read_ahead_max_bytes: DEFAULT_READ_AHEAD_MAX_BYTES,
            max_throttle_time_ms: DEFAULT_MAX_THROTTLE_TIME_MS,
        }
    }
}

impl StorageConfig {
    /// Build from a `ServerConfig`, mapping the relevant fields.
    pub fn from_server_config(cfg: &bookkeeper_config::server::ServerConfig) -> Self {
        Self {
            write_cache_max_size: (cfg.db_storage_write_cache_max_size_mb as usize) * 1024 * 1024,
            read_cache_max_size: (cfg.db_storage_read_cache_max_size_mb as usize) * 1024 * 1024,
            max_entry_log_size: if cfg.log_size_limit > 0 {
                cfg.log_size_limit as u64
            } else {
                DEFAULT_MAX_ENTRY_LOG_SIZE
            },
            read_ahead_batch_size: if cfg.db_storage_read_ahead_cache_batch_size > 0 {
                cfg.db_storage_read_ahead_cache_batch_size as usize
            } else {
                DEFAULT_READ_AHEAD_BATCH_SIZE
            },
            read_ahead_max_bytes: if cfg.db_storage_read_ahead_cache_batch_bytes_size > 0 {
                cfg.db_storage_read_ahead_cache_batch_bytes_size as usize
            } else {
                DEFAULT_READ_AHEAD_MAX_BYTES
            },
            max_throttle_time_ms: if cfg.db_storage_max_throttle_time_ms > 0 {
                cfg.db_storage_max_throttle_time_ms as u64
            } else {
                DEFAULT_MAX_THROTTLE_TIME_MS
            },
        }
    }
}

struct EntryLogState {
    log_id: u32,
    offset: u64,
    /// Persistent file handle for the current entry log. Avoids open/close per write.
    /// Writes go through `LedgerIo::write_batch` using the raw fd from this handle.
    file: Option<fs::File>,
    /// Per-ledger bytes written into the current (still-open) entry log file.
    /// Drives the ledgers map that is appended at end-of-file on rotation,
    /// matching Java's `BufferedLogChannel.ledgersMap` maintained for
    /// `appendLedgersMap()`. Reset whenever a new log file is opened.
    current_log_ledger_sizes: std::collections::HashMap<i64, u64>,
}

/// In-memory snapshot of a ledger's metadata (master key + fenced flag).
/// Mirrors Java's `TransientLedgerInfo` pattern: keeps the hot `validate_for_write`
/// path off RocksDB after the first touch. `lac` and `explicit_lac` live in their
/// own caches — this struct only covers the fields read on every add_entry.
#[derive(Clone)]
struct LedgerMetaCacheEntry {
    fenced: bool,
    master_key: Option<Vec<u8>>,
}

pub struct DbLedgerStorage {
    #[allow(dead_code)]
    base_dir: PathBuf,
    write_cache: RwLock<WriteCache>,
    write_cache_being_flushed: RwLock<WriteCache>,
    read_cache: Arc<ReadCache>,
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
    /// Max old journal files to keep after checkpoint. Matches Java's `maxBackupJournals`.
    max_backup_journals: usize,
    /// In-memory LAC cache. Updated on every add_entry (in-memory only).
    /// Persisted to RocksDB during flush/checkpoint. Matches Java's
    /// TransientLedgerInfo which keeps LAC in memory and only writes to
    /// the ledger index during checkpoint.
    lac_cache: dashmap::DashMap<i64, i64>,
    /// In-memory cache of explicit LAC bodies (matching Java's TransientLedgerInfo).
    explicit_lac_cache: dashmap::DashMap<i64, Vec<u8>>,
    /// In-memory cache of ledger metadata (master key + fenced flag).
    /// Keeps the hot `validate_for_write` path off RocksDB after first touch.
    /// Mirrors Java's TransientLedgerInfo which caches per-ledger state in memory.
    meta_cache: dashmap::DashMap<i64, Arc<LedgerMetaCacheEntry>>,
    /// LAC watcher registry for long poll reads.
    lac_watchers: crate::lac_watcher::LacWatcherRegistry,
    /// Configurable storage parameters (cache sizes, read-ahead, log size).
    storage_config: StorageConfig,
}

impl DbLedgerStorage {
    pub fn open(base_dir: &Path, ledger_io: Arc<dyn crate::io_engine::LedgerIo>) -> Result<Self, StorageError> {
        Self::open_with_conf(base_dir, ledger_io, None, None)
    }

    /// Open the storage, optionally loading rocksdb tuning from the two
    /// Java BK-style INI files. Passing `None` for either falls back to
    /// the embedded defaults (`rocksdb_options::DEFAULT_*_CONF`).
    pub fn open_with_conf(
        base_dir: &Path,
        ledger_io: Arc<dyn crate::io_engine::LedgerIo>,
        entry_location_rocksdb_conf: Option<&Path>,
        ledger_metadata_rocksdb_conf: Option<&Path>,
    ) -> Result<Self, StorageError> {
        Self::open_full(base_dir, ledger_io, entry_location_rocksdb_conf, ledger_metadata_rocksdb_conf, StorageConfig::default())
    }

    /// Full constructor with all configuration options.
    pub fn open_full(
        base_dir: &Path,
        ledger_io: Arc<dyn crate::io_engine::LedgerIo>,
        entry_location_rocksdb_conf: Option<&Path>,
        ledger_metadata_rocksdb_conf: Option<&Path>,
        storage_config: StorageConfig,
    ) -> Result<Self, StorageError> {
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

        let entry_location_index =
            EntryLocationIndex::open(&locations_dir, entry_location_rocksdb_conf)?;
        let ledger_index =
            LedgerMetadataIndex::open(&ledgers_dir, ledger_metadata_rocksdb_conf)?;

        // CRITICAL: scan existing entry log files and start writing into a
        // brand-new file (max_existing + 1). Without this we'd reopen
        // `0.log` at offset = LOGFILE_HEADER_SIZE on every restart and
        // overwrite previously-written entries — readers using the
        // existing entry-location index would then get garbage bytes back
        // and fail digest verification. Matches Java's EntryLogger which
        // also rolls forward on startup.
        let initial_log_id = Self::next_entry_log_id(&entry_log_dir)?;

        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            write_cache: RwLock::new(WriteCache::new(storage_config.write_cache_max_size)),
            write_cache_being_flushed: RwLock::new(WriteCache::new(storage_config.write_cache_max_size)),
            read_cache: Arc::new(ReadCache::new(storage_config.read_cache_max_size, READ_CACHE_SEGMENTS)),
            entry_location_index,
            ledger_index,
            entry_log_dir,
            log_state: Mutex::new(EntryLogState {
                log_id: initial_log_id,
                offset: LOGFILE_HEADER_SIZE as u64,
                file: None,
                current_log_ledger_sizes: std::collections::HashMap::new(),
            }),
            flush_mutex: tokio::sync::Mutex::new(()),
            ledger_io,
            journal: None,
            journal_dir: None,
            max_backup_journals: 5,
            lac_cache: dashmap::DashMap::new(),
            explicit_lac_cache: dashmap::DashMap::new(),
            meta_cache: dashmap::DashMap::new(),
            lac_watchers: crate::lac_watcher::LacWatcherRegistry::new(),
            storage_config,
        })
    }

    /// Scan `entry_log_dir` for `<hex>.log` files and return the next
    /// log id to use (one past the highest existing id). Returns 0 when
    /// the directory is empty (i.e. fresh bookie).
    fn next_entry_log_id(entry_log_dir: &Path) -> Result<u32, StorageError> {
        let mut max_id: Option<u32> = None;
        let read_dir = match fs::read_dir(entry_log_dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e.into()),
        };
        for entry in read_dir {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name_str) = name.to_str() else { continue };
            let Some(stem) = name_str.strip_suffix(".log") else { continue };
            let Ok(id) = u32::from_str_radix(stem, 16) else { continue };
            max_id = Some(max_id.map_or(id, |m| m.max(id)));
        }
        Ok(max_id.map_or(0, |m| m + 1))
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
        #[allow(unused_variables)]
        skip_invalid_record: bool,
    ) -> Result<usize, StorageError> {
        // Read lastMark from the ledger directory. This tells us where
        // the last successful checkpoint was, so we only replay journal
        // entries from that point forward. Matches Java's replay logic.
        let mark_path = self.base_dir.join(CURRENT_DIR).join("lastMark");
        let last_mark = crate::checkpoint::read_last_mark(&mark_path).ok();

        // Replay existing journal entries (crash recovery)
        let replayed = crate::journal::writer::replay_journal_opt(journal_dir, last_mark, skip_invalid_record, &mut |entry_data| {
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
    /// Remove old journal files after checkpoint, keeping `max_backup_journals`
    /// files before the current one. Matches Java's `Journal.checkpointComplete()`
    /// which uses `maxBackupJournals` to retain a safety margin of old journals.
    fn cleanup_journals(&self) {
        let current_id = self.journal.as_ref()
            .and_then(|j| j.current_log_id());
        let Some(current_id) = current_id else { return; };

        if let Some(ref journal_dir) = self.journal_dir {
            let mut old_journals: Vec<(u64, std::path::PathBuf)> = Vec::new();
            if let Ok(entries) = fs::read_dir(journal_dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    if name.ends_with(".txn") {
                        if let Ok(id) = u64::from_str_radix(
                            name.trim_end_matches(".txn"), 16,
                        ) {
                            if id < current_id {
                                old_journals.push((id, entry.path()));
                            }
                        }
                    }
                }
            }
            // Sort descending so we keep the most recent N
            old_journals.sort_by(|a, b| b.0.cmp(&a.0));
            for (_id, path) in old_journals.iter().skip(self.max_backup_journals) {
                let _ = fs::remove_file(path);
            }
        }
    }

    /// Set the max number of old journal files to retain after checkpoint.
    pub fn set_max_backup_journals(&mut self, n: usize) {
        self.max_backup_journals = n;
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

    /// Append the ledgers map to a rotated entry-log file and patch the
    /// header so GC (and any other metadata consumer) can enumerate the
    /// ledgers in this file in O(num_ledgers) instead of O(file_size).
    ///
    /// On-disk layout (matches Java's `DefaultEntryLogger`):
    /// ```text
    ///   [ 1024-byte header  ][ user entries ... ][ ledgers map batches ]
    ///                                            ^ map_offset (u64 big-endian at byte 8)
    ///                                              + num_ledgers (i32 big-endian at byte 16)
    /// ```
    /// Each batch is `encode_ledger_map_batch()`:
    /// `[contentSize:4][INVALID_LID:8][LEDGERS_MAP_ENTRY_ID:8][count:4][(ledgerId,size)...]`
    /// with at most `LEDGERS_MAP_MAX_BATCH_SIZE` ledgers per batch to bound
    /// per-batch memory, matching Java's 10 000-ledger batch cap.
    fn append_ledgers_map(
        file: &mut fs::File,
        ledger_sizes: &std::collections::HashMap<i64, u64>,
    ) -> Result<(), StorageError> {
        use std::io::{Seek, SeekFrom, Write};
        use std::os::unix::fs::FileExt;

        let num_ledgers = ledger_sizes.len();
        if num_ledgers == 0 {
            return Ok(());
        }

        // Start writing at the current end-of-file.
        let map_offset = file.seek(SeekFrom::End(0))?;

        // Collect + sort entries so the on-disk layout is deterministic
        // (makes byte-for-byte reproducibility easier and matches Java's
        // iteration order for ledger maps via ConcurrentLongLongHashMap.forEach).
        let mut entries: Vec<elf::LedgerMapEntry> = ledger_sizes.iter()
            .map(|(&ledger_id, &size)| elf::LedgerMapEntry {
                ledger_id,
                size: size as i64,
            })
            .collect();
        entries.sort_by_key(|e| e.ledger_id);

        // Write in batches of at most LEDGERS_MAP_MAX_BATCH_SIZE ledgers.
        for chunk in entries.chunks(elf::LEDGERS_MAP_MAX_BATCH_SIZE) {
            let batch = elf::encode_ledger_map_batch(chunk);
            file.write_all(&batch)?;
        }
        // Flush map contents to the FS before we publish the pointer in
        // the header — otherwise a crash could leave a header pointing at
        // unwritten / partial map data. Mirrors Java's `super.flush();`
        // call in `appendLedgersMap` before the header patch.
        file.sync_data()?;

        // Patch the header in place: map_offset (8 bytes) + num_ledgers (4 bytes).
        // Offsets inside the header: magic(4) + version(4) = 8, then
        // ledgers_map_offset(8) + ledgers_count(4). Same layout as
        // `EntryLogHeader::to_bytes` above.
        let mut patch = [0u8; 12];
        patch[0..8].copy_from_slice(&(map_offset as i64).to_be_bytes());
        patch[8..12].copy_from_slice(&(num_ledgers as i32).to_be_bytes());
        file.write_all_at(&patch, 8)?;
        file.sync_data()?;

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

        // Ensure ledger exists in index (no-op if validate_for_write already created it).
        // Cache hit means RocksDB already has the row — skip the read entirely.
        if !self.meta_cache.contains_key(&ledger_id)
            && self.ledger_index.get(ledger_id)?.is_none()
        {
            self.ledger_index.set(ledger_id, false, None, None)?;
            self.meta_cache.insert(
                ledger_id,
                Arc::new(LedgerMetaCacheEntry { fenced: false, master_key: None }),
            );
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

        // Try write cache. Use try_read() first to detect lock contention
        // from concurrent flush cache swap (write lock).
        {
            let cache = match self.write_cache.try_read() {
                Some(guard) => guard,
                None => {
                    // Read lock blocked — a flush cache swap is in progress.
                    storage_metrics::ADD_ENTRY_BLOCKED_BY_FLUSH.inc();
                    self.write_cache.read()
                }
            };
            if cache.put(ledger_id, entry_id, entry).is_ok() {
                storage_metrics::ADD_ENTRY.observe(add_start.elapsed().as_secs_f64());
                return Ok(());
            }
        }
        // Cache full — flush and retry with a bounded throttle window.
        //
        // Matches Java's `triggerFlushAndAddEntry`: rather than blocking
        // the caller indefinitely on a slow flush, we enforce a total
        // `max_throttle_time_ms` budget. If we can't make room in that
        // window, the request is rejected with `OperationRejected` so the
        // client retries or fails fast instead of piling up stalled tasks.
        storage_metrics::THROTTLED_WRITE_REQUESTS.inc();
        let throttle_start = Instant::now();
        let max_throttle = std::time::Duration::from_millis(
            self.storage_config.max_throttle_time_ms,
        );

        // Inner retry loop: re-try the cache put between flushes in case
        // a concurrent flush rotated the cache while we were waiting.
        // Java's version also loops on `writeCache.put()` with a sleep.
        loop {
            // Quick re-check: a concurrent flush may have rotated the
            // cache already, so try the put again before starting work.
            {
                let cache = self.write_cache.read();
                if cache.put(ledger_id, entry_id, entry).is_ok() {
                    storage_metrics::THROTTLED_WRITE.observe(throttle_start.elapsed().as_secs_f64());
                    storage_metrics::ADD_ENTRY.observe(add_start.elapsed().as_secs_f64());
                    return Ok(());
                }
            }

            let remaining = max_throttle.saturating_sub(throttle_start.elapsed());
            if remaining.is_zero() {
                storage_metrics::REJECTED_WRITE_REQUESTS.inc();
                return Err(StorageError::OperationRejected);
            }

            // Call flush with a remaining-time deadline. If a peer flush
            // is in progress holding `flush_mutex`, the timeout covers the
            // mutex wait too — so a single slow flush can no longer stall
            // every in-flight add_entry for unbounded time.
            //
            // Cancelling an in-flight flush is safe here: the cache swap
            // is atomic under a parking_lot write lock, and all
            // write_cache_being_flushed mutations are re-done idempotently
            // by a subsequent flush (entries stay in being_flushed until a
            // successful flush clears them — see the bottom of flush()).
            match tokio::time::timeout(remaining, self.flush()).await {
                Ok(Ok(())) => {
                    // Flush succeeded — one more put attempt in next loop iter.
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    // Timed out — reject so the client can retry/failover.
                    storage_metrics::REJECTED_WRITE_REQUESTS.inc();
                    return Err(StorageError::OperationRejected);
                }
            }
        }
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

            // Read-ahead: collect index locations synchronously (fast RocksDB
            // point reads), then spawn the entry log I/O as a background task
            // so the caller gets their entry immediately.
            // Matches Java's DbLedgerStorage readAheadCacheBatchSize behavior.
            let mut ahead_locs: Vec<(i64, u64)> = Vec::new();
            let mut ahead_bytes = data.len();
            let ra_batch = self.storage_config.read_ahead_batch_size;
            let ra_max_bytes = self.storage_config.read_ahead_max_bytes;
            for ahead_eid in (entry_id + 1)..(entry_id + 1 + ra_batch as i64) {
                if ahead_bytes >= ra_max_bytes {
                    break;
                }
                if self.read_cache.get(ledger_id, ahead_eid).is_some() {
                    continue;
                }
                match self.entry_location_index.get(ledger_id, ahead_eid) {
                    Ok(Some(loc)) => {
                        ahead_locs.push((ahead_eid, loc));
                        // Estimate ~4KB per entry for the byte budget
                        ahead_bytes += 4096;
                    }
                    _ => break,
                }
            }

            if !ahead_locs.is_empty() {
                let cache = Arc::clone(&self.read_cache);
                let io = Arc::clone(&self.ledger_io);
                let dir = self.entry_log_dir.clone();
                tokio::spawn(async move {
                    let mut total = 0usize;
                    for (eid, loc_u64) in ahead_locs {
                        if total >= ra_max_bytes {
                            break;
                        }
                        let loc = EntryLocation::from_u64(loc_u64);
                        let pos = loc.position() as u64;
                        if pos < 4 {
                            break;
                        }
                        match io.read_framed_entry(&dir, loc.log_id(), pos - 4, MAX_ENTRY_SIZE).await {
                            Ok(ahead_data) => {
                                total += ahead_data.len();
                                cache.put(ledger_id, eid, &ahead_data);
                            }
                            Err(_) => break,
                        }
                    }
                });
            }

            storage_metrics::READ_ENTRY.observe(read_start.elapsed().as_secs_f64());
            return Ok(Bytes::from(data));
        }
        storage_metrics::READ_ENTRY.observe(read_start.elapsed().as_secs_f64());
        Err(StorageError::EntryNotFound { ledger_id, entry_id })
    }

    fn ledger_exists(&self, ledger_id: i64) -> Result<bool, StorageError> {
        if self.meta_cache.contains_key(&ledger_id) {
            return Ok(true);
        }
        Ok(self.ledger_index.get(ledger_id)?.is_some())
    }

    fn set_fenced(&self, ledger_id: i64) -> Result<bool, StorageError> {
        // Update the in-memory cache BEFORE persisting to RocksDB so that
        // concurrent validate_for_write calls see the fenced bit immediately.
        // A brief false-positive fence (cache says fenced, RocksDB write
        // hasn't completed yet) is safe — it just rejects a write that would
        // have been rejected moments later anyway. The alternative (RocksDB
        // first) leaves a window where writes succeed on a fencing ledger.
        let master_key = self
            .meta_cache
            .get(&ledger_id)
            .and_then(|e| e.master_key.clone());
        self.meta_cache.insert(
            ledger_id,
            Arc::new(LedgerMetaCacheEntry { fenced: true, master_key }),
        );
        self.ledger_index.set_fenced(ledger_id)?;
        Ok(true)
    }

    fn is_fenced(&self, ledger_id: i64) -> Result<bool, StorageError> {
        if let Some(meta) = self.meta_cache.get(&ledger_id) {
            return Ok(meta.fenced);
        }
        Ok(self.ledger_index.get(ledger_id)?.map_or(false, |d| d.fenced))
    }

    fn set_master_key(&self, ledger_id: i64, master_key: &[u8]) -> Result<(), StorageError> {
        let (fenced, existing_lac) = match self.ledger_index.get(ledger_id)? {
            Some(existing) => (existing.fenced, existing.lac),
            None => (false, None),
        };
        self.ledger_index.set(ledger_id, fenced, Some(master_key), existing_lac)?;
        self.meta_cache.insert(
            ledger_id,
            Arc::new(LedgerMetaCacheEntry {
                fenced,
                master_key: Some(master_key.to_vec()),
            }),
        );
        Ok(())
    }

    fn check_master_key(&self, ledger_id: i64, master_key: &[u8]) -> Result<(), StorageError> {
        // Fast path: cached ledger with a master key already set.
        if let Some(meta) = self.meta_cache.get(&ledger_id) {
            if let Some(stored) = &meta.master_key {
                if !constant_time_eq(stored.as_slice(), master_key) {
                    return Err(StorageError::Unauthorized(ledger_id));
                }
                return Ok(());
            }
            // Cached but master key not yet set — fall through to store it.
        }
        match self.ledger_index.get(ledger_id)? {
            Some(data) => {
                if let Some(stored_key) = &data.master_key {
                    if !constant_time_eq(stored_key, master_key) {
                        return Err(StorageError::Unauthorized(ledger_id));
                    }
                    self.meta_cache.insert(
                        ledger_id,
                        Arc::new(LedgerMetaCacheEntry {
                            fenced: data.fenced,
                            master_key: Some(stored_key.clone()),
                        }),
                    );
                    return Ok(());
                }
                self.set_master_key(ledger_id, master_key)
            }
            None => {
                self.ledger_index.set(ledger_id, false, Some(master_key), None)?;
                self.meta_cache.insert(
                    ledger_id,
                    Arc::new(LedgerMetaCacheEntry {
                        fenced: false,
                        master_key: Some(master_key.to_vec()),
                    }),
                );
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
        let encoded_size = (4 + entry.len()) as u64;

        // Reserve offset atomically under the lock. The offset is advanced
        // *before* the I/O so that a concurrent caller gets a distinct
        // position. If the write fails we accept a gap in the log — this
        // is the same trade-off Java makes in SingleDirectoryDbLedgerStorage.
        let (fd, write_offset, log_id, _file_guard) = {
            let mut log_state = self.log_state.lock().unwrap();

            if log_state.offset + encoded_size > self.storage_config.max_entry_log_size {
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
            // Clone to keep the fd valid through the async write phase
            let file_guard = log_state.file.as_ref().unwrap().try_clone()?;
            let write_offset = log_state.offset;
            let log_id = log_state.log_id;

            // Reserve the space atomically before releasing the lock
            log_state.offset += encoded_size;

            (fd, write_offset, log_id, file_guard)
        };

        let framed = Self::frame_entry(entry);
        self.ledger_io.write_batch(fd, write_offset, vec![framed]).await?;

        let position = write_offset + 4;
        let location = EntryLocation::new(log_id, position as u32).to_u64();
        Ok(location)
    }

    fn update_entry_locations(&self, batch: &[(i64, i64, u64)]) -> Result<(), StorageError> {
        // No per-entry flush needed — write_batch already wrote to disk.
        // Just update the RocksDB index.
        self.entry_location_index.put_batch(batch)?;
        self.entry_location_index.flush()
    }

    async fn update_entry_locations_with_flush_lock(
        &self,
        batch: &[(i64, i64, u64)],
    ) -> Result<(), StorageError> {
        // Acquire the flush mutex to prevent a concurrent flush from writing
        // stale pre-compaction locations to the index.
        // Matches Java's SingleDirectoryDbLedgerStorage.updateEntriesLocations()
        // which acquires flushMutex before calling entryLocationIndex.updateLocations().
        let _lock = self.flush_mutex.lock().await;
        self.entry_location_index.put_batch(batch)?;
        self.entry_location_index.flush()
    }

    fn delete_ledger(&self, ledger_id: i64) -> Result<(), StorageError> {
        // Notify long poll watchers that this ledger is gone
        self.lac_watchers.notify_ledger_closed(ledger_id);
        self.ledger_index.delete(ledger_id)?;
        self.entry_location_index.delete_ledger(ledger_id)?;
        self.meta_cache.remove(&ledger_id);
        self.lac_cache.remove(&ledger_id);
        self.explicit_lac_cache.remove(&ledger_id);
        Ok(())
    }

    async fn force_journal_sync(&self) -> Result<(), StorageError> {
        if let Some(journal) = self.journal.as_ref() {
            journal.force_sync().await?;
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), StorageError> {
        let flush_start = Instant::now();
        let _lock = self.flush_mutex.lock().await;

        // Swap: active → being_flushed, being_flushed (cleared) → active.
        // The write lock briefly blocks all concurrent add_entry() read-lock attempts.
        {
            let swap_start = Instant::now();
            let mut active = self.write_cache.write();
            let mut flushing = self.write_cache_being_flushed.write();
            std::mem::swap(&mut *active, &mut *flushing);
            storage_metrics::FLUSH_CACHE_SWAP.observe(swap_start.elapsed().as_secs_f64());
        }

        // Phase 1: Under log_state mutex, collect all framed entries and metadata.
        // We cannot hold non-Send guards (MutexGuard, RwLockReadGuard) across
        // an .await point, so we read from the flushing cache, frame entries,
        // and drop all guards before the async I/O phase.
        struct FlushSegment {
            fd: RawFd,
            /// Keeps the file handle alive so `fd` remains valid through the
            /// async write phase. Without this, log rotation drops the File
            /// and invalidates the fd (bad file descriptor).
            _file_guard: fs::File,
            write_offset: u64,
            framed_entries: Vec<Vec<u8>>,
            locations: Vec<(i64, i64, u32, u64)>, // (ledger_id, entry_id, log_id, position)
        }
        /// A log file that was rotated away mid-flush. Carries the ledger→bytes
        /// map built while the file was active, so Phase 2 can append the
        /// self-describing ledgers map (matching Java's `appendLedgersMap()`
        /// in `DefaultEntryLogger.BufferedLogChannel`). Without this, GC has
        /// to scan the whole log body to discover which ledgers it contains.
        struct RotatedLog {
            log_id: u32,
            file: fs::File,
            ledger_sizes: std::collections::HashMap<i64, u64>,
        }
        let segments: Vec<FlushSegment>;
        let rotated_logs: Vec<RotatedLog>;
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
            let mut rotated: Vec<RotatedLog> = Vec::new();
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

                let file_ref = log_state.file.as_ref().unwrap();
                let mut current = FlushSegment {
                    fd: file_ref.as_raw_fd(),
                    _file_guard: file_ref.try_clone()?,
                    write_offset: log_state.offset,
                    framed_entries: Vec::new(),
                    locations: Vec::new(),
                };

                for &(ledger_id, entry_id, offset, size) in &entries {
                    let entry_data = &flushing_data[offset..offset + size];
                    let encoded_size = (4 + entry_data.len()) as u64;

                    if log_state.offset + encoded_size > self.storage_config.max_entry_log_size {
                        // Push the current segment if it has entries
                        if !current.framed_entries.is_empty() {
                            segs.push(current);
                        }

                        // Rotate: hand off the old file + ledger-size map
                        // to Phase 2 so it can write the ledgers map after
                        // the entry writes land. This mirrors Java's
                        // `EntryLogManagerBase.createNewLog` which calls
                        // `logChannel.appendLedgersMap()` before swapping
                        // in a fresh log channel.
                        let old_log_id = log_state.log_id;
                        let old_file = log_state.file.take().unwrap();
                        let old_sizes = std::mem::take(&mut log_state.current_log_ledger_sizes);
                        rotated.push(RotatedLog {
                            log_id: old_log_id,
                            file: old_file,
                            ledger_sizes: old_sizes,
                        });

                        log_state.log_id += 1;
                        log_state.offset = LOGFILE_HEADER_SIZE as u64;
                        self.ensure_entry_log_exists(log_state.log_id)?;
                        let new_path = self.entry_log_path(log_state.log_id);
                        let file = fs::OpenOptions::new().read(true).write(true).open(&new_path)?;
                        log_state.file = Some(file);

                        let new_file_ref = log_state.file.as_ref().unwrap();
                        current = FlushSegment {
                            fd: new_file_ref.as_raw_fd(),
                            _file_guard: new_file_ref.try_clone()?,
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
                    // Track per-ledger bytes for the currently-open log so
                    // its ledgers map is accurate at rotation time.
                    *log_state.current_log_ledger_sizes
                        .entry(ledger_id).or_insert(0) += encoded_size;
                    flush_bytes += encoded_size;
                }

                if !current.framed_entries.is_empty() {
                    segs.push(current);
                }
            } // log_state MutexGuard dropped

            segments = segs;
            rotated_logs = rotated;
            total_bytes = flush_bytes;
        } // flushing RwLockReadGuard dropped

        // Phase 2: Async I/O (no non-Send guards held)
        let entrylog_start = Instant::now();
        let mut batch = Vec::with_capacity(entry_count);
        let mut file_guards: Vec<fs::File> = Vec::new();
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
            file_guards.push(segment._file_guard);
        }

        // Fsync all entry log files BEFORE updating the RocksDB index.
        // Without this, a crash after the index update but before the
        // entry log data reaches disk would leave the index pointing
        // to unwritten/partial data — corrupting reads after recovery.
        for guard in &file_guards {
            guard.sync_data()?;
        }

        // Append the ledgers map to every log file that rotated during this
        // flush. This makes rotated log files self-describing so GC can
        // enumerate their ledgers without scanning the whole body.
        // Matches Java's `DefaultEntryLogger.BufferedLogChannel.appendLedgersMap()`,
        // invoked from `EntryLogManagerBase.createNewLog` before rolling
        // over to a new log file.
        for mut rl in rotated_logs {
            if let Err(e) = Self::append_ledgers_map(&mut rl.file, &rl.ledger_sizes) {
                // Non-fatal: the file is still readable, GC just falls back
                // to scanning the body as it does for pre-existing files.
                tracing::warn!("failed to append ledgers map for log {:x}: {e}", rl.log_id);
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
        // Two-phase checkpoint protocol matching Java's CheckpointSource:
        // 1. Snapshot journal position BEFORE flush
        // 2. Flush write cache + entry logs + indexes
        // 3. Only persist lastMark AFTER flush succeeds
        //
        // If the flush fails at any point, lastMark stays at the
        // previous safe position — crash recovery replays from there.
        // Snapshot the durable (log_id, offset) mark — Java's
        // `Journal.newCheckpoint()` / `LastLogMark.markLog()`. Using the
        // actual synced offset (not 0) means crash recovery replays only
        // entries written after this point instead of rescanning the whole
        // journal file.
        let cp = self.journal.as_ref()
            .and_then(|j| j.current_mark())
            .map(|(log_id, offset)| {
                crate::checkpoint::Checkpoint::new(log_id as i64, offset as i64)
            });

        // Phase 2: Flush everything
        self.flush().await?;

        // Phase 3: Persist lastMark only after successful flush.
        // Matches Java's `journal.checkpointComplete(cp, true)`.
        if let Some(cp) = cp {
            if let Err(e) = cp.complete(&self.base_dir) {
                tracing::warn!("Failed to write lastMark: {e}");
            }
        }

        self.cleanup_journals();
        Ok(())
    }

    fn validate_for_write(&self, ledger_id: i64, master_key: &[u8]) -> Result<bool, StorageError> {
        // Fast path: cache hit with master key already set.
        // Skips RocksDB entirely on the hot add_entry path — this was ~20% of CPU
        // before, since every request re-read the ledger metadata row.
        if let Some(meta) = self.meta_cache.get(&ledger_id) {
            if let Some(stored) = &meta.master_key {
                if stored.as_slice() != master_key {
                    return Err(StorageError::Unauthorized(ledger_id));
                }
                return Ok(meta.fenced);
            }
            // Cached entry with no master key — fall through so we store one.
        }
        // Slow path: first write to this ledger since process start. Read RocksDB
        // once, then populate the cache so subsequent writes take the fast path.
        match self.ledger_index.get(ledger_id)? {
            Some(data) => {
                if let Some(stored_key) = &data.master_key {
                    if !constant_time_eq(stored_key, master_key) {
                        return Err(StorageError::Unauthorized(ledger_id));
                    }
                    self.meta_cache.insert(
                        ledger_id,
                        Arc::new(LedgerMetaCacheEntry {
                            fenced: data.fenced,
                            master_key: Some(stored_key.clone()),
                        }),
                    );
                } else {
                    // No key stored — store it (preserving existing fenced/lac)
                    self.ledger_index.set(ledger_id, data.fenced, Some(master_key), data.lac)?;
                    self.meta_cache.insert(
                        ledger_id,
                        Arc::new(LedgerMetaCacheEntry {
                            fenced: data.fenced,
                            master_key: Some(master_key.to_vec()),
                        }),
                    );
                }
                Ok(data.fenced)
            }
            None => {
                // Create new ledger with key
                self.ledger_index.set(ledger_id, false, Some(master_key), None)?;
                self.meta_cache.insert(
                    ledger_id,
                    Arc::new(LedgerMetaCacheEntry {
                        fenced: false,
                        master_key: Some(master_key.to_vec()),
                    }),
                );
                Ok(false)
            }
        }
    }

    fn get_entries_of_ledger(&self, ledger_id: i64) -> Result<Vec<i64>, StorageError> {
        let entries = self.entry_location_index.iter_ledger_entries(ledger_id)?;
        Ok(entries.into_iter().map(|(eid, _)| eid).collect())
    }

    async fn local_consistency_check(&self, rate_limit: u64) -> Result<usize, StorageError> {
        let ledgers = self.ledger_index.list_all_ledgers()?;
        let mut errors = 0usize;
        let mut checked = 0u64;
        let rate_interval = if rate_limit > 0 {
            Some(std::time::Duration::from_secs_f64(1.0 / rate_limit as f64))
        } else {
            None
        };
        let mut last_check = std::time::Instant::now();

        for ledger_id in &ledgers {
            let entries = match self.entry_location_index.iter_ledger_entries(*ledger_id) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("scrub: failed to iterate entries for ledger {ledger_id}: {e}");
                    errors += 1;
                    continue;
                }
            };

            for (entry_id, location) in &entries {
                // Rate limiting
                if let Some(interval) = rate_interval {
                    let elapsed = last_check.elapsed();
                    if elapsed < interval {
                        tokio::time::sleep(interval - elapsed).await;
                    }
                    last_check = std::time::Instant::now();
                }

                let loc = elf::EntryLocation::from_u64(*location);
                let log_id = loc.log_id();
                // Position in the entry log (location points to the entry data,
                // which starts 4 bytes after the size prefix)
                let pos = loc.position() as u64;
                let read_offset = pos - 4; // include the size prefix

                match self.ledger_io.read_framed_entry(
                    &self.entry_log_dir, log_id, read_offset, MAX_ENTRY_SIZE,
                ).await {
                    Ok(data) => {
                        // Verify ledger_id and entry_id in the entry header
                        if data.len() < 16 {
                            tracing::warn!(
                                "scrub: entry too short: ledger={ledger_id}, entry={entry_id}, \
                                 log={log_id}, size={}",
                                data.len()
                            );
                            errors += 1;
                            continue;
                        }
                        let stored_lid = i64::from_be_bytes(data[0..8].try_into().unwrap());
                        let stored_eid = i64::from_be_bytes(data[8..16].try_into().unwrap());
                        if stored_lid != *ledger_id || stored_eid != *entry_id {
                            tracing::warn!(
                                "scrub: ID mismatch at log={log_id} pos={pos}: \
                                 expected ({ledger_id},{entry_id}), found ({stored_lid},{stored_eid})"
                            );
                            errors += 1;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "scrub: failed to read entry: ledger={ledger_id}, entry={entry_id}, \
                             log={log_id}, pos={pos}: {e}"
                        );
                        errors += 1;
                    }
                }
                checked += 1;
            }
        }
        tracing::info!("scrub: checked {checked} entries across {} ledgers, {errors} errors", ledgers.len());
        Ok(errors)
    }
}
