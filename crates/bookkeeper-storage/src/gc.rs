use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::storage_metrics;

use crate::entry_log::format::{self as elf, EntryLogHeader, LOGFILE_HEADER_SIZE};
use crate::index::entry_log_meta_map::{EntryLogMeta, EntryLogMetadataMap};
use crate::ledger_storage::LedgerStorage;
use crate::StorageError;

/// GC configuration matching Java's ServerConfiguration GC knobs.
#[derive(Debug, Clone)]
pub struct GcConfig {
    pub minor_compaction_threshold: f64,
    pub major_compaction_threshold: f64,
    /// Max bytes/sec during compaction. 0 = unlimited.
    /// Matches Java's `compactionRateByBytes` / `isThrottleByBytes`.
    pub compaction_rate_bytes_per_sec: u64,
    /// Max time in ms for a single compaction cycle. 0 = unlimited.
    /// Matches Java's `minorCompactionMaxTimeMillis` / `majorCompactionMaxTimeMillis`.
    pub compaction_max_time_ms: u64,
    /// Minor compaction interval in seconds. 0 = disabled.
    pub minor_compaction_interval: i64,
    /// Major compaction interval in seconds. 0 = disabled.
    pub major_compaction_interval: i64,
    /// Max outstanding compaction requests per cycle.
    pub compaction_max_outstanding_requests: i32,
    /// Separate minor compaction max time in ms. -1 = use compaction_max_time_ms.
    pub minor_compaction_max_time_ms: i64,
    /// Separate major compaction max time in ms. -1 = use compaction_max_time_ms.
    pub major_compaction_max_time_ms: i64,
    /// Force GC even when disk is full.
    pub is_force_gc_allow_when_no_space: bool,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            minor_compaction_threshold: 0.2,
            major_compaction_threshold: 0.8,
            compaction_rate_bytes_per_sec: 1_000_000,
            compaction_max_time_ms: 0,
            minor_compaction_interval: 3600,
            major_compaction_interval: 86400,
            compaction_max_outstanding_requests: 10000,
            minor_compaction_max_time_ms: -1,
            major_compaction_max_time_ms: -1,
            is_force_gc_allow_when_no_space: false,
        }
    }
}

/// Garbage collector for BookKeeper storage.
///
/// Matches Java's GarbageCollectorThread + ScanAndCompareGarbageCollector:
/// 1. Compares local ledgers against a set of globally active ledgers
/// 2. Deletes local ledger data (index entries) for ledgers no longer in metadata
/// 3. Scans entry log files and deletes files where all ledgers are gone
/// 4. Compacts entry log files with low usage (minor/major thresholds)
pub struct GarbageCollector {
    storage: Arc<dyn LedgerStorage>,
    ledger_io: Arc<dyn crate::io_engine::LedgerIo>,
    config: GcConfig,
    /// Persistent metadata cache. `None` if disabled (falls back to per-cycle file reads).
    meta_map: Option<EntryLogMetadataMap>,
    /// Suspend flags matching Java's `suspendMajorGC` / `suspendMinorGC`.
    suspend_major: AtomicBool,
    suspend_minor: AtomicBool,
}


impl GarbageCollector {
    pub fn new(storage: Arc<dyn LedgerStorage>, ledger_io: Arc<dyn crate::io_engine::LedgerIo>) -> Self {
        Self::with_config(storage, ledger_io, GcConfig::default(), None)
    }

    pub fn with_config(
        storage: Arc<dyn LedgerStorage>,
        ledger_io: Arc<dyn crate::io_engine::LedgerIo>,
        config: GcConfig,
        meta_map: Option<EntryLogMetadataMap>,
    ) -> Self {
        Self {
            storage,
            ledger_io,
            config,
            meta_map,
            suspend_major: AtomicBool::new(false),
            suspend_minor: AtomicBool::new(false),
        }
    }

    pub fn suspend_major_gc(&self) { self.suspend_major.store(true, Ordering::Release); }
    pub fn resume_major_gc(&self) { self.suspend_major.store(false, Ordering::Release); }
    pub fn is_major_gc_suspended(&self) -> bool { self.suspend_major.load(Ordering::Acquire) }

    pub fn suspend_minor_gc(&self) { self.suspend_minor.store(true, Ordering::Release); }
    pub fn resume_minor_gc(&self) { self.suspend_minor.store(false, Ordering::Release); }
    pub fn is_minor_gc_suspended(&self) -> bool { self.suspend_minor.load(Ordering::Acquire) }

    /// Run one GC cycle.  `is_major_gc` controls the compaction threshold:
    /// - `false` (minor): only compact files with usage < `minor_compaction_threshold`
    /// - `true`  (major): compact files with usage < `major_compaction_threshold`
    ///
    /// Matches Java's GarbageCollectorThread which runs minor and major
    /// compaction on separate timers with different thresholds.
    pub async fn gc(&self, active_ledgers_in_metadata: &HashSet<i64>, is_major_gc: bool) -> Result<usize, StorageError> {
        let gc_start = Instant::now();

        // Step 1: Get all local ledgers (single scan, reused for both steps)
        let mut active_local: HashSet<i64> = self.storage.get_active_ledgers()?.into_iter().collect();

        // Step 2: Find and delete dead ledgers (local but not in metadata)
        let mut deleted_count = 0;
        let dead: Vec<i64> = active_local.iter()
            .filter(|id| !active_ledgers_in_metadata.contains(id))
            .copied().collect();
        for ledger_id in dead {
            if let Err(e) = self.storage.delete_ledger(ledger_id).await {
                tracing::warn!("GC: failed to delete ledger {ledger_id}: {e}");
            } else {
                active_local.remove(&ledger_id);
                deleted_count += 1;
            }
        }

        if deleted_count > 0 {
            storage_metrics::GC_DELETED_LEDGERS.inc_by(deleted_count as u64);
            tracing::info!("GC: deleted {deleted_count} ledger(s) from local storage");

            if let Err(e) = self.storage.compact_entry_locations() {
                tracing::warn!("GC: RocksDB compaction error: {e}");
            }
        }

        // Step 3: Clean up entry log files (delete empty, compact low-usage)
        if let Some(entry_log_dir) = self.storage.entry_log_dir() {
            let current_log_id = self.storage.current_log_id();

            if let Err(e) = self.gc_and_compact_entry_logs(entry_log_dir, current_log_id, &active_local, is_major_gc).await {
                tracing::warn!("GC: entry log cleanup error: {e}");
            }
        }

        storage_metrics::GC_RUN_DURATION.observe(gc_start.elapsed().as_secs_f64());
        Ok(deleted_count)
    }

    /// GC and compact entry log files.
    ///
    /// Matches Java's `GarbageCollectorThread.extractMetaFromEntryLogs()` +
    /// `doCompactEntryLogs()`:
    /// 1. Ensure all flushed entry logs have their metadata in the persistent cache
    /// 2. Remove inactive ledgers from cached metadata, delete empty logs
    /// 3. Compact log files with low usage
    async fn gc_and_compact_entry_logs(
        &self,
        entry_log_dir: &Path,
        current_log_id: Option<u32>,
        active_ledgers: &HashSet<i64>,
        is_major_gc: bool,
    ) -> Result<(), StorageError> {
        // Phase 1: Ensure every entry log file on disk has metadata in the
        // persistent cache. Matches Java's extractMetaFromEntryLogs().
        self.ensure_metadata_cached(entry_log_dir, current_log_id)?;

        // Clean stale cache entries for deleted log files
        if let Some(ref mm) = self.meta_map {
            let removed = mm.remove_stale(entry_log_dir)?;
            if removed > 0 {
                tracing::info!("GC: removed {removed} stale metadata cache entries");
            }
        }

        // Phase 2 & 3: Iterate entry logs, update metadata, delete/compact.
        let dir_entries = fs::read_dir(entry_log_dir)?;
        let mut deleted_files = 0;
        let mut compacted_files = 0;

        let compaction_deadline = if self.config.compaction_max_time_ms > 0 {
            Some(Instant::now() + Duration::from_millis(self.config.compaction_max_time_ms))
        } else {
            None
        };

        for dir_entry in dir_entries {
            if let Some(deadline) = compaction_deadline {
                if Instant::now() >= deadline {
                    tracing::info!("GC: compaction time limit reached ({}ms), deferring remaining logs",
                        self.config.compaction_max_time_ms);
                    break;
                }
            }

            let dir_entry = dir_entry?;
            let path = dir_entry.path();

            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) if n.ends_with(".log") => n.to_string(),
                _ => continue,
            };

            let log_id_str = name.trim_end_matches(".log");
            let log_id = match u32::from_str_radix(log_id_str, 16) {
                Ok(id) => id,
                Err(_) => continue,
            };

            // Skip the active log AND any log_id above the snapshot taken
            // at the start of this GC pass. Those higher ids are either
            // the active log we're about to write to or files produced
            // by a rotation that raced with our snapshot — both must
            // not be examined here. They'll be eligible on the next
            // pass once the writer has settled them.
            if let Some(skip_floor) = current_log_id {
                if log_id >= skip_floor {
                    continue;
                }
            }

            // Get metadata — prefer persistent cache, fall back to file read
            let mut meta = match self.get_or_extract_meta(&path, log_id) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("GC: error reading entry log {}: {e}", name);
                    continue;
                }
            };

            // Remove inactive ledgers from metadata and update cache
            let modified = meta.remove_inactive_ledgers(active_ledgers);

            if !meta.has_active_ledgers(active_ledgers) {
                // No active ledgers — delete the file
                self.ledger_io.evict_fd(entry_log_dir, log_id);
                if let Err(e) = fs::remove_file(&path) {
                    tracing::warn!("GC: failed to delete entry log {}: {e}", name);
                } else {
                    if let Some(ref mm) = self.meta_map {
                        let _ = mm.remove(log_id);
                    }
                    deleted_files += 1;
                }
            } else {
                // Persist updated metadata if ledgers were removed
                if modified {
                    if let Some(ref mm) = self.meta_map {
                        let _ = mm.put(&meta);
                    }
                }

                // Check suspend flags matching Java's suspendMajorGC / suspendMinorGC.
                if is_major_gc && self.suspend_major.load(Ordering::Acquire) {
                    continue;
                }
                if !is_major_gc && self.suspend_minor.load(Ordering::Acquire) {
                    continue;
                }

                let usage = meta.usage(active_ledgers);
                // Minor compaction: only compact files with very low usage
                // (< minor_compaction_threshold, default 0.2).
                // Major compaction: compact files with moderate usage
                // (< major_compaction_threshold, default 0.8).
                // Matches Java's GarbageCollectorThread which runs these on
                // separate timers with different thresholds.
                let compaction_threshold = if is_major_gc {
                    self.config.major_compaction_threshold
                } else {
                    self.config.minor_compaction_threshold
                };
                if usage < compaction_threshold {
                    match self.compact_entry_log(&path, log_id, active_ledgers).await {
                        Ok(entries_moved) => {
                            if entries_moved > 0 {
                                if let Some(ref mm) = self.meta_map {
                                    let _ = mm.remove(log_id);
                                }
                                compacted_files += 1;
                            }
                        }
                        Err(e) => {
                            tracing::warn!("GC: compaction error for {}: {e}", name);
                        }
                    }
                }
            }
        }

        if deleted_files > 0 {
            storage_metrics::GC_DELETED_ENTRY_LOGS.inc_by(deleted_files as u64);
            tracing::info!("GC: deleted {deleted_files} empty entry log file(s)");
        }
        if compacted_files > 0 {
            storage_metrics::GC_COMPACTED_ENTRY_LOGS.inc_by(compacted_files as u64);
            tracing::info!("GC: compacted {compacted_files} entry log file(s)");
        }

        Ok(())
    }

    /// Ensure all entry log files on disk have their metadata in the persistent
    /// cache. Matches Java's `extractMetaFromEntryLogs()` which iterates flushed
    /// log IDs and extracts metadata for any not yet cached.
    fn ensure_metadata_cached(
        &self,
        entry_log_dir: &Path,
        current_log_id: Option<u32>,
    ) -> Result<(), StorageError> {
        let Some(ref mm) = self.meta_map else { return Ok(()) };

        let dir_entries = fs::read_dir(entry_log_dir)?;
        for dir_entry in dir_entries.flatten() {
            let name = dir_entry.file_name();
            let Some(name_str) = name.to_str() else { continue };
            let Some(stem) = name_str.strip_suffix(".log") else { continue };
            let Ok(log_id) = u32::from_str_radix(stem, 16) else { continue };
            // Same skip-floor rule as `gc_and_compact_entry_logs`: don't
            // probe the active log nor any log_id above the snapshot,
            // since those may still be receiving writes.
            if let Some(skip_floor) = current_log_id {
                if log_id >= skip_floor {
                    continue;
                }
            }
            if mm.contains(log_id)? {
                continue;
            }
            // Not cached — extract from file and store
            let path = dir_entry.path();
            match self.extract_entry_log_meta_from_file(&path, log_id) {
                Ok(meta) => {
                    if let Err(e) = mm.put(&meta) {
                        tracing::warn!("GC: failed to cache metadata for {name_str}: {e}");
                    }
                }
                Err(e) => {
                    tracing::warn!("GC: error extracting metadata for {name_str}: {e}");
                }
            }
        }
        Ok(())
    }

    /// Get metadata from persistent cache, or extract from file and cache it.
    fn get_or_extract_meta(
        &self,
        path: &Path,
        log_id: u32,
    ) -> Result<EntryLogMeta, StorageError> {
        if let Some(ref mm) = self.meta_map {
            if let Some(cached) = mm.get(log_id)? {
                return Ok(cached);
            }
        }
        let meta = self.extract_entry_log_meta_from_file(path, log_id)?;
        if let Some(ref mm) = self.meta_map {
            let _ = mm.put(&meta);
        }
        Ok(meta)
    }

    /// Extract metadata (ledger→size map) from an entry log file.
    fn extract_entry_log_meta_from_file(
        &self,
        path: &Path,
        log_id: u32,
    ) -> Result<EntryLogMeta, StorageError> {
        let mut file = fs::File::open(path)?;
        let file_len = file.metadata()?.len();

        if file_len < LOGFILE_HEADER_SIZE as u64 {
            return Ok(EntryLogMeta::new(log_id));
        }

        let mut header_buf = vec![0u8; LOGFILE_HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let header = EntryLogHeader::from_bytes(&header_buf)?;

        let map_offset = header.ledgers_map_offset();
        if map_offset > 0 && (map_offset as u64) < file_len {
            if let Ok(meta) = self.read_ledger_map_as_meta(&mut file, map_offset as u64, log_id) {
                return Ok(meta);
            }
        }

        self.scan_entry_log_for_meta(&mut file, log_id)
    }

    /// Read ledger map from file offset and build metadata.
    fn read_ledger_map_as_meta(
        &self,
        file: &mut fs::File,
        offset: u64,
        log_id: u32,
    ) -> Result<EntryLogMeta, StorageError> {
        file.seek(SeekFrom::Start(offset))?;

        let mut header_buf = [0u8; elf::LEDGERS_MAP_HEADER_SIZE];
        file.read_exact(&mut header_buf)?;

        let _content_size = i32::from_be_bytes(header_buf[0..4].try_into().unwrap());
        let lid = i64::from_be_bytes(header_buf[4..12].try_into().unwrap());
        let map_id = i64::from_be_bytes(header_buf[12..20].try_into().unwrap());

        if lid != elf::INVALID_LID || map_id != elf::LEDGERS_MAP_ENTRY_ID {
            return Err(StorageError::CorruptData("invalid ledger map markers".into()));
        }

        let count = i32::from_be_bytes(header_buf[20..24].try_into().unwrap()) as usize;
        let mut ledgers = HashMap::with_capacity(count);
        let mut total_size: i64 = 0;

        for _ in 0..count {
            let mut entry_buf = [0u8; 16];
            file.read_exact(&mut entry_buf)?;
            let ledger_id = i64::from_be_bytes(entry_buf[0..8].try_into().unwrap());
            let size = i64::from_be_bytes(entry_buf[8..16].try_into().unwrap());
            ledgers.insert(ledger_id, size);
            total_size += size;
        }

        Ok(EntryLogMeta {
            log_id,
            ledgers,
            total_size,
        })
    }

    /// Scan entries to build metadata (fallback when ledger map is missing).
    fn scan_entry_log_for_meta(
        &self,
        file: &mut fs::File,
        log_id: u32,
    ) -> Result<EntryLogMeta, StorageError> {
        file.seek(SeekFrom::Start(LOGFILE_HEADER_SIZE as u64))?;
        let mut ledgers: HashMap<i64, i64> = HashMap::new();
        let mut total_size: i64 = 0;
        let mut pos_buf = [0u8; 4];

        loop {
            if file.read_exact(&mut pos_buf).is_err() {
                break;
            }
            let size = i32::from_be_bytes(pos_buf) as usize;
            if size < elf::MIN_SANE_ENTRY_SIZE || size > 100 * 1024 * 1024 {
                break;
            }
            let mut lid_buf = [0u8; 8];
            if file.read_exact(&mut lid_buf).is_err() {
                break;
            }
            let ledger_id = i64::from_be_bytes(lid_buf);
            if ledger_id == elf::INVALID_LID {
                break;
            }
            let entry_size = (4 + size) as i64;
            *ledgers.entry(ledger_id).or_insert(0) += entry_size;
            total_size += entry_size;
            if file.seek(SeekFrom::Current((size - 8) as i64)).is_err() {
                break;
            }
        }

        Ok(EntryLogMeta {
            log_id,
            ledgers,
            total_size,
        })
    }

    /// Compact an entry log file: copy active entries to a new log, update index, delete old.
    ///
    /// Matches Java's EntryLogCompactor.compact() flush ordering:
    /// 1. Scan entries in the old log
    /// 2. Write active entries to the current log (via write_entry_to_log)
    /// 3. Flush the entry log to ensure durability
    /// 4. Update the entry location index in batch and flush
    /// 5. Delete the old log file (AFTER index is consistent)
    async fn compact_entry_log(
        &self,
        path: &Path,
        log_id: u32,
        active_ledgers: &HashSet<i64>,
    ) -> Result<usize, StorageError> {
        let compact_start = Instant::now();
        let mut file = fs::File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < LOGFILE_HEADER_SIZE as u64 {
            return Ok(0);
        }

        // Step 1: Flush pending writes before compaction to avoid mixing
        self.storage.flush().await?;

        file.seek(SeekFrom::Start(LOGFILE_HEADER_SIZE as u64))?;

        let mut relocated: Vec<(i64, i64, u64)> = Vec::new();
        let mut pos_buf = [0u8; 4];

        // Rate limiter state — matches Java's AbstractLogCompactor.Throttler.
        // Tracks bytes written and sleeps when the rate exceeds the configured limit.
        let rate_limit = self.config.compaction_rate_bytes_per_sec;
        let compaction_start = Instant::now();
        let mut bytes_written: u64 = 0;

        // Step 2: Scan old log, write active entries to current log
        loop {
            if file.read_exact(&mut pos_buf).is_err() {
                break;
            }
            let size = i32::from_be_bytes(pos_buf) as usize;
            if size < elf::MIN_SANE_ENTRY_SIZE || size > 100 * 1024 * 1024 {
                break;
            }
            let mut entry = vec![0u8; size];
            if file.read_exact(&mut entry).is_err() {
                break;
            }
            let ledger_id = i64::from_be_bytes(entry[0..8].try_into().unwrap());
            if ledger_id == elf::INVALID_LID {
                break; // reached ledger map
            }

            // Only copy entries for active ledgers
            if !active_ledgers.contains(&ledger_id) {
                continue;
            }

            let entry_id = i64::from_be_bytes(entry[8..16].try_into().unwrap());

            // Rate limit: if we've written faster than allowed, sleep
            if rate_limit > 0 {
                bytes_written += (4 + size) as u64;
                let elapsed = compaction_start.elapsed();
                let expected = std::time::Duration::from_secs_f64(
                    bytes_written as f64 / rate_limit as f64,
                );
                if expected > elapsed {
                    tokio::time::sleep(expected - elapsed).await;
                }
            }

            // Write to current entry log
            let new_location = self.storage.write_entry_to_log(&entry).await?;
            relocated.push((ledger_id, entry_id, new_location));
        }

        let count = relocated.len();

        // Step 3 & 4: Update index BEFORE deleting old file.
        // Acquire the flush lock to prevent a concurrent flush from writing
        // stale pre-compaction locations. Matches Java's
        // updateEntriesLocations() which acquires flushMutex.
        if !relocated.is_empty() {
            self.storage.update_entry_locations_with_flush_lock(&relocated).await?;
        }

        // Step 5: Delete old log file AFTER index is updated.
        // If crash between step 4 and 5: old file remains, index points to new locations.
        // Old file will be cleaned up on next GC cycle (safe).
        self.ledger_io.evict_fd(path.parent().unwrap_or(path), log_id);
        fs::remove_file(path)?;

        if count > 0 {
            storage_metrics::GC_COMPACTED_ENTRIES.inc_by(count as u64);
        }
        storage_metrics::GC_COMPACTION_DURATION.observe(compact_start.elapsed().as_secs_f64());

        Ok(count)
    }
}
