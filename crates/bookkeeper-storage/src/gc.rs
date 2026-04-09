use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use crate::entry_log::format::{self as elf, EntryLogHeader, LOGFILE_HEADER_SIZE};
use crate::ledger_storage::LedgerStorage;
use crate::StorageError;

/// Default thresholds matching Java's ServerConfiguration.
const MINOR_COMPACTION_THRESHOLD: f64 = 0.2;
const MAJOR_COMPACTION_THRESHOLD: f64 = 0.8;

/// Entry log metadata: tracks which ledgers are in a log file and their sizes.
/// Matches Java's EntryLogMetadata.
struct EntryLogMeta {
    #[allow(dead_code)]
    log_id: u32,
    /// Map of ledger_id → size_in_bytes in this log file.
    ledgers: HashMap<i64, i64>,
    total_size: i64,
}

impl EntryLogMeta {
    /// Calculate usage ratio: remaining active data / total data.
    /// Matches Java's EntryLogMetadata.getUsage().
    fn usage(&self, active_ledgers: &HashSet<i64>) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        let remaining: i64 = self
            .ledgers
            .iter()
            .filter(|(lid, _)| active_ledgers.contains(lid))
            .map(|(_, size)| *size)
            .sum();
        remaining as f64 / self.total_size as f64
    }

    fn has_active_ledgers(&self, active_ledgers: &HashSet<i64>) -> bool {
        self.ledgers.keys().any(|lid| active_ledgers.contains(lid))
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
    #[allow(dead_code)]
    minor_threshold: f64,
    major_threshold: f64,
}


impl GarbageCollector {
    pub fn new(storage: Arc<dyn LedgerStorage>, ledger_io: Arc<dyn crate::io_engine::LedgerIo>) -> Self {
        Self {
            storage,
            ledger_io,
            minor_threshold: MINOR_COMPACTION_THRESHOLD,
            major_threshold: MAJOR_COMPACTION_THRESHOLD,
        }
    }

    /// Run one GC cycle.
    pub async fn gc(&self, active_ledgers_in_metadata: &HashSet<i64>) -> Result<usize, StorageError> {
        // Step 1: Get all local ledgers
        let local_ledgers = self.storage.get_active_ledgers()?;

        // Step 2: Find and delete dead ledgers (local but not in metadata)
        let mut deleted_count = 0;
        for &ledger_id in &local_ledgers {
            if !active_ledgers_in_metadata.contains(&ledger_id) {
                if let Err(e) = self.storage.delete_ledger(ledger_id) {
                    tracing::warn!("GC: failed to delete ledger {ledger_id}: {e}");
                } else {
                    deleted_count += 1;
                }
            }
        }

        if deleted_count > 0 {
            tracing::info!("GC: deleted {deleted_count} ledger(s) from local storage");

            if let Err(e) = self.storage.compact_entry_locations() {
                tracing::warn!("GC: RocksDB compaction error: {e}");
            }
        }

        // Step 3: Clean up entry log files (delete empty, compact low-usage)
        if let Some(entry_log_dir) = self.storage.entry_log_dir() {
            let current_log_id = self.storage.current_log_id();
            let active_local: HashSet<i64> = self
                .storage
                .get_active_ledgers()?
                .into_iter()
                .collect();

            if let Err(e) = self.gc_and_compact_entry_logs(entry_log_dir, current_log_id, &active_local).await {
                tracing::warn!("GC: entry log cleanup error: {e}");
            }
        }

        Ok(deleted_count)
    }

    /// GC and compact entry log files.
    ///
    /// Matches Java's doGcEntryLogs() + doCompactEntryLogs():
    /// 1. Delete entirely empty log files
    /// 2. Compact log files with usage below the major threshold
    async fn gc_and_compact_entry_logs(
        &self,
        entry_log_dir: &Path,
        current_log_id: Option<u32>,
        active_ledgers: &HashSet<i64>,
    ) -> Result<(), StorageError> {
        let dir_entries = fs::read_dir(entry_log_dir)?;
        let mut deleted_files = 0;
        let mut compacted_files = 0;

        for dir_entry in dir_entries {
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

            // Skip the current active log file
            if current_log_id == Some(log_id) {
                continue;
            }

            // Extract metadata for this log file
            let meta = match self.extract_entry_log_meta(&path, log_id) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("GC: error reading entry log {}: {e}", name);
                    continue;
                }
            };

            if !meta.has_active_ledgers(active_ledgers) {
                // No active ledgers — delete the file entirely
                self.ledger_io.evict_fd(entry_log_dir, log_id);
                if let Err(e) = fs::remove_file(&path) {
                    tracing::warn!("GC: failed to delete entry log {}: {e}", name);
                } else {
                    deleted_files += 1;
                }
            } else {
                // Check usage for compaction
                let usage = meta.usage(active_ledgers);
                if usage < self.major_threshold {
                    // Compact: copy live entries to current log, update index, delete old
                    match self.compact_entry_log(&path, log_id, active_ledgers).await {
                        Ok(entries_moved) => {
                            if entries_moved > 0 {
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
            tracing::info!("GC: deleted {deleted_files} empty entry log file(s)");
        }
        if compacted_files > 0 {
            tracing::info!("GC: compacted {compacted_files} entry log file(s)");
        }

        Ok(())
    }

    /// Extract metadata (ledger→size map) from an entry log file.
    fn extract_entry_log_meta(
        &self,
        path: &Path,
        log_id: u32,
    ) -> Result<EntryLogMeta, StorageError> {
        let mut file = fs::File::open(path)?;
        let file_len = file.metadata()?.len();

        if file_len < LOGFILE_HEADER_SIZE as u64 {
            return Ok(EntryLogMeta {
                log_id,
                ledgers: HashMap::new(),
                total_size: 0,
            });
        }

        let mut header_buf = vec![0u8; LOGFILE_HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let header = EntryLogHeader::from_bytes(&header_buf)?;

        // Try reading ledger map from the indexed location
        let map_offset = header.ledgers_map_offset();
        if map_offset > 0 && (map_offset as u64) < file_len {
            if let Ok(meta) = self.read_ledger_map_as_meta(&mut file, map_offset as u64, log_id) {
                return Ok(meta);
            }
        }

        // Fallback: scan entries
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
        _log_id: u32,
        active_ledgers: &HashSet<i64>,
    ) -> Result<usize, StorageError> {
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

            // Write to current entry log
            let new_location = self.storage.write_entry_to_log(&entry).await?;
            relocated.push((ledger_id, entry_id, new_location));
        }

        let count = relocated.len();

        // Step 3 & 4: Update index BEFORE deleting old file.
        // Matches Java's flush ordering: flush log → update index → flush index.
        // This ensures the index is consistent before the old file is removed.
        if !relocated.is_empty() {
            self.storage.update_entry_locations(&relocated)?;
        }

        // Step 5: Delete old log file AFTER index is updated.
        // If crash between step 4 and 5: old file remains, index points to new locations.
        // Old file will be cleaned up on next GC cycle (safe).
        self.ledger_io.evict_fd(path.parent().unwrap_or(path), _log_id);
        fs::remove_file(path)?;

        Ok(count)
    }
}
