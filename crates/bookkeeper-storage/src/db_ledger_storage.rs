use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use bytes::Bytes;
use parking_lot::RwLock;
use crate::cache::write_cache::WriteCache;
use crate::cache::read_cache::ReadCache;
use crate::entry_log::format::{self as elf, EntryLogHeader, EntryLocation, LOGFILE_HEADER_SIZE};
use crate::index::entry_location::EntryLocationIndex;
use crate::index::ledger_index::LedgerMetadataIndex;
use crate::StorageError;
use crate::ledger_storage::LedgerStorage;

const DEFAULT_WRITE_CACHE_SIZE: usize = 64 * 1024 * 1024; // 64MB
const DEFAULT_READ_CACHE_SIZE: usize = 64 * 1024 * 1024;  // 64MB
const READ_CACHE_SEGMENTS: usize = 4;
const MAX_ENTRY_LOG_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
const MAX_ENTRY_SIZE: usize = 100 * 1024 * 1024; // 100MB sanity limit

struct EntryLogState {
    log_id: u32,
    offset: u64,
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
}

impl DbLedgerStorage {
    pub fn open(base_dir: &Path) -> Result<Self, StorageError> {
        let locations_dir = base_dir.join("locations");
        let ledgers_dir = base_dir.join("ledgers");
        let entry_log_dir = base_dir.join("entry_logs");
        fs::create_dir_all(&locations_dir)?;
        fs::create_dir_all(&ledgers_dir)?;
        fs::create_dir_all(&entry_log_dir)?;

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
            }),
            flush_mutex: Mutex::new(()),
        })
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
        use std::io::{Read, Seek, SeekFrom};
        let loc = EntryLocation::from_u64(location);
        let path = self.entry_log_path(loc.log_id());
        let mut file = fs::File::open(&path)?;
        let pos = loc.position() as u64;
        if pos < 4 {
            return Err(StorageError::CorruptData("invalid entry position".into()));
        }
        // Read 4-byte size at (pos - 4)
        file.seek(SeekFrom::Start(pos - 4))?;
        let mut size_buf = [0u8; 4];
        file.read_exact(&mut size_buf)?;
        let size = i32::from_be_bytes(size_buf) as usize;
        if size > MAX_ENTRY_SIZE {
            return Err(StorageError::CorruptData(
                format!("entry size {} exceeds maximum {}", size, MAX_ENTRY_SIZE)));
        }
        // Read entry data at pos
        let mut entry = vec![0u8; size];
        file.read_exact(&mut entry)?;
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

        // Ensure ledger exists in index
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

    fn get_last_add_confirmed(&self, ledger_id: i64) -> Result<i64, StorageError> {
        // Check active write cache first for the most recent entry's LAC field
        {
            let cache = self.write_cache.read();
            if let Some(entry_id) = cache.last_entry_id(ledger_id) {
                if let Some(data) = cache.get(ledger_id, entry_id) {
                    if data.len() >= 24 {
                        return Ok(i64::from_be_bytes(data[16..24].try_into().unwrap()));
                    }
                }
            }
        }
        // Check write cache being flushed
        {
            let cache = self.write_cache_being_flushed.read();
            if let Some(entry_id) = cache.last_entry_id(ledger_id) {
                if let Some(data) = cache.get(ledger_id, entry_id) {
                    if data.len() >= 24 {
                        return Ok(i64::from_be_bytes(data[16..24].try_into().unwrap()));
                    }
                }
            }
        }
        // Fall back to RocksDB ledger index
        match self.ledger_index.get(ledger_id)? {
            Some(data) => Ok(data.lac.unwrap_or(-1)),
            None => Err(StorageError::LedgerNotFound(ledger_id)),
        }
    }

    fn set_explicit_lac(&self, ledger_id: i64, lac: i64) -> Result<(), StorageError> {
        self.ledger_index.update_lac(ledger_id, lac)
    }

    fn delete_ledger(&self, ledger_id: i64) -> Result<(), StorageError> {
        self.ledger_index.delete(ledger_id)?;
        self.entry_location_index.delete_ledger(ledger_id)?;
        Ok(())
    }

    fn flush(&self) -> Result<(), StorageError> {
        use std::io::BufWriter;
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
            self.ensure_entry_log_exists(log_state.log_id)?;
            let path = self.entry_log_path(log_state.log_id);
            let mut writer = BufWriter::new(
                fs::OpenOptions::new().create(true).append(true).open(&path)?
            );

            for &(ledger_id, entry_id, offset, size) in &entries {
                let entry_data = &flushing_data[offset..offset + size];
                let encoded_size = (4 + entry_data.len()) as u64;

                if log_state.offset + encoded_size > MAX_ENTRY_LOG_SIZE {
                    writer.flush()?;
                    drop(writer);
                    log_state.log_id += 1;
                    log_state.offset = LOGFILE_HEADER_SIZE as u64;
                    self.ensure_entry_log_exists(log_state.log_id)?;
                    let new_path = self.entry_log_path(log_state.log_id);
                    writer = BufWriter::new(
                        fs::OpenOptions::new().create(true).append(true).open(&new_path)?
                    );
                }

                writer.write_all(&(entry_data.len() as i32).to_be_bytes())?;
                writer.write_all(entry_data)?;
                let position = log_state.offset + 4;
                let location = EntryLocation::new(log_state.log_id, position as u32).to_u64();
                batch.push((ledger_id, entry_id, location));
                log_state.offset += encoded_size;
            }
            writer.flush()?;
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
        self.flush()
    }
}
