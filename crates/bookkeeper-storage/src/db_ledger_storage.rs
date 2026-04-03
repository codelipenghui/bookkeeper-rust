use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use bytes::Bytes;
use crate::cache::write_cache::WriteCache;
use crate::cache::read_cache::ReadCache;
use crate::entry_log::format::{self as elf, EntryLogHeader, EntryLocation, LOGFILE_HEADER_SIZE};
use crate::index::entry_location::EntryLocationIndex;
use crate::index::ledger_index::LedgerMetadataIndex;
use crate::journal::format::StorageError;
use crate::ledger_storage::LedgerStorage;

const DEFAULT_WRITE_CACHE_SIZE: usize = 64 * 1024 * 1024; // 64MB
const DEFAULT_READ_CACHE_SIZE: usize = 64 * 1024 * 1024;  // 64MB
const READ_CACHE_SEGMENTS: usize = 4;

pub struct DbLedgerStorage {
    #[allow(dead_code)]
    base_dir: PathBuf,
    write_cache: WriteCache,
    read_cache: ReadCache,
    entry_location_index: EntryLocationIndex,
    ledger_index: LedgerMetadataIndex,
    entry_log_dir: PathBuf,
    current_log_id: Mutex<u32>,
    current_log_offset: Mutex<u64>,
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
            write_cache: WriteCache::new(DEFAULT_WRITE_CACHE_SIZE),
            read_cache: ReadCache::new(DEFAULT_READ_CACHE_SIZE, READ_CACHE_SEGMENTS),
            entry_location_index,
            ledger_index,
            entry_log_dir,
            current_log_id: Mutex::new(0),
            current_log_offset: Mutex::new(LOGFILE_HEADER_SIZE as u64),
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

    fn write_entry_to_log(&self, entry: &[u8]) -> Result<u64, StorageError> {
        let log_id = self.current_log_id.lock().unwrap();
        let mut log_offset = self.current_log_offset.lock().unwrap();
        self.ensure_entry_log_exists(*log_id)?;

        let path = self.entry_log_path(*log_id);
        let encoded = elf::encode_entry_log_entry(entry);
        let mut file = fs::OpenOptions::new().create(true).append(true).open(&path)?;
        file.write_all(&encoded)?;

        // position points past the 4-byte size prefix to the actual entry data
        let position = *log_offset + 4;
        let location = EntryLocation::new(*log_id, position as u32).to_u64();
        *log_offset += encoded.len() as u64;

        Ok(location)
    }

    fn read_entry_from_log(&self, location: u64) -> Result<Vec<u8>, StorageError> {
        let loc = EntryLocation::from_u64(location);
        let path = self.entry_log_path(loc.log_id());
        let data = fs::read(&path)?;
        let pos = loc.position() as usize;
        if pos < 4 || pos > data.len() {
            return Err(StorageError::CorruptData("invalid entry position".into()));
        }
        // Read 4-byte size at (pos - 4), then read size bytes at pos
        let size_pos = pos - 4;
        let size = i32::from_be_bytes(data[size_pos..size_pos + 4].try_into().unwrap()) as usize;
        if pos + size > data.len() {
            return Err(StorageError::CorruptData("entry extends past end of log".into()));
        }
        Ok(data[pos..pos + size].to_vec())
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
            self.ledger_index.update_lac(ledger_id, lac).ok();
        }

        // Add to write cache
        self.write_cache.put(ledger_id, entry_id, entry)?;
        Ok(())
    }

    fn get_entry(&self, ledger_id: i64, entry_id: i64) -> Result<Bytes, StorageError> {
        // 1. Check read cache
        if let Some(data) = self.read_cache.get(ledger_id, entry_id) {
            return Ok(Bytes::from(data));
        }
        // 2. Check write cache
        if let Some(data) = self.write_cache.get(ledger_id, entry_id) {
            return Ok(Bytes::from(data));
        }
        // 3. Check RocksDB index → entry log file
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
        self.ledger_index.set(ledger_id, false, Some(master_key), None)
    }

    fn get_last_add_confirmed(&self, ledger_id: i64) -> Result<i64, StorageError> {
        // Check write cache first for the most recent entry's LAC field
        if let Some(entry_id) = self.write_cache.last_entry_id(ledger_id) {
            if let Some(data) = self.write_cache.get(ledger_id, entry_id) {
                if data.len() >= 24 {
                    return Ok(i64::from_be_bytes(data[16..24].try_into().unwrap()));
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
        let _lock = self.flush_mutex.lock().unwrap();
        let entries = self.write_cache.sorted_entries();
        let mut batch = Vec::new();
        for (ledger_id, entry_id, entry_data) in &entries {
            let location = self.write_entry_to_log(entry_data)?;
            batch.push((*ledger_id, *entry_id, location));
        }
        if !batch.is_empty() {
            self.entry_location_index.put_batch(&batch)?;
        }
        self.entry_location_index.flush()?;
        self.ledger_index.flush()?;
        self.write_cache.clear();
        Ok(())
    }

    fn checkpoint(&self) -> Result<(), StorageError> {
        self.flush()
    }
}
