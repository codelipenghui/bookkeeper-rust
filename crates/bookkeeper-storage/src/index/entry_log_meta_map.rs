use std::collections::HashMap;
use std::path::Path;
use rocksdb::{DB, IteratorMode, WriteBatch};
use crate::rocksdb_options;
use crate::StorageError;

/// Serialization version matching Java's `EntryLogMetadata.DEFAULT_SERIALIZATION_VERSION`.
const SERIALIZATION_VERSION: i16 = 1;

/// Persistent RocksDB-backed cache of entry log metadata (ledger→size maps).
///
/// Matches Java's `PersistentEntryLogMetadataMap`: stores per-log-file metadata
/// so that GC cycles don't need to re-read entry log files to determine which
/// ledgers they contain.
///
/// Key: `log_id` as 4 bytes big-endian u32.
/// Value: serialized `EntryLogMeta` (see `serialize` / `deserialize`).
pub struct EntryLogMetadataMap {
    db: DB,
}

/// Entry log metadata: which ledgers are in a log file and their sizes.
#[derive(Debug, Clone)]
pub struct EntryLogMeta {
    pub log_id: u32,
    pub ledgers: HashMap<i64, i64>,
    pub total_size: i64,
}

impl EntryLogMeta {
    pub fn new(log_id: u32) -> Self {
        Self { log_id, ledgers: HashMap::new(), total_size: 0 }
    }

    /// Calculate usage ratio: remaining active data / total data.
    pub fn usage(&self, active_ledgers: &std::collections::HashSet<i64>) -> f64 {
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

    pub fn has_active_ledgers(&self, active_ledgers: &std::collections::HashSet<i64>) -> bool {
        self.ledgers.keys().any(|lid| active_ledgers.contains(lid))
    }

    /// Remove ledgers not in the active set. Returns true if any were removed.
    pub fn remove_inactive_ledgers(&mut self, active_ledgers: &std::collections::HashSet<i64>) -> bool {
        let before = self.ledgers.len();
        self.ledgers.retain(|lid, _| active_ledgers.contains(lid));
        self.ledgers.len() != before
    }

    /// Serialize matching Java's `EntryLogMetadata.serialize()`:
    /// ```text
    /// 2-bytes: version
    /// 8-bytes: log_id (as i64)
    /// 8-bytes: total_size
    /// 8-bytes: remaining_size (same as total_size at write time)
    /// 8-bytes: ledger count
    /// [repeat]: (8-bytes: ledger_id, 8-bytes: size)
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        let cap = 2 + 8 + 8 + 8 + 8 + self.ledgers.len() * 16;
        let mut buf = Vec::with_capacity(cap);
        buf.extend_from_slice(&SERIALIZATION_VERSION.to_be_bytes());
        buf.extend_from_slice(&(self.log_id as i64).to_be_bytes());
        buf.extend_from_slice(&self.total_size.to_be_bytes());
        buf.extend_from_slice(&self.total_size.to_be_bytes()); // remaining_size
        buf.extend_from_slice(&(self.ledgers.len() as i64).to_be_bytes());
        for (&ledger_id, &size) in &self.ledgers {
            buf.extend_from_slice(&ledger_id.to_be_bytes());
            buf.extend_from_slice(&size.to_be_bytes());
        }
        buf
    }

    /// Deserialize matching Java's `EntryLogMetadata.deserialize()`.
    pub fn deserialize(data: &[u8]) -> Result<Self, StorageError> {
        if data.len() < 34 {
            return Err(StorageError::CorruptData("entry log meta too short".into()));
        }
        let mut pos = 0;
        let version = i16::from_be_bytes(data[pos..pos + 2].try_into().unwrap());
        pos += 2;
        if version != SERIALIZATION_VERSION {
            return Err(StorageError::CorruptData(
                format!("entry log meta version mismatch: expected {SERIALIZATION_VERSION}, got {version}"),
            ));
        }
        let log_id = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap()) as u32;
        pos += 8;
        let total_size = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let _remaining_size = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let count = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;

        let mut ledgers = HashMap::with_capacity(count);
        for _ in 0..count {
            if pos + 16 > data.len() {
                break;
            }
            let ledger_id = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let size = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            ledgers.insert(ledger_id, size);
        }

        Ok(Self { log_id, ledgers, total_size })
    }
}

fn encode_key(log_id: u32) -> [u8; 4] {
    log_id.to_be_bytes()
}

impl EntryLogMetadataMap {
    /// Open or create the persistent metadata map at `path`.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        std::fs::create_dir_all(path)?;
        let (opts, _cache) =
            rocksdb_options::load_or_default(None, rocksdb_options::DEFAULT_LEDGER_METADATA_CONF)?;
        let db = DB::open(&opts, path).map_err(|e| StorageError::RocksDb(e.to_string()))?;
        Ok(Self { db })
    }

    /// Check if metadata for a log file is cached.
    pub fn contains(&self, log_id: u32) -> Result<bool, StorageError> {
        let key = encode_key(log_id);
        self.db.get_pinned(&key)
            .map(|v| v.is_some())
            .map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    /// Get cached metadata for a log file.
    pub fn get(&self, log_id: u32) -> Result<Option<EntryLogMeta>, StorageError> {
        let key = encode_key(log_id);
        match self.db.get(&key).map_err(|e| StorageError::RocksDb(e.to_string()))? {
            Some(data) => Ok(Some(EntryLogMeta::deserialize(&data)?)),
            None => Ok(None),
        }
    }

    /// Store metadata for a log file.
    pub fn put(&self, meta: &EntryLogMeta) -> Result<(), StorageError> {
        let key = encode_key(meta.log_id);
        let value = meta.serialize();
        self.db.put(&key, &value).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    /// Remove metadata for a log file (called when the log is deleted or compacted).
    pub fn remove(&self, log_id: u32) -> Result<(), StorageError> {
        let key = encode_key(log_id);
        self.db.delete(&key).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    /// Iterate over all cached metadata entries.
    pub fn for_each<F>(&self, mut f: F) -> Result<(), StorageError>
    where
        F: FnMut(u32, EntryLogMeta),
    {
        let iter = self.db.iterator(IteratorMode::Start);
        for item in iter {
            let (key, value) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;
            if key.len() < 4 {
                continue;
            }
            let log_id = u32::from_be_bytes(key[..4].try_into().unwrap());
            if let Ok(meta) = EntryLogMeta::deserialize(&value) {
                f(log_id, meta);
            }
        }
        Ok(())
    }

    /// Remove entries whose log files no longer exist on disk.
    pub fn remove_stale(&self, entry_log_dir: &Path) -> Result<usize, StorageError> {
        let mut stale = Vec::new();
        let iter = self.db.iterator(IteratorMode::Start);
        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;
            if key.len() < 4 { continue; }
            let log_id = u32::from_be_bytes(key[..4].try_into().unwrap());
            let path = entry_log_dir.join(format!("{:x}.log", log_id));
            if !path.exists() {
                stale.push(log_id);
            }
        }
        let count = stale.len();
        if !stale.is_empty() {
            let mut batch = WriteBatch::default();
            for log_id in stale {
                batch.delete(encode_key(log_id));
            }
            self.db.write(batch).map_err(|e| StorageError::RocksDb(e.to_string()))?;
        }
        Ok(count)
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        self.db.flush().map_err(|e| StorageError::RocksDb(e.to_string()))
    }
}
