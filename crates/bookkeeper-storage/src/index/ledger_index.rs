use std::path::Path;
use std::sync::Mutex;
use prost::Message;
use rocksdb::{Cache, DB, WriteBatch};
use crate::rocksdb_options;
use crate::StorageError;

/// Reserved RocksDB key used to store whole-storage state flags.
/// On-disk compatible with Java's `LedgerMetadataIndex.STORAGE_FLAGS` constant.
const STORAGE_FLAGS_KEY: i64 = -0xeefd;

/// Ledger metadata stored in the bookie's RocksDB.
#[derive(Debug, Clone)]
pub struct LedgerData {
    pub exists: bool,
    pub fenced: bool,
    pub master_key: Option<Vec<u8>>,
    pub lac: Option<i64>,
}

/// RocksDB-backed ledger metadata index.
/// Key: ledgerId as 8 bytes big-endian.
/// Value: LedgerData protobuf (from DbLedgerStorageDataFormats.proto).
pub struct LedgerMetadataIndex {
    db: DB,
    /// rocksdb-rs requires the `Cache` to outlive the `DB`; we hold it
    /// here so it stays alive as long as the index.
    #[allow(dead_code)]
    block_cache: Option<Cache>,
    /// Serializes check-then-write on the reserved storage-flags key so
    /// concurrent `set_storage_state_flag`/`clear_storage_state_flag` callers
    /// don't clobber each other. Matches Java's `synchronized(ledgersDb)`.
    flags_lock: Mutex<()>,
}

impl LedgerMetadataIndex {
    /// Open the index at `path`. If `rocksdb_conf` is supplied, rocksdb
    /// options are loaded from that Java BK-style INI file; otherwise the
    /// built-in defaults (matching Apache BK's shipped
    /// `conf/ledger_metadata_rocksdb.conf`) are used.
    pub fn open(path: &Path, rocksdb_conf: Option<&Path>) -> Result<Self, StorageError> {
        let (opts, block_cache) =
            rocksdb_options::load_or_default(rocksdb_conf, rocksdb_options::DEFAULT_LEDGER_METADATA_CONF)?;
        let db = DB::open(&opts, path)?;
        Ok(Self { db, block_cache, flags_lock: Mutex::new(()) })
    }

    pub fn set(&self, ledger_id: i64, fenced: bool, master_key: Option<&[u8]>, lac: Option<i64>) -> Result<(), StorageError> {
        let key = ledger_id.to_be_bytes();
        let proto = bookkeeper_proto::LedgerData {
            exists: true,
            fenced,
            master_key: master_key.map(|k| k.to_vec()).unwrap_or_default(),
            explicit_lac: lac.map(|l| l.to_be_bytes().to_vec()),
            limbo: None,
        };
        let value = proto.encode_to_vec();
        self.db.put(&key, &value).map_err(StorageError::from)
    }

    pub fn get(&self, ledger_id: i64) -> Result<Option<LedgerData>, StorageError> {
        let key = ledger_id.to_be_bytes();
        match self.db.get(&key)? {
            Some(v) => {
                let proto = bookkeeper_proto::LedgerData::decode(&v[..])
                    .map_err(|e| StorageError::CorruptData(format!("ledger data decode: {e}")))?;
                if !proto.exists {
                    return Ok(None);
                }
                let lac = proto.explicit_lac.as_ref().and_then(|b| {
                    if b.len() >= 8 { Some(i64::from_be_bytes(b[..8].try_into().unwrap())) } else { None }
                });
                Ok(Some(LedgerData {
                    exists: proto.exists,
                    fenced: proto.fenced,
                    master_key: if proto.master_key.is_empty() { None } else { Some(proto.master_key) },
                    lac,
                }))
            }
            None => Ok(None),
        }
    }

    pub fn set_fenced(&self, ledger_id: i64) -> Result<(), StorageError> {
        if let Some(mut data) = self.get_raw(ledger_id)? {
            data.fenced = true;
            self.put_raw(ledger_id, &data)
        } else {
            Err(StorageError::LedgerNotFound(ledger_id))
        }
    }

    pub fn update_lac(&self, ledger_id: i64, lac: i64) -> Result<(), StorageError> {
        if let Some(mut data) = self.get_raw(ledger_id)? {
            data.explicit_lac = Some(lac.to_be_bytes().to_vec());
            self.put_raw(ledger_id, &data)
        } else {
            Err(StorageError::LedgerNotFound(ledger_id))
        }
    }

    /// Persist a batch of `(ledger_id, lac)` updates in a single RocksDB
    /// `WriteBatch`. Reads are coalesced into one `multi_get` so the whole
    /// path is two RocksDB calls (multi_get + write_batch) regardless of
    /// how many ledgers are touched. Called from the write-cache flush path;
    /// Java's `SingleDirectoryDbLedgerStorage.flush` does the same.
    ///
    /// Ledgers missing from the index are silently skipped — the flush path
    /// can race with a concurrent `delete_ledger`, and a missing row is not
    /// a recovery error.
    pub fn batch_update_lac(&self, entries: &[(i64, i64)]) -> Result<(), StorageError> {
        if entries.is_empty() {
            return Ok(());
        }
        let keys: Vec<[u8; 8]> = entries.iter().map(|(id, _)| id.to_be_bytes()).collect();
        let results = self.db.multi_get(keys.iter());
        let mut batch = WriteBatch::default();
        for ((ledger_id, lac), result) in entries.iter().zip(results) {
            let Some(raw) = result? else { continue };
            let mut proto = bookkeeper_proto::LedgerData::decode(&raw[..])
                .map_err(|e| StorageError::CorruptData(format!("ledger data decode: {e}")))?;
            proto.explicit_lac = Some(lac.to_be_bytes().to_vec());
            batch.put(ledger_id.to_be_bytes(), proto.encode_to_vec());
        }
        self.db.write(batch).map_err(StorageError::from)
    }

    /// Enumerate all ledger IDs in the index.
    /// Used by GC to compare local ledgers against metadata store.
    pub fn list_all_ledgers(&self) -> Result<Vec<i64>, StorageError> {
        let mut ledgers = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            if key.len() == 8 {
                let ledger_id = i64::from_be_bytes(key[..8].try_into().unwrap());
                if ledger_id == STORAGE_FLAGS_KEY {
                    continue;
                }
                // Only include ledgers that actually exist
                if let Ok(proto) = bookkeeper_proto::LedgerData::decode(&value[..]) {
                    if proto.exists {
                        ledgers.push(ledger_id);
                    }
                }
            }
        }
        Ok(ledgers)
    }

    /// Store the raw explicit LAC body (from WriteLac) in the LedgerData protobuf.
    /// Matches Java's `LedgerMetadataIndex.setExplicitLac()` which stores the
    /// raw LAC body (with digest) in the `explicitLac` field of `LedgerData`.
    pub fn set_explicit_lac_body(&self, ledger_id: i64, body: &[u8]) -> Result<(), StorageError> {
        let key = ledger_id.to_be_bytes();
        let mut proto = self.get_raw(ledger_id)?.unwrap_or_else(|| bookkeeper_proto::LedgerData {
            exists: true,
            fenced: false,
            master_key: Vec::new(),
            explicit_lac: None,
            limbo: None,
        });
        proto.explicit_lac = Some(body.to_vec());
        let value = proto.encode_to_vec();
        self.db.put(&key, &value).map_err(StorageError::from)
    }

    /// Get the raw explicit LAC body from the LedgerData protobuf.
    /// Matches Java's `LedgerMetadataIndex.getExplicitLac()`.
    pub fn get_explicit_lac_body(&self, ledger_id: i64) -> Result<Option<Vec<u8>>, StorageError> {
        match self.get_raw(ledger_id)? {
            Some(proto) => Ok(proto.explicit_lac),
            None => Ok(None),
        }
    }

    pub fn delete(&self, ledger_id: i64) -> Result<(), StorageError> {
        let key = ledger_id.to_be_bytes();
        self.db.delete(&key).map_err(StorageError::from)
    }

    /// Read the whole-storage state flags bitmask. Returns 0 if unset.
    pub fn get_storage_state_flags(&self) -> Result<u64, StorageError> {
        let key = STORAGE_FLAGS_KEY.to_be_bytes();
        match self.db.get(&key)? {
            Some(v) if v.len() >= 8 => Ok(u64::from_be_bytes(v[..8].try_into().unwrap())),
            _ => Ok(0),
        }
    }

    /// Atomically update the storage state flags from `expected` to `new_flags`.
    /// Returns `true` on success, `false` if the current value doesn't match
    /// `expected` (caller should retry with the fresh value).
    pub fn set_storage_state_flags(&self, expected: u64, new_flags: u64) -> Result<bool, StorageError> {
        let _guard = self.flags_lock.lock().unwrap_or_else(|p| p.into_inner());
        let current = self.get_storage_state_flags()?;
        if current != expected {
            return Ok(false);
        }
        let key = STORAGE_FLAGS_KEY.to_be_bytes();
        self.db.put(&key, &new_flags.to_be_bytes())?;
        Ok(true)
    }

    /// Mark a ledger as in limbo (awaiting data-integrity recovery).
    pub fn set_limbo(&self, ledger_id: i64) -> Result<(), StorageError> {
        let mut proto = self.get_raw(ledger_id)?.ok_or(StorageError::LedgerNotFound(ledger_id))?;
        proto.limbo = Some(true);
        self.put_raw(ledger_id, &proto)
    }

    /// Clear a ledger's limbo state. No-op when the ledger is absent or
    /// already out of limbo (limbo field unset or `Some(false)`).
    pub fn clear_limbo(&self, ledger_id: i64) -> Result<(), StorageError> {
        if let Some(mut proto) = self.get_raw(ledger_id)? {
            if proto.limbo.unwrap_or(false) {
                proto.limbo = Some(false);
                self.put_raw(ledger_id, &proto)?;
            }
        }
        Ok(())
    }

    /// Check whether a ledger is in limbo. Absent/unset means not in limbo.
    pub fn has_limbo(&self, ledger_id: i64) -> Result<bool, StorageError> {
        Ok(self.get_raw(ledger_id)?.and_then(|p| p.limbo).unwrap_or(false))
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        let mut opts = rocksdb::FlushOptions::default();
        opts.set_wait(true);
        self.db.flush_opt(&opts).map_err(StorageError::from)
    }

    fn get_raw(&self, ledger_id: i64) -> Result<Option<bookkeeper_proto::LedgerData>, StorageError> {
        let key = ledger_id.to_be_bytes();
        match self.db.get(&key)? {
            Some(v) => {
                let proto = bookkeeper_proto::LedgerData::decode(&v[..])
                    .map_err(|e| StorageError::CorruptData(format!("ledger data decode: {e}")))?;
                Ok(Some(proto))
            }
            None => Ok(None),
        }
    }

    fn put_raw(&self, ledger_id: i64, data: &bookkeeper_proto::LedgerData) -> Result<(), StorageError> {
        let key = ledger_id.to_be_bytes();
        let value = data.encode_to_vec();
        self.db.put(&key, &value).map_err(StorageError::from)
    }
}
