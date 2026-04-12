use std::path::Path;
use prost::Message;
use rocksdb::{Cache, DB};
use crate::rocksdb_options;
use crate::StorageError;

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
}

impl LedgerMetadataIndex {
    /// Open the index at `path`. If `rocksdb_conf` is supplied, rocksdb
    /// options are loaded from that Java BK-style INI file; otherwise the
    /// built-in defaults (matching Apache BK's shipped
    /// `conf/ledger_metadata_rocksdb.conf`) are used.
    pub fn open(path: &Path, rocksdb_conf: Option<&Path>) -> Result<Self, StorageError> {
        let (opts, block_cache) =
            rocksdb_options::load_or_default(rocksdb_conf, rocksdb_options::DEFAULT_LEDGER_METADATA_CONF)?;
        let db = DB::open(&opts, path).map_err(|e| StorageError::RocksDb(e.to_string()))?;
        Ok(Self { db, block_cache })
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
        self.db.put(&key, &value).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    pub fn get(&self, ledger_id: i64) -> Result<Option<LedgerData>, StorageError> {
        let key = ledger_id.to_be_bytes();
        match self.db.get(&key).map_err(|e| StorageError::RocksDb(e.to_string()))? {
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

    /// Enumerate all ledger IDs in the index.
    /// Used by GC to compare local ledgers against metadata store.
    pub fn list_all_ledgers(&self) -> Result<Vec<i64>, StorageError> {
        let mut ledgers = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;
            if key.len() == 8 {
                let ledger_id = i64::from_be_bytes(key[..8].try_into().unwrap());
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

    /// Store the raw explicit LAC body (from WriteLac) to RocksDB.
    /// Uses a separate key prefix "lac:" to avoid conflicting with ledger metadata.
    pub fn set_explicit_lac_body(&self, ledger_id: i64, body: &[u8]) -> Result<(), StorageError> {
        let mut key = Vec::with_capacity(12);
        key.extend_from_slice(b"lac:");
        key.extend_from_slice(&ledger_id.to_be_bytes());
        self.db.put(&key, body).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    /// Get the raw explicit LAC body from RocksDB.
    pub fn get_explicit_lac_body(&self, ledger_id: i64) -> Result<Option<Vec<u8>>, StorageError> {
        let mut key = Vec::with_capacity(12);
        key.extend_from_slice(b"lac:");
        key.extend_from_slice(&ledger_id.to_be_bytes());
        match self.db.get(&key).map_err(|e| StorageError::RocksDb(e.to_string()))? {
            Some(v) => Ok(Some(v)),
            None => Ok(None),
        }
    }

    pub fn delete(&self, ledger_id: i64) -> Result<(), StorageError> {
        let key = ledger_id.to_be_bytes();
        self.db.delete(&key).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        let mut opts = rocksdb::FlushOptions::default();
        opts.set_wait(true);
        self.db.flush_opt(&opts).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    fn get_raw(&self, ledger_id: i64) -> Result<Option<bookkeeper_proto::LedgerData>, StorageError> {
        let key = ledger_id.to_be_bytes();
        match self.db.get(&key).map_err(|e| StorageError::RocksDb(e.to_string()))? {
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
        self.db.put(&key, &value).map_err(|e| StorageError::RocksDb(e.to_string()))
    }
}
