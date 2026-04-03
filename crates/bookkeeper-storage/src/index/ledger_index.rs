use std::path::Path;
use prost::Message;
use rocksdb::{DB, Options};
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
}

impl LedgerMetadataIndex {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path).map_err(|e| StorageError::RocksDb(e.to_string()))?;
        Ok(Self { db })
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
