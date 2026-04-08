use std::path::Path;
use rocksdb::{DB, Options, WriteBatch};
use crate::StorageError;

/// RocksDB-backed entry location index.
/// Key: (ledgerId, entryId) as 16 bytes big-endian.
/// Value: location as 8 bytes big-endian.
pub struct EntryLocationIndex {
    db: DB,
}

impl EntryLocationIndex {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_write_buffer_number(4);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.set_max_background_jobs(4);
        let block_opts = rocksdb::BlockBasedOptions::default();
        opts.set_block_based_table_factory(&block_opts);
        let db = DB::open(&opts, path).map_err(|e| StorageError::RocksDb(e.to_string()))?;
        Ok(Self { db })
    }

    pub fn put(&self, ledger_id: i64, entry_id: i64, location: u64) -> Result<(), StorageError> {
        let key = encode_location_key(ledger_id, entry_id);
        let value = location.to_be_bytes();
        self.db.put(&key, &value).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    pub fn put_batch(&self, entries: &[(i64, i64, u64)]) -> Result<(), StorageError> {
        let mut batch = WriteBatch::default();
        for &(ledger_id, entry_id, location) in entries {
            let key = encode_location_key(ledger_id, entry_id);
            let value = location.to_be_bytes();
            batch.put(&key, &value);
        }
        self.db.write(batch).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    pub fn get(&self, ledger_id: i64, entry_id: i64) -> Result<Option<u64>, StorageError> {
        let key = encode_location_key(ledger_id, entry_id);
        match self.db.get(&key).map_err(|e| StorageError::RocksDb(e.to_string()))? {
            Some(v) => {
                let loc = u64::from_be_bytes(v[..8].try_into().unwrap());
                Ok(Some(loc))
            }
            None => Ok(None),
        }
    }

    pub fn delete_ledger(&self, ledger_id: i64) -> Result<(), StorageError> {
        // Build start/end keys using raw bytes to cover the full entry range.
        // Keys are 16 bytes: ledgerId(8 BE) + entryId(8 BE).
        // Start: ledgerId with entryId = all-zeros (smallest byte sequence).
        // End: (ledgerId+1) with entryId = all-zeros (exclusive upper bound).
        let mut start = [0u8; 16];
        start[0..8].copy_from_slice(&ledger_id.to_be_bytes());
        // start[8..16] stays all-zeros — smallest possible entryId byte sequence

        let mut end = [0u8; 16];
        end[0..8].copy_from_slice(&(ledger_id + 1).to_be_bytes());
        // end[8..16] stays all-zeros

        let mut batch = WriteBatch::default();
        batch.delete_range(&start, &end);
        self.db.write(batch).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        let mut opts = rocksdb::FlushOptions::default();
        opts.set_wait(true);
        self.db.flush_opt(&opts).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    /// Iterate all entries for a given ledger, returning (entry_id, location) pairs.
    pub fn iter_ledger_entries(&self, ledger_id: i64) -> Result<Vec<(i64, u64)>, StorageError> {
        let mut start = [0u8; 16];
        start[0..8].copy_from_slice(&ledger_id.to_be_bytes());

        let mut end = [0u8; 16];
        end[0..8].copy_from_slice(&(ledger_id + 1).to_be_bytes());

        let mut results = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::From(&start, rocksdb::Direction::Forward));
        for item in iter {
            let (key, value) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;
            if key.len() != 16 || &key[..] >= &end[..] {
                break;
            }
            let entry_id = i64::from_be_bytes(key[8..16].try_into().unwrap());
            let location = u64::from_be_bytes(value[..8].try_into().unwrap());
            results.push((entry_id, location));
        }
        Ok(results)
    }

    /// Trigger RocksDB compaction to reclaim space after ledger deletions.
    /// Matches Java's DbLedgerStorage.entryLocationCompact().
    pub fn compact(&self) -> Result<(), StorageError> {
        self.db.compact_range::<&[u8], &[u8]>(None, None);
        Ok(())
    }
}

fn encode_location_key(ledger_id: i64, entry_id: i64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[0..8].copy_from_slice(&ledger_id.to_be_bytes());
    key[8..16].copy_from_slice(&entry_id.to_be_bytes());
    key
}
