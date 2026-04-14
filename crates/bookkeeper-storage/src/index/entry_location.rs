use std::path::Path;
use rocksdb::{Cache, DB, WriteBatch};
use crate::rocksdb_options;
use crate::StorageError;

/// RocksDB-backed entry location index.
/// Key: (ledgerId, entryId) as 16 bytes big-endian.
/// Value: location as 8 bytes big-endian.
pub struct EntryLocationIndex {
    db: DB,
    /// rocksdb-rs requires the `Cache` to outlive the `DB`; we hold it
    /// here so it stays alive as long as the index.
    #[allow(dead_code)]
    block_cache: Option<Cache>,
}

impl EntryLocationIndex {
    /// Open the index at `path`. If `rocksdb_conf` is supplied, rocksdb
    /// options are loaded from that Java BK-style INI file; otherwise the
    /// built-in defaults (matching Apache BK's shipped
    /// `conf/entry_location_rocksdb.conf`) are used.
    pub fn open(path: &Path, rocksdb_conf: Option<&Path>) -> Result<Self, StorageError> {
        let (opts, block_cache) =
            rocksdb_options::load_or_default(rocksdb_conf, rocksdb_options::DEFAULT_ENTRY_LOCATION_CONF)?;
        let db = DB::open(&opts, path).map_err(|e| StorageError::RocksDb(e.to_string()))?;
        Ok(Self { db, block_cache })
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

    /// Get the last entry ID stored for a ledger by doing a floor scan.
    /// Seeks to `(ledgerId, MAX)` and scans backward to find the largest
    /// entry ID. Returns `None` if no entries exist for the ledger.
    /// Matches Java's `EntryLocationIndex.getLastEntryInLedger()` which uses
    /// `locationsDb.getFloor(maxEntryId)`.
    pub fn get_last_entry_in_ledger(&self, ledger_id: i64) -> Result<Option<i64>, StorageError> {
        // Seek to (ledgerId, MAX_VALUE) — the largest possible key for this ledger.
        let seek_key = encode_location_key(ledger_id, i64::MAX);
        // The exclusive lower bound for this ledger.
        let lower = encode_location_key(ledger_id, 0);

        let iter = self.db.iterator(
            rocksdb::IteratorMode::From(&seek_key, rocksdb::Direction::Reverse),
        );
        for item in iter {
            let (key, _value) = item.map_err(|e| StorageError::RocksDb(e.to_string()))?;
            if key.len() != 16 {
                break;
            }
            // Check that this key still belongs to our ledger
            let key_ledger = i64::from_be_bytes(key[0..8].try_into().unwrap());
            if key_ledger != ledger_id {
                break;
            }
            if &key[..] < &lower[..] {
                break;
            }
            let entry_id = i64::from_be_bytes(key[8..16].try_into().unwrap());
            return Ok(Some(entry_id));
        }
        Ok(None)
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
