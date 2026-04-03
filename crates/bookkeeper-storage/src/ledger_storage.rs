use bytes::Bytes;
use crate::StorageError;

/// The LedgerStorage trait defines the interface for bookie storage backends.
pub trait LedgerStorage: Send + Sync {
    /// Add an entry to storage. Returns after the entry is in the write cache.
    fn add_entry(&self, entry: &[u8]) -> Result<(), StorageError>;

    /// Get an entry by ledger ID and entry ID.
    fn get_entry(&self, ledger_id: i64, entry_id: i64) -> Result<Bytes, StorageError>;

    /// Check if a ledger exists.
    fn ledger_exists(&self, ledger_id: i64) -> Result<bool, StorageError>;

    /// Set a ledger as fenced. Returns true if the ledger was successfully fenced.
    fn set_fenced(&self, ledger_id: i64) -> Result<bool, StorageError>;

    /// Check if a ledger is fenced.
    fn is_fenced(&self, ledger_id: i64) -> Result<bool, StorageError>;

    /// Set the master key for a ledger.
    fn set_master_key(&self, ledger_id: i64, master_key: &[u8]) -> Result<(), StorageError>;

    /// Get the last add confirmed for a ledger.
    fn get_last_add_confirmed(&self, ledger_id: i64) -> Result<i64, StorageError>;

    /// Set the explicit LAC for a ledger.
    fn set_explicit_lac(&self, ledger_id: i64, lac: i64) -> Result<(), StorageError>;

    /// Delete a ledger and all its entries.
    fn delete_ledger(&self, ledger_id: i64) -> Result<(), StorageError>;

    /// Flush all pending data to persistent storage.
    fn flush(&self) -> Result<(), StorageError>;

    /// Perform a checkpoint: flush and return completion.
    fn checkpoint(&self) -> Result<(), StorageError>;
}
