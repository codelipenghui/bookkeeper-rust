use std::path::Path;
use bytes::Bytes;
use crate::StorageError;

/// The LedgerStorage trait defines the interface for bookie storage backends.
pub trait LedgerStorage: Send + Sync {
    /// Add an entry to storage. Returns after the entry is in the write cache.
    /// If a journal is configured, the entry is journaled first for durability.
    fn add_entry(&self, entry: &[u8]) -> Result<(), StorageError>;

    /// Get journal handle for async journaling (used by the request processor
    /// to journal before acknowledging writes).
    fn journal(&self) -> Option<&crate::journal::writer::Journal>;

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

    /// Check the master key for a ledger. On first call, stores the key.
    /// On subsequent calls, compares with stored key. Returns Err if mismatch.
    fn check_master_key(&self, ledger_id: i64, master_key: &[u8]) -> Result<(), StorageError>;

    /// Get the last add confirmed for a ledger.
    fn get_last_add_confirmed(&self, ledger_id: i64) -> Result<i64, StorageError>;

    /// Set the explicit LAC for a ledger.
    fn set_explicit_lac(&self, ledger_id: i64, lac: i64) -> Result<(), StorageError>;

    /// Delete a ledger and all its entries.
    fn delete_ledger(&self, ledger_id: i64) -> Result<(), StorageError>;

    /// List all ledger IDs in local storage.
    fn get_active_ledgers(&self) -> Result<Vec<i64>, StorageError>;

    /// Get the path to the entry log directory (for GC to scan/delete files).
    fn entry_log_dir(&self) -> Option<&Path>;

    /// Get the current active entry log ID (the one being written to).
    /// GC must NOT delete this file.
    fn current_log_id(&self) -> Option<u32>;

    /// Trigger RocksDB compaction on the entry location index.
    /// Matches Java's DbLedgerStorage.entryLocationCompact().
    fn compact_entry_locations(&self) -> Result<(), StorageError>;

    /// Write an entry directly to the entry log (bypassing write cache) and update
    /// the location index. Used by compaction to move entries between log files.
    /// Returns the new location.
    fn write_entry_to_log(&self, entry: &[u8]) -> Result<u64, StorageError>;

    /// Update a batch of entry locations in the index.
    fn update_entry_locations(&self, batch: &[(i64, i64, u64)]) -> Result<(), StorageError>;

    /// Flush all pending data to persistent storage.
    fn flush(&self) -> Result<(), StorageError>;

    /// Perform a checkpoint: flush and return completion.
    fn checkpoint(&self) -> Result<(), StorageError>;

    /// Validate a ledger for writing: check existence, fencing, and master key in one operation.
    /// Creates the ledger if it doesn't exist. Returns true if the ledger is fenced.
    fn validate_for_write(&self, ledger_id: i64, master_key: &[u8]) -> Result<bool, StorageError>;
}
