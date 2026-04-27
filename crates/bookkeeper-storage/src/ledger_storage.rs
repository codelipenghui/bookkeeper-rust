use std::path::Path;
use async_trait::async_trait;
use bytes::Bytes;
use crate::StorageError;

/// Whole-storage state flags persisted alongside ledger metadata.
/// Matches Java's `LedgerStorage.StorageState` enum. Values are bitmask
/// positions so multiple flags can be set simultaneously.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u64)]
pub enum StorageStateFlag {
    /// Set by the data-integrity pre-boot check when the bookie may have
    /// lost data. While this flag is set, the data-integrity service will
    /// keep running full checks until the flag can be cleared.
    NeedsIntegrityCheck = 0x0000_0001,
}

impl StorageStateFlag {
    pub fn bits(self) -> u64 {
        self as u64
    }
}

/// The LedgerStorage trait defines the interface for bookie storage backends.
#[async_trait]
pub trait LedgerStorage: Send + Sync {
    /// Add an entry to storage. Returns after the entry is in the write cache.
    /// If a journal is configured, the entry is journaled first for durability.
    async fn add_entry(&self, entry: &[u8]) -> Result<(), StorageError>;

    /// Get journal handle for async journaling (used by the request processor
    /// to journal before acknowledging writes).
    fn journal(&self) -> Option<&crate::journal::writer::Journal>;

    /// Get an entry by ledger ID and entry ID.
    async fn get_entry(&self, ledger_id: i64, entry_id: i64) -> Result<Bytes, StorageError>;

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

    /// Set the explicit LAC for a ledger (value only).
    fn set_explicit_lac(&self, ledger_id: i64, lac: i64) -> Result<(), StorageError>;

    /// Store the explicit LAC body (raw bytes with digest, from WriteLac request).
    /// Returned by ReadLac so clients can verify the digest.
    fn set_explicit_lac_body(&self, ledger_id: i64, body: &[u8]) -> Result<(), StorageError>;

    /// Get the explicit LAC body previously stored by WriteLac.
    fn get_explicit_lac_body(&self, ledger_id: i64) -> Result<Option<Vec<u8>>, StorageError>;

    /// Delete a ledger and all its entries. Async because the impl
    /// acquires `flush_mutex` to serialise against an in-flight drain
    /// (otherwise `drain_flushing` could re-insert entry-location rows
    /// for the just-deleted ledger).
    async fn delete_ledger(&self, ledger_id: i64) -> Result<(), StorageError>;

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
    async fn write_entry_to_log(&self, entry: &[u8]) -> Result<u64, StorageError>;

    /// Update a batch of entry locations in the index.
    fn update_entry_locations(&self, batch: &[(i64, i64, u64)]) -> Result<(), StorageError>;

    /// Update entry locations while holding the flush lock.
    /// Ensures no concurrent flush can overwrite the compaction's index updates.
    /// Matches Java's `updateEntriesLocations` which acquires flushMutex before
    /// updating the entry location index, preventing a race where a concurrent
    /// flush writes stale pre-compaction locations.
    async fn update_entry_locations_with_flush_lock(
        &self,
        batch: &[(i64, i64, u64)],
    ) -> Result<(), StorageError> {
        // Default: no lock, just delegate. DbLedgerStorage overrides.
        self.update_entry_locations(batch)
    }

    /// Get the LAC watcher registry for long poll reads.
    fn lac_watchers(&self) -> Option<&crate::lac_watcher::LacWatcherRegistry>;

    /// Force the journal to fsync all pending data.
    /// Used by ForceLedger — lighter than a full flush since it only
    /// ensures journal durability without flushing the write cache or
    /// updating the RocksDB index.
    async fn force_journal_sync(&self) -> Result<(), StorageError>;

    /// Flush all pending data to persistent storage.
    async fn flush(&self) -> Result<(), StorageError>;

    /// Perform a checkpoint: flush and return completion.
    async fn checkpoint(&self) -> Result<(), StorageError>;

    /// Run a local consistency check on all stored entries.
    /// Verifies that for each entry in the location index, the entry log
    /// contains the correct `ledgerId` and `entryId` at the indexed position.
    /// Matches Java's `InterleavedLedgerStorage.localConsistencyCheck()`.
    ///
    /// `rate_limit` controls max entries/second to check (0 = unlimited).
    /// Returns the number of errors found.
    async fn local_consistency_check(&self, rate_limit: u64) -> Result<usize, StorageError>;

    /// Validate a ledger for writing: check existence, fencing, and master key in one operation.
    /// Creates the ledger if it doesn't exist. Returns true if the ledger is fenced.
    fn validate_for_write(&self, ledger_id: i64, master_key: &[u8]) -> Result<bool, StorageError>;

    /// Get the list of entry IDs stored for a ledger. Used by GetListOfEntriesOfLedger
    /// operation and admin tooling. Returns a sorted Vec of entry IDs.
    /// Matches Java's `Bookie.getListOfEntriesOfLedger()`.
    fn get_entries_of_ledger(&self, ledger_id: i64) -> Result<Vec<i64>, StorageError>;

    /// Check whether `(ledger_id, entry_id)` is present in local storage.
    /// Returns `false` for `LAST_ADD_CONFIRMED` sentinel (`-1`) — matching
    /// Java's `SingleDirectoryDbLedgerStorage.entryExists`.
    fn entry_exists(&self, ledger_id: i64, entry_id: i64) -> Result<bool, StorageError>;

    /// Read the current whole-storage state flag bitmask.
    fn get_storage_state_flags(&self) -> Result<u64, StorageError>;

    /// Set a single storage-state flag with CAS retries.
    fn set_storage_state_flag(&self, flag: StorageStateFlag) -> Result<(), StorageError>;

    /// Clear a single storage-state flag with CAS retries.
    fn clear_storage_state_flag(&self, flag: StorageStateFlag) -> Result<(), StorageError>;

    /// Mark a ledger as in limbo (data-integrity recovery in progress).
    /// The ledger must already exist.
    fn set_limbo_state(&self, ledger_id: i64) -> Result<(), StorageError>;

    /// Clear a ledger's limbo state. No-op if the ledger doesn't exist
    /// or is not in limbo.
    fn clear_limbo_state(&self, ledger_id: i64) -> Result<(), StorageError>;

    /// Check whether a ledger is currently in limbo.
    fn has_limbo_state(&self, ledger_id: i64) -> Result<bool, StorageError>;
}
