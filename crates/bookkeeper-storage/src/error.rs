use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("corrupt data: {0}")]
    CorruptData(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("entry not found: ledger={ledger_id}, entry={entry_id}")]
    EntryNotFound { ledger_id: i64, entry_id: i64 },
    #[error("ledger not found: {0}")]
    LedgerNotFound(i64),
    #[error("ledger fenced: {0}")]
    Fenced(i64),
    #[error("storage full")]
    StorageFull,
    #[error("RocksDB error: {0}")]
    RocksDb(String),
    #[error("unauthorized access to ledger {0}")]
    Unauthorized(i64),
    /// Write cache full and the bounded throttle period elapsed without
    /// completing a flush. Matches Java's `OperationRejectedException`
    /// returned from `triggerFlushAndAddEntry()` after `maxThrottleTimeNanos`.
    #[error("operation rejected: write cache full, flush throttle timeout")]
    OperationRejected,
    /// Ledger is in limbo: the data-integrity pre-boot pass marked it
    /// pending repair, so reads / writes against it must fail until the
    /// full integrity check has fenced+recovered the ledger. Matches
    /// Java's `BKLedgerNeedsIntegrityCheckException` raised by
    /// `SingleDirectoryDbLedgerStorage.throwIfLimbo()`.
    #[error("ledger {0} is in limbo (pending integrity check)")]
    DataUnknown(i64),
}

impl From<rocksdb::Error> for StorageError {
    fn from(e: rocksdb::Error) -> Self {
        StorageError::RocksDb(e.to_string())
    }
}
