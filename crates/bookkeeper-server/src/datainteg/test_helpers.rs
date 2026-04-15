//! Shared in-memory mocks for data-integrity unit tests.
//!
//! Keeping them in one place so the cookie validator, check orchestrator,
//! and service test suites can all reuse them instead of defining
//! parallel stubs.

#![cfg(test)]

use async_trait::async_trait;
use bookkeeper_metadata::ledger_metadata::{LedgerMetadata, MetadataError};
use bookkeeper_metadata::traits::LedgerManager;
use bookkeeper_metadata::versioned::Versioned;
use bookkeeper_storage::ledger_storage::{LedgerStorage, StorageStateFlag};
use bookkeeper_storage::StorageError;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::check::{LedgerRecoveryClient, RecoverError};

/// Minimal `LedgerRecoveryClient` that panics if called. Enough for
/// tests that don't exercise the limbo-recovery path.
pub struct NoopRecovery;

#[async_trait]
impl LedgerRecoveryClient for NoopRecovery {
    async fn recover_ledger(
        &self,
        _id: i64,
        _md: &LedgerMetadata,
    ) -> Result<LedgerMetadata, RecoverError> {
        unreachable!("tests that hit this must install a real recovery client")
    }
}

/// `LedgerManager` stub that reports no ledgers.
pub struct EmptyLedgerManager;

#[async_trait]
impl LedgerManager for EmptyLedgerManager {
    async fn create_ledger_metadata(
        &self,
        _l: i64,
        _m: &LedgerMetadata,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
        unimplemented!()
    }
    async fn read_ledger_metadata(
        &self,
        ledger_id: i64,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
        Err(MetadataError::NotFound(ledger_id))
    }
    async fn write_ledger_metadata(
        &self,
        _l: i64,
        _m: &LedgerMetadata,
        _v: i32,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
        unimplemented!()
    }
    async fn remove_ledger_metadata(&self, _l: i64, _v: i32) -> Result<(), MetadataError> {
        Ok(())
    }
    async fn get_ledger_ranges(&self) -> Result<Vec<i64>, MetadataError> {
        Ok(vec![])
    }
}

/// Minimal in-memory `LedgerStorage` — only the subset data-integrity
/// tests exercise (flag get/set/clear, flush count). Everything else is
/// stubbed to a no-op or benign default.
pub struct StubStorage {
    pub flags: std::sync::Mutex<u64>,
    pub flush_count: AtomicUsize,
}

impl StubStorage {
    pub fn new(initial_flags: u64) -> Arc<Self> {
        Arc::new(Self {
            flags: std::sync::Mutex::new(initial_flags),
            flush_count: AtomicUsize::new(0),
        })
    }

}

#[async_trait]
impl LedgerStorage for StubStorage {
    async fn add_entry(&self, _e: &[u8]) -> Result<(), StorageError> {
        Ok(())
    }
    fn journal(&self) -> Option<&bookkeeper_storage::journal::writer::Journal> {
        None
    }
    async fn get_entry(&self, _l: i64, _e: i64) -> Result<bytes::Bytes, StorageError> {
        Err(StorageError::LedgerNotFound(0))
    }
    fn ledger_exists(&self, _l: i64) -> Result<bool, StorageError> {
        Ok(false)
    }
    fn set_fenced(&self, _l: i64) -> Result<bool, StorageError> {
        Ok(true)
    }
    fn is_fenced(&self, _l: i64) -> Result<bool, StorageError> {
        Ok(false)
    }
    fn set_master_key(&self, _l: i64, _k: &[u8]) -> Result<(), StorageError> {
        Ok(())
    }
    fn check_master_key(&self, _l: i64, _k: &[u8]) -> Result<(), StorageError> {
        Ok(())
    }
    fn get_last_add_confirmed(&self, _l: i64) -> Result<i64, StorageError> {
        Ok(-1)
    }
    fn set_explicit_lac(&self, _l: i64, _lac: i64) -> Result<(), StorageError> {
        Ok(())
    }
    fn set_explicit_lac_body(&self, _l: i64, _b: &[u8]) -> Result<(), StorageError> {
        Ok(())
    }
    fn get_explicit_lac_body(&self, _l: i64) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(None)
    }
    fn delete_ledger(&self, _l: i64) -> Result<(), StorageError> {
        Ok(())
    }
    fn get_active_ledgers(&self) -> Result<Vec<i64>, StorageError> {
        Ok(vec![])
    }
    fn entry_log_dir(&self) -> Option<&std::path::Path> {
        None
    }
    fn current_log_id(&self) -> Option<u32> {
        None
    }
    fn compact_entry_locations(&self) -> Result<(), StorageError> {
        Ok(())
    }
    async fn write_entry_to_log(&self, _e: &[u8]) -> Result<u64, StorageError> {
        Ok(0)
    }
    fn update_entry_locations(&self, _b: &[(i64, i64, u64)]) -> Result<(), StorageError> {
        Ok(())
    }
    fn lac_watchers(&self) -> Option<&bookkeeper_storage::lac_watcher::LacWatcherRegistry> {
        None
    }
    async fn force_journal_sync(&self) -> Result<(), StorageError> {
        Ok(())
    }
    async fn flush(&self) -> Result<(), StorageError> {
        self.flush_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn checkpoint(&self) -> Result<(), StorageError> {
        Ok(())
    }
    async fn local_consistency_check(&self, _r: u64) -> Result<usize, StorageError> {
        Ok(0)
    }
    fn validate_for_write(&self, _l: i64, _k: &[u8]) -> Result<bool, StorageError> {
        Ok(false)
    }
    fn get_entries_of_ledger(&self, _l: i64) -> Result<Vec<i64>, StorageError> {
        Ok(vec![])
    }
    fn entry_exists(&self, _l: i64, _e: i64) -> Result<bool, StorageError> {
        Ok(true)
    }
    fn get_storage_state_flags(&self) -> Result<u64, StorageError> {
        Ok(*self.flags.lock().unwrap())
    }
    fn set_storage_state_flag(&self, flag: StorageStateFlag) -> Result<(), StorageError> {
        *self.flags.lock().unwrap() |= flag.bits();
        Ok(())
    }
    fn clear_storage_state_flag(&self, flag: StorageStateFlag) -> Result<(), StorageError> {
        *self.flags.lock().unwrap() &= !flag.bits();
        Ok(())
    }
    fn set_limbo_state(&self, _l: i64) -> Result<(), StorageError> {
        Ok(())
    }
    fn clear_limbo_state(&self, _l: i64) -> Result<(), StorageError> {
        Ok(())
    }
    fn has_limbo_state(&self, _l: i64) -> Result<bool, StorageError> {
        Ok(false)
    }
}
