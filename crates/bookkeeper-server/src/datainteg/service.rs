//! Background task that drives [`DataIntegrityCheck::run_full_check`]
//! at a fixed interval. Port of Java's `DataIntegrityService`.
//!
//! While the `NEEDS_INTEGRITY_CHECK` storage flag is set, the service
//! invokes the full check on every tick until it completes cleanly.
//! Transient errors (single-ledger failures) keep the flag set so the
//! service naturally re-attempts them on the next tick.

use std::sync::Arc;
use std::time::Duration;

use tokio::select;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::interval;

use super::check::DataIntegrityCheck;

/// Default tick interval, matches Java's `DataIntegrityService.interval() = 3s`.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(3);

/// Spawn the data-integrity service. Returns a `JoinHandle` the caller
/// can await on shutdown. The task stops as soon as `shutdown_rx`
/// flips to `true`.
pub fn spawn(
    check: Arc<DataIntegrityCheck>,
    tick: Duration,
    shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(run(check, tick, shutdown_rx))
}

async fn run(
    check: Arc<DataIntegrityCheck>,
    tick: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    tracing::info!(interval_ms = tick.as_millis() as u64, "data-integrity service starting");
    let mut ticker = interval(tick);
    // First tick fires immediately; skip that one so we don't double up
    // with any preboot work the caller just finished.
    ticker.tick().await;
    loop {
        select! {
            _ = ticker.tick() => {}
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
                continue;
            }
        }
        match check.needs_full_check() {
            Ok(false) => continue,
            Ok(true) => {}
            Err(e) => {
                tracing::error!(error = ?e, "data-integrity: needs_full_check failed");
                continue;
            }
        }
        tracing::info!("data-integrity: running full check");
        if let Err(e) = check.run_full_check().await {
            tracing::warn!(error = ?e, "data-integrity: full check reported errors; will retry");
        }
    }
    tracing::info!("data-integrity service stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datainteg::check::{DataIntegrityConfig, LedgerRecoveryClient, RecoverError};
    use crate::datainteg::entry_copier::EntryCopier;
    use async_trait::async_trait;
    use bookkeeper_metadata::ledger_metadata::{LedgerMetadata, MetadataError};
    use bookkeeper_metadata::traits::LedgerManager;
    use bookkeeper_metadata::versioned::Versioned;
    use bookkeeper_storage::ledger_storage::{LedgerStorage, StorageStateFlag};
    use bookkeeper_storage::StorageError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // --- Minimal mocks -----------------------------------------------

    struct NoopRecovery;

    #[async_trait]
    impl LedgerRecoveryClient for NoopRecovery {
        async fn recover_ledger(
            &self,
            _id: i64,
            _md: &LedgerMetadata,
        ) -> Result<LedgerMetadata, RecoverError> {
            unreachable!("test never enters limbo recovery")
        }
    }

    struct EmptyLedgerManager;

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

    /// In-memory LedgerStorage stub sufficient to exercise
    /// `needs_full_check` + `run_full_check` flag transitions.
    struct StubStorage {
        flags: std::sync::Mutex<u64>,
        flush_count: AtomicUsize,
    }

    impl StubStorage {
        fn new(initial_flags: u64) -> Arc<Self> {
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
        fn journal(
            &self,
        ) -> Option<&bookkeeper_storage::journal::writer::Journal> {
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
        fn lac_watchers(
            &self,
        ) -> Option<&bookkeeper_storage::lac_watcher::LacWatcherRegistry> {
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

    fn make_check(storage: Arc<StubStorage>) -> Arc<DataIntegrityCheck> {
        let lm: Arc<dyn LedgerManager> = Arc::new(EmptyLedgerManager);
        let copier = Arc::new(EntryCopier::new(
            "bk1:3181".to_string(),
            storage.clone() as Arc<dyn LedgerStorage>,
        ));
        Arc::new(DataIntegrityCheck::with_config(
            "bk1:3181".to_string(),
            lm,
            storage.clone() as Arc<dyn LedgerStorage>,
            copier,
            Arc::new(NoopRecovery),
            DataIntegrityConfig { max_inflight: 4, max_entries_inflight: 4 },
        ))
    }

    #[tokio::test]
    async fn flag_set_triggers_full_check_that_clears_it() {
        let storage = StubStorage::new(StorageStateFlag::NeedsIntegrityCheck.bits());
        let check = make_check(storage.clone());
        let (tx, rx) = watch::channel(false);
        let handle = spawn(Arc::clone(&check), Duration::from_millis(20), rx);

        // Poll the flag until the service clears it, then stop.
        for _ in 0..100 {
            if *storage.flags.lock().unwrap() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let _ = tx.send(true);
        let _ = handle.await;
        assert_eq!(*storage.flags.lock().unwrap(), 0, "flag cleared after clean pass");
    }

    #[tokio::test]
    async fn flag_unset_is_silent() {
        let storage = StubStorage::new(0);
        let check = make_check(storage.clone());
        let (tx, rx) = watch::channel(false);
        let handle = spawn(Arc::clone(&check), Duration::from_millis(20), rx);

        // Let the service run a few ticks; since the flag is clear, it
        // must not call run_full_check (which is what bumps flush_count).
        tokio::time::sleep(Duration::from_millis(100)).await;

        let _ = tx.send(true);
        let _ = handle.await;
        assert_eq!(
            storage.flush_count.load(Ordering::SeqCst),
            0,
            "run_full_check should not have run"
        );
    }

    #[tokio::test]
    async fn shutdown_stops_the_loop() {
        let storage = StubStorage::new(0);
        let check = make_check(storage.clone());
        let (tx, rx) = watch::channel(false);
        // 60s tick; if shutdown isn't honored the test times out.
        let handle = spawn(Arc::clone(&check), Duration::from_secs(60), rx);
        let _ = tx.send(true);
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("service failed to shut down within 1s")
            .unwrap();
    }
}
