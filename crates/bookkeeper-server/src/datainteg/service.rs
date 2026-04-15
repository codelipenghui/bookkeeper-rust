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
    use crate::datainteg::check::DataIntegrityConfig;
    use crate::datainteg::entry_copier::EntryCopier;
    use crate::datainteg::test_helpers::{EmptyLedgerManager, NoopRecovery, StubStorage};
    use bookkeeper_metadata::traits::LedgerManager;
    use bookkeeper_storage::ledger_storage::{LedgerStorage, StorageStateFlag};
    use std::sync::atomic::Ordering;

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
