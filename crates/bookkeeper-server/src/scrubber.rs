//! Local consistency checker (ScrubberService).
//!
//! Matches Java's `ScrubberService` which periodically calls
//! `ledgerStorage.localConsistencyCheck()` to verify entry log integrity.
//!
//! Config keys:
//! - `localScrubInterval`: seconds between scrubs (0 = disabled, default)
//! - `localScrubRateLimit`: max entries/second (0 = unlimited)

use std::sync::Arc;
use std::time::Duration;

use bookkeeper_storage::ledger_storage::LedgerStorage;

/// Start the scrubber background task. Returns a `JoinHandle` that exits
/// cleanly when `shutdown_rx` changes, or can be aborted to stop the service.
///
/// The first scrub is delayed by a random jitter (50%-150% of interval)
/// to avoid thundering herd when multiple bookies restart simultaneously.
/// Matches Java's `ScrubberService.doStart()` random delay logic.
pub fn start_scrubber(
    storage: Arc<dyn LedgerStorage>,
    scrub_interval_secs: u64,
    rate_limit: u64,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Random jitter: 50%-150% of interval (matching Java)
        let jitter_factor = 0.5 + rand::random::<f64>();
        let initial_delay = Duration::from_secs_f64(scrub_interval_secs as f64 * jitter_factor);
        tracing::info!(
            "scrubber: first run in {:.0}s (interval={}s, rate_limit={}/s)",
            initial_delay.as_secs_f64(),
            scrub_interval_secs,
            if rate_limit == 0 { "unlimited".to_string() } else { rate_limit.to_string() }
        );
        tokio::select! {
            _ = tokio::time::sleep(initial_delay) => {}
            _ = shutdown_rx.changed() => return,
        }

        loop {
            tracing::info!("scrubber: starting local consistency check");
            // A scrub pass can be long (proportional to stored data); run it
            // under the shutdown race so shutdown isn't blocked on a full scan.
            tokio::select! {
                result = storage.local_consistency_check(rate_limit) => match result {
                    Ok(0) => {
                        tracing::info!("scrubber: completed — no errors found");
                    }
                    Ok(errors) => {
                        tracing::warn!("scrubber: completed — {errors} errors found");
                        crate::metrics::SCRUB_ERRORS.inc_by(errors as u64);
                    }
                    Err(e) => {
                        tracing::error!("scrubber: fatal error — {e}");
                    }
                },
                _ = shutdown_rx.changed() => return,
            }

            // Schedule next run with jitter
            let jitter = 0.5 + rand::random::<f64>();
            let next_delay = Duration::from_secs_f64(scrub_interval_secs as f64 * jitter);
            tokio::select! {
                _ = tokio::time::sleep(next_delay) => {}
                _ = shutdown_rx.changed() => return,
            }
        }
    })
}
