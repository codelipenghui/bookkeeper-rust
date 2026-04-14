//! Periodic bookie disk info reader for disk-weight-based placement.
//!
//! Matches Java's `BookieInfoReader`:
//! - Periodically polls all bookies via `GetBookieInfo` V2 request
//! - Collects free disk space and total capacity per bookie
//! - Feeds the info into the placement policy via `update_bookie_info`
//! - Triggers immediate scan on bookie membership changes

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::bookie_connection::{BookieConnection, ClientError};
use crate::placement::BookieInfo;
use bookkeeper_protocol::v2::response::V2Response;

/// Configuration for the bookie info reader.
#[derive(Debug, Clone)]
pub struct BookieInfoReaderConfig {
    /// Interval between periodic scans (seconds). Matches Java's `getBookieInfoIntervalSeconds`.
    pub interval_secs: u64,
    /// Retry interval on scan failure (seconds). Matches Java's `getBookieInfoRetryIntervalSeconds`.
    pub retry_interval_secs: u64,
    /// Timeout per bookie request (seconds). Matches Java's `getBookieInfoTimeoutSecs`.
    pub timeout_secs: u64,
}

impl Default for BookieInfoReaderConfig {
    fn default() -> Self {
        Self {
            interval_secs: 86400, // 24 hours
            retry_interval_secs: 60,
            timeout_secs: 5,
        }
    }
}

/// Fetch disk info from a single bookie. Returns `None` on error.
async fn fetch_bookie_info(
    conn: &BookieConnection,
    timeout: Duration,
) -> Option<BookieInfo> {
    // requested bitmask: 1=free space, 2=total capacity, 3=both
    let result = tokio::time::timeout(timeout, conn.send_get_bookie_info(3)).await;
    match result {
        Ok(Ok(V2Response::GetBookieInfo { free_disk_space, total_disk_capacity, .. })) => {
            Some(BookieInfo::new(free_disk_space, total_disk_capacity))
        }
        _ => None,
    }
}

/// Poll all bookies and collect disk info.
/// Errors for individual bookies are logged and skipped.
pub async fn poll_all_bookies(
    connections: &HashMap<String, Arc<BookieConnection>>,
    timeout: Duration,
) -> HashMap<String, BookieInfo> {
    let mut info = HashMap::with_capacity(connections.len());
    for (addr, conn) in connections {
        if let Some(bi) = fetch_bookie_info(conn, timeout).await {
            info.insert(addr.clone(), bi);
        } else {
            tracing::debug!("BookieInfoReader: failed to get info from {addr}, skipping");
        }
    }
    info
}

/// Run the bookie info reader loop. Periodically polls all bookies and
/// sends updated info through the channel.
///
/// The caller (e.g., `ZkBookKeeper`) receives the info via `info_rx` and
/// calls `placement.update_bookie_info(info)`.
///
/// Matches Java's `BookieInfoReader.run()` which periodically scans all
/// bookies at `getBookieInfoIntervalSeconds` and retries on failure.
pub async fn run_bookie_info_reader(
    config: BookieInfoReaderConfig,
    mut connections_rx: watch::Receiver<HashMap<String, Arc<BookieConnection>>>,
    info_tx: watch::Sender<HashMap<String, BookieInfo>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let timeout = Duration::from_secs(config.timeout_secs);
    let interval = Duration::from_secs(config.interval_secs);
    let retry_interval = Duration::from_secs(config.retry_interval_secs);

    // Initial scan
    {
        let conns = connections_rx.borrow().clone();
        if !conns.is_empty() {
            let info = poll_all_bookies(&conns, timeout).await;
            if !info.is_empty() {
                let _ = info_tx.send(info);
            }
        }
    }

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
            _ = connections_rx.changed() => {
                // Bookie list changed — immediate re-scan.
                // Matches Java's BookieInfoReader.availableBookiesChanged().
                let conns = connections_rx.borrow().clone();
                let info = poll_all_bookies(&conns, timeout).await;
                if info.is_empty() {
                    // Retry after short interval on empty result
                    tokio::time::sleep(retry_interval).await;
                    let conns = connections_rx.borrow().clone();
                    let info = poll_all_bookies(&conns, timeout).await;
                    if !info.is_empty() {
                        let _ = info_tx.send(info);
                    }
                } else {
                    let _ = info_tx.send(info);
                }
            }
            _ = tokio::time::sleep(interval) => {
                // Periodic scan
                let conns = connections_rx.borrow().clone();
                let info = poll_all_bookies(&conns, timeout).await;
                if info.is_empty() {
                    tokio::time::sleep(retry_interval).await;
                    let conns = connections_rx.borrow().clone();
                    let info = poll_all_bookies(&conns, timeout).await;
                    if !info.is_empty() {
                        let _ = info_tx.send(info);
                    }
                } else {
                    let _ = info_tx.send(info);
                }
            }
        }
    }
}
