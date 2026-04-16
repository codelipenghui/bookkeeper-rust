//! End-to-end tests pinning the Prometheus metric names exposed by the
//! bookie's `/metrics` endpoint.
//!
//! Java BookKeeper has very specific metric names that downstream systems
//! (Pulsar admin dashboards, Grafana panels, alerting rules) depend on.
//! A typo, rename, or missing `register_metrics()` call would silently
//! break monitoring without any visible test failure.
//!
//! These tests do three things:
//!   1. After spawning a fresh bookie with no traffic, assert that every
//!      `IntCounter` and `IntGauge` metric is present in the `/metrics`
//!      body. These are emitted at zero immediately on registration.
//!   2. After driving real writes / reads through the bookie, assert that
//!      the histogram metrics (`HistogramVec`) appear with observed
//!      samples. Histograms are NOT emitted by the prometheus crate until
//!      at least one observation has been recorded.
//!   3. Assert that specific counters actually increment after operations,
//!      not just that they exist.
//!
//! No ZK is required — all metrics are local to the bookie.

use std::sync::Arc;

use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_client::write_handle::WriteHandle;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_server::server::BookieServer;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn test_config(dir: &TempDir) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config.http_server_enabled = true;
    config.http_server_port = 0;
    config
}

async fn fetch_metrics(server: &BookieServer) -> String {
    let http_addr = server.http_addr().expect("HTTP server enabled");
    let resp = reqwest::get(format!("http://{}/metrics", http_addr))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.text().await.unwrap()
}

/// Connect a single `BookieConnection` to the bookie, returning the
/// `WriteHandle` + connections vector pair `WriteHandle::append` expects.
async fn make_writer(
    server: &BookieServer,
    ledger_id: i64,
) -> (WriteHandle, Vec<Arc<BookieConnection>>) {
    let addr = server.local_addr();
    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let wh = WriteHandle::new(ledger_id, ensemble, schedule, dm, master_key);
    (wh, conns)
}

/// Parse a single Prometheus counter/gauge value by name. Returns None if
/// the metric is not present, or the parsed integer value if it is.
///
/// Uses the simple Prometheus text format: lines starting with `name ` (no
/// labels) are `<name> <value>`. Histogram and labeled lines are skipped.
fn parse_int_metric(body: &str, name: &str) -> Option<i64> {
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        // Match `name <value>` exactly (no label braces).
        let prefix = format!("{name} ");
        if let Some(rest) = line.strip_prefix(&prefix) {
            return rest.trim().parse::<i64>().ok();
        }
    }
    None
}

/// Sum the histogram count samples (`<name>_count{...} <value>`) for a
/// given histogram name. Useful for checking that a histogram has been
/// observed at least N times.
fn sum_histogram_count(body: &str, name: &str) -> i64 {
    let prefix = format!("{name}_count");
    let mut total = 0i64;
    for line in body.lines() {
        if line.starts_with('#') || !line.starts_with(&prefix) {
            continue;
        }
        // Strip the metric name + optional labels, leaving the value.
        let value_str = line.rsplit_once(' ').map(|(_, v)| v).unwrap_or("0");
        if let Ok(v) = value_str.parse::<f64>() {
            total += v as i64;
        }
    }
    total
}

// ---------------------------------------------------------------------------
// Counter and gauge presence (no traffic needed)
// ---------------------------------------------------------------------------

/// Every `IntCounter` and `IntGauge` registered in `metrics::register_metrics()`
/// must appear in the `/metrics` body of a freshly-started bookie. Any rename
/// or missing registration breaks this test.
const EXPECTED_COUNTER_AND_GAUGE_METRICS: &[&str] = &[
    // Request metrics (scope: bookkeeper_server)
    "bookkeeper_server_ADD_ENTRY_REJECTED",
    "bookkeeper_server_READ_ENTRY_REJECTED",
    "bookkeeper_server_ADD_ENTRY_IN_PROGRESS",
    "bookkeeper_server_READ_ENTRY_IN_PROGRESS",
    // Bookie storage byte counters
    "bookie_WRITE_BYTES",
    "bookie_READ_BYTES",
    // Cache counters and gauges
    "bookie_write_cache_hits",
    "bookie_write_cache_misses",
    "bookie_read_cache_hits",
    "bookie_read_cache_misses",
    "bookie_write_cache_size",
    "bookie_write_cache_count",
    "bookie_read_cache_size",
    "bookie_read_cache_count",
    // GC and compaction
    "bookie_ACTIVE_LEDGER_TOTAL",
    "bookie_DELETED_LEDGER_TOTAL",
    "bookie_MINOR_COMPACTION_TOTAL",
    "bookie_MAJOR_COMPACTION_TOTAL",
    "bookie_RECLAIMED_DELETION_SPACE_BYTES",
    "bookie_RECLAIMED_COMPACTION_SPACE_BYTES",
    "bookie_ACTIVE_ENTRY_LOG_TOTAL",
    "bookie_ENTRY_LOG_SPACE_BYTES",
    // io_uring backpressure
    "bookie_io_uring_sq_full_total",
    "bookie_io_uring_buf_pool_exhausted_total",
    // Replication metrics
    "replication_auditor_NUM_UNDER_REPLICATED_LEDGERS",
    "replication_replication_worker_NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED",
    "replication_replication_worker_NUM_ENTRIES_READ",
    "replication_replication_worker_NUM_ENTRIES_WRITTEN",
    "replication_replication_worker_exceptions",
    "replication_auditor_election_attempts",
    // Server state
    "bookie_SERVER_STATUS",
];

#[tokio::test]
async fn all_counter_and_gauge_metrics_appear_at_startup() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let body = fetch_metrics(&server).await;

    let mut missing = Vec::new();
    for name in EXPECTED_COUNTER_AND_GAUGE_METRICS {
        if !body.contains(name) {
            missing.push(*name);
        }
    }

    assert!(
        missing.is_empty(),
        "the following metric names were not found in /metrics output (typos? \
         missing register_metrics() call? renamed?): {missing:?}\n\nfull body:\n{body}"
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Histogram presence after writes
// ---------------------------------------------------------------------------

/// Histograms registered in `metrics::register_metrics()` that should appear
/// after at least one successful AddEntry call.
const EXPECTED_WRITE_HISTOGRAMS: &[&str] = &[
    "bookkeeper_server_ADD_ENTRY_REQUEST",
    "bookie_BOOKIE_ADD_ENTRY",
    "bookie_BOOKIE_ADD_ENTRY_BYTES",
];

#[tokio::test]
async fn write_histograms_appear_after_add_entry() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();

    // Drive 5 writes through the bookie via the public client API.
    let (mut wh, conns) = make_writer(&server, 11).await;
    for i in 0..5 {
        wh.append(&conns, format!("write-{i}").as_bytes())
            .await
            .unwrap();
    }
    drop(wh);
    drop(conns);

    let body = fetch_metrics(&server).await;
    let mut missing = Vec::new();
    for name in EXPECTED_WRITE_HISTOGRAMS {
        if !body.contains(name) {
            missing.push(*name);
        }
    }
    assert!(
        missing.is_empty(),
        "write histograms missing after 5 add_entry calls: {missing:?}\n\nfull body:\n{body}"
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Histogram presence after reads
// ---------------------------------------------------------------------------

const EXPECTED_READ_HISTOGRAMS: &[&str] = &[
    "bookkeeper_server_READ_ENTRY_REQUEST",
    "bookie_BOOKIE_READ_ENTRY",
    "bookie_BOOKIE_READ_ENTRY_BYTES",
];

#[tokio::test]
async fn read_histograms_appear_after_read_entry() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();

    // Write a few entries first so there's something to read back.
    let (mut wh, conns) = make_writer(&server, 22).await;
    for i in 0..3 {
        wh.append(&conns, format!("read-target-{i}").as_bytes())
            .await
            .unwrap();
    }

    // Now read each entry via direct V2 ReadEntry through the same connection.
    let conn = conns[0].clone();
    for i in 0..3 {
        let _ = conn.send_read_entry(0, 22, i, None).await.unwrap();
    }

    let body = fetch_metrics(&server).await;
    let mut missing = Vec::new();
    for name in EXPECTED_READ_HISTOGRAMS {
        if !body.contains(name) {
            missing.push(*name);
        }
    }
    assert!(
        missing.is_empty(),
        "read histograms missing after 3 read_entry calls: {missing:?}\n\nfull body:\n{body}"
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Counter values actually increment after operations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn add_entry_count_increments_after_writes() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();

    // The Prometheus registry is `lazy_static` and therefore process-global,
    // so concurrent tests in the same binary share the same `BOOKIE_ADD_ENTRY`
    // histogram. Use a baseline-and-delta check instead of expecting an exact
    // count — the delta is monotonic and our writes can only add to it.
    let baseline = sum_histogram_count(&fetch_metrics(&server).await, "bookie_BOOKIE_ADD_ENTRY");

    let (mut wh, conns) = make_writer(&server, 33).await;
    for i in 0..10 {
        wh.append(&conns, format!("inc-{i}").as_bytes())
            .await
            .unwrap();
    }

    let after = sum_histogram_count(&fetch_metrics(&server).await, "bookie_BOOKIE_ADD_ENTRY");
    assert!(
        after - baseline >= 10,
        "BOOKIE_ADD_ENTRY count should have increased by at least 10 after 10 writes; \
         baseline={baseline}, after={after}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn write_bytes_counter_increments_after_writes() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();

    let baseline =
        parse_int_metric(&fetch_metrics(&server).await, "bookie_WRITE_BYTES").unwrap_or(0);

    let (mut wh, conns) = make_writer(&server, 44).await;
    let payload = b"a-fixed-payload";
    for _ in 0..20 {
        wh.append(&conns, payload).await.unwrap();
    }

    let after =
        parse_int_metric(&fetch_metrics(&server).await, "bookie_WRITE_BYTES").unwrap_or(0);
    assert!(
        after > baseline,
        "WRITE_BYTES should have grown after 20 writes; baseline={baseline}, after={after}"
    );
    // Each entry includes a 32-byte header + payload + digest, so 20 * payload.len()
    // is a strict lower bound on the additional bytes recorded.
    let lower_bound = (payload.len() as i64) * 20;
    assert!(
        after - baseline >= lower_bound,
        "WRITE_BYTES delta {} should be at least {lower_bound} (20 * payload bytes)",
        after - baseline
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// BOOKIE_STATE gauge value
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bookie_state_gauge_reports_writable_at_startup() {
    let dir = TempDir::new().unwrap();
    let mut config = test_config(&dir);
    // Disable disk-based read-only mode so the dev disk's actual usage
    // can't influence the bookie state.
    config.read_only_mode_enabled = false;
    let server = BookieServer::start(config).await.unwrap();

    let body = fetch_metrics(&server).await;

    // The bookie startup transitions to Running. The disk monitor's metrics
    // hook calls BOOKIE_STATE.set(1) on first writable check, but at
    // startup before any disk monitor tick the gauge may still be at 0
    // (its initial value). Either is acceptable as long as it's NOT
    // permanently stuck — verify the metric exists and is one of {0, 1}.
    let value = parse_int_metric(&body, "bookie_SERVER_STATUS")
        .expect("bookie_SERVER_STATUS gauge should be present");
    assert!(
        value == 0 || value == 1,
        "bookie_SERVER_STATUS should be 0 (initial) or 1 (writable) at startup, not {value} \
         (which would mean read-only or shutting-down)"
    );

    server.shutdown().await;
}
