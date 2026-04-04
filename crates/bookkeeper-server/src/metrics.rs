//! Prometheus metrics for the Rust bookie, matching Java BookKeeper's metric names.
//!
//! Java exposes metrics via StatsProvider → PrometheusMetricsProvider.
//! Metric names follow the pattern: `{scope}_{METRIC_NAME}`.

use lazy_static::lazy_static;
use prometheus::{
    self, Encoder, HistogramOpts, HistogramVec, IntCounter, IntGauge,
    Registry, TextEncoder,
};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // -----------------------------------------------------------------------
    // Request metrics (scope: bookkeeper_server)
    // -----------------------------------------------------------------------

    /// Add entry request latency (seconds).
    pub static ref ADD_ENTRY_REQUEST: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "bookkeeper_server_ADD_ENTRY_REQUEST",
            "Add entry request latency"
        )
        .buckets(latency_buckets()),
        &["success"]
    ).unwrap();

    /// Read entry request latency (seconds).
    pub static ref READ_ENTRY_REQUEST: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "bookkeeper_server_READ_ENTRY_REQUEST",
            "Read entry request latency"
        )
        .buckets(latency_buckets()),
        &["success"]
    ).unwrap();

    /// Rejected add requests.
    pub static ref ADD_ENTRY_REJECTED: IntCounter = IntCounter::new(
        "bookkeeper_server_ADD_ENTRY_REJECTED",
        "Number of rejected add entry requests"
    ).unwrap();

    /// Rejected read requests.
    pub static ref READ_ENTRY_REJECTED: IntCounter = IntCounter::new(
        "bookkeeper_server_READ_ENTRY_REJECTED",
        "Number of rejected read entry requests"
    ).unwrap();

    // -----------------------------------------------------------------------
    // Bookie-level storage metrics (scope: bookie)
    // -----------------------------------------------------------------------

    /// Total bytes written.
    pub static ref WRITE_BYTES: IntCounter = IntCounter::new(
        "bookie_WRITE_BYTES",
        "Total bytes written to bookie"
    ).unwrap();

    /// Total bytes read.
    pub static ref READ_BYTES: IntCounter = IntCounter::new(
        "bookie_READ_BYTES",
        "Total bytes read from bookie"
    ).unwrap();

    /// Add entry latency at the bookie level.
    pub static ref BOOKIE_ADD_ENTRY: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "bookie_BOOKIE_ADD_ENTRY",
            "Bookie add entry latency"
        )
        .buckets(latency_buckets()),
        &["success"]
    ).unwrap();

    /// Read entry latency at the bookie level.
    pub static ref BOOKIE_READ_ENTRY: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "bookie_BOOKIE_READ_ENTRY",
            "Bookie read entry latency"
        )
        .buckets(latency_buckets()),
        &["success"]
    ).unwrap();

    // -----------------------------------------------------------------------
    // Cache metrics (scope: bookie)
    // -----------------------------------------------------------------------

    /// Write cache hit count.
    pub static ref WRITE_CACHE_HITS: IntCounter = IntCounter::new(
        "bookie_write_cache_hits",
        "Write cache hit count"
    ).unwrap();

    /// Write cache miss count.
    pub static ref WRITE_CACHE_MISSES: IntCounter = IntCounter::new(
        "bookie_write_cache_misses",
        "Write cache miss count"
    ).unwrap();

    /// Read cache hit count.
    pub static ref READ_CACHE_HITS: IntCounter = IntCounter::new(
        "bookie_read_cache_hits",
        "Read cache hit count"
    ).unwrap();

    /// Read cache miss count.
    pub static ref READ_CACHE_MISSES: IntCounter = IntCounter::new(
        "bookie_read_cache_misses",
        "Read cache miss count"
    ).unwrap();

    // -----------------------------------------------------------------------
    // GC & Compaction metrics (scope: bookie)
    // -----------------------------------------------------------------------

    /// Active ledger count.
    pub static ref ACTIVE_LEDGER_TOTAL: IntGauge = IntGauge::new(
        "bookie_ACTIVE_LEDGER_TOTAL",
        "Current active ledger count"
    ).unwrap();

    /// Deleted ledger count.
    pub static ref DELETED_LEDGER_TOTAL: IntCounter = IntCounter::new(
        "bookie_DELETED_LEDGER_TOTAL",
        "Total ledgers deleted by GC"
    ).unwrap();

    /// Minor compaction count.
    pub static ref MINOR_COMPACTION_TOTAL: IntCounter = IntCounter::new(
        "bookie_MINOR_COMPACTION_TOTAL",
        "Total minor compactions performed"
    ).unwrap();

    /// Major compaction count.
    pub static ref MAJOR_COMPACTION_TOTAL: IntCounter = IntCounter::new(
        "bookie_MAJOR_COMPACTION_TOTAL",
        "Total major compactions performed"
    ).unwrap();

    /// Bytes reclaimed by deletion.
    pub static ref RECLAIMED_DELETION_SPACE_BYTES: IntCounter = IntCounter::new(
        "bookie_RECLAIMED_DELETION_SPACE_BYTES",
        "Bytes freed via entry log deletion"
    ).unwrap();

    /// Bytes reclaimed by compaction.
    pub static ref RECLAIMED_COMPACTION_SPACE_BYTES: IntCounter = IntCounter::new(
        "bookie_RECLAIMED_COMPACTION_SPACE_BYTES",
        "Bytes freed via entry log compaction"
    ).unwrap();

    /// Active entry log count.
    pub static ref ACTIVE_ENTRY_LOG_TOTAL: IntGauge = IntGauge::new(
        "bookie_ACTIVE_ENTRY_LOG_TOTAL",
        "Current active entry log count"
    ).unwrap();

    // -----------------------------------------------------------------------
    // Replication metrics (scope: replication)
    // -----------------------------------------------------------------------

    /// Number of underreplicated ledgers detected.
    pub static ref NUM_UNDER_REPLICATED_LEDGERS: IntCounter = IntCounter::new(
        "replication_auditor_NUM_UNDER_REPLICATED_LEDGERS",
        "Number of underreplicated ledgers detected"
    ).unwrap();

    /// Number of full or partial ledgers replicated.
    pub static ref NUM_LEDGERS_REPLICATED: IntCounter = IntCounter::new(
        "replication_replication_worker_NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED",
        "Number of ledgers replicated"
    ).unwrap();

    /// Entries read during replication.
    pub static ref REPLICATION_ENTRIES_READ: IntCounter = IntCounter::new(
        "replication_replication_worker_NUM_ENTRIES_READ",
        "Entries read during replication"
    ).unwrap();

    /// Entries written during replication.
    pub static ref REPLICATION_ENTRIES_WRITTEN: IntCounter = IntCounter::new(
        "replication_replication_worker_NUM_ENTRIES_WRITTEN",
        "Entries written during replication"
    ).unwrap();

    /// Replication exceptions.
    pub static ref REPLICATION_EXCEPTIONS: IntCounter = IntCounter::new(
        "replication_replication_worker_exceptions",
        "Replication exceptions"
    ).unwrap();

    // -----------------------------------------------------------------------
    // Server state metrics
    // -----------------------------------------------------------------------

    /// Bookie state: 1=running(writable), 2=readonly, 0=other.
    pub static ref BOOKIE_STATE: IntGauge = IntGauge::new(
        "bookie_SERVER_STATUS",
        "Bookie server status (1=writable, 2=readonly, 0=other)"
    ).unwrap();
}

/// Register all metrics with the registry.
pub fn register_metrics() {
    let _ = REGISTRY.register(Box::new(ADD_ENTRY_REQUEST.clone()));
    let _ = REGISTRY.register(Box::new(READ_ENTRY_REQUEST.clone()));
    let _ = REGISTRY.register(Box::new(ADD_ENTRY_REJECTED.clone()));
    let _ = REGISTRY.register(Box::new(READ_ENTRY_REJECTED.clone()));
    let _ = REGISTRY.register(Box::new(WRITE_BYTES.clone()));
    let _ = REGISTRY.register(Box::new(READ_BYTES.clone()));
    let _ = REGISTRY.register(Box::new(BOOKIE_ADD_ENTRY.clone()));
    let _ = REGISTRY.register(Box::new(BOOKIE_READ_ENTRY.clone()));
    let _ = REGISTRY.register(Box::new(WRITE_CACHE_HITS.clone()));
    let _ = REGISTRY.register(Box::new(WRITE_CACHE_MISSES.clone()));
    let _ = REGISTRY.register(Box::new(READ_CACHE_HITS.clone()));
    let _ = REGISTRY.register(Box::new(READ_CACHE_MISSES.clone()));
    let _ = REGISTRY.register(Box::new(ACTIVE_LEDGER_TOTAL.clone()));
    let _ = REGISTRY.register(Box::new(DELETED_LEDGER_TOTAL.clone()));
    let _ = REGISTRY.register(Box::new(MINOR_COMPACTION_TOTAL.clone()));
    let _ = REGISTRY.register(Box::new(MAJOR_COMPACTION_TOTAL.clone()));
    let _ = REGISTRY.register(Box::new(RECLAIMED_DELETION_SPACE_BYTES.clone()));
    let _ = REGISTRY.register(Box::new(RECLAIMED_COMPACTION_SPACE_BYTES.clone()));
    let _ = REGISTRY.register(Box::new(ACTIVE_ENTRY_LOG_TOTAL.clone()));
    let _ = REGISTRY.register(Box::new(NUM_UNDER_REPLICATED_LEDGERS.clone()));
    let _ = REGISTRY.register(Box::new(NUM_LEDGERS_REPLICATED.clone()));
    let _ = REGISTRY.register(Box::new(REPLICATION_ENTRIES_READ.clone()));
    let _ = REGISTRY.register(Box::new(REPLICATION_ENTRIES_WRITTEN.clone()));
    let _ = REGISTRY.register(Box::new(REPLICATION_EXCEPTIONS.clone()));
    let _ = REGISTRY.register(Box::new(BOOKIE_STATE.clone()));
}

/// Encode all metrics as Prometheus text format.
pub fn encode_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Latency histogram buckets (in seconds) matching common BookKeeper latencies.
fn latency_buckets() -> Vec<f64> {
    vec![
        0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}
