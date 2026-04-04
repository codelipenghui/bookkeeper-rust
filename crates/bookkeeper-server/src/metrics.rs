//! Prometheus metrics for the Rust bookie, matching Java BookKeeper's metric names.
//!
//! Java uses OpStats → Prometheus Summary. Rust uses Prometheus Histogram which is the
//! modern recommended type. Both produce compatible _count and _sum suffixes.
//! Quantiles are computed at query time via histogram_quantile() in Prometheus.

use lazy_static::lazy_static;
use prometheus::{
    self, Encoder, HistogramOpts, HistogramVec, IntCounter, IntGauge, Registry, TextEncoder,
};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // -----------------------------------------------------------------------
    // Request metrics (scope: bookkeeper_server)
    // Matches Java's RequestStats.java
    // -----------------------------------------------------------------------

    pub static ref ADD_ENTRY_REQUEST: HistogramVec = HistogramVec::new(
        HistogramOpts::new("bookkeeper_server_ADD_ENTRY_REQUEST", "Add entry request latency")
            .buckets(latency_buckets()),
        &["success"]
    ).unwrap();

    pub static ref READ_ENTRY_REQUEST: HistogramVec = HistogramVec::new(
        HistogramOpts::new("bookkeeper_server_READ_ENTRY_REQUEST", "Read entry request latency")
            .buckets(latency_buckets()),
        &["success"]
    ).unwrap();

    pub static ref ADD_ENTRY_REJECTED: IntCounter = IntCounter::new(
        "bookkeeper_server_ADD_ENTRY_REJECTED", "Rejected add entry requests"
    ).unwrap();

    pub static ref READ_ENTRY_REJECTED: IntCounter = IntCounter::new(
        "bookkeeper_server_READ_ENTRY_REJECTED", "Rejected read entry requests"
    ).unwrap();

    /// In-flight add requests. Matches Java's ADD_ENTRY_IN_PROGRESS.
    pub static ref ADD_ENTRY_IN_PROGRESS: IntGauge = IntGauge::new(
        "bookkeeper_server_ADD_ENTRY_IN_PROGRESS", "Current in-flight add requests"
    ).unwrap();

    /// In-flight read requests. Matches Java's READ_ENTRY_IN_PROGRESS.
    pub static ref READ_ENTRY_IN_PROGRESS: IntGauge = IntGauge::new(
        "bookkeeper_server_READ_ENTRY_IN_PROGRESS", "Current in-flight read requests"
    ).unwrap();

    /// Fenced read request latency.
    pub static ref READ_ENTRY_FENCE_REQUEST: HistogramVec = HistogramVec::new(
        HistogramOpts::new("bookkeeper_server_READ_ENTRY_FENCE_REQUEST", "Fenced read request latency")
            .buckets(latency_buckets()),
        &["success"]
    ).unwrap();

    // -----------------------------------------------------------------------
    // Bookie storage metrics (scope: bookie)
    // Matches Java's BookieStats.java
    // -----------------------------------------------------------------------

    pub static ref WRITE_BYTES: IntCounter = IntCounter::new(
        "bookie_WRITE_BYTES", "Total bytes written to bookie"
    ).unwrap();

    pub static ref READ_BYTES: IntCounter = IntCounter::new(
        "bookie_READ_BYTES", "Total bytes read from bookie"
    ).unwrap();

    pub static ref BOOKIE_ADD_ENTRY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("bookie_BOOKIE_ADD_ENTRY", "Bookie add entry latency")
            .buckets(latency_buckets()),
        &["success"]
    ).unwrap();

    pub static ref BOOKIE_READ_ENTRY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("bookie_BOOKIE_READ_ENTRY", "Bookie read entry latency")
            .buckets(latency_buckets()),
        &["success"]
    ).unwrap();

    /// Recovery add entry latency. Matches Java's BOOKIE_RECOVERY_ADD_ENTRY.
    pub static ref BOOKIE_RECOVERY_ADD_ENTRY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("bookie_BOOKIE_RECOVERY_ADD_ENTRY", "Recovery add entry latency")
            .buckets(latency_buckets()),
        &["success"]
    ).unwrap();

    /// Add entry bytes distribution. Matches Java's BOOKIE_ADD_ENTRY_BYTES.
    pub static ref BOOKIE_ADD_ENTRY_BYTES: HistogramVec = HistogramVec::new(
        HistogramOpts::new("bookie_BOOKIE_ADD_ENTRY_BYTES", "Add entry bytes distribution")
            .buckets(size_buckets()),
        &["success"]
    ).unwrap();

    /// Read entry bytes distribution. Matches Java's BOOKIE_READ_ENTRY_BYTES.
    pub static ref BOOKIE_READ_ENTRY_BYTES: HistogramVec = HistogramVec::new(
        HistogramOpts::new("bookie_BOOKIE_READ_ENTRY_BYTES", "Read entry bytes distribution")
            .buckets(size_buckets()),
        &["success"]
    ).unwrap();

    // -----------------------------------------------------------------------
    // Cache metrics (scope: bookie)
    // Matches Java's DbLedgerStorageStats.java
    // -----------------------------------------------------------------------

    pub static ref WRITE_CACHE_HITS: IntCounter = IntCounter::new(
        "bookie_write_cache_hits", "Write cache hit count"
    ).unwrap();

    pub static ref WRITE_CACHE_MISSES: IntCounter = IntCounter::new(
        "bookie_write_cache_misses", "Write cache miss count"
    ).unwrap();

    pub static ref READ_CACHE_HITS: IntCounter = IntCounter::new(
        "bookie_read_cache_hits", "Read cache hit count"
    ).unwrap();

    pub static ref READ_CACHE_MISSES: IntCounter = IntCounter::new(
        "bookie_read_cache_misses", "Read cache miss count"
    ).unwrap();

    /// Write cache current size in bytes. Matches Java's write_cache_size.
    pub static ref WRITE_CACHE_SIZE: IntGauge = IntGauge::new(
        "bookie_write_cache_size", "Write cache current size in bytes"
    ).unwrap();

    /// Write cache current entry count. Matches Java's write_cache_count.
    pub static ref WRITE_CACHE_COUNT: IntGauge = IntGauge::new(
        "bookie_write_cache_count", "Write cache current entry count"
    ).unwrap();

    /// Read cache current size in bytes. Matches Java's read_cache_size.
    pub static ref READ_CACHE_SIZE: IntGauge = IntGauge::new(
        "bookie_read_cache_size", "Read cache current size in bytes"
    ).unwrap();

    /// Read cache current entry count. Matches Java's read_cache_count.
    pub static ref READ_CACHE_COUNT: IntGauge = IntGauge::new(
        "bookie_read_cache_count", "Read cache current entry count"
    ).unwrap();

    // -----------------------------------------------------------------------
    // GC & Compaction metrics (scope: bookie)
    // Matches Java's GarbageCollectorStats.java
    // -----------------------------------------------------------------------

    pub static ref ACTIVE_LEDGER_TOTAL: IntGauge = IntGauge::new(
        "bookie_ACTIVE_LEDGER_TOTAL", "Current active ledger count"
    ).unwrap();

    pub static ref DELETED_LEDGER_TOTAL: IntCounter = IntCounter::new(
        "bookie_DELETED_LEDGER_TOTAL", "Total ledgers deleted by GC"
    ).unwrap();

    pub static ref MINOR_COMPACTION_TOTAL: IntCounter = IntCounter::new(
        "bookie_MINOR_COMPACTION_TOTAL", "Total minor compactions"
    ).unwrap();

    pub static ref MAJOR_COMPACTION_TOTAL: IntCounter = IntCounter::new(
        "bookie_MAJOR_COMPACTION_TOTAL", "Total major compactions"
    ).unwrap();

    pub static ref RECLAIMED_DELETION_SPACE_BYTES: IntCounter = IntCounter::new(
        "bookie_RECLAIMED_DELETION_SPACE_BYTES", "Bytes freed via deletion"
    ).unwrap();

    pub static ref RECLAIMED_COMPACTION_SPACE_BYTES: IntCounter = IntCounter::new(
        "bookie_RECLAIMED_COMPACTION_SPACE_BYTES", "Bytes freed via compaction"
    ).unwrap();

    pub static ref ACTIVE_ENTRY_LOG_TOTAL: IntGauge = IntGauge::new(
        "bookie_ACTIVE_ENTRY_LOG_TOTAL", "Current active entry log count"
    ).unwrap();

    pub static ref ENTRY_LOG_SPACE_BYTES: IntGauge = IntGauge::new(
        "bookie_ENTRY_LOG_SPACE_BYTES", "Total entry log space in bytes"
    ).unwrap();

    // -----------------------------------------------------------------------
    // Replication metrics (scope: replication)
    // Matches Java's ReplicationStats.java
    // -----------------------------------------------------------------------

    pub static ref NUM_UNDER_REPLICATED_LEDGERS: IntCounter = IntCounter::new(
        "replication_auditor_NUM_UNDER_REPLICATED_LEDGERS", "Underreplicated ledgers detected"
    ).unwrap();

    pub static ref NUM_LEDGERS_REPLICATED: IntCounter = IntCounter::new(
        "replication_replication_worker_NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED", "Ledgers replicated"
    ).unwrap();

    pub static ref REPLICATION_ENTRIES_READ: IntCounter = IntCounter::new(
        "replication_replication_worker_NUM_ENTRIES_READ", "Entries read during replication"
    ).unwrap();

    pub static ref REPLICATION_ENTRIES_WRITTEN: IntCounter = IntCounter::new(
        "replication_replication_worker_NUM_ENTRIES_WRITTEN", "Entries written during replication"
    ).unwrap();

    pub static ref REPLICATION_EXCEPTIONS: IntCounter = IntCounter::new(
        "replication_replication_worker_exceptions", "Replication exceptions"
    ).unwrap();

    /// Auditor election attempts. Matches Java's ELECTION_ATTEMPTS.
    pub static ref ELECTION_ATTEMPTS: IntCounter = IntCounter::new(
        "replication_auditor_election_attempts", "Auditor election attempts"
    ).unwrap();

    // -----------------------------------------------------------------------
    // Server state
    // -----------------------------------------------------------------------

    pub static ref BOOKIE_STATE: IntGauge = IntGauge::new(
        "bookie_SERVER_STATUS", "Bookie server status (1=writable, 2=readonly, 0=other)"
    ).unwrap();
}

/// Register all metrics with the registry.
pub fn register_metrics() {
    macro_rules! reg {
        ($metric:expr) => { let _ = REGISTRY.register(Box::new($metric.clone())); };
    }
    // Request metrics
    reg!(ADD_ENTRY_REQUEST);
    reg!(READ_ENTRY_REQUEST);
    reg!(ADD_ENTRY_REJECTED);
    reg!(READ_ENTRY_REJECTED);
    reg!(ADD_ENTRY_IN_PROGRESS);
    reg!(READ_ENTRY_IN_PROGRESS);
    reg!(READ_ENTRY_FENCE_REQUEST);
    // Bookie storage
    reg!(WRITE_BYTES);
    reg!(READ_BYTES);
    reg!(BOOKIE_ADD_ENTRY);
    reg!(BOOKIE_READ_ENTRY);
    reg!(BOOKIE_RECOVERY_ADD_ENTRY);
    reg!(BOOKIE_ADD_ENTRY_BYTES);
    reg!(BOOKIE_READ_ENTRY_BYTES);
    // Cache
    reg!(WRITE_CACHE_HITS);
    reg!(WRITE_CACHE_MISSES);
    reg!(READ_CACHE_HITS);
    reg!(READ_CACHE_MISSES);
    reg!(WRITE_CACHE_SIZE);
    reg!(WRITE_CACHE_COUNT);
    reg!(READ_CACHE_SIZE);
    reg!(READ_CACHE_COUNT);
    // GC
    reg!(ACTIVE_LEDGER_TOTAL);
    reg!(DELETED_LEDGER_TOTAL);
    reg!(MINOR_COMPACTION_TOTAL);
    reg!(MAJOR_COMPACTION_TOTAL);
    reg!(RECLAIMED_DELETION_SPACE_BYTES);
    reg!(RECLAIMED_COMPACTION_SPACE_BYTES);
    reg!(ACTIVE_ENTRY_LOG_TOTAL);
    reg!(ENTRY_LOG_SPACE_BYTES);
    // Replication
    reg!(NUM_UNDER_REPLICATED_LEDGERS);
    reg!(NUM_LEDGERS_REPLICATED);
    reg!(REPLICATION_ENTRIES_READ);
    reg!(REPLICATION_ENTRIES_WRITTEN);
    reg!(REPLICATION_EXCEPTIONS);
    reg!(ELECTION_ATTEMPTS);
    // Server
    reg!(BOOKIE_STATE);
}

/// Encode all metrics as Prometheus text format.
pub fn encode_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Latency histogram buckets (seconds).
fn latency_buckets() -> Vec<f64> {
    vec![
        0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}

/// Size histogram buckets (bytes).
fn size_buckets() -> Vec<f64> {
    vec![
        128.0, 256.0, 512.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0,
    ]
}
