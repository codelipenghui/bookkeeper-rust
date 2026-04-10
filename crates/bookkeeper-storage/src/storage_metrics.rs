//! Prometheus metrics for DbLedgerStorage, matching Java BookKeeper's DbLedgerStorageStats.java.
//!
//! All metric names use the `bookie_` prefix to match Java's `BOOKIE_SCOPE`.
//! Hyphens in Java metric names are replaced with underscores for Prometheus compatibility.

use lazy_static::lazy_static;
use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge};

lazy_static! {
    // -----------------------------------------------------------------------
    // Latency histograms (OpStats in Java)
    // -----------------------------------------------------------------------

    /// Per-entry add latency in DbLedgerStorage (from start of add_entry to return).
    /// Matches Java's `add-entry`.
    pub static ref ADD_ENTRY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bookie_add_entry",
            "operation stats of adding entries to db ledger storage",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    /// Per-entry read latency (from start of get_entry to return).
    /// Matches Java's `read-entry`.
    pub static ref READ_ENTRY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bookie_read_entry",
            "operation stats of reading entries from db ledger storage",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    /// Total flush latency (from acquiring flush lock to completing all I/O).
    /// Matches Java's `flush`.
    pub static ref FLUSH: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bookie_flush",
            "operation stats of flushing write cache to entry log files",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    /// Time spent writing entries to the entry log file during flush.
    /// Matches Java's `flush-entrylog`.
    pub static ref FLUSH_ENTRYLOG: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bookie_flush_entrylog",
            "operation stats of flushing to the current entry log file",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    /// Time spent flushing the RocksDB locations index.
    /// Matches Java's `flush-locations-index`.
    pub static ref FLUSH_LOCATIONS_INDEX: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bookie_flush_locations_index",
            "operation stats of flushing to the locations index",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    /// Time spent flushing the RocksDB ledger index.
    /// Matches Java's `flush-ledger-index`.
    pub static ref FLUSH_LEDGER_INDEX: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bookie_flush_ledger_index",
            "operation stats of flushing to the ledger index",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    /// Distribution of bytes flushed per flush cycle.
    /// Matches Java's `flush-size`.
    pub static ref FLUSH_SIZE: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bookie_flush_size",
            "the distribution of number of bytes flushed from write cache to entry log files",
        )
        .buckets(size_buckets()),
    )
    .unwrap();

    /// Latency distribution of writes that were throttled (write cache was full,
    /// had to wait for flush).
    /// Matches Java's `throttled-write`.
    pub static ref THROTTLED_WRITE: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bookie_throttled_write",
            "the stats of throttled write due to write cache is full",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    // -----------------------------------------------------------------------
    // Counters
    // -----------------------------------------------------------------------

    /// Time spent reading from RocksDB locations index (nanoseconds accumulated).
    /// Matches Java's `read-locations-index-time`.
    pub static ref READ_LOCATIONS_INDEX_TIME: IntCounter = IntCounter::new(
        "bookie_read_locations_index_time",
        "time spent reading entries from the locations index of the db ledger storage engine",
    )
    .unwrap();

    /// Time spent reading from entry log files (nanoseconds accumulated).
    /// Matches Java's `read-entrylog-time`.
    pub static ref READ_ENTRYLOG_TIME: IntCounter = IntCounter::new(
        "bookie_read_entrylog_time",
        "time spent reading entries from the entry log files of the db ledger storage engine",
    )
    .unwrap();

    /// Number of throttled write requests (write cache was full).
    /// Matches Java's `throttled-write-requests`.
    pub static ref THROTTLED_WRITE_REQUESTS: IntCounter = IntCounter::new(
        "bookie_throttled_write_requests",
        "the number of requests throttled due to write cache is full",
    )
    .unwrap();

    /// Number of rejected writes (timeout after throttle).
    /// Matches Java's `rejected-write-requests`.
    pub static ref REJECTED_WRITE_REQUESTS: IntCounter = IntCounter::new(
        "bookie_rejected_write_requests",
        "the number of requests rejected due to write cache is full",
    )
    .unwrap();

    // -----------------------------------------------------------------------
    // Cache metrics (moved from bookkeeper-server metrics.rs)
    // Matches Java's DbLedgerStorageStats.java cache counters/gauges
    // -----------------------------------------------------------------------

    /// Write cache hit count. Matches Java's `write-cache-hits`.
    pub static ref WRITE_CACHE_HITS: IntCounter = IntCounter::new(
        "bookie_write_cache_hits", "Write cache hit count"
    ).unwrap();

    /// Write cache miss count. Matches Java's `write-cache-misses`.
    pub static ref WRITE_CACHE_MISSES: IntCounter = IntCounter::new(
        "bookie_write_cache_misses", "Write cache miss count"
    ).unwrap();

    /// Read cache hit count. Matches Java's `read-cache-hits`.
    pub static ref READ_CACHE_HITS: IntCounter = IntCounter::new(
        "bookie_read_cache_hits", "Read cache hit count"
    ).unwrap();

    /// Read cache miss count. Matches Java's `read-cache-misses`.
    pub static ref READ_CACHE_MISSES: IntCounter = IntCounter::new(
        "bookie_read_cache_misses", "Read cache miss count"
    ).unwrap();

    /// Write cache current size in bytes. Matches Java's `write-cache-size`.
    pub static ref WRITE_CACHE_SIZE: IntGauge = IntGauge::new(
        "bookie_write_cache_size", "Write cache current size in bytes"
    ).unwrap();

    /// Write cache current entry count. Matches Java's `write-cache-count`.
    pub static ref WRITE_CACHE_COUNT: IntGauge = IntGauge::new(
        "bookie_write_cache_count", "Write cache current entry count"
    ).unwrap();

    /// Read cache current size in bytes. Matches Java's `read-cache-size`.
    pub static ref READ_CACHE_SIZE: IntGauge = IntGauge::new(
        "bookie_read_cache_size", "Read cache current size in bytes"
    ).unwrap();

    /// Read cache current entry count. Matches Java's `read-cache-count`.
    pub static ref READ_CACHE_COUNT: IntGauge = IntGauge::new(
        "bookie_read_cache_count", "Read cache current entry count"
    ).unwrap();
}

/// Register all storage metrics with the given prometheus Registry.
pub fn register_storage_metrics(registry: &prometheus::Registry) {
    macro_rules! reg {
        ($metric:expr) => {
            let _ = registry.register(Box::new($metric.clone()));
        };
    }
    // Latency histograms
    reg!(ADD_ENTRY);
    reg!(READ_ENTRY);
    reg!(FLUSH);
    reg!(FLUSH_ENTRYLOG);
    reg!(FLUSH_LOCATIONS_INDEX);
    reg!(FLUSH_LEDGER_INDEX);
    reg!(FLUSH_SIZE);
    reg!(THROTTLED_WRITE);
    // Counters
    reg!(READ_LOCATIONS_INDEX_TIME);
    reg!(READ_ENTRYLOG_TIME);
    reg!(THROTTLED_WRITE_REQUESTS);
    reg!(REJECTED_WRITE_REQUESTS);
    // Cache
    reg!(WRITE_CACHE_HITS);
    reg!(WRITE_CACHE_MISSES);
    reg!(READ_CACHE_HITS);
    reg!(READ_CACHE_MISSES);
    reg!(WRITE_CACHE_SIZE);
    reg!(WRITE_CACHE_COUNT);
    reg!(READ_CACHE_SIZE);
    reg!(READ_CACHE_COUNT);
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
        16777216.0, 67108864.0, 268435456.0,
    ]
}
