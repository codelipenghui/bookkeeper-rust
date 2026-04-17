//! Prometheus metrics for DbLedgerStorage, matching Java BookKeeper's DbLedgerStorageStats.java.
//!
//! All metric names use the `bookie_` prefix to match Java's `BOOKIE_SCOPE`.
//! Hyphens in Java metric names are replaced with underscores for Prometheus compatibility.

use lazy_static::lazy_static;
use prometheus::{Gauge, Histogram, HistogramOpts, IntCounter, IntGauge};

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

    /// Canary for `try_push_with_backpressure`. Increments only when all
    /// retry attempts (drain CQEs + retry) fail to free an SQ slot — i.e.,
    /// the kernel's submission queue stayed full across multiple
    /// submit-and-drain cycles. Should be 0 in normal operation. A sustained
    /// non-zero rate points at the helper or the kernel scheduling, not at
    /// `LEDGER_SQ_DEPTH` sizing — the original "raise the constant" response
    /// is no longer correct after the backpressure refactor.
    pub static ref IO_URING_SQ_FULL_TOTAL: IntCounter = IntCounter::new(
        "bookie_io_uring_sq_full_total",
        "io_uring SQ-full failures after backpressure retries (canary; should be 0)",
    )
    .unwrap();

    /// Canary for the `in_flight_pool` gate in `ledger_ring_loop`. With the
    /// gate, PoolReads are never pushed beyond the pool's capacity, so the
    /// kernel should never return `-ENOBUFS`. A non-zero value indicates the
    /// gate's counter is out of sync with the kernel's actual buffer state —
    /// suspect the counter accounting, not the pool size.
    pub static ref IO_URING_BUF_POOL_EXHAUSTED_TOTAL: IntCounter = IntCounter::new(
        "bookie_io_uring_buf_pool_exhausted_total",
        "io_uring buffer-pool ENOBUFS (canary; should be 0 — investigate gating logic)",
    )
    .unwrap();

    // -----------------------------------------------------------------------
    // Flush lock contention metrics (Rust-specific, not in Java)
    // -----------------------------------------------------------------------

    /// Time the write-lock is held during write cache swap in flush().
    /// This exclusive lock blocks all concurrent add_entry read-lock attempts.
    pub static ref FLUSH_CACHE_SWAP: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bookie_flush_cache_swap",
            "latency of the exclusive write-lock during write cache swap in flush",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    // -----------------------------------------------------------------------
    // Cache metrics (moved from bookkeeper-server metrics.rs)
    // Matches Java's DbLedgerStorageStats.java cache counters/gauges
    // -----------------------------------------------------------------------

    /// Write cache current size in bytes. Matches Java's `write-cache-size`.
    pub static ref WRITE_CACHE_SIZE: IntGauge = IntGauge::new(
        "bookie_write_cache_size", "Write cache current size in bytes"
    ).unwrap();

    /// Write cache current entry count. Matches Java's `write-cache-count`.
    pub static ref WRITE_CACHE_COUNT: IntGauge = IntGauge::new(
        "bookie_write_cache_count", "Write cache current entry count"
    ).unwrap();

    /// Write cache fill ratio (bytes_used / max_size) in [0.0, 1.0].
    /// Values approaching 1.0 precede add_entry back-pressure.
    pub static ref WRITE_CACHE_FILL_RATIO: Gauge = Gauge::new(
        "bookie_write_cache_fill_ratio",
        "Write cache fill ratio (bytes_used / max_size), in [0, 1]"
    ).unwrap();

    /// Read cache current size in bytes. Matches Java's `read-cache-size`.
    pub static ref READ_CACHE_SIZE: IntGauge = IntGauge::new(
        "bookie_read_cache_size", "Read cache current size in bytes"
    ).unwrap();

    /// Read cache current entry count. Matches Java's `read-cache-count`.
    pub static ref READ_CACHE_COUNT: IntGauge = IntGauge::new(
        "bookie_read_cache_count", "Read cache current entry count"
    ).unwrap();

    // -----------------------------------------------------------------------
    // GC metrics. Matches Java's GarbageCollectorStats.
    // -----------------------------------------------------------------------

    /// Number of ledgers deleted by GC. Matches Java's `gc-deleted-ledgers`.
    pub static ref GC_DELETED_LEDGERS: IntCounter = IntCounter::new(
        "bookie_gc_deleted_ledgers", "Number of ledgers deleted by GC"
    ).unwrap();

    /// Number of entry log files deleted (all entries dead). Matches Java's `gc-deleted-entry-logs`.
    pub static ref GC_DELETED_ENTRY_LOGS: IntCounter = IntCounter::new(
        "bookie_gc_deleted_entry_logs", "Number of entry log files deleted by GC"
    ).unwrap();

    /// Number of entry log files compacted. Matches Java's `gc-compacted-entry-logs`.
    pub static ref GC_COMPACTED_ENTRY_LOGS: IntCounter = IntCounter::new(
        "bookie_gc_compacted_entry_logs", "Number of entry log files compacted by GC"
    ).unwrap();

    /// Number of entries moved during compaction. Matches Java's `compaction-entries-count`.
    pub static ref GC_COMPACTED_ENTRIES: IntCounter = IntCounter::new(
        "bookie_gc_compacted_entries", "Number of entries relocated during compaction"
    ).unwrap();

    /// Total bytes written during compaction. Matches Java's `compaction-bytes-count`.
    pub static ref GC_COMPACTED_BYTES: IntCounter = IntCounter::new(
        "bookie_gc_compacted_bytes", "Total bytes written during compaction"
    ).unwrap();

    /// Compaction duration histogram (per-file). Matches Java's `compaction-time`.
    pub static ref GC_COMPACTION_DURATION: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bookie_gc_compaction_duration",
            "Time spent compacting individual entry log files",
        ).buckets(latency_buckets()),
    ).unwrap();

    /// Total GC cycle duration. Matches Java's `gc-time`.
    pub static ref GC_RUN_DURATION: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bookie_gc_run_duration",
            "Time spent in a full GC cycle",
        ).buckets(latency_buckets()),
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
    reg!(IO_URING_SQ_FULL_TOTAL);
    reg!(IO_URING_BUF_POOL_EXHAUSTED_TOTAL);
    // Flush lock contention
    reg!(FLUSH_CACHE_SWAP);
    // Cache
    reg!(WRITE_CACHE_SIZE);
    reg!(WRITE_CACHE_COUNT);
    reg!(WRITE_CACHE_FILL_RATIO);
    reg!(READ_CACHE_SIZE);
    reg!(READ_CACHE_COUNT);

    // GC metrics
    reg!(GC_DELETED_LEDGERS);
    reg!(GC_DELETED_ENTRY_LOGS);
    reg!(GC_COMPACTED_ENTRY_LOGS);
    reg!(GC_COMPACTED_ENTRIES);
    reg!(GC_COMPACTED_BYTES);
    reg!(GC_COMPACTION_DURATION);
    reg!(GC_RUN_DURATION);
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
