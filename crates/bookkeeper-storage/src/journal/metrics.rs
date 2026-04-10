//! Prometheus metrics for the journal, matching Java BookKeeper's JournalStats.java.
//!
//! All metric names use the `journal_JOURNAL_*` prefix to match Java's `JOURNAL_SCOPE`.

use lazy_static::lazy_static;
use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge};

lazy_static! {
    // -----------------------------------------------------------------------
    // Latency histograms (OpStats in Java)
    // -----------------------------------------------------------------------

    /// End-to-end add-entry latency: from log_add_entry() call to ACK received.
    pub static ref JOURNAL_ADD_ENTRY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "journal_JOURNAL_ADD_ENTRY",
            "End-to-end latency of recording addEntry requests in the journal",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    /// fdatasync / fsync latency only.
    pub static ref JOURNAL_SYNC: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "journal_JOURNAL_SYNC",
            "Latency of syncing data to journal disks",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    /// Time to write_all the buffered data to filesystem (NOT fsync).
    pub static ref JOURNAL_FLUSH_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "journal_JOURNAL_FLUSH_LATENCY",
            "Latency of flushing data from memory to filesystem (not yet fsynced)",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    /// Time from channel send to channel recv (queue wait time).
    pub static ref JOURNAL_QUEUE_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "journal_JOURNAL_QUEUE_LATENCY",
            "Latency of enqueuing requests to the journal",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    /// Time from dequeue to finish processing (write + optional fsync handoff).
    pub static ref JOURNAL_PROCESS_TIME_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "journal_JOURNAL_PROCESS_TIME_LATENCY",
            "Latency of processing requests in the journal (dequeue to finish)",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    /// Number of force-write requests grouped per sync.
    pub static ref JOURNAL_FORCE_WRITE_GROUPING_COUNT: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "journal_JOURNAL_FORCE_WRITE_GROUPING_COUNT",
            "Distribution of force write requests grouped in a force write",
        )
        .buckets(count_buckets()),
    )
    .unwrap();

    /// Number of entries in each flush batch.
    pub static ref JOURNAL_FORCE_WRITE_BATCH_ENTRIES: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "journal_JOURNAL_FORCE_WRITE_BATCH_ENTRIES",
            "Distribution of entries grouped into a force write request",
        )
        .buckets(count_buckets()),
    )
    .unwrap();

    /// Bytes in each flush batch.
    pub static ref JOURNAL_FORCE_WRITE_BATCH_BYTES: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "journal_JOURNAL_FORCE_WRITE_BATCH_BYTES",
            "Distribution of bytes grouped into a force write request",
        )
        .buckets(size_buckets()),
    )
    .unwrap();

    /// Time to create a new journal file.
    pub static ref JOURNAL_CREATION_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "journal_JOURNAL_CREATION_LATENCY",
            "Latency of creating journal files",
        )
        .buckets(latency_buckets()),
    )
    .unwrap();

    // -----------------------------------------------------------------------
    // Gauges
    // -----------------------------------------------------------------------

    /// Current journal queue depth.
    pub static ref JOURNAL_QUEUE_SIZE: IntGauge = IntGauge::new(
        "journal_JOURNAL_QUEUE_SIZE",
        "The journal queue size",
    )
    .unwrap();

    /// Force-write queue depth.
    pub static ref JOURNAL_FORCE_WRITE_QUEUE_SIZE: IntGauge = IntGauge::new(
        "journal_JOURNAL_FORCE_WRITE_QUEUE_SIZE",
        "The force write queue size",
    )
    .unwrap();

    // -----------------------------------------------------------------------
    // Counters
    // -----------------------------------------------------------------------

    /// Total bytes written to journal.
    pub static ref JOURNAL_WRITE_BYTES: IntCounter = IntCounter::new(
        "journal_JOURNAL_WRITE_BYTES",
        "Total bytes appended to the journal",
    )
    .unwrap();

    /// Flushes triggered by max_group_wait timeout.
    pub static ref JOURNAL_NUM_FLUSH_MAX_WAIT: IntCounter = IntCounter::new(
        "journal_JOURNAL_NUM_FLUSH_MAX_WAIT",
        "Number of journal flushes triggered by MAX_WAIT time",
    )
    .unwrap();

    /// Flushes triggered by buffered_writes_threshold.
    pub static ref JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES: IntCounter = IntCounter::new(
        "journal_JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES",
        "Number of journal flushes triggered by MAX_OUTSTANDING_BYTES",
    )
    .unwrap();

    /// Flushes triggered by empty queue (single writer, no more entries).
    pub static ref JOURNAL_NUM_FLUSH_EMPTY_QUEUE: IntCounter = IntCounter::new(
        "journal_JOURNAL_NUM_FLUSH_EMPTY_QUEUE",
        "Number of journal flushes triggered when journal queue becomes empty",
    )
    .unwrap();
}

/// Register all journal metrics with the given prometheus Registry.
pub fn register_journal_metrics(registry: &prometheus::Registry) {
    macro_rules! reg {
        ($metric:expr) => {
            let _ = registry.register(Box::new($metric.clone()));
        };
    }
    // Latency histograms
    reg!(JOURNAL_ADD_ENTRY);
    reg!(JOURNAL_SYNC);
    reg!(JOURNAL_FLUSH_LATENCY);
    reg!(JOURNAL_QUEUE_LATENCY);
    reg!(JOURNAL_PROCESS_TIME_LATENCY);
    reg!(JOURNAL_FORCE_WRITE_GROUPING_COUNT);
    reg!(JOURNAL_FORCE_WRITE_BATCH_ENTRIES);
    reg!(JOURNAL_FORCE_WRITE_BATCH_BYTES);
    reg!(JOURNAL_CREATION_LATENCY);
    // Gauges
    reg!(JOURNAL_QUEUE_SIZE);
    reg!(JOURNAL_FORCE_WRITE_QUEUE_SIZE);
    // Counters
    reg!(JOURNAL_WRITE_BYTES);
    reg!(JOURNAL_NUM_FLUSH_MAX_WAIT);
    reg!(JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES);
    reg!(JOURNAL_NUM_FLUSH_EMPTY_QUEUE);
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

/// Count histogram buckets (for grouping/batch counts).
fn count_buckets() -> Vec<f64> {
    vec![
        1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0,
    ]
}
