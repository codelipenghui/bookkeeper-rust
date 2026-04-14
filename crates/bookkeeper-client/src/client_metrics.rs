//! Prometheus metrics for the BookKeeper client, matching Java's BookKeeperClientStats.
//!
//! Metric names follow the `bookkeeper_client_` prefix convention matching Java's
//! `bookkeeper_client` scope in BookKeeperClientStats.java.

use lazy_static::lazy_static;
use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge};

/// Latency buckets (seconds) tuned for client-side operations.
/// Covers sub-millisecond to multi-second range.
fn latency_buckets() -> Vec<f64> {
    vec![
        0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}

lazy_static! {
    // -----------------------------------------------------------------------
    // Add entry metrics
    // -----------------------------------------------------------------------

    /// Add entry latency (seconds). Matches Java's `ADD_ENTRY` OpStatsLogger.
    pub static ref ADD_ENTRY: Histogram = Histogram::with_opts(
        HistogramOpts::new("bookkeeper_client_add_entry", "Add entry latency (seconds)")
            .buckets(latency_buckets())
    ).unwrap();

    /// Number of pending add operations. Matches Java's `NUM_PENDING_ADD` Gauge.
    pub static ref PENDING_ADDS: IntGauge = IntGauge::new(
        "bookkeeper_client_pending_adds", "Current pending add operations"
    ).unwrap();

    // -----------------------------------------------------------------------
    // Read entry metrics
    // -----------------------------------------------------------------------

    /// Read entry latency (seconds). Matches Java's `READ_ENTRY` OpStatsLogger.
    pub static ref READ_ENTRY: Histogram = Histogram::with_opts(
        HistogramOpts::new("bookkeeper_client_read_entry", "Read entry latency (seconds)")
            .buckets(latency_buckets())
    ).unwrap();

    /// Read last confirmed and entry (long poll) latency. Matches Java's
    /// `READ_LAST_CONFIRMED_AND_ENTRY` OpStatsLogger.
    pub static ref READ_LAST_CONFIRMED_AND_ENTRY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bookkeeper_client_read_last_confirmed_and_entry",
            "Long poll read latency (seconds)",
        ).buckets(latency_buckets())
    ).unwrap();

    // -----------------------------------------------------------------------
    // Ensemble change metrics
    // -----------------------------------------------------------------------

    /// Number of ensemble changes. Matches Java's `NUM_ENSEMBLE_CHANGE` Counter.
    pub static ref ENSEMBLE_CHANGE: IntCounter = IntCounter::new(
        "bookkeeper_client_ensemble_change", "Total ensemble changes"
    ).unwrap();

    // -----------------------------------------------------------------------
    // Speculative read metrics
    // -----------------------------------------------------------------------

    /// Number of speculative reads issued. Matches Java's `SPECULATIVE_READ_COUNT`.
    pub static ref SPECULATIVE_READ_COUNT: IntCounter = IntCounter::new(
        "bookkeeper_client_speculative_read_count", "Total speculative reads issued"
    ).unwrap();

    // -----------------------------------------------------------------------
    // LAC metrics
    // -----------------------------------------------------------------------

    /// Write LAC latency. Matches Java's `WRITE_LAC` OpStatsLogger.
    pub static ref WRITE_LAC: Histogram = Histogram::with_opts(
        HistogramOpts::new("bookkeeper_client_write_lac", "Write LAC latency (seconds)")
            .buckets(latency_buckets())
    ).unwrap();

    /// Read LAC latency. Matches Java's `READ_LAC` OpStatsLogger.
    pub static ref READ_LAC: Histogram = Histogram::with_opts(
        HistogramOpts::new("bookkeeper_client_read_lac", "Read LAC latency (seconds)")
            .buckets(latency_buckets())
    ).unwrap();

    // -----------------------------------------------------------------------
    // Force metrics
    // -----------------------------------------------------------------------

    /// Force (ForceLedger) latency. Matches Java's `FORCE` OpStatsLogger.
    pub static ref FORCE: Histogram = Histogram::with_opts(
        HistogramOpts::new("bookkeeper_client_force", "Force ledger latency (seconds)")
            .buckets(latency_buckets())
    ).unwrap();

    // -----------------------------------------------------------------------
    // Error / anomaly counters
    // -----------------------------------------------------------------------

    /// Digest mismatches on read. Matches Java's `READ_ENTRY_DM`.
    pub static ref READ_ENTRY_DIGEST_MISMATCH: IntCounter = IntCounter::new(
        "bookkeeper_client_read_entry_dm", "Read entry digest mismatch count"
    ).unwrap();

    /// LAC piggyback cache hits. Matches Java's `LAC_UPDATE_HITS`.
    pub static ref LAC_UPDATE_HITS: IntCounter = IntCounter::new(
        "bookkeeper_client_lac_update_hits", "LAC piggyback cache hits"
    ).unwrap();

    /// LAC piggyback cache misses. Matches Java's `LAC_UPDATE_MISSES`.
    pub static ref LAC_UPDATE_MISSES: IntCounter = IntCounter::new(
        "bookkeeper_client_lac_update_misses", "LAC piggyback cache misses"
    ).unwrap();
}

/// Register all client metrics with the given prometheus registry.
pub fn register_all(registry: &prometheus::Registry) {
    let _ = registry.register(Box::new(ADD_ENTRY.clone()));
    let _ = registry.register(Box::new(PENDING_ADDS.clone()));
    let _ = registry.register(Box::new(READ_ENTRY.clone()));
    let _ = registry.register(Box::new(READ_LAST_CONFIRMED_AND_ENTRY.clone()));
    let _ = registry.register(Box::new(ENSEMBLE_CHANGE.clone()));
    let _ = registry.register(Box::new(SPECULATIVE_READ_COUNT.clone()));
    let _ = registry.register(Box::new(WRITE_LAC.clone()));
    let _ = registry.register(Box::new(READ_LAC.clone()));
    let _ = registry.register(Box::new(FORCE.clone()));
    let _ = registry.register(Box::new(READ_ENTRY_DIGEST_MISMATCH.clone()));
    let _ = registry.register(Box::new(LAC_UPDATE_HITS.clone()));
    let _ = registry.register(Box::new(LAC_UPDATE_MISSES.clone()));
}
