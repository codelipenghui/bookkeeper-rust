pub mod disk;
pub mod error;
pub mod proto_text;
pub mod types;

/// Maximum number of CAS (compare-and-swap) retry attempts for metadata updates.
/// Used by close_ledger, recovery, ensemble change, and replication.
pub const MAX_CAS_RETRIES: usize = 10;

/// Current wall-clock time in milliseconds since the Unix epoch.
/// Returns 0 if the system clock is before the epoch (shouldn't happen).
pub fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
