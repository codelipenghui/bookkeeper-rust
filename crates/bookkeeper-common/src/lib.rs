pub mod error;
pub mod types;

/// Maximum number of CAS (compare-and-swap) retry attempts for metadata updates.
/// Used by close_ledger, recovery, ensemble change, and replication.
pub const MAX_CAS_RETRIES: usize = 10;
