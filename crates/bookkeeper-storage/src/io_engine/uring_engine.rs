//! Composite io_uring engine — Linux only.
//!
//! Wraps [`UringJournalIo`] and [`UringLedgerIo`], falling back to
//! [`StdEngine`] for either component if io_uring initialisation fails.

use std::sync::Arc;

use super::std_engine::StdEngine;
use super::uring_journal::UringJournalIo;
use super::uring_ledger::UringLedgerIo;
use super::{JournalIo, LedgerIo};

/// Holds independently-initialised journal and ledger I/O backends.
///
/// Each component may be either the io_uring implementation or the
/// blocking fallback, depending on whether the uring probe succeeded.
pub struct UringEngine {
    pub journal: Arc<dyn JournalIo>,
    pub ledger: Arc<dyn LedgerIo>,
}

impl UringEngine {
    /// Create a new `UringEngine`, falling back to [`StdEngine`] for any
    /// component whose io_uring probe fails.
    pub fn new() -> Self {
        let journal: Arc<dyn JournalIo> = match UringJournalIo::new() {
            Ok(j) => Arc::new(j),
            Err(e) => {
                tracing::warn!("uring journal init failed, falling back to std: {e}");
                Arc::new(StdEngine::new())
            }
        };
        let ledger: Arc<dyn LedgerIo> = match UringLedgerIo::new() {
            Ok(l) => Arc::new(l),
            Err(e) => {
                tracing::warn!("uring ledger init failed, falling back to std: {e}");
                Arc::new(StdEngine::new())
            }
        };
        Self { journal, ledger }
    }
}
