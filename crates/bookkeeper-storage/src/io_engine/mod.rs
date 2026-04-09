pub mod std_engine;

#[cfg(target_os = "linux")]
pub mod uring_engine;
#[cfg(target_os = "linux")]
pub mod uring_journal;
#[cfg(target_os = "linux")]
pub mod uring_ledger;

use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;

/// Core abstraction over I/O backends (blocking std vs io_uring).
pub trait IoEngine: Send + Sync + 'static {
    fn journal(&self) -> &dyn JournalIo;
    fn ledger(&self) -> &dyn LedgerIo;
}

/// Journal I/O operations: write entries then fsync.
pub trait JournalIo: Send + Sync {
    /// Write all entries sequentially to `fd`, then call `fsync_data`.
    fn write_and_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()>;
}

/// Ledger (entry-log) I/O operations: batched writes and positional reads.
pub trait LedgerIo: Send + Sync {
    /// Write `entries` starting at `offset`. Returns the starting offset of each entry.
    fn write_batch(&self, fd: RawFd, offset: u64, entries: &[&[u8]]) -> io::Result<Vec<u64>>;

    /// Read exactly `len` bytes starting at `offset`.
    fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>>;
}

/// Returns `true` when the io_uring backend is available on this platform.
pub fn uring_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        // Probe whether the kernel supports io_uring by trying to build a
        // tiny ring.  If the syscall fails we fall back to std.
        io_uring::IoUring::builder().build(2).is_ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Pre-split `Arc`s for the journal and ledger I/O backends.
///
/// Avoids the need to extract trait-object `Arc`s from a single
/// `Arc<dyn IoEngine>` later — callers simply clone the relevant field.
pub struct EngineSet {
    pub journal: Arc<dyn JournalIo>,
    pub ledger: Arc<dyn LedgerIo>,
}

/// Select the best available I/O engine for this platform.
///
/// On Linux with a capable kernel this returns the io_uring engine;
/// everywhere else (or when io_uring probing fails) it returns the
/// blocking std engine.
pub fn select_engine() -> EngineSet {
    #[cfg(target_os = "linux")]
    {
        if uring_available() {
            let engine = Arc::new(uring_engine::UringEngine::new());
            return EngineSet {
                journal: engine.clone() as Arc<dyn JournalIo>,
                ledger: engine as Arc<dyn LedgerIo>,
            };
        }
    }
    let engine = Arc::new(std_engine::StdEngine);
    EngineSet {
        journal: engine.clone() as Arc<dyn JournalIo>,
        ledger: engine as Arc<dyn LedgerIo>,
    }
}
