pub mod fd_cache;
pub mod std_engine;

#[cfg(target_os = "linux")]
pub mod uring_engine;
#[cfg(target_os = "linux")]
pub mod uring_journal;
#[cfg(target_os = "linux")]
pub mod uring_ledger;

use std::io;
use std::os::fd::RawFd;
use std::path::Path;
use std::sync::Arc;

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

    /// Read `len` bytes at `offset` from entry log `log_id` in `entry_log_dir`.
    /// The engine may cache the file descriptor for future reads.
    fn read_entry(&self, entry_log_dir: &Path, log_id: u32, offset: u64, len: usize) -> io::Result<Vec<u8>>;

    /// Evict any cached file descriptor for `log_id`. Called by GC before
    /// deleting the entry log file. No-op if the fd is not cached.
    fn evict_fd(&self, _entry_log_dir: &Path, _log_id: u32) {}
}

/// Returns `true` when the io_uring backend is available on this platform.
pub fn uring_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        // Probe whether the kernel supports io_uring by trying to build a
        // tiny ring.  If the syscall fails we fall back to std.
        io_uring::IoUring::<io_uring::squeue::Entry>::builder().build(2).is_ok()
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
///
/// Can be overridden via BOOKKEEPER_IO_ENGINE env var:
/// - "std": force StdEngine
/// - "uring": force UringEngine (Linux only, falls back to StdEngine on other platforms)
/// - "auto" or unset: auto-detect based on platform and kernel capabilities
pub fn select_engine() -> EngineSet {
    let override_val = std::env::var("BOOKKEEPER_IO_ENGINE").unwrap_or_default();
    match override_val.as_str() {
        "std" => {
            tracing::info!("BOOKKEEPER_IO_ENGINE=std — forcing StdEngine");
            let engine = Arc::new(std_engine::StdEngine);
            return EngineSet {
                journal: engine.clone() as Arc<dyn JournalIo>,
                ledger: engine as Arc<dyn LedgerIo>,
            };
        }
        "uring" => {
            #[cfg(target_os = "linux")]
            {
                tracing::info!("BOOKKEEPER_IO_ENGINE=uring — forcing UringEngine");
                let engine = uring_engine::UringEngine::new();
                return EngineSet {
                    journal: engine.journal,
                    ledger: engine.ledger,
                };
            }
            #[cfg(not(target_os = "linux"))]
            {
                tracing::warn!("BOOKKEEPER_IO_ENGINE=uring but not on Linux — using StdEngine");
                let engine = Arc::new(std_engine::StdEngine);
                return EngineSet {
                    journal: engine.clone() as Arc<dyn JournalIo>,
                    ledger: engine as Arc<dyn LedgerIo>,
                };
            }
        }
        _ => {}
    }

    // Auto-detect
    #[cfg(target_os = "linux")]
    {
        if uring_available() {
            tracing::info!("io_uring available — using UringEngine");
            let engine = uring_engine::UringEngine::new();
            return EngineSet {
                journal: engine.journal,
                ledger: engine.ledger,
            };
        }
    }
    tracing::info!("Using StdEngine (standard blocking I/O)");
    let engine = Arc::new(std_engine::StdEngine);
    EngineSet {
        journal: engine.clone() as Arc<dyn JournalIo>,
        ledger: engine as Arc<dyn LedgerIo>,
    }
}
