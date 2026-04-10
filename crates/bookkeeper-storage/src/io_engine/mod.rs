pub mod fd_cache;
pub mod std_engine;

#[cfg(target_os = "linux")]
pub mod uring_engine;
#[cfg(target_os = "linux")]
pub mod uring_journal;
#[cfg(target_os = "linux")]
pub mod uring_ledger;

use async_trait::async_trait;
use std::io;
use std::os::fd::RawFd;
use std::path::Path;
use std::sync::Arc;

/// Journal I/O operations: write entries then optionally fsync.
pub trait JournalIo: Send + Sync {
    /// Write all entries sequentially to `fd`, then call `fsync_data`.
    fn write_and_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()>;

    /// Write all entries sequentially to `fd` without fsync.
    /// Used when `journalSyncData=false` — data reaches the OS page cache
    /// but is not forced to disk. Durable against process crashes but not
    /// power loss. Matches Java's behavior when syncData is disabled.
    fn write_no_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()>;
}

/// Ledger (entry-log) I/O operations: batched writes and positional reads.
#[async_trait]
pub trait LedgerIo: Send + Sync {
    /// Write `entries` starting at `offset`. Returns the starting offset of each entry.
    async fn write_batch(&self, fd: RawFd, offset: u64, entries: Vec<Vec<u8>>) -> io::Result<Vec<u64>>;

    /// Read exactly `len` bytes starting at `offset`.
    async fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>>;

    /// Read `len` bytes at `offset` from entry log `log_id` in `entry_log_dir`.
    /// The engine may cache the file descriptor for future reads.
    async fn read_entry(&self, entry_log_dir: &Path, log_id: u32, offset: u64, len: usize) -> io::Result<Vec<u8>>;

    /// Read a framed entry: parse the 4-byte big-endian size prefix at
    /// `offset`, then read `size` bytes of entry data starting at
    /// `offset + 4`. Returns only the entry data (without the prefix).
    ///
    /// Default implementation does two calls. Engines can override to
    /// combine into a single I/O round-trip.
    async fn read_framed_entry(
        &self,
        entry_log_dir: &Path,
        log_id: u32,
        offset: u64,
        max_size: usize,
    ) -> io::Result<Vec<u8>> {
        let size_buf = self.read_entry(entry_log_dir, log_id, offset, 4).await?;
        if size_buf.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short size prefix read"));
        }
        let size = i32::from_be_bytes(size_buf[..4].try_into().unwrap()) as usize;
        if size > max_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("entry size {} exceeds max {}", size, max_size),
            ));
        }
        self.read_entry(entry_log_dir, log_id, offset + 4, size).await
    }

    /// Evict any cached file descriptor for `log_id`. Called by GC before
    /// deleting the entry log file. No-op if the fd is not cached.
    fn evict_fd(&self, _entry_log_dir: &Path, _log_id: u32) {}
}

/// Entry log filename for a given log_id (lowercase hex + `.log`).
pub fn entry_log_filename(log_id: u32) -> String {
    format!("{:x}.log", log_id)
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
