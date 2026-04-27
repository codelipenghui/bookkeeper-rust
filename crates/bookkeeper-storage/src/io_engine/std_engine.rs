//! Standard blocking I/O engine backed by `std::fs` / `std::io`.

use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::os::fd::RawFd;
use std::path::Path;
use std::sync::Arc;

#[cfg(unix)]
use std::os::unix::io::FromRawFd;

use async_trait::async_trait;

use super::{JournalIo, LedgerIo};

const READ_FD_CACHE_CAPACITY: usize = 64;

/// Blocking I/O engine that uses standard `std::fs` / `std::io` operations.
///
/// Implements both `JournalIo` and `LedgerIo`, so an `Arc<StdEngine>` can
/// be coerced to either trait object. Journal write paths borrow the
/// caller's fd via `ManuallyDrop<File>` (the journal owns its own
/// `File` and is responsible for closing it).
///
/// Caches open file handles for entry-log reads. Handles are returned as
/// `Arc<File>`, so a caller's read remains valid even if a parallel
/// caller's miss triggers eviction — the evicted entry's `File` stays
/// alive (and its fd stays open) until the last `Arc` is dropped.
pub struct StdEngine {
    read_cache: std::sync::Mutex<ReadFdCache>,
}

/// Approximate-LRU bounded fd cache. `tick` is bumped on every access
/// and stamped onto the entry; eviction picks the entry with the
/// smallest `last_used` (linear scan, fine at capacity 64).
struct ReadFdCache {
    map: std::collections::HashMap<(u64, u32), CacheSlot>,
    tick: u64,
}

struct CacheSlot {
    file: Arc<File>,
    last_used: u64,
}

impl ReadFdCache {
    fn new() -> Self {
        Self {
            map: std::collections::HashMap::with_capacity(READ_FD_CACHE_CAPACITY),
            tick: 0,
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.tick = self.tick.wrapping_add(1);
        self.tick
    }

    fn evict_one(&mut self) {
        // Linear scan for the smallest last_used. n is bounded by
        // READ_FD_CACHE_CAPACITY (64) so this stays cheap. Dropping the
        // CacheSlot here decrements the Arc; if any caller still holds
        // an Arc<File> for this key, the underlying fd remains open
        // until they finish their read.
        if let Some(victim_key) = self
            .map
            .iter()
            .min_by_key(|(_, slot)| slot.last_used)
            .map(|(k, _)| *k)
        {
            self.map.remove(&victim_key);
        }
    }
}

impl StdEngine {
    pub fn new() -> Self {
        Self {
            read_cache: std::sync::Mutex::new(ReadFdCache::new()),
        }
    }

    /// Get a cached `Arc<File>` for the (dir, log_id) pair, opening if
    /// necessary. Returning an `Arc<File>` (rather than a `RawFd`)
    /// pins the fd for the caller — concurrent eviction can drop the
    /// cache slot, but the underlying `File` stays alive until every
    /// outstanding `Arc` is dropped.
    fn get_or_open_cached(
        &self,
        entry_log_dir: &Path,
        log_id: u32,
    ) -> io::Result<Arc<File>> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        entry_log_dir.hash(&mut hasher);
        let dir_hash = hasher.finish();
        let key = (dir_hash, log_id);

        let mut cache = self.read_cache.lock()
            .unwrap_or_else(|p| p.into_inner());

        let now = cache.next_tick();
        if let Some(slot) = cache.map.get_mut(&key) {
            slot.last_used = now;
            return Ok(Arc::clone(&slot.file));
        }

        // Open outside the existing cache slot — the lock stays held to
        // serialise insertion, but we accept that two concurrent misses
        // on the same key will both open the file. The second insert
        // overwrites the first; both `Arc<File>`s remain valid for
        // their respective callers.
        let path = entry_log_dir.join(super::entry_log_filename(log_id));
        let file = Arc::new(File::open(&path)?);

        if cache.map.len() >= READ_FD_CACHE_CAPACITY {
            cache.evict_one();
        }
        cache.map.insert(
            key,
            CacheSlot {
                file: Arc::clone(&file),
                last_used: now,
            },
        );
        Ok(file)
    }
}

/// Borrow a raw fd as a [`File`] for the duration of a closure.
///
/// Uses [`ManuallyDrop`] so the fd is **never** closed — even if `f`
/// panics. The previous `from_raw_fd` + `mem::forget` pattern was
/// unsound: a panic between construction and forget would run `File`'s
/// `Drop`, closing the caller's fd.
fn with_file<F, R>(fd: RawFd, f: F) -> R
where
    F: FnOnce(&mut File) -> R,
{
    let mut file = std::mem::ManuallyDrop::new(unsafe { File::from_raw_fd(fd) });
    f(&mut file)
}

impl JournalIo for StdEngine {
    fn write_and_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()> {
        with_file(fd, |file| {
            for entry in entries {
                file.write_all(entry)?;
            }
            file.sync_data()
        })
    }

    fn write_no_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()> {
        with_file(fd, |file| {
            for entry in entries {
                file.write_all(entry)?;
            }
            Ok(())
        })
    }
}

#[async_trait]
impl LedgerIo for StdEngine {
    async fn write_batch(&self, fd: RawFd, offset: u64, entries: Vec<Vec<u8>>) -> io::Result<Vec<u64>> {
        tokio::task::spawn_blocking(move || {
            with_file(fd, |file| {
                file.seek(SeekFrom::Start(offset))?;
                let mut current = offset;
                let mut offsets = Vec::with_capacity(entries.len());
                for entry in &entries {
                    offsets.push(current);
                    file.write_all(entry)?;
                    current += entry.len() as u64;
                }
                Ok(offsets)
            })
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    }

    async fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        tokio::task::spawn_blocking(move || {
            with_file(fd, |file| {
                // Positional read (pread(2)) — does NOT modify the file's
                // internal seek position, so concurrent reads on the same
                // cached fd don't race. Without this, multiple parallel
                // reads clobber each other's seek positions and end up
                // reading garbage offsets (manifested as bogus entry sizes
                // triggering "entry size N exceeds max" errors).
                pread_exact(file, offset, len)
            })
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    }

    async fn read_entry(&self, entry_log_dir: &Path, log_id: u32, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let file = self.get_or_open_cached(entry_log_dir, log_id)?;
        tokio::task::spawn_blocking(move || pread_exact_arc(&file, offset, len))
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    }

    async fn read_framed_entry(
        &self,
        entry_log_dir: &Path,
        log_id: u32,
        offset: u64,
        max_size: usize,
    ) -> io::Result<Vec<u8>> {
        let file = self.get_or_open_cached(entry_log_dir, log_id)?;
        tokio::task::spawn_blocking(move || {
            // Positional reads (pread) — concurrent reads on the same cached
            // fd MUST NOT race on the file's internal seek position. Using
            // seek+read_exact here would interleave seeks between threads and
            // produce garbage offsets (tripping "entry size N exceeds max").
            let size_buf = pread_exact_arc(&file, offset, 4)?;
            let size = i32::from_be_bytes(size_buf[..4].try_into().unwrap()) as usize;
            if size > max_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("entry size {} exceeds max {}", size, max_size),
                ));
            }
            pread_exact_arc(&file, offset + 4, size)
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    }
}

/// Read exactly `len` bytes starting at `offset` using pread(2).
/// Does NOT modify the file's internal seek position, so it is safe to
/// call concurrently on file handles shared via the fd cache.
fn pread_exact(file: &mut File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    let mut buf = vec![0u8; len];
    let mut read_so_far = 0usize;
    while read_so_far < len {
        let n = file.read_at(&mut buf[read_so_far..], offset + read_so_far as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("short pread: got {read_so_far} of {len} bytes at offset {offset}"),
            ));
        }
        read_so_far += n;
    }
    Ok(buf)
}

/// `pread_exact` variant that takes a shared `&File` — used by callers
/// who hold an `Arc<File>` from the read cache and want to perform
/// positional reads without forming a `&mut File`.
fn pread_exact_arc(file: &File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    let mut buf = vec![0u8; len];
    let mut read_so_far = 0usize;
    while read_so_far < len {
        let n = file.read_at(&mut buf[read_so_far..], offset + read_so_far as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("short pread: got {read_so_far} of {len} bytes at offset {offset}"),
            ));
        }
        read_so_far += n;
    }
    Ok(buf)
}
