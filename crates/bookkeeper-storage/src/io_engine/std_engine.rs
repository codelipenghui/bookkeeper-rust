//! Standard blocking I/O engine backed by `std::fs` / `std::io`.

use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;

#[cfg(unix)]
use std::os::unix::io::FromRawFd;

use async_trait::async_trait;

use super::{JournalIo, LedgerIo};

/// Blocking I/O engine that uses standard `std::fs` / `std::io` operations.
///
/// Implements both `JournalIo` and `LedgerIo`, so an `Arc<StdEngine>` can
/// be coerced to either trait object. All methods borrow the raw fd via
/// `ManuallyDrop<File>` which is panic-safe (the fd is never closed).
///
/// Caches open file handles for entry log reads to avoid opening the same
/// file on every read (significant syscall overhead on non-Linux platforms).
pub struct StdEngine {
    /// LRU-ish fd cache for entry log reads. Key: (dir_hash, log_id).
    read_cache: std::sync::Mutex<std::collections::HashMap<(u64, u32), File>>,
}

impl StdEngine {
    pub fn new() -> Self {
        Self {
            read_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn get_or_open_cached(&self, entry_log_dir: &Path, log_id: u32) -> io::Result<RawFd> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        entry_log_dir.hash(&mut hasher);
        let dir_hash = hasher.finish();
        let key = (dir_hash, log_id);

        let mut cache = self.read_cache.lock()
            .unwrap_or_else(|p| p.into_inner());

        if let Some(file) = cache.get(&key) {
            return Ok(file.as_raw_fd());
        }

        let path = entry_log_dir.join(super::entry_log_filename(log_id));
        let file = File::open(&path)?;
        let fd = file.as_raw_fd();
        // Simple eviction: cap at 64 entries
        if cache.len() >= 64 {
            if let Some(&first_key) = cache.keys().next() {
                cache.remove(&first_key);
            }
        }
        cache.insert(key, file);
        Ok(fd)
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

/// Borrow a raw fd as a File reference without taking ownership.
fn with_file_ref(fd: RawFd) -> std::mem::ManuallyDrop<File> {
    std::mem::ManuallyDrop::new(unsafe { File::from_raw_fd(fd) })
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
        let fd = self.get_or_open_cached(entry_log_dir, log_id)?;
        self.read_at(fd, offset, len).await
    }

    async fn read_framed_entry(
        &self,
        entry_log_dir: &Path,
        log_id: u32,
        offset: u64,
        max_size: usize,
    ) -> io::Result<Vec<u8>> {
        let fd = self.get_or_open_cached(entry_log_dir, log_id)?;
        tokio::task::spawn_blocking(move || {
            let mut file = with_file_ref(fd);
            // Positional reads (pread) — concurrent reads on the same cached
            // fd MUST NOT race on the file's internal seek position.  Using
            // seek+read_exact here would interleave seeks between threads and
            // produce garbage offsets (tripping "entry size N exceeds max").
            let size_buf = pread_exact(&mut file, offset, 4)?;
            let size = i32::from_be_bytes(size_buf[..4].try_into().unwrap()) as usize;
            if size > max_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("entry size {} exceeds max {}", size, max_size),
                ));
            }
            pread_exact(&mut file, offset + 4, size)
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
