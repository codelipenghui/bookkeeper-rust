//! Standard blocking I/O engine backed by `std::fs` / `std::io`.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::RawFd;

#[cfg(unix)]
use std::os::unix::io::FromRawFd;

use super::{JournalIo, LedgerIo};

/// Blocking I/O engine that uses standard `std::fs` / `std::io` operations.
///
/// Implements both `JournalIo` and `LedgerIo`, so an `Arc<StdEngine>` can
/// be coerced to either trait object. All methods borrow the raw fd via
/// `ManuallyDrop<File>` which is panic-safe (the fd is never closed).
pub struct StdEngine;

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
}

impl LedgerIo for StdEngine {
    fn write_batch(&self, fd: RawFd, offset: u64, entries: &[&[u8]]) -> io::Result<Vec<u64>> {
        with_file(fd, |file| {
            file.seek(SeekFrom::Start(offset))?;
            let mut current = offset;
            let mut offsets = Vec::with_capacity(entries.len());
            for entry in entries {
                offsets.push(current);
                file.write_all(entry)?;
                current += entry.len() as u64;
            }
            Ok(offsets)
        })
    }

    fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        with_file(fd, |file| {
            file.seek(SeekFrom::Start(offset))?;
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            Ok(buf)
        })
    }
}
