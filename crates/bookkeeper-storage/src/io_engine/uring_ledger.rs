//! io_uring-backed ledger (entry-log) I/O — Linux only.
//!
//! Provides scatter-gather writes at explicit offsets and positional
//! reads, both dispatched through io_uring to avoid blocking
//! system-call overhead.

use std::io;
use std::os::fd::RawFd;
use std::sync::Mutex;

use io_uring::{opcode, types, IoUring};

use super::LedgerIo;

const LEDGER_SQ_DEPTH: u32 = 256;

/// Ledger I/O backend that uses `io_uring` for positional writes and reads.
///
/// The ring is created once in `new()` and reused across all calls.
/// A `Mutex` serializes access since `IoUring` is `!Sync`.
pub struct UringLedgerIo {
    ring: Mutex<IoUring<io_uring::squeue::Entry>>,
}

impl UringLedgerIo {
    pub fn new() -> io::Result<Self> {
        let ring = IoUring::<io_uring::squeue::Entry>::new(LEDGER_SQ_DEPTH)?;
        Ok(Self {
            ring: Mutex::new(ring),
        })
    }
}

impl LedgerIo for UringLedgerIo {
    fn write_batch(&self, fd: RawFd, offset: u64, entries: &[&[u8]]) -> io::Result<Vec<u64>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut ring = self.ring.lock().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "ledger ring lock poisoned")
        })?;

        let iovecs: Vec<libc::iovec> = entries
            .iter()
            .map(|e| libc::iovec {
                iov_base: e.as_ptr() as *mut _,
                iov_len: e.len(),
            })
            .collect();

        let total_bytes: usize = entries.iter().map(|e| e.len()).sum();

        let write_sqe = opcode::Writev::new(
            types::Fd(fd),
            iovecs.as_ptr(),
            iovecs.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(1);

        unsafe {
            ring.submission()
                .push(&write_sqe)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
        }

        ring.submit_and_wait(1)?;

        for cqe in ring.completion() {
            let ret = cqe.result();
            if ret < 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }
            // Short write: retry remainder via blocking fallback
            if (ret as usize) < total_bytes {
                let written = ret as usize;
                retry_short_pwrite(fd, entries, offset, written)?;
            }
        }

        let mut offsets = Vec::with_capacity(entries.len());
        let mut pos = offset;
        for entry in entries {
            offsets.push(pos);
            pos += entry.len() as u64;
        }
        Ok(offsets)
    }

    fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let mut ring = self.ring.lock().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "ledger ring lock poisoned")
        })?;

        let mut buf = vec![0u8; len];

        let read_sqe = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), len as u32)
            .offset(offset)
            .build()
            .user_data(1);

        unsafe {
            ring.submission()
                .push(&read_sqe)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
        }

        ring.submit_and_wait(1)?;

        for cqe in ring.completion() {
            let ret = cqe.result();
            if ret < 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }
            if (ret as usize) < len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("short read: {} of {} bytes", ret, len),
                ));
            }
        }

        Ok(buf)
    }

    fn read_entry(&self, entry_log_dir: &std::path::Path, log_id: u32, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        // Temporary: open + delegate to read_at. Will be replaced by ring thread in Task 4.
        let path = entry_log_dir.join(format!("{:x}.log", log_id));
        let file = std::fs::File::open(&path)?;
        let fd = std::os::fd::AsRawFd::as_raw_fd(&file);
        self.read_at(fd, offset, len)
    }
}

/// Retry remaining bytes after a short WRITEV using blocking pwrite.
fn retry_short_pwrite(
    fd: RawFd,
    entries: &[&[u8]],
    base_offset: u64,
    already_written: usize,
) -> io::Result<()> {
    use std::io::Write;
    let mut file = std::mem::ManuallyDrop::new(unsafe {
        std::fs::File::from_raw_fd(fd)
    });
    // Seek to where the short write left off
    file.seek(std::io::SeekFrom::Start(base_offset + already_written as u64))?;
    let mut skip = already_written;
    for entry in entries {
        if skip >= entry.len() {
            skip -= entry.len();
            continue;
        }
        file.write_all(&entry[skip..])?;
        skip = 0;
    }
    Ok(())
}
