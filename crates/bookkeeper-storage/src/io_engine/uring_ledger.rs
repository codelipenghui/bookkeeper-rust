//! io_uring-backed ledger (entry-log) I/O — Linux only.
//!
//! Provides scatter-gather writes at explicit offsets and positional
//! reads, both dispatched through io_uring to avoid blocking
//! system-call overhead.

use std::io;
use std::os::fd::RawFd;

use io_uring::{opcode, types, IoUring};

use super::LedgerIo;

/// Submission-queue depth for ledger operations.
///
/// Larger than the journal depth because a future batching strategy may
/// submit many independent reads in a single ring.
const LEDGER_SQ_DEPTH: u32 = 256;

/// Ledger I/O backend that uses `io_uring` for positional writes and reads.
pub struct UringLedgerIo;

impl UringLedgerIo {
    /// Create a new instance, verifying that io_uring is functional.
    pub fn new() -> io::Result<Self> {
        let _probe = IoUring::new(2)?;
        Ok(Self)
    }
}

impl LedgerIo for UringLedgerIo {
    fn write_batch(&self, fd: RawFd, offset: u64, entries: &[&[u8]]) -> io::Result<Vec<u64>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut ring = IoUring::new(LEDGER_SQ_DEPTH)?;

        // Build iovec array for scatter-gather write.
        let iovecs: Vec<libc::iovec> = entries
            .iter()
            .map(|e| libc::iovec {
                iov_base: e.as_ptr() as *mut _,
                iov_len: e.len(),
            })
            .collect();

        let write_sqe = opcode::Writev::new(
            types::Fd(fd),
            iovecs.as_ptr(),
            iovecs.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(1);

        // Safety: iovecs and buffer slices live until submit_and_wait returns.
        unsafe {
            ring.submission()
                .push(&write_sqe)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
        }

        ring.submit_and_wait(1)?;

        for cqe in ring.completion() {
            if cqe.result() < 0 {
                return Err(io::Error::from_raw_os_error(-cqe.result()));
            }
        }

        // Compute per-entry offsets.
        let mut offsets = Vec::with_capacity(entries.len());
        let mut pos = offset;
        for entry in entries {
            offsets.push(pos);
            pos += entry.len() as u64;
        }
        Ok(offsets)
    }

    fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let mut ring = IoUring::new(4)?;
        let mut buf = vec![0u8; len];

        let read_sqe = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), len as u32)
            .offset(offset)
            .build()
            .user_data(1);

        // Safety: buf lives until submit_and_wait returns.
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
}
