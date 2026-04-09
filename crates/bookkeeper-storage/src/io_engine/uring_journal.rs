//! io_uring-backed journal I/O — Linux only.
//!
//! Uses a linked pair of SQEs (WRITEV + FSYNC) so the kernel performs
//! the scatter-gather write and the fdatasync in a single submission,
//! avoiding the extra user-kernel round-trip that the blocking path
//! requires.

use std::io;
use std::os::fd::RawFd;

use io_uring::{opcode, types, IoUring};

use super::JournalIo;

/// Submission-queue depth for the per-call io_uring instance.
///
/// 64 is more than enough — we only ever submit 2 SQEs (write + fsync)
/// per call, but a generous depth avoids needing to resize if the
/// batching strategy changes later.
const JOURNAL_SQ_DEPTH: u32 = 64;

/// Journal I/O backend that uses `io_uring` for write + fsync.
pub struct UringJournalIo;

impl UringJournalIo {
    /// Create a new instance, verifying that io_uring is functional on
    /// this kernel by building (and immediately dropping) a probe ring.
    pub fn new() -> io::Result<Self> {
        let _probe = IoUring::new(2)?;
        Ok(Self)
    }
}

impl JournalIo for UringJournalIo {
    fn write_and_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut ring = IoUring::new(JOURNAL_SQ_DEPTH)?;

        // Build iovec array for scatter-gather write.
        let iovecs: Vec<libc::iovec> = entries
            .iter()
            .map(|e| libc::iovec {
                iov_base: e.as_ptr() as *mut _,
                iov_len: e.len(),
            })
            .collect();

        // SQE 1: WRITEV at current file position.
        // offset = u64::MAX (i.e. -1 as unsigned) means "use the file's
        // current position", which for an append-mode journal fd is the
        // end of file.
        let write_sqe = opcode::Writev::new(
            types::Fd(fd),
            iovecs.as_ptr(),
            iovecs.len() as u32,
        )
        .offset(u64::MAX)
        .build()
        .flags(io_uring::squeue::Flags::IO_LINK) // chain: fsync waits for write
        .user_data(1);

        // SQE 2: FSYNC (fdatasync) linked after the write.
        let fsync_sqe = opcode::Fsync::new(types::Fd(fd))
            .build()
            .user_data(2);

        // Safety: the iovecs, fd, and buffer slices all live for the
        // duration of the submit_and_wait call.
        unsafe {
            ring.submission()
                .push(&write_sqe)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
            ring.submission()
                .push(&fsync_sqe)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
        }

        // Submit both SQEs and wait for both CQEs.
        ring.submit_and_wait(2)?;

        // Drain completions and check for errors.
        for cqe in ring.completion() {
            let ret = cqe.result();
            if ret < 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }
        }

        Ok(())
    }
}
