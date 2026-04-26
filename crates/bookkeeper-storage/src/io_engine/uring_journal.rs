//! io_uring-backed journal I/O — Linux only.
//!
//! Uses a linked pair of SQEs (WRITEV + FSYNC) so the kernel performs
//! the scatter-gather write and the fdatasync in a single submission,
//! avoiding the extra user-kernel round-trip that the blocking path
//! requires.

use std::io;
use std::os::fd::RawFd;
use std::os::unix::io::FromRawFd;
use std::sync::Mutex;

use io_uring::{opcode, types, IoUring};

use super::JournalIo;

const JOURNAL_SQ_DEPTH: u32 = 64;

/// Journal I/O backend that uses `io_uring` for write + fsync.
///
/// The ring is created once in `new()` and reused across all calls.
/// A `Mutex` serializes access since `IoUring` is `!Sync`.
pub struct UringJournalIo {
    ring: Mutex<IoUring<io_uring::squeue::Entry>>,
}

impl UringJournalIo {
    pub fn new() -> io::Result<Self> {
        let ring = IoUring::<io_uring::squeue::Entry>::new(JOURNAL_SQ_DEPTH)?;
        Ok(Self {
            ring: Mutex::new(ring),
        })
    }
}

impl JournalIo for UringJournalIo {
    fn write_and_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut ring = self.ring.lock().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "journal ring lock poisoned")
        })?;

        // Build iovec array for scatter-gather write.
        let iovecs: Vec<libc::iovec> = entries
            .iter()
            .map(|e| libc::iovec {
                iov_base: e.as_ptr() as *mut _,
                iov_len: e.len(),
            })
            .collect();

        let total_bytes: usize = entries.iter().map(|e| e.len()).sum();

        // SQE 1: WRITEV at current file position (append).
        let write_sqe = opcode::Writev::new(
            types::Fd(fd),
            iovecs.as_ptr(),
            iovecs.len() as u32,
        )
        .offset(u64::MAX) // -1 = append at current position
        .build()
        .flags(io_uring::squeue::Flags::IO_LINK)
        .user_data(1);

        // SQE 2: FSYNC linked after the write.
        let fsync_sqe = opcode::Fsync::new(types::Fd(fd))
            .build()
            .user_data(2);

        unsafe {
            ring.submission()
                .push(&write_sqe)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
            ring.submission()
                .push(&fsync_sqe)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
        }

        ring.submit_and_wait(2)?;

        // Drain completions into a vec first (avoids holding a borrow on
        // `ring` that conflicts with submit_fsync if we need a short-write retry).
        let cqes: Vec<(i32, u64)> = ring.completion()
            .map(|cqe| (cqe.result(), cqe.user_data()))
            .collect();

        // First pass: classify each CQE. We must NOT eager-return on a
        // negative FSYNC result before we've inspected the WRITEV CQE,
        // because IO_LINK auto-cancels a linked SQE when the previous
        // one is "short" (return -ECANCELED on the FSYNC). That's not
        // a real error — it's the trigger for our short-write retry.
        let mut writev_ret: Option<i32> = None;
        let mut fsync_ret: Option<i32> = None;
        for (ret, user_data) in &cqes {
            match *user_data {
                1 => writev_ret = Some(*ret),
                2 => fsync_ret = Some(*ret),
                _ => {}
            }
        }
        // Real WRITEV errors are still fatal.
        if let Some(r) = writev_ret {
            if r < 0 {
                return Err(io::Error::from_raw_os_error(-r));
            }
            let written = r as usize;
            if written < total_bytes {
                // Short write: complete the missing tail, then retry the
                // FSYNC. The linked FSYNC was auto-cancelled by the
                // kernel; we ignore its CQE result here.
                self.retry_short_write(fd, entries, written)?;
                self.submit_fsync(&mut ring, fd)?;
                return Ok(());
            }
        }
        // No short write — the linked FSYNC must have run normally;
        // surface its error if any.
        if let Some(r) = fsync_ret {
            if r < 0 {
                return Err(io::Error::from_raw_os_error(-r));
            }
        }
        Ok(())
    }

    fn write_no_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut ring = self.ring.lock().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "journal ring lock poisoned")
        })?;

        let iovecs: Vec<libc::iovec> = entries
            .iter()
            .map(|e| libc::iovec {
                iov_base: e.as_ptr() as *mut _,
                iov_len: e.len(),
            })
            .collect();

        let total_bytes: usize = entries.iter().map(|e| e.len()).sum();

        // WRITEV only — no linked FSYNC.
        let write_sqe = opcode::Writev::new(
            types::Fd(fd),
            iovecs.as_ptr(),
            iovecs.len() as u32,
        )
        .offset(u64::MAX)
        .build()
        .user_data(1);

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
            if cqe.user_data() == 1 && (cqe.result() as usize) < total_bytes {
                self.retry_short_write(fd, entries, cqe.result() as usize)?;
            }
        }

        Ok(())
    }
}

impl UringJournalIo {
    /// Retry remaining bytes after a short WRITEV via a fallback write loop.
    fn retry_short_write(
        &self,
        fd: RawFd,
        entries: &[&[u8]],
        already_written: usize,
    ) -> io::Result<()> {
        use std::io::Write;
        let mut file = std::mem::ManuallyDrop::new(unsafe {
            std::fs::File::from_raw_fd(fd)
        });
        // Skip past the bytes already written
        let mut skip = already_written;
        for entry in entries {
            if skip >= entry.len() {
                skip -= entry.len();
                continue;
            }
            file.write_all(&entry[skip..])?;
            skip = 0;
        }
        file.flush()?;
        Ok(())
    }

    /// Submit a standalone FSYNC after a short-write retry.
    fn submit_fsync(
        &self,
        ring: &mut IoUring<io_uring::squeue::Entry>,
        fd: RawFd,
    ) -> io::Result<()> {
        let fsync_sqe = opcode::Fsync::new(types::Fd(fd))
            .build()
            .user_data(99);
        unsafe {
            ring.submission()
                .push(&fsync_sqe)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
        }
        ring.submit_and_wait(1)?;
        for cqe in ring.completion() {
            if cqe.result() < 0 {
                return Err(io::Error::from_raw_os_error(-cqe.result()));
            }
        }
        Ok(())
    }
}
