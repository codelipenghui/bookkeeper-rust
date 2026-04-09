//! io_uring-backed ledger (entry-log) I/O — Linux only.
//!
//! A dedicated `ledger-io` thread owns the `IoUring` ring and an `FdCache`.
//! Callers send requests through a bounded channel; the thread batches them,
//! submits SQEs, and dispatches CQE results back via per-request reply
//! channels.  This replaces the previous `Mutex<IoUring>` design that
//! serialized all I/O.

use std::io;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::JoinHandle;

use async_trait::async_trait;
use io_uring::{opcode, types, IoUring};
use tokio::sync::oneshot;

use super::fd_cache::FdCache;
use super::LedgerIo;

const LEDGER_SQ_DEPTH: u32 = 256;
const CHANNEL_BOUND: usize = 256;
const FD_CACHE_CAPACITY: usize = 256;

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

enum LedgerRequest {
    Read {
        entry_log_dir: PathBuf,
        log_id: u32,
        offset: u64,
        len: usize,
        reply: oneshot::Sender<io::Result<Vec<u8>>>,
    },
    Write {
        fd: RawFd,
        offset: u64,
        entries: Vec<Vec<u8>>,
        reply: oneshot::Sender<io::Result<Vec<u64>>>,
    },
    Evict {
        log_id: u32,
    },
}

// ---------------------------------------------------------------------------
// Public struct
// ---------------------------------------------------------------------------

/// Ledger I/O backend that dispatches reads and writes through a dedicated
/// `ledger-io` thread owning an `io_uring` ring and an LRU fd cache.
pub struct UringLedgerIo {
    tx: SyncSender<LedgerRequest>,
    _ring_thread: Option<JoinHandle<()>>,
}

impl UringLedgerIo {
    pub fn new() -> io::Result<Self> {
        // Verify io_uring works before spawning the thread.
        let _probe = IoUring::<io_uring::squeue::Entry>::new(4)?;

        let (tx, rx) = mpsc::sync_channel(CHANNEL_BOUND);
        let ring_thread = std::thread::Builder::new()
            .name("ledger-io".into())
            .spawn(move || ledger_ring_loop(rx))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(Self {
            tx,
            _ring_thread: Some(ring_thread),
        })
    }
}

// ---------------------------------------------------------------------------
// LedgerIo trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl LedgerIo for UringLedgerIo {
    async fn write_batch(&self, fd: RawFd, offset: u64, entries: Vec<Vec<u8>>) -> io::Result<Vec<u64>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LedgerRequest::Write { fd, offset, entries, reply: reply_tx })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "ring thread closed"))?;
        reply_rx.await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "ring thread dropped reply"))?
    }

    async fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        // Legacy path used by the write side — blocking pread, fast syscall,
        // no ring involvement.
        let mut buf = vec![0u8; len];
        let ret = unsafe {
            libc::pread(
                fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                len,
                offset as libc::off_t,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        if (ret as usize) < len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("short read: {} of {} bytes", ret, len),
            ));
        }
        Ok(buf)
    }

    async fn read_entry(
        &self,
        entry_log_dir: &Path,
        log_id: u32,
        offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LedgerRequest::Read {
                entry_log_dir: entry_log_dir.to_path_buf(),
                log_id,
                offset,
                len,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "ring thread closed"))?;
        reply_rx.await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "ring thread dropped reply"))?
    }

    fn evict_fd(&self, _entry_log_dir: &Path, log_id: u32) {
        let _ = self.tx.send(LedgerRequest::Evict { log_id });
    }
}

// ---------------------------------------------------------------------------
// Ring thread internals
// ---------------------------------------------------------------------------

/// Tracks an in-flight I/O operation so the CQE result can be dispatched
/// back to the correct caller.
enum PendingOp {
    Read {
        buf: Vec<u8>,
        len: usize,
        reply: oneshot::Sender<io::Result<Vec<u8>>>,
    },
    Write {
        entries: Vec<Vec<u8>>,
        offset: u64,
        total_bytes: usize,
        fd: RawFd,
        // Keep iovecs alive until the CQE is reaped — the kernel reads
        // from this memory during submission.
        _iovecs: Vec<libc::iovec>,
        reply: oneshot::Sender<io::Result<Vec<u64>>>,
    },
}

fn ledger_ring_loop(rx: Receiver<LedgerRequest>) {
    let mut ring = match IoUring::<io_uring::squeue::Entry>::new(LEDGER_SQ_DEPTH) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("ledger-io thread: failed to create io_uring: {e}");
            return;
        }
    };
    let mut fd_cache = FdCache::new(FD_CACHE_CAPACITY);

    loop {
        // ---- 1. Block on the first request ----
        let first = match rx.recv() {
            Ok(req) => req,
            Err(_) => break, // channel disconnected = shutdown
        };

        // ---- 2. Drain all immediately available requests ----
        let mut batch = vec![first];
        while let Ok(req) = rx.try_recv() {
            batch.push(req);
        }

        // ---- 3. Build SQEs ----
        // `pending` is indexed by SQE user_data.  Evicts don't get an SQE
        // so they are handled inline and never added here.
        let mut pending: Vec<PendingOp> = Vec::with_capacity(batch.len());
        let mut num_sqes: usize = 0;

        for request in batch {
            match request {
                LedgerRequest::Read {
                    entry_log_dir,
                    log_id,
                    offset,
                    len,
                    reply,
                } => {
                    let fd = match fd_cache.get_or_open(&entry_log_dir, log_id) {
                        Ok((fd, _)) => {
                            // Pin to prevent LRU eviction while SQE is in-flight
                            fd_cache.pin(log_id);
                            fd
                        }
                        Err(e) => {
                            let _ = reply.send(Err(e));
                            continue;
                        }
                    };

                    let mut buf = vec![0u8; len];
                    let sqe_idx = pending.len() as u64;

                    let read_sqe =
                        opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), len as u32)
                            .offset(offset)
                            .build()
                            .user_data(sqe_idx);

                    let pushed = unsafe { ring.submission().push(&read_sqe) };
                    if pushed.is_err() {
                        let _ = reply.send(Err(io::Error::new(
                            io::ErrorKind::Other,
                            "SQ full",
                        )));
                        continue;
                    }

                    pending.push(PendingOp::Read { buf, len, reply });
                    num_sqes += 1;
                }

                LedgerRequest::Write {
                    fd,
                    offset,
                    entries,
                    reply,
                } => {
                    let iovecs: Vec<libc::iovec> = entries
                        .iter()
                        .map(|e| libc::iovec {
                            iov_base: e.as_ptr() as *mut _,
                            iov_len: e.len(),
                        })
                        .collect();

                    let total_bytes: usize = entries.iter().map(|e| e.len()).sum();
                    let sqe_idx = pending.len() as u64;

                    let write_sqe = opcode::Writev::new(
                        types::Fd(fd),
                        iovecs.as_ptr(),
                        iovecs.len() as u32,
                    )
                    .offset(offset)
                    .build()
                    .user_data(sqe_idx);

                    let pushed = unsafe { ring.submission().push(&write_sqe) };
                    if pushed.is_err() {
                        let _ = reply.send(Err(io::Error::new(
                            io::ErrorKind::Other,
                            "SQ full",
                        )));
                        continue;
                    }

                    pending.push(PendingOp::Write {
                        entries,
                        offset,
                        total_bytes,
                        fd,
                        _iovecs: iovecs,
                        reply,
                    });
                    num_sqes += 1;
                }

                LedgerRequest::Evict { log_id } => {
                    fd_cache.evict(log_id);
                }
            }
        }

        if num_sqes == 0 {
            continue;
        }

        // ---- 4. Submit and wait for all I/O SQEs ----
        if let Err(e) = ring.submit_and_wait(num_sqes) {
            // Fatal ring error — notify all pending callers.
            let err_msg = e.to_string();
            let err_kind = e.kind();
            for op in pending {
                match op {
                    PendingOp::Read { reply, .. } => {
                        let _ = reply.send(Err(io::Error::new(err_kind, err_msg.clone())));
                    }
                    PendingOp::Write { reply, .. } => {
                        let _ = reply.send(Err(io::Error::new(err_kind, err_msg.clone())));
                    }
                }
            }
            continue;
        }

        // ---- 5. Collect CQE results keyed by user_data ----
        // Unpin fds now that all SQEs have completed — safe to evict again.
        fd_cache.unpin_all();

        let mut results: Vec<Option<i32>> = vec![None; pending.len()];
        for cqe in ring.completion() {
            let idx = cqe.user_data() as usize;
            if idx < results.len() {
                results[idx] = Some(cqe.result());
            }
        }

        // ---- 6. Dispatch results back to callers ----
        for (idx, op) in pending.into_iter().enumerate() {
            let ret = results[idx].unwrap_or(-libc::EIO);
            match op {
                PendingOp::Read {
                    buf, len, reply, ..
                } => {
                    let result = if ret < 0 {
                        Err(io::Error::from_raw_os_error(-ret))
                    } else if (ret as usize) < len {
                        Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!("short read: {} of {} bytes", ret, len),
                        ))
                    } else {
                        Ok(buf)
                    };
                    let _ = reply.send(result);
                }
                PendingOp::Write {
                    entries,
                    offset,
                    total_bytes,
                    fd,
                    reply,
                    ..
                } => {
                    let result = if ret < 0 {
                        Err(io::Error::from_raw_os_error(-ret))
                    } else {
                        // Handle short writes via blocking fallback.
                        if (ret as usize) < total_bytes {
                            if let Err(e) =
                                retry_short_pwrite(fd, &entries, offset, ret as usize)
                            {
                                Err(e)
                            } else {
                                Ok(build_offset_vec(offset, &entries))
                            }
                        } else {
                            Ok(build_offset_vec(offset, &entries))
                        }
                    };
                    let _ = reply.send(result);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build the per-entry starting-offset vector returned by `write_batch`.
fn build_offset_vec(base_offset: u64, entries: &[Vec<u8>]) -> Vec<u64> {
    let mut offsets = Vec::with_capacity(entries.len());
    let mut pos = base_offset;
    for entry in entries {
        offsets.push(pos);
        pos += entry.len() as u64;
    }
    offsets
}

/// Retry remaining bytes after a short WRITEV using blocking pwrite.
/// Loops on each slice until all bytes are written (handles short
/// pwrite returns correctly).
fn retry_short_pwrite(
    fd: RawFd,
    entries: &[Vec<u8>],
    base_offset: u64,
    already_written: usize,
) -> io::Result<()> {
    let mut off = base_offset + already_written as u64;
    let mut skip = already_written;
    for entry in entries {
        if skip >= entry.len() {
            skip -= entry.len();
            continue;
        }
        let mut data = &entry[skip..];
        while !data.is_empty() {
            let ret = unsafe {
                libc::pwrite(
                    fd,
                    data.as_ptr() as *const libc::c_void,
                    data.len(),
                    off as libc::off_t,
                )
            };
            if ret < 0 {
                return Err(io::Error::last_os_error());
            }
            let written = ret as usize;
            off += written as u64;
            data = &data[written..];
        }
        skip = 0;
    }
    Ok(())
}
