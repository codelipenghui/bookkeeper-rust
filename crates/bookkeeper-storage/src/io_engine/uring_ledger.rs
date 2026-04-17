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
use crate::storage_metrics;

// SQ depth and request-channel bound must be large enough to absorb the
// concurrent reads from Pulsar's catch-up workload (primary request + a
// spawned readahead task per cache miss, each potentially in flight at
// once). 4096 is well under the kernel's IORING_MAX_ENTRIES and costs
// only ~256 KB of pinned ring memory per ring.
const LEDGER_SQ_DEPTH: u32 = 4096;
const CHANNEL_BOUND: usize = 4096;
const FD_CACHE_CAPACITY: usize = 256;

/// Returns an "SQ full" `io::Error` and increments the corresponding
/// metric so operators can see when the ledger ring's submission queue
/// is saturating *after* backpressure attempts. All call sites funnel
/// through here so the counter is authoritative.
fn sq_full_error() -> io::Error {
    storage_metrics::IO_URING_SQ_FULL_TOTAL.inc();
    io::Error::new(io::ErrorKind::Other, "SQ full")
}

/// Push an SQE into the ring with cooperative backpressure.
///
/// Fast path: a direct `submission().push()` succeeds (SQ not full) and
/// returns immediately. Slow path: on `Err` (user-space SQ full because we
/// have `LEDGER_SQ_DEPTH` unsubmitted SQEs queued), call `submit()` to hand
/// the queued SQEs over to the kernel — this empties the user-space SQ —
/// then drain any CQEs that are already ready into `results` so the
/// dispatch loop downstream sees them as completed. Retry the push.
///
/// The drain-into-results step matters because it lets us complete
/// in-flight ops *during* the push loop, which keeps the kernel-side
/// CQ from filling up under sustained pressure.
///
/// Bounded retries (4) followed by a `submit_and_wait(1)` give the kernel
/// time to release a slot. After all attempts fail, falls back to the
/// previous behavior (returns an `SQ full` error and increments the
/// counter) so the caller can surface it as before.
fn try_push_with_backpressure(
    ring: &mut IoUring,
    sqe: &io_uring::squeue::Entry,
    results: &mut [Option<(i32, u32)>],
) -> io::Result<()> {
    // Fast path
    if unsafe { ring.submission().push(sqe) }.is_ok() {
        return Ok(());
    }
    for attempt in 0..4 {
        // Hand queued SQEs to kernel; this empties the user-space SQ.
        ring.submit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("submit: {e}")))?;
        // Opportunistically drain ready CQEs so subsequent pushes don't
        // accumulate them beyond the CQ's capacity.
        for cqe in ring.completion() {
            let idx = cqe.user_data() as usize;
            if idx < results.len() {
                results[idx] = Some((cqe.result(), cqe.flags()));
            }
        }
        if unsafe { ring.submission().push(sqe) }.is_ok() {
            return Ok(());
        }
        // Still full — wait for at least one CQE before the next attempt.
        // Skip on first iter (we just submitted, kernel may not have produced
        // CQEs yet but the SQ should be free; only block if the second push
        // also fails).
        if attempt > 0 {
            ring.submit_and_wait(1).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("submit_and_wait: {e}"))
            })?;
            for cqe in ring.completion() {
                let idx = cqe.user_data() as usize;
                if idx < results.len() {
                    results[idx] = Some((cqe.result(), cqe.flags()));
                }
            }
        }
    }
    Err(sq_full_error())
}

/// Size of each buffer in the provided buffer pool (64 KB).
const BUF_POOL_SIZE: usize = 64 * 1024;
/// Number of buffers in the pool. Sized to absorb bursty catch-up reads —
/// the gating check `in_flight_pool < BUF_POOL_COUNT` in the dispatch loop
/// resets per batch, so under sustained load we can submit up to this many
/// pool reads per poll cycle. When the kernel still returns `-ENOBUFS`
/// despite the gate (race between submit and completion), the CQE handler
/// now falls back to a synchronous `pread` instead of surfacing the error.
/// With 64 KB buffers and 8192 slots the pool reserves ~512 MB up front.
const BUF_POOL_COUNT: u16 = 8192;
/// Buffer group ID registered with the kernel.
const BUF_GROUP_ID: u16 = 0;
/// CQE flag indicating a provided buffer was consumed (IORING_CQE_F_BUFFER).
const CQE_F_BUFFER: u32 = 0x1;

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
    ReadFramed {
        entry_log_dir: PathBuf,
        log_id: u32,
        offset: u64,
        max_size: usize,
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

    async fn read_framed_entry(
        &self,
        entry_log_dir: &Path,
        log_id: u32,
        offset: u64,
        max_size: usize,
    ) -> io::Result<Vec<u8>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LedgerRequest::ReadFramed {
                entry_log_dir: entry_log_dir.to_path_buf(),
                log_id,
                offset,
                max_size,
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
    PoolRead {
        len: usize,
        // Kept so the ENOBUFS (pool exhausted) branch can fall back to a
        // synchronous `libc::pread` instead of bubbling a WouldBlock error
        // up to the bookie client. Without retry data the caller saw EIO
        // and Pulsar's managed-ledger backed off for 15 s — a hiccup in
        // the pool should not be client-visible.
        fd: RawFd,
        offset: u64,
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
        // Concatenated buffer for large writes (> UIO_MAXIOV iovecs).
        _flat_buf: Option<Vec<u8>>,
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

    // Register fixed file descriptor table (all slots empty = -1)
    let empty_fds: Vec<RawFd> = vec![-1; FD_CACHE_CAPACITY];
    let fixed_fds_available = ring.submitter().register_files(&empty_fds).is_ok();
    if fixed_fds_available {
        tracing::debug!("ledger-io: registered {} fixed fd slots", FD_CACHE_CAPACITY);
    } else {
        tracing::warn!("ledger-io: register_files failed, using raw fds");
    }

    let buf_pool: Option<BufferPool> = BufferPool::try_new(&ring);
    if buf_pool.is_some() {
        tracing::debug!(
            "ledger-io: provided buffer pool active ({} x {}KB)",
            BUF_POOL_COUNT,
            BUF_POOL_SIZE / 1024
        );
    } else {
        tracing::debug!("ledger-io: provided buffer pool unavailable, using per-read alloc");
    }

    // Reused across batches to avoid the per-iter alloc + zero of a
    // CHANNEL_BOUND-sized buffer (64 KB at LEDGER_SQ_DEPTH=4096).
    let mut results: Vec<Option<(i32, u32)>> = Vec::new();

    loop {
        // ---- 1. Block on the first request ----
        let first = match rx.recv() {
            Ok(req) => req,
            Err(_) => {
                // Channel disconnected = shutdown.
                // Unregister buf pool while ring is still alive.
                if let Some(ref pool) = buf_pool {
                    pool.unregister(&ring);
                }
                break;
            }
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
        // Reset the hoisted results buffer to all-None for this batch's slots
        // (sqe_idx never exceeds pending.len() ≤ batch.len()).
        results.clear();
        results.resize(batch.len(), None);
        // Tracks PoolRead SQEs pushed in this batch. The kernel-side
        // provided-buffer pool can hand out at most `BUF_POOL_COUNT` buffers
        // before returning `-ENOBUFS`; cap our submissions at that to gate
        // the pool path and fall back to a per-read `Vec<u8>` allocation
        // when the pool would be over-subscribed. Buffers are returned to
        // the pool by the dispatch loop below (`pool.return_buf`), so a
        // fresh batch always starts with a full pool — resetting the
        // counter here is correct.
        let mut in_flight_pool: usize = 0;

        for request in batch {
            match request {
                LedgerRequest::Read {
                    entry_log_dir,
                    log_id,
                    offset,
                    len,
                    reply,
                } => {
                    let (fd, fixed_index) = match fd_cache.get_or_open(&entry_log_dir, log_id) {
                        Ok((fd, fixed_index, is_new)) => {
                            // Pin to prevent LRU eviction while SQE is in-flight
                            fd_cache.pin(log_id);
                            if is_new && fixed_fds_available {
                                let _ = ring.submitter().register_files_update(
                                    fixed_index, &[fd],
                                );
                            }
                            (fd, fixed_index)
                        }
                        Err(e) => {
                            let _ = reply.send(Err(e));
                            continue;
                        }
                    };

                    let use_pool = buf_pool.is_some()
                        && len <= BUF_POOL_SIZE
                        && in_flight_pool < BUF_POOL_COUNT as usize;
                    let sqe_idx = pending.len() as u64;

                    if use_pool {
                        let read_sqe = build_pool_read_sqe(
                            fixed_fds_available, fd, fixed_index,
                            len as u32, offset, BUF_GROUP_ID, sqe_idx,
                        );
                        if let Err(e) = try_push_with_backpressure(
                            &mut ring, &read_sqe, &mut results,
                        ) {
                            let _ = reply.send(Err(e));
                            continue;
                        }
                        pending.push(PendingOp::PoolRead { len, fd, offset, reply });
                        in_flight_pool += 1;
                    } else {
                        let mut buf = vec![0u8; len];
                        let read_sqe = build_read_sqe(
                            fixed_fds_available, fd, fixed_index,
                            buf.as_mut_ptr(), len as u32, offset, sqe_idx,
                        );
                        if let Err(e) = try_push_with_backpressure(
                            &mut ring, &read_sqe, &mut results,
                        ) {
                            let _ = reply.send(Err(e));
                            continue;
                        }
                        pending.push(PendingOp::Read { buf, len, reply });
                    }
                    num_sqes += 1;
                }

                LedgerRequest::ReadFramed {
                    entry_log_dir,
                    log_id,
                    offset,
                    max_size,
                    reply,
                } => {
                    let (fd, fixed_index) = match fd_cache.get_or_open(&entry_log_dir, log_id) {
                        Ok((fd, fixed_index, is_new)) => {
                            fd_cache.pin(log_id);
                            if is_new && fixed_fds_available {
                                let _ = ring.submitter().register_files_update(
                                    fixed_index, &[fd],
                                );
                            }
                            (fd, fixed_index)
                        }
                        Err(e) => {
                            let _ = reply.send(Err(e));
                            continue;
                        }
                    };

                    // Read 4-byte size prefix via blocking pread
                    let mut size_buf = [0u8; 4];
                    let ret = unsafe {
                        libc::pread(
                            fd,
                            size_buf.as_mut_ptr() as *mut libc::c_void,
                            4,
                            offset as libc::off_t,
                        )
                    };
                    if ret < 0 {
                        let _ = reply.send(Err(io::Error::last_os_error()));
                        continue;
                    }
                    if (ret as usize) < 4 {
                        let _ = reply.send(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "short size prefix read",
                        )));
                        continue;
                    }

                    let size = i32::from_be_bytes(size_buf) as usize;
                    if size > max_size {
                        let _ = reply.send(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("entry size {} exceeds max {}", size, max_size),
                        )));
                        continue;
                    }

                    let data_offset = offset + 4;
                    let use_pool = buf_pool.is_some()
                        && size <= BUF_POOL_SIZE
                        && in_flight_pool < BUF_POOL_COUNT as usize;
                    let sqe_idx = pending.len() as u64;

                    if use_pool {
                        let read_sqe = build_pool_read_sqe(
                            fixed_fds_available, fd, fixed_index,
                            size as u32, data_offset, BUF_GROUP_ID, sqe_idx,
                        );
                        if let Err(e) = try_push_with_backpressure(
                            &mut ring, &read_sqe, &mut results,
                        ) {
                            let _ = reply.send(Err(e));
                            continue;
                        }
                        pending.push(PendingOp::PoolRead {
                            len: size, fd, offset: data_offset, reply,
                        });
                        in_flight_pool += 1;
                    } else {
                        let mut buf = vec![0u8; size];
                        let read_sqe = build_read_sqe(
                            fixed_fds_available, fd, fixed_index,
                            buf.as_mut_ptr(), size as u32, data_offset, sqe_idx,
                        );
                        if let Err(e) = try_push_with_backpressure(
                            &mut ring, &read_sqe, &mut results,
                        ) {
                            let _ = reply.send(Err(e));
                            continue;
                        }
                        pending.push(PendingOp::Read { buf, len: size, reply });
                    }
                    num_sqes += 1;
                }

                LedgerRequest::Write {
                    fd,
                    offset,
                    entries,
                    reply,
                } => {
                    let total_bytes: usize = entries.iter().map(|e| e.len()).sum();

                    // UIO_MAXIOV is 1024 — Writev with more iovecs returns EINVAL.
                    // For large batches, concatenate into a single buffer and use
                    // Write instead of Writev.
                    if entries.len() > 1024 {
                        let mut buf = Vec::with_capacity(total_bytes);
                        for e in &entries {
                            buf.extend_from_slice(e);
                        }
                        let sqe_idx = pending.len() as u64;
                        let write_sqe = opcode::Write::new(
                            types::Fd(fd),
                            buf.as_ptr(),
                            buf.len() as u32,
                        )
                        .offset(offset)
                        .build()
                        .user_data(sqe_idx);

                        if let Err(e) = try_push_with_backpressure(
                            &mut ring, &write_sqe, &mut results,
                        ) {
                            let _ = reply.send(Err(e));
                            continue;
                        }

                        pending.push(PendingOp::Write {
                            entries,
                            offset,
                            total_bytes,
                            fd,
                            _iovecs: Vec::new(),
                            _flat_buf: Some(buf),
                            reply,
                        });
                        num_sqes += 1;
                    } else {
                        let iovecs: Vec<libc::iovec> = entries
                            .iter()
                            .map(|e| libc::iovec {
                                iov_base: e.as_ptr() as *mut _,
                                iov_len: e.len(),
                            })
                            .collect();

                        let sqe_idx = pending.len() as u64;
                        let write_sqe = opcode::Writev::new(
                            types::Fd(fd),
                            iovecs.as_ptr(),
                            iovecs.len() as u32,
                        )
                        .offset(offset)
                        .build()
                        .user_data(sqe_idx);

                        if let Err(e) = try_push_with_backpressure(
                            &mut ring, &write_sqe, &mut results,
                        ) {
                            let _ = reply.send(Err(e));
                            continue;
                        }

                        pending.push(PendingOp::Write {
                            entries,
                            offset,
                            total_bytes,
                            fd,
                            _iovecs: iovecs,
                            _flat_buf: None,
                            reply,
                        });
                        num_sqes += 1;
                    }
                }

                LedgerRequest::Evict { log_id } => {
                    if let Some(evicted_idx) = fd_cache.evict(log_id) {
                        if fixed_fds_available {
                            let _ = ring.submitter().register_files_update(
                                evicted_idx, &[-1i32 as RawFd],
                            );
                        }
                    }
                }
            }
        }

        if num_sqes == 0 {
            continue;
        }

        // ---- 4. Submit and wait for the remaining un-completed SQEs ----
        // `try_push_with_backpressure` may have already populated some slots
        // in `results` while making room during the push loop. Wait only for
        // what's still in flight, otherwise `submit_and_wait` would block on
        // CQEs that have already arrived and been consumed.
        let still_in_flight = results[..pending.len()]
            .iter()
            .filter(|r| r.is_none())
            .count();
        if still_in_flight > 0 {
            if let Err(e) = ring.submit_and_wait(still_in_flight) {
                // Fatal ring error — notify all pending callers.
                let err_msg = e.to_string();
                let err_kind = e.kind();
                for op in pending {
                    match op {
                        PendingOp::Read { reply, .. } => {
                            let _ = reply.send(Err(io::Error::new(err_kind, err_msg.clone())));
                        }
                        PendingOp::PoolRead { reply, .. } => {
                            let _ = reply.send(Err(io::Error::new(err_kind, err_msg.clone())));
                        }
                        PendingOp::Write { reply, .. } => {
                            let _ = reply.send(Err(io::Error::new(err_kind, err_msg.clone())));
                        }
                    }
                }
                continue;
            }
        }

        // ---- 5. Collect CQE results keyed by user_data ----
        // Unpin fds now that all SQEs have completed — safe to evict again.
        fd_cache.unpin_all();

        // Drain any CQEs that arrived during the wait into the same `results`
        // buffer the push loop wrote into. Slots already populated inline by
        // `try_push_with_backpressure` are left as-is.
        for cqe in ring.completion() {
            let idx = cqe.user_data() as usize;
            if idx < results.len() {
                results[idx] = Some((cqe.result(), cqe.flags()));
            }
        }

        // ---- 6. Dispatch results back to callers ----
        for (idx, op) in pending.into_iter().enumerate() {
            let (ret, cqe_flags) = results[idx].unwrap_or((-libc::EIO, 0));
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
                PendingOp::PoolRead { len, fd, offset, reply } => {
                    let has_buffer = cqe_flags & CQE_F_BUFFER != 0;
                    let result = if ret == -(libc::ENOBUFS as i32) {
                        // Pool exhausted — fall back to a synchronous
                        // `libc::pread` on this thread. The counter is
                        // still bumped so operators can tell the pool
                        // is undersized for their workload. Previously
                        // this path surfaced `WouldBlock` to the bookie
                        // client, which Pulsar translated into EIO +
                        // a 15-second managed-ledger backoff.
                        storage_metrics::IO_URING_BUF_POOL_EXHAUSTED_TOTAL.inc();
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
                            Err(io::Error::last_os_error())
                        } else if (ret as usize) < len {
                            Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                format!("short read: {} of {} bytes", ret, len),
                            ))
                        } else {
                            Ok(buf)
                        }
                    } else if ret < 0 {
                        if has_buffer {
                            let buf_id = (cqe_flags >> 16) as u16;
                            buf_pool.as_ref().unwrap().return_buf(buf_id);
                        }
                        Err(io::Error::from_raw_os_error(-ret))
                    } else if (ret as usize) < len {
                        if has_buffer {
                            let buf_id = (cqe_flags >> 16) as u16;
                            buf_pool.as_ref().unwrap().return_buf(buf_id);
                        }
                        Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!("short read: {} of {} bytes", ret, len),
                        ))
                    } else {
                        let buf_id = (cqe_flags >> 16) as u16;
                        let pool = buf_pool.as_ref().unwrap();
                        let data = pool.get_buf(buf_id, ret as usize).to_vec();
                        pool.return_buf(buf_id);
                        Ok(data)
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
                    } else if (ret as usize) < total_bytes {
                        if let Err(e) =
                            retry_short_pwrite(fd, &entries, offset, ret as usize)
                        {
                            Err(e)
                        } else {
                            Ok(build_offset_vec(offset, &entries))
                        }
                    } else {
                        Ok(build_offset_vec(offset, &entries))
                    };
                    let _ = reply.send(result);
                    let _ = cqe_flags; // writes don't use CQE flags
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Provided buffer pool (kernel 5.19+)
// ---------------------------------------------------------------------------

/// Pre-allocated buffer pool registered with io_uring's provided buffer ring.
///
/// When the kernel supports it, read SQEs can use `BUFFER_SELECT` to let the
/// kernel pick a buffer from this pool instead of the application allocating
/// one per read.  After the CQE is reaped the buffer is copied out and
/// returned to the ring for reuse.
struct BufferPool {
    /// Contiguous backing storage for all buffers.
    storage: Vec<u8>,
    /// Pointer to the buffer ring memory (page-aligned allocation).
    ring_ptr: *mut io_uring::types::BufRingEntry,
    /// Layout used for deallocation.
    ring_layout: std::alloc::Layout,
}

unsafe impl Send for BufferPool {}

impl BufferPool {
    fn try_new(ring: &IoUring<io_uring::squeue::Entry>) -> Option<Self> {
        let entries = BUF_POOL_COUNT as usize;
        let entry_size = std::mem::size_of::<io_uring::types::BufRingEntry>();
        let ring_size = entry_size * entries;
        let layout = std::alloc::Layout::from_size_align(ring_size, 4096).ok()?;
        let ring_ptr =
            unsafe { std::alloc::alloc_zeroed(layout) } as *mut io_uring::types::BufRingEntry;
        if ring_ptr.is_null() {
            return None;
        }

        if unsafe {
            ring.submitter().register_buf_ring_with_flags(
                ring_ptr as u64,
                BUF_POOL_COUNT,
                BUF_GROUP_ID,
                0,
            )
        }
        .is_err()
        {
            unsafe {
                std::alloc::dealloc(ring_ptr as *mut u8, layout);
            }
            return None;
        }

        let storage = vec![0u8; BUF_POOL_SIZE * entries];

        for i in 0..entries {
            let buf_addr = unsafe { storage.as_ptr().add(i * BUF_POOL_SIZE) };
            let entry = unsafe { &mut *ring_ptr.add(i) };
            entry.set_addr(buf_addr as u64);
            entry.set_len(BUF_POOL_SIZE as u32);
            entry.set_bid(i as u16);
        }

        // BufRingEntry::tail() returns *const AtomicU16 — advance tail to
        // make all entries visible to the kernel.
        let tail_ptr = unsafe {
            io_uring::types::BufRingEntry::tail(
                ring_ptr as *const io_uring::types::BufRingEntry,
            )
        } as *const std::sync::atomic::AtomicU16;
        unsafe {
            (*tail_ptr).store(BUF_POOL_COUNT, std::sync::atomic::Ordering::Release);
        }

        Some(Self {
            storage,
            ring_ptr,
            ring_layout: layout,
        })
    }

    fn get_buf(&self, buf_id: u16, len: usize) -> &[u8] {
        let offset = (buf_id as usize) * BUF_POOL_SIZE;
        &self.storage[offset..offset + len]
    }

    fn return_buf(&self, buf_id: u16) {
        let entry = unsafe { &mut *self.ring_ptr.add(buf_id as usize) };
        let buf_addr =
            unsafe { self.storage.as_ptr().add((buf_id as usize) * BUF_POOL_SIZE) };
        entry.set_addr(buf_addr as u64);
        entry.set_len(BUF_POOL_SIZE as u32);
        entry.set_bid(buf_id);

        let tail_ptr = unsafe {
            io_uring::types::BufRingEntry::tail(
                self.ring_ptr as *const io_uring::types::BufRingEntry,
            )
        } as *const std::sync::atomic::AtomicU16;
        unsafe {
            (*tail_ptr).fetch_add(1, std::sync::atomic::Ordering::Release);
        }
    }

    /// Unregister from io_uring. Must be called while the ring is still alive.
    fn unregister(&self, ring: &IoUring<io_uring::squeue::Entry>) {
        let _ = ring.submitter().unregister_buf_ring(BUF_GROUP_ID);
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.ring_ptr as *mut u8, self.ring_layout);
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a read SQE using either Fixed or Fd depending on availability.
fn build_read_sqe(
    fixed_fds: bool,
    fd: RawFd,
    fixed_index: u32,
    buf_ptr: *mut u8,
    len: u32,
    offset: u64,
    user_data: u64,
) -> io_uring::squeue::Entry {
    if fixed_fds {
        opcode::Read::new(types::Fixed(fixed_index), buf_ptr, len)
            .offset(offset)
            .build()
            .user_data(user_data)
    } else {
        opcode::Read::new(types::Fd(fd), buf_ptr, len)
            .offset(offset)
            .build()
            .user_data(user_data)
    }
}

/// Build a read SQE with buffer selection from the provided buffer pool.
fn build_pool_read_sqe(
    fixed_fds: bool,
    fd: RawFd,
    fixed_index: u32,
    len: u32,
    offset: u64,
    buf_group: u16,
    user_data: u64,
) -> io_uring::squeue::Entry {
    if fixed_fds {
        opcode::Read::new(types::Fixed(fixed_index), std::ptr::null_mut(), len)
            .offset(offset)
            .buf_group(buf_group)
            .build()
            .flags(io_uring::squeue::Flags::BUFFER_SELECT)
            .user_data(user_data)
    } else {
        opcode::Read::new(types::Fd(fd), std::ptr::null_mut(), len)
            .offset(offset)
            .buf_group(buf_group)
            .build()
            .flags(io_uring::squeue::Flags::BUFFER_SELECT)
            .user_data(user_data)
    }
}

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
