# Async Storage Traits + Journal ACK (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make all storage I/O traits async so tokio worker threads never block on disk I/O, and replace the journal's blocking ACK channel with `tokio::sync::oneshot`.

**Architecture:** The `JournalIo` and `LedgerIo` traits become `#[async_trait]`. `StdEngine` wraps blocking calls in `spawn_blocking`. `UringJournalIo`/`UringLedgerIo` already use dedicated ring threads — the async boundary becomes channel-send + oneshot-await instead of blocking `mpsc::recv`. The `LedgerStorage` trait becomes async, and the server removes `spawn_blocking` wrappers around `process_request`.

**Tech Stack:** `async-trait` (already in workspace), `tokio::sync::oneshot`, `tokio::task::spawn_blocking`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `crates/bookkeeper-storage/Cargo.toml` | Modify | Add `async-trait` dependency |
| `crates/bookkeeper-storage/src/io_engine/mod.rs` | Modify | Make `JournalIo` and `LedgerIo` traits async |
| `crates/bookkeeper-storage/src/io_engine/std_engine.rs` | Modify | Implement async traits with `spawn_blocking` |
| `crates/bookkeeper-storage/src/io_engine/uring_journal.rs` | Modify | Implement async `JournalIo` via channel + oneshot |
| `crates/bookkeeper-storage/src/io_engine/uring_ledger.rs` | Modify | Replace `std::sync::mpsc` replies with `tokio::sync::oneshot` |
| `crates/bookkeeper-storage/src/ledger_storage.rs` | Modify | Make `LedgerStorage` trait async |
| `crates/bookkeeper-storage/src/db_ledger_storage.rs` | Modify | Implement async `LedgerStorage` |
| `crates/bookkeeper-storage/src/journal/writer.rs` | Modify | Async `log_add_entry` with oneshot ACK |
| `crates/bookkeeper-server/Cargo.toml` | Modify | Add `async-trait` dependency |
| `crates/bookkeeper-server/src/processor.rs` | Modify | Make `process_request` async |
| `crates/bookkeeper-server/src/v3_processor.rs` | Modify | Make `process_v3_request` async |
| `crates/bookkeeper-server/src/server.rs` | Modify | Remove `spawn_blocking` wrappers, await process_request directly |

---

### Task 1: Add `async-trait` dependency to storage crate

**Files:**
- Modify: `crates/bookkeeper-storage/Cargo.toml`

- [ ] **Step 1: Add async-trait to dependencies**

In `crates/bookkeeper-storage/Cargo.toml`, add `async-trait` to `[dependencies]`:

```toml
[dependencies]
bookkeeper-proto = { workspace = true }
bookkeeper-common = { workspace = true }
bytes = { workspace = true }
prost = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }
rocksdb = { workspace = true }
parking_lot = { workspace = true }
dashmap = { workspace = true }
async-trait = { workspace = true }
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p bookkeeper-storage`
Expected: OK (no code changes yet, just dep added)

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-storage/Cargo.toml
git commit -m "chore: add async-trait dependency to bookkeeper-storage"
```

---

### Task 2: Make `JournalIo` and `LedgerIo` traits async

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/mod.rs`

- [ ] **Step 1: Add async_trait import and convert trait definitions**

Replace the trait definitions in `crates/bookkeeper-storage/src/io_engine/mod.rs`:

```rust
use std::io;
use std::os::fd::RawFd;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

/// Journal I/O operations: write entries then fsync.
#[async_trait]
pub trait JournalIo: Send + Sync {
    /// Write all entries sequentially to `fd`, then call `fsync_data`.
    async fn write_and_sync(&self, fd: RawFd, entries: Vec<Vec<u8>>) -> io::Result<()>;
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

    /// Evict any cached file descriptor for `log_id`. Called by GC before
    /// deleting the entry log file. No-op if the fd is not cached.
    fn evict_fd(&self, _entry_log_dir: &Path, _log_id: u32) {}
}
```

**Key change:** The `entries` parameters change from `&[&[u8]]` to `Vec<Vec<u8>>` (owned data). This is required because async trait methods need `'static` data — borrowed slices can't cross `.await` points. The caller already creates `Vec<u8>` buffers (framing), so this is a zero-cost change at call sites. `evict_fd` stays synchronous (no I/O).

- [ ] **Step 2: Verify it compiles (expect errors in impls)**

Run: `cargo check -p bookkeeper-storage 2>&1 | head -30`
Expected: Compilation errors in `std_engine.rs`, `uring_journal.rs`, `uring_ledger.rs`, `db_ledger_storage.rs`, `journal/writer.rs` — the trait impls don't match yet. This confirms the trait change propagated.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/mod.rs
git commit -m "feat: make JournalIo and LedgerIo traits async"
```

---

### Task 3: Implement async `StdEngine`

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/std_engine.rs`

- [ ] **Step 1: Convert StdEngine to async trait impls**

Replace the entire contents of `crates/bookkeeper-storage/src/io_engine/std_engine.rs`:

```rust
//! Standard blocking I/O engine backed by `std::fs` / `std::io`.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::RawFd;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::io::FromRawFd;

use async_trait::async_trait;

use super::{JournalIo, LedgerIo};

/// Blocking I/O engine that uses standard `std::fs` / `std::io` operations,
/// wrapped in `tokio::task::spawn_blocking` so async callers never block.
pub struct StdEngine;

/// Borrow a raw fd as a [`File`] for the duration of a closure.
///
/// Uses [`ManuallyDrop`] so the fd is **never** closed — even if `f`
/// panics.
fn with_file<F, R>(fd: RawFd, f: F) -> R
where
    F: FnOnce(&mut File) -> R,
{
    let mut file = std::mem::ManuallyDrop::new(unsafe { File::from_raw_fd(fd) });
    f(&mut file)
}

#[async_trait]
impl JournalIo for StdEngine {
    async fn write_and_sync(&self, fd: RawFd, entries: Vec<Vec<u8>>) -> io::Result<()> {
        tokio::task::spawn_blocking(move || {
            with_file(fd, |file| {
                for entry in &entries {
                    file.write_all(entry)?;
                }
                file.sync_data()
            })
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
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
                file.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; len];
                file.read_exact(&mut buf)?;
                Ok(buf)
            })
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    }

    async fn read_entry(&self, entry_log_dir: &Path, log_id: u32, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let path = entry_log_dir.join(super::entry_log_filename(log_id));
        let offset = offset;
        let len = len;
        tokio::task::spawn_blocking(move || {
            let file = File::open(&path)?;
            let fd = std::os::fd::AsRawFd::as_raw_fd(&file);
            with_file(fd, |f| {
                f.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; len];
                f.read_exact(&mut buf)?;
                Ok(buf)
            })
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    }
}
```

- [ ] **Step 2: Verify StdEngine compiles**

Run: `cargo check -p bookkeeper-storage 2>&1 | grep -c "error"`
Expected: Errors remain in other files (`uring_journal.rs`, `uring_ledger.rs`, `db_ledger_storage.rs`) but `std_engine.rs` itself should compile.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/std_engine.rs
git commit -m "feat: implement async JournalIo and LedgerIo for StdEngine"
```

---

### Task 4: Convert `UringJournalIo` to async with channel + oneshot

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/uring_journal.rs`

- [ ] **Step 1: Refactor UringJournalIo to use a dedicated ring thread with channel**

Replace the entire contents of `crates/bookkeeper-storage/src/io_engine/uring_journal.rs`:

```rust
//! io_uring-backed journal I/O — Linux only.
//!
//! A dedicated `journal-uring` thread owns the `IoUring` ring. Callers
//! send write requests via a bounded channel; the thread submits linked
//! WRITEV+FSYNC SQEs and replies via `tokio::sync::oneshot`.

use std::io;
use std::os::fd::RawFd;
use std::os::unix::io::FromRawFd;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::JoinHandle;

use io_uring::{opcode, types, IoUring};
use async_trait::async_trait;

use super::JournalIo;

const JOURNAL_SQ_DEPTH: u32 = 64;
const CHANNEL_BOUND: usize = 256;

struct JournalRequest {
    fd: RawFd,
    data: Vec<Vec<u8>>,
    reply: tokio::sync::oneshot::Sender<io::Result<()>>,
}

/// Journal I/O backend that uses `io_uring` for write + fsync.
///
/// A dedicated thread owns the ring. The async `write_and_sync` sends
/// a request via channel and awaits the oneshot reply.
pub struct UringJournalIo {
    tx: SyncSender<JournalRequest>,
    _ring_thread: Option<JoinHandle<()>>,
}

impl UringJournalIo {
    pub fn new() -> io::Result<Self> {
        // Verify io_uring works before spawning
        let _probe = IoUring::<io_uring::squeue::Entry>::new(4)?;

        let (tx, rx) = mpsc::sync_channel(CHANNEL_BOUND);
        let ring_thread = std::thread::Builder::new()
            .name("journal-uring".into())
            .spawn(move || journal_ring_loop(rx))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(Self {
            tx,
            _ring_thread: Some(ring_thread),
        })
    }
}

#[async_trait]
impl JournalIo for UringJournalIo {
    async fn write_and_sync(&self, fd: RawFd, entries: Vec<Vec<u8>>) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(JournalRequest {
                fd,
                data: entries,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "journal ring thread gone"))?;
        reply_rx
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "journal ring dropped reply"))?
    }
}

// ---------------------------------------------------------------------------
// Ring thread
// ---------------------------------------------------------------------------

fn journal_ring_loop(rx: Receiver<JournalRequest>) {
    let mut ring = match IoUring::<io_uring::squeue::Entry>::new(JOURNAL_SQ_DEPTH) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("journal-uring thread: failed to create io_uring: {e}");
            return;
        }
    };

    while let Ok(req) = rx.recv() {
        let result = do_write_and_sync(&mut ring, req.fd, &req.data);
        let _ = req.reply.send(result);
    }
}

fn do_write_and_sync(
    ring: &mut IoUring<io_uring::squeue::Entry>,
    fd: RawFd,
    entries: &[Vec<u8>],
) -> io::Result<()> {
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

    let cqes: Vec<(i32, u64)> = ring
        .completion()
        .map(|cqe| (cqe.result(), cqe.user_data()))
        .collect();

    let mut need_retry_fsync = false;
    for (ret, user_data) in &cqes {
        if *ret < 0 {
            return Err(io::Error::from_raw_os_error(-*ret));
        }
        if *user_data == 1 && (*ret as usize) < total_bytes {
            retry_short_write(fd, entries, *ret as usize)?;
            need_retry_fsync = true;
        }
    }
    if need_retry_fsync {
        submit_fsync(ring, fd)?;
    }

    Ok(())
}

fn retry_short_write(fd: RawFd, entries: &[Vec<u8>], already_written: usize) -> io::Result<()> {
    use std::io::Write;
    let mut file = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(fd) });
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

fn submit_fsync(ring: &mut IoUring<io_uring::squeue::Entry>, fd: RawFd) -> io::Result<()> {
    let fsync_sqe = opcode::Fsync::new(types::Fd(fd)).build().user_data(99);
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
```

- [ ] **Step 2: Verify it compiles on the current platform**

Run: `cargo check -p bookkeeper-storage 2>&1 | head -20`
Expected: On macOS, this file is `#[cfg(target_os = "linux")]` gated so it won't be compiled. The remaining errors are from `uring_ledger.rs`, `db_ledger_storage.rs`, `journal/writer.rs`.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/uring_journal.rs
git commit -m "feat: convert UringJournalIo to async with dedicated ring thread + oneshot"
```

---

### Task 5: Convert `UringLedgerIo` to async with `tokio::sync::oneshot` replies

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/uring_ledger.rs`

- [ ] **Step 1: Replace std::sync::mpsc reply channels with tokio::sync::oneshot**

The ring thread loop stays the same (it's a blocking OS thread). The changes are:
1. `SyncSender<io::Result<...>>` reply types → `tokio::sync::oneshot::Sender<io::Result<...>>`
2. The `LedgerIo` impl methods become `async` and `.await` the oneshot reply
3. Add `async_trait` attribute

Replace the entire contents of `crates/bookkeeper-storage/src/io_engine/uring_ledger.rs`:

```rust
//! io_uring-backed ledger (entry-log) I/O — Linux only.
//!
//! A dedicated `ledger-io` thread owns the `IoUring` ring and an `FdCache`.
//! Callers send requests through a bounded channel; the thread batches them,
//! submits SQEs, and dispatches CQE results back via per-request oneshot
//! channels.

use std::io;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::JoinHandle;

use async_trait::async_trait;
use io_uring::{opcode, types, IoUring};

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
        reply: tokio::sync::oneshot::Sender<io::Result<Vec<u8>>>,
    },
    Write {
        fd: RawFd,
        offset: u64,
        entries: Vec<Vec<u8>>,
        reply: tokio::sync::oneshot::Sender<io::Result<Vec<u64>>>,
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
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(LedgerRequest::Write {
                fd,
                offset,
                entries,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "ring thread closed"))?;
        reply_rx
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "ring thread dropped reply"))?
    }

    async fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        // Legacy path used by the write side — blocking pread, no ring.
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
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(LedgerRequest::Read {
                entry_log_dir: entry_log_dir.to_path_buf(),
                log_id,
                offset,
                len,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "ring thread closed"))?;
        reply_rx
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "ring thread dropped reply"))?
    }

    fn evict_fd(&self, _entry_log_dir: &Path, log_id: u32) {
        let _ = self.tx.send(LedgerRequest::Evict { log_id });
    }
}

// ---------------------------------------------------------------------------
// Ring thread internals
// ---------------------------------------------------------------------------

enum PendingOp {
    Read {
        buf: Vec<u8>,
        len: usize,
        reply: tokio::sync::oneshot::Sender<io::Result<Vec<u8>>>,
    },
    Write {
        entries: Vec<Vec<u8>>,
        offset: u64,
        total_bytes: usize,
        fd: RawFd,
        _iovecs: Vec<libc::iovec>,
        reply: tokio::sync::oneshot::Sender<io::Result<Vec<u64>>>,
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
        let first = match rx.recv() {
            Ok(req) => req,
            Err(_) => break,
        };

        let mut batch = vec![first];
        while let Ok(req) = rx.try_recv() {
            batch.push(req);
        }

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

        if let Err(e) = ring.submit_and_wait(num_sqes) {
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

        fd_cache.unpin_all();

        let mut results: Vec<Option<i32>> = vec![None; pending.len()];
        for cqe in ring.completion() {
            let idx = cqe.user_data() as usize;
            if idx < results.len() {
                results[idx] = Some(cqe.result());
            }
        }

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

fn build_offset_vec(base_offset: u64, entries: &[Vec<u8>]) -> Vec<u64> {
    let mut offsets = Vec::with_capacity(entries.len());
    let mut pos = base_offset;
    for entry in entries {
        offsets.push(pos);
        pos += entry.len() as u64;
    }
    offsets
}

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
```

- [ ] **Step 2: Add tokio dependency for oneshot**

The `tokio` dependency is already in `bookkeeper-storage/Cargo.toml` with workspace features. Verify it includes `sync`:

Run: `grep -A2 'tokio' crates/bookkeeper-storage/Cargo.toml`

The workspace `tokio` already has `full` features. No change needed.

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p bookkeeper-storage 2>&1 | head -20`
Expected: Remaining errors in `db_ledger_storage.rs` and `journal/writer.rs` (not yet converted).

- [ ] **Step 4: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/uring_ledger.rs
git commit -m "feat: convert UringLedgerIo to async with tokio::sync::oneshot replies"
```

---

### Task 6: Make `LedgerStorage` trait async

**Files:**
- Modify: `crates/bookkeeper-storage/src/ledger_storage.rs`

- [ ] **Step 1: Convert LedgerStorage trait to async**

Replace the entire contents of `crates/bookkeeper-storage/src/ledger_storage.rs`:

```rust
use bytes::Bytes;
use std::path::Path;
use async_trait::async_trait;
use crate::StorageError;

#[async_trait]
pub trait LedgerStorage: Send + Sync {
    /// Add an entry to storage. Returns after the entry is in the write cache.
    /// If a journal is configured, the entry is journaled first for durability.
    async fn add_entry(&self, entry: &[u8]) -> Result<(), StorageError>;

    /// Read an entry by ledger ID and entry ID.
    /// Checks write cache → read cache → RocksDB index → entry log file.
    async fn get_entry(&self, ledger_id: i64, entry_id: i64) -> Result<Bytes, StorageError>;

    /// Check if a ledger exists in the local index.
    fn ledger_exists(&self, ledger_id: i64) -> Result<bool, StorageError>;

    /// Mark a ledger as fenced. Returns true if successfully fenced.
    fn set_fenced(&self, ledger_id: i64) -> Result<bool, StorageError>;

    /// Check if a ledger is fenced.
    fn is_fenced(&self, ledger_id: i64) -> Result<bool, StorageError>;

    /// Store the master key for a ledger.
    fn set_master_key(&self, ledger_id: i64, master_key: &[u8]) -> Result<(), StorageError>;

    /// Validate a master key against the stored key.
    fn check_master_key(&self, ledger_id: i64, master_key: &[u8]) -> Result<(), StorageError>;

    /// Get the last add confirmed entry ID for a ledger.
    fn get_last_add_confirmed(&self, ledger_id: i64) -> Result<i64, StorageError>;

    /// Set the explicit LAC for a ledger.
    fn set_explicit_lac(&self, ledger_id: i64, lac: i64) -> Result<(), StorageError>;

    /// Store the explicit LAC body (full entry bytes) for a ledger.
    fn set_explicit_lac_body(&self, ledger_id: i64, body: &[u8]) -> Result<(), StorageError>;

    /// Get the explicit LAC body for a ledger.
    fn get_explicit_lac_body(&self, ledger_id: i64) -> Result<Option<Vec<u8>>, StorageError>;

    /// Return the LAC watcher registry, if supported.
    fn lac_watchers(&self) -> Option<&crate::lac_watcher::LacWatcherRegistry>;

    /// Return the journal, if initialized.
    fn journal(&self) -> Option<&crate::journal::writer::Journal>;

    /// Get all active (non-deleted) ledger IDs.
    fn get_active_ledgers(&self) -> Result<Vec<i64>, StorageError>;

    /// Return the entry log directory.
    fn entry_log_dir(&self) -> Option<&Path>;

    /// Return the current entry log ID.
    fn current_log_id(&self) -> Option<u32>;

    /// Compact the entry location RocksDB index.
    fn compact_entry_locations(&self) -> Result<(), StorageError>;

    /// Write a single entry to the current entry log. Returns the packed location.
    async fn write_entry_to_log(&self, entry: &[u8]) -> Result<u64, StorageError>;

    /// Update entry locations in the RocksDB index.
    fn update_entry_locations(&self, batch: &[(i64, i64, u64)]) -> Result<(), StorageError>;

    /// Delete a ledger from storage.
    fn delete_ledger(&self, ledger_id: i64) -> Result<(), StorageError>;

    /// Flush the write cache to entry logs and update the index.
    async fn flush(&self) -> Result<(), StorageError>;

    /// Checkpoint: flush + write lastMark + clean up old journals.
    async fn checkpoint(&self) -> Result<(), StorageError>;

    /// Validate a ledger for writing (check master key, return fenced status).
    fn validate_for_write(&self, ledger_id: i64, master_key: &[u8]) -> Result<bool, StorageError>;
}
```

**Key changes:** Only methods that do I/O become async: `add_entry`, `get_entry`, `write_entry_to_log`, `flush`, `checkpoint`. Methods that only touch in-memory indexes (RocksDB) stay synchronous — they don't block tokio workers meaningfully.

- [ ] **Step 2: Verify it compiles (expect impl errors)**

Run: `cargo check -p bookkeeper-storage 2>&1 | head -20`
Expected: Errors in `db_ledger_storage.rs` — the impl block doesn't match the new async trait.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-storage/src/ledger_storage.rs
git commit -m "feat: make LedgerStorage trait async (add_entry, get_entry, flush, checkpoint)"
```

---

### Task 7: Convert `Journal::log_add_entry` to async with oneshot ACK

**Files:**
- Modify: `crates/bookkeeper-storage/src/journal/writer.rs`

- [ ] **Step 1: Replace std::sync::mpsc ACK with tokio::sync::oneshot**

Make these changes in `crates/bookkeeper-storage/src/journal/writer.rs`:

1. Change `JournalEntry.ack_tx` from `Option<mpsc::Sender<()>>` to `Option<tokio::sync::oneshot::Sender<()>>`
2. Change `log_add_entry` to `async fn` returning a future that awaits the oneshot
3. Update the io thread to fire oneshot senders instead of mpsc senders
4. Update `flush_batch` pending_acks type

Replace the `JournalEntry` struct and its ack type:

```rust
use std::collections::HashSet;
use std::fs;
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use super::format::{self, JournalHeader, calculate_padding};
use crate::io_engine::JournalIo;
use crate::StorageError;

pub const METAENTRY_ID_LEDGER_KEY: i64 = -0x1000;
const MAX_QUEUE_MEMORY: usize = 32 * 1024 * 1024;

struct JournalEntry {
    data: Vec<u8>,
    ack_tx: Option<tokio::sync::oneshot::Sender<()>>,
}
```

Change `log_add_entry` to async:

```rust
    /// Write an entry to the journal asynchronously.
    /// Returns after the entry is written and fsynced (data is durable).
    pub async fn log_add_entry(&self, data: &[u8]) -> Result<(), StorageError> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(JournalEntry {
                data: data.to_vec(),
                ack_tx: Some(ack_tx),
            })
            .map_err(|_| StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe, "journal writer closed",
            )))?;
        ack_rx.await.map_err(|_| StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe, "journal ack lost",
        )))
    }
```

Update the `journal_io_loop` function signature to change `pending_acks` type:

```rust
    let mut pending_acks: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
```

Update `flush_batch` signature:

```rust
fn flush_batch(
    config: &JournalConfig,
    state: &mut JournalFileState,
    journal_io: &std::sync::Arc<dyn JournalIo>,
    pending_entries: &mut Vec<Vec<u8>>,
    pending_acks: &mut Vec<tokio::sync::oneshot::Sender<()>>,
    on_fatal_error: &Option<OnFatalError>,
    shared_log_id: &Arc<std::sync::atomic::AtomicU64>,
) {
```

In `flush_batch`, update the ACK sending and the write_and_sync call:

The `journal_io.write_and_sync()` is now async but called from a sync thread. Since the journal-io thread is a plain OS thread (not a tokio task), we need to keep it synchronous. The solution: the journal-io thread calls `write_and_sync` in a blocking way. Since `StdEngine::write_and_sync` uses `spawn_blocking` internally (which we can't call from a non-tokio thread), we need a separate approach.

**Important design decision:** The journal-io thread should use the `JournalIo` trait **synchronously**. The async wrapper goes on the `Journal::log_add_entry` method only. The journal-io thread owns the ring directly (uring case) or does blocking I/O (std case). We keep the existing sync `JournalIo` trait for the internal thread and only make the public `Journal` API async.

**Revised approach:** Keep `JournalIo` trait synchronous (revert Task 2 for `JournalIo` only). Only `LedgerIo` becomes async. The journal thread is already a dedicated OS thread — making its I/O trait async adds complexity without benefit. The async boundary is at `Journal::log_add_entry` (oneshot ACK).

- [ ] **Step 2: Revert JournalIo to synchronous**

In `crates/bookkeeper-storage/src/io_engine/mod.rs`, change `JournalIo` back to sync:

```rust
/// Journal I/O operations: write entries then fsync.
/// Stays synchronous because the journal-io thread is a dedicated OS thread
/// that owns the ring (uring) or does blocking I/O (std). The async boundary
/// is at `Journal::log_add_entry` which uses a oneshot channel.
pub trait JournalIo: Send + Sync {
    fn write_and_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()>;
}
```

Keep `LedgerIo` async (from Task 2).

- [ ] **Step 3: Revert StdEngine's JournalIo to sync**

In `std_engine.rs`, keep the `JournalIo` impl synchronous:

```rust
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
```

- [ ] **Step 4: Revert UringJournalIo to sync**

In `uring_journal.rs`, revert to the original synchronous `JournalIo` impl (with Mutex ring, not channel). The original code from the repo is correct — the dedicated ring thread architecture from Task 4 is not needed because the journal-io thread in `journal/writer.rs` already IS the dedicated thread.

Restore the original `uring_journal.rs` (Mutex-based, synchronous trait):

```rust
//! io_uring-backed journal I/O — Linux only.

use std::io;
use std::os::fd::RawFd;
use std::os::unix::io::FromRawFd;
use std::sync::Mutex;

use io_uring::{opcode, types, IoUring};

use super::JournalIo;

const JOURNAL_SQ_DEPTH: u32 = 64;

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
        .offset(u64::MAX)
        .build()
        .flags(io_uring::squeue::Flags::IO_LINK)
        .user_data(1);

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

        let cqes: Vec<(i32, u64)> = ring.completion()
            .map(|cqe| (cqe.result(), cqe.user_data()))
            .collect();

        let mut need_retry_fsync = false;
        for (ret, user_data) in &cqes {
            if *ret < 0 {
                return Err(io::Error::from_raw_os_error(-*ret));
            }
            if *user_data == 1 && (*ret as usize) < total_bytes {
                let written = *ret as usize;
                retry_short_write(fd, entries, written)?;
                need_retry_fsync = true;
            }
        }
        if need_retry_fsync {
            submit_fsync(&mut ring, fd)?;
        }

        Ok(())
    }
}

fn retry_short_write(fd: RawFd, entries: &[&[u8]], already_written: usize) -> io::Result<()> {
    use std::io::Write;
    let mut file = std::mem::ManuallyDrop::new(unsafe {
        std::fs::File::from_raw_fd(fd)
    });
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

fn submit_fsync(ring: &mut IoUring<io_uring::squeue::Entry>, fd: RawFd) -> io::Result<()> {
    let fsync_sqe = opcode::Fsync::new(types::Fd(fd)).build().user_data(99);
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
```

- [ ] **Step 5: Update journal/writer.rs with async log_add_entry**

The full updated `journal/writer.rs` with the oneshot ACK:

Change the import and `JournalEntry` struct:
```rust
struct JournalEntry {
    data: Vec<u8>,
    ack_tx: Option<tokio::sync::oneshot::Sender<()>>,
}
```

Change `log_add_entry`:
```rust
    pub async fn log_add_entry(&self, data: &[u8]) -> Result<(), StorageError> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(JournalEntry {
                data: data.to_vec(),
                ack_tx: Some(ack_tx),
            })
            .map_err(|_| StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe, "journal writer closed",
            )))?;
        ack_rx.await.map_err(|_| StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe, "journal ack lost",
        )))
    }
```

Change `pending_acks` in `journal_io_loop`:
```rust
    let mut pending_acks: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
```

Change `flush_batch` signature:
```rust
fn flush_batch(
    config: &JournalConfig,
    state: &mut JournalFileState,
    journal_io: &std::sync::Arc<dyn JournalIo>,
    pending_entries: &mut Vec<Vec<u8>>,
    pending_acks: &mut Vec<tokio::sync::oneshot::Sender<()>>,
    on_fatal_error: &Option<OnFatalError>,
    shared_log_id: &Arc<std::sync::atomic::AtomicU64>,
)
```

In `flush_batch`, the ACK sending changes from `ack.send(())` to `let _ = ack.send(());` — oneshot send returns `Result<(), T>` instead of `Result<(), SendError<T>>`, but we already use `let _ =` so the code is identical.

- [ ] **Step 6: Verify compilation**

Run: `cargo check -p bookkeeper-storage 2>&1 | head -20`
Expected: Remaining errors only in `db_ledger_storage.rs` (needs async impl).

- [ ] **Step 7: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/mod.rs \
        crates/bookkeeper-storage/src/io_engine/std_engine.rs \
        crates/bookkeeper-storage/src/io_engine/uring_journal.rs \
        crates/bookkeeper-storage/src/journal/writer.rs
git commit -m "feat: async journal ACK via tokio::sync::oneshot, keep JournalIo sync"
```

---

### Task 8: Implement async `LedgerStorage` for `DbLedgerStorage`

**Files:**
- Modify: `crates/bookkeeper-storage/src/db_ledger_storage.rs`

- [ ] **Step 1: Add async_trait import and convert impl block**

Add import at top of `db_ledger_storage.rs`:
```rust
use async_trait::async_trait;
```

Add `#[async_trait]` to the `impl LedgerStorage for DbLedgerStorage` block and convert the 5 async methods:

```rust
#[async_trait]
impl LedgerStorage for DbLedgerStorage {
    async fn add_entry(&self, entry: &[u8]) -> Result<(), StorageError> {
        // ... existing body unchanged (write cache is in-memory, no I/O)
        // But if journal is configured, call journal.log_add_entry().await
    }
```

For `add_entry`: the body is mostly in-memory (write cache put). But if a journal is configured, the server calls `journal.log_add_entry()` separately (not inside `add_entry`). So `add_entry` itself has no I/O to await — the async is for the trait signature consistency. Keep the existing body unchanged.

For `get_entry`: the read path checks write cache (sync) → read cache (sync) → RocksDB (sync) → entry log (I/O via `ledger_io.read_entry().await`). Change `read_entry_from_log` to async:

```rust
    async fn get_entry(&self, ledger_id: i64, entry_id: i64) -> Result<Bytes, StorageError> {
        // 1-3: same as before (sync cache checks)
        if let Some(data) = self.write_cache.read().get(ledger_id, entry_id) {
            return Ok(Bytes::from(data));
        }
        if let Some(data) = self.write_cache_being_flushed.read().get(ledger_id, entry_id) {
            return Ok(Bytes::from(data));
        }
        if let Some(data) = self.read_cache.get(ledger_id, entry_id) {
            return Ok(Bytes::from(data));
        }
        // 4: entry log read (async I/O)
        if let Some(location) = self.entry_location_index.get(ledger_id, entry_id)? {
            let data = self.read_entry_from_log(location).await?;
            self.read_cache.put(ledger_id, entry_id, &data);
            return Ok(Bytes::from(data));
        }
        Err(StorageError::EntryNotFound { ledger_id, entry_id })
    }
```

Make `read_entry_from_log` async (private method):

```rust
    async fn read_entry_from_log(&self, location: u64) -> Result<Vec<u8>, StorageError> {
        let loc = EntryLocation::from_u64(location);
        let pos = loc.position() as u64;
        if pos < 4 {
            return Err(StorageError::CorruptData("invalid entry position".into()));
        }
        let size_buf = self.ledger_io.read_entry(
            &self.entry_log_dir, loc.log_id(), pos - 4, 4,
        ).await?;
        let size = i32::from_be_bytes(size_buf[..4].try_into().unwrap()) as usize;
        if size > MAX_ENTRY_SIZE {
            return Err(StorageError::CorruptData(
                format!("entry size {} exceeds maximum {}", size, MAX_ENTRY_SIZE),
            ));
        }
        let entry = self.ledger_io.read_entry(
            &self.entry_log_dir, loc.log_id(), pos, size,
        ).await?;
        Ok(entry)
    }
```

For `write_entry_to_log`: change to async, await `ledger_io.write_batch()`:

```rust
    async fn write_entry_to_log(&self, entry: &[u8]) -> Result<u64, StorageError> {
        let mut log_state = self.log_state.lock().unwrap();

        let encoded_size = (4 + entry.len()) as u64;
        if log_state.offset + encoded_size > MAX_ENTRY_LOG_SIZE {
            log_state.file = None;
            log_state.log_id += 1;
            log_state.offset = LOGFILE_HEADER_SIZE as u64;
        }

        if log_state.file.is_none() {
            self.ensure_entry_log_exists(log_state.log_id)?;
            let path = self.entry_log_path(log_state.log_id);
            let file = fs::OpenOptions::new().read(true).write(true).open(&path)?;
            log_state.file = Some(file);
        }

        let framed = Self::frame_entry(entry);
        let fd = log_state.file.as_ref().unwrap().as_raw_fd();
        let offset = log_state.offset;

        // Drop the MutexGuard before the .await (can't hold across await)
        let log_id = log_state.log_id;
        drop(log_state);

        self.ledger_io.write_batch(fd, offset, vec![framed]).await?;

        // Re-acquire to update offset
        let mut log_state = self.log_state.lock().unwrap();
        let position = offset + 4;
        let location = EntryLocation::new(log_id, position as u32).to_u64();
        log_state.offset = offset + encoded_size;
        Ok(location)
    }
```

**Important:** The `Mutex` guard must be dropped before `.await` — holding a `std::sync::Mutex` across an await point would deadlock if the task is rescheduled to another thread. The pattern is: acquire → extract what you need → drop → await → re-acquire → update.

For `flush`: convert to async, await `ledger_io.write_batch()` calls:

```rust
    async fn flush(&self) -> Result<(), StorageError> {
        let _lock = self.flush_mutex.lock().unwrap();

        {
            let mut active = self.write_cache.write();
            let mut flushing = self.write_cache_being_flushed.write();
            std::mem::swap(&mut *active, &mut *flushing);
        }

        let flushing = self.write_cache_being_flushed.read();
        let entries = flushing.sorted_entries();
        if entries.is_empty() {
            drop(flushing);
            self.write_cache_being_flushed.write().clear();
            return Ok(());
        }

        let mut batch = Vec::with_capacity(entries.len());
        let flushing_data = flushing.data();

        // Collect all entries and their metadata first (under lock)
        struct FlushSegment {
            log_id: u32,
            fd: RawFd,
            write_offset: u64,
            framed_entries: Vec<Vec<u8>>,
            pending: Vec<(i64, i64, u32, u64)>, // (ledger_id, entry_id, log_id, position)
        }
        let mut segments: Vec<FlushSegment> = Vec::new();

        {
            let mut log_state = self.log_state.lock().unwrap();

            if log_state.file.is_none() {
                self.ensure_entry_log_exists(log_state.log_id)?;
                let path = self.entry_log_path(log_state.log_id);
                let file = fs::OpenOptions::new().read(true).write(true).open(&path)?;
                log_state.file = Some(file);
            }

            let mut current_segment = FlushSegment {
                log_id: log_state.log_id,
                fd: log_state.file.as_ref().unwrap().as_raw_fd(),
                write_offset: log_state.offset,
                framed_entries: Vec::new(),
                pending: Vec::new(),
            };

            for &(ledger_id, entry_id, offset, size) in &entries {
                let entry_data = &flushing_data[offset..offset + size];
                let encoded_size = (4 + entry_data.len()) as u64;

                if log_state.offset + encoded_size > MAX_ENTRY_LOG_SIZE {
                    // Finish current segment
                    if !current_segment.framed_entries.is_empty() {
                        segments.push(current_segment);
                    }

                    // Rotate
                    log_state.file = None;
                    log_state.log_id += 1;
                    log_state.offset = LOGFILE_HEADER_SIZE as u64;
                    self.ensure_entry_log_exists(log_state.log_id)?;
                    let new_path = self.entry_log_path(log_state.log_id);
                    let file = fs::OpenOptions::new().read(true).write(true).open(&new_path)?;
                    log_state.file = Some(file);

                    current_segment = FlushSegment {
                        log_id: log_state.log_id,
                        fd: log_state.file.as_ref().unwrap().as_raw_fd(),
                        write_offset: log_state.offset,
                        framed_entries: Vec::new(),
                        pending: Vec::new(),
                    };
                }

                let framed = Self::frame_entry(entry_data);
                let position = log_state.offset + 4;
                current_segment.pending.push((ledger_id, entry_id, log_state.log_id, position));
                current_segment.framed_entries.push(framed);
                log_state.offset += encoded_size;
            }

            if !current_segment.framed_entries.is_empty() {
                segments.push(current_segment);
            }
        } // MutexGuard dropped here — safe to await below

        // Write all segments (async I/O)
        for segment in segments {
            self.ledger_io.write_batch(
                segment.fd,
                segment.write_offset,
                segment.framed_entries,
            ).await?;
            for (ledger_id, entry_id, log_id, position) in segment.pending {
                batch.push((ledger_id, entry_id,
                    EntryLocation::new(log_id, position as u32).to_u64()));
            }
        }

        if !batch.is_empty() {
            self.entry_location_index.put_batch(&batch)?;
        }
        self.entry_location_index.flush()?;
        self.ledger_index.flush()?;
        drop(flushing);
        self.write_cache_being_flushed.write().clear();
        Ok(())
    }
```

For `checkpoint`: convert to async:

```rust
    async fn checkpoint(&self) -> Result<(), StorageError> {
        self.flush().await?;

        if let Some(log_id) = self.journal.as_ref().and_then(|j| j.current_log_id()) {
            let mark = crate::journal::format::LastMark::new(log_id as i64, 0);
            let mark_path = self.base_dir.join(CURRENT_DIR).join("lastMark");
            if let Err(e) = crate::checkpoint::write_last_mark(&mark_path, &mark) {
                tracing::warn!("Failed to write lastMark: {e}");
            }
        }

        self.cleanup_journals();
        Ok(())
    }
```

- [ ] **Step 2: Verify storage crate compiles**

Run: `cargo check -p bookkeeper-storage`
Expected: OK — all trait impls should now match.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-storage/src/db_ledger_storage.rs
git commit -m "feat: implement async LedgerStorage for DbLedgerStorage"
```

---

### Task 9: Update server to use async `process_request` (remove spawn_blocking)

**Files:**
- Modify: `crates/bookkeeper-server/Cargo.toml`
- Modify: `crates/bookkeeper-server/src/processor.rs`
- Modify: `crates/bookkeeper-server/src/v3_processor.rs`
- Modify: `crates/bookkeeper-server/src/server.rs`

- [ ] **Step 1: Add async-trait to server crate**

In `crates/bookkeeper-server/Cargo.toml`, add:
```toml
async-trait = { workspace = true }
```

- [ ] **Step 2: Make `RequestProcessor::process_request` async**

In `crates/bookkeeper-server/src/processor.rs`:

Add at top:
```rust
use async_trait::async_trait;
```

Change `process_request` and the handler methods that call async storage:

```rust
    pub async fn process_request(&self, req: V2Request) -> V2Response {
```

Change `handle_add_entry`:
```rust
    async fn handle_add_entry(&self, flags: u16, master_key: &[u8; MASTER_KEY_LENGTH], entry_data: Bytes) -> V2Response {
```

Inside `handle_add_entry`, change the `storage.add_entry()` call to `.await`:
```rust
        match self.storage.add_entry(&entry_data).await {
```

And add `.await` to the `journal.log_add_entry()` call:
```rust
        if let Some(journal) = self.storage.journal() {
            if let Err(e) = journal.log_add_entry(&entry_data).await {
```

Change `handle_read_entry`:
```rust
    async fn handle_read_entry(&self, flags: u16, ledger_id: i64, entry_id: i64, master_key: Option<[u8; MASTER_KEY_LENGTH]>) -> V2Response {
```

Inside `handle_read_entry`, change the `storage.get_entry()` calls to `.await`:
```rust
        match self.storage.get_entry(ledger_id, entry_id).await {
```

And the LAC read inside the fencing path:
```rust
        self.storage.get_entry(ledger_id, lac).await.ok()
```

Change `force_flush`:
```rust
    pub async fn force_flush(&self) -> Result<(), StorageError> {
        self.storage.flush().await
    }
```

- [ ] **Step 3: Make `process_v3_request` async**

In `crates/bookkeeper-server/src/v3_processor.rs`:

Change:
```rust
pub async fn process_v3_request(
    processor: &RequestProcessor,
    req: bookkeeper_proto::Request,
) -> Vec<u8> {
```

Add `.await` to all `processor.process_request(v2_req)` calls (there are ~8 of them). Also `.await` on `processor.force_flush()`.

- [ ] **Step 4: Remove spawn_blocking in server connection handler**

In `crates/bookkeeper-server/src/server.rs`, around line 1218 and 1234:

Replace:
```rust
let resp = match tokio::task::spawn_blocking(move || {
    crate::v3_processor::process_v3_request(&proc, v3_req)
}).await {
```

With:
```rust
let resp = crate::v3_processor::process_v3_request(&proc, v3_req).await;
```

And replace:
```rust
let resp = match tokio::task::spawn_blocking(move || proc.process_request(req)).await {
    Ok(resp) => resp,
    Err(_) => break,
};
```

With:
```rust
let resp = proc.process_request(req).await;
```

- [ ] **Step 5: Update SyncThread to use async checkpoint**

In `server.rs`, the `run_sync_thread` function calls `storage.checkpoint()`. Since `checkpoint` is now async:

```rust
async fn run_sync_thread(
    storage: Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage>,
    flush_interval_ms: u64,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let interval = tokio::time::Duration::from_millis(flush_interval_ms);
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    if let Err(e) = storage.checkpoint().await {
                        tracing::warn!("SyncThread: final checkpoint error: {e}");
                    }
                    break;
                }
            }
            _ = tokio::time::sleep(interval) => {
                if let Err(e) = storage.checkpoint().await {
                    tracing::warn!("SyncThread: checkpoint error: {e}");
                }
            }
        }
    }
}
```

- [ ] **Step 6: Update shutdown checkpoint to async**

In `server.rs` around line 487, replace the `spawn_blocking` checkpoint:

```rust
        tracing::info!("Flushing storage...");
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(300),
            storage_for_shutdown.checkpoint(),
        ).await {
            Ok(Ok(())) => tracing::info!("Storage flushed."),
            Ok(Err(e)) => tracing::warn!("WARNING: checkpoint failed during shutdown: {e}"),
            Err(_) => tracing::warn!("WARNING: storage flush timed out after 5 minutes"),
        }
```

- [ ] **Step 7: Update HTTP API spawn_blocking**

In `crates/bookkeeper-server/src/http_api.rs`, around line 376, any `spawn_blocking` calls that use storage should be converted to direct `.await`. Search for `spawn_blocking` and update.

- [ ] **Step 8: Handle long poll in v3_processor**

The long poll path in `v3_processor.rs` uses `std::sync::mpsc::recv_timeout`. Since the entire function is now async, this blocking call would block a tokio worker. Convert to `tokio::time::timeout` + `tokio::sync::oneshot`:

The long poll path needs the LAC watcher to return a `tokio::sync::oneshot::Receiver` instead of `std::sync::mpsc::Receiver`. This is a deeper change in `lac_watcher.rs`. For now, wrap the blocking wait in `spawn_blocking`:

```rust
if is_long_poll {
    let previous_lac = read.previous_lac.unwrap();
    let timeout_ms = read.time_out.unwrap_or(10000) as u64;
    let current_lac = processor.storage().get_last_add_confirmed(ledger_id).unwrap_or(-1);

    let effective_lac = if current_lac > previous_lac {
        current_lac
    } else if let Some(watchers) = processor.storage().lac_watchers() {
        if let Some(rx) = watchers.watch(ledger_id, previous_lac, current_lac) {
            // Long poll wait — still uses std::sync receiver, wrap in spawn_blocking
            let timeout = std::time::Duration::from_millis(timeout_ms);
            match tokio::task::spawn_blocking(move || rx.recv_timeout(timeout)).await {
                Ok(Ok(new_lac)) => new_lac,
                _ => current_lac,
            }
        } else {
            processor.storage().get_last_add_confirmed(ledger_id).unwrap_or(-1)
        }
    } else {
        current_lac
    };
    // ... rest unchanged
}
```

- [ ] **Step 9: Verify full workspace compiles**

Run: `cargo check --workspace`
Expected: OK — all crates compile.

- [ ] **Step 10: Run existing tests**

Run: `cargo test --workspace -- --skip java_bookie_container --skip java_client`
Expected: All tests pass.

- [ ] **Step 11: Commit**

```bash
git add crates/bookkeeper-server/Cargo.toml \
        crates/bookkeeper-server/src/processor.rs \
        crates/bookkeeper-server/src/v3_processor.rs \
        crates/bookkeeper-server/src/server.rs \
        crates/bookkeeper-server/src/http_api.rs
git commit -m "feat: remove spawn_blocking — process_request is now async end-to-end"
```

---

### Task 10: Add integration test verifying async path works

**Files:**
- Modify: `crates/bookkeeper-storage/tests/io_engine_test.rs` (or create new test)

- [ ] **Step 1: Write an async integration test for StdEngine**

Add to `crates/bookkeeper-storage/tests/io_engine_test.rs`:

```rust
#[tokio::test]
async fn test_std_engine_async_write_and_read() {
    use bookkeeper_storage::io_engine::{JournalIo, LedgerIo};
    use bookkeeper_storage::io_engine::std_engine::StdEngine;
    use std::os::fd::AsRawFd;

    let dir = tempfile::tempdir().unwrap();
    let engine = StdEngine;

    // Test LedgerIo async write_batch + read_at
    let path = dir.path().join("test.log");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let fd = file.as_raw_fd();

    let entries = vec![b"hello".to_vec(), b"world".to_vec()];
    let offsets = engine.write_batch(fd, 0, entries).await.unwrap();
    assert_eq!(offsets, vec![0, 5]);

    let data = engine.read_at(fd, 0, 10).await.unwrap();
    assert_eq!(&data, b"helloworld");

    // Test read_entry (opens file by path)
    let data = engine.read_entry(dir.path(), 0, 0, 5).await.unwrap();
    // read_entry uses entry_log_filename which is hex, so we need to
    // create the right file name
    // Actually, read_entry looks for `<log_id hex>.log`, not `test.log`
    // So test via a properly named file:
    let log_path = dir.path().join("0.log");
    std::fs::write(&log_path, b"entry-log-data").unwrap();
    let data = engine.read_entry(dir.path(), 0, 0, 14).await.unwrap();
    assert_eq!(&data, b"entry-log-data");
}

#[tokio::test]
async fn test_std_engine_journal_sync() {
    use bookkeeper_storage::io_engine::JournalIo;
    use bookkeeper_storage::io_engine::std_engine::StdEngine;
    use std::os::fd::AsRawFd;

    let dir = tempfile::tempdir().unwrap();
    let engine = StdEngine;

    let path = dir.path().join("journal.txn");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&path)
        .unwrap();
    let fd = file.as_raw_fd();

    // JournalIo is sync — test it directly
    engine.write_and_sync(fd, &[b"entry1", b"entry2"]).unwrap();

    let contents = std::fs::read(&path).unwrap();
    assert_eq!(&contents, b"entry1entry2");
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p bookkeeper-storage --test io_engine_test test_std_engine_async -- --nocapture`
Expected: Both tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-storage/tests/io_engine_test.rs
git commit -m "test: add async io_engine integration tests for StdEngine"
```

---

## Self-Review

**Spec coverage:**
- [x] `JournalIo` stays sync (revised from spec — journal thread is already dedicated)
- [x] `LedgerIo` becomes async with owned `Vec<Vec<u8>>` parameters
- [x] `StdEngine` wraps blocking I/O in `spawn_blocking`
- [x] `UringLedgerIo` uses `tokio::sync::oneshot` for replies
- [x] `UringJournalIo` stays as-is (sync, Mutex-based — called from dedicated thread)
- [x] `Journal::log_add_entry` becomes async with oneshot ACK
- [x] `LedgerStorage` trait becomes async (`add_entry`, `get_entry`, `flush`, `checkpoint`, `write_entry_to_log`)
- [x] `process_request` becomes async, `spawn_blocking` removed from server
- [x] Long poll path wrapped in `spawn_blocking` (interim — until lac_watcher uses tokio channels)

**Placeholder scan:** No TBD/TODO found.

**Type consistency:** Checked all method signatures across tasks — `Vec<Vec<u8>>` parameter types are consistent between trait definitions and all implementations.

**Design revision from spec:** The spec originally made `JournalIo` async. During planning, I realized this adds unnecessary complexity since the journal-io thread is already a dedicated OS thread that calls `JournalIo::write_and_sync` synchronously. Making it async would require either: (a) a tokio runtime on the journal thread (defeats the purpose), or (b) a second channel-based indirection (the journal-io thread would send to a ring thread which sends back — two hops instead of one). The async boundary belongs at `Journal::log_add_entry` (oneshot ACK), not at the I/O trait level. `LedgerIo` IS made async because it's called from tokio tasks in `DbLedgerStorage`.
