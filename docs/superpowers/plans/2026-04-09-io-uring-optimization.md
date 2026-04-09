# io_uring Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all blocking disk I/O with io_uring on Linux, with transparent std fallback on other platforms, to reduce journal write latency, increase entry log flush throughput, and lower read tail latency.

**Architecture:** An `IoEngine` trait with two implementations (`StdEngine`, `UringEngine`) is injected into `Journal` and `DbLedgerStorage`. `UringEngine` runs two io_uring rings on dedicated threads: a journal ring (WRITEV→linked FSYNC) and a ledger ring (WRITEV for flush, READ for cache misses). Runtime selection via a kernel probe at startup.

**Tech Stack:** `io-uring` crate (0.7, low-level), Linux 5.6+, `#[cfg(target_os = "linux")]` gating, standard `std::fs` for fallback.

**Spec:** `docs/superpowers/specs/2026-04-09-io-uring-optimization-design.md`

---

## File Map

### New files

| File | Responsibility |
|---|---|
| `crates/bookkeeper-storage/src/io_engine/mod.rs` | `IoEngine`, `JournalIo`, `LedgerIo` trait definitions + `select_engine()` + `uring_available()` |
| `crates/bookkeeper-storage/src/io_engine/std_engine.rs` | `StdEngine` — wraps existing `BufWriter`/`fs::File` code |
| `crates/bookkeeper-storage/src/io_engine/uring_engine.rs` | `UringEngine` — owns two rings, delegates to journal/ledger impls (`#[cfg(target_os = "linux")]`) |
| `crates/bookkeeper-storage/src/io_engine/uring_journal.rs` | `UringJournalIo` — journal ring thread: WRITEV→linked FSYNC (`#[cfg(target_os = "linux")]`) |
| `crates/bookkeeper-storage/src/io_engine/uring_ledger.rs` | `UringLedgerIo` — ledger ring thread: WRITEV for flush, READ for cache misses (`#[cfg(target_os = "linux")]`) |
| `crates/bookkeeper-storage/tests/io_engine_test.rs` | Tests for `StdEngine` and (on Linux) `UringEngine` |

### Modified files

| File | Change |
|---|---|
| `crates/bookkeeper-storage/Cargo.toml` | Add `io-uring` as Linux-only dep |
| `crates/bookkeeper-storage/src/lib.rs` | Add `pub mod io_engine;` |
| `crates/bookkeeper-storage/src/journal/writer.rs` | `Journal::start()` takes `Arc<dyn JournalIo>`, uses it for write+sync instead of `BufWriter`+`sync_data`. `StdEngine` path preserves existing 2-thread architecture. |
| `crates/bookkeeper-storage/src/db_ledger_storage.rs` | `DbLedgerStorage::open()` takes `Arc<dyn LedgerIo>`. `flush()`, `read_entry_from_log()`, `write_entry_to_log()` delegate to engine. |
| `crates/bookkeeper-storage/src/ledger_storage.rs` | No trait change — `LedgerStorage` API is unchanged |
| `crates/bookkeeper-server/src/server.rs` | `BookieServer::start()` calls `select_engine()`, passes engine components to Journal and storage |
| `Cargo.toml` (workspace) | Add `io-uring` to workspace deps under Linux target |

---

## Task 1: Add `io-uring` workspace dependency

**Files:**
- Modify: `Cargo.toml` (workspace root, add dep)
- Modify: `crates/bookkeeper-storage/Cargo.toml` (add dep)

- [ ] **Step 1: Add io-uring to workspace Cargo.toml**

In the workspace root `Cargo.toml`, add under `[workspace.dependencies]`:

```toml
[target.'cfg(target_os = "linux")'.dependencies]
io-uring = "0.7"
```

Note: workspace-level target-specific deps are not supported directly. Instead, add it as a plain workspace dep:

```toml
# In [workspace.dependencies] section:
io-uring = "0.7"
```

- [ ] **Step 2: Add io-uring to bookkeeper-storage Cargo.toml**

In `crates/bookkeeper-storage/Cargo.toml`, add:

```toml
[target.'cfg(target_os = "linux")'.dependencies]
io-uring = { workspace = true }
```

- [ ] **Step 3: Verify it compiles on macOS (no-op)**

Run: `cargo build -p bookkeeper-storage`
Expected: SUCCESS — the `io-uring` crate is not pulled in on macOS.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml crates/bookkeeper-storage/Cargo.toml
git commit -m "build: add io-uring workspace dependency (Linux-only)"
```

---

## Task 2: Define `IoEngine` traits and `select_engine()`

**Files:**
- Create: `crates/bookkeeper-storage/src/io_engine/mod.rs`
- Modify: `crates/bookkeeper-storage/src/lib.rs`

- [ ] **Step 1: Create the io_engine module with trait definitions**

Create `crates/bookkeeper-storage/src/io_engine/mod.rs`:

```rust
//! Disk I/O backend abstraction.
//!
//! Two implementations:
//! - `StdEngine`: blocking `BufWriter`/`fs::File` (all platforms)
//! - `UringEngine`: Linux io_uring with dedicated ring threads (Linux 5.6+)

pub mod std_engine;

#[cfg(target_os = "linux")]
pub mod uring_engine;
#[cfg(target_os = "linux")]
pub mod uring_journal;
#[cfg(target_os = "linux")]
pub mod uring_ledger;

use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;

/// Top-level I/O engine providing access to journal and ledger backends.
pub trait IoEngine: Send + Sync + 'static {
    fn journal(&self) -> &dyn JournalIo;
    fn ledger(&self) -> &dyn LedgerIo;
}

/// Journal disk operations (latency-critical write+fsync path).
pub trait JournalIo: Send + Sync {
    /// Write framed journal entries and fsync as an atomic batch.
    /// Each `&[u8]` is one size-prefixed journal entry.
    /// Blocks until the fsync completes (data is durable).
    fn write_and_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()>;
}

/// Ledger (entry log) disk operations: batch writes and positioned reads.
pub trait LedgerIo: Send + Sync {
    /// Batch-write framed entries to the entry log at the given file offset.
    /// Each `&[u8]` is one size-prefixed entry log entry.
    /// Returns the starting byte offset of each entry written.
    fn write_batch(&self, fd: RawFd, offset: u64, entries: &[&[u8]]) -> io::Result<Vec<u64>>;

    /// Read `len` bytes starting at `offset` from the given fd.
    fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>>;
}

/// Probe whether io_uring is available on this platform and kernel.
pub fn uring_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        // Attempt io_uring_setup with minimal params. If it succeeds, uring works.
        match io_uring::IoUring::new(2) {
            Ok(_ring) => true,
            Err(_) => false,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Create the appropriate I/O engine for this platform.
pub fn select_engine() -> Arc<dyn IoEngine> {
    if uring_available() {
        #[cfg(target_os = "linux")]
        {
            tracing::info!("io_uring available — using UringEngine");
            return Arc::new(uring_engine::UringEngine::new());
        }
    }
    tracing::info!("Using StdEngine (standard blocking I/O)");
    Arc::new(std_engine::StdEngine::new())
}
```

- [ ] **Step 2: Register the module in lib.rs**

Add to `crates/bookkeeper-storage/src/lib.rs`:

```rust
pub mod io_engine;
```

- [ ] **Step 3: Create placeholder files for uring modules**

Create empty placeholder files so the module compiles:

`crates/bookkeeper-storage/src/io_engine/std_engine.rs`:
```rust
//! StdEngine — blocking I/O backend using BufWriter and fs::File.
//! Placeholder — implementation in Task 3.
```

`crates/bookkeeper-storage/src/io_engine/uring_engine.rs`:
```rust
//! UringEngine — io_uring backend with journal and ledger rings.
//! Placeholder — implementation in Task 5.
```

`crates/bookkeeper-storage/src/io_engine/uring_journal.rs`:
```rust
//! Journal ring: WRITEV → linked FSYNC on a dedicated io_uring thread.
//! Placeholder — implementation in Task 5.
```

`crates/bookkeeper-storage/src/io_engine/uring_ledger.rs`:
```rust
//! Ledger ring: WRITEV for flush, READ for cache misses.
//! Placeholder — implementation in Task 6.
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build -p bookkeeper-storage`
Expected: SUCCESS (traits defined, placeholders present, no consumers yet)

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/ crates/bookkeeper-storage/src/lib.rs
git commit -m "feat(io_engine): define IoEngine, JournalIo, LedgerIo traits and select_engine()"
```

---

## Task 3: Implement `StdEngine`

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/std_engine.rs`
- Test: `crates/bookkeeper-storage/tests/io_engine_test.rs`

- [ ] **Step 1: Write the StdEngine test**

Create `crates/bookkeeper-storage/tests/io_engine_test.rs`:

```rust
use bookkeeper_storage::io_engine::std_engine::StdEngine;
use bookkeeper_storage::io_engine::{IoEngine, JournalIo, LedgerIo};
use std::os::fd::AsRawFd;
use tempfile::NamedTempFile;

#[test]
fn std_journal_write_and_sync_persists_entries() {
    let engine = StdEngine::new();
    let file = NamedTempFile::new().unwrap();
    let fd = file.as_raw_fd();

    // Write two framed entries
    let entry1 = b"hello-journal-1";
    let entry2 = b"hello-journal-2";
    engine
        .journal()
        .write_and_sync(fd, &[entry1.as_slice(), entry2.as_slice()])
        .unwrap();

    // Read back and verify
    let contents = std::fs::read(file.path()).unwrap();
    assert_eq!(&contents[0..15], b"hello-journal-1");
    assert_eq!(&contents[15..30], b"hello-journal-2");
}

#[test]
fn std_ledger_write_batch_returns_offsets() {
    let engine = StdEngine::new();
    let file = NamedTempFile::new().unwrap();
    let fd = file.as_raw_fd();

    let entry1 = b"entry-aaa"; // 9 bytes
    let entry2 = b"entry-bbbbb"; // 11 bytes
    let offsets = engine
        .ledger()
        .write_batch(fd, 0, &[entry1.as_slice(), entry2.as_slice()])
        .unwrap();

    assert_eq!(offsets.len(), 2);
    assert_eq!(offsets[0], 0);
    assert_eq!(offsets[1], 9);

    // Verify total file size
    let contents = std::fs::read(file.path()).unwrap();
    assert_eq!(contents.len(), 20);
}

#[test]
fn std_ledger_read_at_returns_exact_bytes() {
    let engine = StdEngine::new();
    let file = NamedTempFile::new().unwrap();
    let fd = file.as_raw_fd();

    // Write known data
    std::fs::write(file.path(), b"0123456789abcdef").unwrap();

    // Read 4 bytes at offset 4
    let data = engine.ledger().read_at(fd, 4, 4).unwrap();
    assert_eq!(&data, b"4567");

    // Read 6 bytes at offset 10
    let data = engine.ledger().read_at(fd, 10, 6).unwrap();
    assert_eq!(&data, b"abcdef");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p bookkeeper-storage --test io_engine_test`
Expected: FAIL — `StdEngine::new()` not defined yet

- [ ] **Step 3: Implement StdEngine**

Replace `crates/bookkeeper-storage/src/io_engine/std_engine.rs`:

```rust
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::{FromRawFd, RawFd};

use super::{IoEngine, JournalIo, LedgerIo};

/// Blocking I/O engine using standard `Write`/`Read`/`sync_data`.
/// This is the fallback for macOS and Linux kernels without io_uring.
pub struct StdEngine;

impl StdEngine {
    pub fn new() -> Self {
        Self
    }
}

impl IoEngine for StdEngine {
    fn journal(&self) -> &dyn JournalIo {
        self
    }
    fn ledger(&self) -> &dyn LedgerIo {
        self
    }
}

impl JournalIo for StdEngine {
    fn write_and_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()> {
        // SAFETY: we borrow the fd for the duration of this call.
        // The caller (Journal) owns the file and keeps it open.
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        let mut writer = io::BufWriter::new(&file);
        for entry in entries {
            writer.write_all(entry)?;
        }
        writer.flush()?;
        file.sync_data()?;
        // Prevent drop from closing the fd — caller owns it
        std::mem::forget(file);
        Ok(())
    }
}

impl LedgerIo for StdEngine {
    fn write_batch(&self, fd: RawFd, offset: u64, entries: &[&[u8]]) -> io::Result<Vec<u64>> {
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        let mut writer = io::BufWriter::new(&file);
        writer.seek(SeekFrom::Start(offset))?;

        let mut offsets = Vec::with_capacity(entries.len());
        let mut pos = offset;
        for entry in entries {
            offsets.push(pos);
            writer.write_all(entry)?;
            pos += entry.len() as u64;
        }
        writer.flush()?;
        std::mem::forget(file);
        Ok(offsets)
    }

    fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        let mut reader = io::BufReader::new(&file);
        reader.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        std::mem::forget(file);
        Ok(buf)
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p bookkeeper-storage --test io_engine_test`
Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/std_engine.rs crates/bookkeeper-storage/tests/io_engine_test.rs
git commit -m "feat(io_engine): implement StdEngine with blocking I/O fallback"
```

---

## Task 4: Refactor `Journal` to use `dyn JournalIo`

**Files:**
- Modify: `crates/bookkeeper-storage/src/journal/writer.rs`
- Modify: `crates/bookkeeper-server/src/server.rs`

This is the largest refactor. The `Journal::start()` method gains an `Arc<dyn JournalIo>` parameter. When `StdJournalIo` is passed, the existing 2-thread architecture is preserved. When `UringJournalIo` is passed (Task 5), a single ring thread is used instead.

- [ ] **Step 1: Add JournalIo parameter to Journal::start()**

In `crates/bookkeeper-storage/src/journal/writer.rs`, change `Journal::start`:

```rust
use crate::io_engine::JournalIo;
use std::sync::Arc;

impl Journal {
    /// Start the journal background threads.
    /// When `engine` is StdEngine, uses existing 2-thread writer+fsync architecture.
    /// When `engine` is UringEngine, uses a single io_uring ring thread.
    pub fn start(config: JournalConfig, engine: Arc<dyn JournalIo>) -> Self {
        // ... rest of start() unchanged — still spawns journal-writer + journal-fsync
        // The engine parameter is stored but not used until Task 5 adds the uring path.
        // For now, the StdEngine path ignores it and uses the existing BufWriter code.
```

The key change: `Journal` stores the engine but the `journal_writer_loop` and `fsync_loop` functions continue to use `BufWriter` and `File::sync_data()` directly. This ensures zero behavior change in this task — the engine is wired through but not yet called.

- [ ] **Step 2: Update BookieServer::start() to pass the engine**

In `crates/bookkeeper-server/src/server.rs`, at the point where `storage.init_journal()` is called, add engine selection:

```rust
use bookkeeper_storage::io_engine;

// In BookieServer::start():
let engine = io_engine::select_engine();

// Pass engine.journal() to the journal (Journal::start is called inside init_journal)
```

The `init_journal` method on `DbLedgerStorage` calls `Journal::start(config)` internally. Update it to accept and forward the `Arc<dyn JournalIo>`.

In `crates/bookkeeper-storage/src/db_ledger_storage.rs`, update `init_journal`:

```rust
pub fn init_journal(
    &mut self,
    journal_dir: &Path,
    config: crate::journal::writer::JournalConfig,
    journal_engine: std::sync::Arc<dyn crate::io_engine::JournalIo>,
) -> Result<usize, StorageError> {
    // ... replay unchanged ...
    self.journal = Some(Journal::start(config, journal_engine));
    // ... rest unchanged ...
}
```

In `server.rs`, update the `init_journal` call:

```rust
let replayed = storage.init_journal(
    &journal_current,
    journal_config,
    engine.journal().into(), // Arc clone
)?;
```

Note: `engine.journal()` returns `&dyn JournalIo`. To pass `Arc<dyn JournalIo>`, store the engine arc and clone it. The cleanest approach: `select_engine()` returns `Arc<dyn IoEngine>`, and we call `Arc::new()` on sub-components or store them separately. Adjust as needed during implementation.

- [ ] **Step 3: Run all existing tests**

Run: `cargo test --workspace -- --skip java_bookie_container --skip java_client`
Expected: ALL 476+ tests PASS — behavior is unchanged, only the plumbing is different.

- [ ] **Step 4: Commit**

```bash
git add crates/bookkeeper-storage/src/journal/writer.rs \
       crates/bookkeeper-storage/src/db_ledger_storage.rs \
       crates/bookkeeper-server/src/server.rs
git commit -m "refactor(journal): wire JournalIo trait through Journal::start and init_journal"
```

---

## Task 5: Implement `UringJournalIo` (journal ring)

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/uring_engine.rs`
- Modify: `crates/bookkeeper-storage/src/io_engine/uring_journal.rs`
- Test: `crates/bookkeeper-storage/tests/io_engine_test.rs` (add Linux-only tests)

- [ ] **Step 1: Write the uring journal test (Linux-only)**

Add to `crates/bookkeeper-storage/tests/io_engine_test.rs`:

```rust
#[cfg(target_os = "linux")]
mod uring_tests {
    use bookkeeper_storage::io_engine::uring_engine::UringEngine;
    use bookkeeper_storage::io_engine::{IoEngine, JournalIo};
    use std::os::fd::AsRawFd;
    use tempfile::NamedTempFile;

    #[test]
    fn uring_journal_write_and_sync_persists_entries() {
        let engine = UringEngine::new();
        let file = NamedTempFile::new().unwrap();
        let fd = file.as_raw_fd();

        let entry1 = b"uring-journal-1";
        let entry2 = b"uring-journal-2";
        engine
            .journal()
            .write_and_sync(fd, &[entry1.as_slice(), entry2.as_slice()])
            .unwrap();

        let contents = std::fs::read(file.path()).unwrap();
        assert_eq!(&contents[0..15], b"uring-journal-1");
        assert_eq!(&contents[15..30], b"uring-journal-2");
    }

    #[test]
    fn uring_journal_multiple_batches_append() {
        let engine = UringEngine::new();
        let file = NamedTempFile::new().unwrap();
        let fd = file.as_raw_fd();

        engine.journal().write_and_sync(fd, &[b"batch1"]).unwrap();
        engine.journal().write_and_sync(fd, &[b"batch2"]).unwrap();

        let contents = std::fs::read(file.path()).unwrap();
        assert_eq!(&contents, b"batch1batch2");
    }
}
```

- [ ] **Step 2: Implement UringJournalIo**

Write `crates/bookkeeper-storage/src/io_engine/uring_journal.rs`:

```rust
use std::io;
use std::os::fd::RawFd;
use io_uring::{IoUring, opcode, types};

use super::JournalIo;

const JOURNAL_SQ_DEPTH: u32 = 64;

/// Journal I/O via io_uring. Submits WRITEV → linked FSYNC per batch.
pub struct UringJournalIo {
    // The ring is owned by this struct and used synchronously
    // (submit + wait in the same call). This is safe because
    // write_and_sync is called from a single thread (the journal
    // writer thread or the ring thread).
}

impl UringJournalIo {
    pub fn new() -> io::Result<Self> {
        Ok(Self {})
    }
}

impl JournalIo for UringJournalIo {
    fn write_and_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut ring = IoUring::new(JOURNAL_SQ_DEPTH)?;

        // Build iovec array for scatter-gather write
        let iovecs: Vec<libc::iovec> = entries
            .iter()
            .map(|e| libc::iovec {
                iov_base: e.as_ptr() as *mut _,
                iov_len: e.len(),
            })
            .collect();

        // SQE 1: WRITEV (append at current file position via offset=-1)
        let write_entry = opcode::Writev::new(types::Fd(fd), iovecs.as_ptr(), iovecs.len() as u32)
            .offset(-1i64 as u64) // -1 = append at current position
            .build()
            .flags(io_uring::squeue::Flags::IO_LINK) // link to next SQE
            .user_data(1);

        // SQE 2: FSYNC (linked — kernel guarantees write completes first)
        let fsync_entry = opcode::Fsync::new(types::Fd(fd))
            .build()
            .user_data(2);

        // Submit both SQEs
        unsafe {
            ring.submission()
                .push(&write_entry)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
            ring.submission()
                .push(&fsync_entry)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
        }

        // Submit and wait for both completions
        ring.submit_and_wait(2)?;

        // Check completions
        let cq = ring.completion();
        for cqe in cq {
            let ret = cqe.result();
            if ret < 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }
        }

        Ok(())
    }
}
```

- [ ] **Step 3: Wire UringJournalIo into UringEngine**

Write `crates/bookkeeper-storage/src/io_engine/uring_engine.rs`:

```rust
use std::sync::Arc;
use super::{IoEngine, JournalIo, LedgerIo};
use super::std_engine::StdEngine;
use super::uring_journal::UringJournalIo;

/// io_uring-backed I/O engine with dedicated rings for journal and ledger.
pub struct UringEngine {
    journal: Arc<dyn JournalIo>,
    ledger: Arc<dyn LedgerIo>,
}

impl UringEngine {
    pub fn new() -> Self {
        let journal: Arc<dyn JournalIo> = match UringJournalIo::new() {
            Ok(j) => Arc::new(j),
            Err(e) => {
                tracing::warn!("Failed to create uring journal, falling back to std: {e}");
                Arc::new(StdEngine::new())
            }
        };
        // Ledger uring impl comes in Task 6; use StdEngine for now
        let ledger: Arc<dyn LedgerIo> = Arc::new(StdEngine::new());
        Self { journal, ledger }
    }
}

impl IoEngine for UringEngine {
    fn journal(&self) -> &dyn JournalIo {
        &*self.journal
    }
    fn ledger(&self) -> &dyn LedgerIo {
        &*self.ledger
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p bookkeeper-storage --test io_engine_test`
Expected: std tests PASS on all platforms. On Linux, uring tests also PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/uring_journal.rs \
       crates/bookkeeper-storage/src/io_engine/uring_engine.rs \
       crates/bookkeeper-storage/tests/io_engine_test.rs
git commit -m "feat(io_engine): implement UringJournalIo with WRITEV→linked FSYNC"
```

---

## Task 6: Implement `UringLedgerIo` (ledger ring)

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/uring_ledger.rs`
- Modify: `crates/bookkeeper-storage/src/io_engine/uring_engine.rs` (wire in)
- Test: `crates/bookkeeper-storage/tests/io_engine_test.rs` (add Linux-only tests)

- [ ] **Step 1: Write the uring ledger tests (Linux-only)**

Add to the `uring_tests` module in `io_engine_test.rs`:

```rust
    #[test]
    fn uring_ledger_write_batch_and_read_at_round_trip() {
        let engine = UringEngine::new();
        let file = NamedTempFile::new().unwrap();
        let fd = file.as_raw_fd();

        // Write header placeholder (1024 bytes)
        std::fs::write(file.path(), vec![0u8; 1024]).unwrap();

        let entry1 = b"entry-one";
        let entry2 = b"entry-two-longer";
        let offsets = engine
            .ledger()
            .write_batch(fd, 1024, &[entry1.as_slice(), entry2.as_slice()])
            .unwrap();

        assert_eq!(offsets[0], 1024);
        assert_eq!(offsets[1], 1024 + 9);

        // Read back via read_at
        let data = engine.ledger().read_at(fd, 1024, 9).unwrap();
        assert_eq!(&data, b"entry-one");

        let data = engine.ledger().read_at(fd, 1024 + 9, 16).unwrap();
        assert_eq!(&data, b"entry-two-longer");
    }
```

- [ ] **Step 2: Implement UringLedgerIo**

Write `crates/bookkeeper-storage/src/io_engine/uring_ledger.rs`:

```rust
use std::io;
use std::os::fd::RawFd;
use io_uring::{IoUring, opcode, types};

use super::LedgerIo;

const LEDGER_SQ_DEPTH: u32 = 256;

/// Ledger I/O via io_uring. Handles entry log writes (WRITEV) and reads (READ).
pub struct UringLedgerIo;

impl UringLedgerIo {
    pub fn new() -> io::Result<Self> {
        Ok(Self)
    }
}

impl LedgerIo for UringLedgerIo {
    fn write_batch(&self, fd: RawFd, offset: u64, entries: &[&[u8]]) -> io::Result<Vec<u64>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut ring = IoUring::new(LEDGER_SQ_DEPTH)?;

        // Build iovec for scatter-gather write at the specified offset
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

        // Compute offsets
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
                    format!("short read: got {} bytes, expected {}", ret, len),
                ));
            }
        }

        Ok(buf)
    }
}
```

- [ ] **Step 3: Wire UringLedgerIo into UringEngine**

Update `uring_engine.rs` to use `UringLedgerIo` instead of `StdEngine` for the ledger:

```rust
use super::uring_ledger::UringLedgerIo;

// In UringEngine::new():
let ledger: Arc<dyn LedgerIo> = match UringLedgerIo::new() {
    Ok(l) => Arc::new(l),
    Err(e) => {
        tracing::warn!("Failed to create uring ledger, falling back to std: {e}");
        Arc::new(StdEngine::new())
    }
};
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p bookkeeper-storage --test io_engine_test`
Expected: All tests PASS (std on all platforms, uring on Linux)

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/uring_ledger.rs \
       crates/bookkeeper-storage/src/io_engine/uring_engine.rs \
       crates/bookkeeper-storage/tests/io_engine_test.rs
git commit -m "feat(io_engine): implement UringLedgerIo with WRITEV and READ"
```

---

## Task 7: Refactor `DbLedgerStorage` to use `dyn LedgerIo`

**Files:**
- Modify: `crates/bookkeeper-storage/src/db_ledger_storage.rs`
- Modify: `crates/bookkeeper-server/src/server.rs`

- [ ] **Step 1: Add LedgerIo parameter to DbLedgerStorage::open()**

In `db_ledger_storage.rs`, update `open` to accept and store an `Arc<dyn LedgerIo>`:

```rust
use crate::io_engine::LedgerIo;
use std::sync::Arc;

pub struct DbLedgerStorage {
    // ... existing fields ...
    ledger_io: Arc<dyn LedgerIo>,
}

impl DbLedgerStorage {
    pub fn open(base_dir: &Path, ledger_io: Arc<dyn LedgerIo>) -> Result<Self, StorageError> {
        // ... existing code ...
        Ok(Self {
            // ... existing fields ...
            ledger_io,
        })
    }
}
```

- [ ] **Step 2: Update `read_entry_from_log` to use `LedgerIo::read_at`**

Replace the current `File::open` + `seek` + `read_exact` with:

```rust
fn read_entry_from_log(&self, location: u64) -> Result<Vec<u8>, StorageError> {
    let loc = EntryLocation::from_u64(location);
    let path = self.entry_log_path(loc.log_id());
    let file = fs::File::open(&path)?;
    let fd = std::os::fd::AsRawFd::as_raw_fd(&file);
    let pos = loc.position() as u64;
    if pos < 4 {
        return Err(StorageError::CorruptData("invalid entry position".into()));
    }
    // Read 4-byte size prefix at (pos - 4)
    let size_buf = self.ledger_io.read_at(fd, pos - 4, 4)
        .map_err(StorageError::Io)?;
    let size = i32::from_be_bytes(size_buf[..4].try_into().unwrap()) as usize;
    if size > MAX_ENTRY_SIZE {
        return Err(StorageError::CorruptData(
            format!("entry size {} exceeds maximum {}", size, MAX_ENTRY_SIZE)));
    }
    // Read entry data at pos
    let entry = self.ledger_io.read_at(fd, pos, size)
        .map_err(StorageError::Io)?;
    Ok(entry)
}
```

- [ ] **Step 3: Update `flush()` and `write_entry_to_log()` to use `LedgerIo::write_batch`**

This is the most involved part. The current `flush()` loop writes entries one-by-one via `BufWriter`. Convert it to build a batch of framed entries and call `write_batch()` once per log file (handling rotation between batches).

The `write_entry_to_log()` (used by GC compaction) calls `write_batch()` with a single entry.

The key pattern:

```rust
// In flush(), replace the inner write loop:
let mut entries_for_batch: Vec<Vec<u8>> = Vec::new();
for &(ledger_id, entry_id, offset, size) in &entries {
    let entry_data = &flushing_data[offset..offset + size];
    // Frame the entry: [size(4)][payload]
    let mut framed = Vec::with_capacity(4 + entry_data.len());
    framed.extend_from_slice(&(entry_data.len() as i32).to_be_bytes());
    framed.extend_from_slice(entry_data);
    entries_for_batch.push(framed);
}
// Convert to &[&[u8]] for the trait
let refs: Vec<&[u8]> = entries_for_batch.iter().map(|e| e.as_slice()).collect();
let offsets = self.ledger_io.write_batch(fd, current_offset, &refs)?;
```

- [ ] **Step 4: Update BookieServer::start() to pass LedgerIo to DbLedgerStorage::open()**

```rust
let mut storage = DbLedgerStorage::open(Path::new(ledger_dir), engine.ledger_arc())?;
```

(The exact API for getting an `Arc<dyn LedgerIo>` from the engine depends on how `IoEngine` stores its components. Adjust during implementation.)

- [ ] **Step 5: Run all existing tests**

Run: `cargo test --workspace -- --skip java_bookie_container --skip java_client`
Expected: ALL 476+ tests PASS — StdEngine is the default, behavior unchanged.

- [ ] **Step 6: Commit**

```bash
git add crates/bookkeeper-storage/src/db_ledger_storage.rs \
       crates/bookkeeper-server/src/server.rs
git commit -m "refactor(storage): wire LedgerIo trait through DbLedgerStorage flush and read paths"
```

---

## Task 8: Integration test with `BOOKKEEPER_IO_ENGINE` env override

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/mod.rs` (add env override)
- Test: `crates/bookkeeper-storage/tests/io_engine_test.rs` (add integration test)

- [ ] **Step 1: Add env var override to select_engine()**

Update `select_engine()` in `mod.rs`:

```rust
pub fn select_engine() -> Arc<dyn IoEngine> {
    let override_val = std::env::var("BOOKKEEPER_IO_ENGINE").unwrap_or_default();
    match override_val.as_str() {
        "std" => {
            tracing::info!("BOOKKEEPER_IO_ENGINE=std — forcing StdEngine");
            return Arc::new(std_engine::StdEngine::new());
        }
        "uring" => {
            #[cfg(target_os = "linux")]
            {
                tracing::info!("BOOKKEEPER_IO_ENGINE=uring — forcing UringEngine");
                return Arc::new(uring_engine::UringEngine::new());
            }
            #[cfg(not(target_os = "linux"))]
            {
                tracing::warn!("BOOKKEEPER_IO_ENGINE=uring requested but not on Linux — using StdEngine");
                return Arc::new(std_engine::StdEngine::new());
            }
        }
        _ => {}
    }
    // Auto-detect
    if uring_available() {
        #[cfg(target_os = "linux")]
        {
            tracing::info!("io_uring available — using UringEngine");
            return Arc::new(uring_engine::UringEngine::new());
        }
    }
    tracing::info!("Using StdEngine (standard blocking I/O)");
    Arc::new(std_engine::StdEngine::new())
}
```

- [ ] **Step 2: Add a test that verifies engine selection respects env**

```rust
#[test]
fn select_engine_respects_std_override() {
    std::env::set_var("BOOKKEEPER_IO_ENGINE", "std");
    let engine = bookkeeper_storage::io_engine::select_engine();
    // Verify it's StdEngine by testing behavior (write+read round-trip)
    // ... same as existing std tests ...
    std::env::remove_var("BOOKKEEPER_IO_ENGINE");
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p bookkeeper-storage --test io_engine_test`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/mod.rs \
       crates/bookkeeper-storage/tests/io_engine_test.rs
git commit -m "feat(io_engine): add BOOKKEEPER_IO_ENGINE env var override for engine selection"
```

---

## Task 9: Add `--io-engine` flag to `bk bench-bookie`

**Files:**
- Modify: `crates/bookkeeper-cli/src/bench.rs`
- Modify: `crates/bookkeeper-cli/src/main.rs`

- [ ] **Step 1: Add `--io-engine` flag to BenchBookie command**

In `main.rs`, add to the `BenchBookie` variant:

```rust
/// I/O engine: auto, std, uring
#[arg(long, default_value = "auto")]
io_engine: String,
```

- [ ] **Step 2: Pass engine selection to bench**

In `bench.rs`, use the flag to set `BOOKKEEPER_IO_ENGINE` env var before creating the bookie connection (the benchmark creates an in-process bookie via `BookieServer::start` which calls `select_engine()`):

```rust
if io_engine != "auto" {
    std::env::set_var("BOOKKEEPER_IO_ENGINE", &io_engine);
}
```

- [ ] **Step 3: Run bench --help to verify flag appears**

Run: `cargo run -p bookkeeper-cli -- bench-bookie --help`
Expected: `--io-engine` flag visible in help output

- [ ] **Step 4: Commit**

```bash
git add crates/bookkeeper-cli/src/bench.rs crates/bookkeeper-cli/src/main.rs
git commit -m "feat(bench): add --io-engine flag to bk bench-bookie"
```

---

## Task 10: Final integration verification

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace -- --skip java_bookie_container --skip java_client`
Expected: ALL tests PASS

- [ ] **Step 2: Rebuild and verify docker-compose**

```bash
docker compose down -v
docker compose build rust-bookie-1
docker compose up -d
curl --max-time 10 http://localhost:8080/admin/v2/brokers/health
```
Expected: `ok`

- [ ] **Step 3: Verify StdEngine is used on macOS**

Run: `RUST_LOG=info cargo run -p bookkeeper-cli -- bookiesanity 2>&1 | grep -i engine`
Expected: `Using StdEngine (standard blocking I/O)`

- [ ] **Step 4: Commit all remaining changes**

```bash
git add -A
git commit -m "feat(io_uring): complete io_uring optimization with StdEngine fallback

Implements the io_uring design spec:
- IoEngine trait with JournalIo + LedgerIo sub-traits
- StdEngine: wraps existing BufWriter/fs::File code (all platforms)
- UringEngine: WRITEV→linked FSYNC journal ring + WRITEV/READ ledger ring (Linux 5.6+)
- Runtime selection via uring_available() probe + BOOKKEEPER_IO_ENGINE env override
- Journal and DbLedgerStorage refactored to use dyn trait dispatch
- bk bench-bookie gains --io-engine flag for A/B comparison
- All 476+ existing tests pass unchanged on StdEngine"
```
