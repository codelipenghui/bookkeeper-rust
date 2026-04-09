# Concurrent io_uring Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the serialized `Mutex<IoUring>` read path with a dedicated ring thread that batches concurrent reads into a single `io_uring_enter()`, plus an LRU fd cache with fixed-file registration.

**Architecture:** A `ledger-io` thread owns the io_uring ring and an LRU fd cache (256 slots). Callers send `ReadRequest`/`WriteRequest` via a bounded `std::sync::mpsc` channel and block on a oneshot reply. The ring thread drains N pending requests, submits N SQEs in one syscall, then dispatches CQEs back via `user_data` → reply mapping. `StdEngine` adds a trivial `read_entry` impl with no thread.

**Tech Stack:** `io-uring` 0.7, `std::sync::mpsc`, `libc` for fixed-file registration, no new external deps.

**Spec:** `docs/superpowers/specs/2026-04-09-concurrent-reads-design.md`

---

## File Map

### New files

| File | Responsibility |
|---|---|
| `crates/bookkeeper-storage/src/io_engine/fd_cache.rs` | LRU fd cache with io_uring fixed-file registration (Linux-only) |

### Modified files

| File | Change |
|---|---|
| `crates/bookkeeper-storage/src/io_engine/mod.rs` | Add `read_entry` to `LedgerIo` trait, add `pub mod fd_cache` |
| `crates/bookkeeper-storage/src/io_engine/std_engine.rs` | Implement `read_entry` (open+seek+read, same as today) |
| `crates/bookkeeper-storage/src/io_engine/uring_ledger.rs` | Replace `Mutex<IoUring>` with ring thread + channel + fd cache |
| `crates/bookkeeper-storage/src/io_engine/uring_engine.rs` | Pass entry_log_dir to UringLedgerIo::new() |
| `crates/bookkeeper-storage/src/db_ledger_storage.rs` | `read_entry_from_log` calls `read_entry` instead of `File::open` + `read_at` |
| `crates/bookkeeper-storage/src/gc.rs` | Add `LedgerIo::evict_fd` call before deleting entry log files |
| `crates/bookkeeper-storage/tests/io_engine_test.rs` | Tests for `read_entry` on both engines |

---

## Task 1: Add `read_entry` and `evict_fd` to `LedgerIo` trait

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/mod.rs`

- [ ] **Step 1: Add the new methods to the trait**

In `mod.rs`, add to the `LedgerIo` trait:

```rust
use std::path::Path;

pub trait LedgerIo: Send + Sync {
    // Existing:
    fn write_batch(&self, fd: RawFd, offset: u64, entries: &[&[u8]]) -> io::Result<Vec<u64>>;
    fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>>;

    // New — engine manages file handles internally:
    /// Read `len` bytes at `offset` from entry log `log_id` in `entry_log_dir`.
    /// The engine may cache the file descriptor for future reads.
    fn read_entry(&self, entry_log_dir: &Path, log_id: u32, offset: u64, len: usize) -> io::Result<Vec<u8>>;

    /// Evict any cached file descriptor for `log_id`. Called by GC before
    /// deleting the entry log file. No-op if the fd is not cached.
    fn evict_fd(&self, _entry_log_dir: &Path, _log_id: u32) {}
}
```

Note: `evict_fd` has a default empty implementation so `StdEngine` doesn't need to implement it (it doesn't cache fds).

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p bookkeeper-storage`
Expected: FAIL — `StdEngine` and `UringLedgerIo` don't implement `read_entry` yet.

- [ ] **Step 3: Add stub implementations**

In `std_engine.rs`, add to the `impl LedgerIo for StdEngine` block:

```rust
fn read_entry(&self, entry_log_dir: &Path, log_id: u32, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let path = entry_log_dir.join(format!("{:x}.log", log_id));
    let file = File::open(&path)?;
    let fd = file.as_raw_fd();
    self.read_at(fd, offset, len)
}
```

Add `use std::path::Path;` and `use std::os::fd::AsRawFd;` to imports.

In `uring_ledger.rs`, add a temporary stub to the `impl LedgerIo` block:

```rust
fn read_entry(&self, entry_log_dir: &std::path::Path, log_id: u32, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    // Temporary: delegate to open+read_at. Will be replaced by ring thread in Task 3.
    let path = entry_log_dir.join(format!("{:x}.log", log_id));
    let file = std::fs::File::open(&path)?;
    let fd = std::os::fd::AsRawFd::as_raw_fd(&file);
    self.read_at(fd, offset, len)
}
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build -p bookkeeper-storage`
Expected: SUCCESS

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(io_engine): add LedgerIo::read_entry and evict_fd to trait"
```

---

## Task 2: Switch `DbLedgerStorage` to use `read_entry`

**Files:**
- Modify: `crates/bookkeeper-storage/src/db_ledger_storage.rs`
- Test: existing `db_ledger_storage_test.rs` and `gc_compaction_test.rs`

- [ ] **Step 1: Update `read_entry_from_log` to use `read_entry`**

In `db_ledger_storage.rs`, replace the current `read_entry_from_log` method (lines 184-203):

```rust
fn read_entry_from_log(&self, location: u64) -> Result<Vec<u8>, StorageError> {
    let loc = EntryLocation::from_u64(location);
    let pos = loc.position() as u64;
    if pos < 4 {
        return Err(StorageError::CorruptData("invalid entry position".into()));
    }
    // Read 4-byte size prefix
    let size_buf = self.ledger_io.read_entry(
        &self.entry_log_dir, loc.log_id(), pos - 4, 4,
    )?;
    let size = i32::from_be_bytes(size_buf[..4].try_into().unwrap()) as usize;
    if size > MAX_ENTRY_SIZE {
        return Err(StorageError::CorruptData(
            format!("entry size {} exceeds maximum {}", size, MAX_ENTRY_SIZE)));
    }
    // Read entry data
    let entry = self.ledger_io.read_entry(
        &self.entry_log_dir, loc.log_id(), pos, size,
    )?;
    Ok(entry)
}
```

This removes the `File::open` and raw `read_at(fd, ...)` calls. The engine now owns file handle lifecycle.

- [ ] **Step 2: Add `evict_fd` call to GC before file deletion**

In `crates/bookkeeper-storage/src/gc.rs`, the `GarbageCollector` needs access to the `LedgerIo` to call `evict_fd`. Add it as a field:

Update the `GarbageCollector` struct and `new()`:

```rust
pub struct GarbageCollector {
    storage: Arc<dyn LedgerStorage>,
    ledger_io: Arc<dyn crate::io_engine::LedgerIo>,
    minor_threshold: f64,
    major_threshold: f64,
}

impl GarbageCollector {
    pub fn new(storage: Arc<dyn LedgerStorage>, ledger_io: Arc<dyn crate::io_engine::LedgerIo>) -> Self {
        Self {
            storage,
            ledger_io,
            minor_threshold: MINOR_COMPACTION_THRESHOLD,
            major_threshold: MAJOR_COMPACTION_THRESHOLD,
        }
    }
}
```

Before every `fs::remove_file(&path)` call in `gc_and_compact_entry_logs` and `compact_entry_log`, add:

```rust
self.ledger_io.evict_fd(entry_log_dir, log_id);
```

Update all callers of `GarbageCollector::new()` to pass the `ledger_io` Arc (server.rs GC loop, test files).

- [ ] **Step 3: Run all tests**

Run: `cargo test -p bookkeeper-storage`
Expected: ALL tests pass — `StdEngine::read_entry` is functionally identical to the old code.

- [ ] **Step 4: Commit**

```bash
git commit -m "refactor: switch read path to LedgerIo::read_entry, add evict_fd to GC"
```

---

## Task 3: Implement the LRU fd cache

**Files:**
- Create: `crates/bookkeeper-storage/src/io_engine/fd_cache.rs`
- Modify: `crates/bookkeeper-storage/src/io_engine/mod.rs` (add module)
- Test: `crates/bookkeeper-storage/tests/io_engine_test.rs`

This is a self-contained data structure, testable independently.

- [ ] **Step 1: Write tests for the fd cache**

Add to `io_engine_test.rs`:

```rust
#[test]
fn fd_cache_open_and_reuse() {
    use bookkeeper_storage::io_engine::fd_cache::FdCache;
    let dir = tempfile::TempDir::new().unwrap();
    // Create two dummy entry log files
    std::fs::write(dir.path().join("0.log"), vec![0u8; 1024]).unwrap();
    std::fs::write(dir.path().join("1.log"), vec![0u8; 1024]).unwrap();

    let mut cache = FdCache::new(4); // capacity=4
    let (fd1, idx1) = cache.get_or_open(dir.path(), 0).unwrap();
    let (fd2, idx2) = cache.get_or_open(dir.path(), 0).unwrap();
    // Same log_id should return same fd and fixed index
    assert_eq!(fd1, fd2);
    assert_eq!(idx1, idx2);

    let (fd3, idx3) = cache.get_or_open(dir.path(), 1).unwrap();
    // Different log_id, different fd, different index
    assert_ne!(fd1, fd3);
    assert_ne!(idx1, idx3);
}

#[test]
fn fd_cache_eviction() {
    use bookkeeper_storage::io_engine::fd_cache::FdCache;
    let dir = tempfile::TempDir::new().unwrap();
    for i in 0..5 {
        std::fs::write(dir.path().join(format!("{:x}.log", i)), vec![0u8; 64]).unwrap();
    }

    let mut cache = FdCache::new(3); // capacity=3
    cache.get_or_open(dir.path(), 0).unwrap();
    cache.get_or_open(dir.path(), 1).unwrap();
    cache.get_or_open(dir.path(), 2).unwrap();
    // Cache full (3/3). Opening log_id=3 evicts LRU (log_id=0).
    cache.get_or_open(dir.path(), 3).unwrap();
    assert_eq!(cache.len(), 3);
    assert!(!cache.contains(0));
    assert!(cache.contains(3));
}

#[test]
fn fd_cache_explicit_evict() {
    use bookkeeper_storage::io_engine::fd_cache::FdCache;
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::write(dir.path().join("0.log"), vec![0u8; 64]).unwrap();

    let mut cache = FdCache::new(4);
    cache.get_or_open(dir.path(), 0).unwrap();
    assert!(cache.contains(0));
    cache.evict(0);
    assert!(!cache.contains(0));
}
```

- [ ] **Step 2: Implement FdCache**

Create `crates/bookkeeper-storage/src/io_engine/fd_cache.rs`:

```rust
//! LRU file-descriptor cache for entry log files.
//!
//! Holds open `File` handles keyed by `log_id`. On Linux with io_uring,
//! the cache also manages a fixed-file registration table so SQEs can
//! use `types::Fixed(index)` for zero-overhead fd resolution.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;

/// Entry in the fd cache.
struct CacheEntry {
    file: File,
    fixed_index: u32,
    /// For LRU ordering: incremented on each access.
    last_used: u64,
}

/// LRU cache of open file descriptors for entry log files.
pub struct FdCache {
    entries: HashMap<u32, CacheEntry>,   // log_id → entry
    capacity: usize,
    access_counter: u64,
    /// Free fixed-index slots (recycled on eviction).
    free_indices: Vec<u32>,
}

impl FdCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            capacity,
            access_counter: 0,
            free_indices: (0..capacity as u32).rev().collect(),
        }
    }

    /// Get the fd and fixed-file index for `log_id`, opening it if needed.
    /// Returns `(raw_fd, fixed_index)`.
    pub fn get_or_open(
        &mut self,
        entry_log_dir: &Path,
        log_id: u32,
    ) -> io::Result<(RawFd, u32)> {
        self.access_counter += 1;
        let counter = self.access_counter;

        if let Some(entry) = self.entries.get_mut(&log_id) {
            entry.last_used = counter;
            return Ok((entry.file.as_raw_fd(), entry.fixed_index));
        }

        // Cache miss — open the file
        if self.entries.len() >= self.capacity {
            self.evict_lru();
        }

        let path = entry_log_dir.join(format!("{:x}.log", log_id));
        let file = File::open(&path)?;
        let fd = file.as_raw_fd();
        let fixed_index = self.free_indices.pop().unwrap_or(0);

        self.entries.insert(log_id, CacheEntry {
            file,
            fixed_index,
            last_used: counter,
        });

        Ok((fd, fixed_index))
    }

    /// Explicitly evict a log_id (called by GC before file deletion).
    pub fn evict(&mut self, log_id: u32) {
        if let Some(entry) = self.entries.remove(&log_id) {
            self.free_indices.push(entry.fixed_index);
        }
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if a log_id is cached.
    pub fn contains(&self, log_id: u32) -> bool {
        self.entries.contains_key(&log_id)
    }

    fn evict_lru(&mut self) {
        if let Some((&lru_id, _)) = self
            .entries
            .iter()
            .min_by_key(|(_, e)| e.last_used)
        {
            if let Some(entry) = self.entries.remove(&lru_id) {
                self.free_indices.push(entry.fixed_index);
            }
        }
    }
}
```

Add `pub mod fd_cache;` to `mod.rs` (NOT gated behind `cfg(linux)` — the cache logic is platform-independent, only the fixed-file registration is Linux-specific).

- [ ] **Step 3: Run tests**

Run: `cargo test -p bookkeeper-storage --test io_engine_test`
Expected: New cache tests PASS + existing tests PASS.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(io_engine): add LRU fd cache for entry log files"
```

---

## Task 4: Replace `UringLedgerIo` with ring thread + channel

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/uring_ledger.rs` (major rewrite)
- Test: `crates/bookkeeper-storage/tests/io_engine_test.rs` (Linux-only concurrent test)

This is the core task. The `Mutex<IoUring>` is replaced with a dedicated thread.

- [ ] **Step 1: Write the concurrent read test (Linux-only)**

Add to `io_engine_test.rs` in the `uring_tests` module:

```rust
#[cfg(target_os = "linux")]
#[test]
fn uring_ledger_concurrent_reads_return_correct_data() {
    use std::sync::Arc;
    use bookkeeper_storage::io_engine::uring_ledger::UringLedgerIo;
    use bookkeeper_storage::io_engine::LedgerIo;

    let dir = tempfile::TempDir::new().unwrap();
    // Create an entry log with known data at known offsets
    let mut data = vec![0u8; 1024]; // header
    for i in 0u32..10 {
        let entry = format!("entry-{:04}", i);
        data.extend_from_slice(entry.as_bytes());
    }
    let path = dir.path().join("0.log");
    std::fs::write(&path, &data).unwrap();

    let engine = Arc::new(UringLedgerIo::new().unwrap());
    let mut handles = Vec::new();

    // Spawn 10 threads, each reading a different entry
    for i in 0u32..10 {
        let engine = engine.clone();
        let dir_path = dir.path().to_path_buf();
        let offset = 1024 + (i as u64 * 10); // each entry is 10 bytes
        handles.push(std::thread::spawn(move || {
            let data = engine.read_entry(&dir_path, 0, offset, 10).unwrap();
            let text = String::from_utf8(data).unwrap();
            assert_eq!(text, format!("entry-{:04}", i));
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}
```

- [ ] **Step 2: Rewrite UringLedgerIo with ring thread**

Replace the contents of `uring_ledger.rs` with:

The new `UringLedgerIo` struct holds:
- `tx: std::sync::mpsc::SyncSender<LedgerRequest>` — bounded channel
- `_ring_thread: Option<std::thread::JoinHandle<()>>` — the ring thread

`LedgerRequest` is an enum with `Read` and `Write` variants, each carrying a oneshot reply sender.

The ring thread loop:
1. `recv()` for first request, then `try_recv()` to drain batch
2. For Read requests: resolve `log_id` via `FdCache::get_or_open`, build `READ` SQE with `types::Fd(fd)` (fixed-file registration is a follow-up), set `user_data` = request index
3. For Write requests: build `WRITEV` SQE as before
4. `submit_and_wait(N)` for all SQEs
5. For each CQE: match `user_data` → send result to reply channel

`read_entry()` sends a `LedgerRequest::Read` and blocks on the reply.
`write_batch()` sends a `LedgerRequest::Write` and blocks on the reply.
`read_at()` delegates to `read_entry` by constructing the path from fd (backward compat; used by write path that passes raw fd).
`evict_fd()` sends an `Evict` request to the ring thread.

- [ ] **Step 3: Run all tests**

Run: `cargo test -p bookkeeper-storage`
Expected: All tests pass. On Linux, the concurrent test verifies batching.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(io_engine): replace UringLedgerIo Mutex with ring thread + fd cache"
```

---

## Task 5: Final integration verification

- [ ] **Step 1: Full workspace test**

Run: `cargo test --workspace -- --skip java_bookie_container --skip java_client`
Expected: ALL tests pass.

- [ ] **Step 2: Linux container test**

```bash
docker compose -f docker-compose-test.yml build test-uring-only
docker compose -f docker-compose-test.yml run --rm test-uring-only \
  cargo test -p bookkeeper-storage --test io_engine_test
```
Expected: All 8+ tests pass including the new concurrent read test.

- [ ] **Step 3: Rebuild and verify docker-compose playground**

```bash
docker compose down -v
docker compose build rust-bookie-1
docker compose up -d
curl --max-time 10 http://localhost:8080/admin/v2/brokers/health
```
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(io_engine): concurrent io_uring reads with fd cache — complete"
```
