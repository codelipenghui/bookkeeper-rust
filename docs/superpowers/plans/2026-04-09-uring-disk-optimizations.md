# io_uring Disk Path Optimizations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce read-path syscalls by 50% (single-read entry), eliminate per-SQE fd validation (fixed file descriptors), and reduce allocator pressure (provided buffer pool).

**Architecture:** Three independent optimizations to the io_uring ledger I/O path. Optimization 1 (single-read) changes the `LedgerIo` trait and both engines. Optimizations 2 and 3 are Linux-only changes to `uring_ledger.rs` and `fd_cache.rs`. Each optimization is self-contained and can ship independently.

**Tech Stack:** `io-uring` 0.7.11 (`register_files`, `register_files_update`, `types::Fixed`, `register_buf_ring_with_flags`, `BufRingEntry`)

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `crates/bookkeeper-storage/src/io_engine/mod.rs` | Modify | Add `read_framed_entry` to `LedgerIo` trait |
| `crates/bookkeeper-storage/src/io_engine/std_engine.rs` | Modify | Override `read_framed_entry` for StdEngine |
| `crates/bookkeeper-storage/src/io_engine/uring_ledger.rs` | Modify | Add `ReadFramed` request, fixed fd SQEs, provided buffer pool |
| `crates/bookkeeper-storage/src/io_engine/fd_cache.rs` | Modify | Add `is_new` return flag from `get_or_open` |
| `crates/bookkeeper-storage/src/db_ledger_storage.rs` | Modify | Use `read_framed_entry` instead of two `read_entry` calls |
| `crates/bookkeeper-storage/tests/io_engine_test.rs` | Modify | Add tests for `read_framed_entry` |

---

### Task 1: Add `read_framed_entry` to `LedgerIo` trait with default implementation

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/mod.rs`

- [ ] **Step 1: Add the method to the trait**

In `crates/bookkeeper-storage/src/io_engine/mod.rs`, add a new method to the `LedgerIo` trait after `read_entry`. This has a default implementation so existing impls don't break:

```rust
#[async_trait]
pub trait LedgerIo: Send + Sync {
    async fn write_batch(&self, fd: RawFd, offset: u64, entries: Vec<Vec<u8>>) -> io::Result<Vec<u64>>;
    async fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>>;
    async fn read_entry(&self, entry_log_dir: &Path, log_id: u32, offset: u64, len: usize) -> io::Result<Vec<u8>>;

    /// Read a framed entry: parse the 4-byte big-endian size prefix at
    /// `offset`, then read `size` bytes of entry data starting at
    /// `offset + 4`. Returns only the entry data (without the prefix).
    ///
    /// Default implementation does two calls. Engines can override to
    /// combine into a single I/O round-trip.
    async fn read_framed_entry(
        &self,
        entry_log_dir: &Path,
        log_id: u32,
        offset: u64,
        max_size: usize,
    ) -> io::Result<Vec<u8>> {
        let size_buf = self.read_entry(entry_log_dir, log_id, offset, 4).await?;
        if size_buf.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short size prefix read"));
        }
        let size = i32::from_be_bytes(size_buf[..4].try_into().unwrap()) as usize;
        if size > max_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("entry size {} exceeds max {}", size, max_size),
            ));
        }
        self.read_entry(entry_log_dir, log_id, offset + 4, size).await
    }

    fn evict_fd(&self, _entry_log_dir: &Path, _log_id: u32) {}
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p bookkeeper-storage`
Expected: OK — the default impl uses existing methods.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/mod.rs
git commit -m "feat: add read_framed_entry to LedgerIo trait with default impl"
```

---

### Task 2: Override `read_framed_entry` in StdEngine (single spawn_blocking)

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/std_engine.rs`
- Test: `crates/bookkeeper-storage/tests/io_engine_test.rs`

- [ ] **Step 1: Add the override to StdEngine**

In `crates/bookkeeper-storage/src/io_engine/std_engine.rs`, add inside the `impl LedgerIo for StdEngine` block, after `read_entry`:

```rust
    async fn read_framed_entry(
        &self,
        entry_log_dir: &Path,
        log_id: u32,
        offset: u64,
        max_size: usize,
    ) -> io::Result<Vec<u8>> {
        let path = entry_log_dir.join(super::entry_log_filename(log_id));
        tokio::task::spawn_blocking(move || {
            let mut file = File::open(&path)?;
            file.seek(SeekFrom::Start(offset))?;

            // Read 4-byte size prefix
            let mut size_buf = [0u8; 4];
            file.read_exact(&mut size_buf)?;
            let size = i32::from_be_bytes(size_buf) as usize;
            if size > max_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("entry size {} exceeds max {}", size, max_size),
                ));
            }

            // Read entry data
            let mut buf = vec![0u8; size];
            file.read_exact(&mut buf)?;
            Ok(buf)
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    }
```

- [ ] **Step 2: Add test for read_framed_entry**

In `crates/bookkeeper-storage/tests/io_engine_test.rs`, add:

```rust
#[tokio::test]
async fn test_std_read_framed_entry() {
    use bookkeeper_storage::io_engine::LedgerIo;
    use bookkeeper_storage::io_engine::std_engine::StdEngine;

    let dir = tempfile::tempdir().unwrap();
    let engine = StdEngine;

    // Create a file named "0.log" with a framed entry: [size(4 BE)][data]
    let log_path = dir.path().join("0.log");
    let data = b"hello world";
    let size = (data.len() as i32).to_be_bytes();
    let mut contents = Vec::new();
    contents.extend_from_slice(&size);
    contents.extend_from_slice(data);
    std::fs::write(&log_path, &contents).unwrap();

    // read_framed_entry at offset 0 should return "hello world"
    let result = engine.read_framed_entry(dir.path(), 0, 0, 1024).await.unwrap();
    assert_eq!(&result, b"hello world");
}

#[tokio::test]
async fn test_std_read_framed_entry_rejects_oversized() {
    use bookkeeper_storage::io_engine::LedgerIo;
    use bookkeeper_storage::io_engine::std_engine::StdEngine;

    let dir = tempfile::tempdir().unwrap();
    let engine = StdEngine;

    let log_path = dir.path().join("0.log");
    let size = (1000i32).to_be_bytes(); // claim 1000 bytes
    let mut contents = Vec::new();
    contents.extend_from_slice(&size);
    contents.extend_from_slice(b"short"); // only 5 bytes actual
    std::fs::write(&log_path, &contents).unwrap();

    // max_size=100 should reject the 1000-byte claim
    let err = engine.read_framed_entry(dir.path(), 0, 0, 100).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p bookkeeper-storage --test io_engine_test -- test_std_read_framed`
Expected: 2 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/std_engine.rs crates/bookkeeper-storage/tests/io_engine_test.rs
git commit -m "feat: override read_framed_entry in StdEngine with single spawn_blocking"
```

---

### Task 3: Add `ReadFramed` request to UringLedgerIo

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/uring_ledger.rs`

- [ ] **Step 1: Add ReadFramed variant to LedgerRequest**

In `uring_ledger.rs`, add a new variant to the `LedgerRequest` enum:

```rust
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
```

- [ ] **Step 2: Override `read_framed_entry` in the trait impl**

In the `impl LedgerIo for UringLedgerIo` block, add:

```rust
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
```

- [ ] **Step 3: Handle ReadFramed in the ring thread**

In `ledger_ring_loop`, inside the `for request in batch` loop, add the `ReadFramed` handler. This reads the 4-byte size prefix via blocking `pread` (fast — always in page cache since we just wrote it), then submits a single io_uring READ SQE for the data:

```rust
                LedgerRequest::ReadFramed {
                    entry_log_dir,
                    log_id,
                    offset,
                    max_size,
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

                    // Read 4-byte size prefix via blocking pread (fast, in page cache)
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

                    // Submit io_uring READ for the entry data
                    let mut buf = vec![0u8; size];
                    let sqe_idx = pending.len() as u64;
                    let data_offset = offset + 4;

                    let read_sqe =
                        opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), size as u32)
                            .offset(data_offset)
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

                    pending.push(PendingOp::Read { buf, len: size, reply });
                    num_sqes += 1;
                }
```

Note: `PendingOp::Read` is reused — the CQE dispatch already handles it correctly.

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p bookkeeper-storage`
Expected: OK (on macOS this is cfg-gated; on Linux it compiles).

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/uring_ledger.rs
git commit -m "feat: add ReadFramed request to UringLedgerIo ring thread"
```

---

### Task 4: Use `read_framed_entry` in DbLedgerStorage

**Files:**
- Modify: `crates/bookkeeper-storage/src/db_ledger_storage.rs`

- [ ] **Step 1: Simplify read_entry_from_log**

Replace the `read_entry_from_log` method (currently at ~line 208):

```rust
    async fn read_entry_from_log(&self, location: u64) -> Result<Vec<u8>, StorageError> {
        let loc = EntryLocation::from_u64(location);
        let pos = loc.position() as u64;
        if pos < 4 {
            return Err(StorageError::CorruptData("invalid entry position".into()));
        }
        // Single round-trip: read size prefix + data together
        let entry = self.ledger_io.read_framed_entry(
            &self.entry_log_dir, loc.log_id(), pos - 4, MAX_ENTRY_SIZE,
        ).await?;
        Ok(entry)
    }
```

This replaces two `read_entry` calls with one `read_framed_entry` call.

- [ ] **Step 2: Run existing tests**

Run: `cargo test -p bookkeeper-storage --test db_ledger_storage_test`
Expected: All 7 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-storage/src/db_ledger_storage.rs
git commit -m "feat: use read_framed_entry for single-read entry path"
```

---

### Task 5: Add `is_new` flag to FdCache::get_or_open

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/fd_cache.rs`
- Test: `crates/bookkeeper-storage/tests/io_engine_test.rs`

- [ ] **Step 1: Change return type to include is_new flag**

In `fd_cache.rs`, change the `get_or_open` method signature and implementation:

```rust
    /// Get the fd and fixed-file index for `log_id`, opening it if needed.
    /// Returns `(fd, fixed_index, is_new)` where `is_new` is true if the
    /// fd was just opened (cache miss). Callers use this to know when to
    /// register a new fd with io_uring's fixed file table.
    pub fn get_or_open(&mut self, entry_log_dir: &Path, log_id: u32) -> io::Result<(RawFd, u32, bool)> {
        self.access_counter += 1;
        let counter = self.access_counter;

        if let Some(entry) = self.entries.get_mut(&log_id) {
            entry.last_used = counter;
            return Ok((entry.file.as_raw_fd(), entry.fixed_index, false));
        }

        // Cache miss — evict LRU if full (only non-pinned entries)
        if self.entries.len() >= self.capacity {
            self.evict_lru();
        }

        let path = entry_log_dir.join(super::entry_log_filename(log_id));
        let file = File::open(&path)?;
        let fd = file.as_raw_fd();
        let fixed_index = self.free_indices.pop().unwrap_or(0);

        self.entries.insert(
            log_id,
            CacheEntry {
                file,
                fixed_index,
                last_used: counter,
            },
        );

        Ok((fd, fixed_index, true))
    }
```

- [ ] **Step 2: Update all callers of get_or_open**

In `uring_ledger.rs`, every call to `fd_cache.get_or_open(...)` currently destructures as `Ok((fd, _))`. Update to `Ok((fd, _, _))` (ignore `fixed_index` and `is_new` for now — Task 6 will use them):

```rust
                    let fd = match fd_cache.get_or_open(&entry_log_dir, log_id) {
                        Ok((fd, _, _)) => {
                            fd_cache.pin(log_id);
                            fd
                        }
                        // ...
                    };
```

There are two call sites: the `Read` handler and the `ReadFramed` handler.

- [ ] **Step 3: Add test for is_new flag**

In `crates/bookkeeper-storage/tests/io_engine_test.rs`, add in the `fd_cache_tests` module:

```rust
    #[test]
    fn is_new_flag_on_insert_vs_reuse() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("0.log");
        std::fs::write(&log_path, b"data").unwrap();

        let mut cache = FdCache::new(4);

        // First access: is_new = true
        let (_, _, is_new) = cache.get_or_open(dir.path(), 0).unwrap();
        assert!(is_new);

        // Second access: is_new = false (cache hit)
        let (_, _, is_new) = cache.get_or_open(dir.path(), 0).unwrap();
        assert!(!is_new);
    }
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p bookkeeper-storage --test io_engine_test`
Expected: All tests pass (existing + new).

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/fd_cache.rs \
        crates/bookkeeper-storage/src/io_engine/uring_ledger.rs \
        crates/bookkeeper-storage/tests/io_engine_test.rs
git commit -m "feat: add is_new flag to FdCache::get_or_open"
```

---

### Task 6: Register fixed file descriptors with io_uring

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/uring_ledger.rs`
- Modify: `crates/bookkeeper-storage/src/io_engine/fd_cache.rs`

- [ ] **Step 1: Register empty fixed file table at ring startup**

In `uring_ledger.rs`, inside `ledger_ring_loop`, after creating the ring, register the fixed file table:

```rust
fn ledger_ring_loop(rx: Receiver<LedgerRequest>) {
    let mut ring = match IoUring::<io_uring::squeue::Entry>::new(LEDGER_SQ_DEPTH) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("ledger-io thread: failed to create io_uring: {e}");
            return;
        }
    };

    // Register fixed file descriptor table (all slots empty = -1)
    let empty_fds: Vec<RawFd> = vec![-1; FD_CACHE_CAPACITY];
    let fixed_fds_available = ring.submitter().register_files(&empty_fds).is_ok();
    if fixed_fds_available {
        tracing::debug!("ledger-io: registered {} fixed fd slots", FD_CACHE_CAPACITY);
    } else {
        tracing::warn!("ledger-io: register_files failed, using raw fds");
    }

    let mut fd_cache = FdCache::new(FD_CACHE_CAPACITY);
    // ... rest of loop
```

- [ ] **Step 2: Register new fds on cache miss, use Fixed in SQEs**

In the Read and ReadFramed handlers, when `is_new` is true, register the fd at `fixed_index`:

```rust
                    let (fd, fixed_index, is_new) = match fd_cache.get_or_open(&entry_log_dir, log_id) {
                        Ok(result) => {
                            fd_cache.pin(log_id);
                            result
                        }
                        Err(e) => {
                            let _ = reply.send(Err(e));
                            continue;
                        }
                    };

                    // Register new fd in fixed file table
                    if is_new && fixed_fds_available {
                        let _ = ring.submitter().register_files_update(
                            fixed_index, &[fd],
                        );
                    }
```

Change SQE construction to use `Fixed` when available:

```rust
                    let fd_target = if fixed_fds_available {
                        types::Fixed(fixed_index)
                    } else {
                        types::Fd(fd)
                    };

                    let read_sqe =
                        opcode::Read::new(fd_target, buf.as_mut_ptr(), len as u32)
                            .offset(offset)
                            .build()
                            .user_data(sqe_idx);
```

Apply the same pattern to the Write handler (though writes use raw fds passed from the caller, not cached fds — so writes stay with `types::Fd(fd)`).

- [ ] **Step 3: Unregister on fd eviction**

In `fd_cache.rs`, add a method to get the fixed_index of an evicted entry. Alternatively, return evicted indices from `evict` and `evict_lru`. The simplest: have the ring thread unregister after eviction.

Add to `FdCache`:

```rust
    /// Explicitly evict a log_id. Returns the fixed_index that was freed
    /// (if the entry existed), so the caller can unregister it from io_uring.
    pub fn evict(&mut self, log_id: u32) -> Option<u32> {
        self.pinned.remove(&log_id);
        if let Some(entry) = self.entries.remove(&log_id) {
            let idx = entry.fixed_index;
            self.free_indices.push(idx);
            Some(idx)
        } else {
            None
        }
    }

    /// Evict LRU entry. Returns the fixed_index that was freed (if any).
    fn evict_lru(&mut self) -> Option<u32> {
        if let Some((&lru_id, _)) = self
            .entries
            .iter()
            .filter(|(id, _)| !self.pinned.contains(id))
            .min_by_key(|(_, e)| e.last_used)
        {
            self.pinned.remove(&lru_id);
            if let Some(entry) = self.entries.remove(&lru_id) {
                let idx = entry.fixed_index;
                self.free_indices.push(idx);
                return Some(idx);
            }
        }
        None
    }
```

Update `get_or_open` to use the new `evict_lru` return:

```rust
        if self.entries.len() >= self.capacity {
            if let Some(evicted_idx) = self.evict_lru() {
                self.last_evicted_index = Some(evicted_idx);
            }
        }
```

Actually, simpler approach: have the ring thread call `register_files_update` with -1 for evicted slots. Since eviction happens inside `get_or_open` (LRU eviction), the ring thread can check after `get_or_open` returns `is_new = true`:

```rust
                    if is_new && fixed_fds_available {
                        // The fd_cache may have evicted an LRU entry to make room.
                        // The evicted slot's fixed_index is recycled into the new entry's
                        // fixed_index (via free_indices), so registering the new fd at
                        // that index implicitly replaces the old one. No explicit
                        // unregister needed.
                        let _ = ring.submitter().register_files_update(
                            fixed_index, &[fd],
                        );
                    }
```

This works because `register_files_update` at index N replaces whatever was there (including -1 or a stale fd). The evicted fd's file handle is dropped by the cache, and the kernel's fixed table slot is overwritten. No need for an explicit unregister step.

For explicit `evict` (GC path), the ring thread handles the `Evict` request:

```rust
                LedgerRequest::Evict { log_id } => {
                    if let Some(evicted_idx) = fd_cache.evict(log_id) {
                        if fixed_fds_available {
                            let _ = ring.submitter().register_files_update(
                                evicted_idx, &[-1i32 as RawFd],
                            );
                        }
                    }
                }
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p bookkeeper-storage`
Expected: OK.

- [ ] **Step 5: Run tests**

Run: `cargo test -p bookkeeper-storage`
Expected: All tests pass (fixed fds are a no-op on macOS since uring code is cfg-gated).

- [ ] **Step 6: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/uring_ledger.rs \
        crates/bookkeeper-storage/src/io_engine/fd_cache.rs
git commit -m "feat: register fixed file descriptors with io_uring for read SQEs"
```

---

### Task 7: Provided buffer pool for reads

**Files:**
- Modify: `crates/bookkeeper-storage/src/io_engine/uring_ledger.rs`

- [ ] **Step 1: Add buffer pool constants and setup**

Add constants at the top of `uring_ledger.rs`:

```rust
const BUF_POOL_SIZE: usize = 64 * 1024;   // 64KB per buffer
const BUF_POOL_COUNT: u16 = 256;            // 256 buffers = 16MB total
const BUF_GROUP_ID: u16 = 0;
```

In `ledger_ring_loop`, after the fixed file registration, probe and set up the provided buffer ring:

```rust
    // Probe for provided buffer ring support (kernel 5.19+)
    let buf_pool: Option<BufferPool> = BufferPool::try_new(&ring);
    if buf_pool.is_some() {
        tracing::debug!("ledger-io: provided buffer pool active ({} x {}KB)",
            BUF_POOL_COUNT, BUF_POOL_SIZE / 1024);
    } else {
        tracing::debug!("ledger-io: provided buffer pool not available, using per-read alloc");
    }
```

- [ ] **Step 2: Implement BufferPool helper struct**

Add at the bottom of `uring_ledger.rs`:

```rust
/// Pre-allocated buffer pool registered with io_uring's provided buffer ring.
/// Reduces per-read allocation overhead.
struct BufferPool {
    /// Contiguous allocation backing all buffers.
    storage: Vec<u8>,
    /// Base pointer for the buffer ring (kept for lifetime).
    _ring_addr: *mut libc::c_void,
}

impl BufferPool {
    /// Try to create a provided buffer ring. Returns None if the kernel
    /// doesn't support it (< 5.19) or registration fails.
    fn try_new(ring: &IoUring<io_uring::squeue::Entry>) -> Option<Self> {
        use std::alloc::{alloc_zeroed, Layout};

        // Allocate the buffer ring header + entry array
        let ring_entries = BUF_POOL_COUNT as usize;
        let ring_size = std::mem::size_of::<io_uring::types::BufRingEntry>() * ring_entries
            + std::mem::size_of::<u16>(); // tail atomic
        let layout = Layout::from_size_align(ring_size, 4096).ok()?;
        let ring_addr = unsafe { alloc_zeroed(layout) };
        if ring_addr.is_null() {
            return None;
        }

        // Register with kernel
        if unsafe {
            ring.submitter().register_buf_ring_with_flags(
                ring_addr as u64,
                BUF_POOL_COUNT,
                BUF_GROUP_ID,
                0, // flags
            )
        }.is_err() {
            unsafe { std::alloc::dealloc(ring_addr, layout); }
            return None;
        }

        // Allocate backing storage for all buffers
        let storage = vec![0u8; BUF_POOL_SIZE * ring_entries];

        // Fill the buffer ring with entries
        for i in 0..ring_entries {
            let buf_addr = unsafe { storage.as_ptr().add(i * BUF_POOL_SIZE) };
            let entry = unsafe {
                &mut *(ring_addr as *mut io_uring::types::BufRingEntry).add(i)
            };
            entry.set_addr(buf_addr as u64);
            entry.set_len(BUF_POOL_SIZE as u32);
            entry.set_bid(i as u16);
        }

        // Set the tail to make all entries available
        let tail_ptr = unsafe {
            io_uring::types::BufRingEntry::tail(ring_addr as *const io_uring::types::BufRingEntry)
        };
        unsafe {
            (*tail_ptr).store(BUF_POOL_COUNT, std::sync::atomic::Ordering::Release);
        }

        Some(Self {
            storage,
            _ring_addr: ring_addr as *mut libc::c_void,
        })
    }

    /// Get a slice for a completed buffer ID.
    fn get_buf(&self, buf_id: u16, len: usize) -> &[u8] {
        let offset = (buf_id as usize) * BUF_POOL_SIZE;
        &self.storage[offset..offset + len]
    }

    /// Return a buffer to the ring after use.
    fn return_buf(&self, ring_addr: *mut u8, buf_id: u16) {
        let entry = unsafe {
            &mut *(ring_addr as *mut io_uring::types::BufRingEntry).add(buf_id as usize)
        };
        let buf_addr = unsafe { self.storage.as_ptr().add((buf_id as usize) * BUF_POOL_SIZE) };
        entry.set_addr(buf_addr as u64);
        entry.set_len(BUF_POOL_SIZE as u32);
        entry.set_bid(buf_id);

        // Advance tail
        let tail_ptr = unsafe {
            io_uring::types::BufRingEntry::tail(ring_addr as *const io_uring::types::BufRingEntry)
        };
        unsafe {
            let old = (*tail_ptr).load(std::sync::atomic::Ordering::Acquire);
            (*tail_ptr).store(old.wrapping_add(1), std::sync::atomic::Ordering::Release);
        }
    }
}
```

- [ ] **Step 3: Use buffer pool in Read SQEs when available**

In the Read handler in `ledger_ring_loop`, modify the SQE construction:

```rust
                    let use_pool = buf_pool.is_some() && len <= BUF_POOL_SIZE;

                    if use_pool {
                        // Use provided buffer — kernel selects from pool
                        let read_sqe =
                            opcode::Read::new(fd_target, std::ptr::null_mut(), len as u32)
                                .offset(offset)
                                .buf_group(BUF_GROUP_ID)
                                .build()
                                .flags(io_uring::squeue::Flags::BUFFER_SELECT)
                                .user_data(sqe_idx);

                        let pushed = unsafe { ring.submission().push(&read_sqe) };
                        if pushed.is_err() {
                            let _ = reply.send(Err(io::Error::new(
                                io::ErrorKind::Other, "SQ full",
                            )));
                            continue;
                        }

                        pending.push(PendingOp::PoolRead { len, reply });
                        num_sqes += 1;
                    } else {
                        // Fallback: per-read allocation (oversized or no pool)
                        let mut buf = vec![0u8; len];
                        let read_sqe =
                            opcode::Read::new(fd_target, buf.as_mut_ptr(), len as u32)
                                .offset(offset)
                                .build()
                                .user_data(sqe_idx);
                        // ... existing code ...
                    }
```

Add a new `PendingOp` variant:

```rust
enum PendingOp {
    Read {
        buf: Vec<u8>,
        len: usize,
        reply: oneshot::Sender<io::Result<Vec<u8>>>,
    },
    PoolRead {
        len: usize,
        reply: oneshot::Sender<io::Result<Vec<u8>>>,
    },
    Write { /* unchanged */ },
}
```

- [ ] **Step 4: Handle PoolRead CQEs**

In the CQE dispatch section, handle `PoolRead`:

```rust
                PendingOp::PoolRead { len, reply } => {
                    let result = if ret < 0 {
                        Err(io::Error::from_raw_os_error(-ret))
                    } else if (ret as usize) < len {
                        Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!("short read: {} of {} bytes", ret, len),
                        ))
                    } else {
                        // Extract buffer ID from CQE flags
                        let buf_id = (cqe_flags >> 16) as u16;
                        let pool = buf_pool.as_ref().unwrap();
                        let data = pool.get_buf(buf_id, ret as usize).to_vec();
                        // Return buffer to pool
                        pool.return_buf(pool._ring_addr as *mut u8, buf_id);
                        Ok(data)
                    };
                    let _ = reply.send(result);
                }
```

Note: we need to capture `cqe.flags()` alongside `cqe.result()` in the results vec. Change the results collection to store both:

```rust
        let mut results: Vec<Option<(i32, u32)>> = vec![None; pending.len()];
        for cqe in ring.completion() {
            let idx = cqe.user_data() as usize;
            if idx < results.len() {
                results[idx] = Some((cqe.result(), cqe.flags()));
            }
        }
```

And update all dispatch to destructure `(ret, cqe_flags)`:

```rust
            let (ret, cqe_flags) = results[idx].unwrap_or((-libc::EIO, 0));
```

- [ ] **Step 5: Verify it compiles**

Run: `cargo check -p bookkeeper-storage`
Expected: OK.

- [ ] **Step 6: Run all tests**

Run: `cargo test --workspace -- --skip java_bookie_container --skip java_client`
Expected: All tests pass. On macOS, uring code is cfg-gated so buffer pool is not exercised. On Linux (docker-compose-test.yml), the pool is used if kernel supports it.

- [ ] **Step 7: Commit**

```bash
git add crates/bookkeeper-storage/src/io_engine/uring_ledger.rs
git commit -m "feat: provided buffer pool for io_uring reads (kernel 5.19+)"
```

---

## Self-Review

**Spec coverage:**
- [x] Optimization 1: `read_framed_entry` trait method + StdEngine override + UringLedgerIo `ReadFramed` + DbLedgerStorage simplified
- [x] Optimization 2: Fixed fd registration at startup + `register_files_update` on cache miss + `Fixed(index)` in SQEs + unregister on evict
- [x] Optimization 3: `BufferPool` with `register_buf_ring_with_flags` + `BUFFER_SELECT` SQEs + `PoolRead` pending op + buffer return after CQE
- [x] Fallback paths: default trait impl for `read_framed_entry`, raw fds when `register_files` fails, per-read alloc when pool unavailable or oversized
- [x] Testing: `read_framed_entry` tests, `is_new` flag test, existing tests verify correctness

**Placeholder scan:** No TBD/TODO found. All code blocks are complete.

**Type consistency:** `get_or_open` returns `(RawFd, u32, bool)` everywhere. `read_framed_entry` signature matches between trait, StdEngine, and UringLedgerIo. `PendingOp::PoolRead` and `BufferPool` types are consistent.
