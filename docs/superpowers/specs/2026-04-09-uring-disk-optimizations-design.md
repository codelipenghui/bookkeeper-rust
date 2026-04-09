# io_uring Disk Path Optimizations Design

## Overview

Three targeted optimizations to the existing io_uring read/write paths that reduce syscall count, eliminate per-SQE fd validation, and reduce allocator pressure. All changes are Linux-only (`#[cfg(target_os = "linux")]`), with StdEngine unaffected except for Optimization 1 which also benefits the blocking path.

---

## Optimization 1: Single-Read Entry Path

### Problem

`DbLedgerStorage::read_entry_from_log` performs two sequential I/O operations per cold read:

```rust
// Read 1: 4-byte size prefix
let size_buf = self.ledger_io.read_entry(&dir, log_id, pos - 4, 4).await?;
let size = i32::from_be_bytes(size_buf[..4].try_into().unwrap()) as usize;

// Read 2: entry data (depends on Read 1)
let entry = self.ledger_io.read_entry(&dir, log_id, pos, size).await?;
```

Each `read_entry` call goes through the io_uring ring thread as a separate SQE submission + CQE completion + channel round-trip. This doubles latency for every cold read.

### Solution

Add a `read_framed_entry` method to `LedgerIo` that reads the 4-byte size prefix and the entry data in a single channel round-trip. The ring thread performs both reads internally (two kernel I/O ops but one caller round-trip).

```rust
#[async_trait]
pub trait LedgerIo: Send + Sync {
    // ... existing methods ...

    /// Read a framed entry: parse the 4-byte big-endian size prefix at
    /// `offset`, then read `size` bytes of entry data starting at
    /// `offset + 4`. Returns only the entry data (without the prefix).
    ///
    /// Default implementation does two calls (backward compatible).
    /// UringLedgerIo overrides to do both reads in one ring submission.
    async fn read_framed_entry(
        &self,
        entry_log_dir: &Path,
        log_id: u32,
        offset: u64,
        max_size: usize,
    ) -> io::Result<Vec<u8>> {
        let size_buf = self.read_entry(entry_log_dir, log_id, offset, 4).await?;
        let size = i32::from_be_bytes(size_buf[..4].try_into().unwrap()) as usize;
        if size > max_size {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                format!("entry size {} exceeds max {}", size, max_size)));
        }
        self.read_entry(entry_log_dir, log_id, offset + 4, size).await
    }
}
```

For `UringLedgerIo`: a new `LedgerRequest::ReadFramed` variant sends the request to the ring thread. The ring thread reads the 4-byte prefix with a blocking `pread`, parses the size, then submits a single io_uring READ SQE for the data. Two kernel I/O ops, but one channel round-trip (instead of two).

For `StdEngine`: the default trait implementation calls `read_entry` twice, which is equivalent to the current behavior. Alternatively, `StdEngine` can override with a single `spawn_blocking` that does both reads in one closure.

`DbLedgerStorage::read_entry_from_log` simplifies to:

```rust
async fn read_entry_from_log(&self, location: u64) -> Result<Vec<u8>, StorageError> {
    let loc = EntryLocation::from_u64(location);
    let pos = loc.position() as u64;
    if pos < 4 {
        return Err(StorageError::CorruptData("invalid entry position".into()));
    }
    let entry = self.ledger_io.read_framed_entry(
        &self.entry_log_dir, loc.log_id(), pos - 4, MAX_ENTRY_SIZE,
    ).await?;
    Ok(entry)
}
```

---

## Optimization 2: Fixed File Descriptors

### Problem

Every io_uring SQE passes a raw file descriptor via `types::Fd(fd)`. The kernel validates this fd against the process fd table on every submission. With registered (fixed) file descriptors, the kernel skips this validation — the fds are pre-validated once during registration.

The `fd_cache.rs` already tracks `fixed_index: u32` per cached entry and `get_or_open` returns `(fd, fixed_index)`, but `uring_ledger.rs` ignores `fixed_index` and always uses `types::Fd(fd)`.

### Solution

Wire the existing `fixed_index` infrastructure to io_uring's fixed file table:

**At ring thread startup:**
```rust
// Pre-allocate fixed file slots (all -1 = empty)
let empty_fds: Vec<RawFd> = vec![-1; FD_CACHE_CAPACITY];
ring.submitter().register_files(&empty_fds)?;
```

**On fd_cache insert (get_or_open opens a new file):**
```rust
// Register the new fd at its fixed_index slot
ring.submitter().register_files_update(fixed_index as u32, &[fd])?;
```

**In SQE construction:**
```rust
// Before: types::Fd(fd)
// After:  types::Fixed(fixed_index)
let read_sqe = opcode::Read::new(types::Fixed(fixed_index), buf.as_mut_ptr(), len as u32)
    .offset(offset)
    .build()
    .user_data(sqe_idx);
```

**On fd_cache eviction:**
```rust
// Unregister the slot (set to -1)
ring.submitter().register_files_update(fixed_index as u32, &[-1])?;
```

Changes are confined to `uring_ledger.rs` and `fd_cache.rs`. The `FdCache` needs a method to report when a slot is newly inserted vs. reused (so the ring thread knows when to call `register_files_update`).

`FdCache::get_or_open` return type changes to include a flag:
```rust
pub fn get_or_open(&mut self, dir: &Path, log_id: u32) -> io::Result<(RawFd, u32, bool)>
//                                                                     fd   idx  is_new
```

The ring thread calls `register_files_update` only when `is_new` is true.

### Scope

- Only affects `UringLedgerIo` (Linux io_uring path)
- `StdEngine` and `UringJournalIo` are unaffected
- The journal uses raw fds directly (not cached), so no fixed fd registration there

---

## Optimization 3: Provided Buffer Pool for Reads

### Problem

Every io_uring read allocates a fresh `Vec<u8>`:
```rust
let mut buf = vec![0u8; len];
```

Under heavy read load, this creates allocator pressure. The kernel also needs to validate the user-space buffer pointer on every SQE.

### Solution

Pre-allocate a pool of fixed-size buffers and register them with io_uring's provided buffer ring (`IORING_REGISTER_PBUF_RING`). Read SQEs use `BUFFER_SELECT` to let the kernel pick a buffer from the pool.

**At ring thread startup:**
```rust
const BUF_SIZE: usize = 65536;  // 64KB per buffer
const BUF_COUNT: usize = 256;   // 256 buffers = 16MB total
const BUF_GROUP_ID: u16 = 0;

// Allocate aligned buffer pool
let pool: Vec<Vec<u8>> = (0..BUF_COUNT).map(|_| vec![0u8; BUF_SIZE]).collect();

// Register as a provided buffer ring
let mut buf_ring = ring.submitter()
    .setup_buf_ring(BUF_COUNT as u32, BUF_GROUP_ID)?;
for (i, buf) in pool.iter().enumerate() {
    buf_ring.push(buf.as_ptr() as *mut u8, BUF_SIZE as u32, i as u16);
}
buf_ring.sync();
```

**Read SQEs with buffer selection:**
```rust
let read_sqe = opcode::Read::new(types::Fixed(fixed_index), std::ptr::null_mut(), BUF_SIZE as u32)
    .offset(offset)
    .buf_group(BUF_GROUP_ID)
    .build()
    .flags(io_uring::squeue::Flags::BUFFER_SELECT)
    .user_data(sqe_idx);
```

**CQE processing:**
```rust
let buf_id = io_uring::cqe_to_buf_id(&cqe);  // extract buffer index from CQE flags
let bytes_read = cqe.result() as usize;
let data = pool[buf_id][..bytes_read].to_vec();  // copy to caller's Vec
// Return buffer to pool by re-pushing to buf_ring
buf_ring.push(pool[buf_id].as_ptr() as *mut u8, BUF_SIZE as u32, buf_id as u16);
buf_ring.sync();
```

**Fallback:** If the buffer pool is exhausted (all 256 buffers in flight), fall back to the current per-read allocation. Also, if the requested read size exceeds `BUF_SIZE` (64KB), use a direct allocation (large entries are rare).

**Kernel requirement:** Provided buffer rings require kernel 5.19+. Probe at startup:
```rust
fn pbuf_ring_available(ring: &IoUring) -> bool {
    // Try to set up a tiny buffer ring; if it fails, feature not available
    match ring.submitter().setup_buf_ring(1, 99) {
        Ok(_) => {
            // Unregister the probe ring
            let _ = ring.submitter().unregister_buf_ring(99);
            true
        }
        Err(_) => false,
    }
}
```

If unavailable, the ring thread runs without provided buffers (current behavior).

### Scope

- Only affects `UringLedgerIo` read path
- Write path is unaffected (writes use caller-provided data)
- `StdEngine` is unaffected
- `UringJournalIo` is unaffected (writes only)

---

## Testing Strategy

- **Optimization 1:** Existing `read_entry_from_log` tests cover correctness. Add a unit test for `read_framed_entry` with valid/oversized/corrupt entries.
- **Optimization 2:** Existing fd_cache tests verify caching. Add a test that the `is_new` flag is correct on insert vs. reuse.
- **Optimization 3:** Test via `docker-compose-test.yml` (Linux io_uring environment). Unit test the buffer pool allocation/return cycle.
- **All:** Run `cargo test --workspace` on macOS (StdEngine path) and Linux Docker (uring path).

## Non-Goals

- Changing the journal I/O path (already well-batched)
- SQPOLL mode (CPU overhead not justified for this workload)
- Multishot operations (not applicable to file I/O)
- Changing the flush/write path (already efficient)
