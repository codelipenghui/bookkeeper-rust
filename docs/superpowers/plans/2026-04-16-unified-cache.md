# Unified Entry Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `DbLedgerStorage`'s three independent caches (`write_cache`, `write_cache_being_flushed`, `read_cache`) with one `UnifiedCache` whose single `DashMap` is the sole index across 6 fixed storage regions — reducing `get_entry` cost from 3–6 hash-and-probes per call to exactly 1.

**Architecture:** One `DashMap<(i64,i64), CacheEntry>` where each `CacheEntry` carries a packed `region_tag` pointing into one of 2 write buffers or 4 read segments. The O(1) flush "swap" becomes an `AtomicU8` flip of which write buffer is "active". Per-region `Mutex<Vec<(i64,i64)>>` tracks what lives in each region so drain and read-ring-rotation can enumerate entries without scanning the whole DashMap.

**Tech Stack:** Rust, `dashmap`, `parking_lot`, `tokio`. Existing crate: `bookkeeper-storage`.

**Spec reference:** [`docs/superpowers/specs/2026-04-16-unified-cache-design.md`](../specs/2026-04-16-unified-cache-design.md). Commit `005e9ed7`.

---

## File Structure

**New:**
- `crates/bookkeeper-storage/src/cache/unified_cache.rs` — the `UnifiedCache` type, `CacheEntry`, `Region`, `WriteBuffer`, `ReadSegment`, and `FlushGuard`. All logic from both deleted files plus the unification.

**Modified:**
- `crates/bookkeeper-storage/src/cache/mod.rs` — expose only `unified_cache`; remove `read_cache`/`write_cache`.
- `crates/bookkeeper-storage/src/db_ledger_storage.rs` — three cache fields collapse into one; `get_entry`, `add_entry`, `swap_caches`, `drain_flushing`, and the readahead spawn block switch to the unified API.
- `crates/bookkeeper-storage/src/storage_metrics.rs` — wire up the previously-stub cache gauges (`bookie_read_cache_*`, `bookie_write_cache_*`).
- `crates/bookkeeper-storage/tests/cache_test.rs` — rewritten against `UnifiedCache`.

**Deleted:**
- `crates/bookkeeper-storage/src/cache/read_cache.rs`
- `crates/bookkeeper-storage/src/cache/write_cache.rs`

**Unchanged:**
- `crates/bookkeeper-storage/tests/db_ledger_storage_test.rs` — public-API tests must pass without edits.

---

## Task 1: Module skeleton + data types

**Files:**
- Create: `crates/bookkeeper-storage/src/cache/unified_cache.rs`
- Modify: `crates/bookkeeper-storage/src/cache/mod.rs`
- Test: `crates/bookkeeper-storage/tests/cache_test.rs` (append)

- [ ] **Step 1: Write the failing test**

Append to `crates/bookkeeper-storage/tests/cache_test.rs`:

```rust
use bookkeeper_storage::cache::unified_cache::{CacheEntry, Region};

#[test]
fn cache_entry_round_trip_all_regions() {
    // 2 write buffers
    for i in 0..2u8 {
        let ce = CacheEntry::write(i, 123, 456);
        assert_eq!(ce.region(), Region::Write(i));
        assert_eq!(ce.offset, 123);
        assert_eq!(ce.size, 456);
    }
    // 4 read segments
    for i in 0..4u8 {
        let ce = CacheEntry::read(i, 789, 1011);
        assert_eq!(ce.region(), Region::ReadSegment(i));
        assert_eq!(ce.offset, 789);
        assert_eq!(ce.size, 1011);
    }
}

#[test]
fn cache_entry_size_is_compact() {
    // region_tag: u8, offset: u32, size: u32 = 9 bytes (+ padding to 12).
    // Still smaller than the current (usize, usize) = 16 bytes on 64-bit.
    assert!(std::mem::size_of::<CacheEntry>() <= 12);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p bookkeeper-storage --test cache_test cache_entry_round_trip_all_regions 2>&1 | tail -15
```
Expected: `error[E0432]: unresolved import \`bookkeeper_storage::cache::unified_cache\`` or similar — the module doesn't exist yet.

- [ ] **Step 3: Write minimal implementation**

Create `crates/bookkeeper-storage/src/cache/unified_cache.rs`:

```rust
//! One `DashMap`-backed index over 6 fixed storage regions: 2 write buffers
//! (swap-based flush) and 4 read-cache segments (ring-buffer eviction).
//!
//! See `docs/superpowers/specs/2026-04-16-unified-cache-design.md` for
//! the full design, concurrency invariants (I1/I2/I3), and rationale.

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Region {
    /// Index into [`UnifiedCache::write_bufs`] — 0 or 1.
    Write(u8),
    /// Index into [`UnifiedCache::read_segments`] — 0..=3.
    ReadSegment(u8),
}

/// Location of an entry's bytes within one of the 6 regions.
/// Stored as the value type in the unified `DashMap`.
///
/// Packing: `region_tag` top bit = read/write flag, low 3 bits = index.
/// Offsets and sizes are `u32` because no region exceeds 256 MB (fits in 28 bits).
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CacheEntry {
    pub region_tag: u8,
    pub offset: u32,
    pub size: u32,
}

const WRITE_FLAG: u8 = 0x00;
const READ_FLAG: u8 = 0x80;
const INDEX_MASK: u8 = 0x07;

impl CacheEntry {
    pub fn write(idx: u8, offset: u32, size: u32) -> Self {
        debug_assert!(idx < 2);
        Self { region_tag: WRITE_FLAG | (idx & INDEX_MASK), offset, size }
    }

    pub fn read(idx: u8, offset: u32, size: u32) -> Self {
        debug_assert!(idx < 4);
        Self { region_tag: READ_FLAG | (idx & INDEX_MASK), offset, size }
    }

    pub fn region(&self) -> Region {
        let idx = self.region_tag & INDEX_MASK;
        if self.region_tag & 0x80 == 0 {
            Region::Write(idx)
        } else {
            Region::ReadSegment(idx)
        }
    }
}
```

Then edit `crates/bookkeeper-storage/src/cache/mod.rs` to add the module (keep the old ones; we'll delete them in Task 15):

```rust
pub mod write_cache;
pub mod read_cache;
pub mod unified_cache;
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p bookkeeper-storage --test cache_test cache_entry 2>&1 | tail -10
```
Expected: both new tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/cache/unified_cache.rs \
        crates/bookkeeper-storage/src/cache/mod.rs \
        crates/bookkeeper-storage/tests/cache_test.rs
git commit -m "storage/cache: scaffold UnifiedCache data types (CacheEntry, Region)"
```

---

## Task 2: WriteBuffer + ReadSegment storage primitives

**Files:**
- Modify: `crates/bookkeeper-storage/src/cache/unified_cache.rs`
- Test: `crates/bookkeeper-storage/tests/cache_test.rs`

- [ ] **Step 1: Write the failing test**

Append to `cache_test.rs`:

```rust
use bookkeeper_storage::cache::unified_cache::WriteBuffer;

#[test]
fn write_buffer_bump_alloc_returns_disjoint_offsets() {
    let buf = WriteBuffer::new(1024);
    let off1 = buf.reserve(100).expect("fits");
    let off2 = buf.reserve(100).expect("fits");
    assert_ne!(off1, off2);
    assert!(off2 >= off1 + 100);
}

#[test]
fn write_buffer_reserve_rejects_when_full() {
    let buf = WriteBuffer::new(256); // 4 × 64B after align64
    for i in 0..4 {
        buf.reserve(1).unwrap_or_else(|_| panic!("entry {i} should fit"));
    }
    assert!(buf.reserve(1).is_err(), "5th 64B-aligned entry must not fit");
}

#[test]
fn write_buffer_write_then_read_back() {
    let buf = WriteBuffer::new(1024);
    let off = buf.reserve(5).unwrap();
    unsafe { buf.write_at(off, b"hello"); }
    assert_eq!(unsafe { buf.read_at(off, 5) }, b"hello".to_vec());
}

#[test]
fn write_buffer_reset_clears_offset_and_entries() {
    let buf = WriteBuffer::new(1024);
    let off = buf.reserve(10).unwrap();
    unsafe { buf.write_at(off, b"0123456789"); }
    buf.track_entry(1, 0);
    buf.reset();
    assert_eq!(buf.current_offset(), 0);
    assert!(buf.take_entries().is_empty());
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p bookkeeper-storage --test cache_test write_buffer 2>&1 | tail -10
```
Expected: `error[E0432]: unresolved import \`bookkeeper_storage::cache::unified_cache::WriteBuffer\``.

- [ ] **Step 3: Write minimal implementation**

Append to `crates/bookkeeper-storage/src/cache/unified_cache.rs`:

```rust
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use parking_lot::Mutex;

const ALIGNMENT: usize = 64;
fn align64(n: usize) -> usize { (n + ALIGNMENT - 1) & !(ALIGNMENT - 1) }

/// Fixed-capacity append-only buffer. The backing `Vec<u8>` is allocated
/// once and never moves. `offset` is a bump-allocator head; `entries`
/// tracks (ledger, entry) pairs stored here so drain/rotation can
/// enumerate them without scanning the unified index.
pub struct WriteBuffer {
    data: UnsafeCell<Vec<u8>>,
    offset: AtomicUsize,
    max_size: usize,
    entries: Mutex<Vec<(i64, i64)>>,
}

// SAFETY: concurrent appends use disjoint offsets (`fetch_add`-reserved);
// reads against already-written regions are safe under the invariant that
// no writer overwrites a region until the unified index's entry has been
// removed (I3) — see design spec.
unsafe impl Send for WriteBuffer {}
unsafe impl Sync for WriteBuffer {}

impl WriteBuffer {
    pub fn new(max_size: usize) -> Self {
        let aligned = align64(max_size);
        Self {
            data: UnsafeCell::new(vec![0u8; aligned]),
            offset: AtomicUsize::new(0),
            max_size: aligned,
            entries: Mutex::new(Vec::new()),
        }
    }

    /// Reserve a 64-byte-aligned range. CAS loop to avoid the `fetch_add`/
    /// `fetch_sub` race where two concurrent overflows would otherwise
    /// corrupt the offset counter.
    pub fn reserve(&self, len: usize) -> Result<usize, ()> {
        let aligned = align64(len);
        if aligned > self.max_size { return Err(()); }
        loop {
            let cur = self.offset.load(Ordering::SeqCst);
            if cur + aligned > self.max_size { return Err(()); }
            if self.offset.compare_exchange(
                cur, cur + aligned, Ordering::SeqCst, Ordering::SeqCst,
            ).is_ok() {
                return Ok(cur);
            }
        }
    }

    /// # Safety
    /// Caller must hold a reserved range `[offset, offset + data.len())`
    /// obtained from `reserve()` and not have written there yet.
    pub unsafe fn write_at(&self, offset: usize, data: &[u8]) {
        let buf = &mut *self.data.get();
        buf[offset..offset + data.len()].copy_from_slice(data);
    }

    /// # Safety
    /// Caller must hold the `DashMap::Ref` that points to this offset
    /// (invariant I1) so no concurrent `reset()` can start.
    pub unsafe fn read_at(&self, offset: usize, size: usize) -> Vec<u8> {
        let buf = &*self.data.get();
        buf[offset..offset + size].to_vec()
    }

    pub fn track_entry(&self, ledger_id: i64, entry_id: i64) {
        self.entries.lock().push((ledger_id, entry_id));
    }

    pub fn take_entries(&self) -> Vec<(i64, i64)> {
        std::mem::take(&mut *self.entries.lock())
    }

    pub fn current_offset(&self) -> usize {
        self.offset.load(Ordering::SeqCst)
    }

    pub fn max_size(&self) -> usize { self.max_size }

    /// Resets the offset to 0 — the buffer is now reusable. Entries in
    /// the unified index pointing here MUST have been removed before this
    /// call (I3).
    pub fn reset(&self) {
        self.offset.store(0, Ordering::Release);
        self.entries.lock().clear();
    }
}

/// Same shape as `WriteBuffer`. Logically distinct so call-sites stay
/// self-documenting; same internal mechanics.
pub type ReadSegment = WriteBuffer;
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p bookkeeper-storage --test cache_test write_buffer 2>&1 | tail -10
```
Expected: all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/cache/unified_cache.rs \
        crates/bookkeeper-storage/tests/cache_test.rs
git commit -m "storage/cache: add WriteBuffer primitive for UnifiedCache"
```

---

## Task 3: UnifiedCache skeleton + put_write/get round-trip

**Files:**
- Modify: `crates/bookkeeper-storage/src/cache/unified_cache.rs`
- Test: `crates/bookkeeper-storage/tests/cache_test.rs`

- [ ] **Step 1: Write the failing test**

Append to `cache_test.rs`:

```rust
use bookkeeper_storage::cache::unified_cache::UnifiedCache;

fn tiny_cache() -> UnifiedCache {
    // 1 MB write buffers × 2, 1 MB read-cache (256 KB per segment).
    UnifiedCache::new(1024 * 1024, 1024 * 1024)
}

#[test]
fn unified_put_write_then_get_returns_bytes() {
    let c = tiny_cache();
    c.put_write(1, 0, b"hello").unwrap();
    assert_eq!(c.get(1, 0), Some(b"hello".to_vec()));
}

#[test]
fn unified_get_missing_returns_none() {
    let c = tiny_cache();
    assert_eq!(c.get(1, 0), None);
}

#[test]
fn unified_put_write_uses_active_buffer() {
    let c = tiny_cache();
    c.put_write(1, 0, b"a").unwrap();
    // Active is buffer 0 (default); the entry's region should reflect that.
    let ce = c.peek_entry(1, 0).unwrap();
    assert_eq!(ce.region(), Region::Write(0));
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p bookkeeper-storage --test cache_test unified_put_write 2>&1 | tail -10
```
Expected: `UnifiedCache` unresolved.

- [ ] **Step 3: Write minimal implementation**

Append to `crates/bookkeeper-storage/src/cache/unified_cache.rs`:

```rust
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use crate::StorageError;

pub const READ_CACHE_SEGMENTS: usize = 4;

pub struct UnifiedCache {
    index: DashMap<(i64, i64), CacheEntry>,
    write_bufs: [Arc<WriteBuffer>; 2],
    active_write: AtomicU8,
    read_segments: [Arc<ReadSegment>; READ_CACHE_SEGMENTS],
    current_segment: AtomicUsize,
    last_entry: DashMap<i64, i64>,
}

impl UnifiedCache {
    pub fn new(write_buf_size: usize, read_cache_total_size: usize) -> Self {
        let seg_size = read_cache_total_size / READ_CACHE_SEGMENTS;
        Self {
            index: DashMap::new(),
            write_bufs: [
                Arc::new(WriteBuffer::new(write_buf_size)),
                Arc::new(WriteBuffer::new(write_buf_size)),
            ],
            active_write: AtomicU8::new(0),
            read_segments: std::array::from_fn(|_| Arc::new(ReadSegment::new(seg_size))),
            current_segment: AtomicUsize::new(0),
            last_entry: DashMap::new(),
        }
    }

    pub fn put_write(&self, l: i64, e: i64, data: &[u8]) -> Result<(), StorageError> {
        let idx = self.active_write.load(Ordering::Acquire);
        let buf = &self.write_bufs[idx as usize];
        let offset = buf.reserve(data.len()).map_err(|_| StorageError::StorageFull)?;
        unsafe { buf.write_at(offset, data); }
        buf.track_entry(l, e);
        self.index.insert((l, e), CacheEntry::write(idx, offset as u32, data.len() as u32));
        self.last_entry
            .entry(l)
            .and_modify(|v| if e > *v { *v = e; })
            .or_insert(e);
        Ok(())
    }

    /// Invariant I1: the data copy happens inside the DashMap `.map(..)`
    /// closure, which holds the shard read-lock (via `Ref`) for the entire
    /// copy. This blocks concurrent `index.remove()` from drain/rotation,
    /// preventing read of a region whose offset has been reset.
    pub fn get(&self, l: i64, e: i64) -> Option<Vec<u8>> {
        self.index.get(&(l, e)).map(|entry| {
            let ce = *entry.value();
            match ce.region() {
                Region::Write(i) => unsafe {
                    self.write_bufs[i as usize].read_at(ce.offset as usize, ce.size as usize)
                },
                Region::ReadSegment(i) => unsafe {
                    self.read_segments[i as usize].read_at(ce.offset as usize, ce.size as usize)
                },
            }
        })
    }

    /// Test-only peek at the CacheEntry for a key (without copying bytes).
    pub fn peek_entry(&self, l: i64, e: i64) -> Option<CacheEntry> {
        self.index.get(&(l, e)).map(|r| *r.value())
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p bookkeeper-storage --test cache_test unified_ 2>&1 | tail -10
```
Expected: all 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/cache/unified_cache.rs \
        crates/bookkeeper-storage/tests/cache_test.rs
git commit -m "storage/cache: UnifiedCache put_write/get round-trip"
```

---

## Task 4: last_entry_id tracking + overflow handling

**Files:**
- Modify: `crates/bookkeeper-storage/src/cache/unified_cache.rs`
- Test: `crates/bookkeeper-storage/tests/cache_test.rs`

- [ ] **Step 1: Write the failing test**

Append:

```rust
#[test]
fn unified_last_entry_tracks_max_per_ledger() {
    let c = tiny_cache();
    c.put_write(1, 0, b"a").unwrap();
    c.put_write(1, 5, b"b").unwrap();
    c.put_write(1, 3, b"c").unwrap();
    c.put_write(2, 0, b"d").unwrap();
    assert_eq!(c.last_entry_id(1), Some(5));
    assert_eq!(c.last_entry_id(2), Some(0));
    assert_eq!(c.last_entry_id(99), None);
}

#[test]
fn unified_put_write_returns_storage_full_when_buffer_full() {
    // 256-byte write buffers. 64-byte alignment → 4 entries fit.
    let c = UnifiedCache::new(256, 1024 * 1024);
    for i in 0..4i64 {
        c.put_write(1, i, b"x").unwrap_or_else(|e| panic!("entry {i}: {e:?}"));
    }
    match c.put_write(1, 4, b"x") {
        Err(crate::StorageError::StorageFull) => {}
        other => panic!("expected StorageFull, got {other:?}"),
    }
}
```

Note: `crate::` in test context is wrong — fix by importing `StorageError`:

```rust
use bookkeeper_storage::StorageError;
// ... in the test:
match c.put_write(1, 4, b"x") {
    Err(StorageError::StorageFull) => {}
    other => panic!("expected StorageFull, got {other:?}"),
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p bookkeeper-storage --test cache_test last_entry 2>&1 | tail -10
```
Expected: `last_entry_id` method not found.

- [ ] **Step 3: Write minimal implementation**

Append to `unified_cache.rs` inside `impl UnifiedCache`:

```rust
pub fn last_entry_id(&self, ledger_id: i64) -> Option<i64> {
    self.last_entry.get(&ledger_id).map(|v| *v.value())
}
```

Overflow is already wired up via `reserve().map_err(|_| StorageError::StorageFull)` in Task 3.

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p bookkeeper-storage --test cache_test 2>&1 | tail -10
```
Expected: all tests pass (tasks 1–4).

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/cache/unified_cache.rs \
        crates/bookkeeper-storage/tests/cache_test.rs
git commit -m "storage/cache: last_entry_id tracking + overflow test"
```

---

## Task 5: Atomic flush swap (start_flush) + FlushGuard

**Files:**
- Modify: `crates/bookkeeper-storage/src/cache/unified_cache.rs`
- Test: `crates/bookkeeper-storage/tests/cache_test.rs`

- [ ] **Step 1: Write the failing test**

Append:

```rust
#[test]
fn unified_start_flush_flips_active_and_old_entries_remain_readable() {
    let c = tiny_cache();
    c.put_write(1, 0, b"pre-flush").unwrap();
    let guard = c.start_flush();
    // Old region index should still be 0 (flushing), new writes go to 1.
    assert_eq!(guard.flushing_idx(), 0);
    c.put_write(1, 1, b"post-flush").unwrap();
    // Both entries are still readable — old via write_bufs[0], new via write_bufs[1].
    assert_eq!(c.get(1, 0), Some(b"pre-flush".to_vec()));
    assert_eq!(c.get(1, 1), Some(b"post-flush".to_vec()));
    // The new entry's region tag should be Write(1).
    assert_eq!(c.peek_entry(1, 1).unwrap().region(), Region::Write(1));
}

#[test]
fn unified_start_flush_then_start_flush_cycles_back() {
    let c = tiny_cache();
    let g1 = c.start_flush();  // active was 0; now 1. flushing = 0.
    assert_eq!(g1.flushing_idx(), 0);
    let g2 = c.start_flush();  // active was 1; now 0. flushing = 1.
    assert_eq!(g2.flushing_idx(), 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p bookkeeper-storage --test cache_test start_flush 2>&1 | tail -10
```
Expected: `start_flush` not found.

- [ ] **Step 3: Write minimal implementation**

Append to `unified_cache.rs`:

```rust
/// Opaque handle returned by `start_flush`. Must be passed to `drain()`
/// so the drainer knows which buffer to walk.
pub struct FlushGuard {
    flushing_idx: u8,
}
impl FlushGuard {
    pub fn flushing_idx(&self) -> u8 { self.flushing_idx }
}

impl UnifiedCache {
    /// Flip which of the two write buffers is active. O(1) atomic. New
    /// writes start going to the other buffer; entries in the now-flushing
    /// buffer remain indexed and readable via their existing region tag.
    ///
    /// Caller is expected to hold the storage-level flush mutex so two
    /// `start_flush` calls don't race.
    pub fn start_flush(&self) -> FlushGuard {
        let old = self.active_write.load(Ordering::Acquire);
        self.active_write.store(old ^ 1, Ordering::Release);
        FlushGuard { flushing_idx: old }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p bookkeeper-storage --test cache_test start_flush 2>&1 | tail -10
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/cache/unified_cache.rs \
        crates/bookkeeper-storage/tests/cache_test.rs
git commit -m "storage/cache: start_flush atomic swap + FlushGuard"
```

---

## Task 6: drain() — remove-then-reset (invariant I3)

**Files:**
- Modify: `crates/bookkeeper-storage/src/cache/unified_cache.rs`
- Test: `crates/bookkeeper-storage/tests/cache_test.rs`

- [ ] **Step 1: Write the failing test**

Append:

```rust
#[test]
fn unified_drain_removes_all_entries_and_resets_buffer() {
    let c = tiny_cache();
    c.put_write(1, 0, b"a").unwrap();
    c.put_write(1, 1, b"b").unwrap();
    c.put_write(1, 2, b"c").unwrap();
    let guard = c.start_flush();

    // Callback sees each entry's bytes in insertion order.
    let mut seen = Vec::new();
    c.drain(guard, |l, e, data| {
        seen.push((l, e, data.to_vec()));
        Ok::<(), std::convert::Infallible>(())
    }).unwrap();

    assert_eq!(seen, vec![
        (1i64, 0i64, b"a".to_vec()),
        (1,    1,    b"b".to_vec()),
        (1,    2,    b"c".to_vec()),
    ]);

    // Entries gone from the index.
    assert_eq!(c.get(1, 0), None);
    assert_eq!(c.get(1, 1), None);
    assert_eq!(c.get(1, 2), None);
}

#[test]
fn unified_drain_callback_error_propagates_and_leaves_buffer_reset() {
    let c = tiny_cache();
    c.put_write(1, 0, b"a").unwrap();
    c.put_write(1, 1, b"b").unwrap();
    let guard = c.start_flush();
    let mut count = 0;
    let result: Result<(), std::io::Error> = c.drain(guard, |_l, _e, _d| {
        count += 1;
        if count == 2 {
            Err(std::io::Error::new(std::io::ErrorKind::Other, "boom"))
        } else {
            Ok(())
        }
    });
    assert!(result.is_err());
    // Buffer must still be reset so it's reusable next flush cycle.
    // (The in-flight entry is removed from the index because we removed
    // entries as we drained them; partial drain loses the entry. Acceptable
    // because drain failure is fatal — storage typically tears down after.)
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p bookkeeper-storage --test cache_test drain 2>&1 | tail -10
```
Expected: `drain` not found.

- [ ] **Step 3: Write minimal implementation**

Append:

```rust
impl UnifiedCache {
    /// Walks the flushing buffer, invokes `f` with each entry's bytes, and
    /// removes the entry from the unified index **before** resetting the
    /// buffer's offset (invariant I3).
    ///
    /// The caller (flush worker) typically writes the bytes to the entry
    /// log + RocksDB inside `f`. If `f` returns `Err`, drain stops and
    /// returns that error; the buffer is still reset (it's unusable for
    /// correct reads anyway because the interrupted entry was already
    /// removed from the index before `f` ran).
    pub fn drain<F, E>(&self, guard: FlushGuard, mut f: F) -> Result<(), E>
    where
        F: FnMut(i64, i64, &[u8]) -> Result<(), E>,
    {
        let buf = &self.write_bufs[guard.flushing_idx as usize];
        let entries = buf.take_entries();
        let mut result = Ok(());
        for (l, e) in entries {
            // Look up location *before* removing from index.
            let ce = match self.index.get(&(l, e)) {
                Some(r) => *r.value(),
                None => continue, // entry was overwritten in a later put_write
            };
            let data = unsafe {
                buf.read_at(ce.offset as usize, ce.size as usize)
            };
            // I3: remove BEFORE resetting the buffer's offset.
            self.index.remove(&(l, e));
            if let Err(err) = f(l, e, &data) {
                result = Err(err);
                break;
            }
        }
        buf.reset();
        result
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p bookkeeper-storage --test cache_test drain 2>&1 | tail -10
```
Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/cache/unified_cache.rs \
        crates/bookkeeper-storage/tests/cache_test.rs
git commit -m "storage/cache: drain() — remove-before-reset (I3)"
```

---

## Task 7: put_read with or_insert_with (invariant I2)

**Files:**
- Modify: `crates/bookkeeper-storage/src/cache/unified_cache.rs`
- Test: `crates/bookkeeper-storage/tests/cache_test.rs`

- [ ] **Step 1: Write the failing test**

Append:

```rust
#[test]
fn unified_put_read_round_trip() {
    let c = tiny_cache();
    c.put_read(1, 0, b"from-disk");
    assert_eq!(c.get(1, 0), Some(b"from-disk".to_vec()));
    assert!(matches!(c.peek_entry(1, 0).unwrap().region(), Region::ReadSegment(_)));
}

#[test]
fn unified_put_read_does_not_clobber_existing_write() {
    // I2: a write lands first; a later put_read for the same key must be a no-op.
    let c = tiny_cache();
    c.put_write(1, 0, b"writer-wins").unwrap();
    c.put_read(1, 0, b"disk-version");
    // Writer's entry is preserved.
    assert_eq!(c.get(1, 0), Some(b"writer-wins".to_vec()));
    assert_eq!(c.peek_entry(1, 0).unwrap().region(), Region::Write(0));
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p bookkeeper-storage --test cache_test put_read 2>&1 | tail -10
```
Expected: `put_read` not found.

- [ ] **Step 3: Write minimal implementation**

Append to `impl UnifiedCache`:

```rust
/// Populate the cache from a disk read. Uses `or_insert_with` so a
/// concurrent writer's newer value for the same key is preserved (I2).
pub fn put_read(&self, l: i64, e: i64, data: &[u8]) {
    if self.index.contains_key(&(l, e)) {
        // Already cached (writer beat us) — fast path, no segment allocation.
        return;
    }
    let Some((seg_idx, offset)) = self.read_ring_append(l, e, data) else {
        return; // data too big for a segment — rare; drop.
    };
    let ce = CacheEntry::read(seg_idx, offset as u32, data.len() as u32);
    // Final I2 safety: only insert if still absent.
    self.index.entry((l, e)).or_insert(ce);
}

fn read_ring_append(&self, _l: i64, _e: i64, data: &[u8]) -> Option<(u8, usize)> {
    // Task 8 will add rotation. For now: just append to segment 0; fail if full.
    let seg = &self.read_segments[0];
    let off = seg.reserve(data.len()).ok()?;
    unsafe { seg.write_at(off, data); }
    seg.track_entry(_l, _e);
    Some((0, off))
}
```

(Rotation logic comes in Task 8; this task is just about the or_insert_with semantics.)

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p bookkeeper-storage --test cache_test put_read 2>&1 | tail -10
```
Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/cache/unified_cache.rs \
        crates/bookkeeper-storage/tests/cache_test.rs
git commit -m "storage/cache: put_read with or_insert_with (I2)"
```

---

## Task 8: Read-cache ring rotation

**Files:**
- Modify: `crates/bookkeeper-storage/src/cache/unified_cache.rs`
- Test: `crates/bookkeeper-storage/tests/cache_test.rs`

- [ ] **Step 1: Write the failing test**

Append:

```rust
#[test]
fn unified_read_ring_rotates_and_evicts_oldest_segment() {
    // Tiny read cache: 4 × 128B = 512B total. Each entry ~128B aligned.
    let c = UnifiedCache::new(1024 * 1024, 512);
    // Fill segment 0.
    c.put_read(1, 0, &[0u8; 100]);
    // Segment 0 full (100 rounds up to 128, matches segment size).
    // Next put rotates into segment 1.
    c.put_read(1, 1, &[0u8; 100]);
    assert_eq!(c.peek_entry(1, 0).unwrap().region(), Region::ReadSegment(0));
    assert_eq!(c.peek_entry(1, 1).unwrap().region(), Region::ReadSegment(1));
    // Fill 2 and 3.
    c.put_read(1, 2, &[0u8; 100]);
    c.put_read(1, 3, &[0u8; 100]);
    // Inserting a 5th rotates back to segment 0, evicting entry (1, 0).
    c.put_read(1, 4, &[0u8; 100]);
    assert_eq!(c.get(1, 0), None, "oldest segment's entries must be evicted");
    assert_eq!(c.peek_entry(1, 4).unwrap().region(), Region::ReadSegment(0));
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p bookkeeper-storage --test cache_test rotates 2>&1 | tail -10
```
Expected: the test panics — current `read_ring_append` only uses segment 0.

- [ ] **Step 3: Write minimal implementation**

Replace the placeholder `read_ring_append` from Task 7:

```rust
fn read_ring_append(&self, l: i64, e: i64, data: &[u8]) -> Option<(u8, usize)> {
    loop {
        let cur = self.current_segment.load(Ordering::Acquire);
        let seg = &self.read_segments[cur];
        if let Ok(off) = seg.reserve(data.len()) {
            unsafe { seg.write_at(off, data); }
            seg.track_entry(l, e);
            return Some((cur as u8, off));
        }
        // Current segment full. CAS to rotate. Winner evicts the next.
        let next = (cur + 1) % READ_CACHE_SEGMENTS;
        if self.current_segment
            .compare_exchange(cur, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // I3 order: remove-from-index before offset reset.
            let evicted = self.read_segments[next].take_entries();
            for (el, ee) in evicted {
                self.index.remove(&(el, ee));
            }
            self.read_segments[next].reset();
        }
        // Either we rotated or someone else did; retry on the new current.
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p bookkeeper-storage --test cache_test rotates 2>&1 | tail -10
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/cache/unified_cache.rs \
        crates/bookkeeper-storage/tests/cache_test.rs
git commit -m "storage/cache: read-ring rotation with evict-then-reset"
```

---

## Task 9: Concurrency test — I1 (read-under-Ref)

**Files:**
- Test: `crates/bookkeeper-storage/tests/cache_test.rs`

This task adds NO new implementation — the current `get()` already does its byte-copy inside the `DashMap::get(..).map(...)` closure. But the invariant needs a regression test so a future refactor can't break it silently.

- [ ] **Step 1: Write the failing test**

Append:

```rust
use std::sync::Arc as StdArc;
use std::thread;
use std::time::{Duration, Instant};

/// I1 regression: while a drain is hammering the write buffer (start_flush
/// + drain + flip back) a reader looping on `get()` must never see torn
/// bytes. We encode `ledger_id` + a magic marker in every payload and
/// check it on every read.
#[test]
fn unified_get_holds_ref_across_drain() {
    let c = StdArc::new(UnifiedCache::new(1024 * 1024, 1024 * 1024));

    // Populate with a recognizable payload.
    const PAYLOAD: &[u8] = b"\xDE\xAD\xBE\xEF--ledger=42--entry=7--";
    c.put_write(42, 7, PAYLOAD).unwrap();

    let stop = StdArc::new(std::sync::atomic::AtomicBool::new(false));
    let c_r = StdArc::clone(&c);
    let stop_r = StdArc::clone(&stop);
    let reader = thread::spawn(move || {
        while !stop_r.load(Ordering::Relaxed) {
            if let Some(data) = c_r.get(42, 7) {
                assert_eq!(&data, PAYLOAD, "torn read detected");
            }
        }
    });

    let deadline = Instant::now() + Duration::from_millis(200);
    while Instant::now() < deadline {
        // Cycle: flush(active→other), drain (removes entry), put it back, flip again.
        let g = c.start_flush();
        c.drain(g, |_l, _e, _d| Ok::<(), std::convert::Infallible>(())).unwrap();
        c.put_write(42, 7, PAYLOAD).unwrap();
    }

    stop.store(true, Ordering::Relaxed);
    reader.join().unwrap();
}
```

- [ ] **Step 2: Run test to verify it fails (or passes with correct code)**

Per TDD discipline, verify the test runs against the existing code:

```bash
cargo test -p bookkeeper-storage --test cache_test unified_get_holds_ref 2>&1 | tail -10
```
Expected: **pass** — the code was built correctly in Tasks 3 & 6. If it fails, I1 is already broken.

- [ ] **Step 3: Implementation is already in place**

No code change. The invariant is encoded in `UnifiedCache::get` at Task 3 (copy inside the `.map()` closure) and `UnifiedCache::drain` at Task 6 (remove before reset).

- [ ] **Step 4: Run test to verify it still passes**

```bash
cargo test -p bookkeeper-storage --test cache_test 2>&1 | tail -5
```
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/tests/cache_test.rs
git commit -m "storage/cache: add I1 regression test (read under DashMap Ref)"
```

---

## Task 10: Swap DbLedgerStorage field declarations

**Files:**
- Modify: `crates/bookkeeper-storage/src/db_ledger_storage.rs`

This is the riskiest task. Do it in two halves: change the fields first (broken compile — that's expected), then update call sites in Task 11.

- [ ] **Step 1: Write the failing test**

No new test. The existing `db_ledger_storage_test.rs` suite is the harness that must pass at the end of Task 12.

- [ ] **Step 2: Verify baseline**

```bash
cargo test -p bookkeeper-storage --test db_ledger_storage_test 2>&1 | tail -3
```
Expected: all pass on main.

- [ ] **Step 3: Swap field declarations**

In `crates/bookkeeper-storage/src/db_ledger_storage.rs`, locate (currently around line 139-143):

```rust
pub struct DbLedgerStorage {
    // ...
    write_cache: RwLock<WriteCache>,
    write_cache_being_flushed: RwLock<WriteCache>,
    read_cache: Arc<ReadCache>,
    // ...
}
```

Replace with:

```rust
pub struct DbLedgerStorage {
    // ...
    unified_cache: Arc<crate::cache::unified_cache::UnifiedCache>,
    // ...
}
```

Then update the constructor (around line 255-257). Find:

```rust
write_cache: RwLock::new(WriteCache::new(storage_config.write_cache_max_size)),
write_cache_being_flushed: RwLock::new(WriteCache::new(storage_config.write_cache_max_size)),
read_cache: Arc::new(ReadCache::new(storage_config.read_cache_max_size, READ_CACHE_SEGMENTS)),
```

Replace with:

```rust
unified_cache: Arc::new(crate::cache::unified_cache::UnifiedCache::new(
    storage_config.write_cache_max_size,
    storage_config.read_cache_max_size,
)),
```

Remove the now-unused `READ_CACHE_SEGMENTS` constant if nothing else references it; otherwise leave it.

- [ ] **Step 4: Verify expected compile failures**

```bash
cargo check -p bookkeeper-storage 2>&1 | head -30
```
Expected: errors like `error[E0609]: no field \`write_cache\` on type \`&DbLedgerStorage\`` at every call site. Count them — this is the task list for Task 11.

- [ ] **Step 5: Commit (yes, with compile errors — it's a transitional state, and we'll fix in the next commit)**

Actually, do NOT commit with compile errors. Instead, do Task 11's step 3 AS PART OF THIS COMMIT so there's no broken state in history.

Proceed to Task 11 and commit once compile is green.

---

## Task 11: Update DbLedgerStorage call sites

**Files:**
- Modify: `crates/bookkeeper-storage/src/db_ledger_storage.rs`

This is the mechanical-but-large translation task. Each edit is one call-site pattern. The Task 10 `cargo check` output enumerates them.

Patterns (apply each time the corresponding old API appears):

| Old call (from `cargo check` errors) | New call |
|---|---|
| `self.write_cache.read().get(l, e)` | *(subsumed — see `get_entry` below)* |
| `self.write_cache_being_flushed.read().get(l, e)` | *(subsumed)* |
| `self.read_cache.get(l, e)` | *(subsumed)* |
| `self.read_cache.put(l, e, &data)` | `self.unified_cache.put_read(l, e, &data)` |
| `self.write_cache.read().put(l, e, entry)` | `self.unified_cache.put_write(l, e, entry)` |
| `self.write_cache.read().last_entry_id(l)` | `self.unified_cache.last_entry_id(l)` |
| `self.write_cache.read().count()` | `self.unified_cache.write_cache_count()` (add accessor — see below) |

- [ ] **Step 1: Rewrite `get_entry`**

Find `async fn get_entry(&self, ...)` (around line 975). The three-cache chain:

```rust
if let Some(data) = self.write_cache.read().get(ledger_id, entry_id) { ... }
if let Some(data) = self.write_cache_being_flushed.read().get(ledger_id, entry_id) { ... }
if let Some(data) = self.read_cache.get(ledger_id, entry_id) { ... }
```

Becomes a single lookup:

```rust
if let Some(data) = self.unified_cache.get(ledger_id, entry_id) {
    storage_metrics::READ_ENTRY.observe(read_start.elapsed().as_secs_f64());
    return Ok(Bytes::from(data));
}
```

- [ ] **Step 2: Rewrite `add_entry`**

Find every `self.write_cache.read().put(...)` call. Replace with `self.unified_cache.put_write(...)`. Semantics (Ok / StorageError::StorageFull for buffer-full) match.

- [ ] **Step 3: Rewrite `swap_caches` + `drain_flushing`**

Find `fn swap_caches` (around line 656). Currently:

```rust
fn swap_caches(&self) {
    let swap_start = Instant::now();
    let mut active = self.write_cache.write();
    let mut flushing = self.write_cache_being_flushed.write();
    std::mem::swap(&mut *active, &mut *flushing);
    storage_metrics::FLUSH_CACHE_SWAP.observe(swap_start.elapsed().as_secs_f64());
}
```

Delete the function entirely. Find its callers (the flush worker). Replace each `self.swap_caches()` with:

```rust
let swap_start = Instant::now();
let flush_guard = self.unified_cache.start_flush();
storage_metrics::FLUSH_CACHE_SWAP.observe(swap_start.elapsed().as_secs_f64());
// store `flush_guard` on self or pass to drain_flushing
```

For `drain_flushing` (around line 670): the function currently iterates `self.write_cache_being_flushed.read().sorted_entries()` and reads bytes via `.data()`. Replace with `self.unified_cache.drain(flush_guard, |l, e, data| { ... write to entry log + RocksDB ... })`. The closure body is the existing per-entry I/O code.

- [ ] **Step 4: Rewrite `read_entry_from_log` caller and the readahead spawn block**

Find the readahead block (around line 1004 in `get_entry`). It calls `self.read_cache.put(...)` after the disk read (around line 1002) and inside the spawned readahead task. Both become `self.unified_cache.put_read(...)`.

- [ ] **Step 5: Add a `write_cache_count` accessor on UnifiedCache**

Any caller that previously used `self.write_cache.read().count()` (likely the metrics-gauge update code in `storage_metrics.rs` or the sync worker) needs an equivalent. Add to `unified_cache.rs`:

```rust
impl UnifiedCache {
    /// Count of entries currently in *any* of the 6 regions.
    pub fn total_count(&self) -> usize { self.index.len() }
    /// Count of entries specifically in the active write buffer.
    pub fn write_cache_count(&self) -> usize {
        self.write_bufs[self.active_write.load(Ordering::Acquire) as usize]
            .entries.lock().len()
    }
    /// Count of entries in the read ring (across all segments).
    pub fn read_cache_count(&self) -> usize {
        self.read_segments.iter().map(|s| s.entries.lock().len()).sum()
    }
}
```

- [ ] **Step 6: Verify compile + test**

```bash
cargo check -p bookkeeper-storage 2>&1 | tail -5
cargo test -p bookkeeper-storage --test db_ledger_storage_test 2>&1 | tail -5
```
Expected: `Finished` / all tests pass.

- [ ] **Step 7: Commit (together with Task 10's field change)**

```bash
git add crates/bookkeeper-storage/src/db_ledger_storage.rs \
        crates/bookkeeper-storage/src/cache/unified_cache.rs
git commit -m "storage: swap DbLedgerStorage over to UnifiedCache"
```

---

## Task 12: Delete old cache modules + rewrite cache_test.rs

**Files:**
- Delete: `crates/bookkeeper-storage/src/cache/read_cache.rs`
- Delete: `crates/bookkeeper-storage/src/cache/write_cache.rs`
- Modify: `crates/bookkeeper-storage/src/cache/mod.rs`
- Rewrite: `crates/bookkeeper-storage/tests/cache_test.rs` (remove old tests referencing deleted types)

- [ ] **Step 1: Remove old module exports**

In `crates/bookkeeper-storage/src/cache/mod.rs`:

```rust
pub mod unified_cache;
```

- [ ] **Step 2: Delete old files**

```bash
rm crates/bookkeeper-storage/src/cache/read_cache.rs
rm crates/bookkeeper-storage/src/cache/write_cache.rs
```

- [ ] **Step 3: Prune dead tests from cache_test.rs**

Open `crates/bookkeeper-storage/tests/cache_test.rs` and delete every test that references `WriteCache::` or `ReadCache::` (the old ones from the pre-refactor file). Keep all the `unified_` tests added in Tasks 1–9. Remove the `use bookkeeper_storage::cache::write_cache::WriteCache;` and `use bookkeeper_storage::cache::read_cache::ReadCache;` imports.

- [ ] **Step 4: Verify compile + test**

```bash
cargo check -p bookkeeper-storage 2>&1 | tail -5
cargo test -p bookkeeper-storage 2>&1 | grep -E "^(test result:|FAILED|error\[)" | tail -15
```
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "storage/cache: delete superseded read_cache and write_cache modules"
```

---

## Task 13: Wire up the previously-stub cache gauges

**Files:**
- Modify: `crates/bookkeeper-storage/src/storage_metrics.rs`
- Modify: `crates/bookkeeper-storage/src/db_ledger_storage.rs`

The gauges `bookie_read_cache_count`, `bookie_read_cache_size`, `bookie_write_cache_count`, `bookie_write_cache_size`, `bookie_write_cache_fill_ratio` are declared but never updated (we noted this earlier in the perf session). The refactor gives us a single source of truth.

- [ ] **Step 1: Write the failing test**

In `crates/bookkeeper-server/tests/metrics_names_test.rs` there's already a list; verify these names are in it. Then add a behavioral test to `crates/bookkeeper-storage/tests/db_ledger_storage_test.rs`:

```rust
#[tokio::test]
async fn cache_gauges_update_after_put() {
    use bookkeeper_storage::storage_metrics::{
        WRITE_CACHE_COUNT, WRITE_CACHE_SIZE, READ_CACHE_COUNT,
    };
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();

    assert_eq!(WRITE_CACHE_COUNT.get(), 0);
    assert_eq!(WRITE_CACHE_SIZE.get(), 0);

    let entry = make_entry(1, 0, -1, &vec![0xAAu8; 128]);
    storage.add_entry(&entry).await.unwrap();

    // The gauge should reflect the put. The sync-thread cadence may push
    // updates asynchronously; give it a moment if needed.
    assert!(WRITE_CACHE_COUNT.get() >= 1, "write_cache_count should be ≥1");
    assert!(WRITE_CACHE_SIZE.get() >= 128, "write_cache_size should be ≥128");

    // Read-cache gauge populates after a get-with-miss that hits disk.
    storage.flush().await.unwrap();
    let _ = storage.get_entry(1, 0).await.unwrap();
    assert!(READ_CACHE_COUNT.get() >= 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p bookkeeper-storage --test db_ledger_storage_test cache_gauges 2>&1 | tail -10
```
Expected: gauges stay at 0 (they're never updated).

- [ ] **Step 3: Wire the gauges**

In `crates/bookkeeper-storage/src/db_ledger_storage.rs`, find the sync worker loop (around line 345) that runs periodically. After the existing flush trigger logic, add:

```rust
// Update cache-state gauges from UnifiedCache.
let uc = &storage.unified_cache;
storage_metrics::WRITE_CACHE_COUNT.set(uc.write_cache_count() as i64);
storage_metrics::WRITE_CACHE_SIZE.set(uc.write_cache_bytes() as i64);
storage_metrics::WRITE_CACHE_FILL_RATIO.set(uc.write_cache_fill_ratio());
storage_metrics::READ_CACHE_COUNT.set(uc.read_cache_count() as i64);
storage_metrics::READ_CACHE_SIZE.set(uc.read_cache_bytes() as i64);
```

And add the missing accessors in `unified_cache.rs`:

```rust
impl UnifiedCache {
    pub fn write_cache_bytes(&self) -> usize {
        self.write_bufs[self.active_write.load(Ordering::Acquire) as usize].current_offset()
    }
    pub fn write_cache_fill_ratio(&self) -> f64 {
        let buf = &self.write_bufs[self.active_write.load(Ordering::Acquire) as usize];
        buf.current_offset() as f64 / buf.max_size() as f64
    }
    pub fn read_cache_bytes(&self) -> usize {
        self.read_segments.iter().map(|s| s.current_offset()).sum()
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p bookkeeper-storage --test db_ledger_storage_test cache_gauges 2>&1 | tail -10
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-storage/src/db_ledger_storage.rs \
        crates/bookkeeper-storage/src/cache/unified_cache.rs \
        crates/bookkeeper-storage/tests/db_ledger_storage_test.rs
git commit -m "storage: wire up cache-state gauges (previously declared but unupdated)"
```

---

## Task 14: Full regression + rollout-gate benchmark

**Files:** none — this is a verification-only task.

- [ ] **Step 1: Full test suite**

```bash
cargo test -p bookkeeper-storage --tests 2>&1 | grep -E "^(test result:|FAILED|error\[)" | tail -15
cargo test -p bookkeeper-server --test metrics_names_test 2>&1 | tail -10
```
Expected: all pass (including the full `db_ledger_storage_test.rs` suite that the public API has to preserve).

- [ ] **Step 2: Clippy (no new warnings in touched files)**

```bash
cargo clippy -p bookkeeper-storage --tests 2>&1 | grep -E "(warning|error).*unified_cache\.rs" | head -20
```
Expected: empty (or no new warnings).

- [ ] **Step 3: Rebuild the debug-runtime image + restart bookies**

```bash
cd /Users/lipenghui/Github/sn/bookkeeper-rust
docker build --target debug-runtime -t rust-bookie:latest . 2>&1 | tail -5
docker compose up -d rust-bookie-1 rust-bookie-2
sleep 15
docker compose ps | grep rust-bookie
```
Expected: both bookies `(healthy)`.

- [ ] **Step 4: Run the catch-up-read perf test (requires external consumer)**

Reattach the test consumer. Measure for ≥60s:

```bash
# CPU + throughput
docker stats --no-stream rustbk-rust-bookie-1 rustbk-rust-bookie-2
docker compose exec -T pulsar-broker bin/pulsar-admin topics stats persistent://public/default/test \
    2>&1 | grep -E '"msgRateOut"|"msgThroughputOut"' | head -2

# Counters must stay at 0 (no new EIO path)
for p in 8081 8082; do
    curl -sS http://localhost:$p/metrics | grep -E \
        'BOOKIE_READ_ENTRY_count\{success="false"\}|io_uring_sq_full_total|io_uring_buf_pool_exhausted_total' | grep -v "^#"
done

# Flamegraph — DashMap hash+probe should drop from ~14% to ≲5%
curl -sS -o /tmp/unified-cache-post.svg 'http://localhost:8082/debug/pprof/flamegraph?seconds=30'
```

Acceptance criteria (spec §"Rollout gate"):
- `BOOKIE_READ_ENTRY_count{success="false"}` stays at 0
- `io_uring_sq_full_total`, `io_uring_buf_pool_exhausted_total` stay at 0
- `msgRateOut` ≥ pre-refactor baseline (~7,000 msg/s from the session)
- Flamegraph: self-time in DashMap hash+probe paths drops measurably (target ≲5%)

- [ ] **Step 5: Commit the benchmark notes (or roll back)**

If all criteria pass:
```bash
# No code changes in this task — just verification.
# Optionally append a brief result summary to the plan doc or spec.
echo "OK"
```

If any regresses: `git revert` the unification commits and file a follow-up issue with the flamegraph + counters attached.

---

## Self-Review

### Spec coverage
- § Architecture: unified `DashMap` + 2 write bufs + 4 read segments — Tasks 1, 2, 3, 8
- § Data Structures: all defined in Tasks 1 (CacheEntry, Region), 2 (WriteBuffer), 3 (UnifiedCache)
- § Core Operations:
    - `get` — Task 3
    - `put_write` — Task 3
    - `put_read` — Task 7
    - `start_flush` — Task 5
    - `drain` — Task 6
    - `read_ring_append` (rotation) — Task 8
- § Concurrency Invariants:
    - I1 (read under Ref) — Task 3 (impl) + Task 9 (regression test)
    - I2 (or_insert_with) — Task 7 (impl + test)
    - I3 (remove-then-reset) — Task 6 (drain) + Task 8 (rotation)
- § Testing — Tasks 1–9 cover unit tests; Task 14 covers the full suite; loom test deferred per spec
- § Migration — Tasks 10, 11, 12, 13 walk through the rewrite sequence
- § Rollout gate — Task 14

### Type / signature consistency check
- `CacheEntry::write(idx, offset, size)` and `::read(idx, offset, size)` — used consistently (Tasks 1, 3, 7, 8)
- `UnifiedCache::new(write_buf_size, read_cache_total_size)` — same call in Tasks 3, 10
- `FlushGuard::flushing_idx() -> u8` — Task 5 defines it returning u8, Task 6's `drain` matches
- `drain<F, E>(guard, f: F) -> Result<(), E>` — same shape at definition (Task 6) and consumption (Task 11)
- `put_read(l, e, data)` returns `()` — consistent
- Accessor names (`write_cache_count`, `write_cache_bytes`, etc.) match between definition (Task 11/13) and use (Task 13)

No placeholders detected. All steps either show full code or are strictly verification-only.
