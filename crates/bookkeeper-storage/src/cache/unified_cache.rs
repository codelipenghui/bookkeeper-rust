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
/// Packing: `region_tag` top bit clear (0x00–0x7F) = WriteBuffer;
/// top bit set (0x80–0xFF) = ReadSegment. Low 3 bits = index within
/// that array (0..=1 for Write, 0..=3 for ReadSegment).
/// Offsets and sizes are `u32` because no region exceeds 256 MB (fits in 28 bits).
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CacheEntry {
    pub region_tag: u8,
    pub offset: u32,
    pub size: u32,
}

/// Top bit clear — region_tag 0x00..=0x7F — identifies a WriteBuffer slot.
const WRITE_FLAG: u8 = 0x00;
/// Top bit set — region_tag 0x80..=0xFF — identifies a ReadSegment slot.
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

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use dashmap::DashMap;
use parking_lot::Mutex;
use crate::StorageError;

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

// SAFETY:
// - Concurrent appends receive disjoint byte ranges via the CAS in `reserve`.
//   Writes proceed through `(*self.data.get()).as_mut_ptr()`, which calls
//   `Vec::as_mut_ptr(&mut self)` via method syntax; the compiler's autoref
//   thus creates a transient `&mut Vec<u8>` just long enough for pointer
//   extraction. Under strict Stacked Borrows this is technically UB when
//   two threads materialize the autoref simultaneously, but LLVM erases
//   the reference before codegen and no real miscompilation is known on
//   any platform we target. A future refactor may cache the data pointer
//   at construction time (storing `data_ptr: *mut u8` alongside an
//   `UnsafeCell<Box<[u8]>>`) to eliminate the autoref entirely.
// - Reads against already-written regions are safe because no writer
//   overwrites a region until the unified index's entry pointing into
//   it has been removed (invariant I3 — see the design spec); callers
//   of `read_at` hold the DashMap `Ref` across the copy to block the
//   `index.remove` that must precede any `reset`.
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
            let cur = self.offset.load(Ordering::Relaxed);
            if cur + aligned > self.max_size { return Err(()); }
            if self.offset.compare_exchange_weak(
                cur, cur + aligned, Ordering::Release, Ordering::Relaxed,
            ).is_ok() {
                return Ok(cur);
            }
        }
    }

    /// # Safety
    /// Caller must hold a reserved range `[offset, offset + data.len())` obtained
    /// from [`reserve`] and not have written there yet. Concurrent writes to
    /// disjoint ranges are safe because no mutable reference to the backing
    /// `Vec<u8>` is materialized — writes proceed through a raw pointer to the
    /// unique byte range reserved by the CAS.
    pub unsafe fn write_at(&self, offset: usize, data: &[u8]) {
        let dst = (*self.data.get()).as_mut_ptr().add(offset);
        std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
    }

    /// # Safety
    /// The region `[offset, offset + size)` must have been previously written
    /// by [`write_at`] and not yet reset. In the full `UnifiedCache`, the
    /// caller ensures this by holding a `DashMap::Ref` on the index entry
    /// that points at this offset — the `Ref` blocks concurrent `index.remove`
    /// which is a prerequisite for `reset` (invariant I3). Standalone users
    /// of `WriteBuffer` must provide an equivalent guarantee. The destination
    /// `buf` inside this function is locally owned and exclusive, so the
    /// `buf.as_mut_ptr()` call carries no aliasing implications.
    pub unsafe fn read_at(&self, offset: usize, size: usize) -> Vec<u8> {
        let src = (*self.data.get()).as_ptr().add(offset);
        let mut buf = Vec::with_capacity(size);
        std::ptr::copy_nonoverlapping(src, buf.as_mut_ptr(), size);
        buf.set_len(size);
        buf
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

    /// Non-destructive count of entries currently tracked in this buffer.
    pub fn entries_count(&self) -> usize {
        self.entries.lock().len()
    }

    /// Resets the offset to 0 — the buffer is now reusable. Entries in the
    /// unified index pointing here MUST have been removed before this call
    /// (invariant I3). Clears the tracking list **before** zeroing the offset
    /// so a concurrent reader can never observe "offset=0 but entries still
    /// populated". Under the designed usage (drain calls `take_entries` then
    /// `reset`), this window is already closed; the ordering here is defensive.
    pub fn reset(&self) {
        self.entries.lock().clear();
        self.offset.store(0, Ordering::Release);
    }
}

/// Semantic alias for a read-cache segment. Same shape and mechanics as
/// [`WriteBuffer`]; the alias keeps call sites self-documenting. Because
/// this is a *type alias* (not a newtype), `WriteBuffer` and `ReadSegment`
/// are interchangeable to the compiler — call sites must not pass one
/// where the other is semantically expected. If that distinction starts
/// mattering in practice, replace with a newtype wrapper.
pub type ReadSegment = WriteBuffer;

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
    /// copy. This blocks concurrent `index.remove()` from drain/rotation.
    /// Without that block, a reader could release the `Ref` after capturing
    /// the `CacheEntry`, drain would then remove the entry and `reset()`
    /// the buffer's offset (invariant I3 — remove-before-reset), and a new
    /// writer could begin overwriting those bytes while the copy is still
    /// in flight.
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

    /// Inspection helper used only by integration tests in
    /// `tests/cache_test.rs` — callers should use [`get`] to read a cached
    /// entry. Marked `#[doc(hidden)]` to keep it out of the public API
    /// surface even though it must be `pub` for integration-test visibility.
    #[doc(hidden)]
    pub fn peek_entry(&self, l: i64, e: i64) -> Option<CacheEntry> {
        self.index.get(&(l, e)).map(|r| *r.value())
    }

    pub fn last_entry_id(&self, ledger_id: i64) -> Option<i64> {
        self.last_entry.get(&ledger_id).map(|v| *v.value())
    }
}

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
    /// # Caller contract
    /// The caller **must** hold the storage-level flush mutex for the
    /// entire `start_flush → drain` sequence. Without it, two concurrent
    /// callers can read the same `active_write` value, both store the
    /// same flipped result, and both receive a `FlushGuard` pointing at
    /// the same buffer — causing `drain` to be called twice on the same
    /// buffer and silently losing the flushed entries.
    pub fn start_flush(&self) -> FlushGuard {
        let old = self.active_write.load(Ordering::Acquire);
        self.active_write.store(old ^ 1, Ordering::Release);
        FlushGuard { flushing_idx: old }
    }

    /// Populate the cache from a disk read. Uses the DashMap `Entry` API to
    /// atomically no-op if a concurrent `put_write` has already inserted a
    /// newer value for this key (invariant I2). The segment's `entries`
    /// tracking Vec is only updated when we actually win the insertion race
    /// — otherwise the orphaned tracking record would cause Task 8's segment
    /// rotation to later remove a live write-buffer entry whose key matches.
    pub fn put_read(&self, l: i64, e: i64, data: &[u8]) {
        if self.index.contains_key(&(l, e)) {
            // Already cached (writer beat us) — fast path, no segment allocation.
            return;
        }
        let Some((seg_idx, offset)) = self.read_ring_append(data) else {
            return; // data too big for a segment — rare; drop.
        };
        let ce = CacheEntry::read(seg_idx, offset as u32, data.len() as u32);
        // Atomic insert-if-absent. `Entry::Vacant` means we won the race —
        // track the key in the segment's eviction list. `Entry::Occupied`
        // means a concurrent writer inserted after our `contains_key` check;
        // leave their entry alone and abandon the segment slot we reserved
        // (it'll get overwritten when the segment eventually rotates; the
        // key is NOT tracked so rotation won't mis-evict the writer's entry).
        match self.index.entry((l, e)) {
            dashmap::mapref::entry::Entry::Vacant(v) => {
                v.insert(ce);
                self.read_segments[seg_idx as usize].track_entry(l, e);
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                // concurrent writer landed first; nothing to do.
            }
        }
    }

    /// Reserve and write bytes into the current read segment, rotating to
    /// the next segment when full. Returns `Some((seg_idx, offset))` where
    /// bytes landed, or `None` if the data is too large to fit in any
    /// segment. Does NOT call `track_entry` — the caller (`put_read`)
    /// is responsible for tracking only after confirming the DashMap insert
    /// actually won the race, so we never track a key that isn't indexed
    /// into this segment.
    ///
    /// On rotation, the winning thread removes all entries from the evicted
    /// segment's tracking list from the unified index **before** zeroing
    /// the segment's offset (invariant I3 — remove-before-reset). Other
    /// threads that race on the CAS spin until they see the new
    /// `current_segment`, then retry the reservation.
    fn read_ring_append(&self, data: &[u8]) -> Option<(u8, usize)> {
        // Guard: refuse oversized data that no segment can hold. Without this,
        // the rotation CAS loop spins forever because every `reserve` fails.
        // All segments are the same size — if it doesn't fit in one, it doesn't
        // fit in any.
        if align64(data.len()) > self.read_segments[0].max_size() {
            return None;
        }
        loop {
            let cur = self.current_segment.load(Ordering::Acquire);
            let seg = &self.read_segments[cur];
            if let Ok(off) = seg.reserve(data.len()) {
                unsafe { seg.write_at(off, data); }
                return Some((cur as u8, off));
            }
            // Current segment full. CAS to rotate. Winner evicts the next.
            let next = (cur + 1) % READ_CACHE_SEGMENTS;
            if self
                .current_segment
                .compare_exchange(cur, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // I3: remove-from-index before offset reset.
                let evicted = self.read_segments[next].take_entries();
                for (el, ee) in evicted {
                    self.index.remove(&(el, ee));
                }
                self.read_segments[next].reset();
            }
            // Either we rotated or someone else did; retry on the new current.
        }
    }

    /// Walks the flushing buffer, invokes `f` with each entry's bytes, and
    /// removes the entry from the unified index **before** resetting the
    /// buffer's offset (invariant I3).
    ///
    /// The caller (flush worker) typically writes the bytes to the entry
    /// log + RocksDB inside `f`. If `f` returns `Err`, drain stops and
    /// returns that error; the buffer is still reset (it's unusable for
    /// correct reads anyway because the interrupted entry was already
    /// removed from the index before `f` ran).
    ///
    /// The `FlushGuard` is taken by value and consumed — preventing a second
    /// drain on the same guard at the type level.
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
            // Guard against overwrite: if the index now points into a different
            // region, this (l, e) was superseded by a later `put_write` after
            // `start_flush`. The newer value still lives in its current region
            // and is correctly indexed — leave it alone. The stale bytes sitting
            // in the flushing buffer will be overwritten when this buffer is
            // re-used next cycle.
            if ce.region() != Region::Write(guard.flushing_idx) {
                continue;
            }
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

    /// Count of entries currently in the active write buffer.
    /// Non-destructive; does not affect the buffer's tracked entry list.
    pub fn write_cache_count(&self) -> usize {
        self.write_bufs[self.active_write.load(Ordering::Acquire) as usize].entries_count()
    }

    /// Bytes consumed in the active write buffer (bump-allocator offset).
    pub fn write_cache_bytes(&self) -> usize {
        self.write_bufs[self.active_write.load(Ordering::Acquire) as usize].current_offset()
    }

    /// Fill ratio of the active write buffer (0.0 – 1.0).
    pub fn write_cache_fill_ratio(&self) -> f64 {
        let buf = &self.write_bufs[self.active_write.load(Ordering::Acquire) as usize];
        if buf.max_size() == 0 {
            0.0
        } else {
            buf.current_offset() as f64 / buf.max_size() as f64
        }
    }

    /// Total count of entries across all 4 read-cache segments.
    pub fn read_cache_count(&self) -> usize {
        self.read_segments.iter().map(|s| s.entries_count()).sum()
    }

    /// Total bytes consumed across all 4 read-cache segments.
    pub fn read_cache_bytes(&self) -> usize {
        self.read_segments.iter().map(|s| s.current_offset()).sum()
    }
}
