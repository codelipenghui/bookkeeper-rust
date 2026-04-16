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
