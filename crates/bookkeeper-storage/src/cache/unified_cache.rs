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
