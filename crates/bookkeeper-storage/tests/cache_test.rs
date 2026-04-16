use bookkeeper_storage::cache::write_cache::WriteCache;
use bookkeeper_storage::cache::read_cache::ReadCache;

#[test]
fn write_cache_put_get() {
    let cache = WriteCache::new(1024 * 1024); // 1MB
    cache.put(1, 0, b"hello").unwrap();
    let data = cache.get(1, 0).unwrap();
    assert_eq!(data, b"hello");
}

#[test]
fn write_cache_missing_entry() {
    let cache = WriteCache::new(1024 * 1024);
    assert!(cache.get(1, 0).is_none());
}

#[test]
fn write_cache_last_entry() {
    let cache = WriteCache::new(1024 * 1024);
    cache.put(1, 0, b"a").unwrap();
    cache.put(1, 5, b"b").unwrap();
    cache.put(1, 3, b"c").unwrap();
    assert_eq!(cache.last_entry_id(1), Some(5));
}

#[test]
fn write_cache_full_returns_error() {
    let cache = WriteCache::new(256); // tiny cache
    // Entries are 64-byte aligned, so even a 1-byte entry takes 64 bytes
    cache.put(1, 0, b"a").unwrap();
    cache.put(1, 1, b"b").unwrap();
    cache.put(1, 2, b"c").unwrap();
    cache.put(1, 3, b"d").unwrap();
    // 4 * 64 = 256, cache is full
    assert!(cache.put(1, 4, b"e").is_err());
}

#[test]
fn write_cache_clear() {
    let cache = WriteCache::new(1024 * 1024);
    cache.put(1, 0, b"hello").unwrap();
    cache.clear();
    assert!(cache.get(1, 0).is_none());
    assert_eq!(cache.count(), 0);
}

#[test]
fn write_cache_sorted_entries() {
    let cache = WriteCache::new(1024 * 1024);
    cache.put(2, 0, b"b0").unwrap();
    cache.put(1, 1, b"a1").unwrap();
    cache.put(1, 0, b"a0").unwrap();
    cache.put(2, 1, b"b1").unwrap();

    let entries = cache.sorted_entries();
    assert_eq!(entries.len(), 4);
    assert_eq!(entries[0].0, 1); // ledgerId
    assert_eq!(entries[0].1, 0); // entryId
    assert_eq!(entries[1].0, 1);
    assert_eq!(entries[1].1, 1);
    assert_eq!(entries[2].0, 2);
    assert_eq!(entries[2].1, 0);
    // Verify data access through data()
    let data = cache.data();
    let (_, _, offset, size) = entries[0];
    assert_eq!(&data[offset..offset + size], b"a0");
}

#[test]
fn read_cache_put_get() {
    let cache = ReadCache::new(1024 * 1024, 2);
    cache.put(1, 0, b"data");
    let data = cache.get(1, 0);
    assert_eq!(data.unwrap(), b"data");
}

#[test]
fn read_cache_eviction() {
    // 2 segments of 512 bytes each = 1024 total
    let cache = ReadCache::new(1024, 2);
    // Fill segment 0 (entries are stored with some overhead)
    for i in 0..6 {
        cache.put(1, i, &[0u8; 64]);
    }
    // This should trigger rotation to segment 1, evicting segment 0
    for i in 6..12 {
        cache.put(1, i, &[0u8; 64]);
    }
    // Recent entries should still be found
    assert!(cache.get(1, 11).is_some());
}

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
