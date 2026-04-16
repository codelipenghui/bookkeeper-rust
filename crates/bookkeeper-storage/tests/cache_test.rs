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

use bookkeeper_storage::cache::unified_cache::{UnifiedCache, WriteBuffer};

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

use bookkeeper_storage::StorageError;

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
        Err(StorageError::StorageFull) => {}
        other => panic!("expected StorageFull, got {other:?}"),
    }
}

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
    // Buffer reusability: the cache must still accept new writes after the error.
    c.put_write(2, 0, b"post-error").unwrap();
    assert_eq!(c.get(2, 0), Some(b"post-error".to_vec()));
}

/// Regression: if a key is overwritten by `put_write` after `start_flush`
/// but before `drain`, the drain loop must skip the stale entry in the
/// flushing buffer and leave the live entry in the active buffer alone.
#[test]
fn unified_drain_skips_entries_superseded_by_later_put_write() {
    let c = tiny_cache();
    c.put_write(1, 0, b"first").unwrap();
    let guard = c.start_flush();
    // Overwrite the same key while it's still "in flight" on the flushing side.
    c.put_write(1, 0, b"second").unwrap();
    // Drain should skip (1, 0) because its index entry now points at the
    // active buffer, not the flushing buffer we're draining.
    let mut seen = Vec::new();
    c.drain(guard, |l, e, data| {
        seen.push((l, e, data.to_vec()));
        Ok::<(), std::convert::Infallible>(())
    }).unwrap();
    // Nothing drained — the only entry tracked in the flushing buffer
    // had been superseded.
    assert!(seen.is_empty(), "drain should have skipped the superseded entry; got {seen:?}");
    // The live entry must still be readable with the NEW bytes.
    assert_eq!(c.get(1, 0), Some(b"second".to_vec()));
    // And it must still be indexed as Write(1) (the active buffer after flush flip).
    assert_eq!(c.peek_entry(1, 0).unwrap().region(), Region::Write(1));
}
