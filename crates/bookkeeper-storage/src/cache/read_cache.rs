use dashmap::DashMap;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Ring-buffer read cache with multiple segments.
pub struct ReadCache {
    segments: Vec<CacheSegment>,
    current_idx: AtomicUsize,
    num_segments: usize,
}

struct CacheSegment {
    data: UnsafeCell<Vec<u8>>,
    offset: AtomicUsize,
    max_size: usize,
    index: DashMap<(i64, i64), (usize, usize)>,
}

// SAFETY: Concurrent writes go to non-overlapping regions via atomic offset.
// Concurrent reads only access regions that were fully written (offset was committed to index).
unsafe impl Send for CacheSegment {}
unsafe impl Sync for CacheSegment {}

impl CacheSegment {
    fn new(size: usize) -> Self {
        Self {
            data: UnsafeCell::new(vec![0u8; size]),
            offset: AtomicUsize::new(0),
            max_size: size,
            index: DashMap::new(),
        }
    }

    fn put(&self, ledger_id: i64, entry_id: i64, entry: &[u8]) -> bool {
        if entry.len() > self.max_size {
            return false;
        }
        // Reserve space atomically via CAS loop (same fix as WriteCache).
        let offset = loop {
            let current = self.offset.load(Ordering::SeqCst);
            if current + entry.len() > self.max_size {
                return false;
            }
            if self.offset.compare_exchange(
                current,
                current + entry.len(),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ).is_ok() {
                break current;
            }
        };
        unsafe {
            let data = &mut *self.data.get();
            data[offset..offset + entry.len()].copy_from_slice(entry);
        }
        self.index.insert((ledger_id, entry_id), (offset, entry.len()));
        true
    }

    fn get(&self, ledger_id: i64, entry_id: i64) -> Option<Vec<u8>> {
        self.index.get(&(ledger_id, entry_id)).map(|e| {
            let (offset, size) = *e.value();
            unsafe {
                let data = &*self.data.get();
                data[offset..offset + size].to_vec()
            }
        })
    }

    fn clear(&self) {
        // Clear the index BEFORE resetting offset. If a concurrent get()
        // looks up an entry after the offset is zeroed but before the
        // index is cleared, it could read from a region being overwritten
        // by a new put(). Clearing the index first ensures get() returns
        // None for any stale entries before new writes begin.
        self.index.clear();
        self.offset.store(0, Ordering::SeqCst);
    }
}

impl ReadCache {
    pub fn new(total_size: usize, num_segments: usize) -> Self {
        let num_segments = num_segments.max(2);
        let segment_size = total_size / num_segments;
        let segments = (0..num_segments).map(|_| CacheSegment::new(segment_size)).collect();
        Self {
            segments,
            current_idx: AtomicUsize::new(0),
            num_segments,
        }
    }

    pub fn put(&self, ledger_id: i64, entry_id: i64, entry: &[u8]) {
        if self.num_segments == 0 || entry.len() > self.segments[0].max_size {
            return;
        }
        let idx = self.current_idx.load(Ordering::SeqCst);
        if !self.segments[idx].put(ledger_id, entry_id, entry) {
            let next = (idx + 1) % self.num_segments;
            // Only one thread should perform the rotation
            if self.current_idx.compare_exchange(idx, next, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                self.segments[next].clear();
            }
            // Try again with the (possibly new) current segment
            let current = self.current_idx.load(Ordering::SeqCst);
            let _ = self.segments[current].put(ledger_id, entry_id, entry);
        }
    }

    pub fn get(&self, ledger_id: i64, entry_id: i64) -> Option<Vec<u8>> {
        let current = self.current_idx.load(Ordering::SeqCst);
        // Search from current backward
        for i in 0..self.num_segments {
            let idx = (current + self.num_segments - i) % self.num_segments;
            if let Some(data) = self.segments[idx].get(ledger_id, entry_id) {
                return Some(data);
            }
        }
        None
    }
}
