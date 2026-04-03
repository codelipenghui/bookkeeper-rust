use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Ring-buffer read cache with multiple segments.
pub struct ReadCache {
    segments: Vec<CacheSegment>,
    current_idx: AtomicUsize,
    num_segments: usize,
}

struct CacheSegment {
    data: Vec<u8>,
    offset: AtomicUsize,
    max_size: usize,
    index: DashMap<(i64, i64), (usize, usize)>,
}

impl CacheSegment {
    fn new(size: usize) -> Self {
        Self {
            data: vec![0u8; size],
            offset: AtomicUsize::new(0),
            max_size: size,
            index: DashMap::new(),
        }
    }

    fn put(&self, ledger_id: i64, entry_id: i64, entry: &[u8]) -> bool {
        let offset = self.offset.fetch_add(entry.len(), Ordering::SeqCst);
        if offset + entry.len() > self.max_size {
            self.offset.fetch_sub(entry.len(), Ordering::SeqCst);
            return false;
        }
        // Safety: we have exclusive access to this range via atomic offset reservation.
        // Each call gets a unique non-overlapping range, and the Vec is pre-allocated.
        unsafe {
            let ptr = self.data.as_ptr().add(offset) as *mut u8;
            std::ptr::copy_nonoverlapping(entry.as_ptr(), ptr, entry.len());
        }
        self.index.insert((ledger_id, entry_id), (offset, entry.len()));
        true
    }

    fn get(&self, ledger_id: i64, entry_id: i64) -> Option<Vec<u8>> {
        self.index.get(&(ledger_id, entry_id)).map(|e| {
            let (offset, size) = *e.value();
            self.data[offset..offset + size].to_vec()
        })
    }

    fn clear(&self) {
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
        let idx = self.current_idx.load(Ordering::SeqCst);
        if !self.segments[idx].put(ledger_id, entry_id, entry) {
            // Current segment full — rotate
            let next = (idx + 1) % self.num_segments;
            self.segments[next].clear();
            self.current_idx.store(next, Ordering::SeqCst);
            self.segments[next].put(ledger_id, entry_id, entry);
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
