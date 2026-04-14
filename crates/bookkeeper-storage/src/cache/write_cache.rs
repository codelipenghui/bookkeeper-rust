use dashmap::DashMap;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::StorageError;

/// In-memory write cache with 64-byte aligned entries.
pub struct WriteCache {
    max_size: usize,
    data: UnsafeCell<Vec<u8>>,
    offset: AtomicUsize,
    index: DashMap<(i64, i64), (usize, usize)>, // (ledgerId, entryId) → (offset, size)
    last_entry: DashMap<i64, i64>, // ledgerId → max entryId
}

// SAFETY: Concurrent writes go to non-overlapping regions via atomic offset.
// Concurrent reads only access regions that were fully written (offset was committed to index).
unsafe impl Send for WriteCache {}
unsafe impl Sync for WriteCache {}

const ALIGNMENT: usize = 64;

fn align64(size: usize) -> usize {
    (size + ALIGNMENT - 1) & !(ALIGNMENT - 1)
}

impl WriteCache {
    pub fn new(max_size: usize) -> Self {
        let aligned_max = align64(max_size);
        Self {
            max_size: aligned_max,
            data: UnsafeCell::new(vec![0u8; aligned_max]),
            offset: AtomicUsize::new(0),
            index: DashMap::new(),
            last_entry: DashMap::new(),
        }
    }

    pub fn put(&self, ledger_id: i64, entry_id: i64, entry: &[u8]) -> Result<(), StorageError> {
        let aligned_size = align64(entry.len());
        if aligned_size > self.max_size {
            return Err(StorageError::StorageFull);
        }
        // Reserve space atomically via CAS loop. This avoids the
        // fetch_add/fetch_sub race where two concurrent overflows
        // could corrupt the offset counter.
        let offset = loop {
            let current = self.offset.load(Ordering::SeqCst);
            if current + aligned_size > self.max_size {
                return Err(StorageError::StorageFull);
            }
            if self.offset.compare_exchange(
                current,
                current + aligned_size,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ).is_ok() {
                break current;
            }
            // CAS failed — another thread won, retry
        };
        unsafe {
            let data = &mut *self.data.get();
            data[offset..offset + entry.len()].copy_from_slice(entry);
        }
        self.index.insert((ledger_id, entry_id), (offset, entry.len()));
        self.last_entry.entry(ledger_id)
            .and_modify(|v| { if entry_id > *v { *v = entry_id; } })
            .or_insert(entry_id);
        Ok(())
    }

    pub fn get(&self, ledger_id: i64, entry_id: i64) -> Option<Vec<u8>> {
        self.index.get(&(ledger_id, entry_id)).map(|e| {
            let (offset, size) = *e.value();
            unsafe {
                let data = &*self.data.get();
                data[offset..offset + size].to_vec()
            }
        })
    }

    pub fn last_entry_id(&self, ledger_id: i64) -> Option<i64> {
        self.last_entry.get(&ledger_id).map(|v| *v.value())
    }

    pub fn count(&self) -> usize {
        self.index.len()
    }

    pub fn clear(&self) {
        self.index.clear();
        self.last_entry.clear();
        self.offset.store(0, Ordering::SeqCst);
    }

    /// Return all entries sorted by (ledgerId, entryId) for flushing.
    /// Note: this collects from DashMap into a Vec then sorts, causing a
    /// transient memory spike proportional to entry count. This is inherent
    /// to DashMap's iterator model and is the same cost Java's ConcurrentMap
    /// pays during flush. The spike is short-lived (freed after flush).
    pub fn sorted_entries(&self) -> Vec<(i64, i64, usize, usize)> {
        let mut entries: Vec<(i64, i64, usize, usize)> = self.index.iter().map(|e| {
            let (ledger_id, entry_id) = *e.key();
            let (offset, size) = *e.value();
            (ledger_id, entry_id, offset, size)
        }).collect();
        entries.sort_by_key(|(lid, eid, _, _)| (*lid, *eid));
        entries
    }

    /// Access the underlying data buffer.
    pub fn data(&self) -> &[u8] {
        unsafe { &*self.data.get() }
    }
}
