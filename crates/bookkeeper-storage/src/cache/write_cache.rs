use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::journal::format::StorageError;

/// In-memory write cache with 64-byte aligned entries.
pub struct WriteCache {
    max_size: usize,
    data: Vec<u8>,
    offset: AtomicUsize,
    index: DashMap<(i64, i64), (usize, usize)>, // (ledgerId, entryId) → (offset, size)
    last_entry: DashMap<i64, i64>, // ledgerId → max entryId
}

const ALIGNMENT: usize = 64;

fn align64(size: usize) -> usize {
    (size + ALIGNMENT - 1) & !(ALIGNMENT - 1)
}

impl WriteCache {
    pub fn new(max_size: usize) -> Self {
        let aligned_max = align64(max_size);
        Self {
            max_size: aligned_max,
            data: vec![0u8; aligned_max],
            offset: AtomicUsize::new(0),
            index: DashMap::new(),
            last_entry: DashMap::new(),
        }
    }

    pub fn put(&self, ledger_id: i64, entry_id: i64, entry: &[u8]) -> Result<(), StorageError> {
        let aligned_size = align64(entry.len());
        let offset = self.offset.fetch_add(aligned_size, Ordering::SeqCst);
        if offset + aligned_size > self.max_size {
            self.offset.fetch_sub(aligned_size, Ordering::SeqCst);
            return Err(StorageError::StorageFull);
        }
        // Safety: we have exclusive access to this range via atomic offset reservation.
        // Each call gets a unique non-overlapping range, and the Vec is pre-allocated.
        unsafe {
            let ptr = self.data.as_ptr().add(offset) as *mut u8;
            std::ptr::copy_nonoverlapping(entry.as_ptr(), ptr, entry.len());
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
            self.data[offset..offset + size].to_vec()
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
    pub fn sorted_entries(&self) -> Vec<(i64, i64, Vec<u8>)> {
        let mut entries: Vec<(i64, i64, Vec<u8>)> = self.index.iter().map(|e| {
            let (ledger_id, entry_id) = *e.key();
            let (offset, size) = *e.value();
            (ledger_id, entry_id, self.data[offset..offset + size].to_vec())
        }).collect();
        entries.sort_by_key(|(lid, eid, _)| (*lid, *eid));
        entries
    }
}
