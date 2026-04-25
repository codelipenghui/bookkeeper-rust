//! LRU file-descriptor cache for entry log files.
//!
//! Holds open `File` handles keyed by `log_id`. Supports pinning to
//! prevent LRU eviction of fds that have outstanding io_uring SQEs.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;

struct CacheEntry {
    file: File,
    fixed_index: u32,
    last_used: u64,
}

pub struct FdCache {
    entries: HashMap<u32, CacheEntry>, // log_id → entry
    capacity: usize,
    access_counter: u64,
    free_indices: Vec<u32>,
    /// Log IDs with outstanding SQEs — exempt from LRU eviction.
    pinned: HashSet<u32>,
}

impl FdCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            capacity,
            access_counter: 0,
            free_indices: (0..capacity as u32).rev().collect(),
            pinned: HashSet::new(),
        }
    }

    /// Get the fd and fixed-file index for `log_id`, opening it if needed.
    ///
    /// Returns `(raw_fd, fixed_index, is_new)` where `is_new` is `true` on a
    /// cache miss (new fd opened) and `false` on a cache hit.
    pub fn get_or_open(&mut self, entry_log_dir: &Path, log_id: u32) -> io::Result<(RawFd, u32, bool)> {
        self.access_counter += 1;
        let counter = self.access_counter;

        if let Some(entry) = self.entries.get_mut(&log_id) {
            entry.last_used = counter;
            return Ok((entry.file.as_raw_fd(), entry.fixed_index, false));
        }

        // Cache miss — evict LRU if full (only non-pinned entries)
        if self.entries.len() >= self.capacity {
            self.evict_lru();
        }

        // If every entry is pinned, evict_lru is a no-op and
        // free_indices stays empty. We MUST refuse the insert in that
        // case: silently reusing slot 0 would alias the new fd onto an
        // existing log_id's fixed-file slot, and any in-flight SQE
        // referencing that slot would read from the wrong file. Surface
        // a transient-busy error so the caller can fail or retry the
        // request rather than corrupt data.
        if self.free_indices.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "fd cache exhausted (capacity={}, all entries pinned)",
                    self.capacity
                ),
            ));
        }

        let path = entry_log_dir.join(super::entry_log_filename(log_id));
        let file = File::open(&path)?;
        let fd = file.as_raw_fd();
        let fixed_index = self.free_indices
            .pop()
            .expect("free_indices non-empty (checked above)");

        self.entries.insert(
            log_id,
            CacheEntry {
                file,
                fixed_index,
                last_used: counter,
            },
        );

        Ok((fd, fixed_index, true))
    }

    /// Pin a log_id to prevent LRU eviction. Call this after building
    /// an SQE that references the fd, before submitting to io_uring.
    pub fn pin(&mut self, log_id: u32) {
        self.pinned.insert(log_id);
    }

    /// Unpin all log_ids. Call this after all CQEs for the batch have
    /// been reaped — the fds are no longer referenced by pending SQEs.
    pub fn unpin_all(&mut self) {
        self.pinned.clear();
    }

    /// Explicitly evict a log_id (called by GC before file deletion).
    ///
    /// Returns the freed `fixed_index` if the entry existed, so the caller
    /// can unregister it from the io_uring fixed file table.
    pub fn evict(&mut self, log_id: u32) -> Option<u32> {
        self.pinned.remove(&log_id);
        if let Some(entry) = self.entries.remove(&log_id) {
            let idx = entry.fixed_index;
            self.free_indices.push(idx);
            Some(idx)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn contains(&self, log_id: u32) -> bool {
        self.entries.contains_key(&log_id)
    }

    fn evict_lru(&mut self) {
        // Only evict non-pinned entries
        if let Some((&lru_id, _)) = self
            .entries
            .iter()
            .filter(|(id, _)| !self.pinned.contains(id))
            .min_by_key(|(_, e)| e.last_used)
        {
            if let Some(entry) = self.entries.remove(&lru_id) {
                self.free_indices.push(entry.fixed_index);
            }
        }
    }
}
