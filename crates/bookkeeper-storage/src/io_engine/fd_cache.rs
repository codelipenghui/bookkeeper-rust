//! LRU file-descriptor cache for entry log files.

use std::collections::HashMap;
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
    free_indices: Vec<u32>, // recycled fixed-file indices
}

impl FdCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            capacity,
            access_counter: 0,
            free_indices: (0..capacity as u32).rev().collect(),
        }
    }

    /// Get the fd and fixed-file index for `log_id`, opening it if needed.
    pub fn get_or_open(&mut self, entry_log_dir: &Path, log_id: u32) -> io::Result<(RawFd, u32)> {
        self.access_counter += 1;
        let counter = self.access_counter;

        if let Some(entry) = self.entries.get_mut(&log_id) {
            entry.last_used = counter;
            return Ok((entry.file.as_raw_fd(), entry.fixed_index));
        }

        // Cache miss — evict LRU if full
        if self.entries.len() >= self.capacity {
            self.evict_lru();
        }

        let path = entry_log_dir.join(format!("{:x}.log", log_id));
        let file = File::open(&path)?;
        let fd = file.as_raw_fd();
        let fixed_index = self.free_indices.pop().unwrap_or(0);

        self.entries.insert(
            log_id,
            CacheEntry {
                file,
                fixed_index,
                last_used: counter,
            },
        );

        Ok((fd, fixed_index))
    }

    /// Explicitly evict a log_id (called by GC before file deletion).
    pub fn evict(&mut self, log_id: u32) {
        if let Some(entry) = self.entries.remove(&log_id) {
            self.free_indices.push(entry.fixed_index);
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn contains(&self, log_id: u32) -> bool {
        self.entries.contains_key(&log_id)
    }

    fn evict_lru(&mut self) {
        if let Some((&lru_id, _)) = self.entries.iter().min_by_key(|(_, e)| e.last_used) {
            if let Some(entry) = self.entries.remove(&lru_id) {
                self.free_indices.push(entry.fixed_index);
            }
        }
    }
}
