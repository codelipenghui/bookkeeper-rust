// Standard blocking I/O engine backed by std::fs.
//
// Full implementation coming in Task 3.

use std::io;
use std::os::fd::RawFd;

use super::{IoEngine, JournalIo, LedgerIo};

/// Blocking I/O engine that uses standard `std::fs` / `std::io` operations.
pub struct StdEngine;

impl IoEngine for StdEngine {
    fn journal(&self) -> &dyn JournalIo {
        self
    }

    fn ledger(&self) -> &dyn LedgerIo {
        self
    }
}

impl JournalIo for StdEngine {
    fn write_and_sync(&self, _fd: RawFd, _entries: &[&[u8]]) -> io::Result<()> {
        todo!("Task 3")
    }
}

impl LedgerIo for StdEngine {
    fn write_batch(&self, _fd: RawFd, _offset: u64, _entries: &[&[u8]]) -> io::Result<Vec<u64>> {
        todo!("Task 3")
    }

    fn read_at(&self, _fd: RawFd, _offset: u64, _len: usize) -> io::Result<Vec<u8>> {
        todo!("Task 3")
    }
}
