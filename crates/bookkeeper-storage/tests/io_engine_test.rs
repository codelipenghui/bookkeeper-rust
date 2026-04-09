use std::io::{Read, Seek, SeekFrom};
use std::os::fd::AsRawFd;

use bookkeeper_storage::io_engine::std_engine::StdEngine;
use bookkeeper_storage::io_engine::{JournalIo, LedgerIo};

#[test]
fn std_journal_write_and_sync_persists_entries() {
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    let fd = tmp.as_raw_fd();

    let engine = StdEngine;
    let entry_a = b"hello";
    let entry_b = b"world";
    engine
        .write_and_sync(fd, &[entry_a.as_slice(), entry_b.as_slice()])
        .unwrap();

    // Read back the entire file and verify contents.
    tmp.seek(SeekFrom::Start(0)).unwrap();
    let mut contents = Vec::new();
    tmp.read_to_end(&mut contents).unwrap();

    assert_eq!(contents.len(), entry_a.len() + entry_b.len());
    assert_eq!(&contents[..entry_a.len()], entry_a);
    assert_eq!(&contents[entry_a.len()..], entry_b);
}

#[tokio::test]
async fn std_ledger_write_batch_returns_correct_offsets() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let fd = tmp.as_raw_fd();

    let engine = StdEngine;
    let entry_a = b"aaaa"; // 4 bytes
    let entry_b = b"bbbbbb"; // 6 bytes

    let offsets = engine
        .write_batch(fd, 0, vec![entry_a.to_vec(), entry_b.to_vec()])
        .await
        .unwrap();

    assert_eq!(offsets, vec![0, 4]);

    // Verify file size matches total written.
    let meta = tmp.as_file().metadata().unwrap();
    assert_eq!(meta.len(), 10);
}

#[tokio::test]
async fn std_ledger_read_at_returns_exact_bytes() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let fd = tmp.as_raw_fd();

    let engine = StdEngine;

    // Write known data: "AABBBBCC"
    let data_a = b"AA";
    let data_b = b"BBBB";
    let data_c = b"CC";
    engine
        .write_batch(fd, 0, vec![data_a.to_vec(), data_b.to_vec(), data_c.to_vec()])
        .await
        .unwrap();

    // Read the middle segment "BBBB" at offset 2, length 4.
    let buf = engine.read_at(fd, 2, 4).await.unwrap();
    assert_eq!(&buf, b"BBBB");

    // Read from the start.
    let buf = engine.read_at(fd, 0, 2).await.unwrap();
    assert_eq!(&buf, b"AA");

    // Read from the end.
    let buf = engine.read_at(fd, 6, 2).await.unwrap();
    assert_eq!(&buf, b"CC");
}

#[tokio::test]
async fn std_ledger_write_batch_at_nonzero_offset() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let fd = tmp.as_raw_fd();

    let engine = StdEngine;

    // Write initial data at offset 0.
    engine.write_batch(fd, 0, vec![b"HEAD".to_vec()]).await.unwrap();

    // Append more data starting at offset 4.
    let offsets = engine
        .write_batch(fd, 4, vec![b"AB".to_vec(), b"CD".to_vec()])
        .await
        .unwrap();

    assert_eq!(offsets, vec![4, 6]);

    // Verify full contents.
    let all = engine.read_at(fd, 0, 8).await.unwrap();
    assert_eq!(&all, b"HEADABCD");
}

// ---------------------------------------------------------------------------
// fd_cache tests — platform independent
// ---------------------------------------------------------------------------

mod fd_cache_tests {
    use bookkeeper_storage::io_engine::fd_cache::FdCache;
    use tempfile::TempDir;

    #[test]
    fn open_and_reuse_returns_same_fd() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("0.log"), vec![0u8; 64]).unwrap();

        let mut cache = FdCache::new(4);
        let (fd1, idx1) = cache.get_or_open(dir.path(), 0).unwrap();
        let (fd2, idx2) = cache.get_or_open(dir.path(), 0).unwrap();
        assert_eq!(fd1, fd2);
        assert_eq!(idx1, idx2);
    }

    #[test]
    fn different_log_ids_get_different_fds() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("0.log"), vec![0u8; 64]).unwrap();
        std::fs::write(dir.path().join("1.log"), vec![0u8; 64]).unwrap();

        let mut cache = FdCache::new(4);
        let (fd1, _) = cache.get_or_open(dir.path(), 0).unwrap();
        let (fd2, _) = cache.get_or_open(dir.path(), 1).unwrap();
        assert_ne!(fd1, fd2);
    }

    #[test]
    fn evicts_lru_when_capacity_exceeded() {
        let dir = TempDir::new().unwrap();
        for i in 0..5u32 {
            std::fs::write(dir.path().join(format!("{:x}.log", i)), vec![0u8; 64]).unwrap();
        }

        let mut cache = FdCache::new(3);
        cache.get_or_open(dir.path(), 0).unwrap();
        cache.get_or_open(dir.path(), 1).unwrap();
        cache.get_or_open(dir.path(), 2).unwrap();
        assert_eq!(cache.len(), 3);

        // Opening log_id=3 evicts LRU (log_id=0)
        cache.get_or_open(dir.path(), 3).unwrap();
        assert_eq!(cache.len(), 3);
        assert!(!cache.contains(0));
        assert!(cache.contains(3));
    }

    #[test]
    fn explicit_evict_removes_entry() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("0.log"), vec![0u8; 64]).unwrap();

        let mut cache = FdCache::new(4);
        cache.get_or_open(dir.path(), 0).unwrap();
        assert!(cache.contains(0));
        cache.evict(0);
        assert!(!cache.contains(0));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn lru_tracks_access_order() {
        let dir = TempDir::new().unwrap();
        for i in 0..4u32 {
            std::fs::write(dir.path().join(format!("{:x}.log", i)), vec![0u8; 64]).unwrap();
        }

        let mut cache = FdCache::new(3);
        cache.get_or_open(dir.path(), 0).unwrap(); // access order: 0
        cache.get_or_open(dir.path(), 1).unwrap(); // access order: 0, 1
        cache.get_or_open(dir.path(), 2).unwrap(); // access order: 0, 1, 2

        // Touch 0 again — makes it recently used
        cache.get_or_open(dir.path(), 0).unwrap(); // access order: 1, 2, 0

        // Opening 3 should evict 1 (oldest), not 0
        cache.get_or_open(dir.path(), 3).unwrap();
        assert!(
            cache.contains(0),
            "0 was recently accessed, should not be evicted"
        );
        assert!(!cache.contains(1), "1 was LRU, should be evicted");
        assert!(cache.contains(2));
        assert!(cache.contains(3));
    }
}

// ---------------------------------------------------------------------------
// io_uring tests — Linux only
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod uring_tests {
    use bookkeeper_storage::io_engine::uring_journal::UringJournalIo;
    use bookkeeper_storage::io_engine::uring_ledger::UringLedgerIo;
    use bookkeeper_storage::io_engine::{JournalIo, LedgerIo};
    use std::os::fd::AsRawFd;
    use tempfile::NamedTempFile;

    #[test]
    fn uring_journal_write_and_sync() {
        let j = UringJournalIo::new().unwrap();
        let f = NamedTempFile::new().unwrap();
        j.write_and_sync(f.as_raw_fd(), &[b"hello", b"world"])
            .unwrap();
        let data = std::fs::read(f.path()).unwrap();
        assert_eq!(&data, b"helloworld");
    }

    #[test]
    fn uring_journal_empty_entries_is_noop() {
        let j = UringJournalIo::new().unwrap();
        let f = NamedTempFile::new().unwrap();
        j.write_and_sync(f.as_raw_fd(), &[]).unwrap();
        let data = std::fs::read(f.path()).unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn uring_ledger_write_and_read_round_trip() {
        let l = UringLedgerIo::new().unwrap();
        let f = NamedTempFile::new().unwrap();
        // Pre-fill 1024-byte header so writes start at a non-zero offset.
        std::fs::write(f.path(), vec![0u8; 1024]).unwrap();

        let offsets = l
            .write_batch(f.as_raw_fd(), 1024, &[b"aaa", b"bbbbb"])
            .unwrap();
        assert_eq!(offsets, vec![1024, 1027]);

        let data = l.read_at(f.as_raw_fd(), 1024, 3).unwrap();
        assert_eq!(&data, b"aaa");

        let data = l.read_at(f.as_raw_fd(), 1027, 5).unwrap();
        assert_eq!(&data, b"bbbbb");
    }

    #[test]
    fn uring_ledger_write_batch_empty() {
        let l = UringLedgerIo::new().unwrap();
        let f = NamedTempFile::new().unwrap();
        let offsets = l.write_batch(f.as_raw_fd(), 0, &[]).unwrap();
        assert!(offsets.is_empty());
    }

    #[test]
    fn uring_ledger_concurrent_reads_return_correct_data() {
        use std::sync::Arc;

        let dir = tempfile::TempDir::new().unwrap();
        // Build a file: 1024-byte header followed by 10 entries of 10 bytes each.
        let mut data = vec![0u8; 1024]; // header
        for i in 0u32..10 {
            let entry = format!("entry-{:04}", i);
            data.extend_from_slice(entry.as_bytes());
        }
        std::fs::write(dir.path().join("0.log"), &data).unwrap();

        let engine = Arc::new(UringLedgerIo::new().unwrap());
        let mut handles = Vec::new();

        for i in 0u32..10 {
            let engine = engine.clone();
            let dir_path = dir.path().to_path_buf();
            let offset = 1024 + (i as u64 * 10);
            handles.push(std::thread::spawn(move || {
                let data = engine.read_entry(&dir_path, 0, offset, 10).unwrap();
                assert_eq!(
                    String::from_utf8(data).unwrap(),
                    format!("entry-{:04}", i)
                );
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}
