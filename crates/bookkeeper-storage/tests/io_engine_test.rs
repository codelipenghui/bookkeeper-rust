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

#[test]
fn std_ledger_write_batch_returns_correct_offsets() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let fd = tmp.as_raw_fd();

    let engine = StdEngine;
    let entry_a = b"aaaa"; // 4 bytes
    let entry_b = b"bbbbbb"; // 6 bytes

    let offsets = engine
        .write_batch(fd, 0, &[entry_a.as_slice(), entry_b.as_slice()])
        .unwrap();

    assert_eq!(offsets, vec![0, 4]);

    // Verify file size matches total written.
    let meta = tmp.as_file().metadata().unwrap();
    assert_eq!(meta.len(), 10);
}

#[test]
fn std_ledger_read_at_returns_exact_bytes() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let fd = tmp.as_raw_fd();

    let engine = StdEngine;

    // Write known data: "AABBBBCC"
    let data_a = b"AA";
    let data_b = b"BBBB";
    let data_c = b"CC";
    engine
        .write_batch(fd, 0, &[data_a.as_slice(), data_b.as_slice(), data_c.as_slice()])
        .unwrap();

    // Read the middle segment "BBBB" at offset 2, length 4.
    let buf = engine.read_at(fd, 2, 4).unwrap();
    assert_eq!(&buf, b"BBBB");

    // Read from the start.
    let buf = engine.read_at(fd, 0, 2).unwrap();
    assert_eq!(&buf, b"AA");

    // Read from the end.
    let buf = engine.read_at(fd, 6, 2).unwrap();
    assert_eq!(&buf, b"CC");
}

#[test]
fn std_ledger_write_batch_at_nonzero_offset() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let fd = tmp.as_raw_fd();

    let engine = StdEngine;

    // Write initial data at offset 0.
    engine.write_batch(fd, 0, &[b"HEAD".as_slice()]).unwrap();

    // Append more data starting at offset 4.
    let offsets = engine
        .write_batch(fd, 4, &[b"AB".as_slice(), b"CD".as_slice()])
        .unwrap();

    assert_eq!(offsets, vec![4, 6]);

    // Verify full contents.
    let all = engine.read_at(fd, 0, 8).unwrap();
    assert_eq!(&all, b"HEADABCD");
}
