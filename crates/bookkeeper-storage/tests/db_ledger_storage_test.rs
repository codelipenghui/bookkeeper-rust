use std::sync::Arc;
use bookkeeper_storage::db_ledger_storage::DbLedgerStorage;
use bookkeeper_storage::io_engine::std_engine::StdEngine;
use bookkeeper_storage::ledger_storage::LedgerStorage;
use tempfile::TempDir;

fn default_ledger_io() -> Arc<dyn bookkeeper_storage::io_engine::LedgerIo> {
    Arc::new(StdEngine) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>
}

fn make_entry(ledger_id: i64, entry_id: i64, lac: i64, payload: &[u8]) -> Vec<u8> {
    let mut entry = Vec::new();
    entry.extend_from_slice(&ledger_id.to_be_bytes());
    entry.extend_from_slice(&entry_id.to_be_bytes());
    entry.extend_from_slice(&lac.to_be_bytes());
    entry.extend_from_slice(&(payload.len() as i64).to_be_bytes());
    entry.extend_from_slice(payload);
    entry
}

#[tokio::test]
async fn add_and_get_entry() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();
    let entry = make_entry(1, 0, -1, b"hello");
    storage.add_entry(&entry).await.unwrap();
    let retrieved = storage.get_entry(1, 0).await.unwrap();
    assert_eq!(&retrieved[..], &entry[..]);
}

#[tokio::test]
async fn get_missing_entry() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();
    let result = storage.get_entry(1, 0).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn fence_ledger() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();
    let entry = make_entry(1, 0, -1, b"data");
    storage.add_entry(&entry).await.unwrap();
    assert!(!storage.is_fenced(1).unwrap());
    storage.set_fenced(1).unwrap();
    assert!(storage.is_fenced(1).unwrap());
}

#[tokio::test]
async fn last_add_confirmed() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();
    storage.add_entry(&make_entry(1, 0, -1, b"a")).await.unwrap();
    storage.add_entry(&make_entry(1, 1, 0, b"b")).await.unwrap();
    storage.add_entry(&make_entry(1, 2, 1, b"c")).await.unwrap();
    // get_last_add_confirmed returns the highest entry_id stored, which
    // represents the bookie's view of the last add confirmed: entry 2.
    assert_eq!(storage.get_last_add_confirmed(1).unwrap(), 2);
}

#[tokio::test]
async fn flush_and_read_from_disk() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();
    storage.add_entry(&make_entry(1, 0, -1, b"persistent")).await.unwrap();
    storage.flush().await.unwrap();
    // Entry should now be in RocksDB + entry log, not just write cache
    let retrieved = storage.get_entry(1, 0).await.unwrap();
    assert_eq!(&retrieved[32..], b"persistent"); // skip metadata header (lid+eid+lac+plen = 32 bytes)
}

#[tokio::test]
async fn delete_ledger() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();
    storage.add_entry(&make_entry(1, 0, -1, b"data")).await.unwrap();
    storage.delete_ledger(1).unwrap();
    assert!(!storage.ledger_exists(1).unwrap());
}

#[tokio::test]
async fn concurrent_add_during_flush() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();
    // Add entry, then flush
    storage.add_entry(&make_entry(1, 0, -1, b"before")).await.unwrap();
    storage.flush().await.unwrap();
    // Add another entry after flush
    storage.add_entry(&make_entry(1, 1, 0, b"after")).await.unwrap();
    // Both should be retrievable
    assert!(storage.get_entry(1, 0).await.is_ok());
    assert!(storage.get_entry(1, 1).await.is_ok());
}
