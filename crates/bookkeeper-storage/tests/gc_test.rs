//! Tests for GarbageCollector — entry log scanning, dead ledger detection,
//! and entry log file deletion.

use std::collections::HashSet;
use std::sync::Arc;

use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_protocol::entry::EntryBuilder;
use bookkeeper_storage::db_ledger_storage::DbLedgerStorage;
use bookkeeper_storage::gc::GarbageCollector;
use bookkeeper_storage::io_engine::std_engine::StdEngine;
use bookkeeper_storage::ledger_storage::LedgerStorage;
use tempfile::TempDir;

fn make_entry(ledger_id: i64, entry_id: i64) -> bytes::Bytes {
    let dm = DigestManager::new(DigestType::Crc32c);
    EntryBuilder::new(&dm)
        .ledger_id(ledger_id)
        .entry_id(entry_id)
        .lac(entry_id - 1)
        .length(8)
        .payload(bytes::Bytes::from_static(b"test-pay"))
        .build()
        .freeze()
}

async fn write_entries(storage: &dyn LedgerStorage, ledger_id: i64, count: i64) {
    // validate_for_write creates the ledger entry on first call
    storage.validate_for_write(ledger_id, b"key").unwrap();
    for eid in 0..count {
        let entry = make_entry(ledger_id, eid);
        storage.add_entry(&entry).await.unwrap();
    }
    storage.flush().await.unwrap();
}

#[tokio::test]
async fn gc_deletes_local_ledger_not_in_metadata() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(DbLedgerStorage::open(dir.path(), Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>).unwrap()) as Arc<dyn LedgerStorage>;

    write_entries(&*storage, 1, 5).await;
    write_entries(&*storage, 2, 5).await;
    write_entries(&*storage, 3, 5).await;

    // Initially all 3 ledgers exist locally
    let local = storage.get_active_ledgers().unwrap();
    assert_eq!(local.len(), 3);

    // Active set says only ledger 2 still exists in metadata
    let mut active = HashSet::new();
    active.insert(2i64);

    let gc = GarbageCollector::new(storage.clone(), Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>);
    gc.gc(&active, false).await.unwrap();

    // After GC, ledgers 1 and 3 should be gone locally
    let local = storage.get_active_ledgers().unwrap();
    assert_eq!(local.len(), 1, "expected only ledger 2 to remain, got {local:?}");
    assert_eq!(local[0], 2);
}

#[tokio::test]
async fn gc_keeps_all_when_metadata_matches_local() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(DbLedgerStorage::open(dir.path(), Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>).unwrap()) as Arc<dyn LedgerStorage>;

    write_entries(&*storage, 10, 3).await;
    write_entries(&*storage, 20, 3).await;

    let mut active = HashSet::new();
    active.insert(10i64);
    active.insert(20i64);

    let gc = GarbageCollector::new(storage.clone(), Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>);
    gc.gc(&active, false).await.unwrap();

    let local = storage.get_active_ledgers().unwrap();
    assert_eq!(local.len(), 2);
}

#[tokio::test]
async fn gc_with_empty_metadata_deletes_all_local() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(DbLedgerStorage::open(dir.path(), Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>).unwrap()) as Arc<dyn LedgerStorage>;

    write_entries(&*storage, 100, 5).await;
    write_entries(&*storage, 200, 5).await;

    let active: HashSet<i64> = HashSet::new();
    let gc = GarbageCollector::new(storage.clone(), Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>);
    gc.gc(&active, false).await.unwrap();

    let local = storage.get_active_ledgers().unwrap();
    assert!(local.is_empty(), "expected all ledgers deleted, got {local:?}");
}

#[tokio::test]
async fn gc_idempotent_when_run_twice() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(DbLedgerStorage::open(dir.path(), Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>).unwrap()) as Arc<dyn LedgerStorage>;

    write_entries(&*storage, 1, 3).await;
    write_entries(&*storage, 2, 3).await;

    let mut active = HashSet::new();
    active.insert(1i64);

    let gc = GarbageCollector::new(storage.clone(), Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>);
    gc.gc(&active, false).await.unwrap();
    gc.gc(&active, false).await.unwrap(); // second run should be no-op

    let local = storage.get_active_ledgers().unwrap();
    assert_eq!(local.len(), 1);
}

#[tokio::test]
async fn gc_with_no_local_ledgers_succeeds() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(DbLedgerStorage::open(dir.path(), Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>).unwrap()) as Arc<dyn LedgerStorage>;

    let active: HashSet<i64> = HashSet::new();
    let gc = GarbageCollector::new(storage, Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>);
    gc.gc(&active, false).await.unwrap();
}
