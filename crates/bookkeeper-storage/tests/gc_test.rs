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
        .payload(b"test-pay")
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

/// GC reads `current_log_id` once at the start of a pass, but the writer
/// thread can rotate to a new log_id between that snapshot and GC's
/// directory scan. Files at any log_id at-or-above the snapshot are
/// either the active log or the result of an in-flight rotation; GC
/// must not delete them just because their metadata cache is empty.
///
/// Reproduce by writing real entries to advance `current_log_id` and
/// then simulating "future" rotations by creating empty header-only
/// `.log` files directly on disk. With the original `==` skip check,
/// GC examines those files, their (header-only) content yields no
/// ledgers, and they are deleted. With the `>=` check, all log_ids at
/// or above the snapshot are skipped — the next GC pass will examine
/// them via scan-fallback once they're settled.
#[tokio::test]
async fn gc_skips_log_ids_at_or_above_snapshot() {
    use bookkeeper_storage::db_ledger_storage::StorageConfig;
    use bookkeeper_storage::entry_log::format::{EntryLogHeader, HEADER_CURRENT_VERSION};
    use std::io::Write;

    let dir = TempDir::new().unwrap();
    let ledger_io = Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>;
    let mut storage_config = StorageConfig::default();
    // Tiny max_entry_log_size so a few entries trigger rotations and
    // current_log_id advances to a non-zero value.
    storage_config.max_entry_log_size = 4096;
    let storage = Arc::new(
        DbLedgerStorage::open_full(dir.path(), ledger_io.clone(), None, None, storage_config).unwrap()
    );
    let storage_dyn: Arc<dyn LedgerStorage> = storage.clone();

    // Write enough entries to advance current_log_id past 0 — each entry
    // is ~3 KiB so two entries trigger a rotation. After ~10 entries the
    // log_id is several files in.
    storage_dyn.validate_for_write(1, b"key").unwrap();
    let dm = DigestManager::new(DigestType::Crc32c);
    for eid in 0..10 {
        let entry = EntryBuilder::new(&dm)
            .ledger_id(1)
            .entry_id(eid)
            .lac(eid - 1)
            .length(3000)
            .payload(&vec![b'x'; 3000])
            .build()
            .freeze();
        storage_dyn.add_entry(&entry).await.unwrap();
    }
    storage_dyn.flush().await.unwrap();

    let current = storage_dyn
        .current_log_id()
        .expect("storage should report a current log id after writes");
    assert!(
        current > 0,
        "test setup expected current_log_id > 0 to differentiate from \
         the 'future' ids; got {current}"
    );

    // Simulate rotations that happened after GC's snapshot of
    // current_log_id by creating empty-header log files at log_ids
    // strictly above `current`.
    let entry_log_dir = dir.path().join("current");
    let header_bytes = EntryLogHeader::new(HEADER_CURRENT_VERSION).to_bytes();
    let future_ids = [current + 1, current + 2, current + 5];
    for &id in &future_ids {
        let path = entry_log_dir.join(format!("{:x}.log", id));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&header_bytes).unwrap();
        f.sync_data().unwrap();
    }

    // Run GC with the *real* ledger as active so the legitimate files
    // (those known to current_log_id) stay; the question is whether GC
    // deletes the future-id files due to the stale snapshot.
    let mut active = HashSet::new();
    active.insert(1i64);
    let gc = GarbageCollector::new(storage_dyn.clone(), ledger_io.clone());
    gc.gc(&active, false).await.unwrap();

    for &id in &future_ids {
        let path = entry_log_dir.join(format!("{:x}.log", id));
        assert!(
            path.exists(),
            "GC deleted file {id:x}.log whose log_id is above the GC's \
             current_log_id snapshot ({current:x}); this is the \
             rotation-race that loses live data",
        );
    }
}
