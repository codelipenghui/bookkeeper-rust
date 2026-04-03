use bookkeeper_storage::index::entry_location::EntryLocationIndex;
use bookkeeper_storage::index::ledger_index::LedgerMetadataIndex;
use tempfile::TempDir;

#[test]
fn entry_location_put_get() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path()).unwrap();
    index.put(1, 0, 0x0000000A_00001000).unwrap(); // logId=10, pos=4096
    let loc = index.get(1, 0).unwrap();
    assert_eq!(loc, Some(0x0000000A_00001000));
}

#[test]
fn entry_location_missing() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path()).unwrap();
    assert_eq!(index.get(999, 0).unwrap(), None);
}

#[test]
fn entry_location_batch_put() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path()).unwrap();
    let batch: Vec<(i64, i64, u64)> = vec![
        (1, 0, 100), (1, 1, 200), (1, 2, 300), (2, 0, 400),
    ];
    index.put_batch(&batch).unwrap();
    assert_eq!(index.get(1, 1).unwrap(), Some(200));
    assert_eq!(index.get(2, 0).unwrap(), Some(400));
}

#[test]
fn entry_location_delete_ledger() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path()).unwrap();
    index.put(1, 0, 100).unwrap();
    index.put(1, 1, 200).unwrap();
    index.put(2, 0, 300).unwrap();
    index.delete_ledger(1).unwrap();
    assert_eq!(index.get(1, 0).unwrap(), None);
    assert_eq!(index.get(1, 1).unwrap(), None);
    assert_eq!(index.get(2, 0).unwrap(), Some(300));
}

#[test]
fn ledger_index_put_get() {
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path()).unwrap();
    index.set(1, false, Some(b"masterkey"), Some(42)).unwrap();
    let data = index.get(1).unwrap().unwrap();
    assert!(!data.fenced);
    assert_eq!(data.master_key, Some(b"masterkey".to_vec()));
    assert_eq!(data.lac, Some(42));
}

#[test]
fn ledger_index_fence() {
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path()).unwrap();
    index.set(1, false, Some(b"key"), None).unwrap();
    index.set_fenced(1).unwrap();
    let data = index.get(1).unwrap().unwrap();
    assert!(data.fenced);
}

#[test]
fn ledger_index_update_lac() {
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path()).unwrap();
    index.set(1, false, Some(b"key"), Some(10)).unwrap();
    index.update_lac(1, 50).unwrap();
    let data = index.get(1).unwrap().unwrap();
    assert_eq!(data.lac, Some(50));
}

#[test]
fn ledger_index_delete() {
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path()).unwrap();
    index.set(1, false, None, None).unwrap();
    index.delete(1).unwrap();
    assert!(index.get(1).unwrap().is_none());
}
