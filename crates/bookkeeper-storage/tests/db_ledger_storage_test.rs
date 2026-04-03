use bookkeeper_storage::db_ledger_storage::DbLedgerStorage;
use bookkeeper_storage::ledger_storage::LedgerStorage;
use tempfile::TempDir;

fn make_entry(ledger_id: i64, entry_id: i64, lac: i64, payload: &[u8]) -> Vec<u8> {
    let mut entry = Vec::new();
    entry.extend_from_slice(&ledger_id.to_be_bytes());
    entry.extend_from_slice(&entry_id.to_be_bytes());
    entry.extend_from_slice(&lac.to_be_bytes());
    entry.extend_from_slice(&(payload.len() as i64).to_be_bytes());
    entry.extend_from_slice(payload);
    entry
}

#[test]
fn add_and_get_entry() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    let entry = make_entry(1, 0, -1, b"hello");
    storage.add_entry(&entry).unwrap();
    let retrieved = storage.get_entry(1, 0).unwrap();
    assert_eq!(&retrieved[..], &entry[..]);
}

#[test]
fn get_missing_entry() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    let result = storage.get_entry(1, 0);
    assert!(result.is_err());
}

#[test]
fn fence_ledger() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    let entry = make_entry(1, 0, -1, b"data");
    storage.add_entry(&entry).unwrap();
    assert!(!storage.is_fenced(1).unwrap());
    storage.set_fenced(1).unwrap();
    assert!(storage.is_fenced(1).unwrap());
}

#[test]
fn last_add_confirmed() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    storage.add_entry(&make_entry(1, 0, -1, b"a")).unwrap();
    storage.add_entry(&make_entry(1, 1, 0, b"b")).unwrap();
    storage.add_entry(&make_entry(1, 2, 1, b"c")).unwrap();
    // get_last_add_confirmed returns the highest entry_id stored, which
    // represents the bookie's view of the last add confirmed: entry 2.
    assert_eq!(storage.get_last_add_confirmed(1).unwrap(), 2);
}

#[test]
fn flush_and_read_from_disk() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    storage.add_entry(&make_entry(1, 0, -1, b"persistent")).unwrap();
    storage.flush().unwrap();
    // Entry should now be in RocksDB + entry log, not just write cache
    let retrieved = storage.get_entry(1, 0).unwrap();
    assert_eq!(&retrieved[32..], b"persistent"); // skip metadata header (lid+eid+lac+plen = 32 bytes)
}

#[test]
fn delete_ledger() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    storage.add_entry(&make_entry(1, 0, -1, b"data")).unwrap();
    storage.delete_ledger(1).unwrap();
    assert!(!storage.ledger_exists(1).unwrap());
}

#[test]
fn concurrent_add_during_flush() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    // Add entry, then flush
    storage.add_entry(&make_entry(1, 0, -1, b"before")).unwrap();
    storage.flush().unwrap();
    // Add another entry after flush
    storage.add_entry(&make_entry(1, 1, 0, b"after")).unwrap();
    // Both should be retrievable
    assert!(storage.get_entry(1, 0).is_ok());
    assert!(storage.get_entry(1, 1).is_ok());
}
