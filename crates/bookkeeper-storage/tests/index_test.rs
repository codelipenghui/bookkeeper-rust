use bookkeeper_storage::index::entry_location::EntryLocationIndex;
use bookkeeper_storage::index::ledger_index::LedgerMetadataIndex;
use tempfile::TempDir;

#[test]
fn entry_location_put_get() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path(), None).unwrap();
    index.put(1, 0, 0x0000000A_00001000).unwrap(); // logId=10, pos=4096
    let loc = index.get(1, 0).unwrap();
    assert_eq!(loc, Some(0x0000000A_00001000));
}

#[test]
fn entry_location_missing() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path(), None).unwrap();
    assert_eq!(index.get(999, 0).unwrap(), None);
}

#[test]
fn entry_location_batch_put() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path(), None).unwrap();
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
    let index = EntryLocationIndex::open(dir.path(), None).unwrap();
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
    let index = LedgerMetadataIndex::open(dir.path(), None).unwrap();
    index.set(1, false, Some(b"masterkey")).unwrap();
    let data = index.get(1).unwrap().unwrap();
    assert!(!data.fenced);
    assert_eq!(data.master_key, Some(b"masterkey".to_vec()));
}

#[test]
fn ledger_index_fence() {
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path(), None).unwrap();
    index.set(1, false, Some(b"key")).unwrap();
    index.set_fenced(1).unwrap();
    let data = index.get(1).unwrap().unwrap();
    assert!(data.fenced);
}

#[test]
fn ledger_index_delete() {
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path(), None).unwrap();
    index.set(1, false, None).unwrap();
    index.delete(1).unwrap();
    assert!(index.get(1).unwrap().is_none());
}

// ---------------------------------------------------------------------------
// ST2: getLastEntryInLedger floor scan
// ---------------------------------------------------------------------------

#[test]
fn get_last_entry_in_ledger_returns_max_entry() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path(), None).unwrap();
    // Insert entries 0, 5, 10 for ledger 1
    index.put(1, 0, 100).unwrap();
    index.put(1, 5, 200).unwrap();
    index.put(1, 10, 300).unwrap();
    index.flush().unwrap();

    let last = index.get_last_entry_in_ledger(1).unwrap();
    assert_eq!(last, Some(10), "should return the highest entry ID");
}

#[test]
fn get_last_entry_in_ledger_empty_returns_none() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path(), None).unwrap();
    let last = index.get_last_entry_in_ledger(999).unwrap();
    assert_eq!(last, None, "should return None for non-existent ledger");
}

#[test]
fn get_last_entry_in_ledger_does_not_cross_ledger_boundary() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path(), None).unwrap();
    // Ledger 1 has entries 0, 1
    index.put(1, 0, 100).unwrap();
    index.put(1, 1, 200).unwrap();
    // Ledger 2 has entry 0
    index.put(2, 0, 300).unwrap();
    index.flush().unwrap();

    let last1 = index.get_last_entry_in_ledger(1).unwrap();
    assert_eq!(last1, Some(1), "ledger 1 last entry should be 1");

    let last2 = index.get_last_entry_in_ledger(2).unwrap();
    assert_eq!(last2, Some(0), "ledger 2 last entry should be 0");

    // Ledger 3 has no entries
    let last3 = index.get_last_entry_in_ledger(3).unwrap();
    assert_eq!(last3, None);
}

// ---------------------------------------------------------------------------
// Data-integrity primitives (storage state flags + limbo state)
// ---------------------------------------------------------------------------

#[test]
fn storage_state_flags_round_trip() {
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path(), None).unwrap();
    assert_eq!(index.get_storage_state_flags().unwrap(), 0);
    assert!(index.set_storage_state_flags(0, 0x1).unwrap());
    assert_eq!(index.get_storage_state_flags().unwrap(), 0x1);
    // CAS with stale expected must fail
    assert!(!index.set_storage_state_flags(0, 0x2).unwrap());
    assert_eq!(index.get_storage_state_flags().unwrap(), 0x1);
}

#[test]
fn storage_flags_key_is_not_a_ledger() {
    // STORAGE_FLAGS_KEY must not appear in list_all_ledgers.
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path(), None).unwrap();
    assert!(index.set_storage_state_flags(0, 0x1).unwrap());
    index.set(42, false, Some(b"pw")).unwrap();
    let ledgers = index.list_all_ledgers().unwrap();
    assert_eq!(ledgers, vec![42]);
}

#[test]
fn limbo_state_round_trip() {
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path(), None).unwrap();
    index.set(7, false, Some(b"pw")).unwrap();
    assert!(!index.has_limbo(7).unwrap());
    index.set_limbo(7).unwrap();
    assert!(index.has_limbo(7).unwrap());
    index.clear_limbo(7).unwrap();
    assert!(!index.has_limbo(7).unwrap());
}

#[test]
fn limbo_requires_existing_ledger() {
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path(), None).unwrap();
    assert!(index.set_limbo(999).is_err());
    // clear is a no-op on absent ledgers
    assert!(index.clear_limbo(999).is_ok());
    assert!(!index.has_limbo(999).unwrap());
}

/// `delete_ledger` and `iter_ledger_entries` build their range bounds
/// as `(ledger_id + 1).to_be_bytes()`. With `ledger_id == i64::MAX`
/// the addition overflows: in debug builds it panics; in release it
/// wraps to `i64::MIN` and the ensuing byte sequence accidentally
/// happens to be a correct exclusive upper bound (because
/// `0x80...00 > 0x7F...FF` lexicographically). The debug-time panic
/// still aborts the bookie. Use a non-panicking byte computation.
#[test]
fn entry_location_delete_ledger_handles_i64_max() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path(), None).unwrap();

    index.put(i64::MAX, 0, 1234).unwrap();
    index.put(i64::MAX, 1, 5678).unwrap();
    index.put(i64::MAX - 1, 0, 9999).unwrap();

    // Pre-fix this panics in debug with "attempt to add with overflow".
    index.delete_ledger(i64::MAX).unwrap();

    // The targeted ledger is gone.
    assert!(index.get(i64::MAX, 0).unwrap().is_none());
    assert!(index.get(i64::MAX, 1).unwrap().is_none());
    // The neighbour ledger is intact.
    assert_eq!(index.get(i64::MAX - 1, 0).unwrap(), Some(9999));
}

/// Same overflow check for `iter_ledger_entries`.
#[test]
fn entry_location_iter_ledger_entries_handles_i64_max() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path(), None).unwrap();

    index.put(i64::MAX, 0, 1).unwrap();
    index.put(i64::MAX, 1, 2).unwrap();
    index.put(i64::MAX, 2, 3).unwrap();

    let entries = index.iter_ledger_entries(i64::MAX).unwrap();
    let mut entry_ids: Vec<i64> = entries.iter().map(|(eid, _)| *eid).collect();
    entry_ids.sort();
    assert_eq!(entry_ids, vec![0, 1, 2]);
}
