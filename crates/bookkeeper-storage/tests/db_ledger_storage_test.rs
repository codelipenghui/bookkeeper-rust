use std::sync::Arc;
use bookkeeper_storage::db_ledger_storage::DbLedgerStorage;
use bookkeeper_storage::io_engine::std_engine::StdEngine;
use bookkeeper_storage::io_engine::select_engine;
use bookkeeper_storage::ledger_storage::LedgerStorage;
use tempfile::TempDir;

fn default_ledger_io() -> Arc<dyn bookkeeper_storage::io_engine::LedgerIo> {
    Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>
}

/// Uses select_engine() — picks UringLedgerIo on Linux, StdEngine elsewhere.
fn platform_ledger_io() -> Arc<dyn bookkeeper_storage::io_engine::LedgerIo> {
    select_engine().ledger
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

/// Tests flush through the platform's best I/O engine (io_uring on Linux).
/// This is the code path the bench exercises that was previously untested
/// with UringLedgerIo.
#[tokio::test]
async fn flush_with_platform_engine() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), platform_ledger_io()).unwrap();
    // Add 100 entries to ensure the write cache has data to flush
    for i in 0..100i64 {
        let entry = make_entry(1, i, i.saturating_sub(1), &vec![0xABu8; 128]);
        storage.add_entry(&entry).await.unwrap();
    }
    // Flush: write cache → entry log via LedgerIo::write_batch
    storage.flush().await.unwrap();
    // Verify entries are readable from the entry log
    let retrieved = storage.get_entry(1, 50).await.unwrap();
    assert_eq!(retrieved.len(), 32 + 128); // header + payload
}

/// Regression: when the bookie restarts, the next flush must NOT overwrite
/// any existing entry log file. Previously `EntryLogState` was hard-coded to
/// `log_id = 0, offset = LOGFILE_HEADER_SIZE` on every `open()`, which meant
/// the second run would scribble new data on top of `0.log` and corrupt every
/// entry the location index pointed at. The bookie should always roll forward
/// to a fresh `<max+1>.log` on restart, matching Java's EntryLogger.
#[tokio::test]
async fn restart_does_not_overwrite_existing_entry_logs() {
    let dir = TempDir::new().unwrap();

    // First "run": add and flush an entry, then drop the storage handle.
    {
        let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();
        storage.add_entry(&make_entry(1, 0, -1, b"first-run")).await.unwrap();
        storage.flush().await.unwrap();
        // Sanity: entry should be readable from disk after flush.
        let got = storage.get_entry(1, 0).await.unwrap();
        assert_eq!(&got[32..], b"first-run");
    }

    // Second "run": reopen the same dir. The next flush must land in a NEW
    // log file, leaving the entry from run 1 intact and readable.
    {
        let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();
        storage.add_entry(&make_entry(2, 0, -1, b"second-run")).await.unwrap();
        storage.flush().await.unwrap();

        // Both entries must still be readable. If the new flush had landed
        // on top of `0.log`, get_entry(1, 0) would either fail or return
        // garbage bytes that don't match the original payload.
        let entry1 = storage.get_entry(1, 0).await.unwrap();
        assert_eq!(&entry1[32..], b"first-run");
        let entry2 = storage.get_entry(2, 0).await.unwrap();
        assert_eq!(&entry2[32..], b"second-run");
    }

    // Third "run": same check across multiple restarts.
    {
        let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();
        storage.add_entry(&make_entry(3, 0, -1, b"third-run")).await.unwrap();
        storage.flush().await.unwrap();
        assert_eq!(&storage.get_entry(1, 0).await.unwrap()[32..], b"first-run");
        assert_eq!(&storage.get_entry(2, 0).await.unwrap()[32..], b"second-run");
        assert_eq!(&storage.get_entry(3, 0).await.unwrap()[32..], b"third-run");
    }
}

/// Verifies that read-ahead populates the read cache for consecutive entries
/// without blocking the original get_entry call. After reading entry 0 from
/// disk, the background read-ahead should eventually cache entries 1..N.
/// Matches Java's `dbStorage_readAheadCacheBatchSize` behavior.
#[tokio::test]
async fn read_ahead_populates_cache() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();

    // Add 10 consecutive entries and flush to entry log so they're on disk
    for i in 0..10i64 {
        let entry = make_entry(1, i, i.saturating_sub(1), &vec![0xAAu8; 128]);
        storage.add_entry(&entry).await.unwrap();
    }
    storage.flush().await.unwrap();

    // Reading entry 0 triggers read-ahead for entries 1..N in the background
    let entry0 = storage.get_entry(1, 0).await.unwrap();
    assert_eq!(entry0.len(), 32 + 128);

    // Give the background read-ahead task time to complete
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Entries 1-9 should now be in the read cache (served without disk I/O).
    // Verify they're all readable and correct.
    for i in 1..10i64 {
        let entry = storage.get_entry(1, i).await.unwrap();
        assert_eq!(entry.len(), 32 + 128, "entry {i} should be 160 bytes");
        // Verify ledger_id and entry_id in the entry header
        let lid = i64::from_be_bytes(entry[0..8].try_into().unwrap());
        let eid = i64::from_be_bytes(entry[8..16].try_into().unwrap());
        assert_eq!(lid, 1);
        assert_eq!(eid, i);
    }
}

/// Tests add_entry overflow → implicit flush with the platform engine.
/// The bench hits this when the write cache fills during iteration.
#[tokio::test]
async fn add_entry_overflow_flush_with_platform_engine() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), platform_ledger_io()).unwrap();
    // Add enough entries to overflow the 64MB write cache and trigger flush.
    // Each entry is ~4KB payload + 32B header ≈ 4KB. 64MB / 4KB = 16384 entries.
    // Use 20000 to ensure at least one overflow flush.
    for i in 0..20000i64 {
        let entry = make_entry(1, i, i.saturating_sub(1), &vec![0xCDu8; 4000]);
        if let Err(e) = storage.add_entry(&entry).await {
            panic!("add_entry failed at entry {}: {:?}", i, e);
        }
    }
    // Verify some entries are readable
    let retrieved = storage.get_entry(1, 0).await.unwrap();
    assert_eq!(retrieved.len(), 32 + 4000);
    let retrieved = storage.get_entry(1, 19999).await.unwrap();
    assert_eq!(retrieved.len(), 32 + 4000);
}

// ---------------------------------------------------------------------------
// Local consistency check tests (I15)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn local_consistency_check_passes_on_clean_data() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();

    // Write entries
    for i in 0..10 {
        let entry = make_entry(1, i, i.saturating_sub(1), b"clean-data");
        storage.add_entry(&entry).await.unwrap();
    }

    // Flush to entry log so the consistency check can read from disk
    storage.flush().await.unwrap();

    // Consistency check should find 0 errors
    let errors = storage.local_consistency_check(0).await.unwrap();
    assert_eq!(errors, 0, "clean data should have no consistency errors");
}

#[tokio::test]
async fn local_consistency_check_with_multiple_ledgers() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();

    // Write entries across multiple ledgers
    for lid in 1..=3 {
        for eid in 0..5 {
            let entry = make_entry(lid, eid, eid.saturating_sub(1), b"multi-ledger");
            storage.add_entry(&entry).await.unwrap();
        }
    }
    storage.flush().await.unwrap();

    let errors = storage.local_consistency_check(0).await.unwrap();
    assert_eq!(errors, 0, "multiple clean ledgers should have no errors");
}
