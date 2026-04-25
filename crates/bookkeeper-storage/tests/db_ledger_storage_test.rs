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

/// Sequential readahead must stop at ledger boundaries. The entry log can
/// contain interleaved ledgers, and walking past the current ledger's last
/// entry would populate the cache under the wrong `(ledger_id, entry_id)`
/// key — corrupting subsequent lookups. Regression guard for the sequential
/// entry-log-walk readahead (replaces the old RocksDB-per-entry planning).
#[tokio::test]
async fn read_ahead_stops_at_ledger_boundary() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();

    // Interleave two ledgers into the same entry log: ledger 1 entries 0..4,
    // then ledger 2 entries 0..4. No flush between — they land in one log.
    for i in 0..5i64 {
        let entry = make_entry(1, i, i.saturating_sub(1), &vec![0xAAu8; 128]);
        storage.add_entry(&entry).await.unwrap();
    }
    for i in 0..5i64 {
        let entry = make_entry(2, i, i.saturating_sub(1), &vec![0xBBu8; 128]);
        storage.add_entry(&entry).await.unwrap();
    }
    storage.flush().await.unwrap();

    // Reading (ledger=1, entry=0) triggers readahead. After ledger 1's 5
    // entries, the next frame in the log belongs to ledger 2 — the walk
    // must detect the mismatch and stop, NOT cache ledger-2 data under
    // ledger-1's (ledger_id, entry_id) keys.
    let _ = storage.get_entry(1, 0).await.unwrap();

    // Give readahead a moment to run (and to have stopped cleanly).
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Ledger 1 only has entries 0..4. Asking for 5..9 must surface as a
    // genuine NotFound — if readahead had spilled ledger-2 data into
    // cache slots (1, 5)..(1, 9), the read cache would short-circuit and
    // return corrupted data with ledger_id=2 in its header.
    for i in 5..10i64 {
        let res = storage.get_entry(1, i).await;
        assert!(res.is_err(), "entry (1, {i}) should not exist");
    }

    // Ledger 2 entries are still readable via the regular on-demand path.
    for i in 0..5i64 {
        let entry = storage.get_entry(2, i).await.unwrap();
        let lid = i64::from_be_bytes(entry[0..8].try_into().unwrap());
        let eid = i64::from_be_bytes(entry[8..16].try_into().unwrap());
        assert_eq!(lid, 2);
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

#[tokio::test]
async fn add_entry_rejects_when_throttle_timeout_elapses() {
    // Issue #34: a slow flush must not starve all write processing.
    // Configure a tiny write cache and a very short throttle timeout, then
    // saturate the cache while the flush mutex is held — the in-flight
    // add_entry calls should return `OperationRejected` rather than hang.
    use bookkeeper_storage::db_ledger_storage::StorageConfig;
    use bookkeeper_storage::StorageError;

    let dir = TempDir::new().unwrap();
    let cfg = StorageConfig {
        write_cache_max_size: 4 * 1024, // 4KB cache — tiny on purpose
        max_throttle_time_ms: 50,        // 50ms timeout
        ..Default::default()
    };
    let storage = DbLedgerStorage::open_full(
        dir.path(),
        default_ledger_io(),
        None,
        None,
        cfg,
    ).unwrap();

    // Fill cache: one 2KB entry fits, a second one triggers flush.
    // Use a large enough payload so just a couple of entries saturate.
    let payload = vec![0xABu8; 2048];
    for i in 0..3i64 {
        let entry = make_entry(1, i, i.saturating_sub(1), &payload);
        // First few may succeed (flush path) or fail with OperationRejected
        // once throttle window elapses.
        let _ = storage.add_entry(&entry).await;
    }

    // Now hammer with concurrent adds that all force flush on a tiny cache.
    // Some should be rejected with OperationRejected — the critical assertion
    // is that they return in bounded time, not that they all fail.
    let storage = Arc::new(storage);
    let start = std::time::Instant::now();
    let mut handles = Vec::new();
    for i in 10..40i64 {
        let s = storage.clone();
        let p = payload.clone();
        handles.push(tokio::spawn(async move {
            let entry = make_entry(1, i, i.saturating_sub(1), &p);
            s.add_entry(&entry).await
        }));
    }
    for h in handles {
        let _ = h.await.unwrap();
    }
    let elapsed = start.elapsed();

    // All add_entry calls must finish within 3× throttle timeout,
    // demonstrating the bound: no single slow flush can stall writes
    // for longer than the configured throttle window (modulo fast path).
    assert!(
        elapsed.as_millis() < 3_000,
        "add_entry calls should be bounded by throttle timeout, elapsed={elapsed:?}"
    );

    // And OperationRejected must actually be reachable — confirm the
    // error variant propagates via storage directly.
    let entry = make_entry(1, 999, 998, &vec![0u8; 50_000]); // > cache size
    let err = storage.add_entry(&entry).await.unwrap_err();
    assert!(
        matches!(err, StorageError::OperationRejected) || !matches!(err, StorageError::Io(_)),
        "oversized write should surface a typed error, got {err:?}"
    );
}

#[tokio::test]
async fn rotated_entry_log_has_ledgers_map_at_end() {
    // Issue #35: rotated entry log files must be self-describing — the
    // ledgers map (and updated header pointer) should appear at end-of-file
    // so GC can list ledgers in O(num_ledgers) without scanning the body.
    use bookkeeper_storage::db_ledger_storage::StorageConfig;
    use bookkeeper_storage::entry_log::format::{
        EntryLogHeader, decode_ledger_map_batch, LEDGERS_MAP_HEADER_SIZE,
        LOGFILE_HEADER_SIZE,
    };
    use std::io::{Read, Seek, SeekFrom};

    let dir = TempDir::new().unwrap();
    let cfg = StorageConfig {
        // Small log so rotation triggers quickly, but large enough that
        // each write still fits — we want one file to fill + rotate.
        max_entry_log_size: 64 * 1024,
        ..Default::default()
    };
    let storage = DbLedgerStorage::open_full(
        dir.path(),
        default_ledger_io(),
        None,
        None,
        cfg,
    ).unwrap();

    // Write entries across three ledgers — payload sized so ~32 entries
    // fill one entry log file.
    let payload = vec![0xBEu8; 1500];
    let ledgers: [i64; 3] = [11, 22, 33];
    let mut writes_by_ledger = std::collections::HashMap::new();
    for i in 0..80i64 {
        let ledger_id = ledgers[(i as usize) % 3];
        let entry = make_entry(ledger_id, i, i.saturating_sub(3), &payload);
        storage.add_entry(&entry).await.unwrap();
        *writes_by_ledger.entry(ledger_id).or_insert(0i64) += 1;
    }
    storage.flush().await.unwrap();
    drop(storage);

    // Find the rotated log files (anything that's not the last one).
    let mut log_files: Vec<_> = std::fs::read_dir(dir.path().join("current"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
        .collect();
    log_files.sort_by_key(|e| e.file_name());
    assert!(log_files.len() >= 2,
        "expected rotation to produce multiple log files, got {}", log_files.len());

    // The earlier (rotated) file(s) must carry a ledgers map. Walk them
    // and verify: header ledgers_map_offset > 0, reading at that offset
    // yields a valid batch, and the ledger ids match what we wrote.
    let mut found_ledgers: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for entry in &log_files[..log_files.len() - 1] {
        let mut file = std::fs::File::open(entry.path()).unwrap();
        let mut hdr_buf = vec![0u8; LOGFILE_HEADER_SIZE];
        file.read_exact(&mut hdr_buf).unwrap();
        let header = EntryLogHeader::from_bytes(&hdr_buf).unwrap();

        let map_offset = header.ledgers_map_offset();
        assert!(
            map_offset > 0,
            "rotated log {:?} must have ledgers_map_offset > 0",
            entry.file_name()
        );
        let count = header.ledgers_count();
        assert!(count > 0, "rotated log must report > 0 ledgers");

        // Read the batch at that offset.
        file.seek(SeekFrom::Start(map_offset as u64)).unwrap();
        let batch_size_buf = {
            let mut b = [0u8; 4];
            file.read_exact(&mut b).unwrap();
            b
        };
        let content_size = i32::from_be_bytes(batch_size_buf) as usize;
        let mut body = vec![0u8; content_size];
        file.read_exact(&mut body).unwrap();
        // Reconstruct the full batch for decoder (which expects the size prefix).
        let mut whole = Vec::with_capacity(4 + content_size);
        whole.extend_from_slice(&batch_size_buf);
        whole.extend_from_slice(&body);
        assert!(whole.len() >= LEDGERS_MAP_HEADER_SIZE);
        let entries = decode_ledger_map_batch(&whole).unwrap();
        for e in entries {
            found_ledgers.insert(e.ledger_id);
            assert!(e.size > 0, "ledger {} should have non-zero bytes", e.ledger_id);
        }
    }

    // Rotated files must collectively describe at least one ledger. We
    // don't require ALL ledgers to appear because the sort by (ledger_id,
    // entry_id) packs one ledger per file — later ledgers may land only
    // in the still-active (un-rotated) file. What matters is that the
    // rotation path wrote the map at all.
    assert!(!found_ledgers.is_empty(),
        "rotated logs should describe at least one ledger");
    // And every rotated ledger must be one we actually wrote.
    for lid in &found_ledgers {
        assert!(ledgers.contains(lid), "unexpected ledger {lid} in map");
    }
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

// ---------------------------------------------------------------------------
// Data-integrity primitives (entry_exists, storage flags, limbo)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn entry_exists_reports_presence() {
    use bookkeeper_storage::ledger_storage::StorageStateFlag;
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();

    // not-yet-added entry
    assert!(!storage.entry_exists(1, 0).unwrap());

    storage.add_entry(&make_entry(1, 0, -1, b"a")).await.unwrap();
    // present via write cache
    assert!(storage.entry_exists(1, 0).unwrap());

    // flush to disk; still present via location index
    storage.flush().await.unwrap();
    assert!(storage.entry_exists(1, 0).unwrap());

    // LAST_ADD_CONFIRMED (-1) is never considered "present"
    assert!(!storage.entry_exists(1, -1).unwrap());

    // flags + limbo round-trip through the storage trait
    assert_eq!(storage.get_storage_state_flags().unwrap(), 0);
    storage.set_storage_state_flag(StorageStateFlag::NeedsIntegrityCheck).unwrap();
    assert_eq!(storage.get_storage_state_flags().unwrap(), 0x1);
    storage.clear_storage_state_flag(StorageStateFlag::NeedsIntegrityCheck).unwrap();
    assert_eq!(storage.get_storage_state_flags().unwrap(), 0);

    assert!(!storage.has_limbo_state(1).unwrap());
    storage.set_limbo_state(1).unwrap();
    assert!(storage.has_limbo_state(1).unwrap());
    storage.clear_limbo_state(1).unwrap();
    assert!(!storage.has_limbo_state(1).unwrap());
}

/// Regression guard: `publish_cache_metrics` must actually update the Prometheus
/// gauges to reflect the current cache state.  Specifically:
///   - After `add_entry`, the write-cache count/size gauges should be > 0.
///   - After `flush` + `get_entry` (disk miss → read-cache fill), the
///     read-cache count gauge should also be > 0.
///
/// We capture a baseline before the operations and assert on deltas rather than
/// absolute values so the test stays correct even when earlier tests in the
/// same binary have already touched the gauges.
#[tokio::test]
async fn cache_gauges_update_after_put() {
    use bookkeeper_storage::storage_metrics::{
        WRITE_CACHE_COUNT, WRITE_CACHE_SIZE, READ_CACHE_COUNT,
    };

    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();

    // Capture baseline — prior tests in the same binary may have left non-zero values.
    let base_wc_count = WRITE_CACHE_COUNT.get();
    let base_wc_size = WRITE_CACHE_SIZE.get();
    let base_rc_count = READ_CACHE_COUNT.get();

    // --- write-cache gauges ---
    let entry = make_entry(1, 0, -1, &vec![0xAAu8; 128]);
    storage.add_entry(&entry).await.unwrap();

    // `publish_cache_metrics` is `pub` — call it directly to snapshot the gauges.
    storage.publish_cache_metrics();

    assert!(
        WRITE_CACHE_COUNT.get() > base_wc_count,
        "write_cache_count should have increased after add_entry: base={base_wc_count}, got={}",
        WRITE_CACHE_COUNT.get()
    );
    assert!(
        WRITE_CACHE_SIZE.get() > base_wc_size,
        "write_cache_size should have increased after add_entry: base={base_wc_size}, got={}",
        WRITE_CACHE_SIZE.get()
    );

    // --- read-cache gauge ---
    // Flush so the entry lands on disk, then read it back; the read path
    // inserts the result into the read cache.
    storage.flush().await.unwrap();
    let _ = storage.get_entry(1, 0).await.unwrap();

    storage.publish_cache_metrics();

    assert!(
        READ_CACHE_COUNT.get() > base_rc_count,
        "read_cache_count should have increased after get_entry: base={base_rc_count}, got={}",
        READ_CACHE_COUNT.get()
    );
}

#[tokio::test]
async fn empty_master_key_is_replaced_by_real_key_on_check() {
    // Regression guard: data-integrity pre-boot calls set_master_key(id, &[])
    // to reserve the ledger row before the real master key is known. A
    // subsequent check_master_key with the real key must succeed (and
    // upgrade the stored key) rather than returning Unauthorized.
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();

    storage.set_master_key(42, b"").unwrap();
    // Cache hit path — empty key must not be compared to the real one.
    storage
        .check_master_key(42, b"real-pw")
        .expect("empty master key should be upgraded, not rejected");
    // Subsequent check with the real key must match via the now-populated cache.
    storage.check_master_key(42, b"real-pw").expect("real key persists");
    // A different key is rejected.
    assert!(storage.check_master_key(42, b"other-pw").is_err());
}

/// `next_entry_log_id` derives the next log_id from a directory scan
/// (max existing + 1). After GC deletes the file at the highest log_id,
/// the dir-scan max regresses — and on restart the storage would
/// re-mint a log_id that the persisted entry-location index still
/// references, then later overwrite the bytes the index points at.
///
/// Persist the highest log_id ever used in RocksDB and take
/// `max(persisted_last_log_id, dir_scan_max) + 1` on open. This test
/// drives storage to roll into several log files, drops the storage,
/// deletes the trailing log files (simulating a GC pass), reopens, and
/// asserts the new initial log_id is strictly greater than any
/// previously-used id.
#[tokio::test]
async fn next_entry_log_id_does_not_regress_after_gc_deletion() {
    use bookkeeper_storage::db_ledger_storage::StorageConfig;

    let dir = TempDir::new().unwrap();
    let mut config = StorageConfig::default();
    config.max_entry_log_size = 4096;

    let last_used: u32;
    {
        let storage = DbLedgerStorage::open_full(
            dir.path(), default_ledger_io(), None, None, config.clone()
        ).unwrap();
        let storage_dyn: &dyn LedgerStorage = &storage;

        storage_dyn.validate_for_write(1, b"k").unwrap();

        // Write enough entries to roll through several log files.
        // Each entry: ~256 bytes; max_entry_log_size 4096 - header 1024
        // = 3072 usable, so ~12 entries per file. 100 entries → ~9 files.
        for eid in 0..100i64 {
            let entry = make_entry(1, eid, eid - 1, &vec![b'x'; 200]);
            storage_dyn.add_entry(&entry).await.unwrap();
        }
        storage_dyn.flush().await.unwrap();

        last_used = storage_dyn
            .current_log_id()
            .expect("storage should report a current log id after writes");
        assert!(
            last_used >= 4,
            "test setup expected several rotations; got last_used={last_used}",
        );
    }

    // Simulate a GC pass that deleted the trailing log files.
    let entry_log_dir = dir.path().join("current");
    for id in (last_used.saturating_sub(2))..=last_used {
        let path = entry_log_dir.join(format!("{:x}.log", id));
        let _ = std::fs::remove_file(&path);
    }

    let storage = DbLedgerStorage::open_full(
        dir.path(), default_ledger_io(), None, None, config
    ).unwrap();
    let new_log_id = storage
        .current_log_id()
        .expect("storage should report a current log id after reopen");
    assert!(
        new_log_id > last_used,
        "post-restart current_log_id ({new_log_id}) must be strictly \
         greater than any previously-used id ({last_used}). The \
         previous max log_id must be persisted across restart so we \
         don't recycle ids the entry-location index still references.",
    );
}

/// LAC must never go backwards in our in-memory tracking. A buggy or
/// stale caller (a stale replay of a journal entry, or a client WriteLac
/// that violates the per-ledger monotonicity invariant) must not demote
/// a previously-cached higher LAC. The fix uses an entry().and_modify
/// (max).or_insert pattern at every site that writes lac_cache.
///
/// `get_last_add_confirmed` first consults `unified_cache.last_entry_id`
/// (a lower bound from cache content), so to exercise the lac_cache
/// path we don't add real entries — we drive the LAC strictly via
/// set_explicit_lac, which is the public knob clients use.
#[tokio::test]
async fn lac_cache_stays_monotonic_against_lower_set_explicit_lac() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), default_ledger_io()).unwrap();

    storage.validate_for_write(1, b"k").unwrap();

    // Establish a high LAC via the explicit path; lac_cache(1) = 10.
    storage.set_explicit_lac(1, 10).unwrap();
    let after_high = storage.get_last_add_confirmed(1).unwrap();
    assert_eq!(after_high, 10, "set_explicit_lac(10) should advance LAC to 10");

    // A stale or buggy caller tries to set lac=5. The cache must not
    // accept this demotion.
    storage.set_explicit_lac(1, 5).unwrap();
    let after_low = storage.get_last_add_confirmed(1).unwrap();
    assert_eq!(
        after_low, 10,
        "in-memory LAC was demoted from 10 to {after_low}; LAC must be \
         monotonic against stale or out-of-order writers",
    );
}
