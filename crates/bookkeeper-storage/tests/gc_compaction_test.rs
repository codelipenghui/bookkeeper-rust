//! Tests for `GarbageCollector::gc_and_compact_entry_logs` — the per-file
//! lifecycle that the existing `gc_test.rs` doesn't cover.
//!
//! The existing tests focus on the **metadata-vs-local** diff (delete
//! ledgers no longer in metadata). This file focuses on what GC does to
//! the entry log **files** themselves:
//!   1. Files where every ledger is dead → deleted entirely
//!   2. Files with active ledgers above the major threshold → preserved
//!   3. Files with low usage (active/total < major_threshold) → compacted:
//!      live entries copied to the current log, index updated, old file
//!      removed
//!   4. The current log file is never touched by GC (writes are in flight)
//!
//! Compaction bugs cause silent data loss — if the compaction copies
//! entries incorrectly, deletes the file before updating the index, or
//! drops live entries, you don't notice until a read fails much later.
//!
//! The tests construct entry log files manually using the public
//! `entry_log::format` module. They use a non-current log_id (5) so the
//! storage's own log file 0 is left alone.

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use bookkeeper_storage::db_ledger_storage::DbLedgerStorage;
use bookkeeper_storage::entry_log::format::{
    encode_entry_log_entry, EntryLogHeader, HEADER_CURRENT_VERSION,
};
use bookkeeper_storage::gc::GarbageCollector;
use bookkeeper_storage::io_engine::std_engine::StdEngine;
use bookkeeper_storage::ledger_storage::LedgerStorage;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a 32-byte fake entry whose first 16 bytes encode `(ledger_id, entry_id)`
/// in big-endian format. The GC compaction code only reads these 16 bytes; the
/// rest can be anything.
fn fake_entry(ledger_id: i64, entry_id: i64) -> Vec<u8> {
    let mut entry = vec![0u8; 32];
    entry[0..8].copy_from_slice(&ledger_id.to_be_bytes());
    entry[8..16].copy_from_slice(&entry_id.to_be_bytes());
    // Fill the rest with a recognizable pattern so we can verify the byte
    // payload survives compaction.
    for (i, byte) in entry[16..].iter_mut().enumerate() {
        *byte = (i as u8).wrapping_add(0xC0);
    }
    entry
}

/// Build a complete entry log file body: 1024-byte header + framed entries.
/// `entries` is a list of `(ledger_id, entry_id)` pairs.
fn build_entry_log_file(entries: &[(i64, i64)]) -> Vec<u8> {
    let header = EntryLogHeader::new(HEADER_CURRENT_VERSION);
    let mut buf = header.to_bytes();
    for (lid, eid) in entries {
        let entry = fake_entry(*lid, *eid);
        let framed = encode_entry_log_entry(&entry);
        buf.extend_from_slice(&framed);
    }
    buf
}

/// Resolve the path the GC and storage will use for a given log_id.
/// The naming convention is `format!("{:x}.log", log_id)` (lowercase hex,
/// no padding).
fn entry_log_path(entry_log_dir: &Path, log_id: u32) -> std::path::PathBuf {
    entry_log_dir.join(format!("{:x}.log", log_id))
}

fn write_entry_log_file(
    entry_log_dir: &Path,
    log_id: u32,
    entries: &[(i64, i64)],
) -> std::path::PathBuf {
    let path = entry_log_path(entry_log_dir, log_id);
    fs::write(&path, build_entry_log_file(entries)).unwrap();
    path
}

fn open_storage(dir: &TempDir) -> Arc<DbLedgerStorage> {
    Arc::new(DbLedgerStorage::open(dir.path(), Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>).unwrap())
}

fn entry_log_dir_for(storage: &DbLedgerStorage) -> &Path {
    storage.entry_log_dir().expect("entry_log_dir is set")
}

/// Layout helper: synthetic test files must live at log_ids strictly
/// below the storage's `current_log_id` so GC's skip-floor doesn't treat
/// them as products of an in-flight rotation. Pre-create the entry-log
/// directory and the synthetic files there before the storage opens —
/// `DbLedgerStorage::open` runs `next_entry_log_id` which discovers them
/// and starts the storage's own log_id strictly above them.
fn pre_create_entry_log_dir(dir: &TempDir) -> std::path::PathBuf {
    let entry_log_dir = dir.path().join("current");
    std::fs::create_dir_all(&entry_log_dir).unwrap();
    entry_log_dir
}

// ---------------------------------------------------------------------------
// Empty file deletion
// ---------------------------------------------------------------------------

#[tokio::test]
async fn gc_deletes_entry_log_file_when_all_ledgers_are_dead() {
    let dir = TempDir::new().unwrap();
    let entry_log_dir = pre_create_entry_log_dir(&dir);
    // Pre-create the synthetic file BEFORE open so `next_entry_log_id`
    // sees it and starts current_log_id strictly above 5.
    let log_path = write_entry_log_file(
        &entry_log_dir,
        5,
        &[(999, 0), (999, 1), (999, 2)],
    );
    let storage = open_storage(&dir);
    assert!(log_path.exists());

    let gc = GarbageCollector::new(storage.clone() as Arc<dyn LedgerStorage>, Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>);
    let active_in_metadata: HashSet<i64> = HashSet::new();
    gc.gc(&active_in_metadata, false).await.unwrap();

    assert!(
        !log_path.exists(),
        "log file 5 should have been deleted because no ledger in it is active"
    );
}

#[tokio::test]
async fn gc_deletes_only_files_with_no_active_ledgers() {
    let dir = TempDir::new().unwrap();
    let entry_log_dir = pre_create_entry_log_dir(&dir);
    // Pre-create both synthetic files so the storage's first log_id
    // is at least 7 — past the GC skip-floor for 5 and 6.
    let dead_path = write_entry_log_file(&entry_log_dir, 5, &[(999, 0), (999, 1)]);
    let live_path = write_entry_log_file(&entry_log_dir, 6, &[(100, 0), (100, 1)]);
    let storage = open_storage(&dir);

    // Make ledger 100 active in the storage's local index
    storage.validate_for_write(100, b"k").unwrap();

    let gc = GarbageCollector::new(storage.clone() as Arc<dyn LedgerStorage>, Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>);
    let active = HashSet::from([100]);
    gc.gc(&active, false).await.unwrap();

    assert!(!dead_path.exists(), "dead file (5) should be removed");
    assert!(
        live_path.exists(),
        "live file (6) at 100% usage should be preserved"
    );
}

// ---------------------------------------------------------------------------
// Compaction (low-usage file) — old file removed, entries relocated
// ---------------------------------------------------------------------------

#[tokio::test]
async fn gc_compacts_low_usage_log_file_and_relocates_live_entries() {
    let dir = TempDir::new().unwrap();
    let entry_log_dir = pre_create_entry_log_dir(&dir);
    // Pre-create the synthetic file so storage opens at log_id > 5 and
    // GC's skip-floor doesn't shield log_id=5.
    let log_path = write_entry_log_file(
        &entry_log_dir,
        5,
        &[
            (100, 42),
            (999, 0),
            (999, 1),
            (999, 2),
            (999, 3),
            (999, 4),
            (999, 5),
            (999, 6),
            (999, 7),
            (999, 8),
        ],
    );

    let storage = open_storage(&dir);
    // Make ledger 100 active in the local index so the GC sees it as live.
    storage.validate_for_write(100, b"k").unwrap();
    let entry_log_dir = entry_log_dir.clone();

    // Storage will rotate into log_id `6` for compaction relocations
    // (one past the highest pre-existing file).
    let current_log_id = storage
        .current_log_id()
        .expect("storage should know its current log_id after open");
    let current_path = entry_log_path(&entry_log_dir, current_log_id);
    let current_size_before = fs::metadata(&current_path)
        .map(|m| m.len())
        .unwrap_or(0);

    let gc = GarbageCollector::new(storage.clone() as Arc<dyn LedgerStorage>, Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>);
    let active = HashSet::from([100]);
    gc.gc(&active, false).await.unwrap();

    // Old file should be gone
    assert!(
        !log_path.exists(),
        "low-usage file (5) should have been compacted away"
    );

    // The current log file should have grown — compaction wrote ledger 100
    // entry 42 to it.
    let current_size_after = fs::metadata(&current_path).unwrap().len();
    assert!(
        current_size_after > current_size_before,
        "current log file should have grown after compaction; \
         before={current_size_before}, after={current_size_after}"
    );

    // The relocated entry should now be readable via storage.get_entry —
    // compaction updated the index to point at the new location.
    let entry = storage.get_entry(100, 42).await.expect(
        "ledger 100 entry 42 should be readable from the new location after compaction",
    );

    // Verify the bytes were copied intact: first 16 bytes are the
    // (ledger_id, entry_id) header from `fake_entry`, plus the recognizable
    // 16-byte filler pattern.
    assert_eq!(&entry[0..8], &100i64.to_be_bytes());
    assert_eq!(&entry[8..16], &42i64.to_be_bytes());
    let expected_filler: Vec<u8> = (0u8..16).map(|i| i.wrapping_add(0xC0)).collect();
    assert_eq!(
        &entry[16..32],
        expected_filler.as_slice(),
        "entry payload should be byte-for-byte identical after compaction"
    );
}

// ---------------------------------------------------------------------------
// High-usage file is left alone
// ---------------------------------------------------------------------------

#[tokio::test]
async fn gc_does_not_compact_high_usage_log_file() {
    let dir = TempDir::new().unwrap();
    let storage = open_storage(&dir);

    // Both ledgers active. usage = 1.0 (100%) → above the 0.8 major
    // threshold → file must NOT be compacted.
    storage.validate_for_write(100, b"k").unwrap();
    storage.validate_for_write(200, b"k").unwrap();

    let entry_log_dir = entry_log_dir_for(&storage).to_path_buf();
    let log_path = write_entry_log_file(
        &entry_log_dir,
        5,
        &[(100, 0), (100, 1), (200, 0), (200, 1)],
    );
    let original_size = fs::metadata(&log_path).unwrap().len();

    let gc = GarbageCollector::new(storage.clone() as Arc<dyn LedgerStorage>, Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>);
    let active = HashSet::from([100, 200]);
    // Use major GC (0.8 threshold) so 100% usage is above threshold and preserved.
    gc.gc(&active, true).await.unwrap();

    assert!(
        log_path.exists(),
        "high-usage file (5) should be preserved by GC"
    );
    let size_after = fs::metadata(&log_path).unwrap().len();
    assert_eq!(
        size_after, original_size,
        "high-usage file size should be unchanged"
    );
}

#[tokio::test]
async fn gc_does_not_compact_file_at_exact_threshold_boundary() {
    // 4 active + 1 dead = 80% usage. The compaction predicate is `< 0.8`
    // (strict less-than), so 0.8 should NOT compact. This pins the
    // boundary.
    let dir = TempDir::new().unwrap();
    let storage = open_storage(&dir);

    storage.validate_for_write(100, b"k").unwrap();

    let entry_log_dir = entry_log_dir_for(&storage).to_path_buf();
    let log_path = write_entry_log_file(
        &entry_log_dir,
        5,
        &[
            (100, 0),
            (100, 1),
            (100, 2),
            (100, 3), // 4 live entries
            (999, 0), // 1 dead entry
        ],
    );

    let gc = GarbageCollector::new(storage.clone() as Arc<dyn LedgerStorage>, Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>);
    let active = HashSet::from([100]);
    // Use major GC (0.8 threshold). Usage is exactly 80% so it should not compact
    // (strict less-than).
    gc.gc(&active, true).await.unwrap();

    assert!(
        log_path.exists(),
        "file at 80% usage should NOT be compacted (strict < threshold); \
         compaction predicate is `usage < major_threshold`"
    );
}

// ---------------------------------------------------------------------------
// Current log file is never touched
// ---------------------------------------------------------------------------

#[tokio::test]
async fn gc_never_deletes_or_compacts_the_current_log_file() {
    use bookkeeper_protocol::digest::{DigestManager, DigestType};
    use bookkeeper_protocol::entry::EntryBuilder;

    let dir = TempDir::new().unwrap();
    let storage = open_storage(&dir);

    // Write a real entry through storage so the current log file (id=0)
    // exists and contains data.
    storage.validate_for_write(100, b"k").unwrap();
    let dm = DigestManager::new(DigestType::Crc32c);
    let entry = EntryBuilder::new(&dm)
        .ledger_id(100)
        .entry_id(0)
        .lac(-1)
        .length(8)
        .payload(b"keep-me!")
        .build()
        .freeze();
    storage.add_entry(&entry).await.unwrap();
    storage.flush().await.unwrap();

    let current_path = entry_log_path(entry_log_dir_for(&storage), 0);
    assert!(current_path.exists(), "current log file should exist after add_entry+flush");

    // Now run GC with NO active ledgers — this would normally try to delete
    // ledger 100's data, then look at log file 0 which contains only dead
    // entries. The GC must NOT touch the current file even though all its
    // entries are now dead.
    let gc = GarbageCollector::new(storage.clone() as Arc<dyn LedgerStorage>, Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>);
    let active: HashSet<i64> = HashSet::new();
    gc.gc(&active, false).await.unwrap();

    assert!(
        current_path.exists(),
        "current log file (0) must be preserved even when all its entries are for dead ledgers"
    );
}

// ---------------------------------------------------------------------------
// Multiple files in one GC pass
// ---------------------------------------------------------------------------

#[tokio::test]
async fn gc_processes_multiple_log_files_in_single_pass() {
    let dir = TempDir::new().unwrap();
    let entry_log_dir = pre_create_entry_log_dir(&dir);

    // Pre-create synthetic files at log_ids 5, 6, 7 so storage opens at
    // log_id 8 (one past the max), keeping them below GC's skip-floor.
    // File 5: all dead → delete
    let dead_path = write_entry_log_file(&entry_log_dir, 5, &[(999, 0), (999, 1)]);
    // File 6: 100% live → keep
    let live_path = write_entry_log_file(&entry_log_dir, 6, &[(100, 0)]);
    // File 7: 1 live, 9 dead → compact
    let compact_path = write_entry_log_file(
        &entry_log_dir,
        7,
        &[
            (100, 100),
            (999, 0),
            (999, 1),
            (999, 2),
            (999, 3),
            (999, 4),
            (999, 5),
            (999, 6),
            (999, 7),
            (999, 8),
        ],
    );

    let storage = open_storage(&dir);
    // Active local ledger 100
    storage.validate_for_write(100, b"k").unwrap();

    let gc = GarbageCollector::new(storage.clone() as Arc<dyn LedgerStorage>, Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>);
    let active = HashSet::from([100]);
    gc.gc(&active, false).await.unwrap();

    assert!(!dead_path.exists(), "file 5 (all dead) should be deleted");
    assert!(live_path.exists(), "file 6 (100% live) should be preserved");
    assert!(
        !compact_path.exists(),
        "file 7 (low usage) should have been compacted away"
    );

    // The compacted entry (100, 100) should still be readable
    let entry = storage.get_entry(100, 100).await.expect(
        "compacted entry should still be readable from its new location",
    );
    assert_eq!(&entry[0..8], &100i64.to_be_bytes());
    assert_eq!(&entry[8..16], &100i64.to_be_bytes());
}
