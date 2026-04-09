//! Integration tests for the Journal write/fsync/replay path.
//!
//! These tests exercise the durability path: start a journal, write entries,
//! shut down, then replay and verify all entries are recovered.

use std::path::PathBuf;
use std::sync::Arc;
use bookkeeper_storage::io_engine::std_engine::StdEngine;
use bookkeeper_storage::io_engine::JournalIo;
use bookkeeper_storage::journal::writer::{Journal, JournalConfig, replay_journal};

fn default_journal_io() -> Arc<dyn JournalIo> {
    Arc::new(StdEngine)
}

fn make_config(dir: PathBuf) -> JournalConfig {
    JournalConfig {
        journal_dir: dir,
        max_file_size: 64 * 1024 * 1024,
        sync_data: true,
        max_group_wait_ms: 1,
        buffered_writes_threshold: 64 * 1024,
        buffered_entries_threshold: 1000,
        alignment_size: 4096,
        format_version: 6,
    }
}

#[test]
fn write_then_replay_recovers_all_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = make_config(tmp.path().to_path_buf());

    // Phase 1: Write entries
    let journal = Journal::start(cfg.clone(), default_journal_io(), None);
    let entries: Vec<Vec<u8>> = (0..50)
        .map(|i| {
            // Build [ledgerId(8)][entryId(8)][lac(8)][length(8)][payload]
            let mut buf = Vec::new();
            buf.extend_from_slice(&1i64.to_be_bytes());       // ledgerId
            buf.extend_from_slice(&(i as i64).to_be_bytes()); // entryId
            buf.extend_from_slice(&((i - 1) as i64).to_be_bytes()); // lac
            buf.extend_from_slice(&(20i64 + i as i64).to_be_bytes()); // length
            buf.extend_from_slice(format!("entry-{i}").as_bytes());
            buf
        })
        .collect();

    for entry in &entries {
        journal.log_add_entry(entry).unwrap();
    }
    drop(journal); // shut down
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Phase 2: Replay
    let mut replayed: Vec<Vec<u8>> = Vec::new();
    let count = replay_journal(tmp.path(), |data| {
        replayed.push(data.to_vec());
    }).unwrap();

    assert_eq!(count, entries.len(), "expected {} replayed entries", entries.len());
    assert_eq!(replayed, entries, "replayed entries don't match written");
}

#[test]
fn empty_journal_dir_replays_zero_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let mut count = 0;
    let n = replay_journal(tmp.path(), |_| count += 1).unwrap();
    assert_eq!(n, 0);
    assert_eq!(count, 0);
}

#[test]
fn master_key_journaling_writes_meta_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = make_config(tmp.path().to_path_buf());
    let journal = Journal::start(cfg, default_journal_io(), None);

    // Journal a master key for ledger 42
    let key = b"my-secret-master-key";
    journal.maybe_journal_master_key(42, key).unwrap();
    // Second call for same ledger should be no-op (cached)
    journal.maybe_journal_master_key(42, key).unwrap();
    // Different ledger
    journal.maybe_journal_master_key(100, key).unwrap();

    drop(journal);
    std::thread::sleep(std::time::Duration::from_millis(200));

    let mut master_key_count = 0;
    let mut ledger_ids = Vec::new();
    replay_journal(tmp.path(), |data| {
        if data.len() >= 16 {
            let lid = i64::from_be_bytes(data[0..8].try_into().unwrap());
            let eid = i64::from_be_bytes(data[8..16].try_into().unwrap());
            // METAENTRY_ID_LEDGER_KEY = -0x1000
            if eid == -0x1000 {
                master_key_count += 1;
                ledger_ids.push(lid);
            }
        }
    }).unwrap();

    assert_eq!(master_key_count, 2, "expected 2 master key entries (one per unique ledger)");
    assert!(ledger_ids.contains(&42));
    assert!(ledger_ids.contains(&100));
}

#[test]
fn group_commit_batches_multiple_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = make_config(tmp.path().to_path_buf());
    let journal = Journal::start(cfg, default_journal_io(), None);

    // Write 100 entries concurrently — group commit should batch them
    let num_threads = 10;
    let entries_per_thread = 10;
    let journal = std::sync::Arc::new(journal);
    let mut handles = Vec::new();
    for tid in 0..num_threads {
        let j = journal.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..entries_per_thread {
                let mut buf = Vec::new();
                buf.extend_from_slice(&(tid as i64).to_be_bytes());
                buf.extend_from_slice(&(i as i64).to_be_bytes());
                buf.extend_from_slice(&((i - 1) as i64).to_be_bytes());
                buf.extend_from_slice(&100i64.to_be_bytes());
                buf.extend_from_slice(b"data");
                j.log_add_entry(&buf).unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    drop(journal);
    std::thread::sleep(std::time::Duration::from_millis(200));

    let mut count = 0;
    replay_journal(tmp.path(), |_| count += 1).unwrap();
    assert_eq!(count, num_threads * entries_per_thread);
}

#[test]
fn journal_rollover_replays_across_multiple_files() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = make_config(tmp.path().to_path_buf());
    // Small file size to force rollover
    cfg.max_file_size = 8192;
    let journal = Journal::start(cfg, default_journal_io(), None);

    // Write enough entries to span multiple journal files
    let entries: Vec<Vec<u8>> = (0..200)
        .map(|i| {
            let mut buf = Vec::with_capacity(256);
            buf.extend_from_slice(&1i64.to_be_bytes());
            buf.extend_from_slice(&(i as i64).to_be_bytes());
            buf.extend_from_slice(&((i - 1) as i64).to_be_bytes());
            buf.extend_from_slice(&100i64.to_be_bytes());
            // 200-byte payload to exceed file size faster
            buf.extend_from_slice(&[b'x'; 200]);
            buf
        })
        .collect();

    for entry in &entries {
        journal.log_add_entry(entry).unwrap();
    }
    drop(journal);
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Verify multiple .txn files exist
    let txn_files: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".txn"))
        .collect();
    assert!(txn_files.len() >= 2, "expected multiple .txn files, got {}", txn_files.len());

    // Replay should recover all entries across all files
    let mut count = 0;
    replay_journal(tmp.path(), |_| count += 1).unwrap();
    assert_eq!(count, entries.len());
}

#[test]
fn sync_data_false_still_replays() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = make_config(tmp.path().to_path_buf());
    cfg.sync_data = false;
    let journal = Journal::start(cfg, default_journal_io(), None);

    for i in 0..20 {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1i64.to_be_bytes());
        buf.extend_from_slice(&(i as i64).to_be_bytes());
        buf.extend_from_slice(&((i - 1) as i64).to_be_bytes());
        buf.extend_from_slice(&100i64.to_be_bytes());
        buf.extend_from_slice(b"data");
        journal.log_add_entry(&buf).unwrap();
    }
    drop(journal);
    std::thread::sleep(std::time::Duration::from_millis(200));

    let mut count = 0;
    replay_journal(tmp.path(), |_| count += 1).unwrap();
    assert_eq!(count, 20);
}
