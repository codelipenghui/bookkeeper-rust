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
    Arc::new(StdEngine::new())
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
        max_queue_memory: 0,
        flush_when_queue_empty: false,
        remove_from_page_cache: false,
        reuse_files: false,
        max_backup_journals: 5,
    }
}

#[tokio::test]
async fn write_then_replay_recovers_all_entries() {
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
        journal.log_add_entry(entry).await.unwrap();
    }
    drop(journal); // shut down
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Phase 2: Replay
    let mut replayed: Vec<Vec<u8>> = Vec::new();
    let count = replay_journal(tmp.path(), None, |data| {
        replayed.push(data.to_vec());
    }).unwrap();

    assert_eq!(count, entries.len(), "expected {} replayed entries", entries.len());
    assert_eq!(replayed, entries, "replayed entries don't match written");
}

#[test]
fn empty_journal_dir_replays_zero_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let mut count = 0;
    let n = replay_journal(tmp.path(), None, |_| count += 1).unwrap();
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
    replay_journal(tmp.path(), None, |data| {
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

#[tokio::test]
async fn group_commit_batches_multiple_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = make_config(tmp.path().to_path_buf());
    let journal = Journal::start(cfg, default_journal_io(), None);

    // Write 100 entries concurrently — group commit should batch them
    let num_tasks = 10;
    let entries_per_task = 10;
    let journal = std::sync::Arc::new(journal);
    let mut handles = Vec::new();
    for tid in 0..num_tasks {
        let j = journal.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..entries_per_task {
                let mut buf = Vec::new();
                buf.extend_from_slice(&(tid as i64).to_be_bytes());
                buf.extend_from_slice(&(i as i64).to_be_bytes());
                buf.extend_from_slice(&((i - 1) as i64).to_be_bytes());
                buf.extend_from_slice(&100i64.to_be_bytes());
                buf.extend_from_slice(b"data");
                j.log_add_entry(&buf).await.unwrap();
            }
        }));
    }
    for h in handles { h.await.unwrap(); }

    drop(journal);
    std::thread::sleep(std::time::Duration::from_millis(200));

    let mut count = 0;
    replay_journal(tmp.path(), None, |_| count += 1).unwrap();
    assert_eq!(count, num_tasks * entries_per_task);
}

#[tokio::test]
async fn journal_rollover_replays_across_multiple_files() {
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
        journal.log_add_entry(entry).await.unwrap();
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
    replay_journal(tmp.path(), None, |_| count += 1).unwrap();
    assert_eq!(count, entries.len());
}

#[tokio::test]
async fn current_mark_advances_after_synced_writes() {
    // Issue #33: Checkpoint offset must track the durable journal position,
    // not stay at 0. After a synced write, current_mark() should return a
    // non-zero offset matching the bytes written (+ header).
    let tmp = tempfile::tempdir().unwrap();
    let cfg = make_config(tmp.path().to_path_buf());
    let journal = Journal::start(cfg, default_journal_io(), None);

    // No writes yet → no mark.
    assert!(journal.current_mark().is_none());

    // Write one entry and wait for fsync ack.
    let mut buf = Vec::new();
    buf.extend_from_slice(&1i64.to_be_bytes());
    buf.extend_from_slice(&0i64.to_be_bytes());
    buf.extend_from_slice(&(-1i64).to_be_bytes());
    buf.extend_from_slice(&100i64.to_be_bytes());
    buf.extend_from_slice(b"first-entry");
    journal.log_add_entry(&buf).await.unwrap();

    let (log_id, offset) = journal.current_mark().expect("mark should advance after synced write");
    assert!(log_id > 0, "log_id must be non-zero after first write");
    // Expect at least: header (8 bytes v6) + 4-byte len + payload.
    // The exact offset depends on padding, but must be well past 0.
    assert!(offset > buf.len() as u64, "offset {offset} must be past the first entry");

    // Additional writes should only advance the mark monotonically.
    let prev_offset = offset;
    for i in 1..5 {
        let mut b = Vec::new();
        b.extend_from_slice(&1i64.to_be_bytes());
        b.extend_from_slice(&(i as i64).to_be_bytes());
        b.extend_from_slice(&((i - 1) as i64).to_be_bytes());
        b.extend_from_slice(&100i64.to_be_bytes());
        b.extend_from_slice(b"more-data");
        journal.log_add_entry(&b).await.unwrap();
    }
    let (_, new_offset) = journal.current_mark().unwrap();
    assert!(new_offset > prev_offset, "mark must advance monotonically");
}

#[tokio::test]
async fn replay_respects_checkpoint_offset() {
    // Issue #33: If lastMark records a non-zero offset, replay must skip
    // data already covered by the checkpoint. Otherwise crash recovery
    // re-scans the whole journal file.
    use bookkeeper_storage::journal::format::LastMark;

    let tmp = tempfile::tempdir().unwrap();
    let cfg = make_config(tmp.path().to_path_buf());
    let journal = Journal::start(cfg, default_journal_io(), None);

    // Write 5 entries, take the mark, then write 5 more.
    for i in 0..5i64 {
        let mut b = Vec::new();
        b.extend_from_slice(&1i64.to_be_bytes());
        b.extend_from_slice(&i.to_be_bytes());
        b.extend_from_slice(&(i - 1).to_be_bytes());
        b.extend_from_slice(&100i64.to_be_bytes());
        b.extend_from_slice(format!("entry-{i}").as_bytes());
        journal.log_add_entry(&b).await.unwrap();
    }
    let (mark_log_id, mark_offset) = journal.current_mark().unwrap();

    for i in 5..10i64 {
        let mut b = Vec::new();
        b.extend_from_slice(&1i64.to_be_bytes());
        b.extend_from_slice(&i.to_be_bytes());
        b.extend_from_slice(&(i - 1).to_be_bytes());
        b.extend_from_slice(&100i64.to_be_bytes());
        b.extend_from_slice(format!("entry-{i}").as_bytes());
        journal.log_add_entry(&b).await.unwrap();
    }

    drop(journal);
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Replay without mark → all 10 entries.
    let mut all_count = 0;
    replay_journal(tmp.path(), None, |_| all_count += 1).unwrap();
    assert_eq!(all_count, 10);

    // Replay with the mark taken after 5 entries → only the latter 5.
    let mark = LastMark::new(mark_log_id as i64, mark_offset as i64);
    let mut post_mark_count = 0;
    let mut ids: Vec<i64> = Vec::new();
    replay_journal(tmp.path(), Some(mark), |data| {
        post_mark_count += 1;
        if data.len() >= 16 {
            ids.push(i64::from_be_bytes(data[8..16].try_into().unwrap()));
        }
    }).unwrap();
    assert_eq!(post_mark_count, 5, "replay should skip entries already covered by mark");
    assert_eq!(ids, vec![5, 6, 7, 8, 9]);
}

#[tokio::test]
async fn current_mark_advances_in_no_sync_mode() {
    // Mode 3 (no sync): mark advances after write_all since that's the
    // strongest durability we have. Still non-zero so recovery can skip.
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = make_config(tmp.path().to_path_buf());
    cfg.sync_data = false;
    let journal = Journal::start(cfg, default_journal_io(), None);

    for i in 0..5i64 {
        let mut b = Vec::new();
        b.extend_from_slice(&1i64.to_be_bytes());
        b.extend_from_slice(&i.to_be_bytes());
        b.extend_from_slice(&(i - 1).to_be_bytes());
        b.extend_from_slice(&100i64.to_be_bytes());
        b.extend_from_slice(b"payload");
        journal.log_add_entry(&b).await.unwrap();
    }

    let (log_id, offset) = journal.current_mark()
        .expect("mark should advance in no-sync mode after writes");
    assert!(log_id > 0);
    assert!(offset > 0);
}

#[tokio::test]
async fn sync_data_false_still_replays() {
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
        journal.log_add_entry(&buf).await.unwrap();
    }
    drop(journal);
    std::thread::sleep(std::time::Duration::from_millis(200));

    let mut count = 0;
    replay_journal(tmp.path(), None, |_| count += 1).unwrap();
    assert_eq!(count, 20);
}
