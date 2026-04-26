use bookkeeper_server::processor::RequestProcessor;
use bookkeeper_server::state::BookieStateManager;
use bookkeeper_storage::db_ledger_storage::DbLedgerStorage;
use bookkeeper_storage::io_engine::std_engine::StdEngine;
use bookkeeper_protocol::v2::request::V2Request;
use bookkeeper_protocol::v2::response::V2Response;
use bookkeeper_protocol::constants::*;
use bytes::Bytes;
use std::sync::Arc;
use tempfile::TempDir;

fn setup() -> (RequestProcessor, TempDir) {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>).unwrap();
    let state = BookieStateManager::new();
    state.transition_to(bookkeeper_server::state::BookieState::Running);
    let processor = RequestProcessor::new(Arc::new(storage), Arc::new(state));
    (processor, dir)
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
async fn add_entry_success() {
    let (processor, _dir) = setup();
    let entry_data = make_entry(1, 0, -1, b"hello");
    let req = V2Request::AddEntry {
        flags: 0,
        master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data), deferred_sync: false,
    };
    let resp = processor.process_request(req).await;
    match resp {
        V2Response::Add { error_code, ledger_id, entry_id } => {
            assert_eq!(error_code, V2_EOK);
            assert_eq!(ledger_id, 1);
            assert_eq!(entry_id, 0);
        }
        _ => panic!("expected Add response"),
    }
}

#[tokio::test]
async fn read_entry_success() {
    let (processor, _dir) = setup();
    // First write an entry
    let entry_data = make_entry(1, 0, -1, b"hello");
    let req = V2Request::AddEntry {
        flags: 0, master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data), deferred_sync: false,
    };
    processor.process_request(req).await;
    // Now read it
    let req = V2Request::ReadEntry {
        flags: 0, ledger_id: 1, entry_id: 0, master_key: None,
    };
    let resp = processor.process_request(req).await;
    match resp {
        V2Response::Read { error_code, ledger_id, entry_id, data } => {
            assert_eq!(error_code, V2_EOK);
            assert_eq!(ledger_id, 1);
            assert_eq!(entry_id, 0);
            assert!(data.is_some());
        }
        _ => panic!("expected Read response"),
    }
}

#[tokio::test]
async fn read_entry_not_found() {
    let (processor, _dir) = setup();
    let req = V2Request::ReadEntry {
        flags: 0, ledger_id: 999, entry_id: 0, master_key: None,
    };
    let resp = processor.process_request(req).await;
    match resp {
        V2Response::Read { error_code, .. } => {
            assert_ne!(error_code, V2_EOK);
        }
        _ => panic!("expected Read response"),
    }
}

#[tokio::test]
async fn add_entry_rejected_when_read_only() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>).unwrap();
    let state = BookieStateManager::new();
    state.transition_to(bookkeeper_server::state::BookieState::Running);
    state.transition_to(bookkeeper_server::state::BookieState::ReadOnly);
    let processor = RequestProcessor::new(Arc::new(storage), Arc::new(state));

    let entry_data = make_entry(1, 0, -1, b"hello");
    let req = V2Request::AddEntry {
        flags: 0, master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data), deferred_sync: false,
    };
    let resp = processor.process_request(req).await;
    match resp {
        V2Response::Add { error_code, .. } => {
            assert_eq!(error_code, bookkeeper_common::error::BKError::ReadOnly.to_v2_code());
        }
        _ => panic!("expected Add response"),
    }
}

#[tokio::test]
async fn fencing_blocks_subsequent_writes() {
    let (processor, _dir) = setup();
    // Write an entry first
    let entry_data = make_entry(1, 0, -1, b"hello");
    processor.process_request(V2Request::AddEntry {
        flags: 0, master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data), deferred_sync: false,
    }).await;
    // Fence via read with FLAG_DO_FENCING
    processor.process_request(V2Request::ReadEntry {
        flags: FLAG_DO_FENCING, ledger_id: 1, entry_id: 0,
        master_key: Some([0u8; 20]),
    }).await;
    // Normal write should be rejected with EFENCED
    let entry_data2 = make_entry(1, 1, 0, b"world");
    let resp = processor.process_request(V2Request::AddEntry {
        flags: 0, master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data2), deferred_sync: false,
    }).await;
    match resp {
        V2Response::Add { error_code, .. } => {
            assert_eq!(error_code, bookkeeper_common::error::BKError::Fenced.to_v2_code());
        }
        _ => panic!("expected Add response"),
    }
}

#[tokio::test]
async fn recovery_add_bypasses_fence() {
    let (processor, _dir) = setup();
    // Write, fence, then recovery add
    let entry_data = make_entry(1, 0, -1, b"hello");
    processor.process_request(V2Request::AddEntry {
        flags: 0, master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data), deferred_sync: false,
    }).await;
    processor.process_request(V2Request::ReadEntry {
        flags: FLAG_DO_FENCING, ledger_id: 1, entry_id: 0,
        master_key: Some([0u8; 20]),
    }).await;
    // Recovery add should succeed
    let entry_data2 = make_entry(1, 1, 0, b"recovery");
    let resp = processor.process_request(V2Request::AddEntry {
        flags: FLAG_RECOVERY_ADD, master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data2), deferred_sync: false,
    }).await;
    match resp {
        V2Response::Add { error_code, .. } => {
            assert_eq!(error_code, V2_EOK);
        }
        _ => panic!("expected Add response"),
    }
}

#[tokio::test]
async fn get_bookie_info() {
    let (processor, _dir) = setup();
    let req = V2Request::GetBookieInfo { flags: 0, requested: 0x03 };
    let resp = processor.process_request(req).await;
    match resp {
        V2Response::GetBookieInfo { error_code, free_disk_space: _, total_disk_capacity, .. } => {
            assert_eq!(error_code, V2_EOK);
            assert!(total_disk_capacity > 0);
        }
        _ => panic!("expected GetBookieInfo response"),
    }
}

#[tokio::test]
async fn master_key_mismatch_rejected() {
    let (processor, _dir) = setup();
    // Write with key A
    let entry_data = make_entry(1, 0, -1, b"hello");
    processor.process_request(V2Request::AddEntry {
        flags: 0, master_key: [1u8; 20],
        entry_data: Bytes::from(entry_data), deferred_sync: false,
    }).await;
    // Write with key B — should be rejected
    let entry_data2 = make_entry(1, 1, 0, b"world");
    let resp = processor.process_request(V2Request::AddEntry {
        flags: 0, master_key: [2u8; 20],
        entry_data: Bytes::from(entry_data2), deferred_sync: false,
    }).await;
    match resp {
        V2Response::Add { error_code, .. } => {
            assert_eq!(error_code, bookkeeper_common::error::BKError::Unauthorized.to_v2_code());
        }
        _ => panic!("expected Add response"),
    }
}

#[tokio::test]
async fn fencing_with_wrong_key_rejected() {
    let (processor, _dir) = setup();
    // Write with key A
    let entry_data = make_entry(1, 0, -1, b"hello");
    processor.process_request(V2Request::AddEntry {
        flags: 0, master_key: [1u8; 20],
        entry_data: Bytes::from(entry_data), deferred_sync: false,
    }).await;
    // Try to fence with key B — should fail
    let resp = processor.process_request(V2Request::ReadEntry {
        flags: FLAG_DO_FENCING, ledger_id: 1, entry_id: 0,
        master_key: Some([2u8; 20]),
    }).await;
    match resp {
        V2Response::Read { error_code, .. } => {
            assert_eq!(error_code, bookkeeper_common::error::BKError::Unauthorized.to_v2_code());
        }
        _ => panic!("expected Read response"),
    }
    // Ledger should NOT be fenced
    let entry_data3 = make_entry(1, 1, 0, b"still works");
    let resp2 = processor.process_request(V2Request::AddEntry {
        flags: 0, master_key: [1u8; 20],
        entry_data: Bytes::from(entry_data3), deferred_sync: false,
    }).await;
    match resp2 {
        V2Response::Add { error_code, .. } => {
            assert_eq!(error_code, V2_EOK);
        }
        _ => panic!("expected Add response"),
    }
}

/// Fence-then-add TOCTOU: handle_add_entry's pre-write
/// `validate_for_write` returns the fence flag, then the actual write
/// lands later (`storage.add_entry` + `journal.log_add_entry`). A
/// concurrent fence between the check and the journal ack lets a NEW
/// add be journaled and acked even though the BK protocol guarantees
/// "after a fence ack, no add for this ledger may be confirmed".
///
/// Reproduce by giving handle_add_entry a real await window: bring up
/// a journal with sync_data=true and a non-trivial group_wait_ms so
/// `journal.log_add_entry().await` suspends. While it's suspended,
/// fence the ledger directly via the storage trait (which is what a
/// concurrent ReadEntry{FENCE_LEDGER} would have done). After the
/// journal ack returns, the add_entry path must re-check `is_fenced`
/// and respond with Fenced rather than EOK.
#[tokio::test]
async fn add_entry_rejects_when_fence_lands_during_journal_await() {
    use bookkeeper_storage::journal::writer::JournalConfig;
    use bookkeeper_storage::ledger_storage::LedgerStorage;
    use std::time::Duration;

    let dir = TempDir::new().unwrap();
    let mut storage = DbLedgerStorage::open(
        dir.path(),
        Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>,
    )
    .unwrap();
    let journal_dir = dir.path().join("journal");
    std::fs::create_dir_all(&journal_dir).unwrap();
    let cfg = JournalConfig {
        journal_dir: journal_dir.clone(),
        max_file_size: 64 * 1024 * 1024,
        sync_data: true,
        // Longer group wait makes log_add_entry deterministically suspend
        // for at least `max_group_wait_ms`, giving the test a window to
        // run the fence between validate_for_write and the post-write
        // recheck.
        max_group_wait_ms: 50,
        buffered_writes_threshold: 1024 * 1024, // big — wait drives the timing
        buffered_entries_threshold: 1024,
        alignment_size: 0,
        format_version: 6,
        max_queue_memory: 0,
        flush_when_queue_empty: false,
        remove_from_page_cache: false,
        reuse_files: false,
        max_backup_journals: 0,
        force_std_engine_mode: false,
        fsync_observer: None,
    };
    let journal_io: Arc<dyn bookkeeper_storage::io_engine::JournalIo> =
        Arc::new(StdEngine::new());
    storage
        .init_journal(&journal_dir, cfg, journal_io, None, false)
        .await
        .unwrap();
    let storage = Arc::new(storage);
    let state = Arc::new(BookieStateManager::new());
    state.transition_to(bookkeeper_server::state::BookieState::Running);

    let processor = Arc::new(RequestProcessor::new(
        storage.clone() as Arc<dyn LedgerStorage>,
        state.clone(),
    ));

    // Initialize ledger 1 cleanly first so subsequent adds skip the
    // ledger-creation path.
    let init = make_entry(1, 0, -1, b"init");
    let resp = processor
        .process_request(V2Request::AddEntry {
            flags: 0,
            master_key: [0u8; 20],
            entry_data: Bytes::from(init),
            deferred_sync: false,
        })
        .await;
    match resp {
        V2Response::Add { error_code, .. } => assert_eq!(error_code, V2_EOK),
        other => panic!("init add failed: {other:?}"),
    }

    // Kick off a NEW add. handle_add_entry will run validate_for_write
    // (returns fenced=false), then storage.add_entry (cache, fast),
    // then journal.log_add_entry which suspends for ≥ 50 ms.
    let processor2 = processor.clone();
    let add_task = tokio::spawn(async move {
        let entry = make_entry(1, 1, 0, b"racing");
        processor2
            .process_request(V2Request::AddEntry {
                flags: 0,
                master_key: [0u8; 20],
                entry_data: Bytes::from(entry),
                deferred_sync: false,
            })
            .await
    });

    // Yield once so the spawned task has a chance to start, then fence
    // the ledger directly via the storage trait. With max_group_wait_ms=50
    // the spawned task is guaranteed to still be in journal.log_add_entry
    // for many milliseconds after this fence call returns.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let storage_dyn: &dyn LedgerStorage = storage.as_ref();
    storage_dyn.set_fenced(1).expect("set_fenced");

    let resp = add_task.await.expect("task did not panic");
    match resp {
        V2Response::Add { error_code, .. } => {
            assert_eq!(
                error_code,
                bookkeeper_common::error::BKError::Fenced.to_v2_code(),
                "add_entry returned EOK ({}) despite a fence landing during \
                 the journal await; this is the V-1 race that lets adds slip \
                 past a fence ack",
                error_code,
            );
        }
        other => panic!("expected Add response, got {other:?}"),
    }
}
