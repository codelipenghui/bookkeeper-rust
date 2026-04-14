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
