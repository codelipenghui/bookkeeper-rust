//! Tests for explicit LAC (WriteLac/ReadLac) and batch read processing.

use std::sync::Arc;

use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::v2::request::V2Request;
use bookkeeper_protocol::v2::response::V2Response;
use bookkeeper_server::processor::RequestProcessor;
use bookkeeper_server::state::{BookieState, BookieStateManager};
use bookkeeper_storage::db_ledger_storage::DbLedgerStorage;
use bookkeeper_storage::io_engine::std_engine::StdEngine;
use bytes::Bytes;
use tempfile::TempDir;

fn setup() -> (RequestProcessor, TempDir) {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), Arc::new(StdEngine::new()) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>).unwrap();
    let state = BookieStateManager::new();
    state.transition_to(BookieState::Running);
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
async fn write_lac_and_read_lac_round_trip() {
    let (processor, _dir) = setup();
    let ledger_id = 42;
    let master_key = [7u8; 20];

    // First add an entry to bootstrap the ledger
    let req = V2Request::AddEntry {
        flags: 0,
        master_key,
        entry_data: Bytes::from(make_entry(ledger_id, 0, -1, b"hello")), deferred_sync: false,
    };
    let resp = processor.process_request(req).await;
    assert!(matches!(resp, V2Response::Add { error_code: V2_EOK, .. }));

    // WriteLac to advance LAC explicitly
    let lac_body = make_entry(ledger_id, 5, 5, b"lac-body");
    let req = V2Request::WriteLac {
        ledger_id,
        lac: 5,
        master_key,
        body: Bytes::from(lac_body.clone()),
    };
    let resp = processor.process_request(req).await;
    match resp {
        V2Response::WriteLac { error_code, .. } => {
            assert_eq!(error_code, V2_EOK, "WriteLac should succeed");
        }
        other => panic!("expected WriteLac response, got {other:?}"),
    }

    // ReadLac should return the explicit LAC body
    let req = V2Request::ReadLac { ledger_id };
    let resp = processor.process_request(req).await;
    match resp {
        V2Response::ReadLac { error_code, lac_body: returned_body, .. } => {
            assert_eq!(error_code, V2_EOK);
            assert!(returned_body.is_some(), "ReadLac should return lac_body");
            assert_eq!(returned_body.unwrap().as_ref(), lac_body.as_slice());
        }
        other => panic!("expected ReadLac response, got {other:?}"),
    }
}

#[tokio::test]
async fn read_lac_on_nonexistent_ledger() {
    let (processor, _dir) = setup();
    let req = V2Request::ReadLac { ledger_id: 999 };
    let resp = processor.process_request(req).await;
    match resp {
        V2Response::ReadLac { error_code, .. } => {
            // Should be an error code (e.g., NoLedger or NoSuchEntry)
            assert_ne!(error_code, V2_EOK);
        }
        other => panic!("expected ReadLac response, got {other:?}"),
    }
}

#[tokio::test]
async fn batch_read_returns_multiple_entries() {
    let (processor, _dir) = setup();
    let ledger_id = 100;
    let master_key = [0u8; 20];

    // Write 5 entries
    for i in 0..5 {
        let payload = format!("payload-{i}");
        let req = V2Request::AddEntry {
            flags: 0,
            master_key,
            entry_data: Bytes::from(make_entry(ledger_id, i, i - 1, payload.as_bytes())), deferred_sync: false,
        };
        processor.process_request(req).await;
    }

    // Batch read entries 0..5
    let req = V2Request::BatchReadEntry {
        flags: 0,
        ledger_id,
        start_entry_id: 0,
        request_id: 1,
        max_count: 5,
        max_size: 1024 * 1024,
        master_key: None,
    };
    let resp = processor.process_request(req).await;
    match resp {
        V2Response::BatchRead { error_code, entries, request_id, .. } => {
            assert_eq!(error_code, V2_EOK);
            assert_eq!(request_id, 1);
            assert!(!entries.is_empty(), "expected entries from batch read");
        }
        other => panic!("expected BatchRead response, got {other:?}"),
    }
}

#[tokio::test]
async fn batch_read_respects_max_count() {
    let (processor, _dir) = setup();
    let ledger_id = 200;
    let master_key = [0u8; 20];

    for i in 0..10 {
        let req = V2Request::AddEntry {
            flags: 0,
            master_key,
            entry_data: Bytes::from(make_entry(ledger_id, i, i - 1, b"data")), deferred_sync: false,
        };
        processor.process_request(req).await;
    }

    // max_count=3 should return at most 3 entries
    let req = V2Request::BatchReadEntry {
        flags: 0,
        ledger_id,
        start_entry_id: 0,
        request_id: 99,
        max_count: 3,
        max_size: 1024 * 1024,
        master_key: None,
    };
    let resp = processor.process_request(req).await;
    match resp {
        V2Response::BatchRead { entries, request_id, .. } => {
            assert_eq!(request_id, 99);
            assert!(entries.len() <= 3, "expected ≤3 entries, got {}", entries.len());
        }
        other => panic!("expected BatchRead response, got {other:?}"),
    }
}

#[tokio::test]
async fn write_lac_with_wrong_master_key_rejected() {
    let (processor, _dir) = setup();
    let ledger_id = 50;
    let master_key = [1u8; 20];

    // Bootstrap with one master key
    let req = V2Request::AddEntry {
        flags: 0,
        master_key,
        entry_data: Bytes::from(make_entry(ledger_id, 0, -1, b"x")), deferred_sync: false,
    };
    processor.process_request(req).await;

    // WriteLac with different master key should fail
    let wrong_key = [2u8; 20];
    let req = V2Request::WriteLac {
        ledger_id,
        lac: 0,
        master_key: wrong_key,
        body: Bytes::from(make_entry(ledger_id, 0, 0, b"body")),
    };
    let resp = processor.process_request(req).await;
    match resp {
        V2Response::WriteLac { error_code, .. } => {
            assert_ne!(error_code, V2_EOK, "WriteLac should reject wrong master key");
        }
        other => panic!("expected WriteLac response, got {other:?}"),
    }
}
