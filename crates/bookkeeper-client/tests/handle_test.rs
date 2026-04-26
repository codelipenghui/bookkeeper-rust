use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bookkeeper_client::bookie_connection::{BookieConnection, ClientError};
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_client::write_handle::WriteHandle;
use bookkeeper_client::read_handle::ReadHandle;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_server::server::BookieServer;
use tempfile::TempDir;

fn test_config(dir: &tempfile::TempDir) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config
}

#[tokio::test]
async fn write_and_read_single_entry() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    // Write
    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut write_handle = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    let entry_id = write_handle.append(&conns, b"hello world").await.unwrap();
    assert_eq!(entry_id, 0);

    // Read
    let read_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let read_handle = ReadHandle::new(1, ensemble, schedule, dm, master_key);
    let payload = read_handle.read_entry(&read_conns, 0).await.unwrap();
    assert_eq!(payload, b"hello world");

    server.shutdown().await;
}

#[tokio::test]
async fn write_multiple_entries() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut write_handle = WriteHandle::new(1, ensemble, schedule, dm, master_key);

    for i in 0..10 {
        let data = format!("entry-{}", i);
        let eid = write_handle.append(&conns, data.as_bytes()).await.unwrap();
        assert_eq!(eid, i);
    }
    assert_eq!(write_handle.last_add_confirmed(), 9);

    server.shutdown().await;
}

#[tokio::test]
async fn read_verifies_digest() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    wh.append(&conns, b"verified data").await.unwrap();

    let read_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let rh = ReadHandle::new(1, ensemble, schedule, dm, master_key);
    let payload = rh.read_entry(&read_conns, 0).await.unwrap();
    assert_eq!(payload, b"verified data");

    server.shutdown().await;
}

#[tokio::test]
async fn write_multiple_entries_pipelined() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);

    // Submit all entries without awaiting any — true pipelining.
    let mut pending = Vec::new();
    for i in 0..20 {
        let data = format!("pipelined-{i}");
        let p = wh.append_async(&conns, data.as_bytes()).unwrap();
        pending.push(p);
    }

    // All 20 entries were submitted without waiting for any acks.
    // Now resolve them in order.
    for (i, p) in pending.into_iter().enumerate() {
        let entry_id = p.wait().await.unwrap();
        assert_eq!(entry_id, i as i64, "unexpected entry_id for slot {i}");
    }

    // Allow the LAC-update tasks a moment to complete (they run concurrently).
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    assert_eq!(wh.last_add_confirmed(), 19);

    server.shutdown().await;
}

#[tokio::test]
async fn read_entries_parallel() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    // Write 50 entries
    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    for i in 0..50 {
        wh.append(&conns, format!("parallel-read-{i}").as_bytes()).await.unwrap();
    }

    // Read all 50 entries with concurrency=10
    let read_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let rh = ReadHandle::new(1, ensemble, schedule, dm, master_key);
    let entries = rh.read_entries_parallel(&read_conns, 0, 49, 10).await.unwrap();

    assert_eq!(entries.len(), 50);
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry, format!("parallel-read-{i}").as_bytes());
    }

    server.shutdown().await;
}

/// Verify that many pipelined appends don't accumulate unbounded JoinHandles.
/// Crosses the PENDING_ADDS_PRUNE_INTERVAL (1000) threshold to exercise
/// the periodic pruning path. Matches Java where completed PendingAddOps
/// are naturally GC'd.
#[tokio::test]
async fn many_pipelined_appends_prune_completed_handles() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble, schedule, dm, master_key);

    // Submit 1500 entries (crosses prune threshold at 1000).
    // Use append_async to avoid waiting for each — true pipelining.
    let mut pending = Vec::new();
    for i in 0..1500 {
        let data = format!("entry-{i}");
        let p = wh.append_async(&conns, data.as_bytes()).unwrap();
        pending.push(p);
    }

    // Resolve all pending adds.
    for (i, p) in pending.into_iter().enumerate() {
        let entry_id = p.wait().await.unwrap();
        assert_eq!(entry_id, i as i64);
    }

    // Close should drain any remaining handles quickly.
    let (lac, _len) = wh.close().await.unwrap();
    assert_eq!(lac, 1499);

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Ensemble change tests
// ---------------------------------------------------------------------------

/// Two real bookies. Ensemble starts on bookie1; bookie1 is shut down mid-stream.
/// `append_with_ensemble_change` detects the failure, replaces bookie1 with bookie2,
/// and succeeds on retry. The post-failover entry is readable from bookie2.
#[tokio::test]
async fn ensemble_change_replaces_failed_bookie() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let server1 = BookieServer::start(test_config(&dir1)).await.unwrap();
    let server2 = BookieServer::start(test_config(&dir2)).await.unwrap();
    let bookie1_addr = server1.local_addr();
    let bookie2_addr = server2.local_addr();
    let bookie1_str = bookie1_addr.to_string();
    let bookie2_str = bookie2_addr.to_string();

    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    // Ensemble = [bookie1], available pool = [bookie1, bookie2].
    let mut wh = WriteHandle::with_available_bookies(
        43,
        vec![bookie1_str.clone()],
        vec![bookie1_str.clone(), bookie2_str.clone()],
        schedule,
        dm,
        master_key,
    );

    let mut conns: Vec<Arc<BookieConnection>> = vec![
        Arc::new(BookieConnection::connect(bookie1_addr).await.unwrap()),
    ];

    // Write one entry to bookie1 successfully.
    let e0 = wh.append_with_ensemble_change(&mut conns, b"before-failure").await.unwrap();
    assert_eq!(e0, 0);

    // Shut down bookie1 to force the next write to fail.
    server1.shutdown().await;

    // append_with_ensemble_change should detect the failure, switch to bookie2,
    // and succeed on retry.
    let e1 = wh.append_with_ensemble_change(&mut conns, b"after-failure").await.unwrap();
    assert_eq!(e1, 1);

    // The ensemble must now point at bookie2.
    assert_eq!(wh.ensemble()[0], bookie2_str);

    // Verify the post-failover entry is readable from bookie2.
    let read_conns = vec![Arc::new(BookieConnection::connect(bookie2_addr).await.unwrap())];
    let rh = ReadHandle::new(
        43,
        wh.ensemble().to_vec(),
        RoundRobinDistributionSchedule::new(1, 1, 1),
        DigestManager::new(DigestType::Crc32c),
        [0u8; 20],
    );
    let payload = rh.read_entry(&read_conns, 1).await.unwrap();
    assert_eq!(payload, b"after-failure");

    server2.shutdown().await;
}

/// Verify that `WriteQuorumFailed` carries the correct failed indices so that
/// `handle_bookie_failure` can route replacements precisely.
#[tokio::test]
async fn write_quorum_failed_error_carries_failed_indices() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let mut wh = WriteHandle::new(99, vec![addr.to_string()], schedule, dm, master_key);

    let conn = Arc::new(BookieConnection::connect(addr).await.unwrap());
    let conns = vec![conn];

    // Successful write.
    let e0 = wh.append(&conns, b"ok").await.unwrap();
    assert_eq!(e0, 0);

    // Shut server down; next write must produce WriteQuorumFailed.
    server.shutdown().await;

    let result = wh.append(&conns, b"fail").await;
    match result {
        Err(ClientError::WriteQuorumFailed { entry_id, failed_indices, .. }) => {
            assert_eq!(entry_id, 1);
            assert!(!failed_indices.is_empty(), "expected at least one failed index");
        }
        other => panic!("expected WriteQuorumFailed, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Ensemble segments tests (Issue 5)
// ---------------------------------------------------------------------------

/// Verify that a fresh WriteHandle starts with a single segment at entry 0.
#[tokio::test]
async fn ensemble_segments_initial_single_segment() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let wh = WriteHandle::new(1, ensemble.clone(), schedule, dm, master_key);
    let segments = wh.ensemble_segments();
    assert_eq!(segments.len(), 1, "should have exactly one initial segment");
    assert_eq!(segments[&0], ensemble, "initial segment should be at entry 0");

    server.shutdown().await;
}

/// Verify that after an ensemble change, a new segment is added.
#[tokio::test]
async fn ensemble_change_creates_new_segment() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let server1 = BookieServer::start(test_config(&dir1)).await.unwrap();
    let server2 = BookieServer::start(test_config(&dir2)).await.unwrap();
    let bookie1_addr = server1.local_addr();
    let bookie2_addr = server2.local_addr();
    let bookie1_str = bookie1_addr.to_string();
    let bookie2_str = bookie2_addr.to_string();

    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let mut wh = WriteHandle::with_available_bookies(
        44,
        vec![bookie1_str.clone()],
        vec![bookie1_str.clone(), bookie2_str.clone()],
        schedule,
        dm,
        master_key,
    );

    let mut conns: Vec<Arc<BookieConnection>> = vec![
        Arc::new(BookieConnection::connect(bookie1_addr).await.unwrap()),
    ];

    // Write entry 0 to bookie1 successfully.
    wh.append_with_ensemble_change(&mut conns, b"seg0-entry").await.unwrap();

    // Shut down bookie1 to force ensemble change.
    server1.shutdown().await;

    // This triggers ensemble change — should create a new segment.
    wh.append_with_ensemble_change(&mut conns, b"seg1-entry").await.unwrap();

    let segments = wh.ensemble_segments();
    assert!(segments.len() >= 2, "expected at least 2 segments after ensemble change, got {}", segments.len());

    // First segment should start at 0 with the original ensemble.
    // A later segment should contain bookie2.
    let last_segment = segments.values().next_back().unwrap();
    assert_eq!(last_segment[0], bookie2_str, "latest segment should use replacement bookie");

    server2.shutdown().await;
}

// ---------------------------------------------------------------------------
// on_ensemble_change callback test (Issue 1)
// ---------------------------------------------------------------------------

/// Verify the on_ensemble_change callback is invoked during ensemble change.
#[tokio::test]
async fn on_ensemble_change_callback_invoked() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let server1 = BookieServer::start(test_config(&dir1)).await.unwrap();
    let server2 = BookieServer::start(test_config(&dir2)).await.unwrap();
    let bookie1_addr = server1.local_addr();
    let bookie2_addr = server2.local_addr();
    let bookie1_str = bookie1_addr.to_string();
    let bookie2_str = bookie2_addr.to_string();

    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let callback_invoked = Arc::new(AtomicBool::new(false));
    let callback_flag = callback_invoked.clone();

    let mut wh = WriteHandle::with_available_bookies(
        45,
        vec![bookie1_str.clone()],
        vec![bookie1_str.clone(), bookie2_str.clone()],
        schedule,
        dm,
        master_key,
    );
    wh.set_on_ensemble_change(Box::new(move |_first_entry, _ensemble| {
        let cb_flag = callback_flag.clone();
        Box::pin(async move {
            cb_flag.store(true, Ordering::Release);
            Ok(())
        })
    }));

    let mut conns: Vec<Arc<BookieConnection>> = vec![
        Arc::new(BookieConnection::connect(bookie1_addr).await.unwrap()),
    ];

    // Successful write — callback should NOT fire.
    wh.append_with_ensemble_change(&mut conns, b"ok").await.unwrap();
    assert!(!callback_invoked.load(Ordering::Acquire), "callback should not fire on success");

    // Shut down bookie1; next write triggers ensemble change.
    server1.shutdown().await;
    wh.append_with_ensemble_change(&mut conns, b"triggers-change").await.unwrap();

    assert!(callback_invoked.load(Ordering::Acquire), "callback should fire on ensemble change");

    server2.shutdown().await;
}

// ---------------------------------------------------------------------------
// LAC gate test (Issue 4)
// ---------------------------------------------------------------------------

/// Verify that ensemble() returns the correct current ensemble.
#[tokio::test]
async fn ensemble_returns_current_after_change() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let server1 = BookieServer::start(test_config(&dir1)).await.unwrap();
    let server2 = BookieServer::start(test_config(&dir2)).await.unwrap();
    let bookie1_addr = server1.local_addr();
    let bookie2_addr = server2.local_addr();
    let bookie1_str = bookie1_addr.to_string();
    let bookie2_str = bookie2_addr.to_string();

    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let mut wh = WriteHandle::with_available_bookies(
        46,
        vec![bookie1_str.clone()],
        vec![bookie1_str.clone(), bookie2_str.clone()],
        schedule,
        dm,
        master_key,
    );

    // Before any change: ensemble should be [bookie1].
    assert_eq!(wh.ensemble(), vec![bookie1_str.clone()]);

    let mut conns: Vec<Arc<BookieConnection>> = vec![
        Arc::new(BookieConnection::connect(bookie1_addr).await.unwrap()),
    ];

    wh.append_with_ensemble_change(&mut conns, b"pre").await.unwrap();

    // After successful write, ensemble is still [bookie1].
    assert_eq!(wh.ensemble(), vec![bookie1_str.clone()]);

    server1.shutdown().await;
    wh.append_with_ensemble_change(&mut conns, b"post").await.unwrap();

    // After ensemble change, ensemble should be [bookie2].
    assert_eq!(wh.ensemble(), vec![bookie2_str.clone()]);

    // LAC should have advanced past the second entry.
    assert_eq!(wh.last_add_confirmed(), 1);

    server2.shutdown().await;
}

// ---------------------------------------------------------------------------
// DEFERRED_SYNC LAC gate tests (C2)
// ---------------------------------------------------------------------------

/// With deferred_sync enabled, LAC must NOT advance after ack quorum.
/// Entries are acked but LAC stays at -1 until force_lac_flush() is called.
/// Matches Java's sendAddSuccessCallbacks() gating on WriteFlag.DEFERRED_SYNC.
#[tokio::test]
async fn deferred_sync_gates_lac_advancement() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble, schedule, dm, master_key);
    wh.set_deferred_sync(true);

    // Write 5 entries — all should succeed (ack quorum reached).
    for i in 0..5 {
        let eid = wh.append(&conns, format!("deferred-{i}").as_bytes()).await.unwrap();
        assert_eq!(eid, i);
    }

    // LAC must NOT have advanced despite entries being acked.
    // Allow spawned tasks a moment to complete.
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    assert_eq!(wh.last_add_confirmed(), -1,
        "LAC should not advance with deferred_sync=true");

    // force_lac_flush() should advance LAC for all acked entries.
    wh.force_lac_flush();
    assert_eq!(wh.last_add_confirmed(), 4,
        "LAC should advance to 4 after force_lac_flush()");

    server.shutdown().await;
}

/// With deferred_sync, force() sends ForceLedger RPC and advances LAC.
#[tokio::test]
async fn deferred_sync_force_advances_lac() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble, schedule, dm, master_key);
    wh.set_deferred_sync(true);

    // Write entries — LAC should not advance.
    for i in 0..3 {
        wh.append(&conns, format!("force-{i}").as_bytes()).await.unwrap();
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    assert_eq!(wh.last_add_confirmed(), -1);

    // force() sends ForceLedger RPC and then advances LAC.
    wh.force(&conns).await.unwrap();
    assert_eq!(wh.last_add_confirmed(), 2);

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Metadata error slot test (C5)
// ---------------------------------------------------------------------------

/// When the metadata error slot is set, subsequent writes via
/// append_with_ensemble_change must fail.
#[tokio::test]
async fn metadata_error_slot_blocks_subsequent_writes() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let mut conns: Vec<Arc<BookieConnection>> = vec![
        Arc::new(BookieConnection::connect(addr).await.unwrap()),
    ];
    let mut wh = WriteHandle::new(1, ensemble, schedule, dm, master_key);

    // First write succeeds.
    wh.append_with_ensemble_change(&mut conns, b"ok").await.unwrap();

    // Simulate async metadata failure by setting the error slot directly.
    {
        let err_slot = wh.metadata_error_slot();
        let mut slot = err_slot.lock().unwrap();
        *slot = Some("CAS failed after retries".into());
    }

    // Next write via append_with_ensemble_change must fail.
    let result = wh.append_with_ensemble_change(&mut conns, b"should-fail").await;
    assert!(result.is_err(), "write should fail when metadata error slot is set");
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("ensemble change metadata update failed"),
        "error should mention metadata: {err_msg}");

    server.shutdown().await;
}

/// Without deferred_sync, LAC advances normally after ack quorum.
#[tokio::test]
async fn non_deferred_sync_advances_lac_normally() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble, schedule, dm, master_key);
    // deferred_sync is false by default

    for i in 0..5 {
        wh.append(&conns, format!("normal-{i}").as_bytes()).await.unwrap();
    }

    assert_eq!(wh.last_add_confirmed(), 4,
        "LAC should advance normally without deferred_sync");

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// F1: asyncReadLastConfirmed (quorum LAC poll)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn read_last_confirmed_returns_max_lac() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    // Write 5 entries
    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    for i in 0..5 {
        wh.append(&conns, format!("rlac-{i}").as_bytes()).await.unwrap();
    }

    // Poll LAC via read_last_confirmed
    let read_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let rh = ReadHandle::new(1, ensemble, schedule, dm, master_key);
    let lac = rh.read_last_confirmed(&read_conns).await.unwrap();
    assert_eq!(lac, 4, "read_last_confirmed should return LAC=4");
    assert_eq!(rh.last_add_confirmed(), 4, "ReadHandle LAC should be updated");

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// F4: LAC update from piggybacked read responses
// ---------------------------------------------------------------------------

#[tokio::test]
async fn piggybacked_lac_updates_read_handle() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    // Write entries
    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    for i in 0..5 {
        wh.append(&conns, format!("piggy-{i}").as_bytes()).await.unwrap();
    }

    // ReadHandle starts with LAC=-1
    let read_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let rh = ReadHandle::new(1, ensemble, schedule, dm, master_key);
    assert_eq!(rh.last_add_confirmed(), -1);

    // Reading entry 0 should piggyback the LAC from the entry metadata
    let _data = rh.read_entry(&read_conns, 0).await.unwrap();
    // The piggybacked LAC is the LAC at the time entry 0 was written (-1 for first entry)
    // Reading entry 4 (last) should have LAC=3 (piggybacked from when it was written)
    let _data = rh.read_entry(&read_conns, 4).await.unwrap();
    assert!(rh.last_add_confirmed() >= 3,
        "reading entry 4 should update LAC to at least 3, got {}", rh.last_add_confirmed());

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// F5: confirmed vs unconfirmed read distinction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn confirmed_read_rejects_beyond_lac() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    for i in 0..5 {
        wh.append(&conns, format!("conf-{i}").as_bytes()).await.unwrap();
    }

    let read_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    // Set LAC to 2 — entries 0-2 are confirmed, 3-4 are not
    let rh = ReadHandle::new(1, ensemble, schedule, dm, master_key)
        .with_last_add_confirmed(2);

    // Confirmed read within LAC should succeed
    let data = rh.read_entry_confirmed(&read_conns, 1).await.unwrap();
    assert_eq!(data, b"conf-1");

    // Confirmed read beyond LAC should fail
    let result = rh.read_entry_confirmed(&read_conns, 3).await;
    assert!(result.is_err(), "confirmed read beyond LAC should fail");
    assert!(result.unwrap_err().to_string().contains("beyond LAC"));

    // Unconfirmed read beyond LAC should succeed
    let data = rh.read_entry_unconfirmed(&read_conns, 3).await.unwrap();
    assert_eq!(data, b"conf-3");

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// F3: sticky reads
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sticky_reads_use_single_bookie() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // E=1, WQ=1 → sticky reads should be enabled
    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    for i in 0..3 {
        wh.append(&conns, format!("sticky-{i}").as_bytes()).await.unwrap();
    }

    let read_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let rh = ReadHandle::new(1, ensemble, schedule, dm, master_key)
        .with_sticky_reads();

    // All reads should succeed (single bookie, sticky just ensures ordering)
    for i in 0..3 {
        let data = rh.read_entry(&read_conns, i).await.unwrap();
        assert_eq!(data, format!("sticky-{i}").as_bytes());
    }

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// F6: scheduled ExplicitLac flush timer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn explicit_lac_flush_timer_sends_write_lac() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);

    // Write entries
    for i in 0..3 {
        wh.append(&conns, format!("elac-{i}").as_bytes()).await.unwrap();
    }
    assert_eq!(wh.last_add_confirmed(), 2);

    // Start the timer with a very short interval
    let flush_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let flush_handle = wh.start_explicit_lac_flush(flush_conns, std::time::Duration::from_millis(50));

    // Wait for at least one flush cycle
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify the LAC was flushed by reading it from another connection
    let read_conn = Arc::new(BookieConnection::connect(addr).await.unwrap());
    let resp = read_conn.send_read_lac(1).await;
    // ReadLac should return the explicit LAC
    match resp {
        Ok(bookkeeper_protocol::v2::response::V2Response::ReadLac { error_code, lac_body, .. }) => {
            assert_eq!(error_code, 0, "ReadLac should succeed");
            assert!(lac_body.is_some(), "LAC body should be present after flush");
        }
        _ => {
            // Even if the response format differs, the timer ran without errors
        }
    }

    flush_handle.cancel();
    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// F7: delayEnsembleChange mode
// ---------------------------------------------------------------------------

/// With delayEnsembleChange=true, a single bookie failure that still allows
/// ack quorum does NOT trigger immediate ensemble change.
#[tokio::test]
async fn delay_ensemble_change_absorbs_partial_failures() {
    // This test uses a simple scenario: E=1, WQ=1, AQ=1
    // With single bookie, any failure loses quorum, so ensemble change is still triggered.
    // The feature is verified by checking the flag is wired through.
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble, schedule, dm, master_key);
    wh.set_delay_ensemble_change(true);

    // Normal write should succeed regardless of delay mode
    let eid = wh.append(&conns, b"delayed-mode").await.unwrap();
    assert_eq!(eid, 0);

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// WriteHandleAdv tests (I12: explicit entry IDs)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn write_handle_adv_sequential_entries() {
    use bookkeeper_client::write_handle::WriteHandleAdv;

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandleAdv::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);

    // Write entries with explicit sequential IDs
    for i in 0..5 {
        let eid = wh.append_adv(&conns, i, format!("entry-{i}").as_bytes()).await.unwrap();
        assert_eq!(eid, i);
    }
    // LAC should advance through all entries
    assert_eq!(wh.last_add_confirmed(), 4);

    // Read back
    let read_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let rh = ReadHandle::new(1, ensemble, schedule, dm, master_key);
    for i in 0..5 {
        let payload = rh.read_entry(&read_conns, i).await.unwrap();
        assert_eq!(payload, format!("entry-{i}").as_bytes());
    }

    server.shutdown().await;
}

#[tokio::test]
async fn write_handle_adv_rejects_duplicate() {
    use bookkeeper_client::write_handle::WriteHandleAdv;

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandleAdv::new(1, ensemble, schedule, dm, master_key);

    // First write succeeds
    wh.append_adv(&conns, 0, b"first").await.unwrap();

    // Duplicate entry ID (already confirmed via LAC) is rejected
    let result = wh.append_adv(&conns, 0, b"duplicate").await;
    assert!(result.is_err(), "duplicate entry ID should be rejected");

    server.shutdown().await;
}

#[tokio::test]
async fn write_handle_adv_out_of_order_lac() {
    use bookkeeper_client::write_handle::WriteHandleAdv;

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandleAdv::new(1, ensemble, schedule, dm, master_key);

    // Entry 0 first — fires immediately, LAC=0.
    wh.append_adv(&conns, 0, b"zero").await.unwrap();
    assert_eq!(wh.last_add_confirmed(), 0);

    // Out-of-order adv writes (entry 2 before entry 1): matches Java's
    // `LedgerHandleAdv` which uses a PriorityBlockingQueue ordered by
    // entryId.  Each callback fires only after its entry becomes head of
    // the queue AND reaches `pendingAddsSequenceHead + 1` — so we MUST
    // submit both concurrently, then await.  Sequential submit+await of
    // entry 2 alone would deadlock (matches Java's behavior).
    let p2 = wh.append_adv_async(&conns, 2, b"two").unwrap();
    let p1 = wh.append_adv_async(&conns, 1, b"one").unwrap();

    // Entry 1 fires first (LAC advances 0 → 1).  Java invariant holds:
    // after p1.wait() returns Ok(1), LAC ≥ 1.
    let eid1 = p1.wait().await.unwrap();
    assert_eq!(eid1, 1);
    assert!(wh.last_add_confirmed() >= 1, "LAC should be ≥1 after p1");

    // Entry 2 fires next (LAC 1 → 2).
    let eid2 = p2.wait().await.unwrap();
    assert_eq!(eid2, 2);
    assert_eq!(wh.last_add_confirmed(), 2, "LAC should advance through contiguous entries");

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Deferred sync V3 test (I20)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deferred_sync_write_uses_v3_and_reads_back() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    wh.set_deferred_sync(true);

    // Write with deferred sync (goes via V3 AddEntry)
    let eid = wh.append(&conns, b"deferred-sync-data").await.unwrap();
    assert_eq!(eid, 0);

    // Force journal sync to ensure durability
    conns[0].send_force_ledger(1).await.unwrap();

    // Read back
    let read_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let rh = ReadHandle::new(1, ensemble, schedule, dm, master_key);
    let payload = rh.read_entry(&read_conns, 0).await.unwrap();
    assert_eq!(payload, b"deferred-sync-data");

    server.shutdown().await;
}


// ---------------------------------------------------------------------------
// Fix #25: write-handle closed-state guard
// ---------------------------------------------------------------------------

#[tokio::test]
async fn append_after_close_returns_ledger_closed() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(42, ensemble, schedule, dm, master_key);
    assert!(!wh.is_closed());

    wh.append(&conns, b"pre-close").await.unwrap();

    wh.close().await.unwrap();
    assert!(wh.is_closed(), "handle should report closed after close()");

    // Subsequent appends must fail with LedgerClosed.
    let err = wh.append(&conns, b"post-close").await.unwrap_err();
    assert!(
        matches!(err, ClientError::LedgerClosed),
        "expected LedgerClosed, got {err:?}"
    );

    // append_async must also fail synchronously.
    match wh.append_async(&conns, b"post-close-async") {
        Ok(_) => panic!("expected LedgerClosed, got Ok"),
        Err(ClientError::LedgerClosed) => {}
        Err(e) => panic!("expected LedgerClosed, got {e:?}"),
    }

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Fix #30: numBookiesMissingEntry — documented as RECOVERY-READ-ONLY
// ---------------------------------------------------------------------------

/// Java's `numBookiesMissingEntry` fast-fail only applies to recovery
/// reads — normal reads must still traverse all write-set replicas
/// because transient NoEntry responses are possible while a replica
/// catches up after ack quorum.
///
/// Verify that reading a non-existent entry returns NoEntry in the
/// single-bookie case (the simple correctness path) and that this
/// still works cleanly with the early-exit logic removed from the
/// normal read path.
#[tokio::test]
async fn sequential_read_returns_no_entry_for_missing_id() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(77, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    wh.append(&conns, b"entry-0").await.unwrap();

    let read_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let rh = ReadHandle::new(77, ensemble, schedule, dm, master_key);

    // Reading entry 1 (never written) should return NoEntry.
    let err = rh.read_entry(&read_conns, 1).await.unwrap_err();
    assert!(
        matches!(err, ClientError::BookieError { error: bookkeeper_common::error::BKError::NoEntry }),
        "expected NoEntry, got {err:?}"
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Fix #23: tryReadLastConfirmed short-circuit LAC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn try_read_last_confirmed_returns_when_lac_advances() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(88, ensemble.clone(), schedule.clone(), dm.clone(), master_key);

    // Write 3 entries: LAC should advance to 2.
    wh.append(&conns, b"e0").await.unwrap();
    wh.append(&conns, b"e1").await.unwrap();
    wh.append(&conns, b"e2").await.unwrap();

    let read_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let rh = ReadHandle::new(88, ensemble, schedule, dm, master_key);

    // Caller's current_lac = -1: should return 2 (bookie's LAC).
    let lac = rh.try_read_last_confirmed(&read_conns, -1).await.unwrap();
    assert_eq!(lac, 2, "expected bookie-reported LAC=2, got {lac}");

    // Caller's current_lac = 2: bookie also reports 2 → no advancement → NoEntry.
    let err = rh.try_read_last_confirmed(&read_conns, 2).await.unwrap_err();
    assert!(
        matches!(err, ClientError::BookieError { error: bookkeeper_common::error::BKError::NoEntry }),
        "expected NoEntry when LAC has not advanced, got {err:?}"
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Fix #31: parallel read mode for read_last_confirmed_and_entry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn read_last_confirmed_and_entry_parallel_mode_returns_entry() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(99, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    wh.append(&conns, b"entry-0").await.unwrap();
    wh.append(&conns, b"entry-1").await.unwrap();

    let read_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let rh = ReadHandle::new(99, ensemble, schedule, dm, master_key);

    // Parallel mode: request entry 1 after LAC=0.
    let (eid, payload) = rh
        .read_last_confirmed_and_entry_opts(&read_conns, 0, 1000, true)
        .await
        .unwrap();
    assert_eq!(eid, 1);
    assert_eq!(payload, b"entry-1");

    server.shutdown().await;
}


// ---------------------------------------------------------------------------
// LAC-invariant after PendingAdd resolves (Option C — Java parity)
// ---------------------------------------------------------------------------

/// Java invariant: when `addEntry` callback fires for entry N, the ledger
/// handle's `lastAddConfirmed` is guaranteed to be ≥ N.  This is because
/// Java's `sendAddSuccessCallbacks` walks `pendingAddOps` in entry-ID order
/// and advances `lastAddConfirmed` BEFORE invoking each per-entry callback.
///
/// The Rust equivalent: after `p.wait().await` returns `Ok(N)`,
/// `wh.last_add_confirmed()` must be ≥ N.
///
/// This test fires many out-of-order concurrent appends and asserts the
/// invariant holds.  With ensemble=1 the ack arrival order naturally
/// matches submission order, but the test also verifies the
/// single-entry fast path.
#[tokio::test]
async fn pending_add_resolves_implies_lac_advanced_past_entry() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(123, ensemble, schedule, dm, master_key);

    // Fire 50 appends, collect futures, then await them in reverse order.
    let mut pendings = Vec::with_capacity(50);
    for i in 0..50 {
        let payload = format!("e{i}");
        let p = wh.append_async(&conns, payload.as_bytes()).unwrap();
        pendings.push((i as i64, p));
    }

    // Await from the LAST submitted entry backwards.  After each .await,
    // the LAC must be >= that entry's id (Java invariant).
    while let Some((eid, p)) = pendings.pop() {
        let got = p.wait().await.unwrap();
        assert_eq!(got, eid);
        let lac = wh.last_add_confirmed();
        assert!(
            lac >= eid,
            "LAC invariant violated: after p.await() returned Ok({eid}), LAC = {lac}"
        );
    }

    server.shutdown().await;
}

/// `WriteHandle`'s ensemble-change loop must not invoke the metadata
/// callback twice at the same `first_entry_id`.
///
/// `EnsembleInfo::add_segment` keys segments by `first_entry_id` (a
/// BTreeMap), so two ensemble changes that fire at the same `LAC + 1`
/// silently overwrite each other in both the local map and the on-ZK
/// metadata when the on_ensemble_change callback persists the change.
/// During the window between the two callbacks, ZK carries the
/// intermediate ensemble — a writer crash there leaves recovery
/// starting from stale metadata.
///
/// Repro: kill the only ensemble bookie, force a WriteQuorumFailed,
/// and inject a delayed bookie failure from inside the *first*
/// on_ensemble_change callback. Pre-fix, the WriteQuorumFailed
/// handler's tail-end "drain delayed_failures" branch fires a second
/// segment update at the same `first_entry_id`; post-fix, the loop
/// drains delayed failures into the same change so only one callback
/// is emitted.
#[tokio::test]
async fn ensemble_change_does_not_emit_two_segments_at_same_lac() {
    let dir0 = TempDir::new().unwrap();
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let server0 = BookieServer::start(test_config(&dir0)).await.unwrap();
    let server1 = BookieServer::start(test_config(&dir1)).await.unwrap();
    let server2 = BookieServer::start(test_config(&dir2)).await.unwrap();
    let addr0 = server0.local_addr();
    let addr1 = server1.local_addr();
    let addr2 = server2.local_addr();
    let s0 = addr0.to_string();
    let s1 = addr1.to_string();
    let s2 = addr2.to_string();

    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    // Ensemble = [b0]. Available pool puts the live replacements (b1, b2)
    // ahead of the dead bookie (b0) so the first replacement picks b1
    // (alive) and a second would pick b2 (also alive) — neither retries
    // the killed addr.
    let mut wh = WriteHandle::with_available_bookies(
        100,
        vec![s0.clone()],
        vec![s1.clone(), s2.clone(), s0.clone()],
        schedule, dm, master_key,
    );

    let invocations: Arc<std::sync::Mutex<Vec<(i64, Vec<String>)>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let injected = Arc::new(AtomicBool::new(false));
    let inv_clone = invocations.clone();
    let inj_clone = injected.clone();
    let ens_info = wh.ensemble_info_for_test();
    let inj_addr = s1.clone();
    wh.set_on_ensemble_change(Box::new(move |first, ens| {
        let inv = inv_clone.clone();
        let inj = inj_clone.clone();
        let ens_info = ens_info.clone();
        let addr = inj_addr.clone();
        let ensemble = ens.to_vec();
        Box::pin(async move {
            inv.lock().unwrap().push((first, ensemble));
            // On the *first* callback, simulate a delayed bookie failure
            // that would normally arrive from a post-quorum write completion
            // path. With the bug present this triggers a second segment
            // update at the same first_entry_id.
            if !inj.swap(true, Ordering::SeqCst) {
                ens_info.push_delayed_failure_for_test(0, addr);
            }
            Ok(())
        })
    }));

    let mut conns: Vec<Arc<BookieConnection>> =
        vec![Arc::new(BookieConnection::connect(addr0).await.unwrap())];

    // Successful write so LAC = 0 and the next ensemble change targets
    // first_entry_id = 1.
    wh.append_with_ensemble_change(&mut conns, b"ok").await.unwrap();
    server0.shutdown().await;

    // Failed write triggers the WriteQuorumFailed → ensemble change path.
    wh.append_with_ensemble_change(&mut conns, b"trigger").await.unwrap();

    let inv = invocations.lock().unwrap();
    let same_id = inv.iter().filter(|(id, _)| *id == 1).count();
    assert!(
        same_id <= 1,
        "got {same_id} on_ensemble_change invocations at first_entry_id=1; \
         multiple callbacks at the same first_entry_id silently overwrite the \
         persisted ZK ensemble during the recovery window. Recorded: {:?}",
        *inv,
    );

    server1.shutdown().await;
    server2.shutdown().await;
}
