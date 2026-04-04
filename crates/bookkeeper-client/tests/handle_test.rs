use std::sync::Arc;

use bookkeeper_client::bookie_connection::{BookieConnection, ClientError};
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_client::write_handle::WriteHandle;
use bookkeeper_client::read_handle::ReadHandle;
use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_server::server::BookieServer;
use bookkeeper_server::config::BookieConfig;
use tempfile::TempDir;

fn test_config(dir: &tempfile::TempDir) -> BookieConfig {
    BookieConfig {
        port: 0,
        ledger_dirs: vec![dir.path().to_string_lossy().to_string()],
        journal_dir: dir.path().to_string_lossy().to_string(),
        max_frame_size: 5 * 1024 * 1024,
        zk_connect: None,
        zk_ledger_root: "/ledgers".to_string(),
        bookie_id: None,
        http_port: None,
    }
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
