use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::client::BookKeeperClient;
use bookkeeper_client::recovery::RecoveryHandle;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_protocol::digest::{DigestManager, DigestType, generate_master_key};
use bookkeeper_server::server::BookieServer;
use std::sync::Arc;
use tempfile::TempDir;

fn test_config(dir: &TempDir) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config
}

#[tokio::test]
async fn fence_ledger_and_get_lac() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // Write 5 entries as a normal writer
    let mut client = BookKeeperClient::new(vec![addr.to_string()]);
    let mut wh = client.create_ledger(1, 1, 1, 1, DigestType::Crc32c, b"").await.unwrap();
    for i in 0..5 {
        wh.append(client.connections(), format!("entry-{i}").as_bytes()).await.unwrap();
    }
    assert_eq!(wh.last_add_confirmed(), 4);

    // Now recover the ledger (simulating writer crash)
    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");

    let conn = Arc::new(BookieConnection::connect(addr).await.unwrap());
    let conns = vec![conn];

    let mut recovery = RecoveryHandle::new(1, ensemble, schedule, dm, master_key);
    let recovery_lac = recovery.fence(&conns, 0).await.unwrap();
    assert_eq!(recovery_lac, 4);

    server.shutdown().await;
}

#[tokio::test]
async fn fence_blocks_normal_writes() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // Write some entries
    let mut client = BookKeeperClient::new(vec![addr.to_string()]);
    let mut wh = client.create_ledger(1, 1, 1, 1, DigestType::Crc32c, b"").await.unwrap();
    wh.append(client.connections(), b"before-fence").await.unwrap();

    // Fence the ledger
    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");
    let conn = Arc::new(BookieConnection::connect(addr).await.unwrap());
    let mut recovery = RecoveryHandle::new(1, ensemble, schedule, dm, master_key);
    recovery.fence(&vec![conn], 0).await.unwrap();

    // Normal write should now fail with EFENCED
    let result = wh.append(client.connections(), b"after-fence").await;
    assert!(result.is_err());

    server.shutdown().await;
}

#[tokio::test]
async fn full_recovery_read_and_close() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // Write entries
    let mut client = BookKeeperClient::new(vec![addr.to_string()]);
    let mut wh = client.create_ledger(1, 1, 1, 1, DigestType::Crc32c, b"").await.unwrap();
    for i in 0..3 {
        wh.append(client.connections(), format!("data-{i}").as_bytes()).await.unwrap();
    }

    // Recover: fence → forward read → close
    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");
    let conn = Arc::new(BookieConnection::connect(addr).await.unwrap());
    let conns = vec![conn];

    let mut recovery = RecoveryHandle::new(1, ensemble, schedule, dm, master_key);
    let recovery_lac = recovery.fence(&conns, 0).await.unwrap();

    // Forward read to find last entry
    let last_entry = recovery.forward_read(&conns, recovery_lac).await.unwrap();
    assert_eq!(last_entry, 2); // entries 0, 1, 2 were written

    // Recovery add (re-replicate) — in single-bookie case, entries already have quorum
    // Close the ledger
    let (last_entry_id, length) = recovery.close(last_entry).unwrap();
    assert_eq!(last_entry_id, 2);
    assert!(length > 0);

    server.shutdown().await;
}

#[tokio::test]
async fn recovery_add_bypasses_fence() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // Write 1 entry
    let mut client = BookKeeperClient::new(vec![addr.to_string()]);
    let mut wh = client.create_ledger(1, 1, 1, 1, DigestType::Crc32c, b"").await.unwrap();
    wh.append(client.connections(), b"original").await.unwrap();

    // Fence
    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");
    let conn = Arc::new(BookieConnection::connect(addr).await.unwrap());
    let conns = vec![conn];
    let mut recovery = RecoveryHandle::new(1, ensemble, schedule, dm, master_key);
    recovery.fence(&conns, 0).await.unwrap();

    // Recovery add should succeed despite fence
    let entry_id = recovery.recovery_add_entry(&conns, 1, b"recovery-data").await.unwrap();
    assert_eq!(entry_id, 1);

    server.shutdown().await;
}

#[tokio::test]
async fn combined_recover_protocol() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // Write entries
    let mut client = BookKeeperClient::new(vec![addr.to_string()]);
    let mut wh = client.create_ledger(1, 1, 1, 1, DigestType::Crc32c, b"").await.unwrap();
    for i in 0..5 {
        wh.append(client.connections(), format!("data-{i}").as_bytes()).await.unwrap();
    }

    // Run combined recovery
    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");
    let conn = Arc::new(BookieConnection::connect(addr).await.unwrap());
    let conns = vec![conn];

    let mut recovery = RecoveryHandle::new(1, ensemble, schedule, dm, master_key);
    let (last_entry_id, total_length) = recovery.recover(&conns, 0).await.unwrap();
    assert_eq!(last_entry_id, 4);
    assert!(total_length > 0);

    server.shutdown().await;
}
