use bookkeeper_client::bookie_connection::BookieConnection;
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
    let mut conns = vec![BookieConnection::connect(addr).await.unwrap()];
    let mut write_handle = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    let entry_id = write_handle.append(&mut conns, b"hello world").await.unwrap();
    assert_eq!(entry_id, 0);

    // Read
    let mut read_conns = vec![BookieConnection::connect(addr).await.unwrap()];
    let read_handle = ReadHandle::new(1, ensemble, schedule, dm, master_key);
    let payload = read_handle.read_entry(&mut read_conns, 0).await.unwrap();
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

    let mut conns = vec![BookieConnection::connect(addr).await.unwrap()];
    let mut write_handle = WriteHandle::new(1, ensemble, schedule, dm, master_key);

    for i in 0..10 {
        let data = format!("entry-{}", i);
        let eid = write_handle.append(&mut conns, data.as_bytes()).await.unwrap();
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

    let mut conns = vec![BookieConnection::connect(addr).await.unwrap()];
    let mut wh = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    wh.append(&mut conns, b"verified data").await.unwrap();

    let mut read_conns = vec![BookieConnection::connect(addr).await.unwrap()];
    let rh = ReadHandle::new(1, ensemble, schedule, dm, master_key);
    let payload = rh.read_entry(&mut read_conns, 0).await.unwrap();
    assert_eq!(payload, b"verified data");

    server.shutdown().await;
}
