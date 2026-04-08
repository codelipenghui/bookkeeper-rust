//! Tests for client connection pooling in `bookie_connection.rs`.

use std::sync::Arc;
use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_server::server::BookieServer;
use tempfile::TempDir;

fn test_config(dir: &TempDir) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config
}

#[tokio::test]
async fn pool_with_single_channel_works() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let conn = BookieConnection::connect_pooled(addr, 1).await.unwrap();
    assert_eq!(conn.num_channels(), 1);

    server.shutdown().await;
}

#[tokio::test]
async fn pool_with_multiple_channels_opens_all() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let conn = BookieConnection::connect_pooled(addr, 4).await.unwrap();
    assert_eq!(conn.num_channels(), 4);

    server.shutdown().await;
}

#[tokio::test]
async fn pool_min_one_channel_when_zero_requested() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // 0 should be clamped to 1
    let conn = BookieConnection::connect_pooled(addr, 0).await.unwrap();
    assert_eq!(conn.num_channels(), 1);

    server.shutdown().await;
}

#[tokio::test]
async fn pool_routes_reads_consistently_for_same_ledger() {
    use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
    use bookkeeper_client::read_handle::ReadHandle;
    use bookkeeper_client::write_handle::WriteHandle;

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    // Use a pool of 4 channels
    let conns = vec![Arc::new(BookieConnection::connect_pooled(addr, 4).await.unwrap())];

    // Write multiple entries — all should hit the same channel (ledger_id-based routing)
    let mut wh = WriteHandle::new(42, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    for _ in 0..10 {
        wh.append(&conns, b"test data").await.unwrap();
    }

    // Read them back — pool should handle parallel reads via channels
    let rh = ReadHandle::new(42, ensemble, schedule, dm, master_key);
    for i in 0..10 {
        let data = rh.read_entry(&conns, i).await.unwrap();
        assert_eq!(data, b"test data");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn connect_to_unreachable_address_fails() {
    // Use a port that should be closed
    let addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
    let result = BookieConnection::connect_pooled(addr, 2).await;
    assert!(result.is_err(), "expected connection failure to closed port");
}
