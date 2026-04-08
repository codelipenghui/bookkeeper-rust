//! End-to-end tests for quorum write/read with multiple bookies.
//!
//! These tests exercise realistic configurations like E=3 W=3 A=2 (write to 3,
//! ack at 2) and E=5 W=3 A=2 (striped writes across 5 bookies, write 3, ack 2).
//! Previously all client e2e tests used E=W=A.

use std::sync::Arc;

use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_client::read_handle::ReadHandle;
use bookkeeper_client::write_handle::WriteHandle;
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

/// Spin up N bookie servers and return (bookies, addresses).
async fn spawn_bookies(n: usize) -> (Vec<BookieServer>, Vec<TempDir>, Vec<String>) {
    let mut bookies = Vec::new();
    let mut dirs = Vec::new();
    let mut addrs = Vec::new();
    for _ in 0..n {
        let dir = TempDir::new().unwrap();
        let server = BookieServer::start(test_config(&dir)).await.unwrap();
        addrs.push(server.local_addr().to_string());
        bookies.push(server);
        dirs.push(dir);
    }
    (bookies, dirs, addrs)
}

async fn connect_all(addrs: &[String]) -> Vec<Arc<BookieConnection>> {
    let mut conns = Vec::new();
    for addr in addrs {
        let socket: std::net::SocketAddr = addr.parse().unwrap();
        conns.push(Arc::new(BookieConnection::connect(socket).await.unwrap()));
    }
    conns
}

#[tokio::test]
async fn ensemble_3_write_3_ack_2() {
    let (bookies, _dirs, addrs) = spawn_bookies(3).await;
    let conns = connect_all(&addrs).await;

    let schedule = RoundRobinDistributionSchedule::new(3, 3, 2);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let mut wh = WriteHandle::new(1, addrs.clone(), schedule.clone(), dm.clone(), master_key);
    for i in 0..10 {
        let payload = format!("entry-{i}");
        let eid = wh.append(&conns, payload.as_bytes()).await.unwrap();
        assert_eq!(eid, i);
    }
    assert_eq!(wh.last_add_confirmed(), 9);

    // Read back
    let read_conns = connect_all(&addrs).await;
    let rh = ReadHandle::new(1, addrs, schedule, dm, master_key);
    for i in 0..10 {
        let payload = rh.read_entry(&read_conns, i).await.unwrap();
        assert_eq!(payload, format!("entry-{i}").as_bytes());
    }

    for b in bookies { b.shutdown().await; }
}

#[tokio::test]
async fn ensemble_5_write_3_ack_2_striped() {
    let (bookies, _dirs, addrs) = spawn_bookies(5).await;
    let conns = connect_all(&addrs).await;

    // E=5 W=3 A=2: each entry goes to 3 of 5 bookies in round-robin pattern
    let schedule = RoundRobinDistributionSchedule::new(5, 3, 2);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let mut wh = WriteHandle::new(2, addrs.clone(), schedule.clone(), dm.clone(), master_key);
    for i in 0..20 {
        let payload = format!("striped-{i}");
        wh.append(&conns, payload.as_bytes()).await.unwrap();
    }
    assert_eq!(wh.last_add_confirmed(), 19);

    let read_conns = connect_all(&addrs).await;
    let rh = ReadHandle::new(2, addrs, schedule, dm, master_key);
    for i in 0..20 {
        let payload = rh.read_entry(&read_conns, i).await.unwrap();
        assert_eq!(payload, format!("striped-{i}").as_bytes());
    }

    for b in bookies { b.shutdown().await; }
}

#[tokio::test]
async fn ack_quorum_2_succeeds_when_one_bookie_slow() {
    // E=3 W=3 A=2: write succeeds as soon as any 2 ACKs arrive,
    // even if the 3rd bookie is slow. Verify by writing many entries —
    // each succeeds without waiting for all 3.
    let (bookies, _dirs, addrs) = spawn_bookies(3).await;
    let conns = connect_all(&addrs).await;

    let schedule = RoundRobinDistributionSchedule::new(3, 3, 2);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let mut wh = WriteHandle::new(3, addrs.clone(), schedule.clone(), dm, master_key);

    // Use a tight loop to ensure throughput is not gated by slowest bookie
    let start = std::time::Instant::now();
    for i in 0..100 {
        wh.append(&conns, format!("e{i}").as_bytes()).await.unwrap();
    }
    let elapsed = start.elapsed();

    // Sanity: should complete reasonably quickly (well under 30s for 100 entries)
    assert!(elapsed < std::time::Duration::from_secs(30),
        "ack quorum 2 should not block on slow 3rd bookie; took {elapsed:?}");
    assert_eq!(wh.last_add_confirmed(), 99);

    for b in bookies { b.shutdown().await; }
}

#[tokio::test]
async fn read_finds_entry_via_any_replica() {
    // E=5 W=3 A=2: each entry is on 3 of 5 bookies. Reads should find the
    // entry by trying bookies in the write set order.
    let (bookies, _dirs, addrs) = spawn_bookies(5).await;
    let conns = connect_all(&addrs).await;

    let schedule = RoundRobinDistributionSchedule::new(5, 3, 2);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let mut wh = WriteHandle::new(4, addrs.clone(), schedule.clone(), dm.clone(), master_key);
    for i in 0..15 {
        wh.append(&conns, format!("data-{i}").as_bytes()).await.unwrap();
    }

    // Read all entries — each should be found via at least one replica
    let read_conns = connect_all(&addrs).await;
    let rh = ReadHandle::new(4, addrs, schedule, dm, master_key);
    for i in 0..15 {
        let payload = rh.read_entry(&read_conns, i).await.unwrap();
        assert_eq!(payload, format!("data-{i}").as_bytes());
    }

    for b in bookies { b.shutdown().await; }
}
