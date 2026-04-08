//! End-to-end tests for all 4 digest types using the Rust client + Rust bookie.
//! Previously only Crc32c was tested at this level.

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

async fn write_read_with_digest(digest_type: DigestType, password: &[u8], ledger_id: i64) {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(digest_type).with_password(password);
    // Master key is SHA1("ledger" + password)
    let master_key = bookkeeper_protocol::digest::generate_master_key(password);

    // Write
    let conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let mut wh = WriteHandle::new(ledger_id, ensemble.clone(), schedule.clone(), dm.clone(), master_key);

    let payloads = [
        &b"first entry"[..],
        &b"second entry"[..],
        &b""[..], // empty payload
        &[0xff_u8; 1024][..], // larger payload
    ];
    for p in &payloads {
        wh.append(&conns, p).await.unwrap();
    }

    // Read back
    let read_conns = vec![Arc::new(BookieConnection::connect(addr).await.unwrap())];
    let rh = ReadHandle::new(ledger_id, ensemble, schedule, dm, master_key);
    for (i, expected) in payloads.iter().enumerate() {
        let actual = rh.read_entry(&read_conns, i as i64).await.unwrap();
        assert_eq!(&actual[..], *expected, "digest {digest_type:?} entry {i} mismatch");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn crc32_write_read() {
    write_read_with_digest(DigestType::Crc32, b"password-crc32", 100).await;
}

#[tokio::test]
async fn crc32c_write_read() {
    write_read_with_digest(DigestType::Crc32c, b"password-crc32c", 200).await;
}

#[tokio::test]
async fn hmac_sha1_write_read() {
    write_read_with_digest(DigestType::HmacSha1, b"password-hmac", 300).await;
}

#[tokio::test]
async fn dummy_digest_write_read() {
    write_read_with_digest(DigestType::Dummy, b"", 400).await;
}

#[tokio::test]
async fn dummy_digest_with_password_still_works() {
    // Dummy digest ignores password but should still allow reading
    write_read_with_digest(DigestType::Dummy, b"some-pw", 401).await;
}
