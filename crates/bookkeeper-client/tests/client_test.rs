use bookkeeper_client::client::BookKeeperClient;
use bookkeeper_protocol::digest::DigestType;
use bookkeeper_server::server::BookieServer;
use bookkeeper_server::config::BookieConfig;
use tempfile::TempDir;

fn test_config(dir: &TempDir) -> BookieConfig {
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
async fn create_write_read_close() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let mut client = BookKeeperClient::new(vec![addr.to_string()]);

    // Create ledger
    let ledger_id = 1;
    let mut wh = client.create_ledger(ledger_id, 1, 1, 1, DigestType::Crc32c, b"")
        .await.unwrap();

    // Write entries
    wh.append(client.connections(), b"entry-0").await.unwrap();
    wh.append(client.connections(), b"entry-1").await.unwrap();
    wh.append(client.connections(), b"entry-2").await.unwrap();

    assert_eq!(wh.last_add_confirmed(), 2);

    // Read entries
    let rh = client.open_ledger(ledger_id, 1, 1, 1, DigestType::Crc32c, b"");
    let data = rh.read_entry(client.connections(), 0).await.unwrap();
    assert_eq!(data, b"entry-0");
    let data = rh.read_entry(client.connections(), 2).await.unwrap();
    assert_eq!(data, b"entry-2");

    server.shutdown().await;
}

#[tokio::test]
async fn write_then_read_all() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let mut client = BookKeeperClient::new(vec![addr.to_string()]);
    let ledger_id = 2;
    let mut wh = client.create_ledger(ledger_id, 1, 1, 1, DigestType::Crc32c, b"")
        .await.unwrap();

    for i in 0..5 {
        wh.append(client.connections(), format!("data-{i}").as_bytes()).await.unwrap();
    }

    let rh = client.open_ledger(ledger_id, 1, 1, 1, DigestType::Crc32c, b"");
    let entries = rh.read_entries(client.connections(), 0, 4).await.unwrap();
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[0], b"data-0");
    assert_eq!(entries[4], b"data-4");

    server.shutdown().await;
}
