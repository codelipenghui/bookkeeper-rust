//! Integration tests for ZkBookKeeper client.
//!
//! These tests require a real ZooKeeper instance. By default they connect to
//! localhost:2181, but you can override via the ZK_CONNECT environment variable.
//!
//! To use the embedded ZK from the Java compat test suite:
//!   cd tests/java-compat && bash run-tests.sh   # starts embedded ZK
//!   # In another terminal:
//!   ZK_CONNECT=localhost:<port> cargo test -p bookkeeper-client --test zk_client_test -- --ignored
//!
//! Or with a Docker ZK:
//!   docker run -d -p 2181:2181 zookeeper:3.9
//!   cargo test -p bookkeeper-client --test zk_client_test -- --ignored

use bookkeeper_client::zk_client::ZkBookKeeper;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_metadata::traits::RegistrationManager;
use bookkeeper_metadata::zk::registration::ZkRegistrationManager;
use bookkeeper_protocol::digest::DigestType;
use bookkeeper_server::server::BookieServer;
use tempfile::TempDir;

fn zk_connect() -> String {
    std::env::var("ZK_CONNECT").unwrap_or_else(|_| "localhost:2181".to_string())
}

/// Create a bookie config that registers with ZK.
fn zk_bookie_config(dir: &TempDir, ledger_root: &str) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config.zk_servers = Some(zk_connect());
    config.zk_ledger_root = ledger_root.to_string();
    config
}

/// Initialize a fresh cluster in ZK.
async fn init_cluster(ledger_root: &str) {
    let mgr = ZkRegistrationManager::new(&zk_connect(), ledger_root)
        .await
        .unwrap();
    let layout = bookkeeper_metadata::traits::LedgerLayout {
        manager_factory_class:
            "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory".to_string(),
        manager_version: 1,
    };
    let instance_id = uuid::Uuid::new_v4().to_string();
    let _ = mgr.init_new_cluster(&layout, &instance_id).await;
}

/// Full lifecycle: create → write → close → open → read → delete
#[tokio::test]
#[ignore]
async fn zk_create_write_close_open_read_delete() {
    let root = "/test-lifecycle";
    init_cluster(root).await;

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(zk_bookie_config(&dir, root)).await.unwrap();
    let addr = server.local_addr();
    println!("Bookie started at {addr}");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut bk = ZkBookKeeper::new(&zk_connect(), root).await.unwrap();

    // Create ledger
    let password = b"test-password";
    let (mut wh, conns) = bk
        .create_ledger(1, 1, 1, DigestType::Crc32c, password)
        .await
        .unwrap();
    let ledger_id = wh.ledger_id();
    println!("Created ledger {ledger_id}");
    assert!(ledger_id >= 0);

    // Write entries
    let num_entries = 10;
    for i in 0..num_entries {
        wh.append(&conns, format!("entry-{i}").as_bytes()).await.unwrap();
    }
    assert_eq!(wh.last_add_confirmed(), num_entries - 1);
    println!("Wrote {num_entries} entries, LAC={}", wh.last_add_confirmed());

    // Close ledger
    bk.close_ledger(&wh).await.unwrap();
    println!("Closed ledger {ledger_id}");

    // Open and read
    let (rh, read_conns) = bk
        .open_ledger(ledger_id, DigestType::Crc32c, password)
        .await
        .unwrap();
    assert_eq!(rh.ledger_id(), ledger_id);

    for i in 0..num_entries {
        let data = rh.read_entry(&read_conns, i).await.unwrap();
        assert_eq!(data, format!("entry-{i}").as_bytes(), "mismatch at entry {i}");
    }
    println!("Read and verified {num_entries} entries");

    // Delete
    bk.delete_ledger(ledger_id).await.unwrap();
    let result = bk.open_ledger(ledger_id, DigestType::Crc32c, password).await;
    assert!(result.is_err(), "expected open to fail after deletion");
    println!("Verified ledger is gone after deletion");

    server.shutdown().await;
    println!("Test passed!");
}

/// Test creating multiple ledgers and reading them independently.
#[tokio::test]
#[ignore]
async fn zk_multiple_ledgers() {
    let root = "/test-multi";
    init_cluster(root).await;

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(zk_bookie_config(&dir, root)).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut bk = ZkBookKeeper::new(&zk_connect(), root).await.unwrap();

    let password = b"multi-test";
    let mut ledger_ids = Vec::new();

    for l in 0..3 {
        let (mut wh, conns) = bk
            .create_ledger(1, 1, 1, DigestType::Crc32c, password)
            .await
            .unwrap();
        let lid = wh.ledger_id();
        ledger_ids.push(lid);

        for i in 0..5 {
            wh.append(&conns, format!("ledger-{l}-entry-{i}").as_bytes())
                .await
                .unwrap();
        }
        bk.close_ledger(&wh).await.unwrap();
    }

    for (l, lid) in ledger_ids.iter().enumerate() {
        let (rh, conns) = bk
            .open_ledger(*lid, DigestType::Crc32c, password)
            .await
            .unwrap();
        for i in 0..5 {
            let data = rh.read_entry(&conns, i).await.unwrap();
            assert_eq!(data, format!("ledger-{l}-entry-{i}").as_bytes());
        }
    }

    for lid in &ledger_ids {
        bk.delete_ledger(*lid).await.unwrap();
    }

    server.shutdown().await;
}

/// Test open with recovery on an unclosed ledger.
#[tokio::test]
#[ignore]
async fn zk_open_with_recovery() {
    let root = "/test-recovery";
    init_cluster(root).await;

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(zk_bookie_config(&dir, root)).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut bk = ZkBookKeeper::new(&zk_connect(), root).await.unwrap();
    let password = b"recovery-test";

    // Create and write without closing (simulate crash)
    let (mut wh, conns) = bk
        .create_ledger(1, 1, 1, DigestType::Crc32c, password)
        .await
        .unwrap();
    let ledger_id = wh.ledger_id();

    for i in 0..5 {
        wh.append(&conns, format!("data-{i}").as_bytes()).await.unwrap();
    }
    assert_eq!(wh.last_add_confirmed(), 4);
    drop(wh);
    drop(conns);

    // Open with recovery
    let (rh, recovery_conns) = bk
        .open_ledger_with_recovery(ledger_id, DigestType::Crc32c, password)
        .await
        .unwrap();

    for i in 0..5 {
        let data = rh.read_entry(&recovery_conns, i).await.unwrap();
        assert_eq!(data, format!("data-{i}").as_bytes());
    }

    // Verify metadata is now CLOSED
    let (rh2, conns2) = bk
        .open_ledger(ledger_id, DigestType::Crc32c, password)
        .await
        .unwrap();
    assert_eq!(rh2.read_entry(&conns2, 0).await.unwrap(), b"data-0");

    bk.delete_ledger(ledger_id).await.unwrap();
    server.shutdown().await;
}

/// Test password verification on open.
#[tokio::test]
#[ignore]
async fn zk_password_mismatch() {
    let root = "/test-password";
    init_cluster(root).await;

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(zk_bookie_config(&dir, root)).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut bk = ZkBookKeeper::new(&zk_connect(), root).await.unwrap();

    let (mut wh, conns) = bk
        .create_ledger(1, 1, 1, DigestType::Crc32c, b"correct-password")
        .await
        .unwrap();
    let ledger_id = wh.ledger_id();
    wh.append(&conns, b"secret-data").await.unwrap();
    bk.close_ledger(&wh).await.unwrap();

    let result = bk
        .open_ledger(ledger_id, DigestType::Crc32c, b"wrong-password")
        .await;
    assert!(result.is_err(), "expected password mismatch error");

    bk.delete_ledger(ledger_id).await.unwrap();
    server.shutdown().await;
}
