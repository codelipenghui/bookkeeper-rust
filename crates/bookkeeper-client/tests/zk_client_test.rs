//! End-to-end tests for `ZkBookKeeper` against a real ZooKeeper.
//!
//! These exercise the full ZK-integrated client lifecycle:
//!   - create / write / close / open / read / delete
//!   - multiple concurrent ledgers
//!   - open-with-recovery on an unclosed ledger
//!   - password mismatch rejection
//!
//! All tests share a single ZK container (started lazily on first use via
//! `OnceCell`) and isolate themselves via per-test ZK ledger root paths,
//! so they can run in parallel without collisions. The bookies are spawned
//! in-process per test, with a fresh tmp dir per bookie.
//!
//! Skipped gracefully if Docker is unavailable.

use std::sync::Arc;
use std::time::Duration;

use bookkeeper_client::zk_client::ZkBookKeeper;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_metadata::traits::{LedgerLayout, RegistrationManager};
use bookkeeper_metadata::zk::registration::ZkRegistrationManager;
use bookkeeper_protocol::digest::DigestType;
use bookkeeper_server::server::BookieServer;
use tempfile::TempDir;
use testcontainers::{
    core::WaitFor, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt,
};
use tokio::sync::OnceCell;

// ---------------------------------------------------------------------------
// Shared ZK fixture
// ---------------------------------------------------------------------------

/// Holds the long-lived ZK container and its host:port. The container handle
/// must outlive every test, so it sits in a process-global `OnceCell` and is
/// dropped when the test binary exits.
struct ZkFixture {
    _container: ContainerAsync<GenericImage>,
    connect: String,
}

static ZK: OnceCell<Option<Arc<ZkFixture>>> = OnceCell::const_new();

/// Lazily start (or reuse) the shared ZK container. Returns `Some(connect)`
/// if Docker is available, `None` otherwise — every test then skips cleanly.
async fn shared_zk() -> Option<String> {
    let fixture = ZK
        .get_or_init(|| async {
            let image = GenericImage::new("zookeeper", "3.8")
                .with_wait_for(WaitFor::message_on_stdout("binding to port"))
                .with_exposed_port(2181.into())
                .with_env_var("ZOO_4LW_COMMANDS_WHITELIST", "*");

            match image.start().await {
                Ok(container) => {
                    let port = container.get_host_port_ipv4(2181).await.ok()?;
                    Some(Arc::new(ZkFixture {
                        _container: container,
                        connect: format!("127.0.0.1:{port}"),
                    }))
                }
                Err(e) => {
                    eprintln!("[SKIP] Docker/zookeeper unavailable: {e}");
                    None
                }
            }
        })
        .await;
    fixture.as_ref().map(|f| f.connect.clone())
}

/// Initialize a fresh BK cluster at the given ZK root.
async fn init_cluster(connect: &str, ledger_root: &str) {
    let mgr = ZkRegistrationManager::new(connect, ledger_root)
        .await
        .unwrap();
    let layout = LedgerLayout {
        manager_factory_class:
            "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory".to_string(),
        manager_version: 1,
    };
    let instance_id = uuid::Uuid::new_v4().to_string();
    mgr.init_new_cluster(&layout, &instance_id).await.unwrap();
}

/// Build a bookie config that registers with ZK at the given ledger root.
fn zk_bookie_config(dir: &TempDir, connect: &str, ledger_root: &str) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.listening_interface = Some("127.0.0.1".to_string());
    // Disable disk-based read-only mode: the default 95% threshold trips on
    // dev machines whose root volume is more full than that, leaving the
    // bookie stuck in ReadOnly at startup and breaking the write path.
    config.read_only_mode_enabled = false;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config.zk_servers = Some(connect.to_string());
    config.zk_ledger_root = ledger_root.to_string();
    config
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Full lifecycle: create → write → close → open → read → delete.
#[tokio::test]
async fn zk_create_write_close_open_read_delete() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test-lifecycle";
    init_cluster(&connect, root).await;

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(zk_bookie_config(&dir, &connect, root))
        .await
        .unwrap();
    // Brief settle for ZK ephemeral registration
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut bk = ZkBookKeeper::new(&connect, root).await.unwrap();

    let password = b"test-password";
    let (mut wh, conns) = bk
        .create_ledger(1, 1, 1, DigestType::Crc32c, password)
        .await
        .unwrap();
    let ledger_id = wh.ledger_id();
    assert!(ledger_id >= 0);

    let num_entries = 10;
    for i in 0..num_entries {
        wh.append(&conns, format!("entry-{i}").as_bytes())
            .await
            .unwrap();
    }
    assert_eq!(wh.last_add_confirmed(), num_entries - 1);

    bk.close_ledger(&wh).await.unwrap();

    let (rh, read_conns) = bk
        .open_ledger(ledger_id, DigestType::Crc32c, password)
        .await
        .unwrap();
    assert_eq!(rh.ledger_id(), ledger_id);
    for i in 0..num_entries {
        let data = rh.read_entry(&read_conns, i).await.unwrap();
        assert_eq!(
            data,
            format!("entry-{i}").as_bytes(),
            "mismatch at entry {i}"
        );
    }

    bk.delete_ledger(ledger_id).await.unwrap();
    let result = bk.open_ledger(ledger_id, DigestType::Crc32c, password).await;
    assert!(result.is_err(), "expected open to fail after deletion");

    server.shutdown().await;
}

/// Multiple ledgers on the same bookie are independent.
#[tokio::test]
async fn zk_multiple_ledgers() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test-multi";
    init_cluster(&connect, root).await;

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(zk_bookie_config(&dir, &connect, root))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut bk = ZkBookKeeper::new(&connect, root).await.unwrap();
    let password = b"multi-test";
    let mut ledger_ids = Vec::new();

    for l in 0..3 {
        let (mut wh, conns) = bk
            .create_ledger(1, 1, 1, DigestType::Crc32c, password)
            .await
            .unwrap();
        ledger_ids.push(wh.ledger_id());
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

/// Open with recovery on an unclosed ledger seals it and exposes all entries.
#[tokio::test]
async fn zk_open_with_recovery() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test-recovery";
    init_cluster(&connect, root).await;

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(zk_bookie_config(&dir, &connect, root))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut bk = ZkBookKeeper::new(&connect, root).await.unwrap();
    let password = b"recovery-test";

    // Write entries without closing (simulate writer crash)
    let (mut wh, conns) = bk
        .create_ledger(1, 1, 1, DigestType::Crc32c, password)
        .await
        .unwrap();
    let ledger_id = wh.ledger_id();
    for i in 0..5 {
        wh.append(&conns, format!("data-{i}").as_bytes())
            .await
            .unwrap();
    }
    assert_eq!(wh.last_add_confirmed(), 4);
    drop(wh);
    drop(conns);

    // Open with recovery — fences the ledger and exposes the committed entries
    let (rh, recovery_conns) = bk
        .open_ledger_with_recovery(ledger_id, DigestType::Crc32c, password)
        .await
        .unwrap();
    for i in 0..5 {
        let data = rh.read_entry(&recovery_conns, i).await.unwrap();
        assert_eq!(data, format!("data-{i}").as_bytes());
    }

    // After recovery the ledger metadata should be CLOSED — a plain open works
    let (rh2, conns2) = bk
        .open_ledger(ledger_id, DigestType::Crc32c, password)
        .await
        .unwrap();
    assert_eq!(rh2.read_entry(&conns2, 0).await.unwrap(), b"data-0");

    bk.delete_ledger(ledger_id).await.unwrap();
    server.shutdown().await;
}

/// Opening a ledger with the wrong password is rejected.
#[tokio::test]
async fn zk_password_mismatch() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test-password";
    init_cluster(&connect, root).await;

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(zk_bookie_config(&dir, &connect, root))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut bk = ZkBookKeeper::new(&connect, root).await.unwrap();
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
