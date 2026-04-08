//! Cross-interop tests: Rust client ↔ Java bookie.
//!
//! These tests spin up a real Java BookKeeper bookie via testcontainers
//! (apachebookkeeper/bookkeeper-current image) and verify that the Rust
//! client can talk to it over the wire protocol.
//!
//! The ZK + Java bookie containers are started **once** per test binary
//! and shared across all tests via a `tokio::sync::OnceCell`. This brings
//! the wall time down from ~60s/test to ~60s for the whole suite, since the
//! JVM bookie's ~30s startup dominates per-container cost.
//!
//! Each test uses its own ledger ID range to avoid colliding state.
//!
//! Skipped gracefully if Docker is unavailable.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::v2::response::V2Response;
use bytes::Bytes;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::sync::OnceCell;
use tokio::time::sleep;

// ---------------------------------------------------------------------------
// Shared fixture
// ---------------------------------------------------------------------------

/// Holds the long-lived containers and the bookie's host-side address.
/// The container handles must outlive every test, so we stash them in a
/// process-global `OnceCell`. They are dropped (and the containers stopped)
/// when the test binary exits.
struct SharedFixture {
    _zk: ContainerAsync<GenericImage>,
    _bookie: ContainerAsync<GenericImage>,
    bookie_addr: SocketAddr,
}

/// Process-global fixture. `None` means we tried to bring it up and Docker
/// (or the bookie) was unavailable — every test then skips gracefully.
static FIXTURE: OnceCell<Option<SharedFixture>> = OnceCell::const_new();

/// Lazily build the shared fixture. Returns the bookie address, or `None`
/// if Docker / the Java bookie image is unavailable on this host.
async fn shared_bookie_addr() -> Option<SocketAddr> {
    let fixture = FIXTURE
        .get_or_init(|| async {
            let (zk, zk_connect) = start_zk().await?;
            init_cluster(&zk_connect).await;
            let (bookie, addr) = start_java_bookie(&zk_connect).await?;
            Some(SharedFixture {
                _zk: zk,
                _bookie: bookie,
                bookie_addr: addr,
            })
        })
        .await;
    fixture.as_ref().map(|f| f.bookie_addr)
}

/// Start a ZooKeeper container and return (container, "host:port").
async fn start_zk() -> Option<(ContainerAsync<GenericImage>, String)> {
    let image = GenericImage::new("zookeeper", "3.8")
        .with_wait_for(WaitFor::message_on_stdout("binding to port"))
        .with_exposed_port(2181.into())
        .with_env_var("ZOO_4LW_COMMANDS_WHITELIST", "*");

    match image.start().await {
        Ok(container) => {
            let port = container.get_host_port_ipv4(2181).await.ok()?;
            Some((container, format!("127.0.0.1:{port}")))
        }
        Err(e) => {
            eprintln!("[SKIP] Docker/zookeeper unavailable: {e}");
            None
        }
    }
}

/// Initialize the BookKeeper cluster in ZK using the Rust admin helper.
async fn init_cluster(connect: &str) {
    use bookkeeper_metadata::traits::{LedgerLayout, RegistrationManager};
    use bookkeeper_metadata::zk::registration::ZkRegistrationManager;

    let mgr = ZkRegistrationManager::new(connect, "/ledgers").await.unwrap();
    let layout = LedgerLayout {
        manager_factory_class: "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory"
            .to_string(),
        manager_version: 1,
    };
    let instance_id = uuid::Uuid::new_v4().to_string();
    mgr.init_new_cluster(&layout, &instance_id).await.unwrap();
}

/// Start a Java BookKeeper bookie wired up to the given ZK.
async fn start_java_bookie(
    zk_host_port: &str,
) -> Option<(ContainerAsync<GenericImage>, SocketAddr)> {
    // The Java bookie connects to ZK via the host's address. On Docker Desktop
    // (macOS/Windows) we use host.docker.internal; on Linux we use the default
    // docker bridge IP. We skip the test if startup fails.
    let port = zk_host_port.rsplit(':').next().unwrap();
    let zk_in_container = if cfg!(target_os = "macos") || cfg!(target_os = "windows") {
        format!("host.docker.internal:{port}")
    } else {
        format!("172.17.0.1:{port}")
    };

    let image = GenericImage::new("apachebookkeeper/bookkeeper-current", "latest")
        .with_wait_for(WaitFor::message_on_stdout(
            "Register myself: /ledgers/available",
        ))
        .with_exposed_port(3181.into())
        .with_env_var("BK_zkServers", &zk_in_container)
        .with_env_var(
            "BK_metadataServiceUri",
            format!("zk+hierarchical://{zk_in_container}/ledgers"),
        )
        .with_env_var("BK_useHostNameAsBookieID", "false")
        .with_env_var("BK_advertisedAddress", "127.0.0.1")
        .with_env_var("BK_httpServerEnabled", "false");

    match tokio::time::timeout(Duration::from_secs(120), image.start()).await {
        Ok(Ok(container)) => {
            let port = container.get_host_port_ipv4(3181.tcp()).await.ok()?;
            let addr = format!("127.0.0.1:{port}").parse().ok()?;
            // Allow the bookie a moment to be fully ready for client connections
            sleep(Duration::from_secs(1)).await;
            Some((container, addr))
        }
        Ok(Err(e)) => {
            eprintln!("[SKIP] Java bookie container failed: {e}");
            None
        }
        Err(_) => {
            eprintln!("[SKIP] Java bookie container startup timed out");
            None
        }
    }
}

fn make_entry(ledger_id: i64, entry_id: i64, lac: i64, payload: &[u8]) -> Vec<u8> {
    let mut entry = Vec::new();
    entry.extend_from_slice(&ledger_id.to_be_bytes());
    entry.extend_from_slice(&entry_id.to_be_bytes());
    entry.extend_from_slice(&lac.to_be_bytes());
    entry.extend_from_slice(&(payload.len() as i64).to_be_bytes());
    entry.extend_from_slice(payload);
    entry
}

// ---------------------------------------------------------------------------
// Tests — each uses its own ledger ID so they can run against the shared bookie.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rust_client_connects_to_java_bookie() {
    let Some(addr) = shared_bookie_addr().await else { return; };

    let conn = BookieConnection::connect(addr).await;
    assert!(conn.is_ok(), "Rust client should connect to Java bookie");
}

#[tokio::test]
async fn rust_client_writes_to_java_bookie_via_v2() {
    let Some(addr) = shared_bookie_addr().await else { return; };

    let conn = Arc::new(BookieConnection::connect(addr).await.unwrap());
    let master_key = bookkeeper_protocol::digest::generate_master_key(b"");

    // Ledger 1 — used only by this test
    let entry_data = make_entry(1, 0, -1, b"hello from rust");
    let resp = conn
        .send_add_entry(0, master_key, Bytes::from(entry_data))
        .await
        .unwrap();

    match resp {
        V2Response::Add { error_code, ledger_id, entry_id } => {
            assert_eq!(error_code, V2_EOK, "Java bookie should accept the add");
            assert_eq!(ledger_id, 1);
            assert_eq!(entry_id, 0);
        }
        other => panic!("expected Add response, got {other:?}"),
    }
}

#[tokio::test]
async fn rust_client_reads_from_java_bookie() {
    let Some(addr) = shared_bookie_addr().await else { return; };

    let conn = Arc::new(BookieConnection::connect(addr).await.unwrap());
    let master_key = bookkeeper_protocol::digest::generate_master_key(b"");

    // Ledger 2 — used only by this test
    for i in 0..5 {
        let entry = make_entry(2, i, i - 1, format!("entry-{i}").as_bytes());
        let resp = conn
            .send_add_entry(0, master_key, Bytes::from(entry))
            .await
            .unwrap();
        match resp {
            V2Response::Add { error_code, .. } => assert_eq!(error_code, V2_EOK),
            other => panic!("expected Add, got {other:?}"),
        }
    }

    for i in 0..5 {
        let resp = conn
            .send_read_entry(0, 2, i, Some(master_key))
            .await
            .unwrap();
        match resp {
            V2Response::Read { error_code, data, .. } => {
                assert_eq!(error_code, V2_EOK, "entry {i} should be readable");
                let data = data.unwrap();
                // Verify the payload portion (skip 32-byte header)
                assert!(data.len() >= 32);
                let payload = &data[32..];
                assert!(
                    payload.ends_with(format!("entry-{i}").as_bytes()),
                    "payload for entry {i} should match"
                );
            }
            other => panic!("expected Read, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn rust_client_get_bookie_info_from_java_bookie() {
    let Some(addr) = shared_bookie_addr().await else { return; };

    let conn = Arc::new(BookieConnection::connect(addr).await.unwrap());

    // Request both free + total disk (bitmask 0x3) — read-only, no ledger needed
    let resp = conn.send_get_bookie_info(0x3).await.unwrap();
    match resp {
        V2Response::GetBookieInfo {
            error_code,
            free_disk_space,
            total_disk_capacity,
            ..
        } => {
            assert_eq!(error_code, V2_EOK);
            assert!(total_disk_capacity > 0, "total disk should be positive");
            assert!(free_disk_space > 0, "free disk should be positive");
            assert!(free_disk_space <= total_disk_capacity);
        }
        other => panic!("expected GetBookieInfo, got {other:?}"),
    }
}
