//! Shared test fixtures for BookKeeper Rust integration tests.
//!
//! This crate exists solely to eliminate cross-test duplication of ZK
//! container startup, cluster bootstrapping, bookie spawning, and ledger
//! creation. It is used only as a `dev-dependency` — production code has
//! no reason to depend on it.
//!
//! Typical usage from an integration test:
//!
//! ```ignore
//! use bookkeeper_test_utils::{shared_zk, bootstrap_cluster, spawn_bookie};
//!
//! #[tokio::test]
//! async fn my_test() {
//!     let Some(connect) = shared_zk().await else { return; };
//!     let root = "/test_mytest";
//!     bootstrap_cluster(&connect, root).await;
//!     let (bookie, _dir) = spawn_bookie(&connect, root).await;
//!     // ... run assertions against `bookie`
//!     bookie.shutdown().await;
//! }
//! ```
//!
//! The `shared_zk()` helper starts a single ZK container per test binary
//! (via `tokio::sync::OnceCell`) and reuses it across all tests in that
//! binary. Each test is responsible for using a unique ZK root path
//! (`/test_<name>`) to avoid colliding with sibling tests.

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
// ZooKeeper container
// ---------------------------------------------------------------------------

/// Long-lived handle to a ZooKeeper container plus its host-side connect
/// string. Typically accessed indirectly via [`shared_zk`].
pub struct ZkFixture {
    _container: ContainerAsync<GenericImage>,
    /// `host:port` string suitable for `zookeeper_client::Client::connect`.
    pub connect: String,
}

/// Process-global ZK fixture. `None` means we attempted to start Docker
/// and failed — subsequent callers then see `None` and skip gracefully.
static SHARED_ZK: OnceCell<Option<Arc<ZkFixture>>> = OnceCell::const_new();

/// Return a ZK connect string from a container shared across all tests
/// in the current test binary.
///
/// The container is started lazily on the first call and reused on every
/// subsequent call. Returns `None` if Docker is unavailable — callers
/// should early-return in that case (graceful skip pattern):
///
/// ```ignore
/// let Some(connect) = shared_zk().await else { return; };
/// ```
pub async fn shared_zk() -> Option<String> {
    let fixture = SHARED_ZK
        .get_or_init(|| async { start_zk_container().await })
        .await;
    fixture.as_ref().map(|f| f.connect.clone())
}

/// Start a fresh, non-shared ZooKeeper container and return the handle
/// plus its connect string. Use this only when a test needs strict
/// isolation from other tests in the same binary; otherwise prefer
/// [`shared_zk`] which is much faster.
pub async fn start_zk() -> Option<(ContainerAsync<GenericImage>, String)> {
    let request = GenericImage::new("zookeeper", "3.8")
        .with_wait_for(WaitFor::message_on_stdout("binding to port"))
        .with_exposed_port(2181.into())
        .with_env_var("ZOO_4LW_COMMANDS_WHITELIST", "*");
    match request.start().await {
        Ok(container) => {
            let port = container.get_host_port_ipv4(2181).await.ok()?;
            Some((container, format!("127.0.0.1:{port}")))
        }
        Err(e) => {
            eprintln!("[SKIP] Docker/ZooKeeper unavailable: {e}");
            None
        }
    }
}

async fn start_zk_container() -> Option<Arc<ZkFixture>> {
    let (container, connect) = start_zk().await?;
    Some(Arc::new(ZkFixture {
        _container: container,
        connect,
    }))
}

// ---------------------------------------------------------------------------
// Cluster bootstrap
// ---------------------------------------------------------------------------

/// Initialize a fresh BookKeeper cluster layout znode at `root` in the
/// given ZK. Uses the `HierarchicalLedgerManagerFactory` and a random UUID
/// as the cluster instance ID.
pub async fn bootstrap_cluster(connect: &str, root: &str) {
    let mgr = ZkRegistrationManager::new(connect, root).await.unwrap();
    let layout = LedgerLayout {
        manager_factory_class:
            "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory".to_string(),
        manager_version: 1,
    };
    mgr.init_new_cluster(&layout, &uuid::Uuid::new_v4().to_string())
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Bookie spawning
// ---------------------------------------------------------------------------

/// Spawn a single in-process Rust bookie that registers with the given ZK.
///
/// The returned `TempDir` holds the bookie's data; keep it alive for the
/// duration of the test (usually via a `let (_server, _dir) = ...` binding).
/// Includes a brief settle delay so the bookie's ephemeral ZK registration
/// has time to land before the test uses it.
pub async fn spawn_bookie(connect: &str, root: &str) -> (BookieServer, TempDir) {
    let (server, dir) = spawn_bookie_raw(connect, root, |_| {}).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    (server, dir)
}

/// Spawn a single in-process bookie with the HTTP admin API enabled on an
/// OS-assigned port. Use [`BookieServer::http_addr`] to get the address.
pub async fn spawn_bookie_with_http(connect: &str, root: &str) -> (BookieServer, TempDir) {
    let (server, dir) = spawn_bookie_raw(connect, root, |config| {
        config.http_server_enabled = true;
        config.http_server_port = 0;
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    (server, dir)
}

/// Spawn a single in-process bookie, applying a config customization
/// callback before starting. Useful for tests that need specific config
/// (e.g., enabling TLS, changing thresholds).
///
/// Includes a settle delay for ZK registration.
pub async fn spawn_bookie_with<F>(connect: &str, root: &str, customize: F) -> (BookieServer, TempDir)
where
    F: FnOnce(&mut ServerConfig),
{
    let (server, dir) = spawn_bookie_raw(connect, root, customize).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    (server, dir)
}

/// Spawn N in-process bookies in sequence. Returns the servers, their
/// temp dirs, and their registered addresses. Has a single settle delay
/// at the end (not per-bookie) to keep the cost O(1) rather than O(N).
pub async fn spawn_bookies(
    n: usize,
    connect: &str,
    root: &str,
) -> (Vec<BookieServer>, Vec<TempDir>, Vec<String>) {
    let mut bookies = Vec::with_capacity(n);
    let mut dirs = Vec::with_capacity(n);
    let mut addrs = Vec::with_capacity(n);
    for _ in 0..n {
        let (server, dir) = spawn_bookie_raw(connect, root, |_| {}).await;
        addrs.push(server.local_addr().to_string());
        bookies.push(server);
        dirs.push(dir);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    (bookies, dirs, addrs)
}

async fn spawn_bookie_raw<F>(
    connect: &str,
    root: &str,
    customize: F,
) -> (BookieServer, TempDir)
where
    F: FnOnce(&mut ServerConfig),
{
    let dir = TempDir::new().unwrap();
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    // Bind to 127.0.0.1 so the bookie registers a client-reachable
    // address with ZK. Otherwise `local_addr.ip()` is 0.0.0.0 and clients
    // can't connect to the advertised bookie ID.
    config.listening_interface = Some("127.0.0.1".to_string());
    // Disable disk-based read-only mode. The default 95% threshold trips
    // on dev hosts whose root volume is more full than that, which would
    // leave the bookie stuck in ReadOnly at startup and break the write
    // path in tests.
    config.read_only_mode_enabled = false;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config.zk_servers = Some(connect.to_string());
    config.zk_ledger_root = root.to_string();
    customize(&mut config);
    let server = BookieServer::start(config).await.unwrap();
    (server, dir)
}

// ---------------------------------------------------------------------------
// Ledger creation
// ---------------------------------------------------------------------------

/// Create a ledger via `ZkBookKeeper`, write `n_entries` entries
/// (`format!("entry-{i}")` payload), close it, and return the ledger ID.
///
/// Uses CRC32C digest. Ensemble/quorum sizes are explicit so the test can
/// match the number of bookies it has spawned.
pub async fn create_ledger_and_close(
    connect: &str,
    root: &str,
    ensemble_size: i32,
    write_quorum_size: i32,
    ack_quorum_size: i32,
    password: &[u8],
    n_entries: i64,
) -> i64 {
    let mut bk = ZkBookKeeper::new(connect, root).await.unwrap();
    let (mut wh, conns) = bk
        .create_ledger(
            ensemble_size,
            write_quorum_size,
            ack_quorum_size,
            DigestType::Crc32c,
            password,
        )
        .await
        .unwrap();
    let lid = wh.ledger_id();
    for i in 0..n_entries {
        wh.append(&conns, format!("entry-{i}").as_bytes())
            .await
            .unwrap();
    }
    bk.close_ledger(&wh).await.unwrap();
    lid
}
