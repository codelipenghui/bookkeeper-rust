//! End-to-end data-integrity recovery test.
//!
//! Scenario: 3 bookies at w=3/a=2 so every bookie holds every entry.
//! Close a ledger of N entries. Shut down bookie 1, wipe its local data
//! (ledger + index + journal + cookie), restart with
//! `data_integrity_checking=true` and `data_integrity_stamp_missing_cookies=true`.
//! The DataIntegrity pre-boot check stamps cookies, the service kicks in,
//! EntryCopier pulls each missing entry from bookies 2 and 3, and bookie 1
//! serves reads again.
//!
//! Skipped gracefully if Docker / ZK is unavailable.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use std::sync::Once;

use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_config::server::ServerConfig;

static LOGGER: Once = Once::new();
fn init_logging() {
    LOGGER.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with_test_writer()
            .try_init();
    });
}
use bookkeeper_protocol::v2::response::V2Response;
use bookkeeper_server::server::BookieServer;
use bookkeeper_test_utils::{bootstrap_cluster, create_ledger_and_close, shared_zk};
use tempfile::TempDir;

const N_ENTRIES: i64 = 50;

/// Spawn a fresh bookie on a pre-existing data directory. Lets the test
/// "restart" a bookie after wiping its data without losing the path.
async fn spawn_on_dir(
    connect: &str,
    root: &str,
    dir: &Path,
    customize: impl FnOnce(&mut ServerConfig),
) -> BookieServer {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.listening_interface = Some("127.0.0.1".to_string());
    config.read_only_mode_enabled = false;
    config.ledger_directories = vec![dir.to_string_lossy().into_owned()];
    config.journal_directory = dir.to_string_lossy().into_owned();
    config.zk_servers = Some(connect.to_string());
    config.zk_ledger_root = root.to_string();
    customize(&mut config);
    BookieServer::start(config).await.expect("bookie start")
}

/// Wipe every file under `dir` so a "restart" starts with a blank disk.
fn wipe_dir(dir: &Path) {
    for entry in std::fs::read_dir(dir).expect("read dir") {
        let entry = entry.expect("read entry");
        let path = entry.path();
        if path.is_dir() {
            std::fs::remove_dir_all(&path).expect("rm dir");
        } else {
            std::fs::remove_file(&path).expect("rm file");
        }
    }
}

async fn read_entry_direct(addr: SocketAddr, ledger_id: i64, entry_id: i64) -> Option<usize> {
    let conn = BookieConnection::connect(addr).await.ok()?;
    match conn.send_read_entry(0, ledger_id, entry_id, None).await.ok()? {
        V2Response::Read { error_code: 0, data: Some(body), .. } => Some(body.len()),
        _ => None,
    }
}

/// Poll `bookie` for `(ledger_id, entry_id)` until it returns the entry,
/// or `deadline` expires. Returns `true` on success.
async fn await_entry_recovered(
    addr: SocketAddr,
    ledger_id: i64,
    entry_id: i64,
    deadline: Duration,
) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if read_entry_direct(addr, ledger_id, entry_id).await.is_some() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

async fn spawn_three(connect: &str, root: &str) -> (Vec<BookieServer>, Vec<TempDir>) {
    let mut servers = Vec::with_capacity(3);
    let mut dirs = Vec::with_capacity(3);
    for _ in 0..3 {
        let dir = TempDir::new().unwrap();
        let server = spawn_on_dir(connect, root, dir.path(), |_| {}).await;
        servers.push(server);
        dirs.push(dir);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    (servers, dirs)
}

/// Scenario 1: closed ledger, wipe one bookie's data, verify full recovery.
#[tokio::test]
async fn recovers_closed_ledger_after_data_wipe() {
    init_logging();
    let Some(connect) = shared_zk().await else {
        eprintln!("[SKIP] Docker/ZK unavailable");
        return;
    };
    let root = "/test_dataint_closed";
    bootstrap_cluster(&connect, root).await;

    // 1. Three bookies at w=3/a=2 so every bookie holds every entry.
    let (mut servers, dirs) = spawn_three(&connect, root).await;

    // 2. Write + close a ledger.
    let ledger_id =
        create_ledger_and_close(&connect, root, 3, 3, 2, b"pw", N_ENTRIES).await;

    // 3. Sanity: the target bookie has all entries right now.
    let target_addr = servers[0].local_addr();
    for eid in 0..N_ENTRIES {
        assert!(
            read_entry_direct(target_addr, ledger_id, eid).await.is_some(),
            "target bookie should serve entry {eid} before wipe"
        );
    }

    // 4. Gracefully stop bookie 0 and wipe its data dir (includes journal,
    // ledger storage, and cookie — so the DataIntegrity cookie validator
    // will need to stamp fresh cookies on restart). Keep the port so the
    // restarted bookie has the SAME bookie_id (host:port) as the original
    // — otherwise the ensemble metadata won't match and the integrity
    // check will find no ledgers to recover.
    let target_dir = dirs[0].path().to_path_buf();
    let original_port = target_addr.port();
    let target = servers.remove(0);
    target.shutdown().await;
    wipe_dir(&target_dir);

    // 5. Restart the wiped bookie with data-integrity enabled on the same
    // port so its identity in the ensembles is preserved.
    let restarted = spawn_on_dir(&connect, root, &target_dir, |c| {
        c.bookie_port = original_port;
        c.data_integrity_checking = true;
        c.data_integrity_stamp_missing_cookies = true;
    })
    .await;
    let new_addr = restarted.local_addr();
    assert_eq!(
        new_addr.port(),
        original_port,
        "restarted bookie must rebind to the same port"
    );
    // Give ZK re-registration a moment.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 6. Poll until the bookie can serve every entry again. Generous timeout
    // so a slow CI host has room; the happy path on a warm laptop finishes
    // in under 2 s.
    let deadline = Duration::from_secs(30);
    for eid in [0i64, N_ENTRIES / 2, N_ENTRIES - 1] {
        assert!(
            await_entry_recovered(new_addr, ledger_id, eid, deadline).await,
            "DataIntegrity service failed to recover entry {eid} within {:?}",
            deadline
        );
    }

    // 7. Full sweep — every entry must now be readable from the restarted
    // bookie. At this point the check has cleared NEEDS_INTEGRITY_CHECK.
    for eid in 0..N_ENTRIES {
        assert!(
            read_entry_direct(new_addr, ledger_id, eid).await.is_some(),
            "entry {eid} missing after data-integrity recovery"
        );
    }

    // Teardown.
    restarted.shutdown().await;
    for s in servers {
        s.shutdown().await;
    }
    let _ = dirs; // drop at end to keep dirs alive
    let _ = Arc::<()>::new(()); // silence unused std::sync::Arc warning if any
}
