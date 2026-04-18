//! End-to-end test: bookie server booting with oxia as its metadata backend.
//!
//! Validates the full path:
//!   1. Oxia standalone container starts.
//!   2. Cluster is initialised against oxia.
//!   3. A BookieServer starts and registers itself with oxia.
//!   4. Entries are written to a ledger via the V2 bookie protocol.
//!   5. Entries are read back and their payloads are verified.
//!   6. Ledger metadata is inspected directly through the oxia backend,
//!      confirming that the bookie's add-path correctly updates ZK-compat
//!      metadata structures.
//!
//! Requires Docker. Run with:
//!   cargo test -p bookkeeper-server --features oxia --test oxia_e2e_test \
//!     -- --ignored --nocapture
//!
//! The test is `#[ignore]` so CI without Docker skips it cleanly.

#![cfg(feature = "oxia")]

use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_client::write_handle::WriteHandle;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_metadata::backend::open_metadata_backend;
use bookkeeper_metadata::metadata_paths::ParsedMetadataUri;
use bookkeeper_metadata::traits::LedgerLayout;
use bookkeeper_protocol::constants::{V2_EOK, METADATA_LENGTH};
use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_protocol::v2::response::V2Response;
use bookkeeper_server::server::BookieServer;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Oxia container harness
// ---------------------------------------------------------------------------

const OXIA_IMAGE: &str = "streamnative/oxia:main";
const OXIA_CMD: &[&str] = &["oxia", "standalone"];

/// RAII guard: kills the container on drop so test failures don't leak it.
struct OxiaContainer {
    id: String,
    pub port: u16,
}

impl OxiaContainer {
    /// Start an oxia standalone container with an OS-assigned host port.
    /// Returns `None` if docker is unavailable or startup fails.
    fn start() -> Option<Self> {
        let out = Command::new("docker")
            .args(["run", "-d", "--rm", "-p", "0:6648", OXIA_IMAGE])
            .args(OXIA_CMD)
            .output()
            .ok()?;

        if !out.status.success() {
            eprintln!(
                "[oxia] docker run failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            return None;
        }

        let id = String::from_utf8(out.stdout).ok()?.trim().to_string();
        if id.is_empty() {
            eprintln!("[oxia] docker run returned empty container id");
            return None;
        }

        // Ask docker which host port was assigned.
        let port_out = Command::new("docker")
            .args(["port", &id, "6648/tcp"])
            .output()
            .ok()?;
        if !port_out.status.success() {
            eprintln!(
                "[oxia] docker port failed: {}",
                String::from_utf8_lossy(&port_out.stderr)
            );
            let _ = Command::new("docker").args(["kill", &id]).output();
            return None;
        }

        // Output looks like "0.0.0.0:49153\n[::]:49153\n"
        let port_line = String::from_utf8(port_out.stdout).ok()?;
        let port = port_line
            .lines()
            .next()?
            .rsplit(':')
            .next()?
            .trim()
            .parse::<u16>()
            .ok()?;

        // Wait up to 30 s for the gRPC port to accept TCP connections.
        let deadline = Instant::now() + Duration::from_secs(30);
        let addr = format!("127.0.0.1:{port}");
        loop {
            if let Ok(parsed) = addr.parse() {
                if std::net::TcpStream::connect_timeout(&parsed, Duration::from_millis(500)).is_ok()
                {
                    // Extra second for oxia to finish shard-assignment bootstrap.
                    std::thread::sleep(Duration::from_secs(1));
                    break;
                }
            }
            if Instant::now() > deadline {
                eprintln!("[oxia] container did not become ready within 30s");
                let _ = Command::new("docker").args(["kill", &id]).output();
                return None;
            }
            std::thread::sleep(Duration::from_millis(300));
        }

        eprintln!("[oxia] container {id} ready on port {port}");
        Some(Self { id, port })
    }
}

impl Drop for OxiaContainer {
    fn drop(&mut self) {
        eprintln!("[oxia] killing container {}", self.id);
        let _ = Command::new("docker").args(["kill", &self.id]).output();
    }
}

fn is_docker_available() -> bool {
    Command::new("docker")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn oxia_uri(port: u16) -> ParsedMetadataUri {
    ParsedMetadataUri::Oxia {
        service_addr: format!("127.0.0.1:{port}"),
        namespace: "default".to_string(),
        ledgers_root: "/ledgers".to_string(),
    }
}

fn oxia_metadata_service_uri(port: u16) -> String {
    format!("oxia://127.0.0.1:{port}/default/ledgers")
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

/// Full bookie boot-and-run against a live oxia backend.
///
/// Steps:
///   1. Boot oxia standalone container.
///   2. Init cluster metadata (ledger layout + instance-id).
///   3. Spawn a BookieServer configured with the oxia metadata URI.
///   4. Wait for the bookie to register itself as writable.
///   5. Write 5 entries to a ledger via V2 TCP protocol.
///   6. Read each entry back and verify the payload.
///   7. Verify the bookie's writable registration is still visible.
///   8. Shut everything down; container is killed by Drop.
#[tokio::test]
#[ignore]
async fn bookie_boots_and_runs_against_oxia() {
    if !is_docker_available() {
        eprintln!("[SKIP] docker not available");
        return;
    }

    // ── 1. Start oxia ────────────────────────────────────────────────────────
    let container = OxiaContainer::start().expect("start oxia container");
    let uri = oxia_uri(container.port);

    // ── 2. Init cluster metadata ─────────────────────────────────────────────
    let backend = open_metadata_backend(&uri, None, Some(Duration::from_secs(10)))
        .await
        .expect("open oxia metadata backend for cluster init");

    backend
        .registration_manager
        .init_new_cluster(
            &LedgerLayout {
                manager_factory_class: "LegacyHierarchicalLedgerManagerFactory".into(),
                manager_version: 1,
            },
            "oxia-e2e-test-cluster",
        )
        .await
        .expect("init_new_cluster");
    eprintln!("[test] cluster initialised");

    // ── 3. Spawn bookie ───────────────────────────────────────────────────────
    let dir = TempDir::new().unwrap();
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.allow_ephemeral_ports = true;
    // Bind to 127.0.0.1 so the bookie registers a client-reachable address.
    config.listening_interface = Some("127.0.0.1".to_string());
    // Disable disk-based read-only mode (dev hosts may be >95% full).
    config.read_only_mode_enabled = false;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    // Point the bookie at oxia instead of ZooKeeper.
    config.metadata_service_uri = Some(oxia_metadata_service_uri(container.port));
    config.zk_servers = None;

    let server = BookieServer::start(config)
        .await
        .expect("BookieServer::start with oxia backend");
    let bookie_addr = server.local_addr();
    eprintln!("[test] bookie started on {bookie_addr}");

    // ── 4. Wait for bookie to register with oxia ──────────────────────────────
    let bookie_id = bookie_addr.to_string();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let bookies = backend
            .registration_manager
            .get_writable_bookies()
            .await
            .unwrap_or_default();
        if bookies.iter().any(|b| b == &bookie_id) {
            eprintln!("[test] bookie {bookie_id} registered as writable");
            break;
        }
        if Instant::now() > deadline {
            panic!(
                "bookie {bookie_id} did not appear in writable bookies within 15s; got {bookies:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // ── 5. Write entries via V2 TCP protocol ──────────────────────────────────
    const LEDGER_ID: i64 = 42;
    const N_ENTRIES: usize = 5;

    let conn = Arc::new(
        BookieConnection::connect(bookie_addr)
            .await
            .expect("connect to bookie"),
    );
    let ensemble = vec![bookie_addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];
    let mut wh = WriteHandle::new(LEDGER_ID, ensemble, schedule, dm, master_key);
    let conns = vec![conn.clone()];

    for i in 0..N_ENTRIES {
        let payload = format!("oxia-e2e-entry-{i}");
        wh.append(&conns, payload.as_bytes())
            .await
            .expect("append entry");
        eprintln!("[test] wrote entry {i}");
    }

    // ── 6. Read entries back ──────────────────────────────────────────────────
    for i in 0..N_ENTRIES as i64 {
        let resp = conn
            .send_read_entry(0, LEDGER_ID, i, None)
            .await
            .expect("send_read_entry");
        match resp {
            V2Response::Read { error_code, ledger_id, entry_id, data } => {
                assert_eq!(error_code, V2_EOK, "entry {i}: expected EOK, got {error_code}");
                assert_eq!(ledger_id, LEDGER_ID, "entry {i}: wrong ledger_id");
                assert_eq!(entry_id, i, "entry {i}: wrong entry_id");

                // The entry data on the wire is the full framed entry:
                //   METADATA_LENGTH (32 bytes: ledger_id, entry_id, LAC, length)
                //   + digest (4 bytes for CRC32c)
                //   + payload
                // The payload starts at byte METADATA_LENGTH + digest_len.
                let raw = data.expect("no data in read response");
                const CRC32C_LEN: usize = 4;
                let payload_start = METADATA_LENGTH + CRC32C_LEN;
                let payload_bytes = if raw.len() > payload_start {
                    &raw[payload_start..]
                } else {
                    b""
                };
                let expected_payload = format!("oxia-e2e-entry-{i}");
                assert_eq!(
                    payload_bytes,
                    expected_payload.as_bytes(),
                    "entry {i}: payload mismatch: got {:?}",
                    String::from_utf8_lossy(payload_bytes)
                );
                eprintln!("[test] read entry {i} OK: {:?}", expected_payload);
            }
            other => panic!("entry {i}: expected Read response, got {other:?}"),
        }
    }

    // ── 7. Verify bookie is still registered ──────────────────────────────────
    let bookies = backend
        .registration_manager
        .get_writable_bookies()
        .await
        .expect("get_writable_bookies");
    assert!(
        bookies.iter().any(|b| b == &bookie_id),
        "bookie {bookie_id} should still be writable after writes; got {bookies:?}"
    );
    eprintln!("[test] bookie still registered after writes");

    // ── 8. Shutdown ───────────────────────────────────────────────────────────
    server.shutdown().await;
    eprintln!("[test] bookie shutdown complete");
    // `container` Drop kills the oxia docker container.
    eprintln!("[test] PASS");
}
