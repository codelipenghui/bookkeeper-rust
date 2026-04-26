//! Integration tests against a real Oxia standalone container.
//!
//! These tests exercise the full end-to-end oxia metadata flow against a live
//! Oxia server, launching a container via the Docker CLI (std::process::Command)
//! because `testcontainers-oxia` conflicts with the workspace's
//! `testcontainers = 0.27` via a bollard-stubs exact-pin.
//!
//! To run:
//!   cargo test -p bookkeeper-metadata --features oxia --test oxia_container_test -- --ignored --nocapture
//!
//! Requires Docker to be available. Tests are `#[ignore]` so CI without
//! Docker skips them cleanly.

#![cfg(feature = "oxia")]

#[path = "common/mod.rs"]
mod common;

use std::process::Command;
use std::time::{Duration, Instant};

use bookkeeper_metadata::backend::open_metadata_backend;
use bookkeeper_metadata::metadata_paths::ParsedMetadataUri;
use bookkeeper_metadata::service_info::BookieServiceInfo;
use bookkeeper_metadata::traits::LedgerLayout;

/// The public oxia standalone image from StreamNative.
const OXIA_IMAGE: &str = "streamnative/oxia:main";

/// Command to run inside the container. The binary is on PATH in /oxia/bin.
const OXIA_CMD: &[&str] = &["oxia", "standalone"];

/// Guard that kills the container on drop.
struct OxiaContainer {
    id: String,
    pub port: u16,
}

impl OxiaContainer {
    /// Start an oxia standalone container with an ephemeral host port.
    /// Returns `None` if docker is unavailable or startup fails.
    fn start() -> Option<Self> {
        // Launch detached with a random host port.
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

        // Read back the mapped host port.
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

        // Output looks like:  "0.0.0.0:49153\n[::]:49153\n"
        let port_line = String::from_utf8(port_out.stdout).ok()?;
        let port = port_line
            .lines()
            .next()?
            .rsplit(':')
            .next()?
            .trim()
            .parse::<u16>()
            .ok()?;

        // Wait for the gRPC port to accept TCP connections (up to 30 s).
        let deadline = Instant::now() + Duration::from_secs(30);
        let addr = format!("127.0.0.1:{port}");
        loop {
            if let Ok(addr_parsed) = addr.parse() {
                if std::net::TcpStream::connect_timeout(&addr_parsed, Duration::from_millis(500))
                    .is_ok()
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

fn oxia_uri(port: u16) -> ParsedMetadataUri {
    ParsedMetadataUri::Oxia {
        service_addr: format!("127.0.0.1:{port}"),
        namespace: "default".to_string(),
        ledgers_root: "/ledgers".to_string(),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// Run the full backend contract suite against a live oxia standalone instance.
#[tokio::test]
#[ignore]
async fn oxia_backend_satisfies_contract() {
    if !is_docker_available() {
        eprintln!("[SKIP] docker not available");
        return;
    }

    let container = OxiaContainer::start().expect("start oxia container");
    let uri = oxia_uri(container.port);

    let backend = open_metadata_backend(&uri, None, Some(Duration::from_secs(10)))
        .await
        .expect("open_metadata_backend");

    backend
        .registration_manager
        .init_new_cluster(
            &LedgerLayout {
                manager_factory_class: "LegacyHierarchicalLedgerManagerFactory".into(),
                manager_version: 1,
            },
            "test-cluster",
        )
        .await
        .expect("init_new_cluster");

    common::contract::run_all(&backend).await;
}

/// Verify that ephemeral bookie registrations are cleaned up when the
/// backend (and its oxia session) is dropped.
///
/// Timeline:
///   1. Open backend-1, register an ephemeral bookie, assert it appears.
///   2. Drop backend-1 — this closes the oxia session.
///   3. Sleep 15 s (1.5× the 10s session_timeout) so oxia expires the session.
///   4. Open backend-2, list writable bookies — expect the registration is gone.
#[tokio::test]
#[ignore]
async fn ephemeral_registration_cleans_up_on_backend_drop() {
    if !is_docker_available() {
        eprintln!("[SKIP] docker not available");
        return;
    }

    let container = OxiaContainer::start().expect("start oxia container");
    let uri = oxia_uri(container.port);

    let info = BookieServiceInfo::new("ephemeral-bookie:3181", "ephemeral-host", 3181);

    {
        let backend = open_metadata_backend(&uri, None, Some(Duration::from_secs(10)))
            .await
            .unwrap();
        backend
            .registration_manager
            .init_new_cluster(
                &LedgerLayout {
                    manager_factory_class: "LegacyHierarchicalLedgerManagerFactory".into(),
                    manager_version: 1,
                },
                "test",
            )
            .await
            .unwrap();
        backend
            .registration_manager
            .register_bookie("ephemeral-bookie:3181", false, &info)
            .await
            .unwrap();
        let list = backend
            .registration_manager
            .get_writable_bookies()
            .await
            .unwrap();
        assert!(
            list.iter().any(|s| s == "ephemeral-bookie:3181"),
            "registration should be visible before drop: {list:?}"
        );
    } // backend (and its OxiaClient) dropped here — session should close

    eprintln!("[oxia] backend dropped; waiting 15s for session expiry …");
    tokio::time::sleep(Duration::from_secs(15)).await;

    let backend2 = open_metadata_backend(&uri, None, Some(Duration::from_secs(10)))
        .await
        .unwrap();
    let list = backend2
        .registration_manager
        .get_writable_bookies()
        .await
        .unwrap();
    assert!(
        !list.iter().any(|s| s == "ephemeral-bookie:3181"),
        "ephemeral registration should have been cleaned up after session expiry, got {list:?}"
    );
}

/// Deleting the `idgen` parent (e.g. by the HOB GC path that wipes a
/// generator directory and lets a later put recreate it) used to reset
/// the suffix back to its initial value because the old `seq_create`
/// keyed off the parent record's `modifications_count`. Two generators
/// straddling that delete then issued the same ledger ID.
///
/// Post-fix the suffix lives in a dedicated counter key (`{idgen}-counter`)
/// outside the parent directory, so deleting `idgen` does not influence
/// the next id issued. This test exercises that property end-to-end.
#[tokio::test]
#[ignore]
async fn idgen_resists_parent_delete_collision() {
    use liboxia::client::OxiaClient;
    use liboxia::client_options::OxiaClientOptions;
    use std::collections::HashSet;

    if !is_docker_available() {
        eprintln!("[SKIP] docker not available");
        return;
    }

    let container = OxiaContainer::start().expect("start oxia container");
    let uri = oxia_uri(container.port);

    let backend1 = open_metadata_backend(&uri, None, Some(Duration::from_secs(10)))
        .await
        .expect("open backend 1");
    backend1
        .registration_manager
        .init_new_cluster(
            &LedgerLayout {
                manager_factory_class: "LegacyHierarchicalLedgerManagerFactory".into(),
                manager_version: 1,
            },
            "idgen-collision-test",
        )
        .await
        .expect("init_new_cluster");

    let mut ids: HashSet<i64> = HashSet::new();
    for _ in 0..5 {
        let id = backend1
            .id_generator
            .generate_ledger_id()
            .await
            .expect("first round generate");
        assert!(
            ids.insert(id),
            "first round emitted duplicate id {id}"
        );
    }
    let first_round: Vec<i64> = ids.iter().copied().collect();

    // Connect a raw oxia client to delete `/ledgers/idgen` directly,
    // simulating the GC-recreate path that used to reset the counter.
    let raw = OxiaClient::new(OxiaClientOptions {
        service_address: format!("http://127.0.0.1:{}", container.port),
        namespace: "default".to_string(),
        identity: format!("test-collision-{}", uuid::Uuid::new_v4()),
        session_timeout: Duration::from_secs(10),
        ..Default::default()
    })
    .await
    .expect("raw oxia client");
    let _ = raw.delete("/ledgers/idgen".to_string()).await;

    // Drop backend1 so a fresh backend2 has to re-ensure the path,
    // forcing it through the recreate code path.
    drop(backend1);

    let backend2 = open_metadata_backend(&uri, None, Some(Duration::from_secs(10)))
        .await
        .expect("open backend 2");

    for _ in 0..5 {
        let id = backend2
            .id_generator
            .generate_ledger_id()
            .await
            .expect("second round generate");
        assert!(
            ids.insert(id),
            "second round emitted id {id} which was already issued before the \
             /ledgers/idgen delete (first round: {first_round:?})",
        );
    }
    raw.shutdown().await.ok();
}
