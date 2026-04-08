//! End-to-end tests for HTTP admin API endpoints that require ZK.
//!
//! The existing `http_api_test.rs` already covers the local-only endpoints
//! (heartbeat, /metrics, /api/v1/bookie/state, GC, etc.) and verifies that
//! the ZK-requiring endpoints return 503 when no ZK is configured. This file
//! covers the OPPOSITE side: with a real ZK + a registered bookie, what
//! does each endpoint return?
//!
//! These differ from the CLI e2e tests in `cli_admin_e2e_test.rs` because
//! the HTTP routes have separate handlers, separate request shapes, and
//! separate query/body parsing — a CLI test doesn't catch HTTP-side bugs
//! like wrong status codes, wrong JSON keys, or query parameter parsing
//! mistakes.
//!
//! Endpoints exercised here:
//!   - GET  /api/v1/bookie/cluster_info
//!   - GET  /api/v1/bookie/list_bookies (default + ?type=ro)
//!   - GET/PUT /api/v1/autorecovery/status
//!   - GET/PUT /api/v1/autorecovery/lost_bookie_recovery_delay
//!   - GET  /api/v1/autorecovery/who_is_auditor
//!   - GET  /api/v1/autorecovery/list_under_replicated_ledger (empty + populated)
//!   - PUT  /api/v1/autorecovery/trigger_audit (happy path + 400 when disabled)
//!   - PUT  /api/v1/autorecovery/bookie (recover trigger)
//!   - PUT  /api/v1/autorecovery/decommission
//!   - GET  /api/v1/ledger/metadata (existing + missing)
//!   - DELETE /api/v1/ledger/delete
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

struct ZkFixture {
    _container: ContainerAsync<GenericImage>,
    connect: String,
}

static ZK: OnceCell<Option<Arc<ZkFixture>>> = OnceCell::const_new();

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

async fn bootstrap_cluster(connect: &str, root: &str) {
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

/// Spawn an in-process Rust bookie wired to the given ZK + ledger root,
/// with the HTTP admin API enabled on an OS-assigned port.
async fn spawn_bookie(connect: &str, root: &str) -> (BookieServer, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    // Bind to 127.0.0.1 so the bookie's ZK registration is connectable
    // from any client running on the same machine.
    config.listening_interface = Some("127.0.0.1".to_string());
    // Disable disk-based read-only mode: the default 95% threshold trips
    // on dev machines whose root volume is more full than that.
    config.read_only_mode_enabled = false;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config.zk_servers = Some(connect.to_string());
    config.zk_ledger_root = root.to_string();
    config.http_server_enabled = true;
    config.http_server_port = 0; // OS-assigned

    let server = BookieServer::start(config).await.unwrap();
    // Brief settle for ZK ephemeral registration + auditor election
    tokio::time::sleep(Duration::from_millis(500)).await;
    (server, dir)
}

fn http_url(server: &BookieServer, path: &str) -> String {
    let addr = server.http_addr().expect("http server enabled");
    format!("http://{addr}{path}")
}

async fn create_test_ledger(connect: &str, root: &str, n_entries: i64) -> i64 {
    let mut bk = ZkBookKeeper::new(connect, root).await.unwrap();
    let (mut wh, conns) = bk
        .create_ledger(1, 1, 1, DigestType::Crc32c, b"http-test")
        .await
        .unwrap();
    let lid = wh.ledger_id();
    for i in 0..n_entries {
        wh.append(&conns, format!("e{i}").as_bytes())
            .await
            .unwrap();
    }
    bk.close_ledger(&wh).await.unwrap();
    lid
}

// ---------------------------------------------------------------------------
// /api/v1/bookie/cluster_info
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cluster_info_with_zk_returns_writable_count_and_recovery_state() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_cluster_info";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    let resp = reqwest::get(http_url(&bookie, "/api/v1/bookie/cluster_info"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["writableBookiesCount"], 1, "should see our bookie");
    assert_eq!(body["readonlyBookiesCount"], 0);
    assert_eq!(body["totalBookiesCount"], 1);
    assert_eq!(
        body["ledgerReplicationEnabled"], true,
        "auto-recovery should be enabled by default"
    );
    assert!(body.get("auditorElected").is_some());

    bookie.shutdown().await;
}

// ---------------------------------------------------------------------------
// /api/v1/bookie/list_bookies
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_bookies_default_returns_writable_bookie_map() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_list_bookies";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    let resp = reqwest::get(http_url(&bookie, "/api/v1/bookie/list_bookies"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let map = body.as_object().expect("response should be a map");

    // Map should contain exactly one bookie (ours)
    assert_eq!(map.len(), 1);
    let bookie_addr = bookie.local_addr().to_string();
    assert!(
        map.contains_key(&bookie_addr),
        "list_bookies map should contain {bookie_addr}; got keys {:?}",
        map.keys().collect::<Vec<_>>()
    );

    bookie.shutdown().await;
}

#[tokio::test]
async fn list_bookies_type_ro_returns_empty_when_no_readonly_bookies() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_list_bookies_ro";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    let resp = reqwest::get(http_url(&bookie, "/api/v1/bookie/list_bookies?type=ro"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let map = body.as_object().unwrap();
    assert_eq!(map.len(), 0, "no readonly bookies expected");

    bookie.shutdown().await;
}

// ---------------------------------------------------------------------------
// /api/v1/autorecovery/status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn autorecovery_status_disable_and_enable_round_trip() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_ar_status";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    // GET initial — should be enabled
    let resp = reqwest::get(http_url(&bookie, "/api/v1/autorecovery/status"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["enabled"], true, "auto-recovery enabled by default");

    // PUT enabled=false (query param)
    let client = reqwest::Client::new();
    let resp = client
        .put(http_url(
            &bookie,
            "/api/v1/autorecovery/status?enabled=false",
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // GET — should now be disabled
    let resp = reqwest::get(http_url(&bookie, "/api/v1/autorecovery/status"))
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["enabled"], false);

    // PUT enabled=true
    let resp = client
        .put(http_url(
            &bookie,
            "/api/v1/autorecovery/status?enabled=true",
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // GET — should be enabled again
    let resp = reqwest::get(http_url(&bookie, "/api/v1/autorecovery/status"))
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["enabled"], true);

    bookie.shutdown().await;
}

// ---------------------------------------------------------------------------
// /api/v1/autorecovery/lost_bookie_recovery_delay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lost_bookie_recovery_delay_get_and_set_round_trip() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_lbrd";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    // PUT 42 — JSON body
    let client = reqwest::Client::new();
    let resp = client
        .put(http_url(
            &bookie,
            "/api/v1/autorecovery/lost_bookie_recovery_delay",
        ))
        .json(&serde_json::json!({ "delay_seconds": 42 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(text.contains("42"), "set response should mention 42; got {text:?}");

    // GET — plain text body containing the value
    let resp = reqwest::get(http_url(
        &bookie,
        "/api/v1/autorecovery/lost_bookie_recovery_delay",
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(
        text.contains("42"),
        "GET should return the previously-set value 42; got {text:?}"
    );

    bookie.shutdown().await;
}

// ---------------------------------------------------------------------------
// /api/v1/autorecovery/who_is_auditor
// ---------------------------------------------------------------------------

#[tokio::test]
async fn who_is_auditor_returns_elected_bookie_id() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_auditor";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    // The bookie's auto-recovery daemon is enabled by default, so it
    // should elect itself as the auditor within a second or two.
    // Poll until the endpoint reports a non-empty auditor.
    let bookie_addr = bookie.local_addr().to_string();
    let mut elected = false;
    for _ in 0..30 {
        let resp = reqwest::get(http_url(&bookie, "/api/v1/autorecovery/who_is_auditor"))
            .await
            .unwrap();
        if resp.status() == 200 {
            let body = resp.text().await.unwrap();
            if body.contains(&bookie_addr) {
                elected = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        elected,
        "the only bookie should have elected itself as auditor within ~6s"
    );

    bookie.shutdown().await;
}

// ---------------------------------------------------------------------------
// /api/v1/autorecovery/list_under_replicated_ledger
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_under_replicated_ledger_empty_returns_404() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_ur_empty";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    let resp = reqwest::get(http_url(
        &bookie,
        "/api/v1/autorecovery/list_under_replicated_ledger",
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().unwrap();
    assert!(
        arr[0].as_str().unwrap().contains("No under replicated"),
        "404 response should describe the empty case; got {body}"
    );

    bookie.shutdown().await;
}

#[tokio::test]
async fn list_under_replicated_ledger_returns_array_when_populated() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_ur_populated";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    // Create a ledger and mark it under-replicated via the UR manager
    // directly (the auditor would normally do this; we're isolating the
    // HTTP read path).
    let lid = create_test_ledger(&connect, root, 2).await;

    let zk_client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = bookkeeper_metadata::zk_path::ZkPaths::new(root);
    let ur_mgr = bookkeeper_replication::underreplication::ZkUnderreplicationManager::new(
        zk_client,
        paths,
        "test-marker".to_string(),
    )
    .await
    .unwrap();
    ur_mgr
        .mark_ledger_underreplicated(lid, &["dead-bookie:3181".to_string()])
        .await
        .unwrap();

    // GET — should now return 200 with our ledger ID
    let resp = reqwest::get(http_url(
        &bookie,
        "/api/v1/autorecovery/list_under_replicated_ledger",
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().unwrap();
    assert!(
        arr.iter().any(|v| v.as_i64() == Some(lid)),
        "response should contain ledger {lid}; got {body}"
    );

    bookie.shutdown().await;
}

// ---------------------------------------------------------------------------
// /api/v1/autorecovery/trigger_audit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn trigger_audit_returns_400_when_replication_disabled() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_trigger_disabled";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    let client = reqwest::Client::new();

    // Disable auto-recovery first
    let _ = client
        .put(http_url(
            &bookie,
            "/api/v1/autorecovery/status?enabled=false",
        ))
        .send()
        .await
        .unwrap();

    // Now trigger_audit should return 400
    let resp = client
        .put(http_url(&bookie, "/api/v1/autorecovery/trigger_audit"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["error"]
            .as_str()
            .map(|s| s.contains("disabled"))
            .unwrap_or(false),
        "400 should explain auto-recovery is disabled; got {body}"
    );

    bookie.shutdown().await;
}

#[tokio::test]
async fn trigger_audit_returns_200_after_auditor_elected() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_trigger_ok";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    // Wait for the bookie's auditor to be elected (via who_is_auditor).
    let bookie_addr = bookie.local_addr().to_string();
    for _ in 0..30 {
        let resp = reqwest::get(http_url(&bookie, "/api/v1/autorecovery/who_is_auditor"))
            .await
            .unwrap();
        if resp.status() == 200 {
            let body = resp.text().await.unwrap();
            if body.contains(&bookie_addr) {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let client = reqwest::Client::new();
    let resp = client
        .put(http_url(&bookie, "/api/v1/autorecovery/trigger_audit"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["triggered"], true);

    bookie.shutdown().await;
}

// ---------------------------------------------------------------------------
// /api/v1/autorecovery/bookie (recover) and /decommission
// ---------------------------------------------------------------------------

#[tokio::test]
async fn recover_bookie_endpoint_accepts_request_and_returns_triggered() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_recover";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    let client = reqwest::Client::new();
    let resp = client
        .put(http_url(&bookie, "/api/v1/autorecovery/bookie"))
        .json(&serde_json::json!({
            "bookie_src": ["dead-bookie-1:3181", "dead-bookie-2:3181"],
            "delete_cookie": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["triggered"], true);

    bookie.shutdown().await;
}

#[tokio::test]
async fn decommission_endpoint_accepts_request_and_returns_triggered() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_decom";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    let client = reqwest::Client::new();
    let resp = client
        .put(http_url(&bookie, "/api/v1/autorecovery/decommission"))
        .json(&serde_json::json!({
            "bookie_src": "dead-bookie:3181",
            "delete_cookie": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["triggered"], true);

    bookie.shutdown().await;
}

// ---------------------------------------------------------------------------
// /api/v1/ledger/metadata + /api/v1/ledger/delete
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ledger_metadata_returns_404_for_missing_ledger() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_ledger_missing";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    let resp = reqwest::get(http_url(
        &bookie,
        "/api/v1/ledger/metadata?ledger_id=99999",
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 404);

    bookie.shutdown().await;
}

#[tokio::test]
async fn ledger_metadata_returns_metadata_for_existing_ledger() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_ledger_meta";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    let lid = create_test_ledger(&connect, root, 3).await;

    let resp = reqwest::get(http_url(
        &bookie,
        &format!("/api/v1/ledger/metadata?ledger_id={lid}"),
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let metadata = &body[lid.to_string()];
    assert_eq!(metadata["ensembleSize"], 1);
    assert_eq!(metadata["writeQuorumSize"], 1);
    assert_eq!(metadata["ackQuorumSize"], 1);
    assert_eq!(metadata["state"], "CLOSED");
    assert_eq!(metadata["lastEntryId"], 2); // 3 entries, 0..2

    bookie.shutdown().await;
}

#[tokio::test]
async fn ledger_delete_removes_ledger_metadata() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_http_ledger_delete";
    bootstrap_cluster(&connect, root).await;
    let (bookie, _dir) = spawn_bookie(&connect, root).await;

    let lid = create_test_ledger(&connect, root, 1).await;

    // Sanity: metadata exists pre-delete
    let resp = reqwest::get(http_url(
        &bookie,
        &format!("/api/v1/ledger/metadata?ledger_id={lid}"),
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);

    // DELETE — uses query param, DELETE method
    let client = reqwest::Client::new();
    let resp = client
        .delete(http_url(
            &bookie,
            &format!("/api/v1/ledger/delete?ledger_id={lid}"),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["deleted"], true);

    // Metadata should now be gone
    let resp = reqwest::get(http_url(
        &bookie,
        &format!("/api/v1/ledger/metadata?ledger_id={lid}"),
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 404);

    bookie.shutdown().await;
}
