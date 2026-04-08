use bookkeeper_config::server::ServerConfig;
use bookkeeper_server::server::BookieServer;
use tempfile::TempDir;

fn test_config(dir: &TempDir) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config.http_server_enabled = true;
    config.http_server_port = 0; // OS-assigned HTTP port
    config
}

#[tokio::test]
async fn heartbeat() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/heartbeat", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "OK");

    server.shutdown().await;
}

#[tokio::test]
async fn readiness() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/bookie/is_ready", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);

    server.shutdown().await;
}

#[tokio::test]
async fn bookie_state() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/bookie/state", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["running"], true);
    assert_eq!(body["readOnly"], false);
    assert_eq!(body["shuttingDown"], false);

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Tests for the expanded HTTP admin API endpoints
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bookie_info_returns_disk_space() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/bookie/info", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    // Java field names: freeSpace and totalSpace (not freeDiskSpace/totalDiskSpace)
    assert!(body["freeSpace"].is_number(), "missing freeSpace field");
    assert!(body["totalSpace"].is_number(), "missing totalSpace field");
    assert!(body["totalSpace"].as_u64().unwrap() > 0);

    server.shutdown().await;
}

#[tokio::test]
async fn metrics_endpoint_returns_prometheus_format() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/metrics", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let content_type = resp.headers().get("content-type").unwrap().to_str().unwrap().to_string();
    assert!(content_type.contains("text/plain"));
    let body = resp.text().await.unwrap();
    // Prometheus metrics include HELP/TYPE comments
    assert!(body.contains("# HELP") || body.contains("# TYPE") || body.is_empty());

    server.shutdown().await;
}

#[tokio::test]
async fn server_config_endpoint() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/config/server_config", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["ledgerDirectories"].is_array());
    assert!(body["journalDirectory"].is_string());

    server.shutdown().await;
}

#[tokio::test]
async fn last_log_mark_endpoint() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/bookie/last_log_mark", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    // Java format: key starts with "LastLogMark: "
    let obj = body.as_object().unwrap();
    let key = obj.keys().next().unwrap();
    assert!(key.starts_with("LastLogMark:"), "key should start with 'LastLogMark:', got: {key}");

    server.shutdown().await;
}

#[tokio::test]
async fn list_disk_files_endpoint() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/bookie/list_disk_file", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let obj = body.as_object().unwrap();
    // All-types: should have all three keys
    assert!(obj.contains_key("journal files"));
    assert!(obj.contains_key("entrylog files"));
    assert!(obj.contains_key("index files"));

    // Filter by type
    let resp = reqwest::get(format!("http://{}/api/v1/bookie/list_disk_file?file_type=journal", http_addr))
        .await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["journal files"].is_string());

    server.shutdown().await;
}

#[tokio::test]
async fn readonly_get_and_put() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    // GET — initially false
    let resp = reqwest::get(format!("http://{}/api/v1/bookie/state/readonly", http_addr))
        .await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["readOnly"], false);

    // PUT — set to read-only
    let client = reqwest::Client::new();
    let resp = client.put(format!("http://{}/api/v1/bookie/state/readonly", http_addr))
        .json(&serde_json::json!({"readOnly": true}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["readOnly"], true);

    // GET — should now be true
    let resp = reqwest::get(format!("http://{}/api/v1/bookie/state/readonly", http_addr))
        .await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["readOnly"], true);

    server.shutdown().await;
}

#[tokio::test]
async fn list_ledgers_local_empty() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/ledger/list", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body.is_object());

    server.shutdown().await;
}

#[tokio::test]
async fn trigger_gc_endpoint() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let client = reqwest::Client::new();
    // PUT with no body — full force
    let resp = client.put(format!("http://{}/api/v1/bookie/gc", http_addr))
        .json(&serde_json::json!({}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["triggered"], true);

    server.shutdown().await;
}

#[tokio::test]
async fn gc_suspend_and_resume() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let client = reqwest::Client::new();

    // GET initial
    let resp = reqwest::get(format!("http://{}/api/v1/bookie/gc/suspend_compaction", http_addr))
        .await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["isMajorGcSuspended"], "false");
    assert_eq!(body["isMinorGcSuspended"], "false");

    // PUT suspend
    let resp = client.put(format!("http://{}/api/v1/bookie/gc/suspend_compaction", http_addr))
        .json(&serde_json::json!({"suspendMajor": true, "suspendMinor": true}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // GET after suspend
    let resp = reqwest::get(format!("http://{}/api/v1/bookie/gc/suspend_compaction", http_addr))
        .await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["isMajorGcSuspended"], "true");
    assert_eq!(body["isMinorGcSuspended"], "true");

    // PUT resume
    let resp = client.put(format!("http://{}/api/v1/bookie/gc/resume_compaction", http_addr))
        .json(&serde_json::json!({"resumeMajor": true, "resumeMinor": true}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // GET after resume
    let resp = reqwest::get(format!("http://{}/api/v1/bookie/gc/suspend_compaction", http_addr))
        .await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["isMajorGcSuspended"], "false");

    server.shutdown().await;
}

#[tokio::test]
async fn entry_location_compact() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let client = reqwest::Client::new();
    let resp = client.put(format!("http://{}/api/v1/bookie/entry_location_compact", http_addr))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);

    server.shutdown().await;
}

#[tokio::test]
async fn gc_details_returns_array() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/bookie/gc_details", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body.is_array());

    server.shutdown().await;
}

#[tokio::test]
async fn sanity_endpoint() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/bookie/sanity", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["passed"], true);

    server.shutdown().await;
}

// Endpoints requiring ZK return 503 when ZK is not configured
#[tokio::test]
async fn cluster_info_without_zk_returns_503() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/bookie/cluster_info", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 503);

    server.shutdown().await;
}

#[tokio::test]
async fn list_bookies_without_zk_returns_503() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/bookie/list_bookies", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 503);

    server.shutdown().await;
}

#[tokio::test]
async fn who_is_auditor_without_zk_returns_503() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/autorecovery/who_is_auditor", http_addr))
        .await.unwrap();
    assert_eq!(resp.status(), 503);

    server.shutdown().await;
}
