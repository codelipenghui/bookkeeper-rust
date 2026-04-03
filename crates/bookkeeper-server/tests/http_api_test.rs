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
        http_port: Some(0), // OS-assigned HTTP port
    }
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
