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
