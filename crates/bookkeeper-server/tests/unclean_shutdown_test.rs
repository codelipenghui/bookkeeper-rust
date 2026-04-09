//! Tests for unclean shutdown detection (DIRTY file).
//!
//! Matches Java's UncleanShutdownDetectionImpl:
//! - DIRTY file created in each ledger dir after journal replay completes
//! - Removed on clean shutdown
//! - Detected on next startup

use bookkeeper_config::server::ServerConfig;
use bookkeeper_server::server::BookieServer;
use tempfile::TempDir;

const DIRTY_FILENAME: &str = "DIRTY";

fn test_config(dir: &TempDir) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config
}

#[tokio::test]
async fn dirty_file_created_on_startup() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();

    // After successful start, DIRTY file should exist in the ledger dir
    let dirty_path = dir.path().join("current").join(DIRTY_FILENAME);
    assert!(dirty_path.exists(), "DIRTY file should exist after startup");

    server.shutdown().await;
}

#[tokio::test]
async fn dirty_file_removed_on_clean_shutdown() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();

    let dirty_path = dir.path().join("current").join(DIRTY_FILENAME);
    assert!(dirty_path.exists());

    server.shutdown().await;

    // After graceful shutdown, DIRTY file should be removed
    assert!(!dirty_path.exists(), "DIRTY file should be removed after clean shutdown");
}

#[tokio::test]
async fn restart_with_dirty_file_succeeds() {
    let dir = TempDir::new().unwrap();

    // First clean cycle
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    server.shutdown().await;

    // Manually create DIRTY file (simulating an unclean shutdown)
    let dirty_path = dir.path().join("current").join(DIRTY_FILENAME);
    std::fs::File::create(&dirty_path).unwrap();
    assert!(dirty_path.exists());

    // Restart should succeed (DIRTY is detected and logged but not blocking)
    let server = BookieServer::start(test_config(&dir)).await.unwrap();

    // After successful restart, DIRTY should still exist (it was recreated)
    assert!(dirty_path.exists());

    server.shutdown().await;

    // After clean shutdown, DIRTY removed again
    assert!(!dirty_path.exists());
}

#[tokio::test]
async fn dirty_file_recreated_on_each_start() {
    let dir = TempDir::new().unwrap();

    // First start: creates DIRTY
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let dirty_path = dir.path().join("current").join(DIRTY_FILENAME);
    assert!(dirty_path.exists());
    server.shutdown().await;
    assert!(!dirty_path.exists());

    // Second start: recreates DIRTY
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    assert!(dirty_path.exists());
    server.shutdown().await;
    assert!(!dirty_path.exists());
}

#[tokio::test]
async fn dirty_file_in_each_ledger_dir() {
    // Create two separate temp dirs as ledger directories
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let journal_dir = TempDir::new().unwrap();

    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![
        dir1.path().to_string_lossy().to_string(),
        dir2.path().to_string_lossy().to_string(),
    ];
    config.journal_directory = journal_dir.path().to_string_lossy().to_string();

    let server = BookieServer::start(config).await.unwrap();

    // DIRTY should exist in BOTH ledger dirs (inside current/)
    assert!(dir1.path().join("current").join(DIRTY_FILENAME).exists());
    assert!(dir2.path().join("current").join(DIRTY_FILENAME).exists());

    server.shutdown().await;

    // Both removed on clean shutdown
    assert!(!dir1.path().join("current").join(DIRTY_FILENAME).exists());
    assert!(!dir2.path().join("current").join(DIRTY_FILENAME).exists());
}
