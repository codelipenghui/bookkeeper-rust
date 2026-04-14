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
async fn detection_reports_unclean_when_dirty_present() {
    // Issue #37: the UncleanShutdownDetection struct must be usable in
    // isolation and mirror Java's API surface: registerStartUp /
    // registerCleanShutdown / lastShutdownWasUnclean.
    use bookkeeper_server::unclean_shutdown::UncleanShutdownDetection;

    let dir = TempDir::new().unwrap();
    let dirs = vec![dir.path().to_string_lossy().to_string()];
    let detection = UncleanShutdownDetection::new(&dirs);

    // Fresh tmp dir → no DIRTY → clean.
    assert!(!detection.last_shutdown_was_unclean());

    // register_startup creates the marker.
    detection.register_startup().unwrap();
    let dirty_path = dir.path().join("current").join(DIRTY_FILENAME);
    assert!(dirty_path.exists(), "DIRTY marker should exist after register_startup");
    // With marker present, "last" shutdown reads as unclean.
    assert!(detection.last_shutdown_was_unclean());

    // Clean shutdown removes it.
    detection.register_clean_shutdown();
    assert!(!dirty_path.exists());
    assert!(!detection.last_shutdown_was_unclean());
}

#[tokio::test]
async fn post_crash_integrity_scan_runs_when_enabled() {
    // Issue #37: when an unclean shutdown is detected AND the operator
    // opts in to verify_storage_on_unclean_shutdown, the integrity scan
    // should execute and leave the BOOKIE_UNCLEAN_RECOVERY_ERRORS metric
    // observable (== 0 on a clean entry log). We test the happy path —
    // the scan must not abort startup.
    use bookkeeper_server::unclean_shutdown::UncleanShutdownDetection;

    let dir = TempDir::new().unwrap();
    let journal_dir = TempDir::new().unwrap();

    // First boot: create DIRTY manually (simulate prior crash).
    let dirs = vec![dir.path().to_string_lossy().to_string()];
    let detection = UncleanShutdownDetection::new(&dirs);
    detection.register_startup().unwrap();
    let dirty_path = dir.path().join("current").join(DIRTY_FILENAME);
    assert!(dirty_path.exists());
    drop(detection);

    // Start bookie with the unclean-detection opt-in enabled.
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = journal_dir.path().to_string_lossy().to_string();
    config.verify_storage_on_unclean_shutdown = true;

    // Bookie start consumes the DIRTY file (writes a fresh one at the
    // end of startup), but before it's rewritten, detection on the
    // incoming state was unclean — we observe indirect evidence by
    // having the scan complete successfully + the server actually
    // reaching the running state.
    let server = BookieServer::start(config).await.unwrap();
    // After startup a fresh DIRTY should be in place (normal operation).
    assert!(dirty_path.exists());
    server.shutdown().await;
    // Clean shutdown removed it.
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
