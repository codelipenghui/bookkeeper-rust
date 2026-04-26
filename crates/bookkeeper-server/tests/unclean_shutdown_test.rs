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

/// `register_clean_shutdown()` must NOT run when the storage flush
/// (`storage.checkpoint()`) failed during shutdown. The DIRTY marker
/// is the bookie's "last shutdown was unclean" signal — removing it
/// after a failed flush tells the next boot the previous run was
/// clean, defeating the recovery path that was designed to surface
/// crash-induced corruption.
///
/// Reproduce by replacing `current/lastMark` with a directory before
/// shutdown so `Checkpoint::complete` returns IsADirectory and
/// `storage.checkpoint()` returns Err. Pre-fix: shutdown swallowed
/// the error and called register_clean_shutdown anyway, removing the
/// DIRTY marker. Post-fix: the DIRTY marker is preserved.
#[tokio::test]
async fn dirty_file_preserved_on_failed_checkpoint_during_shutdown() {
    use bookkeeper_protocol::framing::BookieCodec;
    use bookkeeper_protocol::v3::{decode_v3_response, encode_v3_request};
    use bookkeeper_proto::{
        AddRequest, BkPacketHeader, OperationType, ProtocolVersion, Request, StatusCode,
    };
    use bytes::BytesMut;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio_util::codec::{Decoder, Encoder};

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let current = dir.path().join("current");
    let dirty_path = current.join(DIRTY_FILENAME);
    assert!(dirty_path.exists(), "DIRTY file should exist after startup");

    // Drive one synced AddEntry over V3 so the journal advances its
    // current_mark past 0. Without this the storage's checkpoint()
    // produces no Checkpoint to persist (cp = None), Checkpoint::complete
    // is never called, and the test wouldn't actually exercise the
    // failure path we want to gate.
    let mut entry_body = Vec::with_capacity(40);
    entry_body.extend_from_slice(&1i64.to_be_bytes());     // ledger
    entry_body.extend_from_slice(&0i64.to_be_bytes());     // entry id
    entry_body.extend_from_slice(&(-1i64).to_be_bytes());  // lac
    entry_body.extend_from_slice(&5i64.to_be_bytes());     // length
    entry_body.extend_from_slice(b"hello");
    let master_key = bookkeeper_protocol::digest::generate_master_key(b"test");
    let req = Request {
        header: BkPacketHeader {
            version: ProtocolVersion::VersionThree as i32,
            operation: OperationType::AddEntry as i32,
            txn_id: 1,
            priority: Some(0),
        },
        add_request: Some(AddRequest {
            ledger_id: 1,
            entry_id: 0,
            master_key: master_key.to_vec(),
            body: entry_body,
            flag: None,
            write_flags: None,
        }),
        ..Default::default()
    };
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let mut read_buf = BytesMut::with_capacity(4096);
    let req_bytes = encode_v3_request(&req);
    let mut out = BytesMut::new();
    codec.encode(req_bytes.freeze(), &mut out).unwrap();
    writer.write_all(&out).await.unwrap();
    let resp = loop {
        if let Ok(Some(frame)) = codec.decode(&mut read_buf) {
            break decode_v3_response(&frame).unwrap();
        }
        let n = reader.read_buf(&mut read_buf).await.unwrap();
        assert!(n > 0, "server closed connection during AddEntry");
    };
    assert_eq!(
        resp.status,
        StatusCode::Eok as i32,
        "synced AddEntry must succeed before we sabotage lastMark",
    );
    drop(reader);
    drop(writer);

    // Replace `current/lastMark` with a directory so File::create
    // returns IsADirectory and Checkpoint::complete fails. The
    // checkpoint() call inside shutdown will then return Err.
    let last_mark = current.join("lastMark");
    if last_mark.is_file() {
        std::fs::remove_file(&last_mark).unwrap();
    }
    std::fs::create_dir(&last_mark).unwrap();

    server.shutdown().await;

    assert!(
        dirty_path.exists(),
        "DIRTY file was removed even though checkpoint failed; \
         next boot will think the previous shutdown was clean and \
         skip recovery, masking any crash-induced corruption",
    );
}
