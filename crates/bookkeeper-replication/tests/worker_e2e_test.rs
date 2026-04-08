//! End-to-end tests for `ReplicationWorker` against real Rust bookies and ZK.
//!
//! These tests stand up a real autorecovery pipeline:
//!   1. ZooKeeper container
//!   2. 4 in-process Rust bookies (each registering with ZK)
//!   3. A ledger created with ensemble [b0, b1, b2], entries written, then closed
//!   4. The ledger is marked under-replicated naming b2 as the missing replica
//!   5. ReplicationWorker is spawned and given a chance to claim + repair
//!   6. We assert: metadata's ensemble has b2 replaced by b3, the underrep
//!      marker is gone, and entries are readable from the new bookie
//!
//! Skipped gracefully if Docker is unavailable.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::zk_client::ZkBookKeeper;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_metadata::traits::{LedgerLayout, LedgerManager, RegistrationManager};
use bookkeeper_metadata::zk::ledger_manager::ZkLedgerManager;
use bookkeeper_metadata::zk::registration::ZkRegistrationManager;
use bookkeeper_metadata::zk_path::ZkPaths;
use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::{generate_master_key, DigestType};
use bookkeeper_protocol::v2::response::V2Response;
use bookkeeper_replication::auditor::Auditor;
use bookkeeper_replication::underreplication::ZkUnderreplicationManager;
use bookkeeper_replication::worker::ReplicationWorker;
use bookkeeper_server::server::BookieServer;
use tempfile::TempDir;
use testcontainers::{
    core::WaitFor, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt,
};
use tokio::sync::watch;

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

struct ZkFixture {
    _container: ContainerAsync<GenericImage>,
    connect: String,
}

static ZK: tokio::sync::OnceCell<Option<Arc<ZkFixture>>> = tokio::sync::OnceCell::const_new();

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
        manager_factory_class: "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory"
            .to_string(),
        manager_version: 1,
    };
    mgr.init_new_cluster(&layout, &uuid::Uuid::new_v4().to_string())
        .await
        .unwrap();
}

/// Spawn N in-process Rust bookies registered with the given ZK.
async fn spawn_bookies(
    n: usize,
    zk_connect: &str,
    ledger_root: &str,
) -> (Vec<BookieServer>, Vec<TempDir>, Vec<String>) {
    let mut bookies = Vec::new();
    let mut dirs = Vec::new();
    let mut addrs = Vec::new();
    for _ in 0..n {
        let dir = TempDir::new().unwrap();
        let mut config = ServerConfig::default();
        config.bookie_port = 0;
        config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
        config.journal_directory = dir.path().to_string_lossy().to_string();
        config.zk_servers = Some(zk_connect.to_string());
        config.zk_ledger_root = ledger_root.to_string();

        let server = BookieServer::start(config).await.unwrap();
        addrs.push(server.local_addr().to_string());
        bookies.push(server);
        dirs.push(dir);
    }
    // Give ZK ephemeral registration a moment to land
    tokio::time::sleep(Duration::from_millis(500)).await;
    (bookies, dirs, addrs)
}

/// Read entry payload from a single bookie via direct V2 read.
async fn read_entry_from(
    bookie_addr: &str,
    ledger_id: i64,
    entry_id: i64,
    master_key: [u8; MASTER_KEY_LENGTH],
) -> Option<Vec<u8>> {
    let socket: std::net::SocketAddr = bookie_addr.parse().ok()?;
    let conn = BookieConnection::connect(socket).await.ok()?;
    match conn
        .send_read_entry(0, ledger_id, entry_id, Some(master_key))
        .await
        .ok()?
    {
        V2Response::Read {
            error_code,
            data: Some(data),
            ..
        } if error_code == V2_EOK => Some(data.to_vec()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn worker_replicates_closed_ledger_to_replacement_bookie() {
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/test_worker_closed";
    bootstrap_cluster(&zk_connect, root).await;

    // Spawn 4 bookies — first 3 form the ensemble, 4th is the replacement target.
    let (bookies, _dirs, addrs) = spawn_bookies(4, &zk_connect, root).await;
    assert_eq!(addrs.len(), 4);

    // Create + write + close a ledger via ZkBookKeeper. Use ensemble=write=ack=3
    // so all entries land on all 3 bookies (deterministic).
    let mut bk = ZkBookKeeper::new(&zk_connect, root).await.unwrap();
    let password = b"repl-test";
    let (mut wh, conns) = bk
        .create_ledger(3, 3, 3, DigestType::Crc32c, password)
        .await
        .unwrap();
    let ledger_id = wh.ledger_id();

    for i in 0..10 {
        wh.append(&conns, format!("entry-{i}").as_bytes())
            .await
            .unwrap();
    }
    bk.close_ledger(&wh).await.unwrap();

    // Find which 3 of the 4 bookies actually got selected for the ensemble.
    let zk_client = zookeeper_client::Client::connect(&zk_connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let lm: Arc<dyn LedgerManager> =
        Arc::new(ZkLedgerManager::new(zk_client.clone(), paths.clone()));
    let md = lm.read_ledger_metadata(ledger_id).await.unwrap();
    let original_ensemble: Vec<String> = md.value().all_ensembles()[&0].clone();
    assert_eq!(original_ensemble.len(), 3);
    let original_set: HashSet<String> = original_ensemble.iter().cloned().collect();
    let replacement_target: String = addrs
        .iter()
        .find(|a| !original_set.contains(*a))
        .expect("4th bookie should not be in the original ensemble")
        .clone();
    let victim_bookie = original_ensemble[2].clone();

    // Mark the ledger underreplicated naming the victim as the missing replica.
    // (Normally the auditor does this — we simulate it directly to focus on
    // the worker's behavior.)
    let ur_manager = Arc::new(
        ZkUnderreplicationManager::new(
            zk_client.clone(),
            paths.clone(),
            "test-worker:3181".to_string(),
        )
        .await
        .unwrap(),
    );
    ur_manager
        .mark_ledger_underreplicated(ledger_id, &[victim_bookie.clone()])
        .await
        .unwrap();

    // Take the victim bookie offline so the worker actually has to copy entries
    // from the surviving replicas. Find its index and shut it down. Other bookies
    // remain up and registered in ZK so the worker can pick the replacement target.
    let victim_idx = addrs
        .iter()
        .position(|a| *a == victim_bookie)
        .expect("victim must be one of our spawned bookies");
    let mut bookies_iter = bookies.into_iter().enumerate();
    let mut survivors = Vec::new();
    while let Some((idx, b)) = bookies_iter.next() {
        if idx == victim_idx {
            b.shutdown().await;
        } else {
            survivors.push(b);
        }
    }
    // Let ZK ephemeral expire and the bookie list update
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Spawn the replication worker.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut worker = ReplicationWorker::new(
        ur_manager.clone(),
        lm.clone(),
        "test-worker:3181".to_string(),
        100, // backoff_ms
        shutdown_rx,
    );
    let worker_handle = tokio::spawn(async move { worker.run().await });

    // Poll for completion: the worker should claim the ledger, copy entries,
    // update the ensemble metadata, and clear the underrep marker.
    let mut new_ensemble: Option<Vec<String>> = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let md_now = lm.read_ledger_metadata(ledger_id).await.unwrap();
        let ens = md_now.value().all_ensembles()[&0].clone();
        if ens != original_ensemble {
            new_ensemble = Some(ens);
            break;
        }
    }

    // Stop the worker
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;

    let new_ensemble = new_ensemble.expect(
        "worker should have updated the ensemble metadata within ~10s",
    );

    // The new ensemble should: keep the 2 survivors, swap victim → replacement
    assert_eq!(new_ensemble.len(), 3);
    assert!(
        !new_ensemble.contains(&victim_bookie),
        "victim {victim_bookie} should be removed from ensemble; new = {new_ensemble:?}"
    );
    assert!(
        new_ensemble.contains(&replacement_target),
        "replacement {replacement_target} should be in new ensemble = {new_ensemble:?}"
    );
    let surviving: Vec<&String> = original_ensemble
        .iter()
        .filter(|a| **a != victim_bookie)
        .collect();
    for s in &surviving {
        assert!(
            new_ensemble.contains(s),
            "surviving bookie {s} should still be in ensemble = {new_ensemble:?}"
        );
    }

    // Verify entries are readable from the replacement bookie directly.
    let master_key = generate_master_key(password);
    for i in 0..10 {
        let payload = read_entry_from(&replacement_target, ledger_id, i, master_key)
            .await
            .unwrap_or_else(|| panic!("replacement bookie should serve entry {i}"));
        // Strip the 32-byte entry header (ledger_id, entry_id, lac, length)
        assert!(payload.len() >= 32);
        let body = &payload[32..];
        assert!(
            body.ends_with(format!("entry-{i}").as_bytes()),
            "entry {i} payload mismatch on replacement: got {:?}",
            String::from_utf8_lossy(body)
        );
    }

    // Underrep marker should be gone (mark_ledger_replicated)
    assert_eq!(
        ur_manager.get_ledger_to_rereplicate().await.unwrap(),
        None,
        "no more under-replicated ledgers should remain"
    );

    // Cleanup
    for b in survivors {
        b.shutdown().await;
    }
}

#[tokio::test]
async fn auditor_marks_ledgers_underreplicated_when_bookie_dies() {
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/test_auditor_dead_bookie";
    bootstrap_cluster(&zk_connect, root).await;

    // 3 bookies, all in the ensemble.
    let (bookies, _dirs, addrs) = spawn_bookies(3, &zk_connect, root).await;

    // Create + close a ledger using all 3 bookies.
    let mut bk = ZkBookKeeper::new(&zk_connect, root).await.unwrap();
    let (mut wh, conns) = bk
        .create_ledger(3, 3, 3, DigestType::Crc32c, b"audit-test")
        .await
        .unwrap();
    let ledger_id = wh.ledger_id();
    for i in 0..3 {
        wh.append(&conns, format!("e{i}").as_bytes()).await.unwrap();
    }
    bk.close_ledger(&wh).await.unwrap();

    // Wire up the auditor against the same ZK.
    let zk_client = zookeeper_client::Client::connect(&zk_connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let lm: Arc<dyn LedgerManager> =
        Arc::new(ZkLedgerManager::new(zk_client.clone(), paths.clone()));
    let ur_manager = Arc::new(
        ZkUnderreplicationManager::new(
            zk_client.clone(),
            paths.clone(),
            "auditor:3181".into(),
        )
        .await
        .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut auditor = Auditor::new(
        zk_client.clone(),
        paths.clone(),
        ur_manager.clone(),
        lm.clone(),
        0, // lost_bookie_recovery_delay_secs — no delay for tests
        shutdown_rx,
    );
    let auditor_handle = tokio::spawn(async move { auditor.run().await });

    // Let the auditor reach steady state with all 3 bookies in known set.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Kill bookie 0 — its ZK ephemeral expires, the auditor's watcher fires.
    let mut iter = bookies.into_iter();
    let victim = iter.next().unwrap();
    let victim_addr = addrs[0].clone();
    victim.shutdown().await;
    let survivors: Vec<_> = iter.collect();

    // Poll for the auditor to mark the ledger underreplicated.
    let mut marked = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if ur_manager.get_ledger_to_rereplicate().await.unwrap() == Some(ledger_id) {
            marked = true;
            break;
        }
    }

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), auditor_handle).await;

    assert!(
        marked,
        "auditor should have marked ledger {ledger_id} as under-replicated after \
         bookie {victim_addr} died"
    );

    for b in survivors {
        b.shutdown().await;
    }
}

#[tokio::test]
async fn auditor_plus_worker_full_repair_pipeline() {
    // The complete autorecovery story: auditor detects a dead bookie, marks
    // affected ledgers as under-replicated, the worker claims them and copies
    // entries to a replacement bookie. This is the closest test we have to
    // production behavior — both background tasks running concurrently.
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/test_full_pipeline";
    bootstrap_cluster(&zk_connect, root).await;

    // 4 bookies — 3 form the ensemble, 1 is the available replacement.
    let (bookies, _dirs, addrs) = spawn_bookies(4, &zk_connect, root).await;

    // Create + close a ledger with 5 entries.
    let mut bk = ZkBookKeeper::new(&zk_connect, root).await.unwrap();
    let password = b"e2e";
    let (mut wh, conns) = bk
        .create_ledger(3, 3, 3, DigestType::Crc32c, password)
        .await
        .unwrap();
    let ledger_id = wh.ledger_id();
    for i in 0..5 {
        wh.append(&conns, format!("payload-{i}").as_bytes())
            .await
            .unwrap();
    }
    bk.close_ledger(&wh).await.unwrap();

    // Identify the original ensemble and replacement bookie.
    let zk_client = zookeeper_client::Client::connect(&zk_connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let lm: Arc<dyn LedgerManager> =
        Arc::new(ZkLedgerManager::new(zk_client.clone(), paths.clone()));
    let initial_md = lm.read_ledger_metadata(ledger_id).await.unwrap();
    let original_ensemble: Vec<String> = initial_md.value().all_ensembles()[&0].clone();
    let original_set: HashSet<String> = original_ensemble.iter().cloned().collect();
    let replacement_target: String = addrs
        .iter()
        .find(|a| !original_set.contains(*a))
        .expect("4th bookie must be outside the ensemble")
        .clone();

    // Spawn both background tasks against the same ZK.
    let ur_manager = Arc::new(
        ZkUnderreplicationManager::new(
            zk_client.clone(),
            paths.clone(),
            "pipeline:3181".into(),
        )
        .await
        .unwrap(),
    );

    let (auditor_tx, auditor_rx) = watch::channel(false);
    let mut auditor = Auditor::new(
        zk_client.clone(),
        paths.clone(),
        ur_manager.clone(),
        lm.clone(),
        0, // immediate audit on failure
        auditor_rx,
    );
    let auditor_handle = tokio::spawn(async move { auditor.run().await });

    let (worker_tx, worker_rx) = watch::channel(false);
    let mut worker = ReplicationWorker::new(
        ur_manager.clone(),
        lm.clone(),
        "pipeline:3181".into(),
        100,
        worker_rx,
    );
    let worker_handle = tokio::spawn(async move { worker.run().await });

    // Let both tasks reach steady state with the full bookie list.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Pick a victim from the original ensemble and kill it.
    let victim_addr = original_ensemble[1].clone();
    let victim_idx = addrs.iter().position(|a| *a == victim_addr).unwrap();
    let mut survivors = Vec::new();
    for (idx, b) in bookies.into_iter().enumerate() {
        if idx == victim_idx {
            b.shutdown().await;
        } else {
            survivors.push(b);
        }
    }

    // Wait for the pipeline to converge: auditor detects the failure, marks
    // the ledger UR, worker claims and repairs it.
    let mut new_ensemble: Option<Vec<String>> = None;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let md_now = lm.read_ledger_metadata(ledger_id).await.unwrap();
        let ens = md_now.value().all_ensembles()[&0].clone();
        if ens != original_ensemble {
            new_ensemble = Some(ens);
            break;
        }
    }

    // Stop both tasks.
    let _ = auditor_tx.send(true);
    let _ = worker_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), auditor_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;

    let new_ensemble = new_ensemble.expect(
        "auditor + worker should have repaired the ledger end-to-end within ~20s",
    );

    // Victim must be gone, replacement must be in.
    assert!(!new_ensemble.contains(&victim_addr));
    assert!(new_ensemble.contains(&replacement_target));

    // Replacement bookie should serve all entries.
    let master_key = generate_master_key(password);
    for i in 0..5 {
        let payload = read_entry_from(&replacement_target, ledger_id, i, master_key)
            .await
            .unwrap_or_else(|| panic!("replacement should serve entry {i}"));
        assert!(payload.len() >= 32);
        let body = &payload[32..];
        assert!(
            body.ends_with(format!("payload-{i}").as_bytes()),
            "entry {i} payload mismatch on replacement"
        );
    }

    // Underrep marker should be gone.
    assert_eq!(ur_manager.get_ledger_to_rereplicate().await.unwrap(), None);

    for b in survivors {
        b.shutdown().await;
    }
}

#[tokio::test]
async fn worker_skips_when_replication_disabled() {
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/test_worker_disabled";
    bootstrap_cluster(&zk_connect, root).await;

    let (bookies, _dirs, _addrs) = spawn_bookies(3, &zk_connect, root).await;

    let zk_client = zookeeper_client::Client::connect(&zk_connect).await.unwrap();
    let paths = ZkPaths::new(root);

    let ur_manager = Arc::new(
        ZkUnderreplicationManager::new(
            zk_client.clone(),
            paths.clone(),
            "w:3181".to_string(),
        )
        .await
        .unwrap(),
    );

    // Create a ledger and mark it underreplicated so there IS work to do.
    let mut bk = ZkBookKeeper::new(&zk_connect, root).await.unwrap();
    let (mut wh, conns) = bk
        .create_ledger(3, 3, 3, DigestType::Crc32c, b"")
        .await
        .unwrap();
    let ledger_id = wh.ledger_id();
    wh.append(&conns, b"entry").await.unwrap();
    bk.close_ledger(&wh).await.unwrap();
    ur_manager
        .mark_ledger_underreplicated(ledger_id, &["fake:3181".into()])
        .await
        .unwrap();

    // Disable replication globally by creating the disable znode.
    let disable_path = format!("{}/disable", paths.underreplication());
    zk_client
        .create(
            &disable_path,
            &[],
            &zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all()),
        )
        .await
        .unwrap();

    // Spawn the worker.
    let lm: Arc<dyn LedgerManager> =
        Arc::new(ZkLedgerManager::new(zk_client.clone(), paths.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut worker = ReplicationWorker::new(
        ur_manager.clone(),
        lm,
        "w:3181".into(),
        50,
        shutdown_rx,
    );
    let h = tokio::spawn(async move { worker.run().await });

    // Give the worker a few cycles to (not) do anything.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The marker should still be there — worker must not have claimed it.
    // Re-enable replication briefly so we can use get_ledger_to_rereplicate
    // to verify the marker survived. Delete the disable znode and re-check.
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), h).await;

    zk_client.delete(&disable_path, None).await.unwrap();
    assert_eq!(
        ur_manager.get_ledger_to_rereplicate().await.unwrap(),
        Some(ledger_id),
        "ledger should still be under-replicated — worker should have skipped it while disabled"
    );

    for b in bookies {
        b.shutdown().await;
    }
}
