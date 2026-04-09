//! Integration tests for the replication crate against a real ZooKeeper.
//!
//! Exercises ZkUnderreplicationManager (mark/claim/release) and AuditorElector
//! (leader election) against a live ZK server via testcontainers. Tests share
//! a single ZK container (via `bookkeeper_test_utils::shared_zk`) and isolate
//! themselves with per-test ZK ledger root paths.

use bookkeeper_metadata::zk_path::ZkPaths;
use bookkeeper_replication::underreplication::ZkUnderreplicationManager;
use bookkeeper_test_utils::{bootstrap_cluster, shared_zk};

// ---------------------------------------------------------------------------
// ZkUnderreplicationManager
// ---------------------------------------------------------------------------

#[tokio::test]
async fn underreplication_mark_and_claim_cycle() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_ur";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let mgr = ZkUnderreplicationManager::new(
        client, paths, "worker-1:3181".to_string()
    ).await.unwrap();

    // Mark ledger 42 as under-replicated with missing replica
    mgr.mark_ledger_underreplicated(42, &["dead-bookie:3181".to_string()])
        .await.unwrap();

    // Claim it for replication
    let claimed = mgr.get_ledger_to_rereplicate().await.unwrap();
    assert_eq!(claimed, Some(42), "should claim the under-replicated ledger");

    // Mark as replicated (completes the lifecycle)
    mgr.mark_ledger_replicated(42).await.unwrap();

    // After marking replicated, nothing left to rereplicate
    let empty = mgr.get_ledger_to_rereplicate().await.unwrap();
    assert_eq!(empty, None, "no more ledgers to rereplicate");
}

#[tokio::test]
async fn underreplication_merge_missing_replicas() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_ur_merge";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let mgr = ZkUnderreplicationManager::new(
        client, paths, "w:3181".to_string()
    ).await.unwrap();

    // First mark with one missing replica
    mgr.mark_ledger_underreplicated(77, &["b1:3181".to_string()])
        .await.unwrap();

    // Mark again with a different missing replica — should merge
    mgr.mark_ledger_underreplicated(77, &["b2:3181".to_string()])
        .await.unwrap();

    // Claim it — should still be found
    let claimed = mgr.get_ledger_to_rereplicate().await.unwrap();
    assert_eq!(claimed, Some(77));
}

#[tokio::test]
async fn underreplication_release_lock_allows_re_claim() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_release";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let mgr = ZkUnderreplicationManager::new(
        client, paths, "w:3181".to_string()
    ).await.unwrap();

    mgr.mark_ledger_underreplicated(5, &["x:3181".to_string()])
        .await.unwrap();

    let claimed = mgr.get_ledger_to_rereplicate().await.unwrap();
    assert_eq!(claimed, Some(5));

    // Release lock without marking replicated — another worker can claim it
    mgr.release_lock(5).await.unwrap();

    let reclaimed = mgr.get_ledger_to_rereplicate().await.unwrap();
    assert_eq!(reclaimed, Some(5), "should be re-claimable after release");
}

#[tokio::test]
async fn underreplication_replication_enabled_toggles() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_enabled";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let mgr = ZkUnderreplicationManager::new(
        client.clone(), paths.clone(), "w:3181".to_string()
    ).await.unwrap();

    // Initially enabled (no disable znode)
    assert!(mgr.is_replication_enabled().await.unwrap());

    // Create the disable znode directly
    let disable_path = format!("{}/disable", paths.underreplication());
    let options = zookeeper_client::CreateMode::Persistent
        .with_acls(zookeeper_client::Acls::anyone_all());
    client.create(&disable_path, &[], &options).await.unwrap();

    // Should now report disabled
    assert!(!mgr.is_replication_enabled().await.unwrap());

    // Clean up: delete the disable znode
    client.delete(&disable_path, None).await.unwrap();
    assert!(mgr.is_replication_enabled().await.unwrap());
}

#[tokio::test]
async fn underreplication_get_none_when_no_markers() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_empty_ur";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let mgr = ZkUnderreplicationManager::new(
        client, paths, "w:3181".to_string()
    ).await.unwrap();

    // No markers → get_ledger_to_rereplicate returns None
    let claimed = mgr.get_ledger_to_rereplicate().await.unwrap();
    assert_eq!(claimed, None);
}

// ---------------------------------------------------------------------------
// Auditor election znode presence check
// ---------------------------------------------------------------------------

#[tokio::test]
async fn auditor_election_path_exists_after_underreplication_init() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_elect_path";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);

    // Creating the UR manager should initialize the underreplication subtree.
    let _mgr = ZkUnderreplicationManager::new(
        client.clone(), paths.clone(), "auditor:3181".to_string()
    ).await.unwrap();

    // Verify the underreplication path was created
    let ur_path = paths.underreplication();
    let stat = client.check_stat(ur_path).await.unwrap();
    assert!(stat.is_some(), "underreplication znode should exist after init");

    // And the ledgers/locks subpaths
    let ledgers_path = format!("{}/ledgers", ur_path);
    let locks_path = format!("{}/locks", ur_path);
    assert!(client.check_stat(&ledgers_path).await.unwrap().is_some());
    assert!(client.check_stat(&locks_path).await.unwrap().is_some());
}

#[tokio::test]
async fn underreplication_multiple_ledgers_sequential_lifecycle() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_multi_ur";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let mgr = ZkUnderreplicationManager::new(
        client, paths, "worker:3181".to_string()
    ).await.unwrap();

    // Mark 3 ledgers as under-replicated
    for &lid in &[100i64, 200, 300] {
        mgr.mark_ledger_underreplicated(lid, &["dead:3181".to_string()])
            .await.unwrap();
    }

    // Claim each one and mark replicated before claiming the next (the scan
    // returns the first unlocked underrep ledger it finds in the hex tree,
    // and mark_replicated removes both the marker and the lock).
    let mut claimed = std::collections::HashSet::new();
    for _ in 0..3 {
        let id = mgr.get_ledger_to_rereplicate().await.unwrap()
            .expect("should claim a ledger");
        claimed.insert(id);
        mgr.mark_ledger_replicated(id).await.unwrap();
    }

    assert_eq!(claimed.len(), 3, "should have claimed 3 distinct ledgers");
    assert!(claimed.contains(&100));
    assert!(claimed.contains(&200));
    assert!(claimed.contains(&300));

    // After all are replicated, no more work
    assert_eq!(mgr.get_ledger_to_rereplicate().await.unwrap(), None);
}
