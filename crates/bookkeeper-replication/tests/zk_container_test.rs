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

// ---------------------------------------------------------------------------
// #45 list_ledgers_to_rereplicate — enumeration with predicate filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_ledgers_to_rereplicate_enumerates_with_filter() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_list_ur";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let mgr = ZkUnderreplicationManager::new(
        client, paths, "lister:3181".to_string()
    ).await.unwrap();

    mgr.mark_ledger_underreplicated(10, &["boom:3181".into()]).await.unwrap();
    mgr.mark_ledger_underreplicated(11, &["other:3181".into()]).await.unwrap();
    mgr.mark_ledger_underreplicated(12, &["boom:3181".into(), "other:3181".into()]).await.unwrap();

    // All three.
    let all = mgr.list_ledgers_to_rereplicate(|_, _| true).await.unwrap();
    assert_eq!(all.len(), 3);

    // Filter to "boom": ledgers 10 and 12.
    let boom = mgr.list_ledgers_to_rereplicate(|_, r| r.iter().any(|x| x == "boom:3181"))
        .await.unwrap();
    let boom_ids: std::collections::HashSet<i64> = boom.iter().map(|(l, _)| *l).collect();
    assert_eq!(boom_ids, [10i64, 12i64].into_iter().collect());
}

// ---------------------------------------------------------------------------
// #43 CTime persistence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ctime_set_get_roundtrip() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_ctime";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let mgr = ZkUnderreplicationManager::new(
        client, paths, "auditor:3181".to_string()
    ).await.unwrap();

    // Absent znode reads as -1 (Java default).
    assert_eq!(mgr.get_check_all_ledgers_ctime().await.unwrap(), -1);
    assert_eq!(mgr.get_placement_policy_check_ctime().await.unwrap(), -1);
    assert_eq!(mgr.get_replicas_check_ctime().await.unwrap(), -1);

    mgr.set_check_all_ledgers_ctime(1_700_000_000_000).await.unwrap();
    mgr.set_placement_policy_check_ctime(1_700_000_000_111).await.unwrap();
    mgr.set_replicas_check_ctime(1_700_000_000_222).await.unwrap();

    assert_eq!(mgr.get_check_all_ledgers_ctime().await.unwrap(), 1_700_000_000_000);
    assert_eq!(mgr.get_placement_policy_check_ctime().await.unwrap(), 1_700_000_000_111);
    assert_eq!(mgr.get_replicas_check_ctime().await.unwrap(), 1_700_000_000_222);

    // Updating is an upsert (set→create fallback is transparent).
    mgr.set_check_all_ledgers_ctime(1_700_000_000_999).await.unwrap();
    assert_eq!(mgr.get_check_all_ledgers_ctime().await.unwrap(), 1_700_000_000_999);
}

// ---------------------------------------------------------------------------
// #44 Delay znode watch fires on change
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delay_watch_fires_on_update() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_delay_watch";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let mgr = ZkUnderreplicationManager::new(
        client, paths, "auditor:3181".to_string()
    ).await.unwrap();

    // Prime the znode so watch_lost_bookie_recovery_delay returns a normal watcher.
    mgr.set_lost_bookie_recovery_delay(7).await.unwrap();

    let (initial, watcher) = mgr.watch_lost_bookie_recovery_delay(0).await.unwrap();
    assert_eq!(initial, 7);

    let mgr_clone = std::sync::Arc::new(mgr);
    let updater = mgr_clone.clone();
    tokio::spawn(async move {
        // Small delay so the watcher is really listening.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        updater.set_lost_bookie_recovery_delay(42).await.unwrap();
    });

    // Wait up to 5s for the watch to fire.
    let event = tokio::time::timeout(std::time::Duration::from_secs(5), watcher.changed()).await;
    assert!(event.is_ok(), "delay watcher should fire on update");

    assert_eq!(mgr_clone.get_lost_bookie_recovery_delay(0).await.unwrap(), 42);
}

// ---------------------------------------------------------------------------
// #42 Persistent tree watch fires when ledger marked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn underreplication_tree_watch_fires_on_new_marker() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_tree_watch";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let mgr = std::sync::Arc::new(
        ZkUnderreplicationManager::new(client, paths, "w:3181".into()).await.unwrap()
    );

    let mut watcher = mgr.watch_underreplicated_tree().await.unwrap();

    let marker = mgr.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        marker.mark_ledger_underreplicated(9_999, &["dead:3181".into()]).await.unwrap();
    });

    let event = tokio::time::timeout(std::time::Duration::from_secs(5), watcher.changed()).await;
    assert!(event.is_ok(), "tree watcher should fire when a marker is created");
}
