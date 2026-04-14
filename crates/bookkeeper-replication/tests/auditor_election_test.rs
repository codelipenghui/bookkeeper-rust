//! End-to-end tests for `AuditorElector` leader election against real ZK.
//!
//! Verifies the leader election protocol:
//!   1. A single elector becomes leader after creating its ephemeral
//!      sequential vote znode and stamping the election path with its
//!      bookie ID.
//!   2. With multiple electors, exactly one wins (the one whose ZK
//!      sequence number is the smallest).
//!   3. When the current leader exits cleanly, its successor takes over
//!      without manual intervention — the follower's `wait_for_deletion`
//!      poll wakes up, re-enters election, and is elected next.
//!
//! All tests share a single ZK container via `OnceCell`. Each test uses a
//! distinct ZK ledger root path so they can run in parallel safely.
//!
//! Skipped gracefully if Docker is unavailable.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bookkeeper_metadata::traits::LedgerManager;
use bookkeeper_metadata::zk::ledger_manager::ZkLedgerManager;
use bookkeeper_metadata::zk_path::ZkPaths;
use bookkeeper_replication::auditor_election::AuditorElector;
use bookkeeper_replication::text_format::parse_auditor_vote;
use bookkeeper_replication::underreplication::ZkUnderreplicationManager;
use bookkeeper_test_utils::{bootstrap_cluster, shared_zk};
use tokio::sync::watch;
use zookeeper_client as zk;

/// Spawn an `AuditorElector::run` in a tokio task, returning the shutdown
/// channel and the join handle. Each elector gets its own ZK clients
/// (one for the elector itself, one for the auditor it spawns) so that a
/// shutdown of one elector does NOT affect any other.
async fn spawn_elector(
    connect: &str,
    root: &str,
    bookie_id: &str,
) -> (watch::Sender<bool>, tokio::task::JoinHandle<()>) {
    let elector_zk = zk::Client::connect(connect).await.unwrap();
    let auditor_zk = zk::Client::connect(connect).await.unwrap();
    let ur_zk = zk::Client::connect(connect).await.unwrap();
    let lm_zk = zk::Client::connect(connect).await.unwrap();

    let paths = ZkPaths::new(root);
    let ur_manager = Arc::new(
        ZkUnderreplicationManager::new(ur_zk, paths.clone(), bookie_id.to_string())
            .await
            .unwrap(),
    );
    let ledger_manager: Arc<dyn LedgerManager> =
        Arc::new(ZkLedgerManager::new(lm_zk, paths.clone()));

    let mut elector = AuditorElector::new(elector_zk, &paths, bookie_id.to_string());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let handle = tokio::spawn(async move {
        let _ = elector
            .run(
                ur_manager,
                ledger_manager,
                auditor_zk,
                paths,
                0, // lost_bookie_recovery_delay_secs
                0, // auditor_periodic_check_interval_secs (disabled)
                0, // auditor_periodic_placement_check_interval_secs (disabled)
                0, // auditor_periodic_replicas_check_interval_secs (disabled)
                None, // placement_policy
                shutdown_rx,
            )
            .await;
    });

    (shutdown_tx, handle)
}

/// Read the bookie ID stamped on the election path znode by the current
/// leader. Returns None if the data is empty (no leader has set it yet) or
/// if the path doesn't exist yet.
async fn current_leader(client: &zk::Client, paths: &ZkPaths) -> Option<String> {
    let path = format!("{}/auditorelection", paths.underreplication());
    let (data, _) = client.get_data(&path).await.ok()?;
    if data.is_empty() {
        return None;
    }
    parse_auditor_vote(&data).ok()?.bookie_id
}

/// Poll for `expected` to become the current leader. Returns true on
/// success, false on timeout.
async fn wait_for_leader(
    client: &zk::Client,
    paths: &ZkPaths,
    expected: &str,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if current_leader(client, paths).await.as_deref() == Some(expected) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Poll for ANY leader to be elected. Returns the bookie ID on success.
async fn wait_for_any_leader(
    client: &zk::Client,
    paths: &ZkPaths,
    timeout: Duration,
) -> Option<String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(leader) = current_leader(client, paths).await {
            return Some(leader);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn election_single_candidate_becomes_leader() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_elect_single";
    bootstrap_cluster(&connect, root).await;

    // The underreplication subtree (and the auditorelection path) is created
    // when the UR manager is constructed; spawn_elector handles that.
    let (shutdown, handle) = spawn_elector(&connect, root, "elector-A:3181").await;

    let observer = zk::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);

    let elected =
        wait_for_leader(&observer, &paths, "elector-A:3181", Duration::from_secs(5)).await;
    assert!(elected, "single candidate should become leader within 5s");

    // Cleanup
    let _ = shutdown.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
}

#[tokio::test]
async fn election_two_candidates_only_one_wins_at_a_time() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_elect_two";
    bootstrap_cluster(&connect, root).await;

    let (shutdown_a, handle_a) = spawn_elector(&connect, root, "elector-A:3181").await;
    let (shutdown_b, handle_b) = spawn_elector(&connect, root, "elector-B:3181").await;

    let observer = zk::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);

    // Exactly one of them must be the leader within a few seconds.
    let leader = wait_for_any_leader(&observer, &paths, Duration::from_secs(5))
        .await
        .expect("some leader should be elected within 5s");
    assert!(
        leader == "elector-A:3181" || leader == "elector-B:3181",
        "leader should be one of the two candidates; got {leader}"
    );

    // Verify ZK has exactly 2 votes under the election path (one per
    // candidate, both ephemeral). Sorting by name, the smallest is the
    // leader's vote.
    let election_path = format!("{}/auditorelection", paths.underreplication());
    let (mut children, _) = observer.get_children(&election_path).await.unwrap();
    children.retain(|c| c.starts_with("V_"));
    children.sort();
    assert_eq!(
        children.len(),
        2,
        "election path should contain exactly 2 votes; got {children:?}"
    );

    // The smallest vote's data MUST contain the bookie ID of the leader.
    let smallest_path = format!("{}/{}", election_path, children[0]);
    let (smallest_data, _) = observer.get_data(&smallest_path).await.unwrap();
    let smallest_text = String::from_utf8(smallest_data).unwrap();
    assert!(
        smallest_text.contains(&leader),
        "smallest vote's data should contain the leader's bookie ID; got {smallest_text:?} for leader {leader}"
    );

    // Cleanup both
    let _ = shutdown_a.send(true);
    let _ = shutdown_b.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_a).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_b).await;
}

#[tokio::test]
async fn election_successor_takes_over_when_leader_exits() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test_elect_takeover";
    bootstrap_cluster(&connect, root).await;

    // Start A first so it's guaranteed to have the smallest sequence number.
    let (shutdown_a, handle_a) = spawn_elector(&connect, root, "elector-A:3181").await;

    let observer = zk::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);

    assert!(
        wait_for_leader(&observer, &paths, "elector-A:3181", Duration::from_secs(5)).await,
        "A should become leader first"
    );

    // Now start B as a follower (its vote will be the larger sequence number,
    // so it'll be in the Waiting state watching A's vote).
    let (shutdown_b, handle_b) = spawn_elector(&connect, root, "elector-B:3181").await;

    // Verify B has joined the election (2 votes present) but A is still leader.
    {
        let election_path = format!("{}/auditorelection", paths.underreplication());
        // Allow a brief moment for B's vote to be created.
        for _ in 0..30 {
            let (children, _) = observer.get_children(&election_path).await.unwrap();
            let votes: Vec<_> = children.iter().filter(|c| c.starts_with("V_")).collect();
            if votes.len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let (children, _) = observer.get_children(&election_path).await.unwrap();
        let votes: Vec<_> = children.iter().filter(|c| c.starts_with("V_")).collect();
        assert_eq!(votes.len(), 2, "both A and B should have votes");
        assert_eq!(
            current_leader(&observer, &paths).await.as_deref(),
            Some("elector-A:3181"),
            "A should still be the leader before takeover"
        );
    }

    // Shut A down → its run loop exits → its vote is deleted → B's
    // wait_for_deletion (1s poll) eventually wakes up → B re-enters
    // election → smallest now → elected.
    let _ = shutdown_a.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), handle_a).await;

    // B should become the leader within ~3s (1s poll + election overhead).
    assert!(
        wait_for_leader(&observer, &paths, "elector-B:3181", Duration::from_secs(10)).await,
        "B should take over as leader after A exits"
    );

    // Cleanup B
    let _ = shutdown_b.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_b).await;
}
