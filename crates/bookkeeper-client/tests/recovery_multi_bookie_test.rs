//! Recovery tests with multiple bookies.
//!
//! These exercise the full fence + forward-read + close flow against an
//! ensemble of more than one bookie, with and without bookie failures
//! during recovery.

use std::sync::Arc;

use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::client::BookKeeperClient;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_client::recovery::RecoveryHandle;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_protocol::digest::{generate_master_key, DigestManager, DigestType};
use bookkeeper_server::server::BookieServer;
use tempfile::TempDir;

fn test_config(dir: &TempDir) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config
}

async fn spawn_bookies(n: usize) -> (Vec<BookieServer>, Vec<TempDir>, Vec<String>) {
    let mut bookies = Vec::new();
    let mut dirs = Vec::new();
    let mut addrs = Vec::new();
    for _ in 0..n {
        let dir = TempDir::new().unwrap();
        let server = BookieServer::start(test_config(&dir)).await.unwrap();
        addrs.push(server.local_addr().to_string());
        bookies.push(server);
        dirs.push(dir);
    }
    (bookies, dirs, addrs)
}

async fn connect_all(addrs: &[String]) -> Vec<Arc<BookieConnection>> {
    let mut conns = Vec::new();
    for addr in addrs {
        let socket: std::net::SocketAddr = addr.parse().unwrap();
        conns.push(Arc::new(BookieConnection::connect(socket).await.unwrap()));
    }
    conns
}

#[tokio::test]
async fn recovery_on_3_bookie_ensemble_returns_correct_lac() {
    let (bookies, _dirs, addrs) = spawn_bookies(3).await;

    // Write 10 entries via the standard client (E=W=A=3)
    let mut client = BookKeeperClient::new(addrs.clone());
    let mut wh = client.create_ledger(1, 3, 3, 3, DigestType::Crc32c, b"").await.unwrap();
    for i in 0..10 {
        wh.append(client.connections(), format!("entry-{i}").as_bytes()).await.unwrap();
    }
    assert_eq!(wh.last_add_confirmed(), 9);

    // Now run recovery (simulating writer crash after entry 9)
    let schedule = RoundRobinDistributionSchedule::new(3, 3, 3);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");

    let conns = connect_all(&addrs).await;
    let mut recovery = RecoveryHandle::new(1, addrs.clone(), schedule, dm, master_key);

    let recovery_lac = recovery.fence(&conns, 0).await.unwrap();
    assert_eq!(recovery_lac, 9, "recovery LAC should match the last committed entry");

    for b in bookies { b.shutdown().await; }
}

#[tokio::test]
async fn recovery_after_writer_crash_finds_committed_entries() {
    let (bookies, _dirs, addrs) = spawn_bookies(3).await;

    // Original writer writes 5 entries with E=W=A=3
    {
        let mut client = BookKeeperClient::new(addrs.clone());
        let mut wh = client.create_ledger(2, 3, 3, 3, DigestType::Crc32c, b"").await.unwrap();
        for i in 0..5 {
            wh.append(client.connections(), format!("data-{i}").as_bytes()).await.unwrap();
        }
        // "Crash" — drop the writer without closing
    }

    // Recovery: fence the ledger and run forward read
    let schedule = RoundRobinDistributionSchedule::new(3, 3, 3);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");

    let conns = connect_all(&addrs).await;
    let mut recovery = RecoveryHandle::new(2, addrs.clone(), schedule, dm, master_key);

    let (last_entry, total_length) = recovery.recover(&conns, 0).await.unwrap();
    assert_eq!(last_entry, 4, "should find all 5 committed entries (0..=4)");
    assert!(total_length > 0, "total length should be tracked");

    for b in bookies { b.shutdown().await; }
}

#[tokio::test]
async fn recovery_on_empty_ledger() {
    let (bookies, _dirs, addrs) = spawn_bookies(3).await;

    // Create a ledger but write nothing (simulating writer crash before first add)
    {
        let mut client = BookKeeperClient::new(addrs.clone());
        let _ = client.create_ledger(3, 3, 3, 3, DigestType::Crc32c, b"").await.unwrap();
    }

    let schedule = RoundRobinDistributionSchedule::new(3, 3, 3);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");

    let conns = connect_all(&addrs).await;
    let mut recovery = RecoveryHandle::new(3, addrs.clone(), schedule, dm, master_key);

    // Fencing an empty ledger should return -1 (no committed entries)
    let recovery_lac = recovery.fence(&conns, 0).await.unwrap();
    assert_eq!(recovery_lac, -1, "empty ledger should have LAC of -1");

    for b in bookies { b.shutdown().await; }
}

#[tokio::test]
async fn fence_quorum_failure_when_too_many_bookies_down() {
    // Verify recovery correctly fails when too few bookies remain to
    // rule out a silent committed entry. With E=3, W=3, A=2 (the
    // typical config), an ack quorum is any 2 bookies; if 2 are dead
    // and 1 is alive, the dead pair could have together accepted an
    // entry the surviving bookie never saw — fence must refuse.
    //
    // (Earlier versions of this test used W=3, A=3 and asserted the
    // same thing, but with A=W there is NO ack-quorum-sized subset
    // smaller than the full ensemble — observing any single bookie
    // is enough to know what was committed, so Java's
    // RRQuorumCoverageSet.checkCovered correctly accepts that
    // setup. The Rust port now matches that semantics.)
    let (mut bookies, _dirs, addrs) = spawn_bookies(3).await;

    {
        let mut client = BookKeeperClient::new(addrs.clone());
        let mut wh = client.create_ledger(4, 3, 3, 2, DigestType::Crc32c, b"").await.unwrap();
        for i in 0..6 {
            wh.append(client.connections(), format!("e{i}").as_bytes()).await.unwrap();
        }
    }

    // Kill 2 of 3 bookies — coverage cannot be reached.
    let dead1 = bookies.pop().unwrap();
    let dead2 = bookies.pop().unwrap();
    dead1.shutdown().await;
    dead2.shutdown().await;

    let schedule = RoundRobinDistributionSchedule::new(3, 3, 2);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");

    // Only one bookie remains — connect to whichever survives
    let mut conns: Vec<Arc<BookieConnection>> = Vec::new();
    for addr in &addrs {
        let socket: std::net::SocketAddr = match addr.parse() {
            Ok(s) => s,
            Err(_) => continue,
        };
        if let Ok(conn) = BookieConnection::connect(socket).await {
            conns.push(Arc::new(conn));
        }
    }

    if conns.is_empty() {
        // All bookies dead — nothing to test
        for b in bookies { b.shutdown().await; }
        return;
    }

    let mut recovery = RecoveryHandle::new(4, addrs.clone(), schedule, dm, master_key);
    // With only 1/3 bookies alive and A=2, the two dead bookies
    // together form an ack quorum that could have committed an entry
    // we cannot observe — fence MUST refuse.
    let result = recovery.fence(&conns, 0).await;
    assert!(result.is_err(), "fence must refuse when an ack-quorum-sized window is unobservable");

    for b in bookies { b.shutdown().await; }
}

// The striped-ensemble scenario (E=5, W=3, A=2 with bookies {2, 4}
// silent — the round-robin window starting at index 2 leaves an
// ack-quorum-sized subset unobservable) is asserted at the unit
// level in `tests/distribution_test.rs`, where
// `QuorumCoverageSet::is_covered` returns `false` for that shape
// directly. Reproducing the same condition end-to-end requires a
// way to leave a specific ensemble index "silent" — but the eager
// `BookieConnection::connect` rejects a dead address up-front, and
// a half-open TCP listener would block the test on the
// 30-second per-request timeout. The unit-level proof is sufficient.

#[tokio::test]
async fn read_after_recovery_returns_all_entries() {
    let (bookies, _dirs, addrs) = spawn_bookies(3).await;

    // Write entries
    {
        let mut client = BookKeeperClient::new(addrs.clone());
        let mut wh = client.create_ledger(5, 3, 3, 3, DigestType::Crc32c, b"").await.unwrap();
        for i in 0..8 {
            wh.append(client.connections(), format!("payload-{i}").as_bytes()).await.unwrap();
        }
    }

    // Recover
    let schedule = RoundRobinDistributionSchedule::new(3, 3, 3);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");
    let conns = connect_all(&addrs).await;

    let mut recovery = RecoveryHandle::new(5, addrs.clone(), schedule.clone(), dm.clone(), master_key);
    let (last_entry, _) = recovery.recover(&conns, 0).await.unwrap();
    assert_eq!(last_entry, 7);

    // Read all entries back via standard ReadHandle
    use bookkeeper_client::read_handle::ReadHandle;
    let rh = ReadHandle::new(5, addrs, schedule, dm, master_key);
    for i in 0..8 {
        let payload = rh.read_entry(&conns, i).await.unwrap();
        assert_eq!(payload, format!("payload-{i}").as_bytes());
    }

    for b in bookies { b.shutdown().await; }
}

/// Recovery's `forward_read` must not declare an entry "missing" until it has
/// received `(E - AQ + 1)` *definitive* NoEntry responses (Java's invariant).
/// Otherwise, errors from unreachable bookies are conflated with absence,
/// silently truncating the ledger and losing committed entries.
///
/// Repro: E=3, W=2, A=2 — round-robin places write_set(0)=[0,1].
/// Both bookies 0 and 1 are killed; bookie 2 is alive. Reading entry 0 yields
/// 2 errors (no NoEntry responses at all). Pre-fix the loop sees `found=false`
/// and declares entry 0 missing, returning `last_entry = -1` — silent loss
/// of all 5 committed entries. Threshold for declaring missing is
/// `E - AQ + 1 = 2`, but we have 0 NoEntry replies, so the ambiguity must be
/// surfaced as an error rather than swallowed.
#[tokio::test]
async fn forward_read_does_not_truncate_when_quorum_unreachable() {
    let (mut bookies, _dirs, addrs) = spawn_bookies(3).await;

    // Write 5 entries with E=3, W=2, A=2 — round-robin replicates
    // each entry to a 2-bookie write set.
    {
        let mut client = BookKeeperClient::new(addrs.clone());
        let mut wh = client.create_ledger(7, 3, 2, 2, DigestType::Crc32c, b"").await.unwrap();
        for i in 0..5 {
            wh.append(client.connections(), format!("e{i}").as_bytes()).await.unwrap();
        }
    }

    // Connect to all 3 bookies BEFORE killing them so the conns array
    // preserves ensemble-position indexing (conns[i] = bookie i).
    let conns = connect_all(&addrs).await;

    // Kill bookies 0 and 1. write_set(0) = [0, 1], so entry 0's only
    // copies are now unreachable.
    let dead0 = bookies.remove(0);
    let dead1 = bookies.remove(0); // was bookies[1]
    dead0.shutdown().await;
    dead1.shutdown().await;

    let schedule = RoundRobinDistributionSchedule::new(3, 2, 2);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");
    let mut recovery = RecoveryHandle::new(7, addrs.clone(), schedule, dm, master_key);

    let result = recovery.forward_read(&conns, -1).await;

    // Post-fix: forward_read MUST NOT silently return Ok(-1). With 0 NoEntry
    // responses against a threshold of 2, the recovery cannot determine
    // whether entry 0 exists — so it must surface an error rather than
    // declare the ledger empty.
    match result {
        Ok(last) => panic!(
            "forward_read returned Ok({last}) despite ambiguous quorum: \
             entry 0's write_set [0,1] is fully unreachable, no bookie \
             returned NoEntry, yet recovery declared the entry missing — \
             this silently truncates the ledger and loses committed entries",
        ),
        Err(_) => { /* expected: ambiguous → error */ }
    }

    for b in bookies { b.shutdown().await; }
}

/// Re-replication during recovery must not silently skip entries it cannot
/// re-read. forward_read already established that an entry exists somewhere
/// in the ensemble, but if the bookies that hold it become unreachable
/// before re-replication, we cannot fabricate the missing data — the only
/// safe option is to surface the failure so the caller does not close the
/// ledger with a hole punched in it.
///
/// Repro: with E=3, W=2, A=2, write 5 entries, fence and forward_read with
/// all bookies alive (so last_entry=4 is established), then kill bookies 0
/// and 1. Entries whose write_set is [0,1] (entry 0 and entry 3 in our
/// 5-entry set) are now unreadable. Pre-fix `replicate_range` skipped
/// silently and returned Ok(()), allowing `recover()` to close the ledger
/// at last_entry=4 with entries 0 and 3 missing from re-replication.
#[tokio::test]
async fn recover_errors_when_re_replication_cannot_read_entry() {
    let (mut bookies, _dirs, addrs) = spawn_bookies(3).await;

    {
        let mut client = BookKeeperClient::new(addrs.clone());
        let mut wh = client.create_ledger(8, 3, 2, 2, DigestType::Crc32c, b"").await.unwrap();
        for i in 0..5 {
            wh.append(client.connections(), format!("e{i}").as_bytes()).await.unwrap();
        }
    }

    let conns = connect_all(&addrs).await;

    let schedule = RoundRobinDistributionSchedule::new(3, 2, 2);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");
    let mut recovery = RecoveryHandle::new(8, addrs.clone(), schedule, dm, master_key);

    // Phase 1 + 2 with all bookies alive — establishes last_entry = 4.
    recovery.fence(&conns, 0).await.unwrap();
    let last_entry = recovery.forward_read(&conns, recovery.recovery_lac()).await.unwrap();
    assert_eq!(last_entry, 4);

    // Now kill bookies 0 and 1. write_set(0) = [0, 1] becomes unreachable.
    let dead0 = bookies.remove(0);
    let dead1 = bookies.remove(0);
    dead0.shutdown().await;
    dead1.shutdown().await;

    // Phase 3 (re-replication) must surface the unreadable entry as an error
    // rather than silently skipping it.
    let result = recovery.replicate_range(&conns, 0, last_entry).await;
    match result {
        Ok(()) => panic!(
            "replicate_range returned Ok(()) even though entries 0 and 3 \
             could not be re-read from any bookie — recover() would close the \
             ledger with re-replication holes",
        ),
        Err(_) => { /* expected */ }
    }

    for b in bookies { b.shutdown().await; }
}
