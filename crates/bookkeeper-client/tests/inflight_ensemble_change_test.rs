//! Regression test: an in-flight append whose write set includes a
//! hung bookie must complete via the ensemble-change replacement,
//! not block until the per-write quorum timeout fires.
//!
//! When the writer detects that a bookie in an entry's write set is
//! unreachable, it triggers an ensemble change: the failed bookie is
//! replaced by another from the available pool, and every in-flight
//! entry whose write set included the failed index is re-sent to the
//! replacement. For the original `PendingAdd` to actually complete
//! via the replacement's response, the resend must contribute to
//! the SAME ack quorum the original initial-write tasks are
//! tracking. Pre-fix, the resend was `tokio::spawn`'d and its result
//! discarded — the original ack-collector kept polling the dead
//! connection's response future until the 30-second
//! `add_entry_quorum_timeout` fired.
//!
//! Apache BookKeeper's `PendingAddOp.unsetSuccessAndSendWriteRequest`
//! re-runs `sendWriteRequest(ensemble, bookieIndex)` against the
//! same `PendingAddOp` instance, so the new bookie's reply lands in
//! the same `ackSet` and `sendAddSuccessCallbacks` fires as soon as
//! quorum is reached.

use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_client::write_handle::WriteHandle;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_server::server::BookieServer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};

fn test_config(dir: &TempDir) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config
}

/// Bind a TCP listener that accepts every connection and holds it
/// open without ever responding. Simulates a bookie process that's
/// reachable at the TCP layer but stuck in a non-responsive state
/// (e.g., GC pause, fenced-but-not-closed FD, gateway with backed-up
/// queue). Returns the bound address; the listener task is detached
/// for the lifetime of the test process.
async fn spawn_blackhole_listener() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        // Hold accepted streams forever so the kernel doesn't reset
        // the connection, and the client side sees nothing on the
        // socket — the failure mode we want to provoke.
        let mut held: Vec<TcpStream> = Vec::new();
        loop {
            match listener.accept().await {
                Ok((stream, _)) => held.push(stream),
                Err(_) => return,
            }
        }
    });
    addr
}

#[tokio::test]
async fn inflight_pending_completes_via_resend_after_ensemble_change_swaps_a_hung_bookie() {
    // Two real bookies and a blackhole. The ensemble starts as
    // [B0, B1, blackhole]; B2 is the replacement waiting in the
    // available pool.
    let dir0 = TempDir::new().unwrap();
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let server0 = BookieServer::start(test_config(&dir0)).await.unwrap();
    let server1 = BookieServer::start(test_config(&dir1)).await.unwrap();
    let server2 = BookieServer::start(test_config(&dir2)).await.unwrap();
    let b0 = server0.local_addr();
    let b1 = server1.local_addr();
    let b2 = server2.local_addr();
    let blackhole = spawn_blackhole_listener().await;

    // E=3, W=3, A=3 — every bookie's ack is required, so the
    // blackhole's slot MUST be replaced before quorum can be reached.
    let schedule = RoundRobinDistributionSchedule::new(3, 3, 3);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let mut wh = WriteHandle::with_available_bookies(
        99,
        vec![b0.to_string(), b1.to_string(), blackhole.to_string()],
        vec![
            b0.to_string(),
            b1.to_string(),
            blackhole.to_string(),
            b2.to_string(),
        ],
        schedule,
        dm,
        master_key,
    );

    let mut conns: Vec<Arc<BookieConnection>> = vec![
        Arc::new(BookieConnection::connect(b0).await.unwrap()),
        Arc::new(BookieConnection::connect(b1).await.unwrap()),
        Arc::new(BookieConnection::connect(blackhole).await.unwrap()),
    ];

    // Submit the append non-blocking. The spawned per-bookie tasks
    // run concurrently: B0 and B1 ack quickly; the blackhole's task
    // hangs forever (no protocol response, the kernel TCP buffer
    // absorbs the request bytes). With A=3 the in-flight `PendingAdd`
    // cannot complete on the original bookies alone; the only path
    // to quorum is via the replacement bookie's ack of the resend.
    let pending = wh.append_async(&conns, b"in-flight-resend").unwrap();

    // Give the initial sends a moment to dispatch and for the
    // surviving bookies' acks to land on the shared ctx. 250 ms is
    // more than ample on localhost.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Manually swap the blackhole slot for B2 and trigger the
    // in-flight resend. Pre-fix the resend was a fire-and-forget
    // `tokio::spawn` whose result was discarded; the original
    // ack-collector kept polling the blackhole's response future
    // until the 30s per-write quorum timeout fired. With the ack
    // tracking now anchored on a shared per-entry context, the resend
    // task records its result into the SAME ack set the original
    // tasks are waiting on, so the new bookie's reply pushes ack
    // count to quorum and the user's `PendingAdd` resolves.
    wh.handle_bookie_failure(&mut conns, 2)
        .await
        .expect("ensemble change must succeed (B2 is in the available pool)");
    assert_eq!(wh.ensemble()[2], b2.to_string());
    wh.resend_in_flight_for_test(&conns, &[2]).await;

    // The append must complete well under the 30s per-write quorum
    // timeout that would otherwise fire in the buggy path. Five
    // seconds is overkill for the success path on localhost while
    // still leaving a clear failure signal if the resend doesn't
    // actually feed the original quorum.
    let entry_id = tokio::time::timeout(Duration::from_secs(5), pending.wait())
        .await
        .expect(
            "in-flight PendingAdd must complete via the resend long \
             before the per-write quorum timeout fires; pre-fix the \
             resend was fire-and-forget and the original ack collector \
             timed out at 30s",
        )
        .expect("PendingAdd resolves with success once B2 acks the resend");
    assert_eq!(entry_id, 0);

    server0.shutdown().await;
    server1.shutdown().await;
    server2.shutdown().await;
}
