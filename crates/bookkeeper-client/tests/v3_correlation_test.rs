//! Regression test for the V3 response-correlation bug — long-poll
//! interleaved with another V3 op on the same channel.
//!
//! Setup: a bookie processes V3 requests via `FuturesUnordered`, which
//! lets fast requests finish before slow ones even when slow was
//! submitted first. The previous Rust client paired requests with
//! responses by FIFO arrival order in a `VecDeque<oneshot::Sender>`.
//! Sequence:
//!   1. Submit a V3 long-poll for the next entry — parks server-side
//!      on the LAC watcher until LAC advances or the long-poll
//!      timeout fires.
//!   2. Submit a V3 ReadLac on the same channel — returns immediately.
//!   3. The bookie's `FuturesUnordered` completes step 2 first, so
//!      that response hits the wire BEFORE the parked long-poll's
//!      eventual response.
//!
//! Pre-fix, the FIFO matcher routed the ReadLac response to the
//! long-poll's `oneshot::Sender` (the one queued first). The
//! long-poll caller then received a `V2Response::ReadLac` value, which
//! the type match on `V2Response::Read` would reject — so the visible
//! symptom was a Protocol error or a hang, depending on timing. In
//! the worst case (two ops of the same V2 response shape, e.g. two
//! reads), the long-poll silently received the fast read's payload
//! and labelling — Pulsar managed-ledger cursors would advance to the
//! wrong LAC.
//!
//! After the fix (per-channel `txn_id` allocator + content-keyed
//! matcher mirroring Java's `PerChannelBookieClient.completionObjects`),
//! each caller correctly receives ITS response regardless of the
//! arrival order on the wire.
//!
//! All operations in this test are V3 because the bookie commits to
//! one protocol version per connection (V3 once any V3 frame
//! arrives), so a mixed V2/V3 channel would tear down the connection
//! at the server's V3-decoder fall-through. That lock-in is unrelated
//! to the correlation bug — both V2 and V3 paths are subject to the
//! same FIFO mis-route under their respective code paths.

use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::client::BookKeeperClient;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_client::read_handle::ReadHandle;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_protocol::digest::{DigestType, generate_master_key};
use bookkeeper_protocol::v2::response::V2Response;
use bookkeeper_server::server::BookieServer;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn test_config(dir: &TempDir) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config
}

#[tokio::test]
async fn long_poll_does_not_steal_a_concurrent_v3_readlac_response() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // Writer: create ledger and write three entries so LAC=2.
    // The writer uses its own connection pool inside `client`, so
    // V2 add_entry frames don't land on the same connection that the
    // reader uses below.
    let mut client = BookKeeperClient::new(vec![addr.to_string()]);
    let mut wh = client
        .create_ledger(7, 1, 1, 1, DigestType::Crc32c, b"")
        .await
        .unwrap();
    for i in 0..3i64 {
        wh.append(
            client.connections(),
            format!("entry-{i}").as_bytes(),
        )
        .await
        .unwrap();
    }
    assert_eq!(wh.last_add_confirmed(), 2);

    let ledger_id = 7;
    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = bookkeeper_protocol::digest::DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");

    // The reader uses a SEPARATE connection so the long-poll (V3) +
    // ReadLac (V3) traffic shares one connection_task without any V2
    // frames mixed in. Both ops resolve to `pick_channel(7)` —
    // `BookieConnection::connect` defaults to one channel — so they
    // share the same per-channel matcher.
    let conn = Arc::new(BookieConnection::connect(addr).await.unwrap());

    let rh = Arc::new(ReadHandle::new(
        ledger_id,
        ensemble,
        schedule,
        dm,
        master_key,
    ));

    // (1) Submit the long-poll first. It asks for the entry past the
    // current LAC (entry 3) with `previous_lac = 2`. The bookie has
    // no such entry yet, so the request parks server-side until either
    // a new entry is appended or the timeout fires.
    let rh_lp = rh.clone();
    let conns_lp = vec![conn.clone()];
    let long_poll = tokio::spawn(async move {
        rh_lp
            .read_last_confirmed_and_entry(&conns_lp, 2, 5_000)
            .await
    });

    // Give the long-poll time to reach the bookie and park on the
    // LAC watcher. 250ms is overkill on localhost; we err on the side
    // of determinism.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // (2) Issue a V3 ReadLac on the same channel. The bookie processes
    // it concurrently with the parked long-poll via `FuturesUnordered`
    // and writes the response to the wire BEFORE the long-poll's
    // (still-parked) response.
    let lac_resp = tokio::time::timeout(
        Duration::from_secs(3),
        conn.send_read_lac(ledger_id),
    )
    .await
    .expect("V3 ReadLac did not return within 3s — server response was mis-routed away from this caller")
    .expect("V3 ReadLac call failed");

    // (3) Verify the ReadLac caller received a ReadLac response (not
    // some other op's response and not the long-poll's response).
    // Pre-fix, the FIFO matcher routed this response to the long-poll
    // sender; the long-poll's `match Ok(V2Response::Read { .. })` arm
    // wouldn't match a ReadLac variant, so the long-poll either
    // silently dropped the response (no progress) or — for two ops
    // of the same V2 response shape — mis-labelled the bytes as a
    // legitimate read.
    match lac_resp {
        V2Response::ReadLac { ledger_id: rid, .. } => {
            assert_eq!(rid, ledger_id, "ReadLac response carries a different ledger id");
        }
        other => panic!("ReadLac caller received a non-ReadLac response — V3 was mis-routed: {other:?}"),
    }

    // (4) Append a new entry to wake the long-poll, and verify it
    // returns the right entry. With FIFO matching, the long-poll
    // caller would have already consumed the ReadLac response (and
    // bailed out with a type-mismatch error); the actual long-poll
    // payload would arrive at a sender that's already been dropped.
    wh.append(client.connections(), b"entry-3").await.unwrap();

    let lp_result = tokio::time::timeout(Duration::from_secs(5), long_poll)
        .await
        .expect("long-poll did not finish within 5s")
        .expect("long-poll task panicked");
    let (lp_lac, lp_payload) = lp_result.expect("long-poll returned an error");
    assert_eq!(lp_lac, 3, "long-poll returned wrong LAC");
    assert_eq!(
        lp_payload,
        b"entry-3".to_vec(),
        "long-poll returned wrong payload — V3 response was mis-routed by the matcher"
    );

    server.shutdown().await;
}
