//! End-to-end tests for `ZkBookKeeper` against a real ZooKeeper.
//!
//! These exercise the full ZK-integrated client lifecycle:
//!   - create / write / close / open / read / delete
//!   - multiple concurrent ledgers
//!   - open-with-recovery on an unclosed ledger
//!   - password mismatch rejection
//!
//! Shares ZK + bookie-spawning fixtures via `bookkeeper_test_utils`. Each
//! test uses a distinct ZK ledger root path so they can run in parallel.
//!
//! Skipped gracefully if Docker is unavailable.

use bookkeeper_client::zk_client::ZkBookKeeper;
use bookkeeper_protocol::digest::DigestType;
use bookkeeper_test_utils::{bootstrap_cluster, shared_zk, spawn_bookie};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Full lifecycle: create → write → close → open → read → delete.
#[tokio::test]
async fn zk_create_write_close_open_read_delete() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test-lifecycle";
    bootstrap_cluster(&connect, root).await;
    let (server, _dir) = spawn_bookie(&connect, root).await;

    let mut bk = ZkBookKeeper::new(&connect, root).await.unwrap();

    let password = b"test-password";
    let (mut wh, conns) = bk
        .create_ledger(1, 1, 1, DigestType::Crc32c, password)
        .await
        .unwrap();
    let ledger_id = wh.ledger_id();
    assert!(ledger_id >= 0);

    let num_entries = 10;
    for i in 0..num_entries {
        wh.append(&conns, format!("entry-{i}").as_bytes())
            .await
            .unwrap();
    }
    assert_eq!(wh.last_add_confirmed(), num_entries - 1);

    bk.close_ledger(&wh).await.unwrap();

    let (rh, read_conns) = bk
        .open_ledger(ledger_id, DigestType::Crc32c, password)
        .await
        .unwrap();
    assert_eq!(rh.ledger_id(), ledger_id);
    for i in 0..num_entries {
        let data = rh.read_entry(&read_conns, i).await.unwrap();
        assert_eq!(
            data,
            format!("entry-{i}").as_bytes(),
            "mismatch at entry {i}"
        );
    }

    bk.delete_ledger(ledger_id).await.unwrap();
    let result = bk.open_ledger(ledger_id, DigestType::Crc32c, password).await;
    assert!(result.is_err(), "expected open to fail after deletion");

    server.shutdown().await;
}

/// Multiple ledgers on the same bookie are independent.
#[tokio::test]
async fn zk_multiple_ledgers() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test-multi";
    bootstrap_cluster(&connect, root).await;
    let (server, _dir) = spawn_bookie(&connect, root).await;

    let mut bk = ZkBookKeeper::new(&connect, root).await.unwrap();
    let password = b"multi-test";
    let mut ledger_ids = Vec::new();

    for l in 0..3 {
        let (mut wh, conns) = bk
            .create_ledger(1, 1, 1, DigestType::Crc32c, password)
            .await
            .unwrap();
        ledger_ids.push(wh.ledger_id());
        for i in 0..5 {
            wh.append(&conns, format!("ledger-{l}-entry-{i}").as_bytes())
                .await
                .unwrap();
        }
        bk.close_ledger(&wh).await.unwrap();
    }

    for (l, lid) in ledger_ids.iter().enumerate() {
        let (rh, conns) = bk
            .open_ledger(*lid, DigestType::Crc32c, password)
            .await
            .unwrap();
        for i in 0..5 {
            let data = rh.read_entry(&conns, i).await.unwrap();
            assert_eq!(data, format!("ledger-{l}-entry-{i}").as_bytes());
        }
    }

    for lid in &ledger_ids {
        bk.delete_ledger(*lid).await.unwrap();
    }

    server.shutdown().await;
}

/// Open with recovery on an unclosed ledger seals it and exposes all entries.
#[tokio::test]
async fn zk_open_with_recovery() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test-recovery";
    bootstrap_cluster(&connect, root).await;
    let (server, _dir) = spawn_bookie(&connect, root).await;

    let mut bk = ZkBookKeeper::new(&connect, root).await.unwrap();
    let password = b"recovery-test";

    // Write entries without closing (simulate writer crash)
    let (mut wh, conns) = bk
        .create_ledger(1, 1, 1, DigestType::Crc32c, password)
        .await
        .unwrap();
    let ledger_id = wh.ledger_id();
    for i in 0..5 {
        wh.append(&conns, format!("data-{i}").as_bytes())
            .await
            .unwrap();
    }
    assert_eq!(wh.last_add_confirmed(), 4);
    drop(wh);
    drop(conns);

    // Open with recovery — fences the ledger and exposes the committed entries
    let (rh, recovery_conns) = bk
        .open_ledger_with_recovery(ledger_id, DigestType::Crc32c, password)
        .await
        .unwrap();
    for i in 0..5 {
        let data = rh.read_entry(&recovery_conns, i).await.unwrap();
        assert_eq!(data, format!("data-{i}").as_bytes());
    }

    // After recovery the ledger metadata should be CLOSED — a plain open works
    let (rh2, conns2) = bk
        .open_ledger(ledger_id, DigestType::Crc32c, password)
        .await
        .unwrap();
    assert_eq!(rh2.read_entry(&conns2, 0).await.unwrap(), b"data-0");

    bk.delete_ledger(ledger_id).await.unwrap();
    server.shutdown().await;
}

/// Opening a ledger with the wrong password is rejected.
#[tokio::test]
async fn zk_password_mismatch() {
    let Some(connect) = shared_zk().await else { return; };
    let root = "/test-password";
    bootstrap_cluster(&connect, root).await;
    let (server, _dir) = spawn_bookie(&connect, root).await;

    let mut bk = ZkBookKeeper::new(&connect, root).await.unwrap();
    let (mut wh, conns) = bk
        .create_ledger(1, 1, 1, DigestType::Crc32c, b"correct-password")
        .await
        .unwrap();
    let ledger_id = wh.ledger_id();
    wh.append(&conns, b"secret-data").await.unwrap();
    bk.close_ledger(&wh).await.unwrap();

    let result = bk
        .open_ledger(ledger_id, DigestType::Crc32c, b"wrong-password")
        .await;
    assert!(result.is_err(), "expected password mismatch error");

    bk.delete_ledger(ledger_id).await.unwrap();
    server.shutdown().await;
}
