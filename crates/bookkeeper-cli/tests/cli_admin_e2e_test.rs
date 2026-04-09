//! End-to-end tests for `bk` admin commands against a real ZooKeeper
//! and in-process Rust bookies.
//!
//! These exercise the actual CLI binary (not direct function calls), which
//! catches bugs in argument parsing, command wiring, and output formatting
//! that the existing `cli_test.rs --help` smoke tests miss.
//!
//! Skipped gracefully if Docker is unavailable.

use assert_cmd::Command;
use bookkeeper_test_utils::{create_ledger_and_close, shared_zk, spawn_bookies};
use predicates::prelude::*;

/// Build a `bk` Command with logging suppressed. Without this the CLI's
/// tracing-subscriber writes INFO lines to stdout, contaminating any output
/// the test wants to assert on.
fn bk() -> Command {
    let mut cmd = Command::cargo_bin("bk").unwrap();
    cmd.env("RUST_LOG", "off");
    cmd
}

/// Local wrapper preserving this file's `create_test_ledger(zk, root, n)`
/// signature — all callers in this file use ensemble=3 and the `clitest`
/// password. Tests that need a different shape can call
/// `create_ledger_and_close` directly.
async fn create_test_ledger(zk_connect: &str, root: &str, n_entries: i64) -> i64 {
    create_ledger_and_close(zk_connect, root, 3, 3, 3, b"clitest", n_entries).await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn cli_initnewcluster_creates_layout_and_instance_id() {
    let Some(zk_connect) = shared_zk().await else { return; };

    bk().args([
        "initnewcluster",
        "--zk",
        &zk_connect,
        "--ledger-root",
        "/test_init",
    ])
    .assert()
    .success();

    // Verify by reading the layout via the metadata API.
    let zk_client = zookeeper_client::Client::connect(&zk_connect).await.unwrap();
    let paths = bookkeeper_metadata::zk_path::ZkPaths::new("/test_init");
    let layout_mgr = bookkeeper_metadata::zk::layout::ZkLayoutManager::new(zk_client, paths);
    use bookkeeper_metadata::traits::LayoutManager;
    let layout = layout_mgr.read_ledger_layout().await.unwrap();
    assert!(
        layout.is_some(),
        "initnewcluster should create the layout znode"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_listbookies_shows_registered_bookies() {
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/test_listbookies";

    bk().args(["initnewcluster", "--zk", &zk_connect, "--ledger-root", root])
        .assert()
        .success();
    let (bookies, _dirs, addrs) = spawn_bookies(2, &zk_connect, root).await;

    let assert = bk()
        .args([
            "listbookies",
            "--zk",
            &zk_connect,
            "--ledger-root",
            root,
            "--rw",
        ])
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    for addr in &addrs {
        assert!(
            stdout.contains(addr),
            "listbookies output should contain {addr}; got:\n{stdout}"
        );
    }

    for b in bookies {
        b.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_listledgers_and_ledgerinfo() {
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/test_listledgers";

    bk().args(["initnewcluster", "--zk", &zk_connect, "--ledger-root", root])
        .assert()
        .success();
    let (bookies, _dirs, _) = spawn_bookies(3, &zk_connect, root).await;
    let lid = create_test_ledger(&zk_connect, root, 5).await;

    // listledgers should include our ledger ID
    let assert = bk()
        .args(["listledgers", "--zk", &zk_connect, "--ledger-root", root])
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(
        out.contains(&lid.to_string()),
        "listledgers should print ledger {lid}; got:\n{out}"
    );

    // ledgerinfo should print fields for the specific ledger
    let assert = bk()
        .args([
            "ledgerinfo",
            "--zk",
            &zk_connect,
            "--ledger-root",
            root,
            "--ledger-id",
            &lid.to_string(),
        ])
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(out.contains(&format!("Ledger {lid}")));
    assert!(out.contains("State:"));
    assert!(out.contains("Closed"), "ledger should be closed; got:\n{out}");
    assert!(out.contains("Ensemble:    3"));
    assert!(out.contains("WriteQuorum: 3"));
    assert!(out.contains("AckQuorum:   3"));

    for b in bookies {
        b.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_clusterinfo_and_whatisinstanceid() {
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/test_clusterinfo";

    bk().args(["initnewcluster", "--zk", &zk_connect, "--ledger-root", root])
        .assert()
        .success();
    let (bookies, _dirs, _) = spawn_bookies(2, &zk_connect, root).await;

    let assert = bk()
        .args(["clusterinfo", "--zk", &zk_connect, "--ledger-root", root])
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(out.contains("Writable bookies: 2"));
    assert!(out.contains("Auto-recovery: ENABLED"));
    assert!(out.contains("Instance ID:"));

    // whatisinstanceid should print a UUID
    let assert = bk()
        .args([
            "whatisinstanceid",
            "--zk",
            &zk_connect,
            "--ledger-root",
            root,
        ])
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    // UUIDs are 36 chars with hyphens — just check we got some hex+hyphen output
    assert!(out.contains('-'), "instance ID should be a UUID; got:\n{out}");

    for b in bookies {
        b.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_autorecovery_disable_and_enable_round_trip() {
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/test_autorecovery";

    bk().args(["initnewcluster", "--zk", &zk_connect, "--ledger-root", root])
        .assert()
        .success();
    // Spawn a single bookie just so the underreplication subtree gets created
    // when admin commands touch it.
    let (bookies, _dirs, _) = spawn_bookies(1, &zk_connect, root).await;

    // Disable
    bk().args([
        "autorecovery",
        "--zk",
        &zk_connect,
        "--ledger-root",
        root,
        "--disable",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("DISABLED"));

    // clusterinfo should reflect that
    let assert = bk()
        .args(["clusterinfo", "--zk", &zk_connect, "--ledger-root", root])
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(
        out.contains("Auto-recovery: DISABLED"),
        "clusterinfo should reflect disabled state; got:\n{out}"
    );

    // Re-enable
    bk().args([
        "autorecovery",
        "--zk",
        &zk_connect,
        "--ledger-root",
        root,
        "--enable",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("ENABLED"));

    // Mutual exclusion: --enable + --disable together should fail
    bk().args([
        "autorecovery",
        "--zk",
        &zk_connect,
        "--ledger-root",
        root,
        "--enable",
        "--disable",
    ])
    .assert()
    .failure();

    for b in bookies {
        b.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_listunderreplicated_count_and_filter() {
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/test_listur";

    bk().args(["initnewcluster", "--zk", &zk_connect, "--ledger-root", root])
        .assert()
        .success();
    let (bookies, _dirs, _) = spawn_bookies(3, &zk_connect, root).await;
    let lid = create_test_ledger(&zk_connect, root, 3).await;

    // Initially: no under-replicated ledgers
    bk().args([
        "listunderreplicated",
        "--zk",
        &zk_connect,
        "--ledger-root",
        root,
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("No under-replicated"));

    // Mark the ledger UR via the underreplication API directly
    let zk_client = zookeeper_client::Client::connect(&zk_connect).await.unwrap();
    let paths = bookkeeper_metadata::zk_path::ZkPaths::new(root);
    let ur_mgr = bookkeeper_replication::underreplication::ZkUnderreplicationManager::new(
        zk_client,
        paths,
        "test:3181".to_string(),
    )
    .await
    .unwrap();
    ur_mgr
        .mark_ledger_underreplicated(lid, &["dead-bookie:3181".to_string()])
        .await
        .unwrap();

    // listunderreplicated should now find it
    let assert = bk()
        .args([
            "listunderreplicated",
            "--zk",
            &zk_connect,
            "--ledger-root",
            root,
            "--printmissingreplica",
        ])
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(out.contains(&lid.to_string()));
    assert!(out.contains("dead-bookie:3181"));

    // Count-only mode
    let assert = bk()
        .args([
            "listunderreplicated",
            "--zk",
            &zk_connect,
            "--ledger-root",
            root,
            "--onlydisplayledgercount",
        ])
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone())
        .unwrap()
        .trim()
        .to_string();
    assert_eq!(out, "1", "count-only mode should print just '1'; got: {out:?}");

    for b in bookies {
        b.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_deleteledger_requires_force_and_then_succeeds() {
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/test_delete";

    bk().args(["initnewcluster", "--zk", &zk_connect, "--ledger-root", root])
        .assert()
        .success();
    let (bookies, _dirs, _) = spawn_bookies(3, &zk_connect, root).await;
    let lid = create_test_ledger(&zk_connect, root, 2).await;

    // Without --force: should print warning, NOT delete
    let assert = bk()
        .args([
            "deleteledger",
            "--zk",
            &zk_connect,
            "--ledger-root",
            root,
            "--ledger-id",
            &lid.to_string(),
        ])
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(
        out.contains("--force"),
        "should warn about --force; got:\n{out}"
    );

    // Verify ledger metadata still exists
    bk().args([
        "ledgerinfo",
        "--zk",
        &zk_connect,
        "--ledger-root",
        root,
        "--ledger-id",
        &lid.to_string(),
    ])
    .assert()
    .success();

    // With --force: should delete
    bk().args([
        "deleteledger",
        "--zk",
        &zk_connect,
        "--ledger-root",
        root,
        "--ledger-id",
        &lid.to_string(),
        "--force",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("deleted successfully"));

    // Now ledgerinfo should fail (or report missing)
    bk().args([
        "ledgerinfo",
        "--zk",
        &zk_connect,
        "--ledger-root",
        root,
        "--ledger-id",
        &lid.to_string(),
    ])
    .assert()
    .failure();

    for b in bookies {
        b.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_cookie_get_and_delete() {
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/test_cookie";

    bk().args(["initnewcluster", "--zk", &zk_connect, "--ledger-root", root])
        .assert()
        .success();
    let (bookies, _dirs, addrs) = spawn_bookies(1, &zk_connect, root).await;
    let bookie_id = addrs[0].clone();

    // cookie-get should succeed and print a cookie blob
    let assert = bk()
        .args([
            "cookie-get",
            "--zk",
            &zk_connect,
            "--ledger-root",
            root,
            "--bookie-id",
            &bookie_id,
        ])
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(
        out.contains(&bookie_id) || out.contains("Cookie"),
        "cookie-get should print cookie info; got:\n{out}"
    );

    // Shut down the bookie BEFORE deleting its cookie — otherwise the running
    // bookie holds an ephemeral registration but the cookie is the persistent
    // identity record.
    for b in bookies {
        b.shutdown().await;
    }

    // cookie-delete without --force prints a warning
    let assert = bk()
        .args([
            "cookie-delete",
            "--zk",
            &zk_connect,
            "--ledger-root",
            root,
            "--bookie-id",
            &bookie_id,
        ])
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(
        out.contains("--force"),
        "cookie-delete without --force should warn; got:\n{out}"
    );

    // cookie-delete --force should succeed
    bk().args([
        "cookie-delete",
        "--zk",
        &zk_connect,
        "--ledger-root",
        root,
        "--bookie-id",
        &bookie_id,
        "--force",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("deleted successfully"));

    // After deletion, cookie-get should report missing
    let assert = bk()
        .args([
            "cookie-get",
            "--zk",
            &zk_connect,
            "--ledger-root",
            root,
            "--bookie-id",
            &bookie_id,
        ])
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(
        out.contains("No cookie"),
        "cookie should be gone after delete; got:\n{out}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_lostbookierecoverydelay_get_and_set() {
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/test_lostdelay";

    bk().args(["initnewcluster", "--zk", &zk_connect, "--ledger-root", root])
        .assert()
        .success();
    // Need at least one bookie so the underreplication subtree gets created
    // (this command lives under that subtree).
    let (bookies, _dirs, _) = spawn_bookies(1, &zk_connect, root).await;

    // Set to 42
    bk().args([
        "lostbookierecoverydelay",
        "--zk",
        &zk_connect,
        "--ledger-root",
        root,
        "--set",
        "42",
    ])
    .assert()
    .success();

    // Read back — should print "42" somewhere in the output
    let assert = bk()
        .args([
            "lostbookierecoverydelay",
            "--zk",
            &zk_connect,
            "--ledger-root",
            root,
        ])
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(
        out.contains("42"),
        "lostbookierecoverydelay should report 42; got:\n{out}"
    );

    for b in bookies {
        b.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_readledger_reads_committed_entries() {
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/test_readledger";

    bk().args(["initnewcluster", "--zk", &zk_connect, "--ledger-root", root])
        .assert()
        .success();
    let (bookies, _dirs, _) = spawn_bookies(3, &zk_connect, root).await;
    let lid = create_test_ledger(&zk_connect, root, 4).await;

    let assert = bk()
        .args([
            "readledger",
            "--zk",
            &zk_connect,
            "--ledger-root",
            root,
            "--ledger-id",
            &lid.to_string(),
        ])
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    // Should print all 4 entries with their payloads
    for i in 0..4 {
        assert!(
            out.contains(&format!("Entry {i}")),
            "readledger output should mention entry {i}; got:\n{out}"
        );
    }
    // The CLI prints payloads as a Rust byte-array Debug format. "entry-0" is
    // [101, 110, 116, 114, 121, 45, 48]; check the first entry's bytes appear.
    assert!(
        out.contains("101, 110, 116, 114, 121, 45, 48"),
        "readledger should show payload bytes for 'entry-0'; got:\n{out}"
    );

    for b in bookies {
        b.shutdown().await;
    }
}
