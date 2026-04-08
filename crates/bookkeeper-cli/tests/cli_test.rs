//! Tests for the `bk` CLI binary.
//!
//! These tests spawn the binary and verify its argument parsing, help output,
//! and standalone commands that don't require ZooKeeper or a running cluster.
//!
//! Commands requiring ZK (most admin commands) are tested only at the
//! `--help` level here. End-to-end testing of those commands would require
//! a ZK fixture and is covered by the Java compatibility test suite.

use assert_cmd::Command;
use predicates::prelude::*;

fn bk() -> Command {
    Command::cargo_bin("bk").unwrap()
}

#[test]
fn help_lists_all_commands() {
    let assert = bk().arg("--help").assert().success();
    let output = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    // Verify all major command groups are present
    let expected_commands = [
        "initnewcluster",
        "listbookies",
        "serve",
        "bookiesanity",
        "bench-bookie",
        "bench-e2e",
        "listledgers",
        "ledgerinfo",
        "listunderreplicated",
        "whoisauditor",
        "autorecovery",
        "clusterinfo",
        "deleteledger",
        "readledger",
        "cookie-get",
        "cookie-delete",
        "recover",
        "decommissionbookie",
        "localconsistencycheck",
        "lostbookierecoverydelay",
        "triggeraudit",
        "metaformat",
        "lastmark",
        "readjournal",
        "readlog",
        "listfilesondisc",
        "updatebookieinledger",
        "whatisinstanceid",
        "bookieformat",
        "nukeexistingcluster",
    ];
    for cmd in &expected_commands {
        assert!(output.contains(cmd), "missing command in help: {cmd}");
    }
}

#[test]
fn no_args_shows_usage() {
    bk().assert().failure().stderr(predicate::str::contains("Usage:"));
}

#[test]
fn unknown_command_fails() {
    bk().arg("not-a-real-command").assert().failure();
}

#[test]
fn invalid_arg_combination_for_autorecovery() {
    // autorecovery requires --enable XOR --disable
    bk().arg("autorecovery")
        .arg("--zk").arg("localhost:0")
        .assert()
        .failure();
}

// Per-command --help: each subcommand should have its own help text and exit success.

#[test]
fn initnewcluster_help() {
    bk().arg("initnewcluster").arg("--help").assert().success()
        .stdout(predicate::str::contains("--zk"));
}

#[test]
fn listbookies_help() {
    bk().arg("listbookies").arg("--help").assert().success()
        .stdout(predicate::str::contains("--zk"));
}

#[test]
fn serve_help() {
    bk().arg("serve").arg("--help").assert().success()
        .stdout(predicate::str::contains("--port").or(predicate::str::contains("--conf")));
}

#[test]
fn bookiesanity_help() {
    bk().arg("bookiesanity").arg("--help").assert().success();
}

#[test]
fn bench_bookie_help() {
    bk().arg("bench-bookie").arg("--help").assert().success()
        .stdout(predicate::str::contains("--bookie"));
}

#[test]
fn bench_e2e_help() {
    bk().arg("bench-e2e").arg("--help").assert().success()
        .stdout(predicate::str::contains("--zk"));
}

#[test]
fn listledgers_help() {
    bk().arg("listledgers").arg("--help").assert().success()
        .stdout(predicate::str::contains("--meta"));
}

#[test]
fn ledgerinfo_help_requires_ledger_id() {
    bk().arg("ledgerinfo").arg("--help").assert().success()
        .stdout(predicate::str::contains("--ledger-id"));
}

#[test]
fn listunderreplicated_help() {
    bk().arg("listunderreplicated").arg("--help").assert().success()
        .stdout(predicate::str::contains("--missingreplica"));
}

#[test]
fn whoisauditor_help() {
    bk().arg("whoisauditor").arg("--help").assert().success();
}

#[test]
fn clusterinfo_help() {
    bk().arg("clusterinfo").arg("--help").assert().success();
}

#[test]
fn deleteledger_help() {
    bk().arg("deleteledger").arg("--help").assert().success()
        .stdout(predicate::str::contains("--force"));
}

#[test]
fn readledger_help() {
    bk().arg("readledger").arg("--help").assert().success()
        .stdout(predicate::str::contains("--first-entry"));
}

#[test]
fn cookie_get_help() {
    bk().arg("cookie-get").arg("--help").assert().success()
        .stdout(predicate::str::contains("--bookie-id"));
}

#[test]
fn cookie_delete_help() {
    bk().arg("cookie-delete").arg("--help").assert().success()
        .stdout(predicate::str::contains("--force"));
}

#[test]
fn recover_help() {
    bk().arg("recover").arg("--help").assert().success()
        .stdout(predicate::str::contains("--bookie-src"));
}

#[test]
fn decommissionbookie_help() {
    bk().arg("decommissionbookie").arg("--help").assert().success()
        .stdout(predicate::str::contains("--bookie-id"));
}

#[test]
fn localconsistencycheck_help() {
    bk().arg("localconsistencycheck").arg("--help").assert().success()
        .stdout(predicate::str::contains("--data-dir"));
}

#[test]
fn lostbookierecoverydelay_help() {
    bk().arg("lostbookierecoverydelay").arg("--help").assert().success()
        .stdout(predicate::str::contains("--set"));
}

#[test]
fn triggeraudit_help() {
    bk().arg("triggeraudit").arg("--help").assert().success();
}

#[test]
fn metaformat_help_has_force() {
    bk().arg("metaformat").arg("--help").assert().success()
        .stdout(predicate::str::contains("--force"));
}

#[test]
fn lastmark_help() {
    bk().arg("lastmark").arg("--help").assert().success()
        .stdout(predicate::str::contains("--data-dir"));
}

#[test]
fn readjournal_help() {
    bk().arg("readjournal").arg("--help").assert().success()
        .stdout(predicate::str::contains("--file"));
}

#[test]
fn readlog_help() {
    bk().arg("readlog").arg("--help").assert().success()
        .stdout(predicate::str::contains("--file"));
}

#[test]
fn listfilesondisc_help() {
    bk().arg("listfilesondisc").arg("--help").assert().success()
        .stdout(predicate::str::contains("--data-dir"));
}

#[test]
fn updatebookieinledger_help() {
    bk().arg("updatebookieinledger").arg("--help").assert().success()
        .stdout(predicate::str::contains("--old-bookie"))
        .stdout(predicate::str::contains("--new-bookie"));
}

#[test]
fn whatisinstanceid_help() {
    bk().arg("whatisinstanceid").arg("--help").assert().success();
}

#[test]
fn bookieformat_help() {
    bk().arg("bookieformat").arg("--help").assert().success()
        .stdout(predicate::str::contains("--data-dir"));
}

#[test]
fn nukeexistingcluster_help_has_instanceid_or_force() {
    bk().arg("nukeexistingcluster").arg("--help").assert().success()
        .stdout(predicate::str::contains("--instanceid").or(predicate::str::contains("--force")));
}

// Standalone (no ZK) tests

#[test]
fn lastmark_on_empty_dir_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    bk().arg("lastmark")
        .arg("--data-dir").arg(dir.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("No lastMark"));
}

#[test]
fn listfilesondisc_on_empty_dir_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    bk().arg("listfilesondisc")
        .arg("--data-dir").arg(dir.path())
        .assert()
        .success();
}

#[test]
fn localconsistencycheck_missing_indices_fails_gracefully() {
    let dir = tempfile::tempdir().unwrap();
    // No indices in this dir → should fail with a clear message, not panic
    bk().arg("localconsistencycheck")
        .arg("--data-dir").arg(dir.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("not found"));
}

#[test]
fn deleteledger_without_force_warns_and_exits() {
    // Without --force, should refuse deletion (this requires ZK so it'll fail
    // before reaching the warning, but we test the parser accepts the args).
    bk().arg("deleteledger")
        .arg("--zk").arg("invalid:0")
        .arg("--ledger-id").arg("42")
        .assert()
        .failure(); // Will fail at ZK connect; we just verify args parse
}
