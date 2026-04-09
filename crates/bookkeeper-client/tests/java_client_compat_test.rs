//! Cross-interop tests: Java BookKeeper client → Rust bookie.
//!
//! This test wires up a real Java client (running via Maven/JUnit in
//! `tests/java-compat`) against an in-process Rust bookie. It is the
//! symmetric counterpart to `java_bookie_container_test.rs`, which
//! exercises the Rust client → Java bookie direction.
//!
//! Architecture:
//!   1. Spawn ZooKeeper via testcontainers
//!   2. Initialize the BK cluster in ZK via the Rust admin API
//!   3. Spawn an in-process Rust BookieServer registered with ZK
//!   4. Invoke `mvn test` in tests/java-compat against that ZK
//!   5. Maven downloads / reuses Java BookKeeper client artifacts and
//!      runs the JUnit `RustBookieCompatTest` suite (19 tests covering
//!      V2/V3 wire protocol, multiple digest types, fencing, recovery,
//!      large entries, deletion, LAC, wrong-password rejection, etc.)
//!
//! The test is **skipped gracefully** if any prerequisite is missing:
//!   - Docker (for ZooKeeper container)
//!   - `mvn` on PATH
//!   - The local Apache BookKeeper Java jar at the path baked into
//!     `tests/java-compat/pom.xml` (4.18.0-SNAPSHOT)
//!   - The compiled java-compat target/ classes
//!
//! When all prerequisites are present, this is the highest-fidelity
//! cross-interop coverage we have — the official Apache BookKeeper Java
//! client doing real ZK discovery, ledger creation, fencing, and reads
//! against our Rust bookie.

use std::path::PathBuf;
use std::process::Command as StdCommand;

use bookkeeper_test_utils::{bootstrap_cluster, shared_zk, spawn_bookie};

// ---------------------------------------------------------------------------
// Prerequisite detection
// ---------------------------------------------------------------------------

fn workspace_root() -> PathBuf {
    // crates/bookkeeper-client/tests/java_client_compat_test.rs → ../../..
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .unwrap_or(manifest)
}

fn java_compat_dir() -> PathBuf {
    workspace_root().join("tests").join("java-compat")
}

fn pom_xml() -> PathBuf {
    java_compat_dir().join("pom.xml")
}

fn mvn_available() -> bool {
    StdCommand::new("mvn")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Check that the Java compat project can actually run. Returns the reason
/// for skipping if anything is missing, or None if we can proceed.
fn check_prerequisites() -> Option<&'static str> {
    if !pom_xml().exists() {
        return Some("tests/java-compat/pom.xml not found");
    }
    if !mvn_available() {
        return Some("mvn not on PATH");
    }
    None
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn java_client_runs_full_compat_suite_against_rust_bookie() {
    if let Some(reason) = check_prerequisites() {
        eprintln!("[SKIP] java client compat: {reason}");
        return;
    }
    let Some(zk_connect) = shared_zk().await else { return; };
    let root = "/ledgers"; // hard-coded in pom.xml's metadataServiceUri builder
    bootstrap_cluster(&zk_connect, root).await;

    let (bookie, _dir) = spawn_bookie(&zk_connect, root).await;
    eprintln!(
        "[java-compat] ZK={} bookie={}",
        zk_connect,
        bookie.local_addr()
    );

    // Sanity check: list registered bookies in ZK so we can be sure the
    // Java client will see something on /ledgers/available.
    {
        let zk = zookeeper_client::Client::connect(&zk_connect).await.unwrap();
        let (children, _) = zk.get_children("/ledgers/available").await.unwrap();
        eprintln!(
            "[java-compat] /ledgers/available children: {:?}",
            children
        );
    }

    // Run the Maven test suite. Use --offline to avoid network if the local
    // dependency cache is already populated; fall back to online on first run.
    // We rely on the user having previously run `mvn install` for the local
    // BookKeeper jar (the pom.xml depends on bookkeeper-server 4.18.0-SNAPSHOT).
    let pom = pom_xml();
    let mut cmd = StdCommand::new("mvn");
    cmd.arg("-f")
        .arg(&pom)
        .arg("test")
        .arg(format!("-Dzk.connect={zk_connect}"))
        // Only run the single-bookie compat suite. MixedClusterTest needs a
        // Java bookie too and is exercised by run-mixed-cluster-tests.sh.
        .arg("-Dtest=RustBookieCompatTest")
        // Surefire: fail fast if any test errors so the failure surfaces in
        // cargo's output rather than after a long compile silence.
        .arg("-Dsurefire.skipAfterFailureCount=1");

    eprintln!("[java-compat] running: {:?}", cmd);
    let output = cmd
        .output()
        .expect("failed to spawn mvn — is Maven installed?");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        // Surface enough output to debug failures from cargo's UI.
        eprintln!("=== mvn stdout ===\n{stdout}");
        eprintln!("=== mvn stderr ===\n{stderr}");
        bookie.shutdown().await;
        panic!(
            "Java RustBookieCompatTest suite failed with exit status {:?}",
            output.status.code()
        );
    }

    // Sanity check: surefire reports test totals in non-quiet mode. If we
    // don't see them, the test class may have been filtered out entirely.
    let combined = format!("{stdout}{stderr}");
    if !combined.contains("Tests run") {
        eprintln!("=== mvn stdout ===\n{stdout}");
        eprintln!("=== mvn stderr ===\n{stderr}");
        bookie.shutdown().await;
        panic!(
            "mvn returned success but produced no 'Tests run' line — \
             surefire may have skipped the suite entirely"
        );
    }
    // Print just the summary line so cargo output stays informative.
    for line in combined.lines() {
        if line.contains("Tests run:") && line.contains("RustBookieCompatTest") {
            eprintln!("[java-compat] {line}");
        }
    }

    bookie.shutdown().await;
}
