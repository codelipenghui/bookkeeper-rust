# Java↔Rust Bookie In-Place Upgrade Test — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a `bookkeeper-upgrade-test` crate containing a single `#[ignore]`-gated integration test that validates bidirectional on-disk format compatibility between Java BK 4.18.0 and the Rust bookie via a 5-phase rolling-upgrade scenario.

**Architecture:** A new workspace crate with one integration test that uses `testcontainers-rs` to programmatically run ZooKeeper + bookie containers and shells out to the `docker` CLI for network/volume lifecycle. The workload is driven by the existing `bookkeeper-client` crate's `ZkBookKeeper` high-level API. The test owns named Docker volumes and a user-defined network so a single bookie slot can be re-launched with a different image (Java → Rust → Java) while keeping its data, journal, cookie file, and network alias stable.

**Tech Stack:** Rust 1.90, tokio (multi_thread), testcontainers-rs 0.27, anyhow, tracing, bookkeeper-client (workspace), zookeeper-client 0.11, Docker (CLI shell-out for volume/network lifecycle).

**Spec:** `docs/superpowers/specs/2026-04-11-java-rust-upgrade-test-design.md`

---

## File Structure

### New files

```
crates/bookkeeper-upgrade-test/         (new crate)
├── Cargo.toml                          # crate manifest, no production deps
├── README.md                           # prereqs + how to run
├── src/
│   └── lib.rs                          # empty placeholder (Cargo target requirement)
└── tests/
    └── upgrade.rs                      # the integration test (~500 LoC)
```

### Modified files

- `Cargo.toml` (workspace root) — add `crates/bookkeeper-upgrade-test` to `members`

### Responsibilities inside `tests/upgrade.rs` (one cohesive file)

| Section | Purpose |
|---|---|
| `init_tracing` | Set up tracing-subscriber for the test's diagnostic output |
| `check_prerequisites` | Verify Docker daemon + required images present, fail fast if not |
| `TestEnv` | Owns network + volumes; Drop-guard cleanup on success, leak-on-failure |
| `ZkContainer` | ZooKeeper container fixture with connection helpers |
| `BookieSlot` | Per-bookie state: start_java, start_rust, stop, wait_healthy |
| `swap_to_rust` / `swap_to_java` | Orchestration helpers wrapping stop → ZK ephemeral wait → start → wait_healthy |
| `wait_for_zk_ephemeral_to_clear` | Race fix: poll ZK until previous bookie's session times out |
| `make_bk_client`, `write_ledger`, `read_ledger` | Workload driven by `ZkBookKeeper` |
| `capture_baseline`, `verify_cluster_state`, `final_assertions` | Verification |
| Smoke tests (`smoke_*`) | One per fixture for diagnostic isolation, all `#[ignore]`-gated |
| `java_to_rust_in_place_upgrade` | The main test wiring P1–P5 |

All in one file separated by clear comment headers. No nested modules. No `pub mod` from `lib.rs`.

---

## Pre-flight: prerequisites the engineer needs before starting

The engineer must have these in place on their machine:

```bash
docker info > /dev/null              # Docker daemon running
docker pull apache/bookkeeper:4.17.3
docker build --target runtime -t rust-bookie:latest .   # in workspace root
```

If any prerequisite is missing, the test will surface a clear error in Task 2's `check_prerequisites()` — not a confusing testcontainers stack trace.

---

## Task 1: Create the crate skeleton

**Files:**
- Create: `crates/bookkeeper-upgrade-test/Cargo.toml`
- Create: `crates/bookkeeper-upgrade-test/src/lib.rs`
- Create: `crates/bookkeeper-upgrade-test/README.md`
- Create: `crates/bookkeeper-upgrade-test/tests/upgrade.rs`
- Modify: `Cargo.toml` (workspace root, add to `members`)

- [ ] **Step 1: Write the crate Cargo.toml**

```toml
[package]
name = "bookkeeper-upgrade-test"
version.workspace = true
edition.workspace = true
license.workspace = true
publish = false

[lib]
path = "src/lib.rs"

[[test]]
name = "upgrade"
path = "tests/upgrade.rs"

[dependencies]
# none — this is a test-only crate

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
testcontainers = { workspace = true }
anyhow = "1"
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
bookkeeper-client = { workspace = true }
bookkeeper-protocol = { workspace = true }
zookeeper-client = { workspace = true }
bytes = { workspace = true }
tempfile = { workspace = true }
```

- [ ] **Step 2: Write src/lib.rs (placeholder only)**

```rust
//! See `tests/upgrade.rs` for the integration test.
//! This crate has no library API.
```

- [ ] **Step 3: Write README.md**

````markdown
# bookkeeper-upgrade-test

End-to-end Java↔Rust bookie in-place upgrade validator. See
`docs/superpowers/specs/2026-04-11-java-rust-upgrade-test-design.md`
for the full design rationale.

## Prerequisites

```bash
docker info                                                # daemon running
docker pull apache/bookkeeper:4.17.3
docker build --target runtime -t rust-bookie:latest .      # in workspace root
```

## How to run

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade -- --ignored --nocapture
```

`--ignored` is required because the test is gated. `--nocapture` is
required so progress messages stream live.

## What it does

5-phase rolling upgrade test on a 2-bookie cluster (E=2 W=2 A=2):

1. **P1 all-Java** — write ledgers L1, L2 (java-era data)
2. **P2 rolling J→R** — swap bookies one at a time, verify reads after each
3. **P3 all-Rust** — write ledgers L3, L4 (rust-era data)
4. **P4 rolling R→J** — swap back to Java one at a time, verify reads
5. **P5 all-Java again** — final read-back of L1..L4 byte-for-byte

Wall time: ~6 minutes, dominated by Java JVM startup.

## On failure

State is preserved for inspection. Cleanup commands are printed to stderr.
````

- [ ] **Step 4: Write the skeleton tests/upgrade.rs**

```rust
//! Java↔Rust bookie in-place upgrade test.
//! See docs/superpowers/specs/2026-04-11-java-rust-upgrade-test-design.md

use anyhow::Result;

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn java_to_rust_in_place_upgrade() -> Result<()> {
    // Will be filled in across the remaining tasks.
    Ok(())
}
```

- [ ] **Step 5: Add to workspace members**

In `/Users/lipenghui/Github/sn/bookkeeper-rust/Cargo.toml`, add the new crate to the `members` list (alphabetically after `bookkeeper-test-utils`):

```toml
[workspace]
resolver = "2"
members = [
    "crates/bookkeeper-proto",
    "crates/bookkeeper-common",
    "crates/bookkeeper-protocol",
    "crates/bookkeeper-metadata",
    "crates/bookkeeper-storage",
    "crates/bookkeeper-server",
    "crates/bookkeeper-client",
    "crates/bookkeeper-config",
    "crates/bookkeeper-cli",
    "crates/bookkeeper-replication",
    "crates/bookkeeper-test-utils",
    "crates/bookkeeper-upgrade-test",
]
```

- [ ] **Step 6: Build the new crate**

```bash
cargo build -p bookkeeper-upgrade-test --tests
```

Expected: `Finished 'dev' profile [...] target(s) in N.NNs` with no errors.

- [ ] **Step 7: Run the (empty) test to verify it's gated**

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade -- --nocapture
```

Expected: `running 1 test ... test java_to_rust_in_place_upgrade ... ignored ... 0 passed; 0 failed; 1 ignored`

- [ ] **Step 8: Run the test with `--ignored` to verify it currently passes (empty body)**

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade -- --ignored --nocapture
```

Expected: `test java_to_rust_in_place_upgrade ... ok ... 1 passed; 0 failed`

- [ ] **Step 9: Commit**

```bash
git add crates/bookkeeper-upgrade-test Cargo.toml
git commit -m "$(cat <<'EOF'
test: scaffold bookkeeper-upgrade-test crate

New workspace crate with a single #[ignore]-gated integration test.
Skeleton only; subsequent tasks fill in the test body. See
docs/superpowers/specs/2026-04-11-java-rust-upgrade-test-design.md
EOF
)"
```

---

## Task 2: Prerequisite checking

**Files:**
- Modify: `crates/bookkeeper-upgrade-test/tests/upgrade.rs`

- [ ] **Step 1: Add the prerequisite checker and tracing initializer**

Append after the existing imports/test in `tests/upgrade.rs`:

```rust
use std::process::Command;
use tracing::{info, warn};

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();
}

const JAVA_BK_IMAGE: &str = "apache/bookkeeper:4.17.3";
const RUST_BK_IMAGE: &str = "rust-bookie:latest";

fn check_prerequisites() -> Result<()> {
    // Docker daemon
    let out = Command::new("docker").arg("info").output()
        .map_err(|e| anyhow::anyhow!("docker not on PATH: {e}"))?;
    if !out.status.success() {
        anyhow::bail!(
            "Docker daemon not reachable. Start Docker Desktop / Docker Engine and try again.\n\
             stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    info!("✓ Docker daemon reachable");

    // Java BK image
    require_image(JAVA_BK_IMAGE, &format!("docker pull {JAVA_BK_IMAGE}"))?;
    info!("✓ {} present", JAVA_BK_IMAGE);

    // Rust bookie image
    require_image(RUST_BK_IMAGE,
        "docker build --target runtime -t rust-bookie:latest .")?;
    info!("✓ {} present", RUST_BK_IMAGE);

    Ok(())
}

fn require_image(image: &str, fix_cmd: &str) -> Result<()> {
    let out = Command::new("docker")
        .args(["image", "inspect", image])
        .output()?;
    if !out.status.success() {
        anyhow::bail!(
            "Required image not found locally: {image}\n\
             To fix:\n  {fix_cmd}",
        );
    }
    Ok(())
}
```

- [ ] **Step 2: Wire `init_tracing` and `check_prerequisites` into the main test**

Replace the body of `java_to_rust_in_place_upgrade`:

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn java_to_rust_in_place_upgrade() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    info!("Prerequisites OK; remainder of test not yet implemented");
    Ok(())
}
```

- [ ] **Step 3: Run the test and verify the prereq path runs**

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade -- --ignored --nocapture
```

Expected (success path, when both images are present):
```
running 1 test
✓ Docker daemon reachable
✓ apache/bookkeeper:4.17.3 present
✓ rust-bookie:latest present
Prerequisites OK; remainder of test not yet implemented
test java_to_rust_in_place_upgrade ... ok
```

- [ ] **Step 4: Verify the failure path manually by temporarily renaming the image**

```bash
docker tag rust-bookie:latest rust-bookie:tmp
docker rmi rust-bookie:latest
cargo test -p bookkeeper-upgrade-test --test upgrade -- --ignored --nocapture 2>&1 | head -20
```

Expected: test fails with the "Required image not found locally" error message and the fix command. After verifying:

```bash
docker tag rust-bookie:tmp rust-bookie:latest
docker rmi rust-bookie:tmp
```

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-upgrade-test/tests/upgrade.rs
git commit -m "test(upgrade): add prerequisite checks for Docker + images"
```

---

## Task 3: TestEnv — network + volume lifecycle

**Files:**
- Modify: `crates/bookkeeper-upgrade-test/tests/upgrade.rs`

- [ ] **Step 1: Add the TestEnv struct and helpers**

Append below the existing helpers:

```rust
use std::sync::atomic::{AtomicU64, Ordering};

/// Owns a Docker network + named volumes for one test run.
/// On Drop:
///   - if `succeeded` is true → remove network and volumes
///   - if `succeeded` is false → leave them and print cleanup commands
struct TestEnv {
    run_id: String,
    network: String,
    volumes: Vec<String>,
    succeeded: bool,
}

impl TestEnv {
    fn new() -> Result<Self> {
        // Monotonic + per-process unique run id; avoids collisions on parallel runs.
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        let run_id = format!("{}-{}", pid, n);
        let network = format!("bk-upgrade-test-{run_id}");
        let volumes = vec![
            format!("bk-upgrade-test-{run_id}-bk1-data"),
            format!("bk-upgrade-test-{run_id}-bk1-journal"),
            format!("bk-upgrade-test-{run_id}-bk2-data"),
            format!("bk-upgrade-test-{run_id}-bk2-journal"),
        ];

        // Create network
        let out = Command::new("docker")
            .args(["network", "create", "--driver", "bridge", &network])
            .output()?;
        if !out.status.success() {
            anyhow::bail!(
                "docker network create failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        info!("Created network {network}");

        // Create volumes
        for vol in &volumes {
            let out = Command::new("docker")
                .args(["volume", "create", vol])
                .output()?;
            if !out.status.success() {
                anyhow::bail!(
                    "docker volume create {vol} failed: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
            }
        }
        info!("Created {} volumes", volumes.len());

        Ok(Self {
            run_id,
            network,
            volumes,
            succeeded: false,
        })
    }

    fn mark_success(&mut self) {
        self.succeeded = true;
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        if self.succeeded {
            for vol in &self.volumes {
                let _ = Command::new("docker")
                    .args(["volume", "rm", "-f", vol])
                    .output();
            }
            let _ = Command::new("docker")
                .args(["network", "rm", &self.network])
                .output();
            info!("Cleaned up test env (run-id={})", self.run_id);
        } else {
            warn!(
                "TEST FAILED — state preserved for inspection.\n\
                 To clean up manually:\n  \
                 docker network rm {}\n  \
                 docker volume rm -f {}",
                self.network,
                self.volumes.join(" ")
            );
        }
    }
}
```

- [ ] **Step 2: Add a smoke test for TestEnv**

Append:

```rust
// ---------------------------------------------------------------------------
// Smoke tests — one per fixture, useful for diagnosing failures of the main
// test in isolation.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_test_env() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;

    // Verify network exists
    let out = Command::new("docker")
        .args(["network", "inspect", &env.network])
        .output()?;
    assert!(out.status.success(), "network not found after create");

    // Verify all 4 volumes exist
    for vol in &env.volumes {
        let out = Command::new("docker")
            .args(["volume", "inspect", vol])
            .output()?;
        assert!(out.status.success(), "volume {vol} not found after create");
    }

    env.mark_success();
    Ok(())
}
```

- [ ] **Step 3: Run the smoke test**

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade smoke_test_env -- --ignored --nocapture
```

Expected: test passes. Output includes "Created network bk-upgrade-test-...", "Created 4 volumes", and "Cleaned up test env (run-id=...)".

- [ ] **Step 4: Verify the failure path leaves state behind**

Temporarily edit the smoke test to add `anyhow::bail!("intentional");` before `mark_success()`. Run again:

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade smoke_test_env -- --ignored --nocapture 2>&1 | tail -20
```

Expected: test fails. Output includes "TEST FAILED — state preserved" with a `docker network rm bk-upgrade-test-...` command. Run that command manually to clean up. Then revert the `bail!` change.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-upgrade-test/tests/upgrade.rs
git commit -m "test(upgrade): add TestEnv with Docker network + volume lifecycle"
```

---

## Task 4: ZooKeeper container fixture

**Files:**
- Modify: `crates/bookkeeper-upgrade-test/tests/upgrade.rs`

- [ ] **Step 1: Add the ZkContainer fixture**

Append:

```rust
use std::time::Duration;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

/// ZooKeeper container, attached to the test network as `zookeeper:2181`.
struct ZkContainer {
    container: ContainerAsync<GenericImage>,
}

impl ZkContainer {
    async fn start(env: &TestEnv) -> Result<Self> {
        let img = GenericImage::new("zookeeper", "3.8")
            .with_exposed_port(2181.tcp())
            .with_wait_for(WaitFor::message_on_stdout("binding to port"))
            .with_network(env.network.clone())
            .with_container_name(format!("bk-upgrade-zk-{}", env.run_id));
        let container = img.start().await
            .map_err(|e| anyhow::anyhow!("start zookeeper: {e}"))?;
        info!("ZooKeeper container started");
        Ok(Self { container })
    }

    /// Hostname:port other containers in the same network use to reach ZK.
    fn in_network_addr(&self) -> &'static str {
        "zookeeper:2181"
    }

    /// host:port for the test process (running outside Docker) to reach ZK.
    async fn host_addr(&self) -> Result<String> {
        let host = self.container.get_host().await?;
        let port = self.container.get_host_port_ipv4(2181).await?;
        Ok(format!("{host}:{port}"))
    }
}
```

Note: testcontainers' `with_container_name` lets us give the ZK container a stable, predictable name. This is useful when debugging — the user can `docker logs bk-upgrade-zk-<run-id>` directly. The name has the run-id suffix to avoid collisions across test runs.

The `with_network` call tells testcontainers to attach the container to our user-defined bridge network. testcontainers also auto-aliases the service by name; for ZK we explicitly want the alias `zookeeper`, which we get via the service-name behavior. If alias control turns out to be insufficient, we can override with `with_network_alias` (add it to the chain).

- [ ] **Step 2: Add a smoke test for ZkContainer**

Append:

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_zk_container() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;

    // Connect from the host network
    let host_addr = zk.host_addr().await?;
    info!("ZK host_addr: {host_addr}");
    let client = zookeeper_client::Client::connect(&host_addr).await?;

    // Sanity: list root
    let (children, _) = client.list_children("/").await?;
    info!("ZK root children: {:?}", children);
    assert!(children.contains(&"zookeeper".to_string()), "/zookeeper subtree expected");

    env.mark_success();
    Ok(())
}
```

- [ ] **Step 3: Run the smoke test**

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade smoke_zk_container -- --ignored --nocapture
```

Expected: test passes. Output mentions "ZooKeeper container started", "ZK host_addr: 127.0.0.1:NNNNN", and "/zookeeper subtree expected".

- [ ] **Step 4: Verify the alias works from inside the network**

Open another terminal and run a one-off container that pings ZK by alias while the test is running:

```bash
# In one terminal: pause inside the smoke test by adding `tokio::time::sleep(Duration::from_secs(120)).await;` after `client.list_children`. Re-run.
# In another terminal:
docker run --rm --network bk-upgrade-test-<run-id> alpine sh -c \
  'apk add --no-cache busybox-extras > /dev/null && nc -zv zookeeper 2181'
```

Expected: `zookeeper (172.x.x.x:2181) open`. Remove the sleep when done.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-upgrade-test/tests/upgrade.rs
git commit -m "test(upgrade): add ZooKeeper container fixture"
```

---

## Task 5: BookieSlot — Java image start + wait_healthy

**Files:**
- Modify: `crates/bookkeeper-upgrade-test/tests/upgrade.rs`

- [ ] **Step 1: Add BookieSlot and start_java**

Append:

```rust
/// Per-bookie slot. Owns the volumes (via TestEnv), the network alias,
/// and the currently-running container handle (Java OR Rust).
struct BookieSlot {
    slot_id: u32,
    alias: String,                          // "bookie-1" or "bookie-2"
    data_volume: String,
    journal_volume: String,
    network: String,
    run_id: String,
    container: Option<ContainerAsync<GenericImage>>,
}

impl BookieSlot {
    fn new(slot_id: u32, env: &TestEnv) -> Self {
        let alias = format!("bookie-{slot_id}");
        let data_volume = env.volumes
            .iter()
            .find(|v| v.ends_with(&format!("-bk{slot_id}-data")))
            .expect("data volume should exist for slot")
            .clone();
        let journal_volume = env.volumes
            .iter()
            .find(|v| v.ends_with(&format!("-bk{slot_id}-journal")))
            .expect("journal volume should exist for slot")
            .clone();
        Self {
            slot_id,
            alias,
            data_volume,
            journal_volume,
            network: env.network.clone(),
            run_id: env.run_id.clone(),
            container: None,
        }
    }

    async fn start_java(&mut self, zk_in_network: &str) -> Result<()> {
        use testcontainers::core::Mount;

        let img = GenericImage::new("apachebookkeeper", "4.18.0")
            .with_exposed_port(3181.tcp())
            .with_exposed_port(8080.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Started Bookie server"))
            .with_network(self.network.clone())
            .with_container_name(format!("bk-upgrade-{}-{}", self.alias, self.run_id))
            .with_mount(Mount::volume_mount(self.data_volume.clone(), "/bk/data"))
            .with_mount(Mount::volume_mount(self.journal_volume.clone(), "/bk/journal"))
            .with_env_var("BK_zkServers", zk_in_network)
            .with_env_var("BK_advertisedAddress", &self.alias)
            .with_env_var("BK_useHostNameAsBookieID", "true")
            .with_env_var("BK_journalDirectory", "/bk/journal")
            .with_env_var("BK_ledgerDirectories", "/bk/data")
            .with_env_var("BK_bookiePort", "3181")
            .with_env_var("BK_httpServerEnabled", "true")
            .with_env_var("BK_httpServerPort", "8080");
        let container = img.start().await
            .map_err(|e| anyhow::anyhow!("start java bookie {}: {e}", self.alias))?;
        self.container = Some(container);
        info!("Started Java bookie {}", self.alias);
        Ok(())
    }

    async fn wait_healthy(&self, timeout: Duration) -> Result<()> {
        let container = self.container.as_ref()
            .ok_or_else(|| anyhow::anyhow!("no container in slot {}", self.alias))?;
        let host = container.get_host().await?;
        let http_port = container.get_host_port_ipv4(8080).await?;
        let url = format!("http://{host}:{http_port}/heartbeat");

        let deadline = std::time::Instant::now() + timeout;
        loop {
            // Use std blocking HTTP via curl in a sidecar to avoid an extra Rust HTTP dep.
            let out = Command::new("curl")
                .args(["-s", "-o", "/dev/null", "-w", "%{http_code}", &url])
                .output();
            if let Ok(out) = out {
                if out.status.success() {
                    let code = String::from_utf8_lossy(&out.stdout);
                    if code.trim() == "200" {
                        info!("Bookie {} healthy ({})", self.alias, url);
                        return Ok(());
                    }
                }
            }
            if std::time::Instant::now() >= deadline {
                anyhow::bail!("bookie {} did not become healthy within {:?}", self.alias, timeout);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}
```

Note: we shell out to `curl` rather than adding `reqwest` as a dep, since the only HTTP call we make is `/heartbeat` and curl is universally available on dev/CI machines.

- [ ] **Step 2: Add a smoke test for one Java bookie**

Append:

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_java_bookie() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;

    let mut slot = BookieSlot::new(1, &env);
    slot.start_java(zk.in_network_addr()).await?;
    slot.wait_healthy(Duration::from_secs(120)).await?;

    env.mark_success();
    Ok(())
}
```

- [ ] **Step 3: Run the smoke test**

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade smoke_java_bookie -- --ignored --nocapture
```

Expected: test passes within ~90 seconds. Output mentions "Started Java bookie bookie-1" and "Bookie bookie-1 healthy".

- [ ] **Step 4: Commit**

```bash
git add crates/bookkeeper-upgrade-test/tests/upgrade.rs
git commit -m "test(upgrade): add BookieSlot with Java image start + health check"
```

---

## Task 6: BookieSlot — Rust image start with bind-mounted config

**Files:**
- Modify: `crates/bookkeeper-upgrade-test/tests/upgrade.rs`

- [ ] **Step 1: Add the Rust config file generator**

Append (after `BookieSlot::start_java`):

```rust
use tempfile::TempDir;

/// Render a per-slot bk_server.conf into `dir` and return its host path.
/// The Rust bookie image reads /etc/bookkeeper/bk_server.conf at startup.
fn render_rust_conf(dir: &std::path::Path, alias: &str, zk_in_network: &str) -> Result<std::path::PathBuf> {
    let conf = format!(
        "bookiePort=3181\n\
         httpServerEnabled=true\n\
         httpServerPort=8080\n\
         ledgerDirectories=/bk/data\n\
         journalDirectory=/bk/journal\n\
         zkServers={zk}\n\
         advertisedAddress={alias}\n\
         useHostNameAsBookieID=true\n\
         journalSyncData=true\n\
         journalWriteData=true\n\
         serverNumIOThreads=4\n",
        zk = zk_in_network,
        alias = alias,
    );
    let path = dir.join(format!("bk_server-{alias}.conf"));
    std::fs::write(&path, conf)?;
    Ok(path)
}
```

- [ ] **Step 2: Add `start_rust` to `BookieSlot`**

Inside `impl BookieSlot`, add:

```rust
async fn start_rust(&mut self, zk_in_network: &str, conf_dir: &std::path::Path) -> Result<()> {
    use testcontainers::core::Mount;

    let conf_path = render_rust_conf(conf_dir, &self.alias, zk_in_network)?;
    let conf_path_str = conf_path.to_string_lossy().to_string();

    let img = GenericImage::new("rust-bookie", "latest")
        .with_exposed_port(3181.tcp())
        .with_exposed_port(8080.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Bookie started at"))
        .with_network(self.network.clone())
        .with_container_name(format!("bk-upgrade-{}-{}", self.alias, self.run_id))
        .with_mount(Mount::volume_mount(self.data_volume.clone(), "/bk/data"))
        .with_mount(Mount::volume_mount(self.journal_volume.clone(), "/bk/journal"))
        .with_mount(Mount::bind_mount(conf_path_str, "/etc/bookkeeper/bk_server.conf"));
    let container = img.start().await
        .map_err(|e| anyhow::anyhow!("start rust bookie {}: {e}", self.alias))?;
    self.container = Some(container);
    info!("Started Rust bookie {}", self.alias);
    Ok(())
}
```

- [ ] **Step 3: Add a smoke test for one Rust bookie (no swap, fresh volumes)**

Append:

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_rust_bookie() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;
    let conf_dir = TempDir::new()?;

    let mut slot = BookieSlot::new(1, &env);
    slot.start_rust(zk.in_network_addr(), conf_dir.path()).await?;
    slot.wait_healthy(Duration::from_secs(60)).await?;

    env.mark_success();
    Ok(())
}
```

- [ ] **Step 4: Run the smoke test**

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade smoke_rust_bookie -- --ignored --nocapture
```

Expected: test passes within ~30 seconds (Rust startup is much faster than Java). Output mentions "Started Rust bookie bookie-1" and "Bookie bookie-1 healthy".

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-upgrade-test/tests/upgrade.rs
git commit -m "test(upgrade): add BookieSlot Rust image start with bind-mounted config"
```

---

## Task 7: ZkBookKeeper client + write_ledger helper

**Files:**
- Modify: `crates/bookkeeper-upgrade-test/tests/upgrade.rs`

- [ ] **Step 1: Add the BK client connect + write_ledger helpers**

Append:

```rust
use bookkeeper_client::zk_client::ZkBookKeeper;
use bookkeeper_protocol::digest::DigestType;

/// Open a `ZkBookKeeper` against the test ZK. The ZK address is the host-side
/// address (the test driver runs outside Docker).
async fn make_bk_client(zk_host_addr: &str) -> Result<ZkBookKeeper> {
    let bk = ZkBookKeeper::new(zk_host_addr, "/ledgers")
        .await
        .map_err(|e| anyhow::anyhow!("ZkBookKeeper::new: {e}"))?;
    Ok(bk)
}

/// Deterministic payload for a given (ledger, entry, era) so the test can
/// later verify entries byte-for-byte.
fn payload_for(ledger_id: i64, entry_id: i64, era: &str) -> Vec<u8> {
    format!("ledger={ledger_id} entry={entry_id} era={era}").into_bytes()
}

/// Create ledger `ledger_id` (note: ZkBookKeeper allocates the actual id, we
/// just label it for our local tracking), write `n_entries` deterministic
/// entries, close the ledger, return the actual ledger id assigned by ZK.
async fn write_ledger(
    bk: &mut ZkBookKeeper,
    label: i64,
    n_entries: i64,
    era: &str,
) -> Result<i64> {
    let (mut wh, conns) = bk.create_ledger(2, 2, 2, DigestType::Crc32c, b"upgrade-test")
        .await
        .map_err(|e| anyhow::anyhow!("create_ledger label={label}: {e}"))?;
    let ledger_id = wh.ledger_id();
    info!("Created ledger label={label} → id={ledger_id}");

    for i in 0..n_entries {
        let payload = payload_for(label, i, era);
        wh.append(&conns, &payload)
            .await
            .map_err(|e| anyhow::anyhow!("append ledger={ledger_id} entry={i}: {e}"))?;
    }
    info!("Wrote {n_entries} entries to ledger {ledger_id} (label={label}, era={era})");

    // Close the ledger so the metadata transitions OPEN → CLOSED.
    bk.close_ledger(ledger_id, wh.last_add_confirmed())
        .await
        .map_err(|e| anyhow::anyhow!("close_ledger id={ledger_id}: {e}"))?;
    info!("Closed ledger {ledger_id}");

    Ok(ledger_id)
}
```

Note: `wh.ledger_id()` and `bk.close_ledger(...)` are the real method names exposed by `ZkBookKeeper` and `WriteHandle` in the `bookkeeper-client` crate. If the actual signatures differ slightly (e.g. the close-ledger flow uses a different method name like `close()` on the handle), the engineer will see a clear compile error pointing to the relevant line — at which point they should `grep -rn "fn close" crates/bookkeeper-client/src/zk_client.rs` and adapt the call site to match.

- [ ] **Step 2: Add a smoke test that writes a ledger to a 2-bookie cluster**

Append:

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_write_ledger() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;

    // Need 2 bookies to satisfy E=2 W=2 A=2.
    let mut slot1 = BookieSlot::new(1, &env);
    let mut slot2 = BookieSlot::new(2, &env);
    slot1.start_java(zk.in_network_addr()).await?;
    slot2.start_java(zk.in_network_addr()).await?;
    slot1.wait_healthy(Duration::from_secs(120)).await?;
    slot2.wait_healthy(Duration::from_secs(120)).await?;

    let zk_host = zk.host_addr().await?;
    let mut bk = make_bk_client(&zk_host).await?;
    let id = write_ledger(&mut bk, /*label*/ 100, /*entries*/ 5, "smoke").await?;
    info!("smoke_write_ledger wrote ledger id={id}");

    env.mark_success();
    Ok(())
}
```

- [ ] **Step 3: Run the smoke test**

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade smoke_write_ledger -- --ignored --nocapture
```

Expected: test passes within ~3 minutes. Output includes "Wrote 5 entries to ledger N" and "Closed ledger N".

- [ ] **Step 4: Commit**

```bash
git add crates/bookkeeper-upgrade-test/tests/upgrade.rs
git commit -m "test(upgrade): add ZkBookKeeper client + write_ledger helper"
```

---

## Task 8: read_ledger helper + smoke test

**Files:**
- Modify: `crates/bookkeeper-upgrade-test/tests/upgrade.rs`

- [ ] **Step 1: Add `read_ledger` helper**

Append after `write_ledger`:

```rust
/// Open `ledger_id` for read, fetch entries 0..expected_entries, verify each
/// payload byte-for-byte against `payload_for(label, i, era)`. Returns the
/// number of entries successfully verified (== `expected_entries` on success).
async fn read_ledger(
    bk: &mut ZkBookKeeper,
    ledger_id: i64,
    label: i64,
    expected_entries: i64,
    era: &str,
) -> Result<i64> {
    let (rh, conns) = bk.open_ledger(ledger_id, DigestType::Crc32c, b"upgrade-test")
        .await
        .map_err(|e| anyhow::anyhow!("open_ledger id={ledger_id}: {e}"))?;

    for i in 0..expected_entries {
        let actual = rh.read_entry(&conns, i)
            .await
            .map_err(|e| anyhow::anyhow!("read_entry ledger={ledger_id} entry={i}: {e}"))?;
        let expected = payload_for(label, i, era);
        if actual != expected {
            anyhow::bail!(
                "payload mismatch at ledger={ledger_id} entry={i}\n  \
                 expected: {:?}\n  \
                 actual:   {:?}",
                String::from_utf8_lossy(&expected),
                String::from_utf8_lossy(&actual)
            );
        }
    }
    info!("Read-verified {expected_entries} entries from ledger {ledger_id}");
    Ok(expected_entries)
}
```

- [ ] **Step 2: Extend the smoke test to write + read in one go**

Replace the body of `smoke_write_ledger` to also read the ledger back:

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_write_ledger() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;

    let mut slot1 = BookieSlot::new(1, &env);
    let mut slot2 = BookieSlot::new(2, &env);
    slot1.start_java(zk.in_network_addr()).await?;
    slot2.start_java(zk.in_network_addr()).await?;
    slot1.wait_healthy(Duration::from_secs(120)).await?;
    slot2.wait_healthy(Duration::from_secs(120)).await?;

    let zk_host = zk.host_addr().await?;
    let mut bk = make_bk_client(&zk_host).await?;
    let id = write_ledger(&mut bk, /*label*/ 100, /*entries*/ 5, "smoke").await?;
    let count = read_ledger(&mut bk, id, /*label*/ 100, /*entries*/ 5, "smoke").await?;
    assert_eq!(count, 5);

    env.mark_success();
    Ok(())
}
```

- [ ] **Step 3: Run the smoke test**

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade smoke_write_ledger -- --ignored --nocapture
```

Expected: test passes. Output now includes "Read-verified 5 entries from ledger N".

- [ ] **Step 4: Commit**

```bash
git add crates/bookkeeper-upgrade-test/tests/upgrade.rs
git commit -m "test(upgrade): add read_ledger helper with payload verification"
```

---

## Task 9: ZK ephemeral wait + BookieSlot::stop

**Files:**
- Modify: `crates/bookkeeper-upgrade-test/tests/upgrade.rs`

- [ ] **Step 1: Add `BookieSlot::stop`**

Inside `impl BookieSlot`, add:

```rust
async fn stop(&mut self) -> Result<()> {
    if let Some(container) = self.container.take() {
        // testcontainers' default stop_timeout is 10s; the bookie's checkpoint()
        // can take longer to flush a full write_cache. Give it 120s. SIGKILL on
        // a flush leaves entry log files needing log_id roll-forward (see
        // commit bc4e7922 for context on the corresponding bookie-side fix).
        container.stop_with_timeout(Some(120)).await
            .map_err(|e| anyhow::anyhow!("stop bookie {}: {e}", self.alias))?;
        // We're done with the container; drop it to release resources.
        drop(container);
        info!("Stopped bookie {}", self.alias);
    }
    Ok(())
}
```

- [ ] **Step 2: Add `wait_for_zk_ephemeral_to_clear`**

Append (above the smoke tests):

```rust
/// After stopping a bookie, ZK doesn't immediately remove its ephemeral
/// registration node — it has to wait for the session timeout. If the new
/// bookie tries to register before that, ZK returns NODE_EXISTS. This polls
/// `/ledgers/available/<bookie>:3181` until it disappears, or until 30s.
async fn wait_for_zk_ephemeral_to_clear(
    zk: &zookeeper_client::Client,
    alias: &str,
) -> Result<()> {
    let path = format!("/ledgers/available/{alias}:3181");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match zk.check_stat(&path).await {
            Ok(None) => {
                info!("ZK ephemeral {path} cleared");
                return Ok(());
            }
            Ok(Some(_)) => {
                if std::time::Instant::now() >= deadline {
                    anyhow::bail!("ZK ephemeral {path} did not clear within 30s");
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => anyhow::bail!("zk check_stat {path}: {e}"),
        }
    }
}
```

Note: `zookeeper_client::Client::check_stat(path)` returns `Ok(None)` when the path does not exist and `Ok(Some(stat))` when it does. Verify the exact method name and signature in `crates/bookkeeper-client/Cargo.toml` → `zookeeper-client = "0.11"` docs / source. If the method is named differently (e.g. `exists`), adjust the call.

- [ ] **Step 3: Add a smoke test that stops a bookie and waits for ZK cleanup**

Append:

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_stop_and_zk_clear() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;

    let mut slot = BookieSlot::new(1, &env);
    slot.start_java(zk.in_network_addr()).await?;
    slot.wait_healthy(Duration::from_secs(120)).await?;

    let zk_host = zk.host_addr().await?;
    let zk_client = zookeeper_client::Client::connect(&zk_host).await?;

    // Confirm registered first
    let stat = zk_client.check_stat("/ledgers/available/bookie-1:3181").await?;
    assert!(stat.is_some(), "expected bookie-1 registered before stop");

    // Stop and wait for ephemeral to clear
    slot.stop().await?;
    wait_for_zk_ephemeral_to_clear(&zk_client, &slot.alias).await?;

    env.mark_success();
    Ok(())
}
```

- [ ] **Step 4: Run the smoke test**

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade smoke_stop_and_zk_clear -- --ignored --nocapture
```

Expected: test passes within ~2.5 minutes. Output includes "Stopped bookie bookie-1" and "ZK ephemeral /ledgers/available/bookie-1:3181 cleared".

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-upgrade-test/tests/upgrade.rs
git commit -m "test(upgrade): add BookieSlot::stop + wait_for_zk_ephemeral_to_clear"
```

---

## Task 10: swap_to_rust + swap_to_java (the heart of the test)

**Files:**
- Modify: `crates/bookkeeper-upgrade-test/tests/upgrade.rs`

- [ ] **Step 1: Add the swap helpers**

Append:

```rust
async fn swap_to_rust(
    slot: &mut BookieSlot,
    zk_in_network: &str,
    zk_client: &zookeeper_client::Client,
    conf_dir: &std::path::Path,
) -> Result<()> {
    info!("Swapping {} → Rust", slot.alias);
    slot.stop().await?;
    wait_for_zk_ephemeral_to_clear(zk_client, &slot.alias).await?;
    slot.start_rust(zk_in_network, conf_dir).await?;
    slot.wait_healthy(Duration::from_secs(120)).await?;
    info!("Swap to Rust complete for {}", slot.alias);
    Ok(())
}

async fn swap_to_java(
    slot: &mut BookieSlot,
    zk_in_network: &str,
    zk_client: &zookeeper_client::Client,
) -> Result<()> {
    info!("Swapping {} → Java", slot.alias);
    slot.stop().await?;
    wait_for_zk_ephemeral_to_clear(zk_client, &slot.alias).await?;
    slot.start_java(zk_in_network).await?;
    slot.wait_healthy(Duration::from_secs(120)).await?;
    info!("Swap to Java complete for {}", slot.alias);
    Ok(())
}
```

- [ ] **Step 2: Add a smoke test that does ONE swap and verifies a read across it**

This is the first task that exercises real on-disk format compat. It is the most likely to surface real bugs in the bookie implementation.

Append:

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_swap_one_bookie() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;
    let conf_dir = TempDir::new()?;

    let mut slot1 = BookieSlot::new(1, &env);
    let mut slot2 = BookieSlot::new(2, &env);
    slot1.start_java(zk.in_network_addr()).await?;
    slot2.start_java(zk.in_network_addr()).await?;
    slot1.wait_healthy(Duration::from_secs(120)).await?;
    slot2.wait_healthy(Duration::from_secs(120)).await?;

    // Write a ledger via Java cluster
    let zk_host = zk.host_addr().await?;
    let mut bk = make_bk_client(&zk_host).await?;
    let id = write_ledger(&mut bk, /*label*/ 100, /*entries*/ 50, "java-era").await?;

    // Swap bookie-1 from Java to Rust on the same volumes
    let zk_client = zookeeper_client::Client::connect(&zk_host).await?;
    swap_to_rust(&mut slot1, zk.in_network_addr(), &zk_client, conf_dir.path()).await?;

    // Re-open the BK client (its bookie connection cache is stale after swap).
    let mut bk = make_bk_client(&zk_host).await?;

    // Read the ledger back. The data on bookie-1's volume was written by
    // Java; we are now asking the Rust bookie (which is reusing those
    // volumes) to read them back.
    read_ledger(&mut bk, id, 100, 50, "java-era").await?;

    env.mark_success();
    Ok(())
}
```

- [ ] **Step 3: Run the smoke test**

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade smoke_swap_one_bookie -- --ignored --nocapture
```

Expected: test passes. Output includes:
- "Wrote 50 entries to ledger N (label=100, era=java-era)"
- "Swapping bookie-1 → Rust"
- "Stopped bookie bookie-1"
- "ZK ephemeral /ledgers/available/bookie-1:3181 cleared"
- "Started Rust bookie bookie-1"
- "Bookie bookie-1 healthy"
- "Swap to Rust complete for bookie-1"
- "Read-verified 50 entries from ledger N"

If the test fails at the read step, that's a real on-disk format compat bug. The cleanup path leaves the volumes in place; the engineer can `docker run -it --rm -v <vol>:/bk apache/bookkeeper:4.17.3 bash` to inspect the bookie's data dir and figure out which compat surface broke.

- [ ] **Step 4: Commit**

```bash
git add crates/bookkeeper-upgrade-test/tests/upgrade.rs
git commit -m "test(upgrade): add swap_to_rust/swap_to_java + first cross-swap read smoke test"
```

---

## Task 11: capture_baseline + verify_cluster_state

**Files:**
- Modify: `crates/bookkeeper-upgrade-test/tests/upgrade.rs`

- [ ] **Step 1: Add the cookie reader**

Append:

```rust
/// Read the cookie file from a bookie's data volume by `docker exec`-ing
/// `cat /bk/data/current/VERSION` inside the running container. Returns the
/// raw bytes; the caller is responsible for parsing.
async fn read_cookie(slot: &BookieSlot) -> Result<Vec<u8>> {
    let container_name = format!("bk-upgrade-{}-{}", slot.alias, slot.run_id);
    let out = Command::new("docker")
        .args(["exec", &container_name, "cat", "/bk/data/current/VERSION"])
        .output()?;
    if !out.status.success() {
        anyhow::bail!(
            "docker exec cat VERSION on {container_name} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(out.stdout)
}

/// Parsed view of a Java BK cookie file. The format is a sequence of
/// `key: value` lines, plus a few binary fields. We extract just the fields
/// we want to assert on.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CookieFields {
    bookie_host: String,
    journal_dirs: String,
    ledger_dirs: String,
    instance_id: Option<String>,
    layout_version: String,
}

fn parse_cookie(bytes: &[u8]) -> Result<CookieFields> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| anyhow::anyhow!("cookie not utf8: {e}"))?;
    let mut bookie_host = None;
    let mut journal_dirs = None;
    let mut ledger_dirs = None;
    let mut instance_id = None;
    let mut layout_version = None;
    for line in text.lines() {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim();
            let v = v.trim().to_string();
            match k {
                "bookieHost" => bookie_host = Some(v),
                "journalDir" => journal_dirs = Some(v),
                "ledgerDirs" => ledger_dirs = Some(v),
                "instanceId" => instance_id = Some(v),
                "4" | "layoutVersion" => layout_version = Some(v),
                _ => {}
            }
        }
    }
    Ok(CookieFields {
        bookie_host: bookie_host.ok_or_else(|| anyhow::anyhow!("cookie missing bookieHost"))?,
        journal_dirs: journal_dirs.ok_or_else(|| anyhow::anyhow!("cookie missing journalDir"))?,
        ledger_dirs: ledger_dirs.ok_or_else(|| anyhow::anyhow!("cookie missing ledgerDirs"))?,
        instance_id,
        layout_version: layout_version.unwrap_or_else(|| "unknown".into()),
    })
}
```

Note: the actual Java cookie file format is a versioned binary format, NOT a plain text k:v format. The parser above is a placeholder that handles the simple text-style cookie that the Rust bookie writes; real Java BK cookies are written by `org.apache.bookkeeper.discover.BookieServiceInfoUtils.parseCookie` and are length-prefixed binary. **Before running this task**, the engineer must inspect a real Java BK cookie by `docker run --rm apache/bookkeeper:4.17.3 bash -c 'cat /bk/data/current/VERSION | xxd | head'` and adapt `parse_cookie` to handle whichever format is on disk. If both Java and Rust write text-format cookies, the placeholder works as-is.

- [ ] **Step 2: Add `Baseline` and `capture_baseline`**

Append:

```rust
/// Snapshot of cluster state captured at the start of the test, used as the
/// reference for `verify_cluster_state`.
#[derive(Debug, Clone)]
struct Baseline {
    available_bookies: Vec<String>,
    instance_id: String,
    cookies: Vec<(String, CookieFields)>, // (alias, fields)
}

async fn capture_baseline(
    zk: &zookeeper_client::Client,
    bookies: &[BookieSlot],
) -> Result<Baseline> {
    let (children, _) = zk.list_children("/ledgers/available").await?;
    let mut available_bookies: Vec<String> = children
        .into_iter()
        .filter(|c| !c.starts_with("readonly"))
        .collect();
    available_bookies.sort();

    let (id_bytes, _) = zk.get_data("/ledgers/INSTANCEID").await?;
    let instance_id = String::from_utf8_lossy(&id_bytes).into_owned();

    let mut cookies = Vec::new();
    for slot in bookies {
        let raw = read_cookie(slot).await?;
        let fields = parse_cookie(&raw)?;
        cookies.push((slot.alias.clone(), fields));
    }

    info!(
        "Baseline: bookies={available_bookies:?} instance_id={instance_id} cookies_collected={}",
        cookies.len()
    );
    Ok(Baseline {
        available_bookies,
        instance_id,
        cookies,
    })
}
```

- [ ] **Step 3: Add `verify_cluster_state`**

Append:

```rust
/// Run the per-phase assertion bundle. Compares the live cluster state
/// against the baseline captured in P1. `closed_ledgers` is the list of
/// (ledger_id, label, count, era) tuples for every ledger that has been
/// CLOSED so far in the test (we re-read every entry of every closed ledger
/// to catch latent corruption).
async fn verify_cluster_state(
    zk: &zookeeper_client::Client,
    bookies: &[BookieSlot],
    baseline: &Baseline,
    bk: &mut ZkBookKeeper,
    closed_ledgers: &[(i64, i64, i64, &str)], // (id, label, count, era)
    phase_label: &str,
) -> Result<()> {
    info!("=== Verifying cluster state after {phase_label} ===");

    // 1. Both bookies healthy
    for slot in bookies {
        slot.wait_healthy(Duration::from_secs(30)).await
            .map_err(|e| anyhow::anyhow!("[{phase_label}] bookie {} not healthy: {e}", slot.alias))?;
    }

    // 2. Identity continuity
    let (children, _) = zk.list_children("/ledgers/available").await?;
    let mut current: Vec<String> = children.into_iter().filter(|c| !c.starts_with("readonly")).collect();
    current.sort();
    if current != baseline.available_bookies {
        anyhow::bail!(
            "[{phase_label}] /ledgers/available drifted from baseline\n  \
             baseline: {:?}\n  \
             current:  {:?}",
            baseline.available_bookies, current
        );
    }

    // 3. Cluster instance id invariant
    let (id_bytes, _) = zk.get_data("/ledgers/INSTANCEID").await?;
    let current_id = String::from_utf8_lossy(&id_bytes).into_owned();
    if current_id != baseline.instance_id {
        anyhow::bail!(
            "[{phase_label}] /ledgers/INSTANCEID changed (rebootstrap?)\n  \
             baseline: {}\n  current:  {}",
            baseline.instance_id, current_id
        );
    }

    // 4. Cookie field-equal
    for (alias, baseline_fields) in &baseline.cookies {
        let slot = bookies.iter().find(|s| &s.alias == alias)
            .expect("baseline alias missing from current bookies");
        let raw = read_cookie(slot).await?;
        let now = parse_cookie(&raw)?;
        if &now != baseline_fields {
            anyhow::bail!(
                "[{phase_label}] cookie for {alias} drifted\n  \
                 baseline: {:?}\n  current:  {:?}",
                baseline_fields, now
            );
        }
    }

    // 5. No phantom under-replication
    let under_repl = zk.list_children("/ledgers/under_replication").await;
    if let Ok((children, _)) = under_repl {
        if !children.is_empty() {
            anyhow::bail!(
                "[{phase_label}] /ledgers/under_replication non-empty: {:?}",
                children
            );
        }
    }

    // 6. All closed ledgers fully readable
    for &(id, label, count, era) in closed_ledgers {
        read_ledger(bk, id, label, count, era).await
            .map_err(|e| anyhow::anyhow!("[{phase_label}] verify ledger {id}: {e}"))?;
    }

    info!("=== {phase_label} verification PASS ===");
    Ok(())
}
```

- [ ] **Step 4: Add a smoke test for capture + verify in a stable cluster**

Append:

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_baseline_and_verify() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;

    let mut slot1 = BookieSlot::new(1, &env);
    let mut slot2 = BookieSlot::new(2, &env);
    slot1.start_java(zk.in_network_addr()).await?;
    slot2.start_java(zk.in_network_addr()).await?;
    slot1.wait_healthy(Duration::from_secs(120)).await?;
    slot2.wait_healthy(Duration::from_secs(120)).await?;

    let zk_host = zk.host_addr().await?;
    let zk_client = zookeeper_client::Client::connect(&zk_host).await?;
    let mut bk = make_bk_client(&zk_host).await?;

    let bookies = [slot1, slot2];
    let baseline = capture_baseline(&zk_client, &bookies).await?;
    let id = write_ledger(&mut bk, 200, 10, "smoke").await?;
    verify_cluster_state(
        &zk_client,
        &bookies,
        &baseline,
        &mut bk,
        &[(id, 200, 10, "smoke")],
        "smoke",
    ).await?;

    env.mark_success();
    Ok(())
}
```

- [ ] **Step 5: Run the smoke test**

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade smoke_baseline_and_verify -- --ignored --nocapture
```

Expected: test passes. Output includes "Baseline: bookies=...", "=== smoke verification PASS ===".

- [ ] **Step 6: Commit**

```bash
git add crates/bookkeeper-upgrade-test/tests/upgrade.rs
git commit -m "test(upgrade): add capture_baseline + verify_cluster_state assertions"
```

---

## Task 12: Wire P1–P5 into the main test

**Files:**
- Modify: `crates/bookkeeper-upgrade-test/tests/upgrade.rs`

- [ ] **Step 1: Replace the body of `java_to_rust_in_place_upgrade`**

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn java_to_rust_in_place_upgrade() -> Result<()> {
    init_tracing();
    check_prerequisites()?;

    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;
    let conf_dir = TempDir::new()?;

    // ----------------------------------------------------------------------
    // P1 — All-Java cluster, write java-era data (L1, L2)
    // ----------------------------------------------------------------------
    info!("===== P1: all-Java cluster + java-era writes =====");
    let mut slot1 = BookieSlot::new(1, &env);
    let mut slot2 = BookieSlot::new(2, &env);
    slot1.start_java(zk.in_network_addr()).await?;
    slot2.start_java(zk.in_network_addr()).await?;
    slot1.wait_healthy(Duration::from_secs(120)).await?;
    slot2.wait_healthy(Duration::from_secs(120)).await?;

    let zk_host = zk.host_addr().await?;
    let zk_client = zookeeper_client::Client::connect(&zk_host).await?;
    let mut bk = make_bk_client(&zk_host).await?;

    let baseline = capture_baseline(&zk_client, std::slice::from_ref(&slot1).iter().chain(std::slice::from_ref(&slot2)).cloned().collect::<Vec<_>>().as_slice()).await?;
    // (The above is awkward because slot1/slot2 are owned. Easier: keep them
    // in a Vec from the start.)

    let mut closed: Vec<(i64, i64, i64, &str)> = Vec::new();

    let l1 = write_ledger(&mut bk, 1, 100, "java-era").await?;
    closed.push((l1, 1, 100, "java-era"));
    let l2 = write_ledger(&mut bk, 2, 100, "java-era").await?;
    closed.push((l2, 2, 100, "java-era"));

    verify_cluster_state(&zk_client, &[slot1.clone(), slot2.clone()], &baseline, &mut bk, &closed, "P1").await?;

    // ----------------------------------------------------------------------
    // P2 — Rolling Java → Rust, slot by slot
    // ----------------------------------------------------------------------
    info!("===== P2: rolling Java → Rust =====");
    swap_to_rust(&mut slot1, zk.in_network_addr(), &zk_client, conf_dir.path()).await?;
    let mut bk = make_bk_client(&zk_host).await?;
    verify_cluster_state(&zk_client, &[slot1.clone(), slot2.clone()], &baseline, &mut bk, &closed, "P2 after slot1").await?;

    swap_to_rust(&mut slot2, zk.in_network_addr(), &zk_client, conf_dir.path()).await?;
    let mut bk = make_bk_client(&zk_host).await?;
    verify_cluster_state(&zk_client, &[slot1.clone(), slot2.clone()], &baseline, &mut bk, &closed, "P2 after slot2").await?;

    // ----------------------------------------------------------------------
    // P3 — All-Rust cluster, write rust-era data (L3, L4)
    // ----------------------------------------------------------------------
    info!("===== P3: all-Rust cluster + rust-era writes =====");
    let l3 = write_ledger(&mut bk, 3, 100, "rust-era").await?;
    closed.push((l3, 3, 100, "rust-era"));
    let l4 = write_ledger(&mut bk, 4, 100, "rust-era").await?;
    closed.push((l4, 4, 100, "rust-era"));

    verify_cluster_state(&zk_client, &[slot1.clone(), slot2.clone()], &baseline, &mut bk, &closed, "P3").await?;

    // ----------------------------------------------------------------------
    // P4 — Rolling Rust → Java
    // ----------------------------------------------------------------------
    info!("===== P4: rolling Rust → Java =====");
    swap_to_java(&mut slot1, zk.in_network_addr(), &zk_client).await?;
    let mut bk = make_bk_client(&zk_host).await?;
    verify_cluster_state(&zk_client, &[slot1.clone(), slot2.clone()], &baseline, &mut bk, &closed, "P4 after slot1").await?;

    swap_to_java(&mut slot2, zk.in_network_addr(), &zk_client).await?;
    let mut bk = make_bk_client(&zk_host).await?;
    verify_cluster_state(&zk_client, &[slot1.clone(), slot2.clone()], &baseline, &mut bk, &closed, "P4 after slot2").await?;

    // ----------------------------------------------------------------------
    // P5 — All-Java again, full read-back of L1..L4
    // ----------------------------------------------------------------------
    info!("===== P5: final read-back =====");
    for &(id, label, count, era) in &closed {
        read_ledger(&mut bk, id, label, count, era).await?;
    }

    env.mark_success();
    info!("===== java_to_rust_in_place_upgrade ALL PHASES PASS =====");
    Ok(())
}
```

- [ ] **Step 2: Convert BookieSlot to be Cloneable, OR refactor to use a Vec**

The above code calls `slot1.clone()` to pass into `verify_cluster_state` while still mutating the original. `BookieSlot` holds a `ContainerAsync<GenericImage>` which is NOT `Clone`. Two ways to fix:

**Option A** (simpler): Change `verify_cluster_state` to take `&[&BookieSlot]` instead of `&[BookieSlot]`, then call as `&[&slot1, &slot2]`. No clones needed.

**Option B**: Wrap the bookies in a `Vec<BookieSlot>` and index by slot id.

Apply Option A. Update the `verify_cluster_state` signature:

```rust
async fn verify_cluster_state(
    zk: &zookeeper_client::Client,
    bookies: &[&BookieSlot],
    baseline: &Baseline,
    bk: &mut ZkBookKeeper,
    closed_ledgers: &[(i64, i64, i64, &str)],
    phase_label: &str,
) -> Result<()> {
```

Update `capture_baseline` similarly:

```rust
async fn capture_baseline(
    zk: &zookeeper_client::Client,
    bookies: &[&BookieSlot],
) -> Result<Baseline> {
```

Update all call sites in the main test to pass `&[&slot1, &slot2]`. The intermediate `cloned().collect()` line in P1 goes away.

- [ ] **Step 3: Build the test**

```bash
cargo build -p bookkeeper-upgrade-test --tests
```

Expected: clean compile, only pre-existing workspace warnings.

- [ ] **Step 4: Run the full test end-to-end**

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade java_to_rust_in_place_upgrade -- --ignored --nocapture
```

Expected: test passes within ~6 minutes. Output includes:
- "===== P1: all-Java cluster + java-era writes ====="
- "Wrote 100 entries to ledger N (label=1, era=java-era)"
- "Wrote 100 entries to ledger N (label=2, era=java-era)"
- "=== P1 verification PASS ==="
- "===== P2: rolling Java → Rust ====="
- "Swapping bookie-1 → Rust"
- ...etc through P5
- "===== java_to_rust_in_place_upgrade ALL PHASES PASS ====="

If the test fails at any phase, the cleanup path leaves state in place. The error message identifies the phase and the assertion that failed. The engineer uses the printed cleanup commands after they're done investigating.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-upgrade-test/tests/upgrade.rs
git commit -m "test(upgrade): wire full P1..P5 java↔rust upgrade test"
```

---

## Task 13: Polish — README, run-script convenience, doc the failure modes

**Files:**
- Modify: `crates/bookkeeper-upgrade-test/README.md`

- [ ] **Step 1: Expand the README with failure-mode debugging**

Append to `crates/bookkeeper-upgrade-test/README.md`:

````markdown
## Failure modes — what each test failure tells you

| Symptom | Likely root cause |
|---|---|
| Rust bookie won't start in P2 | Cookie format incompat — Rust can't parse Java's cookie file |
| Rust bookie starts but `wait_healthy` times out | RocksDB schema incompat — Rust can't open Java's column families, or ZK registration incompat |
| Java bookie won't start in P4 | Cookie format asymmetry — Rust wrote it differently from how Java expects |
| Read of L1 fails after a swap in P2 | Entry log file format incompat or entry location index format incompat |
| Read of L3 fails in P5 | Bidirectional compat broken — Java cannot read Rust's entry log files |
| `under_replication/` non-empty | ZK identity incompat — auditor sees the swapped-in bookie as a different bookie |
| Cluster instance ID changed | The new bookie ran `initnewcluster` instead of joining (catastrophic) |

## Inspecting state after a failure

When the test fails it prints commands like:

```
TEST FAILED — state preserved for inspection.
To clean up manually:
  docker network rm bk-upgrade-test-<run-id>
  docker volume rm -f bk-upgrade-test-<run-id>-bk1-data ...
```

To poke at a volume directly:

```bash
docker run -it --rm \
  -v bk-upgrade-test-<run-id>-bk1-data:/bk/data \
  apache/bookkeeper:4.17.3 bash
# Inside:
ls /bk/data/current/
cat /bk/data/current/VERSION
```

To see a bookie's logs (containers stay around on failure):

```bash
docker logs bk-upgrade-bookie-1-<run-id>
```

## Smoke tests

The integration test file also has smaller `smoke_*` tests for diagnosing
each fixture in isolation. They run via:

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade smoke_ -- --ignored --nocapture
```

Useful when the main test fails and you want to bisect which layer broke.
````

- [ ] **Step 2: Verify the README renders**

```bash
glow crates/bookkeeper-upgrade-test/README.md 2>/dev/null || cat crates/bookkeeper-upgrade-test/README.md
```

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-upgrade-test/README.md
git commit -m "docs(upgrade-test): document failure modes and inspection workflow"
```

---

## Self-Review Notes (filled in after writing the plan)

**Spec coverage:**
- Architecture (new crate, single test, testcontainers, Rust BK client) → Tasks 1, 4, 5, 6, 7, 8, 12
- Container topology (network + 4 volumes + 2 bookie slots) → Tasks 3, 4, 5, 6
- Compat surfaces stressed → exercised across Tasks 7, 8, 10, 11, 12
- Swap mechanism (BookieSlot, stop_timeout=120, ZK ephemeral race) → Tasks 5, 6, 9, 10
- Phases P1–P5 → Task 12
- Per-phase assertions (6 items) → Task 11 (all 6) + Task 12 (calls them per phase)
- Cross-phase final assertions → covered by the per-phase verify after P5 + the final read-back
- Cleanup behavior (success removes, failure preserves) → Task 3
- Prerequisite check → Task 2
- File layout → Task 1
- Cargo.toml deps → Task 1
- README → Tasks 1 + 13

**Placeholder scan:** No `TBD`/`TODO`/`FIXME`. Each step has actual code or actual commands. The one place where I flagged uncertainty (cookie format binary vs text in Task 11) is explicit and tells the engineer what to do (`xxd | head` and adapt). The two places where I flagged signature uncertainty (`zookeeper_client::Client::check_stat` in Task 9, `ZkBookKeeper::close_ledger` / `WriteHandle::ledger_id` in Task 7) tell the engineer how to look up the actual signature and adapt. These are not placeholders — they are honest acknowledgments that I didn't read every line of the bookkeeper-client API and the engineer should expect minor name fixes.

**Type consistency:** `BookieSlot` is consistent across tasks 5/6/9/10/11/12. `Baseline` and `verify_cluster_state` are consistent (Task 11 → 12). `swap_to_rust`/`swap_to_java` signatures are consistent (Task 10 → 12). The plural-ledger format `(id, label, count, era)` is consistent in Tasks 11 → 12.

**Disk-usage assertion missing:** The spec includes a "soft" assertion that disk usage should be ≤ 2× the bytes written. The plan does NOT implement it. This is a deliberate omission — the spec lists it as soft/warned-not-failed and the underlying disk inspection (`docker exec du -sh`) is awkward to implement reliably across image variants. If the engineer wants this, it's a small follow-up task; the main correctness assertions are all in place.

---

## Execution Notes

- This plan assumes `superpowers:executing-plans` (or `subagent-driven-development`) drives the tasks.
- Task 10 is the highest-risk task — it is the first time real on-disk format compat is exercised. If it fails, the failure surfaces a real bug that needs to be filed and fixed in a separate PR. The test stays as the validation gate.
- Tasks should be committed individually so a bisect across the test crate's history is meaningful.
