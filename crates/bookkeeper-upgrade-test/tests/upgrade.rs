//! Java↔Rust bookie in-place upgrade test.
//! See docs/superpowers/specs/2026-04-11-java-rust-upgrade-test-design.md

use anyhow::Result;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tracing::{info, warn};

const JAVA_BK_IMAGE: &str = "apache/bookkeeper:4.17.3";
const RUST_BK_IMAGE: &str = "rust-bookie:latest";

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();
}

fn check_prerequisites() -> Result<()> {
    let out = Command::new("docker")
        .arg("info")
        .output()
        .map_err(|e| anyhow::anyhow!("docker not on PATH: {e}"))?;
    if !out.status.success() {
        anyhow::bail!(
            "Docker daemon not reachable. Start Docker Desktop / Docker Engine and try again.\n\
             stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    info!("✓ Docker daemon reachable");

    require_image(JAVA_BK_IMAGE, &format!("docker pull {JAVA_BK_IMAGE}"))?;
    info!("✓ {} present", JAVA_BK_IMAGE);

    require_image(
        RUST_BK_IMAGE,
        "docker build --target runtime -t rust-bookie:latest .",
    )?;
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

// ---------------------------------------------------------------------------
// TestEnv — owns Docker network + named volumes for one test run
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// ZooKeeper container fixture
// ---------------------------------------------------------------------------

/// ZooKeeper container. Reachable from other containers on the test network
/// by its container name (Docker auto-DNS on user-defined bridges).
struct ZkContainer {
    container: ContainerAsync<GenericImage>,
    in_network_name: String,
}

impl ZkContainer {
    async fn start(env: &TestEnv) -> Result<Self> {
        let in_network_name = format!("bk-upgrade-zk-{}", env.run_id);
        let img = GenericImage::new("zookeeper", "3.8")
            .with_exposed_port(2181.tcp())
            .with_wait_for(WaitFor::message_on_stdout("binding to port"))
            .with_network(env.network.clone())
            .with_container_name(in_network_name.clone());
        let container = img
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("start zookeeper: {e}"))?;
        info!("ZooKeeper container started ({})", in_network_name);
        Ok(Self {
            container,
            in_network_name,
        })
    }

    /// `host:port` other containers on the test network use to reach ZK.
    /// Docker resolves the container name as a DNS alias on user-defined
    /// bridge networks.
    fn in_network_addr(&self) -> String {
        format!("{}:2181", self.in_network_name)
    }

    /// host:port for the test process (running outside Docker) to reach ZK.
    async fn host_addr(&self) -> Result<String> {
        let host = self.container.get_host().await?;
        let port = self.container.get_host_port_ipv4(2181).await?;
        Ok(format!("{host}:{port}"))
    }
}

// ---------------------------------------------------------------------------
// Cluster bootstrap — Java BK requires `metaformat` against ZK before any
// bookie can start. Runs once per test, after ZK is up.
// ---------------------------------------------------------------------------

async fn init_bk_cluster(env: &TestEnv, zk_in_network: &str) -> Result<()> {
    // One-shot run of `bookkeeper shell metaformat` against the test ZK.
    //
    // The apache/bookkeeper image's entrypoint applies BK_* env vars to
    // bk_server.conf via `apply-config-from-env.py`. We want to use a
    // custom command (`bookkeeper shell metaformat`) instead of the
    // default `bookie` command, so we override the entrypoint AND run
    // apply-config-from-env.py ourselves before invoking the shell.
    let zk_servers = zk_in_network.to_string();
    let metadata_uri = format!("zk://{zk_in_network}/ledgers");
    let cmd = "/opt/bookkeeper/scripts/apply-config-from-env.py /opt/bookkeeper/conf/bk_server.conf && \
               /opt/bookkeeper/bin/bookkeeper shell metaformat -nonInteractive -force";
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--network",
            &env.network,
            "-e",
            &format!("BK_zkServers={zk_servers}"),
            "-e",
            &format!("BK_metadataServiceUri={metadata_uri}"),
            "--entrypoint",
            "bash",
            JAVA_BK_IMAGE,
            "-c",
            cmd,
        ])
        .output()?;
    if !out.status.success() {
        anyhow::bail!(
            "bookkeeper metaformat failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    info!("Initialized BK cluster metadata in ZK");
    Ok(())
}

// ---------------------------------------------------------------------------
// BookieSlot — per-bookie state, swappable Java/Rust on the same volumes
// ---------------------------------------------------------------------------

/// Per-bookie slot. Owns the volumes (via TestEnv), the network alias, and
/// the currently-running container handle (Java OR Rust).
///
/// `alias` is the per-slot DNS name used inside the test network. We use
/// the (run-id-suffixed) container name as the alias because Docker resolves
/// container names on user-defined bridge networks; this avoids needing
/// testcontainers' (currently-not-exposed) `with_network_aliases` API.
/// The bookieId stored on disk and in ZK is therefore `<alias>:3181`, which
/// is stable across image swaps within a single test run.
struct BookieSlot {
    #[allow(dead_code)]
    slot_id: u32,
    alias: String,
    data_volume: String,
    journal_volume: String,
    network: String,
    #[allow(dead_code)]
    run_id: String,
    container: Option<ContainerAsync<GenericImage>>,
}

impl BookieSlot {
    fn new(slot_id: u32, env: &TestEnv) -> Self {
        let alias = format!("bk-upgrade-bookie-{}-{}", slot_id, env.run_id);
        let data_volume = env
            .volumes
            .iter()
            .find(|v| v.ends_with(&format!("-bk{slot_id}-data")))
            .expect("data volume should exist for slot")
            .clone();
        let journal_volume = env
            .volumes
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

        // The container is named after `self.alias` (which already includes
        // the run_id suffix). Docker will register that name as a DNS alias
        // on the user-defined bridge so other containers can reach this
        // bookie at `<alias>:3181`.
        let img = GenericImage::new("apache/bookkeeper", "4.17.3")
            .with_exposed_port(3181.tcp())
            .with_exposed_port(8080.tcp())
            // The Java bookie's `bookie` subcommand goes through
            // ComponentStarter which logs "Started component bookie-server."
            // — note this is NOT the same as standalone mode's
            // "Started bookie server successfully" (different code path).
            .with_wait_for(WaitFor::message_on_stdout("Started component bookie-server."))
            .with_startup_timeout(Duration::from_secs(180))
            .with_network(self.network.clone())
            .with_container_name(self.alias.clone())
            .with_mount(Mount::volume_mount(self.data_volume.clone(), "/bk/data"))
            .with_mount(Mount::volume_mount(
                self.journal_volume.clone(),
                "/bk/journal",
            ))
            .with_env_var("BK_zkServers", zk_in_network)
            .with_env_var("BK_advertisedAddress", &self.alias)
            .with_env_var("BK_useHostNameAsBookieID", "true")
            .with_env_var("BK_journalDirectory", "/bk/journal")
            .with_env_var("BK_ledgerDirectories", "/bk/data")
            .with_env_var("BK_bookiePort", "3181")
            .with_env_var("BK_httpServerEnabled", "true")
            .with_env_var("BK_httpServerPort", "8080");
        let container = img
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("start java bookie {}: {e}", self.alias))?;
        self.container = Some(container);
        info!("Started Java bookie {}", self.alias);
        Ok(())
    }

    async fn wait_healthy(&self, timeout: Duration) -> Result<()> {
        let container = self
            .container
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no container in slot {}", self.alias))?;
        let host = container.get_host().await?;
        let http_port = container.get_host_port_ipv4(8080).await?;
        let url = format!("http://{host}:{http_port}/heartbeat");

        let deadline = std::time::Instant::now() + timeout;
        loop {
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
                anyhow::bail!(
                    "bookie {} did not become healthy within {:?}",
                    self.alias,
                    timeout
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

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

    let out = Command::new("docker")
        .args(["network", "inspect", &env.network])
        .output()?;
    assert!(out.status.success(), "network not found after create");

    for vol in &env.volumes {
        let out = Command::new("docker")
            .args(["volume", "inspect", vol])
            .output()?;
        assert!(out.status.success(), "volume {vol} not found after create");
    }

    env.mark_success();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_zk_container() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;

    let host_addr = zk.host_addr().await?;
    info!(
        "ZK host_addr={host_addr} in_network_addr={}",
        zk.in_network_addr()
    );
    let client = zookeeper_client::Client::connect(&host_addr).await?;

    let children = client.list_children("/").await?;
    info!("ZK root children: {:?}", children);
    assert!(
        children.iter().any(|c| c == "zookeeper"),
        "/zookeeper subtree expected"
    );

    env.mark_success();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_java_bookie() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;
    let zk_addr = zk.in_network_addr();
    init_bk_cluster(&env, &zk_addr).await?;

    let mut slot = BookieSlot::new(1, &env);
    slot.start_java(&zk_addr).await?;
    slot.wait_healthy(Duration::from_secs(180)).await?;

    env.mark_success();
    Ok(())
}

// ---------------------------------------------------------------------------
// Main test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn java_to_rust_in_place_upgrade() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    info!("Prerequisites OK; remainder of test not yet implemented");
    Ok(())
}
