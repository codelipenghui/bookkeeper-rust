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
        let container = img
            .start()
            .await
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
    info!("ZK host_addr: {host_addr}");
    let client = zookeeper_client::Client::connect(&host_addr).await?;

    let children = client.list_children("/").await?;
    info!("ZK root children: {:?}", children);
    assert!(
        children.iter().any(|c| c == "zookeeper"),
        "/zookeeper subtree expected"
    );

    let _ = Duration::from_secs(0); // touch the import so it doesn't go unused yet
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
