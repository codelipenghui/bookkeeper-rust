//! Java↔Rust bookie in-place upgrade test.
//! See docs/superpowers/specs/2026-04-11-java-rust-upgrade-test-design.md

use anyhow::Result;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tempfile::TempDir;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tracing::{info, warn};

const JAVA_BK_IMAGE: &str = "apache/bookkeeper:4.17.3";
const RUST_BK_IMAGE: &str = "rust-bookie:latest";
const HELPER_IMAGE: &str = "bk-upgrade-helper:latest";

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

    require_image(
        HELPER_IMAGE,
        "docker build -t bk-upgrade-helper:latest crates/bookkeeper-upgrade-test/helper",
    )?;
    info!("✓ {} present", HELPER_IMAGE);

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
// Per-slot bk_server.conf rendering for the Rust bookie image
// ---------------------------------------------------------------------------

/// Render a per-slot bk_server.conf into `dir` and return its host path.
/// The Rust bookie image reads /etc/bookkeeper/bk_server.conf at startup.
fn render_rust_conf(
    dir: &std::path::Path,
    alias: &str,
    zk_in_network: &str,
) -> Result<std::path::PathBuf> {
    let conf = format!(
        "bookiePort=3181\n\
         httpServerEnabled=true\n\
         httpServerPort=8080\n\
         ledgerDirectories=/bk/data\n\
         journalDirectory=/bk/journal\n\
         zkServers={zk}\n\
         metadataServiceUri=zk://{zk}/ledgers\n\
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
        //
        // Both Java and Rust bookies run as root (UID 0) in the test so
        // that swapping between them doesn't hit UID-mismatch errors on
        // the shared volumes. Java's image defaults to UID 1000 and the
        // Rust image to a system user, which would prevent reading each
        // other's data. This is a test-only override.
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
            .with_user("0:0")
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

    async fn start_rust(&mut self, zk_in_network: &str, conf_dir: &std::path::Path) -> Result<()> {
        use testcontainers::core::Mount;

        let conf_path = render_rust_conf(conf_dir, &self.alias, zk_in_network)?;
        let conf_path_str = conf_path.to_string_lossy().to_string();

        let img = GenericImage::new("rust-bookie", "latest")
            .with_exposed_port(3181.tcp())
            .with_exposed_port(8080.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Bookie started at"))
            .with_startup_timeout(Duration::from_secs(120))
            .with_network(self.network.clone())
            .with_user("0:0")
            .with_container_name(self.alias.clone())
            .with_mount(Mount::volume_mount(self.data_volume.clone(), "/bk/data"))
            .with_mount(Mount::volume_mount(
                self.journal_volume.clone(),
                "/bk/journal",
            ))
            .with_mount(Mount::bind_mount(
                conf_path_str,
                "/etc/bookkeeper/bk_server.conf",
            ));
        let container = img
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("start rust bookie {}: {e}", self.alias))?;
        self.container = Some(container);
        info!("Started Rust bookie {}", self.alias);
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(container) = self.container.take() {
            // testcontainers' default stop_timeout is 10s; a bookie's
            // checkpoint() can take longer when write_cache is full.
            // Give it 120s so a clean flush always completes. (See
            // commit bc4e7922 on the bookie side for why this matters.)
            container
                .stop_with_timeout(Some(120))
                .await
                .map_err(|e| anyhow::anyhow!("stop bookie {}: {e}", self.alias))?;
            // Drop the container handle to release testcontainers resources.
            drop(container);
            info!("Stopped bookie {}", self.alias);
        }
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
// ZK helpers
// ---------------------------------------------------------------------------

/// After stopping a bookie, ZK doesn't immediately remove its ephemeral
/// registration node — it has to wait for the session timeout. If the new
/// bookie tries to register before that, ZK returns NODE_EXISTS. Poll the
/// registration path until it disappears, or until 30s.
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

// ---------------------------------------------------------------------------
// Verification — capture a baseline at the start of the test, re-check
// against it after every phase to surface drift.
// ---------------------------------------------------------------------------

/// The cookie fields we compare across swaps. Serialization details may
/// differ slightly between Java and Rust so we compare parsed fields, not
/// raw bytes. Note: Java BK's current VERSION file is a text format with
/// key/value lines prefixed by a layout-version number on line 1.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CookieFields {
    bookie_host: String,
    journal_dir: String,
    ledger_dirs: String,
    instance_id: Option<String>,
    layout_version: String,
}

fn parse_cookie(bytes: &[u8]) -> Result<CookieFields> {
    let text = std::str::from_utf8(bytes).map_err(|e| anyhow::anyhow!("cookie not utf8: {e}"))?;
    let mut lines = text.lines();
    let layout_version = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty cookie"))?
        .trim()
        .to_string();
    let mut bookie_host = None;
    let mut journal_dir = None;
    let mut ledger_dirs = None;
    let mut instance_id = None;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim();
            let mut v = v.trim();
            // Java's cookie file wraps string values in double-quotes.
            if v.starts_with('"') && v.ends_with('"') && v.len() >= 2 {
                v = &v[1..v.len() - 1];
            }
            let v = v.to_string();
            match k {
                "bookieHost" => bookie_host = Some(v),
                "journalDir" => journal_dir = Some(v),
                "ledgerDirs" => ledger_dirs = Some(v),
                "instanceId" => instance_id = Some(v),
                _ => {}
            }
        }
    }
    Ok(CookieFields {
        bookie_host: bookie_host.ok_or_else(|| anyhow::anyhow!("cookie missing bookieHost"))?,
        journal_dir: journal_dir.ok_or_else(|| anyhow::anyhow!("cookie missing journalDir"))?,
        ledger_dirs: ledger_dirs.ok_or_else(|| anyhow::anyhow!("cookie missing ledgerDirs"))?,
        instance_id,
        layout_version,
    })
}

/// Read the cookie file from a running bookie via `docker exec`. The
/// apache/bookkeeper image has a different data path layout than the rust
/// image — both drop the cookie at `/bk/data/current/VERSION` because
/// that's where our configured `ledgerDirectories=/bk/data` puts it.
fn read_cookie(slot: &BookieSlot) -> Result<Vec<u8>> {
    let out = Command::new("docker")
        .args([
            "exec",
            &slot.alias,
            "cat",
            "/bk/data/current/VERSION",
        ])
        .output()?;
    if !out.status.success() {
        anyhow::bail!(
            "docker exec cat VERSION on {} failed: {}",
            slot.alias,
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(out.stdout)
}

/// Snapshot taken at the start of P1 and re-checked after every phase.
#[derive(Debug, Clone)]
struct Baseline {
    available_bookies: Vec<String>,
    instance_id: String,
    cookies: Vec<(String, CookieFields)>, // (alias, fields)
}

async fn capture_baseline(
    zk: &zookeeper_client::Client,
    bookies: &[&BookieSlot],
) -> Result<Baseline> {
    let mut available_bookies: Vec<String> = zk
        .list_children("/ledgers/available")
        .await?
        .into_iter()
        .filter(|c| !c.starts_with("readonly"))
        .collect();
    available_bookies.sort();

    let (id_bytes, _) = zk.get_data("/ledgers/INSTANCEID").await?;
    let instance_id = String::from_utf8_lossy(&id_bytes).into_owned();

    let mut cookies = Vec::new();
    for slot in bookies {
        let raw = read_cookie(slot)?;
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

/// Run the per-phase assertion bundle. `closed_ledgers` is the list of
/// `(id, label, count, era)` tuples for ledgers that have been CLOSED so
/// far in the test; we re-read every entry of every closed ledger as part
/// of the assertion.
#[allow(clippy::too_many_arguments)]
async fn verify_cluster_state(
    env: &TestEnv,
    zk_in_network: &str,
    zk: &zookeeper_client::Client,
    bookies: &[&BookieSlot],
    baseline: &Baseline,
    closed_ledgers: &[(i64, i64, i64, &str)],
    phase_label: &str,
) -> Result<()> {
    info!("=== Verifying cluster state after {phase_label} ===");

    // 1. Both bookies healthy
    for slot in bookies {
        slot.wait_healthy(Duration::from_secs(30)).await.map_err(|e| {
            anyhow::anyhow!("[{phase_label}] bookie {} not healthy: {e}", slot.alias)
        })?;
    }

    // 2. Identity continuity — same set of bookieIds in /ledgers/available
    let mut current: Vec<String> = zk
        .list_children("/ledgers/available")
        .await?
        .into_iter()
        .filter(|c| !c.starts_with("readonly"))
        .collect();
    current.sort();
    if current != baseline.available_bookies {
        anyhow::bail!(
            "[{phase_label}] /ledgers/available drifted from baseline\n  \
             baseline: {:?}\n  current:  {:?}",
            baseline.available_bookies,
            current
        );
    }

    // 3. Cluster instance id invariant
    let (id_bytes, _) = zk.get_data("/ledgers/INSTANCEID").await?;
    let current_id = String::from_utf8_lossy(&id_bytes).into_owned();
    if current_id != baseline.instance_id {
        anyhow::bail!(
            "[{phase_label}] /ledgers/INSTANCEID changed (rebootstrap?)\n  \
             baseline: {}\n  current:  {}",
            baseline.instance_id,
            current_id
        );
    }

    // 4. Cookie field-equal
    for (alias, baseline_fields) in &baseline.cookies {
        let slot = bookies
            .iter()
            .find(|s| &s.alias == alias)
            .ok_or_else(|| {
                anyhow::anyhow!("[{phase_label}] baseline alias {alias} not in current bookies")
            })?;
        let raw = read_cookie(slot)?;
        let now = parse_cookie(&raw)?;
        if &now != baseline_fields {
            anyhow::bail!(
                "[{phase_label}] cookie for {alias} drifted\n  \
                 baseline: {:?}\n  current:  {:?}",
                baseline_fields,
                now
            );
        }
    }

    // 5. No phantom under-replication
    if let Ok(children) = zk.list_children("/ledgers/underreplication/ledgers").await {
        if !children.is_empty() {
            anyhow::bail!(
                "[{phase_label}] /ledgers/underreplication/ledgers non-empty: {:?}",
                children
            );
        }
    }

    // 6. All closed ledgers fully readable (via the sidecar workload).
    for &(id, label, count, era) in closed_ledgers {
        read_ledger(env, zk_in_network, id, label, count, era)
            .map_err(|e| anyhow::anyhow!("[{phase_label}] verify ledger {id}: {e}"))?;
    }

    info!("=== {phase_label} verification PASS ===");
    Ok(())
}

// ---------------------------------------------------------------------------
// Swap orchestration — stop current bookie, wait for ZK cleanup, start
// the new image on the same volumes + alias.
// ---------------------------------------------------------------------------

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
    slot.wait_healthy(Duration::from_secs(180)).await?;
    info!("Swap to Java complete for {}", slot.alias);
    Ok(())
}

// ---------------------------------------------------------------------------
// Workload helpers — drive the Java BK client via the sidecar image
// ---------------------------------------------------------------------------

/// Deterministic payload for a given (label, entry, era). Must match the
/// Java helper's `payloadFor`.
#[allow(dead_code)]
fn payload_for(label: i64, entry_id: i64, era: &str) -> String {
    format!("ledger_label={label} entry={entry_id} era={era}")
}

/// Run the Java BkUpgradeHelper in a one-shot sidecar container on the test
/// network. Returns the combined (stdout, stderr) on success. Bails on
/// non-zero exit with the helper's stderr in the error message.
fn run_helper(env: &TestEnv, args: &[&str]) -> Result<(String, String)> {
    let mut cmd = Command::new("docker");
    cmd.args(["run", "--rm", "--network", &env.network, HELPER_IMAGE]);
    cmd.args(args);
    let out = cmd.output()?;
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    if !out.status.success() {
        anyhow::bail!(
            "helper failed (args {:?})\nstdout: {}\nstderr: {}",
            args,
            stdout,
            stderr
        );
    }
    Ok((stdout, stderr))
}

/// Parse a line like "ledger_id=123" out of the helper stdout.
fn parse_kv(out: &str, key: &str) -> Result<String> {
    let prefix = format!("{key}=");
    for line in out.lines() {
        if let Some(rest) = line.trim().strip_prefix(&prefix) {
            return Ok(rest.to_string());
        }
    }
    anyhow::bail!("helper stdout did not contain `{key}=<value>`:\n{out}")
}

/// Create a ledger, write `n_entries` deterministic entries (label=era in
/// each payload), close it, and return the actual ledger id assigned by ZK.
fn write_ledger(
    env: &TestEnv,
    zk_in_network: &str,
    label: i64,
    n_entries: i64,
    era: &str,
) -> Result<i64> {
    let label_s = label.to_string();
    let n_s = n_entries.to_string();
    let (stdout, _) = run_helper(
        env,
        &[
            "write",
            "--zk",
            zk_in_network,
            "--label",
            &label_s,
            "--entries",
            &n_s,
            "--era",
            era,
        ],
    )?;
    let id: i64 = parse_kv(&stdout, "ledger_id")?.parse()?;
    info!("Wrote {n_entries} entries to ledger {id} (label={label}, era={era})");
    Ok(id)
}

/// Open a ledger, read every entry, verify each payload byte-for-byte
/// against the deterministic format string. Bails on any mismatch.
#[allow(dead_code)]
fn read_ledger(
    env: &TestEnv,
    zk_in_network: &str,
    ledger_id: i64,
    label: i64,
    n_entries: i64,
    era: &str,
) -> Result<i64> {
    let id_s = ledger_id.to_string();
    let label_s = label.to_string();
    let n_s = n_entries.to_string();
    let (stdout, _) = run_helper(
        env,
        &[
            "read",
            "--zk",
            zk_in_network,
            "--ledger-id",
            &id_s,
            "--label",
            &label_s,
            "--entries",
            &n_s,
            "--era",
            era,
        ],
    )?;
    let count: i64 = parse_kv(&stdout, "verified")?.parse()?;
    info!("Read-verified {count} entries from ledger {ledger_id}");
    Ok(count)
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
async fn smoke_stop_and_zk_clear() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;
    let zk_in_net = zk.in_network_addr();
    init_bk_cluster(&env, &zk_in_net).await?;

    let mut slot = BookieSlot::new(1, &env);
    slot.start_java(&zk_in_net).await?;
    slot.wait_healthy(Duration::from_secs(180)).await?;

    // Connect the test's ZK client (host-side) for the ephemeral check.
    let zk_host = zk.host_addr().await?;
    let zk_client = zookeeper_client::Client::connect(&zk_host).await?;

    // Sanity: bookie is registered before stop.
    let path = format!("/ledgers/available/{}:3181", slot.alias);
    let stat = zk_client.check_stat(&path).await?;
    assert!(stat.is_some(), "expected {path} registered before stop");

    // Stop and wait for the ephemeral to disappear.
    slot.stop().await?;
    wait_for_zk_ephemeral_to_clear(&zk_client, &slot.alias).await?;

    env.mark_success();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_write_ledger() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;
    let zk_in_net = zk.in_network_addr();
    init_bk_cluster(&env, &zk_in_net).await?;

    let mut slot1 = BookieSlot::new(1, &env);
    let mut slot2 = BookieSlot::new(2, &env);
    slot1.start_java(&zk_in_net).await?;
    slot2.start_java(&zk_in_net).await?;
    slot1.wait_healthy(Duration::from_secs(180)).await?;
    slot2.wait_healthy(Duration::from_secs(180)).await?;

    // Sidecar-driven write + read. The helper container joins the test
    // network so `bk-upgrade-bookie-N-<run-id>:3181` resolves to the right
    // bookie.
    let id = write_ledger(&env, &zk_in_net, 100, 5, "smoke")?;
    read_ledger(&env, &zk_in_net, id, 100, 5, "smoke")?;

    env.mark_success();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_swap_one_bookie() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;
    let zk_in_net = zk.in_network_addr();
    init_bk_cluster(&env, &zk_in_net).await?;
    let conf_dir = TempDir::new()?;

    let mut slot1 = BookieSlot::new(1, &env);
    let mut slot2 = BookieSlot::new(2, &env);
    slot1.start_java(&zk_in_net).await?;
    slot2.start_java(&zk_in_net).await?;
    slot1.wait_healthy(Duration::from_secs(180)).await?;
    slot2.wait_healthy(Duration::from_secs(180)).await?;

    // Write a ledger via the all-Java cluster.
    let id = write_ledger(&env, &zk_in_net, 100, 20, "java-era")?;

    // Swap bookie-1 from Java to Rust on the same volumes. The Rust bookie
    // has to pick up Java's cookie + RocksDB schemas + entry log format.
    let zk_host = zk.host_addr().await?;
    let zk_client = zookeeper_client::Client::connect(&zk_host).await?;
    swap_to_rust(&mut slot1, &zk_in_net, &zk_client, conf_dir.path()).await?;

    // Read back the Java-written ledger. The Rust bookie is one of two
    // bookies in the ensemble, so read responses may come from either.
    // A successful round-trip through the Rust bookie's on-disk path is
    // the real compat test.
    read_ledger(&env, &zk_in_net, id, 100, 20, "java-era")?;

    env.mark_success();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn smoke_rust_bookie() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;
    let zk_addr = zk.in_network_addr();
    init_bk_cluster(&env, &zk_addr).await?;

    let conf_dir = TempDir::new()?;
    let mut slot = BookieSlot::new(1, &env);
    slot.start_rust(&zk_addr, conf_dir.path()).await?;
    slot.wait_healthy(Duration::from_secs(60)).await?;

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

    let mut env = TestEnv::new()?;
    let zk = ZkContainer::start(&env).await?;
    let zk_in_net = zk.in_network_addr();
    init_bk_cluster(&env, &zk_in_net).await?;
    let conf_dir = TempDir::new()?;

    // ----------------------------------------------------------------------
    // P1 — All-Java cluster + java-era writes
    // ----------------------------------------------------------------------
    info!("===== P1: all-Java cluster + java-era writes =====");
    let mut slot1 = BookieSlot::new(1, &env);
    let mut slot2 = BookieSlot::new(2, &env);
    slot1.start_java(&zk_in_net).await?;
    slot2.start_java(&zk_in_net).await?;
    slot1.wait_healthy(Duration::from_secs(180)).await?;
    slot2.wait_healthy(Duration::from_secs(180)).await?;

    let zk_host = zk.host_addr().await?;
    let zk_client = zookeeper_client::Client::connect(&zk_host).await?;

    let baseline = capture_baseline(&zk_client, &[&slot1, &slot2]).await?;

    let l1 = write_ledger(&env, &zk_in_net, 1, 100, "java-era")?;
    let l2 = write_ledger(&env, &zk_in_net, 2, 100, "java-era")?;
    let mut closed: Vec<(i64, i64, i64, &str)> = vec![
        (l1, 1, 100, "java-era"),
        (l2, 2, 100, "java-era"),
    ];
    verify_cluster_state(
        &env, &zk_in_net, &zk_client, &[&slot1, &slot2], &baseline, &closed, "P1",
    ).await?;

    // ----------------------------------------------------------------------
    // P2 — Rolling Java → Rust, slot by slot
    // ----------------------------------------------------------------------
    info!("===== P2: rolling Java → Rust =====");
    swap_to_rust(&mut slot1, &zk_in_net, &zk_client, conf_dir.path()).await?;
    verify_cluster_state(
        &env, &zk_in_net, &zk_client, &[&slot1, &slot2], &baseline, &closed,
        "P2 after slot1",
    ).await?;

    swap_to_rust(&mut slot2, &zk_in_net, &zk_client, conf_dir.path()).await?;
    verify_cluster_state(
        &env, &zk_in_net, &zk_client, &[&slot1, &slot2], &baseline, &closed,
        "P2 after slot2",
    ).await?;

    // ----------------------------------------------------------------------
    // P3 — All-Rust cluster + rust-era writes
    // ----------------------------------------------------------------------
    info!("===== P3: all-Rust cluster + rust-era writes =====");
    let l3 = write_ledger(&env, &zk_in_net, 3, 100, "rust-era")?;
    let l4 = write_ledger(&env, &zk_in_net, 4, 100, "rust-era")?;
    closed.push((l3, 3, 100, "rust-era"));
    closed.push((l4, 4, 100, "rust-era"));
    verify_cluster_state(
        &env, &zk_in_net, &zk_client, &[&slot1, &slot2], &baseline, &closed, "P3",
    ).await?;

    // ----------------------------------------------------------------------
    // P4 — Rolling Rust → Java
    // ----------------------------------------------------------------------
    info!("===== P4: rolling Rust → Java =====");
    swap_to_java(&mut slot1, &zk_in_net, &zk_client).await?;
    verify_cluster_state(
        &env, &zk_in_net, &zk_client, &[&slot1, &slot2], &baseline, &closed,
        "P4 after slot1",
    ).await?;

    swap_to_java(&mut slot2, &zk_in_net, &zk_client).await?;
    verify_cluster_state(
        &env, &zk_in_net, &zk_client, &[&slot1, &slot2], &baseline, &closed,
        "P4 after slot2",
    ).await?;

    // ----------------------------------------------------------------------
    // P5 — All-Java again, final read-back of L1..L4
    // ----------------------------------------------------------------------
    info!("===== P5: final read-back =====");
    for &(id, label, count, era) in &closed {
        read_ledger(&env, &zk_in_net, id, label, count, era)?;
    }

    env.mark_success();
    info!("===== java_to_rust_in_place_upgrade ALL PHASES PASS =====");
    Ok(())
}
