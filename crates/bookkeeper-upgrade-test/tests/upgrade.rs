//! Java↔Rust bookie in-place upgrade test.
//! See docs/superpowers/specs/2026-04-11-java-rust-upgrade-test-design.md

use anyhow::Result;
use std::process::Command;
use tracing::info;

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

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn java_to_rust_in_place_upgrade() -> Result<()> {
    init_tracing();
    check_prerequisites()?;
    info!("Prerequisites OK; remainder of test not yet implemented");
    Ok(())
}
