//! Integration test: bookie re-registers its ephemeral after a ZK bounce.
//!
//! Regression fixture for the production symptom where a host-level crash
//! (docker daemon died) restarted the bookie container, the `zookeeper-client`
//! crate handed back a *new* session id without firing an `Expired` event
//! in between, and the old session's ephemeral at
//! `/ledgers/available/<bookie-id>` was swept by the ZK server during its
//! own restart — leaving the bookie invisible to clients.
//!
//! Before the fix, `run_session_watcher` only handled `Expired` (by
//! calling `exit(1)`) and ignored `SyncConnected` with a new session id.
//! After the fix, any `SyncConnected` whose session id differs from the
//! last known id triggers `register_bookie` again. This test validates
//! the re-registration path directly against a real ZK ensemble.
//!
//! Requires Docker. Skipped gracefully when docker is unreachable.

use bookkeeper_metadata::service_info::BookieServiceInfo;
use bookkeeper_metadata::traits::RegistrationManager;
use bookkeeper_metadata::zk::registration::ZkRegistrationManager;
use std::time::Duration;
use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt};

async fn start_zk() -> Option<(testcontainers::ContainerAsync<GenericImage>, String, u16)> {
    let image = GenericImage::new("zookeeper", "3.8")
        .with_wait_for(WaitFor::message_on_stdout("binding to port"))
        .with_exposed_port(2181.into())
        .with_env_var("ZOO_4LW_COMMANDS_WHITELIST", "*");

    match image.start().await {
        Ok(container) => {
            let port = container.get_host_port_ipv4(2181).await.ok()?;
            let connect = format!("127.0.0.1:{port}");
            Some((container, connect, port))
        }
        Err(e) => {
            eprintln!("[SKIP] docker/zookeeper unavailable: {e}");
            None
        }
    }
}

async fn writable_bookies(reg: &ZkRegistrationManager) -> Vec<String> {
    reg.get_writable_bookies().await.unwrap_or_default()
}

/// Poll `get_writable_bookies` until `bookie_id` is present, or give up
/// after `timeout`. Needed because session re-establishment + the
/// kernel-side ephemeral create both race the assertion.
async fn wait_until_registered(
    reg: &ZkRegistrationManager,
    bookie_id: &str,
    timeout: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if writable_bookies(reg).await.iter().any(|b| b == bookie_id) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

#[tokio::test]
async fn reregistration_after_zk_bounce_restores_ephemeral() {
    let Some((zk_container, connect, _port)) = start_zk().await else { return; };

    let bookie_id = "test-bookie:3181";
    let info = BookieServiceInfo::new(bookie_id, "127.0.0.1", 3181);

    // Register via a FIRST manager (this is the bookie's startup-time reg).
    let reg1 = ZkRegistrationManager::new(&connect, "/ledgers").await
        .expect("connect to ZK");
    reg1.register_bookie(bookie_id, false, &info).await
        .expect("initial register");
    let bookies_before = writable_bookies(&reg1).await;
    assert!(
        bookies_before.iter().any(|b| b == bookie_id),
        "bookie should be visible immediately after initial register, got {bookies_before:?}"
    );

    // Drop the first client — we want the ZK ensemble to age the session
    // out on its own when we bounce the container. Keeping `reg1` alive
    // would hold the ephemeral via session heartbeats.
    drop(reg1);

    // Bounce the ZK container. This is the production scenario we regressed
    // against: the ensemble restarts while the bookie process is alive
    // and the original session's ephemerals are wiped.
    zk_container.stop().await.expect("stop zk");
    zk_container.start().await.expect("start zk");

    // Wait for the ZK server to accept connections again. Re-query the
    // host port — Docker may remap the ephemeral port across a container
    // stop/start cycle, in which case the old `connect` string becomes
    // invalid and a fresh lookup is required.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let new_port = zk_container
        .get_host_port_ipv4(2181)
        .await
        .expect("re-query zk host port");
    let connect_after = format!("127.0.0.1:{new_port}");

    // Simulate what `run_session_watcher` now does: build a fresh client
    // and re-register. The key invariant is that `register_bookie` is
    // idempotent and succeeds against the fresh ensemble.
    let reg2 = ZkRegistrationManager::new(&connect_after, "/ledgers").await
        .expect("reconnect after ZK bounce");
    reg2.register_bookie(bookie_id, false, &info).await
        .expect("re-register after ZK bounce");

    assert!(
        wait_until_registered(&reg2, bookie_id, Duration::from_secs(10)).await,
        "bookie {bookie_id} did not re-appear in /ledgers/available within 10s"
    );
}

#[tokio::test]
async fn register_bookie_is_idempotent_on_node_exists() {
    let Some((_zk_container, connect, _port)) = start_zk().await else { return; };

    let bookie_id = "idempotent-bookie:3181";
    let info = BookieServiceInfo::new(bookie_id, "127.0.0.1", 3181);
    let reg = ZkRegistrationManager::new(&connect, "/ledgers").await
        .expect("connect to ZK");

    // Two back-to-back registrations against the same session: the
    // second one sees `NodeExists` and must update in place rather than
    // erroring out.
    reg.register_bookie(bookie_id, false, &info).await
        .expect("first register");
    reg.register_bookie(bookie_id, false, &info).await
        .expect("second register must be idempotent (NodeExists → set_data)");

    let listed = writable_bookies(&reg).await;
    assert!(listed.iter().any(|b| b == bookie_id), "expected {bookie_id} in {listed:?}");
}
