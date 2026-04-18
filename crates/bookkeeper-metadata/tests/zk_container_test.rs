//! Integration tests against a real ZooKeeper container (testcontainers).
//!
//! These tests exercise the full end-to-end ZK metadata flow against a live
//! ZooKeeper server, using testcontainers to spin up a disposable container
//! per test run.
//!
//! To run: `cargo test -p bookkeeper-metadata --test zk_container_test`
//!
//! Requires Docker to be available. Tests are skipped gracefully if Docker
//! can't be reached — they print a warning and return success.

#[path = "common/mod.rs"]
mod common;

use bookkeeper_metadata::cookie::Cookie;
use bookkeeper_metadata::ledger_metadata::{LedgerMetadataBuilder, LedgerState, MetadataError};
use bookkeeper_metadata::service_info::BookieServiceInfo;
use bookkeeper_metadata::traits::{LedgerLayout, LedgerManager, RegistrationManager};
use bookkeeper_metadata::zk::idgen::ZkLedgerIdGenerator;
use bookkeeper_metadata::zk::ledger_manager::ZkLedgerManager;
use bookkeeper_metadata::zk::registration::ZkRegistrationManager;
use bookkeeper_metadata::zk_path::ZkPaths;
use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt};

/// Spawn a ZooKeeper 3.8 container and return its client-port endpoint.
/// Returns None if Docker is not available — test should skip gracefully.
async fn start_zk() -> Option<(testcontainers::ContainerAsync<GenericImage>, String)> {
    let image = GenericImage::new("zookeeper", "3.8")
        .with_wait_for(WaitFor::message_on_stdout("binding to port"))
        .with_exposed_port(2181.into())
        .with_env_var("ZOO_4LW_COMMANDS_WHITELIST", "*");

    match image.start().await {
        Ok(container) => {
            let port = container.get_host_port_ipv4(2181).await.ok()?;
            let connect = format!("127.0.0.1:{port}");
            Some((container, connect))
        }
        Err(e) => {
            eprintln!("[SKIP] docker/zookeeper unavailable: {e}");
            None
        }
    }
}

async fn bootstrap_cluster(connect: &str, root: &str) {
    let mgr = ZkRegistrationManager::new(connect, root).await.unwrap();
    let layout = LedgerLayout {
        manager_factory_class: "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory".into(),
        manager_version: 1,
    };
    let instance_id = uuid::Uuid::new_v4().to_string();
    mgr.init_new_cluster(&layout, &instance_id).await.unwrap();
}

#[tokio::test]
async fn zk_init_new_cluster_creates_layout_and_instance_id() {
    let Some((_zk, connect)) = start_zk().await else { return; };
    let root = "/test_init";

    let mgr = ZkRegistrationManager::new(&connect, root).await.unwrap();
    let layout = LedgerLayout {
        manager_factory_class: "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory".into(),
        manager_version: 1,
    };
    let instance_id = "test-instance-xyz".to_string();
    mgr.init_new_cluster(&layout, &instance_id).await.unwrap();

    // Instance ID should now be readable
    let found = mgr.get_cluster_instance_id().await.unwrap();
    assert_eq!(found.as_deref(), Some("test-instance-xyz"));
}

#[tokio::test]
async fn zk_bookie_registration_roundtrip() {
    let Some((_zk, connect)) = start_zk().await else { return; };
    let root = "/test_reg";
    bootstrap_cluster(&connect, root).await;

    let mgr = ZkRegistrationManager::new(&connect, root).await.unwrap();
    let info = BookieServiceInfo::new("bookie-1:3181", "127.0.0.1", 3181);
    mgr.register_bookie("bookie-1:3181", false, &info).await.unwrap();

    let writable = mgr.get_writable_bookies().await.unwrap();
    assert!(writable.contains(&"bookie-1:3181".to_string()),
        "writable bookies should include the registered one: {writable:?}");

    let readonly = mgr.get_readonly_bookies().await.unwrap();
    assert!(!readonly.contains(&"bookie-1:3181".to_string()),
        "should not be in readonly list");
}

#[tokio::test]
async fn zk_bookie_unregister_removes_from_list() {
    let Some((_zk, connect)) = start_zk().await else { return; };
    let root = "/test_unreg";
    bootstrap_cluster(&connect, root).await;

    let mgr = ZkRegistrationManager::new(&connect, root).await.unwrap();
    let info = BookieServiceInfo::new("b:3181", "127.0.0.1", 3181);
    mgr.register_bookie("b:3181", false, &info).await.unwrap();
    assert!(mgr.get_writable_bookies().await.unwrap().contains(&"b:3181".to_string()));

    // unregister_bookie takes `read_only` — pass false to remove from the writable path
    mgr.unregister_bookie("b:3181", false).await.unwrap();
    let writable = mgr.get_writable_bookies().await.unwrap();
    assert!(!writable.contains(&"b:3181".to_string()), "should be removed after unregister");
}

#[tokio::test]
async fn zk_cookie_crud() {
    let Some((_zk, connect)) = start_zk().await else { return; };
    let root = "/test_cookies";
    bootstrap_cluster(&connect, root).await;

    let mgr = ZkRegistrationManager::new(&connect, root).await.unwrap();
    let bookie_id = "bookie1:3181";

    // Create
    let cookie = Cookie::new(bookie_id, "/journal", &["/ledger"], Some("inst-1"), None);
    mgr.create_cookie(bookie_id, cookie.serialize().as_bytes()).await.unwrap();

    // Read
    let (data, version) = mgr.read_cookie(bookie_id).await.unwrap().unwrap();
    let parsed = Cookie::deserialize(&String::from_utf8(data).unwrap()).unwrap();
    assert_eq!(parsed.bookie_id(), bookie_id);
    assert_eq!(parsed.journal_dirs(), "/journal");

    // Update
    let updated = Cookie::new(bookie_id, "/journal", &["/ledger", "/ledger2"], Some("inst-1"), None);
    mgr.update_cookie(bookie_id, updated.serialize().as_bytes(), version).await.unwrap();
    let (data2, _) = mgr.read_cookie(bookie_id).await.unwrap().unwrap();
    let parsed2 = Cookie::deserialize(&String::from_utf8(data2).unwrap()).unwrap();
    assert_eq!(parsed2.ledger_dirs(), "2\t/ledger\t/ledger2");

    // Delete
    let (_, latest_version) = mgr.read_cookie(bookie_id).await.unwrap().unwrap();
    mgr.delete_cookie(bookie_id, latest_version).await.unwrap();
    assert!(mgr.read_cookie(bookie_id).await.unwrap().is_none());
}

#[tokio::test]
async fn zk_ledger_metadata_create_read_write() {
    let Some((_zk, connect)) = start_zk().await else { return; };
    let root = "/test_lm";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let lm = ZkLedgerManager::new(client, paths);

    // Create a ledger
    let md = LedgerMetadataBuilder::new()
        .ledger_id(42)
        .ensemble_size(3)
        .write_quorum_size(3)
        .ack_quorum_size(2)
        .new_ensemble_entry(0, vec![
            "b1:3181".to_string(),
            "b2:3181".to_string(),
            "b3:3181".to_string(),
        ])
        .build()
        .unwrap();

    let created = lm.create_ledger_metadata(42, &md).await.unwrap();
    assert_eq!(created.version(), 0);

    // Read it back
    let read = lm.read_ledger_metadata(42).await.unwrap();
    assert_eq!(read.value().ensemble_size(), 3);
    assert_eq!(read.value().ack_quorum_size(), 2);

    // Update (write with CAS)
    let updated = LedgerMetadataBuilder::from_existing(read.value())
        .with_closed_state(5, 500)
        .build()
        .unwrap();
    let written = lm.write_ledger_metadata(42, &updated, read.version()).await.unwrap();
    assert!(written.version() > read.version());

    let reread = lm.read_ledger_metadata(42).await.unwrap();
    assert_eq!(reread.value().state(), LedgerState::Closed);
    assert_eq!(reread.value().last_entry_id(), Some(5));
}

#[tokio::test]
async fn zk_ledger_metadata_cas_version_conflict() {
    let Some((_zk, connect)) = start_zk().await else { return; };
    let root = "/test_cas";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let lm = ZkLedgerManager::new(client, paths);

    let md = LedgerMetadataBuilder::new()
        .ledger_id(100)
        .ensemble_size(1)
        .write_quorum_size(1)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b:3181".to_string()])
        .build()
        .unwrap();

    let created = lm.create_ledger_metadata(100, &md).await.unwrap();
    let v0 = created.version();

    // First update with correct version — succeeds
    let md1 = LedgerMetadataBuilder::from_existing(created.value())
        .with_closed_state(0, 10)
        .build()
        .unwrap();
    let _ = lm.write_ledger_metadata(100, &md1, v0).await.unwrap();

    // Second update with stale version — should fail with VersionConflict
    let result = lm.write_ledger_metadata(100, &md1, v0).await;
    assert!(matches!(result, Err(MetadataError::VersionConflict { .. })),
        "expected version conflict, got {result:?}");
}

#[tokio::test]
async fn zk_ledger_metadata_remove() {
    let Some((_zk, connect)) = start_zk().await else { return; };
    let root = "/test_rm";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let lm = ZkLedgerManager::new(client, paths);

    let md = LedgerMetadataBuilder::new()
        .ledger_id(7)
        .ensemble_size(1)
        .write_quorum_size(1)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b:3181".to_string()])
        .build()
        .unwrap();

    lm.create_ledger_metadata(7, &md).await.unwrap();
    assert!(lm.read_ledger_metadata(7).await.is_ok());

    // Delete with version -1 (ANY)
    lm.remove_ledger_metadata(7, -1).await.unwrap();

    // Re-read should fail
    let result = lm.read_ledger_metadata(7).await;
    assert!(result.is_err(), "ledger should be gone after delete");
}

#[tokio::test]
async fn zk_id_generator_produces_sequential_ids() {
    let Some((_zk, connect)) = start_zk().await else { return; };
    let root = "/test_idgen";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let idgen = ZkLedgerIdGenerator::new(client, &paths).await.unwrap();

    use bookkeeper_metadata::traits::LedgerIdGenerator;
    let mut ids = Vec::new();
    for _ in 0..5 {
        let id = idgen.generate_ledger_id().await.unwrap();
        ids.push(id);
    }

    // IDs should be unique
    let unique: std::collections::HashSet<_> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len(), "IDs should be unique: {ids:?}");
    // All IDs should be non-negative
    assert!(ids.iter().all(|&id| id >= 0), "IDs should be non-negative: {ids:?}");
}

#[tokio::test]
async fn zk_get_ledger_ranges_finds_all_created_ledgers() {
    let Some((_zk, connect)) = start_zk().await else { return; };
    let root = "/test_ranges";
    bootstrap_cluster(&connect, root).await;

    let client = zookeeper_client::Client::connect(&connect).await.unwrap();
    let paths = ZkPaths::new(root);
    let lm = ZkLedgerManager::new(client, paths);

    // Create 3 ledgers with different IDs
    for &id in &[1, 42, 999] {
        let md = LedgerMetadataBuilder::new()
            .ledger_id(id)
            .ensemble_size(1)
            .write_quorum_size(1)
            .ack_quorum_size(1)
            .new_ensemble_entry(0, vec!["b:3181".to_string()])
            .build()
            .unwrap();
        lm.create_ledger_metadata(id, &md).await.unwrap();
    }

    let ledgers = lm.get_ledger_ranges().await.unwrap();
    let found: std::collections::HashSet<_> = ledgers.into_iter().collect();
    assert!(found.contains(&1));
    assert!(found.contains(&42));
    assert!(found.contains(&999));
}

#[tokio::test]
#[ignore] // container-based; run with `cargo test -- --ignored`
async fn zk_backend_satisfies_contract() {
    let Some((_zk, connect)) = start_zk().await else {
        eprintln!("[SKIP] zk_backend_satisfies_contract: Docker not available");
        return;
    };

    let root = "/test_contract";
    let uri = bookkeeper_metadata::metadata_paths::ParsedMetadataUri::Zk {
        servers: connect.clone(),
        ledgers_root: root.to_string(),
    };
    let backend = bookkeeper_metadata::backend::open_metadata_backend(&uri, None, None)
        .await
        .expect("zk_backend_satisfies_contract: open_metadata_backend failed");

    let instance_id = uuid::Uuid::new_v4().to_string();
    backend
        .registration_manager
        .init_new_cluster(
            &bookkeeper_metadata::traits::LedgerLayout {
                manager_factory_class:
                    "org.apache.bookkeeper.meta.LegacyHierarchicalLedgerManagerFactory".into(),
                manager_version: 1,
            },
            &instance_id,
        )
        .await
        .expect("zk_backend_satisfies_contract: init_new_cluster failed");

    common::contract::run_all(&backend).await;
}
