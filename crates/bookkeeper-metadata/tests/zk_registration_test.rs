use bookkeeper_metadata::service_info::BookieServiceInfo;
use bookkeeper_metadata::traits::{LedgerLayout, RegistrationManager};
use bookkeeper_metadata::zk::registration::ZkRegistrationManager;

/// These tests require a running ZooKeeper at localhost:2181.
/// Run with: cargo test -p bookkeeper-metadata --test zk_registration_test -- --ignored

#[tokio::test]
#[ignore]
async fn register_and_discover_bookie() {
    let mgr = ZkRegistrationManager::new("localhost:2181", "/test-ledgers").await.unwrap();
    let info = BookieServiceInfo::new("test-bookie:3181", "test-bookie", 3181);

    // Init cluster
    let layout = LedgerLayout {
        manager_factory_class: "HierarchicalLedgerManagerFactory".to_string(),
        manager_version: 1,
    };
    let _ = mgr.init_new_cluster(&layout, &uuid::Uuid::new_v4().to_string()).await;

    // Register
    mgr.register_bookie("test-bookie:3181", false, &info).await.unwrap();

    // Discover
    let bookies = mgr.get_writable_bookies().await.unwrap();
    assert!(bookies.contains(&"test-bookie:3181".to_string()));

    // Unregister
    mgr.unregister_bookie("test-bookie:3181", false).await.unwrap();
    let bookies = mgr.get_writable_bookies().await.unwrap();
    assert!(!bookies.contains(&"test-bookie:3181".to_string()));
}

#[tokio::test]
#[ignore]
async fn cookie_round_trip_via_zk() {
    let mgr = ZkRegistrationManager::new("localhost:2181", "/test-ledgers").await.unwrap();
    let cookie_data = b"5\nbookieHost: \"test:3181\"\n";

    // Create
    let _ = mgr.create_cookie("test-cookie-bookie:3181", cookie_data).await;

    // Read
    let (data, version) = mgr.read_cookie("test-cookie-bookie:3181").await.unwrap().unwrap();
    assert_eq!(data, cookie_data);

    // Delete
    mgr.delete_cookie("test-cookie-bookie:3181", version).await.unwrap();
    assert!(mgr.read_cookie("test-cookie-bookie:3181").await.unwrap().is_none());
}
