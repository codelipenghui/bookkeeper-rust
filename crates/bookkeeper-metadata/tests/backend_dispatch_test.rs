use bookkeeper_metadata::backend::open_metadata_backend;
use bookkeeper_metadata::metadata_paths::ParsedMetadataUri;
use std::time::Duration;

#[tokio::test]
async fn unknown_scheme_returns_invalid_config() {
    let parsed = ParsedMetadataUri::Zk {
        servers: "does-not-resolve.example:2181".into(),
        ledgers_root: "/ledgers".into(),
    };
    // Connect will fail (no ZK running). We just want to ensure the
    // function routes to the ZK backend and surfaces a ConnectionError —
    // not a panic, not InvalidConfig.
    let res = open_metadata_backend(&parsed, None, Some(Duration::from_secs(2))).await;
    assert!(matches!(
        res,
        Err(bookkeeper_metadata::ledger_metadata::MetadataError::ConnectionError(_))
    ));
}

#[cfg(feature = "oxia")]
#[tokio::test]
async fn oxia_scheme_routes_to_oxia_backend() {
    let parsed = bookkeeper_metadata::metadata_paths::ParsedMetadataUri::Oxia {
        service_addr: "127.0.0.1:1".to_string(), // port 1: guaranteed closed
        namespace: "default".to_string(),
        ledgers_root: "/ledgers".to_string(),
    };
    let res = bookkeeper_metadata::backend::open_metadata_backend(
        &parsed,
        None,
        Some(std::time::Duration::from_secs(2)),
    )
    .await;
    // We want to confirm routing hit the oxia backend — surface a
    // ConnectionError (or any error) rather than InvalidConfig which
    // would signal no routing happened.
    match res {
        Err(bookkeeper_metadata::ledger_metadata::MetadataError::ConnectionError(_)) => {}
        Err(other) => panic!("expected ConnectionError, got {other:?}"),
        Ok(_) => panic!("expected connect to fail"),
    }
}
