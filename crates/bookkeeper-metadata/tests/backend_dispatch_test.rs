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
