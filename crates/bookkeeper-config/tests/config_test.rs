use bookkeeper_config::properties::parse_properties;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_config::client::ClientConfig;
use std::io::Write;

#[test]
fn parse_properties_basic() {
    let text = "bookiePort=3181\njournalDirectory=/data/journal\n# comment\n\n";
    let props = parse_properties(text);
    assert_eq!(props.get("bookiePort"), Some(&"3181".to_string()));
    assert_eq!(props.get("journalDirectory"), Some(&"/data/journal".to_string()));
    assert_eq!(props.len(), 2);
}

#[test]
fn parse_properties_with_spaces() {
    let text = "key1 = value1\nkey2=  value2  \n";
    let props = parse_properties(text);
    assert_eq!(props.get("key1"), Some(&"value1".to_string()));
    assert_eq!(props.get("key2"), Some(&"value2".to_string()));
}

#[test]
fn server_config_defaults() {
    let config = ServerConfig::default();
    assert_eq!(config.bookie_port, 3181);
    assert_eq!(config.journal_directory, "/tmp/bk-txn");
    assert_eq!(config.ledger_directories, vec!["/tmp/bk-data".to_string()]);
    assert_eq!(config.journal_max_size_mb, 2048);
    assert_eq!(config.journal_alignment_size, 4096);
    assert_eq!(config.disk_usage_threshold, 0.95);
    assert_eq!(config.max_frame_size, 5 * 1024 * 1024);
    assert!(config.read_only_mode_enabled);
    assert!(config.auto_recovery_daemon_enabled);
}

#[test]
fn server_config_from_properties() {
    let text = "bookiePort=4181\njournalDirectory=/data/journal\nledgerDirectories=/data/ledger1,/data/ledger2\njournalMaxSizeMB=1024\ndiskUsageThreshold=0.90\n";
    let props = parse_properties(text);
    let config = ServerConfig::from_properties(&props);
    assert_eq!(config.bookie_port, 4181);
    assert_eq!(config.journal_directory, "/data/journal");
    assert_eq!(config.ledger_directories, vec!["/data/ledger1".to_string(), "/data/ledger2".to_string()]);
    assert_eq!(config.journal_max_size_mb, 1024);
    assert_eq!(config.disk_usage_threshold, 0.90);
}

#[test]
fn server_config_from_file() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("bk_server.conf");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "bookiePort=5181").unwrap();
    writeln!(f, "# This is a comment").unwrap();
    writeln!(f, "journalDirectory=/custom/journal").unwrap();
    drop(f);
    let config = ServerConfig::from_file(&path).unwrap();
    assert_eq!(config.bookie_port, 5181);
    assert_eq!(config.journal_directory, "/custom/journal");
}

/// Regression test for the docker-compose playground's bookie config.
/// Pins the field names so a rename in `from_properties` breaks CI
/// instead of silently making the compose stack start a bookie with
/// default (wrong) values.
#[test]
fn docker_compose_bk_server_conf_parses() {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("docker")
        .join("bk_server.conf");
    let config = ServerConfig::from_file(&path)
        .expect("docker/bk_server.conf should parse");
    assert_eq!(config.bookie_port, 3181);
    assert_eq!(config.listening_interface.as_deref(), Some("0.0.0.0"));
    // advertisedAddress is intentionally absent from the shared config file;
    // the compose stack overrides it per-bookie via `bk serve --advertised-address`
    assert_eq!(config.advertised_address, None);
    assert_eq!(config.zk_servers.as_deref(), Some("zookeeper:2181"));
    assert_eq!(config.zk_ledger_root, "/ledgers");
    assert_eq!(config.journal_directory, "/bk/journal");
    assert_eq!(config.ledger_directories, vec!["/bk/data".to_string()]);
    assert!(!config.read_only_mode_enabled);
    assert!(config.http_server_enabled);
    assert_eq!(config.http_server_port, 8080);
    assert!(config.auto_recovery_daemon_enabled);
}

#[test]
fn client_config_defaults() {
    let config = ClientConfig::default();
    assert_eq!(config.zk_timeout, 10000);
    assert_eq!(config.read_timeout, 5);
    assert_eq!(config.add_entry_timeout, 5);
    assert!(!config.use_v2_wire_protocol);
    assert_eq!(config.num_channels_per_bookie, 1);
}

#[test]
fn client_config_from_properties() {
    let text = "metadataServiceUri=zk+hierarchical://zk1:2181/ledgers\nzkTimeout=30000\nreadTimeout=10\n";
    let props = parse_properties(text);
    let config = ClientConfig::from_properties(&props);
    assert_eq!(config.metadata_service_uri, Some("zk+hierarchical://zk1:2181/ledgers".to_string()));
    assert_eq!(config.zk_timeout, 30000);
    assert_eq!(config.read_timeout, 10);
}
