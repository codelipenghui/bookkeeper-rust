# Phase 8: Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a configuration system that parses Java Properties files (`bk_server.conf`) and provides typed access to all BookKeeper server and client parameters — enabling the Rust bookie to read standard BookKeeper configuration files.

**Architecture:** New `bookkeeper-config` crate with a `ServerConfig` struct (all server parameters with defaults) and a `ClientConfig` struct (all client parameters). Configuration is loaded from Java Properties format files (key=value, `#` comments). The existing `BookieConfig` in `bookkeeper-server` will be replaced by `ServerConfig`. Configuration structs are plain data with `Default` impls matching Java defaults.

**Tech Stack:** `java-properties` crate (parses Java .properties files), `serde` (optional, for future TOML/YAML support)

**Spec reference:** `docs/superpowers/specs/08-configuration.md`

---

## File Structure

```
crates/bookkeeper-config/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── properties.rs        # Java Properties file parser (key=value with # comments)
    ├── server.rs            # ServerConfig with all bookie parameters
    └── client.rs            # ClientConfig with all client parameters
```

---

### Task 1: Create Crate + Properties Parser + ServerConfig

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `crates/bookkeeper-config/Cargo.toml`
- Create: `crates/bookkeeper-config/src/lib.rs`
- Create: `crates/bookkeeper-config/src/properties.rs`
- Create: `crates/bookkeeper-config/src/server.rs`
- Test: `crates/bookkeeper-config/tests/config_test.rs`

- [ ] **Step 1: Add crate to workspace**

Add `"crates/bookkeeper-config"` to members and `bookkeeper-config = { path = "crates/bookkeeper-config" }` to workspace.dependencies.

- [ ] **Step 2: Create Cargo.toml**

```toml
[package]
name = "bookkeeper-config"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
thiserror = { workspace = true }

[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 3: Write failing tests**

`crates/bookkeeper-config/tests/config_test.rs`:
```rust
use bookkeeper_config::properties::parse_properties;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_config::client::ClientConfig;
use std::io::Write;

#[test]
fn parse_properties_basic() {
    let text = "bookiePort=3181\njournaDirectory=/data/journal\n# comment\n\n";
    let props = parse_properties(text);
    assert_eq!(props.get("bookiePort"), Some(&"3181".to_string()));
    assert_eq!(props.get("journaDirectory"), Some(&"/data/journal".to_string()));
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
```

- [ ] **Step 4: Implement properties.rs**

```rust
use std::collections::HashMap;

/// Parse Java Properties format: key=value lines, # comments, blank lines ignored.
pub fn parse_properties(text: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            map.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    map
}
```

- [ ] **Step 5: Implement server.rs**

```rust
use std::collections::HashMap;
use std::path::Path;

/// Complete bookie server configuration with all parameters from the spec.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    // Server Identity
    pub bookie_port: u16,
    pub advertised_address: Option<String>,
    pub use_hostname_as_bookie_id: bool,
    pub use_short_hostname: bool,
    pub listening_interface: Option<String>,
    pub allow_loopback: bool,

    // Storage Directories
    pub journal_directory: String,
    pub ledger_directories: Vec<String>,
    pub index_directories: Option<Vec<String>>,
    pub allow_storage_expansion: bool,

    // Journal
    pub journal_max_size_mb: i32,
    pub journal_max_backups: i32,
    pub journal_sync_data: bool,
    pub journal_adaptive_group_writes: bool,
    pub journal_max_group_wait_ms: i32,
    pub journal_buffered_writes_threshold: i32,
    pub journal_buffered_entries_threshold: i32,
    pub journal_alignment_size: i32,
    pub journal_format_version_to_write: i32,

    // Entry Logs
    pub log_size_limit: i64,
    pub entry_log_per_ledger_enabled: bool,

    // DbLedgerStorage
    pub db_storage_write_cache_max_size_mb: i32,
    pub db_storage_read_cache_max_size_mb: i32,
    pub db_storage_rocksdb_write_buffer_size_mb: i32,
    pub db_storage_rocksdb_block_size: i32,
    pub db_storage_rocksdb_bloom_filter_bits_per_key: i32,

    // Network
    pub server_tcp_no_delay: bool,
    pub server_sock_keepalive: bool,
    pub max_frame_size: usize,
    pub num_add_worker_threads: i32,
    pub num_read_worker_threads: i32,

    // Disk Management
    pub disk_usage_threshold: f64,
    pub disk_usage_warn_threshold: f64,
    pub disk_usage_lwm_threshold: f64,
    pub disk_check_interval: i32,
    pub read_only_mode_enabled: bool,

    // Compaction & GC
    pub minor_compaction_interval: i64,
    pub minor_compaction_threshold: f64,
    pub major_compaction_interval: i64,
    pub major_compaction_threshold: f64,
    pub gc_wait_time: i64,

    // Replication
    pub auto_recovery_daemon_enabled: bool,
    pub lost_bookie_recovery_delay: i32,

    // ZooKeeper
    pub zk_servers: Option<String>,
    pub zk_ledger_root: String,
    pub zk_timeout: i32,

    // HTTP Admin
    pub http_server_enabled: bool,
    pub http_server_port: i32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bookie_port: 3181,
            advertised_address: None,
            use_hostname_as_bookie_id: false,
            use_short_hostname: false,
            listening_interface: None,
            allow_loopback: false,
            journal_directory: "/tmp/bk-txn".to_string(),
            ledger_directories: vec!["/tmp/bk-data".to_string()],
            index_directories: None,
            allow_storage_expansion: true,
            journal_max_size_mb: 2048,
            journal_max_backups: 5,
            journal_sync_data: false,
            journal_adaptive_group_writes: true,
            journal_max_group_wait_ms: 1,
            journal_buffered_writes_threshold: 524288,
            journal_buffered_entries_threshold: 0,
            journal_alignment_size: 4096,
            journal_format_version_to_write: 6,
            log_size_limit: 1073741823,
            entry_log_per_ledger_enabled: false,
            db_storage_write_cache_max_size_mb: 64,
            db_storage_read_cache_max_size_mb: 64,
            db_storage_rocksdb_write_buffer_size_mb: 64,
            db_storage_rocksdb_block_size: 65536,
            db_storage_rocksdb_bloom_filter_bits_per_key: 10,
            server_tcp_no_delay: true,
            server_sock_keepalive: true,
            max_frame_size: 5 * 1024 * 1024,
            num_add_worker_threads: 1,
            num_read_worker_threads: 8,
            disk_usage_threshold: 0.95,
            disk_usage_warn_threshold: 0.90,
            disk_usage_lwm_threshold: 0.90,
            disk_check_interval: 10000,
            read_only_mode_enabled: true,
            minor_compaction_interval: 3600,
            minor_compaction_threshold: 0.1,
            major_compaction_interval: 604800,
            major_compaction_threshold: 0.5,
            gc_wait_time: 900000,
            auto_recovery_daemon_enabled: true,
            lost_bookie_recovery_delay: 1800,
            zk_servers: None,
            zk_ledger_root: "/ledgers".to_string(),
            zk_timeout: 10000,
            http_server_enabled: false,
            http_server_port: 8080,
        }
    }
}

impl ServerConfig {
    pub fn from_properties(props: &HashMap<String, String>) -> Self {
        let mut config = Self::default();
        if let Some(v) = props.get("bookiePort") { if let Ok(n) = v.parse() { config.bookie_port = n; } }
        if let Some(v) = props.get("advertisedAddress") { config.advertised_address = Some(v.clone()); }
        if let Some(v) = props.get("useHostNameAsBookieID") { config.use_hostname_as_bookie_id = v == "true"; }
        if let Some(v) = props.get("useShortHostName") { config.use_short_hostname = v == "true"; }
        if let Some(v) = props.get("listeningInterface") { config.listening_interface = Some(v.clone()); }
        if let Some(v) = props.get("allowLoopback") { config.allow_loopback = v == "true"; }
        if let Some(v) = props.get("journalDirectory") { config.journal_directory = v.clone(); }
        if let Some(v) = props.get("journalDirectories") { config.journal_directory = v.split('|').next().unwrap_or("").to_string(); }
        if let Some(v) = props.get("ledgerDirectories") { config.ledger_directories = v.split(',').map(|s| s.trim().to_string()).collect(); }
        if let Some(v) = props.get("indexDirectories") { config.index_directories = Some(v.split(',').map(|s| s.trim().to_string()).collect()); }
        if let Some(v) = props.get("allowStorageExpansion") { config.allow_storage_expansion = v == "true"; }
        if let Some(v) = props.get("journalMaxSizeMB") { if let Ok(n) = v.parse() { config.journal_max_size_mb = n; } }
        if let Some(v) = props.get("journalMaxBackups") { if let Ok(n) = v.parse() { config.journal_max_backups = n; } }
        if let Some(v) = props.get("journalSyncData") { config.journal_sync_data = v == "true"; }
        if let Some(v) = props.get("journalAdaptiveGroupWrites") { config.journal_adaptive_group_writes = v == "true"; }
        if let Some(v) = props.get("journalMaxGroupWaitMSec") { if let Ok(n) = v.parse() { config.journal_max_group_wait_ms = n; } }
        if let Some(v) = props.get("journalBufferedWritesThreshold") { if let Ok(n) = v.parse() { config.journal_buffered_writes_threshold = n; } }
        if let Some(v) = props.get("journalBufferedEntriesThreshold") { if let Ok(n) = v.parse() { config.journal_buffered_entries_threshold = n; } }
        if let Some(v) = props.get("journalAlignmentSize") { if let Ok(n) = v.parse() { config.journal_alignment_size = n; } }
        if let Some(v) = props.get("journalFormatVersionToWrite") { if let Ok(n) = v.parse() { config.journal_format_version_to_write = n; } }
        if let Some(v) = props.get("logSizeLimit") { if let Ok(n) = v.parse() { config.log_size_limit = n; } }
        if let Some(v) = props.get("entryLogPerLedgerEnabled") { config.entry_log_per_ledger_enabled = v == "true"; }
        if let Some(v) = props.get("dbStorage_writeCacheMaxSizeMb") { if let Ok(n) = v.parse() { config.db_storage_write_cache_max_size_mb = n; } }
        if let Some(v) = props.get("dbStorage_readAheadCacheMaxSizeMb") { if let Ok(n) = v.parse() { config.db_storage_read_cache_max_size_mb = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_writeBufferSizeMB") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_write_buffer_size_mb = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_blockSize") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_block_size = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_bloomFilterBitsPerKey") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_bloom_filter_bits_per_key = n; } }
        if let Some(v) = props.get("serverTcpNoDelay") { config.server_tcp_no_delay = v == "true"; }
        if let Some(v) = props.get("serverSockKeepalive") { config.server_sock_keepalive = v == "true"; }
        if let Some(v) = props.get("nettyMaxFrameSizeBytes") { if let Ok(n) = v.parse() { config.max_frame_size = n; } }
        if let Some(v) = props.get("numAddWorkerThreads") { if let Ok(n) = v.parse() { config.num_add_worker_threads = n; } }
        if let Some(v) = props.get("numReadWorkerThreads") { if let Ok(n) = v.parse() { config.num_read_worker_threads = n; } }
        if let Some(v) = props.get("diskUsageThreshold") { if let Ok(n) = v.parse() { config.disk_usage_threshold = n; } }
        if let Some(v) = props.get("diskUsageWarnThreshold") { if let Ok(n) = v.parse() { config.disk_usage_warn_threshold = n; } }
        if let Some(v) = props.get("diskUsageLwmThreshold") { if let Ok(n) = v.parse() { config.disk_usage_lwm_threshold = n; } }
        if let Some(v) = props.get("diskCheckInterval") { if let Ok(n) = v.parse() { config.disk_check_interval = n; } }
        if let Some(v) = props.get("readOnlyModeEnabled") { config.read_only_mode_enabled = v == "true"; }
        if let Some(v) = props.get("minorCompactionInterval") { if let Ok(n) = v.parse() { config.minor_compaction_interval = n; } }
        if let Some(v) = props.get("minorCompactionThreshold") { if let Ok(n) = v.parse() { config.minor_compaction_threshold = n; } }
        if let Some(v) = props.get("majorCompactionInterval") { if let Ok(n) = v.parse() { config.major_compaction_interval = n; } }
        if let Some(v) = props.get("majorCompactionThreshold") { if let Ok(n) = v.parse() { config.major_compaction_threshold = n; } }
        if let Some(v) = props.get("gcWaitTime") { if let Ok(n) = v.parse() { config.gc_wait_time = n; } }
        if let Some(v) = props.get("autoRecoveryDaemonEnabled") { config.auto_recovery_daemon_enabled = v == "true"; }
        if let Some(v) = props.get("lostBookieRecoveryDelay") { if let Ok(n) = v.parse() { config.lost_bookie_recovery_delay = n; } }
        if let Some(v) = props.get("zkServers") { config.zk_servers = Some(v.clone()); }
        if let Some(v) = props.get("zkLedgersRootPath") { config.zk_ledger_root = v.clone(); }
        if let Some(v) = props.get("zkTimeout") { if let Ok(n) = v.parse() { config.zk_timeout = n; } }
        if let Some(v) = props.get("httpServerEnabled") { config.http_server_enabled = v == "true"; }
        if let Some(v) = props.get("httpServerPort") { if let Ok(n) = v.parse() { config.http_server_port = n; } }
        config
    }

    pub fn from_file(path: &Path) -> Result<Self, std::io::Error> {
        let text = std::fs::read_to_string(path)?;
        let props = crate::properties::parse_properties(&text);
        Ok(Self::from_properties(&props))
    }
}
```

- [ ] **Step 6: Implement client.rs**

```rust
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub metadata_service_uri: Option<String>,
    pub zk_timeout: i32,
    pub use_v2_wire_protocol: bool,
    pub read_timeout: i32,
    pub add_entry_timeout: i32,
    pub add_entry_quorum_timeout: i32,
    pub speculative_read_timeout: i32,
    pub num_channels_per_bookie: i32,
    pub enable_digest_type_autodetection: bool,
    pub explicit_lac_interval_ms: i32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            metadata_service_uri: None,
            zk_timeout: 10000,
            use_v2_wire_protocol: false,
            read_timeout: 5,
            add_entry_timeout: 5,
            add_entry_quorum_timeout: 5,
            speculative_read_timeout: -1,
            num_channels_per_bookie: 1,
            enable_digest_type_autodetection: true,
            explicit_lac_interval_ms: 0,
        }
    }
}

impl ClientConfig {
    pub fn from_properties(props: &HashMap<String, String>) -> Self {
        let mut config = Self::default();
        if let Some(v) = props.get("metadataServiceUri") { config.metadata_service_uri = Some(v.clone()); }
        if let Some(v) = props.get("zkTimeout") { if let Ok(n) = v.parse() { config.zk_timeout = n; } }
        if let Some(v) = props.get("useV2WireProtocol") { config.use_v2_wire_protocol = v == "true"; }
        if let Some(v) = props.get("readTimeout") { if let Ok(n) = v.parse() { config.read_timeout = n; } }
        if let Some(v) = props.get("addEntryTimeout") { if let Ok(n) = v.parse() { config.add_entry_timeout = n; } }
        if let Some(v) = props.get("addEntryQuorumTimeout") { if let Ok(n) = v.parse() { config.add_entry_quorum_timeout = n; } }
        if let Some(v) = props.get("speculativeReadTimeout") { if let Ok(n) = v.parse() { config.speculative_read_timeout = n; } }
        if let Some(v) = props.get("numChannelsPerBookie") { if let Ok(n) = v.parse() { config.num_channels_per_bookie = n; } }
        if let Some(v) = props.get("enableDigestTypeAutodetection") { config.enable_digest_type_autodetection = v == "true"; }
        if let Some(v) = props.get("explicitLacIntervalMs") { if let Ok(n) = v.parse() { config.explicit_lac_interval_ms = n; } }
        config
    }

    pub fn from_file(path: &Path) -> Result<Self, std::io::Error> {
        let text = std::fs::read_to_string(path)?;
        let props = crate::properties::parse_properties(&text);
        Ok(Self::from_properties(&props))
    }
}
```

- [ ] **Step 7: Create lib.rs**

```rust
pub mod properties;
pub mod server;
pub mod client;
```

- [ ] **Step 8: Run tests, commit**

```bash
git commit -m "feat: add bookkeeper-config crate with ServerConfig, ClientConfig, and Properties parser

Parses Java Properties format (bk_server.conf), provides typed access to
all BookKeeper server and client configuration parameters with correct defaults.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Spec Coverage

| Spec Section | Covered | Notes |
|---|---|---|
| §1 Configuration sources | Yes | File parsing + programmatic defaults |
| §2 Server identity | Yes | bookie_port, advertised_address, etc. |
| §3 Storage directories | Yes | journal, ledger, index dirs |
| §4 Journal | Yes | All journal parameters |
| §5 Entry logs | Yes | logSizeLimit, entryLogPerLedger |
| §6 DbLedgerStorage | Partial | Key RocksDB params (cache sizes, bloom filter) |
| §7 Network & threading | Yes | TCP, frame size, worker threads |
| §8 Disk management | Yes | All thresholds |
| §9 Compaction/GC | Yes | All compaction parameters |
| §10 Replication | Partial | autoRecovery, lostBookieRecoveryDelay |
| §11 Client | Yes | All client parameters |
| §12 Placement policy | Deferred | Rack-aware not yet implemented |
| §13 Security/TLS | Deferred | TLS not yet implemented |
| §14 HTTP Admin | Yes | httpServerEnabled, httpServerPort |

## What's Next

| Phase | Scope | Status |
|-------|-------|--------|
| Phase 1-7 | All core components | Complete |
| Phase 8 | Configuration | This plan |
| Phase 9 | Admin HTTP API | Next |
| Phase 10 | CLI Tools | Future |
