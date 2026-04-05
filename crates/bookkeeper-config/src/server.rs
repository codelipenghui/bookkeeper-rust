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

    // Sync / Checkpoint
    pub flush_interval: i32,

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

    // TLS
    pub tls_certificate_path: Option<String>,
    pub tls_key_path: Option<String>,
    pub tls_trust_store_path: Option<String>,
    pub tls_client_authentication: bool,

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
            journal_max_group_wait_ms: 2,
            journal_buffered_writes_threshold: 524288,
            journal_buffered_entries_threshold: 0,
            journal_alignment_size: 4096,
            journal_format_version_to_write: 6,
            flush_interval: 10000,
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
            tls_certificate_path: None,
            tls_key_path: None,
            tls_trust_store_path: None,
            tls_client_authentication: false,
            disk_usage_threshold: 0.95,
            disk_usage_warn_threshold: 0.90,
            disk_usage_lwm_threshold: 0.95,
            disk_check_interval: 10000,
            read_only_mode_enabled: true,
            minor_compaction_interval: 3600,
            minor_compaction_threshold: 0.2,
            major_compaction_interval: 86400,
            major_compaction_threshold: 0.8,
            gc_wait_time: 600000,
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
        if let Some(v) = props.get("useHostNameAsBookieID") { config.use_hostname_as_bookie_id = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("useShortHostName") { config.use_short_hostname = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("listeningInterface") { config.listening_interface = Some(v.clone()); }
        if let Some(v) = props.get("allowLoopback") { config.allow_loopback = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("journalDirectory") { config.journal_directory = v.clone(); }
        if let Some(v) = props.get("journalDirectories") { config.journal_directory = v.split('|').next().unwrap_or("").to_string(); }
        if let Some(v) = props.get("ledgerDirectories") { config.ledger_directories = v.split(',').map(|s| s.trim().to_string()).collect(); }
        if let Some(v) = props.get("indexDirectories") { config.index_directories = Some(v.split(',').map(|s| s.trim().to_string()).collect()); }
        if let Some(v) = props.get("allowStorageExpansion") { config.allow_storage_expansion = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("journalMaxSizeMB") { if let Ok(n) = v.parse() { config.journal_max_size_mb = n; } }
        if let Some(v) = props.get("journalMaxBackups") { if let Ok(n) = v.parse() { config.journal_max_backups = n; } }
        if let Some(v) = props.get("journalSyncData") { config.journal_sync_data = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("journalAdaptiveGroupWrites") { config.journal_adaptive_group_writes = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("journalMaxGroupWaitMSec") { if let Ok(n) = v.parse() { config.journal_max_group_wait_ms = n; } }
        if let Some(v) = props.get("journalBufferedWritesThreshold") { if let Ok(n) = v.parse() { config.journal_buffered_writes_threshold = n; } }
        if let Some(v) = props.get("journalBufferedEntriesThreshold") { if let Ok(n) = v.parse() { config.journal_buffered_entries_threshold = n; } }
        if let Some(v) = props.get("journalAlignmentSize") { if let Ok(n) = v.parse() { config.journal_alignment_size = n; } }
        if let Some(v) = props.get("journalFormatVersionToWrite") { if let Ok(n) = v.parse() { config.journal_format_version_to_write = n; } }
        if let Some(v) = props.get("flushInterval") { if let Ok(n) = v.parse() { config.flush_interval = n; } }
        if let Some(v) = props.get("logSizeLimit") { if let Ok(n) = v.parse() { config.log_size_limit = n; } }
        if let Some(v) = props.get("entryLogPerLedgerEnabled") { config.entry_log_per_ledger_enabled = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("dbStorage_writeCacheMaxSizeMb") { if let Ok(n) = v.parse() { config.db_storage_write_cache_max_size_mb = n; } }
        if let Some(v) = props.get("dbStorage_readAheadCacheMaxSizeMb") { if let Ok(n) = v.parse() { config.db_storage_read_cache_max_size_mb = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_writeBufferSizeMB") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_write_buffer_size_mb = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_blockSize") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_block_size = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_bloomFilterBitsPerKey") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_bloom_filter_bits_per_key = n; } }
        if let Some(v) = props.get("serverTcpNoDelay") { config.server_tcp_no_delay = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("serverSockKeepalive") { config.server_sock_keepalive = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("nettyMaxFrameSizeBytes") { if let Ok(n) = v.parse() { config.max_frame_size = n; } }
        if let Some(v) = props.get("numAddWorkerThreads") { if let Ok(n) = v.parse() { config.num_add_worker_threads = n; } }
        if let Some(v) = props.get("numReadWorkerThreads") { if let Ok(n) = v.parse() { config.num_read_worker_threads = n; } }
        if let Some(v) = props.get("tlsCertificatePath") { config.tls_certificate_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsKeyStore") { config.tls_key_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsKeyFilePath") { config.tls_key_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsTrustStore") { config.tls_trust_store_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsClientAuthentication") { config.tls_client_authentication = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("diskUsageThreshold") { if let Ok(n) = v.parse() { config.disk_usage_threshold = n; } }
        if let Some(v) = props.get("diskUsageWarnThreshold") { if let Ok(n) = v.parse() { config.disk_usage_warn_threshold = n; } }
        if let Some(v) = props.get("diskUsageLwmThreshold") { if let Ok(n) = v.parse() { config.disk_usage_lwm_threshold = n; } }
        if let Some(v) = props.get("diskCheckInterval") { if let Ok(n) = v.parse() { config.disk_check_interval = n; } }
        if let Some(v) = props.get("readOnlyModeEnabled") { config.read_only_mode_enabled = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("minorCompactionInterval") { if let Ok(n) = v.parse() { config.minor_compaction_interval = n; } }
        if let Some(v) = props.get("minorCompactionThreshold") { if let Ok(n) = v.parse() { config.minor_compaction_threshold = n; } }
        if let Some(v) = props.get("majorCompactionInterval") { if let Ok(n) = v.parse() { config.major_compaction_interval = n; } }
        if let Some(v) = props.get("majorCompactionThreshold") { if let Ok(n) = v.parse() { config.major_compaction_threshold = n; } }
        if let Some(v) = props.get("gcWaitTime") { if let Ok(n) = v.parse() { config.gc_wait_time = n; } }
        if let Some(v) = props.get("autoRecoveryDaemonEnabled") { config.auto_recovery_daemon_enabled = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("lostBookieRecoveryDelay") { if let Ok(n) = v.parse() { config.lost_bookie_recovery_delay = n; } }
        if let Some(v) = props.get("zkServers") { config.zk_servers = Some(v.clone()); }
        if let Some(v) = props.get("zkLedgersRootPath") { config.zk_ledger_root = v.clone(); }
        if let Some(v) = props.get("zkTimeout") { if let Ok(n) = v.parse() { config.zk_timeout = n; } }
        if let Some(v) = props.get("httpServerEnabled") { config.http_server_enabled = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("httpServerPort") { if let Ok(n) = v.parse() { config.http_server_port = n; } }
        config
    }

    pub fn from_file(path: &Path) -> Result<Self, std::io::Error> {
        let text = std::fs::read_to_string(path)?;
        let props = crate::properties::parse_properties(&text);
        Ok(Self::from_properties(&props))
    }
}
