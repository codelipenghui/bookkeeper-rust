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
    assert_eq!(config.journal_alignment_size, 512);
    assert_eq!(config.disk_usage_threshold, 0.95);
    assert_eq!(config.max_frame_size, 5 * 1024 * 1024);
    assert!(config.read_only_mode_enabled);
    assert!(!config.auto_recovery_daemon_enabled);
    assert!(config.journal_sync_data);
    assert_eq!(config.lost_bookie_recovery_delay, 0);
    assert_eq!(config.max_reads_in_progress_limit, 10000);
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

// ---------------------------------------------------------------------------
// New config fields (added for Java parity)
// ---------------------------------------------------------------------------

#[test]
fn server_config_new_fields_defaults() {
    let config = ServerConfig::default();
    // Request limits
    assert_eq!(config.max_adds_in_progress_limit, 0);
    assert_eq!(config.max_reads_in_progress_limit, 10000);
    assert_eq!(config.max_connections, 0);
    // Journal extended
    assert!(!config.skip_replay_journal_invalid_record);
    assert_eq!(config.journal_pre_alloc_size_mb, 16);
    assert!(!config.journal_reuse_files);
    assert_eq!(config.journal_queue_size, 10000);
    assert_eq!(config.journal_page_cache_flush_interval_ms, 1000);
    assert_eq!(config.num_journal_callback_threads, 1);
    // GC extended
    assert!(!config.force_allow_compaction);
    assert!(!config.is_throttle_by_bytes);
    assert!(!config.use_transactional_compaction);
    assert_eq!(config.minor_compaction_max_time_ms, -1);
    assert_eq!(config.major_compaction_max_time_ms, -1);
    // Matches Java's default (100_000); prior 10_000 caused 10× more flushes.
    assert_eq!(config.compaction_max_outstanding_requests, 100_000);
    assert!(!config.is_force_gc_allow_when_no_space);
    // Disk extended
    assert!(!config.force_read_only_bookie);
    assert!(!config.persist_bookie_status_enabled);
    assert!(config.read_only_mode_on_any_disk_full_enabled);
    // Network
    assert_eq!(config.server_write_buffer_low_water_mark, 384 * 1024);
    assert_eq!(config.num_high_priority_worker_threads, 8);
    assert_eq!(config.server_num_acceptor_threads, 1);
    assert_eq!(config.wait_timeout_on_response_backpressure_ms, -1);
    assert!(config.read_worker_threads_throttling_enabled);
    // Storage
    assert_eq!(config.open_file_limit, 20000);
    assert_eq!(config.min_usable_size_for_high_priority_writes, 0);
    // Read-ahead
    assert_eq!(config.db_storage_read_ahead_cache_batch_size, 100);
    // Auditor
    assert_eq!(config.auditor_periodic_check_interval, 604800);
    assert_eq!(config.rw_rereplicate_backoff_ms, 5000);
    assert_eq!(config.open_ledger_rereplication_grace_period, 30000);
    assert_eq!(config.lock_release_of_failed_ledger_grace_period, 300000);
    // ZK retry
    assert_eq!(config.zk_retry_backoff_start_ms, 1000);
    // HTTP
    assert_eq!(config.http_server_host, "0.0.0.0");
    assert!(!config.http_server_tls_enable);
    // Data integrity
    assert_eq!(config.local_scrub_interval, 0);
    assert!(!config.local_consistency_check_on_startup);
    assert!(!config.data_integrity_checking);
    // Identity
    assert_eq!(config.bookie_id, None);
    // Statistics
    assert!(config.enable_statistics);
    // Not yet wired
    assert_eq!(config.page_size, 8192);
    assert_eq!(config.page_limit, -1);
    assert!(config.sorted_ledger_storage_enabled);
    assert_eq!(config.server_tcp_linger, 0);
    assert!(!config.allow_ephemeral_ports);
    assert!(!config.sanity_check_metrics_enabled);
}

#[test]
fn server_config_new_fields_from_properties() {
    let text = "\
maxAddsInProgressLimit=5000\n\
maxReadsInProgressLimit=8000\n\
skipReplayJournalInvalidRecord=true\n\
forceReadOnlyBookie=true\n\
persistBookieStatusEnabled=true\n\
auditorPeriodicCheckInterval=86400\n\
httpServerHost=127.0.0.1\n\
dbStorage_readAheadCacheBatchSize=200\n\
localScrubInterval=3600\n\
";
    let props = parse_properties(text);
    let config = ServerConfig::from_properties(&props);
    assert_eq!(config.max_adds_in_progress_limit, 5000);
    assert_eq!(config.max_reads_in_progress_limit, 8000);
    assert!(config.skip_replay_journal_invalid_record);
    assert!(config.force_read_only_bookie);
    assert!(config.persist_bookie_status_enabled);
    assert_eq!(config.auditor_periodic_check_interval, 86400);
    assert_eq!(config.http_server_host, "127.0.0.1");
    assert_eq!(config.db_storage_read_ahead_cache_batch_size, 200);
    assert_eq!(config.local_scrub_interval, 3600);
}

#[test]
fn client_config_new_fields_defaults() {
    let config = ClientConfig::default();
    assert_eq!(config.client_connect_timeout_millis, 10000);
    assert_eq!(config.first_speculative_read_lac_timeout, 1500);
    assert_eq!(config.max_speculative_read_lac_timeout, 5000);
    assert!(config.client_tcp_no_delay);
    assert!(!config.bookie_health_check_enabled);
    assert_eq!(config.bookie_quarantine_time_seconds, 1800);
    assert!(config.reorder_read_sequence_enabled);
    assert!(!config.delay_ensemble_change);
    assert_eq!(config.throttle, 5000);
    assert!(config.enable_bookie_failure_tracking);
    assert_eq!(config.bookie_failure_history_expiration_ms, 60000);
    assert_eq!(config.request_timeout_sec, 30);
    assert!(!config.deferred_sync);
    assert!(!config.batch_read_enabled);
    // New IMPORTANT fields
    assert_eq!(config.timeout_monitor_interval_sec, 0);
    assert_eq!(config.wait_timeout_on_backpressure_ms, -1);
    assert_eq!(config.start_tls_timeout_secs, 10);
    assert_eq!(config.client_write_buffer_low_water_mark, 393216);
    assert_eq!(config.client_write_buffer_high_water_mark, 524288);
    assert!(!config.bookie_connection_error_quarantine_enabled);
    assert_eq!(config.get_bookie_info_interval_seconds, 86400);
    assert_eq!(config.client_role, "standard");
    assert_eq!(config.client_auth_provider_factory_class, None);
    assert!(!config.recovery_batch_read_enabled);
    // Not yet wired
    assert_eq!(config.pcbc_timeout_timer_tick_duration_ms, 100);
    assert_eq!(config.bookie_quarantine_ratio, 1.0);
    assert!(!config.disk_weight_based_placement_enabled);
    assert!(!config.opportunistic_striping);
    assert!(config.netty_use_pooled_buffers);
    assert_eq!(config.tcp_keep_idle, -1);
    assert_eq!(config.client_tcp_user_timeout_millis, -1);
}

#[test]
fn server_config_comprehensive_unused() {
    // A comprehensive Java bk_server.conf with every known property key.
    // Verifies that ALL properties parse without error.
    let text = "\
bookiePort=3181\n\
advertisedAddress=10.0.0.1\n\
journalDirectory=/data/journal\n\
ledgerDirectories=/data/ledger\n\
journalMaxSizeMB=2048\n\
journalMaxBackups=5\n\
journalSyncData=true\n\
journalMaxGroupWaitMSec=2\n\
journalBufferedWritesThreshold=524288\n\
journalAlignmentSize=4096\n\
journalFormatVersionToWrite=6\n\
journalFlushWhenQueueEmpty=true\n\
journalRemoveFromPageCache=true\n\
journalMaxMemorySizeMb=32\n\
journalPreAllocSizeMB=16\n\
journalWriteBufferSizeKB=64\n\
journalReuseFiles=false\n\
journalQueueSize=10000\n\
journalPageCacheFlushIntervalMSec=1000\n\
numJournalCallbackThreads=2\n\
skipReplayJournalInvalidRecord=false\n\
logSizeLimit=1073741823\n\
flushInterval=10000\n\
dbStorage_writeCacheMaxSizeMb=256\n\
dbStorage_readAheadCacheMaxSizeMb=64\n\
dbStorage_readAheadCacheBatchSize=100\n\
dbStorage_readAheadCacheBatchBytesSize=262144\n\
minorCompactionInterval=3600\n\
minorCompactionThreshold=0.2\n\
majorCompactionInterval=86400\n\
majorCompactionThreshold=0.8\n\
gcWaitTime=600000\n\
compactionRateByBytes=1000000\n\
compactionMaxTimeMillis=60000\n\
minorCompactionMaxTimeMillis=30000\n\
majorCompactionMaxTimeMillis=120000\n\
compactionMaxOutstandingRequests=5000\n\
isForceGCAllowWhenNoSpace=true\n\
gcEntryLogMetadataCacheEnabled=true\n\
diskUsageThreshold=0.95\n\
diskUsageWarnThreshold=0.90\n\
diskUsageLwmThreshold=0.90\n\
diskCheckInterval=10000\n\
readOnlyModeEnabled=true\n\
forceReadOnlyBookie=false\n\
persistBookieStatusEnabled=true\n\
readOnlyModeOnAnyDiskFullEnabled=true\n\
maxAddsInProgressLimit=10000\n\
maxReadsInProgressLimit=10000\n\
maxConnections=50000\n\
closeChannelOnResponseTimeout=30000\n\
serverTcpNoDelay=true\n\
numAddWorkerThreads=4\n\
numReadWorkerThreads=8\n\
numHighPriorityWorkerThreads=8\n\
numLongPollWorkerThreads=0\n\
readWorkerThreadsThrottlingEnabled=true\n\
serverNumAcceptorThreads=1\n\
waitTimeoutOnResponseBackpressureMs=5000\n\
requestTimerTickDurationMs=10\n\
requestTimerNumTicks=1024\n\
autoRecoveryDaemonEnabled=true\n\
lostBookieRecoveryDelay=1800\n\
auditorPeriodicCheckInterval=604800\n\
auditorPeriodicBookieCheckInterval=86400\n\
auditorPeriodicPlacementPolicyCheckInterval=0\n\
rwRereplicateBackoffMs=5000\n\
openLedgerRereplicationGracePeriod=30000\n\
lockReleaseOfFailedLedgerGracePeriod=300000\n\
zkServers=zk1:2181,zk2:2181\n\
zkLedgersRootPath=/ledgers\n\
zkTimeout=10000\n\
zkRetryBackoffStartMs=1000\n\
zkRetryBackoffMaxMs=10000\n\
httpServerEnabled=true\n\
httpServerPort=8080\n\
httpServerHost=0.0.0.0\n\
httpServerTlsEnable=false\n\
enableStatistics=true\n\
localScrubInterval=0\n\
dataIntegrityChecking=false\n\
bookieId=bookie-1\n\
openFileLimit=20000\n\
minUsableSizeForHighPriorityWrites=1048576\n\
pageSize=8192\n\
sortedLedgerStorageEnabled=true\n\
serverTcpLinger=0\n\
allowEphemeralPorts=false\n\
";
    let props = parse_properties(text);
    let config = ServerConfig::from_properties(&props);
    // Spot-check a few across categories
    assert_eq!(config.bookie_port, 3181);
    assert!(config.journal_sync_data);
    assert_eq!(config.minor_compaction_max_time_ms, 30000);
    assert_eq!(config.major_compaction_max_time_ms, 120000);
    assert_eq!(config.compaction_max_outstanding_requests, 5000);
    assert!(config.is_force_gc_allow_when_no_space);
    assert_eq!(config.journal_queue_size, 10000);
    assert_eq!(config.num_journal_callback_threads, 2);
    assert!(config.persist_bookie_status_enabled);
    assert_eq!(config.max_connections, 50000);
    assert_eq!(config.wait_timeout_on_response_backpressure_ms, 5000);
    assert_eq!(config.open_ledger_rereplication_grace_period, 30000);
    assert_eq!(config.bookie_id, Some("bookie-1".to_string()));
    assert_eq!(config.min_usable_size_for_high_priority_writes, 1048576);
    assert!(config.enable_statistics);
}

#[test]
fn client_config_new_fields_from_properties() {
    let text = "\
bookieHealthCheckEnabled=true\n\
bookieQuarantineTimeSeconds=600\n\
clientConnectTimeoutMillis=5000\n\
firstSpeculativeReadLACTimeout=2000\n\
ensemblePlacementPolicy=ZoneAware\n\
delayEnsembleChange=true\n\
throttle=10000\n\
enableBookieFailureTracking=false\n\
";
    let props = parse_properties(text);
    let config = ClientConfig::from_properties(&props);
    assert!(config.bookie_health_check_enabled);
    assert_eq!(config.bookie_quarantine_time_seconds, 600);
    assert_eq!(config.client_connect_timeout_millis, 5000);
    assert_eq!(config.first_speculative_read_lac_timeout, 2000);
    assert_eq!(config.ensemble_placement_policy, "ZoneAware");
    assert!(config.delay_ensemble_change);
    assert_eq!(config.throttle, 10000);
    assert!(!config.enable_bookie_failure_tracking);
}

// ---------------------------------------------------------------------------
// Config validation tests
// ---------------------------------------------------------------------------

#[test]
fn valid_default_config_passes_validation() {
    let config = ServerConfig::default();
    assert!(config.validate().is_ok(), "default config should be valid");
}

#[test]
fn invalid_journal_alignment_too_small() {
    let mut config = ServerConfig::default();
    config.journal_alignment_size = 100;
    let result = config.validate();
    assert!(result.is_err());
    let errs = result.unwrap_err();
    assert!(errs.iter().any(|e| e.contains("journalAlignmentSize") && e.contains(">= 512")));
}

#[test]
fn invalid_journal_alignment_not_divisible() {
    let mut config = ServerConfig::default();
    config.journal_alignment_size = 1000; // >= 512 but not divisible by 512
    let result = config.validate();
    assert!(result.is_err());
    let errs = result.unwrap_err();
    assert!(errs.iter().any(|e| e.contains("divisible by 512")));
}

#[test]
fn invalid_bookie_port_zero_without_ephemeral() {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.allow_ephemeral_ports = false;
    let result = config.validate();
    assert!(result.is_err());
    let errs = result.unwrap_err();
    assert!(errs.iter().any(|e| e.contains("bookiePort")));
}

#[test]
fn valid_bookie_port_zero_with_ephemeral() {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.allow_ephemeral_ports = true;
    assert!(config.validate().is_ok());
}

#[test]
fn invalid_minor_compaction_below_gc_wait() {
    let mut config = ServerConfig::default();
    config.minor_compaction_interval = 1; // 1s = 1000ms
    config.gc_wait_time = 600_000; // 600s
    let result = config.validate();
    assert!(result.is_err());
    let errs = result.unwrap_err();
    assert!(errs.iter().any(|e| e.contains("minorCompactionInterval")));
}

#[test]
fn invalid_journal_format_version() {
    let mut config = ServerConfig::default();
    config.journal_format_version_to_write = 4;
    let result = config.validate();
    assert!(result.is_err());
    let errs = result.unwrap_err();
    assert!(errs.iter().any(|e| e.contains("journalFormatVersionToWrite")));
}

#[test]
fn invalid_empty_ledger_directories() {
    let mut config = ServerConfig::default();
    config.ledger_directories = vec![];
    let result = config.validate();
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Missing config options parsing tests
// ---------------------------------------------------------------------------

#[test]
fn parse_metadata_service_uri_extracts_zk_servers() {
    let props = parse_properties(
        "metadataServiceUri=zk+hierarchical://host1:2181,host2:2181/ledgers\n"
    );
    let config = ServerConfig::from_properties(&props);
    assert_eq!(config.metadata_service_uri.as_deref(), Some("zk+hierarchical://host1:2181,host2:2181/ledgers"));
    // Should also populate zk_servers if not explicitly set
    assert_eq!(config.zk_servers.as_deref(), Some("host1:2181,host2:2181"));
}

#[test]
fn zk_servers_takes_precedence_over_metadata_uri() {
    let props = parse_properties(
        "zkServers=explicit:2181\nmetadataServiceUri=zk://implicit:2181/ledgers\n"
    );
    let config = ServerConfig::from_properties(&props);
    assert_eq!(config.zk_servers.as_deref(), Some("explicit:2181"));
}

#[test]
fn parse_new_config_options() {
    let props = parse_properties(
        "minNumRacksPerWriteQuorum=3\n\
         enforceMinNumRacksPerWriteQuorum=true\n\
         enforceMinNumFaultDomainsForWrite=true\n\
         permittedStartupUsers=bk,root\n\
         auditorReplicasCheckInterval=3600\n\
         auditorMaxNumberOfConcurrentOpenLedgerOperations=1000\n\
         bookieDeathWatchInterval=2000\n"
    );
    let config = ServerConfig::from_properties(&props);
    assert_eq!(config.min_num_racks_per_write_quorum, 3);
    assert!(config.enforce_min_num_racks_per_write_quorum);
    assert!(config.enforce_min_num_fault_domains_for_write);
    assert_eq!(config.permitted_startup_users.as_deref(), Some("bk,root"));
    assert_eq!(config.auditor_replicas_check_interval, 3600);
    assert_eq!(config.auditor_max_concurrent_open_ledger_operations, 1000);
    assert_eq!(config.bookie_death_watch_interval, 2000);
}

// ---------------------------------------------------------------------------
// Java config key alias tests
// ---------------------------------------------------------------------------

#[test]
fn client_config_java_key_aliases() {
    // Java uses these key names; Rust must accept them for config compatibility.
    let text = "\
addEntryTimeoutSec=10\n\
readEntryTimeoutSec=15\n\
addEntryQuorumTimeoutSec=8\n\
explicitLacInterval=200\n\
maxNumEnsembleChanges=5\n\
";
    let props = parse_properties(text);
    let config = ClientConfig::from_properties(&props);
    assert_eq!(config.add_entry_timeout, 10);
    assert_eq!(config.read_timeout, 15);
    assert_eq!(config.add_entry_quorum_timeout, 8);
    assert_eq!(config.explicit_lac_interval_ms, 200);
    assert_eq!(config.max_allowed_ensemble_changes, 5);
}

#[test]
fn client_config_rust_keys_override_java_aliases() {
    // When both Java and Rust key names are present, Rust-native key wins
    // (it's parsed after the Java alias).
    let text = "\
addEntryTimeoutSec=10\n\
addEntryTimeout=20\n\
readEntryTimeoutSec=15\n\
readTimeout=25\n\
explicitLacInterval=100\n\
explicitLacIntervalMs=200\n\
maxNumEnsembleChanges=5\n\
maxAllowedEnsembleChanges=10\n\
";
    let props = parse_properties(text);
    let config = ClientConfig::from_properties(&props);
    assert_eq!(config.add_entry_timeout, 20);
    assert_eq!(config.read_timeout, 25);
    assert_eq!(config.explicit_lac_interval_ms, 200);
    assert_eq!(config.max_allowed_ensemble_changes, 10);
}

// ---------------------------------------------------------------------------
// Configuration gap fixes (section 2.3)
// ---------------------------------------------------------------------------

#[test]
fn server_defaults_match_java() {
    let config = ServerConfig::default();
    // I21: journalAlignmentSize matches Java default of 512
    assert_eq!(config.journal_alignment_size, 512);
    // I22: compactionRateByBytes matches Java default of 1MB/s
    assert_eq!(config.compaction_rate_bytes_per_sec, 1_000_000);
    // C5 (prior fix): journalSyncData matches Java default of true
    assert!(config.journal_sync_data);
    // C7: autoRecoveryDaemonEnabled matches Java default of false
    assert!(!config.auto_recovery_daemon_enabled);
    // C8: lostBookieRecoveryDelay matches Java default of 0
    assert_eq!(config.lost_bookie_recovery_delay, 0);
    // C9: maxReadsInProgressLimit matches Java default of 10000
    assert_eq!(config.max_reads_in_progress_limit, 10000);
    // I27: maxBatchReadSize in bytes (Java semantic), default 5MB
    assert_eq!(config.max_batch_read_size_bytes, 5 * 1024 * 1024);
    assert_eq!(config.max_batch_read_count, 10_000);
}

#[test]
fn client_defaults_match_java() {
    let config = ClientConfig::default();
    // I23: addEntryQuorumTimeout matches Java default of -1 (disabled)
    assert_eq!(config.add_entry_quorum_timeout, -1);
    // I24: client buffer sizes match Java default of 1MB
    assert_eq!(config.client_send_buffer_size, 1048576);
    assert_eq!(config.client_receive_buffer_size, 1048576);
}

#[test]
fn new_rocksdb_tuning_configs_parse() {
    let text = "\
dbStorage_rocksDB_blockCacheSize=134217728\n\
dbStorage_rocksDB_sstSizeInMB=128\n\
dbStorage_rocksDB_numLevels=7\n\
dbStorage_rocksDB_numFilesInLevel0=8\n\
dbStorage_rocksDB_maxSizeInLevel1MB=512\n\
";
    let props = parse_properties(text);
    let config = ServerConfig::from_properties(&props);
    assert_eq!(config.db_storage_rocksdb_block_cache_size, 134217728);
    assert_eq!(config.db_storage_rocksdb_sst_size_mb, 128);
    assert_eq!(config.db_storage_rocksdb_num_levels, 7);
    assert_eq!(config.db_storage_rocksdb_num_files_in_level0, 8);
    assert_eq!(config.db_storage_rocksdb_max_size_in_level1_mb, 512);
}

#[test]
fn new_rocksdb_tuning_configs_defaults() {
    let config = ServerConfig::default();
    assert_eq!(config.db_storage_rocksdb_block_cache_size, -1); // auto
    assert_eq!(config.db_storage_rocksdb_sst_size_mb, 64);
    assert_eq!(config.db_storage_rocksdb_num_levels, -1); // RocksDB default
    assert_eq!(config.db_storage_rocksdb_num_files_in_level0, 4);
    assert_eq!(config.db_storage_rocksdb_max_size_in_level1_mb, 256);
}

#[test]
fn stats_config_parse() {
    let text = "\
prometheusStatsHttpPort=9090\n\
statsProviderClass=org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider\n\
";
    let props = parse_properties(text);
    let config = ServerConfig::from_properties(&props);
    assert_eq!(config.prometheus_stats_http_port, 9090);
    assert_eq!(
        config.stats_provider_class.as_deref(),
        Some("org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider")
    );
}

#[test]
fn stats_config_defaults() {
    let config = ServerConfig::default();
    assert_eq!(config.prometheus_stats_http_port, 0); // disabled
    assert_eq!(config.stats_provider_class, None);
}

#[test]
fn max_batch_read_size_bytes_from_java_config() {
    // Java config files use maxBatchReadSize in bytes
    let text = "maxBatchReadSize=10485760\n"; // 10MB
    let props = parse_properties(text);
    let config = ServerConfig::from_properties(&props);
    assert_eq!(config.max_batch_read_size_bytes, 10485760);
    // Entry count default should be unaffected
    assert_eq!(config.max_batch_read_count, 10_000);
}

#[test]
fn max_batch_read_count_parse() {
    let text = "maxBatchReadCount=5000\n";
    let props = parse_properties(text);
    let config = ServerConfig::from_properties(&props);
    assert_eq!(config.max_batch_read_count, 5000);
}

// ---------------------------------------------------------------------------
// Config parity items: env overrides, boolean variants, fixed types/defaults.
// ---------------------------------------------------------------------------

#[test]
fn close_channel_on_response_timeout_parses_as_bool() {
    // C2: field must be a bool, not u64. A Java-shaped "=true" must stick
    // (previously it silently failed to parse and stayed at 0/false).
    let text = "closeChannelOnResponseTimeout=true\n";
    let props = parse_properties(text);
    let config = ServerConfig::from_properties(&props);
    assert!(config.close_channel_on_response_timeout);

    let text = "closeChannelOnResponseTimeout=false\n";
    let props = parse_properties(text);
    let config = ServerConfig::from_properties(&props);
    assert!(!config.close_channel_on_response_timeout);
}

#[test]
fn compaction_and_page_cache_defaults_match_java() {
    // C3 + C4: two defaults drifted from Java. Lock them down so future
    // edits don't silently regress operator-visible behaviour.
    let config = ServerConfig::default();
    assert_eq!(
        config.compaction_max_outstanding_requests, 100_000,
        "compactionMaxOutstandingRequests must match Java default"
    );
    assert!(
        config.journal_remove_from_page_cache,
        "journalRemoveFromPageCache must default true to match Java"
    );
}

#[test]
fn parse_bool_accepts_java_forms_in_server_config() {
    // C5: yes/y/on/1 should all parse as true; garbage as false.
    for (raw, expect) in [
        ("true", true),
        ("Yes", true),
        ("y", true),
        ("on", true),
        ("1", true),
        ("false", false),
        ("no", false),
        ("0", false),
        ("garbage", false),
    ] {
        let text = format!("journalSyncData={raw}\n");
        let props = parse_properties(&text);
        let config = ServerConfig::from_properties(&props);
        assert_eq!(
            config.journal_sync_data, expect,
            "journalSyncData={raw:?} expected {expect}"
        );
    }
}

#[test]
fn env_overrides_apply_on_top_of_file() {
    // C1: env vars prefixed with `BK_` take precedence over the file.
    use bookkeeper_config::properties::{apply_env_overrides_from, parse_properties};
    let text = "bookiePort=3181\njournalDirectory=/data/journal\n";
    let mut props = parse_properties(text);
    // Simulate `BK_bookiePort=3281` + a new key that wasn't in the file.
    apply_env_overrides_from(&mut props, vec![
        ("BK_bookiePort".into(), "3281".into()),
        ("BK_advertisedAddress".into(), "10.0.0.5".into()),
        ("PATH".into(), "/usr/bin".into()), // unrelated env must not leak
    ]);
    let config = ServerConfig::from_properties(&props);
    assert_eq!(config.bookie_port, 3281);
    assert_eq!(config.advertised_address.as_deref(), Some("10.0.0.5"));
    // File value that was NOT overridden should be untouched.
    assert_eq!(config.journal_directory, "/data/journal");
}

#[test]
fn env_overrides_work_for_client_config() {
    use bookkeeper_config::properties::{apply_env_overrides_from, parse_properties};
    let text = "zkTimeout=30000\n";
    let mut props = parse_properties(text);
    apply_env_overrides_from(&mut props, vec![
        ("BK_zkTimeout".into(), "60000".into()),
        ("BK_metadataServiceUri".into(), "zk+hierarchical://zk1:2181/ledgers".into()),
    ]);
    let config = ClientConfig::from_properties(&props);
    assert_eq!(config.zk_timeout, 60000);
    assert_eq!(
        config.metadata_service_uri.as_deref(),
        Some("zk+hierarchical://zk1:2181/ledgers")
    );
}
