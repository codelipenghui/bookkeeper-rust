use std::collections::HashMap;
use std::path::Path;

use crate::properties::parse_bool;

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
    pub journal_write_data: bool,
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

    /// RocksDB block cache size in bytes. -1 = auto (10% of direct memory / ledgers).
    /// Matches Java's `dbStorage_rocksDB_blockCacheSize`. Default -1.
    pub db_storage_rocksdb_block_cache_size: i64,
    /// Target SST file size in MB. Matches Java's `dbStorage_rocksDB_sstSizeInMB`. Default 64.
    pub db_storage_rocksdb_sst_size_mb: i32,
    /// Number of RocksDB levels. -1 = RocksDB default.
    /// Matches Java's `dbStorage_rocksDB_numLevels`. Default -1.
    pub db_storage_rocksdb_num_levels: i32,
    /// Number of files in level 0 before triggering compaction.
    /// Matches Java's `dbStorage_rocksDB_numFilesInLevel0`. Default 4.
    pub db_storage_rocksdb_num_files_in_level0: i32,
    /// Max size of level 1 in MB. Matches Java's `dbStorage_rocksDB_maxSizeInLevel1MB`. Default 256.
    pub db_storage_rocksdb_max_size_in_level1_mb: i32,

    /// Path to a Java BK-style `entry_location_rocksdb.conf` (INI). When
    /// set, its contents override the built-in defaults for the entry
    /// location index's RocksDB. Config key: `entryLocationRocksdbConf`.
    pub entry_location_rocksdb_conf: Option<String>,
    /// Path to a Java BK-style `ledger_metadata_rocksdb.conf`. See
    /// [`entry_location_rocksdb_conf`]. Config key: `ledgerMetadataRocksdbConf`.
    pub ledger_metadata_rocksdb_conf: Option<String>,

    // Runtime
    /// Number of server I/O threads (tokio workers). Handles TCP connections
    /// and request processing. 0 = 2 × CPU cores (matching Java's default).
    /// Config key: serverNumIOThreads
    pub server_num_io_threads: usize,

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
    pub tls_keystore_type: String,
    pub tls_enabled_cipher_suites: Option<String>,
    pub tls_enabled_protocols: Option<String>,
    pub tls_cert_refresh_seconds: i64,

    // Authentication
    pub bookie_auth_provider_factory_class: Option<String>,

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
    pub compaction_rate_bytes_per_sec: u64,
    pub compaction_max_time_ms: u64,
    pub gc_entry_log_metadata_cache_enabled: bool,
    /// Separate minor compaction max time. -1 = use compaction_max_time_ms.
    pub minor_compaction_max_time_ms: i64,
    /// Separate major compaction max time. -1 = use compaction_max_time_ms.
    pub major_compaction_max_time_ms: i64,
    /// Max concurrent compaction outstanding requests.
    pub compaction_max_outstanding_requests: i32,
    /// Force GC even when disk is full (no space).
    pub is_force_gc_allow_when_no_space: bool,

    // Journal (extended)
    /// Max journal queue memory in MB. 0 = default (32MB).
    pub journal_max_queue_memory_mb: usize,
    /// Flush immediately when queue is empty after draining.
    pub journal_flush_when_queue_empty: bool,
    /// Call posix_fadvise(DONTNEED) after journal writes. Linux-only.
    pub journal_remove_from_page_cache: bool,

    // Request Limits — wired to processor.rs semaphores
    /// Max concurrent add requests. 0 = use default (10000).
    pub max_adds_in_progress_limit: usize,
    /// Max concurrent read requests. 0 = use default (10000).
    pub max_reads_in_progress_limit: usize,
    /// Max concurrent connections. 0 = unlimited.
    pub max_connections: usize,
    /// When the server times out sending a response (see
    /// `waitTimeoutOnResponseBackpressureMs`), either close the channel
    /// and drop the response (true) or just drop the response (false).
    /// Matches Java's boolean `closeChannelOnResponseTimeout`. Was
    /// incorrectly typed as `u64` before: `=true` in a Java-shaped
    /// config would silently fail to parse and be ignored.
    pub close_channel_on_response_timeout: bool,

    // Journal (extended)
    /// Skip invalid records during journal replay instead of aborting.
    pub skip_replay_journal_invalid_record: bool,
    /// After an unclean shutdown (DIRTY file present), scan the entry
    /// location index and verify each entry is readable at its recorded
    /// offset. Catches torn writes / half-applied flushes that journal
    /// replay alone cannot detect. Default: false (matches Java, which
    /// only surfaces the unclean flag but doesn't automatically scan).
    pub verify_storage_on_unclean_shutdown: bool,
    /// Pre-allocate journal files (MB). 0 = no pre-allocation.
    pub journal_pre_alloc_size_mb: i32,
    /// Journal write buffer size (KB).
    pub journal_write_buffer_size_kb: i32,
    /// Journal reuse files instead of creating new ones.
    pub journal_reuse_files: bool,
    /// Journal queue size (item count). 0 = use memory-based limit.
    pub journal_queue_size: i32,
    /// Journal page cache flush interval (ms). 0 = disabled.
    pub journal_page_cache_flush_interval_ms: i32,
    /// Number of journal callback threads.
    pub num_journal_callback_threads: i32,

    // GC (extended)
    /// Force allow compaction even when suspended.
    pub force_allow_compaction: bool,
    /// Throttle compaction by bytes (true) or entries (false).
    pub is_throttle_by_bytes: bool,
    /// Verify metadata consistency during GC.
    pub verify_metadata_on_gc: bool,
    /// Use transactional compaction for atomicity.
    pub use_transactional_compaction: bool,

    // Disk Management (extended)
    /// Force bookie to start in read-only mode.
    pub force_read_only_bookie: bool,
    /// Persist bookie status (Running/ReadOnly) to disk.
    pub persist_bookie_status_enabled: bool,
    /// Trigger read-only when ANY single disk is full (not just aggregate).
    pub read_only_mode_on_any_disk_full_enabled: bool,

    // Network (extended)
    /// Server write buffer low water mark (bytes).
    pub server_write_buffer_low_water_mark: usize,
    /// Server write buffer high water mark (bytes).
    pub server_write_buffer_high_water_mark: usize,
    /// Max pending read requests per thread.
    pub max_pending_read_requests_per_thread: i32,
    /// Max pending add requests per thread.
    pub max_pending_add_requests_per_thread: i32,
    /// Number of long poll worker threads. 0 = shared with read workers.
    pub num_long_poll_worker_threads: i32,
    /// Number of high priority worker threads.
    pub num_high_priority_worker_threads: i32,

    // DbLedgerStorage (extended)
    /// Read-ahead batch size for cache miss.
    pub db_storage_read_ahead_cache_batch_size: i32,
    /// Read-ahead max bytes per batch.
    pub db_storage_read_ahead_cache_batch_bytes_size: i32,
    /// Max time an add_entry request waits for a write-cache flush before
    /// being rejected with `OperationRejected`. Matches Java's
    /// `dbStorage_maxThrottleTimeMs` — prevents slow flushes from blocking
    /// all write processing indefinitely. 0 → default (10 000 ms).
    pub db_storage_max_throttle_time_ms: i64,
    /// Max batch read size in bytes. Matches Java's `maxBatchReadSize` semantic.
    /// Default 5MB (5242880). Config key: `maxBatchReadSize`.
    pub max_batch_read_size_bytes: i64,
    /// Max batch read entry count. Rust-specific limit to cap DoS risk.
    /// Default 10000. Config key: `maxBatchReadCount`.
    pub max_batch_read_count: i32,

    // Replication / Auditor
    pub auto_recovery_daemon_enabled: bool,
    pub lost_bookie_recovery_delay: i32,
    /// Auditor periodic check interval for all-ledgers scan (seconds). 0 = disabled.
    pub auditor_periodic_check_interval: i64,
    /// Auditor periodic bookie check interval (seconds).
    pub auditor_periodic_bookie_check_interval: i64,
    /// Auditor placement policy check interval (seconds). 0 = disabled.
    pub auditor_periodic_placement_policy_check_interval: i64,
    /// Auditor replicas-check interval (seconds). 0 = disabled.
    /// Matches Java's `auditorPeriodicReplicasCheckInterval` — probes
    /// each bookie for each entry to detect silent partial data loss.
    pub auditor_periodic_replicas_check_interval: i64,
    /// Replication worker rereplicate backoff (ms).
    pub rw_rereplicate_backoff_ms: i64,

    // ZooKeeper
    pub zk_servers: Option<String>,
    pub zk_ledger_root: String,
    pub zk_timeout: i32,
    /// ZK retry backoff start (ms).
    pub zk_retry_backoff_start_ms: i32,
    /// ZK retry backoff max (ms).
    pub zk_retry_backoff_max_ms: i32,

    // HTTP Admin
    pub http_server_enabled: bool,
    pub http_server_port: i32,
    /// HTTP server bind host.
    pub http_server_host: String,

    // Data Integrity
    pub local_scrub_interval: i64,
    pub local_scrub_rate_limit: i64,
    pub local_consistency_check_on_startup: bool,
    /// Enable data integrity checking on startup.
    pub data_integrity_checking: bool,
    /// Stamp missing cookies during data integrity check.
    pub data_integrity_stamp_missing_cookies: bool,

    // Server identity (extended)
    /// Explicit bookie ID overriding hostname. Matches Java's `bookieId`.
    pub bookie_id: Option<String>,

    // Storage (extended)
    /// Max open file descriptors.
    pub open_file_limit: i32,
    /// Min disk space (bytes) for high-priority (recovery) writes in readonly mode.
    pub min_usable_size_for_high_priority_writes: i64,
    /// Min disk space (bytes) for entry log creation.
    pub min_usable_size_for_entry_log_creation: i64,

    // Network (extended)
    /// Number of acceptor threads.
    pub server_num_acceptor_threads: i32,
    /// Timeout (ms) waiting on response backpressure. -1 = infinite.
    pub wait_timeout_on_response_backpressure_ms: i64,
    /// Enable read worker thread throttling.
    pub read_worker_threads_throttling_enabled: bool,
    /// Request timer tick duration (ms). Netty timer wheel — parsed for compat.
    pub request_timer_tick_duration_ms: i32,
    /// Request timer num ticks. Netty timer wheel — parsed for compat.
    pub request_timer_num_ticks: i32,

    // Auditor (extended)
    /// Grace period (ms) before re-replicating open ledgers.
    pub open_ledger_rereplication_grace_period: i64,
    /// Grace period (ms) for releasing lock on failed ledger.
    pub lock_release_of_failed_ledger_grace_period: i64,

    // HTTP TLS
    /// Enable TLS for HTTP admin server.
    pub http_server_tls_enable: bool,
    /// Keystore path for HTTP TLS.
    pub http_server_key_store_path: Option<String>,
    /// Keystore password for HTTP TLS.
    pub http_server_key_store_password: Option<String>,
    /// Truststore path for HTTP TLS.
    pub http_server_trust_store_path: Option<String>,
    /// Truststore password for HTTP TLS.
    pub http_server_trust_store_password: Option<String>,

    // Statistics
    /// Enable statistics collection.
    pub enable_statistics: bool,
    /// Prometheus stats HTTP port. When set, exposes a standalone metrics endpoint
    /// on this port (separate from the HTTP admin server). Matches Java's
    /// `prometheusStatsHttpPort`. 0 = disabled (use HTTP admin /metrics instead).
    pub prometheus_stats_http_port: i32,
    /// Stats provider class name. Parsed for Java config compatibility.
    /// Rust always uses Prometheus. Matches Java's `statsProviderClass`.
    pub stats_provider_class: Option<String>,

    // ------------------------------------------------------------------
    // Parsed for config-file compatibility, not yet wired in Rust.
    // TODO: wire these as the relevant subsystems are implemented.
    // ------------------------------------------------------------------

    // InterleavedLedgerStorage settings — Rust only implements DbLedgerStorage.
    pub page_size: i32,
    pub page_limit: i32,
    pub file_info_cache_initial_capacity: i32,
    pub file_info_max_idle_time: i64,
    pub file_info_format_version_to_write: i32,
    pub sorted_ledger_storage_enabled: bool,
    pub skip_list_size_limit: i64,
    pub skip_list_arena_chunk_size: i32,
    pub skip_list_arena_max_alloc_size: i32,

    // Network — not yet applied to socket options.
    pub server_tcp_linger: i32,
    pub allow_ephemeral_ports: bool,
    pub enable_local_transport: bool,
    pub disable_server_socket_bind: bool,

    // Buffer allocator — not applicable (no Netty ByteBuf), parsed for compat.
    pub byte_buf_allocator_size_initial: i32,
    pub byte_buf_allocator_size_min: i32,
    pub byte_buf_allocator_size_max: i32,

    // Misc — not yet wired.
    pub extra_server_components: Option<String>,
    pub sanity_check_metrics_enabled: bool,
    pub enable_task_execution_stats: bool,
    pub authorized_roles: Option<String>,
    pub read_buffer_size_bytes: i32,
    pub write_buffer_size_bytes: i32,

    // ------------------------------------------------------------------
    // Missing config options identified in Java compatibility audit
    // ------------------------------------------------------------------

    /// Metadata service URI (alternative to zkServers). e.g. "zk+hierarchical://host:2181/ledgers"
    /// Matches Java's `metadataServiceUri`.
    pub metadata_service_uri: Option<String>,
    /// Min number of racks per write quorum for rack-aware placement.
    /// Matches Java's `minNumRacksPerWriteQuorum`. Default 2.
    pub min_num_racks_per_write_quorum: i32,
    /// Enforce min rack count strictly (reject if not met).
    /// Matches Java's `enforceMinNumRacksPerWriteQuorum`. Default false.
    pub enforce_min_num_racks_per_write_quorum: bool,
    /// Delay write completion until acked bookies span enough fault domains.
    /// Matches Java's `enforceMinNumFaultDomainsForWrite`. Default false.
    pub enforce_min_num_fault_domains_for_write: bool,
    /// Comma-separated list of OS users permitted to start the bookie.
    /// Empty = any user. Matches Java's `permittedStartupUsers`.
    pub permitted_startup_users: Option<String>,
    /// Auto-repair placement policy violations during replication.
    /// Matches Java's `repairedPlacementPolicyNotAdheringBookieEnabled`. Default false.
    pub repaired_placement_policy_not_adhering_bookie_enabled: bool,
    /// Auditor periodic replicas availability check interval (seconds). 0 = disabled.
    /// Matches Java's `auditorReplicasCheckInterval`. Default 0.
    pub auditor_replicas_check_interval: i64,
    /// Max concurrent open ledger operations in auditor. Default 500.
    /// Matches Java's `auditorMaxNumberOfConcurrentOpenLedgerOperations`.
    pub auditor_max_concurrent_open_ledger_operations: i32,
    /// Bookie death watch interval (ms). 0 = disabled.
    /// Matches Java's `bookieDeathWatchInterval`. Default 1000.
    pub bookie_death_watch_interval: i64,
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
            allow_storage_expansion: false,
            journal_max_size_mb: 2048,
            journal_max_backups: 5,
            journal_write_data: true,
            journal_sync_data: true,
            journal_adaptive_group_writes: true,
            journal_max_group_wait_ms: 2,
            journal_buffered_writes_threshold: 524288,
            journal_buffered_entries_threshold: 0,
            journal_alignment_size: 512,
            journal_format_version_to_write: 6,
            flush_interval: 10000,
            log_size_limit: 1073741823,
            entry_log_per_ledger_enabled: false,
            db_storage_write_cache_max_size_mb: 64,
            db_storage_read_cache_max_size_mb: 64,
            db_storage_rocksdb_write_buffer_size_mb: 64,
            db_storage_rocksdb_block_size: 65536,
            db_storage_rocksdb_bloom_filter_bits_per_key: 10,
            db_storage_rocksdb_block_cache_size: -1,
            db_storage_rocksdb_sst_size_mb: 64,
            db_storage_rocksdb_num_levels: -1,
            db_storage_rocksdb_num_files_in_level0: 4,
            db_storage_rocksdb_max_size_in_level1_mb: 256,
            entry_location_rocksdb_conf: None,
            ledger_metadata_rocksdb_conf: None,
            server_num_io_threads: 0,
            server_tcp_no_delay: true,
            server_sock_keepalive: true,
            max_frame_size: 5 * 1024 * 1024,
            num_add_worker_threads: 1,
            num_read_worker_threads: 8,
            tls_certificate_path: None,
            tls_key_path: None,
            tls_trust_store_path: None,
            tls_client_authentication: false,
            tls_keystore_type: "PEM".to_string(),
            tls_enabled_cipher_suites: None,
            tls_enabled_protocols: None,
            tls_cert_refresh_seconds: 0,
            bookie_auth_provider_factory_class: None,
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
            compaction_rate_bytes_per_sec: 1_000_000,
            compaction_max_time_ms: 0,
            gc_entry_log_metadata_cache_enabled: false,
            minor_compaction_max_time_ms: -1,
            major_compaction_max_time_ms: -1,
            // Java's `compactionMaxOutstandingRequests` default is 100_000;
            // 10_000 forced 10× more compaction flushes → higher write
            // amplification. Aligning with the Java default restores the
            // intended batching size.
            compaction_max_outstanding_requests: 100_000,
            is_force_gc_allow_when_no_space: false,
            journal_max_queue_memory_mb: 0,
            journal_flush_when_queue_empty: false,
            // Java defaults `journalRemoveFromPageCache` to true, which
            // calls `posix_fadvise(DONTNEED)` after each journal write so
            // the Linux page cache isn't polluted with journal pages the
            // bookie will never re-read. Leaving this `false` (the prior
            // Rust default) lets journal data accumulate in page cache
            // and creates OS-level memory pressure on busy bookies.
            journal_remove_from_page_cache: true,
            max_adds_in_progress_limit: 0,
            max_reads_in_progress_limit: 10000,
            max_connections: 0,
            close_channel_on_response_timeout: false,
            skip_replay_journal_invalid_record: false,
            verify_storage_on_unclean_shutdown: false,
            journal_pre_alloc_size_mb: 16,
            journal_write_buffer_size_kb: 64,
            journal_reuse_files: false,
            journal_queue_size: 10000,
            journal_page_cache_flush_interval_ms: 1000,
            num_journal_callback_threads: 1,
            force_allow_compaction: false,
            is_throttle_by_bytes: false,
            verify_metadata_on_gc: false,
            use_transactional_compaction: false,
            force_read_only_bookie: false,
            persist_bookie_status_enabled: false,
            read_only_mode_on_any_disk_full_enabled: true,
            server_write_buffer_low_water_mark: 384 * 1024,
            server_write_buffer_high_water_mark: 512 * 1024,
            max_pending_read_requests_per_thread: 10000,
            max_pending_add_requests_per_thread: 10000,
            num_long_poll_worker_threads: 0,
            num_high_priority_worker_threads: 8,
            db_storage_read_ahead_cache_batch_size: 100,
            db_storage_read_ahead_cache_batch_bytes_size: -1,
            db_storage_max_throttle_time_ms: 10_000,
            max_batch_read_size_bytes: 5 * 1024 * 1024, // 5MB, matching Java
            max_batch_read_count: 10_000,
            auto_recovery_daemon_enabled: false,
            lost_bookie_recovery_delay: 0,
            auditor_periodic_check_interval: 604800,
            auditor_periodic_bookie_check_interval: 86400,
            auditor_periodic_placement_policy_check_interval: 0,
            auditor_periodic_replicas_check_interval: 0,
            rw_rereplicate_backoff_ms: 5000,
            zk_servers: None,
            zk_ledger_root: "/ledgers".to_string(),
            zk_timeout: 10000,
            zk_retry_backoff_start_ms: 1000,
            zk_retry_backoff_max_ms: 10000,
            http_server_enabled: false,
            http_server_port: 8080,
            http_server_host: "0.0.0.0".to_string(),
            local_scrub_interval: 0,
            local_scrub_rate_limit: 0,
            local_consistency_check_on_startup: false,
            data_integrity_checking: false,
            data_integrity_stamp_missing_cookies: false,
            bookie_id: None,
            open_file_limit: 20000,
            min_usable_size_for_high_priority_writes: 0,
            min_usable_size_for_entry_log_creation: 0,
            server_num_acceptor_threads: 1,
            wait_timeout_on_response_backpressure_ms: -1,
            read_worker_threads_throttling_enabled: true,
            request_timer_tick_duration_ms: 10,
            request_timer_num_ticks: 1024,
            open_ledger_rereplication_grace_period: 30000,
            lock_release_of_failed_ledger_grace_period: 300000,
            http_server_tls_enable: false,
            http_server_key_store_path: None,
            http_server_key_store_password: None,
            http_server_trust_store_path: None,
            http_server_trust_store_password: None,
            enable_statistics: true,
            prometheus_stats_http_port: 0,
            stats_provider_class: None,
            // Not yet wired
            page_size: 8192,
            page_limit: -1,
            file_info_cache_initial_capacity: 5000,
            file_info_max_idle_time: 0,
            file_info_format_version_to_write: 1,
            sorted_ledger_storage_enabled: true,
            skip_list_size_limit: 67108864,
            skip_list_arena_chunk_size: 4194304,
            skip_list_arena_max_alloc_size: 131072,
            server_tcp_linger: 0,
            allow_ephemeral_ports: false,
            enable_local_transport: false,
            disable_server_socket_bind: false,
            byte_buf_allocator_size_initial: 65536,
            byte_buf_allocator_size_min: 0,
            byte_buf_allocator_size_max: 1048576,
            extra_server_components: None,
            sanity_check_metrics_enabled: false,
            enable_task_execution_stats: false,
            authorized_roles: None,
            read_buffer_size_bytes: 512,
            write_buffer_size_bytes: 65536,
            metadata_service_uri: None,
            min_num_racks_per_write_quorum: 2,
            enforce_min_num_racks_per_write_quorum: false,
            enforce_min_num_fault_domains_for_write: false,
            permitted_startup_users: None,
            repaired_placement_policy_not_adhering_bookie_enabled: false,
            auditor_replicas_check_interval: 0,
            auditor_max_concurrent_open_ledger_operations: 500,
            bookie_death_watch_interval: 1000,
        }
    }
}

impl ServerConfig {
    /// Validate the configuration. Returns a list of errors.
    /// Matches Java's `ServerConfiguration.validate()`.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // Journal alignment must be >= 512 and divisible by 512.
        // Matches Java's: "journalAlignmentSize should be greater than or equal to 512"
        if self.journal_alignment_size < 512 {
            errors.push(format!(
                "journalAlignmentSize ({}) must be >= 512",
                self.journal_alignment_size
            ));
        }
        if self.journal_alignment_size % 512 != 0 {
            errors.push(format!(
                "journalAlignmentSize ({}) must be divisible by 512",
                self.journal_alignment_size
            ));
        }

        // Journal alignment must fit within pre-alloc size.
        let prealloc_bytes = (self.journal_pre_alloc_size_mb as i64) * 1024 * 1024;
        if prealloc_bytes > 0 && (self.journal_alignment_size as i64) > prealloc_bytes {
            errors.push(format!(
                "journalAlignmentSize ({}) must be <= journalPreAllocSizeMB ({} MB)",
                self.journal_alignment_size, self.journal_pre_alloc_size_mb
            ));
        }

        // Bookie port must be > 0 unless ephemeral ports are allowed.
        if self.bookie_port == 0 && !self.allow_ephemeral_ports {
            errors.push(
                "bookiePort must be > 0 (set allowEphemeralPorts=true for random port)".into()
            );
        }

        // Minor compaction interval must be >= gcWaitTime when both are set.
        if self.minor_compaction_interval > 0 && self.gc_wait_time > 0 {
            let minor_ms = self.minor_compaction_interval * 1000;
            if minor_ms < self.gc_wait_time {
                errors.push(format!(
                    "minorCompactionInterval ({}s = {}ms) must be >= gcWaitTime ({}ms)",
                    self.minor_compaction_interval, minor_ms, self.gc_wait_time
                ));
            }
        }

        // Major compaction interval must be >= gcWaitTime when both are set.
        if self.major_compaction_interval > 0 && self.gc_wait_time > 0 {
            let major_ms = self.major_compaction_interval * 1000;
            if major_ms < self.gc_wait_time {
                errors.push(format!(
                    "majorCompactionInterval ({}s = {}ms) must be >= gcWaitTime ({}ms)",
                    self.major_compaction_interval, major_ms, self.gc_wait_time
                ));
            }
        }

        // Disk usage thresholds: warn < threshold, lwm <= threshold.
        if self.disk_usage_warn_threshold > self.disk_usage_threshold {
            errors.push(format!(
                "diskUsageWarnThreshold ({}) must be <= diskUsageThreshold ({})",
                self.disk_usage_warn_threshold, self.disk_usage_threshold
            ));
        }

        // At least one ledger directory required.
        if self.ledger_directories.is_empty() {
            errors.push("at least one ledgerDirectory is required".into());
        }

        // Journal format version must be >= 6 for V5+ alignment support.
        if self.journal_format_version_to_write < 6 {
            errors.push(format!(
                "journalFormatVersionToWrite ({}) must be >= 6",
                self.journal_format_version_to_write
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn from_properties(props: &HashMap<String, String>) -> Self {
        let mut config = Self::default();
        if let Some(v) = props.get("bookiePort") { if let Ok(n) = v.parse() { config.bookie_port = n; } }
        if let Some(v) = props.get("advertisedAddress") { config.advertised_address = Some(v.clone()); }
        if let Some(v) = props.get("useHostNameAsBookieID") { config.use_hostname_as_bookie_id = parse_bool(v); }
        if let Some(v) = props.get("useShortHostName") { config.use_short_hostname = parse_bool(v); }
        if let Some(v) = props.get("listeningInterface") { config.listening_interface = Some(v.clone()); }
        if let Some(v) = props.get("allowLoopback") { config.allow_loopback = parse_bool(v); }
        if let Some(v) = props.get("journalDirectory") { config.journal_directory = v.clone(); }
        if let Some(v) = props.get("journalDirectories") { config.journal_directory = v.split('|').next().unwrap_or("").to_string(); }
        if let Some(v) = props.get("ledgerDirectories") { config.ledger_directories = v.split(',').map(|s| s.trim().to_string()).collect(); }
        if let Some(v) = props.get("indexDirectories") { config.index_directories = Some(v.split(',').map(|s| s.trim().to_string()).collect()); }
        if let Some(v) = props.get("allowStorageExpansion") { config.allow_storage_expansion = parse_bool(v); }
        if let Some(v) = props.get("journalMaxSizeMB") { if let Ok(n) = v.parse() { config.journal_max_size_mb = n; } }
        if let Some(v) = props.get("journalMaxBackups") { if let Ok(n) = v.parse() { config.journal_max_backups = n; } }
        if let Some(v) = props.get("journalWriteData") { config.journal_write_data = parse_bool(v); }
        if let Some(v) = props.get("journalSyncData") { config.journal_sync_data = parse_bool(v); }
        if let Some(v) = props.get("journalAdaptiveGroupWrites") { config.journal_adaptive_group_writes = parse_bool(v); }
        if let Some(v) = props.get("journalMaxGroupWaitMSec") { if let Ok(n) = v.parse() { config.journal_max_group_wait_ms = n; } }
        if let Some(v) = props.get("journalBufferedWritesThreshold") { if let Ok(n) = v.parse() { config.journal_buffered_writes_threshold = n; } }
        if let Some(v) = props.get("journalBufferedEntriesThreshold") { if let Ok(n) = v.parse() { config.journal_buffered_entries_threshold = n; } }
        if let Some(v) = props.get("journalAlignmentSize") { if let Ok(n) = v.parse() { config.journal_alignment_size = n; } }
        if let Some(v) = props.get("journalFormatVersionToWrite") { if let Ok(n) = v.parse() { config.journal_format_version_to_write = n; } }
        if let Some(v) = props.get("flushInterval") { if let Ok(n) = v.parse() { config.flush_interval = n; } }
        if let Some(v) = props.get("logSizeLimit") { if let Ok(n) = v.parse() { config.log_size_limit = n; } }
        if let Some(v) = props.get("entryLogPerLedgerEnabled") { config.entry_log_per_ledger_enabled = parse_bool(v); }
        if let Some(v) = props.get("dbStorage_writeCacheMaxSizeMb") { if let Ok(n) = v.parse() { config.db_storage_write_cache_max_size_mb = n; } }
        if let Some(v) = props.get("dbStorage_readAheadCacheMaxSizeMb") { if let Ok(n) = v.parse() { config.db_storage_read_cache_max_size_mb = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_writeBufferSizeMB") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_write_buffer_size_mb = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_blockSize") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_block_size = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_bloomFilterBitsPerKey") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_bloom_filter_bits_per_key = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_blockCacheSize") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_block_cache_size = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_sstSizeInMB") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_sst_size_mb = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_numLevels") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_num_levels = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_numFilesInLevel0") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_num_files_in_level0 = n; } }
        if let Some(v) = props.get("dbStorage_rocksDB_maxSizeInLevel1MB") { if let Ok(n) = v.parse() { config.db_storage_rocksdb_max_size_in_level1_mb = n; } }
        if let Some(v) = props.get("entryLocationRocksdbConf") { config.entry_location_rocksdb_conf = Some(v.clone()); }
        if let Some(v) = props.get("ledgerMetadataRocksdbConf") { config.ledger_metadata_rocksdb_conf = Some(v.clone()); }
        if let Some(v) = props.get("serverTcpNoDelay") { config.server_tcp_no_delay = parse_bool(v); }
        if let Some(v) = props.get("serverSockKeepalive") { config.server_sock_keepalive = parse_bool(v); }
        if let Some(v) = props.get("nettyMaxFrameSizeBytes") { if let Ok(n) = v.parse() { config.max_frame_size = n; } }
        if let Some(v) = props.get("serverNumIOThreads") { if let Ok(n) = v.parse() { config.server_num_io_threads = n; } }
        if let Some(v) = props.get("numAddWorkerThreads") { if let Ok(n) = v.parse() { config.num_add_worker_threads = n; } }
        if let Some(v) = props.get("numReadWorkerThreads") { if let Ok(n) = v.parse() { config.num_read_worker_threads = n; } }
        if let Some(v) = props.get("tlsCertificatePath") { config.tls_certificate_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsKeyStore") { config.tls_key_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsKeyFilePath") { config.tls_key_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsTrustStore") { config.tls_trust_store_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsClientAuthentication") { config.tls_client_authentication = parse_bool(v); }
        if let Some(v) = props.get("tlsKeyStoreType") { config.tls_keystore_type = v.clone(); }
        if let Some(v) = props.get("tlsEnabledCipherSuites") { config.tls_enabled_cipher_suites = Some(v.clone()); }
        if let Some(v) = props.get("tlsEnabledProtocols") { config.tls_enabled_protocols = Some(v.clone()); }
        if let Some(v) = props.get("tlsCertFilesRefreshDurationSeconds") { if let Ok(n) = v.parse() { config.tls_cert_refresh_seconds = n; } }
        if let Some(v) = props.get("bookieAuthProviderFactoryClass") { config.bookie_auth_provider_factory_class = Some(v.clone()); }
        if let Some(v) = props.get("diskUsageThreshold") { if let Ok(n) = v.parse() { config.disk_usage_threshold = n; } }
        if let Some(v) = props.get("diskUsageWarnThreshold") { if let Ok(n) = v.parse() { config.disk_usage_warn_threshold = n; } }
        if let Some(v) = props.get("diskUsageLwmThreshold") { if let Ok(n) = v.parse() { config.disk_usage_lwm_threshold = n; } }
        if let Some(v) = props.get("diskCheckInterval") { if let Ok(n) = v.parse() { config.disk_check_interval = n; } }
        if let Some(v) = props.get("readOnlyModeEnabled") { config.read_only_mode_enabled = parse_bool(v); }
        if let Some(v) = props.get("minorCompactionInterval") { if let Ok(n) = v.parse() { config.minor_compaction_interval = n; } }
        if let Some(v) = props.get("minorCompactionThreshold") { if let Ok(n) = v.parse() { config.minor_compaction_threshold = n; } }
        if let Some(v) = props.get("majorCompactionInterval") { if let Ok(n) = v.parse() { config.major_compaction_interval = n; } }
        if let Some(v) = props.get("majorCompactionThreshold") { if let Ok(n) = v.parse() { config.major_compaction_threshold = n; } }
        if let Some(v) = props.get("gcWaitTime") { if let Ok(n) = v.parse() { config.gc_wait_time = n; } }
        if let Some(v) = props.get("compactionRateByBytes") { if let Ok(n) = v.parse() { config.compaction_rate_bytes_per_sec = n; } }
        if let Some(v) = props.get("compactionMaxTimeMillis") { if let Ok(n) = v.parse() { config.compaction_max_time_ms = n; } }
        if let Some(v) = props.get("gcEntryLogMetadataCacheEnabled") { config.gc_entry_log_metadata_cache_enabled = parse_bool(v); }
        if let Some(v) = props.get("journalMaxMemorySizeMb") { if let Ok(n) = v.parse() { config.journal_max_queue_memory_mb = n; } }
        if let Some(v) = props.get("journalFlushWhenQueueEmpty") { config.journal_flush_when_queue_empty = parse_bool(v); }
        if let Some(v) = props.get("journalRemoveFromPageCache") { config.journal_remove_from_page_cache = parse_bool(v); }
        // GC extended
        if let Some(v) = props.get("minorCompactionMaxTimeMillis") { if let Ok(n) = v.parse() { config.minor_compaction_max_time_ms = n; } }
        if let Some(v) = props.get("majorCompactionMaxTimeMillis") { if let Ok(n) = v.parse() { config.major_compaction_max_time_ms = n; } }
        if let Some(v) = props.get("compactionMaxOutstandingRequests") { if let Ok(n) = v.parse() { config.compaction_max_outstanding_requests = n; } }
        if let Some(v) = props.get("isForceGCAllowWhenNoSpace") { config.is_force_gc_allow_when_no_space = parse_bool(v); }
        // Journal queue + page cache
        if let Some(v) = props.get("journalQueueSize") { if let Ok(n) = v.parse() { config.journal_queue_size = n; } }
        if let Some(v) = props.get("journalPageCacheFlushIntervalMSec") { if let Ok(n) = v.parse() { config.journal_page_cache_flush_interval_ms = n; } }
        if let Some(v) = props.get("numJournalCallbackThreads") { if let Ok(n) = v.parse() { config.num_journal_callback_threads = n; } }
        // Request limits
        if let Some(v) = props.get("maxAddsInProgressLimit") { if let Ok(n) = v.parse() { config.max_adds_in_progress_limit = n; } }
        if let Some(v) = props.get("maxReadsInProgressLimit") { if let Ok(n) = v.parse() { config.max_reads_in_progress_limit = n; } }
        if let Some(v) = props.get("maxConnections") { if let Ok(n) = v.parse() { config.max_connections = n; } }
        if let Some(v) = props.get("closeChannelOnResponseTimeout") { config.close_channel_on_response_timeout = parse_bool(v); }
        // Journal extended
        if let Some(v) = props.get("skipReplayJournalInvalidRecord") { config.skip_replay_journal_invalid_record = parse_bool(v); }
        if let Some(v) = props.get("verifyStorageOnUncleanShutdown") { config.verify_storage_on_unclean_shutdown = parse_bool(v); }
        if let Some(v) = props.get("journalPreAllocSizeMB") { if let Ok(n) = v.parse() { config.journal_pre_alloc_size_mb = n; } }
        if let Some(v) = props.get("journalWriteBufferSizeKB") { if let Ok(n) = v.parse() { config.journal_write_buffer_size_kb = n; } }
        if let Some(v) = props.get("journalReuseFiles") { config.journal_reuse_files = parse_bool(v); }
        // GC extended
        if let Some(v) = props.get("forceAllowCompaction") { config.force_allow_compaction = parse_bool(v); }
        if let Some(v) = props.get("isThrottleByBytes") { config.is_throttle_by_bytes = parse_bool(v); }
        if let Some(v) = props.get("verifyMetadataOnGC") { config.verify_metadata_on_gc = parse_bool(v); }
        if let Some(v) = props.get("useTransactionalCompaction") { config.use_transactional_compaction = parse_bool(v); }
        // Disk extended
        if let Some(v) = props.get("forceReadOnlyBookie") { config.force_read_only_bookie = parse_bool(v); }
        if let Some(v) = props.get("persistBookieStatusEnabled") { config.persist_bookie_status_enabled = parse_bool(v); }
        if let Some(v) = props.get("readOnlyModeOnAnyDiskFullEnabled") { config.read_only_mode_on_any_disk_full_enabled = parse_bool(v); }
        // Network extended
        if let Some(v) = props.get("serverWriteBufferLowWaterMark") { if let Ok(n) = v.parse() { config.server_write_buffer_low_water_mark = n; } }
        if let Some(v) = props.get("serverWriteBufferHighWaterMark") { if let Ok(n) = v.parse() { config.server_write_buffer_high_water_mark = n; } }
        if let Some(v) = props.get("maxPendingReadRequestsPerThread") { if let Ok(n) = v.parse() { config.max_pending_read_requests_per_thread = n; } }
        if let Some(v) = props.get("maxPendingAddRequestsPerThread") { if let Ok(n) = v.parse() { config.max_pending_add_requests_per_thread = n; } }
        if let Some(v) = props.get("numLongPollWorkerThreads") { if let Ok(n) = v.parse() { config.num_long_poll_worker_threads = n; } }
        if let Some(v) = props.get("numHighPriorityWorkerThreads") { if let Ok(n) = v.parse() { config.num_high_priority_worker_threads = n; } }
        // DbLedgerStorage extended
        if let Some(v) = props.get("dbStorage_readAheadCacheBatchSize") { if let Ok(n) = v.parse() { config.db_storage_read_ahead_cache_batch_size = n; } }
        if let Some(v) = props.get("dbStorage_readAheadCacheBatchBytesSize") { if let Ok(n) = v.parse() { config.db_storage_read_ahead_cache_batch_bytes_size = n; } }
        if let Some(v) = props.get("dbStorage_maxThrottleTimeMs") { if let Ok(n) = v.parse() { config.db_storage_max_throttle_time_ms = n; } }
        if let Some(v) = props.get("maxBatchReadSize") { if let Ok(n) = v.parse() { config.max_batch_read_size_bytes = n; } }
        if let Some(v) = props.get("maxBatchReadCount") { if let Ok(n) = v.parse() { config.max_batch_read_count = n; } }
        // Replication / Auditor
        if let Some(v) = props.get("autoRecoveryDaemonEnabled") { config.auto_recovery_daemon_enabled = parse_bool(v); }
        if let Some(v) = props.get("lostBookieRecoveryDelay") { if let Ok(n) = v.parse() { config.lost_bookie_recovery_delay = n; } }
        if let Some(v) = props.get("auditorPeriodicCheckInterval") { if let Ok(n) = v.parse() { config.auditor_periodic_check_interval = n; } }
        if let Some(v) = props.get("auditorPeriodicBookieCheckInterval") { if let Ok(n) = v.parse() { config.auditor_periodic_bookie_check_interval = n; } }
        if let Some(v) = props.get("auditorPeriodicPlacementPolicyCheckInterval") { if let Ok(n) = v.parse() { config.auditor_periodic_placement_policy_check_interval = n; } }
        if let Some(v) = props.get("auditorPeriodicReplicasCheckInterval") { if let Ok(n) = v.parse() { config.auditor_periodic_replicas_check_interval = n; } }
        if let Some(v) = props.get("rwRereplicateBackoffMs") { if let Ok(n) = v.parse() { config.rw_rereplicate_backoff_ms = n; } }
        // ZooKeeper
        if let Some(v) = props.get("zkServers") { config.zk_servers = Some(v.clone()); }
        if let Some(v) = props.get("zkLedgersRootPath") { config.zk_ledger_root = v.clone(); }
        if let Some(v) = props.get("zkTimeout") { if let Ok(n) = v.parse() { config.zk_timeout = n; } }
        if let Some(v) = props.get("zkRetryBackoffStartMs") { if let Ok(n) = v.parse() { config.zk_retry_backoff_start_ms = n; } }
        if let Some(v) = props.get("zkRetryBackoffMaxMs") { if let Ok(n) = v.parse() { config.zk_retry_backoff_max_ms = n; } }
        // HTTP
        if let Some(v) = props.get("httpServerEnabled") { config.http_server_enabled = parse_bool(v); }
        if let Some(v) = props.get("httpServerPort") { if let Ok(n) = v.parse() { config.http_server_port = n; } }
        if let Some(v) = props.get("httpServerHost") { config.http_server_host = v.clone(); }
        // Data integrity
        if let Some(v) = props.get("localScrubInterval") { if let Ok(n) = v.parse() { config.local_scrub_interval = n; } }
        if let Some(v) = props.get("localScrubRateLimit") { if let Ok(n) = v.parse() { config.local_scrub_rate_limit = n; } }
        if let Some(v) = props.get("localConsistencyCheckOnStartup") { config.local_consistency_check_on_startup = parse_bool(v); }
        if let Some(v) = props.get("dataIntegrityChecking") { config.data_integrity_checking = parse_bool(v); }
        if let Some(v) = props.get("dataIntegrityStampMissingCookies") { config.data_integrity_stamp_missing_cookies = parse_bool(v); }
        if let Some(v) = props.get("bookieId") { config.bookie_id = Some(v.clone()); }
        // Storage extended
        if let Some(v) = props.get("openFileLimit") { if let Ok(n) = v.parse() { config.open_file_limit = n; } }
        if let Some(v) = props.get("minUsableSizeForHighPriorityWrites") { if let Ok(n) = v.parse() { config.min_usable_size_for_high_priority_writes = n; } }
        if let Some(v) = props.get("minUsableSizeForEntryLogCreation") { if let Ok(n) = v.parse() { config.min_usable_size_for_entry_log_creation = n; } }
        // Network extended
        if let Some(v) = props.get("serverNumAcceptorThreads") { if let Ok(n) = v.parse() { config.server_num_acceptor_threads = n; } }
        if let Some(v) = props.get("waitTimeoutOnResponseBackpressureMs") { if let Ok(n) = v.parse() { config.wait_timeout_on_response_backpressure_ms = n; } }
        if let Some(v) = props.get("readWorkerThreadsThrottlingEnabled") { config.read_worker_threads_throttling_enabled = parse_bool(v); }
        if let Some(v) = props.get("requestTimerTickDurationMs") { if let Ok(n) = v.parse() { config.request_timer_tick_duration_ms = n; } }
        if let Some(v) = props.get("requestTimerNumTicks") { if let Ok(n) = v.parse() { config.request_timer_num_ticks = n; } }
        // Auditor extended
        if let Some(v) = props.get("openLedgerRereplicationGracePeriod") { if let Ok(n) = v.parse() { config.open_ledger_rereplication_grace_period = n; } }
        if let Some(v) = props.get("lockReleaseOfFailedLedgerGracePeriod") { if let Ok(n) = v.parse() { config.lock_release_of_failed_ledger_grace_period = n; } }
        // HTTP TLS
        if let Some(v) = props.get("httpServerTlsEnable") { config.http_server_tls_enable = parse_bool(v); }
        if let Some(v) = props.get("httpServerKeyStorePath") { config.http_server_key_store_path = Some(v.clone()); }
        if let Some(v) = props.get("httpServerKeyStorePassword") { config.http_server_key_store_password = Some(v.clone()); }
        if let Some(v) = props.get("httpServerTrustStorePath") { config.http_server_trust_store_path = Some(v.clone()); }
        if let Some(v) = props.get("httpServerTrustStorePassword") { config.http_server_trust_store_password = Some(v.clone()); }
        // Statistics
        if let Some(v) = props.get("enableStatistics") { config.enable_statistics = parse_bool(v); }
        if let Some(v) = props.get("prometheusStatsHttpPort") { if let Ok(n) = v.parse() { config.prometheus_stats_http_port = n; } }
        if let Some(v) = props.get("statsProviderClass") { config.stats_provider_class = Some(v.clone()); }
        // Not yet wired
        if let Some(v) = props.get("pageSize") { if let Ok(n) = v.parse() { config.page_size = n; } }
        if let Some(v) = props.get("pageLimit") { if let Ok(n) = v.parse() { config.page_limit = n; } }
        if let Some(v) = props.get("fileInfoCacheInitialCapacity") { if let Ok(n) = v.parse() { config.file_info_cache_initial_capacity = n; } }
        if let Some(v) = props.get("fileInfoMaxIdleTime") { if let Ok(n) = v.parse() { config.file_info_max_idle_time = n; } }
        if let Some(v) = props.get("fileInfoFormatVersionToWrite") { if let Ok(n) = v.parse() { config.file_info_format_version_to_write = n; } }
        if let Some(v) = props.get("sortedLedgerStorageEnabled") { config.sorted_ledger_storage_enabled = parse_bool(v); }
        if let Some(v) = props.get("skipListSizeLimit") { if let Ok(n) = v.parse() { config.skip_list_size_limit = n; } }
        if let Some(v) = props.get("skipListArenaChunkSize") { if let Ok(n) = v.parse() { config.skip_list_arena_chunk_size = n; } }
        if let Some(v) = props.get("skipListArenaMaxAllocSize") { if let Ok(n) = v.parse() { config.skip_list_arena_max_alloc_size = n; } }
        if let Some(v) = props.get("serverTcpLinger") { if let Ok(n) = v.parse() { config.server_tcp_linger = n; } }
        if let Some(v) = props.get("allowEphemeralPorts") { config.allow_ephemeral_ports = parse_bool(v); }
        if let Some(v) = props.get("enableLocalTransport") { config.enable_local_transport = parse_bool(v); }
        if let Some(v) = props.get("disableServerSocketBind") { config.disable_server_socket_bind = parse_bool(v); }
        if let Some(v) = props.get("byteBufAllocatorSizeInitial") { if let Ok(n) = v.parse() { config.byte_buf_allocator_size_initial = n; } }
        if let Some(v) = props.get("byteBufAllocatorSizeMin") { if let Ok(n) = v.parse() { config.byte_buf_allocator_size_min = n; } }
        if let Some(v) = props.get("byteBufAllocatorSizeMax") { if let Ok(n) = v.parse() { config.byte_buf_allocator_size_max = n; } }
        if let Some(v) = props.get("extraServerComponents") { config.extra_server_components = Some(v.clone()); }
        if let Some(v) = props.get("sanityCheckMetricsEnabled") { config.sanity_check_metrics_enabled = parse_bool(v); }
        if let Some(v) = props.get("enableTaskExecutionStats") { config.enable_task_execution_stats = parse_bool(v); }
        if let Some(v) = props.get("authorizedRoles") { config.authorized_roles = Some(v.clone()); }
        if let Some(v) = props.get("readBufferSizeBytes") { if let Ok(n) = v.parse() { config.read_buffer_size_bytes = n; } }
        if let Some(v) = props.get("writeBufferSizeBytes") { if let Ok(n) = v.parse() { config.write_buffer_size_bytes = n; } }
        // Missing config options from Java compatibility audit
        if let Some(v) = props.get("metadataServiceUri") { config.metadata_service_uri = Some(v.clone()); }
        if let Some(v) = props.get("minNumRacksPerWriteQuorum") { if let Ok(n) = v.parse() { config.min_num_racks_per_write_quorum = n; } }
        if let Some(v) = props.get("enforceMinNumRacksPerWriteQuorum") { config.enforce_min_num_racks_per_write_quorum = parse_bool(v); }
        if let Some(v) = props.get("enforceMinNumFaultDomainsForWrite") { config.enforce_min_num_fault_domains_for_write = parse_bool(v); }
        if let Some(v) = props.get("permittedStartupUsers") { config.permitted_startup_users = Some(v.clone()); }
        if let Some(v) = props.get("repairedPlacementPolicyNotAdheringBookieEnabled") { config.repaired_placement_policy_not_adhering_bookie_enabled = parse_bool(v); }
        if let Some(v) = props.get("auditorReplicasCheckInterval") { if let Ok(n) = v.parse() { config.auditor_replicas_check_interval = n; } }
        if let Some(v) = props.get("auditorMaxNumberOfConcurrentOpenLedgerOperations") { if let Ok(n) = v.parse() { config.auditor_max_concurrent_open_ledger_operations = n; } }
        if let Some(v) = props.get("bookieDeathWatchInterval") { if let Ok(n) = v.parse() { config.bookie_death_watch_interval = n; } }
        // Also parse metadataServiceUri to extract ZK servers if zkServers not set
        if config.zk_servers.is_none() {
            if let Some(ref uri) = config.metadata_service_uri {
                if let Some(zk) = parse_metadata_service_uri(uri) {
                    config.zk_servers = Some(zk);
                }
            }
        }
        config
    }

    /// Load the server config from a Java-Properties-shaped file, then
    /// overlay any `BK_<propertyName>` environment variables so container
    /// platforms can inject secrets/endpoints without rewriting the file.
    /// Matches Apache BookKeeper's Docker `apply-config-from-env.py` flow.
    pub fn from_file(path: &Path) -> Result<Self, std::io::Error> {
        let text = std::fs::read_to_string(path)?;
        let mut props = crate::properties::parse_properties(&text);
        crate::properties::apply_env_overrides(&mut props);
        Ok(Self::from_properties(&props))
    }
}

/// Parse a metadataServiceUri to extract ZK connection string.
/// Handles formats like:
/// - `zk+hierarchical://host1:2181,host2:2181/ledgers`
/// - `zk://host:2181/ledgers`
/// Returns just the `host:port[,host:port]` part.
fn parse_metadata_service_uri(uri: &str) -> Option<String> {
    // Strip scheme prefix (zk+hierarchical://, zk://, zk+flat://)
    let after_scheme = if let Some(rest) = uri.strip_prefix("zk+hierarchical://") {
        rest
    } else if let Some(rest) = uri.strip_prefix("zk+flat://") {
        rest
    } else if let Some(rest) = uri.strip_prefix("zk://") {
        rest
    } else {
        return None;
    };
    // Take everything before the first /
    let zk_part = after_scheme.split('/').next().unwrap_or(after_scheme);
    if zk_part.is_empty() {
        None
    } else {
        Some(zk_part.to_string())
    }
}
