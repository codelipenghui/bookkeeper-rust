use std::collections::HashMap;
use std::path::Path;

use crate::properties::{load_bool, load_opt_string, load_parsed, load_string};

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub metadata_service_uri: Option<String>,
    pub zk_timeout: i32,
    pub use_v2_wire_protocol: bool,
    pub read_timeout: i32,
    pub add_entry_timeout: i32,
    pub add_entry_quorum_timeout: i32,
    pub speculative_read_timeout: i32,
    pub first_speculative_read_timeout: i32,
    pub max_speculative_read_timeout: i32,
    pub speculative_read_timeout_backoff_multiplier: f64,
    pub num_channels_per_bookie: i32,
    pub enable_digest_type_autodetection: bool,
    pub explicit_lac_interval_ms: i32,
    /// Max allowed ensemble changes per ledger. 0 = unlimited.
    pub max_allowed_ensemble_changes: i32,
    /// Enable deferred sync (WRITE_FLAG_DEFERRED_SYNC).
    pub deferred_sync: bool,
    /// Enable batch read protocol.
    pub batch_read_enabled: bool,
    /// Request timeout in seconds (per bookie request). 0 = default (30s).
    pub request_timeout_sec: i32,
    /// Client connect timeout (ms).
    pub client_connect_timeout_millis: i32,

    // Speculative LAC reads
    /// First speculative LAC read timeout (ms).
    pub first_speculative_read_lac_timeout: i32,
    /// Max speculative LAC read timeout (ms).
    pub max_speculative_read_lac_timeout: i32,
    /// Speculative LAC read timeout backoff multiplier.
    pub speculative_read_lac_timeout_backoff_multiplier: f64,

    // Connection tuning
    /// TCP no delay on client connections.
    pub client_tcp_no_delay: bool,
    /// TCP keepalive on client connections.
    pub client_sock_keepalive: bool,
    /// Client send buffer size (bytes). 0 = OS default.
    pub client_send_buffer_size: i32,
    /// Client receive buffer size (bytes). 0 = OS default.
    pub client_receive_buffer_size: i32,

    // Bookie health
    /// Enable bookie health check (auto-quarantine unhealthy bookies).
    pub bookie_health_check_enabled: bool,
    /// Health check interval (seconds).
    pub bookie_health_check_interval_seconds: i32,
    /// Error threshold per interval before quarantine.
    pub bookie_error_threshold_per_interval: i32,
    /// Quarantine time (seconds) for unhealthy bookies.
    pub bookie_quarantine_time_seconds: i32,

    // Placement
    /// Ensemble placement policy class name.
    pub ensemble_placement_policy: String,
    /// Enable reorder read sequence based on bookie health.
    pub reorder_read_sequence_enabled: bool,
    /// Delay ensemble change on write failure.
    pub delay_ensemble_change: bool,

    // Read settings
    /// Enable parallel recovery reads.
    pub enable_parallel_recovery_read: bool,
    /// Recovery read batch size.
    pub recovery_read_batch_size: i32,
    /// Enable sticky reads for locality.
    pub sticky_reads_enabled: bool,

    // Write settings
    /// Throttle value (max pending adds per handle). 0 = unlimited.
    pub throttle: i32,

    // Failure tracking
    /// Enable per-bookie failure tracking.
    pub enable_bookie_failure_tracking: bool,
    /// Failure history expiration (ms).
    pub bookie_failure_history_expiration_ms: i64,

    // Timeout extended
    /// Timeout monitor interval (sec). Scans for hung requests.
    pub timeout_monitor_interval_sec: i32,
    /// Client backpressure wait timeout (ms). -1 = infinite.
    pub wait_timeout_on_backpressure_ms: i64,
    /// StartTLS handshake timeout (sec).
    pub start_tls_timeout_secs: i32,

    // Connection buffer watermarks
    /// Client write buffer low water mark (bytes).
    pub client_write_buffer_low_water_mark: i32,
    /// Client write buffer high water mark (bytes).
    pub client_write_buffer_high_water_mark: i32,

    // Bookie health extended
    /// Quarantine on connection errors (not just request errors).
    pub bookie_connection_error_quarantine_enabled: bool,
    /// Interval (sec) to refresh bookie info for disk-weight placement.
    pub get_bookie_info_interval_seconds: i32,
    /// Retry interval (sec) for bookie info fetch.
    pub get_bookie_info_retry_interval_seconds: i32,
    /// Timeout (sec) for get bookie info call.
    pub get_bookie_info_timeout_secs: i32,

    // Auth
    /// Client role: "standard" or "system".
    pub client_role: String,
    /// Client auth provider factory class.
    pub client_auth_provider_factory_class: Option<String>,

    // Read extended
    /// Enable batch reads during recovery.
    pub recovery_batch_read_enabled: bool,

    // ------------------------------------------------------------------
    // Parsed for config-file compatibility, not yet wired in Rust.
    // TODO: wire these as the relevant subsystems are implemented.
    // ------------------------------------------------------------------

    // Thread pools — Tokio runtime defaults are used today.
    /// Client worker threads. 0 = Tokio default.
    pub num_worker_threads: i32,
    /// Client I/O threads. 0 = Tokio default.
    pub num_io_threads: i32,

    // Timer internals — Tokio timers are used instead.
    pub pcbc_timeout_timer_tick_duration_ms: i32,
    pub pcbc_timeout_timer_num_ticks: i32,
    pub timeout_timer_tick_duration_ms: i32,
    pub timeout_timer_num_ticks: i32,

    // Placement — weighted selection
    pub bookie_quarantine_ratio: f64,
    /// Weight ensemble selection by bookie free disk space.
    pub disk_weight_based_placement_enabled: bool,
    /// Cap how much more likely the heaviest bookie is chosen vs lightest.
    pub bookie_max_multiple_for_weighted_placement: i32,
    pub opportunistic_striping: bool,
    pub network_topology_stabilize_period_seconds: i32,

    // TCP keepalive tuning — applied via socket2 to every bookie connection.
    /// Seconds before first keepalive probe. -1 = OS default.
    pub tcp_keep_idle: i32,
    /// Seconds between keepalive probes. -1 = OS default.
    pub tcp_keep_intvl: i32,
    /// Number of unacknowledged probes before dropping. -1 = OS default.
    pub tcp_keep_cnt: i32,

    // Netty buffer pooling — not applicable (no Netty), parsed for compat.
    pub netty_use_pooled_buffers: bool,

    // Misc — not yet wired.
    pub store_system_time_as_ledger_creation_time: bool,
    pub client_connect_bookie_unavailable_log_throttling: i64,
    pub disable_ensemble_change_feature_name: String,
    pub enable_task_execution_stats: bool,
    pub task_execution_warn_time_micros: i64,
    pub client_tcp_user_timeout_millis: i32,

    // TLS — matches Java's ClientConfiguration TLS fields.
    // TLS is enabled if `tls_provider_factory_class` is set (matches Java).
    pub tls_provider_factory_class: Option<String>,
    pub tls_keystore_type: String,
    pub tls_certificate_path: Option<String>,
    pub tls_key_path: Option<String>,
    pub tls_trust_store_path: Option<String>,
    pub tls_client_authentication: bool,
    pub tls_enabled_protocols: Option<String>,
    pub tls_hostname_verification_enabled: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            metadata_service_uri: None,
            zk_timeout: 10000,
            use_v2_wire_protocol: false,
            read_timeout: 5,
            add_entry_timeout: 5,
            add_entry_quorum_timeout: -1,
            speculative_read_timeout: 2000,
            first_speculative_read_timeout: 0, // 0 = use speculativeReadTimeout; -1 = disabled
            max_speculative_read_timeout: 0,   // 0 = use speculativeReadTimeout
            speculative_read_timeout_backoff_multiplier: 2.0,
            num_channels_per_bookie: 1,
            enable_digest_type_autodetection: true,
            explicit_lac_interval_ms: 0,
            max_allowed_ensemble_changes: 0,
            deferred_sync: false,
            batch_read_enabled: false,
            request_timeout_sec: 30,
            client_connect_timeout_millis: 10000,
            first_speculative_read_lac_timeout: 1500,
            max_speculative_read_lac_timeout: 5000,
            speculative_read_lac_timeout_backoff_multiplier: 2.0,
            client_tcp_no_delay: true,
            client_sock_keepalive: true,
            client_send_buffer_size: 1048576,
            client_receive_buffer_size: 1048576,
            bookie_health_check_enabled: false,
            bookie_health_check_interval_seconds: 60,
            bookie_error_threshold_per_interval: 100,
            bookie_quarantine_time_seconds: 1800,
            ensemble_placement_policy: "RackawareEnsemblePlacementPolicy".to_string(),
            reorder_read_sequence_enabled: true,
            delay_ensemble_change: false,
            enable_parallel_recovery_read: false,
            recovery_read_batch_size: 1,
            sticky_reads_enabled: false,
            throttle: 5000,
            enable_bookie_failure_tracking: true,
            bookie_failure_history_expiration_ms: 60000,
            timeout_monitor_interval_sec: 0,
            wait_timeout_on_backpressure_ms: -1,
            start_tls_timeout_secs: 10,
            client_write_buffer_low_water_mark: 393216,
            client_write_buffer_high_water_mark: 524288,
            bookie_connection_error_quarantine_enabled: false,
            get_bookie_info_interval_seconds: 86400,
            get_bookie_info_retry_interval_seconds: 60,
            get_bookie_info_timeout_secs: 5,
            client_role: "standard".to_string(),
            client_auth_provider_factory_class: None,
            recovery_batch_read_enabled: false,
            // Not yet wired
            num_worker_threads: 0,
            num_io_threads: 0,
            pcbc_timeout_timer_tick_duration_ms: 100,
            pcbc_timeout_timer_num_ticks: 1024,
            timeout_timer_tick_duration_ms: 100,
            timeout_timer_num_ticks: 1024,
            bookie_quarantine_ratio: 1.0,
            disk_weight_based_placement_enabled: false,
            bookie_max_multiple_for_weighted_placement: 3,
            opportunistic_striping: false,
            network_topology_stabilize_period_seconds: 0,
            tcp_keep_idle: -1,
            tcp_keep_intvl: -1,
            tcp_keep_cnt: -1,
            netty_use_pooled_buffers: true,
            store_system_time_as_ledger_creation_time: false,
            client_connect_bookie_unavailable_log_throttling: 5000,
            disable_ensemble_change_feature_name: "FEATURE_DISABLE_ENSEMBLE_CHANGE".to_string(),
            enable_task_execution_stats: false,
            task_execution_warn_time_micros: 1000000,
            client_tcp_user_timeout_millis: -1,
            tls_provider_factory_class: None,
            // Matches Java's `tlsKeyStoreType=JKS` default so config
            // files are portable; the TLS builder rejects JKS at runtime
            // with a clear openssl-conversion message since rustls can't
            // load JKS directly.
            tls_keystore_type: "JKS".to_string(),
            tls_certificate_path: None,
            tls_key_path: None,
            tls_trust_store_path: None,
            tls_client_authentication: false,
            tls_enabled_protocols: None,
            tls_hostname_verification_enabled: false,
        }
    }
}

impl ClientConfig {
    pub fn from_properties(props: &HashMap<String, String>) -> Self {
        let mut config = Self::default();
        let c = &mut config;
        load_opt_string(props, "metadataServiceUri", &mut c.metadata_service_uri);
        load_parsed(props, "zkTimeout", &mut c.zk_timeout);
        load_bool(props, "useV2WireProtocol", &mut c.use_v2_wire_protocol);
        // Accept both the Java canonical key names (with "Sec" suffix) and
        // the Rust-native names. Java keys are checked first; the Rust-native
        // keys override if also present, for backwards compat with existing
        // Rust configs.
        load_parsed(props, "readEntryTimeoutSec", &mut c.read_timeout);
        load_parsed(props, "readTimeout", &mut c.read_timeout);
        load_parsed(props, "addEntryTimeoutSec", &mut c.add_entry_timeout);
        load_parsed(props, "addEntryTimeout", &mut c.add_entry_timeout);
        load_parsed(props, "addEntryQuorumTimeoutSec", &mut c.add_entry_quorum_timeout);
        load_parsed(props, "addEntryQuorumTimeout", &mut c.add_entry_quorum_timeout);
        load_parsed(props, "speculativeReadTimeout", &mut c.speculative_read_timeout);
        load_parsed(props, "firstSpeculativeReadTimeout", &mut c.first_speculative_read_timeout);
        load_parsed(props, "maxSpeculativeReadTimeout", &mut c.max_speculative_read_timeout);
        load_parsed(props, "speculativeReadTimeoutBackoffMultiplier", &mut c.speculative_read_timeout_backoff_multiplier);
        load_parsed(props, "numChannelsPerBookie", &mut c.num_channels_per_bookie);
        load_bool(props, "enableDigestTypeAutodetection", &mut c.enable_digest_type_autodetection);
        load_parsed(props, "explicitLacInterval", &mut c.explicit_lac_interval_ms);
        load_parsed(props, "explicitLacIntervalMs", &mut c.explicit_lac_interval_ms);
        load_parsed(props, "maxNumEnsembleChanges", &mut c.max_allowed_ensemble_changes);
        load_parsed(props, "maxAllowedEnsembleChanges", &mut c.max_allowed_ensemble_changes);
        load_bool(props, "deferredSync", &mut c.deferred_sync);
        load_bool(props, "batchReadEnabled", &mut c.batch_read_enabled);
        load_parsed(props, "requestTimeoutSec", &mut c.request_timeout_sec);
        load_parsed(props, "clientConnectTimeoutMillis", &mut c.client_connect_timeout_millis);
        // Speculative LAC
        load_parsed(props, "firstSpeculativeReadLACTimeout", &mut c.first_speculative_read_lac_timeout);
        load_parsed(props, "maxSpeculativeReadLACTimeout", &mut c.max_speculative_read_lac_timeout);
        load_parsed(props, "speculativeReadLACTimeoutBackoffMultiplier", &mut c.speculative_read_lac_timeout_backoff_multiplier);
        // Connection tuning
        load_bool(props, "clientTcpNoDelay", &mut c.client_tcp_no_delay);
        load_bool(props, "clientSockKeepalive", &mut c.client_sock_keepalive);
        load_parsed(props, "clientSendBufferSize", &mut c.client_send_buffer_size);
        load_parsed(props, "clientReceiveBufferSize", &mut c.client_receive_buffer_size);
        // Bookie health
        load_bool(props, "bookieHealthCheckEnabled", &mut c.bookie_health_check_enabled);
        load_parsed(props, "bookieHealthCheckIntervalSeconds", &mut c.bookie_health_check_interval_seconds);
        load_parsed(props, "bookieErrorThresholdPerInterval", &mut c.bookie_error_threshold_per_interval);
        load_parsed(props, "bookieQuarantineTimeSeconds", &mut c.bookie_quarantine_time_seconds);
        // Placement
        load_string(props, "ensemblePlacementPolicy", &mut c.ensemble_placement_policy);
        load_bool(props, "reorderReadSequenceEnabled", &mut c.reorder_read_sequence_enabled);
        load_bool(props, "delayEnsembleChange", &mut c.delay_ensemble_change);
        // Read
        load_bool(props, "enableParallelRecoveryRead", &mut c.enable_parallel_recovery_read);
        load_parsed(props, "recoveryReadBatchSize", &mut c.recovery_read_batch_size);
        load_bool(props, "stickyReadSEnabled", &mut c.sticky_reads_enabled);
        // Write
        load_parsed(props, "throttle", &mut c.throttle);
        // Failure tracking
        load_bool(props, "enableBookieFailureTracking", &mut c.enable_bookie_failure_tracking);
        load_parsed(props, "bookieFailureHistoryExpirationMSec", &mut c.bookie_failure_history_expiration_ms);
        // Timeout extended
        load_parsed(props, "timeoutMonitorIntervalSec", &mut c.timeout_monitor_interval_sec);
        load_parsed(props, "waitTimeoutOnBackpressureMs", &mut c.wait_timeout_on_backpressure_ms);
        load_parsed(props, "startTLSTimeoutSecs", &mut c.start_tls_timeout_secs);
        // Connection watermarks
        load_parsed(props, "clientWriteBufferLowWaterMark", &mut c.client_write_buffer_low_water_mark);
        load_parsed(props, "clientWriteBufferHighWaterMark", &mut c.client_write_buffer_high_water_mark);
        // Bookie health extended
        load_bool(props, "bookieConnectionErrorQuarantineEnabled", &mut c.bookie_connection_error_quarantine_enabled);
        load_parsed(props, "getBookieInfoIntervalSeconds", &mut c.get_bookie_info_interval_seconds);
        load_parsed(props, "getBookieInfoRetryIntervalSeconds", &mut c.get_bookie_info_retry_interval_seconds);
        load_parsed(props, "getBookieInfoTimeoutSecs", &mut c.get_bookie_info_timeout_secs);
        // Auth
        load_string(props, "clientRole", &mut c.client_role);
        load_opt_string(props, "clientAuthProviderFactoryClass", &mut c.client_auth_provider_factory_class);
        // Read extended
        load_bool(props, "recoveryBatchReadEnabled", &mut c.recovery_batch_read_enabled);
        // Not yet wired
        load_parsed(props, "numWorkerThreads", &mut c.num_worker_threads);
        load_parsed(props, "numIOThreads", &mut c.num_io_threads);
        load_parsed(props, "pcbcTimeoutTimerTickDurationMs", &mut c.pcbc_timeout_timer_tick_duration_ms);
        load_parsed(props, "pcbcTimeoutTimerNumTicks", &mut c.pcbc_timeout_timer_num_ticks);
        load_parsed(props, "timeoutTimerTickDurationMs", &mut c.timeout_timer_tick_duration_ms);
        load_parsed(props, "timeoutTimerNumTicks", &mut c.timeout_timer_num_ticks);
        load_parsed(props, "bookieQuarantineRatio", &mut c.bookie_quarantine_ratio);
        load_bool(props, "diskWeightBasedPlacementEnabled", &mut c.disk_weight_based_placement_enabled);
        load_parsed(props, "bookieMaxMultipleForWeightBasedPlacement", &mut c.bookie_max_multiple_for_weighted_placement);
        load_bool(props, "opportunisticStriping", &mut c.opportunistic_striping);
        load_parsed(props, "networkTopologyStabilizePeriodSeconds", &mut c.network_topology_stabilize_period_seconds);
        load_parsed(props, "tcpKeepIdle", &mut c.tcp_keep_idle);
        load_parsed(props, "tcpKeepIntvl", &mut c.tcp_keep_intvl);
        load_parsed(props, "tcpKeepCnt", &mut c.tcp_keep_cnt);
        load_bool(props, "nettyUsePooledBuffers", &mut c.netty_use_pooled_buffers);
        load_bool(props, "storeSystemtimeAsLedgerCreationTime", &mut c.store_system_time_as_ledger_creation_time);
        load_parsed(props, "clientConnectBookieUnavailableLogThrottling", &mut c.client_connect_bookie_unavailable_log_throttling);
        load_string(props, "disableEnsembleChangeFeatureName", &mut c.disable_ensemble_change_feature_name);
        load_bool(props, "enableTaskExecutionStats", &mut c.enable_task_execution_stats);
        load_parsed(props, "taskExecutionWarnTimeMicros", &mut c.task_execution_warn_time_micros);
        load_parsed(props, "clientTcpUserTimeoutMillis", &mut c.client_tcp_user_timeout_millis);
        // TLS fields
        load_opt_string(props, "tlsProviderFactoryClass", &mut c.tls_provider_factory_class);
        load_string(props, "tlsKeyStoreType", &mut c.tls_keystore_type);
        load_opt_string(props, "tlsCertificatePath", &mut c.tls_certificate_path);
        load_opt_string(props, "tlsKeyStore", &mut c.tls_key_path);
        load_opt_string(props, "tlsTrustStore", &mut c.tls_trust_store_path);
        load_bool(props, "tlsClientAuthentication", &mut c.tls_client_authentication);
        load_opt_string(props, "tlsEnabledProtocols", &mut c.tls_enabled_protocols);
        load_bool(props, "tlsHostnameVerificationEnabled", &mut c.tls_hostname_verification_enabled);
        config
    }

    /// Load the client config from a Java-Properties-shaped file, then
    /// overlay any `BK_<propertyName>` environment variables so the same
    /// env-var convention works for clients running in containers.
    pub fn from_file(path: &Path) -> Result<Self, std::io::Error> {
        let text = std::fs::read_to_string(path)?;
        let mut props = crate::properties::parse_properties(&text);
        crate::properties::apply_env_overrides(&mut props);
        Ok(Self::from_properties(&props))
    }
}
