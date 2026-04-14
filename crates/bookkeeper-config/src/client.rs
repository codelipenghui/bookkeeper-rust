use std::collections::HashMap;
use std::path::Path;

use crate::properties::parse_bool;

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
        if let Some(v) = props.get("metadataServiceUri") { config.metadata_service_uri = Some(v.clone()); }
        if let Some(v) = props.get("zkTimeout") { if let Ok(n) = v.parse() { config.zk_timeout = n; } }
        if let Some(v) = props.get("useV2WireProtocol") { config.use_v2_wire_protocol = parse_bool(v); }
        // Accept both the Java canonical key names (with "Sec" suffix) and
        // the Rust-native names. Java keys are checked first; the Rust-native
        // keys override if also present, for backwards compat with existing
        // Rust configs.
        if let Some(v) = props.get("readEntryTimeoutSec") { if let Ok(n) = v.parse() { config.read_timeout = n; } }
        if let Some(v) = props.get("readTimeout") { if let Ok(n) = v.parse() { config.read_timeout = n; } }
        if let Some(v) = props.get("addEntryTimeoutSec") { if let Ok(n) = v.parse() { config.add_entry_timeout = n; } }
        if let Some(v) = props.get("addEntryTimeout") { if let Ok(n) = v.parse() { config.add_entry_timeout = n; } }
        if let Some(v) = props.get("addEntryQuorumTimeoutSec") { if let Ok(n) = v.parse() { config.add_entry_quorum_timeout = n; } }
        if let Some(v) = props.get("addEntryQuorumTimeout") { if let Ok(n) = v.parse() { config.add_entry_quorum_timeout = n; } }
        if let Some(v) = props.get("speculativeReadTimeout") { if let Ok(n) = v.parse() { config.speculative_read_timeout = n; } }
        if let Some(v) = props.get("firstSpeculativeReadTimeout") { if let Ok(n) = v.parse() { config.first_speculative_read_timeout = n; } }
        if let Some(v) = props.get("maxSpeculativeReadTimeout") { if let Ok(n) = v.parse() { config.max_speculative_read_timeout = n; } }
        if let Some(v) = props.get("speculativeReadTimeoutBackoffMultiplier") { if let Ok(n) = v.parse() { config.speculative_read_timeout_backoff_multiplier = n; } }
        if let Some(v) = props.get("numChannelsPerBookie") { if let Ok(n) = v.parse() { config.num_channels_per_bookie = n; } }
        if let Some(v) = props.get("enableDigestTypeAutodetection") { config.enable_digest_type_autodetection = parse_bool(v); }
        if let Some(v) = props.get("explicitLacInterval") { if let Ok(n) = v.parse() { config.explicit_lac_interval_ms = n; } }
        if let Some(v) = props.get("explicitLacIntervalMs") { if let Ok(n) = v.parse() { config.explicit_lac_interval_ms = n; } }
        if let Some(v) = props.get("maxNumEnsembleChanges") { if let Ok(n) = v.parse() { config.max_allowed_ensemble_changes = n; } }
        if let Some(v) = props.get("maxAllowedEnsembleChanges") { if let Ok(n) = v.parse() { config.max_allowed_ensemble_changes = n; } }
        if let Some(v) = props.get("deferredSync") { config.deferred_sync = parse_bool(v); }
        if let Some(v) = props.get("batchReadEnabled") { config.batch_read_enabled = parse_bool(v); }
        if let Some(v) = props.get("requestTimeoutSec") { if let Ok(n) = v.parse() { config.request_timeout_sec = n; } }
        if let Some(v) = props.get("clientConnectTimeoutMillis") { if let Ok(n) = v.parse() { config.client_connect_timeout_millis = n; } }
        // Speculative LAC
        if let Some(v) = props.get("firstSpeculativeReadLACTimeout") { if let Ok(n) = v.parse() { config.first_speculative_read_lac_timeout = n; } }
        if let Some(v) = props.get("maxSpeculativeReadLACTimeout") { if let Ok(n) = v.parse() { config.max_speculative_read_lac_timeout = n; } }
        if let Some(v) = props.get("speculativeReadLACTimeoutBackoffMultiplier") { if let Ok(n) = v.parse() { config.speculative_read_lac_timeout_backoff_multiplier = n; } }
        // Connection tuning
        if let Some(v) = props.get("clientTcpNoDelay") { config.client_tcp_no_delay = parse_bool(v); }
        if let Some(v) = props.get("clientSockKeepalive") { config.client_sock_keepalive = parse_bool(v); }
        if let Some(v) = props.get("clientSendBufferSize") { if let Ok(n) = v.parse() { config.client_send_buffer_size = n; } }
        if let Some(v) = props.get("clientReceiveBufferSize") { if let Ok(n) = v.parse() { config.client_receive_buffer_size = n; } }
        // Bookie health
        if let Some(v) = props.get("bookieHealthCheckEnabled") { config.bookie_health_check_enabled = parse_bool(v); }
        if let Some(v) = props.get("bookieHealthCheckIntervalSeconds") { if let Ok(n) = v.parse() { config.bookie_health_check_interval_seconds = n; } }
        if let Some(v) = props.get("bookieErrorThresholdPerInterval") { if let Ok(n) = v.parse() { config.bookie_error_threshold_per_interval = n; } }
        if let Some(v) = props.get("bookieQuarantineTimeSeconds") { if let Ok(n) = v.parse() { config.bookie_quarantine_time_seconds = n; } }
        // Placement
        if let Some(v) = props.get("ensemblePlacementPolicy") { config.ensemble_placement_policy = v.clone(); }
        if let Some(v) = props.get("reorderReadSequenceEnabled") { config.reorder_read_sequence_enabled = parse_bool(v); }
        if let Some(v) = props.get("delayEnsembleChange") { config.delay_ensemble_change = parse_bool(v); }
        // Read
        if let Some(v) = props.get("enableParallelRecoveryRead") { config.enable_parallel_recovery_read = parse_bool(v); }
        if let Some(v) = props.get("recoveryReadBatchSize") { if let Ok(n) = v.parse() { config.recovery_read_batch_size = n; } }
        if let Some(v) = props.get("stickyReadSEnabled") { config.sticky_reads_enabled = parse_bool(v); }
        // Write
        if let Some(v) = props.get("throttle") { if let Ok(n) = v.parse() { config.throttle = n; } }
        // Failure tracking
        if let Some(v) = props.get("enableBookieFailureTracking") { config.enable_bookie_failure_tracking = parse_bool(v); }
        if let Some(v) = props.get("bookieFailureHistoryExpirationMSec") { if let Ok(n) = v.parse() { config.bookie_failure_history_expiration_ms = n; } }
        // Timeout extended
        if let Some(v) = props.get("timeoutMonitorIntervalSec") { if let Ok(n) = v.parse() { config.timeout_monitor_interval_sec = n; } }
        if let Some(v) = props.get("waitTimeoutOnBackpressureMs") { if let Ok(n) = v.parse() { config.wait_timeout_on_backpressure_ms = n; } }
        if let Some(v) = props.get("startTLSTimeoutSecs") { if let Ok(n) = v.parse() { config.start_tls_timeout_secs = n; } }
        // Connection watermarks
        if let Some(v) = props.get("clientWriteBufferLowWaterMark") { if let Ok(n) = v.parse() { config.client_write_buffer_low_water_mark = n; } }
        if let Some(v) = props.get("clientWriteBufferHighWaterMark") { if let Ok(n) = v.parse() { config.client_write_buffer_high_water_mark = n; } }
        // Bookie health extended
        if let Some(v) = props.get("bookieConnectionErrorQuarantineEnabled") { config.bookie_connection_error_quarantine_enabled = parse_bool(v); }
        if let Some(v) = props.get("getBookieInfoIntervalSeconds") { if let Ok(n) = v.parse() { config.get_bookie_info_interval_seconds = n; } }
        if let Some(v) = props.get("getBookieInfoRetryIntervalSeconds") { if let Ok(n) = v.parse() { config.get_bookie_info_retry_interval_seconds = n; } }
        if let Some(v) = props.get("getBookieInfoTimeoutSecs") { if let Ok(n) = v.parse() { config.get_bookie_info_timeout_secs = n; } }
        // Auth
        if let Some(v) = props.get("clientRole") { config.client_role = v.clone(); }
        if let Some(v) = props.get("clientAuthProviderFactoryClass") { config.client_auth_provider_factory_class = Some(v.clone()); }
        // Read extended
        if let Some(v) = props.get("recoveryBatchReadEnabled") { config.recovery_batch_read_enabled = parse_bool(v); }
        // Not yet wired
        if let Some(v) = props.get("numWorkerThreads") { if let Ok(n) = v.parse() { config.num_worker_threads = n; } }
        if let Some(v) = props.get("numIOThreads") { if let Ok(n) = v.parse() { config.num_io_threads = n; } }
        if let Some(v) = props.get("pcbcTimeoutTimerTickDurationMs") { if let Ok(n) = v.parse() { config.pcbc_timeout_timer_tick_duration_ms = n; } }
        if let Some(v) = props.get("pcbcTimeoutTimerNumTicks") { if let Ok(n) = v.parse() { config.pcbc_timeout_timer_num_ticks = n; } }
        if let Some(v) = props.get("timeoutTimerTickDurationMs") { if let Ok(n) = v.parse() { config.timeout_timer_tick_duration_ms = n; } }
        if let Some(v) = props.get("timeoutTimerNumTicks") { if let Ok(n) = v.parse() { config.timeout_timer_num_ticks = n; } }
        if let Some(v) = props.get("bookieQuarantineRatio") { if let Ok(n) = v.parse() { config.bookie_quarantine_ratio = n; } }
        if let Some(v) = props.get("diskWeightBasedPlacementEnabled") { config.disk_weight_based_placement_enabled = parse_bool(v); }
        if let Some(v) = props.get("bookieMaxMultipleForWeightBasedPlacement") { if let Ok(n) = v.parse() { config.bookie_max_multiple_for_weighted_placement = n; } }
        if let Some(v) = props.get("opportunisticStriping") { config.opportunistic_striping = parse_bool(v); }
        if let Some(v) = props.get("networkTopologyStabilizePeriodSeconds") { if let Ok(n) = v.parse() { config.network_topology_stabilize_period_seconds = n; } }
        if let Some(v) = props.get("tcpKeepIdle") { if let Ok(n) = v.parse() { config.tcp_keep_idle = n; } }
        if let Some(v) = props.get("tcpKeepIntvl") { if let Ok(n) = v.parse() { config.tcp_keep_intvl = n; } }
        if let Some(v) = props.get("tcpKeepCnt") { if let Ok(n) = v.parse() { config.tcp_keep_cnt = n; } }
        if let Some(v) = props.get("nettyUsePooledBuffers") { config.netty_use_pooled_buffers = parse_bool(v); }
        if let Some(v) = props.get("storeSystemtimeAsLedgerCreationTime") { config.store_system_time_as_ledger_creation_time = parse_bool(v); }
        if let Some(v) = props.get("clientConnectBookieUnavailableLogThrottling") { if let Ok(n) = v.parse() { config.client_connect_bookie_unavailable_log_throttling = n; } }
        if let Some(v) = props.get("disableEnsembleChangeFeatureName") { config.disable_ensemble_change_feature_name = v.clone(); }
        if let Some(v) = props.get("enableTaskExecutionStats") { config.enable_task_execution_stats = parse_bool(v); }
        if let Some(v) = props.get("taskExecutionWarnTimeMicros") { if let Ok(n) = v.parse() { config.task_execution_warn_time_micros = n; } }
        if let Some(v) = props.get("clientTcpUserTimeoutMillis") { if let Ok(n) = v.parse() { config.client_tcp_user_timeout_millis = n; } }
        // TLS fields
        if let Some(v) = props.get("tlsProviderFactoryClass") { config.tls_provider_factory_class = Some(v.clone()); }
        if let Some(v) = props.get("tlsKeyStoreType") { config.tls_keystore_type = v.clone(); }
        if let Some(v) = props.get("tlsCertificatePath") { config.tls_certificate_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsKeyStore") { config.tls_key_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsTrustStore") { config.tls_trust_store_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsClientAuthentication") { config.tls_client_authentication = parse_bool(v); }
        if let Some(v) = props.get("tlsEnabledProtocols") { config.tls_enabled_protocols = Some(v.clone()); }
        if let Some(v) = props.get("tlsHostnameVerificationEnabled") { config.tls_hostname_verification_enabled = parse_bool(v); }
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
