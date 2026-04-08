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
    pub first_speculative_read_timeout: i32,
    pub max_speculative_read_timeout: i32,
    pub speculative_read_timeout_backoff_multiplier: f64,
    pub num_channels_per_bookie: i32,
    pub enable_digest_type_autodetection: bool,
    pub explicit_lac_interval_ms: i32,
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
            add_entry_quorum_timeout: 5,
            speculative_read_timeout: 2000,
            first_speculative_read_timeout: 0, // 0 = use speculativeReadTimeout; -1 = disabled
            max_speculative_read_timeout: 0,   // 0 = use speculativeReadTimeout
            speculative_read_timeout_backoff_multiplier: 2.0,
            num_channels_per_bookie: 1,
            enable_digest_type_autodetection: true,
            explicit_lac_interval_ms: 0,
            tls_provider_factory_class: None,
            tls_keystore_type: "PEM".to_string(),
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
        if let Some(v) = props.get("useV2WireProtocol") { config.use_v2_wire_protocol = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("readTimeout") { if let Ok(n) = v.parse() { config.read_timeout = n; } }
        if let Some(v) = props.get("addEntryTimeout") { if let Ok(n) = v.parse() { config.add_entry_timeout = n; } }
        if let Some(v) = props.get("addEntryQuorumTimeout") { if let Ok(n) = v.parse() { config.add_entry_quorum_timeout = n; } }
        if let Some(v) = props.get("speculativeReadTimeout") { if let Ok(n) = v.parse() { config.speculative_read_timeout = n; } }
        if let Some(v) = props.get("firstSpeculativeReadTimeout") { if let Ok(n) = v.parse() { config.first_speculative_read_timeout = n; } }
        if let Some(v) = props.get("maxSpeculativeReadTimeout") { if let Ok(n) = v.parse() { config.max_speculative_read_timeout = n; } }
        if let Some(v) = props.get("speculativeReadTimeoutBackoffMultiplier") { if let Ok(n) = v.parse() { config.speculative_read_timeout_backoff_multiplier = n; } }
        if let Some(v) = props.get("numChannelsPerBookie") { if let Ok(n) = v.parse() { config.num_channels_per_bookie = n; } }
        if let Some(v) = props.get("enableDigestTypeAutodetection") { config.enable_digest_type_autodetection = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("explicitLacIntervalMs") { if let Ok(n) = v.parse() { config.explicit_lac_interval_ms = n; } }
        // TLS fields
        if let Some(v) = props.get("tlsProviderFactoryClass") { config.tls_provider_factory_class = Some(v.clone()); }
        if let Some(v) = props.get("tlsKeyStoreType") { config.tls_keystore_type = v.clone(); }
        if let Some(v) = props.get("tlsCertificatePath") { config.tls_certificate_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsKeyStore") { config.tls_key_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsTrustStore") { config.tls_trust_store_path = Some(v.clone()); }
        if let Some(v) = props.get("tlsClientAuthentication") { config.tls_client_authentication = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("tlsEnabledProtocols") { config.tls_enabled_protocols = Some(v.clone()); }
        if let Some(v) = props.get("tlsHostnameVerificationEnabled") { config.tls_hostname_verification_enabled = v.eq_ignore_ascii_case("true"); }
        config
    }

    pub fn from_file(path: &Path) -> Result<Self, std::io::Error> {
        let text = std::fs::read_to_string(path)?;
        let props = crate::properties::parse_properties(&text);
        Ok(Self::from_properties(&props))
    }
}
