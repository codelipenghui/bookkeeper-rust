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
        if let Some(v) = props.get("useV2WireProtocol") { config.use_v2_wire_protocol = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("readTimeout") { if let Ok(n) = v.parse() { config.read_timeout = n; } }
        if let Some(v) = props.get("addEntryTimeout") { if let Ok(n) = v.parse() { config.add_entry_timeout = n; } }
        if let Some(v) = props.get("addEntryQuorumTimeout") { if let Ok(n) = v.parse() { config.add_entry_quorum_timeout = n; } }
        if let Some(v) = props.get("speculativeReadTimeout") { if let Ok(n) = v.parse() { config.speculative_read_timeout = n; } }
        if let Some(v) = props.get("numChannelsPerBookie") { if let Ok(n) = v.parse() { config.num_channels_per_bookie = n; } }
        if let Some(v) = props.get("enableDigestTypeAutodetection") { config.enable_digest_type_autodetection = v.eq_ignore_ascii_case("true"); }
        if let Some(v) = props.get("explicitLacIntervalMs") { if let Ok(n) = v.parse() { config.explicit_lac_interval_ms = n; } }
        config
    }

    pub fn from_file(path: &Path) -> Result<Self, std::io::Error> {
        let text = std::fs::read_to_string(path)?;
        let props = crate::properties::parse_properties(&text);
        Ok(Self::from_properties(&props))
    }
}
