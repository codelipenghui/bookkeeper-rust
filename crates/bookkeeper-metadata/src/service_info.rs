use std::collections::BTreeMap;

use prost::Message;

use crate::ledger_metadata::MetadataError;

/// One advertised service endpoint of a bookie. Mirrors Apache
/// BookKeeper's `BookieServiceInfo.Endpoint` (Java) /
/// `BookieServiceInfoFormat.Endpoint` (proto). A bookie may publish
/// several — typically a `bookie-rpc` endpoint plus a `bookie-rpc-tls`
/// alternate, optional `metrics` / `health` sidecars, and security-
/// extension endpoints — and Pulsar deployments rely on the full
/// list being preserved across re-registration.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Endpoint {
    /// Endpoint identifier, opaque to the protocol. Java's
    /// `BookieServiceInfo.Endpoint.id` is typically the bookie id for
    /// the primary `bookie-rpc` endpoint.
    pub id: String,
    /// Hostname or address the endpoint is listening on.
    pub host: String,
    /// TCP port the endpoint is listening on.
    pub port: i32,
    /// Wire protocol name. The convention is `"bookie-rpc"` for the
    /// primary RPC endpoint; alternates may use names like
    /// `"bookie-rpc-tls"` for the TLS variant.
    pub protocol: String,
    /// Names of supported authentication plugins (e.g. `"sasl"`,
    /// `"token"`). Per Java's `Endpoint.auth: List<String>`.
    pub auth: Vec<String>,
    /// Names of supported protocol extensions (e.g. `"long-poll-read"`).
    /// Per Java's `Endpoint.extensions: List<String>`.
    pub extensions: Vec<String>,
}

/// Information about services exposed by a bookie. Mirrors Apache
/// BookKeeper's `org.apache.bookkeeper.discover.BookieServiceInfo`
/// (`BookieServiceInfoFormat` proto on the wire).
///
/// Carries:
///   - a list of [`Endpoint`]s (RPC / TLS / metrics / etc.),
///   - bookie-wide [`properties`](BookieServiceInfo::properties), an
///     unordered string-to-string map used for advertising version,
///     region, supported features, etc.
///
/// Round-trip preservation matters: the writer (this Rust client) is
/// responsible for re-emitting whatever it read on every
/// re-registration. A previous Rust port modelled only a single
/// `(bookie_id, host, port)` triple, which silently dropped every
/// alternate endpoint, every property, and every per-endpoint
/// `auth` / `extensions` list each time the bookie re-registered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BookieServiceInfo {
    endpoints: Vec<Endpoint>,
    properties: BTreeMap<String, String>,
}

impl BookieServiceInfo {
    /// Construct a single-endpoint info with the conventional
    /// `bookie-rpc` protocol. Convenience for the common case where
    /// the bookie publishes only its primary RPC endpoint.
    pub fn new(bookie_id: &str, host: &str, port: u16) -> Self {
        Self {
            endpoints: vec![Endpoint {
                id: bookie_id.to_string(),
                host: host.to_string(),
                port: port as i32,
                protocol: "bookie-rpc".to_string(),
                auth: Vec::new(),
                extensions: Vec::new(),
            }],
            properties: BTreeMap::new(),
        }
    }

    /// Construct from an explicit list of endpoints (no properties).
    pub fn with_endpoints(endpoints: Vec<Endpoint>) -> Self {
        Self { endpoints, properties: BTreeMap::new() }
    }

    /// Set / overwrite a single property entry.
    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.properties.insert(key.into(), value.into());
    }

    /// Replace the entire properties map.
    pub fn set_properties(&mut self, properties: BTreeMap<String, String>) {
        self.properties = properties;
    }

    pub fn endpoints(&self) -> &[Endpoint] { &self.endpoints }
    pub fn properties(&self) -> &BTreeMap<String, String> { &self.properties }

    /// Convenience: the `bookie-rpc` endpoint's id, falling back to
    /// the first endpoint's id when no `bookie-rpc` is present.
    /// Empty string when there are no endpoints at all.
    pub fn bookie_id(&self) -> &str {
        self.primary_endpoint().map(|e| e.id.as_str()).unwrap_or("")
    }

    /// Convenience: the `bookie-rpc` endpoint's host, with the same
    /// fallback rule as [`bookie_id`](Self::bookie_id).
    pub fn host(&self) -> &str {
        self.primary_endpoint().map(|e| e.host.as_str()).unwrap_or("")
    }

    /// Convenience: the `bookie-rpc` endpoint's port, with the same
    /// fallback rule as [`bookie_id`](Self::bookie_id). `0` when
    /// there are no endpoints.
    pub fn port(&self) -> i32 {
        self.primary_endpoint().map(|e| e.port).unwrap_or(0)
    }

    /// The endpoint a client should dial: the `bookie-rpc` one if
    /// present, otherwise the first listed.
    fn primary_endpoint(&self) -> Option<&Endpoint> {
        self.endpoints
            .iter()
            .find(|e| e.protocol == "bookie-rpc")
            .or_else(|| self.endpoints.first())
    }

    /// Encode to the `BookieServiceInfoFormat` protobuf, preserving
    /// every endpoint, every property entry, and the per-endpoint
    /// `auth` / `extensions` lists. Round-trip with
    /// [`from_protobuf_bytes`](Self::from_protobuf_bytes) is
    /// lossless.
    pub fn to_protobuf_bytes(&self) -> Vec<u8> {
        let endpoints = self.endpoints.iter().map(|e| {
            bookkeeper_proto::bookie_service_info_format::Endpoint {
                id: e.id.clone(),
                port: e.port,
                host: e.host.clone(),
                protocol: e.protocol.clone(),
                auth: e.auth.clone(),
                extensions: e.extensions.clone(),
            }
        }).collect();
        // Java emits properties as a `map<string, string>` (proto
        // semantics: insertion-order is not preserved). The Rust
        // proto binding lowers it to a `HashMap`; we keep our
        // canonical model in a `BTreeMap` for deterministic
        // serialisation in tests / logs and convert here.
        let properties: std::collections::HashMap<String, String> =
            self.properties.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let info = bookkeeper_proto::BookieServiceInfoFormat {
            endpoints,
            properties,
        };
        info.encode_to_vec()
    }

    /// Decode from `BookieServiceInfoFormat` bytes, preserving every
    /// endpoint and property the writer included.
    pub fn from_protobuf_bytes(data: &[u8]) -> Result<Self, MetadataError> {
        let info = bookkeeper_proto::BookieServiceInfoFormat::decode(data)
            .map_err(|e| MetadataError::SerializationError(format!("service info decode: {e}")))?;
        if info.endpoints.is_empty() {
            return Err(MetadataError::SerializationError(
                "no endpoints in service info".into(),
            ));
        }
        let endpoints = info.endpoints.into_iter().map(|e| Endpoint {
            id: e.id,
            host: e.host,
            port: e.port,
            protocol: e.protocol,
            auth: e.auth,
            extensions: e.extensions,
        }).collect();
        let properties = info.properties.into_iter().collect::<BTreeMap<_, _>>();
        Ok(Self { endpoints, properties })
    }
}
