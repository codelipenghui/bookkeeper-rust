use prost::Message;
use crate::ledger_metadata::MetadataError;

/// BookieServiceInfo describes a bookie's endpoint for service discovery.
#[derive(Debug, Clone)]
pub struct BookieServiceInfo {
    bookie_id: String,
    host: String,
    port: i32,
}

impl BookieServiceInfo {
    pub fn new(bookie_id: &str, host: &str, port: u16) -> Self {
        Self {
            bookie_id: bookie_id.to_string(),
            host: host.to_string(),
            port: port as i32,
        }
    }

    pub fn bookie_id(&self) -> &str { &self.bookie_id }
    pub fn host(&self) -> &str { &self.host }
    pub fn port(&self) -> i32 { self.port }

    pub fn to_protobuf_bytes(&self) -> Vec<u8> {
        let endpoint = bookkeeper_proto::bookie_service_info_format::Endpoint {
            id: self.bookie_id.clone(),
            port: self.port,
            host: self.host.clone(),
            protocol: "bookie-rpc".to_string(),
            auth: vec![],
            extensions: vec![],
        };
        let info = bookkeeper_proto::BookieServiceInfoFormat {
            endpoints: vec![endpoint],
            properties: std::collections::HashMap::new(),
        };
        info.encode_to_vec()
    }

    pub fn from_protobuf_bytes(data: &[u8]) -> Result<Self, MetadataError> {
        let info = bookkeeper_proto::BookieServiceInfoFormat::decode(data)
            .map_err(|e| MetadataError::SerializationError(format!("service info decode: {e}")))?;
        let endpoint = info.endpoints.first()
            .ok_or_else(|| MetadataError::SerializationError("no endpoints in service info".into()))?;
        Ok(Self {
            bookie_id: endpoint.id.clone(),
            host: endpoint.host.clone(),
            port: endpoint.port,
        })
    }
}
