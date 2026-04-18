use std::sync::Arc;
use std::time::Duration;

use liboxia::client::OxiaClient;
use liboxia::client_options::OxiaClientOptions;

use crate::ledger_metadata::MetadataError;
use crate::metadata_paths::LedgerPaths;

/// Shared oxia client handle. Every manager in this module holds an
/// `Arc<OxiaClient>` so they share a single connection + session
/// manager.
///
/// Note: `OxiaClient` itself already wraps an internal `Arc<Inner>` and
/// implements `Clone`, so wrapping it in another `Arc` here lets
/// managers cheaply clone the handle without re-connecting.
pub struct OxiaClientHandle {
    pub client: Arc<OxiaClient>,
    pub paths: LedgerPaths,
    pub identity: String,
}

impl OxiaClientHandle {
    /// Connect to oxia. The service address is expected in `host:port`
    /// form (no scheme); we prepend `http://` internally because
    /// liboxia wants a full URL.
    pub async fn connect(
        service_addr: &str,
        namespace: &str,
        ledgers_root: &str,
        session_timeout: Option<Duration>,
    ) -> Result<Self, MetadataError> {
        let identity = format!("bookkeeper-rust-{}", uuid::Uuid::new_v4());
        let mut options = OxiaClientOptions::default();
        options.service_address = format!("http://{service_addr}");
        options.namespace = namespace.to_string();
        options.identity = identity.clone();
        options.session_timeout = session_timeout.unwrap_or(Duration::from_secs(30));

        let client = OxiaClient::new(options)
            .await
            .map_err(|e| MetadataError::ConnectionError(format!("oxia connect: {e:?}")))?;

        Ok(Self {
            client: Arc::new(client),
            paths: LedgerPaths::new(ledgers_root),
            identity,
        })
    }
}
