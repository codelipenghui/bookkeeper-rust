use std::sync::Arc;
use std::time::Duration;

use liboxia::client::OxiaClient;
use liboxia::client_options::OxiaClientOptions;

use crate::ledger_metadata::MetadataError;
use crate::metadata_paths::LedgerPaths;

const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(30);

/// Shared oxia client handle. `OxiaClient` is internally
/// `Arc<Inner> + Clone`, so every manager receives a cheap clone of the
/// `Arc<OxiaClient>` stored here.
pub struct OxiaClientHandle {
    pub client: Arc<OxiaClient>,
    pub paths: LedgerPaths,
}

impl OxiaClientHandle {
    /// Connect to oxia. The service address is expected in `host:port`
    /// form (no scheme); we prepend `http://` internally because
    /// liboxia wants a full URL.
    ///
    /// `session_timeout` controls both the oxia session keep-alive
    /// window and the initial connect deadline. Callers who pass a
    /// very short value for testing (e.g. `Some(2s)` in dispatch
    /// tests) will also see connect fail quickly — that's by design.
    pub async fn connect(
        service_addr: &str,
        namespace: &str,
        ledgers_root: &str,
        session_timeout: Option<Duration>,
    ) -> Result<Self, MetadataError> {
        let session_timeout = session_timeout.unwrap_or(DEFAULT_SESSION_TIMEOUT);
        let options = OxiaClientOptions {
            service_address: format!("http://{service_addr}"),
            namespace: namespace.to_string(),
            identity: format!("bookkeeper-rust-{}", uuid::Uuid::new_v4()),
            session_timeout,
            ..Default::default()
        };

        let client = tokio::time::timeout(session_timeout, OxiaClient::new(options))
            .await
            .map_err(|_| {
                MetadataError::ConnectionError(format!(
                    "oxia connect: timed out after {session_timeout:?}"
                ))
            })?
            .map_err(|e| MetadataError::ConnectionError(format!("oxia connect: {e:?}")))?;

        Ok(Self {
            client: Arc::new(client),
            paths: LedgerPaths::new(ledgers_root),
        })
    }
}
