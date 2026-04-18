//! Backend-agnostic metadata factory. Dispatch happens here so the
//! rest of the codebase stays free of `match uri.scheme { ... }`.

use std::sync::Arc;
use std::time::Duration;

use crate::ledger_metadata::MetadataError;
use crate::metadata_paths::{LedgerPaths, ParsedMetadataUri};
use crate::traits::{LayoutManager, LedgerIdGenerator, LedgerManager, RegistrationManager};

/// Raw handles the ZK backend exposes to callers that still reach
/// into the native client. `None` for non-ZK backends; consumers
/// that need these must either fall back to trait operations or
/// return NotSupported.
pub struct ZkRawHandles {
    pub client: zookeeper_client::Client,
    pub paths: LedgerPaths,
}

/// A fully-constructed metadata backend: all four managers plus
/// optional backend-specific escape hatches.
pub struct MetadataBackend {
    pub ledger_manager: Arc<dyn LedgerManager>,
    pub id_generator: Arc<dyn LedgerIdGenerator>,
    pub layout_manager: Arc<dyn LayoutManager>,
    pub registration_manager: Arc<dyn RegistrationManager>,
    pub paths: LedgerPaths,
    pub zk_raw: Option<ZkRawHandles>,
}

/// Open the metadata backend identified by `uri`.
///
/// `runtime` — ZK-only: if `Some`, the ZK client is constructed on
/// that runtime so its internal heartbeat task isn't starved by RPC
/// load on the main runtime. `None` means construct on the caller's
/// current runtime. Ignored for oxia.
///
/// `session_timeout` — applies to both backends; `None` uses the
/// backend default (30s for ZK, 30s for oxia).
pub async fn open_metadata_backend(
    uri: &ParsedMetadataUri,
    runtime: Option<tokio::runtime::Handle>,
    session_timeout: Option<Duration>,
) -> Result<MetadataBackend, MetadataError> {
    match uri {
        ParsedMetadataUri::Zk { servers, ledgers_root } => {
            open_zk_backend(servers, ledgers_root, runtime, session_timeout).await
        }
        ParsedMetadataUri::Oxia { .. } => {
            // Filled in by Phase 3.
            Err(MetadataError::InvalidConfig(
                "oxia backend not yet compiled in; enable the `oxia` feature".into(),
            ))
        }
    }
}

async fn open_zk_backend(
    servers: &str,
    ledgers_root: &str,
    runtime: Option<tokio::runtime::Handle>,
    session_timeout: Option<Duration>,
) -> Result<MetadataBackend, MetadataError> {
    let reg = crate::zk::registration::ZkRegistrationManager::new_isolated(
        servers,
        ledgers_root,
        runtime.clone(),
        session_timeout,
    )
    .await?;
    let client = reg.zk_client().clone();
    let paths = LedgerPaths::new(ledgers_root);
    let ledger_manager = Arc::new(
        crate::zk::ledger_manager::ZkLedgerManager::new(client.clone(), paths.clone()),
    );
    let id_generator = Arc::new(
        crate::zk::idgen::ZkLedgerIdGenerator::new(client.clone(), &paths).await?,
    );
    let layout_manager = Arc::new(
        crate::zk::layout::ZkLayoutManager::new(client.clone(), paths.clone()),
    );
    let registration_manager = Arc::new(reg);
    Ok(MetadataBackend {
        ledger_manager,
        id_generator,
        layout_manager,
        registration_manager,
        paths: paths.clone(),
        zk_raw: Some(ZkRawHandles {
            client,
            paths,
        }),
    })
}
