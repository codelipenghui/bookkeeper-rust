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
        ParsedMetadataUri::Oxia { service_addr, namespace, ledgers_root } => {
            #[cfg(feature = "oxia")]
            {
                open_oxia_backend(service_addr, namespace, ledgers_root, session_timeout).await
            }
            #[cfg(not(feature = "oxia"))]
            {
                let _ = (service_addr, namespace, ledgers_root, session_timeout);
                Err(MetadataError::InvalidConfig(
                    "oxia backend requires the `oxia` cargo feature".into(),
                ))
            }
        }
    }
}

#[cfg(feature = "oxia")]
async fn open_oxia_backend(
    service_addr: &str,
    namespace: &str,
    ledgers_root: &str,
    session_timeout: Option<Duration>,
) -> Result<MetadataBackend, MetadataError> {
    use crate::oxia::{
        client::OxiaClientHandle, idgen::OxiaLedgerIdGenerator,
        layout::OxiaLayoutManager, ledger_manager::OxiaLedgerManager,
        registration::OxiaRegistrationManager,
    };

    let handle = OxiaClientHandle::connect(service_addr, namespace, ledgers_root, session_timeout)
        .await?;
    let paths = handle.paths.clone();

    Ok(MetadataBackend {
        ledger_manager: Arc::new(OxiaLedgerManager::new(handle.client.clone(), paths.clone())),
        id_generator: Arc::new(OxiaLedgerIdGenerator::new(handle.client.clone(), &paths).await?),
        layout_manager: Arc::new(OxiaLayoutManager::new(handle.client.clone(), paths.clone())),
        registration_manager: Arc::new(OxiaRegistrationManager::new(handle.client, paths.clone())),
        paths,
        zk_raw: None,
    })
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
