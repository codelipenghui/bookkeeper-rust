use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::OnceCell;
use zookeeper_client as zk;
use crate::ledger_metadata::MetadataError;
use crate::service_info::BookieServiceInfo;
use crate::traits::{LedgerLayout, RegistrationManager};
use crate::zk_path::ZkPaths;

/// Default ZK session timeout used when the bookie connects.
///
/// The `zookeeper-client` crate defaults to 6s, which derives a 2.4s
/// connection idle-timeout (2/5 of session). Under the bookie's steady-state
/// load (producer + consumer + RPC fan-out), the tokio runtime can starve
/// the ZK heartbeat task for longer than 2.4s — which caused the session
/// client to disconnect and ultimately self-expire, triggering a supervisor
/// restart. Matching Java BK's `zkTimeout=30000ms` default eliminates those
/// false positives.
pub const DEFAULT_ZK_SESSION_TIMEOUT: Duration = Duration::from_secs(30);

pub struct ZkRegistrationManager {
    client: zk::Client,
    paths: ZkPaths,
    parents_ensured: OnceCell<()>,
}

impl ZkRegistrationManager {
    /// Connect to ZK on the *current* tokio runtime using the default
    /// session timeout. Under sustained load the caller should prefer
    /// `new_isolated` to avoid heartbeat starvation.
    pub async fn new(connect_string: &str, ledger_root: &str) -> Result<Self, MetadataError> {
        Self::new_isolated(connect_string, ledger_root, None, None).await
    }

    /// Connect to ZK with an optional dedicated runtime handle and/or an
    /// explicit session timeout.
    ///
    /// If `runtime` is `Some`, the ZK client is constructed via
    /// `handle.spawn`, which causes the `zookeeper-client` crate's
    /// internal session/heartbeat task to run on *that* runtime rather
    /// than whatever runtime the caller is on. Bookies pass a dedicated
    /// 1-worker runtime here so RPC load on the main runtime can't
    /// starve ZK heartbeats.
    ///
    /// `session_timeout` defaults to `DEFAULT_ZK_SESSION_TIMEOUT` (30s).
    pub async fn new_isolated(
        connect_string: &str,
        ledger_root: &str,
        runtime: Option<tokio::runtime::Handle>,
        session_timeout: Option<Duration>,
    ) -> Result<Self, MetadataError> {
        let timeout = session_timeout.unwrap_or(DEFAULT_ZK_SESSION_TIMEOUT);
        let connect = connect_string.to_string();

        let build_client = async move {
            zk::Client::connector()
                .with_session_timeout(timeout)
                .connect(&connect)
                .await
        };

        let client = match runtime {
            Some(rt) => rt
                .spawn(build_client)
                .await
                .map_err(|e| MetadataError::ConnectionError(
                    format!("ZK runtime join: {e}"),
                ))?
                .map_err(|e| MetadataError::ConnectionError(format!("ZK connect: {e}")))?,
            None => build_client
                .await
                .map_err(|e| MetadataError::ConnectionError(format!("ZK connect: {e}")))?,
        };

        let paths = ZkPaths::new(ledger_root);
        Ok(Self { client, paths, parents_ensured: OnceCell::new() })
    }

    pub fn paths(&self) -> &ZkPaths {
        &self.paths
    }

    /// Get the underlying ZK client for sharing with other components.
    pub fn zk_client(&self) -> &zk::Client {
        &self.client
    }

    async fn ensure_parent_paths(&self) -> Result<(), MetadataError> {
        crate::zk_path::ensure_zk_path(&self.client, self.paths.available_readonly()).await?;
        crate::zk_path::ensure_zk_path(&self.client, self.paths.cookies()).await?;
        Ok(())
    }
}

#[async_trait]
impl RegistrationManager for ZkRegistrationManager {
    async fn register_bookie(&self, bookie_id: &str, read_only: bool, info: &BookieServiceInfo) -> Result<(), MetadataError> {
        self.parents_ensured.get_or_try_init(|| self.ensure_parent_paths()).await?;
        let data = info.to_protobuf_bytes();
        let path = if read_only {
            self.paths.bookie_readonly_registration(bookie_id)
        } else {
            self.paths.bookie_registration(bookie_id)
        };
        let options = zk::CreateMode::Ephemeral.with_acls(zk::Acls::anyone_all());
        match self.client.create(&path, &data, &options).await {
            Ok(_) => Ok(()),
            Err(zk::Error::NodeExists) => {
                // Existing ephemeral registration. Two possibilities:
                //   (a) Owned by THIS session — re-register after a
                //       transient state change (read-only flip).
                //       CAS-update is correct.
                //   (b) Owned by a DIFFERENT session — another bookie
                //       claims the same `bookie_id`. ZK lets any client
                //       set_data on an ephemeral regardless of owner
                //       (only delete is owner-bound), so a naive
                //       CAS-update would silently clobber the other
                //       bookie's BookieServiceInfo and leave both
                //       processes running with split-brain on the
                //       advertised endpoint. Mirrors Java's
                //       `ZKRegistrationManager.checkRegNodeAndWaitExpired`
                //       which refuses to overwrite a foreign-session
                //       ephemeral.
                let (_existing, stat) = self.client.get_data(&path).await
                    .map_err(|e| MetadataError::ConnectionError(format!("re-register get: {e}")))?;
                let our_session = self.client.session_id().0;
                if stat.ephemeral_owner != 0 && stat.ephemeral_owner != our_session {
                    return Err(MetadataError::AlreadyExists(format!(
                        "bookie '{bookie_id}' is already registered by another session \
                         (ephemeral_owner=0x{:x}, this session=0x{:x})",
                        stat.ephemeral_owner, our_session,
                    )));
                }
                match self.client.set_data(&path, &data, Some(stat.version)).await {
                    Ok(_) => Ok(()),
                    Err(zk::Error::BadVersion) => Err(MetadataError::VersionConflict {
                        expected: stat.version as i64,
                        actual: -1,
                    }),
                    Err(e) => Err(MetadataError::ConnectionError(format!("update registration: {e}"))),
                }
            }
            Err(e) => Err(MetadataError::ConnectionError(format!("register bookie: {e}"))),
        }
    }

    async fn unregister_bookie(&self, bookie_id: &str, read_only: bool) -> Result<(), MetadataError> {
        let path = if read_only {
            self.paths.bookie_readonly_registration(bookie_id)
        } else {
            self.paths.bookie_registration(bookie_id)
        };
        match self.client.delete(&path, None).await {
            Ok(()) => Ok(()),
            Err(zk::Error::NoNode) => Ok(()), // already gone
            Err(e) => Err(MetadataError::ConnectionError(format!("unregister bookie: {e}"))),
        }
    }

    async fn get_writable_bookies(&self) -> Result<Vec<String>, MetadataError> {
        match self.client.get_children(self.paths.available()).await {
            Ok((children, _)) => {
                // Filter out the "readonly" child which is a subpath, not a bookie
                Ok(children.into_iter().filter(|c| c != "readonly").collect())
            }
            Err(zk::Error::NoNode) => Ok(vec![]),
            Err(e) => Err(MetadataError::ConnectionError(format!("get writable bookies: {e}"))),
        }
    }

    async fn get_readonly_bookies(&self) -> Result<Vec<String>, MetadataError> {
        match self.client.get_children(self.paths.available_readonly()).await {
            Ok((children, _)) => Ok(children),
            Err(zk::Error::NoNode) => Ok(vec![]),
            Err(e) => Err(MetadataError::ConnectionError(format!("get readonly bookies: {e}"))),
        }
    }

    async fn read_cookie(&self, bookie_id: &str) -> Result<Option<(Vec<u8>, i64)>, MetadataError> {
        let path = self.paths.cookie_path(bookie_id);
        match self.client.get_data(&path).await {
            Ok((data, stat)) => Ok(Some((data, stat.version as i64))),
            Err(zk::Error::NoNode) => Ok(None),
            Err(e) => Err(MetadataError::ConnectionError(format!("read cookie: {e}"))),
        }
    }

    async fn create_cookie(&self, bookie_id: &str, data: &[u8]) -> Result<(), MetadataError> {
        crate::zk_path::ensure_zk_path(&self.client, self.paths.cookies()).await?;
        let path = self.paths.cookie_path(bookie_id);
        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        match self.client.create(&path, data, &options).await {
            Ok(_) => Ok(()),
            // Mirror oxia: turn duplicate-cookie into a typed error so
            // callers can distinguish "cookie already exists" (operator
            // action needed) from "ZK is unreachable" (retry).
            Err(zk::Error::NodeExists) => Err(MetadataError::ConnectionError(format!(
                "create cookie: bookie '{bookie_id}' already has a cookie"
            ))),
            Err(e) => Err(MetadataError::ConnectionError(format!("create cookie: {e}"))),
        }
    }

    async fn update_cookie(&self, bookie_id: &str, data: &[u8], version: i64) -> Result<(), MetadataError> {
        let zk_version = i32::try_from(version)
            .map_err(|_| MetadataError::VersionConflict {
                expected: version,
                actual: -1,
            })?;
        let path = self.paths.cookie_path(bookie_id);
        match self.client.set_data(&path, data, Some(zk_version)).await {
            Ok(_) => Ok(()),
            // Distinguish version conflict / vanished cookie from a
            // generic transport error so callers can retry / re-read
            // instead of treating both as opaque ConnectionError.
            Err(zk::Error::BadVersion) => Err(MetadataError::VersionConflict {
                expected: version,
                actual: -1,
            }),
            Err(zk::Error::NoNode) => Err(MetadataError::NotFound(0)),
            Err(e) => Err(MetadataError::ConnectionError(format!("update cookie: {e}"))),
        }
    }

    async fn delete_cookie(&self, bookie_id: &str, version: i64) -> Result<(), MetadataError> {
        let zk_version = i32::try_from(version)
            .map_err(|_| MetadataError::VersionConflict {
                expected: version,
                actual: -1,
            })?;
        let path = self.paths.cookie_path(bookie_id);
        match self.client.delete(&path, Some(zk_version)).await {
            Ok(()) => Ok(()),
            Err(zk::Error::BadVersion) => Err(MetadataError::VersionConflict {
                expected: version,
                actual: -1,
            }),
            Err(zk::Error::NoNode) => Err(MetadataError::NotFound(0)),
            Err(e) => Err(MetadataError::ConnectionError(format!("delete cookie: {e}"))),
        }
    }

    async fn get_cluster_instance_id(&self) -> Result<Option<String>, MetadataError> {
        match self.client.get_data(self.paths.instance_id()).await {
            Ok((data, _)) => {
                let id = String::from_utf8(data)
                    .map_err(|e| MetadataError::SerializationError(format!("instance id: {e}")))?;
                Ok(Some(id))
            }
            Err(zk::Error::NoNode) => Ok(None),
            Err(e) => Err(MetadataError::ConnectionError(format!("get instance id: {e}"))),
        }
    }

    async fn init_new_cluster(&self, layout: &LedgerLayout, instance_id: &str) -> Result<(), MetadataError> {
        // Create root paths first so the LAYOUT create below doesn't
        // fail with NoNode on the parent.
        self.ensure_parent_paths().await?;

        // CAS-create the LAYOUT znode. The previous get_data + create
        // sequence had a TOCTOU window where two concurrent callers
        // could both pass the existence check and both proceed to
        // create — the loser's `create` would race the winner's. ZK's
        // `create` itself returns NodeExists atomically when the node
        // is already there, so use that as the existence check.
        let layout_data = layout.serialize();
        let persistent_options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        match self.client.create(self.paths.layout(), layout_data.as_bytes(), &persistent_options).await {
            Ok(_) => {}
            Err(zk::Error::NodeExists) => {
                return Err(MetadataError::InvalidConfig(
                    "cluster already initialized (LAYOUT exists)".into(),
                ));
            }
            Err(e) => return Err(MetadataError::ConnectionError(format!("create layout: {e}"))),
        }

        // Create instance ID
        self.client.create(self.paths.instance_id(), instance_id.as_bytes(), &persistent_options).await
            .map_err(|e| MetadataError::ConnectionError(format!("create instance id: {e}")))?;

        Ok(())
    }
}
