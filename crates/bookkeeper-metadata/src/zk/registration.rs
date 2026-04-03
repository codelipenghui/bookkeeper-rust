use async_trait::async_trait;
use zookeeper_client as zk;
use crate::ledger_metadata::MetadataError;
use crate::service_info::BookieServiceInfo;
use crate::traits::{LedgerLayout, RegistrationManager};
use crate::zk_path::ZkPaths;

pub struct ZkRegistrationManager {
    client: zk::Client,
    paths: ZkPaths,
}

impl ZkRegistrationManager {
    pub async fn new(connect_string: &str, ledger_root: &str) -> Result<Self, MetadataError> {
        let client = zk::Client::connect(connect_string).await
            .map_err(|e| MetadataError::ConnectionError(format!("ZK connect: {e}")))?;
        let paths = ZkPaths::new(ledger_root);
        Ok(Self { client, paths })
    }

    pub fn paths(&self) -> &ZkPaths {
        &self.paths
    }

    async fn ensure_path_exists(&self, path: &str) -> Result<(), MetadataError> {
        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        match self.client.create(path, &[], &options).await {
            Ok(_) => Ok(()),
            Err(zk::Error::NodeExists) => Ok(()),
            Err(e) => Err(MetadataError::ConnectionError(format!("ensure path {path}: {e}"))),
        }
    }

    async fn ensure_parent_paths(&self) -> Result<(), MetadataError> {
        self.ensure_path_exists(self.paths.root()).await?;
        self.ensure_path_exists(self.paths.available()).await?;
        self.ensure_path_exists(self.paths.available_readonly()).await?;
        self.ensure_path_exists(self.paths.cookies()).await?;
        Ok(())
    }
}

#[async_trait]
impl RegistrationManager for ZkRegistrationManager {
    async fn register_bookie(&self, bookie_id: &str, read_only: bool, info: &BookieServiceInfo) -> Result<(), MetadataError> {
        self.ensure_parent_paths().await?;
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
                // Already registered — update data
                self.client.set_data(&path, &data, None).await
                    .map_err(|e| MetadataError::ConnectionError(format!("update registration: {e}")))?;
                Ok(())
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

    async fn read_cookie(&self, bookie_id: &str) -> Result<Option<(Vec<u8>, i32)>, MetadataError> {
        let path = self.paths.cookie_path(bookie_id);
        match self.client.get_data(&path).await {
            Ok((data, stat)) => Ok(Some((data, stat.version))),
            Err(zk::Error::NoNode) => Ok(None),
            Err(e) => Err(MetadataError::ConnectionError(format!("read cookie: {e}"))),
        }
    }

    async fn create_cookie(&self, bookie_id: &str, data: &[u8]) -> Result<(), MetadataError> {
        self.ensure_path_exists(self.paths.cookies()).await?;
        let path = self.paths.cookie_path(bookie_id);
        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        self.client.create(&path, data, &options).await
            .map_err(|e| MetadataError::ConnectionError(format!("create cookie: {e}")))?;
        Ok(())
    }

    async fn update_cookie(&self, bookie_id: &str, data: &[u8], version: i32) -> Result<(), MetadataError> {
        let path = self.paths.cookie_path(bookie_id);
        self.client.set_data(&path, data, Some(version)).await
            .map_err(|e| MetadataError::ConnectionError(format!("update cookie: {e}")))?;
        Ok(())
    }

    async fn delete_cookie(&self, bookie_id: &str, version: i32) -> Result<(), MetadataError> {
        let path = self.paths.cookie_path(bookie_id);
        self.client.delete(&path, Some(version)).await
            .map_err(|e| MetadataError::ConnectionError(format!("delete cookie: {e}")))?;
        Ok(())
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
        // Create root paths
        self.ensure_path_exists(self.paths.root()).await?;
        self.ensure_path_exists(self.paths.available()).await?;
        self.ensure_path_exists(self.paths.available_readonly()).await?;
        self.ensure_path_exists(self.paths.cookies()).await?;

        // Create LAYOUT
        let layout_data = format!("{}\n{}", layout.manager_factory_class, layout.manager_version);
        let persistent_options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        self.client.create(self.paths.layout(), layout_data.as_bytes(), &persistent_options).await
            .map_err(|e| MetadataError::ConnectionError(format!("create layout: {e}")))?;

        // Create instance ID
        self.client.create(self.paths.instance_id(), instance_id.as_bytes(), &persistent_options).await
            .map_err(|e| MetadataError::ConnectionError(format!("create instance id: {e}")))?;

        Ok(())
    }
}
