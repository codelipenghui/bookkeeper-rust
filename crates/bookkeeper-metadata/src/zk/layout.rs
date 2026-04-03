use async_trait::async_trait;
use zookeeper_client as zk;
use crate::ledger_metadata::MetadataError;
use crate::traits::{LayoutManager, LedgerLayout};
use crate::zk_path::ZkPaths;

const LAYOUT_FORMAT_VERSION: i32 = 2;

pub struct ZkLayoutManager {
    client: zk::Client,
    paths: ZkPaths,
}

impl ZkLayoutManager {
    pub fn new(client: zk::Client, paths: ZkPaths) -> Self {
        Self { client, paths }
    }
}

#[async_trait]
impl LayoutManager for ZkLayoutManager {
    async fn read_ledger_layout(&self) -> Result<Option<LedgerLayout>, MetadataError> {
        match self.client.get_data(self.paths.layout()).await {
            Ok((data, _)) => {
                let text = String::from_utf8(data)
                    .map_err(|e| MetadataError::SerializationError(format!("layout: {e}")))?;
                let mut lines = text.lines();
                // Line 1: layout format version (integer, skip it)
                let _format_version = lines.next()
                    .ok_or_else(|| MetadataError::SerializationError("empty layout".into()))?;
                // Line 2: "{managerFactoryClass}:{managerVersion}"
                let class_version = lines.next()
                    .ok_or_else(|| MetadataError::SerializationError("missing class:version in layout".into()))?;
                let (class, version_str) = class_version.rsplit_once(':')
                    .unwrap_or((class_version, "1"));
                let version: i32 = version_str.parse().unwrap_or(1);
                Ok(Some(LedgerLayout {
                    manager_factory_class: class.to_string(),
                    manager_version: version,
                }))
            }
            Err(zk::Error::NoNode) => Ok(None),
            Err(e) => Err(MetadataError::ConnectionError(format!("read layout: {e}"))),
        }
    }

    async fn store_ledger_layout(&self, layout: &LedgerLayout) -> Result<(), MetadataError> {
        // Java format: "{layoutFormatVersion}\n{class}:{managerVersion}"
        let data = format!("{}\n{}:{}", LAYOUT_FORMAT_VERSION, layout.manager_factory_class, layout.manager_version);
        self.client.create(self.paths.layout(), data.as_bytes(),
            &zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all())).await
            .map_err(|e| MetadataError::ConnectionError(format!("store layout: {e}")))?;
        Ok(())
    }

    async fn delete_ledger_layout(&self) -> Result<(), MetadataError> {
        match self.client.delete(self.paths.layout(), None).await {
            Ok(()) => Ok(()),
            Err(zk::Error::NoNode) => Ok(()),
            Err(e) => Err(MetadataError::ConnectionError(format!("delete layout: {e}"))),
        }
    }
}
