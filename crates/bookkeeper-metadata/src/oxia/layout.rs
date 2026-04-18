use std::sync::Arc;

use async_trait::async_trait;
use liboxia::client::OxiaClient;
use liboxia::errors::OxiaError;

use super::errors::map_oxia_error;
use super::parents::create_parents;
use crate::ledger_metadata::MetadataError;
use crate::metadata_paths::LedgerPaths;
use crate::traits::{LayoutManager, LedgerLayout};

pub struct OxiaLayoutManager {
    client: Arc<OxiaClient>,
    paths: LedgerPaths,
}

impl OxiaLayoutManager {
    pub fn new(client: Arc<OxiaClient>, paths: LedgerPaths) -> Self {
        Self { client, paths }
    }
}

#[async_trait]
impl LayoutManager for OxiaLayoutManager {
    async fn read_ledger_layout(&self) -> Result<Option<LedgerLayout>, MetadataError> {
        match self.client.get(self.paths.layout().to_string()).await {
            Ok(res) => {
                let Some(bytes) = res.value else { return Ok(None) };
                let text = String::from_utf8(bytes)
                    .map_err(|e| MetadataError::SerializationError(format!("layout: {e}")))?;
                Ok(Some(LedgerLayout::deserialize(&text)?))
            }
            Err(OxiaError::KeyNotFound()) => Ok(None),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn store_ledger_layout(&self, layout: &LedgerLayout) -> Result<(), MetadataError> {
        create_parents(&self.client, self.paths.layout()).await?;
        let data = layout.serialize();
        self.client
            .put(self.paths.layout().to_string(), data.into_bytes())
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        Ok(())
    }

    async fn delete_ledger_layout(&self) -> Result<(), MetadataError> {
        match self.client.delete(self.paths.layout().to_string()).await {
            Ok(_) => Ok(()),
            Err(OxiaError::KeyNotFound()) => Ok(()),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }
}
