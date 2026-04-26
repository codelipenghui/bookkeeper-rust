use std::sync::Arc;

use async_trait::async_trait;
use liboxia::client::{GetOption, OxiaClient, PutOption};
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
        match self
            .client
            .get_with_options(
                self.paths.layout().to_string(),
                vec![GetOption::IncludeValue(true)],
            )
            .await
        {
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
        // Refuse to overwrite an existing layout. Two operators (or an
        // accidental re-run of `cluster init`) racing to install a layout
        // would otherwise silently clobber whichever one ran first; the
        // only safe behaviour is to surface the conflict.
        match self
            .client
            .put_with_options(
                self.paths.layout().to_string(),
                data.into_bytes(),
                vec![PutOption::ExpectVersionId(-1)],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(OxiaError::UnexpectedVersionId()) => Err(MetadataError::InvalidConfig(
                "layout already exists; refusing to overwrite an installed cluster layout".into(),
            )),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn delete_ledger_layout(&self) -> Result<(), MetadataError> {
        match self.client.delete(self.paths.layout().to_string()).await {
            Ok(_) => Ok(()),
            Err(OxiaError::KeyNotFound()) => Ok(()),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }
}
