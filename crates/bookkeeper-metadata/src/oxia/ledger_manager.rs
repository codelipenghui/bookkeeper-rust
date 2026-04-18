use std::sync::Arc;

use async_trait::async_trait;
use liboxia::client::{DeleteOption, OxiaClient, PutOption};
use liboxia::errors::OxiaError;

use super::errors::map_oxia_error;
use super::parents::create_parents;
use super::paths::{ancestors_of, child_range};
use crate::ledger_metadata::{LedgerMetadata, MetadataError};
use crate::metadata_paths::{ledger_path, LedgerPaths};
use crate::metadata_serde::{deserialize_metadata, serialize_metadata};
use crate::traits::LedgerManager;
use crate::versioned::Versioned;

pub struct OxiaLedgerManager {
    client: Arc<OxiaClient>,
    paths: LedgerPaths,
}

impl OxiaLedgerManager {
    pub fn new(client: Arc<OxiaClient>, paths: LedgerPaths) -> Self {
        Self { client, paths }
    }

    fn key(&self, ledger_id: i64) -> String {
        ledger_path(self.paths.root(), ledger_id)
    }
}

#[async_trait]
impl LedgerManager for OxiaLedgerManager {
    async fn create_ledger_metadata(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
        let key = self.key(ledger_id);
        create_parents(&self.client, &key).await?;
        let bytes = serialize_metadata(metadata)?;
        match self
            .client
            .put_with_options(
                key,
                bytes,
                vec![PutOption::ExpectVersionId(-1)],
            )
            .await
        {
            Ok(res) => Ok(Versioned::new(metadata.clone(), res.version.version_id)),
            Err(OxiaError::UnexpectedVersionId()) => {
                Err(MetadataError::LedgerExists(ledger_id))
            }
            Err(e) => Err(map_oxia_error(e, Some(ledger_id))),
        }
    }

    async fn read_ledger_metadata(
        &self,
        ledger_id: i64,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
        let key = self.key(ledger_id);
        match self.client.get(key).await {
            Ok(res) => {
                let bytes = res.value.ok_or(MetadataError::NotFound(ledger_id))?;
                let md = deserialize_metadata(ledger_id, &bytes)?;
                Ok(Versioned::new(md, res.version.version_id))
            }
            Err(OxiaError::KeyNotFound()) => Err(MetadataError::NotFound(ledger_id)),
            Err(e) => Err(map_oxia_error(e, Some(ledger_id))),
        }
    }

    async fn write_ledger_metadata(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
        current_version: i64,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
        let key = self.key(ledger_id);
        let bytes = serialize_metadata(metadata)?;
        match self
            .client
            .put_with_options(
                key,
                bytes,
                vec![PutOption::ExpectVersionId(current_version)],
            )
            .await
        {
            Ok(res) => Ok(Versioned::new(metadata.clone(), res.version.version_id)),
            Err(OxiaError::UnexpectedVersionId()) => Err(MetadataError::VersionConflict {
                expected: current_version,
                actual: -1,
            }),
            Err(OxiaError::KeyNotFound()) => Err(MetadataError::NotFound(ledger_id)),
            Err(e) => Err(map_oxia_error(e, Some(ledger_id))),
        }
    }

    async fn remove_ledger_metadata(
        &self,
        ledger_id: i64,
        version: i64,
    ) -> Result<(), MetadataError> {
        let key = self.key(ledger_id);
        match self
            .client
            .delete_with_options(
                key.clone(),
                vec![DeleteOption::ExpectVersionId(version)],
            )
            .await
        {
            Ok(_) => {
                // Best-effort ancestor cleanup — mirrors Java BK's
                // asyncDeleteFullPathOptimistic. Ignore all errors.
                for anc in ancestors_of(&key).into_iter().rev() {
                    if anc == self.paths.root() || !anc.starts_with(self.paths.root()) {
                        break;
                    }
                    let (start, end) = child_range(&anc);
                    let has_children = matches!(
                        self.client.list(start, end).await,
                        Ok(r) if !r.keys.is_empty()
                    );
                    if has_children {
                        break;
                    }
                    let _ = self.client.delete(anc).await;
                }
                Ok(())
            }
            Err(OxiaError::KeyNotFound()) => Err(MetadataError::NotFound(ledger_id)),
            Err(OxiaError::UnexpectedVersionId()) => Err(MetadataError::VersionConflict {
                expected: version,
                actual: -1,
            }),
            Err(e) => Err(map_oxia_error(e, Some(ledger_id))),
        }
    }

    async fn get_ledger_ranges(&self) -> Result<Vec<i64>, MetadataError> {
        // Range-scan over the entire ledger root and parse leaf names of
        // the form "L{NNNN}".
        let (start, end) = child_range(self.paths.root());
        let keys = self
            .client
            .list(start, end)
            .await
            .map_err(|e| map_oxia_error(e, None))?
            .keys;

        let mut ids = Vec::new();
        for key in keys {
            let Some(leaf) = key.rsplit('/').next() else { continue };
            let Some(num) = leaf.strip_prefix('L') else { continue };
            let Ok(id) = num.parse::<i64>() else { continue };
            ids.push(id);
        }
        ids.sort_unstable();
        Ok(ids)
    }
}
