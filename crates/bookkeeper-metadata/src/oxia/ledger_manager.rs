use std::sync::Arc;

use async_trait::async_trait;
use liboxia::client::{DeleteOption, GetOption, OxiaClient, PutOption};
use liboxia::errors::OxiaError;

use super::errors::map_oxia_error;
use super::parents::create_parents;
use super::paths::{ancestors_of, descendant_range};
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
        match self
            .client
            .get_with_options(key, vec![GetOption::IncludeValue()])
            .await
        {
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
                    // Use descendant_range (not child_range) because oxia's
                    // segment-wise ordering requires the upper bound to be
                    // derived by incrementing the last byte of the parent
                    // segment, not by appending a sentinel byte within the
                    // same segment — otherwise deep paths sort outside the
                    // child_range window.
                    let (start, end) = descendant_range(&anc);
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
        // Range-scan over all descendants of the ledger root and filter
        // for leaf keys of the form "L{NNNN}". We must use descendant_range
        // (not child_range) because oxia's segment-wise key ordering
        // places nested paths outside a child_range window — see paths.rs.
        let (start, end) = descendant_range(self.paths.root());
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
