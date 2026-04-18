use std::sync::Arc;

use async_trait::async_trait;
use liboxia::client::{DeleteOption, GetOption, OxiaClient, PutOption};
use liboxia::errors::OxiaError;

use super::errors::map_oxia_error;
use super::parents::create_parents;
use super::paths::child_range;
use crate::ledger_metadata::MetadataError;
use crate::metadata_paths::LedgerPaths;
use crate::service_info::BookieServiceInfo;
use crate::traits::{LedgerLayout, RegistrationManager};

pub struct OxiaRegistrationManager {
    client: Arc<OxiaClient>,
    paths: LedgerPaths,
}

impl OxiaRegistrationManager {
    pub fn new(client: Arc<OxiaClient>, paths: LedgerPaths) -> Self {
        Self { client, paths }
    }
}

#[async_trait]
impl RegistrationManager for OxiaRegistrationManager {
    async fn register_bookie(
        &self,
        bookie_id: &str,
        read_only: bool,
        info: &BookieServiceInfo,
    ) -> Result<(), MetadataError> {
        let key = if read_only {
            self.paths.bookie_readonly_registration(bookie_id)
        } else {
            self.paths.bookie_registration(bookie_id)
        };
        create_parents(&self.client, &key).await?;
        self.client
            .put_with_options(
                key,
                info.to_protobuf_bytes(),
                vec![PutOption::Ephemeral()],
            )
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        Ok(())
    }

    async fn unregister_bookie(
        &self,
        bookie_id: &str,
        read_only: bool,
    ) -> Result<(), MetadataError> {
        let key = if read_only {
            self.paths.bookie_readonly_registration(bookie_id)
        } else {
            self.paths.bookie_registration(bookie_id)
        };
        match self.client.delete(key).await {
            Ok(_) => Ok(()),
            Err(OxiaError::KeyNotFound()) => Ok(()),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn get_writable_bookies(&self) -> Result<Vec<String>, MetadataError> {
        let mut names = list_leaf_names(&self.client, self.paths.available()).await?;
        names.retain(|n| n != "readonly");
        Ok(names)
    }

    async fn get_readonly_bookies(&self) -> Result<Vec<String>, MetadataError> {
        list_leaf_names(&self.client, self.paths.available_readonly()).await
    }

    async fn read_cookie(
        &self,
        bookie_id: &str,
    ) -> Result<Option<(Vec<u8>, i64)>, MetadataError> {
        let key = self.paths.cookie_path(bookie_id);
        match self
            .client
            .get_with_options(key, vec![GetOption::IncludeValue()])
            .await
        {
            Ok(res) => {
                let Some(bytes) = res.value else { return Ok(None) };
                Ok(Some((bytes, res.version.version_id)))
            }
            Err(OxiaError::KeyNotFound()) => Ok(None),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn create_cookie(&self, bookie_id: &str, data: &[u8]) -> Result<(), MetadataError> {
        let key = self.paths.cookie_path(bookie_id);
        create_parents(&self.client, &key).await?;
        self.client
            .put_with_options(
                key,
                data.to_vec(),
                vec![PutOption::ExpectVersionId(-1)],
            )
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        Ok(())
    }

    async fn update_cookie(
        &self,
        bookie_id: &str,
        data: &[u8],
        version: i64,
    ) -> Result<(), MetadataError> {
        let key = self.paths.cookie_path(bookie_id);
        self.client
            .put_with_options(
                key,
                data.to_vec(),
                vec![PutOption::ExpectVersionId(version)],
            )
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        Ok(())
    }

    async fn delete_cookie(&self, bookie_id: &str, version: i64) -> Result<(), MetadataError> {
        let key = self.paths.cookie_path(bookie_id);
        self.client
            .delete_with_options(key, vec![DeleteOption::ExpectVersionId(version)])
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        Ok(())
    }

    async fn get_cluster_instance_id(&self) -> Result<Option<String>, MetadataError> {
        match self
            .client
            .get_with_options(
                self.paths.instance_id().to_string(),
                vec![GetOption::IncludeValue()],
            )
            .await
        {
            Ok(res) => {
                let Some(bytes) = res.value else { return Ok(None) };
                Ok(Some(String::from_utf8(bytes).map_err(|e| {
                    MetadataError::SerializationError(format!("instance id: {e}"))
                })?))
            }
            Err(OxiaError::KeyNotFound()) => Ok(None),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn init_new_cluster(
        &self,
        layout: &LedgerLayout,
        instance_id: &str,
    ) -> Result<(), MetadataError> {
        // Create-or-fail — matches ZK impl. We only need key existence,
        // not the value, so IncludeValue is intentionally omitted here.
        match self.client.get(self.paths.layout().to_string()).await {
            Ok(_) => {
                return Err(MetadataError::InvalidConfig(
                    "cluster already initialized (LAYOUT exists)".into(),
                ))
            }
            Err(OxiaError::KeyNotFound()) => {}
            Err(e) => return Err(map_oxia_error(e, None)),
        }

        create_parents(&self.client, self.paths.layout()).await?;
        self.client
            .put(
                self.paths.layout().to_string(),
                layout.serialize().into_bytes(),
            )
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        self.client
            .put(
                self.paths.instance_id().to_string(),
                instance_id.as_bytes().to_vec(),
            )
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        Ok(())
    }
}

async fn list_leaf_names(
    client: &OxiaClient,
    parent: &str,
) -> Result<Vec<String>, MetadataError> {
    let (start, end) = child_range(parent);
    let keys = client
        .list(start.clone(), end)
        .await
        .map_err(|e| map_oxia_error(e, None))?
        .keys;
    let mut names = Vec::with_capacity(keys.len());
    for k in keys {
        let Some(rest) = k.strip_prefix(&start) else {
            continue;
        };
        // Skip grand-children (keys containing further '/').
        if rest.contains('/') {
            continue;
        }
        names.push(rest.to_string());
    }
    Ok(names)
}
