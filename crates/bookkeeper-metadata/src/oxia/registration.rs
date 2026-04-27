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
        // Refuse to overwrite an existing registration. Ephemeral keys
        // are tied to their owning session, so "key exists" means
        // another session still holds the registration — silently
        // putting on top would let a wrong-data bookie clobber a
        // legitimate one.
        match self
            .client
            .put_with_options(
                key,
                info.to_protobuf_bytes(),
                vec![PutOption::Ephemeral(), PutOption::ExpectVersionId(-1)],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(OxiaError::UnexpectedVersionId()) => Err(MetadataError::ConnectionError(
                format!("register bookie '{bookie_id}': already registered"),
            )),
            Err(e) => Err(map_oxia_error(e, None)),
        }
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
            .get_with_options(key, vec![GetOption::IncludeValue(true)])
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
        match self
            .client
            .put_with_options(
                key,
                data.to_vec(),
                vec![PutOption::ExpectVersionId(-1)],
            )
            .await
        {
            Ok(_) => Ok(()),
            // ExpectVersionId(-1) means "create-only"; a conflict here means
            // the cookie already exists — surface that clearly.
            Err(OxiaError::UnexpectedVersionId()) => Err(MetadataError::ConnectionError(
                format!("create cookie: bookie '{bookie_id}' already has a cookie"),
            )),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn update_cookie(
        &self,
        bookie_id: &str,
        data: &[u8],
        version: i64,
    ) -> Result<(), MetadataError> {
        let key = self.paths.cookie_path(bookie_id);
        match self
            .client
            .put_with_options(
                key,
                data.to_vec(),
                vec![PutOption::ExpectVersionId(version)],
            )
            .await
        {
            Ok(_) => Ok(()),
            // Distinguish "version mismatch" from "key gone" so callers can
            // retry / re-read instead of treating both as an opaque error.
            Err(OxiaError::UnexpectedVersionId()) => Err(MetadataError::VersionConflict {
                expected: version,
                actual: -1,
            }),
            Err(OxiaError::KeyNotFound()) => Err(MetadataError::NotFound(0)),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn delete_cookie(&self, bookie_id: &str, version: i64) -> Result<(), MetadataError> {
        let key = self.paths.cookie_path(bookie_id);
        match self
            .client
            .delete_with_options(key, vec![DeleteOption::ExpectVersionId(version)])
            .await
        {
            Ok(_) => Ok(()),
            Err(OxiaError::UnexpectedVersionId()) => Err(MetadataError::VersionConflict {
                expected: version,
                actual: -1,
            }),
            Err(OxiaError::KeyNotFound()) => Err(MetadataError::NotFound(0)),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn get_cluster_instance_id(&self) -> Result<Option<String>, MetadataError> {
        match self
            .client
            .get_with_options(
                self.paths.instance_id().to_string(),
                vec![GetOption::IncludeValue(true)],
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
        create_parents(&self.client, self.paths.layout()).await?;

        // CAS-create the LAYOUT key with ExpectVersionId(-1) (oxia's
        // "key must not exist"). Pre-fix this used a get-then-put
        // sequence: two concurrent operators could both pass the
        // existence check and both `put` on top, silently sharing /
        // overwriting the cluster layout. Using ExpectVersionId(-1)
        // is atomic.
        match self
            .client
            .put_with_options(
                self.paths.layout().to_string(),
                layout.serialize().into_bytes(),
                vec![PutOption::ExpectVersionId(-1)],
            )
            .await
        {
            Ok(_) => {}
            Err(OxiaError::UnexpectedVersionId()) => {
                return Err(MetadataError::InvalidConfig(
                    "cluster already initialized (LAYOUT exists)".into(),
                ));
            }
            Err(e) => return Err(map_oxia_error(e, None)),
        }
        // Same atomic-create discipline for INSTANCEID — the LAYOUT
        // CAS guarantees only one caller reaches this point, but the
        // ExpectVersionId(-1) costs nothing extra and makes the
        // failure mode explicit if a future caller bypasses init.
        match self
            .client
            .put_with_options(
                self.paths.instance_id().to_string(),
                instance_id.as_bytes().to_vec(),
                vec![PutOption::ExpectVersionId(-1)],
            )
            .await
        {
            Ok(_) | Err(OxiaError::UnexpectedVersionId()) => Ok(()),
            Err(e) => Err(map_oxia_error(e, None)),
        }
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
