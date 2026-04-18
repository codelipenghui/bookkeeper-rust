use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use liboxia::client::{OxiaClient, PutOption};
use liboxia::errors::OxiaError;

use super::errors::map_oxia_error;
use super::paths::descendant_range;
use crate::ledger_metadata::MetadataError;
use crate::metadata_paths::LedgerPaths;
use crate::traits::LedgerIdGenerator;

const SHORT_ID_PREFIX: &str = "ID-";
const HOB_PREFIX: &str = "HOB-";
const HOB_GC_KEEP: usize = 3;

pub struct OxiaLedgerIdGenerator {
    client: Arc<OxiaClient>,
    short_gen_dir: String,   // "{root}/idgen"
    long_gen_dir: String,    // "{root}/idgen-long"
    long_path_known: Mutex<Option<bool>>,
}

impl OxiaLedgerIdGenerator {
    pub fn new(client: Arc<OxiaClient>, paths: &LedgerPaths) -> Self {
        Self {
            client,
            short_gen_dir: paths.idgen().to_string(),
            long_gen_dir: paths.idgen_long().to_string(),
            long_path_known: Mutex::new(None),
        }
    }

    /// Best-effort creation of an empty-valued record at `path`.
    /// Swallows UnexpectedVersionId (already exists).
    async fn ensure_path(&self, path: &str) -> Result<(), MetadataError> {
        match self
            .client
            .put_with_options(
                path.to_string(),
                Vec::new(),
                vec![PutOption::ExpectVersionId(-1)],
            )
            .await
        {
            Ok(_) | Err(OxiaError::UnexpectedVersionId()) => Ok(()),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn is_long_present(&self) -> Result<bool, MetadataError> {
        if let Some(v) = *self.long_path_known.lock().unwrap() {
            return Ok(v);
        }
        let exists = match self.client.get(self.long_gen_dir.clone()).await {
            Ok(_) => true,
            Err(OxiaError::KeyNotFound()) => false,
            Err(e) => return Err(map_oxia_error(e, None)),
        };
        *self.long_path_known.lock().unwrap() = Some(exists);
        Ok(exists)
    }

    /// Do one sequential-create under `parent`: put an empty value on
    /// `parent` itself, read the resulting `modifications_count`, and
    /// use it as the suffix on `child_prefix`. Returns the fabricated
    /// child key. For Pulsar parity we also create+delete the ephemeral
    /// child record so that any observer walking children sees them
    /// transiently.
    async fn seq_create(
        &self,
        parent: &str,
        child_prefix: &str,
    ) -> Result<String, MetadataError> {
        let res = self
            .client
            .put(parent.to_string(), Vec::new())
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        let suffix = res.version.modifications_count;
        let key = format!("{child_prefix}{:010}", suffix);
        // Parity with Pulsar: create+delete the child as an ephemeral
        // marker. Both operations are best-effort.
        let _ = self
            .client
            .put_with_options(
                key.clone(),
                Vec::new(),
                vec![PutOption::Ephemeral()],
            )
            .await;
        let _ = self.client.delete(key.clone()).await;
        Ok(key)
    }

    async fn generate_short(&self) -> Result<i64, MetadataError> {
        self.ensure_path(&self.short_gen_dir).await?;
        let prefix = format!("{}/{}", self.short_gen_dir, SHORT_ID_PREFIX);
        let key = self.seq_create(&self.short_gen_dir, &prefix).await?;
        let suffix = &key[prefix.len()..];
        let id: i64 = suffix
            .parse()
            .map_err(|e| MetadataError::SerializationError(format!("idgen parse: {e}")))?;
        Ok(id)
    }

    async fn list_hobs(&self) -> Result<Vec<i32>, MetadataError> {
        let (start, end) = descendant_range(&self.long_gen_dir);
        let keys = self
            .client
            .list(start.clone(), end)
            .await
            .map_err(|e| map_oxia_error(e, None))?
            .keys;
        let mut hobs: Vec<i32> = keys
            .into_iter()
            .filter_map(|k| {
                let leaf = k.rsplit('/').next()?;
                leaf.strip_prefix(HOB_PREFIX)?.parse::<i32>().ok()
            })
            .collect();
        hobs.sort_unstable();
        Ok(hobs)
    }

    async fn create_hob(&self, hob: i32) -> Result<String, MetadataError> {
        let path = format!("{}/{}{:010}", self.long_gen_dir, HOB_PREFIX, hob);
        self.ensure_path(&path).await?;
        Ok(path)
    }

    async fn gc_old_hobs(&self, hobs: &[i32]) {
        if hobs.len() <= HOB_GC_KEEP {
            return;
        }
        for &hob in &hobs[..hobs.len() - HOB_GC_KEEP] {
            let path = format!("{}/{}{:010}", self.long_gen_dir, HOB_PREFIX, hob);
            let _ = self.client.delete(path).await;
        }
    }

    async fn generate_long(&self) -> Result<i64, MetadataError> {
        for _ in 0..5 {
            let mut hobs = self.list_hobs().await?;
            let high_bits = match hobs.last() {
                Some(&m) => m,
                None => {
                    self.create_hob(1).await?;
                    hobs.push(1);
                    1
                }
            };
            let hob_path = format!("{}/{}{:010}", self.long_gen_dir, HOB_PREFIX, high_bits);
            let prefix = format!("{hob_path}/{}", SHORT_ID_PREFIX);
            let key = self.seq_create(&hob_path, &prefix).await?;
            let suffix: i64 = key[prefix.len()..]
                .parse()
                .map_err(|e| MetadataError::SerializationError(format!("idgen parse: {e}")))?;
            if suffix >= i32::MAX as i64 {
                // Low bits exhausted for this HOB — create next and retry.
                self.create_hob(high_bits + 1).await?;
                continue;
            }
            self.gc_old_hobs(&hobs).await;
            return Ok(((high_bits as i64) << 32) | suffix);
        }
        Err(MetadataError::ConnectionError(
            "oxia long ID generation failed after retries".into(),
        ))
    }
}

#[async_trait]
impl LedgerIdGenerator for OxiaLedgerIdGenerator {
    async fn generate_ledger_id(&self) -> Result<i64, MetadataError> {
        if !self.is_long_present().await? {
            match self.generate_short().await {
                Ok(id) if id < i32::MAX as i64 => return Ok(id),
                Ok(_) => {
                    // Short space exhausted — create long path and fall through.
                    self.ensure_path(&self.long_gen_dir).await?;
                    *self.long_path_known.lock().unwrap() = Some(true);
                }
                Err(e) => return Err(e),
            }
        }
        self.generate_long().await
    }
}
