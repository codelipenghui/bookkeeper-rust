use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use liboxia::client::{GetOption, OxiaClient, PutOption};
use liboxia::errors::OxiaError;

use super::errors::map_oxia_error;
use super::paths::child_range;
use crate::ledger_metadata::MetadataError;
use crate::metadata_paths::{HOB_GC_KEEP, HOB_PREFIX, LedgerPaths, SHORT_ID_PREFIX};
use crate::traits::LedgerIdGenerator;

/// How many times `generate_long` retries on HOB overflow before giving
/// up. Matches the ZK implementation; a real cluster should never need
/// more than 2 (`suffix == i32::MAX` in HOB_N → create HOB_{N+1}, retry).
const LONG_ID_MAX_RETRIES: usize = 5;

pub struct OxiaLedgerIdGenerator {
    client: Arc<OxiaClient>,
    short_gen_dir: String,   // "{root}/idgen"
    long_gen_dir: String,    // "{root}/idgen-long"
    long_path_known: Mutex<Option<bool>>,
    /// Once the short-id directory has been ensured once on the server,
    /// every subsequent ledger create can skip the ensure put RPC. The
    /// cache only ratchets to true; if the server were reset the cluster
    /// wouldn't survive anyway.
    short_gen_dir_ensured: AtomicBool,
}

impl OxiaLedgerIdGenerator {
    pub fn new(client: Arc<OxiaClient>, paths: &LedgerPaths) -> Self {
        Self {
            client,
            short_gen_dir: paths.idgen().to_string(),
            long_gen_dir: paths.idgen_long().to_string(),
            long_path_known: Mutex::new(None),
            short_gen_dir_ensured: AtomicBool::new(false),
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

    fn hob_path(&self, hob: i32) -> String {
        format!("{}/{}{:010}", self.long_gen_dir, HOB_PREFIX, hob)
    }

    /// Counter key for the short-id generator. Stored as a sibling of
    /// `idgen` so the counter outlives any deletion / recreation of
    /// the `idgen` record itself (which would otherwise reset the
    /// suffix back to its initial value and reissue ledger IDs).
    fn short_counter_key(&self) -> String {
        format!("{}-counter", self.short_gen_dir)
    }

    /// Counter key for a long-id HOB. Stored next to (not inside) the
    /// HOB record so HOB GC's delete leaves the counter intact —
    /// ensuring that any future generator that reuses this HOB never
    /// reissues a previously-allocated suffix.
    ///
    /// `list_hobs`'s leaf parser strips `HOB-` then parses the
    /// remainder as an `i32`, so the trailing `.counter` segment of
    /// these counter keys is automatically filtered out and never
    /// mistaken for a real HOB.
    fn hob_counter_key(&self, hob: i32) -> String {
        format!("{}/{}{:010}.counter", self.long_gen_dir, HOB_PREFIX, hob)
    }

    /// Atomically increment the i64 value stored at `key` and return
    /// the new value. Independent of the record's
    /// `modifications_count` so unrelated writers (or a delete that
    /// resets that counter) cannot influence the suffix.
    ///
    /// CAS-retries on `UnexpectedVersionId`, matching the standard
    /// optimistic-locking idiom.
    async fn cas_increment_counter(&self, key: &str) -> Result<i64, MetadataError> {
        loop {
            let (current, expected_version) = match self
                .client
                .get_with_options(key.to_string(), vec![GetOption::IncludeValue(true)])
                .await
            {
                Ok(rec) => {
                    let val = rec
                        .value
                        .as_deref()
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.trim().parse::<i64>().ok())
                        .unwrap_or(0);
                    (val, rec.version.version_id)
                }
                Err(OxiaError::KeyNotFound()) => (0, -1),
                Err(e) => return Err(map_oxia_error(e, None)),
            };
            let next = current + 1;
            let result = self
                .client
                .put_with_options(
                    key.to_string(),
                    next.to_string().into_bytes(),
                    vec![PutOption::ExpectVersionId(expected_version)],
                )
                .await;
            match result {
                Ok(_) => return Ok(next),
                Err(OxiaError::UnexpectedVersionId()) => continue,
                Err(e) => return Err(map_oxia_error(e, None)),
            }
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

    /// Issue the next sequential suffix from a dedicated counter at
    /// `counter_key`, then run the Pulsar-style ephemeral child marker
    /// (create+delete) at `child_prefix{NNN}` and return that key.
    ///
    /// The suffix is taken from the counter's stored i64 value, not
    /// the parent record's `modifications_count`, so unrelated writes
    /// to the parent (or a delete-then-recreate by GC) cannot reissue
    /// a previously-allocated id.
    async fn seq_create(
        &self,
        counter_key: &str,
        child_prefix: &str,
    ) -> Result<String, MetadataError> {
        let suffix = self.cas_increment_counter(counter_key).await?;
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
        if !self.short_gen_dir_ensured.load(Ordering::Acquire) {
            self.ensure_path(&self.short_gen_dir).await?;
            self.short_gen_dir_ensured.store(true, Ordering::Release);
        }
        let prefix = format!("{}/{}", self.short_gen_dir, SHORT_ID_PREFIX);
        let counter_key = self.short_counter_key();
        let key = self.seq_create(&counter_key, &prefix).await?;
        let suffix = &key[prefix.len()..];
        let id: i64 = suffix
            .parse()
            .map_err(|e| MetadataError::SerializationError(format!("idgen parse: {e}")))?;
        Ok(id)
    }

    /// List the immediate HOB children of `idgen-long`. We deliberately
    /// use `child_range` (not `descendant_range`): HOB dirs are direct
    /// children, and constraining the range this way avoids downloading
    /// the `ID-*` grandchildren that accumulate inside each HOB.
    async fn list_hobs(&self) -> Result<Vec<i32>, MetadataError> {
        let (start, end) = child_range(&self.long_gen_dir);
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
        let path = self.hob_path(hob);
        self.ensure_path(&path).await?;
        Ok(path)
    }

    pub async fn gc_old_hobs(&self, hobs: &[i32]) {
        if hobs.len() <= HOB_GC_KEEP {
            return;
        }
        for &hob in &hobs[..hobs.len() - HOB_GC_KEEP] {
            let hob_path = self.hob_path(hob);
            // Skip HOBs that still hold transient child markers (e.g. an
            // in-flight seq_create on this HOB hasn't finished its
            // ephemeral create+delete yet). Deleting such a HOB drops
            // the ephemeral child along with it, breaking the marker
            // semantics and leaving the slow generator pointing at a
            // path the server has forgotten.
            let (start, end) = child_range(&hob_path);
            let has_children = matches!(
                self.client.list(start, end).await,
                Ok(r) if !r.keys.is_empty()
            );
            if has_children {
                continue;
            }
            let _ = self.client.delete(hob_path).await;
        }
    }

    async fn generate_long(&self) -> Result<i64, MetadataError> {
        for _ in 0..LONG_ID_MAX_RETRIES {
            let mut hobs = self.list_hobs().await?;
            let high_bits = match hobs.last() {
                Some(&m) => m,
                None => {
                    self.create_hob(1).await?;
                    hobs.push(1);
                    1
                }
            };
            let hob_path = self.hob_path(high_bits);
            let prefix = format!("{hob_path}/{}", SHORT_ID_PREFIX);
            let counter_key = self.hob_counter_key(high_bits);
            let key = self.seq_create(&counter_key, &prefix).await?;
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
