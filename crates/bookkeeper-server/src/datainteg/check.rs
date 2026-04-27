//! Data-integrity orchestration: pre-boot check and full recovery check.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bookkeeper_metadata::iterator::{stream_all_ledgers, DEFAULT_MAX_INFLIGHT};
use bookkeeper_metadata::ledger_metadata::{LedgerMetadata, LedgerState, MetadataError};
use bookkeeper_metadata::traits::LedgerManager;
use bookkeeper_metadata::versioned::Versioned;
use bookkeeper_storage::ledger_storage::{LedgerStorage, StorageStateFlag};
use bookkeeper_storage::StorageError;
use futures::{stream, StreamExt};
use parking_lot::Mutex as StdMutex;
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

use super::entry_copier::{write_set_for_entry, Batch, EntryCopier, EntryCopyError};

const DEFAULT_MAX_ENTRIES_INFLIGHT: usize = 3000;

#[derive(Debug, Error)]
pub enum DataIntegError {
    #[error("metadata error: {0}")]
    Metadata(#[from] MetadataError),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("entry copy error: {0}")]
    EntryCopy(#[from] EntryCopyError),
    #[error("recovery error: {0}")]
    Recovery(String),
    #[error("ledger deleted during recovery: {0}")]
    LedgerDeleted(i64),
}

/// Error from a ledger-recovery attempt. Implementations return
/// `LedgerDeleted` when the ledger has been removed so the check can
/// move on instead of retrying forever.
#[derive(Debug, Error)]
pub enum RecoverError {
    #[error("ledger deleted")]
    LedgerDeleted,
    #[error("{0}")]
    Other(String),
}

/// Async callback the data-integrity check uses to fence-and-close a
/// ledger that was found in limbo. Implementations typically wrap a
/// `ZkBookKeeper`-style client; the `DataIntegrityCheck` doesn't
/// depend on `bookkeeper-client` directly so it stays testable.
#[async_trait]
pub trait LedgerRecoveryClient: Send + Sync {
    /// Recover an open ledger — fence, read recovery, close — and
    /// return the updated metadata.
    async fn recover_ledger(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
    ) -> Result<LedgerMetadata, RecoverError>;
}

/// Result bucket for a single ledger in the full check.
enum LedgerResult {
    Ok,
    /// Ledger no longer exists in metadata — treat as repaired.
    Missing,
    Error(DataIntegError, Versioned<LedgerMetadata>),
}

/// Runtime configuration for a `DataIntegrityCheck`.
pub struct DataIntegrityConfig {
    pub max_inflight: usize,
    pub max_entries_inflight: usize,
}

impl Default for DataIntegrityConfig {
    fn default() -> Self {
        Self {
            max_inflight: DEFAULT_MAX_INFLIGHT,
            max_entries_inflight: DEFAULT_MAX_ENTRIES_INFLIGHT,
        }
    }
}

/// Bookie-side data-integrity check. Exposes `runPreBootCheck` and
/// `runFullCheck` matching the Java interface of the same names.
pub struct DataIntegrityCheck {
    bookie_id: String,
    ledger_manager: Arc<dyn LedgerManager>,
    storage: Arc<dyn LedgerStorage>,
    entry_copier: Arc<EntryCopier>,
    recovery: Arc<dyn LedgerRecoveryClient>,
    config: DataIntegrityConfig,

    /// Set once `runPreBootCheck` has completed — subsequent calls
    /// return the cached outcome. Matches Java's `synchronized(this) { preBootFuture ... }`.
    preboot_done: AsyncMutex<bool>,

    /// Ledgers owned by this bookie that still need `runFullCheck` attention.
    /// Populated by the pre-boot scan; drained by each full-check pass. After
    /// a clean pass `remaining` is empty and the `NEEDS_INTEGRITY_CHECK` flag
    /// is cleared.
    cached: StdMutex<Option<HashMap<i64, Versioned<LedgerMetadata>>>>,
}

impl DataIntegrityCheck {
    pub fn new(
        bookie_id: String,
        ledger_manager: Arc<dyn LedgerManager>,
        storage: Arc<dyn LedgerStorage>,
        entry_copier: Arc<EntryCopier>,
        recovery: Arc<dyn LedgerRecoveryClient>,
    ) -> Self {
        Self::with_config(
            bookie_id,
            ledger_manager,
            storage,
            entry_copier,
            recovery,
            DataIntegrityConfig::default(),
        )
    }

    pub fn with_config(
        bookie_id: String,
        ledger_manager: Arc<dyn LedgerManager>,
        storage: Arc<dyn LedgerStorage>,
        entry_copier: Arc<EntryCopier>,
        recovery: Arc<dyn LedgerRecoveryClient>,
        config: DataIntegrityConfig,
    ) -> Self {
        Self {
            bookie_id,
            ledger_manager,
            storage,
            entry_copier,
            recovery,
            config,
            preboot_done: AsyncMutex::new(false),
            cached: StdMutex::new(None),
        }
    }

    /// Whether a full check still needs to run. Returns true if the
    /// `NEEDS_INTEGRITY_CHECK` storage flag is set.
    pub fn needs_full_check(&self) -> Result<bool, DataIntegError> {
        Ok(self.storage.get_storage_state_flags()?
            & StorageStateFlag::NeedsIntegrityCheck.bits()
            != 0)
    }

    /// Quick pre-boot check. Marks the bookie as needing an integrity check,
    /// scans every ledger this bookie participates in, and puts any ledger
    /// where we appear in the LAST non-closed ensemble into limbo (+ fence).
    /// Safe to call multiple times — runs at most once per instance.
    pub async fn run_preboot_check(&self, reason: &str) -> Result<(), DataIntegError> {
        let mut done = self.preboot_done.lock().await;
        if *done {
            return Ok(());
        }
        tracing::info!(reason, "data-integrity: pre-boot check starting");

        self.storage.set_storage_state_flag(StorageStateFlag::NeedsIntegrityCheck)?;

        let mut ledgers: HashMap<i64, Versioned<LedgerMetadata>> = HashMap::new();
        let s =
            stream_all_ledgers(Arc::clone(&self.ledger_manager), self.config.max_inflight);
        futures::pin_mut!(s);
        while let Some(item) = s.next().await {
            let (id, versioned) = match item {
                Ok(v) => v,
                // Deleted between list and read: skip.
                Err(MetadataError::NotFound(_)) => continue,
                Err(e) => return Err(e.into()),
            };
            let md = versioned.value();
            if !ensemble_contains_bookie(md, &self.bookie_id) {
                continue;
            }
            self.preboot_ledger(id, md)?;
            ledgers.insert(id, versioned);
        }

        self.storage.flush().await?;
        let count = ledgers.len();
        *self.cached.lock() = Some(ledgers);
        *done = true;
        tracing::info!(reason, count, "data-integrity: pre-boot check complete");
        Ok(())
    }

    fn preboot_ledger(&self, id: i64, md: &LedgerMetadata) -> Result<(), DataIntegError> {
        // Stamp the SHA-1-derived master key (not an empty placeholder) on
        // first creation. The previous empty stamp left the row's master
        // key as `[]`, and `EntryCopier::new_batch` only re-stamps when the
        // row doesn't yet exist — so the real key was never written, and
        // every fenced read returned Unauthorized while the first inbound
        // AddEntry could overwrite the empty key with whatever the caller
        // sent.
        if !self.storage.ledger_exists(id)? {
            let pw = md.password().unwrap_or(&[]);
            let master_key = bookkeeper_protocol::digest::generate_master_key(pw);
            self.storage.set_master_key(id, &master_key)?;
        }
        if md.state() == LedgerState::Closed {
            return Ok(());
        }
        // If we are in the last ensemble of a non-closed ledger, enter
        // limbo to stop serving reads and fence to stop serving writes.
        if let Some((_, last_ensemble)) = md.all_ensembles().iter().next_back() {
            if last_ensemble.iter().any(|b| b == &self.bookie_id) {
                tracing::info!(ledger = id, "data-integrity: marking ledger limbo+fenced");
                self.storage.set_limbo_state(id)?;
                self.storage.set_fenced(id)?;
            }
        }
        Ok(())
    }

    /// Full recovery pass: clear limbo by fencing+recovering each ledger,
    /// then copy any entries this bookie is supposed to own but doesn't.
    /// On a clean pass the `NEEDS_INTEGRITY_CHECK` flag is cleared.
    /// Intermittently-failing ledgers stay in the cache so the next call
    /// retries only them.
    pub async fn run_full_check(&self) -> Result<(), DataIntegError> {
        let ledgers = self.get_or_load_cache().await?;
        tracing::info!(ledger_count = ledgers.len(), "data-integrity: full check starting");

        let mut remaining: HashMap<i64, Versioned<LedgerMetadata>> = HashMap::new();
        let mut first_error: Option<DataIntegError> = None;
        let mut ok = 0usize;
        let mut missing = 0usize;
        let mut errors = 0usize;

        for (id, versioned) in ledgers.into_iter() {
            match self.check_and_recover_ledger(id, &versioned).await {
                LedgerResult::Ok => ok += 1,
                LedgerResult::Missing => missing += 1,
                LedgerResult::Error(e, v) => {
                    errors += 1;
                    first_error.get_or_insert(e);
                    remaining.insert(id, v);
                }
            }
        }

        self.storage.flush().await?;
        if remaining.is_empty() {
            tracing::info!("data-integrity: clearing NEEDS_INTEGRITY_CHECK flag");
            self.storage
                .clear_storage_state_flag(StorageStateFlag::NeedsIntegrityCheck)?;
        }
        *self.cached.lock() = Some(remaining);

        tracing::info!(ok, missing, errors, "data-integrity: full check complete");
        if let Some(e) = first_error {
            return Err(e);
        }
        Ok(())
    }

    async fn get_or_load_cache(
        &self,
    ) -> Result<HashMap<i64, Versioned<LedgerMetadata>>, DataIntegError> {
        if let Some(cached) = self.cached.lock().take() {
            return Ok(cached);
        }
        tracing::info!("data-integrity: refreshing ledger cache");
        let mut m = HashMap::new();
        let s =
            stream_all_ledgers(Arc::clone(&self.ledger_manager), self.config.max_inflight);
        futures::pin_mut!(s);
        while let Some(item) = s.next().await {
            match item {
                Ok((id, v)) => {
                    if ensemble_contains_bookie(v.value(), &self.bookie_id) {
                        m.insert(id, v);
                    }
                }
                Err(MetadataError::NotFound(_)) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(m)
    }

    async fn check_and_recover_ledger(
        &self,
        ledger_id: i64,
        versioned: &Versioned<LedgerMetadata>,
    ) -> LedgerResult {
        // Phase 1: if ledger is in limbo, fence + recover it so the
        // metadata has a definite last entry.
        let effective = match self.recover_if_limbo(ledger_id, versioned.value()).await {
            Ok(Some(md)) => md,
            Ok(None) => return LedgerResult::Missing,
            Err(e) => return LedgerResult::Error(e, versioned.clone()),
        };
        // Phase 2: check + recover entries.
        match self.check_and_recover_entries(ledger_id, &effective).await {
            Ok(()) => LedgerResult::Ok,
            Err(DataIntegError::LedgerDeleted(_)) => LedgerResult::Missing,
            Err(e) => LedgerResult::Error(e, versioned.clone()),
        }
    }

    async fn recover_if_limbo(
        &self,
        ledger_id: i64,
        original: &LedgerMetadata,
    ) -> Result<Option<LedgerMetadata>, DataIntegError> {
        // Pre-boot may have put the local entry in limbo even if the bookie
        // doesn't know about it yet; stamp the real SHA-1 master key first
        // so the storage row exists with a key that matches what real clients
        // will present.
        if !self.storage.ledger_exists(ledger_id)? {
            let pw = original.password().unwrap_or(&[]);
            let master_key = bookkeeper_protocol::digest::generate_master_key(pw);
            self.storage.set_master_key(ledger_id, &master_key)?;
        }
        if !self.storage.has_limbo_state(ledger_id)? {
            return Ok(Some(original.clone()));
        }
        match self.recovery.recover_ledger(ledger_id, original).await {
            Ok(new_md) => {
                self.storage.clear_limbo_state(ledger_id)?;
                Ok(Some(new_md))
            }
            Err(RecoverError::LedgerDeleted) => Ok(None),
            Err(RecoverError::Other(msg)) => Err(DataIntegError::Recovery(msg)),
        }
    }

    async fn check_and_recover_entries(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
    ) -> Result<(), DataIntegError> {
        let last_known = last_known_entry(metadata);
        if last_known < 0 {
            return Ok(());
        }
        let batch = Arc::new(self.entry_copier.new_batch(ledger_id, metadata)?);
        let bookie_id = self.bookie_id.clone();
        let storage = Arc::clone(&self.storage);
        let md = metadata.clone();

        // Fan-out per-entry checks with bounded concurrency. We need owned
        // handles in each future so they outlive the iterator.
        stream::iter(0..=last_known)
            .map(|entry_id| {
                let batch = Arc::clone(&batch);
                let storage = Arc::clone(&storage);
                let bookie_id = bookie_id.clone();
                let md = md.clone();
                async move {
                    maybe_copy_one(&bookie_id, &storage, &md, ledger_id, entry_id, &batch).await
                }
            })
            .buffer_unordered(self.config.max_entries_inflight)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }
}

async fn maybe_copy_one(
    self_bookie: &str,
    storage: &Arc<dyn LedgerStorage>,
    metadata: &LedgerMetadata,
    ledger_id: i64,
    entry_id: i64,
    batch: &Batch,
) -> Result<(), DataIntegError> {
    if !is_entry_owed(self_bookie, metadata, entry_id) {
        return Ok(());
    }
    if storage.entry_exists(ledger_id, entry_id)? {
        return Ok(());
    }
    batch.copy_from_available(entry_id).await?;
    Ok(())
}

fn is_entry_owed(self_bookie: &str, metadata: &LedgerMetadata, entry_id: i64) -> bool {
    let ensemble = match metadata.ensemble_at(entry_id) {
        Some(e) => e,
        None => return false,
    };
    let my_idx = match ensemble.iter().position(|b| b == self_bookie) {
        Some(i) => i,
        None => return false,
    };
    let write_set = write_set_for_entry(
        entry_id,
        ensemble.len() as i32,
        metadata.write_quorum_size(),
    );
    write_set.contains(&my_idx)
}

/// Last entry id the bookie can *expect* to have locally. For closed
/// ledgers that's `last_entry_id`; for open ledgers it's the last entry
/// of the *penultimate* ensemble (since the last ensemble hasn't been
/// fully written yet). Mirrors Java's `DataIntegrityCheckImpl`.
fn last_known_entry(metadata: &LedgerMetadata) -> i64 {
    if metadata.state() == LedgerState::Closed {
        return metadata.last_entry_id().unwrap_or(-1);
    }
    // The first entry id of the last ensemble is a key; the entry before
    // that is the last entry of the previous segment (-1 when there's only
    // one ensemble).
    metadata
        .all_ensembles()
        .keys()
        .next_back()
        .copied()
        .map(|k| k - 1)
        .unwrap_or(-1)
}

fn ensemble_contains_bookie(md: &LedgerMetadata, bookie_id: &str) -> bool {
    md.all_ensembles()
        .values()
        .any(|e| e.iter().any(|b| b == bookie_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bookkeeper_metadata::ledger_metadata::LedgerMetadataBuilder;

    fn md_one_ensemble(bookies: Vec<&str>, closed: bool, last_entry: i64) -> LedgerMetadata {
        let mut b = LedgerMetadataBuilder::new()
            .ledger_id(1)
            .ensemble_size(bookies.len() as i32)
            .write_quorum_size(2)
            .ack_quorum_size(2)
            .new_ensemble_entry(0, bookies.iter().map(|s| s.to_string()).collect());
        if closed {
            b = b.with_closed_state(last_entry, 0);
        }
        b.build().unwrap()
    }

    #[test]
    fn last_known_entry_closed_uses_last_entry_id() {
        let md = md_one_ensemble(vec!["b1:1", "b2:1", "b3:1"], true, 42);
        assert_eq!(last_known_entry(&md), 42);
    }

    #[test]
    fn last_known_entry_open_single_ensemble_is_negative() {
        let md = md_one_ensemble(vec!["b1:1", "b2:1", "b3:1"], false, 0);
        // one ensemble at key 0 → penultimate ensemble end is -1
        assert_eq!(last_known_entry(&md), -1);
    }

    #[test]
    fn last_known_entry_open_multi_ensemble_is_penultimate_last() {
        let md = LedgerMetadataBuilder::new()
            .ledger_id(1)
            .ensemble_size(3)
            .write_quorum_size(2)
            .ack_quorum_size(2)
            .new_ensemble_entry(0, vec!["b1:1".into(), "b2:1".into(), "b3:1".into()])
            .new_ensemble_entry(
                10,
                vec!["b1:1".into(), "b2:1".into(), "b4:1".into()],
            )
            .build()
            .unwrap();
        // Last segment starts at 10, so penultimate ends at entry 9.
        assert_eq!(last_known_entry(&md), 9);
    }

    #[test]
    fn is_entry_owed_matches_writeset_membership() {
        // ensemble=3, wq=2 → entry 0 goes to indices [0, 1]
        let md = md_one_ensemble(vec!["b1:1", "b2:1", "b3:1"], true, 0);
        assert!(is_entry_owed("b1:1", &md, 0));
        assert!(is_entry_owed("b2:1", &md, 0));
        assert!(!is_entry_owed("b3:1", &md, 0));
    }

    #[test]
    fn is_entry_owed_false_for_non_member() {
        let md = md_one_ensemble(vec!["b1:1", "b2:1", "b3:1"], true, 0);
        assert!(!is_entry_owed("someone-else:1", &md, 0));
    }

    #[test]
    fn ensemble_contains_bookie_across_segments() {
        let md = LedgerMetadataBuilder::new()
            .ledger_id(1)
            .ensemble_size(3)
            .write_quorum_size(2)
            .ack_quorum_size(2)
            .new_ensemble_entry(0, vec!["b1:1".into(), "b2:1".into(), "b3:1".into()])
            .new_ensemble_entry(5, vec!["b1:1".into(), "b2:1".into(), "b4:1".into()])
            .build()
            .unwrap();
        assert!(ensemble_contains_bookie(&md, "b4:1"));
        assert!(!ensemble_contains_bookie(&md, "b9:1"));
    }
}
