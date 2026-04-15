//! Bounded-concurrency streaming iteration over all ledger metadata.
//!
//! Port of Java's `MetadataAsyncIterator`. The Java version uses RxJava
//! `Flowable` with per-range `processLedgers` callbacks; Rust returns a
//! `Stream` so callers compose with `for_each_concurrent` / `collect` /
//! `StreamExt::try_for_each`.
//!
//! Semantics: first list all ledger IDs, then fetch per-ledger metadata
//! with `buffer_unordered(max_inflight)`. Errors on individual ledger
//! reads are surfaced as `Err` items in the stream — callers decide
//! whether to abort or skip.

use std::sync::Arc;

use futures::stream::{self, Stream, StreamExt};

use crate::ledger_metadata::{LedgerMetadata, MetadataError};
use crate::traits::LedgerManager;
use crate::versioned::Versioned;

/// Default concurrency limit, matches Java's `DataIntegrityCheckImpl.MAX_INFLIGHT`.
pub const DEFAULT_MAX_INFLIGHT: usize = 300;

/// Stream every `(ledger_id, Versioned<LedgerMetadata>)` pair in the
/// metadata store, reading up to `max_inflight` entries concurrently.
///
/// Ledgers deleted between the ID listing and the metadata read surface
/// as `MetadataError::NotFound(id)` items in the stream — the caller can
/// filter them out with e.g. `stream.filter_map`.
pub fn stream_all_ledgers(
    lm: Arc<dyn LedgerManager>,
    max_inflight: usize,
) -> impl Stream<Item = Result<(i64, Versioned<LedgerMetadata>), MetadataError>> + Send {
    // Wrap the id-listing future in a small stream that yields one list,
    // then flat-maps into the per-ledger metadata reads so everything can
    // be driven from a single stream handle.
    let max_inflight = max_inflight.max(1);
    let list_lm = Arc::clone(&lm);
    stream::once(async move {
        match list_lm.get_ledger_ranges().await {
            Ok(ids) => stream::iter(ids.into_iter().map(Ok)).left_stream(),
            Err(e) => stream::once(async move { Err(e) }).right_stream(),
        }
    })
    .flatten()
    .map(move |id_result| {
        let lm = Arc::clone(&lm);
        async move {
            let id = id_result?;
            let md = lm.read_ledger_metadata(id).await?;
            Ok::<_, MetadataError>((id, md))
        }
    })
    .buffer_unordered(max_inflight)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::StreamExt;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use crate::ledger_metadata::{LedgerMetadata, LedgerMetadataBuilder, MetadataError};
    use crate::versioned::Versioned;

    struct InMemoryLedgerManager {
        ledgers: Mutex<HashMap<i64, Versioned<LedgerMetadata>>>,
    }

    impl InMemoryLedgerManager {
        fn new(ids: &[i64]) -> Arc<Self> {
            let mut map = HashMap::new();
            for &id in ids {
                let md = LedgerMetadataBuilder::new()
                    .ledger_id(id)
                    .ensemble_size(1)
                    .write_quorum_size(1)
                    .ack_quorum_size(1)
                    .new_ensemble_entry(0, vec!["bk:3181".to_string()])
                    .build()
                    .unwrap();
                map.insert(id, Versioned::new(md, 0));
            }
            Arc::new(Self { ledgers: Mutex::new(map) })
        }
    }

    #[async_trait]
    impl LedgerManager for InMemoryLedgerManager {
        async fn create_ledger_metadata(
            &self,
            _ledger_id: i64,
            _metadata: &LedgerMetadata,
        ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
            unimplemented!()
        }
        async fn read_ledger_metadata(
            &self,
            ledger_id: i64,
        ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
            self.ledgers
                .lock()
                .unwrap()
                .get(&ledger_id)
                .cloned()
                .ok_or(MetadataError::NotFound(ledger_id))
        }
        async fn write_ledger_metadata(
            &self,
            _ledger_id: i64,
            _metadata: &LedgerMetadata,
            _current_version: i32,
        ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
            unimplemented!()
        }
        async fn remove_ledger_metadata(
            &self,
            _ledger_id: i64,
            _version: i32,
        ) -> Result<(), MetadataError> {
            unimplemented!()
        }
        async fn get_ledger_ranges(&self) -> Result<Vec<i64>, MetadataError> {
            let mut ids: Vec<i64> = self.ledgers.lock().unwrap().keys().copied().collect();
            ids.sort_unstable();
            Ok(ids)
        }
    }

    #[tokio::test]
    async fn yields_every_ledger() {
        let lm = InMemoryLedgerManager::new(&[1, 2, 3, 4, 5]);
        let stream = stream_all_ledgers(lm, 2);
        let mut ids: Vec<i64> =
            stream.map(|r| r.unwrap().0).collect::<Vec<_>>().await;
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn empty_store_yields_nothing() {
        let lm = InMemoryLedgerManager::new(&[]);
        let count = stream_all_ledgers(lm, 4).count().await;
        assert_eq!(count, 0);
    }

    /// `LedgerManager` wrapper that lists a fixed set of IDs but has only a
    /// subset of them in storage — models the TOCTOU race where a ledger is
    /// deleted between list and read.
    struct GhostLedgerManager {
        listed: Vec<i64>,
        inner: Arc<InMemoryLedgerManager>,
    }

    #[async_trait]
    impl LedgerManager for GhostLedgerManager {
        async fn create_ledger_metadata(
            &self,
            _l: i64,
            _m: &LedgerMetadata,
        ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
            unimplemented!()
        }
        async fn read_ledger_metadata(
            &self,
            ledger_id: i64,
        ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
            self.inner.read_ledger_metadata(ledger_id).await
        }
        async fn write_ledger_metadata(
            &self,
            _l: i64,
            _m: &LedgerMetadata,
            _v: i32,
        ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
            unimplemented!()
        }
        async fn remove_ledger_metadata(&self, _l: i64, _v: i32) -> Result<(), MetadataError> {
            unimplemented!()
        }
        async fn get_ledger_ranges(&self) -> Result<Vec<i64>, MetadataError> {
            Ok(self.listed.clone())
        }
    }

    #[tokio::test]
    async fn individual_errors_surface_without_aborting() {
        // listed contains 2, but storage only has 1 and 3 — read for 2
        // should surface as NotFound(2) without aborting the other reads.
        let inner = InMemoryLedgerManager::new(&[1, 3]);
        let lm = Arc::new(GhostLedgerManager { listed: vec![1, 2, 3], inner });
        let results: Vec<_> = stream_all_ledgers(lm, 8).collect().await;
        let mut oks: Vec<i64> = results
            .iter()
            .filter_map(|r| r.as_ref().ok().map(|(id, _)| *id))
            .collect();
        oks.sort_unstable();
        let errs: Vec<&MetadataError> =
            results.iter().filter_map(|r| r.as_ref().err()).collect();
        assert_eq!(oks, vec![1, 3]);
        assert!(matches!(errs.as_slice(), [MetadataError::NotFound(2)]));
    }

    #[tokio::test]
    async fn max_inflight_of_zero_is_treated_as_one() {
        let lm = InMemoryLedgerManager::new(&[1, 2]);
        let count = stream_all_ledgers(lm, 0).count().await;
        assert_eq!(count, 2);
    }
}
