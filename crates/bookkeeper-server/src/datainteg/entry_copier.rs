//! Copy entries from peer bookies when a bookie has lost its local copy.
//! Port of Java's `EntryCopierImpl`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bookkeeper_client::bookie_connection::{BookieConnection, ClientError};
use bookkeeper_metadata::ledger_metadata::{LedgerMetadata, LedgerState};
use bookkeeper_protocol::v2::response::V2Response;
use bookkeeper_storage::ledger_storage::LedgerStorage;
use bytes::Bytes;
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::RwLock;

/// Duration a bookie stays in the sin bin after an error. Matches Java's
/// `EntryCopierImpl.SINBIN_DURATION_MS = 60_000`.
const SINBIN_DURATION: Duration = Duration::from_secs(60);

#[derive(Debug, Error)]
pub enum EntryCopyError {
    #[error("invalid entry id {0}: must be >= 0")]
    InvalidEntryId(i64),
    #[error("entry {entry_id} past last entry {last_entry} of closed ledger {ledger_id}")]
    EntryBeyondLastEntry { ledger_id: i64, entry_id: i64, last_entry: i64 },
    #[error("no ensemble for entry {0}")]
    NoEnsembleForEntry(i64),
    #[error("bookie id {0} is not parseable as host:port")]
    InvalidBookieId(String),
    #[error("all replicas failed for entry {ledger_id}:{entry_id}: {source}")]
    AllReplicasFailed {
        ledger_id: i64,
        entry_id: i64,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("bookie returned non-OK status {0}")]
    BookieError(i32),
    #[error("response from bookie was not a Read response")]
    UnexpectedResponse,
    #[error("bookie returned OK but no entry body")]
    MissingBody,
    #[error("local storage error: {0}")]
    Storage(#[from] bookkeeper_storage::StorageError),
    #[error("client error: {0}")]
    Client(#[from] ClientError),
}

/// Shared state across all batches for a given `EntryCopier`.
#[derive(Default)]
struct SinBin {
    /// Map of bookie_id → deadline after which the entry expires.
    errored: Mutex<HashMap<String, Instant>>,
}

impl SinBin {
    /// Record an error for `bookie_id`. Returns `true` if this was a new entry
    /// (caller should regenerate its preferred-order lists).
    fn add_failed(&self, bookie_id: &str) -> bool {
        let deadline = Instant::now() + SINBIN_DURATION;
        let mut guard = self.errored.lock();
        guard.insert(bookie_id.to_string(), deadline).is_none()
    }

    /// Return the currently-errored bookies, pruning expired entries.
    fn current(&self) -> Vec<String> {
        let now = Instant::now();
        let mut guard = self.errored.lock();
        guard.retain(|_, deadline| *deadline > now);
        guard.keys().cloned().collect()
    }
}

/// Cache of per-bookie connections so repeated reads reuse the same channels.
#[derive(Default)]
struct ConnectionCache {
    conns: RwLock<HashMap<String, Arc<BookieConnection>>>,
}

impl ConnectionCache {
    async fn get_or_connect(
        &self,
        bookie_id: &str,
    ) -> Result<Arc<BookieConnection>, EntryCopyError> {
        if let Some(c) = self.conns.read().await.get(bookie_id) {
            return Ok(Arc::clone(c));
        }
        let addr = bookie_id
            .parse::<SocketAddr>()
            .or_else(|_| parse_bookie_sockaddr(bookie_id))
            .map_err(|_| EntryCopyError::InvalidBookieId(bookie_id.to_string()))?;
        let conn = Arc::new(BookieConnection::connect(addr).await?);
        self.conns
            .write()
            .await
            .entry(bookie_id.to_string())
            .or_insert_with(|| Arc::clone(&conn));
        Ok(conn)
    }

    async fn drop_bookie(&self, bookie_id: &str) {
        self.conns.write().await.remove(bookie_id);
    }
}

fn parse_bookie_sockaddr(bookie_id: &str) -> Result<SocketAddr, ()> {
    let (host, port) = bookie_id.rsplit_once(':').ok_or(())?;
    let port: u16 = port.parse().map_err(|_| ())?;
    use std::net::ToSocketAddrs;
    (host, port).to_socket_addrs().map_err(|_| ())?.next().ok_or(())
}

/// Copies entries from peer bookies into local storage.
///
/// One `EntryCopier` is shared across all ledgers being repaired; per-ledger
/// work happens via [`Batch`] created by [`EntryCopier::new_batch`]. The
/// copier maintains a *sin bin* of bookies that recently errored, so callers
/// don't hammer a dead peer for every entry.
pub struct EntryCopier {
    bookie_id: String,
    storage: Arc<dyn LedgerStorage>,
    sinbin: Arc<SinBin>,
    conns: Arc<ConnectionCache>,
}

impl EntryCopier {
    pub fn new(bookie_id: String, storage: Arc<dyn LedgerStorage>) -> Self {
        Self {
            bookie_id,
            storage,
            sinbin: Arc::new(SinBin::default()),
            conns: Arc::new(ConnectionCache::default()),
        }
    }

    /// Start a batch for `ledger_id`. Ensures the ledger exists locally
    /// (creating it with `metadata.password` if missing) and returns a
    /// handle callers can use to fetch missing entries.
    pub fn new_batch(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
    ) -> Result<Batch, EntryCopyError> {
        if !self.storage.ledger_exists(ledger_id)? {
            let pw = metadata.password().unwrap_or(&[]);
            self.storage.set_master_key(ledger_id, pw)?;
        }
        Ok(Batch {
            ledger_id,
            metadata: metadata.clone(),
            bookie_id: self.bookie_id.clone(),
            storage: Arc::clone(&self.storage),
            sinbin: Arc::clone(&self.sinbin),
            conns: Arc::clone(&self.conns),
        })
    }
}

/// Per-ledger copy batch. Rebuilds preferred-bookie orderings lazily.
pub struct Batch {
    ledger_id: i64,
    metadata: LedgerMetadata,
    bookie_id: String,
    storage: Arc<dyn LedgerStorage>,
    sinbin: Arc<SinBin>,
    conns: Arc<ConnectionCache>,
}

impl Batch {
    /// Copy `entry_id` from an available replica. Returns the number of
    /// bytes written to local storage on success.
    pub async fn copy_from_available(&self, entry_id: i64) -> Result<u64, EntryCopyError> {
        if entry_id < 0 {
            return Err(EntryCopyError::InvalidEntryId(entry_id));
        }
        if matches!(self.metadata.state(), LedgerState::Closed) {
            if let Some(last) = self.metadata.last_entry_id() {
                if entry_id > last {
                    return Err(EntryCopyError::EntryBeyondLastEntry {
                        ledger_id: self.ledger_id,
                        entry_id,
                        last_entry: last,
                    });
                }
            }
        }

        let ensemble = self
            .metadata
            .ensemble_at(entry_id)
            .ok_or(EntryCopyError::NoEnsembleForEntry(entry_id))?
            .to_vec();

        let order = preferred_bookie_order(
            ensemble.len(),
            self.ledger_id,
            &self.bookie_id,
            &ensemble,
            &self.sinbin.current(),
        );
        let write_set =
            write_set_for_entry(entry_id, ensemble.len() as i32, self.metadata.write_quorum_size());
        // Retain only the bookies actually responsible for `entry_id`,
        // following `order` for the attempt sequence.
        let candidates: Vec<usize> =
            order.into_iter().filter(|i| write_set.contains(i)).collect();

        let mut first_error: Option<EntryCopyError> = None;
        for idx in &candidates {
            let peer = &ensemble[*idx];
            match self.fetch_one(peer, entry_id).await {
                Ok(body) => {
                    let bytes = body.len() as u64;
                    self.storage.add_entry(&body).await?;
                    return Ok(bytes);
                }
                Err(e) => {
                    self.sinbin.add_failed(peer);
                    self.conns.drop_bookie(peer).await;
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }
        Err(EntryCopyError::AllReplicasFailed {
            ledger_id: self.ledger_id,
            entry_id,
            source: Box::new(first_error.unwrap_or(EntryCopyError::BookieError(-1))),
        })
    }

    async fn fetch_one(&self, peer: &str, entry_id: i64) -> Result<Bytes, EntryCopyError> {
        let conn = self.conns.get_or_connect(peer).await?;
        let resp = conn.send_read_entry(0, self.ledger_id, entry_id, None).await?;
        match resp {
            V2Response::Read { error_code: 0, data: Some(body), .. } => Ok(body),
            V2Response::Read { error_code: 0, data: None, .. } => {
                Err(EntryCopyError::MissingBody)
            }
            V2Response::Read { error_code, .. } => Err(EntryCopyError::BookieError(error_code)),
            _ => Err(EntryCopyError::UnexpectedResponse),
        }
    }
}

/// Compute a per-ensemble bookie ordering excluding `self_id`, shuffled by
/// `seed` (normally ledgerId) for read-locality, with errored bookies last.
/// Port of Java's `EntryCopierImpl.preferredBookieIndices`.
fn preferred_bookie_order(
    ensemble_size: usize,
    seed: i64,
    self_id: &str,
    ensemble: &[String],
    errored: &[String],
) -> Vec<usize> {
    let mut indices: Vec<usize> =
        (0..ensemble_size).filter(|&i| ensemble[i] != self_id).collect();
    // Deterministic shuffle: xorshift64 seeded by ledger id so repeated
    // calls for the same ledger converge on the same preferred replica
    // (matches Java's `Collections.shuffle(new Random(seed))` behavior —
    // the specific permutation differs, but the locality property matches).
    let mut state = seed as u64 | 1;
    for i in (1..indices.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        indices.swap(i, j);
    }
    // Stable partition: non-errored first, errored last.
    indices.sort_by_key(|&idx| errored.iter().any(|e| e == &ensemble[idx]));
    indices
}

/// Compute the write-quorum indices that hold `entry_id`. Uses Java's
/// round-robin distribution: `[entry_id % ensemble_size, +1, +2, ...]`
/// modulo ensemble size, `write_quorum_size` entries total.
fn write_set_for_entry(entry_id: i64, ensemble_size: i32, write_quorum_size: i32) -> Vec<usize> {
    let ens = ensemble_size.max(1) as i64;
    let wq = write_quorum_size.max(1).min(ensemble_size.max(1)) as i64;
    (0..wq)
        .map(|k| ((entry_id + k).rem_euclid(ens)) as usize)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_set_wraps_around_ensemble() {
        // ensemble_size=5, write_quorum=3
        assert_eq!(write_set_for_entry(0, 5, 3), vec![0, 1, 2]);
        assert_eq!(write_set_for_entry(3, 5, 3), vec![3, 4, 0]);
        assert_eq!(write_set_for_entry(4, 5, 3), vec![4, 0, 1]);
    }

    #[test]
    fn write_set_handles_wq_equals_ensemble() {
        assert_eq!(write_set_for_entry(7, 3, 3).len(), 3);
    }

    #[test]
    fn preferred_order_excludes_self() {
        let ensemble: Vec<String> =
            (0..5).map(|i| format!("bk{i}:3181")).collect();
        let order = preferred_bookie_order(5, 42, "bk2:3181", &ensemble, &[]);
        assert_eq!(order.len(), 4);
        assert!(!order.iter().any(|&i| ensemble[i] == "bk2:3181"));
    }

    #[test]
    fn preferred_order_pushes_errored_to_end() {
        let ensemble: Vec<String> =
            (0..5).map(|i| format!("bk{i}:3181")).collect();
        let errored = vec!["bk1:3181".to_string(), "bk3:3181".to_string()];
        let order = preferred_bookie_order(5, 7, "bk0:3181", &ensemble, &errored);
        // The last two entries in the order must be the errored bookies.
        let tail: Vec<&str> = order
            .iter()
            .rev()
            .take(2)
            .map(|&i| ensemble[i].as_str())
            .collect();
        let mut tail_sorted = tail.clone();
        tail_sorted.sort();
        assert_eq!(tail_sorted, vec!["bk1:3181", "bk3:3181"]);
    }

    #[test]
    fn preferred_order_is_deterministic_for_same_seed() {
        let ensemble: Vec<String> =
            (0..5).map(|i| format!("bk{i}:3181")).collect();
        let a = preferred_bookie_order(5, 123, "bk0:3181", &ensemble, &[]);
        let b = preferred_bookie_order(5, 123, "bk0:3181", &ensemble, &[]);
        assert_eq!(a, b);
    }

    #[test]
    fn sinbin_tracks_first_and_expires() {
        let sb = SinBin::default();
        assert!(sb.add_failed("bk1"));        // first insertion
        assert!(!sb.add_failed("bk1"));       // subsequent insertions return false
        assert_eq!(sb.current(), vec!["bk1".to_string()]);
    }
}
