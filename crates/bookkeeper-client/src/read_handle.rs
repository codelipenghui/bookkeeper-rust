use std::sync::Arc;

use futures::stream::{self, StreamExt};

use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::DigestManager;
use bookkeeper_protocol::entry::{parse_entry, EntryError};
use bookkeeper_protocol::v2::response::V2Response;
use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;

pub struct ReadHandle {
    ledger_id: i64,
    #[allow(dead_code)]
    ensemble: Vec<String>,
    schedule: RoundRobinDistributionSchedule,
    digest_manager: DigestManager,
    master_key: [u8; MASTER_KEY_LENGTH],
}

impl ReadHandle {
    pub fn new(
        ledger_id: i64,
        ensemble: Vec<String>,
        schedule: RoundRobinDistributionSchedule,
        digest_manager: DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Self {
        Self { ledger_id, ensemble, schedule, digest_manager, master_key }
    }

    pub fn ledger_id(&self) -> i64 {
        self.ledger_id
    }

    /// Read a single entry. Tries bookies in write set order, returns first success.
    pub async fn read_entry(
        &self,
        conns: &[Arc<BookieConnection>],
        entry_id: i64,
    ) -> Result<Vec<u8>, ClientError> {
        let write_set = self.schedule.write_set(entry_id);
        let mut last_error: Option<ClientError> = None;

        for &bookie_idx in &write_set {
            if bookie_idx >= conns.len() {
                continue;
            }
            match conns[bookie_idx].send_read_entry(0, self.ledger_id, entry_id, Some(self.master_key)).await {
                Ok(V2Response::Read { error_code, data, .. }) => {
                    if error_code == V2_EOK {
                        if let Some(entry_data) = data {
                            match parse_entry(&self.digest_manager, entry_data) {
                                Ok(entry) => {
                                    if entry.ledger_id != self.ledger_id || entry.entry_id != entry_id {
                                        last_error = Some(ClientError::Protocol("wrong entry".into()));
                                        continue;
                                    }
                                    return Ok(entry.payload.to_vec());
                                }
                                Err(EntryError::DigestMismatch) => {
                                    last_error = Some(ClientError::Protocol("digest mismatch".into()));
                                    continue;
                                }
                                Err(e) => {
                                    return Err(ClientError::Protocol(format!("entry parse: {e}")));
                                }
                            }
                        }
                    }
                    last_error = Some(ClientError::Protocol(format!("bookie error: {error_code}")));
                }
                Ok(_) => {
                    last_error = Some(ClientError::Protocol("unexpected response".into()));
                }
                Err(e) => {
                    last_error = Some(e);
                    continue;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| ClientError::Protocol(format!(
            "entry not found: ledger={}, entry={}", self.ledger_id, entry_id
        ))))
    }

    /// Read a range of entries [first_entry..=last_entry] sequentially.
    pub async fn read_entries(
        &self,
        conns: &[Arc<BookieConnection>],
        first_entry: i64,
        last_entry: i64,
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        self.read_entries_parallel(conns, first_entry, last_entry, 1).await
    }

    /// Read a range of entries with bounded parallelism.
    /// `concurrency` controls how many reads are in-flight simultaneously.
    /// Entries destined for different bookies (round-robin striping) naturally
    /// parallelize across pipelined connections.
    pub async fn read_entries_parallel(
        &self,
        conns: &[Arc<BookieConnection>],
        first_entry: i64,
        last_entry: i64,
        concurrency: usize,
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        let concurrency = concurrency.max(1);
        let results: Vec<Result<Vec<u8>, ClientError>> = stream::iter(first_entry..=last_entry)
            .map(|eid| self.read_entry(conns, eid))
            .buffered(concurrency)
            .collect()
            .await;

        results.into_iter().collect()
    }
}
