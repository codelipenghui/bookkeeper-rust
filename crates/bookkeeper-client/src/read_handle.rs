use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::DigestManager;
use bookkeeper_protocol::entry::{parse_entry, EntryError};
use bookkeeper_protocol::v2::response::V2Response;
use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;

pub struct ReadHandle {
    ledger_id: i64,
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
        conns: &mut [BookieConnection],
        entry_id: i64,
    ) -> Result<Vec<u8>, ClientError> {
        let write_set = self.schedule.write_set(entry_id);

        for &bookie_idx in &write_set {
            if bookie_idx >= conns.len() {
                continue;
            }
            match conns[bookie_idx].send_read_entry(0, self.ledger_id, entry_id, None).await {
                Ok(V2Response::Read { error_code, data, .. }) => {
                    if error_code == V2_EOK {
                        if let Some(entry_data) = data {
                            // Verify digest
                            match parse_entry(&self.digest_manager, entry_data) {
                                Ok(entry) => {
                                    if entry.ledger_id != self.ledger_id || entry.entry_id != entry_id {
                                        continue; // wrong entry, try next bookie
                                    }
                                    return Ok(entry.payload.to_vec());
                                }
                                Err(EntryError::DigestMismatch) => {
                                    continue; // corrupted, try next bookie
                                }
                                Err(e) => {
                                    return Err(ClientError::Protocol(format!("entry parse: {e}")));
                                }
                            }
                        }
                    }
                    // Non-EOK or no data — try next bookie
                }
                Ok(_) => continue,
                Err(_) => continue, // connection error, try next bookie
            }
        }

        Err(ClientError::Protocol(format!(
            "entry not found: ledger={}, entry={}", self.ledger_id, entry_id
        )))
    }

    /// Read a range of entries [first_entry..=last_entry].
    pub async fn read_entries(
        &self,
        conns: &mut [BookieConnection],
        first_entry: i64,
        last_entry: i64,
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        let mut results = Vec::new();
        for eid in first_entry..=last_entry {
            results.push(self.read_entry(conns, eid).await?);
        }
        Ok(results)
    }
}
