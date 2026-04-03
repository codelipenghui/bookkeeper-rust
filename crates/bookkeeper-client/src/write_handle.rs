use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};

use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::DigestManager;
use bookkeeper_protocol::v2::response::V2Response;
use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;

pub struct WriteHandle {
    ledger_id: i64,
    ensemble: Vec<String>,
    schedule: RoundRobinDistributionSchedule,
    digest_manager: DigestManager,
    master_key: [u8; MASTER_KEY_LENGTH],
    last_add_pushed: i64,
    last_add_confirmed: i64,
    current_length: i64,
}

impl WriteHandle {
    pub fn new(
        ledger_id: i64,
        ensemble: Vec<String>,
        schedule: RoundRobinDistributionSchedule,
        digest_manager: DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Self {
        Self {
            ledger_id,
            ensemble,
            schedule,
            digest_manager,
            master_key,
            last_add_pushed: -1,
            last_add_confirmed: -1,
            current_length: 0,
        }
    }

    pub fn last_add_confirmed(&self) -> i64 {
        self.last_add_confirmed
    }

    pub fn ledger_id(&self) -> i64 {
        self.ledger_id
    }

    pub fn ensemble(&self) -> &[String] {
        &self.ensemble
    }

    /// Append an entry. Builds the entry with digest, sends to write quorum
    /// bookies in parallel, waits for ack quorum, advances LAC. Returns the entry ID.
    pub async fn append(
        &mut self,
        conns: &[Arc<tokio::sync::Mutex<BookieConnection>>],
        payload: &[u8],
    ) -> Result<i64, ClientError> {
        let entry_id = self.last_add_pushed + 1;
        self.last_add_pushed = entry_id;
        self.current_length += payload.len() as i64;

        // Build entry: [ledgerId | entryId | lac | length | digest | payload]
        let entry_bytes = self.build_entry(entry_id, payload);

        // Determine write set
        let write_set = self.schedule.write_set(entry_id);

        // Send to all bookies in parallel
        let mut futures = Vec::new();
        for &bookie_idx in &write_set {
            if bookie_idx >= conns.len() {
                return Err(ClientError::Protocol(format!(
                    "bookie index {bookie_idx} out of range (have {} connections)",
                    conns.len()
                )));
            }
            let conn = conns[bookie_idx].clone();
            let entry = entry_bytes.clone();
            let mk = self.master_key;
            futures.push(tokio::spawn(async move {
                let mut conn = conn.lock().await;
                conn.send_add_entry(0, mk, entry).await
            }));
        }

        // Collect results
        let mut ack_set = self.schedule.new_ack_set();
        for (i, future) in futures.into_iter().enumerate() {
            let bookie_idx = write_set[i];
            match future.await {
                Ok(Ok(V2Response::Add { error_code, .. })) => {
                    if error_code == V2_EOK {
                        if ack_set.complete_bookie(bookie_idx) {
                            self.last_add_confirmed = entry_id;
                            return Ok(entry_id);
                        }
                    } else {
                        if ack_set.fail_bookie(bookie_idx) {
                            return Err(ClientError::Protocol(format!(
                                "write quorum impossible: error_code={error_code}"
                            )));
                        }
                    }
                }
                Ok(Ok(_)) => {
                    if ack_set.fail_bookie(bookie_idx) {
                        return Err(ClientError::Protocol("unexpected response type".into()));
                    }
                }
                Ok(Err(e)) => {
                    if ack_set.fail_bookie(bookie_idx) {
                        return Err(e);
                    }
                }
                Err(e) => {
                    if ack_set.fail_bookie(bookie_idx) {
                        return Err(ClientError::Protocol(format!("task panic: {e}")));
                    }
                }
            }
        }

        if ack_set.is_quorum_reached() {
            self.last_add_confirmed = entry_id;
            Ok(entry_id)
        } else {
            Err(ClientError::Protocol("failed to reach ack quorum".into()))
        }
    }

    fn build_entry(&self, entry_id: i64, payload: &[u8]) -> Bytes {
        let digest_len = self.digest_manager.digest_type().digest_length();
        let total = 32 + digest_len + payload.len();
        let mut buf = BytesMut::with_capacity(total);

        // Metadata (32 bytes)
        buf.put_i64(self.ledger_id);
        buf.put_i64(entry_id);
        buf.put_i64(self.last_add_confirmed);
        buf.put_i64(self.current_length);

        // Digest
        let metadata: [u8; 32] = buf[..32].try_into().unwrap();
        let digest = self.digest_manager.compute(&metadata, payload);
        buf.put_slice(digest.as_slice());

        // Payload
        buf.put_slice(payload);
        buf.freeze()
    }
}
