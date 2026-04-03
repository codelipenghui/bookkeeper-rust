use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;

use bookkeeper_common::error::BKError;
use bookkeeper_common::types::LAST_ADD_CONFIRMED;
use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::DigestManager;
use bookkeeper_protocol::entry::{EntryBuilder, parse_entry};
use bookkeeper_protocol::v2::response::V2Response;
use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;

/// Handles ledger recovery after a writer crash.
///
/// Recovery protocol:
/// 1. Fence: send FLAG_DO_FENCING reads to all bookies, wait for write quorum, determine LAC
/// 2. Forward read: read entries from LAC+1 until ENOENTRY to find true last entry
/// 3. Re-replicate: write missing entries with FLAG_RECOVERY_ADD
/// 4. Close: mark ledger as CLOSED with correct lastEntryId and length
pub struct RecoveryHandle {
    ledger_id: i64,
    ensemble: Vec<String>,
    schedule: RoundRobinDistributionSchedule,
    digest_manager: DigestManager,
    master_key: [u8; MASTER_KEY_LENGTH],
    recovery_lac: i64,
    last_entry_found: i64,
    total_length: i64,
}

impl RecoveryHandle {
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
            recovery_lac: -1,
            last_entry_found: -1,
            total_length: 0,
        }
    }

    pub fn recovery_lac(&self) -> i64 {
        self.recovery_lac
    }

    pub fn last_entry_found(&self) -> i64 {
        self.last_entry_found
    }

    /// Phase 1: Fence the ledger on all bookies.
    /// Sends FLAG_DO_FENCING read to each bookie, waits for write quorum responses.
    /// Returns the recovery LAC (max LAC across all fencing responses).
    pub async fn fence(
        &mut self,
        conns: &[Arc<Mutex<BookieConnection>>],
    ) -> Result<i64, ClientError> {
        let write_quorum = self.schedule.write_quorum_size();
        let mut max_lac: i64 = -1;
        let mut success_count = 0;

        // Send fencing read to ALL bookies in ensemble (not just write set)
        let mut futures = Vec::new();
        for (idx, _bookie) in self.ensemble.iter().enumerate() {
            if idx >= conns.len() {
                continue;
            }
            let conn = conns[idx].clone();
            let ledger_id = self.ledger_id;
            let master_key = self.master_key;
            futures.push(tokio::spawn(async move {
                let mut conn = conn.lock().await;
                conn.send_read_entry(
                    FLAG_DO_FENCING,
                    ledger_id,
                    LAST_ADD_CONFIRMED,
                    Some(master_key),
                ).await
            }));
        }

        let no_entry_code = BKError::NoEntry.to_v2_code();

        // Collect responses
        for future in futures {
            match future.await {
                Ok(Ok(V2Response::Read { error_code, entry_id, .. })) => {
                    if error_code == V2_EOK || error_code == no_entry_code {
                        success_count += 1;
                        // entry_id contains the LAC for LAST_ADD_CONFIRMED reads
                        if entry_id > max_lac {
                            max_lac = entry_id;
                        }
                    }
                }
                _ => {
                    // Fencing failed on this bookie — continue, need write quorum
                }
            }
        }

        if success_count < write_quorum {
            return Err(ClientError::Protocol(format!(
                "fencing quorum not reached: got {success_count}, need {write_quorum}"
            )));
        }

        self.recovery_lac = max_lac;
        Ok(max_lac)
    }

    /// Phase 2: Forward read from recovery_lac+1 to find the true last entry.
    /// Reads entries sequentially until ENOENTRY. Returns the last entry ID found.
    pub async fn forward_read(
        &mut self,
        conns: &[Arc<Mutex<BookieConnection>>],
        recovery_lac: i64,
    ) -> Result<i64, ClientError> {
        let mut last_entry = recovery_lac;
        let mut entry_id = recovery_lac + 1;
        let mut total_length: i64 = 0;

        // First, compute length of entries up to recovery_lac
        for eid in 0..=recovery_lac {
            let write_set = self.schedule.write_set(eid);
            for &bookie_idx in &write_set {
                if bookie_idx >= conns.len() {
                    continue;
                }
                let mut conn = conns[bookie_idx].lock().await;
                match conn.send_read_entry(0, self.ledger_id, eid, None).await {
                    Ok(V2Response::Read { error_code, data, .. }) if error_code == V2_EOK => {
                        if let Some(entry_data) = data {
                            if let Ok(entry) = parse_entry(&self.digest_manager, entry_data) {
                                total_length = entry.length;
                            }
                        }
                        break;
                    }
                    _ => continue,
                }
            }
        }

        // Now forward read from recovery_lac+1
        loop {
            let write_set = self.schedule.write_set(entry_id);
            let mut found = false;

            for &bookie_idx in &write_set {
                if bookie_idx >= conns.len() {
                    continue;
                }
                let mut conn = conns[bookie_idx].lock().await;
                match conn.send_read_entry(0, self.ledger_id, entry_id, None).await {
                    Ok(V2Response::Read { error_code, data, .. }) => {
                        if error_code == V2_EOK {
                            if let Some(entry_data) = data {
                                if let Ok(entry) = parse_entry(&self.digest_manager, entry_data) {
                                    total_length = entry.length;
                                    last_entry = entry_id;
                                    found = true;
                                }
                            }
                            break;
                        }
                    }
                    _ => continue,
                }
            }

            if !found {
                break; // No more entries
            }
            entry_id += 1;
        }

        self.last_entry_found = last_entry;
        self.total_length = total_length;
        Ok(last_entry)
    }

    /// Phase 3: Write a recovery entry with FLAG_RECOVERY_ADD.
    /// Used to re-replicate entries that don't meet write quorum.
    pub async fn recovery_add_entry(
        &self,
        conns: &[Arc<Mutex<BookieConnection>>],
        entry_id: i64,
        payload: &[u8],
    ) -> Result<i64, ClientError> {
        let entry_bytes = EntryBuilder::new(&self.digest_manager)
            .ledger_id(self.ledger_id)
            .entry_id(entry_id)
            .lac(self.recovery_lac)
            .length(payload.len() as i64)
            .payload(Bytes::copy_from_slice(payload))
            .build()
            .freeze();

        let write_set = self.schedule.write_set(entry_id);
        let mut ack_set = self.schedule.new_ack_set();

        let mut futures = Vec::new();
        for &bookie_idx in &write_set {
            if bookie_idx >= conns.len() {
                continue;
            }
            let conn = conns[bookie_idx].clone();
            let entry = entry_bytes.clone();
            let mk = self.master_key;
            futures.push((bookie_idx, tokio::spawn(async move {
                let mut conn = conn.lock().await;
                conn.send_add_entry(FLAG_RECOVERY_ADD, mk, entry).await
            })));
        }

        for (bookie_idx, future) in futures {
            match future.await {
                Ok(Ok(V2Response::Add { error_code, .. })) => {
                    if error_code == V2_EOK {
                        if ack_set.complete_bookie(bookie_idx) {
                            return Ok(entry_id);
                        }
                    } else {
                        ack_set.fail_bookie(bookie_idx);
                    }
                }
                _ => {
                    ack_set.fail_bookie(bookie_idx);
                }
            }
        }

        if ack_set.is_quorum_reached() {
            Ok(entry_id)
        } else {
            Err(ClientError::Protocol("recovery add quorum not reached".into()))
        }
    }

    /// Phase 4: Close the ledger after recovery.
    /// Returns (lastEntryId, totalLength) for metadata update.
    pub fn close(&self, last_entry_id: i64) -> Result<(i64, i64), ClientError> {
        Ok((last_entry_id, self.total_length))
    }
}
