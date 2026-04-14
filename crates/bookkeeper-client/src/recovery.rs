use std::sync::Arc;

use bytes::Bytes;
use futures::future::join_all;

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
    /// Also extracts total_length from the fencing response data when present,
    /// eliminating the need for an O(N) pre-loop in forward_read().
    pub async fn fence(
        &mut self,
        conns: &[Arc<BookieConnection>],
        last_ensemble_entry_id: i64,
    ) -> Result<i64, ClientError> {
        let write_quorum = self.schedule.write_quorum_size();

        let ledger_id = self.ledger_id;
        let master_key = self.master_key;

        // Send fencing read to ALL bookies in ensemble (not just write set) in parallel
        let indices: Vec<usize> = (0..self.ensemble.len())
            .filter(|&idx| idx < conns.len())
            .collect();

        let futures: Vec<_> = indices.iter().map(|&idx| {
            let conn = &*conns[idx];
            async move {
                conn.send_read_entry(
                    FLAG_DO_FENCING,
                    ledger_id,
                    LAST_ADD_CONFIRMED,
                    Some(master_key),
                ).await
            }
        }).collect();

        let responses = join_all(futures).await;

        let mut max_lac: i64 = -1;
        let mut success_count = 0;

        for result in responses {
            match result {
                Ok(V2Response::Read { error_code, entry_id, data, .. }) => {
                    let bk_err = BKError::from_v2_code(error_code);
                    match bk_err {
                        BKError::Ok | BKError::NoEntry => {
                            success_count += 1;
                            if entry_id > max_lac {
                                max_lac = entry_id;
                            }
                            if bk_err == BKError::Ok {
                                if let Some(entry_data) = data {
                                    if let Ok(entry) = parse_entry(&self.digest_manager, entry_data) {
                                        if entry.length > self.total_length {
                                            self.total_length = entry.length;
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            // Fencing failed on this bookie — continue, need write quorum
                        }
                    }
                }
                _ => {
                    // Connection/transport error — continue, need write quorum
                }
            }
        }

        if success_count < write_quorum {
            return Err(ClientError::Protocol(format!(
                "fencing quorum not reached: got {success_count}, need {write_quorum}"
            )));
        }

        self.recovery_lac = std::cmp::max(max_lac, last_ensemble_entry_id - 1);
        Ok(self.recovery_lac)
    }

    /// Phase 2: Forward read from recovery_lac+1 to find the true last entry.
    /// Reads entries sequentially until ENOENTRY. Returns the last entry ID found.
    /// Note: total_length for entries up to recovery_lac is obtained from fence().
    /// This method only scans forward from recovery_lac+1.
    pub async fn forward_read(
        &mut self,
        conns: &[Arc<BookieConnection>],
        recovery_lac: i64,
    ) -> Result<i64, ClientError> {
        let mut last_entry = recovery_lac;
        let mut entry_id = recovery_lac + 1;

        loop {
            let write_set = self.schedule.write_set(entry_id);
            let mut found = false;

            for &bookie_idx in &write_set {
                if bookie_idx >= conns.len() {
                    continue;
                }
                match conns[bookie_idx].send_read_entry(0, self.ledger_id, entry_id, None).await {
                    Ok(V2Response::Read { error_code, data, .. }) => {
                        if error_code == V2_EOK {
                            if let Some(entry_data) = data {
                                if let Ok(entry) = parse_entry(&self.digest_manager, entry_data) {
                                    self.total_length = entry.length;
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
                break;
            }
            entry_id += 1;
        }

        // If no entries were found beyond recovery_lac (or total_length was not set
        // by fence() because the bookie returns data:None for LAST_ADD_CONFIRMED reads),
        // read the recovery_lac entry to obtain the cumulative length.
        if self.total_length == 0 && recovery_lac >= 0 {
            let write_set = self.schedule.write_set(recovery_lac);
            for &bookie_idx in &write_set {
                if bookie_idx >= conns.len() {
                    continue;
                }
                match conns[bookie_idx].send_read_entry(0, self.ledger_id, recovery_lac, None).await {
                    Ok(V2Response::Read { error_code, data, .. }) if error_code == V2_EOK => {
                        if let Some(entry_data) = data {
                            if let Ok(entry) = parse_entry(&self.digest_manager, entry_data) {
                                self.total_length = entry.length;
                            }
                        }
                        break;
                    }
                    _ => continue,
                }
            }
        }

        self.last_entry_found = last_entry;
        Ok(last_entry)
    }

    /// Phase 3: Write a recovery entry with FLAG_RECOVERY_ADD.
    /// Builds a NEW entry from payload bytes (not re-sending raw bytes).
    /// Note: `length` is set to `payload.len()` — use `recovery_add_raw` when
    /// re-replicating existing entries where the cumulative length must be preserved.
    pub async fn recovery_add_entry(
        &self,
        conns: &[Arc<BookieConnection>],
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

        self.scatter_recovery_add(conns, entry_id, entry_bytes).await?;
        Ok(entry_id)
    }

    /// Re-replicate an existing entry (raw bytes) with FLAG_RECOVERY_ADD | FLAG_HIGH_PRIORITY.
    /// Unlike `recovery_add_entry`, this sends the original entry bytes verbatim,
    /// preserving the cumulative `length` field already embedded in the entry.
    async fn recovery_add_raw(
        &self,
        conns: &[Arc<BookieConnection>],
        entry_id: i64,
        entry_data: Bytes,
    ) -> Result<(), ClientError> {
        self.scatter_recovery_add(conns, entry_id, entry_data).await
    }

    /// Send a recovery add to the write set and wait for ack quorum.
    async fn scatter_recovery_add(
        &self,
        conns: &[Arc<BookieConnection>],
        entry_id: i64,
        entry_data: Bytes,
    ) -> Result<(), ClientError> {
        let write_set = self.schedule.write_set(entry_id);
        let mut ack_set = self.schedule.new_ack_set();

        let mk = self.master_key;
        let valid_indices: Vec<usize> = write_set.iter()
            .filter(|&&idx| idx < conns.len())
            .copied()
            .collect();

        let futures: Vec<_> = valid_indices.iter().map(|&bookie_idx| {
            let conn = &*conns[bookie_idx];
            let entry = entry_data.clone();
            async move { conn.send_add_entry(FLAG_RECOVERY_ADD | FLAG_HIGH_PRIORITY, mk, entry).await }
        }).collect();

        let results = join_all(futures).await;

        for (i, result) in results.into_iter().enumerate() {
            let bookie_idx = valid_indices[i];
            match result {
                Ok(V2Response::Add { error_code, .. }) => {
                    let bk_err = BKError::from_v2_code(error_code);
                    match bk_err {
                        BKError::Ok => {
                            if ack_set.complete_bookie(bookie_idx) {
                                return Ok(());
                            }
                        }
                        BKError::Fenced | BKError::Unauthorized => {
                            return Err(ClientError::BookieError { error: bk_err });
                        }
                        _ => {
                            ack_set.fail_bookie(bookie_idx);
                        }
                    }
                }
                _ => {
                    ack_set.fail_bookie(bookie_idx);
                }
            }
        }

        if ack_set.is_quorum_reached() {
            Ok(())
        } else {
            Err(ClientError::Protocol(format!(
                "recovery add quorum not reached for entry {entry_id}"
            )))
        }
    }

    /// Phase 4: Close the ledger after recovery.
    /// Returns (lastEntryId, totalLength) for metadata update.
    pub fn close(&self, last_entry_id: i64) -> Result<(i64, i64), ClientError> {
        Ok((last_entry_id, self.total_length))
    }

    /// Run the complete recovery protocol: fence → forward read → re-replicate → close.
    /// Returns (lastEntryId, totalLength) for metadata update.
    pub async fn recover(
        &mut self,
        conns: &[Arc<BookieConnection>],
        last_ensemble_entry_id: i64,
    ) -> Result<(i64, i64), ClientError> {
        // Phase 1: Fence (also extracts total_length from fencing response data)
        let recovery_lac = self.fence(conns, last_ensemble_entry_id).await?;

        // Phase 2: Forward read to find true last entry
        let last_entry = self.forward_read(conns, recovery_lac).await?;

        // Phase 3: Re-replicate entries from recovery_lac+1 to last_entry.
        // forward_read already verified these entries exist; now re-read and re-replicate
        // to ensure write quorum on all bookies.
        for eid in (recovery_lac + 1)..=last_entry {
            // Read the entry from any bookie in the write set
            let write_set = self.schedule.write_set(eid);
            let mut entry_data: Option<Bytes> = None;
            for &bookie_idx in &write_set {
                if bookie_idx >= conns.len() {
                    continue;
                }
                match conns[bookie_idx].send_read_entry(0, self.ledger_id, eid, None).await {
                    Ok(V2Response::Read { error_code, data, .. }) if error_code == V2_EOK => {
                        if let Some(d) = data {
                            entry_data = Some(d);
                            break;
                        }
                    }
                    _ => continue,
                }
            }

            if let Some(data) = entry_data {
                // Re-replicate with FLAG_RECOVERY_ADD | FLAG_HIGH_PRIORITY
                self.recovery_add_raw(conns, eid, data).await?;
            }
        }

        // Phase 4: Close
        Ok((last_entry, self.total_length))
    }
}
