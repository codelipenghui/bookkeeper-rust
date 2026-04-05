use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;
use bytes::Bytes;
use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_client::recovery::RecoveryHandle;
use bookkeeper_metadata::traits::LedgerManager;
use bookkeeper_metadata::ledger_metadata::{
    DigestType as MetaDigestType, LedgerMetadata, LedgerMetadataBuilder, LedgerState,
};
use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::{DigestManager, DigestType, generate_master_key};
use bookkeeper_protocol::v2::response::V2Response;
use crate::underreplication::ZkUnderreplicationManager;
use crate::ReplicationError;

/// Background worker that claims underreplicated ledgers and repairs them.
///
/// Matches Java's ReplicationWorker with fragment-level replication:
/// - Closed ledgers: copy entries from good replicas to replacement bookies (no fencing)
/// - Open ledgers with missing bookies in last segment: fence + recovery
/// - Updates ensemble metadata per-fragment with old→new bookie mapping
pub struct ReplicationWorker {
    ur_manager: Arc<ZkUnderreplicationManager>,
    ledger_manager: Arc<dyn LedgerManager>,
    bookie_id: String,
    backoff_ms: u64,
    shutdown_rx: watch::Receiver<bool>,
}

impl ReplicationWorker {
    pub fn new(
        ur_manager: Arc<ZkUnderreplicationManager>,
        ledger_manager: Arc<dyn LedgerManager>,
        bookie_id: String,
        backoff_ms: u64,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            ur_manager,
            ledger_manager,
            bookie_id,
            backoff_ms,
            shutdown_rx,
        }
    }

    /// Run the replication worker main loop.
    pub async fn run(&mut self) -> Result<(), ReplicationError> {
        tracing::info!("ReplicationWorker started (bookie: {})", self.bookie_id);

        loop {
            if *self.shutdown_rx.borrow() {
                tracing::info!("ReplicationWorker shutting down");
                break;
            }

            let result = self.try_rereplicate().await;

            match result {
                Ok(true) => {}
                Ok(false) => {
                    tokio::select! {
                        _ = self.shutdown_rx.changed() => {
                            if *self.shutdown_rx.borrow() {
                                tracing::info!("ReplicationWorker shutting down");
                                break;
                            }
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_millis(self.backoff_ms)) => {}
                    }
                }
                Err(e) => {
                    tracing::warn!("ReplicationWorker error: {e}");
                    tokio::select! {
                        _ = self.shutdown_rx.changed() => {
                            if *self.shutdown_rx.borrow() {
                                tracing::info!("ReplicationWorker shutting down");
                                break;
                            }
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_millis(self.backoff_ms)) => {}
                    }
                }
            }
        }
        Ok(())
    }

    async fn try_rereplicate(&self) -> Result<bool, ReplicationError> {
        if !self.ur_manager.is_replication_enabled().await? {
            return Ok(false);
        }

        let ledger_id = match self.ur_manager.get_ledger_to_rereplicate().await? {
            Some(lid) => lid,
            None => return Ok(false),
        };

        tracing::info!("ReplicationWorker: claimed ledger {ledger_id} for replication");

        match self.rereplicate(ledger_id).await {
            Ok(()) => {
                self.ur_manager.mark_ledger_replicated(ledger_id).await?;
                tracing::info!("ReplicationWorker: ledger {ledger_id} replicated successfully");
                Ok(true)
            }
            Err(e) => {
                tracing::warn!("ReplicationWorker: failed to replicate ledger {ledger_id}: {e}");
                self.ur_manager.release_lock(ledger_id).await?;
                Ok(false)
            }
        }
    }

    /// Replicate a ledger using fragment-level approach matching Java's ReplicationWorker.
    ///
    /// For closed ledgers: detect bad bookies per segment, copy entries to replacements.
    /// For open ledgers: use recovery protocol (fence) if last segment has missing bookies.
    async fn rereplicate(&self, ledger_id: i64) -> Result<(), ReplicationError> {
        let versioned_md = self
            .ledger_manager
            .read_ledger_metadata(ledger_id)
            .await
            .map_err(|e| ReplicationError::Failed(format!("read metadata: {e}")))?;

        let md = versioned_md.value();

        let digest_type = match md.digest_type() {
            Some(MetaDigestType::Crc32) => DigestType::Crc32,
            Some(MetaDigestType::Crc32c) => DigestType::Crc32c,
            Some(MetaDigestType::Hmac) => DigestType::HmacSha1,
            Some(MetaDigestType::Dummy) => DigestType::Dummy,
            None => DigestType::Crc32c,
        };
        let password = md.password().unwrap_or(b"");
        let master_key: [u8; MASTER_KEY_LENGTH] = generate_master_key(password);
        let dm = DigestManager::new(digest_type);

        let ensemble_size = md.ensemble_size() as usize;
        let write_quorum = md.write_quorum_size() as usize;
        let ack_quorum = md.ack_quorum_size() as usize;
        let schedule =
            RoundRobinDistributionSchedule::new(ensemble_size, write_quorum, ack_quorum);

        if md.state() == LedgerState::Closed {
            // Closed ledger: fragment-level replication (no fencing needed)
            self.replicate_closed_ledger(ledger_id, md, &schedule, &dm, master_key)
                .await
        } else {
            // Open ledger: use recovery protocol (fence + forward read)
            self.replicate_open_ledger(ledger_id, md, &schedule, dm, master_key)
                .await
        }
    }

    /// Replicate a closed ledger by copying entries fragment-by-fragment.
    ///
    /// Matches Java's ReplicationWorker flow for closed fragments:
    /// 1. For each ensemble segment, detect bad bookies
    /// 2. Select replacement bookies
    /// 3. Copy entries from good replicas to replacement
    /// 4. Update ensemble metadata
    async fn replicate_closed_ledger(
        &self,
        ledger_id: i64,
        md: &LedgerMetadata,
        schedule: &RoundRobinDistributionSchedule,
        _dm: &DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Result<(), ReplicationError> {
        let ensembles = md.all_ensembles();
        let last_entry_id = md.last_entry_id().unwrap_or(-1);
        if last_entry_id < 0 {
            return Ok(()); // empty ledger
        }

        // Build segment boundaries: [(first_entry, last_entry, ensemble)]
        let segments: Vec<(i64, i64, &Vec<String>)> = {
            let keys: Vec<i64> = ensembles.keys().copied().collect();
            let mut segs = Vec::new();
            for (i, &start) in keys.iter().enumerate() {
                let end = if i + 1 < keys.len() {
                    keys[i + 1] - 1
                } else {
                    last_entry_id
                };
                let ens = &ensembles[&start];
                segs.push((start, end, ens));
            }
            segs
        };

        // Get available bookies for replacement selection
        let available = self.get_available_bookies().await?;

        for (seg_start, seg_end, ensemble) in &segments {
            // Detect bad bookies in this segment by probing sample entries
            let bad_indices = self
                .detect_bad_bookies(
                    ledger_id, *seg_start, *seg_end, ensemble, schedule, &master_key,
                )
                .await?;

            if bad_indices.is_empty() {
                continue; // this segment is fine
            }

            tracing::info!(
                "ReplicationWorker: ledger {ledger_id} segment [{seg_start}..{seg_end}] has bad bookies at indices {:?}",
                bad_indices
            );

            // Select replacement bookies
            let mut replacements: HashMap<usize, String> = HashMap::new();
            let mut exclude: HashSet<&str> = ensemble.iter().map(|s| s.as_str()).collect();
            for &bad_idx in &bad_indices {
                if let Some(replacement) = pick_replacement(&available, &exclude) {
                    exclude.insert(replacement);
                    replacements.insert(bad_idx, replacement.to_string());
                } else {
                    return Err(ReplicationError::Failed(
                        "no available replacement bookie".into(),
                    ));
                }
            }

            // Connect to all bookies (original ensemble + replacements)
            let mut conns: HashMap<String, Arc<BookieConnection>> = HashMap::new();
            for addr in ensemble.iter() {
                if !conns.contains_key(addr) {
                    if let Ok(conn) = connect_to_bookie(addr).await {
                        conns.insert(addr.clone(), conn);
                    }
                }
            }
            for addr in replacements.values() {
                if !conns.contains_key(addr) {
                    let conn = connect_to_bookie(addr).await?;
                    conns.insert(addr.clone(), conn);
                }
            }

            // Copy entries from good replicas to replacement bookies
            for entry_id in *seg_start..=*seg_end {
                let write_set = schedule.write_set(entry_id);

                // Read from a good replica
                let entry_data = self
                    .read_entry_from_good_replica(
                        ledger_id,
                        entry_id,
                        ensemble,
                        &bad_indices,
                        &write_set,
                        &conns,
                        &master_key,
                    )
                    .await?;

                // Write to each replacement bookie
                for (&bad_idx, replacement_addr) in &replacements {
                    if !write_set.contains(&bad_idx) {
                        continue; // this entry wasn't on the bad bookie
                    }
                    if let Some(conn) = conns.get(replacement_addr) {
                        let resp = conn
                            .send_add_entry(
                                FLAG_RECOVERY_ADD | FLAG_HIGH_PRIORITY,
                                master_key,
                                entry_data.clone(),
                            )
                            .await
                            .map_err(|e| {
                                ReplicationError::Failed(format!(
                                    "write to replacement {replacement_addr}: {e}"
                                ))
                            })?;
                        match resp {
                            V2Response::Add { error_code, .. } if error_code == V2_EOK => {}
                            V2Response::Add { error_code, .. } => {
                                return Err(ReplicationError::Failed(format!(
                                    "replacement write error code: {error_code}"
                                )));
                            }
                            _ => {
                                return Err(ReplicationError::Failed(
                                    "unexpected response from replacement".into(),
                                ));
                            }
                        }
                    }
                }
            }

            // Update ensemble metadata with old→new bookie mapping
            self.update_ensemble_metadata(ledger_id, *seg_start, ensemble, &replacements)
                .await?;

            tracing::info!(
                "ReplicationWorker: ledger {ledger_id} segment [{seg_start}..{seg_end}] replicated, replacements: {:?}",
                replacements
            );
        }

        Ok(())
    }

    /// Replicate an open ledger using recovery protocol (fence + forward read).
    /// Only used when the ledger is still open and has missing bookies.
    async fn replicate_open_ledger(
        &self,
        ledger_id: i64,
        md: &LedgerMetadata,
        schedule: &RoundRobinDistributionSchedule,
        dm: DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Result<(), ReplicationError> {
        let ensembles = md.all_ensembles();
        let (last_segment_entry, ensemble) = ensembles
            .iter()
            .next_back()
            .ok_or_else(|| ReplicationError::Failed("no ensembles".into()))?;

        let mut conns: Vec<Arc<BookieConnection>> = Vec::new();
        for addr in ensemble {
            let conn = connect_to_bookie(addr).await?;
            conns.push(conn);
        }

        let mut recovery = RecoveryHandle::new(
            ledger_id,
            ensemble.clone(),
            schedule.clone(),
            dm,
            master_key,
        );
        let (last_entry, total_length) =
            recovery.recover(&conns, *last_segment_entry).await?;

        // Update metadata to CLOSED with CAS retry
        for _ in 0..bookkeeper_common::MAX_CAS_RETRIES {
            let versioned = self
                .ledger_manager
                .read_ledger_metadata(ledger_id)
                .await
                .map_err(|e| ReplicationError::Failed(format!("re-read metadata: {e}")))?;

            let current = versioned.value();
            let version = versioned.version();

            if current.state() == LedgerState::Closed {
                break;
            }

            let closed_md = LedgerMetadataBuilder::from_existing(current)
                .with_closed_state(last_entry, total_length)
                .build()
                .map_err(|e| ReplicationError::Failed(format!("build closed md: {e}")))?;

            match self
                .ledger_manager
                .write_ledger_metadata(ledger_id, &closed_md, version)
                .await
            {
                Ok(_) => break,
                Err(bookkeeper_metadata::ledger_metadata::MetadataError::VersionConflict {
                    ..
                }) => continue,
                Err(e) => {
                    return Err(ReplicationError::Failed(format!("close metadata: {e}")));
                }
            }
        }

        Ok(())
    }

    /// Detect bad bookies in a segment by probing sample entries per bookie.
    ///
    /// Matches Java's LedgerChecker.verifyLedgerFragment():
    /// - For each bookie in the ensemble, find entries that SHOULD be on it (via write_set)
    /// - Sample first + last + a few in between
    /// - If any sample read fails, bookie is marked bad
    async fn detect_bad_bookies(
        &self,
        ledger_id: i64,
        first_entry_id: i64,
        last_entry_id: i64,
        ensemble: &[String],
        schedule: &RoundRobinDistributionSchedule,
        master_key: &[u8; MASTER_KEY_LENGTH],
    ) -> Result<Vec<usize>, ReplicationError> {
        let mut bad = Vec::new();

        for (idx, addr) in ensemble.iter().enumerate() {
            let conn = match connect_to_bookie(addr).await {
                Ok(c) => c,
                Err(_) => {
                    bad.push(idx);
                    continue;
                }
            };

            // Find entries in this segment that should be on this bookie
            // (i.e., entries where idx is in the write_set)
            let mut entries_on_bookie: Vec<i64> = Vec::new();
            for eid in first_entry_id..=last_entry_id {
                if schedule.write_set(eid).contains(&idx) {
                    entries_on_bookie.push(eid);
                }
                // Limit scan for large segments — collect enough to sample
                if entries_on_bookie.len() > 1000 {
                    break;
                }
            }

            if entries_on_bookie.is_empty() {
                continue; // this bookie has no entries in this segment
            }

            // Sample: first, last, and up to 3 evenly spaced entries
            let mut samples: Vec<i64> = Vec::new();
            samples.push(entries_on_bookie[0]); // first
            if entries_on_bookie.len() > 1 {
                samples.push(*entries_on_bookie.last().unwrap()); // last
            }
            // Add evenly spaced samples
            let count = entries_on_bookie.len();
            if count > 2 {
                for i in 1..=std::cmp::min(3, count - 2) {
                    let pos = i * count / (std::cmp::min(3, count - 2) + 1);
                    let eid = entries_on_bookie[pos];
                    if !samples.contains(&eid) {
                        samples.push(eid);
                    }
                }
            }

            // Probe each sample entry
            let mut is_bad = false;
            for &eid in &samples {
                match conn
                    .send_read_entry(0, ledger_id, eid, Some(*master_key))
                    .await
                {
                    Ok(V2Response::Read { error_code, .. }) if error_code == V2_EOK => {}
                    _ => {
                        is_bad = true;
                        break;
                    }
                }
            }
            if is_bad {
                bad.push(idx);
            }
        }
        Ok(bad)
    }

    /// Read an entry from any good replica (skipping bad bookies).
    async fn read_entry_from_good_replica(
        &self,
        ledger_id: i64,
        entry_id: i64,
        ensemble: &[String],
        bad_indices: &[usize],
        write_set: &[usize],
        conns: &HashMap<String, Arc<BookieConnection>>,
        master_key: &[u8; MASTER_KEY_LENGTH],
    ) -> Result<Bytes, ReplicationError> {
        for &bookie_idx in write_set {
            if bad_indices.contains(&bookie_idx) {
                continue;
            }
            if bookie_idx >= ensemble.len() {
                continue;
            }
            let addr = &ensemble[bookie_idx];
            if let Some(conn) = conns.get(addr) {
                match conn
                    .send_read_entry(0, ledger_id, entry_id, Some(*master_key))
                    .await
                {
                    Ok(V2Response::Read {
                        error_code, data, ..
                    }) if error_code == V2_EOK => {
                        if let Some(entry_data) = data {
                            return Ok(entry_data);
                        }
                    }
                    _ => continue,
                }
            }
        }
        Err(ReplicationError::Failed(format!(
            "cannot read entry {entry_id} from any good replica"
        )))
    }

    /// Update ensemble metadata to replace bad bookies with replacements.
    /// Uses CAS retry loop matching Java's MetadataUpdateLoop.
    async fn update_ensemble_metadata(
        &self,
        ledger_id: i64,
        segment_start: i64,
        old_ensemble: &[String],
        replacements: &HashMap<usize, String>,
    ) -> Result<(), ReplicationError> {
        for _ in 0..bookkeeper_common::MAX_CAS_RETRIES {
            let versioned = self
                .ledger_manager
                .read_ledger_metadata(ledger_id)
                .await
                .map_err(|e| ReplicationError::Failed(format!("read metadata: {e}")))?;

            let md = versioned.value();
            let version = versioned.version();

            // Build new ensemble with replacements
            let current_ensemble = md
                .all_ensembles()
                .get(&segment_start)
                .cloned()
                .unwrap_or_else(|| old_ensemble.to_vec());

            let new_ensemble: Vec<String> = current_ensemble
                .iter()
                .enumerate()
                .map(|(idx, addr)| {
                    replacements
                        .get(&idx)
                        .cloned()
                        .unwrap_or_else(|| addr.clone())
                })
                .collect();

            // Check if update is still needed
            if new_ensemble == current_ensemble {
                return Ok(()); // already updated by someone else
            }

            let mut builder = LedgerMetadataBuilder::from_existing(md);
            // Replace the target segment's ensemble
            builder.clear_ensembles();
            for (first_entry, ens) in md.all_ensembles() {
                if *first_entry == segment_start {
                    builder = builder.new_ensemble_entry(*first_entry, new_ensemble.clone());
                } else {
                    builder = builder.new_ensemble_entry(*first_entry, ens.clone());
                }
            }
            if md.state() == LedgerState::Closed {
                builder = builder.with_closed_state(
                    md.last_entry_id().unwrap_or(-1),
                    md.length().unwrap_or(0),
                );
            }

            let new_md = builder
                .build()
                .map_err(|e| ReplicationError::Failed(format!("build metadata: {e}")))?;

            match self
                .ledger_manager
                .write_ledger_metadata(ledger_id, &new_md, version)
                .await
            {
                Ok(_) => return Ok(()),
                Err(bookkeeper_metadata::ledger_metadata::MetadataError::VersionConflict {
                    ..
                }) => continue,
                Err(e) => {
                    return Err(ReplicationError::Failed(format!(
                        "update ensemble metadata: {e}"
                    )));
                }
            }
        }
        Err(ReplicationError::Failed(
            "ensemble metadata update CAS failed after 10 retries".into(),
        ))
    }

    async fn get_available_bookies(&self) -> Result<Vec<String>, ReplicationError> {
        // Use ZK to discover available bookies
        let (children, _) = self
            .ur_manager
            .client()
            .get_children(self.ur_manager.paths().available())
            .await
            .map_err(|e| ReplicationError::Zk(format!("list bookies: {e}")))?;
        Ok(children.into_iter().filter(|c| c != "readonly").collect())
    }
}

/// Pick a replacement bookie from available list, excluding specified bookies.
fn pick_replacement<'a>(available: &'a [String], exclude: &HashSet<&str>) -> Option<&'a str> {
    available
        .iter()
        .find(|addr| !exclude.contains(addr.as_str()))
        .map(|s| s.as_str())
}

/// Connect to a bookie by address string.
async fn connect_to_bookie(addr: &str) -> Result<Arc<BookieConnection>, ReplicationError> {
    let socket_addr: SocketAddr = addr
        .parse()
        .map_err(|e| ReplicationError::Failed(format!("parse addr {addr}: {e}")))?;
    let conn = BookieConnection::connect(socket_addr)
        .await
        .map_err(ReplicationError::Client)?;
    Ok(Arc::new(conn))
}
