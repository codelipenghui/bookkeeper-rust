use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;
use bytes::Bytes;
use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_client::placement::{EnsemblePlacementPolicy, PolicyAdherence};
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
/// Max size of the failed-ledger cache matching Java's REPLICATED_FAILED_LEDGERS_MAXSIZE.
const FAILED_LEDGER_CACHE_MAX: usize = 2000;
/// Max exponential backoff retries before capping, matching Java's NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS.
const MAX_BACKOFF_RETRIES: u32 = 5;

pub struct ReplicationWorker {
    ur_manager: Arc<ZkUnderreplicationManager>,
    ledger_manager: Arc<dyn LedgerManager>,
    bookie_id: String,
    base_backoff_ms: u64,
    shutdown_rx: watch::Receiver<bool>,
    /// Consecutive failure count for exponential backoff.
    consecutive_failures: u32,
    /// Recently-failed ledger IDs to avoid re-attempting immediately.
    /// Matches Java's `replicatedFailedLedgers` LoadingCache.
    failed_ledger_cache: std::collections::VecDeque<i64>,
    /// Grace period (ms) before fencing an open ledger.
    /// Gives the writing client time to handle bookie failure itself.
    /// 0 = immediate fencing. Matches Java's `openLedgerRereplicationGracePeriod`.
    open_ledger_grace_period_ms: u64,
    /// Bounds how often the worker walks the ZK under-replication tree.
    /// Each `try_rereplicate` call can enumerate many znodes in a large
    /// cluster, so without a cap a restart storm could flood ZK. The
    /// limiter enforces ≤ `max_claims_per_second` attempts — matches
    /// the spirit of Java's `replicationRateByBytes` / ZK client rate
    /// controls used to protect ZK from compaction-style scans.
    /// 0 = unbounded.
    rate_limit: Option<ClaimRateLimiter>,
    /// Placement policy used to pick replacement bookies and to detect
    /// fragments that violate placement even when availability is fine.
    /// When set, the worker will:
    ///   1. Invoke `replace_bookie(..)` instead of naive first-available
    ///      to honour region/rack-aware constraints.
    ///   2. During closed-ledger repair, check each segment's adherence
    ///      and re-place the full segment when the existing bookies are
    ///      live but violate placement. Matches Java's
    ///      `ReplicationWorker.handlePlacementPolicyViolation` path.
    placement_policy: Option<Arc<dyn EnsemblePlacementPolicy>>,
}

/// Simple sleep-based token bucket for rate-limiting ZK claim attempts.
/// Not thread-safe — the worker is single-tasked by design.
struct ClaimRateLimiter {
    min_interval: std::time::Duration,
    next_allowed: std::time::Instant,
}

impl ClaimRateLimiter {
    fn new(claims_per_second: u32) -> Self {
        let min_interval = std::time::Duration::from_nanos(
            1_000_000_000u64 / claims_per_second.max(1) as u64,
        );
        Self {
            min_interval,
            next_allowed: std::time::Instant::now(),
        }
    }

    /// Await until the next claim is allowed; update the deadline.
    async fn acquire(&mut self) {
        let now = std::time::Instant::now();
        if self.next_allowed > now {
            tokio::time::sleep(self.next_allowed - now).await;
        }
        self.next_allowed = std::time::Instant::now() + self.min_interval;
    }
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
            base_backoff_ms: backoff_ms,
            shutdown_rx,
            consecutive_failures: 0,
            failed_ledger_cache: std::collections::VecDeque::new(),
            open_ledger_grace_period_ms: 30_000, // default 30s matching Java
            rate_limit: None,
            placement_policy: None,
        }
    }

    /// Install the placement policy used for replacement-bookie selection
    /// and placement-violation detection. Matches Java's
    /// `EnsemblePlacementPolicy` wiring in `ReplicationWorker`.
    pub fn set_placement_policy(&mut self, policy: Arc<dyn EnsemblePlacementPolicy>) {
        self.placement_policy = Some(policy);
    }

    /// Set the grace period before fencing open ledgers (ms).
    /// 0 = immediate. Matches Java's `openLedgerRereplicationGracePeriod`.
    pub fn set_open_ledger_grace_period_ms(&mut self, ms: u64) {
        self.open_ledger_grace_period_ms = ms;
    }

    /// Cap how many ZK under-replication scans the worker launches per
    /// second. 0 disables the limiter. Mirrors operator tuning that Java
    /// exposes via `rwReRereplicateBackoffMs` + ZK client throttling —
    /// prevents a newly-woken worker from flooding ZK with tree walks.
    pub fn set_max_claims_per_second(&mut self, cps: u32) {
        self.rate_limit = if cps > 0 { Some(ClaimRateLimiter::new(cps)) } else { None };
    }

    fn current_backoff_ms(&self) -> u64 {
        let exponent = self.consecutive_failures.min(MAX_BACKOFF_RETRIES);
        self.base_backoff_ms * 2u64.pow(exponent)
    }

    fn record_success(&mut self) {
        self.consecutive_failures = 0;
    }

    fn record_failure(&mut self, ledger_id: i64) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if !self.failed_ledger_cache.contains(&ledger_id) {
            if self.failed_ledger_cache.len() >= FAILED_LEDGER_CACHE_MAX {
                self.failed_ledger_cache.pop_front();
            }
            self.failed_ledger_cache.push_back(ledger_id);
        }
    }

    fn is_recently_failed(&self, ledger_id: i64) -> bool {
        self.failed_ledger_cache.contains(&ledger_id)
    }

    /// Run the replication worker main loop.
    ///
    /// Instead of polling, the worker installs a `PERSISTENT_RECURSIVE`
    /// ZK watch on the under-replication tree and sleeps on it when idle.
    /// That matches Java's `getLedgerToRereplicate()` internal
    /// `notifyUnderReplicationLedgerChanged` wait — a new under-replicated
    /// ledger wakes every worker immediately rather than at the next
    /// backoff tick.
    pub async fn run(&mut self) -> Result<(), ReplicationError> {
        tracing::info!("ReplicationWorker started (bookie: {})", self.bookie_id);

        // Arm the persistent watch up front so we never miss events between
        // the scan and the sleep. Re-armed after every fire.
        let mut tree_watcher: Option<zookeeper_client::PersistentWatcher> =
            match self.ur_manager.watch_underreplicated_tree().await {
                Ok(w) => Some(w),
                Err(e) => {
                    tracing::warn!("ReplicationWorker: watch setup failed ({e}) — falling back to polling");
                    None
                }
            };

        loop {
            if *self.shutdown_rx.borrow() {
                tracing::info!("ReplicationWorker shutting down");
                break;
            }

            let result = self.try_rereplicate().await;

            match result {
                Ok(true) => {
                    self.record_success();
                }
                Ok(false) => {
                    // Idle: sleep until the persistent watch fires OR a
                    // backoff timeout elapses. The timeout acts as a
                    // safety net against lost events (ZOOKEEPER-4698).
                    let backoff = self.current_backoff_ms();
                    let timeout = tokio::time::Duration::from_millis(backoff);
                    let watch_fut = async {
                        match tree_watcher.as_mut() {
                            Some(w) => {
                                let _ = w.changed().await;
                            }
                            None => std::future::pending::<()>().await,
                        }
                    };
                    tokio::select! {
                        _ = self.shutdown_rx.changed() => {
                            if *self.shutdown_rx.borrow() { break; }
                        }
                        _ = watch_fut => {
                            // Event received; drop and re-arm so we don't
                            // accumulate dangling subscriptions.
                            tree_watcher = match self.ur_manager.watch_underreplicated_tree().await {
                                Ok(w) => Some(w),
                                Err(e) => {
                                    tracing::warn!("ReplicationWorker: re-arm watch failed: {e}");
                                    None
                                }
                            };
                        }
                        _ = tokio::time::sleep(timeout) => {
                            if tree_watcher.is_none() {
                                tree_watcher = self.ur_manager
                                    .watch_underreplicated_tree().await.ok();
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("ReplicationWorker error: {e}");
                    self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                    let backoff = self.current_backoff_ms();
                    tokio::select! {
                        _ = self.shutdown_rx.changed() => {
                            if *self.shutdown_rx.borrow() { break; }
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_millis(backoff)) => {}
                    }
                }
            }
        }
        Ok(())
    }

    async fn try_rereplicate(&mut self) -> Result<bool, ReplicationError> {
        // Rate-limit ZK scan attempts before any ZK read. Keeps a single
        // worker — or a fleet of them after a bookie restart — from
        // hammering ZK with tree walks.
        if let Some(limiter) = self.rate_limit.as_mut() {
            limiter.acquire().await;
        }

        if !self.ur_manager.is_replication_enabled().await? {
            return Ok(false);
        }

        let ledger_id = match self.ur_manager.get_ledger_to_rereplicate().await? {
            Some(lid) => lid,
            None => return Ok(false),
        };

        // Skip recently-failed ledgers to avoid repeated failures.
        // Matches Java's replicatedFailedLedgers cache check.
        if self.is_recently_failed(ledger_id) {
            tracing::debug!("ReplicationWorker: skipping recently-failed ledger {ledger_id}");
            self.ur_manager.release_lock(ledger_id).await?;
            return Ok(false);
        }

        tracing::info!("ReplicationWorker: claimed ledger {ledger_id} for replication");

        match self.rereplicate(ledger_id).await {
            Ok(()) => {
                self.ur_manager.mark_ledger_replicated(ledger_id).await?;
                tracing::info!("ReplicationWorker: ledger {ledger_id} replicated successfully");
                Ok(true)
            }
            Err(e) => {
                tracing::warn!("ReplicationWorker: failed to replicate ledger {ledger_id}: {e}");
                self.record_failure(ledger_id);
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

        // Write/ack quorum for policy adherence + replace_bookie calls.
        let wq = md.write_quorum_size() as usize;
        let aq = md.ack_quorum_size() as usize;
        let es = md.ensemble_size() as usize;

        for (seg_start, seg_end, ensemble) in &segments {
            // Detect bad bookies by verifying ALL entries per bookie.
            // Returns a map: bad_bookie_index -> set of missing entry IDs.
            let mut bad_bookies = self
                .detect_bad_bookies(
                    ledger_id, *seg_start, *seg_end, ensemble, schedule, &master_key,
                )
                .await?;

            // Placement-aware repair: even when all bookies are alive, an
            // existing ensemble may violate region/rack constraints (for
            // example after a topology change). Mark those bookies as "bad"
            // so they get replaced via the placement policy. Matches Java's
            // `AuditorPlacementPolicyCheckTask` → `ReplicationWorker`
            // cooperative flow.
            if let Some(ref policy) = self.placement_policy {
                if bad_bookies.is_empty()
                    && policy.is_ensemble_adhering(ensemble, wq, aq) == PolicyAdherence::Fail
                {
                    // Nominate the first bookie for replacement — the
                    // replace_bookie call below will pick a location that
                    // actually improves adherence.
                    let missing_range: HashSet<i64> = (*seg_start..=*seg_end)
                        .filter(|eid| schedule.write_set(*eid).contains(&0))
                        .collect();
                    if !missing_range.is_empty() {
                        bad_bookies.insert(0, missing_range);
                        tracing::info!(
                            "ReplicationWorker: ledger {ledger_id} segment [{seg_start}..{seg_end}] \
                             violates placement policy — rereplicating bookie index 0"
                        );
                    }
                }
            }

            if bad_bookies.is_empty() {
                continue; // this segment is fine
            }

            let bad_indices: Vec<usize> = bad_bookies.keys().copied().collect();
            tracing::info!(
                "ReplicationWorker: ledger {ledger_id} segment [{seg_start}..{seg_end}] has bad bookies at indices {:?}",
                bad_indices
            );

            // Select replacement bookies — via placement policy when we
            // have one, otherwise the simple first-fit picker.
            let mut replacements: HashMap<usize, String> = HashMap::new();
            let mut exclude: HashSet<String> = ensemble.iter().cloned().collect();
            for &bad_idx in &bad_indices {
                let bad_addr = ensemble.get(bad_idx).cloned().unwrap_or_default();
                let replacement = if let Some(ref policy) = self.placement_policy {
                    match policy.replace_bookie(es, wq, aq, ensemble, &bad_addr, &exclude) {
                        Ok(addr) => addr,
                        Err(e) => {
                            tracing::warn!(
                                "ReplicationWorker: placement policy replace_bookie failed \
                                 ({e}) — falling back to first-available"
                            );
                            match pick_replacement(&available, &exclude) {
                                Some(a) => a.to_string(),
                                None => return Err(ReplicationError::Failed(
                                    "no available replacement bookie".into(),
                                )),
                            }
                        }
                    }
                } else {
                    match pick_replacement(&available, &exclude) {
                        Some(a) => a.to_string(),
                        None => return Err(ReplicationError::Failed(
                            "no available replacement bookie".into(),
                        )),
                    }
                };
                exclude.insert(replacement.clone());
                replacements.insert(bad_idx, replacement);
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

            // Collect all entry IDs that need to be copied (union of all
            // missing entries across all bad bookies). Using a set avoids
            // reading the same entry twice when multiple bookies are missing it.
            let mut entries_to_copy: HashSet<i64> = HashSet::new();
            for missing in bad_bookies.values() {
                entries_to_copy.extend(missing);
            }
            let mut sorted_entries: Vec<i64> = entries_to_copy.into_iter().collect();
            sorted_entries.sort_unstable();

            // Copy only the actually-missing entries to replacement bookies
            for entry_id in sorted_entries {
                let write_set = schedule.write_set(entry_id);

                // Read from a good replica (any bookie NOT in bad_indices that
                // has this entry in its write set)
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

                // Write to each replacement bookie that is missing this entry
                for (&bad_idx, replacement_addr) in &replacements {
                    let is_missing = bad_bookies.get(&bad_idx)
                        .map_or(false, |missing| missing.contains(&entry_id));
                    if !is_missing {
                        continue; // this entry is NOT missing on this bookie
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
        // Grace period: wait before fencing to let the writing client handle
        // the failure itself. Matches Java's openLedgerRereplicationGracePeriod.
        if self.open_ledger_grace_period_ms > 0 {
            tracing::info!(
                "Waiting {:.1}s grace period before fencing open ledger {ledger_id}",
                self.open_ledger_grace_period_ms as f64 / 1000.0
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(
                self.open_ledger_grace_period_ms,
            ))
            .await;

            // Re-check if the ledger was closed during the grace period
            let versioned = self
                .ledger_manager
                .read_ledger_metadata(ledger_id)
                .await
                .map_err(|e| ReplicationError::Failed(format!("re-read after grace: {e}")))?;
            if versioned.value().state() == LedgerState::Closed {
                tracing::info!(
                    "Ledger {ledger_id} was closed during grace period, treating as closed"
                );
                return self
                    .replicate_closed_ledger(
                        ledger_id,
                        versioned.value(),
                        schedule,
                        &dm,
                        master_key,
                    )
                    .await;
            }
        }

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

    /// Detect bad bookies in a segment by verifying ALL entries per bookie.
    ///
    /// Matches Java's LedgerChecker.verifyLedgerFragment():
    /// - For each bookie in the ensemble, find entries that SHOULD be on it (via write_set)
    /// - Verify every entry exists on the bookie
    /// - Returns a map of bad_bookie_index -> set of missing entry IDs
    ///
    /// Unlike the previous sampling approach, this checks every entry so it
    /// cannot produce false negatives (missing gaps in between samples).
    async fn detect_bad_bookies(
        &self,
        ledger_id: i64,
        first_entry_id: i64,
        last_entry_id: i64,
        ensemble: &[String],
        schedule: &RoundRobinDistributionSchedule,
        master_key: &[u8; MASTER_KEY_LENGTH],
    ) -> Result<HashMap<usize, HashSet<i64>>, ReplicationError> {
        let mut bad: HashMap<usize, HashSet<i64>> = HashMap::new();

        for (idx, addr) in ensemble.iter().enumerate() {
            let conn = match connect_to_bookie(addr).await {
                Ok(c) => c,
                Err(_) => {
                    // Bookie unreachable: mark ALL entries on this bookie as missing
                    let missing: HashSet<i64> = (first_entry_id..=last_entry_id)
                        .filter(|eid| schedule.write_set(*eid).contains(&idx))
                        .collect();
                    if !missing.is_empty() {
                        bad.insert(idx, missing);
                    }
                    continue;
                }
            };

            let mut missing_entries: HashSet<i64> = HashSet::new();

            for eid in first_entry_id..=last_entry_id {
                if !schedule.write_set(eid).contains(&idx) {
                    continue; // this entry is not on this bookie
                }
                match conn
                    .send_read_entry(0, ledger_id, eid, Some(*master_key))
                    .await
                {
                    Ok(V2Response::Read { error_code, .. }) if error_code == V2_EOK => {}
                    _ => {
                        missing_entries.insert(eid);
                    }
                }
            }

            if !missing_entries.is_empty() {
                bad.insert(idx, missing_entries);
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
fn pick_replacement<'a>(available: &'a [String], exclude: &HashSet<String>) -> Option<&'a str> {
    available
        .iter()
        .find(|addr| !exclude.contains(*addr))
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
