use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use futures::stream::{self, StreamExt};
use tokio::sync::watch;
use zookeeper_client as zk;
use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_metadata::zk_path::ZkPaths;
use bookkeeper_metadata::traits::LedgerManager;
use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::generate_master_key;
use bookkeeper_protocol::v2::response::V2Response;
use crate::underreplication::ZkUnderreplicationManager;
use crate::ReplicationError;

/// Max concurrent ledger metadata reads during all-ledgers check.
/// Matches Java's `auditorMaxNumberOfConcurrentOpenLedgerOperations` (default 500).
const AUDIT_CONCURRENCY: usize = 500;

/// Leader-elected auditor that watches for bookie failures
/// and marks affected ledgers as underreplicated.
///
/// Matches Java's Auditor + AuditorBookieCheckTask:
/// 1. Watch both `/ledgers/available` AND `/ledgers/available/readonly` via ZK watchers
/// 2. Detect bookie failures via set difference
/// 3. Build bookie→ledger index by scanning all ledgers via LedgerManager
/// 4. Mark affected ledgers as underreplicated
/// 5. Serializes audit tasks to prevent concurrent overlapping audits
pub struct Auditor {
    client: zk::Client,
    paths: ZkPaths,
    ur_manager: Arc<ZkUnderreplicationManager>,
    ledger_manager: Arc<dyn LedgerManager>,
    known_bookies: HashSet<String>,
    known_readonly_bookies: HashSet<String>,
    lost_bookie_recovery_delay_secs: u64,
    shutdown_rx: watch::Receiver<bool>,
    auditing: Arc<AtomicBool>,
    /// Optional placement policy for placement check auditing.
    placement_policy: Option<Arc<dyn bookkeeper_client::placement::EnsemblePlacementPolicy>>,
    /// Interval for periodic all-ledgers check (seconds). 0 = disabled.
    /// Matches Java's `auditorPeriodicCheckInterval` (default 604800 = 7 days).
    all_ledgers_check_interval_secs: u64,
    /// Interval for periodic placement policy check (seconds). 0 = disabled.
    /// Matches Java's `auditorPeriodicPlacementPolicyCheckInterval` (default 0 = disabled).
    placement_check_interval_secs: u64,
    /// Interval for periodic replicas check (seconds). 0 = disabled.
    /// Matches Java's `auditorPeriodicReplicasCheckInterval` (default 604800 = 7 days).
    /// Unlike `all_ledgers_check`, this actually probes each bookie for
    /// each entry — it detects entries whose replica count has dropped
    /// below the write quorum even when all ensemble members are alive.
    replicas_check_interval_secs: u64,
}

impl Auditor {
    pub fn new(
        client: zk::Client,
        paths: ZkPaths,
        ur_manager: Arc<ZkUnderreplicationManager>,
        ledger_manager: Arc<dyn LedgerManager>,
        lost_bookie_recovery_delay_secs: u64,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            client,
            paths,
            ur_manager,
            ledger_manager,
            known_bookies: HashSet::new(),
            known_readonly_bookies: HashSet::new(),
            lost_bookie_recovery_delay_secs,
            shutdown_rx,
            auditing: Arc::new(AtomicBool::new(false)),
            placement_policy: None,
            all_ledgers_check_interval_secs: 0,
            placement_check_interval_secs: 0,
            replicas_check_interval_secs: 0,
        }
    }

    /// Set the interval for periodic replicas check (0 = disabled).
    /// Matches Java's `auditorPeriodicReplicasCheckInterval`.
    pub fn set_replicas_check_interval(&mut self, secs: u64) {
        self.replicas_check_interval_secs = secs;
    }

    /// Set the interval for periodic all-ledgers availability check.
    /// 0 = disabled. Matches Java's `auditorPeriodicCheckInterval`.
    pub fn set_all_ledgers_check_interval(&mut self, secs: u64) {
        self.all_ledgers_check_interval_secs = secs;
    }

    /// Set the interval for periodic placement policy check.
    /// 0 = disabled. Matches Java's `auditorPeriodicPlacementPolicyCheckInterval`.
    pub fn set_placement_check_interval(&mut self, secs: u64) {
        self.placement_check_interval_secs = secs;
    }

    /// Set the placement policy for placement audit checks.
    pub fn set_placement_policy(&mut self, policy: Arc<dyn bookkeeper_client::placement::EnsemblePlacementPolicy>) {
        self.placement_policy = Some(policy);
    }

    /// Run the auditor main loop.
    ///
    /// Uses ZK watchers for real-time bookie change detection matching Java's
    /// watchBookieChanges() which watches BOTH writable and readonly bookies.
    /// Periodic 60s fallback as safety net.
    pub async fn run(&mut self) -> Result<(), ReplicationError> {
        tracing::info!("Auditor started");

        // Initial bookie list with watchers for both paths
        let writable_watcher = self.refresh_writable_with_watch().await?;
        let readonly_watcher = self.refresh_readonly_with_watch().await?;

        let mut cur_writable_watcher: Option<zk::OneshotWatcher> = Some(writable_watcher);
        let mut cur_readonly_watcher: Option<zk::OneshotWatcher> = Some(readonly_watcher);

        // Run initial audit immediately after election (matching Java's scheduleBookieCheckTask)
        if let Err(e) = self.check_bookies().await {
            tracing::warn!("Auditor initial check error: {e}");
        }

        // Set up periodic check intervals matching Java's scheduled tasks.
        // Each runs independently on its own cadence. When a previous run's
        // timestamp is persisted in ZK (matching Java's checkAllLedgersCTime
        // / placementPolicyCheckCTime), the first tick is deferred so we
        // don't immediately re-run an expensive scan on restart — we pay
        // the full interval minus the time elapsed since the last run.
        let mut all_ledgers_interval = if self.all_ledgers_check_interval_secs > 0 {
            let interval_secs = self.all_ledgers_check_interval_secs;
            let last_run_ms = self.ur_manager.get_check_all_ledgers_ctime().await.unwrap_or(-1);
            let remaining = remaining_before_next_run(last_run_ms, interval_secs);
            let start_at = tokio::time::Instant::now() + remaining;
            let iv = tokio::time::interval_at(
                start_at,
                tokio::time::Duration::from_secs(interval_secs),
            );
            Some(iv)
        } else {
            None
        };
        let mut placement_interval = if self.placement_check_interval_secs > 0 {
            let interval_secs = self.placement_check_interval_secs;
            let last_run_ms = self.ur_manager.get_placement_policy_check_ctime().await.unwrap_or(-1);
            let remaining = remaining_before_next_run(last_run_ms, interval_secs);
            let start_at = tokio::time::Instant::now() + remaining;
            let iv = tokio::time::interval_at(
                start_at,
                tokio::time::Duration::from_secs(interval_secs),
            );
            Some(iv)
        } else {
            None
        };
        let mut replicas_interval = if self.replicas_check_interval_secs > 0 {
            let interval_secs = self.replicas_check_interval_secs;
            let last_run_ms = self.ur_manager.get_replicas_check_ctime().await.unwrap_or(-1);
            let remaining = remaining_before_next_run(last_run_ms, interval_secs);
            let start_at = tokio::time::Instant::now() + remaining;
            let iv = tokio::time::interval_at(
                start_at,
                tokio::time::Duration::from_secs(interval_secs),
            );
            Some(iv)
        } else {
            None
        };

        // Watch the lostBookieRecoveryDelay znode so operator changes via
        // `bookkeeper shell` take effect without waiting for the next
        // audit cycle. Matches Java's
        // `ZkLedgerUnderreplicationManager.notifyLostBookieRecoveryDelayChanged`.
        let mut delay_watcher = match self.ur_manager
            .watch_lost_bookie_recovery_delay(self.lost_bookie_recovery_delay_secs).await {
            Ok((delay, w)) => {
                self.lost_bookie_recovery_delay_secs = delay;
                Some(w)
            }
            Err(e) => {
                tracing::warn!("Auditor: failed to set delay watcher: {e}");
                None
            }
        };

        loop {
            // Build futures for both watchers
            let writable_fut = async {
                match cur_writable_watcher.take() {
                    Some(w) => Some(w.changed().await),
                    None => { std::future::pending::<Option<zk::WatchedEvent>>().await }
                }
            };
            let readonly_fut = async {
                match cur_readonly_watcher.take() {
                    Some(w) => Some(w.changed().await),
                    None => { std::future::pending::<Option<zk::WatchedEvent>>().await }
                }
            };
            // Build periodic check futures (pending forever if disabled)
            let all_ledgers_fut = async {
                match all_ledgers_interval.as_mut() {
                    Some(iv) => iv.tick().await,
                    None => { std::future::pending::<tokio::time::Instant>().await }
                }
            };
            let placement_fut = async {
                match placement_interval.as_mut() {
                    Some(iv) => iv.tick().await,
                    None => { std::future::pending::<tokio::time::Instant>().await }
                }
            };
            let replicas_fut = async {
                match replicas_interval.as_mut() {
                    Some(iv) => iv.tick().await,
                    None => { std::future::pending::<tokio::time::Instant>().await }
                }
            };
            let delay_fut = async {
                match delay_watcher.take() {
                    Some(w) => Some(w.changed().await),
                    None => { std::future::pending::<Option<zk::WatchedEvent>>().await }
                }
            };

            tokio::select! {
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("Auditor shutting down");
                        break;
                    }
                }
                event = writable_fut => {
                    self.handle_watcher_event(event).await;
                    cur_writable_watcher = self.try_set_writable_watch().await;
                }
                event = readonly_fut => {
                    self.handle_watcher_event(event).await;
                    cur_readonly_watcher = self.try_set_readonly_watch().await;
                }
                _ = all_ledgers_fut => {
                    // Periodic all-ledgers check. Matches Java's scheduleCheckAllLedgersTask.
                    tracing::info!("Auditor: running periodic all-ledgers check");
                    match self.check_all_ledgers().await {
                        Ok(n) => {
                            if n > 0 { tracing::info!("Auditor: marked {n} underreplicated ledger(s)"); }
                            // Persist completion timestamp so a restart before
                            // the next interval doesn't re-run the scan.
                            let now_ms = current_time_millis();
                            if let Err(e) = self.ur_manager.set_check_all_ledgers_ctime(now_ms).await {
                                tracing::warn!("Auditor: failed to persist checkAllLedgersCTime: {e}");
                            }
                        }
                        Err(e) => tracing::warn!("Auditor: all-ledgers check error: {e}"),
                    }
                }
                _ = placement_fut => {
                    // Periodic placement policy check. Matches Java's schedulePlacementPolicyCheckTask.
                    tracing::info!("Auditor: running periodic placement policy check");
                    match self.check_placement_policy().await {
                        Ok(n) => {
                            if n > 0 { tracing::info!("Auditor: marked {n} placement-violating ledger(s)"); }
                            let now_ms = current_time_millis();
                            if let Err(e) = self.ur_manager.set_placement_policy_check_ctime(now_ms).await {
                                tracing::warn!("Auditor: failed to persist placementPolicyCheckCTime: {e}");
                            }
                        }
                        Err(e) => tracing::warn!("Auditor: placement check error: {e}"),
                    }
                }
                _ = replicas_fut => {
                    // Periodic replicas check: actually probes bookies for
                    // each entry to detect silent partial data loss.
                    // Matches Java's scheduleReplicasCheckTask.
                    tracing::info!("Auditor: running periodic replicas check");
                    match self.check_replicas().await {
                        Ok(n) => {
                            if n > 0 { tracing::info!("Auditor: replicas check marked {n} under-replicated ledger(s)"); }
                            let now_ms = current_time_millis();
                            if let Err(e) = self.ur_manager.set_replicas_check_ctime(now_ms).await {
                                tracing::warn!("Auditor: failed to persist replicasCheckCTime: {e}");
                            }
                        }
                        Err(e) => tracing::warn!("Auditor: replicas check error: {e}"),
                    }
                }
                event = delay_fut => {
                    // lostBookieRecoveryDelay znode changed — re-read so
                    // the next scheduled audit honours the new value.
                    if event.is_some() {
                        match self.ur_manager
                            .watch_lost_bookie_recovery_delay(self.lost_bookie_recovery_delay_secs).await {
                            Ok((delay, w)) => {
                                if delay != self.lost_bookie_recovery_delay_secs {
                                    tracing::info!(
                                        "Auditor: lostBookieRecoveryDelay changed {} -> {} secs",
                                        self.lost_bookie_recovery_delay_secs, delay
                                    );
                                    self.lost_bookie_recovery_delay_secs = delay;
                                }
                                delay_watcher = Some(w);
                            }
                            Err(e) => {
                                tracing::warn!("Auditor: re-arm delay watcher failed: {e}");
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(60)) => {
                    // Periodic bookie check fallback safety net
                    if let Err(e) = self.check_bookies().await {
                        tracing::warn!("Auditor periodic check error: {e}");
                    }
                    if cur_writable_watcher.is_none() {
                        cur_writable_watcher = self.try_set_writable_watch().await;
                    }
                    if cur_readonly_watcher.is_none() {
                        cur_readonly_watcher = self.try_set_readonly_watch().await;
                    }
                    if delay_watcher.is_none() {
                        if let Ok((delay, w)) = self.ur_manager
                            .watch_lost_bookie_recovery_delay(self.lost_bookie_recovery_delay_secs).await {
                            self.lost_bookie_recovery_delay_secs = delay;
                            delay_watcher = Some(w);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Handle a watcher event. Triggers audit on children change or session reconnect.
    async fn handle_watcher_event(&mut self, event: Option<zk::WatchedEvent>) {
        if let Some(ev) = event {
            match ev.event_type {
                zk::EventType::NodeChildrenChanged => {
                    if let Err(e) = self.check_bookies().await {
                        tracing::warn!("Auditor bookie check error: {e}");
                    }
                }
                zk::EventType::Session => {
                    // ZK session reconnected — re-audit in case bookies changed
                    // while disconnected (matching Java's implicit re-audit on reconnect)
                    tracing::info!("Auditor: ZK session event, triggering re-audit");
                    if let Err(e) = self.check_bookies().await {
                        tracing::warn!("Auditor session re-audit error: {e}");
                    }
                }
                _ => {}
            }
        }
    }

    /// Get writable bookies and set a ZK watcher.
    async fn refresh_writable_with_watch(
        &mut self,
    ) -> Result<zk::OneshotWatcher, ReplicationError> {
        let (children, watcher) = self
            .client
            .list_and_watch_children(self.paths.available())
            .await
            .map_err(|e| ReplicationError::Zk(format!("watch writable bookies: {e}")))?;

        self.known_bookies = children
            .into_iter()
            .filter(|c| c != "readonly")
            .collect();

        Ok(watcher)
    }

    /// Get readonly bookies and set a ZK watcher.
    async fn refresh_readonly_with_watch(
        &mut self,
    ) -> Result<zk::OneshotWatcher, ReplicationError> {
        match self
            .client
            .list_and_watch_children(self.paths.available_readonly())
            .await
        {
            Ok((children, watcher)) => {
                self.known_readonly_bookies = children.into_iter().collect();
                Ok(watcher)
            }
            Err(zk::Error::NoNode) => {
                // Readonly path doesn't exist yet — watch parent for creation
                // and return a watcher that will fire when it appears
                let (_children, watcher) = self
                    .client
                    .list_and_watch_children(self.paths.available())
                    .await
                    .map_err(|e| ReplicationError::Zk(format!("watch fallback: {e}")))?;
                self.known_readonly_bookies = HashSet::new();
                Ok(watcher)
            }
            Err(e) => Err(ReplicationError::Zk(format!("watch readonly bookies: {e}"))),
        }
    }

    async fn try_set_writable_watch(&self) -> Option<zk::OneshotWatcher> {
        match self.client.list_and_watch_children(self.paths.available()).await {
            Ok((_children, watcher)) => Some(watcher),
            Err(e) => {
                tracing::warn!("Auditor: failed to set writable watcher: {e}");
                None
            }
        }
    }

    async fn try_set_readonly_watch(&self) -> Option<zk::OneshotWatcher> {
        match self.client.list_and_watch_children(self.paths.available_readonly()).await {
            Ok((_children, watcher)) => Some(watcher),
            Err(_) => None, // readonly path may not exist
        }
    }

    /// Get all available bookies (writable, excluding "readonly" directory entry).
    async fn get_available_bookies(&self) -> Result<HashSet<String>, ReplicationError> {
        match self.client.get_children(self.paths.available()).await {
            Ok((children, _)) => Ok(children
                .into_iter()
                .filter(|c| c != "readonly")
                .collect()),
            Err(zk::Error::NoNode) => Ok(HashSet::new()),
            Err(e) => Err(ReplicationError::Zk(format!("list bookies: {e}"))),
        }
    }

    /// Get readonly bookies.
    async fn get_readonly_bookies(&self) -> Result<HashSet<String>, ReplicationError> {
        match self.client.get_children(self.paths.available_readonly()).await {
            Ok((children, _)) => Ok(children.into_iter().collect()),
            Err(zk::Error::NoNode) => Ok(HashSet::new()),
            Err(e) => Err(ReplicationError::Zk(format!("list readonly bookies: {e}"))),
        }
    }

    async fn check_bookies(&mut self) -> Result<(), ReplicationError> {
        let current_bookies = self.get_available_bookies().await?;
        let current_readonly = self.get_readonly_bookies().await?;

        // Combine writable + readonly for "all known bookies" (matching Java's getAvailableBookies)
        let mut all_current: HashSet<String> = current_bookies.clone();
        all_current.extend(current_readonly.iter().cloned());

        let mut all_known: HashSet<String> = self.known_bookies.clone();
        all_known.extend(self.known_readonly_bookies.iter().cloned());

        // Detect failed bookies (were known, no longer in either writable or readonly)
        let failed: Vec<String> = all_known
            .difference(&all_current)
            .cloned()
            .collect();

        // Detect new bookies
        let new_bookies: Vec<String> = all_current
            .difference(&all_known)
            .cloned()
            .collect();

        if !new_bookies.is_empty() {
            tracing::info!("Auditor: new bookies detected: {:?}", new_bookies);
        }

        // Probe each apparently-failed bookie. A bookie that disappeared
        // from ZK's list might just be re-registering after a transient
        // session expiry — treating it as failed would unnecessarily
        // mark thousands of ledgers under-replicated. Only declare it
        // failed if we can't establish a TCP connection to it.
        let failed: Vec<String> = if failed.is_empty() {
            failed
        } else {
            let probe_futs = failed.iter().map(|b| {
                let bookie = b.clone();
                async move {
                    let alive = match bookie.parse::<std::net::SocketAddr>() {
                        Ok(sa) => tokio::time::timeout(
                            std::time::Duration::from_secs(2),
                            tokio::net::TcpStream::connect(sa),
                        ).await.map(|r| r.is_ok()).unwrap_or(false),
                        Err(_) => match tokio::net::lookup_host(bookie.as_str()).await {
                            Ok(mut iter) => match iter.next() {
                                Some(sa) => tokio::time::timeout(
                                    std::time::Duration::from_secs(2),
                                    tokio::net::TcpStream::connect(sa),
                                ).await.map(|r| r.is_ok()).unwrap_or(false),
                                None => false,
                            },
                            Err(_) => false,
                        },
                    };
                    (bookie, alive)
                }
            });
            futures::future::join_all(probe_futs)
                .await
                .into_iter()
                .filter_map(|(b, alive)| if alive {
                    tracing::info!("Auditor: bookie {b} disappeared from ZK but is reachable; skipping audit");
                    None
                } else {
                    Some(b)
                })
                .collect()
        };

        if !failed.is_empty() {
            // Check if another audit is already running (deduplication)
            if self.auditing.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                tracing::info!("Auditor: audit already in progress, skipping");
            } else {
                tracing::info!("Auditor: bookie failures detected: {:?}", failed);

                // Read delay from ZK (supports dynamic update).
                // Falls back to constructor default if not set in ZK.
                // Matches Java's ZkLedgerUnderreplicationManager.getLostBookieRecoveryDelay().
                let delay_secs = self.ur_manager
                    .get_lost_bookie_recovery_delay(self.lost_bookie_recovery_delay_secs)
                    .await
                    .unwrap_or(self.lost_bookie_recovery_delay_secs);

                if failed.len() > 1 || delay_secs == 0 {
                    // Multiple failures or no delay: immediate audit
                    let result = self.audit_failed_bookies(&failed).await;
                    self.auditing.store(false, Ordering::SeqCst);
                    if let Err(e) = result {
                        // Don't update known bookies on audit error — retry on next check
                        tracing::warn!("Auditor: audit failed: {e}");
                        return Ok(());
                    }
                } else {
                    // Single failure: delayed audit (matching Java's lostBookieRecoveryDelay)
                    tracing::info!(
                        "Auditor: scheduling delayed check in {delay_secs}s"
                    );
                    let delay = delay_secs;
                    let failed_clone = failed.clone();
                    let ur_manager = self.ur_manager.clone();
                    let ledger_manager = self.ledger_manager.clone();
                    let client = self.client.clone();
                    let paths = self.paths.clone();
                    let auditing = self.auditing.clone();

                    tokio::spawn(async move {
                        tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
                        // Re-check if bookie is still missing
                        let mut current = match client.get_children(paths.available()).await {
                            Ok((c, _)) => c
                                .into_iter()
                                .filter(|c| c != "readonly")
                                .collect::<HashSet<_>>(),
                            Err(_) => {
                                auditing.store(false, Ordering::SeqCst);
                                return;
                            }
                        };
                        // Also check readonly
                        if let Ok((ro, _)) = client.get_children(paths.available_readonly()).await {
                            current.extend(ro);
                        }

                        let still_failed: Vec<String> = failed_clone
                            .into_iter()
                            .filter(|b| !current.contains(b))
                            .collect();

                        if !still_failed.is_empty() {
                            tracing::info!(
                                "Auditor: bookies still missing after delay: {:?}",
                                still_failed
                            );
                            if let Err(e) = handle_lost_bookies(
                                &ur_manager,
                                &*ledger_manager,
                                &still_failed,
                            )
                            .await
                            {
                                tracing::warn!("Auditor: delayed audit error: {e}");
                            }
                        }
                        auditing.store(false, Ordering::SeqCst);
                    });
                }
            }
        }

        self.known_bookies = current_bookies;
        self.known_readonly_bookies = current_readonly;
        Ok(())
    }

    /// Scan all ledgers and mark those with failed bookies in their ensemble.
    async fn audit_failed_bookies(
        &self,
        failed_bookies: &[String],
    ) -> Result<(), ReplicationError> {
        handle_lost_bookies(&self.ur_manager, &*self.ledger_manager, failed_bookies).await
    }

    /// Check ALL ledgers: verify that every ensemble member is an available bookie.
    /// Marks ledgers as underreplicated if any ensemble bookie is unavailable.
    /// Matches Java's `AuditorCheckAllLedgersTask`.
    pub async fn check_all_ledgers(&self) -> Result<usize, ReplicationError> {
        let (availability_marked, _) = self.audit_all_ledgers(true, false).await?;
        Ok(availability_marked)
    }

    /// Check placement policy: verify that every ledger's ensemble meets the
    /// configured placement policy. Matches Java's `AuditorPlacementPolicyCheckTask`.
    ///
    /// Requires `set_placement_policy()` to have been called. If no policy is set,
    /// this is a no-op (returns 0).
    pub async fn check_placement_policy(&self) -> Result<usize, ReplicationError> {
        if self.placement_policy.is_none() {
            tracing::info!("check_placement_policy: no placement policy configured, skipping");
            return Ok(0);
        }
        let (_, placement_marked) = self.audit_all_ledgers(false, true).await?;
        Ok(placement_marked)
    }

    /// Run both availability and placement checks in a single ledger scan.
    /// Returns `(availability_marked, placement_marked)`.
    ///
    /// When both checks are enabled, this halves the ZK metadata reads compared
    /// to calling `check_all_ledgers` and `check_placement_policy` separately.
    pub async fn audit_all_ledgers(
        &self,
        check_availability: bool,
        check_placement: bool,
    ) -> Result<(usize, usize), ReplicationError> {
        let available: Arc<HashSet<String>> = Arc::new(if check_availability {
            let mut all = self.known_bookies.clone();
            all.extend(self.known_readonly_bookies.iter().cloned());
            all
        } else {
            HashSet::new()
        });

        let ledger_ids = self.ledger_manager.get_ledger_ranges().await
            .map_err(|e| ReplicationError::Failed(format!("enumerate ledgers: {e}")))?;

        // Process ledgers concurrently with bounded parallelism.
        // Matches Java's auditorMaxNumberOfConcurrentOpenLedgerOperations
        // semaphore-bounded concurrent processing.
        let lm = &self.ledger_manager;
        let ur = &self.ur_manager;
        let placement = &self.placement_policy;

        let results: (usize, usize) = stream::iter(ledger_ids)
            .map(|ledger_id| {
                let available = available.clone();
                async move {
                let md = match lm.read_ledger_metadata(ledger_id).await {
                    Ok(v) => v,
                    Err(_) => return (false, false),
                };
                let metadata = md.value();
                let mut avail_hit = false;
                let mut place_hit = false;

                // Availability check
                if check_availability {
                    let mut missing = Vec::new();
                    for (_first_entry, ensemble) in metadata.all_ensembles() {
                        for bookie in ensemble {
                            if !available.contains(bookie) {
                                missing.push(bookie.clone());
                            }
                        }
                    }
                    if !missing.is_empty() {
                        let missing_dedup: Vec<String> = {
                            let set: HashSet<String> = missing.into_iter().collect();
                            set.into_iter().collect()
                        };
                        if let Err(e) = ur.mark_ledger_underreplicated(
                            ledger_id, &missing_dedup,
                        ).await {
                            tracing::warn!("audit: failed to mark ledger {ledger_id} (availability): {e}");
                        } else {
                            avail_hit = true;
                        }
                    }
                }

                // Placement policy check
                if check_placement {
                    if let Some(ref policy) = placement {
                        let wq = metadata.write_quorum_size() as usize;
                        let aq = metadata.ack_quorum_size() as usize;
                        for (_first_entry, ensemble) in metadata.all_ensembles() {
                            let adherence = policy.is_ensemble_adhering(ensemble, wq, aq);
                            if adherence == bookkeeper_client::placement::PolicyAdherence::Fail {
                                if let Err(e) = ur.mark_ledger_underreplicated(
                                    ledger_id, &["placement_policy_violation".to_string()],
                                ).await {
                                    tracing::warn!("audit: failed to mark ledger {ledger_id} (placement): {e}");
                                } else {
                                    place_hit = true;
                                }
                                break;
                            }
                        }
                    }
                }

                (avail_hit, place_hit)
            }})
            .buffer_unordered(AUDIT_CONCURRENCY)
            .fold((0usize, 0usize), |mut acc, (a, p)| async move {
                if a { acc.0 += 1; }
                if p { acc.1 += 1; }
                acc
            })
            .await;

        let (availability_marked, placement_marked) = results;

        if check_availability {
            tracing::info!("audit: availability check marked {availability_marked} ledger(s) as underreplicated");
        }
        if check_placement {
            tracing::info!("audit: placement check marked {placement_marked} ledger(s) as underreplicated");
        }
        Ok((availability_marked, placement_marked))
    }

    /// Verify that every entry of every ledger has at least `write_quorum`
    /// replicas actually present on the bookies. Mirrors Java's
    /// `AuditorReplicasCheckTask.replicasCheck()` which, unlike
    /// `check_all_ledgers`, reads from each bookie to detect entries that
    /// are present on the ensemble at metadata level but missing from the
    /// actual storage — the only way to catch silent partial data loss.
    ///
    /// Returns the number of ledgers newly marked as under-replicated.
    /// Skips ledgers that already have an under-replication marker to
    /// avoid redundant work — matches Java's
    /// `checkUnderReplicationForReplicasCheck` short-circuit.
    pub async fn check_replicas(&self) -> Result<usize, ReplicationError> {
        // Pre-compute the set of ledger ids that are already flagged so
        // we don't re-scan them.
        let already_marked: HashSet<i64> = self
            .ur_manager
            .list_ledgers_to_rereplicate(|_, _| true)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|(lid, _)| lid)
            .collect();

        let ledger_ids = self
            .ledger_manager
            .get_ledger_ranges()
            .await
            .map_err(|e| ReplicationError::Failed(format!("enumerate ledgers: {e}")))?;

        let lm = &self.ledger_manager;
        let ur = &self.ur_manager;

        let marked = stream::iter(ledger_ids.into_iter().filter(|lid| !already_marked.contains(lid)))
            .map(|ledger_id| async move {
                // Read metadata. On error we just skip — the availability
                // task will surface missing-metadata cases.
                let md = match lm.read_ledger_metadata(ledger_id).await {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                let metadata = md.value();

                // Empty ledgers have no entries to check.
                let last_entry_id = metadata.last_entry_id().unwrap_or(-1);
                if last_entry_id < 0 {
                    return false;
                }

                let ensemble_size = metadata.ensemble_size() as usize;
                let write_quorum = metadata.write_quorum_size() as usize;
                let ack_quorum = metadata.ack_quorum_size() as usize;
                let schedule = RoundRobinDistributionSchedule::new(
                    ensemble_size,
                    write_quorum,
                    ack_quorum,
                );
                let password = metadata.password().unwrap_or(b"");
                let master_key: [u8; MASTER_KEY_LENGTH] = generate_master_key(password);

                // Build per-segment bounds, same shape as the worker uses.
                let ensembles = metadata.all_ensembles();
                let keys: Vec<i64> = ensembles.keys().copied().collect();
                let segments: Vec<(i64, i64, Vec<String>)> = keys.iter().enumerate().map(|(i, &start)| {
                    let end = if i + 1 < keys.len() { keys[i + 1] - 1 } else { last_entry_id };
                    (start, end, ensembles[&start].clone())
                }).collect();

                // For each segment walk entries and count replicas. If any
                // entry has fewer than `write_quorum` replicas confirmed,
                // mark the ledger. Within each entry the per-bookie reads
                // run in parallel via `join_all` (previously serial, which
                // cost `ensemble_size` round-trips per entry).
                let mut under_replicated = false;
                let mut reason: Vec<String> = Vec::new();
                'segment_loop: for (seg_first, seg_last, ensemble) in &segments {
                    // Open a connection to every bookie in the ensemble
                    // exactly once, in parallel.
                    let conn_futs = ensemble.iter().map(|addr| async move {
                        // SocketAddr::parse rejects "host:port" forms, so a
                        // hostname-based bookie id used to silently produce
                        // None here and the audit became a no-op for the
                        // entire ensemble. Fall back to tokio::net's
                        // hostname resolver when direct parsing fails.
                        let sock: Option<SocketAddr> = match addr.parse::<SocketAddr>() {
                            Ok(sa) => Some(sa),
                            Err(_) => tokio::net::lookup_host(addr.as_str())
                                .await
                                .ok()
                                .and_then(|mut iter| iter.next()),
                        };
                        match sock {
                            Some(sa) => BookieConnection::connect(sa).await.ok().map(Arc::new),
                            None => None,
                        }
                    });
                    let conns: Vec<Option<Arc<BookieConnection>>> =
                        futures::future::join_all(conn_futs).await;

                    for eid in *seg_first..=*seg_last {
                        let write_set = schedule.write_set(eid);
                        let read_futs = write_set.iter().map(|&idx| {
                            let conn = conns.get(idx).and_then(|c| c.as_ref()).cloned();
                            async move {
                                match conn {
                                    Some(c) => matches!(
                                        c.send_read_entry(0, ledger_id, eid, Some(master_key)).await,
                                        Ok(V2Response::Read { error_code, .. }) if error_code == V2_EOK
                                    ),
                                    None => false,
                                }
                            }
                        });
                        let replicas_present = futures::future::join_all(read_futs)
                            .await
                            .into_iter()
                            .filter(|ok| *ok)
                            .count();
                        if replicas_present < write_quorum {
                            under_replicated = true;
                            reason.push(format!("replicas_check_missing_eid_{eid}"));
                            break 'segment_loop;
                        }
                    }
                }

                if under_replicated {
                    if let Err(e) = ur.mark_ledger_underreplicated(ledger_id, &reason).await {
                        tracing::warn!("replicasCheck: failed to mark ledger {ledger_id}: {e}");
                        return false;
                    }
                    true
                } else {
                    false
                }
            })
            .buffer_unordered(AUDIT_CONCURRENCY)
            .fold(0usize, |acc, hit| async move { acc + (hit as usize) })
            .await;

        tracing::info!("replicasCheck: marked {marked} ledger(s) as underreplicated");
        Ok(marked)
    }
}

use bookkeeper_common::current_time_millis;

/// Compute how long to wait before the next periodic audit run, given a
/// previously persisted completion timestamp and the configured interval.
///
/// * If the ZK znode never existed (`last_run_ms < 0`), fire immediately.
/// * If enough time has already elapsed, fire immediately.
/// * Otherwise return the remaining delay.
///
/// Mirrors Java's `AbstractAuditorTask.getScheduledDelay()` which uses
/// `checkAllLedgersCTime` / `placementPolicyCheckCTime` to avoid re-running
/// expensive scans immediately after an auditor restart.
fn remaining_before_next_run(last_run_ms: i64, interval_secs: u64) -> tokio::time::Duration {
    if last_run_ms < 0 {
        return tokio::time::Duration::from_millis(0);
    }
    let now_ms = current_time_millis();
    let elapsed_ms = (now_ms - last_run_ms).max(0) as u64;
    let interval_ms = interval_secs.saturating_mul(1000);
    tokio::time::Duration::from_millis(interval_ms.saturating_sub(elapsed_ms))
}

/// Build bookie→ledger index and mark affected ledgers as underreplicated.
/// Matches Java's BookieLedgerIndexer.getBookieToLedgerIndex() + handleLostBookies().
async fn handle_lost_bookies(
    ur_manager: &ZkUnderreplicationManager,
    ledger_manager: &dyn LedgerManager,
    failed_bookies: &[String],
) -> Result<(), ReplicationError> {
    let ledger_ids = ledger_manager
        .get_ledger_ranges()
        .await
        .map_err(|e| ReplicationError::Failed(format!("get ledger ranges: {e}")))?;

    tracing::info!(
        "Auditor: scanning {} ledgers for failed bookies {:?}",
        ledger_ids.len(),
        failed_bookies
    );

    let bookie_to_ledgers: HashMap<String, Vec<i64>> = {
        // Read ledger metadata concurrently with bounded parallelism.
        // Use fold to build the index without materializing all results into a Vec.
        stream::iter(ledger_ids.iter().copied())
            .map(|lid| async move {
                match ledger_manager.read_ledger_metadata(lid).await {
                    Ok(versioned) => {
                        let md = versioned.value();
                        let mut pairs = Vec::new();
                        for (_first_entry, ensemble) in md.all_ensembles() {
                            for bookie in ensemble {
                                pairs.push((bookie.to_string(), lid));
                            }
                        }
                        pairs
                    }
                    Err(_) => Vec::new(),
                }
            })
            .buffer_unordered(AUDIT_CONCURRENCY)
            .fold(HashMap::new(), |mut acc, pairs| async move {
                for (bookie, lid) in pairs {
                    acc.entry(bookie).or_insert_with(Vec::new).push(lid);
                }
                acc
            })
            .await
    };

    let mut marked_count = 0;
    for bookie in failed_bookies {
        if let Some(ledgers) = bookie_to_ledgers.get(bookie) {
            for lid in ledgers {
                if let Err(e) = ur_manager
                    .mark_ledger_underreplicated(*lid, &[bookie.clone()])
                    .await
                {
                    tracing::warn!("Auditor: failed to mark ledger {lid}: {e}");
                } else {
                    marked_count += 1;
                }
            }
        }
    }

    tracing::info!(
        "Auditor: marked {} ledgers as underreplicated for {} failed bookies",
        marked_count,
        failed_bookies.len()
    );

    Ok(())
}
