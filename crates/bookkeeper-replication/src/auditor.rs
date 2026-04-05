use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::watch;
use zookeeper_client as zk;
use bookkeeper_metadata::zk_path::ZkPaths;
use bookkeeper_metadata::traits::LedgerManager;
use crate::underreplication::ZkUnderreplicationManager;
use crate::ReplicationError;

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
    /// Prevents concurrent audit_failed_bookies() calls.
    /// Matches Java's single-threaded executor serialization.
    auditing: Arc<AtomicBool>,
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
        }
    }

    /// Run the auditor main loop.
    ///
    /// Uses ZK watchers for real-time bookie change detection matching Java's
    /// watchBookieChanges() which watches BOTH writable and readonly bookies.
    /// Periodic 60s fallback as safety net.
    pub async fn run(&mut self) -> Result<(), ReplicationError> {
        println!("Auditor started");

        // Initial bookie list with watchers for both paths
        let writable_watcher = self.refresh_writable_with_watch().await?;
        let readonly_watcher = self.refresh_readonly_with_watch().await?;

        let mut cur_writable_watcher: Option<zk::OneshotWatcher> = Some(writable_watcher);
        let mut cur_readonly_watcher: Option<zk::OneshotWatcher> = Some(readonly_watcher);

        // Run initial audit immediately after election (matching Java's scheduleBookieCheckTask)
        if let Err(e) = self.check_bookies().await {
            eprintln!("Auditor initial check error: {e}");
        }

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

            tokio::select! {
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        println!("Auditor shutting down");
                        break;
                    }
                }
                event = writable_fut => {
                    // Writable bookie list changed or ZK session event
                    self.handle_watcher_event(event).await;
                    // Re-set writable watcher
                    cur_writable_watcher = self.try_set_writable_watch().await;
                }
                event = readonly_fut => {
                    // Readonly bookie list changed or ZK session event
                    self.handle_watcher_event(event).await;
                    // Re-set readonly watcher
                    cur_readonly_watcher = self.try_set_readonly_watch().await;
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(60)) => {
                    // Periodic fallback safety net
                    if let Err(e) = self.check_bookies().await {
                        eprintln!("Auditor periodic check error: {e}");
                    }
                    // Re-set watchers if lost
                    if cur_writable_watcher.is_none() {
                        cur_writable_watcher = self.try_set_writable_watch().await;
                    }
                    if cur_readonly_watcher.is_none() {
                        cur_readonly_watcher = self.try_set_readonly_watch().await;
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
                        eprintln!("Auditor bookie check error: {e}");
                    }
                }
                zk::EventType::Session => {
                    // ZK session reconnected — re-audit in case bookies changed
                    // while disconnected (matching Java's implicit re-audit on reconnect)
                    println!("Auditor: ZK session event, triggering re-audit");
                    if let Err(e) = self.check_bookies().await {
                        eprintln!("Auditor session re-audit error: {e}");
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
                eprintln!("Auditor: failed to set writable watcher: {e}");
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
            println!("Auditor: new bookies detected: {:?}", new_bookies);
        }

        if !failed.is_empty() {
            // Check if another audit is already running (deduplication)
            if self.auditing.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                println!("Auditor: audit already in progress, skipping");
            } else {
                println!("Auditor: bookie failures detected: {:?}", failed);

                if failed.len() > 1 || self.lost_bookie_recovery_delay_secs == 0 {
                    // Multiple failures or no delay: immediate audit
                    let result = self.audit_failed_bookies(&failed).await;
                    self.auditing.store(false, Ordering::SeqCst);
                    if let Err(e) = result {
                        // Don't update known bookies on audit error — retry on next check
                        eprintln!("Auditor: audit failed: {e}");
                        return Ok(());
                    }
                } else {
                    // Single failure: delayed audit (matching Java's lostBookieRecoveryDelay)
                    println!(
                        "Auditor: scheduling delayed check in {}s",
                        self.lost_bookie_recovery_delay_secs
                    );
                    let delay = self.lost_bookie_recovery_delay_secs;
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
                            println!(
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
                                eprintln!("Auditor: delayed audit error: {e}");
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

    println!(
        "Auditor: scanning {} ledgers for failed bookies {:?}",
        ledger_ids.len(),
        failed_bookies
    );

    let mut bookie_to_ledgers: HashMap<String, Vec<i64>> = HashMap::new();

    for lid in &ledger_ids {
        match ledger_manager.read_ledger_metadata(*lid).await {
            Ok(versioned) => {
                let md = versioned.value();
                for (_first_entry, ensemble) in md.all_ensembles() {
                    for bookie in ensemble {
                        bookie_to_ledgers
                            .entry(bookie.to_string())
                            .or_default()
                            .push(*lid);
                    }
                }
            }
            Err(bookkeeper_metadata::ledger_metadata::MetadataError::NotFound(_)) => {
                continue;
            }
            Err(e) => {
                eprintln!("Auditor: failed to read metadata for ledger {lid}: {e}");
                continue;
            }
        }
    }

    let mut marked_count = 0;
    for bookie in failed_bookies {
        if let Some(ledgers) = bookie_to_ledgers.get(bookie) {
            for lid in ledgers {
                if let Err(e) = ur_manager
                    .mark_ledger_underreplicated(*lid, &[bookie.clone()])
                    .await
                {
                    eprintln!("Auditor: failed to mark ledger {lid}: {e}");
                } else {
                    marked_count += 1;
                }
            }
        }
    }

    println!(
        "Auditor: marked {} ledgers as underreplicated for {} failed bookies",
        marked_count,
        failed_bookies.len()
    );

    Ok(())
}
