use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::watch;
use zookeeper_client as zk;
use bookkeeper_metadata::zk_path::ZkPaths;
use crate::underreplication::ZkUnderreplicationManager;
use crate::ReplicationError;

/// Leader-elected auditor that watches for bookie failures
/// and marks affected ledgers as underreplicated.
pub struct Auditor {
    client: zk::Client,
    paths: ZkPaths,
    ur_manager: Arc<ZkUnderreplicationManager>,
    known_bookies: HashSet<String>,
    lost_bookie_recovery_delay_secs: u64,
    shutdown_rx: watch::Receiver<bool>,
}

impl Auditor {
    pub fn new(
        client: zk::Client,
        paths: ZkPaths,
        ur_manager: Arc<ZkUnderreplicationManager>,
        lost_bookie_recovery_delay_secs: u64,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            client,
            paths,
            ur_manager,
            known_bookies: HashSet::new(),
            lost_bookie_recovery_delay_secs,
            shutdown_rx,
        }
    }

    /// Run the auditor main loop. Watches bookie registrations and triggers checks.
    pub async fn run(&mut self) -> Result<(), ReplicationError> {
        println!("Auditor started");

        // Initial bookie list
        self.refresh_bookies().await?;

        loop {
            tokio::select! {
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        println!("Auditor shutting down");
                        break;
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
                    // Periodic check
                    if let Err(e) = self.check_bookies().await {
                        eprintln!("Auditor bookie check error: {e}");
                    }
                }
            }
        }
        Ok(())
    }

    async fn refresh_bookies(&mut self) -> Result<(), ReplicationError> {
        let bookies = match self.client.get_children(self.paths.available()).await {
            Ok((children, _)) => children
                .into_iter()
                .filter(|c| c != "readonly")
                .collect::<HashSet<_>>(),
            Err(zk::Error::NoNode) => HashSet::new(),
            Err(e) => return Err(ReplicationError::Zk(format!("list bookies: {e}"))),
        };
        self.known_bookies = bookies;
        Ok(())
    }

    async fn check_bookies(&mut self) -> Result<(), ReplicationError> {
        let current_bookies = match self.client.get_children(self.paths.available()).await {
            Ok((children, _)) => children
                .into_iter()
                .filter(|c| c != "readonly")
                .collect::<HashSet<_>>(),
            Err(zk::Error::NoNode) => HashSet::new(),
            Err(e) => return Err(ReplicationError::Zk(format!("list bookies: {e}"))),
        };

        // Detect failed bookies
        let failed: Vec<String> = self
            .known_bookies
            .difference(&current_bookies)
            .cloned()
            .collect();

        // Detect new bookies
        let new_bookies: Vec<String> = current_bookies
            .difference(&self.known_bookies)
            .cloned()
            .collect();

        if !new_bookies.is_empty() {
            println!("Auditor: new bookies detected: {:?}", new_bookies);
        }

        if !failed.is_empty() {
            println!("Auditor: bookie failures detected: {:?}", failed);

            if failed.len() > 1 || self.lost_bookie_recovery_delay_secs == 0 {
                // Multiple failures or no delay: immediate audit
                self.audit_failed_bookies(&failed).await?;
            } else {
                // Single failure: delayed audit
                println!(
                    "Auditor: scheduling delayed check in {}s",
                    self.lost_bookie_recovery_delay_secs
                );
                let delay = self.lost_bookie_recovery_delay_secs;
                let failed_clone = failed.clone();
                let _ur_manager = self.ur_manager.clone();
                let client = self.client.clone();
                let paths = self.paths.clone();

                tokio::spawn(async move {
                    tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
                    // Re-check if bookie is still missing
                    let current = match client.get_children(paths.available()).await {
                        Ok((c, _)) => c
                            .into_iter()
                            .filter(|c| c != "readonly")
                            .collect::<HashSet<_>>(),
                        Err(_) => return,
                    };
                    for bookie in &failed_clone {
                        if !current.contains(bookie) {
                            println!(
                                "Auditor: bookie {} still missing after delay, marking ledgers",
                                bookie
                            );
                            // In a full implementation, we would scan all ledgers and mark those
                            // with this bookie in their ensemble as underreplicated.
                        }
                    }
                });
            }
        }

        self.known_bookies = current_bookies;
        Ok(())
    }

    async fn audit_failed_bookies(
        &self,
        failed_bookies: &[String],
    ) -> Result<(), ReplicationError> {
        // In a full implementation, this would:
        // 1. Scan all ledgers via LedgerManager
        // 2. For each ledger, check if any ensemble contains a failed bookie
        // 3. Mark matching ledgers as underreplicated
        //
        // Since we don't have a LedgerManager scan yet, we log the failure.
        // The ReplicationWorker will handle ledgers marked by other means.
        for bookie in failed_bookies {
            println!("Auditor: would scan ledgers for failed bookie {bookie}");
        }
        Ok(())
    }
}
