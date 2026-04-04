use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;
use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_client::recovery::RecoveryHandle;
use bookkeeper_metadata::traits::RegistrationManager;
use bookkeeper_protocol::constants::MASTER_KEY_LENGTH;
use bookkeeper_protocol::digest::{DigestManager, DigestType, generate_master_key};
use crate::underreplication::ZkUnderreplicationManager;
use crate::ReplicationError;

/// Background worker that claims underreplicated ledgers and repairs them.
pub struct ReplicationWorker {
    ur_manager: Arc<ZkUnderreplicationManager>,
    reg_manager: Arc<dyn RegistrationManager>,
    bookie_id: String,
    backoff_ms: u64,
    shutdown_rx: watch::Receiver<bool>,
}

impl ReplicationWorker {
    pub fn new(
        ur_manager: Arc<ZkUnderreplicationManager>,
        reg_manager: Arc<dyn RegistrationManager>,
        bookie_id: String,
        backoff_ms: u64,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            ur_manager,
            reg_manager,
            bookie_id,
            backoff_ms,
            shutdown_rx,
        }
    }

    /// Run the replication worker main loop.
    pub async fn run(&mut self) -> Result<(), ReplicationError> {
        println!("ReplicationWorker started (bookie: {})", self.bookie_id);

        loop {
            // Check shutdown before each iteration
            if *self.shutdown_rx.borrow() {
                println!("ReplicationWorker shutting down");
                break;
            }

            let result = self.try_rereplicate().await;

            match result {
                Ok(true) => {} // replicated one ledger, loop immediately
                Ok(false) => {
                    // No work available, wait before polling again or until shutdown
                    tokio::select! {
                        _ = self.shutdown_rx.changed() => {
                            if *self.shutdown_rx.borrow() {
                                println!("ReplicationWorker shutting down");
                                break;
                            }
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_millis(self.backoff_ms)) => {}
                    }
                }
                Err(e) => {
                    eprintln!("ReplicationWorker error: {e}");
                    tokio::select! {
                        _ = self.shutdown_rx.changed() => {
                            if *self.shutdown_rx.borrow() {
                                println!("ReplicationWorker shutting down");
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
        // Check if replication is enabled
        if !self.ur_manager.is_replication_enabled().await? {
            return Ok(false);
        }

        // Try to claim a ledger
        let ledger_id = match self.ur_manager.get_ledger_to_rereplicate().await? {
            Some(lid) => lid,
            None => return Ok(false),
        };

        println!("ReplicationWorker: claimed ledger {ledger_id} for replication");

        match self.rereplicate(ledger_id).await {
            Ok(()) => {
                self.ur_manager.mark_ledger_replicated(ledger_id).await?;
                println!("ReplicationWorker: ledger {ledger_id} replicated successfully");
                Ok(true)
            }
            Err(e) => {
                eprintln!("ReplicationWorker: failed to replicate ledger {ledger_id}: {e}");
                self.ur_manager.release_lock(ledger_id).await?;
                Ok(false)
            }
        }
    }

    async fn rereplicate(&self, ledger_id: i64) -> Result<(), ReplicationError> {
        // Get available bookies for replacement selection
        let available = self
            .reg_manager
            .get_writable_bookies()
            .await
            .map_err(|e| ReplicationError::Failed(format!("get bookies: {e}")))?;

        if available.is_empty() {
            return Err(ReplicationError::Failed("no available bookies".into()));
        }

        // Connect to the first available bookie to read entries
        let addr: SocketAddr = available[0]
            .parse()
            .map_err(|e| ReplicationError::Failed(format!("parse addr: {e}")))?;

        let conn = Arc::new(BookieConnection::connect(addr).await?);
        let conns = vec![conn];

        // Use recovery to fence (if needed) and read entries
        let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
        let dm = DigestManager::new(DigestType::Crc32c);
        let master_key: [u8; MASTER_KEY_LENGTH] = generate_master_key(b"");

        let mut recovery = RecoveryHandle::new(
            ledger_id,
            available[..1].to_vec(),
            schedule,
            dm,
            master_key,
        );

        // Run recovery protocol: fence -> forward read -> re-replicate -> close
        let (_last_entry, _length) = recovery.recover(&conns, 0).await?;

        println!("ReplicationWorker: ledger {ledger_id} recovery complete");
        Ok(())
    }
}
