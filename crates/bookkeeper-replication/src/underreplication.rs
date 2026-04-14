use std::collections::HashSet;
use zookeeper_client as zk;
use bookkeeper_metadata::zk_path::{ZkPaths, underreplication_ledger_path, underreplication_lock_path};
use bookkeeper_proto::{UnderreplicatedLedgerFormat, LockDataFormat, LedgerRereplicationLayoutFormat};
use crate::text_format;

/// Layout type for the underreplication subsystem, matching Java's constant.
const LAYOUT_TYPE: &str = "BASIC";
const LAYOUT_VERSION: i32 = 1;

/// Maximum CAS retry attempts before giving up.
const MAX_CAS_RETRIES: usize = 20;

/// Manages underreplicated ledger tracking in ZooKeeper.
/// Compatible with Java BookKeeper's ZkLedgerUnderreplicationManager.
pub struct ZkUnderreplicationManager {
    client: zk::Client,
    paths: ZkPaths,
    bookie_id: String,
    /// Pre-computed lock data (protobuf text format with bookieId).
    lock_data: Vec<u8>,
}

impl ZkUnderreplicationManager {
    pub async fn new(
        client: zk::Client,
        paths: ZkPaths,
        bookie_id: String,
    ) -> Result<Self, ReplicationError> {
        let lock_data = text_format::serialize_lock_data(&LockDataFormat {
            bookie_id: Some(bookie_id.clone()),
        });

        let mgr = Self { client, paths, bookie_id, lock_data };

        // Initialize the underreplication tree and LAYOUT znode (matches Java).
        mgr.init_layout().await?;

        Ok(mgr)
    }

    /// Initialize the underreplication LAYOUT znode.
    /// Java creates: {underreplication}/LAYOUT with LedgerRereplicationLayoutFormat text.
    async fn init_layout(&self) -> Result<(), ReplicationError> {
        let ur_base = self.paths.underreplication();

        // Ensure base paths exist
        self.ensure_path(ur_base).await?;
        let ledgers_path = format!("{}/ledgers", ur_base);
        self.ensure_path(&ledgers_path).await?;
        let locks_path = format!("{}/locks", ur_base);
        self.ensure_path(&locks_path).await?;

        let layout_path = format!("{}/LAYOUT", ur_base);
        let layout_msg = LedgerRereplicationLayoutFormat {
            r#type: LAYOUT_TYPE.to_string(),
            version: LAYOUT_VERSION,
        };
        let layout_data = text_format::serialize_layout(&layout_msg);

        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        match self.client.create(&layout_path, &layout_data, &options).await {
            Ok(_) => {}
            Err(zk::Error::NodeExists) => {
                // Verify existing layout is compatible
                let (data, _) = self.client.get_data(&layout_path).await
                    .map_err(|e| ReplicationError::Zk(format!("read layout: {e}")))?;
                let existing = text_format::parse_layout(&data)
                    .map_err(|e| ReplicationError::Zk(format!("parse layout: {e}")))?;
                if existing.r#type != LAYOUT_TYPE || existing.version != LAYOUT_VERSION {
                    return Err(ReplicationError::Zk(format!(
                        "incompatible underreplication layout: type={}, version={}",
                        existing.r#type, existing.version
                    )));
                }
            }
            Err(e) => return Err(ReplicationError::Zk(format!("create layout: {e}"))),
        }

        Ok(())
    }

    /// Mark a ledger as underreplicated with the list of missing bookie addresses.
    /// Uses CAS retry logic matching Java's handleLedgerUnderreplicatedAlreadyMarked.
    pub async fn mark_ledger_underreplicated(
        &self,
        ledger_id: i64,
        missing_replicas: &[String],
    ) -> Result<(), ReplicationError> {
        let ur_path = underreplication_ledger_path(
            &format!("{}/ledgers", self.paths.underreplication()),
            ledger_id,
        );

        let proto = UnderreplicatedLedgerFormat {
            replica: missing_replicas.to_vec(),
            ctime: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            ),
        };
        let data = text_format::serialize_underreplicated_ledger(&proto);

        // Create parent paths if needed
        self.ensure_parents(&ur_path).await?;

        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        match self.client.create(&ur_path, &data, &options).await {
            Ok(_) => Ok(()),
            Err(zk::Error::NodeExists) => {
                // Node already exists — merge replicas with CAS retry
                self.merge_underreplicated(&ur_path, missing_replicas).await
            }
            Err(e) => Err(ReplicationError::Zk(format!("mark underrep: {e}"))),
        }
    }

    /// Merge new missing replicas into an existing underreplicated marker.
    /// Uses version-checked (CAS) updates with retry on BadVersion, matching Java.
    async fn merge_underreplicated(
        &self,
        znode_path: &str,
        new_replicas: &[String],
    ) -> Result<(), ReplicationError> {
        for _ in 0..MAX_CAS_RETRIES {
            // Read current data with version
            let (existing_data, stat) = match self.client.get_data(znode_path).await {
                Ok(result) => result,
                Err(zk::Error::NoNode) => {
                    // Node was deleted between our check and read — retry creation
                    let proto = UnderreplicatedLedgerFormat {
                        replica: new_replicas.to_vec(),
                        ctime: Some(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as i64,
                        ),
                    };
                    let data = text_format::serialize_underreplicated_ledger(&proto);
                    let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
                    match self.client.create(znode_path, &data, &options).await {
                        Ok(_) => return Ok(()),
                        Err(zk::Error::NodeExists) => continue, // retry merge
                        Err(e) => return Err(ReplicationError::Zk(format!("recreate: {e}"))),
                    }
                }
                Err(e) => return Err(ReplicationError::Zk(format!("read underrep: {e}"))),
            };

            // Parse existing and merge replicas
            let mut existing = text_format::parse_underreplicated_ledger(&existing_data)
                .map_err(|e| ReplicationError::Zk(format!("parse underrep: {e}")))?;

            let existing_set: HashSet<String> = existing.replica.iter().cloned().collect();
            for r in new_replicas {
                if !existing_set.contains(r) {
                    existing.replica.push(r.clone());
                }
            }
            // Update ctime
            existing.ctime = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            );

            let merged_data = text_format::serialize_underreplicated_ledger(&existing);

            // CAS update with version
            match self.client.set_data(znode_path, &merged_data, Some(stat.version)).await {
                Ok(_) => return Ok(()),
                Err(zk::Error::BadVersion) => continue, // retry
                Err(zk::Error::NoNode) => continue, // node deleted, retry
                Err(e) => return Err(ReplicationError::Zk(format!("update underrep: {e}"))),
            }
        }

        Err(ReplicationError::Zk(format!(
            "failed to mark underreplicated after {} retries", MAX_CAS_RETRIES
        )))
    }

    /// Try to claim a ledger for replication. Returns Some(ledger_id) if lock acquired.
    pub async fn get_ledger_to_rereplicate(&self) -> Result<Option<i64>, ReplicationError> {
        let ur_ledgers_path = format!("{}/ledgers", self.paths.underreplication());

        // Scan for underreplicated ledgers by listing children recursively.
        let children = match self.client.get_children(&ur_ledgers_path).await {
            Ok((c, _)) => c,
            Err(zk::Error::NoNode) => return Ok(None),
            Err(e) => return Err(ReplicationError::Zk(format!("list underrep: {e}"))),
        };

        // Recursively find urL* znodes
        for dir1 in &children {
            if let Ok(Some(lid)) = self
                .find_underreplicated_in_tree(&format!("{}/{}", ur_ledgers_path, dir1))
                .await
            {
                // Try to acquire ephemeral lock
                let lock_path = underreplication_lock_path(
                    &format!("{}/locks", self.paths.underreplication()),
                    lid,
                );
                self.ensure_parents(&lock_path).await?;
                let options = zk::CreateMode::Ephemeral.with_acls(zk::Acls::anyone_all());
                match self.client.create(&lock_path, &self.lock_data, &options).await {
                    Ok(_) => return Ok(Some(lid)),
                    Err(zk::Error::NodeExists) => continue, // already locked
                    Err(e) => {
                        return Err(ReplicationError::Zk(format!("lock: {e}")))
                    }
                }
            }
        }
        Ok(None)
    }

    /// Mark a ledger as replicated (done). Deletes the marker and lock.
    pub async fn mark_ledger_replicated(&self, ledger_id: i64) -> Result<(), ReplicationError> {
        let base = format!("{}/ledgers", self.paths.underreplication());
        let ur_path = underreplication_ledger_path(&base, ledger_id);
        let lock_path = underreplication_lock_path(
            &format!("{}/locks", self.paths.underreplication()),
            ledger_id,
        );

        // Delete marker — ignore BadVersion (another worker may have updated it)
        match self.client.delete(&ur_path, None).await {
            Ok(()) => {}
            Err(zk::Error::NoNode) => {} // already deleted
            Err(zk::Error::BadVersion) => {} // leave it, someone re-marked
            Err(e) => return Err(ReplicationError::Zk(format!("delete marker: {e}"))),
        }

        // Clean up empty parent directories in the hierarchy.
        // Matches Java's ZkLedgerUnderreplicationManager which walks up the
        // 4-level hex hierarchy and deletes empty parent znodes.
        self.cleanup_hierarchy(&ur_path, &base).await;

        // Release lock
        let _ = self.client.delete(&lock_path, None).await;
        Ok(())
    }

    /// Walk up the hierarchy from `znode_path` to `stop_at_prefix` and delete
    /// empty intermediate directories. Ignores errors since other workers may
    /// be concurrently creating/deleting in the same hierarchy.
    async fn cleanup_hierarchy(&self, znode_path: &str, stop_at_prefix: &str) {
        let mut path = znode_path.to_string();
        loop {
            // Move up to parent
            if let Some(slash_pos) = path.rfind('/') {
                path.truncate(slash_pos);
            } else {
                break;
            }
            // Don't go above the base directory
            if path.len() <= stop_at_prefix.len() {
                break;
            }
            // Try to delete — only succeeds if empty (ZK enforces this)
            match self.client.delete(&path, None).await {
                Ok(()) => {} // deleted empty dir, continue up
                Err(zk::Error::NoNode) => {} // already deleted by another worker
                Err(zk::Error::NotEmpty) => break, // has other children, stop
                Err(_) => break, // any other error, stop
            }
        }
    }

    /// Release lock without deleting marker (for retry).
    pub async fn release_lock(&self, ledger_id: i64) -> Result<(), ReplicationError> {
        let lock_path = underreplication_lock_path(
            &format!("{}/locks", self.paths.underreplication()),
            ledger_id,
        );
        let _ = self.client.delete(&lock_path, None).await;
        Ok(())
    }

    /// Check if replication is enabled (absence of disable znode).
    pub async fn is_replication_enabled(&self) -> Result<bool, ReplicationError> {
        let disable_path = format!("{}/disable", self.paths.underreplication());
        match self.client.check_stat(&disable_path).await {
            Ok(Some(_)) => Ok(false), // disable znode exists -> disabled
            Ok(None) => Ok(true),     // no disable znode -> enabled
            Err(e) => Err(ReplicationError::Zk(format!("check disable: {e}"))),
        }
    }

    /// Get the bookie ID of this manager instance.
    pub fn bookie_id(&self) -> &str {
        &self.bookie_id
    }

    /// Get the ZK client (for advanced use like bookie discovery).
    pub fn client(&self) -> &zk::Client {
        &self.client
    }

    /// Get the ZK paths.
    pub fn paths(&self) -> &ZkPaths {
        &self.paths
    }

    /// Get the lost bookie recovery delay from ZK (seconds).
    /// Returns the default if not set. Matches Java's
    /// `ZkLedgerUnderreplicationManager.getLostBookieRecoveryDelay()`.
    pub async fn get_lost_bookie_recovery_delay(&self, default_secs: u64) -> Result<u64, ReplicationError> {
        let delay_path = format!("{}/lostBookieRecoveryDelay", self.paths.underreplication());
        match self.client.get_data(&delay_path).await {
            Ok((data, _stat)) => {
                let s = String::from_utf8_lossy(&data);
                Ok(s.trim().parse::<u64>().unwrap_or(default_secs))
            }
            Err(zk::Error::NoNode) => Ok(default_secs),
            Err(e) => Err(ReplicationError::Zk(format!("read delay: {e}"))),
        }
    }

    /// Set the lost bookie recovery delay in ZK (seconds).
    /// Matches Java's `ZkLedgerUnderreplicationManager.setLostBookieRecoveryDelay()`.
    pub async fn set_lost_bookie_recovery_delay(&self, secs: u64) -> Result<(), ReplicationError> {
        let delay_path = format!("{}/lostBookieRecoveryDelay", self.paths.underreplication());
        let data = secs.to_string().into_bytes();
        match self.client.set_data(&delay_path, &data, None).await {
            Ok(_) => Ok(()),
            Err(zk::Error::NoNode) => {
                let opts = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
                self.client.create(&delay_path, &data, &opts).await
                    .map(|_| ())
                    .map_err(|e| ReplicationError::Zk(format!("create delay znode: {e}")))
            }
            Err(e) => Err(ReplicationError::Zk(format!("set delay: {e}"))),
        }
    }

    fn find_underreplicated_in_tree<'a>(
        &'a self,
        path: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<i64>, ReplicationError>> + Send + 'a>> {
        Box::pin(async move {
            let children = match self.client.get_children(path).await {
                Ok((c, _)) => c,
                Err(zk::Error::NoNode) => return Ok(None),
                Err(e) => return Err(ReplicationError::Zk(format!("list: {e}"))),
            };
            for child in &children {
                if let Some(suffix) = child.strip_prefix("urL") {
                    // Extract ledger ID (may be zero-padded)
                    if let Ok(lid) = suffix.parse::<i64>() {
                        return Ok(Some(lid));
                    }
                }
                // Recurse into subdirectories
                let child_path = format!("{}/{}", path, child);
                if let Ok(Some(lid)) = self
                    .find_underreplicated_in_tree(&child_path)
                    .await
                {
                    return Ok(Some(lid));
                }
            }
            Ok(None)
        })
    }

    async fn ensure_parents(&self, path: &str) -> Result<(), ReplicationError> {
        bookkeeper_metadata::zk_path::ensure_zk_parents(&self.client, path)
            .await
            .map_err(|e| ReplicationError::Zk(format!("{e}")))
    }

    async fn ensure_path(&self, path: &str) -> Result<(), ReplicationError> {
        bookkeeper_metadata::zk_path::ensure_zk_path(&self.client, path)
            .await
            .map_err(|e| ReplicationError::Zk(format!("{e}")))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("ZooKeeper error: {0}")]
    Zk(String),
    #[error("client error: {0}")]
    Client(#[from] bookkeeper_client::bookie_connection::ClientError),
    #[error("replication failed: {0}")]
    Failed(String),
}
