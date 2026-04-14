use std::collections::HashSet;
use rand::seq::SliceRandom;
use zookeeper_client as zk;
use bookkeeper_metadata::zk_path::{ZkPaths, underreplication_ledger_path, underreplication_lock_path};
use bookkeeper_proto::{UnderreplicatedLedgerFormat, LockDataFormat, LedgerRereplicationLayoutFormat,
    CheckAllLedgersFormat, PlacementPolicyCheckFormat, ReplicasCheckFormat};
use prost::Message;
use crate::text_format;

/// Layout type for the underreplication subsystem, matching Java's constant.
const LAYOUT_TYPE: &str = "BASIC";
const LAYOUT_VERSION: i32 = 1;

/// Maximum CAS retry attempts before giving up on an underreplication CAS.
/// Higher than `bookkeeper_common::MAX_CAS_RETRIES` (10) because marker updates
/// are lower-priority than ledger-metadata updates and we'd rather retry longer
/// than lose the mark.
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
    ///
    /// Walks the 4-level hex hierarchy under `ur/ledgers`, **shuffling children
    /// at every depth** so that multiple workers scanning the same tree do
    /// not all start at the lowest-ID ledger and collide on the same lock.
    /// Matches Java's `ZkLedgerUnderreplicationManager.getLedgerToRereplicateFromHierarchy()`
    /// which calls `Collections.shuffle(children)` at each recursion level.
    pub async fn get_ledger_to_rereplicate(&self) -> Result<Option<i64>, ReplicationError> {
        let ur_ledgers_path = format!("{}/ledgers", self.paths.underreplication());

        // Scan for underreplicated ledgers by listing children recursively.
        let mut children = match self.client.get_children(&ur_ledgers_path).await {
            Ok((c, _)) => c,
            Err(zk::Error::NoNode) => return Ok(None),
            Err(e) => return Err(ReplicationError::Zk(format!("list underrep: {e}"))),
        };
        children.shuffle(&mut rand::rng());

        // Recursively find urL* znodes (shuffled inside the recursive helper too).
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

    /// Enumerate all under-replicated ledgers, optionally filtering by a
    /// predicate on `(ledger_id, missing_replicas)`. Matches Java's
    /// `ZkLedgerUnderreplicationManager.listLedgersToRereplicate(Predicate)`
    /// used by admin/HTTP tools and the auditor's replicasCheck.
    ///
    /// Does NOT acquire locks — this is a read-only scan.
    pub async fn list_ledgers_to_rereplicate<F>(
        &self,
        mut predicate: F,
    ) -> Result<Vec<(i64, Vec<String>)>, ReplicationError>
    where
        F: FnMut(i64, &[String]) -> bool + Send,
    {
        let ur_ledgers_path = format!("{}/ledgers", self.paths.underreplication());
        let mut out: Vec<(i64, Vec<String>)> = Vec::new();
        self.walk_underreplicated(&ur_ledgers_path, &mut |lid, replicas| {
            predicate(lid, replicas)
        }, &mut out).await?;
        Ok(out)
    }

    /// DFS through the hierarchy and invoke `visit` for every `urL*` leaf.
    /// Collects entries that the predicate accepts. Silent on transient
    /// NoNode errors (another worker may be cleaning up in parallel), like
    /// Java's traversal which catches `KeeperException.NoNodeException`.
    fn walk_underreplicated<'a, F>(
        &'a self,
        path: &'a str,
        visit: &'a mut F,
        out: &'a mut Vec<(i64, Vec<String>)>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ReplicationError>> + Send + 'a>>
    where
        F: FnMut(i64, &[String]) -> bool + Send,
    {
        Box::pin(async move {
            let children = match self.client.get_children(path).await {
                Ok((c, _)) => c,
                Err(zk::Error::NoNode) => return Ok(()),
                Err(e) => return Err(ReplicationError::Zk(format!("list: {e}"))),
            };
            for child in &children {
                if let Some(suffix) = child.strip_prefix("urL") {
                    if let Ok(lid) = suffix.parse::<i64>() {
                        // Read the marker payload to get missing_replicas.
                        let leaf_path = format!("{}/{}", path, child);
                        match self.client.get_data(&leaf_path).await {
                            Ok((data, _)) => {
                                let parsed = text_format::parse_underreplicated_ledger(&data)
                                    .unwrap_or(UnderreplicatedLedgerFormat::default());
                                if visit(lid, &parsed.replica) {
                                    out.push((lid, parsed.replica));
                                }
                            }
                            Err(zk::Error::NoNode) => {} // cleaned up between listing and reading
                            Err(e) => return Err(ReplicationError::Zk(format!("read leaf: {e}"))),
                        }
                        continue;
                    }
                }
                let child_path = format!("{}/{}", path, child);
                self.walk_underreplicated(&child_path, visit, out).await?;
            }
            Ok(())
        })
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

    /// Install a persistent-recursive watch on the under-replication tree.
    /// The returned [`zk::PersistentWatcher`] fires on any create/delete
    /// anywhere under `ur/ledgers` — used by the worker to sleep until a
    /// new ledger is marked for replication (or an existing one is
    /// removed by a peer) without polling.
    ///
    /// Matches Java's
    /// `ZkLedgerUnderreplicationManager.notifyUnderReplicationLedgerChanged`
    /// which calls `zkc.addWatch(urLedgerPath, PERSISTENT_RECURSIVE)`.
    pub async fn watch_underreplicated_tree(&self) -> Result<zk::PersistentWatcher, ReplicationError> {
        let ur_ledgers_path = format!("{}/ledgers", self.paths.underreplication());
        self.ensure_path(&ur_ledgers_path).await?;
        self.client
            .watch(&ur_ledgers_path, zk::AddWatchMode::PersistentRecursive)
            .await
            .map_err(|e| ReplicationError::Zk(format!("persistent watch: {e}")))
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

    /// Same as `get_lost_bookie_recovery_delay` but also installs a ZK
    /// watch on the delay znode so the caller can react to runtime
    /// changes. Matches Java's
    /// `ZkLedgerUnderreplicationManager.notifyLostBookieRecoveryDelayChanged(GenericCallback)`
    /// which internally uses `zkc.getData(path, watcher, null)` so the
    /// operator changing the delay via `bookkeeper shell` triggers the
    /// auditor to re-read the value rather than waiting for the next
    /// audit cycle.
    ///
    /// If the znode doesn't exist, a watch is installed via
    /// `check_and_watch_stat` so creation also fires the watcher.
    pub async fn watch_lost_bookie_recovery_delay(
        &self,
        default_secs: u64,
    ) -> Result<(u64, zk::OneshotWatcher), ReplicationError> {
        let delay_path = format!("{}/lostBookieRecoveryDelay", self.paths.underreplication());
        match self.client.get_and_watch_data(&delay_path).await {
            Ok((data, _stat, watcher)) => {
                let s = String::from_utf8_lossy(&data);
                let secs = s.trim().parse::<u64>().unwrap_or(default_secs);
                Ok((secs, watcher))
            }
            Err(zk::Error::NoNode) => {
                // Watch for creation of the znode.
                match self.client.check_and_watch_stat(&delay_path).await {
                    Ok((_stat, watcher)) => Ok((default_secs, watcher)),
                    Err(e) => Err(ReplicationError::Zk(format!("watch delay (absent): {e}"))),
                }
            }
            Err(e) => Err(ReplicationError::Zk(format!("watch delay: {e}"))),
        }
    }

    /// Last-run timestamp (ms since epoch) for the periodic full-ledger
    /// availability check. Returns -1 when the znode has never been
    /// written, matching Java's `getCheckAllLedgersCTime()`.
    pub async fn get_check_all_ledgers_ctime(&self) -> Result<i64, ReplicationError> {
        let path = format!("{}/checkallledgersctime", self.paths.underreplication());
        match self.client.get_data(&path).await {
            Ok((data, _)) => {
                let fmt = CheckAllLedgersFormat::decode(&data[..])
                    .map_err(|e| ReplicationError::Zk(format!("parse ctime: {e}")))?;
                Ok(fmt.check_all_ledgers_c_time.unwrap_or(-1))
            }
            Err(zk::Error::NoNode) => Ok(-1),
            Err(e) => Err(ReplicationError::Zk(format!("read ctime: {e}"))),
        }
    }

    pub async fn set_check_all_ledgers_ctime(&self, ctime: i64) -> Result<(), ReplicationError> {
        let path = format!("{}/checkallledgersctime", self.paths.underreplication());
        let fmt = CheckAllLedgersFormat { check_all_ledgers_c_time: Some(ctime) };
        let data = fmt.encode_to_vec();
        self.upsert_znode(&path, &data).await
    }

    /// Last-run timestamp (ms since epoch) for the periodic placement-policy
    /// check. Returns -1 when never run. Matches Java's
    /// `getPlacementPolicyCheckCTime()`.
    pub async fn get_placement_policy_check_ctime(&self) -> Result<i64, ReplicationError> {
        let path = format!("{}/placementpolicycheckctime", self.paths.underreplication());
        match self.client.get_data(&path).await {
            Ok((data, _)) => {
                let fmt = PlacementPolicyCheckFormat::decode(&data[..])
                    .map_err(|e| ReplicationError::Zk(format!("parse ctime: {e}")))?;
                Ok(fmt.placement_policy_check_c_time.unwrap_or(-1))
            }
            Err(zk::Error::NoNode) => Ok(-1),
            Err(e) => Err(ReplicationError::Zk(format!("read ctime: {e}"))),
        }
    }

    pub async fn set_placement_policy_check_ctime(&self, ctime: i64) -> Result<(), ReplicationError> {
        let path = format!("{}/placementpolicycheckctime", self.paths.underreplication());
        let fmt = PlacementPolicyCheckFormat { placement_policy_check_c_time: Some(ctime) };
        let data = fmt.encode_to_vec();
        self.upsert_znode(&path, &data).await
    }

    /// Last-run timestamp (ms since epoch) for the periodic replicas check
    /// (which verifies that every fragment's replica count matches the
    /// expected write quorum). Matches Java's `getReplicasCheckCTime()`.
    pub async fn get_replicas_check_ctime(&self) -> Result<i64, ReplicationError> {
        let path = format!("{}/replicascheckctime", self.paths.underreplication());
        match self.client.get_data(&path).await {
            Ok((data, _)) => {
                let fmt = ReplicasCheckFormat::decode(&data[..])
                    .map_err(|e| ReplicationError::Zk(format!("parse ctime: {e}")))?;
                Ok(fmt.replicas_check_c_time.unwrap_or(-1))
            }
            Err(zk::Error::NoNode) => Ok(-1),
            Err(e) => Err(ReplicationError::Zk(format!("read ctime: {e}"))),
        }
    }

    pub async fn set_replicas_check_ctime(&self, ctime: i64) -> Result<(), ReplicationError> {
        let path = format!("{}/replicascheckctime", self.paths.underreplication());
        let fmt = ReplicasCheckFormat { replicas_check_c_time: Some(ctime) };
        let data = fmt.encode_to_vec();
        self.upsert_znode(&path, &data).await
    }

    /// Write or create a persistent znode with the given bytes. Helper used
    /// by the CTime setters — matches Java's pattern of `exists + setData
    /// else create(PERSISTENT)` in `setCheckAllLedgersCTime` et al.
    async fn upsert_znode(&self, path: &str, data: &[u8]) -> Result<(), ReplicationError> {
        match self.client.set_data(path, data, None).await {
            Ok(_) => Ok(()),
            Err(zk::Error::NoNode) => {
                let opts = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
                self.client.create(path, data, &opts).await
                    .map(|_| ())
                    .map_err(|e| ReplicationError::Zk(format!("create ctime znode: {e}")))
            }
            Err(e) => Err(ReplicationError::Zk(format!("set ctime: {e}"))),
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
            let mut children = match self.client.get_children(path).await {
                Ok((c, _)) => c,
                Err(zk::Error::NoNode) => return Ok(None),
                Err(e) => return Err(ReplicationError::Zk(format!("list: {e}"))),
            };
            // Shuffle at every recursion level so concurrent workers don't
            // converge on the same ledger. Matches Java's
            // `Collections.shuffle(children)` in
            // `getLedgerToRereplicateFromHierarchy`.
            children.shuffle(&mut rand::rng());
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
