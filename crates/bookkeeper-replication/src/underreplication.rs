use prost::Message;
use zookeeper_client as zk;
use bookkeeper_metadata::zk_path::{ZkPaths, underreplication_ledger_path};

/// Manages underreplicated ledger tracking in ZooKeeper.
pub struct ZkUnderreplicationManager {
    client: zk::Client,
    paths: ZkPaths,
}

impl ZkUnderreplicationManager {
    pub fn new(client: zk::Client, paths: ZkPaths) -> Self {
        Self { client, paths }
    }

    /// Mark a ledger as underreplicated with the list of missing bookie addresses.
    pub async fn mark_ledger_underreplicated(
        &self,
        ledger_id: i64,
        missing_replicas: &[String],
    ) -> Result<(), ReplicationError> {
        let ur_path = underreplication_ledger_path(
            &format!("{}/ledgers", self.paths.underreplication()),
            ledger_id,
        );

        // Build protobuf content
        let proto = bookkeeper_proto::UnderreplicatedLedgerFormat {
            replica: missing_replicas.to_vec(),
            ctime: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            ),
        };
        let data = proto.encode_to_vec();

        // Create parent paths if needed
        self.ensure_parents(&ur_path).await?;

        // Create or update the znode
        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        match self.client.create(&ur_path, &data, &options).await {
            Ok(_) => Ok(()),
            Err(zk::Error::NodeExists) => {
                // Merge: read existing, add new replicas
                self.client
                    .set_data(&ur_path, &data, None)
                    .await
                    .map_err(|e| ReplicationError::Zk(format!("update underrep: {e}")))?;
                Ok(())
            }
            Err(e) => Err(ReplicationError::Zk(format!("mark underrep: {e}"))),
        }
    }

    /// Try to claim a ledger for replication. Returns Some(ledger_id) if lock acquired.
    pub async fn get_ledger_to_rereplicate(&self) -> Result<Option<i64>, ReplicationError> {
        let ur_ledgers_path = format!("{}/ledgers", self.paths.underreplication());

        // Scan for underreplicated ledgers by listing children recursively.
        // For simplicity, scan the top-level hex directories.
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
                // Try to acquire lock
                let lock_path =
                    format!("{}/locks/urL{}", self.paths.underreplication(), lid);
                self.ensure_parents(&lock_path).await?;
                let lock_data = b"{}";
                let options = zk::CreateMode::Ephemeral.with_acls(zk::Acls::anyone_all());
                match self.client.create(&lock_path, lock_data, &options).await {
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
        let ur_path = underreplication_ledger_path(
            &format!("{}/ledgers", self.paths.underreplication()),
            ledger_id,
        );
        let lock_path = format!(
            "{}/locks/urL{}",
            self.paths.underreplication(),
            ledger_id
        );

        // Delete marker
        let _ = self.client.delete(&ur_path, None).await;
        // Release lock
        let _ = self.client.delete(&lock_path, None).await;
        Ok(())
    }

    /// Release lock without deleting marker (for retry).
    pub async fn release_lock(&self, ledger_id: i64) -> Result<(), ReplicationError> {
        let lock_path = format!(
            "{}/locks/urL{}",
            self.paths.underreplication(),
            ledger_id
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
                    // Extract ledger ID
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
        // Create parent directories one by one
        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        let mut current = String::new();
        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        for (i, part) in parts.iter().enumerate() {
            current.push('/');
            current.push_str(part);
            if i < parts.len() - 1 {
                // don't create the leaf node
                match self.client.create(&current, &[], &options).await {
                    Ok(_) | Err(zk::Error::NodeExists) => {}
                    Err(e) => {
                        return Err(ReplicationError::Zk(format!(
                            "ensure {current}: {e}"
                        )))
                    }
                }
            }
        }
        Ok(())
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
