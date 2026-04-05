use async_trait::async_trait;
use zookeeper_client as zk;

use crate::ledger_metadata::{LedgerMetadata, MetadataError};
use crate::metadata_serde::{serialize_metadata, deserialize_metadata};
use crate::traits::LedgerManager;
use crate::versioned::Versioned;
use crate::zk_path::{ZkPaths, ledger_path};

/// ZooKeeper-backed LedgerManager using the hybrid hierarchical path scheme.
///
/// Matches Java's HierarchicalLedgerManager:
/// - Short paths (2-4-4) for ledger IDs < i32::MAX
/// - Long paths (3-4-4-4-4) for ledger IDs >= i32::MAX
/// - Metadata stored as v3 binary protobuf with version header
/// - CAS writes via ZK stat version
pub struct ZkLedgerManager {
    client: zk::Client,
    paths: ZkPaths,
}

impl ZkLedgerManager {
    pub fn new(client: zk::Client, paths: ZkPaths) -> Self {
        Self { client, paths }
    }

    /// Get the full ZK path for a ledger's metadata znode.
    fn ledger_znode_path(&self, ledger_id: i64) -> String {
        ledger_path(self.paths.root(), ledger_id)
    }

    async fn ensure_parents(&self, path: &str) -> Result<(), MetadataError> {
        crate::zk_path::ensure_zk_parents(&self.client, path).await
    }

    /// Delete a znode and clean up empty parent directories.
    /// Matches Java's ZkUtils.asyncDeleteFullPathOptimistic.
    async fn delete_with_parent_cleanup(
        &self,
        path: &str,
        version: Option<i32>,
    ) -> Result<(), MetadataError> {
        // Delete the leaf node
        self.client
            .delete(path, version)
            .await
            .map_err(|e| match e {
                zk::Error::NoNode => MetadataError::NotFound(0),
                zk::Error::BadVersion => MetadataError::VersionConflict {
                    expected: version.unwrap_or(-1),
                    actual: -1,
                },
                other => MetadataError::ZkError(format!("delete {path}: {other}")),
            })?;

        // Clean up empty parents (stop at ledger root)
        let root = self.paths.root();
        let mut current = path.to_string();
        loop {
            if let Some(pos) = current.rfind('/') {
                current.truncate(pos);
                if current.len() <= root.len() || current == root {
                    break;
                }
                // Try to delete empty parent — ignore failures
                match self.client.delete(&current, None).await {
                    Ok(()) => {} // deleted, continue up
                    Err(_) => break, // not empty or error, stop
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Recursively scan the hierarchical ZK tree to find all ledger IDs.
    /// Matches Java's LongHierarchicalLedgerManager.asyncProcessLedgers().
    fn scan_ledgers_recursive<'a>(
        &'a self,
        path: &'a str,
        depth: usize,
        max_depth: usize,
        results: &'a mut Vec<i64>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), MetadataError>> + Send + 'a>> {
        Box::pin(async move {
            let children = match self.client.get_children(path).await {
                Ok((c, _)) => c,
                Err(zk::Error::NoNode) => return Ok(()),
                Err(e) => return Err(MetadataError::ZkError(format!("list {path}: {e}"))),
            };

            if depth < max_depth {
                // Intermediate level: recurse into numeric subdirectories
                for child in &children {
                    // Filter: only numeric directory names (matching Java's isLedgerParentNode)
                    if child.chars().all(|c| c.is_ascii_digit()) {
                        let child_path = format!("{}/{}", path, child);
                        self.scan_ledgers_recursive(&child_path, depth + 1, max_depth, results)
                            .await?;
                    }
                }
            } else {
                // Leaf level: extract ledger IDs from "L{digits}" nodes
                for child in &children {
                    if let Some(id_str) = child.strip_prefix('L') {
                        if let Ok(lid) = id_str.parse::<i64>() {
                            // Reconstruct full ledger ID from path + leaf
                            let full_id = reconstruct_ledger_id(path, self.paths.root(), lid);
                            results.push(full_id);
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

/// Reconstruct ledger ID from the hierarchical path components.
/// Short hierarchy: root/XX/XXXX/L{XXXX} → concatenate XX+XXXX+XXXX = 10-digit ID
/// Long hierarchy: root/XXX/XXXX/XXXX/XXXX/L{XXXX} → concatenate XXX+XXXX+XXXX+XXXX+XXXX = 19-digit ID
fn reconstruct_ledger_id(parent_path: &str, root: &str, leaf_id: i64) -> i64 {
    // Strip root prefix to get the relative path
    let relative = if parent_path.len() > root.len() {
        &parent_path[root.len() + 1..]
    } else {
        return leaf_id;
    };

    // Extract all numeric parts from the directory path
    let mut prefix_digits = String::new();
    for part in relative.split('/') {
        for ch in part.chars() {
            if ch.is_ascii_digit() {
                prefix_digits.push(ch);
            }
        }
    }

    // Compute the ID: path digits form the high-order part, leaf the low-order
    // For short: "00/0000" → prefix_digits="000000", leaf "L0001" → leaf_id=1
    //   Full number = "000000" + "0001" = "0000000001" = 1
    // For long: "000/0000/0000/0000" → prefix_digits="000000000000000", leaf "L0001" → leaf_id=1
    //   Full number = "000000000000000" + "0001" = "0000000000000000001" = 1
    let leaf_str = format!("{:04}", leaf_id);
    let full_str = format!("{}{}", prefix_digits, leaf_str);
    full_str.parse::<i64>().unwrap_or(leaf_id)
}

#[async_trait]
impl LedgerManager for ZkLedgerManager {
    async fn create_ledger_metadata(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
        let path = self.ledger_znode_path(ledger_id);
        let data = serialize_metadata(metadata)?;

        // Ensure parent directories exist
        self.ensure_parents(&path).await?;

        // Create the znode
        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        match self.client.create(&path, &data, &options).await {
            Ok(_) => Ok(Versioned::new(metadata.clone(), 0)),
            Err(zk::Error::NodeExists) => Err(MetadataError::LedgerExists(ledger_id)),
            Err(e) => Err(MetadataError::ZkError(format!(
                "create ledger {ledger_id}: {e}"
            ))),
        }
    }

    async fn read_ledger_metadata(
        &self,
        ledger_id: i64,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
        let path = self.ledger_znode_path(ledger_id);

        let (data, stat) = self
            .client
            .get_data(&path)
            .await
            .map_err(|e| match e {
                zk::Error::NoNode => MetadataError::NotFound(ledger_id),
                other => MetadataError::ZkError(format!("read ledger {ledger_id}: {other}")),
            })?;

        let metadata = deserialize_metadata(ledger_id, &data)?;
        Ok(Versioned::new(metadata, stat.version))
    }

    async fn write_ledger_metadata(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
        current_version: i32,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
        let path = self.ledger_znode_path(ledger_id);
        let data = serialize_metadata(metadata)?;

        // CAS update with version check
        let stat = self
            .client
            .set_data(&path, &data, Some(current_version))
            .await
            .map_err(|e| match e {
                zk::Error::BadVersion => MetadataError::VersionConflict {
                    expected: current_version,
                    actual: -1,
                },
                zk::Error::NoNode => MetadataError::NotFound(ledger_id),
                other => MetadataError::ZkError(format!("write ledger {ledger_id}: {other}")),
            })?;

        Ok(Versioned::new(metadata.clone(), stat.version))
    }

    async fn remove_ledger_metadata(
        &self,
        ledger_id: i64,
        version: i32,
    ) -> Result<(), MetadataError> {
        let path = self.ledger_znode_path(ledger_id);
        self.delete_with_parent_cleanup(&path, Some(version)).await
    }

    /// Scan the entire hierarchical ZK tree to find all ledger IDs.
    /// Handles both short (3-level) and long (5-level) hierarchies.
    async fn get_ledger_ranges(&self) -> Result<Vec<i64>, MetadataError> {
        let root = self.paths.root();
        let mut results = Vec::new();

        // List top-level children of the ledger root
        let children = match self.client.get_children(root).await {
            Ok((c, _)) => c,
            Err(zk::Error::NoNode) => return Ok(results),
            Err(e) => return Err(MetadataError::ZkError(format!("list root: {e}"))),
        };

        for child in &children {
            // Skip non-numeric children (e.g., "available", "cookies", "LAYOUT", etc.)
            if !child.chars().all(|c| c.is_ascii_digit()) {
                continue;
            }

            let child_path = format!("{}/{}", root, child);

            // Determine hierarchy depth based on prefix length:
            // - 2-digit prefix → short hierarchy (3 levels: XX/XXXX/L{XXXX})
            // - 3-digit prefix → long hierarchy (5 levels: XXX/XXXX/XXXX/XXXX/L{XXXX})
            let max_depth = if child.len() == 2 { 2 } else { 4 };

            self.scan_ledgers_recursive(&child_path, 1, max_depth, &mut results)
                .await?;
        }

        results.sort_unstable();
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reconstruct_short_id() {
        // Short: /ledgers/00/0000/L0001 → ID = 1
        let id = reconstruct_ledger_id("/ledgers/00/0000", "/ledgers", 1);
        assert_eq!(id, 1);
    }

    #[test]
    fn test_reconstruct_short_id_large() {
        // Short: /ledgers/12/3456/L7890 → ID = 1234567890
        let id = reconstruct_ledger_id("/ledgers/12/3456", "/ledgers", 7890);
        assert_eq!(id, 1234567890);
    }

    #[test]
    fn test_reconstruct_long_id() {
        // Long: /ledgers/000/0000/0000/0000/L0001 → ID = 1
        let id = reconstruct_ledger_id("/ledgers/000/0000/0000/0000", "/ledgers", 1);
        assert_eq!(id, 1);
    }

    #[test]
    fn test_reconstruct_long_id_large() {
        // ID 2147483648 (i32::MAX + 1): format!("{:019}", 2147483648) = "0000000002147483648"
        // Split 3-4-4-4-4: "000/0000/0021/4748/L3648"
        let id = reconstruct_ledger_id("/ledgers/000/0000/0021/4748", "/ledgers", 3648);
        assert_eq!(id, 2147483648);
    }

    #[test]
    fn test_ledger_path_roundtrip_short() {
        // ledger_path for ID=12345 → /ledgers/00/0001/L2345
        let path = ledger_path("/ledgers", 12345);
        assert_eq!(path, "/ledgers/00/0001/L2345");

        // Extract: parent="/ledgers/00/0001", leaf=2345
        let parent = "/ledgers/00/0001";
        let leaf_id = 2345i64;
        let reconstructed = reconstruct_ledger_id(parent, "/ledgers", leaf_id);
        assert_eq!(reconstructed, 12345);
    }

    #[test]
    fn test_ledger_path_roundtrip_long() {
        let original_id: i64 = i32::MAX as i64; // 2147483647
        let path = ledger_path("/ledgers", original_id);
        // For 2147483647: format!("{:019}", 2147483647) = "0000000002147483647"
        // Split: "000/0000/0021/4748/L3647"
        assert_eq!(path, "/ledgers/000/0000/0021/4748/L3647");

        let parent = "/ledgers/000/0000/0021/4748";
        let leaf_id = 3647i64;
        let reconstructed = reconstruct_ledger_id(parent, "/ledgers", leaf_id);
        assert_eq!(reconstructed, 2147483647);
    }
}
