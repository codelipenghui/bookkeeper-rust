// ZK-specific path helpers. Generic (backend-agnostic) path helpers
// live in `crate::metadata_paths`.

pub use crate::metadata_paths::{ledger_path, underreplication_ledger_path, underreplication_lock_path};

/// Alias kept for callers that still spell it `ZkPaths`. New code
/// should use `metadata_paths::LedgerPaths` directly.
pub type ZkPaths = crate::metadata_paths::LedgerPaths;

/// Create all ZK znodes along a path (like `mkdir -p`), including the leaf.
/// Ignores `NodeExists` errors.
pub async fn ensure_zk_path(
    client: &zookeeper_client::Client,
    path: &str,
) -> Result<(), crate::ledger_metadata::MetadataError> {
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    let mut current = String::new();
    let options = zookeeper_client::CreateMode::Persistent
        .with_acls(zookeeper_client::Acls::anyone_all());
    for part in &parts {
        current.push('/');
        current.push_str(part);
        match client.create(&current, &[], &options).await {
            Ok(_) | Err(zookeeper_client::Error::NodeExists) => {}
            Err(e) => {
                return Err(crate::ledger_metadata::MetadataError::ZkError(format!(
                    "ensure {current}: {e}"
                )));
            }
        }
    }
    Ok(())
}

/// Create parent ZK znodes for a path (everything except the leaf).
pub async fn ensure_zk_parents(
    client: &zookeeper_client::Client,
    path: &str,
) -> Result<(), crate::ledger_metadata::MetadataError> {
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    let mut current = String::new();
    let options = zookeeper_client::CreateMode::Persistent
        .with_acls(zookeeper_client::Acls::anyone_all());
    for (i, part) in parts.iter().enumerate() {
        current.push('/');
        current.push_str(part);
        if i < parts.len() - 1 {
            match client.create(&current, &[], &options).await {
                Ok(_) | Err(zookeeper_client::Error::NodeExists) => {}
                Err(e) => {
                    return Err(crate::ledger_metadata::MetadataError::ZkError(format!(
                        "ensure {current}: {e}"
                    )));
                }
            }
        }
    }
    Ok(())
}
