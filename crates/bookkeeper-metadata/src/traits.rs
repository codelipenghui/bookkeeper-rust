use async_trait::async_trait;

use crate::ledger_metadata::{LedgerMetadata, MetadataError};
use crate::versioned::Versioned;
use crate::service_info::BookieServiceInfo;

const LAYOUT_FORMAT_VERSION: i32 = 2;

/// The primary interface for ledger metadata operations.
/// All write operations use compare-and-swap via the version parameter.
#[async_trait]
pub trait LedgerManager: Send + Sync {
    async fn create_ledger_metadata(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError>;

    async fn read_ledger_metadata(
        &self,
        ledger_id: i64,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError>;

    async fn write_ledger_metadata(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
        current_version: i32,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError>;

    async fn remove_ledger_metadata(
        &self,
        ledger_id: i64,
        version: i32,
    ) -> Result<(), MetadataError>;

    /// Iterate over all ledger IDs in the metadata store.
    /// Used by the Auditor to build bookie-to-ledger indexes.
    async fn get_ledger_ranges(&self) -> Result<Vec<i64>, MetadataError>;
}

/// Generates unique ledger IDs.
#[async_trait]
pub trait LedgerIdGenerator: Send + Sync {
    async fn generate_ledger_id(&self) -> Result<i64, MetadataError>;
}

/// Layout information stored in ZK to identify the metadata format.
#[derive(Debug, Clone)]
pub struct LedgerLayout {
    pub manager_factory_class: String,
    pub manager_version: i32,
}

impl LedgerLayout {
    pub fn serialize(&self) -> String {
        format!("{}\n{}:{}", LAYOUT_FORMAT_VERSION, self.manager_factory_class, self.manager_version)
    }

    pub fn deserialize(data: &str) -> Result<Self, MetadataError> {
        let mut lines = data.lines();
        // Line 1: layout format version (skip)
        let _format_version = lines.next()
            .ok_or_else(|| MetadataError::SerializationError("empty layout".into()))?;
        // Line 2: "{class}:{version}"
        let class_version = lines.next()
            .ok_or_else(|| MetadataError::SerializationError("missing class:version in layout".into()))?;
        let (class, version_str) = class_version.rsplit_once(':')
            .ok_or_else(|| MetadataError::SerializationError(
                format!("invalid layout format, expected 'class:version' got '{class_version}'")))?;
        let version: i32 = version_str.parse()
            .map_err(|e| MetadataError::SerializationError(format!("invalid layout version: {e}")))?;
        Ok(LedgerLayout {
            manager_factory_class: class.to_string(),
            manager_version: version,
        })
    }
}

/// Manages the cluster metadata layout.
#[async_trait]
pub trait LayoutManager: Send + Sync {
    async fn read_ledger_layout(&self) -> Result<Option<LedgerLayout>, MetadataError>;
    async fn store_ledger_layout(&self, layout: &LedgerLayout) -> Result<(), MetadataError>;
    async fn delete_ledger_layout(&self) -> Result<(), MetadataError>;
}

/// Manages bookie registration and discovery in the metadata store.
#[async_trait]
pub trait RegistrationManager: Send + Sync {
    async fn register_bookie(&self, bookie_id: &str, read_only: bool, info: &BookieServiceInfo) -> Result<(), MetadataError>;
    async fn unregister_bookie(&self, bookie_id: &str, read_only: bool) -> Result<(), MetadataError>;
    async fn get_writable_bookies(&self) -> Result<Vec<String>, MetadataError>;
    async fn get_readonly_bookies(&self) -> Result<Vec<String>, MetadataError>;
    async fn read_cookie(&self, bookie_id: &str) -> Result<Option<(Vec<u8>, i32)>, MetadataError>;
    async fn create_cookie(&self, bookie_id: &str, data: &[u8]) -> Result<(), MetadataError>;
    async fn update_cookie(&self, bookie_id: &str, data: &[u8], version: i32) -> Result<(), MetadataError>;
    async fn delete_cookie(&self, bookie_id: &str, version: i32) -> Result<(), MetadataError>;
    async fn get_cluster_instance_id(&self) -> Result<Option<String>, MetadataError>;
    async fn init_new_cluster(&self, layout: &LedgerLayout, instance_id: &str) -> Result<(), MetadataError>;
}
