use async_trait::async_trait;

use crate::ledger_metadata::{LedgerMetadata, MetadataError};
use crate::versioned::Versioned;
use crate::service_info::BookieServiceInfo;

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

/// Manages the cluster metadata layout.
#[async_trait]
pub trait LayoutManager: Send + Sync {
    async fn read_ledger_layout(&self) -> Result<Option<LedgerLayout>, MetadataError>;
    async fn store_ledger_layout(&self, layout: &LedgerLayout) -> Result<(), MetadataError>;
    async fn delete_ledger_layout(&self) -> Result<(), MetadataError>;
}

/// Server-side metadata driver (used by bookies).
#[async_trait]
pub trait MetadataBookieDriver: Send + Sync {
    fn get_layout_manager(&self) -> &dyn LayoutManager;
    async fn close(&self);
}

/// Client-side metadata driver (used by BK clients).
#[async_trait]
pub trait MetadataClientDriver: Send + Sync {
    fn get_ledger_manager(&self) -> &dyn LedgerManager;
    fn get_ledger_id_generator(&self) -> &dyn LedgerIdGenerator;
    fn get_layout_manager(&self) -> &dyn LayoutManager;
    async fn close(&self);
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
