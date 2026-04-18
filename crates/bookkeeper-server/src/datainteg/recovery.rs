//! `LedgerRecoveryClient` adapter backed by a [`ZkBookKeeper`].
//!
//! `ZkBookKeeper::open_ledger_with_recovery` takes `&mut self` because it
//! mutates its per-bookie connection cache. The data-integrity full-check
//! recovers ledgers serially (see `DataIntegrityCheck::run_full_check`),
//! so wrapping the client in a `tokio::sync::Mutex` is fine.

use std::sync::Arc;

use async_trait::async_trait;
use bookkeeper_client::zk_client::ZkBookKeeper;
use bookkeeper_metadata::ledger_metadata::LedgerMetadata;
use bookkeeper_protocol::digest::DigestType;
use tokio::sync::Mutex;

use super::check::{LedgerRecoveryClient, RecoverError};

pub struct ZkRecoveryClient {
    client: Arc<Mutex<ZkBookKeeper>>,
}

impl ZkRecoveryClient {
    pub fn new(client: Arc<Mutex<ZkBookKeeper>>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl LedgerRecoveryClient for ZkRecoveryClient {
    async fn recover_ledger(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
    ) -> Result<LedgerMetadata, RecoverError> {
        let digest = match metadata.digest_type() {
            Some(dt) => meta_to_proto_digest(dt),
            // No digest in metadata means the ledger was never written to.
            // Java treats this as an empty successful recovery.
            None => return Ok(metadata.clone()),
        };
        let password = metadata.password().unwrap_or(&[]).to_vec();
        let mut guard = self.client.lock().await;
        let (read_handle, _conns) = guard
            .open_ledger_with_recovery(ledger_id, digest, &password)
            .await
            .map_err(|e| RecoverError::Other(format!("open_ledger_with_recovery: {e}")))?;
        // The recovered handle carries fresh metadata; read it back via
        // the ZkLedgerManager so we return a LedgerMetadata with the
        // post-recovery last_entry_id/length/state.
        let version =
            guard.ledger_manager().read_ledger_metadata(ledger_id).await.map_err(
                |e| RecoverError::Other(format!("re-read metadata: {e}")),
            )?;
        drop(read_handle);
        Ok(version.value().clone())
    }
}

fn meta_to_proto_digest(dt: bookkeeper_metadata::ledger_metadata::DigestType) -> DigestType {
    use bookkeeper_metadata::ledger_metadata::DigestType as M;
    match dt {
        M::Crc32 => DigestType::Crc32,
        M::Crc32c => DigestType::Crc32c,
        M::Hmac => DigestType::HmacSha1,
        M::Dummy => DigestType::Dummy,
    }
}
