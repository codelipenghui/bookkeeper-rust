use std::net::SocketAddr;
use std::sync::Arc;

use bookkeeper_protocol::digest::{DigestManager, DigestType};
use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;
use crate::read_handle::ReadHandle;
use crate::write_handle::WriteHandle;

fn derive_master_key(password: &[u8]) -> [u8; 20] {
    use sha1::Digest;
    let mut hasher = sha1::Sha1::new();
    hasher.update(b"ledger");
    hasher.update(password);
    let result = hasher.finalize();
    let mut key = [0u8; 20];
    key.copy_from_slice(&result);
    key
}

/// Minimal BookKeeper client for creating, writing, and reading ledgers.
/// Uses a fixed set of bookie addresses (no ZK discovery in this phase).
pub struct BookKeeperClient {
    bookie_addrs: Vec<String>,
    connections: Vec<Arc<tokio::sync::Mutex<BookieConnection>>>,
}

impl BookKeeperClient {
    pub fn new(bookie_addrs: Vec<String>) -> Self {
        Self {
            bookie_addrs,
            connections: Vec::new(),
        }
    }

    /// Ensure connections to all bookies are established.
    pub async fn ensure_connections(&mut self) -> Result<(), ClientError> {
        if self.connections.is_empty() {
            for addr_str in &self.bookie_addrs {
                let addr: SocketAddr = addr_str.parse()
                    .map_err(|e| ClientError::Protocol(format!("bad address '{addr_str}': {e}")))?;
                let conn = BookieConnection::connect(addr).await?;
                self.connections.push(Arc::new(tokio::sync::Mutex::new(conn)));
            }
        }
        Ok(())
    }

    pub fn connections(&self) -> &[Arc<tokio::sync::Mutex<BookieConnection>>] {
        &self.connections
    }

    /// Create a new ledger with the given parameters. Returns a WriteHandle.
    pub async fn create_ledger(
        &mut self,
        ledger_id: i64,
        ensemble_size: usize,
        write_quorum: usize,
        ack_quorum: usize,
        digest_type: DigestType,
        password: &[u8],
    ) -> Result<WriteHandle, ClientError> {
        self.ensure_connections().await?;

        // Select ensemble (simple: use first N bookies)
        if self.bookie_addrs.len() < ensemble_size {
            return Err(ClientError::Protocol(format!(
                "not enough bookies: need {ensemble_size}, have {}",
                self.bookie_addrs.len()
            )));
        }
        let ensemble: Vec<String> = self.bookie_addrs[..ensemble_size].to_vec();
        let schedule = RoundRobinDistributionSchedule::new(ensemble_size, write_quorum, ack_quorum);
        let dm = DigestManager::new(digest_type).with_password(password);

        let master_key = derive_master_key(password);

        Ok(WriteHandle::new(ledger_id, ensemble, schedule, dm, master_key))
    }

    /// Open a ledger for reading. Returns a ReadHandle.
    pub fn open_ledger(
        &self,
        ledger_id: i64,
        ensemble_size: usize,
        write_quorum: usize,
        ack_quorum: usize,
        digest_type: DigestType,
        password: &[u8],
    ) -> ReadHandle {
        let ensemble: Vec<String> = self.bookie_addrs[..ensemble_size].to_vec();
        let schedule = RoundRobinDistributionSchedule::new(ensemble_size, write_quorum, ack_quorum);
        let dm = DigestManager::new(digest_type).with_password(password);

        let master_key = derive_master_key(password);

        ReadHandle::new(ledger_id, ensemble, schedule, dm, master_key)
    }
}
