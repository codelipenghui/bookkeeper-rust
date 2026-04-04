use std::net::SocketAddr;
use std::sync::Arc;

use bookkeeper_protocol::digest::{DigestManager, DigestType, generate_master_key};
use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;
use crate::read_handle::ReadHandle;
use crate::write_handle::WriteHandle;

/// Minimal BookKeeper client for creating, writing, and reading ledgers.
/// Uses a fixed set of bookie addresses (no ZK discovery in this phase).
pub struct BookKeeperClient {
    bookie_addrs: Vec<String>,
    connections: Vec<Arc<BookieConnection>>,
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
            let mut conns = Vec::new();
            for addr_str in &self.bookie_addrs {
                let addr: SocketAddr = addr_str.parse()
                    .map_err(|e| ClientError::Protocol(format!("bad address '{addr_str}': {e}")))?;
                let conn = BookieConnection::connect(addr).await?;
                conns.push(Arc::new(conn));
            }
            self.connections = conns;
        }
        Ok(())
    }

    pub fn connections(&self) -> &[Arc<BookieConnection>] {
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
        let available_bookies: Vec<String> = self.bookie_addrs.clone();
        let schedule = RoundRobinDistributionSchedule::new(ensemble_size, write_quorum, ack_quorum);
        let dm = DigestManager::new(digest_type).with_password(password);

        let master_key = generate_master_key(password);

        Ok(WriteHandle::with_available_bookies(
            ledger_id,
            ensemble,
            available_bookies,
            schedule,
            dm,
            master_key,
        ))
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
    ) -> Result<ReadHandle, ClientError> {
        if self.bookie_addrs.len() < ensemble_size {
            return Err(ClientError::Protocol(format!(
                "not enough bookies: need {ensemble_size}, have {}",
                self.bookie_addrs.len()
            )));
        }
        let ensemble: Vec<String> = self.bookie_addrs[..ensemble_size].to_vec();
        let schedule = RoundRobinDistributionSchedule::new(ensemble_size, write_quorum, ack_quorum);
        let dm = DigestManager::new(digest_type).with_password(password);

        let master_key = generate_master_key(password);

        Ok(ReadHandle::new(ledger_id, ensemble, schedule, dm, master_key))
    }
}
