use std::net::SocketAddr;
use std::sync::Arc;

use zookeeper_client as zk;

use bookkeeper_metadata::ledger_metadata::{
    DigestType as MetaDigestType, LedgerMetadataBuilder, LedgerState, MetadataError,
};
use bookkeeper_metadata::traits::{LedgerIdGenerator, LedgerManager};
use bookkeeper_metadata::zk::idgen::ZkLedgerIdGenerator;
use bookkeeper_metadata::zk::ledger_manager::ZkLedgerManager;
use bookkeeper_metadata::zk_path::ZkPaths;
use bookkeeper_protocol::digest::{DigestManager, DigestType, generate_master_key};

use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;
use crate::read_handle::ReadHandle;
use crate::recovery::RecoveryHandle;
use crate::write_handle::WriteHandle;

/// ZooKeeper-integrated BookKeeper client.
///
/// Matches Java's BookKeeper client class:
/// - Discovers bookies from ZK `/ledgers/available`
/// - Allocates ledger IDs via sequential znodes at `/ledgers/idgen/ID-`
/// - Stores/reads ledger metadata in ZK using hierarchical paths
/// - Supports full ledger lifecycle: create → write → close → open → read → delete
pub struct ZkBookKeeper {
    zk_client: zk::Client,
    paths: ZkPaths,
    ledger_manager: Arc<ZkLedgerManager>,
    id_generator: ZkLedgerIdGenerator,
    /// Cached writable bookie list. Refreshed on each create_ledger call.
    bookie_addrs: Vec<String>,
    /// Active connections keyed by bookie address.
    connections: std::collections::HashMap<String, Arc<BookieConnection>>,
}

impl ZkBookKeeper {
    /// Create a new BookKeeper client connected to ZooKeeper.
    ///
    /// ```ignore
    /// let bk = ZkBookKeeper::new("localhost:2181", "/ledgers").await?;
    /// ```
    pub async fn new(zk_connect: &str, ledger_root: &str) -> Result<Self, ClientError> {
        let zk_client = zk::Client::connect(zk_connect)
            .await
            .map_err(|e| ClientError::Protocol(format!("ZK connect: {e}")))?;

        let paths = ZkPaths::new(ledger_root);

        let ledger_manager = Arc::new(ZkLedgerManager::new(zk_client.clone(), paths.clone()));

        let id_generator = ZkLedgerIdGenerator::new(zk_client.clone(), &paths)
            .await
            .map_err(|e| ClientError::Protocol(format!("init idgen: {e}")))?;

        let mut client = Self {
            zk_client,
            paths,
            ledger_manager,
            id_generator,
            bookie_addrs: Vec::new(),
            connections: std::collections::HashMap::new(),
        };

        // Initial bookie discovery
        client.refresh_bookies().await?;

        Ok(client)
    }

    /// Refresh the list of available writable bookies from ZK.
    async fn refresh_bookies(&mut self) -> Result<(), ClientError> {
        let available_path = self.paths.available();
        let (children, _) = self
            .zk_client
            .get_children(available_path)
            .await
            .map_err(|e| ClientError::Protocol(format!("list bookies: {e}")))?;

        self.bookie_addrs = children
            .into_iter()
            .filter(|c| c != "readonly")
            .collect();

        Ok(())
    }

    /// Get or create a connection to a bookie.
    async fn get_connection(&mut self, addr: &str) -> Result<Arc<BookieConnection>, ClientError> {
        if let Some(conn) = self.connections.get(addr) {
            return Ok(conn.clone());
        }
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| ClientError::Protocol(format!("bad address '{addr}': {e}")))?;
        let conn = Arc::new(BookieConnection::connect(socket_addr).await?);
        self.connections.insert(addr.to_string(), conn.clone());
        Ok(conn)
    }

    /// Get connections for an ensemble (ordered).
    async fn ensemble_connections(
        &mut self,
        ensemble: &[String],
    ) -> Result<Vec<Arc<BookieConnection>>, ClientError> {
        let mut conns = Vec::with_capacity(ensemble.len());
        for addr in ensemble {
            conns.push(self.get_connection(addr).await?);
        }
        Ok(conns)
    }

    /// Select bookies for a new ensemble.
    /// Simple round-robin selection matching Java's DefaultEnsemblePlacementPolicy
    /// for single-rack deployments.
    fn select_ensemble(
        &self,
        ensemble_size: usize,
        exclude: &[String],
    ) -> Result<Vec<String>, ClientError> {
        let mut ensemble = Vec::with_capacity(ensemble_size);
        for addr in &self.bookie_addrs {
            if !exclude.contains(addr) {
                ensemble.push(addr.clone());
                if ensemble.len() == ensemble_size {
                    break;
                }
            }
        }
        if ensemble.len() < ensemble_size {
            return Err(ClientError::Protocol(format!(
                "not enough bookies: need {}, available {} (excluding {})",
                ensemble_size,
                self.bookie_addrs.len(),
                exclude.len()
            )));
        }
        Ok(ensemble)
    }

    /// Create a new ledger. Allocates ID from ZK, selects ensemble, stores metadata.
    ///
    /// Matches Java's `BookKeeper.createLedger()`:
    /// 1. Allocate unique ledger ID via ZK sequential znode
    /// 2. Select bookies from available pool
    /// 3. Store initial metadata (OPEN state) in ZK
    /// 4. Return WriteHandle bound to the ensemble
    pub async fn create_ledger(
        &mut self,
        ensemble_size: i32,
        write_quorum: i32,
        ack_quorum: i32,
        digest_type: DigestType,
        password: &[u8],
    ) -> Result<(WriteHandle, Vec<Arc<BookieConnection>>), ClientError> {
        // Refresh bookie list
        self.refresh_bookies().await?;

        // Step 1: Allocate ledger ID
        let ledger_id = self
            .id_generator
            .generate_ledger_id()
            .await
            .map_err(|e| ClientError::Protocol(format!("allocate id: {e}")))?;

        // Step 2: Select ensemble
        let ensemble = self.select_ensemble(ensemble_size as usize, &[])?;

        // Step 3: Build and store metadata
        let meta_digest = to_meta_digest(digest_type);
        let ctime = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let metadata = LedgerMetadataBuilder::new()
            .ledger_id(ledger_id)
            .metadata_format_version(3)
            .ensemble_size(ensemble_size)
            .write_quorum_size(write_quorum)
            .ack_quorum_size(ack_quorum)
            .with_digest_type(meta_digest)
            .with_password(password.to_vec())
            .with_ctime(ctime)
            .new_ensemble_entry(0, ensemble.clone())
            .build()
            .map_err(|e| ClientError::Protocol(format!("build metadata: {e}")))?;

        self.ledger_manager
            .create_ledger_metadata(ledger_id, &metadata)
            .await
            .map_err(|e| ClientError::Protocol(format!("store metadata: {e}")))?;

        // Step 4: Build WriteHandle
        let schedule = RoundRobinDistributionSchedule::new(
            ensemble_size as usize,
            write_quorum as usize,
            ack_quorum as usize,
        );
        let dm = DigestManager::new(digest_type).with_password(password);
        let master_key = generate_master_key(password);

        let available = self.bookie_addrs.clone();
        let mut wh = WriteHandle::with_available_bookies(
            ledger_id,
            ensemble.clone(),
            available,
            schedule,
            dm,
            master_key,
        );

        // Wire ensemble change callback to update metadata in ZK
        let lm = self.ledger_manager.clone();
        let pw = password.to_vec();
        let meta_dt = meta_digest;
        wh.set_on_ensemble_change(Box::new(move |first_entry_id, new_ensemble| {
            let lm = lm.clone();
            let pw = pw.clone();
            let ensemble_owned: Vec<String> = new_ensemble.to_vec();
            // Spawn async metadata update (fire-and-forget from the callback context)
            tokio::spawn(async move {
                if let Err(e) = update_ensemble_metadata(
                    &lm, ledger_id, first_entry_id, &ensemble_owned, ensemble_size,
                    write_quorum, ack_quorum, meta_dt, &pw,
                )
                .await
                {
                    eprintln!("ensemble change metadata update failed: {e}");
                }
            });
        }));

        // Step 5: Get connections
        let conns = self.ensemble_connections(&ensemble).await?;

        Ok((wh, conns))
    }

    /// Close a ledger (seal it). Updates metadata to CLOSED state.
    ///
    /// Matches Java's `LedgerHandle.close()`:
    /// 1. Record final LAC and length
    /// 2. Update metadata in ZK to CLOSED state with CAS retry
    pub async fn close_ledger(&self, wh: &WriteHandle) -> Result<(), ClientError> {
        let ledger_id = wh.ledger_id();
        let lac = wh.last_add_confirmed();
        let length = wh.current_length();
        let segments = wh.ensemble_segments();

        // CAS retry loop matching Java's MetadataUpdateLoop
        for _ in 0..10 {
            let versioned = self
                .ledger_manager
                .read_ledger_metadata(ledger_id)
                .await
                .map_err(|e| ClientError::Protocol(format!("read metadata for close: {e}")))?;

            let version = versioned.version();
            let md = versioned.value();

            let closed_md = build_closed_metadata(md, ledger_id, lac, length, Some(&segments))?;

            match self.ledger_manager.write_ledger_metadata(ledger_id, &closed_md, version).await {
                Ok(_) => return Ok(()),
                Err(MetadataError::VersionConflict { .. }) => continue, // retry
                Err(e) => return Err(ClientError::Protocol(format!("close metadata: {e}"))),
            }
        }
        Err(ClientError::Protocol("close_ledger CAS failed after 10 retries".into()))
    }

    /// Open a ledger for reading. Reads metadata from ZK.
    ///
    /// Matches Java's `BookKeeper.openLedger()`:
    /// 1. Read ledger metadata from ZK
    /// 2. Extract ensemble, digest type, password verification
    /// 3. Return ReadHandle
    pub async fn open_ledger(
        &mut self,
        ledger_id: i64,
        digest_type: DigestType,
        password: &[u8],
    ) -> Result<(ReadHandle, Vec<Arc<BookieConnection>>), ClientError> {
        // Read metadata from ZK
        let versioned = self
            .ledger_manager
            .read_ledger_metadata(ledger_id)
            .await
            .map_err(|e| ClientError::Protocol(format!("read metadata: {e}")))?;

        let md = versioned.value();

        // Verify password matches (if metadata has one)
        if let Some(stored_pw) = md.password() {
            if stored_pw != password {
                return Err(ClientError::Protocol(
                    "password mismatch".into(),
                ));
            }
        }

        // Use the last ensemble for reading
        let ensembles = md.all_ensembles();
        let (_first_entry, ensemble) = ensembles
            .iter()
            .next_back()
            .ok_or_else(|| ClientError::Protocol("no ensembles in metadata".into()))?;

        let ensemble_size = md.ensemble_size() as usize;
        let write_quorum = md.write_quorum_size() as usize;
        let ack_quorum = md.ack_quorum_size() as usize;

        let schedule =
            RoundRobinDistributionSchedule::new(ensemble_size, write_quorum, ack_quorum);
        let dm = DigestManager::new(digest_type).with_password(password);
        let master_key = generate_master_key(password);

        let rh = ReadHandle::new(
            ledger_id,
            ensemble.clone(),
            schedule,
            dm,
            master_key,
        );

        let conns = self.ensemble_connections(ensemble).await?;

        Ok((rh, conns))
    }

    /// Open a ledger with recovery (fencing). Matches Java's `openLedger()` with recovery.
    ///
    /// 1. Read metadata from ZK
    /// 2. Fence the ledger on all bookies
    /// 3. Forward read to find true last entry
    /// 4. Update metadata to CLOSED
    /// 5. Return ReadHandle
    pub async fn open_ledger_with_recovery(
        &mut self,
        ledger_id: i64,
        digest_type: DigestType,
        password: &[u8],
    ) -> Result<(ReadHandle, Vec<Arc<BookieConnection>>), ClientError> {
        let versioned = self
            .ledger_manager
            .read_ledger_metadata(ledger_id)
            .await
            .map_err(|e| ClientError::Protocol(format!("read metadata: {e}")))?;

        let md = versioned.value();

        // If already closed, just return a read handle
        if md.state() == LedgerState::Closed {
            return self.open_ledger(ledger_id, digest_type, password).await;
        }

        // Get last ensemble for recovery
        let ensembles = md.all_ensembles();
        let (last_segment_entry, ensemble) = ensembles
            .iter()
            .next_back()
            .ok_or_else(|| ClientError::Protocol("no ensembles".into()))?;

        let ensemble_size = md.ensemble_size() as usize;
        let write_quorum = md.write_quorum_size() as usize;
        let ack_quorum = md.ack_quorum_size() as usize;

        let schedule =
            RoundRobinDistributionSchedule::new(ensemble_size, write_quorum, ack_quorum);
        let dm = DigestManager::new(digest_type).with_password(password);
        let master_key = generate_master_key(password);

        let conns = self.ensemble_connections(ensemble).await?;

        // Run recovery: fence → forward read → re-replicate
        let mut recovery = RecoveryHandle::new(
            ledger_id,
            ensemble.clone(),
            schedule.clone(),
            dm.clone(),
            master_key,
        );
        let (last_entry, total_length) =
            recovery.recover(&conns, *last_segment_entry).await?;

        // Update metadata to CLOSED with CAS retry
        for _ in 0..10 {
            let versioned = self
                .ledger_manager
                .read_ledger_metadata(ledger_id)
                .await
                .map_err(|e| ClientError::Protocol(format!("read metadata for recovery close: {e}")))?;

            let version = versioned.version();
            let md = versioned.value();

            let closed_md = build_closed_metadata(md, ledger_id, last_entry, total_length, None)?;

            match self.ledger_manager.write_ledger_metadata(ledger_id, &closed_md, version).await {
                Ok(_) => {
                    let rh = ReadHandle::new(ledger_id, ensemble.clone(), schedule, dm, master_key);
                    return Ok((rh, conns));
                }
                Err(MetadataError::VersionConflict { .. }) => continue,
                Err(e) => return Err(ClientError::Protocol(format!("close after recovery: {e}"))),
            }
        }
        Err(ClientError::Protocol("recovery close CAS failed after 10 retries".into()))
    }

    /// Delete a ledger. Removes metadata from ZK.
    pub async fn delete_ledger(&self, ledger_id: i64) -> Result<(), ClientError> {
        // Read to get version for version-checked delete
        let versioned = self
            .ledger_manager
            .read_ledger_metadata(ledger_id)
            .await
            .map_err(|e| ClientError::Protocol(format!("read for delete: {e}")))?;

        self.ledger_manager
            .remove_ledger_metadata(ledger_id, versioned.version())
            .await
            .map_err(|e| ClientError::Protocol(format!("delete ledger: {e}")))?;

        Ok(())
    }

    /// Get the underlying LedgerManager (for advanced use).
    pub fn ledger_manager(&self) -> &Arc<ZkLedgerManager> {
        &self.ledger_manager
    }

    /// Get the ZkPaths (for advanced use).
    pub fn paths(&self) -> &ZkPaths {
        &self.paths
    }
}

/// Build CLOSED metadata from existing metadata, preserving all fields.
/// If `segments_override` is Some, uses those segments; otherwise copies from existing metadata.
fn build_closed_metadata(
    md: &bookkeeper_metadata::ledger_metadata::LedgerMetadata,
    ledger_id: i64,
    last_entry_id: i64,
    length: i64,
    segments_override: Option<&std::collections::BTreeMap<i64, Vec<String>>>,
) -> Result<bookkeeper_metadata::ledger_metadata::LedgerMetadata, ClientError> {
    let mut builder = LedgerMetadataBuilder::new()
        .ledger_id(ledger_id)
        .metadata_format_version(3)
        .ensemble_size(md.ensemble_size())
        .write_quorum_size(md.write_quorum_size())
        .ack_quorum_size(md.ack_quorum_size());

    if let Some(dt) = md.digest_type() {
        builder = builder.with_digest_type(dt);
    }
    if let Some(pw) = md.password() {
        builder = builder.with_password(pw.to_vec());
    }
    if md.store_ctime() {
        builder = builder.with_ctime(md.ctime());
    }

    // Copy ensemble segments
    if let Some(segments) = segments_override {
        for (first_entry, ensemble) in segments {
            builder = builder.new_ensemble_entry(*first_entry, ensemble.clone());
        }
    } else {
        for (first_entry, ensemble) in md.all_ensembles() {
            builder = builder.new_ensemble_entry(*first_entry, ensemble.clone());
        }
    }

    builder = builder.with_closed_state(last_entry_id, length);

    builder
        .build()
        .map_err(|e| ClientError::Protocol(format!("build closed metadata: {e}")))
}

/// Update ledger metadata in ZK when an ensemble change occurs.
/// Reads current metadata, adds the new segment, writes back with CAS.
async fn update_ensemble_metadata(
    lm: &ZkLedgerManager,
    ledger_id: i64,
    first_entry_id: i64,
    new_ensemble: &[String],
    ensemble_size: i32,
    write_quorum: i32,
    ack_quorum: i32,
    digest_type: MetaDigestType,
    password: &[u8],
) -> Result<(), MetadataError> {
    // CAS retry loop
    for _ in 0..10 {
        let versioned = lm.read_ledger_metadata(ledger_id).await?;
        let md = versioned.value();
        let version = versioned.version();

        let mut builder = LedgerMetadataBuilder::new()
            .ledger_id(ledger_id)
            .metadata_format_version(3)
            .ensemble_size(ensemble_size)
            .write_quorum_size(write_quorum)
            .ack_quorum_size(ack_quorum)
            .with_digest_type(digest_type)
            .with_password(password.to_vec());

        // Copy existing segments
        for (first_entry, ens) in md.all_ensembles() {
            builder = builder.new_ensemble_entry(*first_entry, ens.clone());
        }
        // Add new segment
        builder = builder.new_ensemble_entry(first_entry_id, new_ensemble.to_vec());

        let new_md = builder.build().map_err(|e| {
            MetadataError::SerializationError(format!("build ensemble change metadata: {e}"))
        })?;

        match lm.write_ledger_metadata(ledger_id, &new_md, version).await {
            Ok(_) => return Ok(()),
            Err(MetadataError::VersionConflict { .. }) => continue, // retry
            Err(e) => return Err(e),
        }
    }
    Err(MetadataError::ZkError(
        "ensemble change CAS failed after 10 retries".into(),
    ))
}

/// Convert protocol DigestType to metadata DigestType.
fn to_meta_digest(dt: DigestType) -> MetaDigestType {
    match dt {
        DigestType::Crc32 => MetaDigestType::Crc32,
        DigestType::Crc32c => MetaDigestType::Crc32c,
        DigestType::HmacSha1 => MetaDigestType::Hmac,
        DigestType::Dummy => MetaDigestType::Dummy,
    }
}
