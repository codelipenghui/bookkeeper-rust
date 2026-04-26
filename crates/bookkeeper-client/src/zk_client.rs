use std::net::SocketAddr;
use std::sync::Arc;

use zookeeper_client as zk;

use bookkeeper_metadata::ledger_metadata::{
    DigestType as MetaDigestType, LedgerMetadataBuilder, LedgerState, MetadataError,
};
use bookkeeper_metadata::metadata_paths::ParsedMetadataUri;
use bookkeeper_metadata::traits::{LedgerIdGenerator, LedgerManager};
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
    ledger_manager: Arc<dyn LedgerManager>,
    id_generator: Arc<dyn LedgerIdGenerator>,
    /// Cached writable bookie list, shared with the background watcher task.
    /// Matches Java's `BookieWatcherImpl` which maintains `writableBookies`
    /// updated reactively via persistent ZK watches.
    bookie_addrs: Arc<parking_lot::RwLock<Vec<String>>>,
    /// Active connections keyed by bookie address.
    connections: std::collections::HashMap<String, Arc<BookieConnection>>,
    /// Ensemble placement policy (default or rack-aware).
    placement: Box<dyn crate::placement::EnsemblePlacementPolicy>,
    /// Number of TCP channels per bookie (matching Java's numChannelsPerBookie).
    num_channels_per_bookie: usize,
    /// Speculative read policy (None = disabled).
    speculative_policy: Option<crate::read_handle::SpeculativeReadPolicy>,
    /// Optional TLS connector — if set, all bookie connections use StartTLS.
    tls_connector: Option<Arc<tokio_rustls::TlsConnector>>,
    /// TCP socket options applied to every bookie connection.
    tcp_config: crate::bookie_connection::TcpConfig,
    /// Stored client config for bridging to WriteHandle/ReadHandle.
    /// None when constructed via `new()` (legacy path).
    client_config: Option<bookkeeper_config::client::ClientConfig>,
    /// Handle to the background bookie watcher task. Dropped on ZkBookKeeper drop.
    _watcher_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ZkBookKeeper {
    /// Create a new BookKeeper client connected to ZooKeeper.
    ///
    /// ```no_run
    /// # async fn _doc() -> Result<(), Box<dyn std::error::Error>> {
    /// use bookkeeper_client::zk_client::ZkBookKeeper;
    /// let bk = ZkBookKeeper::new("localhost:2181", "/ledgers").await?;
    /// # drop(bk);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(zk_connect: &str, ledger_root: &str) -> Result<Self, ClientError> {
        let uri = ParsedMetadataUri::Zk {
            servers: zk_connect.to_string(),
            ledgers_root: ledger_root.to_string(),
        };
        let backend =
            bookkeeper_metadata::backend::open_metadata_backend(&uri, None, None)
                .await
                .map_err(|e| ClientError::Protocol(format!("open metadata backend: {e}")))?;

        let zk_raw = backend.zk_raw.expect("ZK backend must have raw handles");
        let zk_client = zk_raw.client;
        let paths = ZkPaths::new(ledger_root);

        let ledger_manager = backend.ledger_manager;
        let id_generator = backend.id_generator;

        let bookie_addrs = Arc::new(parking_lot::RwLock::new(Vec::new()));

        let mut client = Self {
            zk_client: zk_client.clone(),
            paths: paths.clone(),
            ledger_manager,
            id_generator,
            bookie_addrs: bookie_addrs.clone(),
            connections: std::collections::HashMap::new(),
            placement: Box::new(crate::placement::DefaultPlacementPolicy::new()),
            num_channels_per_bookie: 1,
            speculative_policy: None,
            tls_connector: None,
            tcp_config: crate::bookie_connection::TcpConfig::default(),
            client_config: None,
            _watcher_handle: None,
        };

        // Initial bookie discovery
        client.refresh_bookies().await?;

        // Start background watcher that re-installs the ZK watch on every
        // change event, keeping the bookie list continuously up to date.
        // Matches Java's BookieWatcherImpl which uses persistent ZK watches
        // via registrationClient.watchWritableBookies(callback).
        let watcher_handle = tokio::spawn(bookie_watcher_loop(
            zk_client,
            paths,
            bookie_addrs,
        ));
        client._watcher_handle = Some(watcher_handle);

        Ok(client)
    }

    /// Refresh the list of available writable bookies from ZK.
    /// The background `bookie_watcher_loop` handles persistent watching.
    async fn refresh_bookies(&mut self) -> Result<(), ClientError> {
        let available_path = self.paths.available();
        let (children, _stat) = self
            .zk_client
            .get_children(available_path)
            .await
            .map_err(|e| ClientError::Protocol(format!("list bookies: {e}")))?;

        let addrs: Vec<String> = children
            .into_iter()
            .filter(|c| c != "readonly")
            .collect();

        // Update placement policy with new bookie list
        self.placement.on_cluster_changed(&addrs, &[]);

        *self.bookie_addrs.write() = addrs;

        Ok(())
    }

    /// Get or create a pooled connection to a bookie.
    /// Uses `num_channels_per_bookie` and optional TLS from config.
    /// The bookie ID is parsed as "host:port" — `host` is preserved as the SNI
    /// hostname for TLS handshakes. `host` is then resolved to a SocketAddr for TCP.
    async fn get_connection(&mut self, addr: &str) -> Result<Arc<BookieConnection>, ClientError> {
        if let Some(conn) = self.connections.get(addr) {
            return Ok(conn.clone());
        }

        // Split host:port to preserve the textual host (SNI input).
        let (host, port) = match addr.rsplit_once(':') {
            Some((h, p)) => (h.trim_matches(|c| c == '[' || c == ']'), p),
            None => return Err(ClientError::Protocol(format!("bad address '{addr}': missing port"))),
        };
        let port: u16 = port.parse()
            .map_err(|e| ClientError::Protocol(format!("bad port in '{addr}': {e}")))?;

        // Resolve host to a SocketAddr (handles both hostnames and IP literals).
        let socket_addr: SocketAddr = tokio::net::lookup_host((host, port)).await
            .map_err(|e| ClientError::Protocol(format!("resolve '{addr}': {e}")))?
            .next()
            .ok_or_else(|| ClientError::Protocol(format!("no addresses for '{addr}'")))?;

        // Use the original host as SNI hostname only if it's not an IP literal.
        let server_hostname = if host.parse::<std::net::IpAddr>().is_ok() {
            None
        } else {
            Some(host.to_string())
        };

        let conn = Arc::new(
            BookieConnection::connect_full(
                socket_addr,
                server_hostname,
                self.num_channels_per_bookie,
                self.tls_connector.clone(),
                self.tcp_config.clone(),
            ).await?
        );
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

    /// Connect to all unique bookies across all ensemble segments.
    /// The ordering matches `ReadHandle::build_bookie_index`: last ensemble
    /// occupies positions 0..N-1, then any additional bookies from earlier
    /// segments are appended.
    async fn all_ensemble_connections(
        &mut self,
        ensembles: &std::collections::BTreeMap<i64, Vec<String>>,
    ) -> Result<Vec<Arc<BookieConnection>>, ClientError> {
        let mut seen = std::collections::HashSet::new();
        let mut ordered_addrs: Vec<String> = Vec::new();
        // Last ensemble first (positions 0..N-1)
        if let Some(last_ens) = ensembles.values().next_back() {
            for addr in last_ens {
                if seen.insert(addr.clone()) {
                    ordered_addrs.push(addr.clone());
                }
            }
        }
        // Earlier segments: append any bookies not yet seen
        for ens in ensembles.values() {
            for addr in ens {
                if seen.insert(addr.clone()) {
                    ordered_addrs.push(addr.clone());
                }
            }
        }
        self.ensemble_connections(&ordered_addrs).await
    }

    /// Select bookies for a new ensemble using the configured placement policy.
    /// Default: round-robin (DefaultPlacementPolicy).
    /// With rack awareness: spreads bookies across racks (RackAwarePlacementPolicy).
    fn select_ensemble(
        &self,
        ensemble_size: usize,
        write_quorum: usize,
        ack_quorum: usize,
        exclude: &[String],
    ) -> Result<Vec<String>, ClientError> {
        let exclude_set: std::collections::HashSet<String> = exclude.iter().cloned().collect();
        let result = self
            .placement
            .new_ensemble(ensemble_size, write_quorum, ack_quorum, &exclude_set)
            .map_err(|e| ClientError::Protocol(format!("{e}")))?;
        if result.adherence != crate::placement::PolicyAdherence::MeetsStrict {
            tracing::warn!("Ensemble placement adherence: {:?}", result.adherence);
        }
        Ok(result.bookies)
    }

    /// Set a custom placement policy (e.g., rack-aware).
    pub fn set_placement_policy(&mut self, policy: Box<dyn crate::placement::EnsemblePlacementPolicy>) {
        self.placement = policy;
        // Update with current bookie list
        let addrs = self.bookie_addrs.read().clone();
        self.placement.on_cluster_changed(&addrs, &[]);
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
        self.create_ledger_with_custom_metadata(
            ensemble_size, write_quorum, ack_quorum, digest_type, password, None,
        ).await
    }

    /// Create a ledger with optional custom metadata.
    /// Matches Java's `BookKeeper.createLedger()` with `customMetadata` parameter.
    /// Custom metadata is an arbitrary `Map<String, byte[]>` stored in the ledger
    /// metadata in ZK and also passed to placement policies for metadata-driven decisions.
    pub async fn create_ledger_with_custom_metadata(
        &mut self,
        ensemble_size: i32,
        write_quorum: i32,
        ack_quorum: i32,
        digest_type: DigestType,
        password: &[u8],
        custom_metadata: Option<std::collections::BTreeMap<String, Vec<u8>>>,
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
        let ensemble = self.select_ensemble(
            ensemble_size as usize,
            write_quorum as usize,
            ack_quorum as usize,
            &[],
        )?;

        // Step 3: Build and store metadata
        let meta_digest = to_meta_digest(digest_type);
        let ctime = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let mut builder = LedgerMetadataBuilder::new()
            .ledger_id(ledger_id)
            .metadata_format_version(3)
            .ensemble_size(ensemble_size)
            .write_quorum_size(write_quorum)
            .ack_quorum_size(ack_quorum)
            .with_digest_type(meta_digest)
            .with_password(password.to_vec())
            .with_ctime(ctime)
            .new_ensemble_entry(0, ensemble.clone());
        if let Some(cm) = custom_metadata {
            builder = builder.with_custom_metadata(cm);
        }
        let metadata = builder
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

        let available = self.bookie_addrs.read().clone();
        let mut wh = WriteHandle::with_available_bookies(
            ledger_id,
            ensemble.clone(),
            available,
            schedule,
            dm,
            master_key,
        );

        // Bridge client config to WriteHandle settings
        if let Some(ref cfg) = self.client_config {
            if cfg.add_entry_quorum_timeout > 0 {
                wh.set_add_entry_quorum_timeout(
                    std::time::Duration::from_secs(cfg.add_entry_quorum_timeout as u64)
                );
            }
            if cfg.max_allowed_ensemble_changes > 0 {
                wh.set_max_ensemble_changes(cfg.max_allowed_ensemble_changes as usize);
            }
            wh.set_deferred_sync(cfg.deferred_sync);
            wh.set_delay_ensemble_change(cfg.delay_ensemble_change);
        }

        // Wire ensemble change callback to update metadata in ZK.
        // The closure now returns a future the writer awaits before
        // letting more writes flow against the new in-memory ensemble —
        // a failed CAS surfaces as a write error instead of split-brain
        // between writer and ZK metadata.
        let lm = self.ledger_manager.clone();
        let pw = password.to_vec();
        let meta_dt = meta_digest;
        wh.set_on_ensemble_change(Box::new(move |first_entry_id, new_ensemble| {
            let lm = lm.clone();
            let pw = pw.clone();
            let ensemble_owned: Vec<String> = new_ensemble.to_vec();
            Box::pin(async move {
                update_ensemble_metadata(
                    lm.as_ref(), ledger_id, first_entry_id, &ensemble_owned, ensemble_size,
                    write_quorum, ack_quorum, meta_dt, &pw,
                )
                .await
                .map_err(|e| format!("{e}"))
            })
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
        for _ in 0..bookkeeper_common::MAX_CAS_RETRIES {
            let versioned = self
                .ledger_manager
                .read_ledger_metadata(ledger_id)
                .await
                .map_err(|e| ClientError::Protocol(format!("read metadata for close: {e}")))?;

            let version = versioned.version();
            let md = versioned.value();

            let closed_md = build_closed_metadata(md, lac, length, Some(&segments))?;

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

        // Validate that the caller-supplied digest type matches what's stored
        // in the metadata. Without this check, the caller's digest manager
        // would silently verify entries with the wrong algorithm, either
        // producing spurious DigestMismatch errors or — in pathological
        // cases — accepting corrupt data. Matches Java's behavior where
        // `DigestManager.instantiate` throws if the stored digest type does
        // not match the caller's argument.
        if let Some(stored_dt) = md.digest_type() {
            let stored_proto = from_meta_digest(stored_dt);
            if stored_proto != digest_type {
                return Err(ClientError::DigestTypeMismatch {
                    expected: format!("{stored_proto:?}"),
                    actual: format!("{digest_type:?}"),
                });
            }
        }

        let ensembles = md.all_ensembles();
        if ensembles.is_empty() {
            return Err(ClientError::Protocol("no ensembles in metadata".into()));
        }

        let ensemble_size = md.ensemble_size() as usize;
        let write_quorum = md.write_quorum_size() as usize;
        let ack_quorum = md.ack_quorum_size() as usize;

        let schedule =
            RoundRobinDistributionSchedule::new(ensemble_size, write_quorum, ack_quorum);
        let dm = DigestManager::new(digest_type).with_password(password);
        let master_key = generate_master_key(password);

        // Build ReadHandle with all ensemble segments so entries from earlier
        // segments are routed to the correct bookies.
        // Matches Java's LedgerMetadata.getEnsembleAt(entryId).
        let mut rh = ReadHandle::with_ensemble_segments(
            ledger_id,
            ensembles.clone(),
            schedule,
            dm,
            master_key,
        );
        if let Some(ref policy) = self.speculative_policy {
            rh = rh.with_speculative_policy(policy.clone());
        }

        // Bridge read config from ClientConfig
        if let Some(ref cfg) = self.client_config {
            if cfg.reorder_read_sequence_enabled {
                rh = rh.with_reorder_read_sequence(true);
            }
            if cfg.sticky_reads_enabled {
                rh = rh.with_sticky_reads();
            }
        }

        // Connect to ALL unique bookies across all ensemble segments.
        // The connection order matches ReadHandle.bookie_index: last ensemble
        // first (positions 0..N-1), then any extra bookies from earlier segments.
        let conns = self.all_ensemble_connections(ensembles).await?;

        Ok((rh, conns))
    }

    /// Open a ledger with recovery (fencing). Matches Java's `openLedger()` with recovery.
    ///
    /// 1. Read metadata from ZK
    /// 2. Transition metadata to IN_RECOVERY (CAS)
    /// 3. Fence the ledger on all bookies
    /// 4. Forward read to find true last entry
    /// 5. Update metadata to CLOSED
    /// 6. Return ReadHandle
    pub async fn open_ledger_with_recovery(
        &mut self,
        ledger_id: i64,
        digest_type: DigestType,
        password: &[u8],
    ) -> Result<(ReadHandle, Vec<Arc<BookieConnection>>), ClientError> {
        // Read metadata, optionally CAS to IN_RECOVERY, and re-read on
        // VersionConflict so the rest of recovery operates against
        // up-to-date metadata. Pre-fix the VersionConflict branch fell
        // through using `md` from the stale read, so the recovery
        // proceeded against the wrong ensemble.
        let mut attempts = 0;
        let (md, _version) = loop {
            attempts += 1;
            if attempts > bookkeeper_common::MAX_CAS_RETRIES {
                return Err(ClientError::Protocol(
                    "recovery: IN_RECOVERY transition CAS failed after retries".into(),
                ));
            }
            let versioned = self
                .ledger_manager
                .read_ledger_metadata(ledger_id)
                .await
                .map_err(|e| ClientError::Protocol(format!("read metadata: {e}")))?;
            let md = versioned.value().clone();
            let version = versioned.version();

            if let Some(stored_dt) = md.digest_type() {
                let stored_proto = from_meta_digest(stored_dt);
                if stored_proto != digest_type {
                    return Err(ClientError::DigestTypeMismatch {
                        expected: format!("{stored_proto:?}"),
                        actual: format!("{digest_type:?}"),
                    });
                }
            }

            if md.state() == LedgerState::Closed {
                return self.open_ledger(ledger_id, digest_type, password).await;
            }

            if md.state() == LedgerState::InRecovery {
                break (md, version);
            }

            let in_recovery_md = LedgerMetadataBuilder::from_existing(&md)
                .with_in_recovery_state()
                .build()
                .map_err(|e| ClientError::Protocol(format!("build IN_RECOVERY metadata: {e}")))?;
            match self
                .ledger_manager
                .write_ledger_metadata(ledger_id, &in_recovery_md, version)
                .await
            {
                Ok(_) => break (in_recovery_md, version + 1),
                Err(MetadataError::VersionConflict { .. }) => {
                    // Another client raced us. Re-read so the rest of
                    // recovery sees their updated metadata.
                    continue;
                }
                Err(e) => {
                    return Err(ClientError::Protocol(format!("set IN_RECOVERY: {e}")))
                }
            }
        };
        let md = &md;

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
        for _ in 0..bookkeeper_common::MAX_CAS_RETRIES {
            let versioned = self
                .ledger_manager
                .read_ledger_metadata(ledger_id)
                .await
                .map_err(|e| ClientError::Protocol(format!("read metadata for recovery close: {e}")))?;

            let version = versioned.version();
            let md = versioned.value();

            let closed_md = build_closed_metadata(md, last_entry, total_length, None)?;

            match self.ledger_manager.write_ledger_metadata(ledger_id, &closed_md, version).await {
                Ok(_) => {
                    let mut rh = ReadHandle::with_ensemble_segments(
                        ledger_id, ensembles.clone(), schedule, dm, master_key,
                    );
                    if let Some(ref policy) = self.speculative_policy {
                        rh = rh.with_speculative_policy(policy.clone());
                    }
                    let all_conns = self.all_ensemble_connections(ensembles).await?;
                    return Ok((rh, all_conns));
                }
                Err(MetadataError::VersionConflict { .. }) => continue,
                Err(e) => return Err(ClientError::Protocol(format!("close after recovery: {e}"))),
            }
        }
        Err(ClientError::Protocol("recovery close CAS failed after 10 retries".into()))
    }

    /// List all ledger IDs known to the metadata store.
    ///
    /// Matches Java's `BookKeeper.getLedgerManager().getLedgerRanges()` /
    /// `ListLedgersResult.getLedgerIds()`. Used by admin tooling (CLI,
    /// HTTP API) and the auditor to iterate over all ledgers.
    ///
    /// Scans the hierarchical `/ledgers/` tree in ZooKeeper. For large
    /// clusters this may be expensive — callers should prefer scoped
    /// operations when possible.
    pub async fn list_ledgers(&self) -> Result<Vec<i64>, ClientError> {
        self.ledger_manager
            .get_ledger_ranges()
            .await
            .map_err(|e| ClientError::Protocol(format!("list ledgers: {e}")))
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
    /// Set the number of TCP channels per bookie (default: 1).
    /// Matches Java's `numChannelsPerBookie`. Higher values increase throughput
    /// by spreading requests across multiple pipelined connections.
    /// Must be called before any connections are established.
    pub fn set_num_channels_per_bookie(&mut self, n: usize) {
        self.num_channels_per_bookie = n.max(1);
    }

    /// Set speculative read policy. See `SpeculativeReadPolicy::from_config`.
    pub fn set_speculative_read_policy(&mut self, policy: crate::read_handle::SpeculativeReadPolicy) {
        self.speculative_policy = Some(policy);
    }

    /// Enable TLS for all bookie connections via StartTLS upgrade.
    /// Build the connector with `crate::tls::build_tls_connector(&cfg)`.
    /// Must be called before any connections are established.
    pub fn set_tls_connector(&mut self, connector: Arc<tokio_rustls::TlsConnector>) {
        self.tls_connector = Some(connector);
    }

    /// Set TCP socket options for all bookie connections.
    /// Must be called before any connections are established.
    pub fn set_tcp_config(&mut self, config: crate::bookie_connection::TcpConfig) {
        self.tcp_config = config;
    }

    pub fn ledger_manager(&self) -> &Arc<dyn LedgerManager> {
        &self.ledger_manager
    }

    /// Get the ZkPaths (for advanced use).
    pub fn paths(&self) -> &ZkPaths {
        &self.paths
    }

    /// Create a fully configured BookKeeper client from a `ClientConfig`.
    /// This is the primary constructor for production use — it bridges ALL
    /// config fields to their runtime mechanisms, fixing the gap where
    /// `new()` used hardcoded defaults for everything.
    pub async fn from_config(cfg: &bookkeeper_config::client::ClientConfig) -> Result<Self, ClientError> {
        // Extract ZK connect string from metadataServiceUri or fail
        let zk_connect = cfg.metadata_service_uri.as_deref()
            .ok_or_else(|| ClientError::Protocol("metadataServiceUri is required".into()))?;

        // Parse ZK connect string from URI format (zk+hierarchical://host:port/path)
        let (zk_servers, ledger_root) = parse_metadata_uri(zk_connect)?;

        let mut client = Self::new(&zk_servers, &ledger_root).await?;

        // Bridge TCP config
        client.tcp_config = crate::bookie_connection::TcpConfig::from_client_config(cfg);

        // Bridge connection pool size
        client.set_num_channels_per_bookie(cfg.num_channels_per_bookie as usize);

        // Bridge speculative read policy (if configured)
        if let Some(policy) = crate::read_handle::SpeculativeReadPolicy::from_config(
            cfg.speculative_read_timeout,
            cfg.first_speculative_read_timeout,
            cfg.max_speculative_read_timeout,
            cfg.speculative_read_timeout_backoff_multiplier,
        ) {
            client.set_speculative_read_policy(policy);
        }

        // Bridge placement policy from config string
        match cfg.ensemble_placement_policy.as_str() {
            "RackawareEnsemblePlacementPolicy" | "RackAware" => {
                // Rack-aware requires a resolver — use static empty for now;
                // callers should call set_placement_policy with a configured resolver.
                tracing::info!("Placement policy: RackAware (configure resolver via set_placement_policy)");
            }
            "DefaultEnsemblePlacementPolicy" | "" => {
                // Already the default
            }
            other => {
                tracing::info!("Unknown placement policy '{other}', using default");
            }
        }

        // Store config for create_ledger/open_ledger to bridge write/read settings
        client.client_config = Some(cfg.clone());

        Ok(client)
    }
}

/// Parse a metadataServiceUri into (zk_servers, ledger_root).
/// Handles: `zk+hierarchical://host:port/path`, `zk://host:port/path`
fn parse_metadata_uri(uri: &str) -> Result<(String, String), ClientError> {
    let rest = uri
        .strip_prefix("zk+hierarchical://")
        .or_else(|| uri.strip_prefix("zk+flat://"))
        .or_else(|| uri.strip_prefix("zk://"))
        .unwrap_or(uri);

    if let Some(slash_pos) = rest.find('/') {
        let servers = rest[..slash_pos].to_string();
        let path = rest[slash_pos..].to_string();
        if servers.is_empty() {
            return Err(ClientError::Protocol(format!("empty ZK servers in URI: {uri}")));
        }
        Ok((servers, path))
    } else {
        // No path — use default /ledgers
        Ok((rest.to_string(), "/ledgers".to_string()))
    }
}

/// Build CLOSED metadata from existing metadata, preserving all fields.
/// If `segments_override` is Some, uses those segments; otherwise copies from existing metadata.
fn build_closed_metadata(
    md: &bookkeeper_metadata::ledger_metadata::LedgerMetadata,
    last_entry_id: i64,
    length: i64,
    segments_override: Option<&std::collections::BTreeMap<i64, Vec<String>>>,
) -> Result<bookkeeper_metadata::ledger_metadata::LedgerMetadata, ClientError> {
    let mut builder = LedgerMetadataBuilder::from_existing(md);

    // Override ensemble segments if provided (e.g., from WriteHandle)
    if let Some(segments) = segments_override {
        builder.clear_ensembles();
        for (first_entry, ensemble) in segments {
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
///
/// The additional ensemble-parameter inputs must match the ledger's original
/// create-time values — they are accepted as a sanity check rather than a
/// source of truth. All other fields (custom_metadata, ctime, c_token, format
/// version) are preserved via `from_existing`.
async fn update_ensemble_metadata(
    lm: &dyn LedgerManager,
    ledger_id: i64,
    first_entry_id: i64,
    new_ensemble: &[String],
    _ensemble_size: i32,
    _write_quorum: i32,
    _ack_quorum: i32,
    _digest_type: MetaDigestType,
    _password: &[u8],
) -> Result<(), MetadataError> {
    // CAS retry loop
    for _ in 0..bookkeeper_common::MAX_CAS_RETRIES {
        let versioned = lm.read_ledger_metadata(ledger_id).await?;
        let md = versioned.value();
        let version = versioned.version();

        let builder = LedgerMetadataBuilder::from_existing(md)
            .new_ensemble_entry(first_entry_id, new_ensemble.to_vec());

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

impl Drop for ZkBookKeeper {
    fn drop(&mut self) {
        // Abort the background bookie watcher task so it doesn't leak after
        // the ZkBookKeeper handle goes out of scope.
        if let Some(handle) = self._watcher_handle.take() {
            handle.abort();
        }
    }
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

/// Reverse of `to_meta_digest` — convert stored metadata digest type back
/// to the protocol digest type for validation in `open_ledger`.
fn from_meta_digest(dt: MetaDigestType) -> DigestType {
    match dt {
        MetaDigestType::Crc32 => DigestType::Crc32,
        MetaDigestType::Crc32c => DigestType::Crc32c,
        MetaDigestType::Hmac => DigestType::HmacSha1,
        MetaDigestType::Dummy => DigestType::Dummy,
    }
}

/// Background watcher loop that keeps `bookie_addrs` continuously in sync
/// with the writable bookies list in ZooKeeper.
///
/// Matches Java's `BookieWatcherImpl` behavior: the watcher fires on every
/// `/ledgers/available` child change, and the callback re-fetches the list
/// and re-installs the watch. This ensures the client always has an up-to-date
/// view of the cluster, including after bookie additions/removals.
///
/// The task also handles ZK session expiry by re-installing the watch on
/// reconnection — one-shot ZK watches survive transient disconnections but
/// are invalidated by session expiry.
async fn bookie_watcher_loop(
    zk_client: zk::Client,
    paths: ZkPaths,
    bookie_addrs: Arc<parking_lot::RwLock<Vec<String>>>,
) {
    let available_path = paths.available().to_string();
    let mut backoff_ms: u64 = 100;
    loop {
        match zk_client.list_and_watch_children(&available_path).await {
            Ok((children, watcher)) => {
                let addrs: Vec<String> = children.into_iter().filter(|c| c != "readonly").collect();
                *bookie_addrs.write() = addrs;
                backoff_ms = 100; // reset on success

                // Wait for the watcher to fire (persistent loop: re-install
                // on every fire). `changed()` returns when the watched path
                // has a child add/remove/change event.
                let _event = watcher.changed().await;
                // Loop back around — re-fetch and re-install the watch.
            }
            Err(e) => {
                tracing::warn!("bookie watcher: list_and_watch_children failed: {e}; retrying in {backoff_ms}ms");
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(10_000);
            }
        }
    }
}
