use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_util::codec::{Decoder, Encoder};

use bookkeeper_protocol::framing::BookieCodec;
use bookkeeper_protocol::v2::request::decode_request;
use bookkeeper_protocol::v2::response::encode_response;
use bookkeeper_storage::db_ledger_storage::DbLedgerStorage;

use bookkeeper_metadata::traits::RegistrationManager;

use bookkeeper_config::server::ServerConfig;
use crate::http_api;
use crate::processor::RequestProcessor;
use crate::state::{BookieState, BookieStateManager};

/// Filename for unclean shutdown detection, matching Java's UncleanShutdownDetectionImpl.
const DIRTY_FILENAME: &str = "DIRTY";

pub struct BookieServer {
    local_addr: SocketAddr,
    http_addr: Option<SocketAddr>,
    shutdown_tx: watch::Sender<bool>,
    accept_handle: tokio::task::JoinHandle<()>,
    storage: Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage>,
    state: Arc<BookieStateManager>,
    zk_reg: Option<(bookkeeper_metadata::zk::registration::ZkRegistrationManager, String)>,
    auditor_handle: Option<tokio::task::JoinHandle<()>>,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
    gc_handle: Option<tokio::task::JoinHandle<()>>,
    disk_monitor_handle: Option<tokio::task::JoinHandle<()>>,
    sync_handle: Option<tokio::task::JoinHandle<()>>,
    session_watcher_handle: Option<tokio::task::JoinHandle<()>>,
    /// Ledger directories — used to create/remove DIRTY files.
    ledger_directories: Vec<String>,
}

impl BookieServer {
    pub async fn start(config: ServerConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Bind TCP listener. Honor `listeningInterface` if set so the bookie
        // can register a routable address with ZK; default to 0.0.0.0.
        let bind_ip = config
            .listening_interface
            .clone()
            .unwrap_or_else(|| "0.0.0.0".to_string());
        let addr = format!("{bind_ip}:{}", config.bookie_port);
        let listener = TcpListener::bind(&addr).await?;
        let local_addr = listener.local_addr()?;

        // Open storage using first ledger dir
        let ledger_dir = config
            .ledger_directories
            .first()
            .ok_or("no ledger dirs configured")?;
        let mut storage = DbLedgerStorage::open(Path::new(ledger_dir))?;

        // Initialize journal for write-ahead logging (crash recovery)
        let journal_config = bookkeeper_storage::journal::writer::JournalConfig {
            journal_dir: std::path::PathBuf::from(&config.journal_directory),
            max_file_size: (config.journal_max_size_mb as u64) * 1024 * 1024,
            sync_data: config.journal_sync_data,
            max_group_wait_ms: config.journal_max_group_wait_ms as u64,
            buffered_writes_threshold: config.journal_buffered_writes_threshold as usize,
            buffered_entries_threshold: config.journal_buffered_entries_threshold as usize,
            alignment_size: config.journal_alignment_size as usize,
            format_version: config.journal_format_version_to_write,
        };
        let replayed = storage.init_journal(
            Path::new(&config.journal_directory),
            journal_config,
        )?;
        if replayed > 0 {
            tracing::info!("Journal recovery: replayed {replayed} entries");
        }

        // Check for unclean shutdown (DIRTY file in any ledger directory).
        // Java defaults to unclean=true on any I/O error (fail-safe).
        let was_unclean = config.ledger_directories.iter().any(|dir| {
            let dirty_path = Path::new(dir).join(DIRTY_FILENAME);
            match dirty_path.try_exists() {
                Ok(exists) => exists,
                Err(e) => {
                    tracing::error!("Failed to check DIRTY file in {dir}: {e} — assuming unclean");
                    true // fail-safe: assume unclean
                }
            }
        });
        if was_unclean {
            tracing::info!("Unclean shutdown detected (DIRTY file present). Journal replay completed.");
        }

        let storage: Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage> =
            Arc::new(storage);

        // Create DIRTY file in each ledger directory — signals unclean shutdown
        // if the process crashes before clean shutdown removes it.
        // Matches Java: written AFTER bookie.start() succeeds, BEFORE accepting connections.
        // Java aborts startup if DIRTY creation fails (re-throws IOException).
        for dir in &config.ledger_directories {
            let dirty_path = Path::new(dir).join(DIRTY_FILENAME);
            std::fs::File::create(&dirty_path)
                .map_err(|e| format!("Failed to create DIRTY file in {dir}: {e}"))?;
        }

        // Create state manager and transition to Running
        let state = Arc::new(BookieStateManager::new());
        state.transition_to(BookieState::Running);
        crate::metrics::BOOKIE_STATE.set(1); // 1 = writable/running

        // Initialize Prometheus metrics
        crate::metrics::register_metrics();

        // Create request processor
        let storage_for_shutdown = storage.clone();
        let processor = Arc::new(RequestProcessor::new(storage.clone(), state.clone()));

        // Shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let max_frame_size = config.max_frame_size;

        // Optionally register with ZooKeeper
        let mut auditor_handle: Option<tokio::task::JoinHandle<()>> = None;
        let mut worker_handle: Option<tokio::task::JoinHandle<()>> = None;
        let mut session_watcher_handle: Option<tokio::task::JoinHandle<()>> = None;

        let zk_reg = if let Some(ref zk_servers) = config.zk_servers {
            let bookie_id = config.advertised_address.clone().unwrap_or_else(|| {
                format!("{}:{}", local_addr.ip(), local_addr.port())
            });
            let reg_mgr = bookkeeper_metadata::zk::registration::ZkRegistrationManager::new(
                zk_servers,
                &config.zk_ledger_root,
            )
            .await
            .map_err(|e| format!("ZK connect: {e}"))?;
            validate_cookie(&reg_mgr, &bookie_id, &config).await?;
            let info = bookkeeper_metadata::service_info::BookieServiceInfo::new(
                &bookie_id,
                &local_addr.ip().to_string(),
                local_addr.port(),
            );
            reg_mgr
                .register_bookie(&bookie_id, false, &info)
                .await
                .map_err(|e| format!("ZK register: {e}"))?;

            // Start ZK session watcher — monitors for session expiry.
            // On expiry, exits the process for supervisor restart (matching Java behavior).
            {
                let watcher_zk_servers = zk_servers.clone();
                let watcher_ledger_root = config.zk_ledger_root.clone();
                let watcher_bookie_id = bookie_id.clone();
                let watcher_shutdown = shutdown_rx.clone();
                let watcher_state = state.clone();
                let mut session_watcher = reg_mgr.zk_client().state_watcher();
                let watcher_local_addr = local_addr;

                session_watcher_handle = Some(tokio::spawn(async move {
                    run_session_watcher(
                        &mut session_watcher,
                        &watcher_zk_servers,
                        &watcher_ledger_root,
                        &watcher_bookie_id,
                        watcher_local_addr,
                        watcher_state,
                        watcher_shutdown,
                    ).await;
                }));
            }

            // Start auto-recovery daemon if enabled
            if config.auto_recovery_daemon_enabled {
                let (ah, wh) = start_auto_recovery(
                    reg_mgr.zk_client().clone(),
                    reg_mgr.paths().clone(),
                    &bookie_id,
                    config.lost_bookie_recovery_delay as u64,
                    shutdown_rx.clone(),
                )
                .await?;
                auditor_handle = Some(ah);
                worker_handle = Some(wh);
            }

            Some((reg_mgr, bookie_id))
        } else {
            None
        };

        // Shared GC control flags — used by both the GC loop and HTTP API.
        let gc_suspend_major = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gc_suspend_minor = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gc_force_major = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gc_force_minor = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Start GC thread if ZK is available
        let gc_handle = if zk_reg.is_some() {
            let gc_storage = storage.clone();
            let gc_zk = zk_reg.as_ref().unwrap().0.zk_client().clone();
            let gc_paths = zk_reg.as_ref().unwrap().0.paths().clone();
            let gc_wait_ms = config.gc_wait_time as u64;
            let gc_shutdown = shutdown_rx.clone();
            let gc_suspend_major_c = gc_suspend_major.clone();
            let gc_suspend_minor_c = gc_suspend_minor.clone();
            let gc_force_major_c = gc_force_major.clone();
            let gc_force_minor_c = gc_force_minor.clone();

            Some(tokio::spawn(async move {
                run_gc_loop(
                    gc_storage, gc_zk, gc_paths, gc_wait_ms, gc_shutdown,
                    gc_suspend_major_c, gc_suspend_minor_c,
                    gc_force_major_c, gc_force_minor_c,
                ).await;
            }))
        } else {
            None
        };

        // Start disk monitoring thread for read-only mode transitions
        let disk_monitor_handle = if config.read_only_mode_enabled {
            let monitor_state = state.clone();
            // Monitor both ledger directories AND journal directory (matching Java)
            let mut monitor_dirs = config.ledger_directories.clone();
            if !monitor_dirs.contains(&config.journal_directory) {
                monitor_dirs.push(config.journal_directory.clone());
            }
            let monitor_threshold = config.disk_usage_threshold;
            let monitor_lwm = config.disk_usage_lwm_threshold;
            let monitor_interval_ms = config.disk_check_interval as u64;
            let monitor_shutdown = shutdown_rx.clone();
            // Pass ZK registration info for updating registration on state change
            let monitor_zk_reg = zk_reg.as_ref().map(|(reg, bid)| {
                (reg.zk_client().clone(), reg.paths().clone(), bid.clone())
            });

            Some(tokio::spawn(async move {
                run_disk_monitor(
                    monitor_state,
                    monitor_dirs,
                    monitor_threshold,
                    monitor_lwm,
                    monitor_interval_ms,
                    monitor_zk_reg,
                    monitor_shutdown,
                ).await;
            }))
        } else {
            None
        };

        // Optionally start HTTP admin API server
        let http_addr = if config.http_server_enabled {
            let http_port = config.http_server_port as u16;
            let (zk_client, zk_paths, http_bookie_id) = match &zk_reg {
                Some((reg, bid)) => (
                    Some(reg.zk_client().clone()),
                    Some(reg.paths().clone()),
                    Some(bid.clone()),
                ),
                None => (None, None, None),
            };
            let http_state = http_api::HttpState {
                state_manager: state.clone(),
                storage: Some(storage.clone()),
                ledger_dirs: config.ledger_directories.clone(),
                journal_dir: config.journal_directory.clone(),
                zk_client,
                zk_paths,
                bookie_id: http_bookie_id,
                gc_suspend_major: gc_suspend_major.clone(),
                gc_suspend_minor: gc_suspend_minor.clone(),
                gc_force_major: gc_force_major.clone(),
                gc_force_minor: gc_force_minor.clone(),
            };
            let router = http_api::create_router(http_state);
            let http_listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{http_port}")).await?;
            let http_local_addr = http_listener.local_addr()?;
            tokio::spawn(async move {
                axum::serve(http_listener, router).await.ok();
            });
            Some(http_local_addr)
        } else {
            None
        };

        // Start SyncThread — periodic flush of write cache to entry logs.
        // Matches Java's SyncThread which runs every flushInterval ms (default 10s).
        let sync_storage = storage.clone();
        let sync_shutdown = shutdown_rx.clone();
        let flush_interval_ms = config.flush_interval as u64;
        let sync_handle = Some(tokio::spawn(async move {
            run_sync_thread(sync_storage, flush_interval_ms, sync_shutdown).await;
        }));

        // Build TLS acceptor if configured
        let tls_acceptor = if let (Some(ref cert), Some(ref key)) =
            (&config.tls_certificate_path, &config.tls_key_path)
        {
            let tls_cfg = crate::tls::TlsConfig {
                cert_path: cert.clone(),
                key_path: key.clone(),
                trust_store_path: config.tls_trust_store_path.clone(),
                client_auth: config.tls_client_authentication,
                keystore_type: config.tls_keystore_type.clone(),
                enabled_protocols: config.tls_enabled_protocols.clone(),
                cert_refresh_seconds: config.tls_cert_refresh_seconds,
            };

            let reloadable = Arc::new(crate::tls::ReloadableTlsAcceptor::new(tls_cfg)?);
            if config.tls_cert_refresh_seconds > 0 {
                reloadable.start_refresh_task(
                    config.tls_cert_refresh_seconds as u64,
                    shutdown_rx.clone(),
                );
                tracing::info!("TLS enabled with cert refresh every {}s (cert: {cert}, key: {key})",
                    config.tls_cert_refresh_seconds);
            } else {
                tracing::info!("TLS enabled (cert: {cert}, key: {key})");
            }
            Some(reloadable)
        } else {
            None
        };

        // Spawn accept loop
        let accept_handle = tokio::spawn(accept_loop(
            listener,
            processor,
            state.clone(),
            shutdown_rx,
            max_frame_size,
            tls_acceptor,
        ));

        Ok(BookieServer {
            local_addr,
            http_addr,
            shutdown_tx,
            accept_handle,
            storage: storage_for_shutdown,
            state,
            zk_reg,
            auditor_handle,
            worker_handle,
            gc_handle,
            disk_monitor_handle,
            sync_handle,
            session_watcher_handle,
            ledger_directories: config.ledger_directories.clone(),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn http_addr(&self) -> Option<SocketAddr> {
        self.http_addr
    }

    /// Update ZK registration when bookie state changes (e.g., to/from read-only).
    pub async fn update_registration(&self, read_only: bool) -> Result<(), Box<dyn std::error::Error>> {
        if let Some((ref reg_mgr, ref bookie_id)) = self.zk_reg {
            use bookkeeper_metadata::traits::RegistrationManager;
            reg_mgr.unregister_bookie(bookie_id, !read_only).await
                .map_err(|e| format!("unregister from old path: {e}"))?;
            let info = bookkeeper_metadata::service_info::BookieServiceInfo::new(
                bookie_id, &self.local_addr.ip().to_string(), self.local_addr.port(),
            );
            reg_mgr.register_bookie(bookie_id, read_only, &info).await
                .map_err(|e| format!("register on new path: {e}"))?;
        }
        Ok(())
    }

    /// Graceful shutdown matching Java's BookieImpl.shutdown() sequence:
    ///
    /// Java order: lock → forceToReadOnly → SyncThread.shutdown → Journal.shutdown
    ///             → LedgerStorage.shutdown → DiskMonitor.shutdown → StateManager.close (ZK)
    pub async fn shutdown(self) {
        tracing::info!("Graceful shutdown starting...");

        // Step 1: Transition to READ_ONLY to reject new writes immediately.
        // Matches Java's stateManager.forceToReadOnly() (BookieImpl.java:897).
        // This ensures no new writes are accepted while we flush.
        self.state.transition_to(BookieState::ReadOnly);
        tracing::info!("Transitioned to read-only.");

        // Step 2: Signal shutdown — stops accept loop and background tasks
        let _ = self.shutdown_tx.send(true);

        // Step 3: Stop background tasks BEFORE flush (matching Java's order:
        // SyncThread → Journal → Storage → DiskMonitor).
        // GC and replication must stop before flushing to avoid concurrent modifications.
        if let Some(h) = self.auditor_handle {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(10), h).await;
        }
        if let Some(h) = self.worker_handle {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(10), h).await;
        }
        if let Some(h) = self.gc_handle {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(10), h).await;
        }
        if let Some(h) = self.disk_monitor_handle {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(5), h).await;
        }
        if let Some(h) = self.sync_handle {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(10), h).await;
        }
        if let Some(h) = self.session_watcher_handle {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(5), h).await;
        }
        tracing::info!("Background tasks stopped.");

        // Step 4: Wait for accept loop to finish (drains in-flight connections)
        let _ = self.accept_handle.await;
        tracing::info!("Accept loop stopped.");

        // Step 5: Flush storage — persist write cache to entry logs + checkpoint.
        // Matches Java's SyncThread.shutdown() which waits up to 5 minutes.
        // Journal cleanup happens inside checkpoint().
        tracing::info!("Flushing storage...");
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(300),
            tokio::task::spawn_blocking({
                let storage = self.storage.clone();
                move || {
                    if let Err(e) = storage.checkpoint() {
                        tracing::warn!("WARNING: checkpoint failed during shutdown: {e}");
                    }
                }
            })
        ).await {
            Ok(Ok(())) => tracing::info!("Storage flushed."),
            Ok(Err(e)) => tracing::warn!("WARNING: storage flush task panicked: {e}"),
            Err(_) => tracing::warn!("WARNING: storage flush timed out after 5 minutes"),
        }

        // Step 6: Remove DIRTY files — clean shutdown confirmed.
        // Matches Java: deleted AFTER bookie.shutdown() completes (all flushed).
        for dir in &self.ledger_directories {
            let dirty_path = Path::new(dir).join(DIRTY_FILENAME);
            if dirty_path.exists() {
                if let Err(e) = std::fs::remove_file(&dirty_path) {
                    tracing::warn!("Failed to remove DIRTY file in {dir}: {e}");
                }
            }
        }

        // Step 7: Transition to ShuttingDown
        self.state.transition_to(BookieState::ShuttingDown);

        // Step 8: Unregister from ZooKeeper LAST.
        // Matches Java's StateManager.close() as the final step.
        if let Some((reg_mgr, bookie_id)) = self.zk_reg {
            let _ = reg_mgr.unregister_bookie(&bookie_id, false).await;
            tracing::info!("Unregistered from ZooKeeper.");
        }

        tracing::info!("Graceful shutdown complete.");
    }
}

async fn validate_cookie(
    zk_reg: &dyn bookkeeper_metadata::traits::RegistrationManager,
    bookie_id: &str,
    config: &ServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_metadata::cookie::Cookie;

    let journal_dir = std::path::Path::new(&config.journal_directory);
    let disk_cookie_path = journal_dir.join("COOKIE");

    // Read cookie from ZK
    let zk_cookie = zk_reg.read_cookie(bookie_id).await
        .map_err(|e| format!("read ZK cookie: {e}"))?;

    // Read cookie from disk
    let disk_cookie = if disk_cookie_path.exists() {
        let data = std::fs::read_to_string(&disk_cookie_path)
            .map_err(|e| format!("read disk cookie: {e}"))?;
        Some(Cookie::deserialize(&data).map_err(|e| format!("parse disk cookie: {e}"))?)
    } else {
        None
    };

    match (zk_cookie, disk_cookie) {
        (Some((zk_data, _version)), Some(disk_cookie)) => {
            // Both exist — validate
            let zk_str = String::from_utf8(zk_data)
                .map_err(|e| format!("ZK cookie not UTF-8: {e}"))?;
            let zk_cookie = Cookie::deserialize(&zk_str)
                .map_err(|e| format!("parse ZK cookie: {e}"))?;
            disk_cookie.validate_against(&zk_cookie)
                .map_err(|e| format!("cookie validation failed: {e}"))?;
        }
        (None, None) => {
            // First time — create cookie in both ZK and disk
            let ledger_dirs: Vec<&str> = config.ledger_directories.iter().map(|s| s.as_str()).collect();
            let cookie = Cookie::new(
                bookie_id,
                &config.journal_directory,
                &ledger_dirs,
                zk_reg.get_cluster_instance_id().await
                    .map_err(|e| format!("get instance id: {e}"))?
                    .as_deref(),
                None,
            );
            let cookie_str = cookie.serialize();
            zk_reg.create_cookie(bookie_id, cookie_str.as_bytes()).await
                .map_err(|e| format!("create ZK cookie: {e}"))?;
            std::fs::create_dir_all(journal_dir)
                .map_err(|e| format!("create journal dir: {e}"))?;
            std::fs::write(&disk_cookie_path, &cookie_str)
                .map_err(|e| format!("write disk cookie: {e}"))?;
        }
        (Some(_), None) => {
            // ZK has cookie but disk doesn't — error
            return Err("ZK cookie exists but disk cookie is missing".into());
        }
        (None, Some(_)) => {
            // Disk has cookie but ZK doesn't — error
            return Err("disk cookie exists but ZK cookie is missing".into());
        }
    }
    Ok(())
}

/// Monitors ZK session state for fatal events.
///
/// Matches Java's actual behavior on session expiry:
/// - Disconnected: log warning, wait for auto-reconnect (ephemeral nodes survive within session)
/// - Expired: session is dead, ephemeral registration gone, all ZK-dependent components broken.
///   Java's BookieStateManager attempts re-registration but fails (stale ZK handle), then calls
///   shutdownHandler.shutdown(ZK_REG_FAIL). We match this by exiting the process so the external
///   supervisor (systemd, k8s) can restart it with a fresh ZK session.
/// - AuthFailed: fatal, exit process.
async fn run_session_watcher(
    watcher: &mut zookeeper_client::StateWatcher,
    _zk_servers: &str,
    _ledger_root: &str,
    bookie_id: &str,
    _local_addr: SocketAddr,
    _state: Arc<BookieStateManager>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    use zookeeper_client::SessionState;

    loop {
        tokio::select! {
            session_state = watcher.changed() => {
                match session_state {
                    SessionState::Disconnected => {
                        tracing::warn!("ZK session disconnected — waiting for reconnect \
                            (ephemeral nodes survive within same session)");
                    }
                    SessionState::SyncConnected => {
                        tracing::info!("ZK session reconnected (same session)");
                    }
                    SessionState::Expired => {
                        // Session expired = ephemeral registration gone, all ZK components stale.
                        // Java: ZooKeeperClient calls Runtime.exit(-1) on reconnect failure;
                        // BookieStateManager calls shutdownHandler.shutdown(ZK_REG_FAIL).
                        // Match this by exiting the process for supervisor restart.
                        tracing::error!(
                            "ZK session expired for bookie {bookie_id}. \
                             Ephemeral registration lost. All ZK-dependent components are stale. \
                             Exiting process for supervisor restart (matching Java ZK_REG_FAIL)."
                        );
                        std::process::exit(1);
                    }
                    SessionState::AuthFailed => {
                        tracing::error!(
                            "ZK authentication failed for bookie {bookie_id}. \
                             Exiting process."
                        );
                        std::process::exit(1);
                    }
                    SessionState::Closed => {
                        return;
                    }
                    _ => {}
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    return;
                }
            }
        }
    }
}

/// Decision returned by [`decide_disk_monitor_action`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskMonitorAction {
    /// No state change required.
    Stay,
    /// Transition Running → ReadOnly because at least one ledger directory
    /// is over the high-water threshold.
    GoReadOnly,
    /// Transition ReadOnly → Running because total disk usage dropped below
    /// the low-water mark.
    GoRunning,
}

/// Pure policy decision for the disk monitor: given the current bookie
/// state and the observed disk usage, decide whether to transition.
///
/// Extracted from [`run_disk_monitor`] so it can be unit-tested without a
/// real filesystem or tokio runtime. Mirrors Java's LedgerDirsMonitor:
/// - any directory over `threshold` triggers a Running → ReadOnly transition
/// - aggregate usage below `lwm_threshold` recovers ReadOnly → Running
///   (only when at least one directory was successfully measured)
pub fn decide_disk_monitor_action(
    current: BookieState,
    any_full: bool,
    total_usage: f64,
    lwm_threshold: f64,
    dir_count: usize,
) -> DiskMonitorAction {
    if any_full && current == BookieState::Running {
        return DiskMonitorAction::GoReadOnly;
    }
    if current == BookieState::ReadOnly
        && dir_count > 0
        && total_usage < lwm_threshold
    {
        return DiskMonitorAction::GoRunning;
    }
    DiskMonitorAction::Stay
}

/// Periodic disk usage monitor for read-only mode transitions.
///
/// Matches Java's LedgerDirsMonitor:
/// - Checks disk usage every `disk_check_interval` ms
/// - Transitions to ReadOnly when usage > `disk_usage_threshold` (95%)
/// - Transitions back to Running when usage < `disk_usage_lwm_threshold` (90%)
async fn run_disk_monitor(
    state: Arc<BookieStateManager>,
    ledger_dirs: Vec<String>,
    threshold: f64,
    lwm_threshold: f64,
    interval_ms: u64,
    zk_info: Option<(zookeeper_client::Client, bookkeeper_metadata::zk_path::ZkPaths, String)>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let interval = tokio::time::Duration::from_millis(interval_ms);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
            _ = tokio::time::sleep(interval) => {
                let mut any_full = false;
                let mut total_used: f64 = 0.0;
                let mut total_capacity: f64 = 0.0;
                let mut dir_count = 0;

                for dir in &ledger_dirs {
                    match get_disk_usage_details(dir) {
                        Ok((usage, used_bytes, total_bytes)) => {
                            if usage > threshold {
                                any_full = true;
                            }
                            total_used += used_bytes;
                            total_capacity += total_bytes;
                            dir_count += 1;
                        }
                        Err(e) => {
                            tracing::warn!("Disk monitor: error checking {dir}: {e}");
                        }
                    }
                }

                // For recovery: use total usage across all directories (matching Java)
                let total_usage = if total_capacity > 0.0 {
                    total_used / total_capacity
                } else {
                    0.0
                };

                let current = state.state();
                match decide_disk_monitor_action(
                    current, any_full, total_usage, lwm_threshold, dir_count,
                ) {
                    DiskMonitorAction::GoReadOnly => {
                        if state.transition_to(BookieState::ReadOnly) {
                            crate::metrics::BOOKIE_STATE.set(2); // 2 = readonly
                            tracing::info!("Disk monitor: disk usage exceeds {:.0}% threshold, transitioning to READ-ONLY", threshold * 100.0);
                            update_zk_registration(&zk_info, true).await;
                        }
                    }
                    DiskMonitorAction::GoRunning => {
                        if state.transition_to(BookieState::Running) {
                            crate::metrics::BOOKIE_STATE.set(1); // 1 = writable
                            tracing::info!("Disk monitor: total disk usage ({:.1}%) below {:.0}% LWM, transitioning back to WRITABLE",
                                total_usage * 100.0, lwm_threshold * 100.0);
                            update_zk_registration(&zk_info, false).await;
                        }
                    }
                    DiskMonitorAction::Stay => {}
                }
            }
        }
    }
}

/// Update ZK registration when bookie transitions between writable and read-only.
///
/// Matches Java's ordering:
/// - To read-only: register as readonly FIRST, then unregister from writable
/// - To writable: register as writable FIRST, then unregister from readonly
/// This ensures the bookie is always visible in at least one path.
async fn update_zk_registration(
    zk_info: &Option<(zookeeper_client::Client, bookkeeper_metadata::zk_path::ZkPaths, String)>,
    read_only: bool,
) {
    if let Some((ref zk_client, ref paths, ref bookie_id)) = zk_info {
        let info = bookkeeper_metadata::service_info::BookieServiceInfo::new(
            bookie_id, "", 0,
        );
        let data = info.to_protobuf_bytes();
        let options = zookeeper_client::CreateMode::Ephemeral
            .with_acls(zookeeper_client::Acls::anyone_all());

        // Step 1: Register on NEW path FIRST (ensures always visible)
        let new_path = if read_only {
            paths.bookie_readonly_registration(bookie_id)
        } else {
            paths.bookie_registration(bookie_id)
        };
        let _ = zk_client.create(&new_path, &data, &options).await;

        // Step 2: Unregister from OLD path
        let old_path = if read_only {
            paths.bookie_registration(bookie_id)
        } else {
            paths.bookie_readonly_registration(bookie_id)
        };
        let _ = zk_client.delete(&old_path, None).await;

        tracing::info!(
            "Disk monitor: ZK registration updated (read_only={})",
            read_only
        );
    }
}

/// Get disk usage details for a directory path.
/// Returns (usage_ratio, used_bytes, total_bytes).
fn get_disk_usage_details(path: &str) -> Result<(f64, f64, f64), String> {
    let p = std::path::Path::new(path);
    if !p.exists() {
        return Err(format!("path does not exist: {path}"));
    }

    #[cfg(unix)]
    {
        use std::ffi::CString;
        let c_path = CString::new(path).map_err(|e| format!("invalid path: {e}"))?;
        unsafe {
            let mut stat: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(c_path.as_ptr(), &mut stat) != 0 {
                return Err(format!("statvfs failed for {path}"));
            }
            let total = stat.f_blocks as f64 * stat.f_frsize as f64;
            let available = stat.f_bavail as f64 * stat.f_frsize as f64;
            if total == 0.0 {
                return Ok((0.0, 0.0, 0.0));
            }
            let used = total - available;
            let usage = used / total;
            Ok((usage, used, total))
        }
    }

    #[cfg(not(unix))]
    {
        Ok((0.0, 0.0, 1.0))
    }
}

/// Periodic garbage collection loop.
///
/// Matches Java's GarbageCollectorThread + ScanAndCompareGarbageCollector:
/// 1. Get all ledger IDs from metadata store (ZK)
/// Periodic sync thread — flushes write cache to entry logs and checkpoints.
///
/// Matches Java's SyncThread (BookieImpl.java:480):
/// - Runs every `flushInterval` ms (default 10 seconds)
/// - Calls storage.checkpoint() which flushes write cache → entry logs,
///   updates RocksDB index, and cleans up old journal files
async fn run_sync_thread(
    storage: Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage>,
    flush_interval_ms: u64,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let interval = tokio::time::Duration::from_millis(flush_interval_ms);
    tracing::info!("SyncThread started (interval: {}ms)", flush_interval_ms);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    tracing::info!("SyncThread shutting down");
                    // Final flush before exit
                    if let Err(e) = storage.checkpoint() {
                        tracing::warn!("SyncThread: final checkpoint error: {e}");
                    }
                    break;
                }
            }
            _ = tokio::time::sleep(interval) => {
                // Periodic checkpoint: flush write cache → entry logs → clean journals
                if let Err(e) = storage.checkpoint() {
                    tracing::warn!("SyncThread: checkpoint error: {e}");
                }
            }
        }
    }
}

/// 2. Compare against local storage
/// 3. Delete local data for ledgers no longer in metadata
/// 4. Delete empty entry log files
async fn run_gc_loop(
    storage: Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage>,
    zk_client: zookeeper_client::Client,
    paths: bookkeeper_metadata::zk_path::ZkPaths,
    gc_wait_ms: u64,
    mut shutdown_rx: watch::Receiver<bool>,
    suspend_major: Arc<std::sync::atomic::AtomicBool>,
    suspend_minor: Arc<std::sync::atomic::AtomicBool>,
    force_major: Arc<std::sync::atomic::AtomicBool>,
    force_minor: Arc<std::sync::atomic::AtomicBool>,
) {
    use bookkeeper_metadata::zk::ledger_manager::ZkLedgerManager;
    use bookkeeper_metadata::traits::LedgerManager;
    use bookkeeper_storage::gc::GarbageCollector;
    use std::collections::HashSet;
    use std::sync::atomic::Ordering;

    let ledger_manager = ZkLedgerManager::new(zk_client, paths);
    let gc = GarbageCollector::new(storage);
    let interval = tokio::time::Duration::from_millis(gc_wait_ms);
    // Force-trigger checks at a faster cadence (1s) so HTTP-triggered GC is responsive.
    let force_check_interval = tokio::time::Duration::from_secs(1);

    tracing::info!("GC thread started (interval: {}ms)", gc_wait_ms);

    let mut last_run = tokio::time::Instant::now();
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    tracing::info!("GC thread shutting down");
                    break;
                }
            }
            _ = tokio::time::sleep(force_check_interval) => {
                // Check force flags first (HTTP-triggered immediate GC)
                let forced = force_major.swap(false, Ordering::Relaxed)
                    || force_minor.swap(false, Ordering::Relaxed);
                let should_run = forced || last_run.elapsed() >= interval;
                if !should_run {
                    continue;
                }

                // Honor suspension flags. If both major and minor are suspended,
                // skip the GC cycle entirely. (This is a coarse-grained match to
                // Java's per-compaction suspension; the underlying GC implementation
                // does not yet distinguish major vs minor compaction tiers.)
                let major_suspended = suspend_major.load(Ordering::Relaxed);
                let minor_suspended = suspend_minor.load(Ordering::Relaxed);
                if major_suspended && minor_suspended && !forced {
                    last_run = tokio::time::Instant::now();
                    continue;
                }

                last_run = tokio::time::Instant::now();
                match ledger_manager.get_ledger_ranges().await {
                    Ok(metadata_ledgers) => {
                        let active: HashSet<i64> = metadata_ledgers.into_iter().collect();
                        if let Err(e) = gc.gc(&active) {
                            tracing::warn!("GC cycle error: {e}");
                        }
                    }
                    Err(e) => {
                        tracing::warn!("GC: failed to get metadata ledgers: {e}");
                    }
                }
            }
        }
    }
}

/// Start the auto-recovery daemon: Auditor + ReplicationWorker.
///
/// Matches Java's AutoRecoveryMain which runs both components in the bookie process.
/// - Auditor: monitors bookie registrations and marks affected ledgers as underreplicated
/// - ReplicationWorker: claims underreplicated ledgers and re-replicates them
async fn start_auto_recovery(
    zk_client: zookeeper_client::Client,
    paths: bookkeeper_metadata::zk_path::ZkPaths,
    bookie_id: &str,
    lost_bookie_recovery_delay_secs: u64,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>), Box<dyn std::error::Error>> {
    use bookkeeper_metadata::zk::ledger_manager::ZkLedgerManager;
    use bookkeeper_replication::auditor_election::AuditorElector;
    use bookkeeper_replication::underreplication::ZkUnderreplicationManager;
    use bookkeeper_replication::worker::ReplicationWorker;

    // Create shared components
    let ledger_manager = Arc::new(ZkLedgerManager::new(zk_client.clone(), paths.clone()));

    let ur_manager = Arc::new(
        ZkUnderreplicationManager::new(zk_client.clone(), paths.clone(), bookie_id.to_string())
            .await
            .map_err(|e| format!("init underreplication manager: {e}"))?,
    );

    // Start AuditorElector (only the elected leader runs the Auditor)
    let elector_shutdown = shutdown_rx.clone();
    let elector_zk = zk_client.clone();
    let elector_paths = paths.clone();
    let elector_ur = ur_manager.clone();
    let elector_lm: Arc<dyn bookkeeper_metadata::traits::LedgerManager> = ledger_manager.clone();
    let elector_bookie_id = bookie_id.to_string();
    let auditor_zk = zk_client;
    let auditor_paths = paths;

    let auditor_handle = tokio::spawn(async move {
        let mut elector = AuditorElector::new(elector_zk, &elector_paths, elector_bookie_id);
        if let Err(e) = elector
            .run(
                elector_ur,
                elector_lm,
                auditor_zk,
                auditor_paths,
                lost_bookie_recovery_delay_secs,
                elector_shutdown,
            )
            .await
        {
            tracing::warn!("AuditorElector exited with error: {e}");
        }
    });

    // Start ReplicationWorker
    let worker_bookie_id = bookie_id.to_string();
    let worker_lm: Arc<dyn bookkeeper_metadata::traits::LedgerManager> = ledger_manager;

    let worker_handle = tokio::spawn(async move {
        let mut worker = ReplicationWorker::new(
            ur_manager,
            worker_lm,
            worker_bookie_id,
            5000, // backoff ms
            shutdown_rx,
        );
        if let Err(e) = worker.run().await {
            tracing::warn!("ReplicationWorker exited with error: {e}");
        }
    });

    tracing::info!("Auto-recovery daemon started (auditor elector + replication worker)");
    Ok((auditor_handle, worker_handle))
}

async fn accept_loop(
    listener: TcpListener,
    processor: Arc<RequestProcessor>,
    state: Arc<BookieStateManager>,
    mut shutdown_rx: watch::Receiver<bool>,
    max_frame_size: usize,
    tls_acceptor: Option<Arc<crate::tls::ReloadableTlsAcceptor>>,
) {
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _peer_addr)) => {
                        let proc = processor.clone();
                        if let Some(ref tls) = tls_acceptor {
                            let tls = tls.clone();
                            tokio::spawn(async move {
                                let acceptor = tls.acceptor().await;
                                handle_starttls_connection(stream, proc, max_frame_size, acceptor).await;
                            });
                        } else {
                            // Plain TCP connection (no TLS configured)
                            tokio::spawn(handle_connection(stream, proc, max_frame_size));
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    state.transition_to(BookieState::ShuttingDown);
                    break;
                }
            }
        }
    }
}

/// Handle a connection with StartTLS upgrade support.
/// Matches Java's BookieNettyServer + BookieRequestProcessor StartTLS flow:
/// 1. Accept plaintext connection
/// 2. Read first frame — expect V3 StartTLSRequest
/// 3. Send StartTLSResponse
/// 4. Upgrade connection to TLS
/// 5. Continue handling encrypted frames
async fn handle_starttls_connection(
    stream: TcpStream,
    processor: Arc<RequestProcessor>,
    max_frame_size: usize,
    tls_acceptor: tokio_rustls::TlsAcceptor,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::codec::{Decoder, Encoder};

    let (mut reader, mut writer) = stream.into_split();
    let mut codec = BookieCodec::new(max_frame_size);
    let mut buf = BytesMut::with_capacity(4096);
    let mut out = BytesMut::with_capacity(256);

    // Read first frame (plaintext — should be StartTLSRequest)
    loop {
        match codec.decode(&mut buf) {
            Ok(Some(frame)) => {
                let frame_bytes = frame.freeze();
                // Try to decode as V3 protobuf
                if let Some(v3_req) = crate::v3_processor::try_decode_v3(&frame_bytes) {
                    let op = v3_req.header.operation;
                    if op == bookkeeper_proto::OperationType::StartTls as i32 {
                        // StartTLS request — send response and upgrade
                        let txn_id = v3_req.header.txn_id;
                        let resp = bookkeeper_proto::Response {
                            header: bookkeeper_proto::BkPacketHeader {
                                version: bookkeeper_proto::ProtocolVersion::VersionThree as i32,
                                operation: bookkeeper_proto::OperationType::StartTls as i32,
                                txn_id,
                                priority: None,
                            },
                            status: bookkeeper_proto::StatusCode::Eok as i32,
                            ..Default::default()
                        };
                        let resp_bytes = Bytes::from(prost::Message::encode_to_vec(&resp));
                        out.clear();
                        if codec.encode(resp_bytes, &mut out).is_ok() {
                            if writer.write_all(&out).await.is_err() {
                                return;
                            }
                        }

                        // Upgrade to TLS
                        let tcp_stream = reader.reunite(writer).unwrap();
                        match tls_acceptor.accept(tcp_stream).await {
                            Ok(tls_stream) => {
                                let (tls_reader, tls_writer) = tokio::io::split(tls_stream);
                                handle_connection_io(tls_reader, tls_writer, processor, max_frame_size).await;
                            }
                            Err(e) => {
                                tracing::warn!("TLS handshake failed after StartTLS: {e}");
                            }
                        }
                        return;
                    } else {
                        // Not a StartTLS request — this is a plaintext V3 request.
                        // Process it and continue as plaintext (shouldn't happen if TLS required).
                        tracing::warn!("Expected StartTLSRequest but got op={op}, processing as plaintext");
                        let tcp_stream = reader.reunite(writer).unwrap();
                        handle_connection(tcp_stream, processor, max_frame_size).await;
                        return;
                    }
                } else {
                    // V2 binary request — process as plaintext
                    let tcp_stream = reader.reunite(writer).unwrap();
                    handle_connection(tcp_stream, processor, max_frame_size).await;
                    return;
                }
            }
            Ok(None) => {
                match reader.read_buf(&mut buf).await {
                    Ok(0) => return, // EOF
                    Ok(_) => continue,
                    Err(_) => return,
                }
            }
            Err(_) => return,
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    processor: Arc<RequestProcessor>,
    max_frame_size: usize,
) {
    let (reader, writer) = stream.into_split();
    handle_connection_io(reader, writer, processor, max_frame_size).await;
}

async fn handle_connection_io<R, W>(
    mut reader: R,
    mut writer: W,
    processor: Arc<RequestProcessor>,
    max_frame_size: usize,
)
where
    R: tokio::io::AsyncReadExt + Unpin,
    W: tokio::io::AsyncWriteExt + Unpin,
{
    let mut codec = BookieCodec::new(max_frame_size);
    let mut buf = BytesMut::with_capacity(4096);
    let mut out = BytesMut::with_capacity(256);
    let mut is_v3: Option<bool> = None; // None = not yet detected

    loop {
        // Try to decode a complete frame from the buffer first
        match codec.decode(&mut buf) {
            Ok(Some(frame)) => {
                let frame_bytes = frame.freeze();
                let response_bytes: Bytes;

                // Auto-detect protocol version on first frame
                let use_v3 = is_v3.unwrap_or_else(|| {
                    // Try V3 protobuf decode first (Java client defaults to V3)
                    crate::v3_processor::try_decode_v3(&frame_bytes).is_some()
                });

                if use_v3 {
                    is_v3 = Some(true);
                    if let Some(v3_req) = crate::v3_processor::try_decode_v3(&frame_bytes) {
                        let proc = processor.clone();
                        let resp = match tokio::task::spawn_blocking(move || {
                            crate::v3_processor::process_v3_request(&proc, v3_req)
                        }).await {
                            Ok(resp) => resp,
                            Err(_) => break,
                        };
                        response_bytes = Bytes::from(resp);
                    } else {
                        // Sticky V3 connection but frame failed to decode — close
                        break;
                    }
                } else {
                    is_v3 = Some(false);
                    match decode_request(frame_bytes) {
                        Ok(req) => {
                            let proc = processor.clone();
                            let resp = match tokio::task::spawn_blocking(move || proc.process_request(req)).await {
                                Ok(resp) => resp,
                                Err(_) => break, // task panicked or cancelled
                            };
                            let resp_encoded = encode_response(&resp);
                            response_bytes = resp_encoded.freeze();
                        }
                        Err(_) => {
                            // Unknown or malformed request — close connection
                            break;
                        }
                    }
                }

                // Encode the response with length-prefix framing
                out.clear();
                if codec.encode(response_bytes, &mut out).is_ok() {
                    if writer.write_all(&out).await.is_err() {
                        break;
                    }
                } else {
                    break;
                }
            }
            Ok(None) => {
                // Need more data — read from socket
                match reader.read_buf(&mut buf).await {
                    Ok(0) => {
                        // EOF — client closed connection
                        break;
                    }
                    Ok(_) => {
                        // Got more data — loop back to decode
                        continue;
                    }
                    Err(_) => {
                        // Read error
                        break;
                    }
                }
            }
            Err(_) => {
                // Frame decode error (e.g. too large)
                break;
            }
        }
    }
}
