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

pub struct BookieServer {
    local_addr: SocketAddr,
    http_addr: Option<SocketAddr>,
    shutdown_tx: watch::Sender<bool>,
    // Join handle for the accept loop task
    accept_handle: tokio::task::JoinHandle<()>,
    storage: Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage>,
    zk_reg: Option<(bookkeeper_metadata::zk::registration::ZkRegistrationManager, String)>, // (manager, bookie_id)
    // Auto-recovery background tasks
    auditor_handle: Option<tokio::task::JoinHandle<()>>,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
    gc_handle: Option<tokio::task::JoinHandle<()>>,
    disk_monitor_handle: Option<tokio::task::JoinHandle<()>>,
}

impl BookieServer {
    pub async fn start(config: ServerConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Bind TCP listener
        let addr = format!("0.0.0.0:{}", config.bookie_port);
        let listener = TcpListener::bind(&addr).await?;
        let local_addr = listener.local_addr()?;

        // Open storage using first ledger dir
        let ledger_dir = config
            .ledger_directories
            .first()
            .ok_or("no ledger dirs configured")?;
        let storage = DbLedgerStorage::open(Path::new(ledger_dir))?;
        let storage: Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage> =
            Arc::new(storage);

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

        // Start GC thread if ZK is available
        let gc_handle = if zk_reg.is_some() {
            let gc_storage = storage.clone();
            let gc_zk = zk_reg.as_ref().unwrap().0.zk_client().clone();
            let gc_paths = zk_reg.as_ref().unwrap().0.paths().clone();
            let gc_wait_ms = config.gc_wait_time as u64;
            let gc_shutdown = shutdown_rx.clone();

            Some(tokio::spawn(async move {
                run_gc_loop(gc_storage, gc_zk, gc_paths, gc_wait_ms, gc_shutdown).await;
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
            let http_state = http_api::HttpState { state_manager: state.clone() };
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

        // Spawn accept loop
        let accept_handle = tokio::spawn(accept_loop(
            listener,
            processor,
            state,
            shutdown_rx,
            max_frame_size,
        ));

        Ok(BookieServer {
            local_addr,
            http_addr,
            shutdown_tx,
            accept_handle,
            storage: storage_for_shutdown,
            zk_reg,
            auditor_handle,
            worker_handle,
            gc_handle,
            disk_monitor_handle,
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

    pub async fn shutdown(self) {
        // Signal the accept loop and replication tasks to stop
        let _ = self.shutdown_tx.send(true);
        // Wait for the accept loop to finish
        let _ = self.accept_handle.await;
        // Wait for replication tasks with timeout (matching Java's 10s per component)
        if let Some(h) = self.auditor_handle {
            let _ = tokio::time::timeout(
                tokio::time::Duration::from_secs(10), h
            ).await;
        }
        if let Some(h) = self.worker_handle {
            let _ = tokio::time::timeout(
                tokio::time::Duration::from_secs(10), h
            ).await;
        }
        if let Some(h) = self.gc_handle {
            let _ = tokio::time::timeout(
                tokio::time::Duration::from_secs(10), h
            ).await;
        }
        if let Some(h) = self.disk_monitor_handle {
            let _ = tokio::time::timeout(
                tokio::time::Duration::from_secs(5), h
            ).await;
        }
        // Unregister from ZooKeeper if configured
        if let Some((reg_mgr, bookie_id)) = self.zk_reg {
            let _ = reg_mgr.unregister_bookie(&bookie_id, false).await;
        }
        // Flush storage to persist any pending data
        let _ = self.storage.flush();
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
                            eprintln!("Disk monitor: error checking {dir}: {e}");
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
                if any_full && current == BookieState::Running {
                    if state.transition_to(BookieState::ReadOnly) {
                        crate::metrics::BOOKIE_STATE.set(2); // 2 = readonly
                        println!("Disk monitor: disk usage exceeds {:.0}% threshold, transitioning to READ-ONLY", threshold * 100.0);
                        update_zk_registration(&zk_info, true).await;
                    }
                } else if total_usage < lwm_threshold && current == BookieState::ReadOnly && dir_count > 0 {
                    // Recover: total usage across all dirs is below LWM (matching Java)
                    if state.transition_to(BookieState::Running) {
                        crate::metrics::BOOKIE_STATE.set(1); // 1 = writable
                        println!("Disk monitor: total disk usage ({:.1}%) below {:.0}% LWM, transitioning back to WRITABLE",
                            total_usage * 100.0, lwm_threshold * 100.0);
                        update_zk_registration(&zk_info, false).await;
                    }
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

        println!(
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
/// 2. Compare against local storage
/// 3. Delete local data for ledgers no longer in metadata
/// 4. Delete empty entry log files
async fn run_gc_loop(
    storage: Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage>,
    zk_client: zookeeper_client::Client,
    paths: bookkeeper_metadata::zk_path::ZkPaths,
    gc_wait_ms: u64,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    use bookkeeper_metadata::zk::ledger_manager::ZkLedgerManager;
    use bookkeeper_metadata::traits::LedgerManager;
    use bookkeeper_storage::gc::GarbageCollector;
    use std::collections::HashSet;

    let ledger_manager = ZkLedgerManager::new(zk_client, paths);
    let gc = GarbageCollector::new(storage);
    let interval = tokio::time::Duration::from_millis(gc_wait_ms);

    println!("GC thread started (interval: {}ms)", gc_wait_ms);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    println!("GC thread shutting down");
                    break;
                }
            }
            _ = tokio::time::sleep(interval) => {
                // Get all ledger IDs from metadata store
                match ledger_manager.get_ledger_ranges().await {
                    Ok(metadata_ledgers) => {
                        let active: HashSet<i64> = metadata_ledgers.into_iter().collect();
                        if let Err(e) = gc.gc(&active) {
                            eprintln!("GC cycle error: {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!("GC: failed to get metadata ledgers: {e}");
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
            eprintln!("AuditorElector exited with error: {e}");
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
            eprintln!("ReplicationWorker exited with error: {e}");
        }
    });

    println!("Auto-recovery daemon started (auditor elector + replication worker)");
    Ok((auditor_handle, worker_handle))
}

async fn accept_loop(
    listener: TcpListener,
    processor: Arc<RequestProcessor>,
    state: Arc<BookieStateManager>,
    mut shutdown_rx: watch::Receiver<bool>,
    max_frame_size: usize,
) {
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _peer_addr)) => {
                        let proc = processor.clone();
                        tokio::spawn(handle_connection(stream, proc, max_frame_size));
                    }
                    Err(_) => {
                        // Listener error — stop the loop
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

async fn handle_connection(
    stream: TcpStream,
    processor: Arc<RequestProcessor>,
    max_frame_size: usize,
) {
    let (mut reader, mut writer) = stream.into_split();
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
