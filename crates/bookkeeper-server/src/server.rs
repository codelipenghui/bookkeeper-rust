use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_util::codec::{Decoder, Encoder};

use bookkeeper_protocol::constants::{FLAG_DO_FENCING, V2_EOK};
use bookkeeper_protocol::framing::BookieCodec;
use bookkeeper_protocol::v2::request::{decode_request, V2Request};
use bookkeeper_protocol::v2::response::{encode_response, V2Response};
use bookkeeper_storage::db_ledger_storage::{DbLedgerStorage, CURRENT_DIR};

use bookkeeper_config::server::ServerConfig;
use crate::http_api;
use crate::processor::RequestProcessor;
use crate::state::{BookieState, BookieStateManager};
use crate::unclean_shutdown::UncleanShutdownDetection;

/// Prefer `config.metadata_service_uri` if set; otherwise synthesise a
/// ZK URI from `config.zk_servers` + `config.zk_ledger_root`. Delegates
/// to the shared helper so CLI and server agree on precedence.
fn resolve_metadata_uri(
    config: &bookkeeper_config::server::ServerConfig,
) -> Result<bookkeeper_metadata::metadata_paths::ParsedMetadataUri, String> {
    bookkeeper_metadata::metadata_paths::resolve_metadata_uri(
        config.metadata_service_uri.as_deref(),
        config.zk_servers.as_deref(),
        &config.zk_ledger_root,
        bookkeeper_config::parse_metadata_service_uri,
    )
    .map(|(uri, _display)| uri)
}

pub struct BookieServer {
    local_addr: SocketAddr,
    http_addr: Option<SocketAddr>,
    shutdown_tx: watch::Sender<bool>,
    accept_handle: Option<tokio::task::JoinHandle<()>>,
    /// In-flight connection tasks. shutdown() waits for this to drain
    /// before checkpointing so we don't ack a write that's about to be
    /// dropped.
    active_connections: Arc<std::sync::atomic::AtomicUsize>,
    storage: Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage>,
    state: Arc<BookieStateManager>,
    zk_reg: Option<(bookkeeper_metadata::backend::MetadataBackend, String)>,
    auditor_handle: Option<tokio::task::JoinHandle<()>>,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
    gc_handle: Option<tokio::task::JoinHandle<()>>,
    disk_monitor_handle: Option<tokio::task::JoinHandle<()>>,
    sync_handle: Option<tokio::task::JoinHandle<()>>,
    session_watcher_handle: Option<tokio::task::JoinHandle<()>>,
    scrubber_handle: Option<tokio::task::JoinHandle<()>>,
    /// Dedicated tokio runtime for the ZK client.
    ///
    /// Isolating the ZK session/heartbeat tasks from the main runtime
    /// prevents RPC-load-induced scheduler starvation from expiring the
    /// ZK session. Populated when ZK is configured; stopped via
    /// `shutdown_background()` on drop so that the enclosing async
    /// runtime is never blocked (important both in production and in
    /// `#[tokio::test]` contexts).
    zk_runtime: Option<tokio::runtime::Runtime>,
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

        // Environment validation at startup.
        // Matches Java's BookieImpl.checkEnvironment().
        check_environment(&config)?;

        // Multi-directory warning: the Rust bookie currently uses only the
        // first ledger directory and a single journal directory.  If the
        // operator has configured multiple directories (comma-separated
        // ledgerDirectories or pipe-separated journalDirectories), warn
        // loudly so this operational difference from Java is visible.
        // Matches Java's DbLedgerStorage which supports multi-directory striping.
        if config.ledger_directories.len() > 1 {
            tracing::warn!(
                "Multiple ledger directories configured ({:?}), but the Rust bookie \
                currently uses only the FIRST directory ({}). Data will NOT be striped \
                across disks. This differs from Java BookKeeper's DbLedgerStorage.",
                config.ledger_directories,
                config.ledger_directories[0],
            );
        }
        // Warn if a pipe-separated journalDirectories was collapsed to a
        // single journal_directory by the config parser.  The parser takes
        // the first element; a pipe in the journal_directory would indicate
        // the operator tried to supply multiple directories.
        if config.journal_directory.contains('|') {
            tracing::warn!(
                "journal_directory appears to contain multiple paths ('{}'), but the Rust \
                bookie uses a single journal directory. Journal I/O will NOT be parallelized.",
                config.journal_directory,
            );
        }

        // Open storage using first ledger dir
        let ledger_dir = config
            .ledger_directories
            .first()
            .ok_or("no ledger dirs configured")?;
        let io_engines = bookkeeper_storage::io_engine::select_engine();
        let entry_location_conf = config.entry_location_rocksdb_conf.as_deref().map(Path::new);
        let ledger_metadata_conf = config.ledger_metadata_rocksdb_conf.as_deref().map(Path::new);
        let storage_config = bookkeeper_storage::db_ledger_storage::StorageConfig::from_server_config(&config);
        let mut storage = DbLedgerStorage::open_full(
            Path::new(ledger_dir),
            io_engines.ledger.clone(),
            entry_location_conf,
            ledger_metadata_conf,
            storage_config,
        )?;

        // Create state manager early so the journal fatal-error callback can
        // reference it. Transition to Running happens after DIRTY file setup.
        let state = if config.persist_bookie_status_enabled {
            let status_path = std::path::Path::new(&config.ledger_directories[0])
                .join(bookkeeper_storage::db_ledger_storage::CURRENT_DIR)
                .join("bookie_status");
            Arc::new(BookieStateManager::with_persistence(&status_path))
        } else {
            Arc::new(BookieStateManager::new())
        };

        // Unclean-shutdown detection must happen BEFORE journal replay so
        // the replay can opt in to lenient scanning (skip_invalid_record)
        // when we know the last shutdown was abrupt. Matches Java where
        // `UncleanShutdownDetection` is checked at boot and informs the
        // operator / drives the optional integrity scan below.
        let unclean_detection = UncleanShutdownDetection::new(&config.ledger_directories);
        let was_unclean = unclean_detection.last_shutdown_was_unclean();
        if was_unclean {
            crate::metrics::BOOKIE_UNCLEAN_SHUTDOWN_DETECTED.set(1);
            tracing::warn!(
                "Unclean shutdown detected — forcing skip_invalid_record=true for journal replay \
                 and scheduling a post-replay integrity scan (if enabled)."
            );
        } else {
            crate::metrics::BOOKIE_UNCLEAN_SHUTDOWN_DETECTED.set(0);
        }

        // Initialize journal for write-ahead logging (crash recovery).
        // Controlled by `journalWriteData` (default true). When false, the
        // journal is skipped entirely — writes go directly to the write cache
        // without durability guarantees. Matches Java's behavior.
        if config.journal_write_data {
            let journal_current = Path::new(&config.journal_directory).join(CURRENT_DIR);
            std::fs::create_dir_all(&journal_current)?;
            let journal_config = bookkeeper_storage::journal::writer::JournalConfig {
                journal_dir: journal_current.clone(),
                max_file_size: (config.journal_max_size_mb as u64) * 1024 * 1024,
                sync_data: config.journal_sync_data,
                max_group_wait_ms: config.journal_max_group_wait_ms as u64,
                buffered_writes_threshold: config.journal_buffered_writes_threshold as usize,
                buffered_entries_threshold: config.journal_buffered_entries_threshold as usize,
                alignment_size: config.journal_alignment_size as usize,
                format_version: config.journal_format_version_to_write,
                max_queue_memory: config.journal_max_queue_memory_mb * 1024 * 1024,
                flush_when_queue_empty: config.journal_flush_when_queue_empty,
                remove_from_page_cache: config.journal_remove_from_page_cache,
                reuse_files: config.journal_reuse_files,
                max_backup_journals: config.journal_max_backups as usize,
                force_std_engine_mode: false,
                fsync_observer: None,
            };

            let fatal_state = state.clone();
            let on_journal_fatal: bookkeeper_storage::journal::writer::OnFatalError =
                Box::new(move |e| {
                    tracing::error!("Journal fatal error — transitioning to READ-ONLY: {e}");
                    fatal_state.transition_to(BookieState::ReadOnly);
                    crate::metrics::BOOKIE_STATE.set(2); // 2 = readonly
                });

            // On unclean shutdown, tolerate torn records at the tail of the
            // journal — they correspond to entries that were mid-write when
            // the process died. Clean shutdowns keep strict parsing.
            let skip_invalid = config.skip_replay_journal_invalid_record || was_unclean;
            let replayed = storage.init_journal(
                &journal_current,
                journal_config,
                io_engines.journal,
                Some(on_journal_fatal),
                skip_invalid,
            ).await?;
            if replayed > 0 {
                tracing::info!("Journal recovery: replayed {replayed} entries");
            }
        } else {
            tracing::info!("Journal disabled (journalWriteData=false)");
        }

        let storage_concrete = Arc::new(storage);
        storage_concrete.start_flush_worker();
        let metrics_sampler_storage = storage_concrete.clone();
        let storage: Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage> =
            storage_concrete;

        // Post-crash integrity scan — walks the entry_location_index and
        // verifies each entry can be read from its recorded entry log
        // position, catching torn writes / half-applied flushes that the
        // journal replay alone cannot detect. Gated on config so operators
        // can skip it when they have external tooling.
        if was_unclean && config.verify_storage_on_unclean_shutdown {
            let scan_start = std::time::Instant::now();
            match storage.local_consistency_check(0).await {
                Ok(errors) => {
                    crate::metrics::BOOKIE_UNCLEAN_RECOVERY_ERRORS.set(errors as i64);
                    tracing::info!(
                        "Post-crash integrity scan completed in {:?} — {} mismatches",
                        scan_start.elapsed(), errors
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "Post-crash integrity scan failed after {:?}: {e}",
                        scan_start.elapsed()
                    );
                }
            }
        }

        // Create DIRTY file in each ledger directory — signals unclean shutdown
        // if the process crashes before clean shutdown removes it.
        // Matches Java: written AFTER bookie.start() succeeds, BEFORE accepting connections.
        // Java aborts startup if DIRTY creation fails (re-throws IOException).
        unclean_detection.register_startup()
            .map_err(|e| format!("Failed to register startup (DIRTY file): {e}"))?;

        // Transition to Running or ReadOnly based on config.
        if config.force_read_only_bookie {
            state.transition_to(BookieState::Running); // must go through Running first
            state.transition_to(BookieState::ReadOnly);
            crate::metrics::BOOKIE_STATE.set(2); // 2 = readonly
            tracing::info!("Bookie starting in forced READ-ONLY mode");
        } else {
            state.transition_to(BookieState::Running);
            crate::metrics::BOOKIE_STATE.set(1); // 1 = writable/running
        }

        // Initialize Prometheus metrics
        crate::metrics::register_metrics();

        // Create request processor
        let storage_for_shutdown = storage.clone();
        let max_adds = if config.max_adds_in_progress_limit > 0 { config.max_adds_in_progress_limit } else { 10000 };
        let max_reads = if config.max_reads_in_progress_limit > 0 { config.max_reads_in_progress_limit } else { 10000 };
        let mut processor_inner = RequestProcessor::with_limits(storage.clone(), state.clone(), max_adds, max_reads);
        processor_inner.set_max_batch_read_count(config.max_batch_read_count);
        processor_inner.set_max_batch_read_size_bytes(config.max_batch_read_size_bytes);

        // Wire backpressure behavior from config.
        // Matches Java's waitTimeoutOnBackpressureMillis:
        // - Negative (default): block indefinitely (u64::MAX sentinel, handled
        //   by acquire_permit() as a plain .await to avoid tokio timer overhead).
        // - Zero: reject immediately with ETOOMANYREQUESTS (legacy mode).
        // - Positive: wait up to this duration, then reject.
        let backpressure_ms = config.wait_timeout_on_response_backpressure_ms;
        if backpressure_ms < 0 {
            processor_inner.set_backpressure_timeout_ms(u64::MAX);
        } else {
            processor_inner.set_backpressure_timeout_ms(backpressure_ms as u64);
        }

        // Wire auth enforcement: if an auth provider is configured (non-disabled),
        // connections must authenticate before other requests are accepted.
        // An unknown plugin class is a HARD ERROR (not a silent fallback to
        // disabled auth) so an operator typo cannot accidentally turn off
        // authentication in production.
        let auth_factory = crate::auth::create_auth_factory(
            config.bookie_auth_provider_factory_class.as_deref(),
            config.tls_client_authentication,
        )?;
        let auth_disabled = auth_factory.plugin_name() == "AUTHENTICATION_DISABLED";
        if !auth_disabled {
            processor_inner.set_require_auth(true);
            tracing::info!("Auth enabled: {} (connections must authenticate)", auth_factory.plugin_name());
        }

        let processor = Arc::new(processor_inner);

        // Shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let max_frame_size = config.max_frame_size;

        // Optionally register with ZooKeeper
        let mut auditor_handle: Option<tokio::task::JoinHandle<()>> = None;
        let mut worker_handle: Option<tokio::task::JoinHandle<()>> = None;
        let mut session_watcher_handle: Option<tokio::task::JoinHandle<()>> = None;

        // Dedicated tokio runtime for the ZK client. The
        // `zookeeper-client` crate spawns its session/heartbeat loop via
        // `tokio::spawn` inside `connect()`; isolating that on a
        // dedicated runtime prevents RPC load on the main runtime from
        // starving the heartbeat (which would cause a false
        // "session expired" under sustained load).
        //
        // One worker thread is enough — the ZK client is purely I/O-bound.
        let mut zk_runtime: Option<tokio::runtime::Runtime> = None;

        // Built inside the zk_reg block when either data-integrity flag
        // is set, so the cookie validator's optional pre-boot hook and
        // the later DataIntegrityService share one instance.
        let mut data_integ_check:
            Option<Arc<crate::datainteg::check::DataIntegrityCheck>> = None;

        let zk_reg = if config.zk_servers.is_some() || config.metadata_service_uri.is_some() {
            // The host to publish in the metadata store. When `advertisedAddress`
            // is set it wins (important for docker-compose style deployments where
            // the bind address is 0.0.0.0 but peers reach us via a DNS name from
            // the overlay network). Otherwise fall back to the actual bind address.
            // Bookie identity resolution order (matching Java):
            // 1. Explicit bookieId config (highest priority)
            // 2. advertisedAddress config
            // 3. Actual bind address (fallback)
            let (advertised_host, bookie_id) = if let Some(ref explicit_id) = config.bookie_id {
                // bookieId overrides everything. Extract host portion for display.
                let host = explicit_id.split(':').next().unwrap_or(explicit_id).to_string();
                (host, explicit_id.clone())
            } else {
                match &config.advertised_address {
                    Some(adv) => {
                        let (host, port) = match adv.split_once(':') {
                            Some((h, p)) => (
                                h.to_string(),
                                p.parse::<u16>().unwrap_or(local_addr.port()),
                            ),
                            None => (adv.clone(), local_addr.port()),
                        };
                        let id = format!("{}:{}", host, port);
                        (host, id)
                    }
                    None => {
                        let host = local_addr.ip().to_string();
                        (host.clone(), format!("{}:{}", host, local_addr.port()))
                    }
                }
            };

            let uri = resolve_metadata_uri(&config)
                .map_err(|e| format!("metadata URI: {e}"))?;

            // Dedicated ZK runtime — only for the ZK backend. Phase 3 oxia skips this.
            let zk_handle = match &uri {
                bookkeeper_metadata::metadata_paths::ParsedMetadataUri::Zk { .. } => {
                    let rt = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(1)
                        .thread_name("zk-runtime")
                        .enable_all()
                        .build()
                        .map_err(|e| format!("ZK runtime: {e}"))?;
                    let h = rt.handle().clone();
                    zk_runtime = Some(rt);
                    Some(h)
                }
                _ => None,
            };

            let backend = bookkeeper_metadata::backend::open_metadata_backend(
                &uri,
                zk_handle,
                None,
            )
            .await
            .map_err(|e| format!("metadata connect: {e}"))?;

            data_integ_check =
                build_data_integrity_check(&config, &bookie_id, storage.clone()).await?;
            run_cookie_validation(
                backend.registration_manager.as_ref(),
                &bookie_id,
                &config,
                data_integ_check.clone(),
            )
            .await?;
            let info = bookkeeper_metadata::service_info::BookieServiceInfo::new(
                &bookie_id,
                &advertised_host,
                local_addr.port(),
            );
            backend.registration_manager
                .register_bookie(&bookie_id, false, &info)
                .await
                .map_err(|e| format!("metadata register: {e}"))?;

            // Start ZK session watcher — monitors for session changes and
            // re-registers the bookie's ephemeral znode when the session ID
            // flips. A session replacement (seen as either a fresh
            // `SyncConnected` with a new session id, or `Expired` followed
            // by reconnect) wipes the ephemeral at
            // `/ledgers/available/<bookie-id>`; without re-registration the
            // bookie becomes invisible to clients even though the new
            // session is healthy. This matches Java BookKeeper's
            // BookieStateManager which re-registers rather than exits.
            if let Some(ref raw) = backend.zk_raw {
                let watcher_bookie_id = bookie_id.clone();
                let watcher_shutdown = shutdown_rx.clone();
                let mut session_watcher = raw.client.state_watcher();
                let watcher_client = raw.client.clone();
                let watcher_reg_path =
                    raw.paths.bookie_registration(&bookie_id);
                let watcher_reg_data =
                    bookkeeper_metadata::service_info::BookieServiceInfo::new(
                        &bookie_id,
                        &advertised_host,
                        local_addr.port(),
                    )
                    .to_protobuf_bytes();
                let initial_session = raw.client.session_id();

                session_watcher_handle = Some(tokio::spawn(async move {
                    run_session_watcher(
                        &mut session_watcher,
                        &watcher_bookie_id,
                        &watcher_client,
                        &watcher_reg_path,
                        &watcher_reg_data,
                        initial_session,
                        watcher_shutdown,
                    ).await;
                }));
            }

            // Start auto-recovery daemon if enabled
            if config.auto_recovery_daemon_enabled {
                if let Some(ref raw) = backend.zk_raw {
                    let (ah, wh) = start_auto_recovery(
                        raw.client.clone(),
                        raw.paths.clone(),
                        backend.ledger_manager.clone(),
                        &bookie_id,
                        config.lost_bookie_recovery_delay as u64,
                        config.rw_rereplicate_backoff_ms as u64,
                        config.auditor_periodic_check_interval as u64,
                        config.auditor_periodic_placement_policy_check_interval as u64,
                        config.auditor_periodic_replicas_check_interval as u64,
                        shutdown_rx.clone(),
                    )
                    .await?;
                    auditor_handle = Some(ah);
                    worker_handle = Some(wh);
                }
            }

            Some((backend, bookie_id))
        } else {
            None
        };

        // Shared GC control flags — used by both the GC loop and HTTP API.
        let gc_suspend_major = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gc_suspend_minor = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gc_force_major = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gc_force_minor = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Start GC thread if metadata backend is available
        let gc_handle = if zk_reg.is_some() {
            let gc_storage = storage.clone();
            let gc_ledger_io = io_engines.ledger.clone();
            let gc_ledger_manager = zk_reg.as_ref().unwrap().0.ledger_manager.clone();
            let gc_wait_ms = config.gc_wait_time as u64;
            let gc_config = bookkeeper_storage::gc::GcConfig {
                minor_compaction_threshold: config.minor_compaction_threshold,
                major_compaction_threshold: config.major_compaction_threshold,
                compaction_rate_bytes_per_sec: config.compaction_rate_bytes_per_sec,
                compaction_max_time_ms: config.compaction_max_time_ms,
                minor_compaction_interval: config.minor_compaction_interval,
                major_compaction_interval: config.major_compaction_interval,
                compaction_max_outstanding_requests: config.compaction_max_outstanding_requests,
                minor_compaction_max_time_ms: config.minor_compaction_max_time_ms,
                major_compaction_max_time_ms: config.major_compaction_max_time_ms,
                is_force_gc_allow_when_no_space: config.is_force_gc_allow_when_no_space,
            };
            let metadata_cache_dir = if config.gc_entry_log_metadata_cache_enabled {
                Some(std::path::Path::new(&config.ledger_directories[0])
                    .join(bookkeeper_storage::db_ledger_storage::CURRENT_DIR)
                    .join("metadata-cache"))
            } else {
                None
            };
            let gc_shutdown = shutdown_rx.clone();
            let gc_suspend_major_c = gc_suspend_major.clone();
            let gc_suspend_minor_c = gc_suspend_minor.clone();
            let gc_force_major_c = gc_force_major.clone();
            let gc_force_minor_c = gc_force_minor.clone();

            let gc_shutdown_tx = shutdown_tx.clone();
            Some(tokio::spawn(async move {
                use futures::future::FutureExt;
                let fut = std::panic::AssertUnwindSafe(run_gc_loop(
                    gc_storage, gc_ledger_io, gc_ledger_manager, gc_wait_ms,
                    gc_config, metadata_cache_dir, gc_shutdown,
                    gc_suspend_major_c, gc_suspend_minor_c,
                    gc_force_major_c, gc_force_minor_c,
                )).catch_unwind().await;
                if let Err(e) = fut {
                    let msg = match e.downcast_ref::<&'static str>() {
                        Some(s) => s.to_string(),
                        None => match e.downcast_ref::<String>() {
                            Some(s) => s.clone(),
                            None => "(non-string panic payload)".into(),
                        },
                    };
                    tracing::error!(panic = %msg, "GC loop panicked — initiating bookie shutdown");
                    let _ = gc_shutdown_tx.send(true);
                }
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
            let monitor_warn = config.disk_usage_warn_threshold;
            let monitor_interval_ms = config.disk_check_interval as u64;
            let monitor_shutdown = shutdown_rx.clone();
            // Pass ZK registration info for updating registration on state change
            let monitor_zk_reg = zk_reg.as_ref().and_then(|(backend, bid)| {
                backend.zk_raw.as_ref().map(|raw| {
                    (raw.client.clone(), raw.paths.clone(), bid.clone())
                })
            });

            let monitor_hp_flag = processor.high_priority_writes_flag();
            let monitor_min_hp_size = config.min_usable_size_for_high_priority_writes;
            let monitor_shutdown_tx = shutdown_tx.clone();

            Some(tokio::spawn(async move {
                run_disk_monitor(
                    monitor_state,
                    monitor_dirs,
                    monitor_threshold,
                    monitor_lwm,
                    monitor_warn,
                    monitor_interval_ms,
                    monitor_zk_reg,
                    monitor_shutdown,
                    monitor_hp_flag,
                    monitor_min_hp_size,
                    monitor_shutdown_tx,
                ).await;
            }))
        } else {
            None
        };

        // Optionally start HTTP admin API server
        let http_addr = if config.http_server_enabled {
            let http_port = config.http_server_port as u16;
            let (zk_client, zk_paths, http_bookie_id, http_ledger_manager) = match &zk_reg {
                Some((backend, bid)) => (
                    backend.zk_raw.as_ref().map(|r| r.client.clone()),
                    backend.zk_raw.as_ref().map(|r| r.paths.clone()),
                    Some(bid.clone()),
                    Some(backend.ledger_manager.clone()),
                ),
                None => (None, None, None, None),
            };
            let http_state = http_api::HttpState {
                state_manager: state.clone(),
                storage: Some(storage.clone()),
                ledger_dirs: config.ledger_directories.clone(),
                journal_dir: config.journal_directory.clone(),
                zk_client,
                zk_paths,
                bookie_id: http_bookie_id,
                ledger_manager: http_ledger_manager,
                gc_suspend_major: gc_suspend_major.clone(),
                gc_suspend_minor: gc_suspend_minor.clone(),
                gc_force_major: gc_force_major.clone(),
                gc_force_minor: gc_force_minor.clone(),
                gc_status: http_api::GcStatus::default(),
                http_admin_token: config.http_admin_token.clone(),
                // Cap concurrent recover-bookie / decommission spawns. 16
                // is generous for a real operator workload and bounds the
                // amplification factor for a misbehaving / malicious
                // caller.
                recover_inflight: Arc::new(tokio::sync::Semaphore::new(16)),
            };
            let router = http_api::create_router(http_state);
            let http_host = &config.http_server_host;
            let http_listener = tokio::net::TcpListener::bind(format!("{http_host}:{http_port}")).await?;
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
        let sync_shutdown_tx = shutdown_tx.clone();
        let sync_handle = Some(tokio::spawn(async move {
            use futures::future::FutureExt;
            let fut = std::panic::AssertUnwindSafe(
                run_sync_thread(sync_storage, flush_interval_ms, sync_shutdown),
            )
            .catch_unwind()
            .await;
            if let Err(e) = fut {
                let msg = match e.downcast_ref::<&'static str>() {
                    Some(s) => s.to_string(),
                    None => match e.downcast_ref::<String>() {
                        Some(s) => s.clone(),
                        None => "(non-string panic payload)".into(),
                    },
                };
                tracing::error!(panic = %msg, "sync thread panicked — initiating bookie shutdown");
                let _ = sync_shutdown_tx.send(true);
            }
        }));

        // Sample write-cache gauges once per second. Kept off the add-entry
        // hot path because the underlying counters walk every DashMap shard.
        let metrics_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut shutdown = metrics_shutdown;
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = shutdown.changed() => if *shutdown.borrow() { break },
                    _ = tick.tick() => metrics_sampler_storage.publish_cache_metrics(),
                }
            }
        });

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

        // Shared counter for in-flight connection tasks. Exposed to
        // shutdown() so checkpoint waits for connections to drain
        // instead of running while writes are still being acked.
        let active_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accept_handle = tokio::spawn(accept_loop(
            listener,
            processor,
            state.clone(),
            shutdown_rx,
            max_frame_size,
            tls_acceptor,
            config.server_sock_keepalive,
            config.server_tcp_no_delay,
            config.max_connections,
            active_connections.clone(),
        ));

        // Start scrubber if localScrubInterval > 0
        let scrubber_handle = if config.local_scrub_interval > 0 {
            Some(crate::scrubber::start_scrubber(
                storage_for_shutdown.clone(),
                config.local_scrub_interval as u64,
                config.local_scrub_rate_limit as u64,
                shutdown_tx.subscribe(),
            ))
        } else {
            None
        };

        // Optionally start the data-integrity service (Java parity).
        // Gated on `data_integrity_checking` — disabled by default.
        // Reuses the DataIntegrityCheck built earlier for the cookie
        // validator so pre-boot and full-check share cached ledger state.
        let _data_integ_handle = match (&data_integ_check, config.data_integrity_checking) {
            (Some(check), true) => Some(spawn_data_integrity_service(
                Arc::clone(check),
                shutdown_tx.subscribe(),
            )),
            _ => None,
        };

        Ok(BookieServer {
            local_addr,
            http_addr,
            shutdown_tx,
            accept_handle: Some(accept_handle),
            active_connections,
            storage: storage_for_shutdown,
            state,
            zk_reg,
            auditor_handle,
            worker_handle,
            gc_handle,
            disk_monitor_handle,
            sync_handle,
            session_watcher_handle,
            scrubber_handle,
            zk_runtime,
            ledger_directories: config.ledger_directories.clone(),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn http_addr(&self) -> Option<SocketAddr> {
        self.http_addr
    }

    /// Access the bookie's state manager (for tests and HTTP admin).
    pub fn state_manager(&self) -> Arc<BookieStateManager> {
        self.state.clone()
    }


    /// Graceful shutdown matching Java's BookieImpl.shutdown() sequence:
    ///
    /// Java order: lock → forceToReadOnly → SyncThread.shutdown → Journal.shutdown
    ///             → LedgerStorage.shutdown → DiskMonitor.shutdown → StateManager.close (ZK)
    pub async fn shutdown(mut self) {
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
        if let Some(h) = self.auditor_handle.take() {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(10), h).await;
        }
        if let Some(h) = self.worker_handle.take() {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(10), h).await;
        }
        if let Some(h) = self.gc_handle.take() {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(10), h).await;
        }
        if let Some(h) = self.disk_monitor_handle.take() {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(5), h).await;
        }
        if let Some(h) = self.sync_handle.take() {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(10), h).await;
        }
        if let Some(h) = self.session_watcher_handle.take() {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(5), h).await;
        }
        if let Some(h) = self.scrubber_handle.take() {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(10), h).await;
        }
        tracing::info!("Background tasks stopped.");

        // Step 4: Wait for accept loop to finish (stops accepting new
        // connections), then drain in-flight per-connection tasks.
        if let Some(h) = self.accept_handle.take() {
            h.await.ok();
        }
        tracing::info!("Accept loop stopped.");
        // Wait up to 10s for active connection tasks to drain. Without
        // this, checkpoint can run while a connection task is still
        // emitting an add-entry response — leading to writes that were
        // ack'd but whose journal record didn't make it to disk before
        // the journal was flushed and rotated.
        {
            let drain_deadline = tokio::time::Instant::now()
                + tokio::time::Duration::from_secs(10);
            while self.active_connections.load(std::sync::atomic::Ordering::Acquire) > 0 {
                if tokio::time::Instant::now() >= drain_deadline {
                    let stragglers = self
                        .active_connections
                        .load(std::sync::atomic::Ordering::Acquire);
                    tracing::warn!(
                        stragglers,
                        "shutdown drain timed out — connections still in flight; \
                         proceeding to checkpoint anyway",
                    );
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            tracing::info!("In-flight connections drained.");
        }

        // Step 5: Flush storage — persist write cache to entry logs + checkpoint.
        // Matches Java's SyncThread.shutdown() which waits up to 5 minutes.
        // Journal cleanup happens inside checkpoint().
        tracing::info!("Flushing storage...");
        let checkpoint_clean = match tokio::time::timeout(
            tokio::time::Duration::from_secs(300),
            self.storage.checkpoint(),
        ).await {
            Ok(Ok(())) => {
                tracing::info!("Storage flushed.");
                true
            }
            Ok(Err(e)) => {
                tracing::warn!("WARNING: checkpoint failed during shutdown: {e}");
                false
            }
            Err(_) => {
                tracing::warn!("WARNING: storage flush timed out after 5 minutes");
                false
            }
        };

        // Step 6: Remove DIRTY files — clean shutdown confirmed.
        // ONLY remove the markers if the checkpoint actually succeeded.
        // If the flush failed or timed out, the in-memory state may not
        // be on disk; leaving the DIRTY markers in place tells the next
        // boot the previous shutdown was unclean and triggers the
        // recovery path that surfaces any partial-flush corruption.
        // Matches Java's behavior of deleting DIRTY only AFTER
        // bookie.shutdown() completes successfully.
        if checkpoint_clean {
            UncleanShutdownDetection::new(&self.ledger_directories).register_clean_shutdown();
        } else {
            tracing::warn!(
                "Leaving DIRTY markers in place because storage flush did \
                 not complete cleanly; next boot will run unclean-shutdown \
                 recovery."
            );
        }

        // Step 7: Transition to ShuttingDown
        self.state.transition_to(BookieState::ShuttingDown);

        // Step 8: Unregister from metadata store LAST.
        // Matches Java's StateManager.close() as the final step.
        if let Some((backend, bookie_id)) = self.zk_reg.take() {
            let _ = backend.registration_manager.unregister_bookie(&bookie_id, false).await;
            tracing::info!("Unregistered from metadata store.");
        }

        // Step 9: Stop the dedicated ZK runtime via shutdown_background so the
        // enclosing runtime is never blocked. The Drop impl also does this as a
        // safety net; shutdown_background is idempotent (the take() makes it a no-op
        // on the subsequent Drop).
        if let Some(rt) = self.zk_runtime.take() {
            rt.shutdown_background();
        }

        tracing::info!("Graceful shutdown complete.");
    }
}

impl Drop for BookieServer {
    /// Stop the dedicated ZK runtime without blocking the current thread.
    ///
    /// Tokio's `Runtime::drop` performs a *blocking* shutdown, which panics
    /// when called from within an async context (e.g. `#[tokio::test]`).
    /// `shutdown_background()` schedules cleanup on a background thread
    /// instead, satisfying the no-blocking-in-async-context constraint.
    fn drop(&mut self) {
        if let Some(rt) = self.zk_runtime.take() {
            rt.shutdown_background();
        }
    }
}

async fn validate_cookie(
    zk_reg: &dyn bookkeeper_metadata::traits::RegistrationManager,
    bookie_id: &str,
    config: &ServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_metadata::cookie::Cookie;

    // Java stores the cookie as a `VERSION` file inside each `current/`
    // directory. Match that convention so an in-place upgrade from Java
    // finds the file where it expects it.
    //
    // Validate against ALL directories (journal + ledger + index) to detect
    // inconsistencies. Matches Java's Cookie.verify() which checks all dirs.
    let journal_current = std::path::Path::new(&config.journal_directory).join(CURRENT_DIR);
    let disk_cookie_path = journal_current.join("VERSION");

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
            // First time — create cookie in ZK, journal dir, and ALL ledger dirs.
            // Matches Java's Cookie.writeToRegistrationManager() + Cookie.writeToDirectory().
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
            // Write to journal dir
            std::fs::create_dir_all(&journal_current)
                .map_err(|e| format!("create journal dir: {e}"))?;
            std::fs::write(&disk_cookie_path, &cookie_str)
                .map_err(|e| format!("write disk cookie: {e}"))?;
            // Write to all ledger and index dirs
            for dir in config.ledger_directories.iter().chain(
                config.index_directories.iter().flat_map(|v| v.iter()),
            ) {
                write_cookie_to_dir(dir, &cookie_str)?;
            }
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

    // Validate cookies in all ledger and index directories against the journal cookie.
    // Matches Java's Cookie.verify() which checks every directory for consistency.
    if disk_cookie_path.exists() {
        let ref_str = std::fs::read_to_string(&disk_cookie_path)
            .map_err(|e| format!("re-read journal cookie: {e}"))?;
        let ref_cookie = Cookie::deserialize(&ref_str)
            .map_err(|e| format!("re-parse journal cookie: {e}"))?;

        for dir in config.ledger_directories.iter().chain(
            config.index_directories.iter().flat_map(|v| v.iter()),
        ) {
            validate_cookie_in_dir(dir, &ref_cookie)?;
        }
    }

    Ok(())
}

/// Monitors ZK session state and re-registers the bookie's ephemeral
/// registration whenever the session ID changes.
///
/// Matches Java BookKeeper's `BookieStateManager` which re-registers on
/// session loss rather than killing the process. Two distinct failure
/// modes motivate this:
///
/// 1. **Expired**: the session is dead, ephemeral nodes were cleaned up
///    by the ZK ensemble. The `zookeeper-client` crate's Client tears
///    down and re-establishes internally, yielding a fresh session id.
/// 2. **Silent session replacement**: after a `Disconnected` +
///    `SyncConnected` cycle the crate can hand back a *different*
///    session id without firing an `Expired` event in between (observed
///    when the ZK server restarts within the session timeout). In this
///    case the old session's ephemerals are still gone, but the state
///    transitions skipped the `Expired` signal.
///
/// In both cases we observe `SyncConnected` on a new session id, and
/// call `re_register` (which uses `create → NodeExists → set_data` and
/// is therefore idempotent). Previously we hard-exited on `Expired`,
/// relying on a supervisor to restart the bookie — fine on k8s but
/// unnecessarily disruptive, and it did nothing for the silent-replace
/// case which is what actually bit us in practice.
///
/// `AuthFailed` remains fatal: the bookie's credentials are wrong and
/// a restart under a supervisor is appropriate.
async fn run_session_watcher(
    watcher: &mut zookeeper_client::StateWatcher,
    bookie_id: &str,
    client: &zookeeper_client::Client,
    reg_path: &str,
    reg_data: &[u8],
    initial_session: zookeeper_client::SessionId,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    use zookeeper_client::SessionState;
    let mut last_session = initial_session;

    loop {
        tokio::select! {
            session_state = watcher.changed() => {
                match session_state {
                    SessionState::Disconnected => {
                        tracing::warn!("ZK session disconnected — waiting for reconnect \
                            (ephemeral nodes survive within same session)");
                    }
                    SessionState::SyncConnected => {
                        let current = client.session_id();
                        if current == last_session {
                            tracing::info!("ZK session reconnected (same session: {current:?})");
                        } else {
                            tracing::warn!(
                                "ZK session replaced silently: {last_session:?} → {current:?}. \
                                 Ephemeral registration was dropped; re-registering."
                            );
                            if let Err(e) = re_register_ephemeral(
                                client, reg_path, reg_data,
                            ).await {
                                tracing::error!(
                                    "Re-registration after session replacement failed: {e}. \
                                     Bookie may be invisible to clients until next session event."
                                );
                            } else {
                                tracing::info!(
                                    "Bookie {bookie_id} re-registered on session {current:?}"
                                );
                            }
                            last_session = current;
                        }
                    }
                    SessionState::Expired => {
                        // The old session is gone; the crate will reconnect
                        // and surface a fresh `SyncConnected` with a new
                        // session id. Defer the re-registration to that
                        // event so we don't race the reconnect. If the
                        // client never comes back, the bookie simply stays
                        // unregistered until a supervisor intervenes —
                        // same outcome as the previous `exit(1)` policy,
                        // without taking the whole process down.
                        tracing::warn!(
                            "ZK session expired for bookie {bookie_id}. \
                             Waiting for reconnect to re-register ephemeral."
                        );
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

/// Idempotent create-or-update of the bookie's ephemeral registration
/// znode. `NodeExists` falls back to `set_data` — the stale ephemeral
/// from the previous session is swept by the ZK server ~session-timeout
/// after expiry, so a quick reconnect can legitimately land on an
/// already-present znode owned by the previous session's inode.
async fn re_register_ephemeral(
    client: &zookeeper_client::Client,
    path: &str,
    data: &[u8],
) -> Result<(), zookeeper_client::Error> {
    let options = zookeeper_client::CreateMode::Ephemeral
        .with_acls(zookeeper_client::Acls::anyone_all());
    match client.create(path, data, &options).await {
        Ok(_) => Ok(()),
        Err(zookeeper_client::Error::NodeExists) => {
            client.set_data(path, data, None).await.map(|_| ())
        }
        Err(e) => Err(e),
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
    /// Fatal disk I/O error — trigger bookie shutdown.
    /// Matches Java's DiskErrorException → triggerBookieShutdown.
    Shutdown,
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
    all_below_lwm: bool,
    _total_usage: f64,
    lwm_threshold: f64,
    dir_count: usize,
    has_disk_error: bool,
) -> DiskMonitorAction {
    let _ = (lwm_threshold,); // kept in signature for compatibility

    // Disk I/O error → shutdown. Matches Java's DiskErrorException handling
    // where LedgerDirsMonitor calls listener.diskFailed() → triggerBookieShutdown.
    if has_disk_error {
        return DiskMonitorAction::Shutdown;
    }

    if any_full && current == BookieState::Running {
        return DiskMonitorAction::GoReadOnly;
    }
    // Recovery: ALL directories must be below LWM (per-dir check).
    // Matches Java's LedgerDirsMonitor which re-qualifies individual dirs.
    if current == BookieState::ReadOnly
        && dir_count > 0
        && all_below_lwm
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
    warn_threshold: f64,
    interval_ms: u64,
    zk_info: Option<(zookeeper_client::Client, bookkeeper_metadata::zk_path::ZkPaths, String)>,
    mut shutdown_rx: watch::Receiver<bool>,
    high_priority_writes_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    min_usable_size_for_high_priority_writes: i64,
    shutdown_tx: watch::Sender<bool>,
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
                let mut all_below_lwm = true;
                let mut total_used: f64 = 0.0;
                let mut total_capacity: f64 = 0.0;
                let mut dir_count = 0;
                let mut has_disk_error = false;

                for dir in &ledger_dirs {
                    match get_disk_usage_details(dir) {
                        Ok((usage, used_bytes, total_bytes)) => {
                            if usage > threshold {
                                any_full = true;
                            }
                            if usage > warn_threshold && usage <= threshold {
                                tracing::warn!(
                                    "Disk usage for {dir}: {:.1}% exceeds warn threshold ({:.1}%)",
                                    usage * 100.0,
                                    warn_threshold * 100.0,
                                );
                            }
                            if usage >= lwm_threshold {
                                all_below_lwm = false;
                            }
                            total_used += used_bytes;
                            total_capacity += total_bytes;
                            dir_count += 1;
                        }
                        Err(e) => {
                            // Distinguish I/O errors from transient issues.
                            // If the directory is unreachable or unreadable,
                            // this is a physical disk failure.
                            // Matches Java's DiskErrorException detection.
                            tracing::error!("Disk monitor: I/O error checking {dir}: {e}");
                            has_disk_error = true;
                            all_below_lwm = false;
                        }
                    }
                }

                let total_usage = if total_capacity > 0.0 {
                    total_used / total_capacity
                } else {
                    0.0
                };

                let current = state.state();
                match decide_disk_monitor_action(
                    current, any_full, all_below_lwm, total_usage, lwm_threshold, dir_count, has_disk_error,
                ) {
                    DiskMonitorAction::GoReadOnly => {
                        if state.transition_to(BookieState::ReadOnly) {
                            crate::metrics::BOOKIE_STATE.set(2); // 2 = readonly
                            tracing::info!("Disk monitor: disk usage exceeds {:.0}% threshold, transitioning to READ-ONLY", threshold * 100.0);

                            // Determine if high-priority writes should remain available.
                            // Matches Java's LedgerDirsMonitor.allDisksFull(highPriorityWritesAllowed):
                            // if any directory has enough free space for high-priority writes,
                            // keep accepting recovery adds even in read-only mode.
                            let hp_allowed = if min_usable_size_for_high_priority_writes > 0 {
                                ledger_dirs.iter().any(|dir| {
                                    if let Ok((_usage, used, total)) = get_disk_usage_details(dir) {
                                        let available = total - used;
                                        available >= min_usable_size_for_high_priority_writes as f64
                                    } else {
                                        false
                                    }
                                })
                            } else {
                                // When minUsableSizeForHighPriorityWrites is 0 (default),
                                // always allow high-priority writes in read-only mode.
                                // Matches Java default: availableForHighPriorityWrites starts
                                // as true and is only disabled when explicitly configured and
                                // disk space is below the threshold.
                                true
                            };
                            high_priority_writes_flag.store(hp_allowed, std::sync::atomic::Ordering::Release);
                            if hp_allowed {
                                tracing::info!("Disk monitor: high-priority writes (recovery adds) remain enabled in read-only mode");
                            } else {
                                tracing::info!("Disk monitor: high-priority writes disabled — insufficient reserved space");
                            }

                            update_zk_registration(&zk_info, true).await;
                        }
                    }
                    DiskMonitorAction::GoRunning => {
                        // Use try_recover_to_running which respects the manual read-only flag.
                        // Matches Java's LedgerDirsMonitor which checks isManuallyModified.
                        if state.try_recover_to_running() {
                            crate::metrics::BOOKIE_STATE.set(1); // 1 = writable
                            tracing::info!("Disk monitor: total disk usage ({:.1}%) below {:.0}% LWM, transitioning back to WRITABLE",
                                total_usage * 100.0, lwm_threshold * 100.0);
                            // Re-enable high-priority writes when recovering to writable.
                            // Matches Java's BookieImpl.allDisksFull(true) → setHighPriorityWritesAvailability(true).
                            high_priority_writes_flag.store(true, std::sync::atomic::Ordering::Release);
                            update_zk_registration(&zk_info, false).await;
                        }
                    }
                    DiskMonitorAction::Shutdown => {
                        tracing::error!("Disk monitor: fatal disk I/O error detected, initiating shutdown");
                        state.transition_to(BookieState::ShuttingDown);
                        // Tell the rest of the bookie to wind down. Without
                        // this, the listener accept-loop and processor tasks
                        // would carry on accepting connections while the
                        // state machine sits in ShuttingDown — defeating
                        // the whole point of the fatal-disk action.
                        let _ = shutdown_tx.send(true);
                        break;
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
        // Reconstruct host/port from the `host:port` bookie_id so the
        // BookieServiceInfo znode data has a client-reachable address.
        let (host, port) = bookie_id
            .rsplit_once(':')
            .and_then(|(h, p)| p.parse::<u16>().ok().map(|p| (h.to_string(), p)))
            .unwrap_or_else(|| (String::new(), 0));
        let info = bookkeeper_metadata::service_info::BookieServiceInfo::new(
            bookie_id, &host, port,
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

    bookkeeper_common::disk::statvfs_usage(std::path::Path::new(path))
        .map_err(|e| format!("statvfs failed for {path}: {e}"))
}

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
                    if let Err(e) = storage.checkpoint().await {
                        tracing::warn!("SyncThread: final checkpoint error: {e}");
                    }
                    break;
                }
            }
            _ = tokio::time::sleep(interval) => {
                // Periodic checkpoint: flush write cache → entry logs → clean journals
                if let Err(e) = storage.checkpoint().await {
                    tracing::warn!("SyncThread: checkpoint error: {e}");
                }
            }
        }
    }
}

fn write_cookie_to_dir(dir: &str, cookie_str: &str) -> Result<(), Box<dyn std::error::Error>> {
    let current = std::path::Path::new(dir).join(CURRENT_DIR);
    std::fs::create_dir_all(&current)
        .map_err(|e| format!("create dir {dir}/current: {e}"))?;
    std::fs::write(current.join("VERSION"), cookie_str)
        .map_err(|e| format!("write cookie {dir}: {e}"))?;
    Ok(())
}

fn validate_cookie_in_dir(
    dir: &str,
    ref_cookie: &bookkeeper_metadata::cookie::Cookie,
) -> Result<(), Box<dyn std::error::Error>> {
    let version_path = std::path::Path::new(dir).join(CURRENT_DIR).join("VERSION");
    if version_path.exists() {
        let s = std::fs::read_to_string(&version_path)
            .map_err(|e| format!("read cookie {dir}: {e}"))?;
        let cookie = bookkeeper_metadata::cookie::Cookie::deserialize(&s)
            .map_err(|e| format!("parse cookie {dir}: {e}"))?;
        cookie.validate_against(ref_cookie)
            .map_err(|e| format!("cookie mismatch {dir}: {e}"))?;
    }
    Ok(())
}

fn check_environment(config: &ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Validate journal directory exists or can be created
    let journal_dir = &config.journal_directory;
    let journal_current = std::path::Path::new(journal_dir).join(CURRENT_DIR);
    std::fs::create_dir_all(&journal_current)
        .map_err(|e| format!("cannot create journal dir {journal_dir}/current: {e}"))?;

    // Validate all ledger directories
    for dir in &config.ledger_directories {
        let ledger_current = std::path::Path::new(dir).join(CURRENT_DIR);
        std::fs::create_dir_all(&ledger_current)
            .map_err(|e| format!("cannot create ledger dir {dir}/current: {e}"))?;
    }

    // Validate index directories if configured
    if let Some(ref index_dirs) = config.index_directories {
        for dir in index_dirs {
            let idx_current = std::path::Path::new(dir).join(CURRENT_DIR);
            std::fs::create_dir_all(&idx_current)
                .map_err(|e| format!("cannot create index dir {dir}/current: {e}"))?;
        }
    }

    // Warn if journal and ledger dirs are on the same filesystem.
    // Matches Java's checkIfDirsOnSameDiskPartition().
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Ok(journal_meta) = std::fs::metadata(journal_dir) {
            let journal_dev = journal_meta.dev();
            for dir in &config.ledger_directories {
                if let Ok(ledger_meta) = std::fs::metadata(dir) {
                    if ledger_meta.dev() == journal_dev && dir != journal_dir {
                        tracing::warn!(
                            "Journal dir ({journal_dir}) and ledger dir ({dir}) are on the same filesystem. \
                             This may impact performance. Java BK rejects this unless \
                             allowMultipleDirsUnderSameDiskPartition=true."
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

/// 4. Delete empty entry log files
async fn run_gc_loop(
    storage: Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage>,
    ledger_io: Arc<dyn bookkeeper_storage::io_engine::LedgerIo>,
    ledger_manager: Arc<dyn bookkeeper_metadata::traits::LedgerManager>,
    gc_wait_ms: u64,
    gc_config: bookkeeper_storage::gc::GcConfig,
    metadata_cache_dir: Option<std::path::PathBuf>,
    mut shutdown_rx: watch::Receiver<bool>,
    suspend_major: Arc<std::sync::atomic::AtomicBool>,
    suspend_minor: Arc<std::sync::atomic::AtomicBool>,
    force_major: Arc<std::sync::atomic::AtomicBool>,
    force_minor: Arc<std::sync::atomic::AtomicBool>,
) {
    use bookkeeper_storage::gc::GarbageCollector;
    use bookkeeper_storage::index::entry_log_meta_map::EntryLogMetadataMap;
    use std::collections::HashSet;
    use std::sync::atomic::Ordering;

    let meta_map = metadata_cache_dir.and_then(|dir| {
        match EntryLogMetadataMap::open(&dir) {
            Ok(m) => {
                tracing::info!("GC: persistent entry log metadata cache opened at {}", dir.display());
                Some(m)
            }
            Err(e) => {
                tracing::warn!("GC: failed to open persistent metadata cache: {e}");
                None
            }
        }
    });
    let minor_interval_secs = gc_config.minor_compaction_interval;
    let major_interval_secs = gc_config.major_compaction_interval;
    let gc = GarbageCollector::with_config(storage, ledger_io, gc_config, meta_map);
    let interval = tokio::time::Duration::from_millis(gc_wait_ms);
    // Force-trigger checks at a faster cadence (1s) so HTTP-triggered GC is responsive.
    let force_check_interval = tokio::time::Duration::from_secs(1);

    tracing::info!("GC thread started (interval: {}ms, minor: {}s, major: {}s)",
        gc_wait_ms, minor_interval_secs, major_interval_secs);

    let mut last_gc_run = tokio::time::Instant::now();
    // Track minor and major compaction timers separately.
    // Matches Java's GarbageCollectorThread which schedules minor and major
    // compaction on independent timers (minorCompactionInterval, majorCompactionInterval).
    let mut last_minor = tokio::time::Instant::now();
    let mut last_major = tokio::time::Instant::now();

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
                let forced_major = force_major.swap(false, Ordering::Relaxed);
                let forced_minor = force_minor.swap(false, Ordering::Relaxed);
                let forced = forced_major || forced_minor;
                let should_run = forced || last_gc_run.elapsed() >= interval;
                if !should_run {
                    continue;
                }

                last_gc_run = tokio::time::Instant::now();

                // Determine if minor and/or major compaction should run this cycle.
                // Matches Java's separate scheduling of minor and major compaction.
                let run_minor = forced_minor
                    || (minor_interval_secs > 0
                        && last_minor.elapsed() >= tokio::time::Duration::from_secs(minor_interval_secs as u64)
                        && !suspend_minor.load(Ordering::Relaxed));
                let run_major = forced_major
                    || (major_interval_secs > 0
                        && last_major.elapsed() >= tokio::time::Duration::from_secs(major_interval_secs as u64)
                        && !suspend_major.load(Ordering::Relaxed));

                match ledger_manager.get_ledger_ranges().await {
                    Ok(metadata_ledgers) => {
                        let active: HashSet<i64> = metadata_ledgers.into_iter().collect();
                        // Run with the major flag if either a major cycle is due
                        // or both are due (major subsumes minor since its threshold
                        // is higher). If only minor is due, run minor.
                        // Ledger deletion (step 1-2 of gc()) runs regardless.
                        let is_major = run_major;
                        if run_minor || run_major {
                            if let Err(e) = gc.gc(&active, is_major).await {
                                tracing::warn!("GC cycle error: {e}");
                            }
                            if run_minor { last_minor = tokio::time::Instant::now(); }
                            if run_major { last_major = tokio::time::Instant::now(); }
                        } else {
                            // No compaction due, but still run ledger deletion
                            if let Err(e) = gc.gc(&active, false).await {
                                tracing::warn!("GC cycle error: {e}");
                            }
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
    ledger_manager: Arc<dyn bookkeeper_metadata::traits::LedgerManager>,
    bookie_id: &str,
    lost_bookie_recovery_delay_secs: u64,
    rw_rereplicate_backoff_ms: u64,
    auditor_periodic_check_interval: u64,
    auditor_periodic_placement_policy_check_interval: u64,
    auditor_periodic_replicas_check_interval: u64,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>), Box<dyn std::error::Error>> {
    use bookkeeper_replication::auditor_election::AuditorElector;
    use bookkeeper_replication::underreplication::ZkUnderreplicationManager;
    use bookkeeper_replication::worker::ReplicationWorker;

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
        // Build a default placement policy instance so the auditor's
        // periodic placement check is actually applied. Previously this
        // was None and `check_placement_policy()` was a silent no-op.
        // Rack-aware and region-aware policies can be swapped in by
        // operators — this default matches Java's
        // `DefaultEnsemblePlacementPolicy` which is the out-of-the-box
        // choice when `ensemblePlacementPolicy` isn't explicitly set.
        let policy: Arc<dyn bookkeeper_client::placement::EnsemblePlacementPolicy> =
            Arc::new(bookkeeper_client::placement::DefaultPlacementPolicy::new());

        if let Err(e) = elector
            .run(
                elector_ur,
                elector_lm,
                auditor_zk,
                auditor_paths,
                lost_bookie_recovery_delay_secs,
                auditor_periodic_check_interval,
                auditor_periodic_placement_policy_check_interval,
                auditor_periodic_replicas_check_interval,
                Some(policy),
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
            rw_rereplicate_backoff_ms,
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
    tcp_keepalive: bool,
    tcp_no_delay: bool,
    max_connections: usize,
    active_connections: Arc<std::sync::atomic::AtomicUsize>,
) {

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _peer_addr)) => {
                        // Connection limit check
                        if max_connections > 0 {
                            let current = active_connections.load(std::sync::atomic::Ordering::Relaxed);
                            if current >= max_connections {
                                tracing::warn!("Connection limit reached ({max_connections}), rejecting");
                                drop(stream);
                                continue;
                            }
                        }
                        active_connections.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let conn_count = active_connections.clone();

                        let _ = stream.set_nodelay(tcp_no_delay);
                        if tcp_keepalive {
                            let sock = socket2::SockRef::from(&stream);
                            let _ = sock.set_tcp_keepalive(&socket2::TcpKeepalive::new());
                        }
                        let proc = processor.clone();
                        // RAII guard so the connection counter decrements
                        // on every exit path: success, error, panic, or
                        // abort. Pre-fix, a panic in the connection task
                        // left the counter stuck and a long-lived bookie
                        // would eventually wedge at max_connections.
                        struct ConnCounterGuard(Arc<std::sync::atomic::AtomicUsize>);
                        impl Drop for ConnCounterGuard {
                            fn drop(&mut self) {
                                self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                        let guard = ConnCounterGuard(conn_count);
                        if let Some(ref tls) = tls_acceptor {
                            let tls = tls.clone();
                            tokio::spawn(async move {
                                let _guard = guard;
                                let acceptor = tls.acceptor().await;
                                handle_starttls_connection(stream, proc, max_frame_size, acceptor).await;
                            });
                        } else {
                            tokio::spawn(async move {
                                let _guard = guard;
                                handle_connection(stream, proc, max_frame_size).await;
                            });
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
                        // Not a StartTLS request — TLS is configured but client didn't
                        // initiate StartTLS. Reject the connection. Matches Java's
                        // behavior where TLS-configured bookies require StartTLS.
                        tracing::warn!("TLS configured but client sent non-StartTLS request (op={op}), closing connection");
                        return;
                    }
                } else {
                    // V2 binary request on a TLS-configured bookie — reject.
                    tracing::warn!("TLS configured but client sent plaintext V2 request, closing connection");
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

/// End-to-end "request" classification used by the `*_REQUEST` histograms.
/// Matches Java's `RequestStats` per-op families; timing runs from frame
/// decode (`enqueued_at`) until the response bytes are flushed on the socket.
#[derive(Clone, Copy)]
pub(crate) enum RequestKind {
    Add,
    Read,
    ReadFence,
    WriteLac,
    ReadLac,
    GetBookieInfo,
    ForceLedger,
    Other,
}

/// A response queued for the writer task, carrying the metadata needed to
/// attribute end-to-end request latency when the socket write completes.
pub(crate) struct EnqueuedResponse {
    pub bytes: Bytes,
    /// Java's `enqueueNanos` — captured when the frame was decoded, used to
    /// observe end-to-end `*_REQUEST` latency once the response is flushed.
    pub enqueued_at: std::time::Instant,
    /// Captured the moment the response was sent into the mpsc channel.
    /// Used to observe `RESPONSE_QUEUE_WAIT` = writer-dequeue - queued_at.
    pub queued_at: std::time::Instant,
    pub kind: RequestKind,
    pub success: bool,
}

/// Observe `*_REQUEST` latency (Java `ADD_ENTRY_REQUEST` etc.) once a
/// response has been flushed on the wire.
pub(crate) fn observe_request_latency(kind: RequestKind, success: bool, elapsed: f64) {
    use crate::metrics::*;
    match kind {
        RequestKind::Add => {
            if success { ADD_ENTRY_REQUEST_OK.observe(elapsed); }
            else       { ADD_ENTRY_REQUEST_FAIL.observe(elapsed); }
        }
        RequestKind::Read => {
            if success { READ_ENTRY_REQUEST_OK.observe(elapsed); }
            else       { READ_ENTRY_REQUEST_FAIL.observe(elapsed); }
        }
        RequestKind::ReadFence => {
            if success {
                READ_ENTRY_REQUEST_OK.observe(elapsed);
                READ_ENTRY_FENCE_REQUEST_OK.observe(elapsed);
            } else {
                READ_ENTRY_REQUEST_FAIL.observe(elapsed);
                READ_ENTRY_FENCE_REQUEST_FAIL.observe(elapsed);
            }
        }
        RequestKind::WriteLac => {
            if success { WRITE_LAC_REQUEST_OK.observe(elapsed); }
            else       { WRITE_LAC_REQUEST_FAIL.observe(elapsed); }
        }
        RequestKind::ReadLac => {
            if success { READ_LAC_REQUEST_OK.observe(elapsed); }
            else       { READ_LAC_REQUEST_FAIL.observe(elapsed); }
        }
        RequestKind::GetBookieInfo => {
            if success { GET_BOOKIE_INFO_REQUEST_OK.observe(elapsed); }
            else       { GET_BOOKIE_INFO_REQUEST_FAIL.observe(elapsed); }
        }
        RequestKind::ForceLedger => {
            if success { FORCE_LEDGER_REQUEST_OK.observe(elapsed); }
            else       { FORCE_LEDGER_REQUEST_FAIL.observe(elapsed); }
        }
        RequestKind::Other => {}
    }
}

/// Map a V2 request to its `RequestKind` + a stable flag indicating whether
/// the fence flag is set (so a later response we can mark `ReadFence`).
pub(crate) fn v2_request_kind(req: &V2Request) -> RequestKind {
    match req {
        V2Request::AddEntry { .. } => RequestKind::Add,
        V2Request::ReadEntry { flags, .. } | V2Request::BatchReadEntry { flags, .. } => {
            if flags & FLAG_DO_FENCING != 0 { RequestKind::ReadFence } else { RequestKind::Read }
        }
        V2Request::WriteLac { .. } => RequestKind::WriteLac,
        V2Request::ReadLac { .. } => RequestKind::ReadLac,
        V2Request::GetBookieInfo { .. } => RequestKind::GetBookieInfo,
        V2Request::Auth { .. } => RequestKind::Other,
    }
}

/// Extract the EOK/non-EOK status out of a V2 response for request-success
/// attribution.
pub(crate) fn v2_response_success(resp: &V2Response) -> bool {
    match resp {
        V2Response::Add { error_code, .. }
        | V2Response::Read { error_code, .. }
        | V2Response::WriteLac { error_code, .. }
        | V2Response::ReadLac { error_code, .. }
        | V2Response::GetBookieInfo { error_code, .. }
        | V2Response::BatchRead { error_code, .. } => *error_code == V2_EOK,
        V2Response::Auth { .. } => true,
    }
}

/// V3 operation-type → `RequestKind` (without fence detail, which lives
/// inside the read request body and is handled uniformly in the processor).
pub(crate) fn v3_request_kind(operation: i32) -> RequestKind {
    match operation {
        x if x == bookkeeper_proto::OperationType::AddEntry as i32 => RequestKind::Add,
        x if x == bookkeeper_proto::OperationType::ReadEntry as i32 => RequestKind::Read,
        x if x == bookkeeper_proto::OperationType::WriteLac as i32 => RequestKind::WriteLac,
        x if x == bookkeeper_proto::OperationType::ReadLac as i32 => RequestKind::ReadLac,
        x if x == bookkeeper_proto::OperationType::GetBookieInfo as i32 => RequestKind::GetBookieInfo,
        x if x == bookkeeper_proto::OperationType::ForceLedger as i32 => RequestKind::ForceLedger,
        _ => RequestKind::Other,
    }
}

/// Pipelined connection handler: reads frames concurrently with request
/// processing. Each request is dispatched as a separate tokio task so
/// journal writes from multiple in-flight requests can batch naturally.
///
/// Architecture:
///   Reader loop → decode frame → spawn processing task
///   Processing task → process_request().await → send response to writer channel
///   Writer task → recv responses → write to socket
///
/// This matches Java BookKeeper's pipelined request processing where
/// multiple add_entry requests can be in-flight simultaneously, allowing
/// the journal's group commit to batch their writes.
async fn handle_connection_io<R, W>(
    mut reader: R,
    mut writer: W,
    processor: Arc<RequestProcessor>,
    max_frame_size: usize,
)
where
    R: tokio::io::AsyncReadExt + Unpin,
    W: tokio::io::AsyncWriteExt + Unpin + Send + 'static,
{
    // Unbounded channel: backpressure is already enforced by the TCP socket's
    // write buffer. The bounded variant was ~6% of hot-path CPU because every
    // send/recv acquired a batch_semaphore permit — unbounded is a simple
    // atomic linked-list push/pop.
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel::<EnqueuedResponse>();

    // Writer task: sends framed responses to the socket.
    // Matches Java's closeChannelOnResponseTimeout: if a write takes longer
    // than the timeout, close the connection to free resources.
    let write_timeout = std::time::Duration::from_secs(30);
    let writer_handle = tokio::spawn(async move {
        let mut codec = BookieCodec::new(max_frame_size);
        let mut out = BytesMut::with_capacity(256);
        while let Some(enq) = resp_rx.recv().await {
            // Observe mpsc queue wait: how long the response sat between
            // the processor enqueue and the writer dequeue. If this shows
            // the tail, the response pipeline is the bottleneck.
            crate::metrics::RESPONSE_QUEUE_WAIT.observe(enq.queued_at.elapsed().as_secs_f64());
            out.clear();
            if codec.encode(enq.bytes, &mut out).is_err() {
                break;
            }
            let write_start = std::time::Instant::now();
            let write_result = tokio::time::timeout(
                write_timeout,
                writer.write_all(&out),
            ).await;
            let write_elapsed = write_start.elapsed().as_secs_f64();
            // End-to-end "request" latency = time from frame decode to response flushed.
            // Matches Java's RequestStats `*_REQUEST` family (see PacketProcessorBaseV3).
            let request_elapsed = enq.enqueued_at.elapsed().as_secs_f64();
            match write_result {
                Ok(Ok(())) => {
                    crate::metrics::CHANNEL_WRITE_OK.observe(write_elapsed);
                    observe_request_latency(enq.kind, enq.success, request_elapsed);
                }
                Ok(Err(_)) => {
                    crate::metrics::CHANNEL_WRITE_FAIL.observe(write_elapsed);
                    observe_request_latency(enq.kind, false, request_elapsed);
                    break;
                }
                Err(_) => {
                    // Write timeout — slow client. Close connection.
                    tracing::warn!("Closing slow client: write timed out after {:.1}s", write_elapsed);
                    crate::metrics::CHANNEL_WRITE_FAIL.observe(write_elapsed);
                    observe_request_latency(enq.kind, false, request_elapsed);
                    break;
                }
            }
        }
    });

    // Reader loop: decode frames, dispatch for concurrent processing.
    //
    // In-flight per-request processing lives in a `FuturesUnordered` that
    // is polled cooperatively with the socket read on this same task.
    // Each decoded frame used to be handed to `tokio::spawn`, which costs
    // a task-struct allocation + Arc clones (of the processor and response
    // channel) on every request. For a Pulsar-style broker that pins one
    // connection per bookie and multiplexes thousands of add_entry rps
    // over it, those allocations dominate the profile.
    //
    // The futures borrow `processor` and `resp_tx` directly from this
    // function's scope — no per-frame Arc clone — and complete in place
    // without spawning. Response ordering onto the socket is still owned
    // by `writer_handle` via `resp_tx` (unchanged), so client-observable
    // pipelining semantics match the previous behavior.
    use futures::future::BoxFuture;
    use futures::stream::{FuturesUnordered, StreamExt};

    let mut codec = BookieCodec::new(max_frame_size);
    let mut buf = BytesMut::with_capacity(4096);
    let mut is_v3: Option<bool> = None;
    // Per-connection auth gate. When auth is disabled (default), all
    // connections start authenticated. Otherwise, non-AUTH requests
    // are rejected with EUA until the handshake completes.
    let mut authenticated = !processor.require_auth();

    let processor_ref: &RequestProcessor = &processor;
    let resp_tx_ref = &resp_tx;
    let mut in_flight: FuturesUnordered<BoxFuture<'_, ()>> = FuturesUnordered::new();

    'outer: loop {
        // Decode as many frames as are already buffered, pushing processing
        // futures into `in_flight` without yielding. Only yield to the
        // runtime when we need more bytes or when we want to let in-flight
        // futures make progress.
        'decode: loop {
            match codec.decode(&mut buf) {
                Ok(Some(frame)) => {
                    let frame_bytes = frame.freeze();
                    // Java's `enqueueNanos` — start of the end-to-end `*_REQUEST` timer.
                    let enqueued_at = std::time::Instant::now();

                    let decoded_v3 = match is_v3 {
                        Some(true) => crate::v3_processor::try_decode_v3(&frame_bytes),
                        Some(false) => None,
                        None => crate::v3_processor::try_decode_v3(&frame_bytes),
                    };
                    let use_v3 = decoded_v3.is_some() || is_v3 == Some(true);

                    if use_v3 {
                        is_v3 = Some(true);
                        let Some(v3_req) = decoded_v3 else { break 'outer };
                        let is_auth_op = v3_req.header.operation
                            == bookkeeper_proto::OperationType::Auth as i32;
                        if !authenticated && !is_auth_op {
                            // Fast synchronous reject path — no processing needed.
                            let txn_id = v3_req.header.txn_id as u64;
                            let operation = v3_req.header.operation;
                            let kind = v3_request_kind(operation);
                            let resp = crate::v3_processor::build_v3_error_response(
                                txn_id,
                                operation,
                                bookkeeper_common::error::BKError::Unauthorized,
                            );
                            let _ = resp_tx_ref.send(EnqueuedResponse {
                                bytes: Bytes::from(resp),
                                enqueued_at,
                                queued_at: std::time::Instant::now(),
                                kind,
                                success: false,
                            });
                            continue 'decode;
                        }
                        if is_auth_op {
                            authenticated = true;
                        }
                        let kind = v3_request_kind(v3_req.header.operation);
                        in_flight.push(Box::pin(async move {
                            let (resp_bytes, success) =
                                crate::v3_processor::process_v3_request(processor_ref, v3_req)
                                    .await;
                            let _ = resp_tx_ref.send(EnqueuedResponse {
                                bytes: Bytes::from(resp_bytes),
                                enqueued_at,
                                queued_at: std::time::Instant::now(),
                                kind,
                                success,
                            });
                        }));
                    } else {
                        is_v3 = Some(false);
                        let req = match decode_request(frame_bytes) {
                            Ok(r) => r,
                            Err(_) => break 'outer,
                        };
                        let is_auth_req = matches!(&req, V2Request::Auth { .. });
                        if !authenticated && !is_auth_req {
                            in_flight.push(Box::pin(async move {
                                let resp = processor_ref
                                    .process_request(V2Request::Auth {
                                        payload: bytes::Bytes::new(),
                                    })
                                    .await;
                                let _ = resp_tx_ref.send(EnqueuedResponse {
                                    bytes: encode_response(&resp).freeze(),
                                    enqueued_at,
                                    queued_at: std::time::Instant::now(),
                                    kind: RequestKind::Other,
                                    success: false,
                                });
                            }));
                            continue 'decode;
                        }
                        if is_auth_req {
                            authenticated = true;
                        }
                        let kind = v2_request_kind(&req);
                        in_flight.push(Box::pin(async move {
                            let resp = processor_ref.process_request(req).await;
                            let success = v2_response_success(&resp);
                            let resp_encoded = encode_response(&resp);
                            let _ = resp_tx_ref.send(EnqueuedResponse {
                                bytes: resp_encoded.freeze(),
                                enqueued_at,
                                queued_at: std::time::Instant::now(),
                                kind,
                                success,
                            });
                        }));
                    }
                }
                Ok(None) => break 'decode,
                Err(_) => break 'outer,
            }
        }

        // Need more bytes from the socket, and meanwhile in_flight futures
        // are making progress. `select!` over (read, next) lets the runtime
        // advance whichever is ready first without spawning.
        tokio::select! {
            read_result = reader.read_buf(&mut buf) => {
                match read_result {
                    Ok(0) => break,
                    Ok(_) => {},
                    Err(_) => break,
                }
            }
            Some(()) = in_flight.next(), if !in_flight.is_empty() => {
                // Completed future already enqueued its response; just
                // loop back to the decoder in case more frames accumulated.
            }
        }
    }

    // Let any still-in-flight processing finish so the writer task can
    // flush pending responses before we drop the channel. Matches the
    // previous `tokio::spawn`'d behavior where detached tasks ran to
    // completion even after the reader exited.
    while (in_flight.next().await).is_some() {}
    // Explicit drop releases the `&resp_tx` borrow held by the (now-empty)
    // futures collection so we can move `resp_tx` below.
    drop(in_flight);

    // Drop the sender so the writer task exits after draining.
    drop(resp_tx);
    let _ = writer_handle.await;
}

/// Build the shared `DataIntegrityCheck` instance if either the cookie
/// validator or the full-check service is going to need it. Returns
/// `None` when both flags are off or ZK is unconfigured.
///
/// Pulling construction up here means the pre-boot check runs against
/// the same storage + metadata + client objects the service will later
/// use, matching Java's lifecycle.
async fn build_data_integrity_check(
    config: &ServerConfig,
    bookie_id: &str,
    storage: Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage>,
) -> Result<Option<Arc<crate::datainteg::check::DataIntegrityCheck>>, Box<dyn std::error::Error>>
{
    if !config.data_integrity_checking && !config.data_integrity_stamp_missing_cookies {
        return Ok(None);
    }
    let Some(zk_servers) = config.zk_servers.as_deref() else {
        // Stamping a missing cookie without an integrity-check pre-boot
        // can silently legitimise stale local data. Treat the
        // misconfiguration as fatal rather than a warning. (When only
        // `data_integrity_checking` is on without ZK we still warn-and-skip
        // — that path doesn't risk legitimising stale data.)
        if config.data_integrity_stamp_missing_cookies {
            return Err(
                "data_integrity_stamp_missing_cookies requires zk_servers — stamping \
                 without the data-integrity pre-boot pass would silently legitimise \
                 stale local data".into(),
            );
        }
        tracing::warn!("data-integrity enabled but no ZK servers configured — skipping");
        return Ok(None);
    };

    use crate::datainteg::check::DataIntegrityCheck;
    use crate::datainteg::entry_copier::EntryCopier;
    use crate::datainteg::recovery::ZkRecoveryClient;
    use bookkeeper_client::zk_client::ZkBookKeeper;

    let uri = bookkeeper_metadata::metadata_paths::ParsedMetadataUri::Zk {
        servers: zk_servers.to_string(),
        ledgers_root: config.zk_ledger_root.clone(),
    };
    let backend = bookkeeper_metadata::backend::open_metadata_backend(&uri, None, None)
        .await
        .map_err(|e| format!("data-integ: metadata connect: {e}"))?;
    let ledger_manager = backend.ledger_manager;

    let bk_client = ZkBookKeeper::new(zk_servers, &config.zk_ledger_root)
        .await
        .map_err(|e| format!("data-integ: bookkeeper client: {e}"))?;
    let recovery = Arc::new(ZkRecoveryClient::new(Arc::new(tokio::sync::Mutex::new(bk_client))));

    let entry_copier = Arc::new(EntryCopier::new(bookie_id.to_string(), storage.clone()));
    Ok(Some(Arc::new(DataIntegrityCheck::new(
        bookie_id.to_string(),
        ledger_manager,
        storage,
        entry_copier,
        recovery,
    ))))
}

/// Spawn the data-integrity service using a pre-built check. Callers
/// should skip the spawn when `data_integrity_checking` is off even
/// if a check was built for the cookie validator's pre-boot hook.
fn spawn_data_integrity_service(
    check: Arc<crate::datainteg::check::DataIntegrityCheck>,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    use crate::datainteg::service;
    tracing::info!("data-integrity: service starting");
    service::spawn(check, service::DEFAULT_INTERVAL, shutdown_rx)
}

/// Run the cookie validation. When `data_integrity_stamp_missing_cookies`
/// is enabled, uses the DataIntegrityCookieValidator path (stamp-on-mismatch).
/// Otherwise falls through to the existing strict `validate_cookie` flow.
async fn run_cookie_validation(
    reg: &dyn bookkeeper_metadata::traits::RegistrationManager,
    bookie_id: &str,
    config: &ServerConfig,
    check: Option<Arc<crate::datainteg::check::DataIntegrityCheck>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if !config.data_integrity_stamp_missing_cookies {
        return validate_cookie(reg, bookie_id, config).await;
    }
    use crate::datainteg::cookie_validator::{CookieLayout, DataIntegrityCookieValidator};
    // Build a fresh RegistrationManager for the validator via the factory so we
    // don't need to clone/share the bookie's primary handle. An extra connection
    // is minor.
    let uri = resolve_metadata_uri(config)
        .map_err(|e| format!("data-integ cookie validation: {e}"))?;
    let secondary = bookkeeper_metadata::backend::open_metadata_backend(&uri, None, None)
        .await
        .map_err(|e| format!("data-integ: metadata connect: {e}"))?;
    let layout = CookieLayout {
        bookie_id: bookie_id.to_string(),
        journal_dir: std::path::PathBuf::from(&config.journal_directory),
        ledger_dirs: config
            .ledger_directories
            .iter()
            .map(std::path::PathBuf::from)
            .collect(),
        index_dirs: config
            .index_directories
            .as_ref()
            .map(|v| v.iter().map(std::path::PathBuf::from).collect())
            .unwrap_or_default(),
    };
    // When `check` is Some, mismatched cookies trigger a pre-boot pass
    // (limbo+fence the local open-ensemble ledgers) before stamping, so
    // we don't serve stale data after the new cookie takes effect.
    // Matches Java's `DataIntegrityCookieValidation.checkCookies`.
    let validator = DataIntegrityCookieValidator::new(
        layout,
        secondary.registration_manager,
        check,
        config.data_integrity_stamp_missing_cookies,
    );
    validator
        .check_and_stamp()
        .await
        .map_err(|e| format!("data-integ cookie validation: {e}").into())
}
