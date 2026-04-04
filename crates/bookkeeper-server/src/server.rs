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

use crate::config::BookieConfig;
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
}

impl BookieServer {
    pub async fn start(config: BookieConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Bind TCP listener
        let addr = format!("0.0.0.0:{}", config.port);
        let listener = TcpListener::bind(&addr).await?;
        let local_addr = listener.local_addr()?;

        // Open storage using first ledger dir
        let ledger_dir = config
            .ledger_dirs
            .first()
            .ok_or("no ledger dirs configured")?;
        let storage = DbLedgerStorage::open(Path::new(ledger_dir))?;
        let storage: Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage> =
            Arc::new(storage);

        // Create state manager and transition to Running
        let state = Arc::new(BookieStateManager::new());
        state.transition_to(BookieState::Running);

        // Create request processor
        let storage_for_shutdown = storage.clone();
        let processor = Arc::new(RequestProcessor::new(storage, state.clone()));

        // Shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let max_frame_size = config.max_frame_size;

        // Optionally register with ZooKeeper
        let zk_reg = if let Some(ref zk_connect) = config.zk_connect {
            let bookie_id = config.bookie_id.clone().unwrap_or_else(|| {
                format!("{}:{}", local_addr.ip(), local_addr.port())
            });
            let reg_mgr = bookkeeper_metadata::zk::registration::ZkRegistrationManager::new(
                zk_connect,
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
            Some((reg_mgr, bookie_id))
        } else {
            None
        };


        // Optionally start HTTP admin API server
        let http_addr = if let Some(http_port) = config.http_port {
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
        // Signal the accept loop to stop
        let _ = self.shutdown_tx.send(true);
        // Wait for the accept loop to finish
        let _ = self.accept_handle.await;
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
    config: &BookieConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_metadata::cookie::Cookie;

    let journal_dir = std::path::Path::new(&config.journal_dir);
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
            let ledger_dirs: Vec<&str> = config.ledger_dirs.iter().map(|s| s.as_str()).collect();
            let cookie = Cookie::new(
                bookie_id,
                &config.journal_dir,
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
