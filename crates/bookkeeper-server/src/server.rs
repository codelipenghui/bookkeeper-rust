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
use crate::processor::RequestProcessor;
use crate::state::{BookieState, BookieStateManager};

pub struct BookieServer {
    local_addr: SocketAddr,
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
            shutdown_tx,
            accept_handle,
            storage: storage_for_shutdown,
            zk_reg,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
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

    loop {
        // Try to decode a complete frame from the buffer first
        match codec.decode(&mut buf) {
            Ok(Some(frame)) => {
                // Decode the request from the frame
                match decode_request(frame.freeze()) {
                    Ok(req) => {
                        let proc = processor.clone();
                        let resp = match tokio::task::spawn_blocking(move || proc.process_request(req)).await {
                            Ok(resp) => resp,
                            Err(_) => break, // task panicked or cancelled
                        };
                        let resp_bytes = encode_response(&resp);

                        // Encode the response with length-prefix framing
                        out.clear();
                        if codec
                            .encode(Bytes::from(resp_bytes.freeze()), &mut out)
                            .is_ok()
                        {
                            if writer.write_all(&out).await.is_err() {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    Err(_) => {
                        // Unknown or malformed request — close connection
                        break;
                    }
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
