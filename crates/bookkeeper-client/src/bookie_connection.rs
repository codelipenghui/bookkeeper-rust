use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_rustls::TlsConnector;
use tokio_util::codec::{Decoder, Encoder};

use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::framing::BookieCodec;
use bookkeeper_protocol::v2::request::{V2Request, encode_request};
use bookkeeper_protocol::v2::response::{V2Response, decode_response};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("connection closed")]
    ConnectionClosed,
    /// Write quorum could not be reached; carries which bookie indices in the
    /// write set failed so the caller can trigger ensemble change.
    #[error("write quorum failed for entry {entry_id}: {message}")]
    WriteQuorumFailed {
        message: String,
        /// Indices into the ensemble (write-set positions) that failed.
        failed_indices: Vec<usize>,
        entry_id: i64,
    },
}

struct PendingRequest {
    data: Bytes,
    response_tx: oneshot::Sender<Result<V2Response, ClientError>>,
}

/// Minimum interval between reconnection attempts to the same bookie.
/// Prevents flooding connect calls when a bookie is down.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(1);

/// A single pipelined TCP channel to a bookie with auto-reconnection.
/// Matches Java's PerChannelBookieClient: connects lazily on first use,
/// reconnects transparently when a connection drops, with backoff to
/// avoid flooding connect attempts to an unavailable bookie.
struct BookieChannel {
    addr: SocketAddr,
    /// Original hostname (when dialed via "host:port"), used as SNI server name
    /// for TLS handshakes. None when dialed via IP literal.
    server_hostname: Option<String>,
    tls: Option<Arc<TlsConnector>>,
    state: Mutex<ChannelState>,
}

struct ChannelState {
    sender: Option<mpsc::Sender<PendingRequest>>,
    /// Last time a reconnection was attempted (for backoff).
    last_connect_attempt: Option<tokio::time::Instant>,
}

impl BookieChannel {
    fn new(addr: SocketAddr, server_hostname: Option<String>, tls: Option<Arc<TlsConnector>>) -> Self {
        Self {
            addr,
            server_hostname,
            tls,
            state: Mutex::new(ChannelState {
                sender: None,
                last_connect_attempt: None,
            }),
        }
    }

    /// Connect eagerly (used during pool initialization when possible).
    async fn try_connect(&self) {
        if let Ok(tx) = Self::open_connection(
            self.addr, self.server_hostname.as_deref(), self.tls.clone()
        ).await {
            let mut state = self.state.lock().await;
            state.sender = Some(tx);
            state.last_connect_attempt = Some(tokio::time::Instant::now());
        }
    }

    /// Open a new TCP connection (and optionally upgrade to TLS via StartTLS),
    /// then spawn the background task.
    async fn open_connection(
        addr: SocketAddr,
        server_hostname: Option<&str>,
        tls: Option<Arc<TlsConnector>>,
    ) -> Result<mpsc::Sender<PendingRequest>, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        let (request_tx, request_rx) = mpsc::channel::<PendingRequest>(1024);

        match tls {
            None => {
                let (reader, writer) = stream.into_split();
                tokio::spawn(connection_task(reader, writer, request_rx));
            }
            Some(connector) => {
                // Perform StartTLS handshake first
                let tls_stream = perform_starttls(stream, addr, server_hostname, &connector).await?;
                let (reader, writer) = tokio::io::split(tls_stream);
                tokio::spawn(connection_task(reader, writer, request_rx));
            }
        }
        Ok(request_tx)
    }

    /// Get or establish a connection, then send the request.
    /// Reconnects transparently if the previous connection is dead.
    /// Enforces a minimum backoff between reconnection attempts.
    async fn send_request(&self, req: V2Request) -> Result<V2Response, ClientError> {
        let req_bytes = encode_request(&req);
        let mut framed = BytesMut::new();
        BookieCodec::new(5 * 1024 * 1024)
            .encode(req_bytes.freeze(), &mut framed)
            .map_err(|e| ClientError::Protocol(format!("encode: {e}")))?;
        let data = framed.freeze();

        let (response_tx, response_rx) = oneshot::channel();

        {
            let mut state = self.state.lock().await;
            let needs_reconnect = match &state.sender {
                None => true,
                Some(tx) => tx.is_closed(),
            };
            if needs_reconnect {
                // Enforce backoff: don't reconnect if we tried too recently
                let now = tokio::time::Instant::now();
                if let Some(last) = state.last_connect_attempt {
                    if now.duration_since(last) < RECONNECT_BACKOFF {
                        state.sender = None;
                        return Err(ClientError::ConnectionClosed);
                    }
                }
                state.last_connect_attempt = Some(now);
                match Self::open_connection(
                    self.addr, self.server_hostname.as_deref(), self.tls.clone()
                ).await {
                    Ok(tx) => state.sender = Some(tx),
                    Err(e) => {
                        state.sender = None;
                        return Err(e);
                    }
                }
            }
            let tx = state.sender.as_ref().unwrap();
            tx.send(PendingRequest { data, response_tx })
                .await
                .map_err(|_| ClientError::ConnectionClosed)?;
        }

        response_rx.await.map_err(|_| ClientError::ConnectionClosed)?
    }
}

/// Pooled connection to a single bookie with N pipelined TCP channels.
/// Matches Java's DefaultPerChannelBookieClientPool:
/// - N channels per bookie (configurable via `numChannelsPerBookie`)
/// - Ledger-ID-based channel selection (`key % N`) for per-ledger ordering
/// - Lazy connection with transparent reconnection on failure
///
/// All methods take `&self` — thread-safe.
pub struct BookieConnection {
    channels: Vec<BookieChannel>,
}

impl BookieConnection {
    /// Connect to a bookie with a single plaintext channel (default).
    pub async fn connect(addr: SocketAddr) -> Result<Self, ClientError> {
        Self::connect_pooled_with_tls(addr, 1, None).await
    }

    /// Connect to a bookie with N pooled plaintext channels.
    pub async fn connect_pooled(addr: SocketAddr, num_channels: usize) -> Result<Self, ClientError> {
        Self::connect_pooled_with_tls(addr, num_channels, None).await
    }

    /// Connect to a bookie with N pooled channels and optional TLS.
    /// When `tls` is `Some`, each channel performs a StartTLS upgrade after
    /// the plaintext TCP connection is established.
    /// Channels connect eagerly but tolerate partial failure — at least one
    /// channel must connect. Failed channels will reconnect lazily on use.
    pub async fn connect_pooled_with_tls(
        addr: SocketAddr,
        num_channels: usize,
        tls: Option<Arc<TlsConnector>>,
    ) -> Result<Self, ClientError> {
        Self::connect_pooled_with_tls_named(addr, None, num_channels, tls).await
    }

    /// Like `connect_pooled_with_tls` but accepts an optional hostname for SNI.
    /// Use this when the bookie was dialed via a hostname (e.g., "host.example.com:3181")
    /// — the hostname will be used as the SNI server name during the TLS handshake,
    /// matching Java's behavior and RFC 6066 (which forbids IP literals in SNI).
    pub async fn connect_pooled_with_tls_named(
        addr: SocketAddr,
        server_hostname: Option<String>,
        num_channels: usize,
        tls: Option<Arc<TlsConnector>>,
    ) -> Result<Self, ClientError> {
        let num_channels = num_channels.max(1);
        let mut channels = Vec::with_capacity(num_channels);
        let mut any_connected = false;

        for _ in 0..num_channels {
            let ch = BookieChannel::new(addr, server_hostname.clone(), tls.clone());
            ch.try_connect().await;
            if ch.state.lock().await.sender.is_some() {
                any_connected = true;
            }
            channels.push(ch);
        }

        if !any_connected {
            return Err(ClientError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("failed to connect any channel to {addr}"),
            )));
        }

        Ok(Self { channels })
    }

    /// Number of pooled channels.
    pub fn num_channels(&self) -> usize {
        self.channels.len()
    }

    /// Select channel by ledger ID (matching Java's signSafeMod).
    /// All operations for the same ledger go to the same channel,
    /// preserving per-ledger ordering within the pool.
    fn pick_channel(&self, key: i64) -> &BookieChannel {
        let idx = sign_safe_mod(key, self.channels.len() as i64) as usize;
        &self.channels[idx]
    }

    pub async fn send_add_entry(
        &self,
        flags: u16,
        master_key: [u8; MASTER_KEY_LENGTH],
        entry_data: Bytes,
    ) -> Result<V2Response, ClientError> {
        // Extract ledger_id from entry data header for channel selection
        let ledger_id = if entry_data.len() >= 8 {
            i64::from_be_bytes(entry_data[0..8].try_into().unwrap())
        } else {
            0
        };
        let req = V2Request::AddEntry { flags, master_key, entry_data };
        self.pick_channel(ledger_id).send_request(req).await
    }

    pub async fn send_read_entry(
        &self,
        flags: u16,
        ledger_id: i64,
        entry_id: i64,
        master_key: Option<[u8; MASTER_KEY_LENGTH]>,
    ) -> Result<V2Response, ClientError> {
        let req = V2Request::ReadEntry { flags, ledger_id, entry_id, master_key };
        self.pick_channel(ledger_id).send_request(req).await
    }

    /// Request bookie disk space info via V2 GetBookieInfo (opcode 6).
    /// `requested` bitmask: 1=free space, 2=total capacity, 3=both.
    pub async fn send_get_bookie_info(&self, requested: i64) -> Result<V2Response, ClientError> {
        let req = V2Request::GetBookieInfo { flags: 0, requested };
        self.pick_channel(0).send_request(req).await
    }
}

/// Matches Java's MathUtils.signSafeMod: always returns a non-negative result.
fn sign_safe_mod(dividend: i64, divisor: i64) -> i64 {
    let r = dividend % divisor;
    if r < 0 { r + divisor } else { r }
}

/// Perform the StartTLS protocol upgrade on a plaintext TCP connection.
/// Sends a V3 StartTLSRequest, awaits the StartTLSResponse, then upgrades to TLS.
/// Matches Java's PerChannelBookieClient.initiateTLS() flow.
///
/// `server_hostname` is used for SNI when present (matching Java which uses
/// `address.getHostName()`); when None, falls back to the IP literal.
async fn perform_starttls(
    mut stream: TcpStream,
    addr: SocketAddr,
    server_hostname: Option<&str>,
    connector: &TlsConnector,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, ClientError> {
    use bookkeeper_proto::{
        BkPacketHeader, OperationType, ProtocolVersion, Request, Response, StatusCode,
        StartTlsRequest,
    };
    use prost::Message;
    use rustls::pki_types::ServerName;

    // Build V3 StartTLSRequest
    let req = Request {
        header: BkPacketHeader {
            version: ProtocolVersion::VersionThree as i32,
            operation: OperationType::StartTls as i32,
            txn_id: 1,
            priority: None,
        },
        start_tls_request: Some(StartTlsRequest {}),
        ..Default::default()
    };
    let payload = req.encode_to_vec();
    let mut framed = BytesMut::new();
    BookieCodec::new(5 * 1024 * 1024)
        .encode(Bytes::from(payload), &mut framed)
        .map_err(|e| ClientError::Protocol(format!("encode StartTLSRequest: {e}")))?;

    // Send plaintext request
    stream.write_all(&framed).await
        .map_err(|e| ClientError::Io(e))?;

    // Read plaintext response
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let mut buf = BytesMut::with_capacity(256);
    let resp_frame = loop {
        if let Some(f) = codec.decode(&mut buf)
            .map_err(|e| ClientError::Protocol(format!("decode StartTLS: {e}")))?
        {
            break f;
        }
        let n = stream.read_buf(&mut buf).await
            .map_err(|e| ClientError::Io(e))?;
        if n == 0 {
            return Err(ClientError::ConnectionClosed);
        }
    };

    let resp = Response::decode(resp_frame.freeze())
        .map_err(|e| ClientError::Protocol(format!("decode StartTLS response: {e}")))?;
    // Note: Java's StartTLSCompletion checks both connection state AND status.
    // We only check status because perform_starttls runs synchronously inside
    // open_connection — there is no concurrent state machine that could change
    // underneath us. Any future refactor that introduces concurrent close/abort
    // during connection setup must reintroduce a state check here.
    if resp.status != StatusCode::Eok as i32 {
        return Err(ClientError::Protocol(format!(
            "StartTLS rejected: status={}", resp.status
        )));
    }

    // Upgrade to TLS. Prefer hostname for SNI (matching Java's
    // `newTLSHandler(host, port)` and RFC 6066 which forbids IP literals in SNI).
    // Fall back to IP literal only when no hostname is available.
    let server_name = match server_hostname {
        Some(host) => match ServerName::try_from(host.to_string()) {
            Ok(name) => name,
            Err(_) => match addr.ip() {
                std::net::IpAddr::V4(ip) => ServerName::IpAddress(ip.into()),
                std::net::IpAddr::V6(ip) => ServerName::IpAddress(ip.into()),
            },
        },
        None => match addr.ip() {
            std::net::IpAddr::V4(ip) => ServerName::IpAddress(ip.into()),
            std::net::IpAddr::V6(ip) => ServerName::IpAddress(ip.into()),
        },
    };
    let tls_stream = connector.connect(server_name, stream).await
        .map_err(|e| ClientError::Protocol(format!("TLS handshake: {e}")))?;
    Ok(tls_stream)
}

async fn connection_task<R, W>(
    mut reader: R,
    mut writer: W,
    mut request_rx: mpsc::Receiver<PendingRequest>,
)
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let mut pending: VecDeque<oneshot::Sender<Result<V2Response, ClientError>>> = VecDeque::new();
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut codec = BookieCodec::new(5 * 1024 * 1024);

    loop {
        // First, try to decode any buffered responses
        loop {
            match codec.decode(&mut read_buf) {
                Ok(Some(frame)) => {
                    if let Some(tx) = pending.pop_front() {
                        let resp = decode_response(frame.freeze())
                            .map_err(|e| ClientError::Protocol(format!("decode: {e}")));
                        let _ = tx.send(resp);
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    // Protocol error — drain all pending waiters and shut down
                    for tx in pending.drain(..) {
                        let _ = tx.send(Err(ClientError::Protocol(format!("frame: {e}"))));
                    }
                    return;
                }
            }
        }

        tokio::select! {
            // Accept new requests to send
            req = request_rx.recv() => {
                match req {
                    Some(first_req) => {
                        // Batch: gather first request + all immediately available
                        let mut batch = BytesMut::from(first_req.data.as_ref());
                        pending.push_back(first_req.response_tx);

                        // Drain without blocking
                        while let Ok(more) = request_rx.try_recv() {
                            batch.extend_from_slice(&more.data);
                            pending.push_back(more.response_tx);
                        }

                        if writer.write_all(&batch).await.is_err() {
                            // Fail all pending including the ones we just queued
                            for tx in pending.drain(..) {
                                let _ = tx.send(Err(ClientError::ConnectionClosed));
                            }
                            return;
                        }
                    }
                    None => {
                        // Channel closed — drain remaining pending responses
                        while !pending.is_empty() {
                            // Try to decode any already-buffered frames first
                            loop {
                                match codec.decode(&mut read_buf) {
                                    Ok(Some(frame)) => {
                                        if let Some(tx) = pending.pop_front() {
                                            let resp = decode_response(frame.freeze())
                                                .map_err(|e| ClientError::Protocol(format!("decode: {e}")));
                                            let _ = tx.send(resp);
                                        }
                                    }
                                    _ => break,
                                }
                            }
                            if pending.is_empty() { break; }
                            // Read more data from socket
                            match tokio::time::timeout(
                                std::time::Duration::from_secs(5),
                                reader.read_buf(&mut read_buf)
                            ).await {
                                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => {
                                    // EOF, error, or timeout — fail remaining
                                    for tx in pending.drain(..) {
                                        let _ = tx.send(Err(ClientError::ConnectionClosed));
                                    }
                                    break;
                                }
                                Ok(Ok(_)) => continue,
                            }
                        }
                        return;
                    }
                }
            }
            // Read responses from the TCP stream
            result = reader.read_buf(&mut read_buf) => {
                match result {
                    Ok(0) | Err(_) => {
                        // Connection closed — fail all pending
                        for tx in pending.drain(..) {
                            let _ = tx.send(Err(ClientError::ConnectionClosed));
                        }
                        return;
                    }
                    Ok(_) => {
                        // Responses will be decoded at the top of the loop
                    }
                }
            }
        }
    }
}
