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

/// TCP socket options applied to every bookie connection.
/// Matches Java's `ClientConfiguration` TCP tuning knobs.
#[derive(Debug, Clone)]
pub struct TcpConfig {
    pub tcp_no_delay: bool,
    pub keepalive: bool,
    /// Seconds before first keepalive probe. -1 = OS default.
    pub keep_idle_secs: i32,
    /// Seconds between keepalive probes. -1 = OS default.
    pub keep_interval_secs: i32,
    /// Number of unacknowledged probes before dropping. -1 = OS default.
    pub keep_count: i32,
    /// Send buffer size in bytes. 0 = OS default.
    /// Matches Java's `clientSendBufferSize` (default 1MB).
    pub send_buffer_size: i32,
    /// Receive buffer size in bytes. 0 = OS default.
    /// Matches Java's `clientReceiveBufferSize` (default 1MB).
    pub recv_buffer_size: i32,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            tcp_no_delay: true,
            keepalive: true,
            keep_idle_secs: -1,
            keep_interval_secs: -1,
            keep_count: -1,
            send_buffer_size: 1_048_576, // 1MB, matching Java
            recv_buffer_size: 1_048_576,
        }
    }
}

impl TcpConfig {
    /// Build from a `ClientConfig`, mapping all TCP tuning fields.
    pub fn from_client_config(cfg: &bookkeeper_config::client::ClientConfig) -> Self {
        Self {
            tcp_no_delay: cfg.client_tcp_no_delay,
            keepalive: cfg.client_sock_keepalive,
            keep_idle_secs: cfg.tcp_keep_idle,
            keep_interval_secs: cfg.tcp_keep_intvl,
            keep_count: cfg.tcp_keep_cnt,
            send_buffer_size: cfg.client_send_buffer_size,
            recv_buffer_size: cfg.client_receive_buffer_size,
        }
    }
}

/// Apply TCP socket options to a connected stream.
/// Uses `socket2::SockRef` for cross-platform keepalive support.
pub fn configure_tcp_stream(stream: &TcpStream, cfg: &TcpConfig) -> std::io::Result<()> {
    stream.set_nodelay(cfg.tcp_no_delay)?;

    let sock = socket2::SockRef::from(stream);

    // Apply send/receive buffer sizes when configured (>0).
    // Matches Java's ClientConfiguration.getClientSendBufferSize() /
    // getClientReceiveBufferSize() which are applied via Netty's
    // SO_SNDBUF / SO_RCVBUF channel options.
    if cfg.send_buffer_size > 0 {
        sock.set_send_buffer_size(cfg.send_buffer_size as usize)?;
    }
    if cfg.recv_buffer_size > 0 {
        sock.set_recv_buffer_size(cfg.recv_buffer_size as usize)?;
    }

    if cfg.keepalive {
        let mut ka = socket2::TcpKeepalive::new();
        if cfg.keep_idle_secs > 0 {
            ka = ka.with_time(Duration::from_secs(cfg.keep_idle_secs as u64));
        }
        if cfg.keep_interval_secs > 0 {
            ka = ka.with_interval(Duration::from_secs(cfg.keep_interval_secs as u64));
        }
        #[cfg(any(
            target_os = "linux", target_os = "macos", target_os = "ios",
            target_os = "freebsd", target_os = "netbsd", target_os = "android",
        ))]
        if cfg.keep_count > 0 {
            ka = ka.with_retries(cfg.keep_count as u32);
        }
        sock.set_tcp_keepalive(&ka)?;
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("connection closed")]
    ConnectionClosed,
    /// Bookie returned a non-OK status code. Carries the typed error so callers
    /// can branch on the error kind (fenced, readonly, unauthorized, etc.),
    /// matching Java's `CompletionValue.statusCodeToExceptionCode()`.
    #[error("bookie error: {error}")]
    BookieError {
        error: bookkeeper_common::error::BKError,
    },
    /// Write quorum could not be reached; carries which bookie indices in the
    /// write set failed so the caller can trigger ensemble change.
    #[error("write quorum failed for entry {entry_id}: {message}")]
    WriteQuorumFailed {
        message: String,
        /// Indices into the ensemble (write-set positions) that failed.
        failed_indices: Vec<usize>,
        entry_id: i64,
    },
    /// Unrecoverable error during write — ledger fenced by another client or
    /// unauthorized access. Matches Java's `handleUnrecoverableErrorDuringAdd`.
    #[error("unrecoverable write error for entry {entry_id}: {error}")]
    UnrecoverableWrite {
        error: bookkeeper_common::error::BKError,
        entry_id: i64,
    },
    /// Attempt to use a WriteHandle after it has been closed.
    /// Matches Java's `BKLedgerClosedException` raised by `isHandleWritable()`
    /// when an app calls `addEntry()` after `close()`.
    #[error("ledger handle is closed")]
    LedgerClosed,
    /// The caller-supplied digest type does not match the digest type stored
    /// in the ledger metadata.  Matches Java's `DigestManager.instantiate`
    /// failure when the client opens with the wrong digest type.
    #[error("digest type mismatch: metadata has {expected:?}, caller supplied {actual:?}")]
    DigestTypeMismatch {
        expected: String,
        actual: String,
    },
}

impl ClientError {
    /// Create a `BookieError` from a raw V2 error code.
    pub fn from_v2_code(code: i32) -> Self {
        Self::BookieError {
            error: bookkeeper_common::error::BKError::from_v2_code(code),
        }
    }

    /// Returns the `BKError` if this is a `BookieError` variant.
    pub fn bookie_error(&self) -> Option<bookkeeper_common::error::BKError> {
        match self {
            Self::BookieError { error } => Some(*error),
            Self::UnrecoverableWrite { error, .. } => Some(*error),
            _ => None,
        }
    }
}

struct PendingRequest {
    data: Bytes,
    response_tx: oneshot::Sender<Result<V2Response, ClientError>>,
}

/// Default per-request timeout. Matches Java's `addEntryTimeoutSec` (30s default).
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// Per-request timeout. Matches Java's `addEntryTimeoutNanos` / `readEntryTimeoutNanos`.
    request_timeout: Duration,
    /// TCP socket options (keepalive, nodelay, etc.).
    tcp_config: TcpConfig,
}

struct ChannelState {
    sender: Option<mpsc::Sender<PendingRequest>>,
    /// Last time a reconnection was attempted (for backoff).
    last_connect_attempt: Option<tokio::time::Instant>,
    /// Current backoff duration (exponential: 1s → 2s → 4s → ... → 30s).
    current_backoff: Duration,
}

const MIN_RECONNECT_BACKOFF: Duration = Duration::from_secs(1);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);

impl BookieChannel {
    fn new(
        addr: SocketAddr,
        server_hostname: Option<String>,
        tls: Option<Arc<TlsConnector>>,
        request_timeout: Duration,
        tcp_config: TcpConfig,
    ) -> Self {
        Self {
            addr,
            server_hostname,
            tls,
            state: Mutex::new(ChannelState {
                sender: None,
                last_connect_attempt: None,
                current_backoff: MIN_RECONNECT_BACKOFF,
            }),
            request_timeout,
            tcp_config,
        }
    }

    /// Connect eagerly (used during pool initialization when possible).
    async fn try_connect(&self) {
        if let Ok(tx) = Self::open_connection(
            self.addr, self.server_hostname.as_deref(), self.tls.clone(), &self.tcp_config,
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
        tcp_config: &TcpConfig,
    ) -> Result<mpsc::Sender<PendingRequest>, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        configure_tcp_stream(&stream, tcp_config)?;

        // Bounded per-channel request queue (1024 slots).  This provides
        // application-level backpressure: once the queue is full, the
        // `send_request` caller's `.await` yields until space is available.
        //
        // Note on Java parity: Java's `PerChannelBookieClient.isWritable()`
        // inspects the Netty channel's TCP send buffer directly to detect
        // kernel-level backpressure and short-circuit writes with
        // `BKClientClosedException` or fail-fast.  The Rust implementation
        // does not expose a direct TCP-buffer readiness API via tokio's
        // `TcpStream`, so we approximate with this bounded channel — a
        // slightly coarser-grained but still-correct backpressure mechanism.
        // See [KNOWN DIVERGENCE #26] in the comprehensive review.
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
        let response_rx = self.enqueue(framed.freeze()).await?;
        match tokio::time::timeout(self.request_timeout, response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ClientError::ConnectionClosed),
            Err(_) => Err(ClientError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("request timed out after {:?}", self.request_timeout),
            ))),
        }
    }

    /// Send pre-framed bytes and await a response with a specified timeout.
    async fn send_raw(&self, data: Bytes, timeout: Duration) -> Result<V2Response, ClientError> {
        let response_rx = self.enqueue(data).await?;
        match tokio::time::timeout(timeout, response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ClientError::ConnectionClosed),
            Err(_) => Err(ClientError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut, "request timed out",
            ))),
        }
    }

    /// Ensure connection is live (reconnecting with backoff if needed),
    /// then enqueue a request and return the response receiver.
    async fn enqueue(
        &self,
        data: Bytes,
    ) -> Result<oneshot::Receiver<Result<V2Response, ClientError>>, ClientError> {
        let (response_tx, response_rx) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            let needs_reconnect = match &state.sender {
                None => true,
                Some(tx) => tx.is_closed(),
            };
            if needs_reconnect {
                let now = tokio::time::Instant::now();
                if let Some(last) = state.last_connect_attempt {
                    if now.duration_since(last) < state.current_backoff {
                        state.sender = None;
                        return Err(ClientError::ConnectionClosed);
                    }
                }
                state.last_connect_attempt = Some(now);
                match Self::open_connection(
                    self.addr, self.server_hostname.as_deref(), self.tls.clone(), &self.tcp_config,
                ).await {
                    Ok(tx) => {
                        state.sender = Some(tx);
                        state.current_backoff = MIN_RECONNECT_BACKOFF;
                    }
                    Err(e) => {
                        state.sender = None;
                        state.current_backoff = (state.current_backoff * 2).min(MAX_RECONNECT_BACKOFF);
                        return Err(e);
                    }
                }
            }
            state.sender.as_ref().unwrap()
                .send(PendingRequest { data, response_tx })
                .await
                .map_err(|_| ClientError::ConnectionClosed)?;
        }
        Ok(response_rx)
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
    pub async fn connect_pooled_with_tls(
        addr: SocketAddr,
        num_channels: usize,
        tls: Option<Arc<TlsConnector>>,
    ) -> Result<Self, ClientError> {
        Self::connect_pooled_with_tls_named(addr, None, num_channels, tls).await
    }

    /// Like `connect_pooled_with_tls` but accepts an optional hostname for SNI.
    pub async fn connect_pooled_with_tls_named(
        addr: SocketAddr,
        server_hostname: Option<String>,
        num_channels: usize,
        tls: Option<Arc<TlsConnector>>,
    ) -> Result<Self, ClientError> {
        Self::connect_full(addr, server_hostname, num_channels, tls, TcpConfig::default()).await
    }

    /// Full constructor with all options.
    pub async fn connect_full(
        addr: SocketAddr,
        server_hostname: Option<String>,
        num_channels: usize,
        tls: Option<Arc<TlsConnector>>,
        tcp_config: TcpConfig,
    ) -> Result<Self, ClientError> {
        let num_channels = num_channels.max(1);
        let mut channels = Vec::with_capacity(num_channels);
        let mut any_connected = false;

        for _ in 0..num_channels {
            let ch = BookieChannel::new(
                addr, server_hostname.clone(), tls.clone(),
                DEFAULT_REQUEST_TIMEOUT, tcp_config.clone(),
            );
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

    /// Frame a V3 protobuf Request and send it on the channel for `ledger_id`.
    /// Used for WriteLac, ReadLac, GetBookieInfo, and ForceLedger which must
    /// go over V3 protobuf for Java bookie compatibility.
    async fn send_v3(
        &self,
        ledger_id: i64,
        v3_req: bookkeeper_proto::Request,
        timeout: Duration,
    ) -> Result<V2Response, ClientError> {
        use prost::Message;
        let payload = Bytes::from(v3_req.encode_to_vec());
        let mut framed = BytesMut::new();
        BookieCodec::new(5 * 1024 * 1024)
            .encode(payload, &mut framed)
            .map_err(|e| ClientError::Protocol(format!("encode V3: {e}")))?;
        self.pick_channel(ledger_id).send_raw(framed.freeze(), timeout).await
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
        self.send_add_entry_with_flags(flags, master_key, entry_data, false).await
    }

    pub async fn send_add_entry_with_flags(
        &self,
        flags: u16,
        master_key: [u8; MASTER_KEY_LENGTH],
        entry_data: Bytes,
        deferred_sync: bool,
    ) -> Result<V2Response, ClientError> {
        let ledger_id = if entry_data.len() >= 8 {
            i64::from_be_bytes(entry_data[0..8].try_into().unwrap())
        } else {
            0
        };

        if deferred_sync {
            // DEFERRED_SYNC requires V3 protobuf to carry the writeFlags field.
            // V2 wire format has no write-flags field, so deferred_sync would be
            // silently dropped. Matches Java's PerChannelBookieClient which
            // rejects DEFERRED_SYNC on V2 with IllegalOpException.
            use bookkeeper_proto::*;
            let entry_id = if entry_data.len() >= 16 {
                i64::from_be_bytes(entry_data[8..16].try_into().unwrap())
            } else {
                0
            };
            let flag = if flags & FLAG_RECOVERY_ADD != 0 {
                Some(add_request::Flag::RecoveryAdd as i32)
            } else {
                None
            };
            let v3_req = Request {
                header: BkPacketHeader {
                    version: ProtocolVersion::VersionThree as i32,
                    operation: OperationType::AddEntry as i32,
                    txn_id: 0,
                    priority: None,
                },
                add_request: Some(AddRequest {
                    ledger_id,
                    entry_id,
                    master_key: master_key.to_vec(),
                    body: entry_data.to_vec(),
                    flag,
                    write_flags: Some(WRITE_FLAG_DEFERRED_SYNC),
                }),
                ..Default::default()
            };
            return self.send_v3(ledger_id, v3_req, Duration::from_secs(30)).await;
        }

        let req = V2Request::AddEntry { flags, master_key, entry_data, deferred_sync: false };
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

    /// Request bookie disk space info via V3 protobuf GetBookieInfo.
    /// `requested` bitmask: 1=free space, 2=total capacity, 3=both.
    /// Java bookies don't support GetBookieInfo in V2 mode (opcode 6 is ignored
    /// by PreV3 decoder), so V3 is required for interoperability.
    pub async fn send_get_bookie_info(&self, requested: i64) -> Result<V2Response, ClientError> {
        use bookkeeper_proto::*;
        let v3_req = Request {
            header: BkPacketHeader {
                version: ProtocolVersion::VersionThree as i32,
                operation: OperationType::GetBookieInfo as i32,
                txn_id: 0,
                priority: None,
            },
            get_bookie_info_request: Some(GetBookieInfoRequest {
                requested: Some(requested),
            }),
            ..Default::default()
        };
        self.send_v3(0, v3_req, Duration::from_secs(10)).await
    }

    /// Send a V3 long poll read request.
    /// Matches Java's `ReadLastConfirmedAndEntryOp` which sends a ReadRequest
    /// with `previousLAC` and `timeOut` to enable server-side blocking until
    /// LAC advances. Critical for tailing reads (Pulsar managed ledger cursors).
    pub async fn send_long_poll_read(
        &self,
        ledger_id: i64,
        entry_id: i64,
        previous_lac: i64,
        timeout_ms: i64,
    ) -> Result<V2Response, ClientError> {
        use bookkeeper_proto::*;
        use bookkeeper_proto::read_request::Flag;
        let v3_req = Request {
            header: BkPacketHeader {
                version: ProtocolVersion::VersionThree as i32,
                operation: OperationType::ReadEntry as i32,
                txn_id: 0,
                priority: None,
            },
            read_request: Some(ReadRequest {
                flag: Some(Flag::EntryPiggyback as i32),
                ledger_id,
                entry_id,
                master_key: None,
                previous_lac: Some(previous_lac),
                time_out: Some(timeout_ms),
            }),
            ..Default::default()
        };
        // Long poll timeout is server-side; use a generous client timeout
        let client_timeout = Duration::from_millis(timeout_ms as u64 + 10_000);
        self.send_v3(ledger_id, v3_req, client_timeout).await
    }

    /// Send a V2 BatchReadEntry request. Returns multiple entries in one round-trip.
    /// Matches Java's `BatchedReadOp`.
    pub async fn send_batch_read_entry(
        &self,
        ledger_id: i64,
        start_entry_id: i64,
        max_count: i32,
        max_size: i64,
    ) -> Result<V2Response, ClientError> {
        let req = V2Request::BatchReadEntry {
            flags: 0,
            ledger_id,
            start_entry_id,
            request_id: start_entry_id, // use entry_id as request_id
            max_count,
            max_size,
            master_key: None, // no fencing on regular batch reads
        };
        self.pick_channel(ledger_id).send_request(req).await
    }

    /// Send a WriteLac request to a bookie.
    /// Matches Java's `PendingWriteLacOp`.
    /// Send a WriteLac request via V3 protobuf.
    /// Java's PerChannelBookieClient.writeLac() ALWAYS uses VERSION_THREE
    /// (no V2 fallback), so we must match to ensure Java bookie compatibility.
    pub async fn send_write_lac(
        &self,
        ledger_id: i64,
        lac: i64,
        master_key: [u8; MASTER_KEY_LENGTH],
        lac_body: Bytes,
    ) -> Result<V2Response, ClientError> {
        use bookkeeper_proto::*;
        let v3_req = Request {
            header: BkPacketHeader {
                version: ProtocolVersion::VersionThree as i32,
                operation: OperationType::WriteLac as i32,
                txn_id: 0,
                priority: None,
            },
            write_lac_request: Some(WriteLacRequest {
                ledger_id,
                lac,
                master_key: master_key.to_vec(),
                body: lac_body.to_vec(),
            }),
            ..Default::default()
        };
        self.send_v3(ledger_id, v3_req, Duration::from_secs(30)).await
    }

    /// Send a ReadLac request via V3 protobuf.
    /// Java's PerChannelBookieClient.readLac() uses V3 protobuf in non-V2 mode.
    /// V2 mode sends a normal ReadEntry which is handled differently.
    /// We default to V3 for Java bookie compatibility.
    pub async fn send_read_lac(
        &self,
        ledger_id: i64,
    ) -> Result<V2Response, ClientError> {
        use bookkeeper_proto::*;
        let v3_req = Request {
            header: BkPacketHeader {
                version: ProtocolVersion::VersionThree as i32,
                operation: OperationType::ReadLac as i32,
                txn_id: 0,
                priority: None,
            },
            read_lac_request: Some(ReadLacRequest { ledger_id }),
            ..Default::default()
        };
        self.send_v3(ledger_id, v3_req, Duration::from_secs(30)).await
    }

    /// Send a V3 ForceLedger request to a bookie.
    /// Forces the bookie to fsync all deferred-sync entries for this ledger.
    /// Matches Java's `ForceLedgerOp` which sends to all bookies in the ensemble.
    pub async fn send_force_ledger(
        &self,
        ledger_id: i64,
    ) -> Result<V2Response, ClientError> {
        use bookkeeper_proto::*;
        let v3_req = Request {
            header: BkPacketHeader {
                version: ProtocolVersion::VersionThree as i32,
                operation: OperationType::ForceLedger as i32,
                txn_id: 0,
                priority: None,
            },
            force_ledger_request: Some(ForceLedgerRequest { ledger_id }),
            ..Default::default()
        };
        self.send_v3(ledger_id, v3_req, Duration::from_secs(30)).await
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
