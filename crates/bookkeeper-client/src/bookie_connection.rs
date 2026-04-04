use std::collections::VecDeque;
use std::net::SocketAddr;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};
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
}

struct PendingRequest {
    data: Bytes,
    response_tx: oneshot::Sender<Result<V2Response, ClientError>>,
}

/// A pipelined TCP connection to a single bookie.
/// All methods take `&self` — the connection is driven by a background task.
pub struct BookieConnection {
    request_tx: mpsc::Sender<PendingRequest>,
}

impl BookieConnection {
    pub async fn connect(addr: SocketAddr) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = stream.into_split();

        let (request_tx, request_rx) = mpsc::channel::<PendingRequest>(1024);

        tokio::spawn(connection_task(reader, writer, request_rx));

        Ok(Self { request_tx })
    }

    async fn send_request(&self, req: V2Request) -> Result<V2Response, ClientError> {
        let req_bytes = encode_request(&req);
        let mut framed = BytesMut::new();
        BookieCodec::new(5 * 1024 * 1024)
            .encode(req_bytes.freeze(), &mut framed)
            .map_err(|e| ClientError::Protocol(format!("encode: {e}")))?;

        let (response_tx, response_rx) = oneshot::channel();
        self.request_tx
            .send(PendingRequest {
                data: framed.freeze(),
                response_tx,
            })
            .await
            .map_err(|_| ClientError::ConnectionClosed)?;

        response_rx.await.map_err(|_| ClientError::ConnectionClosed)?
    }

    pub async fn send_add_entry(
        &self,
        flags: u16,
        master_key: [u8; MASTER_KEY_LENGTH],
        entry_data: Bytes,
    ) -> Result<V2Response, ClientError> {
        let req = V2Request::AddEntry { flags, master_key, entry_data };
        self.send_request(req).await
    }

    pub async fn send_read_entry(
        &self,
        flags: u16,
        ledger_id: i64,
        entry_id: i64,
        master_key: Option<[u8; MASTER_KEY_LENGTH]>,
    ) -> Result<V2Response, ClientError> {
        let req = V2Request::ReadEntry { flags, ledger_id, entry_id, master_key };
        self.send_request(req).await
    }
}

async fn connection_task(
    mut reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    mut request_rx: mpsc::Receiver<PendingRequest>,
) {
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
                    Some(req) => {
                        if writer.write_all(&req.data).await.is_err() {
                            let _ = req.response_tx.send(Err(ClientError::ConnectionClosed));
                            for tx in pending.drain(..) {
                                let _ = tx.send(Err(ClientError::ConnectionClosed));
                            }
                            return;
                        }
                        pending.push_back(req.response_tx);
                    }
                    None => {
                        // Sender side dropped — no more requests will come
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
