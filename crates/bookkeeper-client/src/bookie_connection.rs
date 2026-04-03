use std::net::SocketAddr;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
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

/// A TCP connection to a single bookie.
pub struct BookieConnection {
    reader: tokio::net::tcp::OwnedReadHalf,
    writer: tokio::net::tcp::OwnedWriteHalf,
    codec: BookieCodec,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl BookieConnection {
    pub async fn connect(addr: SocketAddr) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader,
            writer,
            codec: BookieCodec::new(5 * 1024 * 1024),
            read_buf: BytesMut::with_capacity(64 * 1024),
            write_buf: BytesMut::with_capacity(4096),
        })
    }

    async fn send_request(&mut self, req: V2Request) -> Result<V2Response, ClientError> {
        // Encode and send
        let req_bytes = encode_request(&req);
        self.write_buf.clear();
        self.codec.encode(req_bytes.freeze(), &mut self.write_buf)
            .map_err(|e| ClientError::Protocol(format!("encode: {e}")))?;
        self.writer.write_all(&self.write_buf).await?;

        // Read response
        loop {
            match self.codec.decode(&mut self.read_buf) {
                Ok(Some(frame)) => {
                    return decode_response(frame.freeze())
                        .map_err(|e| ClientError::Protocol(format!("decode: {e}")));
                }
                Ok(None) => {
                    let n = self.reader.read_buf(&mut self.read_buf).await?;
                    if n == 0 {
                        return Err(ClientError::ConnectionClosed);
                    }
                }
                Err(e) => return Err(ClientError::Protocol(format!("frame: {e}"))),
            }
        }
    }

    pub async fn send_add_entry(
        &mut self,
        flags: u16,
        master_key: [u8; MASTER_KEY_LENGTH],
        entry_data: Bytes,
    ) -> Result<V2Response, ClientError> {
        let req = V2Request::AddEntry { flags, master_key, entry_data };
        self.send_request(req).await
    }

    pub async fn send_read_entry(
        &mut self,
        flags: u16,
        ledger_id: i64,
        entry_id: i64,
        master_key: Option<[u8; MASTER_KEY_LENGTH]>,
    ) -> Result<V2Response, ClientError> {
        let req = V2Request::ReadEntry { flags, ledger_id, entry_id, master_key };
        self.send_request(req).await
    }
}
