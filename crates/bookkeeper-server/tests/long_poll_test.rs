//! End-to-end tests for V3 long-poll reads.
//!
//! Tests the full flow through `process_v3_request`:
//! - When previous_lac < current LAC, returns immediately
//! - When previous_lac == current LAC, blocks until LAC advances or timeout
//! - When timeout expires with no advance, returns the existing entry/error

use std::time::Duration;

use bookkeeper_config::server::ServerConfig;
use bookkeeper_protocol::framing::BookieCodec;
use bookkeeper_protocol::v3::{decode_v3_response, encode_v3_request};
use bookkeeper_proto::*;
use bookkeeper_server::server::BookieServer;
use bytes::BytesMut;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder};

fn test_config(dir: &TempDir) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config
}

fn build_entry_body(ledger_id: i64, entry_id: i64, lac: i64, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(32 + payload.len());
    body.extend_from_slice(&ledger_id.to_be_bytes());
    body.extend_from_slice(&entry_id.to_be_bytes());
    body.extend_from_slice(&lac.to_be_bytes());
    body.extend_from_slice(&(payload.len() as i64).to_be_bytes());
    body.extend_from_slice(payload);
    body
}

fn make_long_poll_read(
    txn_id: u64,
    ledger_id: i64,
    entry_id: i64,
    previous_lac: i64,
    timeout_ms: i64,
) -> Request {
    Request {
        header: BkPacketHeader {
            version: ProtocolVersion::VersionThree as i32,
            operation: OperationType::ReadEntry as i32,
            txn_id,
            priority: Some(0),
        },
        read_request: Some(ReadRequest {
            ledger_id,
            entry_id,
            flag: None,
            master_key: None,
            previous_lac: Some(previous_lac),
            time_out: Some(timeout_ms),
        }),
        ..Default::default()
    }
}

fn make_v3_add_request(
    txn_id: u64,
    ledger_id: i64,
    entry_id: i64,
    master_key: &[u8],
    body: &[u8],
) -> Request {
    Request {
        header: BkPacketHeader {
            version: ProtocolVersion::VersionThree as i32,
            operation: OperationType::AddEntry as i32,
            txn_id,
            priority: Some(0),
        },
        add_request: Some(AddRequest {
            ledger_id,
            entry_id,
            master_key: master_key.to_vec(),
            body: body.to_vec(),
            flag: None,
            write_flags: None,
        }),
        ..Default::default()
    }
}

async fn open_v3_conn(addr: std::net::SocketAddr) -> (
    tokio::net::tcp::OwnedReadHalf,
    tokio::net::tcp::OwnedWriteHalf,
    BookieCodec,
    BytesMut,
) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, writer) = stream.into_split();
    (reader, writer, BookieCodec::new(5 * 1024 * 1024), BytesMut::with_capacity(4096))
}

async fn send_v3_request(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    codec: &mut BookieCodec,
    read_buf: &mut BytesMut,
    req: &Request,
) -> Response {
    let req_bytes = encode_v3_request(req);
    let mut out = BytesMut::new();
    codec.encode(req_bytes.freeze(), &mut out).unwrap();
    writer.write_all(&out).await.unwrap();

    loop {
        match codec.decode(read_buf) {
            Ok(Some(frame)) => return decode_v3_response(&frame).unwrap(),
            Ok(None) => {
                let n = reader.read_buf(read_buf).await.unwrap();
                assert!(n > 0, "server closed connection");
            }
            Err(e) => panic!("decode error: {e}"),
        }
    }
}

#[tokio::test]
async fn long_poll_returns_immediately_when_lac_already_advanced() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let (mut reader, mut writer, mut codec, mut buf) = open_v3_conn(addr).await;
    let master_key = bookkeeper_protocol::digest::generate_master_key(b"test");

    // Add entries 0, 1, 2 — LAC will advance to 1 (after entry 2 references LAC=1)
    for i in 0..3 {
        let body = build_entry_body(7, i, i - 1, b"data");
        let req = make_v3_add_request(i as u64, 7, i, &master_key, &body);
        let resp = send_v3_request(&mut reader, &mut writer, &mut codec, &mut buf, &req).await;
        assert_eq!(resp.status, StatusCode::Eok as i32);
    }

    // Long-poll read with previous_lac = -1: LAC has advanced past it,
    // so the read should return immediately (no blocking).
    let start = std::time::Instant::now();
    let req = make_long_poll_read(99, 7, 0, -1, 5000);
    let resp = send_v3_request(&mut reader, &mut writer, &mut codec, &mut buf, &req).await;
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_millis(500),
        "long-poll should return quickly when LAC already advanced; took {elapsed:?}");
    assert_eq!(resp.status, StatusCode::Eok as i32);
    assert!(resp.read_response.is_some());

    server.shutdown().await;
}

#[tokio::test]
async fn long_poll_blocks_until_lac_advances() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // Pre-populate: write entries 0 and 1, so current_lac (highest entry id) = 1.
    let (mut wr_reader, mut wr_writer, mut wr_codec, mut wr_buf) = open_v3_conn(addr).await;
    let master_key = bookkeeper_protocol::digest::generate_master_key(b"test");

    let body = build_entry_body(8, 0, -1, b"first");
    let req = make_v3_add_request(1, 8, 0, &master_key, &body);
    let resp = send_v3_request(&mut wr_reader, &mut wr_writer, &mut wr_codec, &mut wr_buf, &req).await;
    assert_eq!(resp.status, StatusCode::Eok as i32);

    let body = build_entry_body(8, 1, 0, b"second");
    let req = make_v3_add_request(2, 8, 1, &master_key, &body);
    let resp = send_v3_request(&mut wr_reader, &mut wr_writer, &mut wr_codec, &mut wr_buf, &req).await;
    assert_eq!(resp.status, StatusCode::Eok as i32);

    // Open a separate reader connection and start a long-poll for previous_lac=1.
    // Since current_lac == 1, the watcher should register and block.
    let read_addr = addr;
    let reader_handle = tokio::spawn(async move {
        let (mut r_reader, mut r_writer, mut r_codec, mut r_buf) = open_v3_conn(read_addr).await;
        let req = make_long_poll_read(99, 8, 1, 1, 10000);
        let start = std::time::Instant::now();
        let resp = send_v3_request(&mut r_reader, &mut r_writer, &mut r_codec, &mut r_buf, &req).await;
        (resp, start.elapsed())
    });

    // Give the reader time to register its watcher
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Now add entry 2 with lac=1 (notify(1) — doesn't wake watcher waiting for >1)
    // then entry 3 with lac=2 → notify(2) → 2 > 1 → wakes the watcher.
    let body = build_entry_body(8, 2, 1, b"third");
    let req = make_v3_add_request(3, 8, 2, &master_key, &body);
    let resp = send_v3_request(&mut wr_reader, &mut wr_writer, &mut wr_codec, &mut wr_buf, &req).await;
    assert_eq!(resp.status, StatusCode::Eok as i32);

    let body = build_entry_body(8, 3, 2, b"fourth");
    let req = make_v3_add_request(4, 8, 3, &master_key, &body);
    let resp = send_v3_request(&mut wr_reader, &mut wr_writer, &mut wr_codec, &mut wr_buf, &req).await;
    assert_eq!(resp.status, StatusCode::Eok as i32);

    // Reader should now wake up and return
    let (read_resp, elapsed) = tokio::time::timeout(
        Duration::from_secs(5), reader_handle
    ).await.unwrap().unwrap();

    assert_eq!(read_resp.status, StatusCode::Eok as i32);
    assert!(elapsed >= Duration::from_millis(250),
        "long-poll should have blocked at least until LAC advanced; took {elapsed:?}");
    assert!(elapsed < Duration::from_secs(5),
        "long-poll should have woken when LAC advanced; took {elapsed:?}");

    server.shutdown().await;
}

#[tokio::test]
async fn long_poll_times_out_when_no_advance() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let (mut reader, mut writer, mut codec, mut buf) = open_v3_conn(addr).await;
    let master_key = bookkeeper_protocol::digest::generate_master_key(b"test");

    // Add one entry to bootstrap the ledger
    let body = build_entry_body(9, 0, -1, b"only");
    let req = make_v3_add_request(1, 9, 0, &master_key, &body);
    let resp = send_v3_request(&mut reader, &mut writer, &mut codec, &mut buf, &req).await;
    assert_eq!(resp.status, StatusCode::Eok as i32);

    // Long-poll for previous_lac=100 (which is way ahead) with a 500ms timeout.
    // No further writes will happen, so this should time out.
    let start = std::time::Instant::now();
    let req = make_long_poll_read(99, 9, 0, 100, 500);
    let resp = send_v3_request(&mut reader, &mut writer, &mut codec, &mut buf, &req).await;
    let elapsed = start.elapsed();

    // Should have waited at least ~400ms (allowing some slack)
    assert!(elapsed >= Duration::from_millis(400),
        "should have waited for timeout; took {elapsed:?}");
    // And not absurdly long
    assert!(elapsed < Duration::from_secs(5));
    // Response should still come back (may be Eok with the existing entry, or error)
    let _ = resp;

    server.shutdown().await;
}
