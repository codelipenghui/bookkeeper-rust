use bookkeeper_config::server::ServerConfig;
use bookkeeper_server::server::BookieServer;
use bookkeeper_protocol::framing::BookieCodec;
use bookkeeper_protocol::v3::{encode_v3_request, decode_v3_response};
use bookkeeper_proto::*;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder};
use tempfile::TempDir;

fn test_config(dir: &TempDir) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config
}

fn make_v3_add_request(txn_id: u64, ledger_id: i64, entry_id: i64, master_key: &[u8], body: &[u8]) -> Request {
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

fn make_v3_read_request(txn_id: u64, ledger_id: i64, entry_id: i64) -> Request {
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
            previous_lac: None,
            time_out: None,
        }),
        ..Default::default()
    }
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
            Ok(Some(frame)) => {
                return decode_v3_response(&frame).unwrap();
            }
            Ok(None) => {
                let n = reader.read_buf(read_buf).await.unwrap();
                assert!(n > 0, "server closed connection");
            }
            Err(e) => panic!("decode error: {e}"),
        }
    }
}

#[tokio::test]
async fn v3_add_and_read_entry() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let mut read_buf = BytesMut::with_capacity(4096);

    // Build entry body with metadata
    let entry_body = build_entry_body(1, 0, -1, b"hello v3");
    let master_key = bookkeeper_protocol::digest::generate_master_key(b"test");

    // V3 AddEntry
    let add_req = make_v3_add_request(1, 1, 0, &master_key, &entry_body);
    let add_resp = send_v3_request(&mut reader, &mut writer, &mut codec, &mut read_buf, &add_req).await;
    assert_eq!(add_resp.status, StatusCode::Eok as i32);
    let add_r = add_resp.add_response.unwrap();
    assert_eq!(add_r.ledger_id, 1);
    assert_eq!(add_r.entry_id, 0);
    // Verify txnId is echoed back
    assert_eq!(add_resp.header.txn_id, 1);

    // V3 ReadEntry
    let read_req = make_v3_read_request(2, 1, 0);
    let read_resp = send_v3_request(&mut reader, &mut writer, &mut codec, &mut read_buf, &read_req).await;
    assert_eq!(read_resp.status, StatusCode::Eok as i32);
    let read_r = read_resp.read_response.unwrap();
    assert_eq!(read_r.ledger_id, 1);
    assert_eq!(read_r.entry_id, 0);
    assert!(read_r.body.is_some());

    server.shutdown().await;
}

#[tokio::test]
async fn v3_auto_detection_alongside_v2() {
    // Verify the server still handles V2 requests after V3 support is added
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // Send a V2 request (existing test pattern)
    use bookkeeper_protocol::v2::request::{V2Request, encode_request};
    use bookkeeper_protocol::v2::response::{V2Response, decode_response};

    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let mut read_buf = BytesMut::with_capacity(4096);

    let entry_data = Bytes::from(build_entry_body(1, 0, -1, b"hello v2"));
    let req = V2Request::AddEntry {
        flags: 0,
        master_key: [0u8; 20],
        entry_data,
        deferred_sync: false,
    };
    let req_bytes = encode_request(&req);
    let mut out = BytesMut::new();
    codec.encode(req_bytes.freeze(), &mut out).unwrap();
    writer.write_all(&out).await.unwrap();

    let resp = loop {
        match codec.decode(&mut read_buf) {
            Ok(Some(frame)) => break decode_response(frame.freeze()).unwrap(),
            Ok(None) => { reader.read_buf(&mut read_buf).await.unwrap(); }
            Err(e) => panic!("{e}"),
        }
    };
    match resp {
        V2Response::Add { error_code, .. } => assert_eq!(error_code, 0),
        _ => panic!("expected Add"),
    }

    server.shutdown().await;
}

/// V3 header priority > 0 must route the request to the high-priority pool
/// and succeed even when the bookie is in read-only mode with a full disk
/// (as long as high-priority writes are still available).  This mirrors
/// Java's RequestUtils.isHighPriority() + BookieRequestProcessor behavior.
#[tokio::test]
async fn v3_high_priority_add_accepted_in_readonly_mode() {
    use bookkeeper_server::state::BookieState;

    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // First, add a regular entry to create the ledger.
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let mut read_buf = BytesMut::with_capacity(4096);

    let entry_body = build_entry_body(77, 0, -1, b"create");
    let master_key = bookkeeper_protocol::digest::generate_master_key(b"mk");
    let req = make_v3_add_request(1, 77, 0, &master_key, &entry_body);
    let resp = send_v3_request(&mut reader, &mut writer, &mut codec, &mut read_buf, &req).await;
    assert_eq!(resp.status, StatusCode::Eok as i32);

    // Transition bookie to read-only mode.
    server.state_manager().transition_to(BookieState::ReadOnly);

    // A normal-priority write should now fail with ReadOnly.
    let entry_body = build_entry_body(77, 1, 0, b"lp-normal");
    let req = make_v3_add_request(2, 77, 1, &master_key, &entry_body);
    let resp = send_v3_request(&mut reader, &mut writer, &mut codec, &mut read_buf, &req).await;
    let add_r = resp.add_response.expect("should have AddResponse");
    assert_eq!(add_r.status, StatusCode::Ereadonly as i32,
        "non-priority write should be rejected in read-only mode");

    // A high-priority write (header.priority > 0) should succeed because the
    // default `availableForHighPriorityWrites` flag is true.
    let entry_body = build_entry_body(77, 1, 0, b"lp-hp");
    let mut req = make_v3_add_request(3, 77, 1, &master_key, &entry_body);
    req.header.priority = Some(1); // >0 → high priority
    let resp = send_v3_request(&mut reader, &mut writer, &mut codec, &mut read_buf, &req).await;
    let add_r = resp.add_response.expect("should have AddResponse");
    assert_eq!(add_r.status, StatusCode::Eok as i32,
        "high-priority write (header.priority > 0) should succeed in read-only mode");

    server.shutdown().await;
}

/// V3 START_TLS op on a non-TLS bookie must return EBADREQ (matches Java's
/// BookieRequestProcessor.processStartTLSRequestV3 when shFactory == null).
#[tokio::test]
async fn v3_start_tls_on_non_tls_bookie_returns_ebadreq() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let mut read_buf = BytesMut::with_capacity(4096);

    let req = Request {
        header: BkPacketHeader {
            version: ProtocolVersion::VersionThree as i32,
            operation: OperationType::StartTls as i32,
            txn_id: 42,
            priority: Some(0),
        },
        start_tls_request: Some(StartTlsRequest {}),
        ..Default::default()
    };
    let resp = send_v3_request(&mut reader, &mut writer, &mut codec, &mut read_buf, &req).await;
    // Status should be EBADREQ (not silently EOK).
    assert_eq!(resp.status, StatusCode::Ebadreq as i32,
        "START_TLS on non-TLS bookie must return EBADREQ");
    assert_eq!(resp.header.txn_id, 42);

    server.shutdown().await;
}

/// V3 AddEntry must reject masterKeys whose length isn't exactly 20.
/// Pre-fix the V3→V2 conversion silently zero-padded a short key (so a
/// wrong-data client masquerades as the legitimate one) or truncated a
/// long key (so subsequent ops against the same ledger always
/// mismatched).
#[tokio::test]
async fn v3_add_entry_rejects_wrong_length_master_key() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let mut read_buf = BytesMut::with_capacity(4096);

    let entry_body = build_entry_body(1, 0, -1, b"hello");

    // Short key (5 bytes)
    let short_req = make_v3_add_request(10, 1, 0, b"short", &entry_body);
    let short_resp = send_v3_request(&mut reader, &mut writer, &mut codec, &mut read_buf, &short_req).await;
    assert_eq!(
        short_resp.status,
        StatusCode::Ebadreq as i32,
        "AddEntry with a 5-byte masterKey must return EBADREQ"
    );

    // Long key (40 bytes)
    let long_key = vec![0xAAu8; 40];
    let long_req = make_v3_add_request(11, 1, 0, &long_key, &entry_body);
    let long_resp = send_v3_request(&mut reader, &mut writer, &mut codec, &mut read_buf, &long_req).await;
    assert_eq!(
        long_resp.status,
        StatusCode::Ebadreq as i32,
        "AddEntry with a 40-byte masterKey must return EBADREQ"
    );

    server.shutdown().await;
}
