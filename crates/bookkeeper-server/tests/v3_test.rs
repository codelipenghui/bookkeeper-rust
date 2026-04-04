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
