use bookkeeper_config::server::ServerConfig;
use bookkeeper_server::server::BookieServer;
use bookkeeper_protocol::framing::BookieCodec;
use bookkeeper_protocol::v2::request::{encode_request, V2Request};
use bookkeeper_protocol::v2::response::{decode_response, V2Response};
use bookkeeper_protocol::constants::V2_EOK;
use bytes::{Bytes, BytesMut};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder};

fn test_config(port: u16, ledger_dir: &str) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = port;
    config.ledger_directories = vec![ledger_dir.to_string()];
    config.journal_directory = ledger_dir.to_string();
    config
}

fn make_entry(ledger_id: i64, entry_id: i64, lac: i64, payload: &[u8]) -> Bytes {
    let mut entry = Vec::new();
    entry.extend_from_slice(&ledger_id.to_be_bytes());
    entry.extend_from_slice(&entry_id.to_be_bytes());
    entry.extend_from_slice(&lac.to_be_bytes());
    entry.extend_from_slice(&(payload.len() as i64).to_be_bytes());
    entry.extend_from_slice(payload);
    Bytes::from(entry)
}

#[tokio::test]
async fn server_binds_and_shuts_down() {
    let dir = TempDir::new().unwrap();
    let config = test_config(0, dir.path().to_str().unwrap());

    let server = BookieServer::start(config).await.unwrap();
    let addr = server.local_addr();

    // Port should be OS-assigned (non-zero)
    assert_ne!(addr.port(), 0);

    // Shutdown should complete without hanging
    server.shutdown().await;
}

#[tokio::test]
async fn server_add_and_read_entry() {
    let dir = TempDir::new().unwrap();
    let config = test_config(0, dir.path().to_str().unwrap());

    let server = BookieServer::start(config).await.unwrap();
    let addr = server.local_addr();

    // Connect a TCP client
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();

    let mut codec = BookieCodec::new(5 * 1024 * 1024);

    // Build an ADD_ENTRY request
    let entry_data = make_entry(1, 0, -1, b"hello bookkeeper");
    let req = V2Request::AddEntry {
        flags: 0,
        master_key: [0u8; 20],
        entry_data,
        deferred_sync: false,
    };

    // Encode request into a framed wire message
    let req_bytes = encode_request(&req);
    let mut out = BytesMut::new();
    codec.encode(Bytes::from(req_bytes.freeze()), &mut out).unwrap();

    // Send to server
    writer.write_all(&out).await.unwrap();

    // Read response frames from server
    let mut read_buf = BytesMut::with_capacity(4096);
    let resp = loop {
        match codec.decode(&mut read_buf) {
            Ok(Some(frame)) => {
                break decode_response(frame.freeze()).unwrap();
            }
            Ok(None) => {
                let n = reader.read_buf(&mut read_buf).await.unwrap();
                assert!(n > 0, "server closed connection before sending response");
            }
            Err(e) => panic!("decode error: {e}"),
        }
    };

    // Verify response is EOK
    match resp {
        V2Response::Add { error_code, ledger_id, entry_id } => {
            assert_eq!(error_code, V2_EOK, "expected EOK, got {error_code}");
            assert_eq!(ledger_id, 1);
            assert_eq!(entry_id, 0);
        }
        other => panic!("expected Add response, got {other:?}"),
    }

    server.shutdown().await;
}
