use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_protocol::v2::response::V2Response;
use bookkeeper_protocol::constants::V2_EOK;
use bookkeeper_server::server::BookieServer;
use bytes::Bytes;
use tempfile::TempDir;

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
async fn connect_and_add_entry() {
    let dir = TempDir::new().unwrap();
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    let server = BookieServer::start(config).await.unwrap();
    let addr = server.local_addr();

    let conn = BookieConnection::connect(addr).await.unwrap();

    let entry_data = make_entry(1, 0, -1, b"hello from client");
    let resp = conn.send_add_entry(0, [0u8; 20], entry_data).await.unwrap();

    match resp {
        V2Response::Add { error_code, ledger_id, entry_id } => {
            assert_eq!(error_code, V2_EOK);
            assert_eq!(ledger_id, 1);
            assert_eq!(entry_id, 0);
        }
        _ => panic!("expected Add response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn connect_and_read_entry() {
    let dir = TempDir::new().unwrap();
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    let server = BookieServer::start(config).await.unwrap();
    let addr = server.local_addr();

    let conn = BookieConnection::connect(addr).await.unwrap();

    // Write
    let entry_data = make_entry(1, 0, -1, b"test data");
    conn.send_add_entry(0, [0u8; 20], entry_data).await.unwrap();

    // Read
    let resp = conn.send_read_entry(0, 1, 0, None).await.unwrap();
    match resp {
        V2Response::Read { error_code, data, .. } => {
            assert_eq!(error_code, V2_EOK);
            assert!(data.is_some());
        }
        _ => panic!("expected Read response"),
    }

    server.shutdown().await;
}
