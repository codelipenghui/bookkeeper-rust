//! TLS integration tests: Rust client ↔ Rust server with self-signed certs.
//!
//! Verifies the StartTLS upgrade flow works end-to-end with a real TLS handshake,
//! and that subsequent reads/writes succeed over the encrypted channel.

use std::sync::Arc;

use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_client::read_handle::ReadHandle;
use bookkeeper_client::tls::{build_tls_connector, ClientTlsConfig};
use bookkeeper_client::write_handle::WriteHandle;
use bookkeeper_config::server::ServerConfig;
use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_server::server::BookieServer;
use tempfile::TempDir;

/// Generate a self-signed certificate and return (cert_pem_path, key_pem_path).
/// The same cert serves as both the server cert and the trust store.
fn generate_self_signed_cert(dir: &std::path::Path) -> (String, String) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();

    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();

    (cert_path.to_string_lossy().to_string(), key_path.to_string_lossy().to_string())
}

fn tls_server_config(dir: &TempDir, cert_path: &str, key_path: &str) -> ServerConfig {
    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir.path().to_string_lossy().to_string()];
    config.journal_directory = dir.path().to_string_lossy().to_string();
    config.tls_certificate_path = Some(cert_path.to_string());
    config.tls_key_path = Some(key_path.to_string());
    config.tls_keystore_type = "PEM".to_string();
    config
}

fn make_client_tls_config(trust_store_path: &str) -> ClientTlsConfig {
    ClientTlsConfig {
        trust_store_path: Some(trust_store_path.to_string()),
        cert_path: None,
        key_path: None,
        client_authentication: false,
        enabled_protocols: None,
        hostname_verification_enabled: false,
        keystore_type: "PEM".to_string(),
    }
}

#[tokio::test]
async fn tls_handshake_succeeds_via_starttls() {
    let cert_dir = TempDir::new().unwrap();
    let (cert_path, key_path) = generate_self_signed_cert(cert_dir.path());

    let server_dir = TempDir::new().unwrap();
    let server_config = tls_server_config(&server_dir, &cert_path, &key_path);
    let server = BookieServer::start(server_config).await.unwrap();
    let addr = server.local_addr();

    // Client uses the server's cert as its trust store
    let client_tls = make_client_tls_config(&cert_path);
    let connector = build_tls_connector(&client_tls).unwrap();
    let connector = Arc::new(connector);

    // Connect with TLS — should perform StartTLS upgrade successfully
    let conn = BookieConnection::connect_pooled_with_tls(addr, 1, Some(connector))
        .await
        .expect("TLS handshake should succeed");
    assert_eq!(conn.num_channels(), 1);

    server.shutdown().await;
}

#[tokio::test]
async fn write_and_read_over_tls() {
    let cert_dir = TempDir::new().unwrap();
    let (cert_path, key_path) = generate_self_signed_cert(cert_dir.path());

    let server_dir = TempDir::new().unwrap();
    let server = BookieServer::start(tls_server_config(&server_dir, &cert_path, &key_path)).await.unwrap();
    let addr = server.local_addr();

    let client_tls = make_client_tls_config(&cert_path);
    let connector = Arc::new(build_tls_connector(&client_tls).unwrap());

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    // Write 10 entries via TLS
    let conns = vec![Arc::new(
        BookieConnection::connect_pooled_with_tls(addr, 1, Some(connector.clone()))
            .await.unwrap()
    )];
    let mut wh = WriteHandle::new(99, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    for i in 0..10 {
        let payload = format!("encrypted-payload-{i}");
        wh.append(&conns, payload.as_bytes()).await.unwrap();
    }

    // Read them back via TLS
    let read_conns = vec![Arc::new(
        BookieConnection::connect_pooled_with_tls(addr, 1, Some(connector))
            .await.unwrap()
    )];
    let rh = ReadHandle::new(99, ensemble, schedule, dm, master_key);
    for i in 0..10 {
        let payload = rh.read_entry(&read_conns, i).await.unwrap();
        let expected = format!("encrypted-payload-{i}");
        assert_eq!(payload, expected.as_bytes());
    }

    server.shutdown().await;
}

#[tokio::test]
async fn empty_trust_store_path_is_rejected() {
    let cfg = ClientTlsConfig {
        trust_store_path: None, // Java fails fast on missing trust store
        cert_path: None,
        key_path: None,
        client_authentication: false,
        enabled_protocols: None,
        hostname_verification_enabled: false,
        keystore_type: "PEM".to_string(),
    };
    let result = build_tls_connector(&cfg);
    assert!(result.is_err(), "expected build to fail without trust store");
    let err = result.err().unwrap().to_string();
    assert!(
        err.contains("tlsTrustStore") || err.contains("CA") || err.contains("required"),
        "error should mention trust store: {err}"
    );
}

#[tokio::test]
async fn unsupported_keystore_type_is_rejected() {
    let cfg = ClientTlsConfig {
        trust_store_path: Some("/tmp/dummy".to_string()),
        cert_path: None,
        key_path: None,
        client_authentication: false,
        enabled_protocols: None,
        hostname_verification_enabled: false,
        keystore_type: "JKS".to_string(),
    };
    let result = build_tls_connector(&cfg);
    assert!(result.is_err(), "JKS should be rejected (PEM only)");
}

#[tokio::test]
async fn pooled_tls_channels_all_connect() {
    let cert_dir = TempDir::new().unwrap();
    let (cert_path, key_path) = generate_self_signed_cert(cert_dir.path());

    let server_dir = TempDir::new().unwrap();
    let server = BookieServer::start(tls_server_config(&server_dir, &cert_path, &key_path)).await.unwrap();
    let addr = server.local_addr();

    let client_tls = make_client_tls_config(&cert_path);
    let connector = Arc::new(build_tls_connector(&client_tls).unwrap());

    // Pool of 4 channels — each performs its own StartTLS
    let conn = BookieConnection::connect_pooled_with_tls(addr, 4, Some(connector))
        .await
        .unwrap();
    assert_eq!(conn.num_channels(), 4);

    server.shutdown().await;
}

#[tokio::test]
async fn plain_client_against_tls_server_still_works() {
    // The Rust server with TLS configured still accepts plain V2 connections
    // (StartTLS is opt-in per connection, matching Java behavior).
    let cert_dir = TempDir::new().unwrap();
    let (cert_path, key_path) = generate_self_signed_cert(cert_dir.path());

    let server_dir = TempDir::new().unwrap();
    let server = BookieServer::start(tls_server_config(&server_dir, &cert_path, &key_path)).await.unwrap();
    let addr = server.local_addr();

    // Plain (no TLS) connection
    let conn = BookieConnection::connect(addr).await.unwrap();
    assert_eq!(conn.num_channels(), 1);

    server.shutdown().await;
}
