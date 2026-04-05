use std::fs;
use std::io::BufReader;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

/// Build a TLS acceptor from PEM certificate and key files.
///
/// Matches Java's TLSContextFactory for PEM keystore type:
/// - tlsCertificatePath → certificate chain (PEM)
/// - tlsKeyStore/tlsKeyFilePath → private key (PEM)
/// - tlsTrustStore → CA certificates for client auth (PEM, optional)
/// - tlsClientAuthentication → require client certs (mTLS)
pub fn build_tls_acceptor(
    cert_path: &str,
    key_path: &str,
    trust_store_path: Option<&str>,
    client_auth: bool,
) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    // Load server certificate chain
    let cert_file = fs::File::open(cert_path)?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()?;

    if certs.is_empty() {
        return Err("no certificates found in cert file".into());
    }

    // Load server private key
    let key_file = fs::File::open(key_path)?;
    let mut key_reader = BufReader::new(key_file);
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or("no private key found in key file")?;

    // Build server config
    let mut config = if client_auth {
        // mTLS: require client certificates
        let trust_path = trust_store_path
            .ok_or("tlsTrustStore required when tlsClientAuthentication=true")?;
        let trust_file = fs::File::open(trust_path)?;
        let mut trust_reader = BufReader::new(trust_file);
        let mut root_store = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut trust_reader) {
            root_store.add(cert?)?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()?;
        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)?
    } else {
        // No client auth
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?
    };

    // No ALPN — BookKeeper protocol is not HTTP.
    // Java BookKeeper doesn't negotiate ALPN for bookie connections.

    Ok(TlsAcceptor::from(Arc::new(config)))
}
