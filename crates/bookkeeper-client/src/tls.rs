//! Client-side TLS support matching Java BookKeeper's TLSContextFactory.
//!
//! TLS is enabled when `tlsProviderFactoryClass` is set in client config.
//! All TLS connections use the StartTLS protocol upgrade flow:
//! 1. Open plaintext TCP connection
//! 2. Send V3 StartTLSRequest
//! 3. Receive V3 StartTLSResponse (still plaintext)
//! 4. Perform TLS handshake
//! 5. All subsequent traffic is encrypted

use std::fs;
use std::io::BufReader;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

/// Client TLS configuration matching Java's ClientConfiguration TLS fields.
#[derive(Debug, Clone, Default)]
pub struct ClientTlsConfig {
    /// Trust store path (PEM format) for verifying the server certificate chain.
    pub trust_store_path: Option<String>,
    /// Client certificate chain for mTLS (PEM format).
    pub cert_path: Option<String>,
    /// Client private key for mTLS (PEM format).
    pub key_path: Option<String>,
    /// Enable mTLS (client presents its certificate).
    pub client_authentication: bool,
    /// Comma-separated list of enabled protocols, e.g. "TLSv1.2,TLSv1.3".
    pub enabled_protocols: Option<String>,
    /// Whether to verify the server's hostname against the certificate.
    /// Java defaults to false.
    pub hostname_verification_enabled: bool,
    /// Keystore type — only "PEM" is supported. JKS/PKCS12 must be converted.
    pub keystore_type: String,
}

/// Ensure rustls has a default CryptoProvider installed. When the workspace
/// has multiple crypto provider features enabled (e.g. `ring` and `aws-lc-rs`),
/// rustls refuses to auto-select one and panics. Explicitly install aws-lc-rs
/// (rustls's default-features provider) which is always available because
/// bookkeeper-client depends on rustls with default features. Safe to call
/// multiple times — subsequent calls are no-ops.
fn ensure_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // Ignore Err (already installed by another crate or thread).
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Build a TLS connector from the given client config.
pub fn build_tls_connector(cfg: &ClientTlsConfig) -> Result<TlsConnector, Box<dyn std::error::Error + Send + Sync>> {
    ensure_crypto_provider();

    if !cfg.keystore_type.eq_ignore_ascii_case("PEM") {
        // rustls can't read JKS keystores; give the operator the
        // exact conversion command rather than a generic "unsupported"
        // message. Java defaults to JKS so config files arriving from
        // a Java bookie deployment hit this path first.
        let hint = if cfg.keystore_type.eq_ignore_ascii_case("JKS") {
            " (convert with `keytool -importkeystore -srckeystore client.jks \
              -destkeystore client.p12 -deststoretype PKCS12` then split to \
              PEM via openssl)"
        } else {
            " (convert with openssl)"
        };
        return Err(format!(
            "tlsKeyStoreType={} is not supported by the client (only PEM){hint}",
            cfg.keystore_type
        ).into());
    }

    // Load trust store. Java's TLSContextFactory.updateClientContext throws
    // SecurityException("CA Certificate required") if tlsTrustStore is empty for
    // PEM keystore. Fail loudly to match.
    let trust_path = cfg.trust_store_path.as_deref()
        .ok_or("tlsTrustStore is required (CA certificate path) — set tlsTrustStore in client config")?;
    let root_store = load_trust_store(trust_path)?;
    if root_store.is_empty() {
        return Err(format!("tlsTrustStore at {trust_path} contains no certificates").into());
    }

    // Parse protocol versions
    let protocol_versions: Option<Vec<&'static rustls::SupportedProtocolVersion>> =
        cfg.enabled_protocols.as_ref().and_then(|protocols| {
            let mut versions = Vec::new();
            for p in protocols.split(',') {
                match p.trim() {
                    "TLSv1.3" | "TLS1.3" => versions.push(&rustls::version::TLS13),
                    "TLSv1.2" | "TLS1.2" => versions.push(&rustls::version::TLS12),
                    other => tracing::warn!("client TLS: unsupported protocol {other}, ignoring"),
                }
            }
            if versions.is_empty() { None } else { Some(versions) }
        });

    // Build base config
    let builder = if let Some(ref versions) = protocol_versions {
        ClientConfig::builder_with_protocol_versions(versions)
    } else {
        ClientConfig::builder()
    };

    let builder = if cfg.hostname_verification_enabled {
        builder.with_root_certificates(root_store)
    } else {
        // Java defaults to NO hostname verification. Install a custom verifier
        // that validates the chain but skips hostname matching.
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoHostnameVerifier::new(root_store)?))
    };

    let mut config = if cfg.client_authentication {
        let cert_path = cfg.cert_path.as_deref()
            .ok_or("tlsCertificatePath required when tlsClientAuthentication=true")?;
        let key_path = cfg.key_path.as_deref()
            .ok_or("tlsKeyStore (key file) required when tlsClientAuthentication=true")?;
        let (certs, key) = load_pem(cert_path, key_path)?;
        builder.with_client_auth_cert(certs, key)?
    } else {
        builder.with_no_client_auth()
    };

    // BookKeeper uses no ALPN
    config.alpn_protocols.clear();

    Ok(TlsConnector::from(Arc::new(config)))
}

fn load_pem(cert_path: &str, key_path: &str)
    -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), Box<dyn std::error::Error + Send + Sync>>
{
    let cert_file = fs::File::open(cert_path)?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()?;

    let key_file = fs::File::open(key_path)?;
    let mut key_reader = BufReader::new(key_file);
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or("no private key found in key file")?;

    Ok((certs, key))
}

fn load_trust_store(path: &str) -> Result<RootCertStore, Box<dyn std::error::Error + Send + Sync>> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut root_store = RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut reader) {
        root_store.add(cert?)?;
    }
    Ok(root_store)
}

/// Server certificate verifier that validates the chain but skips hostname matching.
/// Matches Java's default behavior (tlsHostnameVerificationEnabled=false).
#[derive(Debug)]
struct NoHostnameVerifier {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
}

impl NoHostnameVerifier {
    fn new(root_store: RootCertStore) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let inner = rustls::client::WebPkiServerVerifier::builder(Arc::new(root_store)).build()?;
        Ok(Self { inner })
    }
}

impl rustls::client::danger::ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // Pass a placeholder server name; the inner verifier will skip hostname
        // matching for our purposes since we don't expose its result.
        // Use a wildcard-like name that the inner verifier will accept the chain for.
        // Since rustls always validates SAN, we instead bypass the hostname check
        // by directly verifying the cert chain via webpki.
        // Try with a dummy name first; if that fails on hostname, manually verify chain.
        let dummy = ServerName::try_from("localhost").unwrap();
        match self.inner.verify_server_cert(end_entity, intermediates, &dummy, ocsp_response, now) {
            Ok(v) => Ok(v),
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
                // Hostname mismatch but chain is otherwise valid — accept it.
                // The chain validation already passed before the hostname check.
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
            Err(e) => Err(e),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
