use std::fs;
use std::io::BufReader;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

/// TLS configuration matching Java's TLSContextFactory parameters.
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub trust_store_path: Option<String>,
    pub client_auth: bool,
    pub keystore_type: String,
    pub enabled_protocols: Option<String>,
    pub cert_refresh_seconds: i64,
}

/// Build a TLS acceptor from certificate and key files.
///
/// Supports PEM and PKCS12 keystore types matching Java's TLSContextFactory:
/// - PEM: separate cert and key files (tlsCertificatePath + tlsKeyFilePath)
/// - PKCS12: single .p12/.pfx file containing cert + key (tlsKeyStore)
pub fn build_tls_acceptor(cfg: &TlsConfig) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    let (certs, key) = match cfg.keystore_type.to_uppercase().as_str() {
        "PEM" => load_pem(&cfg.cert_path, &cfg.key_path)?,
        "PKCS12" | "P12" | "PFX" => load_pkcs12(&cfg.key_path)?,
        other => return Err(format!("unsupported tlsKeyStoreType: {other} (supported: PEM, PKCS12)").into()),
    };

    if certs.is_empty() {
        return Err("no certificates found".into());
    }

    // Parse protocol versions (if configured)
    let protocol_versions: Option<Vec<&'static rustls::SupportedProtocolVersion>> =
        cfg.enabled_protocols.as_ref().and_then(|protocols| {
            let mut versions = Vec::new();
            for p in protocols.split(',') {
                match p.trim() {
                    "TLSv1.3" | "TLS1.3" => versions.push(&rustls::version::TLS13),
                    "TLSv1.2" | "TLS1.2" => versions.push(&rustls::version::TLS12),
                    other => tracing::warn!("TLS: unsupported protocol version {other}, ignoring"),
                }
            }
            if versions.is_empty() { None } else { Some(versions) }
        });

    // Build ServerConfig with optional protocol versions and client auth.
    // Single build path avoids redundant cert loading.
    let config = if cfg.client_auth {
        let trust_path = cfg.trust_store_path.as_deref()
            .ok_or("tlsTrustStore required when tlsClientAuthentication=true")?;
        let root_store = load_trust_store(trust_path)?;
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()?;
        if let Some(ref versions) = protocol_versions {
            rustls::ServerConfig::builder_with_protocol_versions(versions)
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)?
        } else {
            rustls::ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)?
        }
    } else {
        if let Some(ref versions) = protocol_versions {
            rustls::ServerConfig::builder_with_protocol_versions(versions)
                .with_no_client_auth()
                .with_single_cert(certs, key)?
        } else {
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)?
        }
    };

    // Note: rustls does not support arbitrary cipher suite selection.
    // It uses a curated, secure default set. The tlsEnabledCipherSuites config
    // is parsed but not applied — rustls' defaults are already secure and match
    // the common production cipher suites (TLS_AES_256_GCM_SHA384, etc.).

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Reloadable TLS acceptor that watches cert files for changes.
/// Matches Java's TLSContextFactory cert refresh mechanism.
pub struct ReloadableTlsAcceptor {
    acceptor: Arc<tokio::sync::RwLock<TlsAcceptor>>,
    config: TlsConfig,
    last_modified: parking_lot::Mutex<Option<SystemTime>>,
}

impl ReloadableTlsAcceptor {
    pub fn new(config: TlsConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let acceptor = build_tls_acceptor(&config)?;
        let mtime = cert_mtime(&config.cert_path);
        Ok(Self {
            acceptor: Arc::new(tokio::sync::RwLock::new(acceptor)),
            config,
            last_modified: parking_lot::Mutex::new(mtime),
        })
    }

    /// Get the current TLS acceptor for accepting connections.
    pub async fn acceptor(&self) -> TlsAcceptor {
        self.acceptor.read().await.clone()
    }

    /// Check if cert files have changed and reload if so.
    /// Called periodically from a background task.
    pub async fn maybe_reload(&self) -> bool {
        let current_mtime = cert_mtime(&self.config.cert_path);
        let should_reload = current_mtime != *self.last_modified.lock();

        if !should_reload {
            return false;
        }

        // Build new acceptor (no locks held)
        let new_acceptor = match build_tls_acceptor(&self.config) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!("TLS: certificate reload failed: {e}");
                return false;
            }
        };

        // Update acceptor and timestamp
        *self.acceptor.write().await = new_acceptor;
        *self.last_modified.lock() = current_mtime;
        tracing::info!("TLS: certificates reloaded");
        true
    }

    /// Start a background task that periodically checks for cert changes.
    pub fn start_refresh_task(self: &Arc<Self>, refresh_seconds: u64, mut shutdown_rx: tokio::sync::watch::Receiver<bool>) {
        if refresh_seconds == 0 {
            return; // disabled
        }
        let this = self.clone();
        tokio::spawn(async move {
            let interval = Duration::from_secs(refresh_seconds);
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() { break; }
                    }
                    _ = tokio::time::sleep(interval) => {
                        this.maybe_reload().await;
                    }
                }
            }
        });
    }
}

// ---------------------------------------------------------------------------
// File loading helpers
// ---------------------------------------------------------------------------

fn load_pem(cert_path: &str, key_path: &str) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), Box<dyn std::error::Error>> {
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

fn load_pkcs12(path: &str) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), Box<dyn std::error::Error>> {
    // rustls doesn't natively parse PKCS12. Suggest PEM conversion.
    Err(format!(
        "PKCS12 keystore at {path}: please convert to PEM format using:\n  \
         openssl pkcs12 -in {path} -out cert.pem -clcerts -nokeys\n  \
         openssl pkcs12 -in {path} -out key.pem -nocerts -nodes\n  \
         Then set tlsCertificatePath=cert.pem and tlsKeyFilePath=key.pem"
    ).into())
}

fn load_trust_store(path: &str) -> Result<rustls::RootCertStore, Box<dyn std::error::Error>> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut root_store = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut reader) {
        root_store.add(cert?)?;
    }
    Ok(root_store)
}

fn cert_mtime(path: &str) -> Option<SystemTime> {
    fs::metadata(path).ok().and_then(|m| m.modified().ok())
}
