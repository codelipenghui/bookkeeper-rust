//! Authentication framework matching Java's BookieAuthProvider / ClientAuthProvider.
//!
//! Auth is per-connection: the first AUTH request triggers the handshake,
//! and once complete, all subsequent requests on that connection are authorized.

/// Auth provider trait (server-side).
/// One instance per connection, created by the factory.
pub trait BookieAuthProvider: Send + Sync {
    /// Process an incoming auth message. Returns response payload.
    /// `plugin_name` must match the expected plugin name.
    /// Returns Ok(Some(response)) for multi-round, Ok(None) when complete.
    fn process(&mut self, plugin_name: &str, payload: &[u8]) -> Result<AuthResult, AuthError>;

    /// Called when TLS upgrade completes (for cert-based auth).
    fn on_protocol_upgrade(&mut self) {}
}

pub enum AuthResult {
    /// Auth handshake continues — send this response to client.
    Continue(Vec<u8>),
    /// Auth complete — connection is now authenticated.
    Complete,
}

#[derive(Debug)]
pub enum AuthError {
    /// Plugin name mismatch.
    PluginMismatch(String),
    /// Authentication failed.
    Unauthorized(String),
}

/// Factory for creating auth providers.
pub trait BookieAuthProviderFactory: Send + Sync {
    fn plugin_name(&self) -> &str;
    fn new_provider(&self) -> Box<dyn BookieAuthProvider>;
}

// ---------------------------------------------------------------------------
// Built-in: Disabled auth (always succeeds immediately)
// ---------------------------------------------------------------------------

/// Default auth provider — authentication disabled, all connections allowed.
/// Matches Java's AuthenticationDisabledAuthProviderFactory.
pub struct DisabledAuthProvider;

impl BookieAuthProvider for DisabledAuthProvider {
    fn process(&mut self, _plugin_name: &str, _payload: &[u8]) -> Result<AuthResult, AuthError> {
        Ok(AuthResult::Complete)
    }
}

pub struct DisabledAuthProviderFactory;

impl BookieAuthProviderFactory for DisabledAuthProviderFactory {
    fn plugin_name(&self) -> &str {
        "AUTHENTICATION_DISABLED"
    }
    fn new_provider(&self) -> Box<dyn BookieAuthProvider> {
        Box::new(DisabledAuthProvider)
    }
}

// ---------------------------------------------------------------------------
// Per-connection auth state
// ---------------------------------------------------------------------------

/// Tracks authentication state for a single connection.
/// Matches Java's AuthHandler.ServerSideHandler.
pub struct ConnectionAuthState {
    authenticated: bool,
    provider: Box<dyn BookieAuthProvider>,
    plugin_name: String,
}

impl ConnectionAuthState {
    pub fn new(factory: &dyn BookieAuthProviderFactory) -> Self {
        Self {
            authenticated: false,
            provider: factory.new_provider(),
            plugin_name: factory.plugin_name().to_string(),
        }
    }

    /// Is this connection authenticated?
    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    /// Process an AUTH request. Returns the response payload (if any).
    pub fn process_auth(
        &mut self,
        plugin_name: &str,
        payload: &[u8],
    ) -> Result<Option<Vec<u8>>, AuthError> {
        // Validate plugin name matches
        if plugin_name != self.plugin_name {
            return Err(AuthError::PluginMismatch(format!(
                "expected '{}', got '{plugin_name}'",
                self.plugin_name
            )));
        }
        match self.provider.process(plugin_name, payload)? {
            AuthResult::Continue(response) => Ok(Some(response)),
            AuthResult::Complete => {
                self.authenticated = true;
                Ok(None) // no response needed, auth complete
            }
        }
    }

    /// Notify that TLS upgrade completed (for cert-based auth).
    pub fn on_protocol_upgrade(&mut self) {
        self.provider.on_protocol_upgrade();
    }
}

// ---------------------------------------------------------------------------
// Factory selection from config
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Built-in: TLS/mTLS-based auth (client certificate authentication)
// ---------------------------------------------------------------------------

/// TLS auth provider — authenticates connections based on client certificate
/// verification during the TLS handshake. When `tlsClientAuthentication=true`,
/// the TLS acceptor already validates the client cert; this provider marks the
/// connection as authenticated when it receives the `on_protocol_upgrade` callback.
///
/// Matches Java's pattern where TLS client auth is enforced at the transport layer
/// and the auth provider simply records that the TLS handshake succeeded.
pub struct TlsAuthProvider {
    tls_authenticated: bool,
}

impl BookieAuthProvider for TlsAuthProvider {
    fn process(&mut self, _plugin_name: &str, _payload: &[u8]) -> Result<AuthResult, AuthError> {
        // If TLS auth is enabled, the TLS handshake already validated the cert.
        // Any explicit AUTH request just confirms completion.
        if self.tls_authenticated {
            Ok(AuthResult::Complete)
        } else {
            Err(AuthError::Unauthorized("TLS client auth required but not completed".into()))
        }
    }

    fn on_protocol_upgrade(&mut self) {
        // Called after successful TLS handshake with client cert verification.
        self.tls_authenticated = true;
    }
}

pub struct TlsAuthProviderFactory;

impl BookieAuthProviderFactory for TlsAuthProviderFactory {
    fn plugin_name(&self) -> &str {
        "TLS_AUTH"
    }
    fn new_provider(&self) -> Box<dyn BookieAuthProvider> {
        Box::new(TlsAuthProvider { tls_authenticated: false })
    }
}

/// Error type for auth factory configuration.
#[derive(Debug, thiserror::Error)]
pub enum AuthFactoryError {
    #[error(
        "unsupported bookieAuthProviderFactoryClass '{class}': only TLS-based auth plugins are \
        currently supported in the Rust implementation. Setting an unknown plugin would silently \
        disable authentication (security regression). If you need SASL/Kerberos/custom auth, \
        either remove the config (disables auth) or use a supported TLS-based plugin."
    )]
    UnsupportedPlugin { class: String },
}

/// Create an auth provider factory from config.
/// Matches Java's bookieAuthProviderFactoryClass config.
///
/// Returns an error when the configured factory class is not recognized,
/// rather than silently falling back to disabled auth. This prevents the
/// security regression of an operator typo or a missing plugin silently
/// turning authentication off.
pub fn create_auth_factory(
    factory_class: Option<&str>,
    tls_client_auth_enabled: bool,
) -> Result<Box<dyn BookieAuthProviderFactory>, AuthFactoryError> {
    // If TLS client auth is enabled, use TLS auth provider
    if tls_client_auth_enabled {
        tracing::info!("Auth: using TLS client certificate authentication");
        return Ok(Box::new(TlsAuthProviderFactory));
    }
    match factory_class {
        None | Some("") => {
            Ok(Box::new(DisabledAuthProviderFactory))
        }
        Some(class) if class.trim().is_empty() => {
            Ok(Box::new(DisabledAuthProviderFactory))
        }
        Some(class)
            if class.contains("AuthenticationDisabled")
                || class.contains("authentication_disabled") =>
        {
            Ok(Box::new(DisabledAuthProviderFactory))
        }
        Some(class) if class.contains("TLS") || class.contains("tls") => {
            Ok(Box::new(TlsAuthProviderFactory))
        }
        Some(class) => {
            Err(AuthFactoryError::UnsupportedPlugin { class: class.to_string() })
        }
    }
}
