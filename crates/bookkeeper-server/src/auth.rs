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

/// Create an auth provider factory from config.
/// Matches Java's bookieAuthProviderFactoryClass config.
pub fn create_auth_factory(
    factory_class: Option<&str>,
) -> Box<dyn BookieAuthProviderFactory> {
    match factory_class {
        None | Some("") => {
            // Default: auth disabled
            Box::new(DisabledAuthProviderFactory)
        }
        Some(class) => {
            eprintln!("Auth: unknown factory class '{class}', using disabled auth");
            Box::new(DisabledAuthProviderFactory)
        }
    }
}
