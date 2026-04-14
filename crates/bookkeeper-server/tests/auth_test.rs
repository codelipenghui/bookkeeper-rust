//! Tests for the authentication framework.
//!
//! Auth is per-connection: the first AUTH request triggers a handshake
//! (single-round for DisabledAuthProvider), and once complete, subsequent
//! requests are authorized.

use bookkeeper_server::auth::*;

// ---------------------------------------------------------------------------
// DisabledAuthProvider — single-round, always succeeds
// ---------------------------------------------------------------------------

#[test]
fn disabled_provider_completes_on_first_call() {
    let mut provider = DisabledAuthProvider;
    let result = provider.process("AUTHENTICATION_DISABLED", b"any-payload").unwrap();
    assert!(matches!(result, AuthResult::Complete));
}

#[test]
fn disabled_factory_plugin_name() {
    let factory = DisabledAuthProviderFactory;
    assert_eq!(factory.plugin_name(), "AUTHENTICATION_DISABLED");
}

#[test]
fn disabled_factory_creates_provider() {
    let factory = DisabledAuthProviderFactory;
    let mut provider = factory.new_provider();
    let result = provider.process("AUTHENTICATION_DISABLED", b"").unwrap();
    assert!(matches!(result, AuthResult::Complete));
}

// ---------------------------------------------------------------------------
// ConnectionAuthState
// ---------------------------------------------------------------------------

#[test]
fn connection_auth_state_starts_unauthenticated() {
    let factory = DisabledAuthProviderFactory;
    let state = ConnectionAuthState::new(&factory);
    assert!(!state.is_authenticated());
}

#[test]
fn auth_completes_after_correct_plugin_name() {
    let factory = DisabledAuthProviderFactory;
    let mut state = ConnectionAuthState::new(&factory);

    let response = state.process_auth("AUTHENTICATION_DISABLED", b"").unwrap();
    // Disabled provider returns None (no response needed)
    assert!(response.is_none());
    assert!(state.is_authenticated());
}

#[test]
fn auth_rejects_wrong_plugin_name() {
    let factory = DisabledAuthProviderFactory;
    let mut state = ConnectionAuthState::new(&factory);

    let result = state.process_auth("WRONG_PLUGIN", b"");
    assert!(result.is_err());
    match result {
        Err(AuthError::PluginMismatch(msg)) => {
            assert!(msg.contains("WRONG_PLUGIN"));
            assert!(msg.contains("AUTHENTICATION_DISABLED"));
        }
        other => panic!("expected PluginMismatch, got {other:?}"),
    }
    // Still not authenticated after failed handshake
    assert!(!state.is_authenticated());
}

#[test]
fn auth_state_persists_after_completion() {
    let factory = DisabledAuthProviderFactory;
    let mut state = ConnectionAuthState::new(&factory);

    state.process_auth("AUTHENTICATION_DISABLED", b"").unwrap();
    assert!(state.is_authenticated());
    // Subsequent calls don't change state
    state.process_auth("AUTHENTICATION_DISABLED", b"").unwrap();
    assert!(state.is_authenticated());
}

#[test]
fn on_protocol_upgrade_does_not_panic() {
    let factory = DisabledAuthProviderFactory;
    let mut state = ConnectionAuthState::new(&factory);
    // Should be a no-op for DisabledAuthProvider
    state.on_protocol_upgrade();
    assert!(!state.is_authenticated());
}

// ---------------------------------------------------------------------------
// create_auth_factory
// ---------------------------------------------------------------------------

#[test]
fn create_factory_with_no_class_returns_disabled() {
    let factory = create_auth_factory(None, false).unwrap();
    assert_eq!(factory.plugin_name(), "AUTHENTICATION_DISABLED");
}

#[test]
fn create_factory_with_empty_class_returns_disabled() {
    let factory = create_auth_factory(Some(""), false).unwrap();
    assert_eq!(factory.plugin_name(), "AUTHENTICATION_DISABLED");
}

#[test]
fn create_factory_with_unknown_class_is_hard_error() {
    // Critical security fix: unknown auth plugin must be a hard error rather
    // than a silent fallback to disabled auth, so an operator typo cannot
    // accidentally turn off authentication.  Matches the spirit of Java's
    // AuthProviderFactoryFactory which raises ClassNotFoundException.
    match create_auth_factory(Some("org.example.MyAuthProvider"), false) {
        Ok(_) => panic!("expected UnsupportedPlugin error, got Ok"),
        Err(AuthFactoryError::UnsupportedPlugin { class }) => {
            assert_eq!(class, "org.example.MyAuthProvider");
        }
    }
}

#[test]
fn create_factory_with_tls_class_returns_tls_factory() {
    let factory = create_auth_factory(Some("my.auth.TLSAuthProviderFactory"), false).unwrap();
    assert_eq!(factory.plugin_name(), "TLS_AUTH");
}

#[test]
fn create_factory_with_authentication_disabled_class_is_ok() {
    // Java's `AuthenticationDisabledAuthProviderFactory` must still map to
    // disabled auth without triggering the hard-error guard.
    let factory = create_auth_factory(
        Some("org.apache.bookkeeper.auth.AuthenticationDisabledAuthProviderFactory"),
        false,
    ).unwrap();
    assert_eq!(factory.plugin_name(), "AUTHENTICATION_DISABLED");
}

#[test]
fn create_factory_with_tls_client_auth_enabled_returns_tls_factory() {
    // If tlsClientAuthentication=true, TLS factory is used regardless of class.
    let factory = create_auth_factory(None, true).unwrap();
    assert_eq!(factory.plugin_name(), "TLS_AUTH");
}

// ---------------------------------------------------------------------------
// Multi-round provider — verify framework supports it
// ---------------------------------------------------------------------------

struct TwoRoundProvider {
    rounds: usize,
}

impl BookieAuthProvider for TwoRoundProvider {
    fn process(&mut self, _plugin: &str, _payload: &[u8]) -> Result<AuthResult, AuthError> {
        self.rounds += 1;
        if self.rounds == 1 {
            Ok(AuthResult::Continue(b"challenge".to_vec()))
        } else {
            Ok(AuthResult::Complete)
        }
    }
}

struct TwoRoundFactory;
impl BookieAuthProviderFactory for TwoRoundFactory {
    fn plugin_name(&self) -> &str { "TWO_ROUND" }
    fn new_provider(&self) -> Box<dyn BookieAuthProvider> {
        Box::new(TwoRoundProvider { rounds: 0 })
    }
}

#[test]
fn multi_round_handshake_continues_then_completes() {
    let factory = TwoRoundFactory;
    let mut state = ConnectionAuthState::new(&factory);

    // Round 1: Continue with challenge
    let resp = state.process_auth("TWO_ROUND", b"hello").unwrap();
    assert_eq!(resp, Some(b"challenge".to_vec()));
    assert!(!state.is_authenticated());

    // Round 2: Complete
    let resp = state.process_auth("TWO_ROUND", b"response").unwrap();
    assert!(resp.is_none());
    assert!(state.is_authenticated());
}

struct UnauthorizedProvider;
impl BookieAuthProvider for UnauthorizedProvider {
    fn process(&mut self, _plugin: &str, _payload: &[u8]) -> Result<AuthResult, AuthError> {
        Err(AuthError::Unauthorized("bad credentials".into()))
    }
}

struct UnauthorizedFactory;
impl BookieAuthProviderFactory for UnauthorizedFactory {
    fn plugin_name(&self) -> &str { "UNAUTHORIZED" }
    fn new_provider(&self) -> Box<dyn BookieAuthProvider> {
        Box::new(UnauthorizedProvider)
    }
}

#[test]
fn unauthorized_provider_rejects_handshake() {
    let factory = UnauthorizedFactory;
    let mut state = ConnectionAuthState::new(&factory);

    let result = state.process_auth("UNAUTHORIZED", b"creds");
    assert!(matches!(result, Err(AuthError::Unauthorized(_))));
    assert!(!state.is_authenticated());
}
