use bookkeeper_metadata::service_info::{BookieServiceInfo, Endpoint};

#[test]
fn default_service_info() {
    let info = BookieServiceInfo::new("bookie1", "bookie1", 3181);
    assert_eq!(info.bookie_id(), "bookie1");
    let bytes = info.to_protobuf_bytes();
    assert!(!bytes.is_empty());
    let parsed = BookieServiceInfo::from_protobuf_bytes(&bytes).unwrap();
    assert_eq!(parsed.bookie_id(), "bookie1");
}

#[test]
fn service_info_endpoint() {
    let info = BookieServiceInfo::new("host1:3181", "host1", 3181);
    let bytes = info.to_protobuf_bytes();
    let parsed = BookieServiceInfo::from_protobuf_bytes(&bytes).unwrap();
    assert_eq!(parsed.host(), "host1");
    assert_eq!(parsed.port(), 3181);
}

// --- round-trip preserves all proto fields ---
//
// Mirrors Apache BookKeeper's `BookieServiceInfoFormat` which carries
// a `repeated Endpoint endpoints`, a `map<string, string> properties`,
// and per-endpoint `repeated string auth` and
// `repeated string extensions`. A previous Rust port modelled only a
// single `(bookie_id, host, port)` triple — every round-trip dropped
// the alternates plus all properties + per-endpoint auth/extensions.
// Service-mesh deployments that rely on multiple endpoints (plain +
// TLS), Pulsar managed clusters that publish feature flags via
// `properties`, and security-extended bookies that advertise
// supported `auth` plugins all observe silent metadata loss on the
// next Rust-driven re-registration.

#[test]
fn round_trip_preserves_all_endpoints() {
    let info = BookieServiceInfo::with_endpoints(vec![
        Endpoint {
            id: "primary".into(),
            host: "bookie-1.svc".into(),
            port: 3181,
            protocol: "bookie-rpc".into(),
            auth: vec!["sasl".into()],
            extensions: vec!["tls-supported".into()],
        },
        Endpoint {
            id: "secure".into(),
            host: "bookie-1.svc".into(),
            port: 4181,
            protocol: "bookie-rpc-tls".into(),
            auth: vec!["sasl".into(), "token".into()],
            extensions: vec!["tls-supported".into()],
        },
        Endpoint {
            id: "stream".into(),
            host: "bookie-1.svc".into(),
            port: 4281,
            protocol: "bookie-stream".into(),
            auth: vec![],
            extensions: vec![],
        },
    ]);

    let bytes = info.to_protobuf_bytes();
    let parsed = BookieServiceInfo::from_protobuf_bytes(&bytes).unwrap();
    assert_eq!(parsed.endpoints().len(), 3, "all 3 endpoints must round-trip");
    assert_eq!(parsed.endpoints(), info.endpoints(),
        "every endpoint field must round-trip exactly");
}

#[test]
fn round_trip_preserves_properties() {
    let mut info = BookieServiceInfo::new("bookie-1:3181", "bookie-1", 3181);
    info.set_property("server-version", "5.4.0");
    info.set_property("feature.tls-only", "true");
    info.set_property("region", "us-east-1");

    let bytes = info.to_protobuf_bytes();
    let parsed = BookieServiceInfo::from_protobuf_bytes(&bytes).unwrap();
    assert_eq!(parsed.properties().get("server-version"), Some(&"5.4.0".to_string()));
    assert_eq!(parsed.properties().get("feature.tls-only"), Some(&"true".to_string()));
    assert_eq!(parsed.properties().get("region"), Some(&"us-east-1".to_string()));
    assert_eq!(parsed.properties().len(), 3);
}

#[test]
fn round_trip_preserves_per_endpoint_auth_and_extensions() {
    let info = BookieServiceInfo::with_endpoints(vec![
        Endpoint {
            id: "rpc".into(),
            host: "h".into(),
            port: 3181,
            protocol: "bookie-rpc".into(),
            auth: vec!["sasl".into(), "token-v2".into(), "kerberos".into()],
            extensions: vec!["fence-v3".into(), "long-poll-read".into()],
        },
    ]);
    let parsed = BookieServiceInfo::from_protobuf_bytes(&info.to_protobuf_bytes()).unwrap();
    assert_eq!(parsed.endpoints()[0].auth, vec![
        "sasl".to_string(), "token-v2".to_string(), "kerberos".to_string(),
    ]);
    assert_eq!(parsed.endpoints()[0].extensions, vec![
        "fence-v3".to_string(), "long-poll-read".to_string(),
    ]);
}

#[test]
fn convenience_accessors_pick_the_bookie_rpc_endpoint() {
    // Multi-endpoint info should still expose the bookie_id / host /
    // port of the bookie-rpc endpoint via the existing accessors so
    // the wide-spread call sites that just want the rpc address
    // don't have to learn about the new model.
    let info = BookieServiceInfo::with_endpoints(vec![
        Endpoint {
            id: "sidecar".into(),
            host: "sidecar.h".into(),
            port: 9999,
            protocol: "metrics".into(),
            auth: vec![],
            extensions: vec![],
        },
        Endpoint {
            id: "rpc".into(),
            host: "real.h".into(),
            port: 3181,
            protocol: "bookie-rpc".into(),
            auth: vec![],
            extensions: vec![],
        },
    ]);
    assert_eq!(info.bookie_id(), "rpc");
    assert_eq!(info.host(), "real.h");
    assert_eq!(info.port(), 3181);
}
