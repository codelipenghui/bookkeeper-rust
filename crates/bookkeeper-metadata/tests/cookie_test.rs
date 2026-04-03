use bookkeeper_metadata::cookie::Cookie;

#[test]
fn cookie_round_trip() {
    let cookie = Cookie::new(
        "bookie1:3181",
        "/data/journal",
        &["/data/ledgers"],
        Some("test-instance-id"),
        None,
    );

    let serialized = cookie.serialize();
    let parsed = Cookie::deserialize(&serialized).unwrap();

    assert_eq!(parsed.bookie_id(), "bookie1:3181");
    assert_eq!(parsed.journal_dirs(), "/data/journal");
    assert_eq!(parsed.instance_id(), Some("test-instance-id"));
}

#[test]
fn cookie_with_multiple_ledger_dirs() {
    let cookie = Cookie::new(
        "host:3181",
        "/journal",
        &["/ledgers1", "/ledgers2", "/ledgers3"],
        None,
        None,
    );

    let serialized = cookie.serialize();
    assert!(serialized.starts_with("5\n"));
    let parsed = Cookie::deserialize(&serialized).unwrap();
    assert_eq!(parsed.ledger_dirs(), "3\t/ledgers1\t/ledgers2\t/ledgers3");
}

#[test]
fn cookie_with_index_dirs() {
    let cookie = Cookie::new(
        "host:3181",
        "/journal",
        &["/ledgers"],
        Some("uuid-123"),
        Some(&["/index1", "/index2"]),
    );

    let serialized = cookie.serialize();
    let parsed = Cookie::deserialize(&serialized).unwrap();
    assert_eq!(parsed.index_dirs(), Some("2\t/index1\t/index2"));
}

#[test]
fn cookie_starts_with_version_5() {
    let cookie = Cookie::new("b:3181", "/j", &["/l"], None, None);
    let s = cookie.serialize();
    assert!(s.starts_with("5\n"));
}

#[test]
fn cookie_validate_match() {
    let a = Cookie::new("b:3181", "/j", &["/l"], Some("id1"), None);
    let b = Cookie::new("b:3181", "/j", &["/l"], Some("id1"), None);
    assert!(a.validate_against(&b).is_ok());
}

#[test]
fn cookie_validate_bookie_id_mismatch() {
    let a = Cookie::new("b1:3181", "/j", &["/l"], None, None);
    let b = Cookie::new("b2:3181", "/j", &["/l"], None, None);
    assert!(a.validate_against(&b).is_err());
}

#[test]
fn cookie_validate_ledger_dirs_superset_ok() {
    let a = Cookie::new("b:3181", "/j", &["/l1", "/l2"], None, None);
    let b = Cookie::new("b:3181", "/j", &["/l1"], None, None);
    // b is a subset of a — expansion is allowed
    assert!(a.validate_against(&b).is_ok());
}
