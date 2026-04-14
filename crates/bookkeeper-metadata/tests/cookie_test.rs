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

#[test]
fn cookie_validate_journal_dirs_mismatch_fails() {
    let a = Cookie::new("b:3181", "/j1", &["/l"], None, None);
    let b = Cookie::new("b:3181", "/j2", &["/l"], None, None);
    let err = a.validate_against(&b).unwrap_err();
    assert!(format!("{err:?}").contains("journal"));
}

#[test]
fn cookie_validate_instance_id_mismatch_fails() {
    let a = Cookie::new("b:3181", "/j", &["/l"], Some("inst-1"), None);
    let b = Cookie::new("b:3181", "/j", &["/l"], Some("inst-2"), None);
    let err = a.validate_against(&b).unwrap_err();
    assert!(format!("{err:?}").contains("instance"));
}

#[test]
fn cookie_validate_instance_id_one_missing_ok() {
    // If only one side has instance_id, validation passes (legacy compat).
    let a = Cookie::new("b:3181", "/j", &["/l"], Some("inst-1"), None);
    let b = Cookie::new("b:3181", "/j", &["/l"], None, None);
    assert!(a.validate_against(&b).is_ok());
}

#[test]
fn cookie_validate_ledger_dirs_subset_fails() {
    // a has fewer dirs than b → b's dirs are not a subset of a → fails
    let a = Cookie::new("b:3181", "/j", &["/l1"], None, None);
    let b = Cookie::new("b:3181", "/j", &["/l1", "/l2"], None, None);
    assert!(a.validate_against(&b).is_err());
}

#[test]
fn cookie_deserialize_invalid_version_fails() {
    // Version 1 or 2 (below v3) is rejected
    let bad = "1\nbookieHost: \"b:3181\"\n";
    assert!(Cookie::deserialize(bad).is_err());
    let bad = "2\nbookieHost: \"b:3181\"\n";
    assert!(Cookie::deserialize(bad).is_err());
}

#[test]
fn cookie_deserialize_empty_fails() {
    assert!(Cookie::deserialize("").is_err());
}

#[test]
fn cookie_v3_legacy_format_parses() {
    // v3 format: version, bookie_id, journal_dirs, ledger_dirs (each on its own line)
    let v3 = "3\nbookie1:3181\n/journal\n2\t/ledger1\t/ledger2\n";
    let parsed = Cookie::deserialize(v3).unwrap();
    assert_eq!(parsed.bookie_id(), "bookie1:3181");
    assert_eq!(parsed.journal_dirs(), "/journal");
}

#[test]
fn cookie_round_trip_with_index_dirs() {
    let original = Cookie::new(
        "host:3181",
        "/journal",
        &["/l1", "/l2"],
        Some("instance-uuid"),
        Some(&["/idx1", "/idx2"]),
    );
    let s = original.serialize();
    let parsed = Cookie::deserialize(&s).unwrap();
    assert_eq!(parsed.bookie_id(), original.bookie_id());
    assert_eq!(parsed.journal_dirs(), original.journal_dirs());
    assert_eq!(parsed.ledger_dirs(), original.ledger_dirs());
    assert_eq!(parsed.instance_id(), original.instance_id());
    assert_eq!(parsed.index_dirs(), original.index_dirs());
}

#[test]
fn cookie_validate_self_is_ok() {
    let c = Cookie::new("b:3181", "/j", &["/l"], Some("id"), None);
    assert!(c.validate_against(&c).is_ok());
}

#[test]
fn cookie_java_format_with_escaped_tabs_parses() {
    // Java's TextFormat.printToString() escapes tab characters as \t in the
    // quoted string values. This test verifies the Rust parser correctly
    // unescapes them, matching Java's TextFormat.merge() behavior.
    let java_cookie = "5\n\
        bookieHost: \"bookie1:3181\"\n\
        journalDir: \"/data/journal\"\n\
        ledgerDirs: \"1\\t/data/ledgers\"\n\
        instanceId: \"test-id\"\n";
    let parsed = Cookie::deserialize(java_cookie).unwrap();
    assert_eq!(parsed.bookie_id(), "bookie1:3181");
    assert_eq!(parsed.journal_dirs(), "/data/journal");
    // After unescaping, the tab byte should be present
    assert_eq!(parsed.ledger_dirs(), "1\t/data/ledgers");
    assert_eq!(parsed.instance_id(), Some("test-id"));
}

#[test]
fn cookie_serialize_escapes_tabs() {
    // Verify that serialization escapes tab characters in ledger_dirs
    // to match Java's TextFormat output format.
    let cookie = Cookie::new("b:3181", "/j", &["/l1", "/l2"], None, None);
    let serialized = cookie.serialize();
    // The ledger_dirs "2\t/l1\t/l2" should be escaped as "2\\t/l1\\t/l2"
    assert!(serialized.contains("ledgerDirs: \"2\\t/l1\\t/l2\""),
        "serialized cookie should contain escaped tabs: {serialized}");
}

#[test]
fn cookie_java_multi_dir_round_trip() {
    // Simulate a Java-written cookie with multiple ledger dirs (escaped tabs)
    // and verify it round-trips correctly through Rust.
    let java_cookie = "5\n\
        bookieHost: \"host:3181\"\n\
        journalDir: \"/journal\"\n\
        ledgerDirs: \"3\\t/l1\\t/l2\\t/l3\"\n\
        instanceId: \"uuid-123\"\n\
        indexDirs: \"2\\t/idx1\\t/idx2\"\n";
    let parsed = Cookie::deserialize(java_cookie).unwrap();
    assert_eq!(parsed.ledger_dirs(), "3\t/l1\t/l2\t/l3");
    assert_eq!(parsed.index_dirs(), Some("2\t/idx1\t/idx2"));
    // Re-serialize and verify it produces the same escaped format
    let reserialized = parsed.serialize();
    assert!(reserialized.contains("ledgerDirs: \"3\\t/l1\\t/l2\\t/l3\""));
    assert!(reserialized.contains("indexDirs: \"2\\t/idx1\\t/idx2\""));
}
