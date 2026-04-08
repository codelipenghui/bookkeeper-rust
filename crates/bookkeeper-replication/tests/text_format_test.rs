//! Tests for protobuf text format serialization/deserialization.
//!
//! These tests verify byte-level compatibility with Java's
//! `com.google.protobuf.TextFormat` output, which is what BookKeeper uses
//! for storing under-replication, lock, layout, and auditor election data
//! in ZooKeeper.

use bookkeeper_proto::{
    AuditorVoteFormat, LedgerRereplicationLayoutFormat, LockDataFormat,
    UnderreplicatedLedgerFormat,
};
use bookkeeper_replication::text_format::*;

// ---------------------------------------------------------------------------
// UnderreplicatedLedgerFormat
// ---------------------------------------------------------------------------

#[test]
fn underreplicated_ledger_serialize_basic() {
    let msg = UnderreplicatedLedgerFormat {
        replica: vec!["bookie1:3181".into(), "bookie2:3181".into()],
        ctime: Some(1234567890),
    };
    let bytes = serialize_underreplicated_ledger(&msg);
    let text = String::from_utf8(bytes).unwrap();
    // Java format puts each replica on its own line
    assert!(text.contains("replica: \"bookie1:3181\""));
    assert!(text.contains("replica: \"bookie2:3181\""));
    assert!(text.contains("ctime: 1234567890"));
}

#[test]
fn underreplicated_ledger_serialize_no_ctime() {
    let msg = UnderreplicatedLedgerFormat {
        replica: vec!["bookie1:3181".into()],
        ctime: None,
    };
    let bytes = serialize_underreplicated_ledger(&msg);
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("replica: \"bookie1:3181\""));
    assert!(!text.contains("ctime"), "ctime should be omitted when None");
}

#[test]
fn underreplicated_ledger_round_trip() {
    let original = UnderreplicatedLedgerFormat {
        replica: vec!["a:1".into(), "b:2".into(), "c:3".into()],
        ctime: Some(42),
    };
    let bytes = serialize_underreplicated_ledger(&original);
    let parsed = parse_underreplicated_ledger(&bytes).unwrap();
    assert_eq!(parsed.replica, original.replica);
    assert_eq!(parsed.ctime, original.ctime);
}

#[test]
fn underreplicated_ledger_empty_replicas() {
    let msg = UnderreplicatedLedgerFormat { replica: vec![], ctime: Some(0) };
    let bytes = serialize_underreplicated_ledger(&msg);
    let parsed = parse_underreplicated_ledger(&bytes).unwrap();
    assert!(parsed.replica.is_empty());
    assert_eq!(parsed.ctime, Some(0));
}

#[test]
fn underreplicated_ledger_parse_extra_whitespace() {
    let text = "  replica: \"bookie1:3181\"  \n   ctime: 100  \n";
    let parsed = parse_underreplicated_ledger(text.as_bytes()).unwrap();
    assert_eq!(parsed.replica, vec!["bookie1:3181".to_string()]);
    assert_eq!(parsed.ctime, Some(100));
}

#[test]
fn underreplicated_ledger_parse_invalid_utf8_fails() {
    let bad = vec![0xff, 0xfe, 0xfd];
    assert!(parse_underreplicated_ledger(&bad).is_err());
}

// ---------------------------------------------------------------------------
// LockDataFormat
// ---------------------------------------------------------------------------

#[test]
fn lock_data_serialize() {
    let msg = LockDataFormat { bookie_id: Some("bookie1:3181".into()) };
    let bytes = serialize_lock_data(&msg);
    let text = String::from_utf8(bytes).unwrap();
    assert_eq!(text.trim(), "bookieId: \"bookie1:3181\"");
}

#[test]
fn lock_data_round_trip() {
    let msg = LockDataFormat { bookie_id: Some("hostname-with-dash:3181".into()) };
    let bytes = serialize_lock_data(&msg);
    let parsed = parse_lock_data(&bytes).unwrap();
    assert_eq!(parsed.bookie_id, msg.bookie_id);
}

#[test]
fn lock_data_empty() {
    let msg = LockDataFormat { bookie_id: None };
    let bytes = serialize_lock_data(&msg);
    let parsed = parse_lock_data(&bytes).unwrap();
    assert!(parsed.bookie_id.is_none() || parsed.bookie_id.as_deref() == Some(""));
}

// ---------------------------------------------------------------------------
// LedgerRereplicationLayoutFormat
// ---------------------------------------------------------------------------

#[test]
fn layout_serialize() {
    let msg = LedgerRereplicationLayoutFormat {
        r#type: "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory".into(),
        version: 1,
    };
    let bytes = serialize_layout(&msg);
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("type: \"org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory\""));
    assert!(text.contains("version: 1"));
}

#[test]
fn layout_round_trip() {
    let msg = LedgerRereplicationLayoutFormat {
        r#type: "FlatLedgerManager".into(),
        version: 2,
    };
    let bytes = serialize_layout(&msg);
    let parsed = parse_layout(&bytes).unwrap();
    assert_eq!(parsed.r#type, msg.r#type);
    assert_eq!(parsed.version, msg.version);
}

// ---------------------------------------------------------------------------
// AuditorVoteFormat
// ---------------------------------------------------------------------------

#[test]
fn auditor_vote_serialize() {
    let msg = AuditorVoteFormat { bookie_id: Some("auditor:3181".into()) };
    let bytes = serialize_auditor_vote(&msg);
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("bookieId: \"auditor:3181\""));
}

#[test]
fn parse_lock_data_matches_auditor_vote_format() {
    // AuditorVoteFormat and LockDataFormat both contain a single bookieId field
    // and produce identical wire format. parse_lock_data should be able to read
    // an auditor vote.
    let auditor = AuditorVoteFormat { bookie_id: Some("voter:3181".into()) };
    let bytes = serialize_auditor_vote(&auditor);
    let lock = parse_lock_data(&bytes).unwrap();
    assert_eq!(lock.bookie_id, Some("voter:3181".into()));
}

// ---------------------------------------------------------------------------
// Special characters and escaping
// ---------------------------------------------------------------------------

#[test]
fn round_trip_with_quote_in_bookie_id() {
    // Bookie IDs shouldn't normally have quotes, but the format must escape them.
    let msg = LockDataFormat { bookie_id: Some(r#"weird"id"#.into()) };
    let bytes = serialize_lock_data(&msg);
    let parsed = parse_lock_data(&bytes).unwrap();
    assert_eq!(parsed.bookie_id, msg.bookie_id);
}

#[test]
fn round_trip_with_backslash_in_bookie_id() {
    let msg = LockDataFormat { bookie_id: Some(r"path\subpath".into()) };
    let bytes = serialize_lock_data(&msg);
    let parsed = parse_lock_data(&bytes).unwrap();
    assert_eq!(parsed.bookie_id, msg.bookie_id);
}

#[test]
fn underreplicated_with_negative_ctime() {
    let msg = UnderreplicatedLedgerFormat {
        replica: vec!["b:1".into()],
        ctime: Some(-1),
    };
    let bytes = serialize_underreplicated_ledger(&msg);
    let parsed = parse_underreplicated_ledger(&bytes).unwrap();
    assert_eq!(parsed.ctime, Some(-1));
}
