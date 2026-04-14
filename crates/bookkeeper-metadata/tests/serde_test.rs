use bookkeeper_metadata::ledger_metadata::*;
use bookkeeper_metadata::metadata_serde::{serialize_metadata, deserialize_metadata};

#[test]
fn v3_round_trip_open_ledger() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(42)
        .ensemble_size(3)
        .write_quorum_size(2)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b1:3181".into(), "b2:3181".into(), "b3:3181".into()])
        .with_digest_type(DigestType::Crc32c)
        .with_password(b"test".to_vec())
        .build()
        .unwrap();

    let bytes = serialize_metadata(&md).unwrap();
    let parsed = deserialize_metadata(42, &bytes).unwrap();

    assert_eq!(parsed.ledger_id(), 42);
    assert_eq!(parsed.ensemble_size(), 3);
    assert_eq!(parsed.write_quorum_size(), 2);
    assert_eq!(parsed.ack_quorum_size(), 1);
    assert_eq!(parsed.state(), LedgerState::Open);
    assert_eq!(parsed.digest_type(), Some(DigestType::Crc32c));
    assert_eq!(parsed.password(), Some(&b"test"[..]));
    assert_eq!(parsed.all_ensembles().len(), 1);
}

#[test]
fn v3_round_trip_closed_ledger() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(3)
        .write_quorum_size(3)
        .ack_quorum_size(2)
        .new_ensemble_entry(0, vec!["a:3181".into(), "b:3181".into(), "c:3181".into()])
        .new_ensemble_entry(50, vec!["a:3181".into(), "d:3181".into(), "c:3181".into()])
        .with_closed_state(99, 4096)
        .with_digest_type(DigestType::Hmac)
        .with_password(b"pw".to_vec())
        .build()
        .unwrap();

    let bytes = serialize_metadata(&md).unwrap();
    let parsed = deserialize_metadata(1, &bytes).unwrap();

    assert_eq!(parsed.state(), LedgerState::Closed);
    assert_eq!(parsed.last_entry_id(), Some(99));
    assert_eq!(parsed.length(), Some(4096));
    assert_eq!(parsed.all_ensembles().len(), 2);
    assert_eq!(parsed.ensemble_at(0).unwrap()[1], "b:3181");
    assert_eq!(parsed.ensemble_at(50).unwrap()[1], "d:3181");
}

#[test]
fn v3_header_starts_with_version_string() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(1)
        .write_quorum_size(1)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b:3181".into()])
        .build()
        .unwrap();

    let bytes = serialize_metadata(&md).unwrap();
    let header = "BookieMetadataFormatVersion\t3\n";
    assert_eq!(&bytes[..header.len()], header.as_bytes());
}

#[test]
fn v3_with_custom_metadata() {
    let mut custom = std::collections::BTreeMap::new();
    custom.insert("key1".to_string(), b"val1".to_vec());
    custom.insert("key2".to_string(), b"val2".to_vec());

    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(1)
        .write_quorum_size(1)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b:3181".into()])
        .with_custom_metadata(custom)
        .build()
        .unwrap();

    let bytes = serialize_metadata(&md).unwrap();
    let parsed = deserialize_metadata(1, &bytes).unwrap();
    assert_eq!(parsed.custom_metadata().get("key1").unwrap(), &b"val1"[..]);
    assert_eq!(parsed.custom_metadata().get("key2").unwrap(), &b"val2"[..]);
}

#[test]
fn v3_with_ctime_and_ctoken() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(1)
        .write_quorum_size(1)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b:3181".into()])
        .with_ctime(1234567890)
        .with_c_token(999)
        .build()
        .unwrap();

    let bytes = serialize_metadata(&md).unwrap();
    let parsed = deserialize_metadata(1, &bytes).unwrap();
    assert_eq!(parsed.ctime(), 1234567890);
    assert_eq!(parsed.c_token(), 999);
}

// ---------------------------------------------------------------------------
// V1 text format deserialization
// ---------------------------------------------------------------------------

#[test]
fn v1_closed_ledger() {
    // Matches Java's v1 text format
    let v1_data = b"BookieMetadataFormatVersion\t1\n\
        2\n\
        3\n\
        4096\n\
        0\tbookie1:3181\tbookie2:3181\tbookie3:3181\n\
        99\tCLOSED\n";

    let parsed = deserialize_metadata(42, v1_data).unwrap();
    assert_eq!(parsed.ledger_id(), 42);
    assert_eq!(parsed.ensemble_size(), 3);
    assert_eq!(parsed.write_quorum_size(), 2);
    assert_eq!(parsed.ack_quorum_size(), 2); // v1: ack_quorum = write_quorum
    assert_eq!(parsed.state(), LedgerState::Closed);
    assert_eq!(parsed.last_entry_id(), Some(99));
    assert_eq!(parsed.length(), Some(4096));
    assert_eq!(parsed.all_ensembles().len(), 1);
}

#[test]
fn v1_in_recovery_ledger() {
    let v1_data = b"BookieMetadataFormatVersion\t1\n\
        2\n\
        3\n\
        0\n\
        0\ta:3181\tb:3181\tc:3181\n\
        -102\tCLOSED\n";

    let parsed = deserialize_metadata(10, v1_data).unwrap();
    assert_eq!(parsed.state(), LedgerState::InRecovery);
}

#[test]
fn v1_open_ledger() {
    let v1_data = b"BookieMetadataFormatVersion\t1\n\
        3\n\
        3\n\
        0\n\
        0\ta:3181\tb:3181\tc:3181\n";

    let parsed = deserialize_metadata(5, v1_data).unwrap();
    assert_eq!(parsed.state(), LedgerState::Open);
    assert_eq!(parsed.ensemble_size(), 3);
    assert_eq!(parsed.write_quorum_size(), 3);
}

// ---------------------------------------------------------------------------
// V2 protobuf TextFormat deserialization
// ---------------------------------------------------------------------------

#[test]
fn v2_closed_ledger() {
    let v2_data = b"BookieMetadataFormatVersion\t2\n\
ensembleSize: 3\n\
quorumSize: 2\n\
ackQuorumSize: 1\n\
length: 2048\n\
lastEntryId: 49\n\
state: CLOSED\n\
segment {\n\
  firstEntryId: 0\n\
  ensembleMember: \"bk1:3181\"\n\
  ensembleMember: \"bk2:3181\"\n\
  ensembleMember: \"bk3:3181\"\n\
}\n";

    let parsed = deserialize_metadata(7, v2_data).unwrap();
    assert_eq!(parsed.ledger_id(), 7);
    assert_eq!(parsed.ensemble_size(), 3);
    assert_eq!(parsed.write_quorum_size(), 2);
    assert_eq!(parsed.ack_quorum_size(), 1);
    assert_eq!(parsed.state(), LedgerState::Closed);
    assert_eq!(parsed.last_entry_id(), Some(49));
    assert_eq!(parsed.length(), Some(2048));
    let ens = parsed.ensemble_at(0).unwrap();
    assert_eq!(ens[0], "bk1:3181");
    assert_eq!(ens[1], "bk2:3181");
}

#[test]
fn v2_open_ledger_with_ctime() {
    let v2_data = b"BookieMetadataFormatVersion\t2\n\
ensembleSize: 2\n\
quorumSize: 2\n\
state: OPEN\n\
ctime: 1700000000000\n\
segment {\n\
  firstEntryId: 0\n\
  ensembleMember: \"a:3181\"\n\
  ensembleMember: \"b:3181\"\n\
}\n";

    let parsed = deserialize_metadata(1, v2_data).unwrap();
    assert_eq!(parsed.state(), LedgerState::Open);
    assert_eq!(parsed.ctime(), 1700000000000);
    assert_eq!(parsed.ensemble_size(), 2);
}

// ---------------------------------------------------------------------------
// V3 IN_RECOVERY state round-trip (C4)
// ---------------------------------------------------------------------------

/// Verify that IN_RECOVERY state round-trips through v3 serialization.
/// Matches Java's LedgerMetadata.State.IN_RECOVERY used during recovery.
#[test]
fn v3_in_recovery_state_round_trip() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(10)
        .ensemble_size(3)
        .write_quorum_size(2)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["a:3181".into(), "b:3181".into(), "c:3181".into()])
        .with_in_recovery_state()
        .with_digest_type(DigestType::Crc32c)
        .with_password(b"test".to_vec())
        .build()
        .unwrap();

    assert_eq!(md.state(), LedgerState::InRecovery);
    assert_eq!(md.last_entry_id(), None);
    assert_eq!(md.length(), None);

    let bytes = serialize_metadata(&md).unwrap();
    let parsed = deserialize_metadata(10, &bytes).unwrap();

    assert_eq!(parsed.state(), LedgerState::InRecovery);
    // IN_RECOVERY ledger has no last_entry_id (same as OPEN), matching Java
    assert_eq!(parsed.last_entry_id(), None);
    assert_eq!(parsed.ensemble_size(), 3);
}

/// Verify that from_existing + with_in_recovery_state correctly transitions
/// from OPEN to IN_RECOVERY, preserving all other fields.
#[test]
fn v3_transition_open_to_in_recovery() {
    let open_md = LedgerMetadataBuilder::new()
        .ledger_id(20)
        .ensemble_size(3)
        .write_quorum_size(2)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["a:3181".into(), "b:3181".into(), "c:3181".into()])
        .with_digest_type(DigestType::Crc32c)
        .with_password(b"pw".to_vec())
        .build()
        .unwrap();

    assert_eq!(open_md.state(), LedgerState::Open);

    let recovery_md = LedgerMetadataBuilder::from_existing(&open_md)
        .with_in_recovery_state()
        .build()
        .unwrap();

    assert_eq!(recovery_md.state(), LedgerState::InRecovery);
    assert_eq!(recovery_md.ensemble_size(), 3);
    assert_eq!(recovery_md.write_quorum_size(), 2);
    assert_eq!(recovery_md.digest_type(), Some(DigestType::Crc32c));

    // Round-trip through serialization
    let bytes = serialize_metadata(&recovery_md).unwrap();
    let parsed = deserialize_metadata(20, &bytes).unwrap();
    assert_eq!(parsed.state(), LedgerState::InRecovery);
}

// ---------------------------------------------------------------------------
// V2 digest type parsing — proto: CRC32=1, HMAC=2, CRC32C=3, DUMMY=4
// ---------------------------------------------------------------------------

/// Helper to build a V2 text-format metadata blob with a given digestType value.
fn v2_with_digest(digest_value: &str) -> Vec<u8> {
    format!(
        "BookieMetadataFormatVersion\t2\n\
ensembleSize: 3\n\
quorumSize: 2\n\
ackQuorumSize: 1\n\
state: OPEN\n\
digestType: {digest_value}\n\
password: \"secret\"\n\
segment {{\n\
  firstEntryId: 0\n\
  ensembleMember: \"b1:3181\"\n\
  ensembleMember: \"b2:3181\"\n\
  ensembleMember: \"b3:3181\"\n\
}}\n"
    )
    .into_bytes()
}

#[test]
fn v2_digest_type_crc32_by_name() {
    let data = v2_with_digest("CRC32");
    let parsed = deserialize_metadata(1, &data).unwrap();
    assert_eq!(parsed.digest_type(), Some(DigestType::Crc32));
}

#[test]
fn v2_digest_type_hmac_by_name() {
    let data = v2_with_digest("HMAC");
    let parsed = deserialize_metadata(1, &data).unwrap();
    assert_eq!(parsed.digest_type(), Some(DigestType::Hmac));
}

#[test]
fn v2_digest_type_crc32c_by_name() {
    let data = v2_with_digest("CRC32C");
    let parsed = deserialize_metadata(1, &data).unwrap();
    assert_eq!(parsed.digest_type(), Some(DigestType::Crc32c));
}

#[test]
fn v2_digest_type_crc32c_alt_name() {
    let data = v2_with_digest("CRC32_C");
    let parsed = deserialize_metadata(1, &data).unwrap();
    assert_eq!(parsed.digest_type(), Some(DigestType::Crc32c));
}

#[test]
fn v2_digest_type_dummy_by_name() {
    let data = v2_with_digest("DUMMY");
    let parsed = deserialize_metadata(1, &data).unwrap();
    assert_eq!(parsed.digest_type(), Some(DigestType::Dummy));
}

/// Protobuf TextFormat allows numeric enum values.
/// When Java writes `digestType: 3`, it means CRC32C (proto CRC32C=3).
#[test]
fn v2_digest_type_crc32c_by_number() {
    let data = v2_with_digest("3");
    let parsed = deserialize_metadata(1, &data).unwrap();
    assert_eq!(parsed.digest_type(), Some(DigestType::Crc32c));
}

#[test]
fn v2_digest_type_dummy_by_number() {
    let data = v2_with_digest("4");
    let parsed = deserialize_metadata(1, &data).unwrap();
    assert_eq!(parsed.digest_type(), Some(DigestType::Dummy));
}

#[test]
fn v2_digest_type_crc32_by_number() {
    let data = v2_with_digest("1");
    let parsed = deserialize_metadata(1, &data).unwrap();
    assert_eq!(parsed.digest_type(), Some(DigestType::Crc32));
}

#[test]
fn v2_digest_type_hmac_by_number() {
    let data = v2_with_digest("2");
    let parsed = deserialize_metadata(1, &data).unwrap();
    assert_eq!(parsed.digest_type(), Some(DigestType::Hmac));
}
