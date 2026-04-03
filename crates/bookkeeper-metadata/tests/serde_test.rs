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
