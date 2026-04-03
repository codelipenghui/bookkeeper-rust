use bookkeeper_metadata::ledger_metadata::*;

#[test]
fn build_open_ledger() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(42)
        .ensemble_size(3)
        .write_quorum_size(2)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["bookie1:3181".into(), "bookie2:3181".into(), "bookie3:3181".into()])
        .build()
        .unwrap();

    assert_eq!(md.ledger_id(), 42);
    assert_eq!(md.ensemble_size(), 3);
    assert_eq!(md.write_quorum_size(), 2);
    assert_eq!(md.ack_quorum_size(), 1);
    assert_eq!(md.state(), LedgerState::Open);
    assert_eq!(md.all_ensembles().len(), 1);
    assert_eq!(md.ensemble_at(0).unwrap().len(), 3);
}

#[test]
fn build_closed_ledger() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(3)
        .write_quorum_size(3)
        .ack_quorum_size(2)
        .new_ensemble_entry(0, vec!["b1:3181".into(), "b2:3181".into(), "b3:3181".into()])
        .with_closed_state(99, 1024)
        .build()
        .unwrap();

    assert_eq!(md.state(), LedgerState::Closed);
    assert_eq!(md.last_entry_id(), Some(99));
    assert_eq!(md.length(), Some(1024));
}

#[test]
fn build_fails_without_ensemble() {
    let result = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .build();
    assert!(result.is_err());
}

#[test]
fn build_fails_invalid_quorum() {
    let result = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(2)
        .write_quorum_size(3)
        .new_ensemble_entry(0, vec!["b1:3181".into(), "b2:3181".into()])
        .build();
    assert!(result.is_err());
}

#[test]
fn ensemble_at_with_segments() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(3)
        .write_quorum_size(2)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["a:3181".into(), "b:3181".into(), "c:3181".into()])
        .new_ensemble_entry(100, vec!["a:3181".into(), "d:3181".into(), "c:3181".into()])
        .build()
        .unwrap();

    assert_eq!(md.all_ensembles().len(), 2);
    let e0 = md.ensemble_at(50).unwrap();
    assert_eq!(e0[1], "b:3181");
    let e100 = md.ensemble_at(100).unwrap();
    assert_eq!(e100[1], "d:3181");
    let e200 = md.ensemble_at(200).unwrap();
    assert_eq!(e200[1], "d:3181");
}

#[test]
fn with_digest_and_password() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(1)
        .write_quorum_size(1)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b:3181".into()])
        .with_digest_type(DigestType::Crc32c)
        .with_password(b"secret".to_vec())
        .build()
        .unwrap();

    assert_eq!(md.digest_type(), Some(DigestType::Crc32c));
    assert_eq!(md.password(), Some(&b"secret"[..]));
}

#[test]
fn with_custom_metadata() {
    let mut custom = std::collections::BTreeMap::new();
    custom.insert("app".to_string(), b"myapp".to_vec());
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(1)
        .write_quorum_size(1)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b:3181".into()])
        .with_custom_metadata(custom)
        .build()
        .unwrap();

    assert_eq!(md.custom_metadata().get("app").unwrap(), &b"myapp"[..]);
}
