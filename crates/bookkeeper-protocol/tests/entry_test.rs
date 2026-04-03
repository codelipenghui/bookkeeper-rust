use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_protocol::entry::EntryBuilder;
use bytes::Bytes;

#[test]
fn entry_round_trip_crc32c() {
    let dm = DigestManager::new(DigestType::Crc32c);
    let payload = Bytes::from_static(b"hello world");
    let entry_bytes = EntryBuilder::new(&dm)
        .ledger_id(42).entry_id(7).lac(-1).length(11)
        .payload(payload.clone()).build();
    assert_eq!(entry_bytes.len(), 32 + 8 + 11);
    let parsed = bookkeeper_protocol::entry::parse_entry(&dm, &entry_bytes).unwrap();
    assert_eq!(parsed.ledger_id, 42);
    assert_eq!(parsed.entry_id, 7);
    assert_eq!(parsed.lac, -1);
    assert_eq!(parsed.length, 11);
    assert_eq!(parsed.payload, payload);
}

#[test]
fn entry_round_trip_dummy() {
    let dm = DigestManager::new(DigestType::Dummy);
    let payload = Bytes::from_static(b"test");
    let entry_bytes = EntryBuilder::new(&dm)
        .ledger_id(1).entry_id(0).lac(-1).length(4)
        .payload(payload.clone()).build();
    assert_eq!(entry_bytes.len(), 36);
    let parsed = bookkeeper_protocol::entry::parse_entry(&dm, &entry_bytes).unwrap();
    assert_eq!(parsed.payload, payload);
}

#[test]
fn entry_corrupt_digest_detected() {
    let dm = DigestManager::new(DigestType::Crc32c);
    let mut entry_bytes = EntryBuilder::new(&dm)
        .ledger_id(1).entry_id(0).lac(-1).length(5)
        .payload(Bytes::from_static(b"hello")).build();
    let last = entry_bytes.len() - 1;
    entry_bytes[last] ^= 0xFF;
    let result = bookkeeper_protocol::entry::parse_entry(&dm, &entry_bytes);
    assert!(result.is_err());
}
