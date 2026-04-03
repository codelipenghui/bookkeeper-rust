use bookkeeper_protocol::digest::{DigestManager, DigestType};

#[test]
fn crc32_digest_round_trip() {
    let dm = DigestManager::new(DigestType::Crc32);
    let metadata = build_metadata(1, 0, -1, 0);
    let payload = b"hello world";
    let digest = dm.compute(&metadata, payload);
    assert_eq!(digest.len(), 8);
    assert!(dm.verify(&metadata, payload, &digest));
}

#[test]
fn crc32c_digest_round_trip() {
    let dm = DigestManager::new(DigestType::Crc32c);
    let metadata = build_metadata(1, 0, -1, 11);
    let payload = b"hello world";
    let digest = dm.compute(&metadata, payload);
    assert_eq!(digest.len(), 4);  // Was 8, now 4
    assert!(dm.verify(&metadata, payload, &digest));
}

#[test]
fn hmac_digest_round_trip() {
    let dm = DigestManager::new(DigestType::HmacSha1).with_password(b"secret");
    let metadata = build_metadata(1, 0, -1, 11);
    let payload = b"hello world";
    let digest = dm.compute(&metadata, payload);
    assert_eq!(digest.len(), 20);
    assert!(dm.verify(&metadata, payload, &digest));
}

#[test]
fn dummy_digest() {
    let dm = DigestManager::new(DigestType::Dummy);
    let metadata = build_metadata(1, 0, -1, 0);
    let digest = dm.compute(&metadata, b"anything");
    assert_eq!(digest.len(), 0);
    assert!(dm.verify(&metadata, b"anything", &digest));
}

#[test]
fn crc32_digest_detects_corruption() {
    let dm = DigestManager::new(DigestType::Crc32);
    let metadata = build_metadata(1, 0, -1, 11);
    let digest = dm.compute(&metadata, b"hello world");
    assert!(!dm.verify(&metadata, b"HELLO world", &digest));
}

fn build_metadata(ledger_id: i64, entry_id: i64, lac: i64, length: i64) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[0..8].copy_from_slice(&ledger_id.to_be_bytes());
    buf[8..16].copy_from_slice(&entry_id.to_be_bytes());
    buf[16..24].copy_from_slice(&lac.to_be_bytes());
    buf[24..32].copy_from_slice(&length.to_be_bytes());
    buf
}
