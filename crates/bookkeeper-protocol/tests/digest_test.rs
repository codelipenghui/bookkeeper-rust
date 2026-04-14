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
    assert_eq!(digest.len(), 4);
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

// ---------------------------------------------------------------------------
// WriteLac body digest tests (C10)
// ---------------------------------------------------------------------------

/// Verify that compute_lac produces a digest over the 16-byte [ledgerId:8][lac:8]
/// metadata, matching Java's LAC_METADATA_LENGTH = 16.
#[test]
fn lac_digest_crc32c_format() {
    let dm = DigestManager::new(DigestType::Crc32c);
    let mut lac_meta = [0u8; 16];
    lac_meta[0..8].copy_from_slice(&42i64.to_be_bytes());  // ledger_id
    lac_meta[8..16].copy_from_slice(&10i64.to_be_bytes()); // lac
    let digest = dm.compute_lac(&lac_meta);
    assert_eq!(digest.len(), 4, "CRC32C LAC digest should be 4 bytes");

    // Build the full WriteLac body: [ledgerId:8][lac:8][digest:4] = 20 bytes
    let mut body = Vec::with_capacity(20);
    body.extend_from_slice(&lac_meta);
    body.extend_from_slice(digest.as_slice());
    assert_eq!(body.len(), 20);

    // Verify the body is parseable: ledger_id and lac match
    let parsed_lid = i64::from_be_bytes(body[0..8].try_into().unwrap());
    let parsed_lac = i64::from_be_bytes(body[8..16].try_into().unwrap());
    assert_eq!(parsed_lid, 42);
    assert_eq!(parsed_lac, 10);
}

#[test]
fn lac_digest_crc32_format() {
    let dm = DigestManager::new(DigestType::Crc32);
    let mut lac_meta = [0u8; 16];
    lac_meta[0..8].copy_from_slice(&1i64.to_be_bytes());
    lac_meta[8..16].copy_from_slice(&5i64.to_be_bytes());
    let digest = dm.compute_lac(&lac_meta);
    assert_eq!(digest.len(), 8, "CRC32 LAC digest should be 8 bytes (i64)");
    // Total body = 16 + 8 = 24 bytes
}

#[test]
fn lac_digest_hmac_format() {
    let dm = DigestManager::new(DigestType::HmacSha1).with_password(b"pw");
    let mut lac_meta = [0u8; 16];
    lac_meta[0..8].copy_from_slice(&1i64.to_be_bytes());
    lac_meta[8..16].copy_from_slice(&0i64.to_be_bytes());
    let digest = dm.compute_lac(&lac_meta);
    assert_eq!(digest.len(), 20, "HMAC-SHA1 LAC digest should be 20 bytes");
    // Total body = 16 + 20 = 36 bytes
}

#[test]
fn lac_digest_dummy_format() {
    let dm = DigestManager::new(DigestType::Dummy);
    let mut lac_meta = [0u8; 16];
    lac_meta[0..8].copy_from_slice(&1i64.to_be_bytes());
    lac_meta[8..16].copy_from_slice(&0i64.to_be_bytes());
    let digest = dm.compute_lac(&lac_meta);
    assert_eq!(digest.len(), 0, "Dummy LAC digest should be 0 bytes");
    // Total body = 16 + 0 = 16 bytes
}

fn build_metadata(ledger_id: i64, entry_id: i64, lac: i64, length: i64) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[0..8].copy_from_slice(&ledger_id.to_be_bytes());
    buf[8..16].copy_from_slice(&entry_id.to_be_bytes());
    buf[16..24].copy_from_slice(&lac.to_be_bytes());
    buf[24..32].copy_from_slice(&length.to_be_bytes());
    buf
}
