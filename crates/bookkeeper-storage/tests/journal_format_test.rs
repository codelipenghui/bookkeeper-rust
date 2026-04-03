use bookkeeper_storage::journal::format::*;

#[test]
fn journal_header_v6_is_512_bytes() {
    let header = JournalHeader::new(6);
    let bytes = header.to_bytes();
    assert_eq!(bytes.len(), SECTOR_SIZE);
    assert_eq!(&bytes[0..4], JOURNAL_MAGIC);
    assert_eq!(i32::from_be_bytes(bytes[4..8].try_into().unwrap()), 6);
    // Bytes 8..512 should be zero
    assert!(bytes[8..].iter().all(|&b| b == 0));
}

#[test]
fn journal_header_round_trip() {
    let header = JournalHeader::new(6);
    let bytes = header.to_bytes();
    let parsed = JournalHeader::from_bytes(&bytes).unwrap();
    assert_eq!(parsed.version(), 6);
}

#[test]
fn journal_header_bad_magic() {
    let mut bytes = vec![0u8; SECTOR_SIZE];
    bytes[0..4].copy_from_slice(b"XXXX");
    assert!(JournalHeader::from_bytes(&bytes).is_err());
}

#[test]
fn test_encode_journal_entry() {
    let payload = b"hello journal";
    let encoded = encode_journal_entry(payload);
    // 4-byte size prefix + payload
    assert_eq!(encoded.len(), 4 + payload.len());
    let size = i32::from_be_bytes(encoded[0..4].try_into().unwrap());
    assert_eq!(size as usize, payload.len());
    assert_eq!(&encoded[4..], payload);
}

#[test]
fn padding_calculation() {
    // At position 520 with alignment 4096:
    // Next aligned = 4096, padding needed = 4096 - 520 = 3576
    let padding = calculate_padding(520, 4096);
    assert_eq!(padding.len(), 3576);
    // First 4 bytes: PADDING_MASK
    assert_eq!(i32::from_be_bytes(padding[0..4].try_into().unwrap()), PADDING_MASK);
    // Next 4 bytes: padding length
    assert_eq!(i32::from_be_bytes(padding[4..8].try_into().unwrap()), 3576);
    // Rest: zeros
    assert!(padding[8..].iter().all(|&b| b == 0));
}

#[test]
fn padding_minimum_size() {
    // At position 4088, alignment 4096: only 8 bytes to next boundary
    let padding = calculate_padding(4088, 4096);
    assert_eq!(padding.len(), 8);
}

#[test]
fn padding_at_boundary_uses_full_alignment() {
    // At position 4096 (already aligned): pad to NEXT boundary = 4096 bytes
    let padding = calculate_padding(4096, 4096);
    assert_eq!(padding.len(), 4096);
}

#[test]
fn scan_entries_from_buffer() {
    let mut buf = Vec::new();
    // Entry 1: "hello"
    buf.extend_from_slice(&5i32.to_be_bytes());
    buf.extend_from_slice(b"hello");
    // Entry 2: "world!"
    buf.extend_from_slice(&6i32.to_be_bytes());
    buf.extend_from_slice(b"world!");
    // Padding
    let pad = calculate_padding(buf.len() as u64, 4096);
    buf.extend_from_slice(&pad);
    // Entry 3 after padding: "after"
    buf.extend_from_slice(&5i32.to_be_bytes());
    buf.extend_from_slice(b"after");

    let entries = scan_journal_entries(&buf, 6);
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0], b"hello");
    assert_eq!(entries[1], b"world!");
    assert_eq!(entries[2], b"after");
}

#[test]
fn last_mark_round_trip() {
    let mark = LastMark::new(42, 1024);
    let bytes = mark.to_bytes();
    assert_eq!(bytes.len(), 16);
    let parsed = LastMark::from_bytes(&bytes).unwrap();
    assert_eq!(parsed.log_id(), 42);
    assert_eq!(parsed.offset(), 1024);
}

#[test]
fn journal_position_encoding() {
    let pos = JournalPosition::new(5, 1000);
    assert_eq!(pos.to_u64(), (5u64 << 32) | 1000);
    let decoded = JournalPosition::from_u64(pos.to_u64());
    assert_eq!(decoded.file_id(), 5);
    assert_eq!(decoded.offset(), 1000);
}
