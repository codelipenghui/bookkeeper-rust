use bookkeeper_storage::entry_log::format::*;

#[test]
fn entry_log_header_is_1024_bytes() {
    let header = EntryLogHeader::new(1);
    let bytes = header.to_bytes();
    assert_eq!(bytes.len(), LOGFILE_HEADER_SIZE);
    assert_eq!(&bytes[0..4], ENTRY_LOG_MAGIC);
    assert_eq!(i32::from_be_bytes(bytes[4..8].try_into().unwrap()), HEADER_V1);
}

#[test]
fn entry_log_header_round_trip() {
    let mut header = EntryLogHeader::new(1);
    header.set_ledgers_map_offset(50000);
    header.set_ledgers_count(5);
    let bytes = header.to_bytes();
    let parsed = EntryLogHeader::from_bytes(&bytes).unwrap();
    assert_eq!(parsed.version(), HEADER_V1);
    assert_eq!(parsed.ledgers_map_offset(), 50000);
    assert_eq!(parsed.ledgers_count(), 5);
}

#[test]
fn entry_log_header_bad_magic() {
    let mut bytes = vec![0u8; LOGFILE_HEADER_SIZE];
    bytes[0..4].copy_from_slice(b"XXXX");
    assert!(EntryLogHeader::from_bytes(&bytes).is_err());
}

#[test]
fn test_encode_entry_log_entry() {
    let payload = b"entry data here";
    let encoded = encode_entry_log_entry(payload);
    assert_eq!(encoded.len(), 4 + payload.len());
    let size = i32::from_be_bytes(encoded[0..4].try_into().unwrap());
    assert_eq!(size as usize, payload.len());
}

#[test]
fn entry_location_encoding() {
    let loc = EntryLocation::new(10, 2048).unwrap();
    let encoded = loc.to_u64();
    assert_eq!(encoded, (10u64 << 32) | 2048);
    let decoded = EntryLocation::from_u64(encoded);
    assert_eq!(decoded.log_id(), 10);
    assert_eq!(decoded.position(), 2048);
}

#[test]
fn ledger_map_batch_round_trip() {
    let mut entries = Vec::new();
    entries.push(LedgerMapEntry { ledger_id: 1, size: 1024 });
    entries.push(LedgerMapEntry { ledger_id: 2, size: 2048 });
    entries.push(LedgerMapEntry { ledger_id: 100, size: 512 });

    let batch_bytes = encode_ledger_map_batch(&entries);
    let parsed = decode_ledger_map_batch(&batch_bytes).unwrap();
    assert_eq!(parsed.len(), 3);
    assert_eq!(parsed[0].ledger_id, 1);
    assert_eq!(parsed[0].size, 1024);
    assert_eq!(parsed[2].ledger_id, 100);
}

#[test]
fn scan_entries_from_entry_log() {
    let mut buf = Vec::new();
    // Entry 1: ledgerId=1, entryId=0, payload
    let entry1 = make_test_entry(1, 0, b"data1");
    buf.extend_from_slice(&(entry1.len() as i32).to_be_bytes());
    buf.extend_from_slice(&entry1);
    // Entry 2: ledgerId=1, entryId=1, payload
    let entry2 = make_test_entry(1, 1, b"data2");
    buf.extend_from_slice(&(entry2.len() as i32).to_be_bytes());
    buf.extend_from_slice(&entry2);

    let entries = scan_entry_log_entries(&buf);
    assert_eq!(entries.len(), 2);
}

fn make_test_entry(ledger_id: i64, entry_id: i64, payload: &[u8]) -> Vec<u8> {
    let mut entry = Vec::new();
    entry.extend_from_slice(&ledger_id.to_be_bytes());
    entry.extend_from_slice(&entry_id.to_be_bytes());
    entry.extend_from_slice(&(-1i64).to_be_bytes()); // lac
    entry.extend_from_slice(&(payload.len() as i64).to_be_bytes()); // length
    entry.extend_from_slice(payload);
    entry
}
