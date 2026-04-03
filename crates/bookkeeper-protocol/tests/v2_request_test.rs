use bookkeeper_protocol::v2::request::{V2Request, encode_request, decode_request};
use bytes::Bytes;

#[test]
fn add_entry_round_trip() {
    // entry_data is the FULL entry bytes (ledgerId + entryId + lac + length + ...)
    let mut entry_data = Vec::new();
    entry_data.extend_from_slice(&42i64.to_be_bytes());  // ledgerId
    entry_data.extend_from_slice(&7i64.to_be_bytes());   // entryId
    entry_data.extend_from_slice(&(-1i64).to_be_bytes()); // lac
    entry_data.extend_from_slice(&15i64.to_be_bytes());  // length
    entry_data.extend_from_slice(b"test entry data");     // payload (no digest for simplicity)
    let entry_data = Bytes::from(entry_data);

    let req = V2Request::AddEntry {
        flags: 0,
        master_key: [0u8; 20],
        entry_data: entry_data.clone(),
    };

    let encoded = encode_request(&req);
    let decoded = decode_request(encoded.freeze()).unwrap();

    match &decoded {
        V2Request::AddEntry { entry_data: data, .. } => {
            assert_eq!(*data, entry_data);
        }
        _ => panic!("expected AddEntry"),
    }
    // Test helper methods
    assert_eq!(decoded.ledger_id(), Some(42));
    assert_eq!(decoded.entry_id(), Some(7));
}

#[test]
fn read_entry_with_fencing() {
    let req = V2Request::ReadEntry { flags: 0x0001, ledger_id: 100, entry_id: 50, master_key: Some([1u8; 20]) };
    let encoded = encode_request(&req);
    let decoded = decode_request(encoded.freeze()).unwrap();
    match decoded {
        V2Request::ReadEntry { flags, ledger_id, entry_id, master_key } => {
            assert_eq!(flags, 0x0001); assert_eq!(ledger_id, 100); assert_eq!(entry_id, 50); assert!(master_key.is_some());
        }
        _ => panic!("expected ReadEntry"),
    }
}

#[test]
fn read_entry_no_fencing() {
    let req = V2Request::ReadEntry { flags: 0, ledger_id: 10, entry_id: 0, master_key: None };
    let encoded = encode_request(&req);
    let decoded = decode_request(encoded.freeze()).unwrap();
    match decoded { V2Request::ReadEntry { master_key, .. } => assert!(master_key.is_none()), _ => panic!() }
}

#[test]
fn get_bookie_info_round_trip() {
    let req = V2Request::GetBookieInfo { flags: 0, requested: 0x03 };
    let encoded = encode_request(&req);
    let decoded = decode_request(encoded.freeze()).unwrap();
    match decoded { V2Request::GetBookieInfo { requested, .. } => assert_eq!(requested, 0x03), _ => panic!() }
}

#[test]
fn batch_read_round_trip() {
    let req = V2Request::BatchReadEntry {
        flags: 0,
        ledger_id: 1,
        start_entry_id: 0,
        request_id: 12345,
        max_count: 100,
        max_size: 1048576,
    };
    let encoded = encode_request(&req);
    let decoded = decode_request(encoded.freeze()).unwrap();
    match decoded {
        V2Request::BatchReadEntry { ledger_id, request_id, max_count, .. } => {
            assert_eq!(ledger_id, 1);
            assert_eq!(request_id, 12345);
            assert_eq!(max_count, 100);
        }
        _ => panic!("expected BatchReadEntry"),
    }
}
