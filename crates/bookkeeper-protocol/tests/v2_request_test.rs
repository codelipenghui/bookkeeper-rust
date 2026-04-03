use bookkeeper_protocol::v2::request::{V2Request, encode_request, decode_request};
use bytes::Bytes;

#[test]
fn add_entry_round_trip() {
    let req = V2Request::AddEntry { flags: 0, master_key: [0u8; 20], ledger_id: 42, entry_id: 7, entry_data: Bytes::from_static(b"test entry data") };
    let encoded = encode_request(&req);
    let decoded = decode_request(&encoded).unwrap();
    match decoded {
        V2Request::AddEntry { ledger_id, entry_id, entry_data, .. } => {
            assert_eq!(ledger_id, 42); assert_eq!(entry_id, 7); assert_eq!(entry_data, &b"test entry data"[..]);
        }
        _ => panic!("expected AddEntry"),
    }
}

#[test]
fn read_entry_with_fencing() {
    let req = V2Request::ReadEntry { flags: 0x0001, ledger_id: 100, entry_id: 50, master_key: Some([1u8; 20]) };
    let encoded = encode_request(&req);
    let decoded = decode_request(&encoded).unwrap();
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
    let decoded = decode_request(&encoded).unwrap();
    match decoded { V2Request::ReadEntry { master_key, .. } => assert!(master_key.is_none()), _ => panic!() }
}

#[test]
fn get_bookie_info_round_trip() {
    let req = V2Request::GetBookieInfo { flags: 0, requested: 0x03 };
    let encoded = encode_request(&req);
    let decoded = decode_request(&encoded).unwrap();
    match decoded { V2Request::GetBookieInfo { requested, .. } => assert_eq!(requested, 0x03), _ => panic!() }
}
