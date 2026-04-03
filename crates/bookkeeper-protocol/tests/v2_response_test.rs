use bookkeeper_protocol::v2::response::{V2Response, encode_response, decode_response};
use bytes::Bytes;

#[test]
fn add_response_round_trip() {
    let resp = V2Response::Add { error_code: 0, ledger_id: 42, entry_id: 7 };
    let encoded = encode_response(&resp);
    assert_eq!(encoded.len(), 24);
    let decoded = decode_response(encoded.freeze()).unwrap();
    match decoded { V2Response::Add { error_code, ledger_id, entry_id } => { assert_eq!(error_code, 0); assert_eq!(ledger_id, 42); assert_eq!(entry_id, 7); } _ => panic!() }
}

#[test]
fn read_response_with_data() {
    let data = Bytes::from_static(b"entry data here");
    let resp = V2Response::Read { error_code: 0, ledger_id: 1, entry_id: 0, data: Some(data.clone()) };
    let encoded = encode_response(&resp);
    let decoded = decode_response(encoded.freeze()).unwrap();
    match decoded { V2Response::Read { error_code, data: d, .. } => { assert_eq!(error_code, 0); assert_eq!(d.unwrap(), data); } _ => panic!() }
}

#[test]
fn read_response_error() {
    let resp = V2Response::Read { error_code: 2, ledger_id: 1, entry_id: 99, data: None };
    let encoded = encode_response(&resp);
    let decoded = decode_response(encoded.freeze()).unwrap();
    match decoded { V2Response::Read { error_code, data, .. } => { assert_eq!(error_code, 2); assert!(data.is_none()); } _ => panic!() }
}
