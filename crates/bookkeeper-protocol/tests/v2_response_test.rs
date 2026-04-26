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

/// V2 length-prefixed payload decoders cast `i32` lengths to `usize`
/// before any range check. A negative length on the wire becomes a
/// huge unsigned value, the `pos + len <= data.len()` check fails,
/// and the affected field silently becomes `None` instead of
/// erroring. Recovery — which trusts these fields — then treats the
/// payload as absent rather than corrupt and a malicious or buggy
/// peer can mask data without the client noticing.
#[test]
fn read_lac_with_negative_length_errors() {
    use bookkeeper_protocol::constants::{OP_READ_LAC, V2_EOK};
    use bookkeeper_protocol::v2::header::PacketHeader;
    use bookkeeper_protocol::v2::response::ResponseDecodeError;
    use bytes::BufMut;

    // Build a valid response header + lac_len = -1 (i32) and nothing more.
    let header = PacketHeader::new(2, OP_READ_LAC, 0);
    let mut buf = bytes::BytesMut::new();
    buf.put_u32(header.to_u32());
    buf.put_i32(V2_EOK);
    buf.put_i64(7);   // ledger_id
    buf.put_i64(0);   // entry_id
    buf.put_i32(-1);  // lac_len: negative — must NOT silently become None

    let result = decode_response(buf.freeze());
    assert!(
        matches!(result, Err(ResponseDecodeError::TooShort | ResponseDecodeError::Malformed(_))),
        "decode_response must reject a negative lac_len, got {result:?}",
    );
}

#[test]
fn batch_read_with_negative_entry_size_errors() {
    use bookkeeper_protocol::constants::{OP_BATCH_READ_ENTRY, V2_EOK};
    use bookkeeper_protocol::v2::header::PacketHeader;
    use bookkeeper_protocol::v2::response::ResponseDecodeError;
    use bytes::BufMut;

    let header = PacketHeader::new(2, OP_BATCH_READ_ENTRY, 0);
    let mut buf = bytes::BytesMut::new();
    buf.put_u32(header.to_u32());
    buf.put_i32(V2_EOK);
    buf.put_i64(1);   // ledger_id
    buf.put_i64(0);   // entry_id
    buf.put_i64(99);  // request_id
    buf.put_i32(-1);  // entry size: negative

    let result = decode_response(buf.freeze());
    assert!(
        matches!(result, Err(ResponseDecodeError::TooShort | ResponseDecodeError::Malformed(_))),
        "decode_response must reject a negative entry size in BatchRead, got {result:?}",
    );
}
