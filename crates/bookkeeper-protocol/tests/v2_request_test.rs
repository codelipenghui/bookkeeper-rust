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
        entry_data: entry_data.clone(), deferred_sync: false,
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
        master_key: None,
    };
    let encoded = encode_request(&req);
    let decoded = decode_request(encoded.freeze()).unwrap();
    match decoded {
        V2Request::BatchReadEntry { ledger_id, request_id, max_count, master_key, .. } => {
            assert_eq!(ledger_id, 1);
            assert_eq!(request_id, 12345);
            assert_eq!(max_count, 100);
            assert!(master_key.is_none(), "no fencing flag ⇒ no masterKey");
        }
        _ => panic!("expected BatchReadEntry"),
    }
}

#[test]
fn batch_read_with_fencing_round_trips_master_key() {
    use bookkeeper_protocol::constants::{FLAG_DO_FENCING, MASTER_KEY_LENGTH};
    let mk = [42u8; MASTER_KEY_LENGTH];
    let req = V2Request::BatchReadEntry {
        flags: FLAG_DO_FENCING,
        ledger_id: 7,
        start_entry_id: 3,
        request_id: 99,
        max_count: 10,
        max_size: 65536,
        master_key: Some(mk),
    };
    let encoded = encode_request(&req);
    let decoded = decode_request(encoded.freeze()).unwrap();
    match decoded {
        V2Request::BatchReadEntry { flags, master_key, .. } => {
            assert_eq!(flags & FLAG_DO_FENCING, FLAG_DO_FENCING);
            assert_eq!(master_key, Some(mk),
                "fenced batch-read must carry the 20-byte masterKey through the wire");
        }
        _ => panic!("expected BatchReadEntry"),
    }
}

// ---------------------------------------------------------------------------
// V2 Auth protobuf AuthMessage wire format (P2)
// ---------------------------------------------------------------------------

/// V2 Auth request encodes payload as protobuf AuthMessage.
/// Matches Java's BookieProtoEncoding.RequestEnDeCoderPreV3 for AUTH.
#[test]
fn auth_request_encodes_as_protobuf_auth_message() {
    let req = V2Request::Auth { payload: Bytes::from_static(b"my-token") };
    let encoded = encode_request(&req);
    let decoded = decode_request(encoded.freeze()).unwrap();
    match decoded {
        V2Request::Auth { payload } => {
            assert_eq!(payload.as_ref(), b"my-token");
        }
        _ => panic!("expected Auth"),
    }
}

/// V2 Auth response encodes payload as protobuf AuthMessage (not standard header).
/// Matches Java's BookieProtoEncoding.ResponseEnDeCoderPreV3.
#[test]
fn auth_response_java_compatible_format() {
    use bookkeeper_protocol::v2::response::{V2Response, encode_response, decode_response};
    let resp = V2Response::Auth { error_code: 0, payload: Bytes::from_static(b"auth-reply") };
    let encoded = encode_response(&resp);
    let decoded = decode_response(encoded.freeze()).unwrap();
    match decoded {
        V2Response::Auth { payload, .. } => {
            assert_eq!(payload.as_ref(), b"auth-reply");
        }
        _ => panic!("expected Auth response"),
    }
}

/// A `ReadEntry` request that advertises FLAG_DO_FENCING but whose
/// payload is truncated before the 20-byte masterKey must be rejected
/// by the decoder. The previous decoder silently swallowed the
/// truncation, returning `master_key: None`, which the handler treats
/// as "no fencing required" — letting the read through *unauthorized*.
#[test]
fn read_entry_with_fencing_flag_but_no_master_key_is_rejected() {
    use bookkeeper_protocol::constants::{FLAG_DO_FENCING, OP_READ_ENTRY};
    use bookkeeper_protocol::v2::header::PacketHeader;
    use bookkeeper_protocol::v2::request::{decode_request, RequestDecodeError};
    use bytes::BufMut;

    // Build a ReadEntry with FLAG_DO_FENCING set but no master_key bytes
    // appended after ledger_id + entry_id.
    let header = PacketHeader::new(2, OP_READ_ENTRY, FLAG_DO_FENCING);
    let mut buf = bytes::BytesMut::new();
    buf.put_u32(header.to_u32());
    buf.put_i64(123); // ledger_id
    buf.put_i64(0);   // entry_id
    // (no master_key)

    let result = decode_request(buf.freeze());
    assert!(
        matches!(result, Err(RequestDecodeError::TooShort)),
        "FLAG_DO_FENCING with no master_key bytes must error \
         with TooShort, got {result:?} — silently accepting it would \
         let the read bypass the fencing check",
    );
}

/// Same invariant for `BatchReadEntry`.
#[test]
fn batch_read_with_fencing_flag_but_no_master_key_is_rejected() {
    use bookkeeper_protocol::constants::{FLAG_DO_FENCING, OP_BATCH_READ_ENTRY};
    use bookkeeper_protocol::v2::header::PacketHeader;
    use bookkeeper_protocol::v2::request::{decode_request, RequestDecodeError};
    use bytes::BufMut;

    let header = PacketHeader::new(2, OP_BATCH_READ_ENTRY, FLAG_DO_FENCING);
    let mut buf = bytes::BytesMut::new();
    buf.put_u32(header.to_u32());
    buf.put_i64(7);     // ledger_id
    buf.put_i64(0);     // start_entry_id
    buf.put_i64(0);     // request_id
    buf.put_i32(10);    // max_count
    buf.put_i64(65536); // max_size
    // (no master_key)

    let result = decode_request(buf.freeze());
    assert!(
        matches!(result, Err(RequestDecodeError::TooShort)),
        "FLAG_DO_FENCING with no master_key bytes must error \
         with TooShort, got {result:?}",
    );
}
