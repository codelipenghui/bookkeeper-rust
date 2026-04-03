use bookkeeper_protocol::v3::{encode_v3_request, decode_v3_request, encode_v3_response, decode_v3_response};

#[test]
fn v3_add_request_round_trip() {
    let req = bookkeeper_proto::Request {
        header: bookkeeper_proto::BkPacketHeader {
            version: bookkeeper_proto::ProtocolVersion::VersionThree as i32,
            operation: bookkeeper_proto::OperationType::AddEntry as i32,
            txn_id: 1,
            priority: Some(0),
        },
        add_request: Some(bookkeeper_proto::AddRequest {
            ledger_id: 42,
            entry_id: 7,
            master_key: vec![0u8; 20],
            body: b"hello".to_vec(),
            flag: None,
            write_flags: None,
        }),
        ..Default::default()
    };
    let encoded = encode_v3_request(&req);
    let decoded = decode_v3_request(&encoded).unwrap();
    let add = decoded.add_request.unwrap();
    assert_eq!(add.ledger_id, 42);
    assert_eq!(add.entry_id, 7);
    assert_eq!(add.body, b"hello");
}

#[test]
fn v3_read_response_round_trip() {
    let resp = bookkeeper_proto::Response {
        header: bookkeeper_proto::BkPacketHeader {
            version: bookkeeper_proto::ProtocolVersion::VersionThree as i32,
            operation: bookkeeper_proto::OperationType::ReadEntry as i32,
            txn_id: 5,
            priority: Some(0),
        },
        status: bookkeeper_proto::StatusCode::Eok as i32,
        read_response: Some(bookkeeper_proto::ReadResponse {
            status: bookkeeper_proto::StatusCode::Eok as i32,
            ledger_id: 1,
            entry_id: 0,
            body: Some(b"data".to_vec()),
            max_lac: Some(0),
            lac_update_timestamp: None,
        }),
        ..Default::default()
    };
    let encoded = encode_v3_response(&resp);
    let decoded = decode_v3_response(&encoded).unwrap();
    assert_eq!(decoded.status, bookkeeper_proto::StatusCode::Eok as i32);
    let read = decoded.read_response.unwrap();
    assert_eq!(read.body.unwrap(), b"data");
}
