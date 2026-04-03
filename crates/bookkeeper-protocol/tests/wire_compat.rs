use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::v2::header::PacketHeader;
use bookkeeper_protocol::v2::request::{V2Request, encode_request, decode_request};
use bookkeeper_protocol::v2::response::{V2Response, encode_response, decode_response};
use bookkeeper_protocol::framing::BookieCodec;
use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_protocol::entry::{EntryBuilder, parse_entry};
use bytes::{Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[test]
fn packet_header_java_compat() {
    assert_eq!(PacketHeader::new(2, 1, 0).to_u32(), 0x02010000);
    assert_eq!(PacketHeader::new(2, 2, 0x0001).to_u32(), 0x02020001);
    assert_eq!(PacketHeader::new(2, 1, 0x0002).to_u32(), 0x02010002);
}

#[test]
fn add_entry_request_layout() {
    let master_key = [0xABu8; 20];
    let entry_data = Bytes::from_static(b"DATA");
    let req = V2Request::AddEntry {
        flags: FLAG_RECOVERY_ADD,
        master_key,
        ledger_id: 1,
        entry_id: 0,
        entry_data: entry_data.clone(),
    };
    let buf = encode_request(&req);
    assert_eq!(buf.len(), 44);
    let header_val = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    assert_eq!(header_val, 0x02010002);
    assert_eq!(&buf[4..24], &[0xAB; 20]);
    assert_eq!(i64::from_be_bytes(buf[24..32].try_into().unwrap()), 1);
    assert_eq!(i64::from_be_bytes(buf[32..40].try_into().unwrap()), 0);
    assert_eq!(&buf[40..], b"DATA");
}

#[test]
fn response_header_24_bytes() {
    let resp = V2Response::Add { error_code: 0, ledger_id: 42, entry_id: 7 };
    let buf = encode_response(&resp);
    assert_eq!(buf.len(), 24);
    let error_code = i32::from_be_bytes(buf[4..8].try_into().unwrap());
    assert_eq!(error_code, V2_EOK);
    assert_eq!(i64::from_be_bytes(buf[8..16].try_into().unwrap()), 42);
    assert_eq!(i64::from_be_bytes(buf[16..24].try_into().unwrap()), 7);
}

#[test]
fn entry_format_layout() {
    let dm = DigestManager::new(DigestType::Crc32c);
    let payload = Bytes::from_static(b"test");
    let entry = EntryBuilder::new(&dm)
        .ledger_id(100)
        .entry_id(5)
        .lac(4)
        .length(4)
        .payload(payload)
        .build();
    assert_eq!(entry.len(), 44);
    assert_eq!(i64::from_be_bytes(entry[0..8].try_into().unwrap()), 100);
    assert_eq!(i64::from_be_bytes(entry[8..16].try_into().unwrap()), 5);
    assert_eq!(i64::from_be_bytes(entry[16..24].try_into().unwrap()), 4);
    assert_eq!(i64::from_be_bytes(entry[24..32].try_into().unwrap()), 4);
    assert_eq!(&entry[40..44], b"test");
}

#[test]
fn framing_round_trip() {
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let payload = Bytes::from_static(b"framed payload");
    let mut wire = BytesMut::new();
    codec.encode(payload.clone(), &mut wire).unwrap();
    let decoded = codec.decode(&mut wire).unwrap().unwrap();
    assert_eq!(decoded, payload);
}

#[test]
fn full_stack_add_entry() {
    let dm = DigestManager::new(DigestType::Crc32c);
    let payload = Bytes::from_static(b"hello bookkeeper");
    let entry_bytes = EntryBuilder::new(&dm)
        .ledger_id(1)
        .entry_id(0)
        .lac(-1)
        .length(16)
        .payload(payload)
        .build();
    let req = V2Request::AddEntry {
        flags: 0,
        master_key: [0u8; 20],
        ledger_id: 1,
        entry_id: 0,
        entry_data: entry_bytes.freeze(),
    };
    let req_bytes = encode_request(&req);
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let mut wire = BytesMut::new();
    codec.encode(req_bytes.freeze(), &mut wire).unwrap();
    let frame = codec.decode(&mut wire).unwrap().unwrap();
    let decoded_req = decode_request(&frame).unwrap();
    match decoded_req {
        V2Request::AddEntry { ledger_id, entry_id, entry_data, .. } => {
            assert_eq!(ledger_id, 1);
            assert_eq!(entry_id, 0);
            let entry = parse_entry(&dm, &entry_data).unwrap();
            assert_eq!(entry.ledger_id, 1);
            assert_eq!(entry.payload, &b"hello bookkeeper"[..]);
        }
        _ => panic!("expected AddEntry"),
    }
}
