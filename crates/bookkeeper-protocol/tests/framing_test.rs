use bookkeeper_protocol::framing::BookieCodec;
use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[test]
fn encode_frame() {
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let payload = Bytes::from_static(b"hello");
    let mut buf = BytesMut::new();
    codec.encode(payload, &mut buf).unwrap();
    assert_eq!(buf.len(), 9);
    assert_eq!(&buf[0..4], &[0, 0, 0, 5]);
    assert_eq!(&buf[4..9], b"hello");
}

#[test]
fn decode_frame() {
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let mut buf = BytesMut::new();
    buf.put_u32(5);
    buf.put_slice(b"world");
    let frame = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame, &b"world"[..]);
    assert!(buf.is_empty());
}

#[test]
fn decode_partial_frame() {
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let mut buf = BytesMut::new();
    buf.put_u32(10);
    buf.put_slice(b"hello");
    let result = codec.decode(&mut buf).unwrap();
    assert!(result.is_none());
}

#[test]
fn decode_frame_too_large() {
    let mut codec = BookieCodec::new(100);
    let mut buf = BytesMut::new();
    buf.put_u32(200);
    assert!(codec.decode(&mut buf).is_err());
}

#[test]
fn decode_multiple_frames() {
    let mut codec = BookieCodec::new(1024);
    let mut buf = BytesMut::new();
    buf.put_u32(3);
    buf.put_slice(b"aaa");
    buf.put_u32(2);
    buf.put_slice(b"bb");
    let f1 = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(f1, &b"aaa"[..]);
    let f2 = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(f2, &b"bb"[..]);
}
