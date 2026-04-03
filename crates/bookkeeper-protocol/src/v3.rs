use bytes::BytesMut;
use prost::Message;
use bookkeeper_proto::{Request, Response};

pub fn encode_v3_request(req: &Request) -> BytesMut {
    let len = req.encoded_len();
    let mut buf = BytesMut::with_capacity(len);
    req.encode(&mut buf).expect("encode cannot fail with sufficient capacity");
    buf
}

pub fn decode_v3_request(data: &[u8]) -> Result<Request, prost::DecodeError> {
    Request::decode(data)
}

pub fn encode_v3_response(resp: &Response) -> BytesMut {
    let len = resp.encoded_len();
    let mut buf = BytesMut::with_capacity(len);
    resp.encode(&mut buf).expect("encode cannot fail with sufficient capacity");
    buf
}

pub fn decode_v3_response(data: &[u8]) -> Result<Response, prost::DecodeError> {
    Response::decode(data)
}
