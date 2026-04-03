use bytes::BytesMut;
use prost::Message;
use bookkeeper_proto::{Request, Response};

fn encode_proto<M: Message>(msg: &M) -> BytesMut {
    let mut buf = BytesMut::with_capacity(msg.encoded_len());
    msg.encode(&mut buf).expect("encode cannot fail with sufficient capacity");
    buf
}

pub fn encode_v3_request(req: &Request) -> BytesMut { encode_proto(req) }
pub fn decode_v3_request(data: &[u8]) -> Result<Request, prost::DecodeError> { Request::decode(data) }
pub fn encode_v3_response(resp: &Response) -> BytesMut { encode_proto(resp) }
pub fn decode_v3_response(data: &[u8]) -> Result<Response, prost::DecodeError> { Response::decode(data) }
