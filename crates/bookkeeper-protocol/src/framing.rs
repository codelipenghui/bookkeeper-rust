use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub struct BookieCodec {
    max_frame_size: usize,
}

impl BookieCodec {
    pub fn new(max_frame_size: usize) -> Self {
        Self { max_frame_size }
    }
}

impl Decoder for BookieCodec {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        if length > self.max_frame_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame size {length} exceeds max {}", self.max_frame_size),
            ));
        }
        if src.len() < 4 + length {
            src.reserve(4 + length - src.len());
            return Ok(None);
        }
        src.advance(4);
        Ok(Some(src.split_to(length)))
    }
}

impl Encoder<Bytes> for BookieCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let len = item.len();
        if len > self.max_frame_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame size {len} exceeds max {}", self.max_frame_size),
            ));
        }
        dst.reserve(4 + len);
        dst.put_u32(len as u32);
        dst.put_slice(&item);
        Ok(())
    }
}
