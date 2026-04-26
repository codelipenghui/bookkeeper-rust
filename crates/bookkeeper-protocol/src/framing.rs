use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// Hard upper bound on `max_frame_size` regardless of caller input.
/// 128 MB is well above any sane bookie message and prevents a
/// misconfiguration / hostile peer from coaxing the codec into
/// reserving up to `usize::MAX` per connection.
const ABSOLUTE_MAX_FRAME_SIZE: usize = 128 * 1024 * 1024;

/// Cap how much we'll reserve per partial-frame growth. The decoder
/// will still complete the frame — `reserve` is amortized — but we
/// won't grow the buffer to the full advertised length until we have
/// actually received that many bytes.
const RESERVE_CHUNK: usize = 1024 * 1024;

pub struct BookieCodec {
    max_frame_size: usize,
}

impl BookieCodec {
    pub fn new(max_frame_size: usize) -> Self {
        Self {
            max_frame_size: max_frame_size.min(ABSOLUTE_MAX_FRAME_SIZE),
        }
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
            // Grow the buffer in bounded chunks rather than reserving the
            // entire advertised length up front. A connection that sends
            // a 4-byte header claiming a huge length but never the body
            // would otherwise pin `length` bytes of capacity per
            // connection — multiplied by the connection limit, an OOM.
            let needed = 4 + length - src.len();
            let chunk = needed.min(RESERVE_CHUNK);
            src.reserve(chunk);
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
