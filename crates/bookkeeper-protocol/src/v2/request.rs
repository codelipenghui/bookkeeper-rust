use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::constants::*;
use crate::v2::header::PacketHeader;

#[derive(Debug, Clone)]
pub enum V2Request {
    AddEntry { flags: u16, master_key: [u8; MASTER_KEY_LENGTH], ledger_id: i64, entry_id: i64, entry_data: Bytes },
    ReadEntry { flags: u16, ledger_id: i64, entry_id: i64, master_key: Option<[u8; MASTER_KEY_LENGTH]> },
    Auth { payload: Bytes },
    WriteLac { ledger_id: i64, lac: i64, master_key: [u8; MASTER_KEY_LENGTH], body: Bytes },
    ReadLac { ledger_id: i64 },
    GetBookieInfo { flags: u16, requested: i64 },
    BatchReadEntry { flags: u16, ledger_id: i64, start_entry_id: i64, max_count: i32, max_size: i64 },
}

pub fn encode_request(req: &V2Request) -> BytesMut {
    match req {
        V2Request::AddEntry { flags, master_key, ledger_id, entry_id, entry_data } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_ADD_ENTRY, *flags);
            let mut buf = BytesMut::with_capacity(4 + MASTER_KEY_LENGTH + 16 + entry_data.len());
            buf.put_u32(header.to_u32());
            buf.put_slice(master_key);
            buf.put_i64(*ledger_id);
            buf.put_i64(*entry_id);
            buf.put_slice(entry_data);
            buf
        }
        V2Request::ReadEntry { flags, ledger_id, entry_id, master_key } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_READ_ENTRY, *flags);
            let mut buf = BytesMut::with_capacity(4 + 16 + if master_key.is_some() { MASTER_KEY_LENGTH } else { 0 });
            buf.put_u32(header.to_u32());
            buf.put_i64(*ledger_id);
            buf.put_i64(*entry_id);
            if let Some(key) = master_key { buf.put_slice(key); }
            buf
        }
        V2Request::Auth { payload } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_AUTH, FLAG_NONE);
            let mut buf = BytesMut::with_capacity(4 + payload.len());
            buf.put_u32(header.to_u32());
            buf.put_slice(payload);
            buf
        }
        V2Request::WriteLac { ledger_id, lac, master_key, body } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_WRITE_LAC, FLAG_NONE);
            let mut buf = BytesMut::with_capacity(4 + 16 + MASTER_KEY_LENGTH + body.len());
            buf.put_u32(header.to_u32());
            buf.put_i64(*ledger_id);
            buf.put_i64(*lac);
            buf.put_slice(master_key);
            buf.put_slice(body);
            buf
        }
        V2Request::ReadLac { ledger_id } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_READ_LAC, FLAG_NONE);
            let mut buf = BytesMut::with_capacity(12);
            buf.put_u32(header.to_u32());
            buf.put_i64(*ledger_id);
            buf
        }
        V2Request::GetBookieInfo { flags, requested } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_GET_BOOKIE_INFO, *flags);
            let mut buf = BytesMut::with_capacity(12);
            buf.put_u32(header.to_u32());
            buf.put_i64(*requested);
            buf
        }
        V2Request::BatchReadEntry { flags, ledger_id, start_entry_id, max_count, max_size } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_BATCH_READ_ENTRY, *flags);
            let mut buf = BytesMut::with_capacity(32);
            buf.put_u32(header.to_u32());
            buf.put_i64(*ledger_id);
            buf.put_i64(*start_entry_id);
            buf.put_i32(*max_count);
            buf.put_i64(*max_size);
            buf
        }
    }
}

pub fn decode_request(data: &[u8]) -> Result<V2Request, RequestDecodeError> {
    if data.len() < 4 { return Err(RequestDecodeError::TooShort); }
    let mut buf = &data[..];
    let header = PacketHeader::from_u32(buf.get_u32());
    let flags = header.flags();
    match header.opcode() {
        OP_ADD_ENTRY => {
            if buf.remaining() < MASTER_KEY_LENGTH + 16 { return Err(RequestDecodeError::TooShort); }
            let mut master_key = [0u8; MASTER_KEY_LENGTH];
            buf.copy_to_slice(&mut master_key);
            let ledger_id = buf.get_i64();
            let entry_id = buf.get_i64();
            let entry_data = Bytes::copy_from_slice(buf);
            Ok(V2Request::AddEntry { flags, master_key, ledger_id, entry_id, entry_data })
        }
        OP_READ_ENTRY => {
            if buf.remaining() < 16 { return Err(RequestDecodeError::TooShort); }
            let ledger_id = buf.get_i64();
            let entry_id = buf.get_i64();
            let master_key = if header.has_flag(FLAG_DO_FENCING) && buf.remaining() >= MASTER_KEY_LENGTH {
                let mut key = [0u8; MASTER_KEY_LENGTH];
                buf.copy_to_slice(&mut key);
                Some(key)
            } else { None };
            Ok(V2Request::ReadEntry { flags, ledger_id, entry_id, master_key })
        }
        OP_AUTH => {
            Ok(V2Request::Auth { payload: Bytes::copy_from_slice(buf) })
        }
        OP_WRITE_LAC => {
            if buf.remaining() < 16 + MASTER_KEY_LENGTH { return Err(RequestDecodeError::TooShort); }
            let ledger_id = buf.get_i64();
            let lac = buf.get_i64();
            let mut master_key = [0u8; MASTER_KEY_LENGTH];
            buf.copy_to_slice(&mut master_key);
            let body = Bytes::copy_from_slice(buf);
            Ok(V2Request::WriteLac { ledger_id, lac, master_key, body })
        }
        OP_READ_LAC => {
            if buf.remaining() < 8 { return Err(RequestDecodeError::TooShort); }
            Ok(V2Request::ReadLac { ledger_id: buf.get_i64() })
        }
        OP_GET_BOOKIE_INFO => {
            if buf.remaining() < 8 { return Err(RequestDecodeError::TooShort); }
            Ok(V2Request::GetBookieInfo { flags, requested: buf.get_i64() })
        }
        OP_BATCH_READ_ENTRY => {
            if buf.remaining() < 28 { return Err(RequestDecodeError::TooShort); }
            let ledger_id = buf.get_i64();
            let start_entry_id = buf.get_i64();
            let max_count = buf.get_i32();
            let max_size = buf.get_i64();
            Ok(V2Request::BatchReadEntry { flags, ledger_id, start_entry_id, max_count, max_size })
        }
        op => Err(RequestDecodeError::UnknownOpcode(op)),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RequestDecodeError {
    #[error("message too short")]
    TooShort,
    #[error("unknown opcode: {0}")]
    UnknownOpcode(u8),
}
