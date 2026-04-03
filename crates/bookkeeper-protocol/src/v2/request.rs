use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::constants::*;
use crate::v2::header::PacketHeader;

#[derive(Debug, Clone)]
pub enum V2Request {
    AddEntry { flags: u16, master_key: [u8; MASTER_KEY_LENGTH], entry_data: Bytes },
    ReadEntry { flags: u16, ledger_id: i64, entry_id: i64, master_key: Option<[u8; MASTER_KEY_LENGTH]> },
    Auth { payload: Bytes },
    WriteLac { ledger_id: i64, lac: i64, master_key: [u8; MASTER_KEY_LENGTH], body: Bytes },
    ReadLac { ledger_id: i64 },
    GetBookieInfo { flags: u16, requested: i64 },
    BatchReadEntry { flags: u16, ledger_id: i64, start_entry_id: i64, request_id: i64, max_count: i32, max_size: i64 },
}

impl V2Request {
    /// For AddEntry: peek at ledgerId from the entry data (first 8 bytes).
    pub fn ledger_id(&self) -> Option<i64> {
        match self {
            V2Request::AddEntry { entry_data, .. } if entry_data.len() >= 8 => {
                Some(i64::from_be_bytes(entry_data[0..8].try_into().unwrap()))
            }
            V2Request::AddEntry { .. } => None,
            V2Request::ReadEntry { ledger_id, .. } => Some(*ledger_id),
            V2Request::WriteLac { ledger_id, .. } => Some(*ledger_id),
            V2Request::ReadLac { ledger_id, .. } => Some(*ledger_id),
            V2Request::GetBookieInfo { .. } => None,
            V2Request::BatchReadEntry { ledger_id, .. } => Some(*ledger_id),
            V2Request::Auth { .. } => None,
        }
    }

    /// For AddEntry: peek at entryId from the entry data (bytes 8..16).
    pub fn entry_id(&self) -> Option<i64> {
        match self {
            V2Request::AddEntry { entry_data, .. } if entry_data.len() >= 16 => {
                Some(i64::from_be_bytes(entry_data[8..16].try_into().unwrap()))
            }
            V2Request::AddEntry { .. } => None,
            V2Request::ReadEntry { entry_id, .. } => Some(*entry_id),
            V2Request::WriteLac { .. } => None,
            V2Request::ReadLac { .. } => None,
            V2Request::GetBookieInfo { .. } => None,
            V2Request::BatchReadEntry { start_entry_id, .. } => Some(*start_entry_id),
            V2Request::Auth { .. } => None,
        }
    }
}

pub fn encode_request(req: &V2Request) -> BytesMut {
    match req {
        V2Request::AddEntry { flags, master_key, entry_data } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_ADD_ENTRY, *flags);
            let mut buf = BytesMut::with_capacity(4 + MASTER_KEY_LENGTH + entry_data.len());
            buf.put_u32(header.to_u32());
            buf.put_slice(master_key);
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
        V2Request::BatchReadEntry { flags, ledger_id, start_entry_id, request_id, max_count, max_size } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_BATCH_READ_ENTRY, *flags);
            let mut buf = BytesMut::with_capacity(4 + 8 + 8 + 8 + 4 + 8);
            buf.put_u32(header.to_u32());
            buf.put_i64(*ledger_id);
            buf.put_i64(*start_entry_id);
            buf.put_i64(*request_id);
            buf.put_i32(*max_count);
            buf.put_i64(*max_size);
            buf
        }
    }
}

pub fn decode_request(data: Bytes) -> Result<V2Request, RequestDecodeError> {
    if data.len() < 4 { return Err(RequestDecodeError::TooShort); }
    let mut buf = &data[..];
    let header = PacketHeader::from_u32(buf.get_u32());
    let flags = header.flags();
    match header.opcode() {
        OP_ADD_ENTRY => {
            if data.len() < 4 + MASTER_KEY_LENGTH { return Err(RequestDecodeError::TooShort); }
            let mut master_key = [0u8; MASTER_KEY_LENGTH];
            master_key.copy_from_slice(&data[4..4 + MASTER_KEY_LENGTH]);
            let entry_data = data.slice(4 + MASTER_KEY_LENGTH..);
            Ok(V2Request::AddEntry { flags, master_key, entry_data })
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
            let payload = data.slice(4..);
            Ok(V2Request::Auth { payload })
        }
        OP_WRITE_LAC => {
            if data.len() < 4 + 16 + MASTER_KEY_LENGTH { return Err(RequestDecodeError::TooShort); }
            let mut cursor = &data[4..];
            let ledger_id = cursor.get_i64();
            let lac = cursor.get_i64();
            let mut master_key = [0u8; MASTER_KEY_LENGTH];
            master_key.copy_from_slice(&data[20..20 + MASTER_KEY_LENGTH]);
            let body = data.slice(4 + 16 + MASTER_KEY_LENGTH..);
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
            if buf.remaining() < 36 { return Err(RequestDecodeError::TooShort); }
            let ledger_id = buf.get_i64();
            let start_entry_id = buf.get_i64();
            let request_id = buf.get_i64();
            let max_count = buf.get_i32();
            let max_size = buf.get_i64();
            Ok(V2Request::BatchReadEntry { flags, ledger_id, start_entry_id, request_id, max_count, max_size })
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
