use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::constants::*;
use crate::v2::header::PacketHeader;

#[derive(Debug, Clone)]
pub enum V2Response {
    Add { error_code: i32, ledger_id: i64, entry_id: i64 },
    Read { error_code: i32, ledger_id: i64, entry_id: i64, data: Option<Bytes> },
    Auth { error_code: i32, payload: Bytes },
    WriteLac { error_code: i32, ledger_id: i64, entry_id: i64 },
    ReadLac { error_code: i32, ledger_id: i64, entry_id: i64, lac_body: Option<Bytes>, last_entry_body: Option<Bytes> },
    GetBookieInfo { error_code: i32, ledger_id: i64, entry_id: i64, free_disk_space: i64, total_disk_capacity: i64 },
    BatchRead { error_code: i32, ledger_id: i64, entry_id: i64, request_id: i64, entries: Vec<Bytes> },
}

fn encode_response_header(opcode: u8, error_code: i32, ledger_id: i64, entry_id: i64, extra_capacity: usize) -> BytesMut {
    let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, opcode, FLAG_NONE);
    let mut buf = BytesMut::with_capacity(V2_RESPONSE_HEADER_SIZE + extra_capacity);
    buf.put_u32(header.to_u32());
    buf.put_i32(error_code);
    buf.put_i64(ledger_id);
    buf.put_i64(entry_id);
    buf
}

pub fn encode_response(resp: &V2Response) -> BytesMut {
    match resp {
        V2Response::Add { error_code, ledger_id, entry_id } => {
            encode_response_header(OP_ADD_ENTRY, *error_code, *ledger_id, *entry_id, 0)
        }
        V2Response::Read { error_code, ledger_id, entry_id, data } => {
            let data_len = data.as_ref().map_or(0, |d| d.len());
            let mut buf = encode_response_header(OP_READ_ENTRY, *error_code, *ledger_id, *entry_id, data_len);
            if let Some(d) = data { buf.put_slice(d); }
            buf
        }
        V2Response::Auth { error_code, payload } => {
            let mut buf = encode_response_header(OP_AUTH, *error_code, 0, 0, payload.len());
            buf.put_slice(payload);
            buf
        }
        V2Response::WriteLac { error_code, ledger_id, entry_id } => {
            encode_response_header(OP_WRITE_LAC, *error_code, *ledger_id, *entry_id, 0)
        }
        V2Response::ReadLac { error_code, ledger_id, entry_id, lac_body, last_entry_body } => {
            let lac_len = lac_body.as_ref().map_or(0, |b| b.len());
            let entry_len = last_entry_body.as_ref().map_or(0, |b| b.len());
            let mut buf = encode_response_header(OP_READ_LAC, *error_code, *ledger_id, *entry_id, lac_len + entry_len);
            if let Some(b) = lac_body { buf.put_slice(b); }
            if let Some(b) = last_entry_body { buf.put_slice(b); }
            buf
        }
        V2Response::GetBookieInfo { error_code, ledger_id, entry_id, free_disk_space, total_disk_capacity } => {
            let mut buf = encode_response_header(OP_GET_BOOKIE_INFO, *error_code, *ledger_id, *entry_id, 16);
            buf.put_i64(*free_disk_space);
            buf.put_i64(*total_disk_capacity);
            buf
        }
        V2Response::BatchRead { error_code, ledger_id, entry_id, request_id, entries } => {
            let entries_size: usize = entries.iter().map(|e| 4 + e.len()).sum();
            let mut buf = encode_response_header(OP_BATCH_READ_ENTRY, *error_code, *ledger_id, *entry_id, 8 + entries_size);
            buf.put_i64(*request_id);
            for entry in entries { buf.put_i32(entry.len() as i32); buf.put_slice(entry); }
            buf
        }
    }
}

pub fn decode_response(data: Bytes) -> Result<V2Response, ResponseDecodeError> {
    if data.len() < V2_RESPONSE_HEADER_SIZE { return Err(ResponseDecodeError::TooShort); }
    let mut buf = &data[..];
    let header = PacketHeader::from_u32(buf.get_u32());
    let error_code = buf.get_i32();
    let ledger_id = buf.get_i64();
    let entry_id = buf.get_i64();
    match header.opcode() {
        OP_ADD_ENTRY => Ok(V2Response::Add { error_code, ledger_id, entry_id }),
        OP_READ_ENTRY => {
            let body = if error_code == V2_EOK && data.len() > V2_RESPONSE_HEADER_SIZE {
                Some(data.slice(V2_RESPONSE_HEADER_SIZE..))
            } else {
                None
            };
            Ok(V2Response::Read { error_code, ledger_id, entry_id, data: body })
        }
        OP_AUTH => Ok(V2Response::Auth { error_code, payload: data.slice(V2_RESPONSE_HEADER_SIZE..) }),
        OP_WRITE_LAC => Ok(V2Response::WriteLac { error_code, ledger_id, entry_id }),
        OP_READ_LAC => {
            let lac_body = if data.len() > V2_RESPONSE_HEADER_SIZE {
                Some(data.slice(V2_RESPONSE_HEADER_SIZE..))
            } else {
                None
            };
            Ok(V2Response::ReadLac { error_code, ledger_id, entry_id, lac_body, last_entry_body: None })
        }
        OP_GET_BOOKIE_INFO => {
            if buf.remaining() < 16 { return Err(ResponseDecodeError::TooShort); }
            let free_disk_space = buf.get_i64();
            let total_disk_capacity = buf.get_i64();
            Ok(V2Response::GetBookieInfo { error_code, ledger_id, entry_id, free_disk_space, total_disk_capacity })
        }
        OP_BATCH_READ_ENTRY => {
            if buf.remaining() < 8 { return Err(ResponseDecodeError::TooShort); }
            let request_id = buf.get_i64();
            let mut offset = V2_RESPONSE_HEADER_SIZE + 8;
            let mut entries = Vec::new();
            while offset + 4 <= data.len() {
                let size = i32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + size > data.len() { break; }
                entries.push(data.slice(offset..offset + size));
                offset += size;
            }
            Ok(V2Response::BatchRead { error_code, ledger_id, entry_id, request_id, entries })
        }
        op => Err(ResponseDecodeError::UnknownOpcode(op)),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ResponseDecodeError {
    #[error("message too short")]
    TooShort,
    #[error("unknown opcode: {0}")]
    UnknownOpcode(u8),
}
