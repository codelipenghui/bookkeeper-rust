use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::constants::*;
use crate::v2::header::PacketHeader;

#[derive(Debug, Clone)]
pub enum V2Request {
    AddEntry { flags: u16, master_key: [u8; MASTER_KEY_LENGTH], entry_data: Bytes, deferred_sync: bool },
    ReadEntry { flags: u16, ledger_id: i64, entry_id: i64, master_key: Option<[u8; MASTER_KEY_LENGTH]> },
    Auth { payload: Bytes },
    WriteLac { ledger_id: i64, lac: i64, master_key: [u8; MASTER_KEY_LENGTH], body: Bytes },
    ReadLac { ledger_id: i64 },
    GetBookieInfo { flags: u16, requested: i64 },
    BatchReadEntry {
        flags: u16,
        ledger_id: i64,
        start_entry_id: i64,
        request_id: i64,
        max_count: i32,
        max_size: i64,
        /// Optional masterKey, present only when FLAG_DO_FENCING is set.
        /// Matches Java's BookieProtoEncoding.RequestEnDeCoderPreV3 which
        /// appends the 20-byte master key after the fixed header when the
        /// fencing flag is set for BATCH_READ_ENTRY.
        master_key: Option<[u8; MASTER_KEY_LENGTH]>,
    },
}

impl V2Request {
    pub fn ledger_id(&self) -> Option<i64> {
        match self {
            V2Request::AddEntry { entry_data, .. } => {
                crate::entry::peek_ids(entry_data).map(|(l, _)| l)
            }
            V2Request::ReadEntry { ledger_id, .. }
            | V2Request::WriteLac { ledger_id, .. }
            | V2Request::ReadLac { ledger_id, .. }
            | V2Request::BatchReadEntry { ledger_id, .. } => Some(*ledger_id),
            V2Request::GetBookieInfo { .. } | V2Request::Auth { .. } => None,
        }
    }

    pub fn entry_id(&self) -> Option<i64> {
        match self {
            V2Request::AddEntry { entry_data, .. } => {
                crate::entry::peek_ids(entry_data).map(|(_, e)| e)
            }
            V2Request::ReadEntry { entry_id, .. } => Some(*entry_id),
            V2Request::BatchReadEntry { start_entry_id, .. } => Some(*start_entry_id),
            V2Request::WriteLac { .. }
            | V2Request::ReadLac { .. }
            | V2Request::GetBookieInfo { .. }
            | V2Request::Auth { .. } => None,
        }
    }
}

pub fn encode_request(req: &V2Request) -> BytesMut {
    match req {
        V2Request::AddEntry { flags, master_key, entry_data, .. } => {
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
            // Java's V2 Auth encoding wraps the payload in a protobuf AuthMessage:
            // [4-byte header][protobuf-serialized AuthMessage{authPluginName, payload}]
            // Matches Java's BookieProtoEncoding.RequestEnDeCoderPreV3 for AUTH.
            use prost::Message;
            let auth_msg = bookkeeper_proto::AuthMessage {
                auth_plugin_name: String::new(),
                payload: payload.to_vec(),
            };
            let proto_bytes = auth_msg.encode_to_vec();
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_AUTH, FLAG_NONE);
            let mut buf = BytesMut::with_capacity(4 + proto_bytes.len());
            buf.put_u32(header.to_u32());
            buf.put_slice(&proto_bytes);
            buf
        }
        // NOTE: WriteLac, ReadLac, and GetBookieInfo are V3-only operations
        // in Java (PerChannelBookieClient always uses protobuf for these).
        // These V2 encodings are dead code — the Rust client sends all three
        // via V3 protobuf in bookie_connection.rs. Kept for completeness of
        // the V2 codec but never used in production.
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
        V2Request::BatchReadEntry {
            flags, ledger_id, start_entry_id, request_id, max_count, max_size, master_key,
        } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_BATCH_READ_ENTRY, *flags);
            let mk_len = if master_key.is_some() { MASTER_KEY_LENGTH } else { 0 };
            let mut buf = BytesMut::with_capacity(4 + 8 + 8 + 8 + 4 + 8 + mk_len);
            buf.put_u32(header.to_u32());
            buf.put_i64(*ledger_id);
            buf.put_i64(*start_entry_id);
            buf.put_i64(*request_id);
            buf.put_i32(*max_count);
            buf.put_i64(*max_size);
            if let Some(key) = master_key {
                buf.put_slice(key);
            }
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
            Ok(V2Request::AddEntry { flags, master_key, entry_data, deferred_sync: false })
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
            // Java's V2 Auth encoding wraps payload in protobuf AuthMessage.
            // Try to decode it; if it fails (e.g., raw bytes from old client),
            // fall back to treating the whole thing as the payload.
            let raw = data.slice(4..);
            let payload = if let Ok(auth_msg) = prost::Message::decode(raw.as_ref() as &[u8]) {
                let auth_msg: bookkeeper_proto::AuthMessage = auth_msg;
                Bytes::from(auth_msg.payload)
            } else {
                raw
            };
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
            // When FLAG_DO_FENCING is set, Java appends the 20-byte masterKey.
            // Matches Java's BookieProtoEncoding for a fenced batch read.
            let master_key = if header.has_flag(FLAG_DO_FENCING) && buf.remaining() >= MASTER_KEY_LENGTH {
                let mut key = [0u8; MASTER_KEY_LENGTH];
                buf.copy_to_slice(&mut key);
                Some(key)
            } else {
                None
            };
            Ok(V2Request::BatchReadEntry { flags, ledger_id, start_entry_id, request_id, max_count, max_size, master_key })
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
