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
        V2Response::Auth { error_code: _, payload } => {
            // Java V2 Auth response format: [4-byte header][protobuf AuthMessage]
            // NOT the standard 24-byte response header. Matches Java's
            // BookieProtoEncoding.ResponseEnDeCoderPreV3 for AUTH.
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
        V2Response::WriteLac { error_code, ledger_id, entry_id } => {
            encode_response_header(OP_WRITE_LAC, *error_code, *ledger_id, *entry_id, 0)
        }
        V2Response::ReadLac { error_code, ledger_id, entry_id, lac_body, last_entry_body } => {
            // Encode with 4-byte length prefix for each body, so the client can
            // distinguish where lac_body ends and last_entry_body begins.
            let lac_len = lac_body.as_ref().map_or(0, |b| b.len());
            let entry_len = last_entry_body.as_ref().map_or(0, |b| b.len());
            let data_size = 4 + lac_len + 4 + entry_len;
            let mut buf = encode_response_header(OP_READ_LAC, *error_code, *ledger_id, *entry_id, data_size);
            buf.put_i32(lac_len as i32);
            if let Some(b) = lac_body { buf.put_slice(b); }
            buf.put_i32(entry_len as i32);
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
    // Try V3 protobuf first. A V3 response starts with a protobuf-encoded
    // `bookkeeper_proto::Response` which looks different from a V2 header.
    // V2 headers have version byte at bits 31-24 (0x02), while protobuf
    // messages start with field/wire-type tags. Try V3 and fall through to V2.
    if let Some(v2) = try_decode_v3_to_v2(&data) {
        return Ok(v2);
    }

    // Read the 4-byte header first, then branch by opcode.
    // AUTH has a non-standard response format (no error_code/ledger_id/entry_id).
    if data.len() < 4 { return Err(ResponseDecodeError::TooShort); }
    let mut buf = &data[..];
    let header = PacketHeader::from_u32(buf.get_u32());

    // AUTH response: [header:4][protobuf AuthMessage] — no standard 20-byte fields.
    // Matches Java's BookieProtoEncoding.ResponseDecodePreV3 which parses
    // AuthMessage directly after the header.
    if header.opcode() == OP_AUTH {
        use prost::Message;
        let remaining = data.slice(4..);
        if let Ok(auth_msg) = bookkeeper_proto::AuthMessage::decode(remaining.as_ref() as &[u8]) {
            return Ok(V2Response::Auth { error_code: V2_EOK, payload: Bytes::from(auth_msg.payload) });
        }
        // Fallback: treat remaining as raw payload
        return Ok(V2Response::Auth { error_code: V2_EOK, payload: data.slice(4..) });
    }

    // Standard response format: [header:4][error_code:4][ledger_id:8][entry_id:8][data...]
    if data.len() < V2_RESPONSE_HEADER_SIZE { return Err(ResponseDecodeError::TooShort); }
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
        // OP_AUTH is handled above before reading the standard header.
        OP_AUTH => Ok(V2Response::Auth { error_code: V2_EOK, payload: Bytes::new() }),
        OP_WRITE_LAC => Ok(V2Response::WriteLac { error_code, ledger_id, entry_id }),
        OP_READ_LAC => {
            let mut lac_body = None;
            let mut last_entry_body = None;
            let mut pos = V2_RESPONSE_HEADER_SIZE;
            // Decode length-prefixed lac_body
            if pos + 4 <= data.len() {
                let lac_len = i32::from_be_bytes(data[pos..pos+4].try_into().unwrap()) as usize;
                pos += 4;
                if lac_len > 0 && pos + lac_len <= data.len() {
                    lac_body = Some(data.slice(pos..pos + lac_len));
                    pos += lac_len;
                }
            }
            // Decode length-prefixed last_entry_body
            if pos + 4 <= data.len() {
                let entry_len = i32::from_be_bytes(data[pos..pos+4].try_into().unwrap()) as usize;
                pos += 4;
                if entry_len > 0 && pos + entry_len <= data.len() {
                    last_entry_body = Some(data.slice(pos..pos + entry_len));
                }
            }
            Ok(V2Response::ReadLac { error_code, ledger_id, entry_id, lac_body, last_entry_body })
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

/// Try to decode a frame as a V3 protobuf `Response` and convert to `V2Response`.
/// Returns `None` if the data is not valid V3 protobuf.
fn try_decode_v3_to_v2(data: &Bytes) -> Option<V2Response> {
    use prost::Message;
    let resp = bookkeeper_proto::Response::decode(data.as_ref()).ok()?;
    let status = resp.status;
    let v2_code = v3_status_to_v2_code(status);
    let op = resp.header.operation;

    if let Some(add) = resp.add_response {
        Some(V2Response::Add {
            error_code: v2_code,
            ledger_id: add.ledger_id,
            entry_id: add.entry_id,
        })
    } else if let Some(read) = resp.read_response {
        Some(V2Response::Read {
            error_code: v2_code,
            ledger_id: read.ledger_id,
            entry_id: read.entry_id,
            data: read.body.map(Bytes::from),
        })
    } else if let Some(wl) = resp.write_lac_response {
        Some(V2Response::WriteLac {
            error_code: v2_code,
            ledger_id: wl.ledger_id,
            entry_id: 0,
        })
    } else if let Some(rl) = resp.read_lac_response {
        Some(V2Response::ReadLac {
            error_code: v2_code,
            ledger_id: rl.ledger_id,
            entry_id: 0,
            lac_body: rl.lac_body.map(Bytes::from),
            last_entry_body: rl.last_entry_body.map(Bytes::from),
        })
    } else if let Some(bi) = resp.get_bookie_info_response {
        Some(V2Response::GetBookieInfo {
            error_code: v2_code,
            ledger_id: 0,
            entry_id: 0,
            free_disk_space: bi.free_disk_space.unwrap_or(0),
            total_disk_capacity: bi.total_disk_capacity.unwrap_or(0),
        })
    } else if resp.force_ledger_response.is_some() {
        // ForceLedger has no equivalent V2 response type.
        // Return an Add response with the status code — the caller
        // (send_force_ledger) checks for Ok/Err, not the response variant.
        let fl = resp.force_ledger_response.unwrap();
        Some(V2Response::Add {
            error_code: v2_code,
            ledger_id: fl.ledger_id,
            entry_id: 0,
        })
    } else if resp.auth_response.is_some() {
        let auth = resp.auth_response.unwrap();
        Some(V2Response::Auth {
            error_code: v2_code,
            payload: Bytes::from(auth.payload),
        })
    } else if op == bookkeeper_proto::OperationType::ForceLedger as i32 {
        // Fallback for ForceLedger with no specific response body
        Some(V2Response::Add { error_code: v2_code, ledger_id: 0, entry_id: 0 })
    } else {
        None
    }
}

/// Convert a V3 StatusCode to a V2 error code.
fn v3_status_to_v2_code(status: i32) -> i32 {
    // Inline mapping to avoid cross-crate dependency.
    // V3 StatusCode → V2 error code.
    match status {
        0 => V2_EOK,                            // EOK
        402 => V2_ENOLEDGER,                     // ENOLEDGER
        403 => V2_ENOENTRY,                      // ENOENTRY
        404 => V2_EBADREQ,                       // EBADREQ
        501 => V2_EIO,                            // EIO
        502 => V2_EUA,                            // EUA
        503 => V2_EBADVERSION,                    // EBADVERSION
        504 => V2_EFENCED,                        // EFENCED
        505 => V2_EREADONLY,                       // EREADONLY
        506 => V2_ETOOMANYREQUESTS,                // ETOOMANYREQUESTS
        507 => V2_EUNKNOWNLEDGERSTATE,             // EUNKNOWNLEDGERSTATE
        _ => V2_EBADREQ,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ResponseDecodeError {
    #[error("message too short")]
    TooShort,
    #[error("unknown opcode: {0}")]
    UnknownOpcode(u8),
}
