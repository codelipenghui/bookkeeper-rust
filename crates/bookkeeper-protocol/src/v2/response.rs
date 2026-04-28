use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::constants::*;
use crate::v2::header::PacketHeader;

/// Per-response correlation key, used by the client connection task to
/// route a decoded response back to the matching request's
/// `oneshot::Sender`. Mirrors Java's `CompletionKey` hierarchy in
/// `PerChannelBookieClient`:
///
/// - V3 frames: `TxnCompletionKey(txnId, op)` — every response's
///   `BkPacketHeader.txn_id` echoes the request's allocator-assigned
///   id, so correlation is unambiguous regardless of server-side
///   reorder. Java line 1537 (`readV3Response`).
/// - V2 entry ops (Add / Read): `EntryCompletionKey(ledgerId, entryId,
///   op)` — V2 has no free txn_id field on the wire, so the natural
///   `(op, ledger_id, entry_id)` triple is used. Java line 1417.
/// - V2 BatchRead: `TxnCompletionKey(requestId, BATCH_READ_ENTRY)` —
///   the client-allocated `request_id` IS on the wire and serves as
///   the txn id. Java line 1415.
/// - Other V2 ops: keyed by `(op, ledger_id)` (WriteLac / ReadLac /
///   ForceLedger) or singleton (Auth / GetBookieInfo). The Rust
///   client matches the FIFO order within a single key bucket via a
///   per-key VecDeque on the matcher side, mirroring Java's
///   `completionObjectsV2Conflicts: SynchronizedHashMultiMap`.
///
/// Without content-keyed dispatch, the previous Rust client used a
/// FIFO `VecDeque<oneshot::Sender>` matcher. The bookie processes
/// requests via `FuturesUnordered` and writes responses to the wire
/// in completion order — so any two requests on the same channel
/// where one finishes faster (e.g. a long-poll read parked on
/// `lac_watcher` while a co-located fast read returns immediately)
/// produced silently mis-routed responses. The long-poll caller
/// would receive the fast read's payload and labelling, which Pulsar
/// managed-ledger cursors then advanced against — silent corruption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CorrelationKey {
    /// V3 protobuf frame. `txn_id` echoes the client allocator value.
    V3 { txn_id: u64 },
    /// V2 Add or Read response: keyed by (op, ledger_id, entry_id).
    V2Entry { op: u8, ledger_id: i64, entry_id: i64 },
    /// V2 BatchRead: keyed by the client-supplied request_id.
    V2BatchRead { request_id: i64 },
    /// V2 WriteLac / ReadLac response: keyed by (op, ledger_id). Two
    /// concurrent in-flight ops of the same kind on the same ledger
    /// fall back to FIFO within the bucket.
    V2Ledger { op: u8, ledger_id: i64 },
    /// V2 Auth: single-shot during connection setup. FIFO within the
    /// op bucket is fine because a connection only ever has one
    /// outstanding auth round at a time (the auth state machine waits
    /// for completion before dispatching the next message).
    V2Auth,
}

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

/// Decode a wire-format response and return both the decoded
/// [`V2Response`] and the [`CorrelationKey`] derived from it. The key
/// lets the client connection task route the response to the matching
/// request's `oneshot::Sender` via content-based dispatch instead of
/// the (broken) FIFO arrival-order pop. See [`CorrelationKey`] for the
/// keying scheme per response shape.
pub fn decode_response_with_key(
    data: Bytes,
) -> Result<(CorrelationKey, V2Response), ResponseDecodeError> {
    // V3 first — extract txn_id from the proto header *before* losing
    // it through the V3->V2 conversion in `try_decode_v3_to_v2`.
    if let Some((txn_id, v2)) = try_decode_v3_with_txn_id(&data) {
        return Ok((CorrelationKey::V3 { txn_id }, v2));
    }
    let resp = decode_response_v2(data)?;
    let key = correlation_key_for_response(&resp)?;
    Ok((key, resp))
}

fn correlation_key_for_response(resp: &V2Response) -> Result<CorrelationKey, ResponseDecodeError> {
    Ok(match resp {
        V2Response::Add { ledger_id, entry_id, .. } => CorrelationKey::V2Entry {
            op: OP_ADD_ENTRY, ledger_id: *ledger_id, entry_id: *entry_id,
        },
        V2Response::Read { ledger_id, entry_id, .. } => CorrelationKey::V2Entry {
            op: OP_READ_ENTRY, ledger_id: *ledger_id, entry_id: *entry_id,
        },
        V2Response::WriteLac { ledger_id, .. } => CorrelationKey::V2Ledger {
            op: OP_WRITE_LAC, ledger_id: *ledger_id,
        },
        V2Response::ReadLac { ledger_id, .. } => CorrelationKey::V2Ledger {
            op: OP_READ_LAC, ledger_id: *ledger_id,
        },
        V2Response::BatchRead { request_id, .. } => CorrelationKey::V2BatchRead {
            request_id: *request_id,
        },
        V2Response::Auth { .. } => CorrelationKey::V2Auth,
        // GetBookieInfo is only ever sent as V3 by this client (Java's
        // `BookieProtoEncoding` doesn't decode opcode 6 in V2 mode at
        // all). Receiving it as a V2 response is unexpected; reject so
        // the client surfaces the protocol error rather than silently
        // mis-routing under any matcher scheme.
        V2Response::GetBookieInfo { .. } => return Err(ResponseDecodeError::Malformed(
            "GetBookieInfo received as V2 response — only V3 is supported".into(),
        )),
    })
}

pub fn decode_response(data: Bytes) -> Result<V2Response, ResponseDecodeError> {
    // Try V3 protobuf first. A V3 response starts with a protobuf-encoded
    // `bookkeeper_proto::Response` which looks different from a V2 header.
    // V2 headers have version byte at bits 31-24 (0x02), while protobuf
    // messages start with field/wire-type tags. Try V3 and fall through to V2.
    if let Some(v2) = try_decode_v3_to_v2(&data) {
        return Ok(v2);
    }
    decode_response_v2(data)
}

fn decode_response_v2(data: Bytes) -> Result<V2Response, ResponseDecodeError> {

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
        OP_WRITE_LAC => Ok(V2Response::WriteLac { error_code, ledger_id, entry_id }),
        OP_READ_LAC => {
            let mut lac_body = None;
            let mut last_entry_body = None;
            let mut pos = V2_RESPONSE_HEADER_SIZE;
            // Decode length-prefixed lac_body. A negative length on the
            // wire is a hard protocol error — silently casting it to a
            // huge usize and falling out of the bounds check would
            // collapse the field to None and let recovery treat a
            // corrupt response as a legitimate "absent" answer.
            if pos + 4 <= data.len() {
                let lac_len_raw = i32::from_be_bytes(data[pos..pos+4].try_into().unwrap());
                pos += 4;
                if lac_len_raw < 0 {
                    return Err(ResponseDecodeError::Malformed(format!(
                        "ReadLac: negative lac_body length {lac_len_raw}"
                    )));
                }
                let lac_len = lac_len_raw as usize;
                if lac_len > 0 {
                    if pos.checked_add(lac_len).map_or(true, |end| end > data.len()) {
                        return Err(ResponseDecodeError::Malformed(format!(
                            "ReadLac: lac_body length {lac_len} exceeds payload"
                        )));
                    }
                    lac_body = Some(data.slice(pos..pos + lac_len));
                    pos += lac_len;
                }
            }
            // Same protocol invariant for the trailing last_entry_body.
            if pos + 4 <= data.len() {
                let entry_len_raw = i32::from_be_bytes(data[pos..pos+4].try_into().unwrap());
                pos += 4;
                if entry_len_raw < 0 {
                    return Err(ResponseDecodeError::Malformed(format!(
                        "ReadLac: negative last_entry length {entry_len_raw}"
                    )));
                }
                let entry_len = entry_len_raw as usize;
                if entry_len > 0 {
                    if pos.checked_add(entry_len).map_or(true, |end| end > data.len()) {
                        return Err(ResponseDecodeError::Malformed(format!(
                            "ReadLac: last_entry length {entry_len} exceeds payload"
                        )));
                    }
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
                let size_raw = i32::from_be_bytes(data[offset..offset + 4].try_into().unwrap());
                offset += 4;
                // A negative size is a protocol violation. Casting it to
                // usize and then "breaking" on the bounds check would
                // truncate the entry list and quietly hide the error.
                if size_raw < 0 {
                    return Err(ResponseDecodeError::Malformed(format!(
                        "BatchRead: negative entry size {size_raw}"
                    )));
                }
                let size = size_raw as usize;
                if offset.checked_add(size).map_or(true, |end| end > data.len()) {
                    return Err(ResponseDecodeError::Malformed(format!(
                        "BatchRead: entry size {size} exceeds payload"
                    )));
                }
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
    try_decode_v3_with_txn_id(data).map(|(_, v2)| v2)
}

/// Same as [`try_decode_v3_to_v2`] but also returns the V3
/// `BkPacketHeader.txn_id` so the client can correlate by it. The
/// txn_id is what the server echoes from the original request's
/// header — the only reliable correlation key for V3 frames since
/// the bookie processes requests via `FuturesUnordered` and writes
/// responses in completion order, not request order.
fn try_decode_v3_with_txn_id(data: &Bytes) -> Option<(u64, V2Response)> {
    use prost::Message;
    let resp = bookkeeper_proto::Response::decode(data.as_ref()).ok()?;
    let txn_id = resp.header.txn_id;
    let v2_code = bookkeeper_common::error::BKError::from_v3_code(resp.status).to_v2_code();
    let op = resp.header.operation;

    let v2 = if let Some(add) = resp.add_response {
        V2Response::Add {
            error_code: v2_code,
            ledger_id: add.ledger_id,
            entry_id: add.entry_id,
        }
    } else if let Some(read) = resp.read_response {
        V2Response::Read {
            error_code: v2_code,
            ledger_id: read.ledger_id,
            entry_id: read.entry_id,
            data: read.body.map(Bytes::from),
        }
    } else if let Some(wl) = resp.write_lac_response {
        V2Response::WriteLac {
            error_code: v2_code,
            ledger_id: wl.ledger_id,
            entry_id: 0,
        }
    } else if let Some(rl) = resp.read_lac_response {
        V2Response::ReadLac {
            error_code: v2_code,
            ledger_id: rl.ledger_id,
            entry_id: 0,
            lac_body: rl.lac_body.map(Bytes::from),
            last_entry_body: rl.last_entry_body.map(Bytes::from),
        }
    } else if let Some(bi) = resp.get_bookie_info_response {
        V2Response::GetBookieInfo {
            error_code: v2_code,
            ledger_id: 0,
            entry_id: 0,
            free_disk_space: bi.free_disk_space.unwrap_or(0),
            total_disk_capacity: bi.total_disk_capacity.unwrap_or(0),
        }
    } else if let Some(fl) = resp.force_ledger_response {
        // ForceLedger has no equivalent V2 response type.
        // Return an Add response with the status code — the caller
        // (send_force_ledger) checks for Ok/Err, not the response variant.
        V2Response::Add {
            error_code: v2_code,
            ledger_id: fl.ledger_id,
            entry_id: 0,
        }
    } else if let Some(auth) = resp.auth_response {
        V2Response::Auth {
            error_code: v2_code,
            payload: Bytes::from(auth.payload),
        }
    } else if op == bookkeeper_proto::OperationType::ForceLedger as i32 {
        // Fallback for ForceLedger with no specific response body
        V2Response::Add { error_code: v2_code, ledger_id: 0, entry_id: 0 }
    } else {
        return None;
    };
    Some((txn_id, v2))
}

#[derive(Debug, thiserror::Error)]
pub enum ResponseDecodeError {
    #[error("message too short")]
    TooShort,
    #[error("unknown opcode: {0}")]
    UnknownOpcode(u8),
    #[error("malformed response: {0}")]
    Malformed(String),
}
