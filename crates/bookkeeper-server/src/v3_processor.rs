use bytes::Bytes;
use prost::Message;

use bookkeeper_common::error::BKError;
use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::v2::request::V2Request;
use bookkeeper_protocol::v2::response::V2Response;
use crate::processor::RequestProcessor;

/// Try to decode a frame as a V3 protobuf Request.
/// Returns None if the frame is not valid protobuf (likely V2 binary).
pub fn try_decode_v3(data: &[u8]) -> Option<bookkeeper_proto::Request> {
    bookkeeper_proto::Request::decode(data).ok()
}

/// Process a V3 protobuf request by converting to V2 internal representation,
/// running through the same handler, and converting the response back to V3 protobuf.
pub fn process_v3_request(
    processor: &RequestProcessor,
    req: bookkeeper_proto::Request,
) -> Vec<u8> {
    let header = &req.header;
    let txn_id = header.txn_id;
    let operation = header.operation;

    // Convert V3 request to V2 internal request and process
    let v2_response = match operation {
        op if op == bookkeeper_proto::OperationType::AddEntry as i32 => {
            if let Some(add) = req.add_request {
                let mut master_key = [0u8; MASTER_KEY_LENGTH];
                let key_len = add.master_key.len().min(MASTER_KEY_LENGTH);
                master_key[..key_len].copy_from_slice(&add.master_key[..key_len]);

                let flags = if add.flag == Some(bookkeeper_proto::add_request::Flag::RecoveryAdd as i32) {
                    FLAG_RECOVERY_ADD
                } else {
                    0
                };

                let v2_req = V2Request::AddEntry {
                    flags,
                    master_key,
                    entry_data: Bytes::from(add.body),
                };
                processor.process_request(v2_req)
            } else {
                return build_v3_error_response(txn_id, operation, BKError::BadRequest);
            }
        }
        op if op == bookkeeper_proto::OperationType::ReadEntry as i32 => {
            if let Some(read) = req.read_request {
                // Check for long poll: presence of previousLAC field.
                // Matches Java's RequestUtils.isLongPollReadRequest().
                let is_long_poll = read.previous_lac.is_some()
                    && read.flag != Some(bookkeeper_proto::read_request::Flag::FenceLedger as i32);

                let mut flags: u16 = 0;
                let mut master_key_opt = None;
                if read.flag == Some(bookkeeper_proto::read_request::Flag::FenceLedger as i32) {
                    flags |= FLAG_DO_FENCING;
                    if let Some(mk) = read.master_key {
                        let mut key = [0u8; MASTER_KEY_LENGTH];
                        let len = mk.len().min(MASTER_KEY_LENGTH);
                        key[..len].copy_from_slice(&mk[..len]);
                        master_key_opt = Some(key);
                    }
                }

                if is_long_poll {
                    // Long poll read matching Java's LongPollReadEntryProcessorV3.
                    // Uses std::sync::mpsc (not tokio) to avoid block_on deadlock.
                    let previous_lac = read.previous_lac.unwrap();
                    let timeout_ms = read.time_out.unwrap_or(10000) as u64;
                    let ledger_id = read.ledger_id;
                    let piggyback = read.flag == Some(bookkeeper_proto::read_request::Flag::EntryPiggyback as i32);

                    let current_lac = processor.storage().get_last_add_confirmed(ledger_id)
                        .unwrap_or(-1);

                    // Determine the effective LAC (may wait for update)
                    let effective_lac = if current_lac > previous_lac {
                        current_lac // already advanced
                    } else if let Some(watchers) = processor.storage().lac_watchers() {
                        if let Some(rx) = watchers.watch(ledger_id, previous_lac, current_lac) {
                            // Wait using sync recv_timeout (safe in spawn_blocking context)
                            match rx.recv_timeout(std::time::Duration::from_millis(timeout_ms)) {
                                Ok(new_lac) => new_lac,
                                Err(_) => current_lac, // timeout
                            }
                        } else {
                            // Entry became available between check and watch
                            processor.storage().get_last_add_confirmed(ledger_id).unwrap_or(-1)
                        }
                    } else {
                        current_lac
                    };

                    // Read the entry
                    let read_entry_id = if piggyback && effective_lac > previous_lac {
                        previous_lac + 1
                    } else {
                        read.entry_id
                    };
                    let v2_req = V2Request::ReadEntry {
                        flags, ledger_id, entry_id: read_entry_id, master_key: master_key_opt,
                    };
                    processor.process_request(v2_req)
                } else {
                    // Normal read (not long poll)
                    let v2_req = V2Request::ReadEntry {
                        flags,
                        ledger_id: read.ledger_id,
                        entry_id: read.entry_id,
                        master_key: master_key_opt,
                    };
                    processor.process_request(v2_req)
                }
            } else {
                return build_v3_error_response(txn_id, operation, BKError::BadRequest);
            }
        }
        op if op == bookkeeper_proto::OperationType::WriteLac as i32 => {
            if let Some(wl) = req.write_lac_request {
                let mut master_key = [0u8; MASTER_KEY_LENGTH];
                let key_len = wl.master_key.len().min(MASTER_KEY_LENGTH);
                master_key[..key_len].copy_from_slice(&wl.master_key[..key_len]);
                let v2_req = V2Request::WriteLac {
                    ledger_id: wl.ledger_id,
                    lac: wl.lac,
                    master_key,
                    body: Bytes::from(wl.body),
                };
                processor.process_request(v2_req)
            } else {
                return build_v3_error_response(txn_id, operation, BKError::BadRequest);
            }
        }
        op if op == bookkeeper_proto::OperationType::ReadLac as i32 => {
            if let Some(rl) = req.read_lac_request {
                let v2_req = V2Request::ReadLac { ledger_id: rl.ledger_id };
                processor.process_request(v2_req)
            } else {
                return build_v3_error_response(txn_id, operation, BKError::BadRequest);
            }
        }
        op if op == bookkeeper_proto::OperationType::GetBookieInfo as i32 => {
            let requested = req.get_bookie_info_request.map(|r| r.requested.unwrap_or(0x03)).unwrap_or(0x03);
            let v2_req = V2Request::GetBookieInfo { flags: 0, requested };
            processor.process_request(v2_req)
        }
        op if op == bookkeeper_proto::OperationType::ForceLedger as i32 => {
            if let Some(fl) = req.force_ledger_request {
                let _ = processor.force_flush();
                return build_v3_force_ledger_response(txn_id, fl.ledger_id, BKError::Ok);
            } else {
                return build_v3_error_response(txn_id, operation, BKError::BadRequest);
            }
        }
        op if op == bookkeeper_proto::OperationType::Auth as i32 => {
            // Auth disabled: return AUTHENTICATION_DISABLED with empty payload.
            // Matches Java's BookieRequestProcessor when no auth provider is configured.
            let resp = bookkeeper_proto::Response {
                header: make_v3_header(txn_id, operation),
                status: bookkeeper_proto::StatusCode::Eok as i32,
                auth_response: Some(bookkeeper_proto::AuthMessage {
                    auth_plugin_name: "AUTHENTICATION_DISABLED".to_string(),
                    payload: Vec::new(),
                }),
                ..Default::default()
            };
            return resp.encode_to_vec();
        }
        _ => {
            return build_v3_error_response(txn_id, operation, BKError::BadRequest);
        }
    };

    // Convert V2 response to V3 protobuf
    v2_response_to_v3(txn_id, operation, v2_response)
}

fn v2_response_to_v3(txn_id: u64, operation: i32, v2_resp: V2Response) -> Vec<u8> {
    let resp = match v2_resp {
        V2Response::Add { error_code, ledger_id, entry_id } => {
            bookkeeper_proto::Response {
                header: make_v3_header(txn_id, operation),
                status: v2_to_v3_status(error_code),
                add_response: Some(bookkeeper_proto::AddResponse {
                    status: v2_to_v3_status(error_code),
                    ledger_id,
                    entry_id,
                }),
                ..Default::default()
            }
        }
        V2Response::Read { error_code, ledger_id, entry_id, data } => {
            // For long poll reads, include maxLAC matching Java's ReadResponse.
            // entry_id for LAST_ADD_CONFIRMED reads contains the actual LAC value.
            let max_lac = if entry_id >= 0 { Some(entry_id) } else { None };
            bookkeeper_proto::Response {
                header: make_v3_header(txn_id, operation),
                status: v2_to_v3_status(error_code),
                read_response: Some(bookkeeper_proto::ReadResponse {
                    status: v2_to_v3_status(error_code),
                    ledger_id,
                    entry_id,
                    body: data.map(|d| d.to_vec()),
                    max_lac,
                    lac_update_timestamp: None,
                }),
                ..Default::default()
            }
        }
        V2Response::WriteLac { error_code, ledger_id, .. } => {
            bookkeeper_proto::Response {
                header: make_v3_header(txn_id, operation),
                status: v2_to_v3_status(error_code),
                write_lac_response: Some(bookkeeper_proto::WriteLacResponse {
                    status: v2_to_v3_status(error_code),
                    ledger_id,
                }),
                ..Default::default()
            }
        }
        V2Response::ReadLac { error_code, ledger_id, lac_body, last_entry_body, .. } => {
            bookkeeper_proto::Response {
                header: make_v3_header(txn_id, operation),
                status: v2_to_v3_status(error_code),
                read_lac_response: Some(bookkeeper_proto::ReadLacResponse {
                    status: v2_to_v3_status(error_code),
                    ledger_id,
                    lac_body: lac_body.map(|b| b.to_vec()),
                    last_entry_body: last_entry_body.map(|b| b.to_vec()),
                }),
                ..Default::default()
            }
        }
        V2Response::GetBookieInfo { error_code, free_disk_space, total_disk_capacity, .. } => {
            bookkeeper_proto::Response {
                header: make_v3_header(txn_id, operation),
                status: v2_to_v3_status(error_code),
                get_bookie_info_response: Some(bookkeeper_proto::GetBookieInfoResponse {
                    status: v2_to_v3_status(error_code),
                    total_disk_capacity: Some(total_disk_capacity),
                    free_disk_space: Some(free_disk_space),
                }),
                ..Default::default()
            }
        }
        V2Response::Auth { error_code, payload } => {
            bookkeeper_proto::Response {
                header: make_v3_header(txn_id, operation),
                status: v2_to_v3_status(error_code),
                auth_response: Some(bookkeeper_proto::AuthMessage {
                    auth_plugin_name: String::new(),
                    payload: payload.to_vec(),
                }),
                ..Default::default()
            }
        }
        V2Response::BatchRead { error_code, .. } => {
            // Simplified batch read response — no V3 BatchRead message type
            bookkeeper_proto::Response {
                header: make_v3_header(txn_id, operation),
                status: v2_to_v3_status(error_code),
                ..Default::default()
            }
        }
    };
    resp.encode_to_vec()
}

fn make_v3_header(txn_id: u64, operation: i32) -> bookkeeper_proto::BkPacketHeader {
    bookkeeper_proto::BkPacketHeader {
        version: bookkeeper_proto::ProtocolVersion::VersionThree as i32,
        operation,
        txn_id,
        priority: Some(0),
    }
}

fn v2_to_v3_status(v2_code: i32) -> i32 {
    BKError::from_v2_code(v2_code).to_v3_code()
}

fn build_v3_error_response(txn_id: u64, operation: i32, err: BKError) -> Vec<u8> {
    let resp = bookkeeper_proto::Response {
        header: make_v3_header(txn_id, operation),
        status: err.to_v3_code(),
        ..Default::default()
    };
    resp.encode_to_vec()
}

fn build_v3_force_ledger_response(txn_id: u64, ledger_id: i64, err: BKError) -> Vec<u8> {
    let resp = bookkeeper_proto::Response {
        header: make_v3_header(txn_id, bookkeeper_proto::OperationType::ForceLedger as i32),
        status: err.to_v3_code(),
        force_ledger_response: Some(bookkeeper_proto::ForceLedgerResponse {
            status: err.to_v3_code(),
            ledger_id,
        }),
        ..Default::default()
    };
    resp.encode_to_vec()
}
