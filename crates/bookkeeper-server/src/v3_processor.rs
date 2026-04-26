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
/// Returns the encoded response bytes plus a success flag (status == EOK) used
/// by the writer task to attribute the end-to-end `*_REQUEST` latency.
pub async fn process_v3_request(
    processor: &RequestProcessor,
    req: bookkeeper_proto::Request,
) -> (Vec<u8>, bool) {
    let header = &req.header;
    let txn_id = header.txn_id;
    let operation = header.operation;
    // Java's RequestUtils.isHighPriority(request): header.getPriority() > 0.
    // High-priority requests are routed to the high-priority thread pool so
    // recovery and fencing ops make progress even when normal pools are saturated.
    let header_high_priority = header.priority.unwrap_or(0) > 0;

    // Convert V3 request to V2 internal request and process
    let v2_response = match operation {
        op if op == bookkeeper_proto::OperationType::AddEntry as i32 => {
            if let Some(add) = req.add_request {
                // Reject keys that are shorter or longer than 20 bytes —
                // Java's WriteEntryProcessorV3 requires an exactly-sized
                // masterKey. Silently zero-padding a short key would let a
                // wrong-data client masquerade as the legitimate one;
                // truncating a long key would mismatch on every subsequent
                // op against the same ledger.
                if add.master_key.len() != MASTER_KEY_LENGTH {
                    return (build_v3_error_response(txn_id, operation, BKError::BadRequest), false);
                }
                let mut master_key = [0u8; MASTER_KEY_LENGTH];
                master_key.copy_from_slice(&add.master_key);

                let mut flags: u16 = 0;
                if add.flag == Some(bookkeeper_proto::add_request::Flag::RecoveryAdd as i32) {
                    flags |= FLAG_RECOVERY_ADD;
                }
                // Propagate header-level priority to the internal flag.
                // Matches Java's WriteEntryProcessorV3 which routes through
                // the high-priority thread pool when isHighPriority(request).
                if header_high_priority {
                    flags |= FLAG_HIGH_PRIORITY;
                }

                // Parse DEFERRED_SYNC from V3 writeFlags field.
                // Matches Java's WriteEntryProcessorV3: ackBeforeSync = writeFlags.contains(DEFERRED_SYNC)
                let deferred_sync = add.write_flags.unwrap_or(0) & WRITE_FLAG_DEFERRED_SYNC != 0;

                let v2_req = V2Request::AddEntry {
                    flags,
                    master_key,
                    entry_data: Bytes::from(add.body),
                    deferred_sync,
                };
                processor.process_request(v2_req).await
            } else {
                return (build_v3_error_response(txn_id, operation, BKError::BadRequest), false);
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
                        // Same exact-length requirement as AddEntry — see comment there.
                        if mk.len() != MASTER_KEY_LENGTH {
                            return (build_v3_error_response(txn_id, operation, BKError::BadRequest), false);
                        }
                        let mut key = [0u8; MASTER_KEY_LENGTH];
                        key.copy_from_slice(&mk);
                        master_key_opt = Some(key);
                    }
                }
                // Propagate header-level priority.  Matches Java's
                // BookieRequestProcessor which routes isHighPriority reads to
                // highPriorityThreadPool.  FENCE_LEDGER is already high-priority
                // via FLAG_DO_FENCING, so this covers the remaining case of
                // header.priority > 0 on a non-fencing read.
                if header_high_priority {
                    flags |= FLAG_HIGH_PRIORITY;
                }

                if is_long_poll {
                    // Long poll read matching Java's LongPollReadEntryProcessorV3.
                    let previous_lac = read.previous_lac.unwrap();
                    let timeout_ms = read.time_out.unwrap_or(10000) as u64;
                    let ledger_id = read.ledger_id;
                    let piggyback = read.flag == Some(bookkeeper_proto::read_request::Flag::EntryPiggyback as i32);

                    let current_lac = processor.storage().get_last_add_confirmed(ledger_id)
                        .unwrap_or(-1);

                    // Determine the effective LAC (may wait for update).
                    // Uses async watchers — no OS thread per long poll.
                    // Matches Java's LongPollReadEntryProcessorV3 which uses a
                    // non-blocking Watcher + HashedWheelTimer for timeout.
                    let effective_lac = if current_lac > previous_lac {
                        current_lac // already advanced
                    } else if let Some(watchers) = processor.storage().lac_watchers() {
                        // Use the recheck variant so a notify that fires
                        // between our `get_last_add_confirmed` snapshot and
                        // watcher registration doesn't leave us blocked
                        // until timeout. The recheck closure runs under the
                        // watcher mutex, so notify_lac_advance can't race.
                        let storage = processor.storage();
                        if let Some(rx) = watchers.watch_async_with_recheck(
                            ledger_id,
                            previous_lac,
                            current_lac,
                            || storage.get_last_add_confirmed(ledger_id).unwrap_or(-1),
                        ) {
                            let timeout = std::time::Duration::from_millis(timeout_ms);
                            match tokio::time::timeout(timeout, rx).await {
                                Ok(Ok(new_lac)) => new_lac,
                                _ => current_lac,
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
                    processor.process_request(v2_req).await
                } else {
                    // Normal read (not long poll)
                    let v2_req = V2Request::ReadEntry {
                        flags,
                        ledger_id: read.ledger_id,
                        entry_id: read.entry_id,
                        master_key: master_key_opt,
                    };
                    processor.process_request(v2_req).await
                }
            } else {
                return (build_v3_error_response(txn_id, operation, BKError::BadRequest), false);
            }
        }
        op if op == bookkeeper_proto::OperationType::WriteLac as i32 => {
            if let Some(wl) = req.write_lac_request {
                // Exact 20-byte requirement (see AddEntry comment).
                if wl.master_key.len() != MASTER_KEY_LENGTH {
                    return (build_v3_error_response(txn_id, operation, BKError::BadRequest), false);
                }
                let mut master_key = [0u8; MASTER_KEY_LENGTH];
                master_key.copy_from_slice(&wl.master_key);
                let v2_req = V2Request::WriteLac {
                    ledger_id: wl.ledger_id,
                    lac: wl.lac,
                    master_key,
                    body: Bytes::from(wl.body),
                };
                processor.process_request(v2_req).await
            } else {
                return (build_v3_error_response(txn_id, operation, BKError::BadRequest), false);
            }
        }
        op if op == bookkeeper_proto::OperationType::ReadLac as i32 => {
            if let Some(rl) = req.read_lac_request {
                let v2_req = V2Request::ReadLac { ledger_id: rl.ledger_id };
                processor.process_request(v2_req).await
            } else {
                return (build_v3_error_response(txn_id, operation, BKError::BadRequest), false);
            }
        }
        op if op == bookkeeper_proto::OperationType::GetBookieInfo as i32 => {
            let requested = req.get_bookie_info_request.map(|r| r.requested.unwrap_or(0x03)).unwrap_or(0x03);
            let v2_req = V2Request::GetBookieInfo { flags: 0, requested };
            processor.process_request(v2_req).await
        }
        op if op == bookkeeper_proto::OperationType::ForceLedger as i32 => {
            if let Some(fl) = req.force_ledger_request {
                // ForceLedger fsyncs the journal — refuse on a read-only
                // bookie unless high-priority writes are enabled.
                if !processor.is_writable()
                    && !processor.high_priority_writes_enabled()
                {
                    return (
                        build_v3_force_ledger_response(txn_id, fl.ledger_id, BKError::ReadOnly),
                        false,
                    );
                }
                // `FORCE_LEDGER_REQUEST` is recorded end-to-end by the writer task
                // once the response is flushed (matches Java's
                // PacketProcessorBaseV3.sendResponse → enqueueNanos semantics).
                match processor.force_journal_sync().await {
                    Ok(_) => {
                        return (build_v3_force_ledger_response(txn_id, fl.ledger_id, BKError::Ok), true);
                    }
                    Err(_) => {
                        return (build_v3_force_ledger_response(txn_id, fl.ledger_id, BKError::Io), false);
                    }
                }
            } else {
                return (build_v3_error_response(txn_id, operation, BKError::BadRequest), false);
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
            return (resp.encode_to_vec(), true);
        }
        op if op == bookkeeper_proto::OperationType::StartTls as i32 => {
            // START_TLS is handled at the connection level in handle_starttls_connection.
            // If the request reaches the V3 processor, it means either:
            // 1. TLS is not configured on this bookie → return EBADREQ (matches Java
            //    BookieRequestProcessor.processStartTLSRequestV3 when shFactory == null)
            // 2. The connection is already TLS-upgraded → START_TLS mid-stream is invalid
            //
            // In both cases, return EBADREQ matching Java's behavior.
            return (build_v3_error_response(txn_id, operation, BKError::BadRequest), false);
        }
        op if op == bookkeeper_proto::OperationType::GetListOfEntriesOfLedger as i32 => {
            if let Some(req) = req.get_list_of_entries_of_ledger_request {
                let ledger_id = req.ledger_id;
                match processor.storage().get_entries_of_ledger(ledger_id) {
                    Ok(entries) => {
                        // Encode as Java's AvailabilityOfEntriesOfLedger format:
                        // A sequence of (firstEntry:i64, lastEntry:i64) pairs representing
                        // contiguous ranges, preceded by a header long = number of ranges.
                        let ranges = entries_to_ranges(&entries);
                        let mut buf = Vec::with_capacity(8 + ranges.len() * 16);
                        buf.extend_from_slice(&(ranges.len() as i64).to_be_bytes());
                        for (first, last) in &ranges {
                            buf.extend_from_slice(&first.to_be_bytes());
                            buf.extend_from_slice(&last.to_be_bytes());
                        }
                        let resp = bookkeeper_proto::Response {
                            header: make_v3_header(txn_id, operation),
                            status: bookkeeper_proto::StatusCode::Eok as i32,
                            get_list_of_entries_of_ledger_response: Some(
                                bookkeeper_proto::GetListOfEntriesOfLedgerResponse {
                                    status: bookkeeper_proto::StatusCode::Eok as i32,
                                    ledger_id,
                                    availability_of_entries_of_ledger: Some(buf),
                                },
                            ),
                            ..Default::default()
                        };
                        return (resp.encode_to_vec(), true);
                    }
                    Err(_) => {
                        return (build_v3_error_response(txn_id, operation, BKError::NoLedger), false);
                    }
                }
            } else {
                return (build_v3_error_response(txn_id, operation, BKError::BadRequest), false);
            }
        }
        _ => {
            return (build_v3_error_response(txn_id, operation, BKError::BadRequest), false);
        }
    };

    // Convert V2 response to V3 protobuf. Success flag derives from the V2
    // response's error_code (EOK == success) so the writer task can attribute
    // end-to-end `*_REQUEST` latency to the OK/FAIL histogram.
    let success = crate::server::v2_response_success(&v2_response);
    (v2_response_to_v3(txn_id, operation, v2_response), success)
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

pub fn build_v3_error_response(txn_id: u64, operation: i32, err: BKError) -> Vec<u8> {
    let resp = bookkeeper_proto::Response {
        header: make_v3_header(txn_id, operation),
        status: err.to_v3_code(),
        ..Default::default()
    };
    resp.encode_to_vec()
}

/// Convert a sorted list of entry IDs into contiguous (first, last) ranges.
fn entries_to_ranges(entries: &[i64]) -> Vec<(i64, i64)> {
    if entries.is_empty() {
        return Vec::new();
    }
    let mut ranges = Vec::new();
    let mut first = entries[0];
    let mut last = entries[0];
    for &eid in &entries[1..] {
        if eid == last + 1 {
            last = eid;
        } else {
            ranges.push((first, last));
            first = eid;
            last = eid;
        }
    }
    ranges.push((first, last));
    ranges
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
