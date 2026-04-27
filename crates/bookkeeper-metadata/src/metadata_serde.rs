use prost::Message;

use crate::ledger_metadata::*;

const VERSION_HEADER: &[u8] = b"BookieMetadataFormatVersion\t";
const LINE_SPLITTER: u8 = b'\n';

/// Serialize LedgerMetadata to v3 binary protobuf format.
/// Format: "BookieMetadataFormatVersion\t3\n" + varint-length-prefixed protobuf.
pub fn serialize_metadata(md: &LedgerMetadata) -> Result<Vec<u8>, MetadataError> {
    let proto = to_proto(md);
    let msg_len = proto.encoded_len();
    let mut buf = Vec::with_capacity(VERSION_HEADER.len() + 2 + 10 + msg_len);

    // Write header
    buf.extend_from_slice(VERSION_HEADER);
    buf.extend_from_slice(b"3");
    buf.push(LINE_SPLITTER);

    // Write delimited (varint length prefix + message)
    prost::encode_length_delimiter(msg_len, &mut buf)
        .map_err(|e| MetadataError::SerializationError(e.to_string()))?;
    proto.encode(&mut buf)
        .map_err(|e| MetadataError::SerializationError(e.to_string()))?;

    Ok(buf)
}

/// Deserialize LedgerMetadata from bytes (auto-detects version from header).
/// Supports v1 (text), v2 (protobuf TextFormat), and v3 (binary protobuf),
/// matching Java's `LedgerMetadataSerDe.parseConfig()`.
pub fn deserialize_metadata(ledger_id: i64, data: &[u8]) -> Result<LedgerMetadata, MetadataError> {
    let (version, body) = parse_header(data)?;
    match version {
        1 => parse_v1(ledger_id, body),
        2 => parse_v2(ledger_id, body),
        3 => parse_v3(ledger_id, body),
        v => Err(MetadataError::UnknownVersion(v)),
    }
}

fn parse_header(data: &[u8]) -> Result<(i32, &[u8]), MetadataError> {
    if data.len() < VERSION_HEADER.len() + 2 {
        return Err(MetadataError::SerializationError("data too short for header".into()));
    }
    if &data[..VERSION_HEADER.len()] != VERSION_HEADER {
        return Err(MetadataError::SerializationError("invalid header prefix".into()));
    }

    let after_prefix = &data[VERSION_HEADER.len()..];
    let newline_pos = after_prefix.iter().position(|&b| b == LINE_SPLITTER)
        .ok_or_else(|| MetadataError::SerializationError("no newline after version".into()))?;

    let version_str = std::str::from_utf8(&after_prefix[..newline_pos])
        .map_err(|e| MetadataError::SerializationError(e.to_string()))?;
    let version: i32 = version_str.parse()
        .map_err(|e: std::num::ParseIntError| MetadataError::SerializationError(format!("invalid version: {e}")))?;

    let body = &after_prefix[newline_pos + 1..];
    Ok((version, body))
}

fn parse_v3(ledger_id: i64, body: &[u8]) -> Result<LedgerMetadata, MetadataError> {
    let proto = bookkeeper_proto::LedgerMetadataFormat::decode_length_delimited(body)
        .map_err(|e| MetadataError::SerializationError(format!("protobuf decode: {e}")))?;
    from_proto(ledger_id, proto)
}

/// V1 text format constants matching Java's LedgerMetadataSerDe.
const V1_FIELD_SPLITTER: &str = "\t";
const V1_CLOSED_TAG: &str = "CLOSED";
const V1_IN_RECOVERY_ENTRY_ID: i64 = -102;

/// Parse v1 text format. Matches Java's `parseVersion1Config`.
/// Format:
/// ```text
/// {quorumSize}
/// {ensembleSize}
/// {length}
/// {firstEntryId}\t{bookie1}\t{bookie2}...
/// ...
/// {lastEntryId|-102}\tCLOSED
/// ```
fn parse_v1(ledger_id: i64, body: &[u8]) -> Result<LedgerMetadata, MetadataError> {
    let text = std::str::from_utf8(body)
        .map_err(|e| MetadataError::SerializationError(format!("v1 UTF-8 error: {e}")))?;
    let mut lines = text.lines();

    let quorum_size: i32 = lines.next()
        .ok_or_else(|| MetadataError::SerializationError("v1: missing quorum_size".into()))?
        .parse()
        .map_err(|e: std::num::ParseIntError| MetadataError::SerializationError(format!("v1 quorum_size: {e}")))?;
    let ensemble_size: i32 = lines.next()
        .ok_or_else(|| MetadataError::SerializationError("v1: missing ensemble_size".into()))?
        .parse()
        .map_err(|e: std::num::ParseIntError| MetadataError::SerializationError(format!("v1 ensemble_size: {e}")))?;
    let length: i64 = lines.next()
        .ok_or_else(|| MetadataError::SerializationError("v1: missing length".into()))?
        .parse()
        .map_err(|e: std::num::ParseIntError| MetadataError::SerializationError(format!("v1 length: {e}")))?;

    let mut builder = LedgerMetadataBuilder::new()
        .ledger_id(ledger_id)
        .metadata_format_version(1)
        .ensemble_size(ensemble_size)
        .write_quorum_size(quorum_size)
        .ack_quorum_size(quorum_size);

    for line in lines {
        let parts: Vec<&str> = line.split(V1_FIELD_SPLITTER).collect();
        if parts.len() < 2 {
            continue;
        }
        if parts[1] == V1_CLOSED_TAG {
            let entry_id: i64 = parts[0].parse()
                .map_err(|e: std::num::ParseIntError| MetadataError::SerializationError(format!("v1 closed entry: {e}")))?;
            if entry_id == V1_IN_RECOVERY_ENTRY_ID {
                builder = builder.with_in_recovery_state();
            } else {
                builder = builder.with_closed_state(entry_id, length);
            }
            break;
        }
        let first_entry_id: i64 = parts[0].parse()
            .map_err(|e: std::num::ParseIntError| MetadataError::SerializationError(format!("v1 entry_id: {e}")))?;
        let members: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();
        builder = builder.new_ensemble_entry(first_entry_id, members);
    }

    builder.build()
}

/// Parse v2 protobuf TextFormat. Matches Java's `parseVersion2Config`.
/// The body is the protobuf text representation of `LedgerMetadataFormat`.
fn parse_v2(ledger_id: i64, body: &[u8]) -> Result<LedgerMetadata, MetadataError> {
    let text = std::str::from_utf8(body)
        .map_err(|e| MetadataError::SerializationError(format!("v2 UTF-8 error: {e}")))?;

    // Parse protobuf TextFormat manually since prost doesn't support TextFormat.
    // Extract fields by line parsing — this is simpler than pulling in a full
    // protobuf TextFormat parser and handles the actual BK v2 metadata format.
    let mut ensemble_size: i32 = 0;
    let mut quorum_size: i32 = 0;
    let mut ack_quorum_size: Option<i32> = None;
    let mut state_val: i32 = 1; // OPEN
    let mut last_entry_id: i64 = -1;
    let mut length: i64 = 0;
    let mut ctime: Option<i64> = None;
    let mut password: Option<Vec<u8>> = None;
    let mut digest_type: Option<i32> = None;
    let mut segments: Vec<(i64, Vec<String>)> = Vec::new();

    // Simple state machine for parsing protobuf text format
    let mut in_segment = false;
    let mut seg_first_entry: i64 = 0;
    let mut seg_members: Vec<String> = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if line == "segment {" {
            in_segment = true;
            seg_first_entry = 0;
            seg_members.clear();
            continue;
        }
        if line == "}" {
            if in_segment {
                segments.push((seg_first_entry, std::mem::take(&mut seg_members)));
                in_segment = false;
            }
            continue;
        }

        if let Some((key, value)) = line.split_once(':') {
            let key = key.trim();
            let value = value.trim();
            if in_segment {
                match key {
                    "firstEntryId" => {
                        seg_first_entry = value.parse().map_err(|e| {
                            MetadataError::SerializationError(format!(
                                "parse firstEntryId='{value}': {e}"
                            ))
                        })?;
                    }
                    "ensembleMember" => { seg_members.push(value.trim_matches('"').to_string()); }
                    _ => {}
                }
            } else {
                match key {
                    "ensembleSize" => {
                        ensemble_size = value.parse().map_err(|e| {
                            MetadataError::SerializationError(format!(
                                "parse ensembleSize='{value}': {e}"
                            ))
                        })?;
                    }
                    "quorumSize" => {
                        quorum_size = value.parse().map_err(|e| {
                            MetadataError::SerializationError(format!(
                                "parse quorumSize='{value}': {e}"
                            ))
                        })?;
                    }
                    "ackQuorumSize" => { ack_quorum_size = value.parse().ok(); }
                    "state" => {
                        state_val = match value {
                            "OPEN" => 1,
                            "IN_RECOVERY" => 2,
                            "CLOSED" => 3,
                            other => other.parse().map_err(|e| {
                                MetadataError::SerializationError(format!(
                                    "parse state='{other}': {e}"
                                ))
                            })?,
                        };
                    }
                    "lastEntryId" => {
                        last_entry_id = value.parse().map_err(|e| {
                            MetadataError::SerializationError(format!(
                                "parse lastEntryId='{value}': {e}"
                            ))
                        })?;
                    }
                    "length" => {
                        length = value.parse().map_err(|e| {
                            MetadataError::SerializationError(format!(
                                "parse length='{value}': {e}"
                            ))
                        })?;
                    }
                    "ctime" => { ctime = value.parse().ok(); }
                    "digestType" => {
                        // Values must match proto: CRC32=1, HMAC=2, CRC32C=3, DUMMY=4
                        digest_type = Some(match value {
                            "CRC32" => 1,
                            "HMAC" => 2,
                            "CRC32C" | "CRC32_C" => 3,
                            "DUMMY" => 4,
                            other => other.parse().map_err(|e| {
                                MetadataError::SerializationError(format!(
                                    "parse digestType='{other}': {e}"
                                ))
                            })?,
                        });
                    }
                    "password" => {
                        let cleaned = value.trim_matches('"');
                        password = Some(cleaned.as_bytes().to_vec());
                    }
                    _ => {}
                }
            }
        }
    }

    let mut builder = LedgerMetadataBuilder::new()
        .ledger_id(ledger_id)
        .metadata_format_version(2)
        .ensemble_size(ensemble_size)
        .write_quorum_size(quorum_size)
        .ack_quorum_size(ack_quorum_size.unwrap_or(quorum_size));

    for (first_entry, members) in segments {
        builder = builder.new_ensemble_entry(first_entry, members);
    }

    if state_val == 2 {
        builder = builder.with_in_recovery_state();
    } else if state_val == 3 {
        builder = builder.with_closed_state(last_entry_id, length);
    }

    if let Some(pw) = password {
        // Match against proto values: CRC32=1, HMAC=2, CRC32C=3, DUMMY=4
        let dt = match digest_type {
            Some(1) => DigestType::Crc32,
            Some(2) => DigestType::Hmac,
            Some(3) => DigestType::Crc32c,
            Some(4) => DigestType::Dummy,
            _ => DigestType::Crc32,
        };
        builder = builder.with_digest_type(dt).with_password(pw);
    }

    if let Some(ct) = ctime {
        builder = builder.with_ctime(ct);
    }

    builder.build()
}

fn to_proto(md: &LedgerMetadata) -> bookkeeper_proto::LedgerMetadataFormat {
    use bookkeeper_proto::ledger_metadata_format as lmf;

    let state = match md.state() {
        LedgerState::Open => lmf::State::Open as i32,
        LedgerState::InRecovery => lmf::State::InRecovery as i32,
        LedgerState::Closed => lmf::State::Closed as i32,
    };

    let digest_type = md.digest_type().map(|dt| match dt {
        DigestType::Crc32 => lmf::DigestType::Crc32 as i32,
        DigestType::Crc32c => lmf::DigestType::Crc32c as i32,
        DigestType::Hmac => lmf::DigestType::Hmac as i32,
        DigestType::Dummy => lmf::DigestType::Dummy as i32,
    });

    let segments: Vec<lmf::Segment> = md.all_ensembles().iter().map(|(first_entry_id, members)| {
        lmf::Segment {
            ensemble_member: members.clone(),
            first_entry_id: *first_entry_id,
        }
    }).collect();

    let custom_metadata: Vec<lmf::CMetadataMapEntry> = md.custom_metadata().iter().map(|(k, v)| {
        lmf::CMetadataMapEntry {
            key: Some(k.clone()),
            value: Some(v.clone()),
        }
    }).collect();

    let (proto_digest_type, proto_password) = if let Some(pw) = md.password() {
        (digest_type, Some(pw.to_vec()))
    } else {
        (None, None)
    };

    bookkeeper_proto::LedgerMetadataFormat {
        quorum_size: md.write_quorum_size(),
        ensemble_size: md.ensemble_size(),
        length: md.length().unwrap_or(0),
        last_entry_id: Some(md.last_entry_id().unwrap_or(-1)),
        state,
        segment: segments,
        digest_type: proto_digest_type,
        password: proto_password,
        ack_quorum_size: Some(md.ack_quorum_size()),
        ctime: if md.store_ctime() { Some(md.ctime()) } else { None },
        custom_metadata,
        c_token: if md.c_token() != 0 { Some(md.c_token()) } else { None },
    }
}

fn from_proto(ledger_id: i64, proto: bookkeeper_proto::LedgerMetadataFormat) -> Result<LedgerMetadata, MetadataError> {
    use bookkeeper_proto::ledger_metadata_format as lmf;

    let mut builder = LedgerMetadataBuilder::new()
        .ledger_id(ledger_id)
        .metadata_format_version(3)
        .ensemble_size(proto.ensemble_size)
        .write_quorum_size(proto.quorum_size)
        .ack_quorum_size(proto.ack_quorum_size.unwrap_or(proto.quorum_size));

    // State (required field, plain i32 in prost generated code)
    let state = proto.state;
    if state == lmf::State::InRecovery as i32 {
        builder = builder.with_in_recovery_state();
    } else if state == lmf::State::Closed as i32 {
        builder = builder.with_closed_state(
            proto.last_entry_id.unwrap_or(-1),
            proto.length,
        );
    }

    // Ensembles — consume the vec directly to avoid cloning each member
    for seg in proto.segment {
        builder = builder.new_ensemble_entry(seg.first_entry_id, seg.ensemble_member);
    }

    // Digest — move password out of proto instead of cloning
    if let Some(password) = proto.password {
        let digest_type = match proto.digest_type {
            Some(dt) if dt == lmf::DigestType::Crc32 as i32 => DigestType::Crc32,
            Some(dt) if dt == lmf::DigestType::Crc32c as i32 => DigestType::Crc32c,
            Some(dt) if dt == lmf::DigestType::Hmac as i32 => DigestType::Hmac,
            Some(dt) if dt == lmf::DigestType::Dummy as i32 => DigestType::Dummy,
            _ => DigestType::Crc32,
        };
        builder = builder.with_digest_type(digest_type).with_password(password);
    }

    // Ctime
    if let Some(ctime) = proto.ctime {
        builder = builder.with_ctime(ctime);
    }

    // Custom metadata
    if !proto.custom_metadata.is_empty() {
        let mut map = std::collections::BTreeMap::new();
        for entry in proto.custom_metadata {
            if let (Some(k), Some(v)) = (entry.key, entry.value) {
                map.insert(k, v);
            }
        }
        builder = builder.with_custom_metadata(map);
    }

    // CToken
    if let Some(ct) = proto.c_token {
        builder = builder.with_c_token(ct);
    }

    builder.build()
}
