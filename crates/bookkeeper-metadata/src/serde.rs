use prost::Message;

use crate::ledger_metadata::*;

const VERSION_HEADER: &[u8] = b"BookieMetadataFormatVersion\t";
const LINE_SPLITTER: u8 = b'\n';

/// Serialize LedgerMetadata to v3 binary protobuf format.
/// Format: "BookieMetadataFormatVersion\t3\n" + varint-length-prefixed protobuf.
pub fn serialize_metadata(md: &LedgerMetadata) -> Result<Vec<u8>, MetadataError> {
    let mut buf = Vec::new();

    // Write header
    buf.extend_from_slice(VERSION_HEADER);
    buf.extend_from_slice(b"3");
    buf.push(LINE_SPLITTER);

    // Build protobuf
    let proto = to_proto(md);

    // Write delimited (varint length prefix + message)
    let msg_len = proto.encoded_len();
    prost::encode_length_delimiter(msg_len, &mut buf)
        .map_err(|e| MetadataError::SerializationError(e.to_string()))?;
    proto.encode(&mut buf)
        .map_err(|e| MetadataError::SerializationError(e.to_string()))?;

    Ok(buf)
}

/// Deserialize LedgerMetadata from bytes (auto-detects version from header).
pub fn deserialize_metadata(ledger_id: i64, data: &[u8]) -> Result<LedgerMetadata, MetadataError> {
    let (version, body) = parse_header(data)?;
    match version {
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
    from_proto(ledger_id, &proto)
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

fn from_proto(ledger_id: i64, proto: &bookkeeper_proto::LedgerMetadataFormat) -> Result<LedgerMetadata, MetadataError> {
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

    // Ensembles
    for seg in &proto.segment {
        builder = builder.new_ensemble_entry(seg.first_entry_id, seg.ensemble_member.clone());
    }

    // Digest
    if let Some(password) = &proto.password {
        let digest_type = match proto.digest_type {
            Some(dt) if dt == lmf::DigestType::Crc32 as i32 => DigestType::Crc32,
            Some(dt) if dt == lmf::DigestType::Crc32c as i32 => DigestType::Crc32c,
            Some(dt) if dt == lmf::DigestType::Hmac as i32 => DigestType::Hmac,
            Some(dt) if dt == lmf::DigestType::Dummy as i32 => DigestType::Dummy,
            _ => DigestType::Crc32,
        };
        builder = builder.with_digest_type(digest_type).with_password(password.clone());
    }

    // Ctime
    if let Some(ctime) = proto.ctime {
        builder = builder.with_ctime(ctime);
    }

    // Custom metadata
    if !proto.custom_metadata.is_empty() {
        let mut map = std::collections::BTreeMap::new();
        for entry in &proto.custom_metadata {
            if let (Some(k), Some(v)) = (&entry.key, &entry.value) {
                map.insert(k.clone(), v.clone());
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
