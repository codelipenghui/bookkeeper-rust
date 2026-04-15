//! Protobuf text format serialization compatible with Java's `Message.toString()`.
//!
//! Java BookKeeper stores underreplication data in ZooKeeper using protobuf text format
//! (not binary). This module provides serialization/deserialization that matches Java's
//! `com.google.protobuf.TextFormat` output exactly.

use bookkeeper_proto::{UnderreplicatedLedgerFormat, LockDataFormat, LedgerRereplicationLayoutFormat, AuditorVoteFormat};

// ---------------------------------------------------------------------------
// Serialize
// ---------------------------------------------------------------------------

pub fn serialize_underreplicated_ledger(msg: &UnderreplicatedLedgerFormat) -> Vec<u8> {
    let mut out = String::new();
    for r in &msg.replica {
        out.push_str(&format!("replica: \"{}\"\n", escape_proto_string(r)));
    }
    if let Some(ctime) = msg.ctime {
        out.push_str(&format!("ctime: {}\n", ctime));
    }
    out.into_bytes()
}

pub fn serialize_lock_data(msg: &LockDataFormat) -> Vec<u8> {
    let mut out = String::new();
    if let Some(ref bookie_id) = msg.bookie_id {
        out.push_str(&format!("bookieId: \"{}\"\n", escape_proto_string(bookie_id)));
    }
    out.into_bytes()
}

pub fn serialize_layout(msg: &LedgerRereplicationLayoutFormat) -> Vec<u8> {
    let mut out = String::new();
    out.push_str(&format!("type: \"{}\"\n", escape_proto_string(&msg.r#type)));
    out.push_str(&format!("version: {}\n", msg.version));
    out.into_bytes()
}

pub fn serialize_auditor_vote(msg: &AuditorVoteFormat) -> Vec<u8> {
    let mut out = String::new();
    if let Some(ref bookie_id) = msg.bookie_id {
        out.push_str(&format!("bookieId: \"{}\"\n", escape_proto_string(bookie_id)));
    }
    out.into_bytes()
}

// ---------------------------------------------------------------------------
// Deserialize
// ---------------------------------------------------------------------------

pub fn parse_underreplicated_ledger(data: &[u8]) -> Result<UnderreplicatedLedgerFormat, String> {
    let text = std::str::from_utf8(data).map_err(|e| format!("invalid utf8: {e}"))?;
    let mut replicas = Vec::new();
    let mut ctime = None;

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(value) = strip_field(line, "replica") {
            replicas.push(unquote_string(value)?);
        } else if let Some(value) = strip_field(line, "ctime") {
            ctime = Some(value.parse::<i64>().map_err(|e| format!("bad ctime: {e}"))?);
        }
    }

    Ok(UnderreplicatedLedgerFormat { replica: replicas, ctime })
}

pub fn parse_lock_data(data: &[u8]) -> Result<LockDataFormat, String> {
    let text = std::str::from_utf8(data).map_err(|e| format!("invalid utf8: {e}"))?;
    let mut bookie_id = None;

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(value) = strip_field(line, "bookieId") {
            bookie_id = Some(unquote_string(value)?);
        }
    }

    Ok(LockDataFormat { bookie_id })
}

pub fn parse_auditor_vote(data: &[u8]) -> Result<AuditorVoteFormat, String> {
    let text = std::str::from_utf8(data).map_err(|e| format!("invalid utf8: {e}"))?;
    let mut bookie_id = None;

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(value) = strip_field(line, "bookieId") {
            bookie_id = Some(unquote_string(value)?);
        }
    }

    Ok(AuditorVoteFormat { bookie_id })
}

pub fn parse_layout(data: &[u8]) -> Result<LedgerRereplicationLayoutFormat, String> {
    let text = std::str::from_utf8(data).map_err(|e| format!("invalid utf8: {e}"))?;
    let mut layout_type = None;
    let mut version = None;

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(value) = strip_field(line, "type") {
            layout_type = Some(unquote_string(value)?);
        } else if let Some(value) = strip_field(line, "version") {
            version = Some(value.parse::<i32>().map_err(|e| format!("bad version: {e}"))?);
        }
    }

    Ok(LedgerRereplicationLayoutFormat {
        r#type: layout_type.unwrap_or_default(),
        version: version.unwrap_or_default(),
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn strip_field<'a>(line: &'a str, field: &str) -> Option<&'a str> {
    line.strip_prefix(field)
        .and_then(|rest| rest.strip_prefix(':'))
        .map(|rest| rest.trim_start())
}

fn unquote_string(s: &str) -> Result<String, String> {
    let s = s.trim();
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        Ok(unescape_proto_string(&s[1..s.len() - 1]))
    } else {
        Err(format!("expected quoted string, got: {s}"))
    }
}

fn escape_proto_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out
}

fn unescape_proto_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => out.push('\\'),
                Some('"') => out.push('"'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('t') => out.push('\t'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_underreplicated_ledger_roundtrip() {
        let msg = UnderreplicatedLedgerFormat {
            replica: vec!["host1:3181".to_string(), "host2:3181".to_string()],
            ctime: Some(1234567890),
        };
        let data = serialize_underreplicated_ledger(&msg);
        let text = String::from_utf8(data.clone()).unwrap();
        assert_eq!(text, "replica: \"host1:3181\"\nreplica: \"host2:3181\"\nctime: 1234567890\n");

        let parsed = parse_underreplicated_ledger(&data).unwrap();
        assert_eq!(parsed.replica, msg.replica);
        assert_eq!(parsed.ctime, msg.ctime);
    }

    #[test]
    fn test_lock_data_roundtrip() {
        let msg = LockDataFormat {
            bookie_id: Some("myhost:3181".to_string()),
        };
        let data = serialize_lock_data(&msg);
        let text = String::from_utf8(data.clone()).unwrap();
        assert_eq!(text, "bookieId: \"myhost:3181\"\n");

        let parsed = parse_lock_data(&data).unwrap();
        assert_eq!(parsed.bookie_id, msg.bookie_id);
    }

    #[test]
    fn test_lock_data_empty() {
        let msg = LockDataFormat { bookie_id: None };
        let data = serialize_lock_data(&msg);
        assert_eq!(data, b"");
        let parsed = parse_lock_data(&data).unwrap();
        assert_eq!(parsed.bookie_id, None);
    }

    #[test]
    fn test_layout_roundtrip() {
        let msg = LedgerRereplicationLayoutFormat {
            r#type: "BASIC".to_string(),
            version: 1,
        };
        let data = serialize_layout(&msg);
        let text = String::from_utf8(data.clone()).unwrap();
        assert_eq!(text, "type: \"BASIC\"\nversion: 1\n");

        let parsed = parse_layout(&data).unwrap();
        assert_eq!(parsed.r#type, "BASIC");
        assert_eq!(parsed.version, 1);
    }

    #[test]
    fn test_underreplicated_no_ctime() {
        let msg = UnderreplicatedLedgerFormat {
            replica: vec!["host:3181".to_string()],
            ctime: None,
        };
        let data = serialize_underreplicated_ledger(&msg);
        let text = String::from_utf8(data.clone()).unwrap();
        assert_eq!(text, "replica: \"host:3181\"\n");

        let parsed = parse_underreplicated_ledger(&data).unwrap();
        assert_eq!(parsed.ctime, None);
    }
}
