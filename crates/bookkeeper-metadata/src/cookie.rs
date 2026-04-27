use crate::ledger_metadata::MetadataError;

const CURRENT_COOKIE_LAYOUT_VERSION: i32 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cookie {
    bookie_id: String,
    journal_dirs: String,
    ledger_dirs: String,
    instance_id: Option<String>,
    index_dirs: Option<String>,
}

impl Cookie {
    pub fn new(
        bookie_id: &str,
        journal_dirs: &str,
        ledger_dirs: &[&str],
        instance_id: Option<&str>,
        index_dirs: Option<&[&str]>,
    ) -> Self {
        Self {
            bookie_id: bookie_id.to_string(),
            journal_dirs: journal_dirs.to_string(),
            ledger_dirs: encode_dir_paths(ledger_dirs),
            instance_id: instance_id.map(|s| s.to_string()),
            index_dirs: index_dirs.map(encode_dir_paths),
        }
    }

    pub fn bookie_id(&self) -> &str { &self.bookie_id }
    pub fn journal_dirs(&self) -> &str { &self.journal_dirs }
    pub fn ledger_dirs(&self) -> &str { &self.ledger_dirs }
    pub fn instance_id(&self) -> Option<&str> { self.instance_id.as_deref() }
    pub fn index_dirs(&self) -> Option<&str> { self.index_dirs.as_deref() }

    /// Serialize to the v5 cookie format: version line + protobuf text format.
    ///
    /// String values are escaped to match Java's `TextFormat.printToString()`:
    /// tab → `\t`, backslash → `\\`, double-quote → `\"`, newline → `\n`.
    /// This is critical because `ledger_dirs` and `index_dirs` use tab characters
    /// as separators (via `encode_dir_paths`), and Java's protobuf TextFormat
    /// escapes them on write and unescapes them on read.
    pub fn serialize(&self) -> String {
        use std::fmt::Write;
        let mut s = String::with_capacity(256);
        writeln!(s, "{}", CURRENT_COOKIE_LAYOUT_VERSION).unwrap();
        writeln!(s, "bookieHost: \"{}\"", proto_text_escape(&self.bookie_id)).unwrap();
        writeln!(s, "journalDir: \"{}\"", proto_text_escape(&self.journal_dirs)).unwrap();
        writeln!(s, "ledgerDirs: \"{}\"", proto_text_escape(&self.ledger_dirs)).unwrap();
        if let Some(ref id) = self.instance_id {
            if !id.is_empty() {
                writeln!(s, "instanceId: \"{}\"", proto_text_escape(id)).unwrap();
            }
        }
        if let Some(ref dirs) = self.index_dirs {
            if !dirs.is_empty() {
                writeln!(s, "indexDirs: \"{}\"", proto_text_escape(dirs)).unwrap();
            }
        }
        s
    }

    /// Deserialize from cookie string (v4+ protobuf text or v3 plain text).
    pub fn deserialize(data: &str) -> Result<Self, MetadataError> {
        let mut lines = data.lines();
        let version_line = lines.next()
            .ok_or_else(|| MetadataError::SerializationError("empty cookie".into()))?;
        let version: i32 = version_line.trim().parse()
            .map_err(|e| MetadataError::SerializationError(format!("invalid cookie version: {e}")))?;

        if version >= 4 {
            // Parse protobuf text format manually
            let remaining: String = lines.collect::<Vec<_>>().join("\n");
            parse_proto_text_cookie(&remaining)
        } else if version == 3 {
            let bookie_id = lines.next()
                .ok_or_else(|| MetadataError::SerializationError("missing bookie_id".into()))?
                .to_string();
            let journal_dirs = lines.next()
                .ok_or_else(|| MetadataError::SerializationError("missing journal_dirs".into()))?
                .to_string();
            let ledger_dirs = lines.next()
                .ok_or_else(|| MetadataError::SerializationError("missing ledger_dirs".into()))?
                .to_string();
            Ok(Cookie {
                bookie_id,
                journal_dirs,
                ledger_dirs,
                instance_id: None,
                index_dirs: None,
            })
        } else {
            Err(MetadataError::SerializationError(format!("unsupported cookie version: {version}")))
        }
    }

    /// Validate this cookie against another (e.g., local vs ZK).
    /// Ledger/index dirs may be a superset (directory expansion is allowed).
    pub fn validate_against(&self, other: &Cookie) -> Result<(), MetadataError> {
        if self.bookie_id != other.bookie_id {
            return Err(MetadataError::InvalidConfig(
                format!("bookie_id mismatch: '{}' vs '{}'", self.bookie_id, other.bookie_id)));
        }
        if self.journal_dirs != other.journal_dirs {
            return Err(MetadataError::InvalidConfig(
                format!("journal_dirs mismatch: '{}' vs '{}'", self.journal_dirs, other.journal_dirs)));
        }
        // Ledger dirs: self may be a superset of other (expansion)
        if !is_superset_dirs(&self.ledger_dirs, &other.ledger_dirs) {
            return Err(MetadataError::InvalidConfig(
                format!("ledger_dirs mismatch: '{}' vs '{}'", self.ledger_dirs, other.ledger_dirs)));
        }
        // Index dirs: same superset rule as ledger_dirs. Missing index
        // dirs were silently accepted before, so a deployment that
        // dropped an index dir would pass validation and the bookie
        // would happily start without that dir's RocksDB available.
        match (&self.index_dirs, &other.index_dirs) {
            (Some(s), Some(o)) => {
                if !is_superset_dirs(s, o) {
                    return Err(MetadataError::InvalidConfig(format!(
                        "index_dirs mismatch: '{s}' vs '{o}'"
                    )));
                }
            }
            (None, Some(o)) => {
                return Err(MetadataError::InvalidConfig(format!(
                    "index_dirs missing locally; ZK cookie expects: '{o}'"
                )));
            }
            // (Some, None) is fine — adding index dirs to a previously
            // index-less deployment is allowed (matches the ledger-dirs
            // expansion rule).
            // (None, None) is also fine.
            _ => {}
        }
        // Instance ID must match if both present
        if let (Some(a), Some(b)) = (&self.instance_id, &other.instance_id) {
            if a != b {
                return Err(MetadataError::InvalidConfig(
                    format!("instance_id mismatch: '{}' vs '{}'", a, b)));
            }
        }
        Ok(())
    }
}

fn encode_dir_paths(dirs: &[&str]) -> String {
    let mut s = dirs.len().to_string();
    for d in dirs {
        s.push('\t');
        s.push_str(d);
    }
    s
}

fn decode_dir_paths(encoded: &str) -> Vec<&str> {
    if let Some(idx) = encoded.find('\t') {
        encoded[idx + 1..].split('\t').collect()
    } else {
        vec![]
    }
}

fn is_superset_dirs(a: &str, b: &str) -> bool {
    let dirs_a: std::collections::HashSet<&str> = decode_dir_paths(a).into_iter().collect();
    let dirs_b: std::collections::HashSet<&str> = decode_dir_paths(b).into_iter().collect();
    dirs_b.is_subset(&dirs_a)
}

fn parse_proto_text_cookie(text: &str) -> Result<Cookie, MetadataError> {
    let mut bookie_id = String::new();
    let mut journal_dirs = String::new();
    let mut ledger_dirs = String::new();
    let mut instance_id = None;
    let mut index_dirs = None;

    for line in text.lines() {
        let line = line.trim();
        if let Some(val) = extract_proto_text_field(line, "bookieHost") {
            bookie_id = val;
        } else if let Some(val) = extract_proto_text_field(line, "journalDir") {
            journal_dirs = val;
        } else if let Some(val) = extract_proto_text_field(line, "ledgerDirs") {
            ledger_dirs = val;
        } else if let Some(val) = extract_proto_text_field(line, "instanceId") {
            if !val.is_empty() { instance_id = Some(val); }
        } else if let Some(val) = extract_proto_text_field(line, "indexDirs") {
            if !val.is_empty() { index_dirs = Some(val); }
        }
    }

    if bookie_id.is_empty() {
        return Err(MetadataError::SerializationError("missing bookieHost in cookie".into()));
    }

    Ok(Cookie { bookie_id, journal_dirs, ledger_dirs, instance_id, index_dirs })
}

/// Extract a field value from a protobuf text format line.
/// Returns the unescaped string value.
fn extract_proto_text_field(line: &str, field_name: &str) -> Option<String> {
    let rest = line.strip_prefix(field_name)?;
    let rest = rest.strip_prefix(": \"")?;
    let raw = rest.strip_suffix('"')?;
    Some(proto_text_unescape(raw))
}

use bookkeeper_common::proto_text::{escape as proto_text_escape, unescape as proto_text_unescape};
