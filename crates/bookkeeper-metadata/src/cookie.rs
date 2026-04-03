use crate::ledger_metadata::MetadataError;

const CURRENT_COOKIE_LAYOUT_VERSION: i32 = 5;

#[derive(Debug, Clone)]
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
            index_dirs: index_dirs.map(|dirs| encode_dir_paths(dirs)),
        }
    }

    pub fn bookie_id(&self) -> &str { &self.bookie_id }
    pub fn journal_dirs(&self) -> &str { &self.journal_dirs }
    pub fn ledger_dirs(&self) -> &str { &self.ledger_dirs }
    pub fn instance_id(&self) -> Option<&str> { self.instance_id.as_deref() }
    pub fn index_dirs(&self) -> Option<&str> { self.index_dirs.as_deref() }

    /// Serialize to the v5 cookie format: version line + protobuf text format.
    pub fn serialize(&self) -> String {
        let mut s = format!("{}\n", CURRENT_COOKIE_LAYOUT_VERSION);
        s.push_str(&format!("bookie_host: \"{}\"\n", self.bookie_id));
        s.push_str(&format!("journal_dir: \"{}\"\n", self.journal_dirs));
        s.push_str(&format!("ledger_dirs: \"{}\"\n", self.ledger_dirs));
        if let Some(ref id) = self.instance_id {
            if !id.is_empty() {
                s.push_str(&format!("instance_id: \"{}\"\n", id));
            }
        }
        if let Some(ref dirs) = self.index_dirs {
            if !dirs.is_empty() {
                s.push_str(&format!("index_dirs: \"{}\"\n", dirs));
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
        if let Some(val) = extract_proto_text_field(line, "bookie_host") {
            bookie_id = val;
        } else if let Some(val) = extract_proto_text_field(line, "journal_dir") {
            journal_dirs = val;
        } else if let Some(val) = extract_proto_text_field(line, "ledger_dirs") {
            ledger_dirs = val;
        } else if let Some(val) = extract_proto_text_field(line, "instance_id") {
            if !val.is_empty() { instance_id = Some(val); }
        } else if let Some(val) = extract_proto_text_field(line, "index_dirs") {
            if !val.is_empty() { index_dirs = Some(val); }
        }
    }

    if bookie_id.is_empty() {
        return Err(MetadataError::SerializationError("missing bookie_host in cookie".into()));
    }

    Ok(Cookie { bookie_id, journal_dirs, ledger_dirs, instance_id, index_dirs })
}

fn extract_proto_text_field(line: &str, field_name: &str) -> Option<String> {
    let prefix = format!("{}: \"", field_name);
    if line.starts_with(&prefix) && line.ends_with('"') {
        Some(line[prefix.len()..line.len() - 1].to_string())
    } else {
        None
    }
}
