# Phase 2: Metadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the BookKeeper metadata layer — LedgerMetadata struct with v3 serialization, ZK path computation, cookie format, and the LedgerManager/MetadataDriver traits — enabling the Rust implementation to read and write metadata compatible with Java BookKeeper clusters.

**Architecture:** New `bookkeeper-metadata` crate with pure types and serialization (no ZK client dependency yet). LedgerMetadata is an immutable struct built via a builder pattern. Serialization uses the prost-generated `LedgerMetadataFormat` protobuf. ZK path computation is pure string manipulation. Cookie uses prost-generated `CookieFormat`. The `LedgerManager` and `MetadataDriver` are async traits that will be implemented with a real ZK backend in a later phase.

**Tech Stack:** `bookkeeper-proto` (protobuf types), `bookkeeper-common` (BookieId, LedgerId), `prost` (protobuf encoding), `bytes`, `thiserror`, `async-trait`

**Spec reference:** `docs/superpowers/specs/02-metadata.md`

---

## File Structure

```
crates/bookkeeper-metadata/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── ledger_metadata.rs        # LedgerMetadata struct + builder
    ├── serde.rs                  # Serialize/deserialize LedgerMetadata (v3 binary protobuf)
    ├── zk_path.rs                # ZK path computation (hierarchical ledger paths)
    ├── cookie.rs                 # Cookie format (serialize/deserialize)
    ├── versioned.rs              # Versioned<T> wrapper for optimistic concurrency
    └── traits.rs                 # LedgerManager, MetadataDriver, LayoutManager traits
```

---

### Task 1: Create bookkeeper-metadata Crate with LedgerMetadata Struct

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `crates/bookkeeper-metadata/Cargo.toml`
- Create: `crates/bookkeeper-metadata/src/lib.rs`
- Create: `crates/bookkeeper-metadata/src/ledger_metadata.rs`
- Test: `crates/bookkeeper-metadata/tests/ledger_metadata_test.rs`

- [ ] **Step 1: Add crate to workspace**

Add `"crates/bookkeeper-metadata"` to the `members` list in `Cargo.toml` and add to workspace dependencies:
```toml
bookkeeper-metadata = { path = "crates/bookkeeper-metadata" }
async-trait = "0.1"
```

- [ ] **Step 2: Create crate Cargo.toml**

`crates/bookkeeper-metadata/Cargo.toml`:
```toml
[package]
name = "bookkeeper-metadata"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
bookkeeper-proto = { workspace = true }
bookkeeper-common = { workspace = true }
prost = { workspace = true }
bytes = { workspace = true }
thiserror = { workspace = true }
async-trait = { workspace = true }
```

- [ ] **Step 3: Write failing tests**

`crates/bookkeeper-metadata/tests/ledger_metadata_test.rs`:
```rust
use bookkeeper_metadata::ledger_metadata::*;

#[test]
fn build_open_ledger() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(42)
        .ensemble_size(3)
        .write_quorum_size(2)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["bookie1:3181".into(), "bookie2:3181".into(), "bookie3:3181".into()])
        .build()
        .unwrap();

    assert_eq!(md.ledger_id(), 42);
    assert_eq!(md.ensemble_size(), 3);
    assert_eq!(md.write_quorum_size(), 2);
    assert_eq!(md.ack_quorum_size(), 1);
    assert_eq!(md.state(), LedgerState::Open);
    assert_eq!(md.all_ensembles().len(), 1);
    assert_eq!(md.ensemble_at(0).unwrap().len(), 3);
}

#[test]
fn build_closed_ledger() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(3)
        .write_quorum_size(3)
        .ack_quorum_size(2)
        .new_ensemble_entry(0, vec!["b1:3181".into(), "b2:3181".into(), "b3:3181".into()])
        .with_closed_state(99, 1024)
        .build()
        .unwrap();

    assert_eq!(md.state(), LedgerState::Closed);
    assert_eq!(md.last_entry_id(), Some(99));
    assert_eq!(md.length(), Some(1024));
}

#[test]
fn build_fails_without_ensemble() {
    let result = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .build();
    assert!(result.is_err());
}

#[test]
fn build_fails_invalid_quorum() {
    let result = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(2)
        .write_quorum_size(3)
        .new_ensemble_entry(0, vec!["b1:3181".into(), "b2:3181".into()])
        .build();
    assert!(result.is_err());
}

#[test]
fn ensemble_at_with_segments() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(3)
        .write_quorum_size(2)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["a:3181".into(), "b:3181".into(), "c:3181".into()])
        .new_ensemble_entry(100, vec!["a:3181".into(), "d:3181".into(), "c:3181".into()])
        .build()
        .unwrap();

    assert_eq!(md.all_ensembles().len(), 2);
    let e0 = md.ensemble_at(50).unwrap();
    assert_eq!(e0[1], "b:3181");
    let e100 = md.ensemble_at(100).unwrap();
    assert_eq!(e100[1], "d:3181");
    let e200 = md.ensemble_at(200).unwrap();
    assert_eq!(e200[1], "d:3181");
}

#[test]
fn with_digest_and_password() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(1)
        .write_quorum_size(1)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b:3181".into()])
        .with_digest_type(DigestType::Crc32c)
        .with_password(b"secret".to_vec())
        .build()
        .unwrap();

    assert_eq!(md.digest_type(), Some(DigestType::Crc32c));
    assert_eq!(md.password(), Some(&b"secret"[..]));
}

#[test]
fn with_custom_metadata() {
    let mut custom = std::collections::BTreeMap::new();
    custom.insert("app".to_string(), b"myapp".to_vec());
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(1)
        .write_quorum_size(1)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b:3181".into()])
        .with_custom_metadata(custom)
        .build()
        .unwrap();

    assert_eq!(md.custom_metadata().get("app").unwrap(), &b"myapp"[..]);
}
```

- [ ] **Step 4: Implement ledger_metadata.rs**

```rust
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedgerState {
    Open,
    InRecovery,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DigestType {
    Crc32,
    Crc32c,
    Hmac,
    Dummy,
}

/// Immutable ledger metadata.
#[derive(Debug, Clone)]
pub struct LedgerMetadata {
    ledger_id: i64,
    metadata_format_version: i32,
    ensemble_size: i32,
    write_quorum_size: i32,
    ack_quorum_size: i32,
    state: LedgerState,
    last_entry_id: Option<i64>,
    length: Option<i64>,
    ensembles: BTreeMap<i64, Vec<String>>,
    digest_type: Option<DigestType>,
    password: Option<Vec<u8>>,
    ctime: i64,
    store_ctime: bool,
    custom_metadata: BTreeMap<String, Vec<u8>>,
    c_token: i64,
}

impl LedgerMetadata {
    pub fn ledger_id(&self) -> i64 { self.ledger_id }
    pub fn metadata_format_version(&self) -> i32 { self.metadata_format_version }
    pub fn ensemble_size(&self) -> i32 { self.ensemble_size }
    pub fn write_quorum_size(&self) -> i32 { self.write_quorum_size }
    pub fn ack_quorum_size(&self) -> i32 { self.ack_quorum_size }
    pub fn state(&self) -> LedgerState { self.state }
    pub fn last_entry_id(&self) -> Option<i64> { self.last_entry_id }
    pub fn length(&self) -> Option<i64> { self.length }
    pub fn digest_type(&self) -> Option<DigestType> { self.digest_type }
    pub fn password(&self) -> Option<&[u8]> { self.password.as_deref() }
    pub fn ctime(&self) -> i64 { self.ctime }
    pub fn store_ctime(&self) -> bool { self.store_ctime }
    pub fn custom_metadata(&self) -> &BTreeMap<String, Vec<u8>> { &self.custom_metadata }
    pub fn c_token(&self) -> i64 { self.c_token }
    pub fn all_ensembles(&self) -> &BTreeMap<i64, Vec<String>> { &self.ensembles }

    /// Get the ensemble responsible for the given entry ID.
    /// Returns the ensemble from the highest segment whose firstEntryId <= entryId.
    pub fn ensemble_at(&self, entry_id: i64) -> Option<&Vec<String>> {
        self.ensembles.range(..=entry_id).next_back().map(|(_, v)| v)
    }
}

pub struct LedgerMetadataBuilder {
    ledger_id: i64,
    metadata_format_version: i32,
    ensemble_size: i32,
    write_quorum_size: i32,
    ack_quorum_size: i32,
    state: LedgerState,
    last_entry_id: Option<i64>,
    length: Option<i64>,
    ensembles: BTreeMap<i64, Vec<String>>,
    digest_type: Option<DigestType>,
    password: Option<Vec<u8>>,
    ctime: i64,
    store_ctime: bool,
    custom_metadata: BTreeMap<String, Vec<u8>>,
    c_token: i64,
}

impl LedgerMetadataBuilder {
    pub fn new() -> Self {
        Self {
            ledger_id: -1,
            metadata_format_version: 3,
            ensemble_size: 3,
            write_quorum_size: 3,
            ack_quorum_size: 2,
            state: LedgerState::Open,
            last_entry_id: None,
            length: None,
            ensembles: BTreeMap::new(),
            digest_type: None,
            password: None,
            ctime: -1,
            store_ctime: false,
            custom_metadata: BTreeMap::new(),
            c_token: 0,
        }
    }

    pub fn ledger_id(mut self, v: i64) -> Self { self.ledger_id = v; self }
    pub fn metadata_format_version(mut self, v: i32) -> Self { self.metadata_format_version = v; self }
    pub fn ensemble_size(mut self, v: i32) -> Self { self.ensemble_size = v; self }
    pub fn write_quorum_size(mut self, v: i32) -> Self { self.write_quorum_size = v; self }
    pub fn ack_quorum_size(mut self, v: i32) -> Self { self.ack_quorum_size = v; self }
    pub fn with_digest_type(mut self, v: DigestType) -> Self { self.digest_type = Some(v); self }
    pub fn with_password(mut self, v: Vec<u8>) -> Self { self.password = Some(v); self }
    pub fn with_ctime(mut self, v: i64) -> Self { self.ctime = v; self.store_ctime = true; self }
    pub fn with_custom_metadata(mut self, v: BTreeMap<String, Vec<u8>>) -> Self { self.custom_metadata = v; self }
    pub fn with_c_token(mut self, v: i64) -> Self { self.c_token = v; self }

    pub fn new_ensemble_entry(mut self, first_entry_id: i64, ensemble: Vec<String>) -> Self {
        self.ensembles.insert(first_entry_id, ensemble);
        self
    }

    pub fn with_in_recovery_state(mut self) -> Self {
        self.state = LedgerState::InRecovery;
        self
    }

    pub fn with_closed_state(mut self, last_entry_id: i64, length: i64) -> Self {
        self.state = LedgerState::Closed;
        self.last_entry_id = Some(last_entry_id);
        self.length = Some(length);
        self
    }

    pub fn build(self) -> Result<LedgerMetadata, MetadataError> {
        if self.ledger_id < 0 {
            return Err(MetadataError::InvalidConfig("ledger_id must be >= 0".into()));
        }
        if self.ensemble_size < self.write_quorum_size {
            return Err(MetadataError::InvalidConfig(
                format!("ensemble_size ({}) must be >= write_quorum_size ({})",
                        self.ensemble_size, self.write_quorum_size)));
        }
        if self.write_quorum_size < self.ack_quorum_size {
            return Err(MetadataError::InvalidConfig(
                format!("write_quorum_size ({}) must be >= ack_quorum_size ({})",
                        self.write_quorum_size, self.ack_quorum_size)));
        }
        if self.ensembles.is_empty() {
            return Err(MetadataError::InvalidConfig("at least one ensemble segment required".into()));
        }
        for (_, ensemble) in &self.ensembles {
            if ensemble.len() != self.ensemble_size as usize {
                return Err(MetadataError::InvalidConfig(
                    format!("ensemble has {} members but ensemble_size is {}",
                            ensemble.len(), self.ensemble_size)));
            }
        }
        if self.state == LedgerState::Closed && (self.last_entry_id.is_none() || self.length.is_none()) {
            return Err(MetadataError::InvalidConfig(
                "CLOSED state requires last_entry_id and length".into()));
        }
        Ok(LedgerMetadata {
            ledger_id: self.ledger_id,
            metadata_format_version: self.metadata_format_version,
            ensemble_size: self.ensemble_size,
            write_quorum_size: self.write_quorum_size,
            ack_quorum_size: self.ack_quorum_size,
            state: self.state,
            last_entry_id: self.last_entry_id,
            length: self.length,
            ensembles: self.ensembles,
            digest_type: self.digest_type,
            password: self.password,
            ctime: self.ctime,
            store_ctime: self.store_ctime,
            custom_metadata: self.custom_metadata,
            c_token: self.c_token,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("serialization error: {0}")]
    SerializationError(String),
    #[error("unknown metadata version: {0}")]
    UnknownVersion(i32),
}
```

- [ ] **Step 5: Create lib.rs**

```rust
pub mod ledger_metadata;
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p bookkeeper-metadata`
Expected: All 7 tests pass

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates/bookkeeper-metadata/
git commit -m "feat: add LedgerMetadata struct with builder pattern and validation

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: LedgerMetadata Serialization (V3 Binary Protobuf)

**Files:**
- Create: `crates/bookkeeper-metadata/src/serde.rs`
- Modify: `crates/bookkeeper-metadata/src/lib.rs`
- Test: `crates/bookkeeper-metadata/tests/serde_test.rs`

- [ ] **Step 1: Write failing tests**

`crates/bookkeeper-metadata/tests/serde_test.rs`:
```rust
use bookkeeper_metadata::ledger_metadata::*;
use bookkeeper_metadata::serde::{serialize_metadata, deserialize_metadata};

#[test]
fn v3_round_trip_open_ledger() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(42)
        .ensemble_size(3)
        .write_quorum_size(2)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b1:3181".into(), "b2:3181".into(), "b3:3181".into()])
        .with_digest_type(DigestType::Crc32c)
        .with_password(b"test".to_vec())
        .build()
        .unwrap();

    let bytes = serialize_metadata(&md).unwrap();
    let parsed = deserialize_metadata(42, &bytes).unwrap();

    assert_eq!(parsed.ledger_id(), 42);
    assert_eq!(parsed.ensemble_size(), 3);
    assert_eq!(parsed.write_quorum_size(), 2);
    assert_eq!(parsed.ack_quorum_size(), 1);
    assert_eq!(parsed.state(), LedgerState::Open);
    assert_eq!(parsed.digest_type(), Some(DigestType::Crc32c));
    assert_eq!(parsed.password(), Some(&b"test"[..]));
    assert_eq!(parsed.all_ensembles().len(), 1);
}

#[test]
fn v3_round_trip_closed_ledger() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(3)
        .write_quorum_size(3)
        .ack_quorum_size(2)
        .new_ensemble_entry(0, vec!["a:3181".into(), "b:3181".into(), "c:3181".into()])
        .new_ensemble_entry(50, vec!["a:3181".into(), "d:3181".into(), "c:3181".into()])
        .with_closed_state(99, 4096)
        .with_digest_type(DigestType::Hmac)
        .with_password(b"pw".to_vec())
        .build()
        .unwrap();

    let bytes = serialize_metadata(&md).unwrap();
    let parsed = deserialize_metadata(1, &bytes).unwrap();

    assert_eq!(parsed.state(), LedgerState::Closed);
    assert_eq!(parsed.last_entry_id(), Some(99));
    assert_eq!(parsed.length(), Some(4096));
    assert_eq!(parsed.all_ensembles().len(), 2);
    assert_eq!(parsed.ensemble_at(0).unwrap()[1], "b:3181");
    assert_eq!(parsed.ensemble_at(50).unwrap()[1], "d:3181");
}

#[test]
fn v3_header_starts_with_version_string() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(1)
        .write_quorum_size(1)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b:3181".into()])
        .build()
        .unwrap();

    let bytes = serialize_metadata(&md).unwrap();
    let header = "BookieMetadataFormatVersion\t3\n";
    assert_eq!(&bytes[..header.len()], header.as_bytes());
}

#[test]
fn v3_with_custom_metadata() {
    let mut custom = std::collections::BTreeMap::new();
    custom.insert("key1".to_string(), b"val1".to_vec());
    custom.insert("key2".to_string(), b"val2".to_vec());

    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(1)
        .write_quorum_size(1)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b:3181".into()])
        .with_custom_metadata(custom)
        .build()
        .unwrap();

    let bytes = serialize_metadata(&md).unwrap();
    let parsed = deserialize_metadata(1, &bytes).unwrap();
    assert_eq!(parsed.custom_metadata().get("key1").unwrap(), &b"val1"[..]);
    assert_eq!(parsed.custom_metadata().get("key2").unwrap(), &b"val2"[..]);
}

#[test]
fn v3_with_ctime_and_ctoken() {
    let md = LedgerMetadataBuilder::new()
        .ledger_id(1)
        .ensemble_size(1)
        .write_quorum_size(1)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b:3181".into()])
        .with_ctime(1234567890)
        .with_c_token(999)
        .build()
        .unwrap();

    let bytes = serialize_metadata(&md).unwrap();
    let parsed = deserialize_metadata(1, &bytes).unwrap();
    assert_eq!(parsed.ctime(), 1234567890);
    assert_eq!(parsed.c_token(), 999);
}
```

- [ ] **Step 2: Implement serde.rs**

```rust
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
        .map_err(|e| MetadataError::SerializationError(format!("invalid version: {e}")))?;

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

    bookkeeper_proto::LedgerMetadataFormat {
        quorum_size: md.write_quorum_size(),
        ensemble_size: md.ensemble_size(),
        length: md.length().unwrap_or(0),
        last_entry_id: md.last_entry_id(),
        state: Some(state),
        segment: segments,
        digest_type,
        password: md.password().map(|p| p.to_vec()),
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

    // State
    let state = proto.state.unwrap_or(lmf::State::Open as i32);
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
```

- [ ] **Step 3: Add module to lib.rs**

```rust
pub mod ledger_metadata;
pub mod serde;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p bookkeeper-metadata`
Expected: All 12 tests pass (7 from task 1 + 5 new)

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-metadata/
git commit -m "feat: add LedgerMetadata v3 binary protobuf serialization

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: ZooKeeper Path Computation

**Files:**
- Create: `crates/bookkeeper-metadata/src/zk_path.rs`
- Test: `crates/bookkeeper-metadata/tests/zk_path_test.rs`

- [ ] **Step 1: Write failing tests**

`crates/bookkeeper-metadata/tests/zk_path_test.rs`:
```rust
use bookkeeper_metadata::zk_path::*;

#[test]
fn short_path_ledger_1() {
    assert_eq!(ledger_path("/ledgers", 1), "/ledgers/00/0000/L0001");
}

#[test]
fn short_path_ledger_12345678() {
    assert_eq!(ledger_path("/ledgers", 12345678), "/ledgers/00/1234/L5678");
}

#[test]
fn short_path_max_int_minus_1() {
    // 2^31 - 2 = 2147483646
    assert_eq!(ledger_path("/ledgers", 2147483646), "/ledgers/21/4748/L3646");
}

#[test]
fn long_path_max_int() {
    // 2^31 - 1 = 2147483647 → uses long path
    let path = ledger_path("/ledgers", 2147483647);
    assert_eq!(path, "/ledgers/000/0021/4748/3647/L");
    // Wait — let me recalculate. 2147483647 → 19-digit: "0000000002147483647"
    // split 3-4-4-4-4: "000" / "0000" / "0021" / "4748" / "3647"
    // path: /000/0000/0021/4748/L3647
}

#[test]
fn long_path_large_id() {
    // 4294967296 → "0000000004294967296" → 000/0000/0042/9496/L7296
    assert_eq!(
        ledger_path("/ledgers", 4294967296),
        "/ledgers/000/0000/0042/9496/L7296"
    );
}

#[test]
fn underreplication_path() {
    // ledgerId = 0x00010002000300040005 would be huge, let's use a simpler one
    // ledgerId = 0x0001_0002_0003_0004 = 281483566841860
    let path = underreplication_ledger_path("/ledgers/underreplication/ledgers", 281483566841860);
    assert_eq!(path, "/ledgers/underreplication/ledgers/0001/0002/0003/0004/urL281483566841860");
}

#[test]
fn underreplication_path_zero() {
    let path = underreplication_ledger_path("/ledgers/underreplication/ledgers", 0);
    assert_eq!(path, "/ledgers/underreplication/ledgers/0000/0000/0000/0000/urL0");
}

#[test]
fn base_paths() {
    let paths = ZkPaths::new("/ledgers");
    assert_eq!(paths.available(), "/ledgers/available");
    assert_eq!(paths.available_readonly(), "/ledgers/available/readonly");
    assert_eq!(paths.cookies(), "/ledgers/cookies");
    assert_eq!(paths.layout(), "/ledgers/LAYOUT");
    assert_eq!(paths.instance_id(), "/ledgers/instanceid");
    assert_eq!(paths.idgen_long(), "/ledgers/idgen-long");
}
```

- [ ] **Step 2: Implement zk_path.rs**

```rust
/// Compute the ZK path for a ledger ID using the hybrid hierarchical scheme.
/// For ledgerId < i32::MAX (2^31 - 1): 3-level (short) hierarchy with 2-4-4 split.
/// For ledgerId >= i32::MAX: 5-level (long) hierarchy with 3-4-4-4-4 split.
pub fn ledger_path(root: &str, ledger_id: i64) -> String {
    if ledger_id < i32::MAX as i64 {
        short_hierarchical_path(root, ledger_id)
    } else {
        long_hierarchical_path(root, ledger_id)
    }
}

fn short_hierarchical_path(root: &str, ledger_id: i64) -> String {
    let s = format!("{:010}", ledger_id);
    format!("{}/{}/{}/L{}", root, &s[0..2], &s[2..6], &s[6..10])
}

fn long_hierarchical_path(root: &str, ledger_id: i64) -> String {
    let s = format!("{:019}", ledger_id);
    format!("{}/{}/{}/{}/{}/L{}", root, &s[0..3], &s[3..7], &s[7..11], &s[11..15], &s[15..19])
}

/// Compute the ZK path for an underreplicated ledger marker.
/// Uses 4-level hex hierarchy from 64-bit ledger ID.
pub fn underreplication_ledger_path(base: &str, ledger_id: i64) -> String {
    let id = ledger_id as u64;
    format!(
        "{}/{:04x}/{:04x}/{:04x}/{:04x}/urL{}",
        base,
        (id >> 48) & 0xFFFF,
        (id >> 32) & 0xFFFF,
        (id >> 16) & 0xFFFF,
        id & 0xFFFF,
        ledger_id,
    )
}

/// Well-known ZK paths relative to the ledger root.
pub struct ZkPaths {
    root: String,
}

impl ZkPaths {
    pub fn new(root: &str) -> Self {
        Self { root: root.to_string() }
    }

    pub fn root(&self) -> &str { &self.root }
    pub fn available(&self) -> String { format!("{}/available", self.root) }
    pub fn available_readonly(&self) -> String { format!("{}/available/readonly", self.root) }
    pub fn cookies(&self) -> String { format!("{}/cookies", self.root) }
    pub fn cookie_path(&self, bookie_id: &str) -> String { format!("{}/cookies/{}", self.root, bookie_id) }
    pub fn layout(&self) -> String { format!("{}/LAYOUT", self.root) }
    pub fn instance_id(&self) -> String { format!("{}/instanceid", self.root) }
    pub fn idgen(&self) -> String { format!("{}/idgen", self.root) }
    pub fn idgen_long(&self) -> String { format!("{}/idgen-long", self.root) }
    pub fn underreplication(&self) -> String { format!("{}/underreplication", self.root) }
    pub fn bookie_registration(&self, bookie_id: &str) -> String {
        format!("{}/available/{}", self.root, bookie_id)
    }
    pub fn bookie_readonly_registration(&self, bookie_id: &str) -> String {
        format!("{}/available/readonly/{}", self.root, bookie_id)
    }
}
```

- [ ] **Step 3: Fix the test for long_path_max_int**

After implementing, check what `ledger_path("/ledgers", 2147483647)` produces. `i32::MAX = 2147483647`. Since the condition is `< i32::MAX`, value `2147483647` uses the LONG path:
- `format!("{:019}", 2147483647)` = `"0000000002147483647"`
- Split 3-4-4-4-4: `"000"/"0000"/"0021"/"4748"/"3647"`
- Path: `/ledgers/000/0000/0021/4748/L3647`

Update the test accordingly.

- [ ] **Step 4: Add module to lib.rs**

```rust
pub mod ledger_metadata;
pub mod serde;
pub mod zk_path;
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p bookkeeper-metadata`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add crates/bookkeeper-metadata/
git commit -m "feat: add ZK path computation (hierarchical ledger paths, underreplication)

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Cookie Format

**Files:**
- Create: `crates/bookkeeper-metadata/src/cookie.rs`
- Test: `crates/bookkeeper-metadata/tests/cookie_test.rs`

- [ ] **Step 1: Write failing tests**

`crates/bookkeeper-metadata/tests/cookie_test.rs`:
```rust
use bookkeeper_metadata::cookie::Cookie;

#[test]
fn cookie_round_trip() {
    let cookie = Cookie::new(
        "bookie1:3181",
        "/data/journal",
        &["/data/ledgers"],
        Some("test-instance-id"),
        None,
    );

    let serialized = cookie.serialize();
    let parsed = Cookie::deserialize(&serialized).unwrap();

    assert_eq!(parsed.bookie_id(), "bookie1:3181");
    assert_eq!(parsed.journal_dirs(), "/data/journal");
    assert_eq!(parsed.instance_id(), Some("test-instance-id"));
}

#[test]
fn cookie_with_multiple_ledger_dirs() {
    let cookie = Cookie::new(
        "host:3181",
        "/journal",
        &["/ledgers1", "/ledgers2", "/ledgers3"],
        None,
        None,
    );

    let serialized = cookie.serialize();
    assert!(serialized.starts_with("5\n"));
    let parsed = Cookie::deserialize(&serialized).unwrap();
    assert_eq!(parsed.ledger_dirs(), "3\t/ledgers1\t/ledgers2\t/ledgers3");
}

#[test]
fn cookie_with_index_dirs() {
    let cookie = Cookie::new(
        "host:3181",
        "/journal",
        &["/ledgers"],
        Some("uuid-123"),
        Some(&["/index1", "/index2"]),
    );

    let serialized = cookie.serialize();
    let parsed = Cookie::deserialize(&serialized).unwrap();
    assert_eq!(parsed.index_dirs(), Some("2\t/index1\t/index2"));
}

#[test]
fn cookie_starts_with_version_5() {
    let cookie = Cookie::new("b:3181", "/j", &["/l"], None, None);
    let s = cookie.serialize();
    assert!(s.starts_with("5\n"));
}

#[test]
fn cookie_validate_match() {
    let a = Cookie::new("b:3181", "/j", &["/l"], Some("id1"), None);
    let b = Cookie::new("b:3181", "/j", &["/l"], Some("id1"), None);
    assert!(a.validate_against(&b).is_ok());
}

#[test]
fn cookie_validate_bookie_id_mismatch() {
    let a = Cookie::new("b1:3181", "/j", &["/l"], None, None);
    let b = Cookie::new("b2:3181", "/j", &["/l"], None, None);
    assert!(a.validate_against(&b).is_err());
}

#[test]
fn cookie_validate_ledger_dirs_superset_ok() {
    let a = Cookie::new("b:3181", "/j", &["/l1", "/l2"], None, None);
    let b = Cookie::new("b:3181", "/j", &["/l1"], None, None);
    // b is a subset of a — expansion is allowed
    assert!(a.validate_against(&b).is_ok());
}
```

- [ ] **Step 2: Implement cookie.rs**

```rust
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
        let mut builder = bookkeeper_proto::CookieFormat {
            bookie_host: self.bookie_id.clone(),
            journal_dir: self.journal_dirs.clone(),
            ledger_dirs: self.ledger_dirs.clone(),
            instance_id: self.instance_id.clone().unwrap_or_default(),
            index_dirs: self.index_dirs.clone().unwrap_or_default(),
        };
        // prost doesn't have TextFormat. We manually format to match Java's toString().
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
```

- [ ] **Step 3: Add module to lib.rs**

```rust
pub mod ledger_metadata;
pub mod serde;
pub mod zk_path;
pub mod cookie;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p bookkeeper-metadata`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-metadata/
git commit -m "feat: add Cookie format serialization, deserialization, and validation

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Versioned Wrapper and Manager Traits

**Files:**
- Create: `crates/bookkeeper-metadata/src/versioned.rs`
- Create: `crates/bookkeeper-metadata/src/traits.rs`
- Test: `crates/bookkeeper-metadata/tests/versioned_test.rs`

- [ ] **Step 1: Write failing tests**

`crates/bookkeeper-metadata/tests/versioned_test.rs`:
```rust
use bookkeeper_metadata::versioned::Versioned;

#[test]
fn versioned_wraps_value_and_version() {
    let v = Versioned::new("hello", 5);
    assert_eq!(v.value(), &"hello");
    assert_eq!(v.version(), 5);
}

#[test]
fn versioned_map() {
    let v = Versioned::new(42, 1);
    let v2 = v.map(|x| x * 2);
    assert_eq!(v2.value(), &84);
    assert_eq!(v2.version(), 1);
}
```

- [ ] **Step 2: Implement versioned.rs**

```rust
/// A value paired with a version for optimistic concurrency control.
/// The version is opaque — for ZK, it's the znode stat version (i32).
#[derive(Debug, Clone)]
pub struct Versioned<T> {
    value: T,
    version: i32,
}

impl<T> Versioned<T> {
    pub fn new(value: T, version: i32) -> Self {
        Self { value, version }
    }

    pub fn value(&self) -> &T { &self.value }
    pub fn into_value(self) -> T { self.value }
    pub fn version(&self) -> i32 { self.version }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Versioned<U> {
        Versioned { value: f(self.value), version: self.version }
    }
}
```

- [ ] **Step 3: Implement traits.rs**

```rust
use async_trait::async_trait;

use crate::ledger_metadata::{LedgerMetadata, MetadataError};
use crate::versioned::Versioned;

/// The primary interface for ledger metadata operations.
/// All write operations use compare-and-swap via the version parameter.
#[async_trait]
pub trait LedgerManager: Send + Sync {
    async fn create_ledger_metadata(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError>;

    async fn read_ledger_metadata(
        &self,
        ledger_id: i64,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError>;

    async fn write_ledger_metadata(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
        current_version: i32,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError>;

    async fn remove_ledger_metadata(
        &self,
        ledger_id: i64,
        version: i32,
    ) -> Result<(), MetadataError>;
}

/// Generates unique ledger IDs.
#[async_trait]
pub trait LedgerIdGenerator: Send + Sync {
    async fn generate_ledger_id(&self) -> Result<i64, MetadataError>;
}

/// Layout information stored in ZK to identify the metadata format.
#[derive(Debug, Clone)]
pub struct LedgerLayout {
    pub manager_factory_class: String,
    pub manager_version: i32,
}

/// Manages the cluster metadata layout.
#[async_trait]
pub trait LayoutManager: Send + Sync {
    async fn read_ledger_layout(&self) -> Result<Option<LedgerLayout>, MetadataError>;
    async fn store_ledger_layout(&self, layout: &LedgerLayout) -> Result<(), MetadataError>;
    async fn delete_ledger_layout(&self) -> Result<(), MetadataError>;
}

/// Server-side metadata driver (used by bookies).
#[async_trait]
pub trait MetadataBookieDriver: Send + Sync {
    fn get_layout_manager(&self) -> &dyn LayoutManager;
    async fn close(&self);
}

/// Client-side metadata driver (used by BK clients).
#[async_trait]
pub trait MetadataClientDriver: Send + Sync {
    fn get_ledger_manager(&self) -> &dyn LedgerManager;
    fn get_ledger_id_generator(&self) -> &dyn LedgerIdGenerator;
    fn get_layout_manager(&self) -> &dyn LayoutManager;
    async fn close(&self);
}
```

- [ ] **Step 4: Update lib.rs**

```rust
pub mod ledger_metadata;
pub mod serde;
pub mod zk_path;
pub mod cookie;
pub mod versioned;
pub mod traits;
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p bookkeeper-metadata`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add crates/bookkeeper-metadata/
git commit -m "feat: add Versioned<T> wrapper and LedgerManager/MetadataDriver async traits

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## What's Next

This plan covers **Phase 2: Metadata** (types, serialization, paths, cookies, traits). The traits are defined but not yet implemented with a real ZK backend. The ZK implementation will come in **Phase 4 (Bookie Server)** and **Phase 5 (Bookie Registration)** when the actual ZK client is integrated.

| Phase | Scope | Status |
|-------|-------|--------|
| Phase 1 | Foundation + Wire Protocol | Complete |
| Phase 2 | Metadata | This plan |
| Phase 3 | Storage Engine | Next |
