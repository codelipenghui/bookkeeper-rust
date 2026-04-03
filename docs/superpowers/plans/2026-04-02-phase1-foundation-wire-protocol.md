# Phase 1: Foundation + Wire Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Set up the Rust workspace and implement the complete BookKeeper wire protocol codec (v2 binary + v3 protobuf) with entry format, digest computation, and TCP framing — enabling communication with Java BookKeeper 4.18.0 bookies.

**Architecture:** Cargo workspace with 3 crates: `bookkeeper-proto` (protobuf codegen), `bookkeeper-common` (shared types, errors), and `bookkeeper-protocol` (wire protocol codec, digest, entry format, TCP framing). The protocol crate uses `bytes` for zero-copy buffer management and `tokio-util` for the TCP framing codec.

**Tech Stack:** Rust 2021 edition, `prost` + `prost-build` (protobuf), `bytes` (buffers), `crc32fast` + `crc32c` (checksums), `hmac` + `sha1` (HMAC-SHA1), `tokio` + `tokio-util` (async I/O + codec), `thiserror` (errors)

**Spec references:** `docs/superpowers/specs/01-wire-protocol.md`, `docs/superpowers/specs/2026-04-02-bookkeeper-spec-design.md` §3.2 (entry format)

***

## File Structure

```
bookkeeper-rust/
├── Cargo.toml                              # Workspace root
├── crates/
│   ├── bookkeeper-proto/                   # Protobuf codegen
│   │   ├── Cargo.toml
│   │   ├── build.rs                        # prost-build script
│   │   ├── proto/
│   │   │   ├── BookkeeperProtocol.proto    # Copied from Java project
│   │   │   ├── DataFormats.proto           # Copied from Java project
│   │   │   └── DbLedgerStorageDataFormats.proto
│   │   └── src/lib.rs                      # Re-exports generated code
│   ├── bookkeeper-common/                  # Shared types and errors
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── types.rs                    # BookieId, LedgerId, EntryId
│   │       └── error.rs                    # BKError enum
│   └── bookkeeper-protocol/                # Wire protocol codec
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs
│           ├── constants.rs                # Protocol constants
│           ├── digest.rs                   # CRC32, CRC32C, HMAC, DUMMY
│           ├── entry.rs                    # Entry format serialize/deserialize
│           ├── v2/
│           │   ├── mod.rs
│           │   ├── header.rs               # PacketHeader encode/decode
│           │   ├── request.rs              # Pre-v3 request encode/decode
│           │   └── response.rs             # Pre-v3 response encode/decode
│           ├── v3.rs                       # V3 protobuf request/response wrappers
│           └── framing.rs                  # TCP length-prefixed codec
└── tests/
    └── wire_compat.rs                      # Wire compatibility integration tests
```

***

### Task 1: Initialize Cargo Workspace

**Files:**

* Create: `Cargo.toml`

* Create: `crates/bookkeeper-proto/Cargo.toml`

* Create: `crates/bookkeeper-common/Cargo.toml`

* Create: `crates/bookkeeper-protocol/Cargo.toml`

* Create: `crates/bookkeeper-proto/src/lib.rs`

* Create: `crates/bookkeeper-common/src/lib.rs`

* Create: `crates/bookkeeper-protocol/src/lib.rs`

* [ ] **Step 1: Create workspace root Cargo.toml**

```toml
[workspace]
resolver = "2"
members = [
    "crates/bookkeeper-proto",
    "crates/bookkeeper-common",
    "crates/bookkeeper-protocol",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "Apache-2.0"

[workspace.dependencies]
bookkeeper-proto = { path = "crates/bookkeeper-proto" }
bookkeeper-common = { path = "crates/bookkeeper-common" }
bookkeeper-protocol = { path = "crates/bookkeeper-protocol" }

bytes = "1"
prost = "0.13"
prost-build = "0.13"
thiserror = "2"
crc32fast = "1"
crc32c = "0.6"
hmac = "0.12"
sha1 = "0.10"
tokio = { version = "1", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec"] }
```

* [ ] **Step 2: Create bookkeeper-proto Cargo.toml**

```toml
[package]
name = "bookkeeper-proto"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
prost = { workspace = true }

[build-dependencies]
prost-build = { workspace = true }
```

* [ ] **Step 3: Create bookkeeper-common Cargo.toml**

```toml
[package]
name = "bookkeeper-common"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
thiserror = { workspace = true }
```

* [ ] **Step 4: Create bookkeeper-protocol Cargo.toml**

```toml
[package]
name = "bookkeeper-protocol"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
bookkeeper-proto = { workspace = true }
bookkeeper-common = { workspace = true }
bytes = { workspace = true }
prost = { workspace = true }
crc32fast = { workspace = true }
crc32c = { workspace = true }
hmac = { workspace = true }
sha1 = { workspace = true }
tokio-util = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
```

* [ ] **Step 5: Create minimal lib.rs stubs**

`crates/bookkeeper-proto/src/lib.rs`:

```rust
// Generated protobuf code will be included here
```

`crates/bookkeeper-common/src/lib.rs`:

```rust
pub mod error;
pub mod types;
```

`crates/bookkeeper-common/src/error.rs`:

```rust
// Placeholder — populated in Task 3
```

`crates/bookkeeper-common/src/types.rs`:

```rust
// Placeholder — populated in Task 3
```

`crates/bookkeeper-protocol/src/lib.rs`:

```rust
pub mod constants;
```

`crates/bookkeeper-protocol/src/constants.rs`:

```rust
// Placeholder — populated in Task 4
```

* [ ] **Step 6: Verify workspace builds**

Run: `cargo check`
Expected: Compiles with no errors

* [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates/
git commit -m "feat: initialize Cargo workspace with proto, common, protocol crates"
```

***

### Task 2: Set Up Protobuf Codegen

**Files:**

* Create: `crates/bookkeeper-proto/proto/BookkeeperProtocol.proto`

* Create: `crates/bookkeeper-proto/proto/DataFormats.proto`

* Create: `crates/bookkeeper-proto/proto/DbLedgerStorageDataFormats.proto`

* Create: `crates/bookkeeper-proto/build.rs`

* Modify: `crates/bookkeeper-proto/src/lib.rs`

* [ ] **Step 1: Copy proto files from Java project**

```bash
cp /Users/lipenghui/Github/apache/bookkeeper/bookkeeper-proto/src/main/proto/BookkeeperProtocol.proto crates/bookkeeper-proto/proto/
cp /Users/lipenghui/Github/apache/bookkeeper/bookkeeper-proto/src/main/proto/DataFormats.proto crates/bookkeeper-proto/proto/
cp /Users/lipenghui/Github/apache/bookkeeper/bookkeeper-proto/src/main/proto/DbLedgerStorageDataFormats.proto crates/bookkeeper-proto/proto/
```

* [ ] **Step 2: Create build.rs**

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(
        &[
            "proto/BookkeeperProtocol.proto",
            "proto/DataFormats.proto",
            "proto/DbLedgerStorageDataFormats.proto",
        ],
        &["proto/"],
    )?;
    Ok(())
}
```

* [ ] **Step 3: Write lib.rs with generated module includes**

```rust
/// BookKeeper wire protocol messages (BookkeeperProtocol.proto).
///
/// This is the default package since BookkeeperProtocol.proto has no `package` directive.
#[allow(clippy::all)]
mod bk_protocol {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

pub use bk_protocol::*;

/// Metadata data formats (DataFormats.proto).
/// Also has no package directive, so it merges into the default module.
/// prost merges them into the same output file.

// DbLedgerStorageDataFormats.proto also has no package — its `LedgerData`
// message will appear in the same default module alongside the others.
```

Note: Since all three proto files use `syntax = "proto2"` with no `package` directive, prost generates them all into a single `_.rs` file in `OUT_DIR`. All messages are accessible directly from the crate root.

* [ ] **Step 4: Verify proto codegen builds**

Run: `cargo build -p bookkeeper-proto`
Expected: Compiles, generates Rust structs for all protobuf messages

* [ ] **Step 5: Verify key types exist**

Run: `cargo doc -p bookkeeper-proto --no-deps --open`
Check that these types are generated:

* `Request`, `Response`, `BkPacketHeader`

* `ReadRequest`, `AddRequest`, `ReadResponse`, `AddResponse`

* `LedgerMetadataFormat`, `CookieFormat`, `BookieServiceInfoFormat`

* `LedgerData` (from DbLedgerStorageDataFormats)

* `StatusCode`, `OperationType`, `ProtocolVersion` enums

* [ ] **Step 6: Commit**

```bash
git add crates/bookkeeper-proto/
git commit -m "feat: add protobuf codegen for BookKeeper protocol and data formats"
```

***

### Task 3: Common Types and Errors

**Files:**

* Modify: `crates/bookkeeper-common/src/types.rs`

* Modify: `crates/bookkeeper-common/src/error.rs`

* Create: `crates/bookkeeper-common/tests/types_test.rs`

* [ ] **Step 1: Write failing test for BookieId**

`crates/bookkeeper-common/tests/types_test.rs`:

```rust
use bookkeeper_common::types::BookieId;

#[test]
fn bookie_id_from_host_port() {
    let id = BookieId::new("bookie1", 3181);
    assert_eq!(id.to_string(), "bookie1:3181");
    assert_eq!(id.host(), "bookie1");
    assert_eq!(id.port(), 3181);
}

#[test]
fn bookie_id_parse() {
    let id: BookieId = "bookie1:3181".parse().unwrap();
    assert_eq!(id.host(), "bookie1");
    assert_eq!(id.port(), 3181);
}

#[test]
fn bookie_id_parse_invalid() {
    assert!("invalid".parse::<BookieId>().is_err());
}
```

* [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bookkeeper-common`
Expected: FAIL — types not defined yet

* [ ] **Step 3: Implement types.rs**

```rust
use std::fmt;
use std::str::FromStr;

/// Type aliases for clarity throughout the codebase.
pub type LedgerId = i64;
pub type EntryId = i64;

/// Special entry ID values.
pub const INVALID_ENTRY_ID: EntryId = -1;
pub const LAST_ADD_CONFIRMED: EntryId = -1;

/// A bookie's network identity: hostname (or IP) and port.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BookieId {
    host: String,
    port: u16,
}

impl BookieId {
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port,
        }
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for BookieId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl FromStr for BookieId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (host, port_str) = s.rsplit_once(':')
            .ok_or_else(|| format!("invalid bookie id: missing ':' in '{s}'"))?;
        let port: u16 = port_str.parse()
            .map_err(|e| format!("invalid port in '{s}': {e}"))?;
        Ok(Self::new(host, port))
    }
}
```

* [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p bookkeeper-common`
Expected: All 3 tests pass

* [ ] **Step 5: Write error.rs**

```rust
use thiserror::Error;

/// BookKeeper error codes matching the protocol status codes.
///
/// Pre-v3 and v3 use different integer values for the same logical error.
/// This enum is the canonical representation; conversion to/from wire codes
/// is handled in the protocol crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BKError {
    #[error("OK")]
    Ok,
    #[error("no such ledger")]
    NoLedger,
    #[error("no such entry")]
    NoEntry,
    #[error("bad request")]
    BadRequest,
    #[error("I/O error")]
    Io,
    #[error("unauthorized")]
    Unauthorized,
    #[error("bad version")]
    BadVersion,
    #[error("ledger fenced")]
    Fenced,
    #[error("bookie read-only")]
    ReadOnly,
    #[error("too many requests")]
    TooManyRequests,
    #[error("unknown ledger state")]
    UnknownLedgerState,
}

impl BKError {
    /// Convert from pre-v3 (binary protocol) error code.
    pub fn from_v2_code(code: i32) -> Self {
        match code {
            0 => Self::Ok,
            1 => Self::NoLedger,
            2 => Self::NoEntry,
            100 => Self::BadRequest,
            101 => Self::Io,
            102 => Self::Unauthorized,
            103 => Self::BadVersion,
            104 => Self::Fenced,
            105 => Self::ReadOnly,
            106 => Self::TooManyRequests,
            107 => Self::UnknownLedgerState,
            _ => Self::BadRequest,
        }
    }

    /// Convert to pre-v3 (binary protocol) error code.
    pub fn to_v2_code(self) -> i32 {
        match self {
            Self::Ok => 0,
            Self::NoLedger => 1,
            Self::NoEntry => 2,
            Self::BadRequest => 100,
            Self::Io => 101,
            Self::Unauthorized => 102,
            Self::BadVersion => 103,
            Self::Fenced => 104,
            Self::ReadOnly => 105,
            Self::TooManyRequests => 106,
            Self::UnknownLedgerState => 107,
        }
    }

    /// Convert from v3 (protobuf) StatusCode integer value.
    pub fn from_v3_code(code: i32) -> Self {
        match code {
            0 => Self::Ok,
            402 => Self::NoLedger,
            403 => Self::NoEntry,
            404 => Self::BadRequest,
            501 => Self::Io,
            502 => Self::Unauthorized,
            503 => Self::BadVersion,
            504 => Self::Fenced,
            505 => Self::ReadOnly,
            506 => Self::TooManyRequests,
            507 => Self::UnknownLedgerState,
            _ => Self::BadRequest,
        }
    }

    /// Convert to v3 (protobuf) StatusCode integer value.
    pub fn to_v3_code(self) -> i32 {
        match self {
            Self::Ok => 0,
            Self::NoLedger => 402,
            Self::NoEntry => 403,
            Self::BadRequest => 404,
            Self::Io => 501,
            Self::Unauthorized => 502,
            Self::BadVersion => 503,
            Self::Fenced => 504,
            Self::ReadOnly => 505,
            Self::TooManyRequests => 506,
            Self::UnknownLedgerState => 507,
        }
    }
}
```

* [ ] **Step 6: Verify everything builds and tests pass**

Run: `cargo test -p bookkeeper-common`
Expected: All tests pass

* [ ] **Step 7: Commit**

```bash
git add crates/bookkeeper-common/
git commit -m "feat: add common types (BookieId, LedgerId) and BKError with v2/v3 code conversion"
```

***

### Task 4: Protocol Constants

**Files:**

* Modify: `crates/bookkeeper-protocol/src/constants.rs`

* [ ] **Step 1: Write constants.rs**

```rust
/// Pre-v3 binary protocol constants from BookieProtocol.java.

// Protocol versions
pub const CURRENT_PROTOCOL_VERSION: u8 = 2;
pub const LOWEST_COMPAT_PROTOCOL_VERSION: u8 = 0;

// Pre-v3 operation codes
pub const OP_ADD_ENTRY: u8 = 1;
pub const OP_READ_ENTRY: u8 = 2;
pub const OP_AUTH: u8 = 3;
pub const OP_READ_LAC: u8 = 4;
pub const OP_WRITE_LAC: u8 = 5;
pub const OP_GET_BOOKIE_INFO: u8 = 6;
pub const OP_BATCH_READ_ENTRY: u8 = 7;

// Flags (16-bit)
pub const FLAG_NONE: u16 = 0x0000;
pub const FLAG_DO_FENCING: u16 = 0x0001;
pub const FLAG_RECOVERY_ADD: u16 = 0x0002;
pub const FLAG_HIGH_PRIORITY: u16 = 0x0004;

// Write flags (v3 AddRequest.writeFlags)
pub const WRITE_FLAG_DEFERRED_SYNC: i32 = 0x1;

// Pre-v3 error codes
pub const V2_EOK: i32 = 0;
pub const V2_ENOLEDGER: i32 = 1;
pub const V2_ENOENTRY: i32 = 2;
pub const V2_EBADREQ: i32 = 100;
pub const V2_EIO: i32 = 101;
pub const V2_EUA: i32 = 102;
pub const V2_EBADVERSION: i32 = 103;
pub const V2_EFENCED: i32 = 104;
pub const V2_EREADONLY: i32 = 105;
pub const V2_ETOOMANYREQUESTS: i32 = 106;
pub const V2_EUNKNOWNLEDGERSTATE: i32 = 107;

// Master key
pub const MASTER_KEY_LENGTH: usize = 20;

// Entry format
pub const METADATA_LENGTH: usize = 32; // 4 × i64: ledgerId + entryId + lac + length

// Special values
pub const INVALID_ENTRY_ID: i64 = -1;
pub const LAST_ADD_CONFIRMED_ENTRY_ID: i64 = -1;
pub const SMALL_ENTRY_SIZE_THRESHOLD: usize = 16384; // 16 KB

// GetBookieInfo flags
pub const BOOKIE_INFO_FREE_DISK_SPACE: i64 = 0x01;
pub const BOOKIE_INFO_TOTAL_DISK_CAPACITY: i64 = 0x02;

// Pre-v3 response header size (shared by all pre-v3 responses)
// packetHeader(4) + errorCode(4) + ledgerId(8) + entryId(8) = 24
pub const V2_RESPONSE_HEADER_SIZE: usize = 24;
```

* [ ] **Step 2: Verify builds**

Run: `cargo check -p bookkeeper-protocol`
Expected: Compiles

* [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-protocol/src/constants.rs
git commit -m "feat: add protocol constants (opcodes, flags, error codes, sizes)"
```

***

### Task 5: Digest Computation

**Files:**

* Create: `crates/bookkeeper-protocol/src/digest.rs`

* Create: `crates/bookkeeper-protocol/tests/digest_test.rs`

* [ ] **Step 1: Write failing tests for digest computation**

`crates/bookkeeper-protocol/tests/digest_test.rs`:

```rust
use bookkeeper_protocol::digest::{DigestManager, DigestType};

#[test]
fn crc32_digest_round_trip() {
    let dm = DigestManager::new(DigestType::Crc32);
    let metadata = build_metadata(1, 0, -1, 0);
    let payload = b"hello world";
    let digest = dm.compute(&metadata, payload);
    assert_eq!(digest.len(), 8); // CRC32 stored as i64
    assert!(dm.verify(&metadata, payload, &digest));
}

#[test]
fn crc32c_digest_round_trip() {
    let dm = DigestManager::new(DigestType::Crc32c);
    let metadata = build_metadata(1, 0, -1, 11);
    let payload = b"hello world";
    let digest = dm.compute(&metadata, payload);
    assert_eq!(digest.len(), 8);
    assert!(dm.verify(&metadata, payload, &digest));
}

#[test]
fn hmac_digest_round_trip() {
    let dm = DigestManager::new(DigestType::HmacSha1);
    let dm = dm.with_password(b"secret");
    let metadata = build_metadata(1, 0, -1, 11);
    let payload = b"hello world";
    let digest = dm.compute(&metadata, payload);
    assert_eq!(digest.len(), 20); // HMAC-SHA1
    assert!(dm.verify(&metadata, payload, &digest));
}

#[test]
fn dummy_digest() {
    let dm = DigestManager::new(DigestType::Dummy);
    let metadata = build_metadata(1, 0, -1, 0);
    let digest = dm.compute(&metadata, b"anything");
    assert_eq!(digest.len(), 0);
    assert!(dm.verify(&metadata, b"anything", &digest));
}

#[test]
fn crc32_digest_detects_corruption() {
    let dm = DigestManager::new(DigestType::Crc32);
    let metadata = build_metadata(1, 0, -1, 11);
    let digest = dm.compute(&metadata, b"hello world");
    assert!(!dm.verify(&metadata, b"HELLO world", &digest));
}

fn build_metadata(ledger_id: i64, entry_id: i64, lac: i64, length: i64) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[0..8].copy_from_slice(&ledger_id.to_be_bytes());
    buf[8..16].copy_from_slice(&entry_id.to_be_bytes());
    buf[16..24].copy_from_slice(&lac.to_be_bytes());
    buf[24..32].copy_from_slice(&length.to_be_bytes());
    buf
}
```

* [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bookkeeper-protocol`
Expected: FAIL — digest module doesn't exist

* [ ] **Step 3: Implement digest.rs**

```rust
use hmac::{Hmac, Mac};
use sha1::Sha1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DigestType {
    Crc32,
    Crc32c,
    HmacSha1,
    Dummy,
}

impl DigestType {
    /// Number of bytes in the digest output.
    pub fn digest_length(&self) -> usize {
        match self {
            Self::Crc32 | Self::Crc32c => 8, // stored as i64
            Self::HmacSha1 => 20,
            Self::Dummy => 0,
        }
    }
}

/// Computes and verifies digests for BookKeeper entries.
///
/// Digest is computed over: metadata (32 bytes) + payload.
/// For CRC32/CRC32C the 4-byte checksum is stored as a big-endian i64 (8 bytes).
/// For HMAC-SHA1 the 20-byte MAC is stored directly.
#[derive(Debug, Clone)]
pub struct DigestManager {
    digest_type: DigestType,
    password: Vec<u8>,
}

impl DigestManager {
    pub fn new(digest_type: DigestType) -> Self {
        Self {
            digest_type,
            password: Vec::new(),
        }
    }

    pub fn with_password(mut self, password: &[u8]) -> Self {
        self.password = password.to_vec();
        self
    }

    pub fn digest_type(&self) -> DigestType {
        self.digest_type
    }

    /// Compute digest over metadata + payload. Returns the digest bytes.
    pub fn compute(&self, metadata: &[u8; 32], payload: &[u8]) -> Vec<u8> {
        match self.digest_type {
            DigestType::Crc32 => {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(metadata);
                hasher.update(payload);
                let crc = hasher.finalize();
                (crc as i64).to_be_bytes().to_vec()
            }
            DigestType::Crc32c => {
                let mut crc = crc32c::crc32c(metadata);
                crc = crc32c::crc32c_append(crc, payload);
                (crc as i64).to_be_bytes().to_vec()
            }
            DigestType::HmacSha1 => {
                type HmacSha1 = Hmac<Sha1>;
                let mut mac = HmacSha1::new_from_slice(&self.password)
                    .expect("HMAC accepts any key length");
                mac.update(metadata);
                mac.update(payload);
                mac.finalize().into_bytes().to_vec()
            }
            DigestType::Dummy => Vec::new(),
        }
    }

    /// Verify digest over metadata + payload against expected digest bytes.
    pub fn verify(&self, metadata: &[u8; 32], payload: &[u8], expected: &[u8]) -> bool {
        if self.digest_type == DigestType::Dummy {
            return true;
        }
        let computed = self.compute(metadata, payload);
        computed == expected
    }
}
```

* [ ] **Step 4: Add digest module to lib.rs**

Update `crates/bookkeeper-protocol/src/lib.rs`:

```rust
pub mod constants;
pub mod digest;
```

* [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p bookkeeper-protocol`
Expected: All 5 digest tests pass

* [ ] **Step 6: Commit**

```bash
git add crates/bookkeeper-protocol/
git commit -m "feat: add digest computation (CRC32, CRC32C, HMAC-SHA1, DUMMY)"
```

***

### Task 6: Entry Format

**Files:**

* Create: `crates/bookkeeper-protocol/src/entry.rs`

* Create: `crates/bookkeeper-protocol/tests/entry_test.rs`

* [ ] **Step 1: Write failing tests for entry serialization**

`crates/bookkeeper-protocol/tests/entry_test.rs`:

```rust
use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_protocol::entry::EntryBuilder;
use bytes::Bytes;

#[test]
fn entry_round_trip_crc32c() {
    let dm = DigestManager::new(DigestType::Crc32c);
    let payload = Bytes::from_static(b"hello world");

    let entry_bytes = EntryBuilder::new(&dm)
        .ledger_id(42)
        .entry_id(7)
        .lac(-1)
        .length(11)
        .payload(payload.clone())
        .build();

    // Header: 32 bytes metadata + 8 bytes CRC32C digest + 11 bytes payload = 51
    assert_eq!(entry_bytes.len(), 32 + 8 + 11);

    let parsed = bookkeeper_protocol::entry::parse_entry(&dm, &entry_bytes).unwrap();
    assert_eq!(parsed.ledger_id, 42);
    assert_eq!(parsed.entry_id, 7);
    assert_eq!(parsed.lac, -1);
    assert_eq!(parsed.length, 11);
    assert_eq!(parsed.payload, payload);
}

#[test]
fn entry_round_trip_dummy() {
    let dm = DigestManager::new(DigestType::Dummy);
    let payload = Bytes::from_static(b"test");

    let entry_bytes = EntryBuilder::new(&dm)
        .ledger_id(1)
        .entry_id(0)
        .lac(-1)
        .length(4)
        .payload(payload.clone())
        .build();

    // 32 metadata + 0 digest + 4 payload
    assert_eq!(entry_bytes.len(), 36);

    let parsed = bookkeeper_protocol::entry::parse_entry(&dm, &entry_bytes).unwrap();
    assert_eq!(parsed.payload, payload);
}

#[test]
fn entry_corrupt_digest_detected() {
    let dm = DigestManager::new(DigestType::Crc32c);
    let mut entry_bytes = EntryBuilder::new(&dm)
        .ledger_id(1)
        .entry_id(0)
        .lac(-1)
        .length(5)
        .payload(Bytes::from_static(b"hello"))
        .build();

    // Corrupt a payload byte
    let last = entry_bytes.len() - 1;
    entry_bytes[last] ^= 0xFF;

    let result = bookkeeper_protocol::entry::parse_entry(&dm, &entry_bytes);
    assert!(result.is_err());
}
```

* [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bookkeeper-protocol`
Expected: FAIL — entry module doesn't exist

* [ ] **Step 3: Implement entry.rs**

```rust
use bytes::{BufMut, Bytes, BytesMut};

use crate::constants::METADATA_LENGTH;
use crate::digest::DigestManager;

/// A parsed entry.
#[derive(Debug, Clone)]
pub struct Entry {
    pub ledger_id: i64,
    pub entry_id: i64,
    pub lac: i64,
    pub length: i64,
    pub payload: Bytes,
}

/// Builds a serialized entry with metadata + digest + payload.
pub struct EntryBuilder<'a> {
    dm: &'a DigestManager,
    ledger_id: i64,
    entry_id: i64,
    lac: i64,
    length: i64,
    payload: Bytes,
}

impl<'a> EntryBuilder<'a> {
    pub fn new(dm: &'a DigestManager) -> Self {
        Self {
            dm,
            ledger_id: 0,
            entry_id: 0,
            lac: -1,
            length: 0,
            payload: Bytes::new(),
        }
    }

    pub fn ledger_id(mut self, v: i64) -> Self { self.ledger_id = v; self }
    pub fn entry_id(mut self, v: i64) -> Self { self.entry_id = v; self }
    pub fn lac(mut self, v: i64) -> Self { self.lac = v; self }
    pub fn length(mut self, v: i64) -> Self { self.length = v; self }
    pub fn payload(mut self, v: Bytes) -> Self { self.payload = v; self }

    /// Serialize to wire format: [metadata(32) | digest(N) | payload].
    pub fn build(self) -> BytesMut {
        let digest_len = self.dm.digest_type().digest_length();
        let total = METADATA_LENGTH + digest_len + self.payload.len();
        let mut buf = BytesMut::with_capacity(total);

        // Metadata (32 bytes)
        buf.put_i64(self.ledger_id);
        buf.put_i64(self.entry_id);
        buf.put_i64(self.lac);
        buf.put_i64(self.length);

        // Compute digest over metadata + payload
        let metadata: [u8; 32] = buf[..32].try_into().unwrap();
        let digest = self.dm.compute(&metadata, &self.payload);
        buf.put_slice(&digest);

        // Payload
        buf.put_slice(&self.payload);
        buf
    }
}

/// Parse a serialized entry, verifying the digest.
pub fn parse_entry(dm: &DigestManager, data: &[u8]) -> Result<Entry, EntryError> {
    let digest_len = dm.digest_type().digest_length();
    let min_len = METADATA_LENGTH + digest_len;
    if data.len() < min_len {
        return Err(EntryError::TooShort {
            actual: data.len(),
            minimum: min_len,
        });
    }

    let metadata: &[u8; 32] = data[..32].try_into().unwrap();
    let ledger_id = i64::from_be_bytes(data[0..8].try_into().unwrap());
    let entry_id = i64::from_be_bytes(data[8..16].try_into().unwrap());
    let lac = i64::from_be_bytes(data[16..24].try_into().unwrap());
    let length = i64::from_be_bytes(data[24..32].try_into().unwrap());

    let digest = &data[METADATA_LENGTH..METADATA_LENGTH + digest_len];
    let payload = &data[METADATA_LENGTH + digest_len..];

    if !dm.verify(metadata, payload, digest) {
        return Err(EntryError::DigestMismatch);
    }

    Ok(Entry {
        ledger_id,
        entry_id,
        lac,
        length,
        payload: Bytes::copy_from_slice(payload),
    })
}

#[derive(Debug, thiserror::Error)]
pub enum EntryError {
    #[error("entry too short: {actual} bytes, need at least {minimum}")]
    TooShort { actual: usize, minimum: usize },
    #[error("digest mismatch")]
    DigestMismatch,
}
```

* [ ] **Step 4: Add entry module and thiserror dep to lib.rs**

Update `crates/bookkeeper-protocol/src/lib.rs`:

```rust
pub mod constants;
pub mod digest;
pub mod entry;
```

* [ ] **Step 5: Run tests**

Run: `cargo test -p bookkeeper-protocol`
Expected: All entry + digest tests pass

* [ ] **Step 6: Commit**

```bash
git add crates/bookkeeper-protocol/
git commit -m "feat: add entry format serialization/deserialization with digest verification"
```

***

### Task 7: Pre-v3 Packet Header

**Files:**

* Create: `crates/bookkeeper-protocol/src/v2/mod.rs`

* Create: `crates/bookkeeper-protocol/src/v2/header.rs`

* Create: `crates/bookkeeper-protocol/tests/v2_header_test.rs`

* [ ] **Step 1: Write failing test for packet header**

`crates/bookkeeper-protocol/tests/v2_header_test.rs`:

```rust
use bookkeeper_protocol::v2::header::PacketHeader;

#[test]
fn encode_v2_header() {
    let h = PacketHeader::new(2, 1, 0); // version=2, opcode=ADDENTRY, flags=0
    let encoded = h.to_u32();
    assert_eq!(encoded, 0x02_01_0000);
}

#[test]
fn decode_v2_header() {
    let h = PacketHeader::from_u32(0x02_01_0000);
    assert_eq!(h.version(), 2);
    assert_eq!(h.opcode(), 1);
    assert_eq!(h.flags(), 0);
}

#[test]
fn encode_v2_header_with_flags() {
    let h = PacketHeader::new(2, 2, 0x0001); // READENTRY + FLAG_DO_FENCING
    let encoded = h.to_u32();
    assert_eq!(encoded, 0x02_02_0001);
}

#[test]
fn v0_backward_compat() {
    // Version 0: entire int is the opcode
    let h = PacketHeader::from_u32(1); // opcode=1 (ADDENTRY), version=0
    assert_eq!(h.version(), 0);
    assert_eq!(h.opcode(), 1);
    assert_eq!(h.flags(), 0);
}

#[test]
fn round_trip() {
    let original = PacketHeader::new(2, 5, 0x0004); // WRITE_LAC + HIGH_PRIORITY
    let decoded = PacketHeader::from_u32(original.to_u32());
    assert_eq!(decoded.version(), 2);
    assert_eq!(decoded.opcode(), 5);
    assert_eq!(decoded.flags(), 0x0004);
}
```

* [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bookkeeper-protocol`
Expected: FAIL — v2 module doesn't exist

* [ ] **Step 3: Implement header.rs**

`crates/bookkeeper-protocol/src/v2/header.rs`:

```rust
/// Pre-v3 packet header: version(8) | opcode(8) | flags(16), packed into a u32.
///
/// For version 0 (legacy), the entire u32 is the opcode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketHeader {
    version: u8,
    opcode: u8,
    flags: u16,
}

impl PacketHeader {
    pub fn new(version: u8, opcode: u8, flags: u16) -> Self {
        Self { version, opcode, flags }
    }

    pub fn version(&self) -> u8 { self.version }
    pub fn opcode(&self) -> u8 { self.opcode }
    pub fn flags(&self) -> u16 { self.flags }

    pub fn to_u32(&self) -> u32 {
        if self.version == 0 {
            self.opcode as u32
        } else {
            ((self.version as u32) << 24)
                | ((self.opcode as u32) << 16)
                | (self.flags as u32)
        }
    }

    pub fn from_u32(v: u32) -> Self {
        let version = (v >> 24) as u8;
        if version == 0 {
            Self {
                version: 0,
                opcode: v as u8,
                flags: 0,
            }
        } else {
            Self {
                version,
                opcode: ((v >> 16) & 0xFF) as u8,
                flags: (v & 0xFFFF) as u16,
            }
        }
    }

    pub fn has_flag(&self, flag: u16) -> bool {
        self.flags & flag != 0
    }
}
```

`crates/bookkeeper-protocol/src/v2/mod.rs`:

```rust
pub mod header;
pub mod request;
pub mod response;
```

Create empty stubs for now:

`crates/bookkeeper-protocol/src/v2/request.rs`:

```rust
// Populated in Task 8
```

`crates/bookkeeper-protocol/src/v2/response.rs`:

```rust
// Populated in Task 9
```

* [ ] **Step 4: Add v2 module to lib.rs**

Update `crates/bookkeeper-protocol/src/lib.rs`:

```rust
pub mod constants;
pub mod digest;
pub mod entry;
pub mod v2;
```

* [ ] **Step 5: Run tests**

Run: `cargo test -p bookkeeper-protocol`
Expected: All tests pass

* [ ] **Step 6: Commit**

```bash
git add crates/bookkeeper-protocol/
git commit -m "feat: add pre-v3 packet header encode/decode"
```

***

### Task 8: Pre-v3 Request Codec

**Files:**

* Modify: `crates/bookkeeper-protocol/src/v2/request.rs`

* Create: `crates/bookkeeper-protocol/tests/v2_request_test.rs`

* [ ] **Step 1: Write failing test for ADD\_ENTRY request encoding**

`crates/bookkeeper-protocol/tests/v2_request_test.rs`:

```rust
use bookkeeper_protocol::v2::request::{V2Request, encode_request, decode_request};
use bytes::{Bytes, BytesMut};

#[test]
fn add_entry_round_trip() {
    let master_key = [0u8; 20];
    let entry_data = Bytes::from_static(b"test entry data");
    let req = V2Request::AddEntry {
        flags: 0,
        master_key,
        ledger_id: 42,
        entry_id: 7,
        entry_data: entry_data.clone(),
    };

    let encoded = encode_request(&req);
    let decoded = decode_request(&encoded).unwrap();

    match decoded {
        V2Request::AddEntry { ledger_id, entry_id, entry_data: data, .. } => {
            assert_eq!(ledger_id, 42);
            assert_eq!(entry_id, 7);
            assert_eq!(data, entry_data);
        }
        _ => panic!("expected AddEntry"),
    }
}

#[test]
fn read_entry_round_trip() {
    let req = V2Request::ReadEntry {
        flags: 0x0001, // FLAG_DO_FENCING
        ledger_id: 100,
        entry_id: 50,
        master_key: Some([1u8; 20]),
    };

    let encoded = encode_request(&req);
    let decoded = decode_request(&encoded).unwrap();

    match decoded {
        V2Request::ReadEntry { flags, ledger_id, entry_id, master_key } => {
            assert_eq!(flags, 0x0001);
            assert_eq!(ledger_id, 100);
            assert_eq!(entry_id, 50);
            assert!(master_key.is_some());
        }
        _ => panic!("expected ReadEntry"),
    }
}

#[test]
fn read_entry_no_fencing() {
    let req = V2Request::ReadEntry {
        flags: 0,
        ledger_id: 10,
        entry_id: 0,
        master_key: None,
    };

    let encoded = encode_request(&req);
    let decoded = decode_request(&encoded).unwrap();

    match decoded {
        V2Request::ReadEntry { master_key, .. } => {
            assert!(master_key.is_none());
        }
        _ => panic!("expected ReadEntry"),
    }
}

#[test]
fn get_bookie_info_round_trip() {
    let req = V2Request::GetBookieInfo {
        flags: 0,
        requested: 0x03, // FREE_DISK_SPACE | TOTAL_DISK_CAPACITY
    };

    let encoded = encode_request(&req);
    let decoded = decode_request(&encoded).unwrap();

    match decoded {
        V2Request::GetBookieInfo { requested, .. } => {
            assert_eq!(requested, 0x03);
        }
        _ => panic!("expected GetBookieInfo"),
    }
}
```

* [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bookkeeper-protocol`
Expected: FAIL

* [ ] **Step 3: Implement request.rs**

```rust
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::constants::*;
use crate::v2::header::PacketHeader;

/// Pre-v3 request types.
#[derive(Debug, Clone)]
pub enum V2Request {
    AddEntry {
        flags: u16,
        master_key: [u8; MASTER_KEY_LENGTH],
        ledger_id: i64,
        entry_id: i64,
        entry_data: Bytes,
    },
    ReadEntry {
        flags: u16,
        ledger_id: i64,
        entry_id: i64,
        master_key: Option<[u8; MASTER_KEY_LENGTH]>,
    },
    Auth {
        payload: Bytes,
    },
    WriteLac {
        ledger_id: i64,
        lac: i64,
        master_key: [u8; MASTER_KEY_LENGTH],
        body: Bytes,
    },
    ReadLac {
        ledger_id: i64,
    },
    GetBookieInfo {
        flags: u16,
        requested: i64,
    },
    BatchReadEntry {
        flags: u16,
        ledger_id: i64,
        start_entry_id: i64,
        max_count: i32,
        max_size: i64,
    },
}

/// Encode a pre-v3 request to bytes (without the 4-byte frame length prefix).
pub fn encode_request(req: &V2Request) -> BytesMut {
    match req {
        V2Request::AddEntry { flags, master_key, ledger_id, entry_id, entry_data } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_ADD_ENTRY, *flags);
            let mut buf = BytesMut::with_capacity(4 + MASTER_KEY_LENGTH + 16 + entry_data.len());
            buf.put_u32(header.to_u32());
            buf.put_slice(master_key);
            buf.put_i64(*ledger_id);
            buf.put_i64(*entry_id);
            buf.put_slice(entry_data);
            buf
        }
        V2Request::ReadEntry { flags, ledger_id, entry_id, master_key } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_READ_ENTRY, *flags);
            let has_key = master_key.is_some();
            let mut buf = BytesMut::with_capacity(4 + 16 + if has_key { MASTER_KEY_LENGTH } else { 0 });
            buf.put_u32(header.to_u32());
            buf.put_i64(*ledger_id);
            buf.put_i64(*entry_id);
            if let Some(key) = master_key {
                buf.put_slice(key);
            }
            buf
        }
        V2Request::Auth { payload } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_AUTH, FLAG_NONE);
            let mut buf = BytesMut::with_capacity(4 + payload.len());
            buf.put_u32(header.to_u32());
            buf.put_slice(payload);
            buf
        }
        V2Request::WriteLac { ledger_id, lac, master_key, body } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_WRITE_LAC, FLAG_NONE);
            let mut buf = BytesMut::with_capacity(4 + 16 + MASTER_KEY_LENGTH + body.len());
            buf.put_u32(header.to_u32());
            buf.put_i64(*ledger_id);
            buf.put_i64(*lac);
            buf.put_slice(master_key);
            buf.put_slice(body);
            buf
        }
        V2Request::ReadLac { ledger_id } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_READ_LAC, FLAG_NONE);
            let mut buf = BytesMut::with_capacity(4 + 8);
            buf.put_u32(header.to_u32());
            buf.put_i64(*ledger_id);
            buf
        }
        V2Request::GetBookieInfo { flags, requested } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_GET_BOOKIE_INFO, *flags);
            let mut buf = BytesMut::with_capacity(4 + 8);
            buf.put_u32(header.to_u32());
            buf.put_i64(*requested);
            buf
        }
        V2Request::BatchReadEntry { flags, ledger_id, start_entry_id, max_count, max_size } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_BATCH_READ_ENTRY, *flags);
            let mut buf = BytesMut::with_capacity(4 + 28);
            buf.put_u32(header.to_u32());
            buf.put_i64(*ledger_id);
            buf.put_i64(*start_entry_id);
            buf.put_i32(*max_count);
            buf.put_i64(*max_size);
            buf
        }
    }
}

/// Decode a pre-v3 request from bytes (without the 4-byte frame length prefix).
pub fn decode_request(data: &[u8]) -> Result<V2Request, RequestDecodeError> {
    if data.len() < 4 {
        return Err(RequestDecodeError::TooShort);
    }
    let mut buf = &data[..];
    let header = PacketHeader::from_u32(buf.get_u32());
    let flags = header.flags();

    match header.opcode() {
        OP_ADD_ENTRY => {
            if buf.remaining() < MASTER_KEY_LENGTH + 16 {
                return Err(RequestDecodeError::TooShort);
            }
            let mut master_key = [0u8; MASTER_KEY_LENGTH];
            buf.copy_to_slice(&mut master_key);
            let ledger_id = buf.get_i64();
            let entry_id = buf.get_i64();
            let entry_data = Bytes::copy_from_slice(buf);
            Ok(V2Request::AddEntry { flags, master_key, ledger_id, entry_id, entry_data })
        }
        OP_READ_ENTRY => {
            if buf.remaining() < 16 {
                return Err(RequestDecodeError::TooShort);
            }
            let ledger_id = buf.get_i64();
            let entry_id = buf.get_i64();
            let master_key = if header.has_flag(FLAG_DO_FENCING) && buf.remaining() >= MASTER_KEY_LENGTH {
                let mut key = [0u8; MASTER_KEY_LENGTH];
                buf.copy_to_slice(&mut key);
                Some(key)
            } else {
                None
            };
            Ok(V2Request::ReadEntry { flags, ledger_id, entry_id, master_key })
        }
        OP_AUTH => {
            let payload = Bytes::copy_from_slice(buf);
            Ok(V2Request::Auth { payload })
        }
        OP_WRITE_LAC => {
            if buf.remaining() < 16 + MASTER_KEY_LENGTH {
                return Err(RequestDecodeError::TooShort);
            }
            let ledger_id = buf.get_i64();
            let lac = buf.get_i64();
            let mut master_key = [0u8; MASTER_KEY_LENGTH];
            buf.copy_to_slice(&mut master_key);
            let body = Bytes::copy_from_slice(buf);
            Ok(V2Request::WriteLac { ledger_id, lac, master_key, body })
        }
        OP_READ_LAC => {
            if buf.remaining() < 8 {
                return Err(RequestDecodeError::TooShort);
            }
            let ledger_id = buf.get_i64();
            Ok(V2Request::ReadLac { ledger_id })
        }
        OP_GET_BOOKIE_INFO => {
            if buf.remaining() < 8 {
                return Err(RequestDecodeError::TooShort);
            }
            let requested = buf.get_i64();
            Ok(V2Request::GetBookieInfo { flags, requested })
        }
        OP_BATCH_READ_ENTRY => {
            if buf.remaining() < 28 {
                return Err(RequestDecodeError::TooShort);
            }
            let ledger_id = buf.get_i64();
            let start_entry_id = buf.get_i64();
            let max_count = buf.get_i32();
            let max_size = buf.get_i64();
            Ok(V2Request::BatchReadEntry { flags, ledger_id, start_entry_id, max_count, max_size })
        }
        op => Err(RequestDecodeError::UnknownOpcode(op)),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RequestDecodeError {
    #[error("message too short")]
    TooShort,
    #[error("unknown opcode: {0}")]
    UnknownOpcode(u8),
}
```

* [ ] **Step 4: Run tests**

Run: `cargo test -p bookkeeper-protocol`
Expected: All tests pass

* [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-protocol/
git commit -m "feat: add pre-v3 request encode/decode for all operation types"
```

***

### Task 9: Pre-v3 Response Codec

**Files:**

* Modify: `crates/bookkeeper-protocol/src/v2/response.rs`

* Create: `crates/bookkeeper-protocol/tests/v2_response_test.rs`

* [ ] **Step 1: Write failing test for response encoding**

`crates/bookkeeper-protocol/tests/v2_response_test.rs`:

```rust
use bookkeeper_protocol::v2::response::{V2Response, encode_response, decode_response};
use bytes::Bytes;

#[test]
fn add_response_round_trip() {
    let resp = V2Response::Add {
        error_code: 0,
        ledger_id: 42,
        entry_id: 7,
    };
    let encoded = encode_response(&resp);
    assert_eq!(encoded.len(), 24); // Exactly 24 bytes
    let decoded = decode_response(&encoded).unwrap();
    match decoded {
        V2Response::Add { error_code, ledger_id, entry_id } => {
            assert_eq!(error_code, 0);
            assert_eq!(ledger_id, 42);
            assert_eq!(entry_id, 7);
        }
        _ => panic!("expected Add"),
    }
}

#[test]
fn read_response_with_data() {
    let data = Bytes::from_static(b"entry data here");
    let resp = V2Response::Read {
        error_code: 0,
        ledger_id: 1,
        entry_id: 0,
        data: Some(data.clone()),
    };
    let encoded = encode_response(&resp);
    let decoded = decode_response(&encoded).unwrap();
    match decoded {
        V2Response::Read { error_code, data: d, .. } => {
            assert_eq!(error_code, 0);
            assert_eq!(d.unwrap(), data);
        }
        _ => panic!("expected Read"),
    }
}

#[test]
fn read_response_error() {
    let resp = V2Response::Read {
        error_code: 2, // ENOENTRY
        ledger_id: 1,
        entry_id: 99,
        data: None,
    };
    let encoded = encode_response(&resp);
    let decoded = decode_response(&encoded).unwrap();
    match decoded {
        V2Response::Read { error_code, data, .. } => {
            assert_eq!(error_code, 2);
            assert!(data.is_none());
        }
        _ => panic!("expected Read"),
    }
}
```

* [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bookkeeper-protocol`
Expected: FAIL

* [ ] **Step 3: Implement response.rs**

```rust
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::constants::*;
use crate::v2::header::PacketHeader;

/// Pre-v3 response types.
#[derive(Debug, Clone)]
pub enum V2Response {
    Add {
        error_code: i32,
        ledger_id: i64,
        entry_id: i64,
    },
    Read {
        error_code: i32,
        ledger_id: i64,
        entry_id: i64,
        data: Option<Bytes>,
    },
    Auth {
        error_code: i32,
        payload: Bytes,
    },
    WriteLac {
        error_code: i32,
        ledger_id: i64,
        entry_id: i64,
    },
    ReadLac {
        error_code: i32,
        ledger_id: i64,
        entry_id: i64,
        lac_body: Option<Bytes>,
        last_entry_body: Option<Bytes>,
    },
    GetBookieInfo {
        error_code: i32,
        ledger_id: i64,
        entry_id: i64,
        free_disk_space: i64,
        total_disk_capacity: i64,
    },
    BatchRead {
        error_code: i32,
        ledger_id: i64,
        entry_id: i64,
        request_id: i64,
        entries: Vec<Bytes>,
    },
}

/// Encode a pre-v3 response to bytes (without the 4-byte frame length prefix).
pub fn encode_response(resp: &V2Response) -> BytesMut {
    match resp {
        V2Response::Add { error_code, ledger_id, entry_id } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_ADD_ENTRY, FLAG_NONE);
            let mut buf = BytesMut::with_capacity(V2_RESPONSE_HEADER_SIZE);
            buf.put_u32(header.to_u32());
            buf.put_i32(*error_code);
            buf.put_i64(*ledger_id);
            buf.put_i64(*entry_id);
            buf
        }
        V2Response::Read { error_code, ledger_id, entry_id, data } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_READ_ENTRY, FLAG_NONE);
            let data_len = data.as_ref().map_or(0, |d| d.len());
            let mut buf = BytesMut::with_capacity(V2_RESPONSE_HEADER_SIZE + data_len);
            buf.put_u32(header.to_u32());
            buf.put_i32(*error_code);
            buf.put_i64(*ledger_id);
            buf.put_i64(*entry_id);
            if let Some(d) = data {
                buf.put_slice(d);
            }
            buf
        }
        V2Response::Auth { error_code, payload } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_AUTH, FLAG_NONE);
            let mut buf = BytesMut::with_capacity(V2_RESPONSE_HEADER_SIZE + payload.len());
            buf.put_u32(header.to_u32());
            buf.put_i32(*error_code);
            buf.put_i64(0); // ledgerId=0
            buf.put_i64(0); // entryId=0
            buf.put_slice(payload);
            buf
        }
        V2Response::WriteLac { error_code, ledger_id, entry_id } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_WRITE_LAC, FLAG_NONE);
            let mut buf = BytesMut::with_capacity(V2_RESPONSE_HEADER_SIZE);
            buf.put_u32(header.to_u32());
            buf.put_i32(*error_code);
            buf.put_i64(*ledger_id);
            buf.put_i64(*entry_id);
            buf
        }
        V2Response::ReadLac { error_code, ledger_id, entry_id, lac_body, last_entry_body } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_READ_LAC, FLAG_NONE);
            let lac_len = lac_body.as_ref().map_or(0, |b| b.len());
            let entry_len = last_entry_body.as_ref().map_or(0, |b| b.len());
            let mut buf = BytesMut::with_capacity(V2_RESPONSE_HEADER_SIZE + lac_len + entry_len);
            buf.put_u32(header.to_u32());
            buf.put_i32(*error_code);
            buf.put_i64(*ledger_id);
            buf.put_i64(*entry_id);
            if let Some(b) = lac_body {
                buf.put_slice(b);
            }
            if let Some(b) = last_entry_body {
                buf.put_slice(b);
            }
            buf
        }
        V2Response::GetBookieInfo { error_code, ledger_id, entry_id, free_disk_space, total_disk_capacity } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_GET_BOOKIE_INFO, FLAG_NONE);
            let mut buf = BytesMut::with_capacity(V2_RESPONSE_HEADER_SIZE + 16);
            buf.put_u32(header.to_u32());
            buf.put_i32(*error_code);
            buf.put_i64(*ledger_id);
            buf.put_i64(*entry_id);
            buf.put_i64(*free_disk_space);
            buf.put_i64(*total_disk_capacity);
            buf
        }
        V2Response::BatchRead { error_code, ledger_id, entry_id, request_id, entries } => {
            let header = PacketHeader::new(CURRENT_PROTOCOL_VERSION, OP_BATCH_READ_ENTRY, FLAG_NONE);
            let entries_size: usize = entries.iter().map(|e| 4 + e.len()).sum();
            let mut buf = BytesMut::with_capacity(V2_RESPONSE_HEADER_SIZE + 8 + entries_size);
            buf.put_u32(header.to_u32());
            buf.put_i32(*error_code);
            buf.put_i64(*ledger_id);
            buf.put_i64(*entry_id);
            buf.put_i64(*request_id);
            for entry in entries {
                buf.put_i32(entry.len() as i32);
                buf.put_slice(entry);
            }
            buf
        }
    }
}

/// Decode a pre-v3 response from bytes (without the 4-byte frame length prefix).
pub fn decode_response(data: &[u8]) -> Result<V2Response, ResponseDecodeError> {
    if data.len() < V2_RESPONSE_HEADER_SIZE {
        return Err(ResponseDecodeError::TooShort);
    }
    let mut buf = &data[..];
    let header = PacketHeader::from_u32(buf.get_u32());
    let error_code = buf.get_i32();
    let ledger_id = buf.get_i64();
    let entry_id = buf.get_i64();

    match header.opcode() {
        OP_ADD_ENTRY => {
            Ok(V2Response::Add { error_code, ledger_id, entry_id })
        }
        OP_READ_ENTRY => {
            let data = if error_code == V2_EOK && buf.has_remaining() {
                Some(Bytes::copy_from_slice(buf))
            } else {
                None
            };
            Ok(V2Response::Read { error_code, ledger_id, entry_id, data })
        }
        OP_AUTH => {
            let payload = Bytes::copy_from_slice(buf);
            Ok(V2Response::Auth { error_code, payload })
        }
        OP_WRITE_LAC => {
            Ok(V2Response::WriteLac { error_code, ledger_id, entry_id })
        }
        OP_READ_LAC => {
            // ReadLac response has optional lac_body and last_entry_body
            // In practice these are concatenated; separation requires protocol knowledge.
            // For now, treat remaining bytes as lac_body.
            let lac_body = if buf.has_remaining() {
                Some(Bytes::copy_from_slice(buf))
            } else {
                None
            };
            Ok(V2Response::ReadLac {
                error_code, ledger_id, entry_id,
                lac_body,
                last_entry_body: None,
            })
        }
        OP_GET_BOOKIE_INFO => {
            if buf.remaining() < 16 {
                return Err(ResponseDecodeError::TooShort);
            }
            let free_disk_space = buf.get_i64();
            let total_disk_capacity = buf.get_i64();
            Ok(V2Response::GetBookieInfo {
                error_code, ledger_id, entry_id, free_disk_space, total_disk_capacity,
            })
        }
        OP_BATCH_READ_ENTRY => {
            if buf.remaining() < 8 {
                return Err(ResponseDecodeError::TooShort);
            }
            let request_id = buf.get_i64();
            let mut entries = Vec::new();
            while buf.remaining() >= 4 {
                let size = buf.get_i32() as usize;
                if buf.remaining() < size {
                    break;
                }
                entries.push(Bytes::copy_from_slice(&buf[..size]));
                buf.advance(size);
            }
            Ok(V2Response::BatchRead {
                error_code, ledger_id, entry_id, request_id, entries,
            })
        }
        op => Err(ResponseDecodeError::UnknownOpcode(op)),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ResponseDecodeError {
    #[error("message too short")]
    TooShort,
    #[error("unknown opcode: {0}")]
    UnknownOpcode(u8),
}
```

* [ ] **Step 4: Run tests**

Run: `cargo test -p bookkeeper-protocol`
Expected: All tests pass

* [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-protocol/
git commit -m "feat: add pre-v3 response encode/decode for all operation types"
```

***

### Task 10: V3 Protobuf Request/Response Wrappers

**Files:**

* Create: `crates/bookkeeper-protocol/src/v3.rs`

* Create: `crates/bookkeeper-protocol/tests/v3_test.rs`

* [ ] **Step 1: Write failing test for v3 encode/decode**

`crates/bookkeeper-protocol/tests/v3_test.rs`:

```rust
use bookkeeper_protocol::v3::{encode_v3_request, decode_v3_request, encode_v3_response, decode_v3_response};
use bookkeeper_proto::{OperationType, ProtocolVersion, StatusCode};

#[test]
fn v3_add_request_round_trip() {
    let req = bookkeeper_proto::Request {
        header: Some(bookkeeper_proto::BkPacketHeader {
            version: ProtocolVersion::VersionThree as i32,
            operation: OperationType::AddEntry as i32,
            txn_id: 1,
            priority: Some(0),
        }),
        add_request: Some(bookkeeper_proto::AddRequest {
            ledger_id: 42,
            entry_id: 7,
            master_key: vec![0u8; 20],
            body: b"hello".to_vec(),
            flag: None,
            write_flags: None,
        }),
        ..Default::default()
    };

    let encoded = encode_v3_request(&req);
    let decoded = decode_v3_request(&encoded).unwrap();
    let add = decoded.add_request.unwrap();
    assert_eq!(add.ledger_id, 42);
    assert_eq!(add.entry_id, 7);
    assert_eq!(add.body, b"hello");
}

#[test]
fn v3_read_response_round_trip() {
    let resp = bookkeeper_proto::Response {
        header: Some(bookkeeper_proto::BkPacketHeader {
            version: ProtocolVersion::VersionThree as i32,
            operation: OperationType::ReadEntry as i32,
            txn_id: 5,
            priority: Some(0),
        }),
        status: StatusCode::Eok as i32,
        read_response: Some(bookkeeper_proto::ReadResponse {
            status: StatusCode::Eok as i32,
            ledger_id: 1,
            entry_id: 0,
            body: Some(b"data".to_vec()),
            max_lac: Some(0),
            lac_update_timestamp: None,
        }),
        ..Default::default()
    };

    let encoded = encode_v3_response(&resp);
    let decoded = decode_v3_response(&encoded).unwrap();
    assert_eq!(decoded.status, StatusCode::Eok as i32);
    let read = decoded.read_response.unwrap();
    assert_eq!(read.body.unwrap(), b"data");
}
```

* [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bookkeeper-protocol`
Expected: FAIL

* [ ] **Step 3: Implement v3.rs**

```rust
use bytes::{BufMut, BytesMut};
use prost::Message;

use bookkeeper_proto::{Request, Response};

/// Encode a v3 protobuf Request to bytes (without 4-byte frame length prefix).
pub fn encode_v3_request(req: &Request) -> BytesMut {
    let len = req.encoded_len();
    let mut buf = BytesMut::with_capacity(len);
    req.encode(&mut buf).expect("encode cannot fail with sufficient capacity");
    buf
}

/// Decode a v3 protobuf Request from bytes.
pub fn decode_v3_request(data: &[u8]) -> Result<Request, prost::DecodeError> {
    Request::decode(data)
}

/// Encode a v3 protobuf Response to bytes (without 4-byte frame length prefix).
pub fn encode_v3_response(resp: &Response) -> BytesMut {
    let len = resp.encoded_len();
    let mut buf = BytesMut::with_capacity(len);
    resp.encode(&mut buf).expect("encode cannot fail with sufficient capacity");
    buf
}

/// Decode a v3 protobuf Response from bytes.
pub fn decode_v3_response(data: &[u8]) -> Result<Response, prost::DecodeError> {
    Response::decode(data)
}
```

* [ ] **Step 4: Add v3 module to lib.rs**

Update `crates/bookkeeper-protocol/src/lib.rs`:

```rust
pub mod constants;
pub mod digest;
pub mod entry;
pub mod v2;
pub mod v3;
```

* [ ] **Step 5: Run tests**

Run: `cargo test -p bookkeeper-protocol`
Expected: All tests pass

* [ ] **Step 6: Commit**

```bash
git add crates/bookkeeper-protocol/
git commit -m "feat: add v3 protobuf request/response encode/decode wrappers"
```

***

### Task 11: TCP Framing Codec

**Files:**

* Create: `crates/bookkeeper-protocol/src/framing.rs`

* Create: `crates/bookkeeper-protocol/tests/framing_test.rs`

* [ ] **Step 1: Write failing test for framing codec**

`crates/bookkeeper-protocol/tests/framing_test.rs`:

```rust
use bookkeeper_protocol::framing::BookieCodec;
use bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[test]
fn encode_frame() {
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let payload = BytesMut::from(&b"hello"[..]);
    let mut buf = BytesMut::new();
    codec.encode(payload.freeze(), &mut buf).unwrap();

    // Should be: [0,0,0,5] + "hello"
    assert_eq!(buf.len(), 9);
    assert_eq!(&buf[0..4], &[0, 0, 0, 5]); // length prefix
    assert_eq!(&buf[4..9], b"hello");
}

#[test]
fn decode_frame() {
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let mut buf = BytesMut::new();
    buf.put_u32(5); // length
    buf.put_slice(b"world");

    let frame = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame, &b"world"[..]);
    assert!(buf.is_empty());
}

#[test]
fn decode_partial_frame() {
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let mut buf = BytesMut::new();
    buf.put_u32(10);
    buf.put_slice(b"hello"); // Only 5 of 10 bytes

    let result = codec.decode(&mut buf).unwrap();
    assert!(result.is_none()); // Not enough data yet
}

#[test]
fn decode_frame_too_large() {
    let mut codec = BookieCodec::new(100); // max 100 bytes
    let mut buf = BytesMut::new();
    buf.put_u32(200); // Claims 200 bytes

    let result = codec.decode(&mut buf);
    assert!(result.is_err());
}

#[test]
fn decode_multiple_frames() {
    let mut codec = BookieCodec::new(1024);
    let mut buf = BytesMut::new();
    buf.put_u32(3);
    buf.put_slice(b"aaa");
    buf.put_u32(2);
    buf.put_slice(b"bb");

    let f1 = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(f1, &b"aaa"[..]);
    let f2 = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(f2, &b"bb"[..]);
}
```

* [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bookkeeper-protocol`
Expected: FAIL

* [ ] **Step 3: Implement framing.rs**

```rust
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// Length-prefixed framing codec for BookKeeper TCP protocol.
///
/// Each frame is: [4-byte big-endian length] [payload of that length].
/// The length field does NOT include itself.
pub struct BookieCodec {
    max_frame_size: usize,
}

impl BookieCodec {
    pub fn new(max_frame_size: usize) -> Self {
        Self { max_frame_size }
    }
}

impl Decoder for BookieCodec {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least 4 bytes for the length prefix
        if src.len() < 4 {
            return Ok(None);
        }

        // Peek at the length without consuming
        let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        if length > self.max_frame_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame size {length} exceeds max {}", self.max_frame_size),
            ));
        }

        // Check if we have the full frame
        if src.len() < 4 + length {
            // Reserve space for the rest of the frame
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        // Consume the length prefix
        src.advance(4);
        // Split off the frame payload
        let frame = src.split_to(length);
        Ok(Some(frame))
    }
}

impl Encoder<Bytes> for BookieCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let len = item.len();
        if len > self.max_frame_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame size {len} exceeds max {}", self.max_frame_size),
            ));
        }
        dst.reserve(4 + len);
        dst.put_u32(len as u32);
        dst.put_slice(&item);
        Ok(())
    }
}
```

* [ ] **Step 4: Add framing module to lib.rs**

Update `crates/bookkeeper-protocol/src/lib.rs`:

```rust
pub mod constants;
pub mod digest;
pub mod entry;
pub mod framing;
pub mod v2;
pub mod v3;
```

* [ ] **Step 5: Run tests**

Run: `cargo test -p bookkeeper-protocol`
Expected: All tests pass

* [ ] **Step 6: Commit**

```bash
git add crates/bookkeeper-protocol/
git commit -m "feat: add TCP length-prefixed framing codec (tokio-util)"
```

***

### Task 12: Wire Compatibility Integration Tests

**Files:**

* Create: `tests/wire_compat.rs`

This task verifies that our codec can produce and parse byte sequences that match what the Java BookKeeper implementation would produce.

* [ ] **Step 1: Write wire-level integration tests**

`tests/wire_compat.rs`:

```rust
//! Integration tests verifying wire format compatibility with Java BookKeeper.

use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::v2::header::PacketHeader;
use bookkeeper_protocol::v2::request::{V2Request, encode_request, decode_request};
use bookkeeper_protocol::v2::response::{V2Response, encode_response, decode_response};
use bookkeeper_protocol::framing::BookieCodec;
use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_protocol::entry::{EntryBuilder, parse_entry};
use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// Verify packet header encoding matches Java's PacketHeader.toInt().
#[test]
fn packet_header_java_compat() {
    // Java: PacketHeader.toInt((byte)2, (byte)1, (short)0) = 0x02010000
    assert_eq!(PacketHeader::new(2, 1, 0).to_u32(), 0x02010000);
    // With fencing flag
    assert_eq!(PacketHeader::new(2, 2, 0x0001).to_u32(), 0x02020001);
    // With recovery add flag
    assert_eq!(PacketHeader::new(2, 1, 0x0002).to_u32(), 0x02010002);
}

/// Verify ADD_ENTRY request byte layout matches Java wire format.
/// Java layout: [header(4) | masterKey(20) | ledgerId(8) | entryId(8) | entryData(...)]
#[test]
fn add_entry_request_layout() {
    let master_key = [0xABu8; 20];
    let entry_data = Bytes::from_static(b"DATA");
    let req = V2Request::AddEntry {
        flags: FLAG_RECOVERY_ADD,
        master_key,
        ledger_id: 1,
        entry_id: 0,
        entry_data: entry_data.clone(),
    };

    let buf = encode_request(&req);
    // Total: 4 + 20 + 8 + 8 + 4 = 44
    assert_eq!(buf.len(), 44);

    // Verify header: version=2, opcode=1, flags=0x0002
    let header_val = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    assert_eq!(header_val, 0x02010002);

    // Verify master key at offset 4..24
    assert_eq!(&buf[4..24], &[0xAB; 20]);

    // Verify ledgerId at offset 24..32
    assert_eq!(i64::from_be_bytes(buf[24..32].try_into().unwrap()), 1);

    // Verify entryId at offset 32..40
    assert_eq!(i64::from_be_bytes(buf[32..40].try_into().unwrap()), 0);

    // Verify entry data at offset 40..
    assert_eq!(&buf[40..], b"DATA");
}

/// Verify response header is exactly 24 bytes for simple responses.
#[test]
fn response_header_24_bytes() {
    let resp = V2Response::Add { error_code: 0, ledger_id: 42, entry_id: 7 };
    let buf = encode_response(&resp);
    assert_eq!(buf.len(), 24);

    // Byte layout: header(4) + errorCode(4) + ledgerId(8) + entryId(8)
    let error_code = i32::from_be_bytes(buf[4..8].try_into().unwrap());
    assert_eq!(error_code, V2_EOK);
    let ledger_id = i64::from_be_bytes(buf[8..16].try_into().unwrap());
    assert_eq!(ledger_id, 42);
    let entry_id = i64::from_be_bytes(buf[16..24].try_into().unwrap());
    assert_eq!(entry_id, 7);
}

/// Verify entry format matches Java DigestManager output.
/// Java layout: [ledgerId(8) | entryId(8) | lac(8) | length(8) | digest(N) | payload]
#[test]
fn entry_format_layout() {
    let dm = DigestManager::new(DigestType::Crc32c);
    let payload = Bytes::from_static(b"test");
    let entry = EntryBuilder::new(&dm)
        .ledger_id(100)
        .entry_id(5)
        .lac(4)
        .length(4)
        .payload(payload)
        .build();

    // 32 metadata + 8 CRC32C digest + 4 payload = 44
    assert_eq!(entry.len(), 44);

    // Verify metadata fields
    assert_eq!(i64::from_be_bytes(entry[0..8].try_into().unwrap()), 100);   // ledgerId
    assert_eq!(i64::from_be_bytes(entry[8..16].try_into().unwrap()), 5);    // entryId
    assert_eq!(i64::from_be_bytes(entry[16..24].try_into().unwrap()), 4);   // lac
    assert_eq!(i64::from_be_bytes(entry[24..32].try_into().unwrap()), 4);   // length

    // Verify payload at end
    assert_eq!(&entry[40..44], b"test");
}

/// Verify framing round-trip: frame → bytes → frame.
#[test]
fn framing_round_trip() {
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let payload = Bytes::from_static(b"framed payload");

    // Encode
    let mut wire = BytesMut::new();
    codec.encode(payload.clone(), &mut wire).unwrap();

    // Decode
    let decoded = codec.decode(&mut wire).unwrap().unwrap();
    assert_eq!(decoded, payload);
}

/// Full stack test: build entry → wrap in ADD_ENTRY request → frame → decode.
#[test]
fn full_stack_add_entry() {
    let dm = DigestManager::new(DigestType::Crc32c);
    let payload = Bytes::from_static(b"hello bookkeeper");

    // Build entry
    let entry_bytes = EntryBuilder::new(&dm)
        .ledger_id(1)
        .entry_id(0)
        .lac(-1)
        .length(16)
        .payload(payload)
        .build();

    // Wrap in ADD_ENTRY request
    let req = V2Request::AddEntry {
        flags: 0,
        master_key: [0u8; 20],
        ledger_id: 1,
        entry_id: 0,
        entry_data: entry_bytes.freeze(),
    };
    let req_bytes = encode_request(&req);

    // Frame it
    let mut codec = BookieCodec::new(5 * 1024 * 1024);
    let mut wire = BytesMut::new();
    codec.encode(req_bytes.freeze(), &mut wire).unwrap();

    // Decode frame
    let frame = codec.decode(&mut wire).unwrap().unwrap();

    // Decode request
    let decoded_req = decode_request(&frame).unwrap();
    match decoded_req {
        V2Request::AddEntry { ledger_id, entry_id, entry_data, .. } => {
            assert_eq!(ledger_id, 1);
            assert_eq!(entry_id, 0);

            // Parse entry and verify digest
            let entry = parse_entry(&dm, &entry_data).unwrap();
            assert_eq!(entry.ledger_id, 1);
            assert_eq!(entry.payload, b"hello bookkeeper"[..]);
        }
        _ => panic!("expected AddEntry"),
    }
}
```

* [ ] **Step 2: Add workspace-level test dependency**

Add to the root `Cargo.toml`:

```toml
[dev-dependencies]
bookkeeper-proto = { path = "crates/bookkeeper-proto" }
bookkeeper-common = { path = "crates/bookkeeper-common" }
bookkeeper-protocol = { path = "crates/bookkeeper-protocol" }
bytes = "1"
tokio-util = { version = "0.7", features = ["codec"] }
```

* [ ] **Step 3: Run integration tests**

Run: `cargo test --test wire_compat`
Expected: All tests pass

* [ ] **Step 4: Run full test suite**

Run: `cargo test --workspace`
Expected: All tests pass across all crates

* [ ] **Step 5: Commit**

```bash
git add tests/ Cargo.toml
git commit -m "feat: add wire compatibility integration tests for full protocol stack"
```

***

## What's Next

This plan covers **Phase 1: Foundation + Wire Protocol**. The remaining phases, each getting their own plan:

| Phase    | Scope                                               | Depends On    |
| -------- | --------------------------------------------------- | ------------- |
| Phase 2  | Metadata (ZK integration, ledger metadata, cookies) | Phase 1       |
| Phase 3  | Storage Engine (journal, DbLedgerStorage, RocksDB)  | Phase 1       |
| Phase 4  | Bookie Server (TCP server, request pipeline)        | Phase 1, 2, 3 |
| Phase 5  | Bookie Registration (ZK registration, discovery)    | Phase 2       |
| Phase 6  | Client Library (write/read paths, placement)        | Phase 1, 2    |
| Phase 7  | Replication & Recovery (auditor, worker)            | Phase 4, 6    |
| Phase 8  | Configuration (full config parsing)                 | Phase 1       |
| Phase 9  | Admin HTTP API                                      | Phase 4       |
| Phase 10 | CLI Tools                                           | Phase 4, 6    |

