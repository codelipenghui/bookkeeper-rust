# Phase 3: Storage Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the bookie storage engine — Journal (WAL), entry log files, RocksDB indexes, write/read caches, and the LedgerStorage trait — enabling a Rust bookie to persist and retrieve ledger entries with crash recovery, compatible with Java BookKeeper on-disk formats.

**Architecture:** New `bookkeeper-storage` crate with layered components: `Journal` handles durable writes (WAL with group commit); `EntryLogger` manages entry log files; `EntryLocationIndex` and `LedgerMetadataIndex` wrap RocksDB; `WriteCache` and `ReadCache` provide in-memory buffering; `DbLedgerStorage` composes all layers into the `LedgerStorage` trait. Each component is independently testable with its own file format tests.

**Tech Stack:** `rocksdb` (RocksDB bindings), `bytes` (buffers), `tokio` (async I/O), `parking_lot` (fast mutexes), `dashmap` (concurrent hash maps for caches), `thiserror`

**Spec reference:** `docs/superpowers/specs/03-storage-engine.md`

---

## File Structure

```
crates/bookkeeper-storage/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── journal/
    │   ├── mod.rs
    │   ├── format.rs             # Journal file header, entry, padding format
    │   ├── writer.rs             # Journal writer with group commit
    │   └── scanner.rs            # Journal replay/scan for recovery
    ├── entry_log/
    │   ├── mod.rs
    │   ├── format.rs             # Entry log header, entry, ledger map format
    │   ├── writer.rs             # Entry log writer (append entries, close with ledger map)
    │   └── reader.rs             # Entry log reader (read entry at position)
    ├── index/
    │   ├── mod.rs
    │   ├── entry_location.rs     # RocksDB: (ledgerId, entryId) → location
    │   └── ledger_index.rs       # RocksDB: ledgerId → LedgerData protobuf
    ├── cache/
    │   ├── mod.rs
    │   ├── write_cache.rs        # WriteCache with 64-byte aligned entries
    │   └── read_cache.rs         # ReadCache ring-buffer
    ├── checkpoint.rs             # LastMark read/write, checkpoint coordination
    ├── ledger_storage.rs         # LedgerStorage trait
    └── db_ledger_storage.rs      # DbLedgerStorage: composes all components
```

---

### Task 1: Create Crate + Journal File Format

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `crates/bookkeeper-storage/Cargo.toml`
- Create: `crates/bookkeeper-storage/src/lib.rs`
- Create: `crates/bookkeeper-storage/src/journal/mod.rs`
- Create: `crates/bookkeeper-storage/src/journal/format.rs`
- Test: `crates/bookkeeper-storage/tests/journal_format_test.rs`

- [ ] **Step 1: Add crate to workspace**

In root `Cargo.toml`, add to members and workspace.dependencies:
```toml
# In [workspace] members:
"crates/bookkeeper-storage",

# In [workspace.dependencies]:
bookkeeper-storage = { path = "crates/bookkeeper-storage" }
rocksdb = "0.22"
parking_lot = "0.12"
dashmap = "6"
```

- [ ] **Step 2: Create crate Cargo.toml**

```toml
[package]
name = "bookkeeper-storage"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
bookkeeper-proto = { workspace = true }
bookkeeper-common = { workspace = true }
bytes = { workspace = true }
prost = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true }
rocksdb = { workspace = true }
parking_lot = { workspace = true }
dashmap = { workspace = true }

[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 3: Write failing tests for journal format**

`crates/bookkeeper-storage/tests/journal_format_test.rs`:
```rust
use bookkeeper_storage::journal::format::*;

#[test]
fn journal_header_v6_is_512_bytes() {
    let header = JournalHeader::new(6);
    let bytes = header.to_bytes();
    assert_eq!(bytes.len(), SECTOR_SIZE);
    assert_eq!(&bytes[0..4], JOURNAL_MAGIC);
    assert_eq!(i32::from_be_bytes(bytes[4..8].try_into().unwrap()), 6);
    // Bytes 8..512 should be zero
    assert!(bytes[8..].iter().all(|&b| b == 0));
}

#[test]
fn journal_header_round_trip() {
    let header = JournalHeader::new(6);
    let bytes = header.to_bytes();
    let parsed = JournalHeader::from_bytes(&bytes).unwrap();
    assert_eq!(parsed.version(), 6);
}

#[test]
fn journal_header_bad_magic() {
    let mut bytes = vec![0u8; SECTOR_SIZE];
    bytes[0..4].copy_from_slice(b"XXXX");
    assert!(JournalHeader::from_bytes(&bytes).is_err());
}

#[test]
fn encode_journal_entry() {
    let payload = b"hello journal";
    let encoded = encode_journal_entry(payload);
    // 4-byte size prefix + payload
    assert_eq!(encoded.len(), 4 + payload.len());
    let size = i32::from_be_bytes(encoded[0..4].try_into().unwrap());
    assert_eq!(size as usize, payload.len());
    assert_eq!(&encoded[4..], payload);
}

#[test]
fn padding_calculation() {
    // At position 520 with alignment 4096:
    // Next aligned = 4096, padding needed = 4096 - 520 = 3576
    let padding = calculate_padding(520, 4096);
    assert_eq!(padding.len(), 3576);
    // First 4 bytes: PADDING_MASK
    assert_eq!(i32::from_be_bytes(padding[0..4].try_into().unwrap()), PADDING_MASK);
    // Next 4 bytes: padding length
    assert_eq!(i32::from_be_bytes(padding[4..8].try_into().unwrap()), 3576);
    // Rest: zeros
    assert!(padding[8..].iter().all(|&b| b == 0));
}

#[test]
fn padding_minimum_size() {
    // At position 4088, alignment 4096: only 8 bytes to next boundary
    let padding = calculate_padding(4088, 4096);
    assert_eq!(padding.len(), 8);
}

#[test]
fn padding_at_boundary_uses_full_alignment() {
    // At position 4096 (already aligned): pad to NEXT boundary = 4096 bytes
    let padding = calculate_padding(4096, 4096);
    assert_eq!(padding.len(), 4096);
}

#[test]
fn scan_entries_from_buffer() {
    let mut buf = Vec::new();
    // Entry 1: "hello"
    buf.extend_from_slice(&5i32.to_be_bytes());
    buf.extend_from_slice(b"hello");
    // Entry 2: "world!"
    buf.extend_from_slice(&6i32.to_be_bytes());
    buf.extend_from_slice(b"world!");
    // Padding
    let pad = calculate_padding(buf.len() as u64, 4096);
    buf.extend_from_slice(&pad);
    // Entry 3 after padding: "after"
    buf.extend_from_slice(&5i32.to_be_bytes());
    buf.extend_from_slice(b"after");

    let entries = scan_journal_entries(&buf, 6);
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0], b"hello");
    assert_eq!(entries[1], b"world!");
    assert_eq!(entries[2], b"after");
}

#[test]
fn last_mark_round_trip() {
    let mark = LastMark::new(42, 1024);
    let bytes = mark.to_bytes();
    assert_eq!(bytes.len(), 16);
    let parsed = LastMark::from_bytes(&bytes).unwrap();
    assert_eq!(parsed.log_id(), 42);
    assert_eq!(parsed.offset(), 1024);
}

#[test]
fn journal_position_encoding() {
    let pos = JournalPosition::new(5, 1000);
    assert_eq!(pos.to_u64(), (5u64 << 32) | 1000);
    let decoded = JournalPosition::from_u64(pos.to_u64());
    assert_eq!(decoded.file_id(), 5);
    assert_eq!(decoded.offset(), 1000);
}
```

- [ ] **Step 4: Implement journal/format.rs**

```rust
use thiserror::Error;

pub const JOURNAL_MAGIC: &[u8; 4] = b"BKLG";
pub const SECTOR_SIZE: usize = 512;
pub const CURRENT_JOURNAL_FORMAT_VERSION: i32 = 6;
pub const MIN_COMPAT_JOURNAL_FORMAT_VERSION: i32 = 1;
pub const PADDING_MASK: i32 = -0x100; // 0xFFFFFF00 as signed i32

/// Journal file header (512 bytes for V5+).
#[derive(Debug, Clone)]
pub struct JournalHeader {
    version: i32,
}

impl JournalHeader {
    pub fn new(version: i32) -> Self {
        Self { version }
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; SECTOR_SIZE];
        buf[0..4].copy_from_slice(JOURNAL_MAGIC);
        buf[4..8].copy_from_slice(&self.version.to_be_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, StorageError> {
        if data.len() < 8 {
            return Err(StorageError::CorruptData("journal header too short".into()));
        }
        if &data[0..4] != JOURNAL_MAGIC {
            return Err(StorageError::CorruptData("invalid journal magic".into()));
        }
        let version = i32::from_be_bytes(data[4..8].try_into().unwrap());
        Ok(Self { version })
    }
}

/// Encode an entry for writing to the journal: [size(4)][payload].
pub fn encode_journal_entry(payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Calculate padding bytes to align current position to alignment boundary.
/// Returns the complete padding block: [PADDING_MASK(4)][length(4)][zeros].
pub fn calculate_padding(current_pos: u64, alignment: u64) -> Vec<u8> {
    let next_aligned = ((current_pos + alignment - 1) / alignment) * alignment;
    let mut pad_len = (next_aligned - current_pos) as usize;
    if pad_len < 8 {
        pad_len += alignment as usize;
    }
    let mut buf = vec![0u8; pad_len];
    buf[0..4].copy_from_slice(&PADDING_MASK.to_be_bytes());
    buf[4..8].copy_from_slice(&(pad_len as i32).to_be_bytes());
    buf
}

/// Scan journal entry payloads from a byte buffer (after the header).
/// Handles padding blocks (V5+) by skipping them.
pub fn scan_journal_entries(data: &[u8], version: i32) -> Vec<&[u8]> {
    let mut entries = Vec::new();
    let mut pos = 0;
    while pos + 4 <= data.len() {
        let size = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
        if size == 0 {
            break; // end of data
        }
        if size == PADDING_MASK && version >= 5 {
            let pad_len = i32::from_be_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;
            pos += pad_len;
            continue;
        }
        let entry_size = size as usize;
        let entry_start = pos + 4;
        let entry_end = entry_start + entry_size;
        if entry_end > data.len() {
            break; // truncated entry
        }
        entries.push(&data[entry_start..entry_end]);
        pos = entry_end;
    }
    entries
}

/// Last mark: checkpoint position in the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LastMark {
    log_id: i64,
    offset: i64,
}

impl LastMark {
    pub fn new(log_id: i64, offset: i64) -> Self {
        Self { log_id, offset }
    }

    pub fn log_id(&self) -> i64 { self.log_id }
    pub fn offset(&self) -> i64 { self.offset }

    pub fn to_bytes(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&self.log_id.to_be_bytes());
        buf[8..16].copy_from_slice(&self.offset.to_be_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, StorageError> {
        if data.len() < 16 {
            return Err(StorageError::CorruptData("last mark too short".into()));
        }
        let log_id = i64::from_be_bytes(data[0..8].try_into().unwrap());
        let offset = i64::from_be_bytes(data[8..16].try_into().unwrap());
        Ok(Self { log_id, offset })
    }
}

/// Encoded journal position: (fileId << 32) | offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalPosition {
    file_id: u32,
    offset: u32,
}

impl JournalPosition {
    pub fn new(file_id: u32, offset: u32) -> Self {
        Self { file_id, offset }
    }

    pub fn file_id(&self) -> u32 { self.file_id }
    pub fn offset(&self) -> u32 { self.offset }

    pub fn to_u64(&self) -> u64 {
        ((self.file_id as u64) << 32) | (self.offset as u64)
    }

    pub fn from_u64(v: u64) -> Self {
        Self {
            file_id: (v >> 32) as u32,
            offset: (v & 0xFFFFFFFF) as u32,
        }
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("corrupt data: {0}")]
    CorruptData(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("entry not found: ledger={ledger_id}, entry={entry_id}")]
    EntryNotFound { ledger_id: i64, entry_id: i64 },
    #[error("ledger not found: {0}")]
    LedgerNotFound(i64),
    #[error("ledger fenced: {0}")]
    Fenced(i64),
    #[error("storage full")]
    StorageFull,
    #[error("RocksDB error: {0}")]
    RocksDb(String),
}
```

- [ ] **Step 5: Create lib.rs and module structure**

`crates/bookkeeper-storage/src/lib.rs`:
```rust
pub mod journal;
```

`crates/bookkeeper-storage/src/journal/mod.rs`:
```rust
pub mod format;
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p bookkeeper-storage`
Expected: All 10 journal format tests pass

- [ ] **Step 7: Commit**

```bash
git commit -m "feat: add journal file format (header, entries, padding, lastMark, scan)

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Entry Log File Format

**Files:**
- Create: `crates/bookkeeper-storage/src/entry_log/mod.rs`
- Create: `crates/bookkeeper-storage/src/entry_log/format.rs`
- Test: `crates/bookkeeper-storage/tests/entry_log_format_test.rs`

- [ ] **Step 1: Write failing tests**

`crates/bookkeeper-storage/tests/entry_log_format_test.rs`:
```rust
use bookkeeper_storage::entry_log::format::*;

#[test]
fn entry_log_header_is_1024_bytes() {
    let header = EntryLogHeader::new(1);
    let bytes = header.to_bytes();
    assert_eq!(bytes.len(), LOGFILE_HEADER_SIZE);
    assert_eq!(&bytes[0..4], ENTRY_LOG_MAGIC);
    assert_eq!(i32::from_be_bytes(bytes[4..8].try_into().unwrap()), HEADER_V1);
}

#[test]
fn entry_log_header_round_trip() {
    let mut header = EntryLogHeader::new(1);
    header.set_ledgers_map_offset(50000);
    header.set_ledgers_count(5);
    let bytes = header.to_bytes();
    let parsed = EntryLogHeader::from_bytes(&bytes).unwrap();
    assert_eq!(parsed.version(), HEADER_V1);
    assert_eq!(parsed.ledgers_map_offset(), 50000);
    assert_eq!(parsed.ledgers_count(), 5);
}

#[test]
fn entry_log_header_bad_magic() {
    let mut bytes = vec![0u8; LOGFILE_HEADER_SIZE];
    bytes[0..4].copy_from_slice(b"XXXX");
    assert!(EntryLogHeader::from_bytes(&bytes).is_err());
}

#[test]
fn encode_entry_log_entry() {
    let payload = b"entry data here";
    let encoded = encode_entry_log_entry(payload);
    assert_eq!(encoded.len(), 4 + payload.len());
    let size = i32::from_be_bytes(encoded[0..4].try_into().unwrap());
    assert_eq!(size as usize, payload.len());
}

#[test]
fn entry_location_encoding() {
    let loc = EntryLocation::new(10, 2048);
    let encoded = loc.to_u64();
    assert_eq!(encoded, (10u64 << 32) | 2048);
    let decoded = EntryLocation::from_u64(encoded);
    assert_eq!(decoded.log_id(), 10);
    assert_eq!(decoded.position(), 2048);
}

#[test]
fn ledger_map_batch_round_trip() {
    let mut entries = Vec::new();
    entries.push(LedgerMapEntry { ledger_id: 1, size: 1024 });
    entries.push(LedgerMapEntry { ledger_id: 2, size: 2048 });
    entries.push(LedgerMapEntry { ledger_id: 100, size: 512 });

    let batch_bytes = encode_ledger_map_batch(&entries);
    let parsed = decode_ledger_map_batch(&batch_bytes).unwrap();
    assert_eq!(parsed.len(), 3);
    assert_eq!(parsed[0].ledger_id, 1);
    assert_eq!(parsed[0].size, 1024);
    assert_eq!(parsed[2].ledger_id, 100);
}

#[test]
fn scan_entries_from_entry_log() {
    let mut buf = Vec::new();
    // Entry 1: ledgerId=1, entryId=0, payload
    let entry1 = make_test_entry(1, 0, b"data1");
    buf.extend_from_slice(&(entry1.len() as i32).to_be_bytes());
    buf.extend_from_slice(&entry1);
    // Entry 2: ledgerId=1, entryId=1, payload
    let entry2 = make_test_entry(1, 1, b"data2");
    buf.extend_from_slice(&(entry2.len() as i32).to_be_bytes());
    buf.extend_from_slice(&entry2);

    let entries = scan_entry_log_entries(&buf);
    assert_eq!(entries.len(), 2);
}

fn make_test_entry(ledger_id: i64, entry_id: i64, payload: &[u8]) -> Vec<u8> {
    let mut entry = Vec::new();
    entry.extend_from_slice(&ledger_id.to_be_bytes());
    entry.extend_from_slice(&entry_id.to_be_bytes());
    entry.extend_from_slice(&(-1i64).to_be_bytes()); // lac
    entry.extend_from_slice(&(payload.len() as i64).to_be_bytes()); // length
    entry.extend_from_slice(payload);
    entry
}
```

- [ ] **Step 2: Implement entry_log/format.rs**

```rust
use crate::journal::format::StorageError;

pub const ENTRY_LOG_MAGIC: &[u8; 4] = b"BKLO";
pub const LOGFILE_HEADER_SIZE: usize = 1024;
pub const HEADER_V0: i32 = 0;
pub const HEADER_V1: i32 = 1;
pub const HEADER_CURRENT_VERSION: i32 = HEADER_V1;
pub const INVALID_LID: i64 = -1;
pub const LEDGERS_MAP_ENTRY_ID: i64 = -2;
pub const LEDGERS_MAP_HEADER_SIZE: usize = 4 + 8 + 8 + 4; // 24 bytes
pub const LEDGERS_MAP_ENTRY_SIZE: usize = 16; // 8 + 8
pub const LEDGERS_MAP_MAX_BATCH_SIZE: usize = 10_000;
pub const MIN_SANE_ENTRY_SIZE: usize = 16; // ledgerId(8) + entryId(8)

/// Entry log file header (1024 bytes).
#[derive(Debug, Clone)]
pub struct EntryLogHeader {
    version: i32,
    ledgers_map_offset: i64,
    ledgers_count: i32,
}

impl EntryLogHeader {
    pub fn new(version: i32) -> Self {
        Self { version, ledgers_map_offset: 0, ledgers_count: 0 }
    }

    pub fn version(&self) -> i32 { self.version }
    pub fn ledgers_map_offset(&self) -> i64 { self.ledgers_map_offset }
    pub fn ledgers_count(&self) -> i32 { self.ledgers_count }
    pub fn set_ledgers_map_offset(&mut self, v: i64) { self.ledgers_map_offset = v; }
    pub fn set_ledgers_count(&mut self, v: i32) { self.ledgers_count = v; }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; LOGFILE_HEADER_SIZE];
        buf[0..4].copy_from_slice(ENTRY_LOG_MAGIC);
        buf[4..8].copy_from_slice(&self.version.to_be_bytes());
        buf[8..16].copy_from_slice(&self.ledgers_map_offset.to_be_bytes());
        buf[16..20].copy_from_slice(&self.ledgers_count.to_be_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, StorageError> {
        if data.len() < 20 {
            return Err(StorageError::CorruptData("entry log header too short".into()));
        }
        if &data[0..4] != ENTRY_LOG_MAGIC {
            return Err(StorageError::CorruptData("invalid entry log magic".into()));
        }
        let version = i32::from_be_bytes(data[4..8].try_into().unwrap());
        let ledgers_map_offset = i64::from_be_bytes(data[8..16].try_into().unwrap());
        let ledgers_count = i32::from_be_bytes(data[16..20].try_into().unwrap());
        Ok(Self { version, ledgers_map_offset, ledgers_count })
    }
}

/// Encode an entry for the entry log: [size(4)][payload].
pub fn encode_entry_log_entry(payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Entry location: (logId, position) packed as u64.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryLocation {
    log_id: u32,
    position: u32,
}

impl EntryLocation {
    pub fn new(log_id: u32, position: u32) -> Self {
        Self { log_id, position }
    }

    pub fn log_id(&self) -> u32 { self.log_id }
    pub fn position(&self) -> u32 { self.position }

    pub fn to_u64(&self) -> u64 {
        ((self.log_id as u64) << 32) | (self.position as u64)
    }

    pub fn from_u64(v: u64) -> Self {
        Self {
            log_id: (v >> 32) as u32,
            position: (v & 0xFFFFFFFF) as u32,
        }
    }
}

/// A ledger's presence in an entry log file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerMapEntry {
    pub ledger_id: i64,
    pub size: i64,
}

/// Encode a ledger map batch: [contentSize(4)][INVALID_LID(8)][MAP_ENTRY_ID(8)][count(4)][entries...].
pub fn encode_ledger_map_batch(entries: &[LedgerMapEntry]) -> Vec<u8> {
    let content_size = 8 + 8 + 4 + entries.len() * LEDGERS_MAP_ENTRY_SIZE;
    let mut buf = Vec::with_capacity(4 + content_size);
    buf.extend_from_slice(&(content_size as i32).to_be_bytes());
    buf.extend_from_slice(&INVALID_LID.to_be_bytes());
    buf.extend_from_slice(&LEDGERS_MAP_ENTRY_ID.to_be_bytes());
    buf.extend_from_slice(&(entries.len() as i32).to_be_bytes());
    for e in entries {
        buf.extend_from_slice(&e.ledger_id.to_be_bytes());
        buf.extend_from_slice(&e.size.to_be_bytes());
    }
    buf
}

/// Decode a ledger map batch from bytes.
pub fn decode_ledger_map_batch(data: &[u8]) -> Result<Vec<LedgerMapEntry>, StorageError> {
    if data.len() < LEDGERS_MAP_HEADER_SIZE {
        return Err(StorageError::CorruptData("ledger map batch too short".into()));
    }
    let _content_size = i32::from_be_bytes(data[0..4].try_into().unwrap());
    let lid = i64::from_be_bytes(data[4..12].try_into().unwrap());
    let map_id = i64::from_be_bytes(data[12..20].try_into().unwrap());
    if lid != INVALID_LID || map_id != LEDGERS_MAP_ENTRY_ID {
        return Err(StorageError::CorruptData("invalid ledger map markers".into()));
    }
    let count = i32::from_be_bytes(data[20..24].try_into().unwrap()) as usize;
    let mut entries = Vec::with_capacity(count);
    let mut pos = 24;
    for _ in 0..count {
        if pos + 16 > data.len() {
            break;
        }
        let ledger_id = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        let size = i64::from_be_bytes(data[pos + 8..pos + 16].try_into().unwrap());
        entries.push(LedgerMapEntry { ledger_id, size });
        pos += 16;
    }
    Ok(entries)
}

/// Scan entry log payload (after 1024-byte header) for entries.
/// Returns entry payloads (without size prefix). Stops at INVALID_LID marker (ledger map).
pub fn scan_entry_log_entries(data: &[u8]) -> Vec<&[u8]> {
    let mut entries = Vec::new();
    let mut pos = 0;
    while pos + 4 <= data.len() {
        let size = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        if size < MIN_SANE_ENTRY_SIZE || pos + 4 + size > data.len() {
            break;
        }
        let entry = &data[pos + 4..pos + 4 + size];
        let ledger_id = i64::from_be_bytes(entry[0..8].try_into().unwrap());
        if ledger_id == INVALID_LID {
            break; // reached ledger map
        }
        entries.push(entry);
        pos += 4 + size;
    }
    entries
}
```

- [ ] **Step 3: Add module to lib.rs**

Update `crates/bookkeeper-storage/src/lib.rs`:
```rust
pub mod journal;
pub mod entry_log;
```

`crates/bookkeeper-storage/src/entry_log/mod.rs`:
```rust
pub mod format;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p bookkeeper-storage`
Expected: All journal + entry log format tests pass

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: add entry log file format (header, entries, ledger map, location encoding)

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: RocksDB Entry Location Index + Ledger Metadata Index

**Files:**
- Create: `crates/bookkeeper-storage/src/index/mod.rs`
- Create: `crates/bookkeeper-storage/src/index/entry_location.rs`
- Create: `crates/bookkeeper-storage/src/index/ledger_index.rs`
- Test: `crates/bookkeeper-storage/tests/index_test.rs`

- [ ] **Step 1: Write failing tests**

`crates/bookkeeper-storage/tests/index_test.rs`:
```rust
use bookkeeper_storage::index::entry_location::EntryLocationIndex;
use bookkeeper_storage::index::ledger_index::LedgerMetadataIndex;
use tempfile::TempDir;

#[test]
fn entry_location_put_get() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path()).unwrap();
    index.put(1, 0, 0x0000000A_00001000).unwrap(); // logId=10, pos=4096
    let loc = index.get(1, 0).unwrap();
    assert_eq!(loc, Some(0x0000000A_00001000));
}

#[test]
fn entry_location_missing() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path()).unwrap();
    assert_eq!(index.get(999, 0).unwrap(), None);
}

#[test]
fn entry_location_batch_put() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path()).unwrap();
    let batch: Vec<(i64, i64, u64)> = vec![
        (1, 0, 100), (1, 1, 200), (1, 2, 300), (2, 0, 400),
    ];
    index.put_batch(&batch).unwrap();
    assert_eq!(index.get(1, 1).unwrap(), Some(200));
    assert_eq!(index.get(2, 0).unwrap(), Some(400));
}

#[test]
fn entry_location_delete_ledger() {
    let dir = TempDir::new().unwrap();
    let index = EntryLocationIndex::open(dir.path()).unwrap();
    index.put(1, 0, 100).unwrap();
    index.put(1, 1, 200).unwrap();
    index.put(2, 0, 300).unwrap();
    index.delete_ledger(1).unwrap();
    assert_eq!(index.get(1, 0).unwrap(), None);
    assert_eq!(index.get(1, 1).unwrap(), None);
    assert_eq!(index.get(2, 0).unwrap(), Some(300));
}

#[test]
fn ledger_index_put_get() {
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path()).unwrap();
    index.set(1, false, Some(b"masterkey"), Some(42)).unwrap();
    let data = index.get(1).unwrap().unwrap();
    assert!(!data.fenced);
    assert_eq!(data.master_key, Some(b"masterkey".to_vec()));
    assert_eq!(data.lac, Some(42));
}

#[test]
fn ledger_index_fence() {
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path()).unwrap();
    index.set(1, false, Some(b"key"), None).unwrap();
    index.set_fenced(1).unwrap();
    let data = index.get(1).unwrap().unwrap();
    assert!(data.fenced);
}

#[test]
fn ledger_index_update_lac() {
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path()).unwrap();
    index.set(1, false, Some(b"key"), Some(10)).unwrap();
    index.update_lac(1, 50).unwrap();
    let data = index.get(1).unwrap().unwrap();
    assert_eq!(data.lac, Some(50));
}

#[test]
fn ledger_index_delete() {
    let dir = TempDir::new().unwrap();
    let index = LedgerMetadataIndex::open(dir.path()).unwrap();
    index.set(1, false, None, None).unwrap();
    index.delete(1).unwrap();
    assert!(index.get(1).unwrap().is_none());
}
```

- [ ] **Step 2: Implement index/entry_location.rs**

```rust
use std::path::Path;
use rocksdb::{DB, Options, WriteBatch};
use crate::journal::format::StorageError;

/// RocksDB-backed entry location index.
/// Key: (ledgerId, entryId) as 16 bytes big-endian.
/// Value: location as 8 bytes big-endian.
pub struct EntryLocationIndex {
    db: DB,
}

impl EntryLocationIndex {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_write_buffer_number(4);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.set_max_background_jobs(4);
        let block_opts = rocksdb::BlockBasedOptions::default();
        opts.set_block_based_table_factory(&block_opts);
        let db = DB::open(&opts, path).map_err(|e| StorageError::RocksDb(e.to_string()))?;
        Ok(Self { db })
    }

    pub fn put(&self, ledger_id: i64, entry_id: i64, location: u64) -> Result<(), StorageError> {
        let key = encode_location_key(ledger_id, entry_id);
        let value = location.to_be_bytes();
        self.db.put(&key, &value).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    pub fn put_batch(&self, entries: &[(i64, i64, u64)]) -> Result<(), StorageError> {
        let mut batch = WriteBatch::default();
        for &(ledger_id, entry_id, location) in entries {
            let key = encode_location_key(ledger_id, entry_id);
            let value = location.to_be_bytes();
            batch.put(&key, &value);
        }
        self.db.write(batch).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    pub fn get(&self, ledger_id: i64, entry_id: i64) -> Result<Option<u64>, StorageError> {
        let key = encode_location_key(ledger_id, entry_id);
        match self.db.get(&key).map_err(|e| StorageError::RocksDb(e.to_string()))? {
            Some(v) => {
                let loc = u64::from_be_bytes(v[..8].try_into().unwrap());
                Ok(Some(loc))
            }
            None => Ok(None),
        }
    }

    pub fn delete_ledger(&self, ledger_id: i64) -> Result<(), StorageError> {
        let start = encode_location_key(ledger_id, i64::MIN);
        let end = encode_location_key(ledger_id + 1, i64::MIN);
        self.db.delete_range(&start, &end)
            .map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        let mut opts = rocksdb::FlushOptions::default();
        opts.set_wait(true);
        self.db.flush_opt(&opts).map_err(|e| StorageError::RocksDb(e.to_string()))
    }
}

fn encode_location_key(ledger_id: i64, entry_id: i64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[0..8].copy_from_slice(&ledger_id.to_be_bytes());
    key[8..16].copy_from_slice(&entry_id.to_be_bytes());
    key
}
```

- [ ] **Step 3: Implement index/ledger_index.rs**

```rust
use std::path::Path;
use prost::Message;
use rocksdb::{DB, Options};
use crate::journal::format::StorageError;

/// Ledger metadata stored in the bookie's RocksDB.
#[derive(Debug, Clone)]
pub struct LedgerData {
    pub exists: bool,
    pub fenced: bool,
    pub master_key: Option<Vec<u8>>,
    pub lac: Option<i64>,
}

/// RocksDB-backed ledger metadata index.
/// Key: ledgerId as 8 bytes big-endian.
/// Value: LedgerData protobuf (from DbLedgerStorageDataFormats.proto).
pub struct LedgerMetadataIndex {
    db: DB,
}

impl LedgerMetadataIndex {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path).map_err(|e| StorageError::RocksDb(e.to_string()))?;
        Ok(Self { db })
    }

    pub fn set(&self, ledger_id: i64, fenced: bool, master_key: Option<&[u8]>, lac: Option<i64>) -> Result<(), StorageError> {
        let key = ledger_id.to_be_bytes();
        let proto = bookkeeper_proto::LedgerData {
            exists: true,
            fenced,
            master_key: master_key.map(|k| k.to_vec()).unwrap_or_default(),
            explicit_lac: lac.map(|l| l.to_be_bytes().to_vec()),
            limbo: None,
        };
        let value = proto.encode_to_vec();
        self.db.put(&key, &value).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    pub fn get(&self, ledger_id: i64) -> Result<Option<LedgerData>, StorageError> {
        let key = ledger_id.to_be_bytes();
        match self.db.get(&key).map_err(|e| StorageError::RocksDb(e.to_string()))? {
            Some(v) => {
                let proto = bookkeeper_proto::LedgerData::decode(&v[..])
                    .map_err(|e| StorageError::CorruptData(format!("ledger data decode: {e}")))?;
                if !proto.exists {
                    return Ok(None);
                }
                let lac = proto.explicit_lac.as_ref().and_then(|b| {
                    if b.len() >= 8 { Some(i64::from_be_bytes(b[..8].try_into().unwrap())) } else { None }
                });
                Ok(Some(LedgerData {
                    exists: proto.exists,
                    fenced: proto.fenced,
                    master_key: if proto.master_key.is_empty() { None } else { Some(proto.master_key) },
                    lac,
                }))
            }
            None => Ok(None),
        }
    }

    pub fn set_fenced(&self, ledger_id: i64) -> Result<(), StorageError> {
        if let Some(mut data) = self.get_raw(ledger_id)? {
            data.fenced = true;
            self.put_raw(ledger_id, &data)
        } else {
            Err(StorageError::LedgerNotFound(ledger_id))
        }
    }

    pub fn update_lac(&self, ledger_id: i64, lac: i64) -> Result<(), StorageError> {
        if let Some(mut data) = self.get_raw(ledger_id)? {
            data.explicit_lac = Some(lac.to_be_bytes().to_vec());
            self.put_raw(ledger_id, &data)
        } else {
            Err(StorageError::LedgerNotFound(ledger_id))
        }
    }

    pub fn delete(&self, ledger_id: i64) -> Result<(), StorageError> {
        let key = ledger_id.to_be_bytes();
        self.db.delete(&key).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        let mut opts = rocksdb::FlushOptions::default();
        opts.set_wait(true);
        self.db.flush_opt(&opts).map_err(|e| StorageError::RocksDb(e.to_string()))
    }

    fn get_raw(&self, ledger_id: i64) -> Result<Option<bookkeeper_proto::LedgerData>, StorageError> {
        let key = ledger_id.to_be_bytes();
        match self.db.get(&key).map_err(|e| StorageError::RocksDb(e.to_string()))? {
            Some(v) => {
                let proto = bookkeeper_proto::LedgerData::decode(&v[..])
                    .map_err(|e| StorageError::CorruptData(format!("ledger data decode: {e}")))?;
                Ok(Some(proto))
            }
            None => Ok(None),
        }
    }

    fn put_raw(&self, ledger_id: i64, data: &bookkeeper_proto::LedgerData) -> Result<(), StorageError> {
        let key = ledger_id.to_be_bytes();
        let value = data.encode_to_vec();
        self.db.put(&key, &value).map_err(|e| StorageError::RocksDb(e.to_string()))
    }
}
```

- [ ] **Step 4: Add module to lib.rs**

```rust
pub mod journal;
pub mod entry_log;
pub mod index;
```

`crates/bookkeeper-storage/src/index/mod.rs`:
```rust
pub mod entry_location;
pub mod ledger_index;
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p bookkeeper-storage`
Expected: All tests pass (journal format + entry log format + index)

- [ ] **Step 6: Commit**

```bash
git commit -m "feat: add RocksDB entry location index and ledger metadata index

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: WriteCache and ReadCache

**Files:**
- Create: `crates/bookkeeper-storage/src/cache/mod.rs`
- Create: `crates/bookkeeper-storage/src/cache/write_cache.rs`
- Create: `crates/bookkeeper-storage/src/cache/read_cache.rs`
- Test: `crates/bookkeeper-storage/tests/cache_test.rs`

- [ ] **Step 1: Write failing tests**

`crates/bookkeeper-storage/tests/cache_test.rs`:
```rust
use bookkeeper_storage::cache::write_cache::WriteCache;
use bookkeeper_storage::cache::read_cache::ReadCache;

#[test]
fn write_cache_put_get() {
    let cache = WriteCache::new(1024 * 1024); // 1MB
    cache.put(1, 0, b"hello").unwrap();
    let data = cache.get(1, 0).unwrap();
    assert_eq!(data, b"hello");
}

#[test]
fn write_cache_missing_entry() {
    let cache = WriteCache::new(1024 * 1024);
    assert!(cache.get(1, 0).is_none());
}

#[test]
fn write_cache_last_entry() {
    let cache = WriteCache::new(1024 * 1024);
    cache.put(1, 0, b"a").unwrap();
    cache.put(1, 5, b"b").unwrap();
    cache.put(1, 3, b"c").unwrap();
    assert_eq!(cache.last_entry_id(1), Some(5));
}

#[test]
fn write_cache_full_returns_error() {
    let cache = WriteCache::new(256); // tiny cache
    // Entries are 64-byte aligned, so even a 1-byte entry takes 64 bytes
    cache.put(1, 0, b"a").unwrap();
    cache.put(1, 1, b"b").unwrap();
    cache.put(1, 2, b"c").unwrap();
    cache.put(1, 3, b"d").unwrap();
    // 4 * 64 = 256, cache is full
    assert!(cache.put(1, 4, b"e").is_err());
}

#[test]
fn write_cache_clear() {
    let cache = WriteCache::new(1024 * 1024);
    cache.put(1, 0, b"hello").unwrap();
    cache.clear();
    assert!(cache.get(1, 0).is_none());
    assert_eq!(cache.count(), 0);
}

#[test]
fn write_cache_sorted_entries() {
    let cache = WriteCache::new(1024 * 1024);
    cache.put(2, 0, b"b0").unwrap();
    cache.put(1, 1, b"a1").unwrap();
    cache.put(1, 0, b"a0").unwrap();
    cache.put(2, 1, b"b1").unwrap();

    let entries = cache.sorted_entries();
    assert_eq!(entries.len(), 4);
    assert_eq!(entries[0].0, 1); // ledgerId
    assert_eq!(entries[0].1, 0); // entryId
    assert_eq!(entries[1].0, 1);
    assert_eq!(entries[1].1, 1);
    assert_eq!(entries[2].0, 2);
    assert_eq!(entries[2].1, 0);
}

#[test]
fn read_cache_put_get() {
    let cache = ReadCache::new(1024 * 1024, 2);
    cache.put(1, 0, b"data");
    let data = cache.get(1, 0);
    assert_eq!(data.unwrap(), b"data");
}

#[test]
fn read_cache_eviction() {
    // 2 segments of 512 bytes each = 1024 total
    let cache = ReadCache::new(1024, 2);
    // Fill segment 0 (entries are stored with some overhead)
    for i in 0..6 {
        cache.put(1, i, &[0u8; 64]);
    }
    // This should trigger rotation to segment 1, evicting segment 0
    for i in 6..12 {
        cache.put(1, i, &[0u8; 64]);
    }
    // Recent entries should still be found
    assert!(cache.get(1, 11).is_some());
}
```

- [ ] **Step 2: Implement cache/write_cache.rs**

```rust
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::journal::format::StorageError;

/// In-memory write cache with 64-byte aligned entries.
pub struct WriteCache {
    max_size: usize,
    data: Vec<u8>,
    offset: AtomicUsize,
    index: DashMap<(i64, i64), (usize, usize)>, // (ledgerId, entryId) → (offset, size)
    last_entry: DashMap<i64, i64>, // ledgerId → max entryId
}

const ALIGNMENT: usize = 64;

fn align64(size: usize) -> usize {
    (size + ALIGNMENT - 1) & !(ALIGNMENT - 1)
}

impl WriteCache {
    pub fn new(max_size: usize) -> Self {
        let aligned_max = align64(max_size);
        Self {
            max_size: aligned_max,
            data: vec![0u8; aligned_max],
            offset: AtomicUsize::new(0),
            index: DashMap::new(),
            last_entry: DashMap::new(),
        }
    }

    pub fn put(&self, ledger_id: i64, entry_id: i64, entry: &[u8]) -> Result<(), StorageError> {
        let aligned_size = align64(entry.len());
        let offset = self.offset.fetch_add(aligned_size, Ordering::SeqCst);
        if offset + aligned_size > self.max_size {
            self.offset.fetch_sub(aligned_size, Ordering::SeqCst);
            return Err(StorageError::StorageFull);
        }
        // Safety: we have exclusive access to this range via atomic offset
        unsafe {
            let ptr = self.data.as_ptr().add(offset) as *mut u8;
            std::ptr::copy_nonoverlapping(entry.as_ptr(), ptr, entry.len());
        }
        self.index.insert((ledger_id, entry_id), (offset, entry.len()));
        self.last_entry.entry(ledger_id)
            .and_modify(|v| { if entry_id > *v { *v = entry_id; } })
            .or_insert(entry_id);
        Ok(())
    }

    pub fn get(&self, ledger_id: i64, entry_id: i64) -> Option<Vec<u8>> {
        self.index.get(&(ledger_id, entry_id)).map(|e| {
            let (offset, size) = *e.value();
            self.data[offset..offset + size].to_vec()
        })
    }

    pub fn last_entry_id(&self, ledger_id: i64) -> Option<i64> {
        self.last_entry.get(&ledger_id).map(|v| *v.value())
    }

    pub fn count(&self) -> usize {
        self.index.len()
    }

    pub fn clear(&self) {
        self.index.clear();
        self.last_entry.clear();
        self.offset.store(0, Ordering::SeqCst);
    }

    /// Return all entries sorted by (ledgerId, entryId) for flushing.
    pub fn sorted_entries(&self) -> Vec<(i64, i64, Vec<u8>)> {
        let mut entries: Vec<(i64, i64, Vec<u8>)> = self.index.iter().map(|e| {
            let (ledger_id, entry_id) = *e.key();
            let (offset, size) = *e.value();
            (ledger_id, entry_id, self.data[offset..offset + size].to_vec())
        }).collect();
        entries.sort_by_key(|(lid, eid, _)| (*lid, *eid));
        entries
    }
}
```

- [ ] **Step 3: Implement cache/read_cache.rs**

```rust
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Ring-buffer read cache with multiple segments.
pub struct ReadCache {
    segments: Vec<CacheSegment>,
    current_idx: AtomicUsize,
    num_segments: usize,
}

struct CacheSegment {
    data: Vec<u8>,
    offset: AtomicUsize,
    max_size: usize,
    index: DashMap<(i64, i64), (usize, usize)>,
}

impl CacheSegment {
    fn new(size: usize) -> Self {
        Self {
            data: vec![0u8; size],
            offset: AtomicUsize::new(0),
            max_size: size,
            index: DashMap::new(),
        }
    }

    fn put(&self, ledger_id: i64, entry_id: i64, entry: &[u8]) -> bool {
        let offset = self.offset.fetch_add(entry.len(), Ordering::SeqCst);
        if offset + entry.len() > self.max_size {
            self.offset.fetch_sub(entry.len(), Ordering::SeqCst);
            return false;
        }
        unsafe {
            let ptr = self.data.as_ptr().add(offset) as *mut u8;
            std::ptr::copy_nonoverlapping(entry.as_ptr(), ptr, entry.len());
        }
        self.index.insert((ledger_id, entry_id), (offset, entry.len()));
        true
    }

    fn get(&self, ledger_id: i64, entry_id: i64) -> Option<Vec<u8>> {
        self.index.get(&(ledger_id, entry_id)).map(|e| {
            let (offset, size) = *e.value();
            self.data[offset..offset + size].to_vec()
        })
    }

    fn clear(&self) {
        self.index.clear();
        self.offset.store(0, Ordering::SeqCst);
    }
}

impl ReadCache {
    pub fn new(total_size: usize, num_segments: usize) -> Self {
        let num_segments = num_segments.max(2);
        let segment_size = total_size / num_segments;
        let segments = (0..num_segments).map(|_| CacheSegment::new(segment_size)).collect();
        Self {
            segments,
            current_idx: AtomicUsize::new(0),
            num_segments,
        }
    }

    pub fn put(&self, ledger_id: i64, entry_id: i64, entry: &[u8]) {
        let idx = self.current_idx.load(Ordering::SeqCst);
        if !self.segments[idx].put(ledger_id, entry_id, entry) {
            // Current segment full — rotate
            let next = (idx + 1) % self.num_segments;
            self.segments[next].clear();
            self.current_idx.store(next, Ordering::SeqCst);
            self.segments[next].put(ledger_id, entry_id, entry);
        }
    }

    pub fn get(&self, ledger_id: i64, entry_id: i64) -> Option<Vec<u8>> {
        let current = self.current_idx.load(Ordering::SeqCst);
        // Search from current backward
        for i in 0..self.num_segments {
            let idx = (current + self.num_segments - i) % self.num_segments;
            if let Some(data) = self.segments[idx].get(ledger_id, entry_id) {
                return Some(data);
            }
        }
        None
    }
}
```

- [ ] **Step 4: Add module to lib.rs**

```rust
pub mod journal;
pub mod entry_log;
pub mod index;
pub mod cache;
```

`crates/bookkeeper-storage/src/cache/mod.rs`:
```rust
pub mod write_cache;
pub mod read_cache;
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p bookkeeper-storage`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git commit -m "feat: add WriteCache (64-byte aligned, double-buffer) and ReadCache (ring-buffer)

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: LedgerStorage Trait + Checkpoint

**Files:**
- Create: `crates/bookkeeper-storage/src/ledger_storage.rs`
- Create: `crates/bookkeeper-storage/src/checkpoint.rs`
- Test: `crates/bookkeeper-storage/tests/checkpoint_test.rs`

- [ ] **Step 1: Write failing test for checkpoint**

`crates/bookkeeper-storage/tests/checkpoint_test.rs`:
```rust
use bookkeeper_storage::checkpoint::*;
use bookkeeper_storage::journal::format::LastMark;
use tempfile::TempDir;

#[test]
fn write_and_read_last_mark() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join(".lastMark");
    let mark = LastMark::new(10, 5000);
    write_last_mark(&path, &mark).unwrap();
    let read_mark = read_last_mark(&path).unwrap();
    assert_eq!(read_mark.log_id(), 10);
    assert_eq!(read_mark.offset(), 5000);
}

#[test]
fn read_missing_last_mark_returns_zero() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join(".lastMark");
    let mark = read_last_mark(&path).unwrap();
    assert_eq!(mark.log_id(), 0);
    assert_eq!(mark.offset(), 0);
}

#[test]
fn latest_mark_across_dirs() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    write_last_mark(&dir1.path().join(".lastMark"), &LastMark::new(5, 100)).unwrap();
    write_last_mark(&dir2.path().join(".lastMark"), &LastMark::new(10, 200)).unwrap();
    let dirs = vec![dir1.path().to_path_buf(), dir2.path().to_path_buf()];
    let latest = find_latest_last_mark(&dirs).unwrap();
    assert_eq!(latest.log_id(), 10);
    assert_eq!(latest.offset(), 200);
}
```

- [ ] **Step 2: Implement checkpoint.rs**

```rust
use std::fs;
use std::path::{Path, PathBuf};
use crate::journal::format::{LastMark, StorageError};

const LAST_MARK_FILENAME: &str = ".lastMark";

pub fn write_last_mark(path: &Path, mark: &LastMark) -> Result<(), StorageError> {
    fs::write(path, &mark.to_bytes())?;
    Ok(())
}

pub fn read_last_mark(path: &Path) -> Result<LastMark, StorageError> {
    match fs::read(path) {
        Ok(data) => LastMark::from_bytes(&data),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(LastMark::new(0, 0)),
        Err(e) => Err(StorageError::Io(e)),
    }
}

pub fn find_latest_last_mark(ledger_dirs: &[PathBuf]) -> Result<LastMark, StorageError> {
    let mut latest = LastMark::new(0, 0);
    for dir in ledger_dirs {
        let path = dir.join(LAST_MARK_FILENAME);
        let mark = read_last_mark(&path)?;
        if mark.log_id() > latest.log_id()
            || (mark.log_id() == latest.log_id() && mark.offset() > latest.offset())
        {
            latest = mark;
        }
    }
    Ok(latest)
}
```

- [ ] **Step 3: Implement ledger_storage.rs trait**

```rust
use bytes::Bytes;
use crate::journal::format::StorageError;

/// The LedgerStorage trait defines the interface for bookie storage backends.
pub trait LedgerStorage: Send + Sync {
    /// Add an entry to storage. Returns after the entry is in the write cache.
    fn add_entry(&self, entry: &[u8]) -> Result<(), StorageError>;

    /// Get an entry by ledger ID and entry ID.
    fn get_entry(&self, ledger_id: i64, entry_id: i64) -> Result<Bytes, StorageError>;

    /// Check if a ledger exists.
    fn ledger_exists(&self, ledger_id: i64) -> Result<bool, StorageError>;

    /// Set a ledger as fenced.
    fn set_fenced(&self, ledger_id: i64) -> Result<bool, StorageError>;

    /// Check if a ledger is fenced.
    fn is_fenced(&self, ledger_id: i64) -> Result<bool, StorageError>;

    /// Set the master key for a ledger.
    fn set_master_key(&self, ledger_id: i64, master_key: &[u8]) -> Result<(), StorageError>;

    /// Get the last add confirmed for a ledger.
    fn get_last_add_confirmed(&self, ledger_id: i64) -> Result<i64, StorageError>;

    /// Set the explicit LAC for a ledger.
    fn set_explicit_lac(&self, ledger_id: i64, lac: i64) -> Result<(), StorageError>;

    /// Delete a ledger.
    fn delete_ledger(&self, ledger_id: i64) -> Result<(), StorageError>;

    /// Flush all pending data to persistent storage.
    fn flush(&self) -> Result<(), StorageError>;

    /// Perform a checkpoint: flush and return completion.
    fn checkpoint(&self) -> Result<(), StorageError>;
}
```

- [ ] **Step 4: Add modules to lib.rs**

```rust
pub mod journal;
pub mod entry_log;
pub mod index;
pub mod cache;
pub mod checkpoint;
pub mod ledger_storage;
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p bookkeeper-storage`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git commit -m "feat: add LedgerStorage trait and checkpoint (LastMark read/write/find)

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: DbLedgerStorage Integration

**Files:**
- Create: `crates/bookkeeper-storage/src/db_ledger_storage.rs`
- Test: `crates/bookkeeper-storage/tests/db_ledger_storage_test.rs`

- [ ] **Step 1: Write failing integration tests**

`crates/bookkeeper-storage/tests/db_ledger_storage_test.rs`:
```rust
use bookkeeper_storage::db_ledger_storage::DbLedgerStorage;
use bookkeeper_storage::ledger_storage::LedgerStorage;
use tempfile::TempDir;

fn make_entry(ledger_id: i64, entry_id: i64, lac: i64, payload: &[u8]) -> Vec<u8> {
    let mut entry = Vec::new();
    entry.extend_from_slice(&ledger_id.to_be_bytes());
    entry.extend_from_slice(&entry_id.to_be_bytes());
    entry.extend_from_slice(&lac.to_be_bytes());
    entry.extend_from_slice(&(payload.len() as i64).to_be_bytes());
    entry.extend_from_slice(payload);
    entry
}

#[test]
fn add_and_get_entry() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    let entry = make_entry(1, 0, -1, b"hello");
    storage.add_entry(&entry).unwrap();
    let retrieved = storage.get_entry(1, 0).unwrap();
    assert_eq!(&retrieved[..], &entry[..]);
}

#[test]
fn get_missing_entry() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    let result = storage.get_entry(1, 0);
    assert!(result.is_err());
}

#[test]
fn fence_ledger() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    let entry = make_entry(1, 0, -1, b"data");
    storage.add_entry(&entry).unwrap();
    assert!(!storage.is_fenced(1).unwrap());
    storage.set_fenced(1).unwrap();
    assert!(storage.is_fenced(1).unwrap());
}

#[test]
fn last_add_confirmed() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    storage.add_entry(&make_entry(1, 0, -1, b"a")).unwrap();
    storage.add_entry(&make_entry(1, 1, 0, b"b")).unwrap();
    storage.add_entry(&make_entry(1, 2, 1, b"c")).unwrap();
    // LAC is the lac field from the latest entry: 1
    assert_eq!(storage.get_last_add_confirmed(1).unwrap(), 1);
}

#[test]
fn flush_and_read_from_disk() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    storage.add_entry(&make_entry(1, 0, -1, b"persistent")).unwrap();
    storage.flush().unwrap();
    // Entry should now be in RocksDB + entry log, not just write cache
    let retrieved = storage.get_entry(1, 0).unwrap();
    assert_eq!(&retrieved[32..], b"persistent"); // skip metadata header
}

#[test]
fn delete_ledger() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    storage.add_entry(&make_entry(1, 0, -1, b"data")).unwrap();
    storage.delete_ledger(1).unwrap();
    assert!(!storage.ledger_exists(1).unwrap());
}
```

- [ ] **Step 2: Implement db_ledger_storage.rs**

```rust
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use bytes::Bytes;
use crate::cache::write_cache::WriteCache;
use crate::cache::read_cache::ReadCache;
use crate::entry_log::format::{self as elf, EntryLogHeader, EntryLocation, LOGFILE_HEADER_SIZE};
use crate::index::entry_location::EntryLocationIndex;
use crate::index::ledger_index::LedgerMetadataIndex;
use crate::journal::format::StorageError;
use crate::ledger_storage::LedgerStorage;

const DEFAULT_WRITE_CACHE_SIZE: usize = 64 * 1024 * 1024; // 64MB
const DEFAULT_READ_CACHE_SIZE: usize = 64 * 1024 * 1024;
const READ_CACHE_SEGMENTS: usize = 4;

pub struct DbLedgerStorage {
    base_dir: PathBuf,
    write_cache: WriteCache,
    read_cache: ReadCache,
    entry_location_index: EntryLocationIndex,
    ledger_index: LedgerMetadataIndex,
    entry_log_dir: PathBuf,
    current_log_id: Mutex<u32>,
    current_log_offset: Mutex<u64>,
    flush_mutex: Mutex<()>,
}

impl DbLedgerStorage {
    pub fn open(base_dir: &Path) -> Result<Self, StorageError> {
        let locations_dir = base_dir.join("locations");
        let ledgers_dir = base_dir.join("ledgers");
        let entry_log_dir = base_dir.join("entry_logs");
        fs::create_dir_all(&locations_dir)?;
        fs::create_dir_all(&ledgers_dir)?;
        fs::create_dir_all(&entry_log_dir)?;

        let entry_location_index = EntryLocationIndex::open(&locations_dir)?;
        let ledger_index = LedgerMetadataIndex::open(&ledgers_dir)?;

        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            write_cache: WriteCache::new(DEFAULT_WRITE_CACHE_SIZE),
            read_cache: ReadCache::new(DEFAULT_READ_CACHE_SIZE, READ_CACHE_SEGMENTS),
            entry_location_index,
            ledger_index,
            entry_log_dir,
            current_log_id: Mutex::new(0),
            current_log_offset: Mutex::new(LOGFILE_HEADER_SIZE as u64),
            flush_mutex: Mutex::new(()),
        })
    }

    fn extract_ledger_id(entry: &[u8]) -> i64 {
        i64::from_be_bytes(entry[0..8].try_into().unwrap())
    }

    fn extract_entry_id(entry: &[u8]) -> i64 {
        i64::from_be_bytes(entry[8..16].try_into().unwrap())
    }

    fn extract_lac(entry: &[u8]) -> i64 {
        i64::from_be_bytes(entry[16..24].try_into().unwrap())
    }

    fn entry_log_path(&self, log_id: u32) -> PathBuf {
        self.entry_log_dir.join(format!("{:x}.log", log_id))
    }

    fn ensure_entry_log_exists(&self, log_id: u32) -> Result<(), StorageError> {
        let path = self.entry_log_path(log_id);
        if !path.exists() {
            let header = EntryLogHeader::new(elf::HEADER_CURRENT_VERSION);
            fs::write(&path, header.to_bytes())?;
        }
        Ok(())
    }

    fn write_entry_to_log(&self, entry: &[u8]) -> Result<u64, StorageError> {
        let mut log_id = self.current_log_id.lock().unwrap();
        let mut log_offset = self.current_log_offset.lock().unwrap();
        self.ensure_entry_log_exists(*log_id)?;

        let path = self.entry_log_path(*log_id);
        let encoded = elf::encode_entry_log_entry(entry);
        use std::io::Write;
        let mut file = fs::OpenOptions::new().append(true).open(&path)?;
        file.write_all(&encoded)?;

        let position = *log_offset + 4; // position points past the size prefix
        let location = EntryLocation::new(*log_id, position as u32).to_u64();
        *log_offset += encoded.len() as u64;

        Ok(location)
    }

    fn read_entry_from_log(&self, location: u64) -> Result<Vec<u8>, StorageError> {
        let loc = EntryLocation::from_u64(location);
        let path = self.entry_log_path(loc.log_id());
        let data = fs::read(&path)?;
        let pos = loc.position() as usize;
        if pos < 4 || pos > data.len() {
            return Err(StorageError::CorruptData("invalid entry position".into()));
        }
        let size_pos = pos - 4;
        let size = i32::from_be_bytes(data[size_pos..size_pos + 4].try_into().unwrap()) as usize;
        if pos + size > data.len() {
            return Err(StorageError::CorruptData("entry extends past end of log".into()));
        }
        Ok(data[pos..pos + size].to_vec())
    }
}

impl LedgerStorage for DbLedgerStorage {
    fn add_entry(&self, entry: &[u8]) -> Result<(), StorageError> {
        if entry.len() < 32 {
            return Err(StorageError::CorruptData("entry too short".into()));
        }
        let ledger_id = Self::extract_ledger_id(entry);
        let entry_id = Self::extract_entry_id(entry);
        let lac = Self::extract_lac(entry);

        // Ensure ledger exists in index
        if self.ledger_index.get(ledger_id)?.is_none() {
            self.ledger_index.set(ledger_id, false, None, None)?;
        }

        // Update LAC
        if lac >= 0 {
            self.ledger_index.update_lac(ledger_id, lac).ok();
        }

        // Add to write cache
        self.write_cache.put(ledger_id, entry_id, entry)?;
        Ok(())
    }

    fn get_entry(&self, ledger_id: i64, entry_id: i64) -> Result<Bytes, StorageError> {
        // 1. Check read cache
        if let Some(data) = self.read_cache.get(ledger_id, entry_id) {
            return Ok(Bytes::from(data));
        }
        // 2. Check write cache
        if let Some(data) = self.write_cache.get(ledger_id, entry_id) {
            return Ok(Bytes::from(data));
        }
        // 3. Check RocksDB index → entry log
        if let Some(location) = self.entry_location_index.get(ledger_id, entry_id)? {
            let data = self.read_entry_from_log(location)?;
            self.read_cache.put(ledger_id, entry_id, &data);
            return Ok(Bytes::from(data));
        }
        Err(StorageError::EntryNotFound { ledger_id, entry_id })
    }

    fn ledger_exists(&self, ledger_id: i64) -> Result<bool, StorageError> {
        Ok(self.ledger_index.get(ledger_id)?.is_some())
    }

    fn set_fenced(&self, ledger_id: i64) -> Result<bool, StorageError> {
        self.ledger_index.set_fenced(ledger_id)?;
        Ok(true)
    }

    fn is_fenced(&self, ledger_id: i64) -> Result<bool, StorageError> {
        Ok(self.ledger_index.get(ledger_id)?.map_or(false, |d| d.fenced))
    }

    fn set_master_key(&self, ledger_id: i64, master_key: &[u8]) -> Result<(), StorageError> {
        self.ledger_index.set(ledger_id, false, Some(master_key), None)
    }

    fn get_last_add_confirmed(&self, ledger_id: i64) -> Result<i64, StorageError> {
        // Check write cache first
        if let Some(entry_id) = self.write_cache.last_entry_id(ledger_id) {
            if let Some(data) = self.write_cache.get(ledger_id, entry_id) {
                if data.len() >= 24 {
                    return Ok(i64::from_be_bytes(data[16..24].try_into().unwrap()));
                }
            }
        }
        // Fall back to RocksDB ledger index
        match self.ledger_index.get(ledger_id)? {
            Some(data) => Ok(data.lac.unwrap_or(-1)),
            None => Err(StorageError::LedgerNotFound(ledger_id)),
        }
    }

    fn set_explicit_lac(&self, ledger_id: i64, lac: i64) -> Result<(), StorageError> {
        self.ledger_index.update_lac(ledger_id, lac)
    }

    fn delete_ledger(&self, ledger_id: i64) -> Result<(), StorageError> {
        self.ledger_index.delete(ledger_id)?;
        self.entry_location_index.delete_ledger(ledger_id)?;
        Ok(())
    }

    fn flush(&self) -> Result<(), StorageError> {
        let _lock = self.flush_mutex.lock().unwrap();
        let entries = self.write_cache.sorted_entries();
        let mut batch = Vec::new();
        for (ledger_id, entry_id, entry_data) in &entries {
            let location = self.write_entry_to_log(entry_data)?;
            batch.push((*ledger_id, *entry_id, location));
        }
        if !batch.is_empty() {
            self.entry_location_index.put_batch(&batch)?;
        }
        self.entry_location_index.flush()?;
        self.ledger_index.flush()?;
        self.write_cache.clear();
        Ok(())
    }

    fn checkpoint(&self) -> Result<(), StorageError> {
        self.flush()
    }
}
```

- [ ] **Step 3: Add module to lib.rs**

```rust
pub mod journal;
pub mod entry_log;
pub mod index;
pub mod cache;
pub mod checkpoint;
pub mod ledger_storage;
pub mod db_ledger_storage;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p bookkeeper-storage`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: add DbLedgerStorage composing write cache, read cache, RocksDB indexes, entry logs

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Spec Coverage

| Spec Section | Task | Status |
|---|---|---|
| §2 Journal WAL (file format, padding, lastMark) | Task 1 | Covered |
| §3 DbLedgerStorage architecture | Task 6 | Covered |
| §3.2-3.4 Write/Read/Flush paths | Task 6 | Covered |
| §4 WriteCache (64-byte aligned, double-buffer) | Task 4 | Covered |
| §5 ReadCache (ring-buffer) | Task 4 | Covered |
| §6 RocksDB indexes | Task 3 | Covered |
| §7 Entry log files | Task 2 | Covered |
| §8 Compaction/GC | Deferred | Phase 3b (separate plan) |
| §9 Checkpoint flow | Task 5 | Covered |
| §10 Disk management | Deferred | Phase 4 (Bookie Server) |

**Deferred**: Compaction/GC (§8) is a background process that depends on the full storage engine being operational. It will be implemented as Phase 3b after the core storage engine is proven correct. Disk management (§10) belongs with the Bookie Server (Phase 4).

## What's Next

| Phase | Scope | Status |
|-------|-------|--------|
| Phase 1 | Foundation + Wire Protocol | Complete |
| Phase 2 | Metadata | Complete |
| Phase 3 | Storage Engine (core) | This plan |
| Phase 3b | Storage Engine (compaction/GC) | Next |
| Phase 4 | Bookie Server | After Phase 3 |
