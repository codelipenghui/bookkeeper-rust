# Phase 4: Bookie Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a runnable bookie server that accepts TCP connections, decodes BookKeeper protocol requests, dispatches them to the storage engine, and sends responses — the first end-to-end functional bookie.

**Architecture:** New `bookkeeper-server` crate with a `BookieServer` that binds a TCP port, uses `BookieCodec` (Phase 1) for framing, decodes v2 requests via `decode_request`, processes them against `DbLedgerStorage` (Phase 3), and sends v2 responses via `encode_response`. Request processing is split into a `RequestProcessor` that maps opcodes to handler functions. Bookie state (Running/ReadOnly/ShuttingDown) is managed via an atomic state machine.

**Tech Stack:** `tokio` (async TCP server), `bookkeeper-protocol` (codec), `bookkeeper-storage` (DbLedgerStorage), `parking_lot` (RwLock for state)

**Spec reference:** `docs/superpowers/specs/04-bookie-server.md`

---

## File Structure

```
crates/bookkeeper-server/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── state.rs              # BookieState enum + atomic state machine
    ├── processor.rs          # RequestProcessor: dispatch + handle all operations
    ├── server.rs             # BookieServer: TCP listener + connection handler
    └── config.rs             # BookieConfig (minimal for now)
```

---

### Task 1: Create Crate + Bookie State Machine

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `crates/bookkeeper-server/Cargo.toml`
- Create: `crates/bookkeeper-server/src/lib.rs`
- Create: `crates/bookkeeper-server/src/state.rs`
- Test: `crates/bookkeeper-server/tests/state_test.rs`

- [ ] **Step 1: Add crate to workspace**

In root `Cargo.toml`, add to members:
```toml
"crates/bookkeeper-server",
```
Add to workspace.dependencies:
```toml
bookkeeper-server = { path = "crates/bookkeeper-server" }
```

- [ ] **Step 2: Create crate Cargo.toml**

```toml
[package]
name = "bookkeeper-server"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
bookkeeper-proto = { workspace = true }
bookkeeper-common = { workspace = true }
bookkeeper-protocol = { workspace = true }
bookkeeper-storage = { workspace = true }
bytes = { workspace = true }
tokio = { workspace = true }
tokio-util = { workspace = true }
parking_lot = { workspace = true }
thiserror = { workspace = true }
prost = { workspace = true }

[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 3: Write failing tests for state machine**

`crates/bookkeeper-server/tests/state_test.rs`:
```rust
use bookkeeper_server::state::{BookieState, BookieStateManager};

#[test]
fn initial_state_is_starting() {
    let mgr = BookieStateManager::new();
    assert_eq!(mgr.state(), BookieState::Starting);
}

#[test]
fn transition_to_running() {
    let mgr = BookieStateManager::new();
    assert!(mgr.transition_to(BookieState::Running));
    assert_eq!(mgr.state(), BookieState::Running);
}

#[test]
fn running_to_read_only() {
    let mgr = BookieStateManager::new();
    mgr.transition_to(BookieState::Running);
    assert!(mgr.transition_to(BookieState::ReadOnly));
    assert_eq!(mgr.state(), BookieState::ReadOnly);
}

#[test]
fn read_only_back_to_running() {
    let mgr = BookieStateManager::new();
    mgr.transition_to(BookieState::Running);
    mgr.transition_to(BookieState::ReadOnly);
    assert!(mgr.transition_to(BookieState::Running));
}

#[test]
fn any_to_shutting_down() {
    let mgr = BookieStateManager::new();
    mgr.transition_to(BookieState::Running);
    assert!(mgr.transition_to(BookieState::ShuttingDown));
    assert_eq!(mgr.state(), BookieState::ShuttingDown);
}

#[test]
fn cannot_transition_from_shutting_down() {
    let mgr = BookieStateManager::new();
    mgr.transition_to(BookieState::Running);
    mgr.transition_to(BookieState::ShuttingDown);
    assert!(!mgr.transition_to(BookieState::Running));
    assert_eq!(mgr.state(), BookieState::ShuttingDown);
}

#[test]
fn is_writable() {
    let mgr = BookieStateManager::new();
    assert!(!mgr.is_writable()); // Starting is not writable
    mgr.transition_to(BookieState::Running);
    assert!(mgr.is_writable());
    mgr.transition_to(BookieState::ReadOnly);
    assert!(!mgr.is_writable());
}

#[test]
fn is_readable() {
    let mgr = BookieStateManager::new();
    mgr.transition_to(BookieState::Running);
    assert!(mgr.is_readable());
    mgr.transition_to(BookieState::ReadOnly);
    assert!(mgr.is_readable()); // Read-only can still serve reads
    mgr.transition_to(BookieState::ShuttingDown);
    assert!(!mgr.is_readable());
}
```

- [ ] **Step 4: Implement state.rs**

```rust
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BookieState {
    Starting = 0,
    Running = 1,
    ReadOnly = 2,
    ShuttingDown = 3,
}

impl BookieState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Starting,
            1 => Self::Running,
            2 => Self::ReadOnly,
            3 => Self::ShuttingDown,
            _ => Self::ShuttingDown,
        }
    }
}

pub struct BookieStateManager {
    state: AtomicU8,
}

impl BookieStateManager {
    pub fn new() -> Self {
        Self { state: AtomicU8::new(BookieState::Starting as u8) }
    }

    pub fn state(&self) -> BookieState {
        BookieState::from_u8(self.state.load(Ordering::SeqCst))
    }

    pub fn transition_to(&self, new_state: BookieState) -> bool {
        let current = self.state();
        let allowed = match (current, new_state) {
            (BookieState::ShuttingDown, _) => false,
            (_, BookieState::ShuttingDown) => true,
            (BookieState::Starting, BookieState::Running) => true,
            (BookieState::Running, BookieState::ReadOnly) => true,
            (BookieState::ReadOnly, BookieState::Running) => true,
            _ => false,
        };
        if allowed {
            self.state.store(new_state as u8, Ordering::SeqCst);
        }
        allowed
    }

    pub fn is_writable(&self) -> bool {
        self.state() == BookieState::Running
    }

    pub fn is_readable(&self) -> bool {
        matches!(self.state(), BookieState::Running | BookieState::ReadOnly)
    }

    pub fn is_shutting_down(&self) -> bool {
        self.state() == BookieState::ShuttingDown
    }
}
```

- [ ] **Step 5: Create lib.rs**

```rust
pub mod state;
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p bookkeeper-server`
Expected: All 8 tests pass

- [ ] **Step 7: Commit**

```bash
git commit -m "feat: add BookieStateManager with atomic state machine (Starting/Running/ReadOnly/ShuttingDown)

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Request Processor

**Files:**
- Create: `crates/bookkeeper-server/src/processor.rs`
- Create: `crates/bookkeeper-server/src/config.rs`
- Test: `crates/bookkeeper-server/tests/processor_test.rs`

- [ ] **Step 1: Write failing tests**

`crates/bookkeeper-server/tests/processor_test.rs`:
```rust
use bookkeeper_server::processor::RequestProcessor;
use bookkeeper_server::state::BookieStateManager;
use bookkeeper_storage::db_ledger_storage::DbLedgerStorage;
use bookkeeper_protocol::v2::request::{V2Request, encode_request};
use bookkeeper_protocol::v2::response::V2Response;
use bookkeeper_protocol::constants::*;
use bytes::Bytes;
use std::sync::Arc;
use tempfile::TempDir;

fn setup() -> (RequestProcessor, TempDir) {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    let state = BookieStateManager::new();
    state.transition_to(bookkeeper_server::state::BookieState::Running);
    let processor = RequestProcessor::new(Arc::new(storage), Arc::new(state));
    (processor, dir)
}

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
fn add_entry_success() {
    let (processor, _dir) = setup();
    let entry_data = make_entry(1, 0, -1, b"hello");
    let req = V2Request::AddEntry {
        flags: 0,
        master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data),
    };
    let resp = processor.process_request(req);
    match resp {
        V2Response::Add { error_code, ledger_id, entry_id } => {
            assert_eq!(error_code, V2_EOK);
            assert_eq!(ledger_id, 1);
            assert_eq!(entry_id, 0);
        }
        _ => panic!("expected Add response"),
    }
}

#[test]
fn read_entry_success() {
    let (processor, _dir) = setup();
    // First write an entry
    let entry_data = make_entry(1, 0, -1, b"hello");
    let req = V2Request::AddEntry {
        flags: 0, master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data),
    };
    processor.process_request(req);
    // Now read it
    let req = V2Request::ReadEntry {
        flags: 0, ledger_id: 1, entry_id: 0, master_key: None,
    };
    let resp = processor.process_request(req);
    match resp {
        V2Response::Read { error_code, ledger_id, entry_id, data } => {
            assert_eq!(error_code, V2_EOK);
            assert_eq!(ledger_id, 1);
            assert_eq!(entry_id, 0);
            assert!(data.is_some());
        }
        _ => panic!("expected Read response"),
    }
}

#[test]
fn read_entry_not_found() {
    let (processor, _dir) = setup();
    let req = V2Request::ReadEntry {
        flags: 0, ledger_id: 999, entry_id: 0, master_key: None,
    };
    let resp = processor.process_request(req);
    match resp {
        V2Response::Read { error_code, .. } => {
            assert_ne!(error_code, V2_EOK);
        }
        _ => panic!("expected Read response"),
    }
}

#[test]
fn add_entry_rejected_when_read_only() {
    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path()).unwrap();
    let state = BookieStateManager::new();
    state.transition_to(bookkeeper_server::state::BookieState::Running);
    state.transition_to(bookkeeper_server::state::BookieState::ReadOnly);
    let processor = RequestProcessor::new(Arc::new(storage), Arc::new(state));

    let entry_data = make_entry(1, 0, -1, b"hello");
    let req = V2Request::AddEntry {
        flags: 0, master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data),
    };
    let resp = processor.process_request(req);
    match resp {
        V2Response::Add { error_code, .. } => {
            assert_eq!(error_code, bookkeeper_common::error::BKError::ReadOnly.to_v2_code());
        }
        _ => panic!("expected Add response"),
    }
}

#[test]
fn fencing_blocks_subsequent_writes() {
    let (processor, _dir) = setup();
    // Write an entry first
    let entry_data = make_entry(1, 0, -1, b"hello");
    processor.process_request(V2Request::AddEntry {
        flags: 0, master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data),
    });
    // Fence via read with FLAG_DO_FENCING
    processor.process_request(V2Request::ReadEntry {
        flags: FLAG_DO_FENCING, ledger_id: 1, entry_id: 0,
        master_key: Some([0u8; 20]),
    });
    // Normal write should be rejected with EFENCED
    let entry_data2 = make_entry(1, 1, 0, b"world");
    let resp = processor.process_request(V2Request::AddEntry {
        flags: 0, master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data2),
    });
    match resp {
        V2Response::Add { error_code, .. } => {
            assert_eq!(error_code, bookkeeper_common::error::BKError::Fenced.to_v2_code());
        }
        _ => panic!("expected Add response"),
    }
}

#[test]
fn recovery_add_bypasses_fence() {
    let (processor, _dir) = setup();
    // Write, fence, then recovery add
    let entry_data = make_entry(1, 0, -1, b"hello");
    processor.process_request(V2Request::AddEntry {
        flags: 0, master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data),
    });
    processor.process_request(V2Request::ReadEntry {
        flags: FLAG_DO_FENCING, ledger_id: 1, entry_id: 0,
        master_key: Some([0u8; 20]),
    });
    // Recovery add should succeed
    let entry_data2 = make_entry(1, 1, 0, b"recovery");
    let resp = processor.process_request(V2Request::AddEntry {
        flags: FLAG_RECOVERY_ADD, master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data2),
    });
    match resp {
        V2Response::Add { error_code, .. } => {
            assert_eq!(error_code, V2_EOK);
        }
        _ => panic!("expected Add response"),
    }
}

#[test]
fn get_bookie_info() {
    let (processor, _dir) = setup();
    let req = V2Request::GetBookieInfo { flags: 0, requested: 0x03 };
    let resp = processor.process_request(req);
    match resp {
        V2Response::GetBookieInfo { error_code, free_disk_space, total_disk_capacity, .. } => {
            assert_eq!(error_code, V2_EOK);
            assert!(total_disk_capacity > 0);
        }
        _ => panic!("expected GetBookieInfo response"),
    }
}
```

- [ ] **Step 2: Implement config.rs**

```rust
/// Minimal bookie configuration for Phase 4.
#[derive(Debug, Clone)]
pub struct BookieConfig {
    pub port: u16,
    pub ledger_dirs: Vec<String>,
    pub journal_dir: String,
    pub max_frame_size: usize,
}

impl Default for BookieConfig {
    fn default() -> Self {
        Self {
            port: 3181,
            ledger_dirs: vec!["/tmp/bk-data".to_string()],
            journal_dir: "/tmp/bk-txn".to_string(),
            max_frame_size: 5 * 1024 * 1024,
        }
    }
}
```

- [ ] **Step 3: Implement processor.rs**

```rust
use std::sync::Arc;
use bytes::Bytes;

use bookkeeper_common::error::BKError;
use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::v2::request::V2Request;
use bookkeeper_protocol::v2::response::V2Response;
use bookkeeper_storage::ledger_storage::LedgerStorage;
use crate::state::BookieStateManager;

pub struct RequestProcessor {
    storage: Arc<dyn LedgerStorage>,
    state: Arc<BookieStateManager>,
}

impl RequestProcessor {
    pub fn new(storage: Arc<dyn LedgerStorage>, state: Arc<BookieStateManager>) -> Self {
        Self { storage, state }
    }

    pub fn process_request(&self, req: V2Request) -> V2Response {
        match req {
            V2Request::AddEntry { flags, master_key, entry_data } => {
                self.handle_add_entry(flags, &master_key, entry_data)
            }
            V2Request::ReadEntry { flags, ledger_id, entry_id, master_key } => {
                self.handle_read_entry(flags, ledger_id, entry_id, master_key)
            }
            V2Request::WriteLac { ledger_id, lac, master_key, body } => {
                self.handle_write_lac(ledger_id, lac)
            }
            V2Request::ReadLac { ledger_id } => {
                self.handle_read_lac(ledger_id)
            }
            V2Request::GetBookieInfo { flags, requested } => {
                self.handle_get_bookie_info(requested)
            }
            V2Request::Auth { payload } => {
                V2Response::Auth { error_code: V2_EOK, payload }
            }
            V2Request::BatchReadEntry { flags, ledger_id, start_entry_id, request_id, max_count, max_size } => {
                self.handle_batch_read(flags, ledger_id, start_entry_id, request_id, max_count, max_size)
            }
        }
    }

    fn handle_add_entry(&self, flags: u16, master_key: &[u8; MASTER_KEY_LENGTH], entry_data: Bytes) -> V2Response {
        // Extract ledger_id and entry_id from entry data
        if entry_data.len() < 16 {
            return self.add_error_response(BKError::BadRequest, 0, 0);
        }
        let ledger_id = i64::from_be_bytes(entry_data[0..8].try_into().unwrap());
        let entry_id = i64::from_be_bytes(entry_data[8..16].try_into().unwrap());
        let is_recovery = flags & FLAG_RECOVERY_ADD != 0;

        // Check bookie is writable
        if !self.state.is_writable() {
            return self.add_error_response(BKError::ReadOnly, ledger_id, entry_id);
        }

        // Check fencing (skip for recovery adds)
        if !is_recovery {
            if let Ok(fenced) = self.storage.is_fenced(ledger_id) {
                if fenced {
                    return self.add_error_response(BKError::Fenced, ledger_id, entry_id);
                }
            }
        } else {
            // Recovery add: set fenced if not already
            let _ = self.storage.set_fenced(ledger_id);
        }

        // Set master key on first write
        let _ = self.storage.set_master_key(ledger_id, master_key);

        // Write to storage
        match self.storage.add_entry(&entry_data) {
            Ok(()) => V2Response::Add {
                error_code: V2_EOK,
                ledger_id,
                entry_id,
            },
            Err(e) => self.add_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id),
        }
    }

    fn handle_read_entry(&self, flags: u16, ledger_id: i64, entry_id: i64, master_key: Option<[u8; MASTER_KEY_LENGTH]>) -> V2Response {
        // Handle fencing
        if flags & FLAG_DO_FENCING != 0 {
            if let Some(_key) = master_key {
                let _ = self.storage.set_fenced(ledger_id);
            }
        }

        // Special case: LAST_ADD_CONFIRMED
        if entry_id == LAST_ADD_CONFIRMED_ENTRY_ID {
            match self.storage.get_last_add_confirmed(ledger_id) {
                Ok(lac) => {
                    // Build a minimal response with LAC info
                    return V2Response::Read {
                        error_code: V2_EOK,
                        ledger_id,
                        entry_id: lac,
                        data: None,
                    };
                }
                Err(_) => {
                    return V2Response::Read {
                        error_code: BKError::NoLedger.to_v2_code(),
                        ledger_id,
                        entry_id,
                        data: None,
                    };
                }
            }
        }

        // Normal read
        match self.storage.get_entry(ledger_id, entry_id) {
            Ok(data) => V2Response::Read {
                error_code: V2_EOK,
                ledger_id,
                entry_id,
                data: Some(data),
            },
            Err(e) => V2Response::Read {
                error_code: self.storage_error_to_bk(&e).to_v2_code(),
                ledger_id,
                entry_id,
                data: None,
            },
        }
    }

    fn handle_write_lac(&self, ledger_id: i64, lac: i64) -> V2Response {
        match self.storage.set_explicit_lac(ledger_id, lac) {
            Ok(()) => V2Response::WriteLac {
                error_code: V2_EOK,
                ledger_id,
                entry_id: 0,
            },
            Err(e) => V2Response::WriteLac {
                error_code: self.storage_error_to_bk(&e).to_v2_code(),
                ledger_id,
                entry_id: 0,
            },
        }
    }

    fn handle_read_lac(&self, ledger_id: i64) -> V2Response {
        match self.storage.get_last_add_confirmed(ledger_id) {
            Ok(lac) => V2Response::ReadLac {
                error_code: V2_EOK,
                ledger_id,
                entry_id: lac,
                lac_body: None,
                last_entry_body: None,
            },
            Err(e) => V2Response::ReadLac {
                error_code: self.storage_error_to_bk(&e).to_v2_code(),
                ledger_id,
                entry_id: 0,
                lac_body: None,
                last_entry_body: None,
            },
        }
    }

    fn handle_get_bookie_info(&self, _requested: i64) -> V2Response {
        // Get disk space info from the system
        let (free, total) = get_disk_space();
        V2Response::GetBookieInfo {
            error_code: V2_EOK,
            ledger_id: 0,
            entry_id: 0,
            free_disk_space: free,
            total_disk_capacity: total,
        }
    }

    fn handle_batch_read(&self, flags: u16, ledger_id: i64, start_entry_id: i64, request_id: i64, max_count: i32, max_size: i64) -> V2Response {
        // Handle fencing
        if flags & FLAG_DO_FENCING != 0 {
            let _ = self.storage.set_fenced(ledger_id);
        }

        let mut entries = Vec::new();
        let mut total_size: i64 = 0;
        for i in 0..max_count as i64 {
            let eid = start_entry_id + i;
            match self.storage.get_entry(ledger_id, eid) {
                Ok(data) => {
                    total_size += data.len() as i64;
                    if total_size > max_size && !entries.is_empty() {
                        break;
                    }
                    entries.push(data);
                }
                Err(_) => break,
            }
        }

        V2Response::BatchRead {
            error_code: if entries.is_empty() {
                BKError::NoEntry.to_v2_code()
            } else {
                V2_EOK
            },
            ledger_id,
            entry_id: start_entry_id,
            request_id,
            entries,
        }
    }

    fn add_error_response(&self, err: BKError, ledger_id: i64, entry_id: i64) -> V2Response {
        V2Response::Add {
            error_code: err.to_v2_code(),
            ledger_id,
            entry_id,
        }
    }

    fn storage_error_to_bk(&self, err: &bookkeeper_storage::StorageError) -> BKError {
        match err {
            bookkeeper_storage::StorageError::EntryNotFound { .. } => BKError::NoEntry,
            bookkeeper_storage::StorageError::LedgerNotFound(_) => BKError::NoLedger,
            bookkeeper_storage::StorageError::Fenced(_) => BKError::Fenced,
            bookkeeper_storage::StorageError::StorageFull => BKError::ReadOnly,
            _ => BKError::Io,
        }
    }
}

fn get_disk_space() -> (i64, i64) {
    // Platform-specific disk space query
    // For now return reasonable defaults; will be implemented properly with statvfs
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // Simple fallback: report root filesystem
        match nix_disk_space("/") {
            Some((free, total)) => (free, total),
            None => (0, 0),
        }
    }
    #[cfg(not(unix))]
    {
        (0, 0)
    }
}

#[cfg(unix)]
fn nix_disk_space(path: &str) -> Option<(i64, i64)> {
    use std::ffi::CString;
    let c_path = CString::new(path).ok()?;
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
            let total = (stat.f_blocks as i64) * (stat.f_frsize as i64);
            let free = (stat.f_bavail as i64) * (stat.f_frsize as i64);
            Some((free, total))
        } else {
            None
        }
    }
}
```

- [ ] **Step 4: Add libc dependency to Cargo.toml**

Add to crate dependencies:
```toml
[target.'cfg(unix)'.dependencies]
libc = "0.2"
```

- [ ] **Step 5: Update lib.rs**

```rust
pub mod state;
pub mod config;
pub mod processor;
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p bookkeeper-server`
Expected: All 7 processor tests pass + 8 state tests

- [ ] **Step 7: Commit**

```bash
git commit -m "feat: add RequestProcessor handling ADD_ENTRY, READ_ENTRY, fencing, LAC, batch reads

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: TCP Server

**Files:**
- Create: `crates/bookkeeper-server/src/server.rs`
- Test: `crates/bookkeeper-server/tests/server_test.rs`

- [ ] **Step 1: Write failing integration test**

`crates/bookkeeper-server/tests/server_test.rs`:
```rust
use bookkeeper_server::server::BookieServer;
use bookkeeper_server::config::BookieConfig;
use bookkeeper_protocol::framing::BookieCodec;
use bookkeeper_protocol::v2::request::{V2Request, encode_request};
use bookkeeper_protocol::v2::response::{V2Response, decode_response};
use bookkeeper_protocol::constants::*;
use bytes::Bytes;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, Decoder, Encoder};
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

#[tokio::test]
async fn server_add_and_read_entry() {
    let dir = TempDir::new().unwrap();
    let config = BookieConfig {
        port: 0, // OS-assigned port
        ledger_dirs: vec![dir.path().to_string_lossy().to_string()],
        journal_dir: dir.path().to_string_lossy().to_string(),
        max_frame_size: 5 * 1024 * 1024,
    };
    let server = BookieServer::start(config).await.unwrap();
    let addr = server.local_addr();

    // Connect client
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut codec = BookieCodec::new(5 * 1024 * 1024);

    // Send ADD_ENTRY
    let entry_data = make_entry(1, 0, -1, b"hello bookie");
    let req = V2Request::AddEntry {
        flags: 0, master_key: [0u8; 20],
        entry_data: Bytes::from(entry_data),
    };
    let req_bytes = encode_request(&req);
    let mut buf = bytes::BytesMut::new();
    codec.encode(req_bytes.freeze(), &mut buf).unwrap();

    use tokio::io::AsyncWriteExt;
    let (mut reader, mut writer) = stream.into_split();
    writer.write_all(&buf).await.unwrap();

    // Read response
    use tokio::io::AsyncReadExt;
    let mut resp_buf = bytes::BytesMut::with_capacity(1024);
    let mut tmp = vec![0u8; 1024];
    let n = reader.read(&mut tmp).await.unwrap();
    resp_buf.extend_from_slice(&tmp[..n]);
    let frame = codec.decode(&mut resp_buf).unwrap().unwrap();
    let resp = decode_response(frame.freeze()).unwrap();

    match resp {
        V2Response::Add { error_code, ledger_id, entry_id } => {
            assert_eq!(error_code, V2_EOK);
            assert_eq!(ledger_id, 1);
            assert_eq!(entry_id, 0);
        }
        _ => panic!("expected Add response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn server_binds_and_shuts_down() {
    let dir = TempDir::new().unwrap();
    let config = BookieConfig {
        port: 0,
        ledger_dirs: vec![dir.path().to_string_lossy().to_string()],
        journal_dir: dir.path().to_string_lossy().to_string(),
        max_frame_size: 5 * 1024 * 1024,
    };
    let server = BookieServer::start(config).await.unwrap();
    let addr = server.local_addr();
    assert_ne!(addr.port(), 0);
    server.shutdown().await;
}
```

- [ ] **Step 2: Implement server.rs**

```rust
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::watch;
use bytes::Bytes;
use tokio_util::codec::{Decoder, Encoder};

use bookkeeper_protocol::framing::BookieCodec;
use bookkeeper_protocol::v2::request::decode_request;
use bookkeeper_protocol::v2::response::encode_response;
use bookkeeper_storage::db_ledger_storage::DbLedgerStorage;
use bookkeeper_storage::ledger_storage::LedgerStorage;
use crate::config::BookieConfig;
use crate::processor::RequestProcessor;
use crate::state::{BookieState, BookieStateManager};

pub struct BookieServer {
    local_addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    state: Arc<BookieStateManager>,
    storage: Arc<DbLedgerStorage>,
}

impl BookieServer {
    pub async fn start(config: BookieConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let bind_addr = format!("0.0.0.0:{}", config.port);
        let listener = TcpListener::bind(&bind_addr).await?;
        let local_addr = listener.local_addr()?;

        let ledger_dir = config.ledger_dirs.first()
            .map(|s| std::path::PathBuf::from(s))
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp/bk-data"));

        let storage = Arc::new(DbLedgerStorage::open(&ledger_dir)?);
        let state = Arc::new(BookieStateManager::new());
        state.transition_to(BookieState::Running);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let processor = Arc::new(RequestProcessor::new(
            storage.clone() as Arc<dyn LedgerStorage>,
            state.clone(),
        ));
        let max_frame_size = config.max_frame_size;

        // Spawn accept loop
        let mut accept_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, _addr)) => {
                                let processor = processor.clone();
                                let conn_shutdown_rx = shutdown_rx.clone();
                                tokio::spawn(handle_connection(stream, processor, max_frame_size, conn_shutdown_rx));
                            }
                            Err(_) => break,
                        }
                    }
                    _ = accept_shutdown_rx.changed() => {
                        break;
                    }
                }
            }
        });

        Ok(Self { local_addr, shutdown_tx, state, storage })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn shutdown(self) {
        self.state.transition_to(BookieState::ShuttingDown);
        let _ = self.shutdown_tx.send(true);
        let _ = self.storage.flush();
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    processor: Arc<RequestProcessor>,
    max_frame_size: usize,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (mut reader, mut writer) = stream.into_split();
    let mut read_buf = bytes::BytesMut::with_capacity(64 * 1024);
    let mut codec = BookieCodec::new(max_frame_size);

    loop {
        let mut tmp = vec![0u8; 32 * 1024];
        tokio::select! {
            result = reader.read(&mut tmp) => {
                match result {
                    Ok(0) => break, // connection closed
                    Ok(n) => {
                        read_buf.extend_from_slice(&tmp[..n]);
                        // Process all complete frames
                        while let Some(frame) = codec.decode(&mut read_buf).ok().flatten() {
                            match decode_request(frame.freeze()) {
                                Ok(request) => {
                                    let response = processor.process_request(request);
                                    let resp_bytes = encode_response(&response);
                                    let mut send_buf = bytes::BytesMut::new();
                                    if codec.encode(resp_bytes.freeze(), &mut send_buf).is_ok() {
                                        if writer.write_all(&send_buf).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            _ = shutdown_rx.changed() => {
                break;
            }
        }
    }
}
```

- [ ] **Step 3: Update lib.rs**

```rust
pub mod state;
pub mod config;
pub mod processor;
pub mod server;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p bookkeeper-server`
Expected: All tests pass (8 state + 7 processor + 2 server integration)

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: add BookieServer with TCP listener, connection handler, and shutdown

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Spec Coverage

| Spec Section | Task | Notes |
|---|---|---|
| §1.1 Startup | Task 3 (server.rs start) | Simplified: no ZK registration, no journal replay yet |
| §1.2 Shutdown | Task 3 (server.rs shutdown) | Drain + flush + state transition |
| §2.1 TCP Pipeline | Task 3 | BookieCodec → decode → process → encode → send |
| §2.3 Request Dispatch | Task 2 | process_request matches on V2Request variant |
| §3 Write Path | Task 2 | ADD_ENTRY → fencing check → master key → storage |
| §3.2 Recovery Write | Task 2 | FLAG_RECOVERY_ADD bypasses fence |
| §4.1 Normal Read | Task 2 | READ_ENTRY → fencing → LAC special case → storage |
| §4.3 Batch Read | Task 2 | Sequential read with maxCount/maxSize |
| §5 Fencing | Task 2 | FLAG_DO_FENCING → set_fenced, blocks normal writes |
| §6 Explicit LAC | Task 2 | WRITE_LAC, READ_LAC |
| §7 FORCE_LEDGER | Deferred | Needs journal integration |
| §8 GET_BOOKIE_INFO | Task 2 | Returns disk space via statvfs |
| §9 GET_LIST_OF_ENTRIES | Deferred | Needs entry enumeration in storage |
| §10 State Machine | Task 1 | Starting/Running/ReadOnly/ShuttingDown |
| §10.2 Disk Monitoring | Deferred | Phase 4b |
| §11 Rate Limiting | Deferred | Phase 4b |
| §4.2 Long-Poll Read | Deferred | Requires watcher infrastructure |

## What's Next

| Phase | Scope | Status |
|-------|-------|--------|
| Phase 1 | Foundation + Wire Protocol | Complete |
| Phase 2 | Metadata | Complete |
| Phase 3 | Storage Engine | Complete |
| Phase 4 | Bookie Server (core) | This plan |
| Phase 5 | Bookie Registration (ZK) | Next |
