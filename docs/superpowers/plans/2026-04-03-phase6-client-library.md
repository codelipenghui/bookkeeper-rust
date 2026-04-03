# Phase 6: Client Library Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a BookKeeper client library that can create ledgers, write entries (with digest + quorum acks), read entries (with digest verification), and close/delete ledgers — enabling end-to-end integration tests with the Rust bookie server.

**Architecture:** New `bookkeeper-client` crate with: `BookieConnection` for TCP communication with a single bookie using `BookieCodec`; `DistributionSchedule` for round-robin write set computation; `WriteHandle` that builds entries with digest, sends to write quorum, tracks acks, advances LAC; `ReadHandle` that reads from bookies with fallback and digest verification; `BookKeeperClient` that orchestrates ledger lifecycle via metadata service. Minimal scope: random ensemble placement (no rack-aware), no speculative reads, no ensemble change, no recovery.

**Tech Stack:** `tokio` (async networking), `bookkeeper-protocol` (codec + digest + entry format), `bookkeeper-metadata` (ledger metadata + ZK traits), `bytes`

**Spec reference:** `docs/superpowers/specs/06-client.md`

---

## File Structure

```
crates/bookkeeper-client/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── bookie_connection.rs      # TCP connection to a single bookie
    ├── distribution.rs           # RoundRobinDistributionSchedule + AckSet
    ├── write_handle.rs           # WriteHandle: append entries, track acks, advance LAC
    ├── read_handle.rs            # ReadHandle: read entries, verify digest
    └── client.rs                 # BookKeeperClient: create/open/close/delete ledgers
```

---

### Task 1: Create Crate + Distribution Schedule + AckSet

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `crates/bookkeeper-client/Cargo.toml`
- Create: `crates/bookkeeper-client/src/lib.rs`
- Create: `crates/bookkeeper-client/src/distribution.rs`
- Test: `crates/bookkeeper-client/tests/distribution_test.rs`

- [ ] **Step 1: Add crate to workspace**

In root `Cargo.toml` members, add `"crates/bookkeeper-client"`. Add to workspace.dependencies:
```toml
bookkeeper-client = { path = "crates/bookkeeper-client" }
```

- [ ] **Step 2: Create crate Cargo.toml**

```toml
[package]
name = "bookkeeper-client"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
bookkeeper-proto = { workspace = true }
bookkeeper-common = { workspace = true }
bookkeeper-protocol = { workspace = true }
bookkeeper-metadata = { workspace = true }
bytes = { workspace = true }
tokio = { workspace = true }
tokio-util = { workspace = true }
thiserror = { workspace = true }
prost = { workspace = true }
rand = "0.8"

[dev-dependencies]
tempfile = "3"
bookkeeper-storage = { workspace = true }
bookkeeper-server = { workspace = true }
```

- [ ] **Step 3: Write failing tests**

`crates/bookkeeper-client/tests/distribution_test.rs`:
```rust
use bookkeeper_client::distribution::{RoundRobinDistributionSchedule, AckSet};

#[test]
fn write_set_for_entry_0() {
    let sched = RoundRobinDistributionSchedule::new(5, 3, 2);
    let ws = sched.write_set(0);
    assert_eq!(ws, vec![0, 1, 2]);
}

#[test]
fn write_set_wraps_around() {
    let sched = RoundRobinDistributionSchedule::new(5, 3, 2);
    let ws = sched.write_set(4);
    assert_eq!(ws, vec![4, 0, 1]);
}

#[test]
fn write_set_entry_7() {
    let sched = RoundRobinDistributionSchedule::new(5, 3, 2);
    let ws = sched.write_set(7);
    assert_eq!(ws, vec![2, 3, 4]);
}

#[test]
fn ack_set_quorum_reached() {
    let mut ack = AckSet::new(3, 2);
    assert!(!ack.complete_bookie(0));
    assert!(ack.complete_bookie(1)); // 2 acks = quorum
}

#[test]
fn ack_set_failure_detected() {
    let mut ack = AckSet::new(3, 2);
    assert!(!ack.fail_bookie(0)); // 1 fail, still possible
    assert!(ack.fail_bookie(1));  // 2 fails > (3-2)=1, quorum impossible
}

#[test]
fn ack_set_mixed() {
    let mut ack = AckSet::new(3, 2);
    ack.fail_bookie(0);
    assert!(!ack.complete_bookie(1)); // 1 ack, need 2
    assert!(ack.complete_bookie(2));  // 2 acks = quorum
}

#[test]
fn write_set_ensemble_equals_quorum() {
    let sched = RoundRobinDistributionSchedule::new(3, 3, 2);
    assert_eq!(sched.write_set(0), vec![0, 1, 2]);
    assert_eq!(sched.write_set(1), vec![1, 2, 0]);
}
```

- [ ] **Step 4: Implement distribution.rs**

```rust
/// Round-robin distribution schedule.
/// For entry `e`, write set = [(e+0)%E, (e+1)%E, ..., (e+W-1)%E]
/// where E=ensembleSize and W=writeQuorumSize.
#[derive(Debug, Clone)]
pub struct RoundRobinDistributionSchedule {
    ensemble_size: usize,
    write_quorum_size: usize,
    ack_quorum_size: usize,
}

impl RoundRobinDistributionSchedule {
    pub fn new(ensemble_size: usize, write_quorum_size: usize, ack_quorum_size: usize) -> Self {
        Self { ensemble_size, write_quorum_size, ack_quorum_size }
    }

    pub fn ensemble_size(&self) -> usize { self.ensemble_size }
    pub fn write_quorum_size(&self) -> usize { self.write_quorum_size }
    pub fn ack_quorum_size(&self) -> usize { self.ack_quorum_size }

    /// Compute write set indices for a given entry ID.
    pub fn write_set(&self, entry_id: i64) -> Vec<usize> {
        let e = entry_id as usize;
        (0..self.write_quorum_size)
            .map(|i| (e + i) % self.ensemble_size)
            .collect()
    }

    /// Create a new AckSet for tracking write acknowledgments.
    pub fn new_ack_set(&self) -> AckSet {
        AckSet::new(self.write_quorum_size, self.ack_quorum_size)
    }
}

/// Tracks which bookies in a write set have acknowledged.
#[derive(Debug)]
pub struct AckSet {
    write_quorum_size: usize,
    ack_quorum_size: usize,
    ack_count: usize,
    fail_count: usize,
}

impl AckSet {
    pub fn new(write_quorum_size: usize, ack_quorum_size: usize) -> Self {
        Self { write_quorum_size, ack_quorum_size, ack_count: 0, fail_count: 0 }
    }

    /// Record a successful ack. Returns true if ack quorum is now reached.
    pub fn complete_bookie(&mut self, _bookie_index: usize) -> bool {
        self.ack_count += 1;
        self.ack_count >= self.ack_quorum_size
    }

    /// Record a failure. Returns true if ack quorum is now impossible.
    pub fn fail_bookie(&mut self, _bookie_index: usize) -> bool {
        self.fail_count += 1;
        self.fail_count > (self.write_quorum_size - self.ack_quorum_size)
    }

    pub fn is_quorum_reached(&self) -> bool {
        self.ack_count >= self.ack_quorum_size
    }
}
```

- [ ] **Step 5: Create lib.rs**

```rust
pub mod distribution;
```

- [ ] **Step 6: Run tests, commit**

Run: `cargo test -p bookkeeper-client`

```bash
git commit -m "feat: add RoundRobinDistributionSchedule and AckSet for write quorum tracking

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Bookie Connection (TCP Client)

**Files:**
- Create: `crates/bookkeeper-client/src/bookie_connection.rs`
- Test: `crates/bookkeeper-client/tests/bookie_connection_test.rs`

- [ ] **Step 1: Write failing integration test**

`crates/bookkeeper-client/tests/bookie_connection_test.rs`:
```rust
use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_protocol::v2::request::V2Request;
use bookkeeper_protocol::v2::response::V2Response;
use bookkeeper_protocol::constants::V2_EOK;
use bookkeeper_server::server::BookieServer;
use bookkeeper_server::config::BookieConfig;
use bytes::Bytes;
use tempfile::TempDir;

fn make_entry(ledger_id: i64, entry_id: i64, lac: i64, payload: &[u8]) -> Bytes {
    let mut entry = Vec::new();
    entry.extend_from_slice(&ledger_id.to_be_bytes());
    entry.extend_from_slice(&entry_id.to_be_bytes());
    entry.extend_from_slice(&lac.to_be_bytes());
    entry.extend_from_slice(&(payload.len() as i64).to_be_bytes());
    entry.extend_from_slice(payload);
    Bytes::from(entry)
}

#[tokio::test]
async fn connect_and_add_entry() {
    let dir = TempDir::new().unwrap();
    let config = BookieConfig {
        port: 0,
        ledger_dirs: vec![dir.path().to_string_lossy().to_string()],
        journal_dir: dir.path().to_string_lossy().to_string(),
        max_frame_size: 5 * 1024 * 1024,
        zk_connect: None,
        zk_ledger_root: "/ledgers".to_string(),
        bookie_id: None,
    };
    let server = BookieServer::start(config).await.unwrap();
    let addr = server.local_addr();

    let mut conn = BookieConnection::connect(addr).await.unwrap();

    let entry_data = make_entry(1, 0, -1, b"hello from client");
    let resp = conn.send_add_entry(0, [0u8; 20], entry_data).await.unwrap();

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
async fn connect_and_read_entry() {
    let dir = TempDir::new().unwrap();
    let config = BookieConfig {
        port: 0,
        ledger_dirs: vec![dir.path().to_string_lossy().to_string()],
        journal_dir: dir.path().to_string_lossy().to_string(),
        max_frame_size: 5 * 1024 * 1024,
        zk_connect: None,
        zk_ledger_root: "/ledgers".to_string(),
        bookie_id: None,
    };
    let server = BookieServer::start(config).await.unwrap();
    let addr = server.local_addr();

    let mut conn = BookieConnection::connect(addr).await.unwrap();

    // Write
    let entry_data = make_entry(1, 0, -1, b"test data");
    conn.send_add_entry(0, [0u8; 20], entry_data).await.unwrap();

    // Read
    let resp = conn.send_read_entry(0, 1, 0, None).await.unwrap();
    match resp {
        V2Response::Read { error_code, data, .. } => {
            assert_eq!(error_code, V2_EOK);
            assert!(data.is_some());
        }
        _ => panic!("expected Read response"),
    }

    server.shutdown().await;
}
```

- [ ] **Step 2: Implement bookie_connection.rs**

```rust
use std::net::SocketAddr;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder};

use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::framing::BookieCodec;
use bookkeeper_protocol::v2::request::{V2Request, encode_request};
use bookkeeper_protocol::v2::response::{V2Response, decode_response};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("connection closed")]
    ConnectionClosed,
}

/// A TCP connection to a single bookie.
pub struct BookieConnection {
    reader: tokio::net::tcp::OwnedReadHalf,
    writer: tokio::net::tcp::OwnedWriteHalf,
    codec: BookieCodec,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl BookieConnection {
    pub async fn connect(addr: SocketAddr) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader,
            writer,
            codec: BookieCodec::new(5 * 1024 * 1024),
            read_buf: BytesMut::with_capacity(64 * 1024),
            write_buf: BytesMut::with_capacity(4096),
        })
    }

    async fn send_request(&mut self, req: V2Request) -> Result<V2Response, ClientError> {
        // Encode and send
        let req_bytes = encode_request(&req);
        self.write_buf.clear();
        self.codec.encode(req_bytes.freeze(), &mut self.write_buf)
            .map_err(|e| ClientError::Protocol(format!("encode: {e}")))?;
        self.writer.write_all(&self.write_buf).await?;

        // Read response
        loop {
            match self.codec.decode(&mut self.read_buf) {
                Ok(Some(frame)) => {
                    return decode_response(frame.freeze())
                        .map_err(|e| ClientError::Protocol(format!("decode: {e}")));
                }
                Ok(None) => {
                    let n = self.reader.read_buf(&mut self.read_buf).await?;
                    if n == 0 {
                        return Err(ClientError::ConnectionClosed);
                    }
                }
                Err(e) => return Err(ClientError::Protocol(format!("frame: {e}"))),
            }
        }
    }

    pub async fn send_add_entry(
        &mut self,
        flags: u16,
        master_key: [u8; MASTER_KEY_LENGTH],
        entry_data: Bytes,
    ) -> Result<V2Response, ClientError> {
        let req = V2Request::AddEntry { flags, master_key, entry_data };
        self.send_request(req).await
    }

    pub async fn send_read_entry(
        &mut self,
        flags: u16,
        ledger_id: i64,
        entry_id: i64,
        master_key: Option<[u8; MASTER_KEY_LENGTH]>,
    ) -> Result<V2Response, ClientError> {
        let req = V2Request::ReadEntry { flags, ledger_id, entry_id, master_key };
        self.send_request(req).await
    }
}
```

- [ ] **Step 3: Update lib.rs**

```rust
pub mod distribution;
pub mod bookie_connection;
```

- [ ] **Step 4: Run tests, commit**

```bash
git commit -m "feat: add BookieConnection for TCP client communication with bookies

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: WriteHandle + ReadHandle

**Files:**
- Create: `crates/bookkeeper-client/src/write_handle.rs`
- Create: `crates/bookkeeper-client/src/read_handle.rs`
- Test: `crates/bookkeeper-client/tests/handle_test.rs`

- [ ] **Step 1: Write failing integration test**

`crates/bookkeeper-client/tests/handle_test.rs`:
```rust
use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_client::write_handle::WriteHandle;
use bookkeeper_client::read_handle::ReadHandle;
use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_server::server::BookieServer;
use bookkeeper_server::config::BookieConfig;
use tempfile::TempDir;

fn test_config(dir: &tempfile::TempDir) -> BookieConfig {
    BookieConfig {
        port: 0,
        ledger_dirs: vec![dir.path().to_string_lossy().to_string()],
        journal_dir: dir.path().to_string_lossy().to_string(),
        max_frame_size: 5 * 1024 * 1024,
        zk_connect: None,
        zk_ledger_root: "/ledgers".to_string(),
        bookie_id: None,
    }
}

#[tokio::test]
async fn write_and_read_single_entry() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    // Write
    let mut conns = vec![BookieConnection::connect(addr).await.unwrap()];
    let mut write_handle = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    let entry_id = write_handle.append(&mut conns, b"hello world").await.unwrap();
    assert_eq!(entry_id, 0);

    // Read
    let mut read_conns = vec![BookieConnection::connect(addr).await.unwrap()];
    let read_handle = ReadHandle::new(1, ensemble, schedule, dm, master_key);
    let payload = read_handle.read_entry(&mut read_conns, 0).await.unwrap();
    assert_eq!(payload, b"hello world");

    server.shutdown().await;
}

#[tokio::test]
async fn write_multiple_entries() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let mut conns = vec![BookieConnection::connect(addr).await.unwrap()];
    let mut write_handle = WriteHandle::new(1, ensemble, schedule, dm, master_key);

    for i in 0..10 {
        let data = format!("entry-{}", i);
        let eid = write_handle.append(&mut conns, data.as_bytes()).await.unwrap();
        assert_eq!(eid, i);
    }
    assert_eq!(write_handle.last_add_confirmed(), 9);

    server.shutdown().await;
}

#[tokio::test]
async fn read_verifies_digest() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = [0u8; 20];

    let mut conns = vec![BookieConnection::connect(addr).await.unwrap()];
    let mut wh = WriteHandle::new(1, ensemble.clone(), schedule.clone(), dm.clone(), master_key);
    wh.append(&mut conns, b"verified data").await.unwrap();

    let mut read_conns = vec![BookieConnection::connect(addr).await.unwrap()];
    let rh = ReadHandle::new(1, ensemble, schedule, dm, master_key);
    let payload = rh.read_entry(&mut read_conns, 0).await.unwrap();
    assert_eq!(payload, b"verified data");

    server.shutdown().await;
}
```

- [ ] **Step 2: Implement write_handle.rs**

```rust
use bytes::{BufMut, Bytes, BytesMut};

use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::DigestManager;
use bookkeeper_protocol::v2::response::V2Response;
use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;

pub struct WriteHandle {
    ledger_id: i64,
    ensemble: Vec<String>,
    schedule: RoundRobinDistributionSchedule,
    digest_manager: DigestManager,
    master_key: [u8; MASTER_KEY_LENGTH],
    last_add_pushed: i64,
    last_add_confirmed: i64,
    current_length: i64,
}

impl WriteHandle {
    pub fn new(
        ledger_id: i64,
        ensemble: Vec<String>,
        schedule: RoundRobinDistributionSchedule,
        digest_manager: DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Self {
        Self {
            ledger_id,
            ensemble,
            schedule,
            digest_manager,
            master_key,
            last_add_pushed: -1,
            last_add_confirmed: -1,
            current_length: 0,
        }
    }

    pub fn last_add_confirmed(&self) -> i64 {
        self.last_add_confirmed
    }

    pub fn ledger_id(&self) -> i64 {
        self.ledger_id
    }

    /// Append an entry. Builds the entry with digest, sends to write quorum,
    /// waits for ack quorum, advances LAC. Returns the entry ID.
    pub async fn append(
        &mut self,
        conns: &mut [BookieConnection],
        payload: &[u8],
    ) -> Result<i64, ClientError> {
        let entry_id = self.last_add_pushed + 1;
        self.last_add_pushed = entry_id;
        self.current_length += payload.len() as i64;

        // Build entry: [ledgerId | entryId | lac | length | digest | payload]
        let entry_bytes = self.build_entry(entry_id, payload);

        // Determine write set
        let write_set = self.schedule.write_set(entry_id);
        let mut ack_set = self.schedule.new_ack_set();

        // Send to each bookie in write set
        for &bookie_idx in &write_set {
            if bookie_idx >= conns.len() {
                return Err(ClientError::Protocol(format!(
                    "bookie index {bookie_idx} out of range (have {} connections)",
                    conns.len()
                )));
            }
            let resp = conns[bookie_idx]
                .send_add_entry(0, self.master_key, entry_bytes.clone())
                .await?;

            match resp {
                V2Response::Add { error_code, .. } => {
                    if error_code == V2_EOK {
                        if ack_set.complete_bookie(bookie_idx) {
                            // Quorum reached
                            self.last_add_confirmed = entry_id;
                            return Ok(entry_id);
                        }
                    } else {
                        if ack_set.fail_bookie(bookie_idx) {
                            return Err(ClientError::Protocol(format!(
                                "write quorum impossible: error_code={error_code}"
                            )));
                        }
                    }
                }
                _ => {
                    return Err(ClientError::Protocol("unexpected response type".into()));
                }
            }
        }

        // If we get here, we sent to all bookies but didn't reach quorum
        if ack_set.is_quorum_reached() {
            self.last_add_confirmed = entry_id;
            Ok(entry_id)
        } else {
            Err(ClientError::Protocol("failed to reach ack quorum".into()))
        }
    }

    fn build_entry(&self, entry_id: i64, payload: &[u8]) -> Bytes {
        let digest_len = self.digest_manager.digest_type().digest_length();
        let total = 32 + digest_len + payload.len();
        let mut buf = BytesMut::with_capacity(total);

        // Metadata (32 bytes)
        buf.put_i64(self.ledger_id);
        buf.put_i64(entry_id);
        buf.put_i64(self.last_add_confirmed);
        buf.put_i64(self.current_length);

        // Digest
        let metadata: [u8; 32] = buf[..32].try_into().unwrap();
        let digest = self.digest_manager.compute(&metadata, payload);
        buf.put_slice(digest.as_slice());

        // Payload
        buf.put_slice(payload);
        buf.freeze()
    }
}
```

- [ ] **Step 3: Implement read_handle.rs**

```rust
use bytes::Bytes;

use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::DigestManager;
use bookkeeper_protocol::entry::{parse_entry, EntryError};
use bookkeeper_protocol::v2::response::V2Response;
use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;

pub struct ReadHandle {
    ledger_id: i64,
    ensemble: Vec<String>,
    schedule: RoundRobinDistributionSchedule,
    digest_manager: DigestManager,
    master_key: [u8; MASTER_KEY_LENGTH],
}

impl ReadHandle {
    pub fn new(
        ledger_id: i64,
        ensemble: Vec<String>,
        schedule: RoundRobinDistributionSchedule,
        digest_manager: DigestManager,
        master_key: [u8; MASTER_KEY_LENGTH],
    ) -> Self {
        Self { ledger_id, ensemble, schedule, digest_manager, master_key }
    }

    pub fn ledger_id(&self) -> i64 {
        self.ledger_id
    }

    /// Read a single entry. Tries bookies in write set order, returns first success.
    pub async fn read_entry(
        &self,
        conns: &mut [BookieConnection],
        entry_id: i64,
    ) -> Result<Vec<u8>, ClientError> {
        let write_set = self.schedule.write_set(entry_id);

        for &bookie_idx in &write_set {
            if bookie_idx >= conns.len() {
                continue;
            }
            match conns[bookie_idx].send_read_entry(0, self.ledger_id, entry_id, None).await {
                Ok(V2Response::Read { error_code, data, .. }) => {
                    if error_code == V2_EOK {
                        if let Some(entry_data) = data {
                            // Verify digest
                            match parse_entry(&self.digest_manager, entry_data) {
                                Ok(entry) => {
                                    if entry.ledger_id != self.ledger_id || entry.entry_id != entry_id {
                                        continue; // wrong entry, try next bookie
                                    }
                                    return Ok(entry.payload.to_vec());
                                }
                                Err(EntryError::DigestMismatch) => {
                                    continue; // corrupted, try next bookie
                                }
                                Err(e) => {
                                    return Err(ClientError::Protocol(format!("entry parse: {e}")));
                                }
                            }
                        }
                    }
                    // Non-EOK or no data — try next bookie
                }
                Ok(_) => continue,
                Err(_) => continue, // connection error, try next bookie
            }
        }

        Err(ClientError::Protocol(format!(
            "entry not found: ledger={}, entry={}", self.ledger_id, entry_id
        )))
    }

    /// Read a range of entries.
    pub async fn read_entries(
        &self,
        conns: &mut [BookieConnection],
        first_entry: i64,
        last_entry: i64,
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        let mut results = Vec::new();
        for eid in first_entry..=last_entry {
            results.push(self.read_entry(conns, eid).await?);
        }
        Ok(results)
    }
}
```

- [ ] **Step 4: Update lib.rs**

```rust
pub mod distribution;
pub mod bookie_connection;
pub mod write_handle;
pub mod read_handle;
```

- [ ] **Step 5: Run tests, commit**

```bash
git commit -m "feat: add WriteHandle (entry build + quorum ack) and ReadHandle (read + digest verify)

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: BookKeeperClient (Ledger Lifecycle)

**Files:**
- Create: `crates/bookkeeper-client/src/client.rs`
- Test: `crates/bookkeeper-client/tests/client_test.rs`

- [ ] **Step 1: Write failing integration test**

`crates/bookkeeper-client/tests/client_test.rs`:
```rust
use bookkeeper_client::client::BookKeeperClient;
use bookkeeper_protocol::digest::DigestType;
use bookkeeper_server::server::BookieServer;
use bookkeeper_server::config::BookieConfig;
use tempfile::TempDir;

fn test_config(dir: &TempDir) -> BookieConfig {
    BookieConfig {
        port: 0,
        ledger_dirs: vec![dir.path().to_string_lossy().to_string()],
        journal_dir: dir.path().to_string_lossy().to_string(),
        max_frame_size: 5 * 1024 * 1024,
        zk_connect: None,
        zk_ledger_root: "/ledgers".to_string(),
        bookie_id: None,
    }
}

#[tokio::test]
async fn create_write_read_close() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let mut client = BookKeeperClient::new(vec![addr.to_string()]);

    // Create ledger
    let ledger_id = 1;
    let mut wh = client.create_ledger(ledger_id, 1, 1, 1, DigestType::Crc32c, b"")
        .await.unwrap();

    // Write entries
    wh.append(&mut client.connections_mut(), b"entry-0").await.unwrap();
    wh.append(&mut client.connections_mut(), b"entry-1").await.unwrap();
    wh.append(&mut client.connections_mut(), b"entry-2").await.unwrap();

    assert_eq!(wh.last_add_confirmed(), 2);

    // Read entries
    let rh = client.open_ledger(ledger_id, 1, 1, 1, DigestType::Crc32c, b"");
    let data = rh.read_entry(&mut client.connections_mut(), 0).await.unwrap();
    assert_eq!(data, b"entry-0");
    let data = rh.read_entry(&mut client.connections_mut(), 2).await.unwrap();
    assert_eq!(data, b"entry-2");

    server.shutdown().await;
}

#[tokio::test]
async fn write_then_read_all() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    let mut client = BookKeeperClient::new(vec![addr.to_string()]);
    let ledger_id = 2;
    let mut wh = client.create_ledger(ledger_id, 1, 1, 1, DigestType::Crc32c, b"")
        .await.unwrap();

    for i in 0..5 {
        wh.append(&mut client.connections_mut(), format!("data-{i}").as_bytes()).await.unwrap();
    }

    let rh = client.open_ledger(ledger_id, 1, 1, 1, DigestType::Crc32c, b"");
    let entries = rh.read_entries(&mut client.connections_mut(), 0, 4).await.unwrap();
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[0], b"data-0");
    assert_eq!(entries[4], b"data-4");

    server.shutdown().await;
}
```

- [ ] **Step 2: Implement client.rs**

```rust
use std::net::SocketAddr;

use bookkeeper_protocol::constants::MASTER_KEY_LENGTH;
use bookkeeper_protocol::digest::{DigestManager, DigestType};
use crate::bookie_connection::BookieConnection;
use crate::distribution::RoundRobinDistributionSchedule;
use crate::read_handle::ReadHandle;
use crate::write_handle::WriteHandle;
use crate::bookie_connection::ClientError;

/// Minimal BookKeeper client for creating, writing, and reading ledgers.
/// Uses a fixed set of bookie addresses (no ZK discovery in this phase).
pub struct BookKeeperClient {
    bookie_addrs: Vec<String>,
    connections: Vec<BookieConnection>,
}

impl BookKeeperClient {
    pub fn new(bookie_addrs: Vec<String>) -> Self {
        Self {
            bookie_addrs,
            connections: Vec::new(),
        }
    }

    /// Ensure connections to all bookies are established.
    async fn ensure_connections(&mut self) -> Result<(), ClientError> {
        if self.connections.is_empty() {
            for addr_str in &self.bookie_addrs {
                let addr: SocketAddr = addr_str.parse()
                    .map_err(|e| ClientError::Protocol(format!("bad address '{addr_str}': {e}")))?;
                let conn = BookieConnection::connect(addr).await?;
                self.connections.push(conn);
            }
        }
        Ok(())
    }

    pub fn connections_mut(&mut self) -> &mut [BookieConnection] {
        &mut self.connections
    }

    /// Create a new ledger with the given parameters. Returns a WriteHandle.
    pub async fn create_ledger(
        &mut self,
        ledger_id: i64,
        ensemble_size: usize,
        write_quorum: usize,
        ack_quorum: usize,
        digest_type: DigestType,
        password: &[u8],
    ) -> Result<WriteHandle, ClientError> {
        self.ensure_connections().await?;

        // Select ensemble (simple: use first N bookies)
        if self.bookie_addrs.len() < ensemble_size {
            return Err(ClientError::Protocol(format!(
                "not enough bookies: need {ensemble_size}, have {}",
                self.bookie_addrs.len()
            )));
        }
        let ensemble: Vec<String> = self.bookie_addrs[..ensemble_size].to_vec();
        let schedule = RoundRobinDistributionSchedule::new(ensemble_size, write_quorum, ack_quorum);
        let dm = DigestManager::new(digest_type).with_password(password);

        let mut master_key = [0u8; MASTER_KEY_LENGTH];
        // Derive master key from password (simplified — use first 20 bytes or pad)
        let key_bytes = std::cmp::min(password.len(), MASTER_KEY_LENGTH);
        master_key[..key_bytes].copy_from_slice(&password[..key_bytes]);

        Ok(WriteHandle::new(ledger_id, ensemble, schedule, dm, master_key))
    }

    /// Open a ledger for reading.
    pub fn open_ledger(
        &self,
        ledger_id: i64,
        ensemble_size: usize,
        write_quorum: usize,
        ack_quorum: usize,
        digest_type: DigestType,
        password: &[u8],
    ) -> ReadHandle {
        let ensemble: Vec<String> = self.bookie_addrs[..ensemble_size].to_vec();
        let schedule = RoundRobinDistributionSchedule::new(ensemble_size, write_quorum, ack_quorum);
        let dm = DigestManager::new(digest_type).with_password(password);

        let mut master_key = [0u8; MASTER_KEY_LENGTH];
        let key_bytes = std::cmp::min(password.len(), MASTER_KEY_LENGTH);
        master_key[..key_bytes].copy_from_slice(&password[..key_bytes]);

        ReadHandle::new(ledger_id, ensemble, schedule, dm, master_key)
    }
}
```

- [ ] **Step 3: Update lib.rs**

```rust
pub mod distribution;
pub mod bookie_connection;
pub mod write_handle;
pub mod read_handle;
pub mod client;
```

- [ ] **Step 4: Run tests, commit**

```bash
git commit -m "feat: add BookKeeperClient with create_ledger, open_ledger, write, and read

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Spec Coverage

| Spec Section | Task | Notes |
|---|---|---|
| §1 Client initialization | Task 4 (BookKeeperClient) | Simplified: fixed bookie list, no ZK discovery |
| §2 Ledger creation | Task 4 | Simplified: no ZK metadata, local ledger ID |
| §3.1 Entry append | Task 3 (WriteHandle) | Full: digest + quorum acks + LAC advancement |
| §3.2 Ack handling | Task 1 (AckSet) + Task 3 | Complete |
| §3.3 LAC advancement | Task 3 | Sequential in-order advancement |
| §4 Ensemble change | Deferred | Phase 6b |
| §5.1 Single entry read | Task 3 (ReadHandle) | With digest verification + bookie fallback |
| §5.2 Speculative reads | Deferred | Phase 6b |
| §5.3 Digest verification | Task 3 | Via parse_entry from bookkeeper-protocol |
| §6 Distribution schedule | Task 1 | Round-robin write set |
| §6.2 AckSet | Task 1 | Complete |
| §7 Ensemble placement | Task 4 | Simplified: sequential selection |
| §8 Ledger recovery | Deferred | Phase 7 |
| §9 Ledger close | Deferred | Needs metadata update |
| §10 Connection management | Task 2 | Single connection per bookie |

## What's Next

| Phase | Scope | Status |
|-------|-------|--------|
| Phase 1-5 | Wire Protocol, Metadata, Storage, Server, Registration | Complete |
| Phase 6 | Client Library (core) | This plan |
| Phase 6b | Client: ensemble change, speculative reads, recovery | Next |
| Phase 7 | Replication & Recovery | After Phase 6 |
