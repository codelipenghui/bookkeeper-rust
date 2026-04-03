# Phase 7: Ledger Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the client-side ledger recovery protocol — fencing a crashed writer's ledger, forward-reading to find the last entry, re-replicating under-replicated entries, and closing the ledger — enabling correct recovery of open ledgers after writer failure.

**Architecture:** Add a `RecoveryHandle` to the `bookkeeper-client` crate that orchestrates the recovery protocol: Phase 1 (fence all bookies in ensemble via FLAG_DO_FENCING reads, determine recovery LAC), Phase 2 (forward read from LAC+1 to find true last entry), Phase 3 (re-replicate entries with FLAG_RECOVERY_ADD to ensure quorum), Phase 4 (close ledger by updating metadata with lastEntryId). Uses the existing `BookieConnection`, `DistributionSchedule`, and `DigestManager`.

**Tech Stack:** `bookkeeper-client` (BookieConnection, WriteHandle, ReadHandle), `bookkeeper-protocol` (FLAG_DO_FENCING, FLAG_RECOVERY_ADD, LAST_ADD_CONFIRMED)

**Spec reference:** `docs/superpowers/specs/07-replication-recovery.md` §5 (Fencing Protocol), §4.2 (Per-Ledger Replication), §6.4 (LAC in Recovery)

---

## File Structure

```
crates/bookkeeper-client/
└── src/
    ├── lib.rs                    # Add pub mod recovery
    └── recovery.rs               # NEW: RecoveryHandle — fencing + forward read + re-replicate + close
```

---

### Task 1: Fencing Protocol

**Files:**
- Create: `crates/bookkeeper-client/src/recovery.rs`
- Modify: `crates/bookkeeper-client/src/lib.rs`
- Test: `crates/bookkeeper-client/tests/recovery_test.rs`

- [ ] **Step 1: Write failing tests**

`crates/bookkeeper-client/tests/recovery_test.rs`:
```rust
use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_client::client::BookKeeperClient;
use bookkeeper_client::recovery::RecoveryHandle;
use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
use bookkeeper_protocol::digest::{DigestManager, DigestType, generate_master_key};
use bookkeeper_server::server::BookieServer;
use bookkeeper_server::config::BookieConfig;
use std::sync::Arc;
use tokio::sync::Mutex;
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
async fn fence_ledger_and_get_lac() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // Write 5 entries as a normal writer
    let mut client = BookKeeperClient::new(vec![addr.to_string()]);
    let mut wh = client.create_ledger(1, 1, 1, 1, DigestType::Crc32c, b"").await.unwrap();
    for i in 0..5 {
        wh.append(client.connections(), format!("entry-{i}").as_bytes()).await.unwrap();
    }
    assert_eq!(wh.last_add_confirmed(), 4);

    // Now recover the ledger (simulating writer crash)
    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");

    let conn = Arc::new(Mutex::new(BookieConnection::connect(addr).await.unwrap()));
    let conns = vec![conn];

    let mut recovery = RecoveryHandle::new(1, ensemble, schedule, dm, master_key);
    let recovery_lac = recovery.fence(&conns).await.unwrap();
    assert_eq!(recovery_lac, 4);

    server.shutdown().await;
}

#[tokio::test]
async fn fence_blocks_normal_writes() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // Write some entries
    let mut client = BookKeeperClient::new(vec![addr.to_string()]);
    let mut wh = client.create_ledger(1, 1, 1, 1, DigestType::Crc32c, b"").await.unwrap();
    wh.append(client.connections(), b"before-fence").await.unwrap();

    // Fence the ledger
    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");
    let conn = Arc::new(Mutex::new(BookieConnection::connect(addr).await.unwrap()));
    let mut recovery = RecoveryHandle::new(1, ensemble, schedule, dm, master_key);
    recovery.fence(&vec![conn]).await.unwrap();

    // Normal write should now fail with EFENCED
    let result = wh.append(client.connections(), b"after-fence").await;
    assert!(result.is_err());

    server.shutdown().await;
}

#[tokio::test]
async fn full_recovery_read_and_close() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // Write entries
    let mut client = BookKeeperClient::new(vec![addr.to_string()]);
    let mut wh = client.create_ledger(1, 1, 1, 1, DigestType::Crc32c, b"").await.unwrap();
    for i in 0..3 {
        wh.append(client.connections(), format!("data-{i}").as_bytes()).await.unwrap();
    }

    // Recover: fence → forward read → close
    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");
    let conn = Arc::new(Mutex::new(BookieConnection::connect(addr).await.unwrap()));
    let conns = vec![conn];

    let mut recovery = RecoveryHandle::new(1, ensemble, schedule, dm, master_key);
    let recovery_lac = recovery.fence(&conns).await.unwrap();

    // Forward read to find last entry
    let last_entry = recovery.forward_read(&conns, recovery_lac).await.unwrap();
    assert_eq!(last_entry, 2); // entries 0, 1, 2 were written

    // Recovery add (re-replicate) — in single-bookie case, entries already have quorum
    // Close the ledger
    let (last_entry_id, length) = recovery.close(last_entry).unwrap();
    assert_eq!(last_entry_id, 2);
    assert!(length > 0);

    server.shutdown().await;
}

#[tokio::test]
async fn recovery_add_bypasses_fence() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let addr = server.local_addr();

    // Write 1 entry
    let mut client = BookKeeperClient::new(vec![addr.to_string()]);
    let mut wh = client.create_ledger(1, 1, 1, 1, DigestType::Crc32c, b"").await.unwrap();
    wh.append(client.connections(), b"original").await.unwrap();

    // Fence
    let ensemble = vec![addr.to_string()];
    let schedule = RoundRobinDistributionSchedule::new(1, 1, 1);
    let dm = DigestManager::new(DigestType::Crc32c);
    let master_key = generate_master_key(b"");
    let conn = Arc::new(Mutex::new(BookieConnection::connect(addr).await.unwrap()));
    let conns = vec![conn];
    let mut recovery = RecoveryHandle::new(1, ensemble, schedule, dm, master_key);
    recovery.fence(&conns).await.unwrap();

    // Recovery add should succeed despite fence
    let entry_id = recovery.recovery_add_entry(&conns, 1, b"recovery-data").await.unwrap();
    assert_eq!(entry_id, 1);

    server.shutdown().await;
}
```

- [ ] **Step 2: Implement recovery.rs**

```rust
use std::sync::Arc;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::Mutex;

use bookkeeper_common::types::LAST_ADD_CONFIRMED;
use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::DigestManager;
use bookkeeper_protocol::entry::{EntryBuilder, parse_entry};
use bookkeeper_protocol::v2::response::V2Response;
use crate::bookie_connection::{BookieConnection, ClientError};
use crate::distribution::RoundRobinDistributionSchedule;

/// Handles ledger recovery after a writer crash.
///
/// Recovery protocol:
/// 1. Fence: send FLAG_DO_FENCING reads to all bookies, wait for write quorum, determine LAC
/// 2. Forward read: read entries from LAC+1 until ENOENTRY to find true last entry
/// 3. Re-replicate: write missing entries with FLAG_RECOVERY_ADD
/// 4. Close: mark ledger as CLOSED with correct lastEntryId and length
pub struct RecoveryHandle {
    ledger_id: i64,
    ensemble: Vec<String>,
    schedule: RoundRobinDistributionSchedule,
    digest_manager: DigestManager,
    master_key: [u8; MASTER_KEY_LENGTH],
    recovery_lac: i64,
    last_entry_found: i64,
    total_length: i64,
}

impl RecoveryHandle {
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
            recovery_lac: -1,
            last_entry_found: -1,
            total_length: 0,
        }
    }

    pub fn recovery_lac(&self) -> i64 {
        self.recovery_lac
    }

    pub fn last_entry_found(&self) -> i64 {
        self.last_entry_found
    }

    /// Phase 1: Fence the ledger on all bookies.
    /// Sends FLAG_DO_FENCING read to each bookie, waits for write quorum responses.
    /// Returns the recovery LAC (max LAC across all fencing responses).
    pub async fn fence(
        &mut self,
        conns: &[Arc<Mutex<BookieConnection>>],
    ) -> Result<i64, ClientError> {
        let write_quorum = self.schedule.write_quorum_size();
        let mut max_lac: i64 = -1;
        let mut success_count = 0;

        // Send fencing read to ALL bookies in ensemble (not just write set)
        let mut futures = Vec::new();
        for (idx, _bookie) in self.ensemble.iter().enumerate() {
            if idx >= conns.len() {
                continue;
            }
            let conn = conns[idx].clone();
            let ledger_id = self.ledger_id;
            let master_key = self.master_key;
            futures.push(tokio::spawn(async move {
                let mut conn = conn.lock().await;
                conn.send_read_entry(
                    FLAG_DO_FENCING,
                    ledger_id,
                    LAST_ADD_CONFIRMED,
                    Some(master_key),
                ).await
            }));
        }

        // Collect responses
        for future in futures {
            match future.await {
                Ok(Ok(V2Response::Read { error_code, entry_id, .. })) => {
                    if error_code == V2_EOK || error_code == bookkeeper_common::error::BKError::NoEntry.to_v2_code() {
                        success_count += 1;
                        // entry_id contains the LAC for LAST_ADD_CONFIRMED reads
                        if entry_id > max_lac {
                            max_lac = entry_id;
                        }
                    }
                }
                _ => {
                    // Fencing failed on this bookie — continue, need write quorum
                }
            }
        }

        if success_count < write_quorum {
            return Err(ClientError::Protocol(format!(
                "fencing quorum not reached: got {success_count}, need {write_quorum}"
            )));
        }

        self.recovery_lac = max_lac;
        Ok(max_lac)
    }

    /// Phase 2: Forward read from recovery_lac+1 to find the true last entry.
    /// Reads entries sequentially until ENOENTRY. Returns the last entry ID found.
    pub async fn forward_read(
        &mut self,
        conns: &[Arc<Mutex<BookieConnection>>],
        recovery_lac: i64,
    ) -> Result<i64, ClientError> {
        let mut last_entry = recovery_lac;
        let mut entry_id = recovery_lac + 1;
        let mut total_length: i64 = 0;

        // First, compute length of entries up to recovery_lac
        for eid in 0..=recovery_lac {
            let write_set = self.schedule.write_set(eid);
            for &bookie_idx in &write_set {
                if bookie_idx >= conns.len() {
                    continue;
                }
                let mut conn = conns[bookie_idx].lock().await;
                match conn.send_read_entry(0, self.ledger_id, eid, None).await {
                    Ok(V2Response::Read { error_code, data, .. }) if error_code == V2_EOK => {
                        if let Some(entry_data) = data {
                            if let Ok(entry) = parse_entry(&self.digest_manager, entry_data) {
                                total_length = entry.length;
                            }
                        }
                        break;
                    }
                    _ => continue,
                }
            }
        }

        // Now forward read from recovery_lac+1
        loop {
            let write_set = self.schedule.write_set(entry_id);
            let mut found = false;

            for &bookie_idx in &write_set {
                if bookie_idx >= conns.len() {
                    continue;
                }
                let mut conn = conns[bookie_idx].lock().await;
                match conn.send_read_entry(0, self.ledger_id, entry_id, None).await {
                    Ok(V2Response::Read { error_code, data, .. }) => {
                        if error_code == V2_EOK {
                            if let Some(entry_data) = data {
                                if let Ok(entry) = parse_entry(&self.digest_manager, entry_data) {
                                    total_length = entry.length;
                                    last_entry = entry_id;
                                    found = true;
                                }
                            }
                            break;
                        }
                    }
                    _ => continue,
                }
            }

            if !found {
                break; // No more entries
            }
            entry_id += 1;
        }

        self.last_entry_found = last_entry;
        self.total_length = total_length;
        Ok(last_entry)
    }

    /// Phase 3: Write a recovery entry with FLAG_RECOVERY_ADD.
    /// Used to re-replicate entries that don't meet write quorum.
    pub async fn recovery_add_entry(
        &self,
        conns: &[Arc<Mutex<BookieConnection>>],
        entry_id: i64,
        payload: &[u8],
    ) -> Result<i64, ClientError> {
        let entry_bytes = EntryBuilder::new(&self.digest_manager)
            .ledger_id(self.ledger_id)
            .entry_id(entry_id)
            .lac(self.recovery_lac)
            .length(payload.len() as i64)
            .payload(Bytes::copy_from_slice(payload))
            .build()
            .freeze();

        let write_set = self.schedule.write_set(entry_id);
        let mut ack_set = self.schedule.new_ack_set();

        let mut futures = Vec::new();
        for &bookie_idx in &write_set {
            if bookie_idx >= conns.len() {
                continue;
            }
            let conn = conns[bookie_idx].clone();
            let entry = entry_bytes.clone();
            let mk = self.master_key;
            futures.push(tokio::spawn(async move {
                let mut conn = conn.lock().await;
                conn.send_add_entry(FLAG_RECOVERY_ADD, mk, entry).await
            }));
        }

        for (i, future) in futures.into_iter().enumerate() {
            let bookie_idx = write_set[i];
            match future.await {
                Ok(Ok(V2Response::Add { error_code, .. })) => {
                    if error_code == V2_EOK {
                        if ack_set.complete_bookie(bookie_idx) {
                            return Ok(entry_id);
                        }
                    } else {
                        ack_set.fail_bookie(bookie_idx);
                    }
                }
                _ => {
                    ack_set.fail_bookie(bookie_idx);
                }
            }
        }

        if ack_set.is_quorum_reached() {
            Ok(entry_id)
        } else {
            Err(ClientError::Protocol("recovery add quorum not reached".into()))
        }
    }

    /// Phase 4: Close the ledger after recovery.
    /// Returns (lastEntryId, totalLength) for metadata update.
    pub fn close(&self, last_entry_id: i64) -> Result<(i64, i64), ClientError> {
        Ok((last_entry_id, self.total_length))
    }
}
```

- [ ] **Step 3: Update lib.rs**

Add `pub mod recovery;` to `crates/bookkeeper-client/src/lib.rs`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p bookkeeper-client`
Expected: All tests pass (14 existing + 4 new recovery tests)

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: add RecoveryHandle with fencing, forward read, recovery add, and close

Implements the client-side ledger recovery protocol:
- Phase 1: Fence all bookies via FLAG_DO_FENCING parallel reads, determine recovery LAC
- Phase 2: Forward read from LAC+1 to find true last entry
- Phase 3: Re-replicate entries with FLAG_RECOVERY_ADD for quorum
- Phase 4: Close ledger with correct lastEntryId and totalLength

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Spec Coverage

| Spec Section | Task | Notes |
|---|---|---|
| §5.1 Purpose | Task 1 | Fencing prevents split-brain |
| §5.2 Protocol steps — Initiator | Task 1 (fence method) | Parallel FLAG_DO_FENCING to all bookies, wait for write quorum |
| §5.2 Protocol steps — Bookie | Already implemented | Server handles FLAG_DO_FENCING in processor.rs |
| §5.2 Protocol steps — Collecting responses | Task 1 | max(all LAC values) = recovery LAC |
| §5.3 Effect on old writer | Task 1 test | fence_blocks_normal_writes test verifies EFENCED |
| §5.4 Recovery add | Task 1 (recovery_add_entry) | FLAG_RECOVERY_ADD bypasses fence |
| §4.2 Per-ledger replication — forward read | Task 1 (forward_read) | Sequential read from LAC+1 until ENOENTRY |
| §4.2 Per-ledger replication — re-replicate | Task 1 (recovery_add_entry) | FLAG_RECOVERY_ADD to write quorum |
| §6.4 LAC in Recovery | Task 1 | recovery_lac = max(fencing LACs) |
| §2 Auditor | Deferred | Phase 7b — ZK leader election + bookie monitoring |
| §3 UnderreplicationManager | Deferred | Phase 7b — ZK underreplication tracking |
| §4.1 ReplicationWorker main loop | Deferred | Phase 7b — background daemon |
| §7 Decommissioning | Deferred | Phase 7b |

## What's Next

| Phase | Scope | Status |
|-------|-------|--------|
| Phase 1-6 | Wire Protocol, Metadata, Storage, Server, Registration, Client | Complete |
| Phase 7 | Recovery (client-side fencing + forward read + close) | This plan |
| Phase 7b | Auditor, ReplicationWorker, UnderreplicationManager | Next |
| Phase 8 | Configuration | Future |
