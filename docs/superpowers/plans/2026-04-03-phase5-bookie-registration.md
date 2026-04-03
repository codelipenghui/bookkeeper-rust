# Phase 5: Bookie Registration & Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate ZooKeeper into the bookie server for registration (ephemeral znodes), cookie validation, layout management, and cluster initialization — enabling the Rust bookie to join an existing Java BookKeeper cluster.

**Architecture:** Add the `zookeeper-client` crate to `bookkeeper-metadata` and implement the `RegistrationManager` trait (new) and `LayoutManager` trait (existing) with ZK backends. Add a `ZkRegistrationManager` that manages ephemeral znodes for bookie registration, persistent znodes for cookies, and layout/instance ID management. Integrate with `BookieServer` for startup registration and read-only transitions.

**Tech Stack:** `zookeeper-client` (async ZK client), `bookkeeper-metadata` (traits + ZkPaths), `uuid` (instance ID generation), `prost` (BookieServiceInfo protobuf)

**Spec reference:** `docs/superpowers/specs/05-bookie-registration.md`

---

## File Structure

```
crates/bookkeeper-metadata/
└── src/
    ├── traits.rs                  # Add RegistrationManager trait
    ├── service_info.rs            # NEW: BookieServiceInfo builder
    └── zk/
        ├── mod.rs                 # NEW: ZK backend module
        ├── registration.rs        # NEW: ZkRegistrationManager
        └── layout.rs              # NEW: ZkLayoutManager
```

---

### Task 1: Add RegistrationManager Trait + BookieServiceInfo

**Files:**
- Modify: `crates/bookkeeper-metadata/src/traits.rs`
- Create: `crates/bookkeeper-metadata/src/service_info.rs`
- Modify: `crates/bookkeeper-metadata/src/lib.rs`
- Modify: `Cargo.toml` (workspace root — add `zookeeper-client` and `uuid`)
- Modify: `crates/bookkeeper-metadata/Cargo.toml`
- Test: `crates/bookkeeper-metadata/tests/service_info_test.rs`

- [ ] **Step 1: Add workspace dependencies**

In root `Cargo.toml` workspace.dependencies:
```toml
zookeeper-client = "0.11"
uuid = { version = "1", features = ["v4"] }
```

In `crates/bookkeeper-metadata/Cargo.toml` dependencies, add:
```toml
zookeeper-client = { workspace = true }
uuid = { workspace = true }
```

- [ ] **Step 2: Write failing tests for BookieServiceInfo**

`crates/bookkeeper-metadata/tests/service_info_test.rs`:
```rust
use bookkeeper_metadata::service_info::BookieServiceInfo;

#[test]
fn default_service_info() {
    let info = BookieServiceInfo::new("bookie1", "bookie1", 3181);
    assert_eq!(info.bookie_id(), "bookie1");
    let bytes = info.to_protobuf_bytes();
    assert!(!bytes.is_empty());
    let parsed = BookieServiceInfo::from_protobuf_bytes(&bytes).unwrap();
    assert_eq!(parsed.bookie_id(), "bookie1");
}

#[test]
fn service_info_endpoint() {
    let info = BookieServiceInfo::new("host1:3181", "host1", 3181);
    let bytes = info.to_protobuf_bytes();
    let parsed = BookieServiceInfo::from_protobuf_bytes(&bytes).unwrap();
    assert_eq!(parsed.host(), "host1");
    assert_eq!(parsed.port(), 3181);
}
```

- [ ] **Step 3: Implement service_info.rs**

```rust
use prost::Message;
use crate::ledger_metadata::MetadataError;

/// BookieServiceInfo describes a bookie's endpoint for service discovery.
#[derive(Debug, Clone)]
pub struct BookieServiceInfo {
    bookie_id: String,
    host: String,
    port: i32,
}

impl BookieServiceInfo {
    pub fn new(bookie_id: &str, host: &str, port: u16) -> Self {
        Self {
            bookie_id: bookie_id.to_string(),
            host: host.to_string(),
            port: port as i32,
        }
    }

    pub fn bookie_id(&self) -> &str { &self.bookie_id }
    pub fn host(&self) -> &str { &self.host }
    pub fn port(&self) -> i32 { self.port }

    pub fn to_protobuf_bytes(&self) -> Vec<u8> {
        let endpoint = bookkeeper_proto::bookie_service_info_format::Endpoint {
            id: self.bookie_id.clone(),
            port: self.port,
            host: self.host.clone(),
            protocol: "bookie-rpc".to_string(),
            auth: vec![],
            extensions: vec![],
        };
        let info = bookkeeper_proto::BookieServiceInfoFormat {
            endpoints: vec![endpoint],
            properties: std::collections::HashMap::new(),
        };
        info.encode_to_vec()
    }

    pub fn from_protobuf_bytes(data: &[u8]) -> Result<Self, MetadataError> {
        let info = bookkeeper_proto::BookieServiceInfoFormat::decode(data)
            .map_err(|e| MetadataError::SerializationError(format!("service info decode: {e}")))?;
        let endpoint = info.endpoints.first()
            .ok_or_else(|| MetadataError::SerializationError("no endpoints in service info".into()))?;
        Ok(Self {
            bookie_id: endpoint.id.clone(),
            host: endpoint.host.clone(),
            port: endpoint.port,
        })
    }
}
```

- [ ] **Step 4: Add RegistrationManager trait to traits.rs**

Append to `crates/bookkeeper-metadata/src/traits.rs`:
```rust
use crate::service_info::BookieServiceInfo;

/// Manages bookie registration and discovery in the metadata store.
#[async_trait]
pub trait RegistrationManager: Send + Sync {
    async fn register_bookie(&self, bookie_id: &str, read_only: bool, info: &BookieServiceInfo) -> Result<(), MetadataError>;
    async fn unregister_bookie(&self, bookie_id: &str, read_only: bool) -> Result<(), MetadataError>;
    async fn get_writable_bookies(&self) -> Result<Vec<String>, MetadataError>;
    async fn get_readonly_bookies(&self) -> Result<Vec<String>, MetadataError>;
    async fn read_cookie(&self, bookie_id: &str) -> Result<Option<(Vec<u8>, i32)>, MetadataError>;
    async fn create_cookie(&self, bookie_id: &str, data: &[u8]) -> Result<(), MetadataError>;
    async fn update_cookie(&self, bookie_id: &str, data: &[u8], version: i32) -> Result<(), MetadataError>;
    async fn delete_cookie(&self, bookie_id: &str, version: i32) -> Result<(), MetadataError>;
    async fn get_cluster_instance_id(&self) -> Result<Option<String>, MetadataError>;
    async fn init_new_cluster(&self, layout: &LedgerLayout, instance_id: &str) -> Result<(), MetadataError>;
}
```

- [ ] **Step 5: Update lib.rs**

Add `pub mod service_info;` to lib.rs.

- [ ] **Step 6: Run tests**

Run: `cargo test -p bookkeeper-metadata`
Expected: All tests pass including new service_info tests

- [ ] **Step 7: Commit**

```bash
git commit -m "feat: add RegistrationManager trait and BookieServiceInfo protobuf serialization

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: ZK Registration Manager Implementation

**Files:**
- Create: `crates/bookkeeper-metadata/src/zk/mod.rs`
- Create: `crates/bookkeeper-metadata/src/zk/registration.rs`
- Create: `crates/bookkeeper-metadata/src/zk/layout.rs`
- Modify: `crates/bookkeeper-metadata/src/lib.rs`
- Test: `crates/bookkeeper-metadata/tests/zk_registration_test.rs`

NOTE: Tests for the ZK implementation require a running ZooKeeper instance. These tests should be marked `#[ignore]` by default and run manually with `--ignored` when a ZK instance is available. Alternatively, we test the logic with unit tests that mock the ZK client. For this phase, we'll write the implementation with compile-time verification and a few `#[ignore]` integration tests.

- [ ] **Step 1: Create ZK module structure**

`crates/bookkeeper-metadata/src/zk/mod.rs`:
```rust
pub mod registration;
pub mod layout;
```

- [ ] **Step 2: Implement ZkRegistrationManager**

`crates/bookkeeper-metadata/src/zk/registration.rs`:
```rust
use async_trait::async_trait;
use zookeeper_client as zk;
use crate::ledger_metadata::MetadataError;
use crate::service_info::BookieServiceInfo;
use crate::traits::{LedgerLayout, RegistrationManager};
use crate::zk_path::ZkPaths;

pub struct ZkRegistrationManager {
    client: zk::Client,
    paths: ZkPaths,
}

impl ZkRegistrationManager {
    pub async fn new(connect_string: &str, ledger_root: &str) -> Result<Self, MetadataError> {
        let client = zk::Client::connect(connect_string).await
            .map_err(|e| MetadataError::ConnectionError(format!("ZK connect: {e}")))?;
        let paths = ZkPaths::new(ledger_root);
        Ok(Self { client, paths })
    }

    pub fn paths(&self) -> &ZkPaths {
        &self.paths
    }

    async fn ensure_path_exists(&self, path: &str) -> Result<(), MetadataError> {
        // Create path and all parents if they don't exist
        match self.client.create(path, &[], &zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all())).await {
            Ok(_) => Ok(()),
            Err(zk::Error::NodeExists) => Ok(()),
            Err(e) => Err(MetadataError::ConnectionError(format!("ensure path {path}: {e}"))),
        }
    }

    async fn ensure_parent_paths(&self) -> Result<(), MetadataError> {
        self.ensure_path_exists(self.paths.root()).await?;
        self.ensure_path_exists(self.paths.available()).await?;
        self.ensure_path_exists(self.paths.available_readonly()).await?;
        self.ensure_path_exists(self.paths.cookies()).await?;
        Ok(())
    }
}

#[async_trait]
impl RegistrationManager for ZkRegistrationManager {
    async fn register_bookie(&self, bookie_id: &str, read_only: bool, info: &BookieServiceInfo) -> Result<(), MetadataError> {
        self.ensure_parent_paths().await?;
        let data = info.to_protobuf_bytes();
        let path = if read_only {
            self.paths.bookie_readonly_registration(bookie_id)
        } else {
            self.paths.bookie_registration(bookie_id)
        };
        match self.client.create(&path, &data, &zk::CreateMode::Ephemeral.with_acls(zk::Acls::anyone_all())).await {
            Ok(_) => Ok(()),
            Err(zk::Error::NodeExists) => {
                // Already registered — update data
                self.client.set_data(&path, &data, None).await
                    .map_err(|e| MetadataError::ConnectionError(format!("update registration: {e}")))?;
                Ok(())
            }
            Err(e) => Err(MetadataError::ConnectionError(format!("register bookie: {e}"))),
        }
    }

    async fn unregister_bookie(&self, bookie_id: &str, read_only: bool) -> Result<(), MetadataError> {
        let path = if read_only {
            self.paths.bookie_readonly_registration(bookie_id)
        } else {
            self.paths.bookie_registration(bookie_id)
        };
        match self.client.delete(&path, None).await {
            Ok(()) => Ok(()),
            Err(zk::Error::NoNode) => Ok(()), // already gone
            Err(e) => Err(MetadataError::ConnectionError(format!("unregister bookie: {e}"))),
        }
    }

    async fn get_writable_bookies(&self) -> Result<Vec<String>, MetadataError> {
        match self.client.get_children(self.paths.available()).await {
            Ok((children, _)) => {
                // Filter out the "readonly" child which is a subpath, not a bookie
                Ok(children.into_iter().filter(|c| c != "readonly").collect())
            }
            Err(zk::Error::NoNode) => Ok(vec![]),
            Err(e) => Err(MetadataError::ConnectionError(format!("get writable bookies: {e}"))),
        }
    }

    async fn get_readonly_bookies(&self) -> Result<Vec<String>, MetadataError> {
        match self.client.get_children(self.paths.available_readonly()).await {
            Ok((children, _)) => Ok(children),
            Err(zk::Error::NoNode) => Ok(vec![]),
            Err(e) => Err(MetadataError::ConnectionError(format!("get readonly bookies: {e}"))),
        }
    }

    async fn read_cookie(&self, bookie_id: &str) -> Result<Option<(Vec<u8>, i32)>, MetadataError> {
        let path = self.paths.cookie_path(bookie_id);
        match self.client.get_data(&path).await {
            Ok((data, stat)) => Ok(Some((data, stat.version))),
            Err(zk::Error::NoNode) => Ok(None),
            Err(e) => Err(MetadataError::ConnectionError(format!("read cookie: {e}"))),
        }
    }

    async fn create_cookie(&self, bookie_id: &str, data: &[u8]) -> Result<(), MetadataError> {
        self.ensure_path_exists(self.paths.cookies()).await?;
        let path = self.paths.cookie_path(bookie_id);
        self.client.create(&path, data, &zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all())).await
            .map_err(|e| MetadataError::ConnectionError(format!("create cookie: {e}")))?;
        Ok(())
    }

    async fn update_cookie(&self, bookie_id: &str, data: &[u8], version: i32) -> Result<(), MetadataError> {
        let path = self.paths.cookie_path(bookie_id);
        self.client.set_data(&path, data, Some(version)).await
            .map_err(|e| MetadataError::ConnectionError(format!("update cookie: {e}")))?;
        Ok(())
    }

    async fn delete_cookie(&self, bookie_id: &str, version: i32) -> Result<(), MetadataError> {
        let path = self.paths.cookie_path(bookie_id);
        self.client.delete(&path, Some(version)).await
            .map_err(|e| MetadataError::ConnectionError(format!("delete cookie: {e}")))?;
        Ok(())
    }

    async fn get_cluster_instance_id(&self) -> Result<Option<String>, MetadataError> {
        match self.client.get_data(self.paths.instance_id()).await {
            Ok((data, _)) => {
                let id = String::from_utf8(data)
                    .map_err(|e| MetadataError::SerializationError(format!("instance id: {e}")))?;
                Ok(Some(id))
            }
            Err(zk::Error::NoNode) => Ok(None),
            Err(e) => Err(MetadataError::ConnectionError(format!("get instance id: {e}"))),
        }
    }

    async fn init_new_cluster(&self, layout: &LedgerLayout, instance_id: &str) -> Result<(), MetadataError> {
        // Create root paths
        self.ensure_path_exists(self.paths.root()).await?;
        self.ensure_path_exists(self.paths.available()).await?;
        self.ensure_path_exists(self.paths.available_readonly()).await?;
        self.ensure_path_exists(self.paths.cookies()).await?;

        // Create LAYOUT
        let layout_data = format!("{}\n{}", layout.manager_factory_class, layout.manager_version);
        self.client.create(self.paths.layout(), layout_data.as_bytes(),
            &zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all())).await
            .map_err(|e| MetadataError::ConnectionError(format!("create layout: {e}")))?;

        // Create instance ID
        self.client.create(self.paths.instance_id(), instance_id.as_bytes(),
            &zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all())).await
            .map_err(|e| MetadataError::ConnectionError(format!("create instance id: {e}")))?;

        Ok(())
    }
}
```

- [ ] **Step 3: Implement ZkLayoutManager**

`crates/bookkeeper-metadata/src/zk/layout.rs`:
```rust
use async_trait::async_trait;
use zookeeper_client as zk;
use crate::ledger_metadata::MetadataError;
use crate::traits::{LayoutManager, LedgerLayout};
use crate::zk_path::ZkPaths;

pub struct ZkLayoutManager {
    client: zk::Client,
    paths: ZkPaths,
}

impl ZkLayoutManager {
    pub fn new(client: zk::Client, paths: ZkPaths) -> Self {
        Self { client, paths }
    }
}

#[async_trait]
impl LayoutManager for ZkLayoutManager {
    async fn read_ledger_layout(&self) -> Result<Option<LedgerLayout>, MetadataError> {
        match self.client.get_data(self.paths.layout()).await {
            Ok((data, _)) => {
                let text = String::from_utf8(data)
                    .map_err(|e| MetadataError::SerializationError(format!("layout: {e}")))?;
                let mut lines = text.lines();
                let class = lines.next()
                    .ok_or_else(|| MetadataError::SerializationError("empty layout".into()))?
                    .to_string();
                let version: i32 = lines.next()
                    .unwrap_or("1")
                    .parse()
                    .unwrap_or(1);
                Ok(Some(LedgerLayout {
                    manager_factory_class: class,
                    manager_version: version,
                }))
            }
            Err(zk::Error::NoNode) => Ok(None),
            Err(e) => Err(MetadataError::ConnectionError(format!("read layout: {e}"))),
        }
    }

    async fn store_ledger_layout(&self, layout: &LedgerLayout) -> Result<(), MetadataError> {
        let data = format!("{}\n{}", layout.manager_factory_class, layout.manager_version);
        self.client.create(self.paths.layout(), data.as_bytes(),
            &zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all())).await
            .map_err(|e| MetadataError::ConnectionError(format!("store layout: {e}")))?;
        Ok(())
    }

    async fn delete_ledger_layout(&self) -> Result<(), MetadataError> {
        match self.client.delete(self.paths.layout(), None).await {
            Ok(()) => Ok(()),
            Err(zk::Error::NoNode) => Ok(()),
            Err(e) => Err(MetadataError::ConnectionError(format!("delete layout: {e}"))),
        }
    }
}
```

- [ ] **Step 4: Update lib.rs**

```rust
pub mod ledger_metadata;
pub mod metadata_serde;
pub mod zk_path;
pub mod cookie;
pub mod versioned;
pub mod traits;
pub mod service_info;
pub mod zk;
```

- [ ] **Step 5: Write integration test (marked #[ignore])**

`crates/bookkeeper-metadata/tests/zk_registration_test.rs`:
```rust
use bookkeeper_metadata::service_info::BookieServiceInfo;
use bookkeeper_metadata::traits::{LedgerLayout, RegistrationManager};
use bookkeeper_metadata::zk::registration::ZkRegistrationManager;

/// These tests require a running ZooKeeper at localhost:2181.
/// Run with: cargo test -p bookkeeper-metadata --test zk_registration_test -- --ignored

#[tokio::test]
#[ignore]
async fn register_and_discover_bookie() {
    let mgr = ZkRegistrationManager::new("localhost:2181", "/test-ledgers").await.unwrap();
    let info = BookieServiceInfo::new("test-bookie:3181", "test-bookie", 3181);

    // Init cluster
    let layout = LedgerLayout {
        manager_factory_class: "HierarchicalLedgerManagerFactory".to_string(),
        manager_version: 1,
    };
    let _ = mgr.init_new_cluster(&layout, &uuid::Uuid::new_v4().to_string()).await;

    // Register
    mgr.register_bookie("test-bookie:3181", false, &info).await.unwrap();

    // Discover
    let bookies = mgr.get_writable_bookies().await.unwrap();
    assert!(bookies.contains(&"test-bookie:3181".to_string()));

    // Unregister
    mgr.unregister_bookie("test-bookie:3181", false).await.unwrap();
    let bookies = mgr.get_writable_bookies().await.unwrap();
    assert!(!bookies.contains(&"test-bookie:3181".to_string()));
}

#[tokio::test]
#[ignore]
async fn cookie_round_trip_via_zk() {
    let mgr = ZkRegistrationManager::new("localhost:2181", "/test-ledgers").await.unwrap();
    let cookie_data = b"5\nbookieHost: \"test:3181\"\n";

    // Create
    let _ = mgr.create_cookie("test-cookie-bookie:3181", cookie_data).await;

    // Read
    let (data, version) = mgr.read_cookie("test-cookie-bookie:3181").await.unwrap().unwrap();
    assert_eq!(data, cookie_data);

    // Delete
    mgr.delete_cookie("test-cookie-bookie:3181", version).await.unwrap();
    assert!(mgr.read_cookie("test-cookie-bookie:3181").await.unwrap().is_none());
}
```

- [ ] **Step 6: Verify compilation**

Run: `cargo check -p bookkeeper-metadata`
Expected: Compiles (ZK tests are #[ignore] so they won't run without ZK)

Run: `cargo test -p bookkeeper-metadata`
Expected: All non-ignored tests pass

- [ ] **Step 7: Commit**

```bash
git commit -m "feat: add ZkRegistrationManager and ZkLayoutManager with ZooKeeper integration

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Integrate Registration with BookieServer

**Files:**
- Modify: `crates/bookkeeper-server/src/config.rs`
- Modify: `crates/bookkeeper-server/src/server.rs`
- Modify: `crates/bookkeeper-server/Cargo.toml`

- [ ] **Step 1: Add metadata dependency to server crate**

In `crates/bookkeeper-server/Cargo.toml`:
```toml
bookkeeper-metadata = { workspace = true }
```

- [ ] **Step 2: Add ZK config to BookieConfig**

In `crates/bookkeeper-server/src/config.rs`:
```rust
#[derive(Debug, Clone)]
pub struct BookieConfig {
    pub port: u16,
    pub ledger_dirs: Vec<String>,
    pub journal_dir: String,
    pub max_frame_size: usize,
    pub zk_connect: Option<String>,
    pub zk_ledger_root: String,
    pub bookie_id: Option<String>,
}

impl Default for BookieConfig {
    fn default() -> Self {
        Self {
            port: 3181,
            ledger_dirs: vec!["/tmp/bk-data".to_string()],
            journal_dir: "/tmp/bk-txn".to_string(),
            max_frame_size: 5 * 1024 * 1024,
            zk_connect: None,
            zk_ledger_root: "/ledgers".to_string(),
            bookie_id: None,
        }
    }
}
```

- [ ] **Step 3: Integrate ZK registration into BookieServer::start()**

In `server.rs`, after creating the storage and transitioning to Running, optionally register with ZK:

```rust
// After state.transition_to(BookieState::Running):
if let Some(ref zk_connect) = config.zk_connect {
    let bookie_id = config.bookie_id.clone()
        .unwrap_or_else(|| format!("{}:{}", hostname::get().unwrap_or_default().to_string_lossy(), config.port));
    let reg_mgr = bookkeeper_metadata::zk::registration::ZkRegistrationManager::new(
        zk_connect, &config.zk_ledger_root
    ).await.map_err(|e| format!("ZK connect: {e}"))?;
    let info = bookkeeper_metadata::service_info::BookieServiceInfo::new(
        &bookie_id, &local_addr.ip().to_string(), config.port,
    );
    reg_mgr.register_bookie(&bookie_id, false, &info).await
        .map_err(|e| format!("ZK register: {e}"))?;
}
```

Store the registration manager in the BookieServer struct for use during shutdown.

- [ ] **Step 4: Unregister on shutdown**

In `shutdown()`, if ZK is configured, unregister before stopping.

- [ ] **Step 5: Verify compilation**

Run: `cargo check --workspace`
Expected: Compiles

Run: `cargo test --workspace`
Expected: All existing tests pass (ZK tests are #[ignore])

- [ ] **Step 6: Commit**

```bash
git commit -m "feat: integrate ZK registration into BookieServer startup and shutdown

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Spec Coverage

| Spec Section | Task | Notes |
|---|---|---|
| §1.1 Startup registration | Task 2 (register_bookie) + Task 3 | Ephemeral znode with ServiceInfo |
| §1.2 Ephemeral znodes | Task 2 | CreateMode::Ephemeral |
| §1.3 Re-registration | Deferred | Requires ZK session watcher |
| §1.4 Read-only transition | Task 2 (unregister + re-register) | Via server state transitions |
| §2 BookieServiceInfo | Task 1 | Protobuf serialization |
| §3 RegistrationManager | Task 1 (trait) + Task 2 (ZK impl) | Full interface |
| §4 Discovery | Task 2 (get_writable/readonly_bookies) | Watchers deferred to client phase |
| §5 Cookie validation | Task 2 (read/create/update/delete_cookie) | Server-side cookie ops |
| §6 Layout manager | Task 2 (ZkLayoutManager) | Read/store/delete layout |
| §7 Instance ID | Task 2 (get_cluster_instance_id) | Via ZK znode |
| §8.1 initNewCluster | Task 2 (init_new_cluster) | Creates all root paths + layout + instance ID |

## What's Next

| Phase | Scope | Status |
|-------|-------|--------|
| Phase 1 | Foundation + Wire Protocol | Complete |
| Phase 2 | Metadata | Complete |
| Phase 3 | Storage Engine | Complete |
| Phase 4 | Bookie Server | Complete |
| Phase 5 | Bookie Registration | This plan |
| Phase 6 | Client Library | Next |
