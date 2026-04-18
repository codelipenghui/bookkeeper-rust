# Oxia Metadata Backend — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add oxia as a second metadata backend alongside ZooKeeper, wire-compatible with Apache Pulsar's `pulsar-metadata/bookkeeper` driver so a bookkeeper-rust bookie can join a Pulsar+oxia cluster.

**Architecture:** Three ordered phases. Phase 1 widens the CAS version field from `i32` to `i64` across trait layer and consumers. Phase 2 introduces a `MetadataBackend` factory that hides backend-selection behind the metadata-service URI scheme; all existing ZK call sites migrate to it. Phase 3 adds the oxia backend (`liboxia 0.0.2`), sharing the hierarchical ledger-path helpers with the ZK backend and porting the HOB ledger-ID generator bit-for-bit from Pulsar.

**Tech Stack:** Rust 2021, tokio, `zookeeper-client` (existing), `liboxia = "0.0.2"` (new), `testcontainers-oxia = "0.0.1"` (dev).

**Spec:** [`docs/superpowers/specs/2026-04-17-oxia-metadata-backend-design.md`](../specs/2026-04-17-oxia-metadata-backend-design.md)

---

# Phase 1 — i64 CAS version widening

**Why first:** Neutral refactor that compiles and passes all tests unchanged on completion. No behavioural change. Unblocks oxia in Phase 3 (oxia's `version_id` is `i64` and must not be truncated).

**Scope — sites to touch:**
- `crates/bookkeeper-metadata/src/versioned.rs` — field + getters
- `crates/bookkeeper-metadata/src/traits.rs` — `write_ledger_metadata`, `read_cookie`, `update_cookie`, `delete_cookie` version params
- `crates/bookkeeper-metadata/src/zk/ledger_manager.rs` — cast `stat.version` to i64, convert back to i32 when calling `set_data`/`delete`
- `crates/bookkeeper-metadata/src/zk/registration.rs` — cookie `stat.version` cast, same conversion on write/delete
- `crates/bookkeeper-metadata/src/iterator.rs` — mock impl signatures
- `crates/bookkeeper-server/src/datainteg/check.rs` — `Versioned<LedgerMetadata>` users
- `crates/bookkeeper-server/src/datainteg/test_helpers.rs` — mock impl
- `crates/bookkeeper-server/src/datainteg/cookie_validator.rs` — cookie version
- `crates/bookkeeper-server/src/datainteg/recovery.rs` — `Versioned` returns
- `crates/bookkeeper-client/src/zk_client.rs` — `versioned.version()` callers
- `crates/bookkeeper-cli/src/admin.rs` — `versioned.version()` callers (3 sites)
- `crates/bookkeeper-replication/src/underreplication.rs` — if any

**Sites to NOT touch (these i32s are format versions, not CAS versions):**
- `cookie.rs` — cookie binary-format version number
- `metadata_serde.rs` — ledger-metadata format version
- `traits.rs::LedgerLayout::manager_version` — layout format version
- `storage/journal/*`, `storage/entry_log/*` — on-disk format versions

## Task 1.1: Widen `Versioned<T>::version` to `i64`

**Files:**
- Modify: `crates/bookkeeper-metadata/src/versioned.rs`

- [ ] **Step 1: Edit `versioned.rs` to use `i64`**

Replace the whole file with:

```rust
/// A value paired with a version for optimistic concurrency control.
/// Version semantics are backend-specific — for ZK it is the znode
/// `stat.version` (widened from i32 on read); for oxia it is
/// `Version.version_id`. Both fit in i64.
#[derive(Debug, Clone)]
pub struct Versioned<T> {
    value: T,
    version: i64,
}

impl<T> Versioned<T> {
    pub fn new(value: T, version: i64) -> Self {
        Self { value, version }
    }

    pub fn value(&self) -> &T { &self.value }
    pub fn into_value(self) -> T { self.value }
    pub fn version(&self) -> i64 { self.version }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Versioned<U> {
        Versioned { value: f(self.value), version: self.version }
    }
}
```

- [ ] **Step 2: Run `cargo check -p bookkeeper-metadata` to see call-site errors**

Run: `cd /Users/lipenghui/Github/sn/bookkeeper-rust && cargo check -p bookkeeper-metadata`
Expected: errors referencing `i32` vs `i64` at trait signatures and ZK implementations. Use the list to drive the next tasks.

- [ ] **Step 3: Do not commit yet** — code does not compile. Proceed to 1.2.

## Task 1.2: Widen trait signatures

**Files:**
- Modify: `crates/bookkeeper-metadata/src/traits.rs`

- [ ] **Step 1: Change `LedgerManager::write_ledger_metadata` parameter**

In `traits.rs`, change `current_version: i32` to `current_version: i64`:

```rust
    async fn write_ledger_metadata(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
        current_version: i64,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError>;
```

Change `remove_ledger_metadata` the same way:

```rust
    async fn remove_ledger_metadata(
        &self,
        ledger_id: i64,
        version: i64,
    ) -> Result<(), MetadataError>;
```

- [ ] **Step 2: Change `RegistrationManager` cookie methods**

In the same file, change:

```rust
    async fn read_cookie(&self, bookie_id: &str) -> Result<Option<(Vec<u8>, i64)>, MetadataError>;
    async fn update_cookie(&self, bookie_id: &str, data: &[u8], version: i64) -> Result<(), MetadataError>;
    async fn delete_cookie(&self, bookie_id: &str, version: i64) -> Result<(), MetadataError>;
```

- [ ] **Step 3: Do not commit yet** — do not run tests; compilation still fails. Proceed to 1.3.

## Task 1.3: Fix ZK `LedgerManager` implementation

**Files:**
- Modify: `crates/bookkeeper-metadata/src/zk/ledger_manager.rs`

- [ ] **Step 1: Cast ZK `stat.version` to i64 on read**

Find the two `Versioned::new(..., stat.version)` call sites (in `read_ledger_metadata` and `write_ledger_metadata` — search for `stat.version`). Change to `Versioned::new(..., stat.version as i64)`.

In `create_ledger_metadata`, the current call is `Versioned::new(metadata.clone(), 0)` — change to `Versioned::new(metadata.clone(), 0i64)`.

- [ ] **Step 2: Convert i64 back to i32 when calling ZK**

In `write_ledger_metadata`, the signature now takes `current_version: i64`. ZK's `set_data` wants `Option<i32>`:

```rust
        let zk_version = i32::try_from(current_version)
            .map_err(|_| MetadataError::VersionConflict {
                expected: current_version,
                actual: -1,
            })?;
        let stat = self
            .client
            .set_data(&path, &data, Some(zk_version))
            .await
            .map_err(|e| match e {
                zk::Error::BadVersion => MetadataError::VersionConflict {
                    expected: current_version,
                    actual: -1,
                },
                zk::Error::NoNode => MetadataError::NotFound(ledger_id),
                other => MetadataError::ZkError(format!("write ledger {ledger_id}: {other}")),
            })?;
```

- [ ] **Step 3: Same conversion in `remove_ledger_metadata`**

In `remove_ledger_metadata`, convert `version: i64` to `Option<i32>` for `delete_with_parent_cleanup`:

```rust
        let zk_version = i32::try_from(version)
            .map_err(|_| MetadataError::VersionConflict {
                expected: version,
                actual: -1,
            })?;
        self.delete_with_parent_cleanup(&path, Some(zk_version))
            .await
            .map_err(...)
```

- [ ] **Step 4: Widen `MetadataError::VersionConflict` fields if needed**

Check `crates/bookkeeper-metadata/src/ledger_metadata.rs` for the `VersionConflict` variant. If `expected: i32`, widen to `i64`; `actual` stays `i64` (it's already used as `-1` placeholder in the ZK code).

- [ ] **Step 5: Do not commit yet** — ZK builds but the registration manager breaks. Proceed to 1.4.

## Task 1.4: Fix ZK `RegistrationManager` cookie version

**Files:**
- Modify: `crates/bookkeeper-metadata/src/zk/registration.rs`

- [ ] **Step 1: Widen `read_cookie` return**

Change the `Ok((data, stat.version))` to `Ok((data, stat.version as i64))` — the trait now returns i64.

- [ ] **Step 2: Narrow i64 to i32 in `update_cookie` and `delete_cookie`**

In `update_cookie`:

```rust
    async fn update_cookie(&self, bookie_id: &str, data: &[u8], version: i64) -> Result<(), MetadataError> {
        let zk_version = i32::try_from(version)
            .map_err(|_| MetadataError::VersionConflict {
                expected: version,
                actual: -1,
            })?;
        let path = self.paths.cookie_path(bookie_id);
        self.client.set_data(&path, data, Some(zk_version)).await
            .map_err(|e| MetadataError::ConnectionError(format!("update cookie: {e}")))?;
        Ok(())
    }
```

Same pattern for `delete_cookie`.

- [ ] **Step 3: Compile-check**

Run: `cargo check -p bookkeeper-metadata`
Expected: compiles cleanly, or only remaining errors are in `iterator.rs` / tests.

- [ ] **Step 4: Fix `iterator.rs` mock impls**

Search for `_current_version: i32` and `_version: i32` in `iterator.rs` — change both to `i64`.

- [ ] **Step 5: Run `bookkeeper-metadata` unit tests**

Run: `cargo test -p bookkeeper-metadata --lib`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add crates/bookkeeper-metadata
git commit -m "metadata: widen Versioned version from i32 to i64

Prepares for the oxia backend where version_id is i64 natively.
ZK stat.version (i32) widens losslessly on read; writes/deletes
narrow back to i32 for the zookeeper-client API, returning
VersionConflict on the impossible-today overflow case."
```

## Task 1.5: Fix server-side `Versioned` consumers

**Files:**
- Modify: `crates/bookkeeper-server/src/datainteg/check.rs`
- Modify: `crates/bookkeeper-server/src/datainteg/test_helpers.rs`
- Modify: `crates/bookkeeper-server/src/datainteg/cookie_validator.rs`
- Modify: `crates/bookkeeper-server/src/datainteg/recovery.rs`

- [ ] **Step 1: Run `cargo check -p bookkeeper-server`**

Run: `cargo check -p bookkeeper-server`
Expected: mismatched type errors at the four files above.

- [ ] **Step 2: Widen `datainteg/test_helpers.rs` mock impl**

In every `impl LedgerManager for ...` and `impl RegistrationManager for ...` in this file, change the trait-method parameter types from `i32` to `i64` to match the updated traits. Also widen any `Versioned::new(..., 0)` → `Versioned::new(..., 0i64)` calls.

- [ ] **Step 3: Widen `datainteg/cookie_validator.rs`**

Change the test helper `with_cookie(instance_id: &str, data: &[u8], version: i32)` to take `version: i64`. Change the field `version: i32` in `FakeRegMgr` (search for it) to `i64`. Adjust callers that pass literal `0` / `1` — they're fine as-is since integer literals coerce.

- [ ] **Step 4: Widen `datainteg/check.rs` and `datainteg/recovery.rs`**

No signature changes expected here — they pass `Versioned::version()` through; only widen explicit `i32` type annotations if `cargo check` flags them.

- [ ] **Step 5: Run the full server test suite**

Run: `cargo test -p bookkeeper-server --lib`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add crates/bookkeeper-server
git commit -m "server: follow i64 metadata version widening"
```

## Task 1.6: Fix client + CLI + replication `Versioned` consumers

**Files:**
- Modify: `crates/bookkeeper-client/src/zk_client.rs`
- Modify: `crates/bookkeeper-cli/src/admin.rs`
- Modify: `crates/bookkeeper-replication/src/underreplication.rs` (only if `cargo check` flags it)

- [ ] **Step 1: Run `cargo check --workspace`**

Run: `cargo check --workspace`
Expected: remaining errors in client/cli/replication only.

- [ ] **Step 2: Fix client**

In `bookkeeper-client/src/zk_client.rs`, the call `remove_ledger_metadata(ledger_id, versioned.version())` already returns i64 from `version()` — should compile once the trait signature has been updated. If a nearby `as i32` cast exists, remove it.

- [ ] **Step 3: Fix CLI**

In `cli/admin.rs`, the three call sites at lines 797, 882, 1573 pass `versioned.version()` (now i64) to `write_ledger_metadata` (now takes i64). Should compile with no edit. The `println!("Ledger {} (version {})", ledger_id, versioned.version())` at line 107 also compiles unchanged.

- [ ] **Step 4: Compile + test**

Run: `cargo check --workspace`
Expected: clean.

Run: `cargo test --workspace --lib`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates
git commit -m "client,cli,replication: follow i64 metadata version widening"
```

---

# Phase 2 — `MetadataBackend` factory + shared path helpers

**Why:** Phase 3 needs a single place to instantiate backends; leaving direct `ZkLedgerManager::new(...)` calls scattered across the codebase would force every call site to grow a `match` over URI scheme. The factory is also how we preserve the "raw ZK client escape hatch" some http_api endpoints need.

Keep this phase **ZK-only** — no oxia references yet. Every test that passed before Phase 2 must still pass.

## Task 2.1: Extract `metadata_paths.rs` from `zk_path.rs`

**Files:**
- Create: `crates/bookkeeper-metadata/src/metadata_paths.rs`
- Modify: `crates/bookkeeper-metadata/src/zk_path.rs`
- Modify: `crates/bookkeeper-metadata/src/lib.rs`

**Rationale:** The `ledger_path`, `underreplication_ledger_path`, and `underreplication_lock_path` helpers produce hierarchical paths that are identical under both backends (Pulsar uses the same format on oxia). Separate them from the ZK-specific `ensure_zk_path` / `ZkPaths` helpers.

- [ ] **Step 1: Create `metadata_paths.rs`**

Content:

```rust
//! Path helpers shared by every metadata backend. The paths produced
//! here are the same Apache Pulsar uses in its `pulsar-metadata`
//! driver, so they are wire-compatible with a Pulsar-managed oxia
//! metadata store.

/// Compute the path for a ledger ID using the hybrid hierarchical scheme.
/// For `ledger_id < i32::MAX` (2^31 - 1): 3-level (short) hierarchy with 2-4-4 split.
/// For `ledger_id >= i32::MAX`: 5-level (long) hierarchy with 3-4-4-4-4 split.
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

/// Compute the path for an underreplicated ledger marker.
/// Uses 4-level hex hierarchy from 64-bit ledger ID.
/// Ledger ID in the leaf name is zero-padded to 10 digits (%010d), matching Java.
pub fn underreplication_ledger_path(base: &str, ledger_id: i64) -> String {
    let id = ledger_id as u64;
    format!(
        "{}/{:04x}/{:04x}/{:04x}/{:04x}/urL{:010}",
        base,
        (id >> 48) & 0xFFFF,
        (id >> 32) & 0xFFFF,
        (id >> 16) & 0xFFFF,
        id & 0xFFFF,
        ledger_id,
    )
}

/// Compute the lock path for an underreplicated ledger.
/// Lock path uses flat structure: `{base}/urL{%010d}`.
pub fn underreplication_lock_path(base: &str, ledger_id: i64) -> String {
    format!("{}/urL{:010}", base, ledger_id)
}

/// Well-known sub-paths below the ledger root. Pure strings — no
/// backend-specific semantics. Both the ZK and oxia backends use the
/// same names to stay wire-compatible with Apache Pulsar.
#[derive(Clone, Debug)]
pub struct LedgerPaths {
    root: String,
    available: String,
    available_readonly: String,
    cookies: String,
    layout: String,
    instance_id: String,
    idgen: String,
    idgen_long: String,
    underreplication: String,
}

impl LedgerPaths {
    pub fn new(root: &str) -> Self {
        Self {
            available: format!("{}/available", root),
            available_readonly: format!("{}/available/readonly", root),
            cookies: format!("{}/cookies", root),
            layout: format!("{}/LAYOUT", root),
            instance_id: format!("{}/INSTANCEID", root),
            idgen: format!("{}/idgen", root),
            idgen_long: format!("{}/idgen-long", root),
            underreplication: format!("{}/underreplication", root),
            root: root.to_string(),
        }
    }

    pub fn root(&self) -> &str { &self.root }
    pub fn available(&self) -> &str { &self.available }
    pub fn available_readonly(&self) -> &str { &self.available_readonly }
    pub fn cookies(&self) -> &str { &self.cookies }
    pub fn layout(&self) -> &str { &self.layout }
    pub fn instance_id(&self) -> &str { &self.instance_id }
    pub fn idgen(&self) -> &str { &self.idgen }
    pub fn idgen_long(&self) -> &str { &self.idgen_long }
    pub fn underreplication(&self) -> &str { &self.underreplication }

    pub fn cookie_path(&self, bookie_id: &str) -> String {
        format!("{}/{}", self.cookies, bookie_id)
    }
    pub fn bookie_registration(&self, bookie_id: &str) -> String {
        format!("{}/{}", self.available, bookie_id)
    }
    pub fn bookie_readonly_registration(&self, bookie_id: &str) -> String {
        format!("{}/{}", self.available_readonly, bookie_id)
    }
}
```

- [ ] **Step 2: Strip the extracted code from `zk_path.rs`**

Edit `crates/bookkeeper-metadata/src/zk_path.rs` to keep ONLY:
- `ensure_zk_path` / `ensure_zk_parents` (ZK-specific — use `zk::CreateMode::Persistent`)
- A `ZkPaths` type alias: `pub type ZkPaths = crate::metadata_paths::LedgerPaths;` so existing callers compile.
- A re-export `pub use crate::metadata_paths::{ledger_path, underreplication_ledger_path, underreplication_lock_path};` so existing `crate::zk_path::ledger_path(...)` callers compile.

The full new content of `zk_path.rs`:

```rust
// ZK-specific path helpers. Generic (backend-agnostic) path helpers
// live in `crate::metadata_paths`.

pub use crate::metadata_paths::{ledger_path, underreplication_ledger_path, underreplication_lock_path};

/// Alias kept for callers that still spell it `ZkPaths`. New code
/// should use `metadata_paths::LedgerPaths` directly.
pub type ZkPaths = crate::metadata_paths::LedgerPaths;

/// Create all ZK znodes along a path (like `mkdir -p`), including the leaf.
/// Ignores `NodeExists` errors.
pub async fn ensure_zk_path(
    client: &zookeeper_client::Client,
    path: &str,
) -> Result<(), crate::ledger_metadata::MetadataError> {
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    let mut current = String::new();
    let options = zookeeper_client::CreateMode::Persistent
        .with_acls(zookeeper_client::Acls::anyone_all());
    for part in &parts {
        current.push('/');
        current.push_str(part);
        match client.create(&current, &[], &options).await {
            Ok(_) | Err(zookeeper_client::Error::NodeExists) => {}
            Err(e) => {
                return Err(crate::ledger_metadata::MetadataError::ZkError(format!(
                    "ensure {current}: {e}"
                )));
            }
        }
    }
    Ok(())
}

/// Create parent ZK znodes for a path (everything except the leaf).
pub async fn ensure_zk_parents(
    client: &zookeeper_client::Client,
    path: &str,
) -> Result<(), crate::ledger_metadata::MetadataError> {
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    let mut current = String::new();
    let options = zookeeper_client::CreateMode::Persistent
        .with_acls(zookeeper_client::Acls::anyone_all());
    for (i, part) in parts.iter().enumerate() {
        current.push('/');
        current.push_str(part);
        if i < parts.len() - 1 {
            match client.create(&current, &[], &options).await {
                Ok(_) | Err(zookeeper_client::Error::NodeExists) => {}
                Err(e) => {
                    return Err(crate::ledger_metadata::MetadataError::ZkError(format!(
                        "ensure {current}: {e}"
                    )));
                }
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 3: Add `pub mod metadata_paths;` to `lib.rs`**

Edit `crates/bookkeeper-metadata/src/lib.rs` and add the line between the existing modules (keep alphabetical order near `ledger_metadata`).

- [ ] **Step 4: Compile + test**

Run: `cargo test -p bookkeeper-metadata`
Expected: all pass (only the file layout changed; behaviour identical).

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-metadata
git commit -m "metadata: extract backend-neutral path helpers into metadata_paths

Same hierarchical paths the oxia backend in phase 3 will need.
ZkPaths becomes an alias for the shared LedgerPaths so every
existing caller compiles unchanged."
```

## Task 2.2: Introduce `MetadataBackend` and `open_metadata_backend`

**Files:**
- Create: `crates/bookkeeper-metadata/src/backend.rs`
- Modify: `crates/bookkeeper-metadata/src/lib.rs`

- [ ] **Step 1: Define the `ParsedMetadataUri` type**

Edit `crates/bookkeeper-metadata/src/metadata_paths.rs` — add at the bottom:

```rust
/// The parsed form of a `metadataServiceUri` — a sum type over the
/// supported backends. Produced by the parser in the config crate;
/// consumed by `crate::backend::open_metadata_backend`.
#[derive(Debug, Clone, PartialEq)]
pub enum ParsedMetadataUri {
    Zk { servers: String, ledgers_root: String },
    Oxia { service_addr: String, namespace: String, ledgers_root: String },
}

impl ParsedMetadataUri {
    pub fn ledgers_root(&self) -> &str {
        match self {
            Self::Zk { ledgers_root, .. } | Self::Oxia { ledgers_root, .. } => ledgers_root,
        }
    }
}
```

- [ ] **Step 2: Write the failing test for the factory wiring**

Create `crates/bookkeeper-metadata/tests/backend_dispatch_test.rs`:

```rust
use bookkeeper_metadata::backend::open_metadata_backend;
use bookkeeper_metadata::metadata_paths::ParsedMetadataUri;

#[tokio::test]
async fn unknown_scheme_returns_invalid_config() {
    let parsed = ParsedMetadataUri::Zk {
        servers: "does-not-resolve.example:2181".into(),
        ledgers_root: "/ledgers".into(),
    };
    // Connect will fail (no ZK running). We just want to ensure the
    // function routes to the ZK backend and surfaces a ConnectionError —
    // not a panic, not InvalidConfig.
    let res = open_metadata_backend(&parsed, None, None).await;
    assert!(matches!(
        res,
        Err(bookkeeper_metadata::ledger_metadata::MetadataError::ConnectionError(_))
    ));
}
```

- [ ] **Step 3: Run the test to verify it fails at compile time**

Run: `cargo test -p bookkeeper-metadata --test backend_dispatch_test`
Expected: build error — `backend` module not found.

- [ ] **Step 4: Create `backend.rs` skeleton**

Content:

```rust
//! Backend-agnostic metadata factory. Dispatch happens here so the
//! rest of the codebase stays free of `match uri.scheme { ... }`.

use std::sync::Arc;
use std::time::Duration;

use crate::ledger_metadata::MetadataError;
use crate::metadata_paths::{LedgerPaths, ParsedMetadataUri};
use crate::traits::{LayoutManager, LedgerIdGenerator, LedgerManager, RegistrationManager};

/// Raw handles the ZK backend exposes to callers that still reach
/// into the native client. `None` for non-ZK backends; consumers
/// that need these must either fall back to trait operations or
/// return NotSupported.
pub struct ZkRawHandles {
    pub client: zookeeper_client::Client,
    pub paths: LedgerPaths,
    /// The dedicated runtime for heartbeat isolation, kept alive for
    /// the backend's lifetime.
    pub _runtime: Option<tokio::runtime::Runtime>,
}

/// A fully-constructed metadata backend: all four managers plus
/// optional backend-specific escape hatches.
pub struct MetadataBackend {
    pub ledger_manager: Arc<dyn LedgerManager>,
    pub id_generator: Arc<dyn LedgerIdGenerator>,
    pub layout_manager: Arc<dyn LayoutManager>,
    pub registration_manager: Arc<dyn RegistrationManager>,
    pub paths: LedgerPaths,
    pub zk_raw: Option<ZkRawHandles>,
}

/// Open the metadata backend identified by `uri`.
///
/// `runtime` — ZK-only: if `Some`, the ZK client is constructed on
/// that runtime so its internal heartbeat task isn't starved by RPC
/// load on the main runtime. `None` means construct on the caller's
/// current runtime. Ignored for oxia.
///
/// `session_timeout` — applies to both backends; `None` uses the
/// backend default (30s for ZK, 30s for oxia).
pub async fn open_metadata_backend(
    uri: &ParsedMetadataUri,
    runtime: Option<tokio::runtime::Handle>,
    session_timeout: Option<Duration>,
) -> Result<MetadataBackend, MetadataError> {
    match uri {
        ParsedMetadataUri::Zk { servers, ledgers_root } => {
            open_zk_backend(servers, ledgers_root, runtime, session_timeout).await
        }
        ParsedMetadataUri::Oxia { .. } => {
            // Filled in by Phase 3.
            Err(MetadataError::InvalidConfig(
                "oxia backend not yet compiled in; enable the `oxia` feature".into(),
            ))
        }
    }
}

async fn open_zk_backend(
    servers: &str,
    ledgers_root: &str,
    runtime: Option<tokio::runtime::Handle>,
    session_timeout: Option<Duration>,
) -> Result<MetadataBackend, MetadataError> {
    let reg = crate::zk::registration::ZkRegistrationManager::new_isolated(
        servers,
        ledgers_root,
        runtime.clone(),
        session_timeout,
    )
    .await?;
    let client = reg.zk_client().clone();
    let paths = LedgerPaths::new(ledgers_root);
    let ledger_manager = Arc::new(
        crate::zk::ledger_manager::ZkLedgerManager::new(client.clone(), paths.clone()),
    );
    let id_generator = Arc::new(
        crate::zk::idgen::ZkLedgerIdGenerator::new(client.clone(), &paths).await?,
    );
    let layout_manager = Arc::new(
        crate::zk::layout::ZkLayoutManager::new(client.clone(), paths.clone()),
    );
    let registration_manager = Arc::new(reg);
    Ok(MetadataBackend {
        ledger_manager,
        id_generator,
        layout_manager,
        registration_manager,
        paths: paths.clone(),
        zk_raw: Some(ZkRawHandles {
            client,
            paths,
            _runtime: None, // the caller owns the runtime
        }),
    })
}
```

- [ ] **Step 5: Add `pub mod backend;` to `lib.rs`**

- [ ] **Step 6: Run the test**

Run: `cargo test -p bookkeeper-metadata --test backend_dispatch_test`
Expected: PASS (fails on connect → `ConnectionError`, which is what the test expects).

- [ ] **Step 7: Commit**

```bash
git add crates/bookkeeper-metadata
git commit -m "metadata: add MetadataBackend factory (ZK wired; oxia stub)

Single entry point open_metadata_backend(uri, runtime, session_timeout)
returns all four managers as trait objects. Phase 3 fills in the oxia
arm; today it returns InvalidConfig."
```

## Task 2.3: Parse `metadata_service_uri` into `ParsedMetadataUri`

**Files:**
- Modify: `crates/bookkeeper-config/src/server.rs` — `parse_metadata_service_uri`
- Modify: `crates/bookkeeper-config/tests/config_test.rs` — add oxia-URI parse test
- Modify: `crates/bookkeeper-config/Cargo.toml` — if not already a dep, add `bookkeeper-metadata = { workspace = true }` (so the config crate can return `ParsedMetadataUri`)

- [ ] **Step 1: Check current dep graph**

Run: `cargo tree -p bookkeeper-config --depth 1 | grep bookkeeper-metadata`
If it shows the dep: nothing to do. If not: add to `Cargo.toml` under `[dependencies]`:

```toml
bookkeeper-metadata = { workspace = true }
```

- [ ] **Step 2: Write the failing test**

Add to `crates/bookkeeper-config/tests/config_test.rs`:

```rust
use bookkeeper_metadata::metadata_paths::ParsedMetadataUri;
use bookkeeper_config::parse_metadata_service_uri;

#[test]
fn parses_zk_hierarchical_uri() {
    let got = parse_metadata_service_uri("zk+hierarchical://host1:2181,host2:2181/ledgers")
        .expect("recognised");
    assert_eq!(
        got,
        ParsedMetadataUri::Zk {
            servers: "host1:2181,host2:2181".into(),
            ledgers_root: "/ledgers".into(),
        }
    );
}

#[test]
fn parses_plain_oxia_uri_with_default_root() {
    let got = parse_metadata_service_uri("oxia://host:6648/mynamespace")
        .expect("recognised");
    assert_eq!(
        got,
        ParsedMetadataUri::Oxia {
            service_addr: "host:6648".into(),
            namespace: "mynamespace".into(),
            ledgers_root: "/ledgers".into(),
        }
    );
}

#[test]
fn parses_oxia_uri_with_custom_ledgers_root() {
    let got = parse_metadata_service_uri("oxia://host:6648/ns/app/bk")
        .expect("recognised");
    assert_eq!(
        got,
        ParsedMetadataUri::Oxia {
            service_addr: "host:6648".into(),
            namespace: "ns".into(),
            ledgers_root: "/app/bk".into(),
        }
    );
}

#[test]
fn parses_metadata_store_oxia_uri() {
    let got = parse_metadata_service_uri("metadata-store:oxia://host:6648/ns")
        .expect("recognised");
    assert_eq!(
        got,
        ParsedMetadataUri::Oxia {
            service_addr: "host:6648".into(),
            namespace: "ns".into(),
            ledgers_root: "/ledgers".into(),
        }
    );
}
```

- [ ] **Step 3: Run — expect compile failure**

Run: `cargo test -p bookkeeper-config --test config_test`
Expected: `parse_metadata_service_uri` does not return the new type.

- [ ] **Step 4: Rewrite `parse_metadata_service_uri`**

Replace the existing `fn parse_metadata_service_uri(uri: &str) -> Option<String>` in `crates/bookkeeper-config/src/server.rs` with:

```rust
pub fn parse_metadata_service_uri(
    uri: &str,
) -> Option<bookkeeper_metadata::metadata_paths::ParsedMetadataUri> {
    use bookkeeper_metadata::metadata_paths::ParsedMetadataUri;

    // Strip the optional "metadata-store:" prefix used by Pulsar.
    let uri = uri.strip_prefix("metadata-store:").unwrap_or(uri);

    if let Some(rest) = uri
        .strip_prefix("zk+hierarchical://")
        .or_else(|| uri.strip_prefix("zk+flat://"))
        .or_else(|| uri.strip_prefix("zk://"))
    {
        // <host:port[,...]>[/ledgers-root]
        let (servers, ledgers_root) = match rest.split_once('/') {
            Some((hosts, path)) if !path.is_empty() => {
                (hosts.to_string(), format!("/{path}"))
            }
            Some((hosts, _)) => (hosts.to_string(), "/ledgers".to_string()),
            None => (rest.to_string(), "/ledgers".to_string()),
        };
        if servers.is_empty() {
            return None;
        }
        return Some(ParsedMetadataUri::Zk { servers, ledgers_root });
    }

    if let Some(rest) = uri.strip_prefix("oxia://") {
        // <host:port[,...]>/<namespace>[/<ledgers-root>]
        let (addr, tail) = rest.split_once('/')?;
        if addr.is_empty() {
            return None;
        }
        let (namespace, ledgers_root) = match tail.split_once('/') {
            Some((ns, root)) if !ns.is_empty() && !root.is_empty() => {
                (ns.to_string(), format!("/{root}"))
            }
            Some((ns, _)) if !ns.is_empty() => (ns.to_string(), "/ledgers".to_string()),
            None if !tail.is_empty() => (tail.to_string(), "/ledgers".to_string()),
            _ => return None,
        };
        return Some(ParsedMetadataUri::Oxia {
            service_addr: addr.to_string(),
            namespace,
            ledgers_root,
        });
    }

    None
}

/// Back-compat convenience for call sites that only wanted ZK servers.
/// Returns `None` for oxia URIs.
pub fn parse_zk_servers_from_uri(uri: &str) -> Option<String> {
    match parse_metadata_service_uri(uri)? {
        bookkeeper_metadata::metadata_paths::ParsedMetadataUri::Zk { servers, .. } => {
            Some(servers)
        }
        _ => None,
    }
}
```

Update the one internal caller in `server.rs` (inside `from_properties` around line 863-864) from `parse_metadata_service_uri(uri)` to `parse_zk_servers_from_uri(uri)` — that caller only wants the ZK host list.

Also expose the new function(s) from `crates/bookkeeper-config/src/lib.rs` with `pub use server::{parse_metadata_service_uri, parse_zk_servers_from_uri};`.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p bookkeeper-config --test config_test`
Expected: all pass.

- [ ] **Step 6: Update existing tests that used the old signature**

Search for references to `parse_metadata_service_uri` in `config_test.rs` — if any assert against `Some("host:port")` (the old `Option<String>` form), rewrite them to use `parse_zk_servers_from_uri`.

Run: `cargo test -p bookkeeper-config`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add crates/bookkeeper-config
git commit -m "config: parse metadataServiceUri into ParsedMetadataUri enum

Accepts oxia:// and metadata-store:oxia:// in addition to the
three existing zk variants. Defaults ledgers-root to /ledgers
(Pulsar's default). Legacy ZK-only callers use the
parse_zk_servers_from_uri convenience wrapper."
```

## Task 2.4: Migrate `bookkeeper-server` to the factory

**Files:**
- Modify: `crates/bookkeeper-server/src/server.rs`
- Modify: `crates/bookkeeper-server/src/http_api.rs`

- [ ] **Step 1: Add a helper to resolve a `ParsedMetadataUri` from config**

In `server.rs` near the top, add:

```rust
/// Prefer `config.metadata_service_uri` if set; otherwise synthesise a
/// ZK URI from `config.zk_servers` + `config.zk_ledger_root`.
fn resolve_metadata_uri(
    config: &bookkeeper_config::ServerConfiguration,
) -> Result<bookkeeper_metadata::metadata_paths::ParsedMetadataUri, String> {
    use bookkeeper_metadata::metadata_paths::ParsedMetadataUri;
    if let Some(ref uri) = config.metadata_service_uri {
        return bookkeeper_config::parse_metadata_service_uri(uri)
            .ok_or_else(|| format!("unrecognised metadataServiceUri: {uri}"));
    }
    let Some(ref zk_servers) = config.zk_servers else {
        return Err("metadataServiceUri or zkServers required".into());
    };
    Ok(ParsedMetadataUri::Zk {
        servers: zk_servers.clone(),
        ledgers_root: config.zk_ledger_root.clone(),
    })
}
```

- [ ] **Step 2: Rewrite the big `zk_reg = if let Some(ref zk_servers) = config.zk_servers { ... }` block around line 302**

Replace the ZK-specific construction with a call to the factory. Keep the bookie-id/advertised-host resolution code (it's backend-agnostic). The new shape:

```rust
let uri = resolve_metadata_uri(&config).map_err(|e| e)?;

// Dedicated ZK runtime (phase 3 will make this conditional on scheme).
let zk_handle = match &uri {
    bookkeeper_metadata::metadata_paths::ParsedMetadataUri::Zk { .. } => {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("zk-runtime")
            .enable_all()
            .build()
            .map_err(|e| format!("ZK runtime: {e}"))?;
        let h = rt.handle().clone();
        zk_runtime = Some(rt);
        Some(h)
    }
    _ => None,
};

let backend = bookkeeper_metadata::backend::open_metadata_backend(
    &uri,
    zk_handle,
    None, // default session timeout
)
.await
.map_err(|e| format!("metadata connect: {e}"))?;

// Bookie ID / advertised host resolution — unchanged code block.
let (advertised_host, bookie_id) = { /* existing logic */ };

data_integ_check = build_data_integrity_check(&config, &bookie_id, storage.clone()).await?;
run_cookie_validation(
    backend.registration_manager.as_ref(),
    &bookie_id,
    &config,
    data_integ_check.clone(),
)
.await?;

let info = bookkeeper_metadata::service_info::BookieServiceInfo::new(
    &bookie_id,
    &advertised_host,
    local_addr.port(),
);
backend.registration_manager
    .register_bookie(&bookie_id, false, &info)
    .await
    .map_err(|e| format!("register: {e}"))?;
```

- [ ] **Step 3: Adjust callers that expected concrete `ZkRegistrationManager`**

Search `server.rs` for `run_cookie_validation`. Its signature currently takes `&ZkRegistrationManager` — change to `&dyn RegistrationManager` (or `impl RegistrationManager + ?Sized`). Update the body if it reached into ZK specifics.

Same for `start_zk_session_watcher` (search for its name). That function reads the native ZK session ID — it's ZK-specific. Keep it gated behind `if let Some(raw) = &backend.zk_raw { start_zk_session_watcher(&raw.client, ...) }`.

- [ ] **Step 4: Update `zk_reg` field on the bookie server struct**

Before Phase 2 it stored `Option<(ZkRegistrationManager, String)>`. Replace with the factory's outputs. Minimum change: store `Option<MetadataBackend>` so `shutdown` can unregister via `backend.registration_manager.unregister_bookie(...)`.

- [ ] **Step 5: Fix the remaining direct `ZkLedgerManager::new(...)` sites in `server.rs`**

Those are inside admin handlers at lines 1360, 1475, 2093 and helper around 2149. Each one currently reopens a ZK connection for a short-lived admin task. Pass `backend.ledger_manager.clone()` into these functions instead of constructing a new one; delete the helper `fn` that reopened a ZK client.

Same treatment for `http_api.rs:667, 1127, 1226, 1288`.

- [ ] **Step 6: Compile + test**

Run: `cargo check --workspace`
Expected: clean.

Run: `cargo test -p bookkeeper-server --lib`
Expected: all pass (unit tests don't need real ZK).

- [ ] **Step 7: Run the ZK container integration test**

Run: `cargo test -p bookkeeper-server --test zk_e2e_test -- --nocapture` (or whichever test file exists — check `bookkeeper-server/tests/`).
Expected: all pass (this is the regression gate for Phase 2).

- [ ] **Step 8: Commit**

```bash
git add crates/bookkeeper-server
git commit -m "server: route all metadata through MetadataBackend factory

No behavioural change — the factory still returns ZK managers. The
ZK-specific dedicated-runtime creation and session watcher remain on
the ZK arm only; oxia (phase 3) skips them."
```

## Task 2.5: Migrate CLI + replication to the factory

**Files:**
- Modify: `crates/bookkeeper-cli/src/admin.rs`
- Modify: `crates/bookkeeper-cli/src/main.rs`
- Modify: `crates/bookkeeper-replication/src/*.rs` (files that construct ZK managers directly)

- [ ] **Step 1: Enumerate sites**

Run: `grep -rn "ZkLedgerManager\|ZkRegistrationManager\|ZkLayoutManager\|ZkLedgerIdGenerator" crates/bookkeeper-cli crates/bookkeeper-replication --include="*.rs"`

- [ ] **Step 2: Replace each construction with `open_metadata_backend`**

At each site:
1. Read config / URI from the caller.
2. Call `open_metadata_backend(&uri, None, None).await?`.
3. Use `backend.ledger_manager` / `.registration_manager` etc.

For short-lived CLI commands, returning the whole `MetadataBackend` (which drops the ZK client at end of scope) is fine.

- [ ] **Step 3: For each auditor / replication worker**

`bookkeeper-replication` has long-lived workers. Follow the same pattern as `server.rs` — create the dedicated runtime only on the ZK arm, keep the `MetadataBackend` alive for the worker lifetime.

- [ ] **Step 4: Compile + test**

Run: `cargo test --workspace`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates
git commit -m "cli,replication: route metadata through MetadataBackend factory"
```

## Task 2.6: Compile-only sanity check across the workspace

- [ ] **Step 1: Verify no direct Zk*Manager construction remains outside `bookkeeper-metadata/src/zk/`**

Run: `grep -rn "ZkRegistrationManager::new\|ZkLedgerManager::new\|ZkLayoutManager::new\|ZkLedgerIdGenerator::new" crates --include="*.rs" | grep -v "/zk/" | grep -v test`

Expected: no output. (Tests may still do direct construction — that's fine.)

- [ ] **Step 2: Workspace test**

Run: `cargo test --workspace`
Expected: clean.

- [ ] **Step 3: Commit (empty if nothing changed, skip in that case)**

---

# Phase 3 — Oxia backend

**Why:** With Phase 2 in place, adding oxia is just writing a second set of manager impls and wiring the factory's oxia arm. The rest of the codebase doesn't notice.

**Feature flag:** Gate under `oxia` (default = on). Ungated callers outside `bookkeeper-metadata` don't reference `liboxia` types.

## Task 3.1: Add `liboxia` dependency and feature gate

**Files:**
- Modify: `Cargo.toml` (workspace)
- Modify: `crates/bookkeeper-metadata/Cargo.toml`

- [ ] **Step 1: Workspace dep**

Add under `[workspace.dependencies]`:

```toml
liboxia = { version = "=0.0.2", default-features = false }
testcontainers-oxia = "0.0.1"
```

- [ ] **Step 2: Metadata crate**

Edit `crates/bookkeeper-metadata/Cargo.toml`:

```toml
[features]
default = ["oxia"]
oxia = ["dep:liboxia"]

[dependencies]
# ...existing deps...
liboxia = { workspace = true, optional = true }

[dev-dependencies]
# ...existing dev-deps...
testcontainers-oxia = { workspace = true }
```

- [ ] **Step 3: Compile-check**

Run: `cargo check -p bookkeeper-metadata`
Expected: clean.

Run: `cargo check -p bookkeeper-metadata --no-default-features`
Expected: clean — must still build without oxia.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml crates/bookkeeper-metadata/Cargo.toml Cargo.lock
git commit -m "metadata: add liboxia 0.0.2 under the oxia feature"
```

## Task 3.2: `oxia` module skeleton and `OxiaClient` wrapper

**Files:**
- Create: `crates/bookkeeper-metadata/src/oxia/mod.rs`
- Create: `crates/bookkeeper-metadata/src/oxia/client.rs`
- Create: `crates/bookkeeper-metadata/src/oxia/errors.rs`
- Modify: `crates/bookkeeper-metadata/src/lib.rs`

- [ ] **Step 1: Create `oxia/mod.rs`**

```rust
//! Oxia metadata backend. Wire-compatible with Apache Pulsar's
//! pulsar-metadata/bookkeeper driver: same keys, same CAS contract.

pub mod client;
pub mod errors;
pub mod idgen;
pub mod layout;
pub mod ledger_manager;
pub mod parents;
pub mod paths;
pub mod registration;
```

- [ ] **Step 2: Create `oxia/errors.rs`**

```rust
use liboxia::errors::OxiaError;

use crate::ledger_metadata::MetadataError;

/// Map a liboxia error into the backend-neutral MetadataError.
/// `ledger_id_hint`: `Some(id)` when the operation was ledger-scoped;
/// `None` otherwise (use 0 in the resulting NotFound).
pub fn map_oxia_error(err: OxiaError, ledger_id_hint: Option<i64>) -> MetadataError {
    match err {
        OxiaError::KeyNotFound => MetadataError::NotFound(ledger_id_hint.unwrap_or(0)),
        OxiaError::UnexpectedVersionId => MetadataError::VersionConflict {
            expected: -1,
            actual: -1,
        },
        OxiaError::SessionDoesNotExist => {
            MetadataError::ConnectionError("oxia session does not exist".into())
        }
        OxiaError::KeyLeaderNotFound => {
            MetadataError::ConnectionError("oxia leader not found (retriable)".into())
        }
        OxiaError::IllegalArgument(msg) => MetadataError::InvalidConfig(msg),
        OxiaError::UnexpectedStatus(code) => {
            MetadataError::ConnectionError(format!("oxia status {code:?}"))
        }
        other => MetadataError::ConnectionError(format!("oxia: {other:?}")),
    }
}
```

(Adjust the match arms to the actual liboxia 0.0.2 `OxiaError` variants — if a variant has a different name, use the compiler error to guide you. Do NOT invent variants; check at implementation time.)

- [ ] **Step 3: Create `oxia/client.rs`**

```rust
use std::sync::Arc;
use std::time::Duration;

use liboxia::client::OxiaClient;
use liboxia::client_options::OxiaClientOptions;

use crate::ledger_metadata::MetadataError;
use crate::metadata_paths::LedgerPaths;

/// Shared oxia client handle. Every manager below holds an `Arc<OxiaClient>`
/// so they share a single connection + session manager.
pub struct OxiaClientHandle {
    pub client: Arc<OxiaClient>,
    pub paths: LedgerPaths,
    pub identity: String,
}

impl OxiaClientHandle {
    pub async fn connect(
        service_addr: &str,
        namespace: &str,
        ledgers_root: &str,
        session_timeout: Option<Duration>,
    ) -> Result<Self, MetadataError> {
        let identity = format!("bookkeeper-rust-{}", uuid::Uuid::new_v4());
        let mut options = OxiaClientOptions::default();
        options.service_address = format!("http://{service_addr}");
        options.namespace = namespace.to_string();
        options.identity = identity.clone();
        if let Some(t) = session_timeout {
            options.session_timeout = t;
        } else {
            options.session_timeout = Duration::from_secs(30);
        }

        let client = OxiaClient::new(options)
            .await
            .map_err(|e| MetadataError::ConnectionError(format!("oxia connect: {e:?}")))?;

        Ok(Self {
            client: Arc::new(client),
            paths: LedgerPaths::new(ledgers_root),
            identity,
        })
    }
}
```

- [ ] **Step 4: Compile-check**

Add `pub mod oxia;` to `lib.rs` under `#[cfg(feature = "oxia")]`:

```rust
#[cfg(feature = "oxia")]
pub mod oxia;
```

Run: `cargo check -p bookkeeper-metadata --features oxia`
Expected: clean, possibly flagging unused imports — fix them.

- [ ] **Step 5: Commit**

```bash
git add crates/bookkeeper-metadata
git commit -m "metadata/oxia: scaffold module and client handle"
```

## Task 3.3: `oxia/paths.rs` + `oxia/parents.rs`

**Files:**
- Create: `crates/bookkeeper-metadata/src/oxia/paths.rs`
- Create: `crates/bookkeeper-metadata/src/oxia/parents.rs`

- [ ] **Step 1: Write `paths.rs` with child-listing helper and tests**

```rust
/// Build the (start, end) key bounds for listing immediate children
/// of `parent`. Matches the convention in Pulsar's
/// `OxiaMetadataStore.getChildrenFromStore`: `{parent}/` to
/// `{parent}/\u{00ff}`. We use a high-codepoint upper bound to avoid
/// false ends on keys containing `/`.
pub fn child_range(parent: &str) -> (String, String) {
    let mut start = parent.to_string();
    if !start.ends_with('/') {
        start.push('/');
    }
    let mut end = start.clone();
    end.push('\u{00ff}');
    (start, end)
}

/// Split a full key into (parent, leaf) on the last '/'. Returns
/// `None` when the key has no parent (top-level / single segment).
pub fn split_parent<'a>(key: &'a str) -> Option<(&'a str, &'a str)> {
    let idx = key.rfind('/')?;
    Some((&key[..idx], &key[idx + 1..]))
}

/// Given an absolute key, yield every non-empty ancestor in order
/// root-first, excluding the key itself.
pub fn ancestors_of(key: &str) -> Vec<String> {
    let mut acc = Vec::new();
    let trimmed = key.trim_start_matches('/');
    let mut current = String::from("/");
    let parts: Vec<&str> = trimmed.split('/').collect();
    for part in &parts[..parts.len().saturating_sub(1)] {
        if !current.ends_with('/') {
            current.push('/');
        }
        current.push_str(part);
        acc.push(current.clone());
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_range_appends_slash_and_sentinel() {
        let (lo, hi) = child_range("/ledgers/cookies");
        assert_eq!(lo, "/ledgers/cookies/");
        assert_eq!(hi, "/ledgers/cookies/\u{00ff}");
    }

    #[test]
    fn ancestors_of_full_ledger_path() {
        let got = ancestors_of("/ledgers/00/0001/L2345");
        assert_eq!(got, vec![
            "/ledgers".to_string(),
            "/ledgers/00".to_string(),
            "/ledgers/00/0001".to_string(),
        ]);
    }

    #[test]
    fn ancestors_of_top_level_is_empty() {
        assert!(ancestors_of("/ledgers").is_empty());
    }
}
```

- [ ] **Step 2: Write `parents.rs`**

```rust
use liboxia::client::{OxiaClient, PutOption};
use liboxia::errors::OxiaError;

use super::errors::map_oxia_error;
use super::paths::ancestors_of;
use crate::ledger_metadata::MetadataError;

/// Ensure every ancestor directory of `key` exists as an empty-valued
/// record. Matches `OxiaMetadataStore.createParents` in Pulsar.
///
/// Swallows `UnexpectedVersionId` (another client raced) — that
/// means the record already exists, which is fine.
pub async fn create_parents(client: &OxiaClient, key: &str) -> Result<(), MetadataError> {
    for ancestor in ancestors_of(key) {
        match client
            .put_with_options(
                ancestor.clone(),
                Vec::new(),
                vec![PutOption::ExpectedRecordNotExists()],
            )
            .await
        {
            Ok(_) => {}
            Err(OxiaError::UnexpectedVersionId) => {}
            Err(e) => return Err(map_oxia_error(e, None)),
        }
    }
    Ok(())
}
```

- [ ] **Step 3: Test + commit**

Run: `cargo test -p bookkeeper-metadata --features oxia --lib oxia::paths`
Expected: all pass.

```bash
git add crates/bookkeeper-metadata/src/oxia
git commit -m "metadata/oxia: add path helpers and createParents"
```

## Task 3.4: `OxiaLayoutManager`

**Files:**
- Create: `crates/bookkeeper-metadata/src/oxia/layout.rs`

- [ ] **Step 1: Write the manager**

```rust
use std::sync::Arc;

use async_trait::async_trait;
use liboxia::client::OxiaClient;
use liboxia::errors::OxiaError;

use super::errors::map_oxia_error;
use super::parents::create_parents;
use crate::ledger_metadata::MetadataError;
use crate::metadata_paths::LedgerPaths;
use crate::traits::{LayoutManager, LedgerLayout};

pub struct OxiaLayoutManager {
    client: Arc<OxiaClient>,
    paths: LedgerPaths,
}

impl OxiaLayoutManager {
    pub fn new(client: Arc<OxiaClient>, paths: LedgerPaths) -> Self {
        Self { client, paths }
    }
}

#[async_trait]
impl LayoutManager for OxiaLayoutManager {
    async fn read_ledger_layout(&self) -> Result<Option<LedgerLayout>, MetadataError> {
        match self.client.get(self.paths.layout().to_string()).await {
            Ok(res) => {
                let Some(bytes) = res.value else { return Ok(None) };
                let text = String::from_utf8(bytes)
                    .map_err(|e| MetadataError::SerializationError(format!("layout: {e}")))?;
                Ok(Some(LedgerLayout::deserialize(&text)?))
            }
            Err(OxiaError::KeyNotFound) => Ok(None),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn store_ledger_layout(&self, layout: &LedgerLayout) -> Result<(), MetadataError> {
        create_parents(&self.client, self.paths.layout()).await?;
        let data = layout.serialize();
        self.client
            .put(self.paths.layout().to_string(), data.into_bytes())
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        Ok(())
    }

    async fn delete_ledger_layout(&self) -> Result<(), MetadataError> {
        match self.client.delete(self.paths.layout().to_string()).await {
            Ok(_) => Ok(()),
            Err(OxiaError::KeyNotFound) => Ok(()),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }
}
```

- [ ] **Step 2: Compile-check**

Run: `cargo check -p bookkeeper-metadata --features oxia`
Expected: clean. Fix any liboxia API drift (the method signatures above are per `liboxia-native` 0.0.2 reading; check against the crate docs if needed).

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-metadata/src/oxia/layout.rs
git commit -m "metadata/oxia: add LayoutManager"
```

## Task 3.5: `OxiaRegistrationManager`

**Files:**
- Create: `crates/bookkeeper-metadata/src/oxia/registration.rs`

- [ ] **Step 1: Write the manager**

```rust
use std::sync::Arc;

use async_trait::async_trait;
use liboxia::client::{OxiaClient, PutOption, DeleteOption};
use liboxia::errors::OxiaError;

use super::errors::map_oxia_error;
use super::parents::create_parents;
use super::paths::child_range;
use crate::ledger_metadata::MetadataError;
use crate::metadata_paths::LedgerPaths;
use crate::service_info::BookieServiceInfo;
use crate::traits::{LedgerLayout, RegistrationManager};

pub struct OxiaRegistrationManager {
    client: Arc<OxiaClient>,
    paths: LedgerPaths,
}

impl OxiaRegistrationManager {
    pub fn new(client: Arc<OxiaClient>, paths: LedgerPaths) -> Self {
        Self { client, paths }
    }
}

#[async_trait]
impl RegistrationManager for OxiaRegistrationManager {
    async fn register_bookie(
        &self,
        bookie_id: &str,
        read_only: bool,
        info: &BookieServiceInfo,
    ) -> Result<(), MetadataError> {
        let key = if read_only {
            self.paths.bookie_readonly_registration(bookie_id)
        } else {
            self.paths.bookie_registration(bookie_id)
        };
        create_parents(&self.client, &key).await?;
        self.client
            .put_with_options(
                key,
                info.to_protobuf_bytes(),
                vec![PutOption::Ephemeral()],
            )
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        Ok(())
    }

    async fn unregister_bookie(
        &self,
        bookie_id: &str,
        read_only: bool,
    ) -> Result<(), MetadataError> {
        let key = if read_only {
            self.paths.bookie_readonly_registration(bookie_id)
        } else {
            self.paths.bookie_registration(bookie_id)
        };
        match self.client.delete(key).await {
            Ok(_) => Ok(()),
            Err(OxiaError::KeyNotFound) => Ok(()),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn get_writable_bookies(&self) -> Result<Vec<String>, MetadataError> {
        list_leaf_names(&self.client, self.paths.available()).await.map(|mut v| {
            // Filter the "readonly" sub-directory marker if present.
            v.retain(|n| n != "readonly");
            v
        })
    }

    async fn get_readonly_bookies(&self) -> Result<Vec<String>, MetadataError> {
        list_leaf_names(&self.client, self.paths.available_readonly()).await
    }

    async fn read_cookie(
        &self,
        bookie_id: &str,
    ) -> Result<Option<(Vec<u8>, i64)>, MetadataError> {
        let key = self.paths.cookie_path(bookie_id);
        match self.client.get(key).await {
            Ok(res) => {
                let Some(bytes) = res.value else { return Ok(None) };
                Ok(Some((bytes, res.version.version_id)))
            }
            Err(OxiaError::KeyNotFound) => Ok(None),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn create_cookie(&self, bookie_id: &str, data: &[u8]) -> Result<(), MetadataError> {
        let key = self.paths.cookie_path(bookie_id);
        create_parents(&self.client, &key).await?;
        self.client
            .put_with_options(
                key,
                data.to_vec(),
                vec![PutOption::ExpectedRecordNotExists()],
            )
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        Ok(())
    }

    async fn update_cookie(
        &self,
        bookie_id: &str,
        data: &[u8],
        version: i64,
    ) -> Result<(), MetadataError> {
        let key = self.paths.cookie_path(bookie_id);
        self.client
            .put_with_options(
                key,
                data.to_vec(),
                vec![PutOption::ExpectVersionId(version)],
            )
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        Ok(())
    }

    async fn delete_cookie(&self, bookie_id: &str, version: i64) -> Result<(), MetadataError> {
        let key = self.paths.cookie_path(bookie_id);
        self.client
            .delete_with_options(key, vec![DeleteOption::ExpectVersionId(version)])
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        Ok(())
    }

    async fn get_cluster_instance_id(&self) -> Result<Option<String>, MetadataError> {
        match self.client.get(self.paths.instance_id().to_string()).await {
            Ok(res) => {
                let Some(bytes) = res.value else { return Ok(None) };
                Ok(Some(String::from_utf8(bytes).map_err(|e| {
                    MetadataError::SerializationError(format!("instance id: {e}"))
                })?))
            }
            Err(OxiaError::KeyNotFound) => Ok(None),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn init_new_cluster(
        &self,
        layout: &LedgerLayout,
        instance_id: &str,
    ) -> Result<(), MetadataError> {
        // Create-or-fail — matches ZK impl.
        match self.client.get(self.paths.layout().to_string()).await {
            Ok(_) => {
                return Err(MetadataError::InvalidConfig(
                    "cluster already initialized (LAYOUT exists)".into(),
                ))
            }
            Err(OxiaError::KeyNotFound) => {}
            Err(e) => return Err(map_oxia_error(e, None)),
        }

        create_parents(&self.client, self.paths.layout()).await?;
        self.client
            .put(self.paths.layout().to_string(), layout.serialize().into_bytes())
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        self.client
            .put(self.paths.instance_id().to_string(), instance_id.as_bytes().to_vec())
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        Ok(())
    }
}

async fn list_leaf_names(
    client: &OxiaClient,
    parent: &str,
) -> Result<Vec<String>, MetadataError> {
    let (start, end) = child_range(parent);
    let keys = client
        .list(start.clone(), end)
        .await
        .map_err(|e| map_oxia_error(e, None))?
        .keys;
    let mut names = Vec::with_capacity(keys.len());
    for k in keys {
        let Some(rest) = k.strip_prefix(&start) else { continue };
        // Skip grand-children (keys containing further `/`).
        if rest.contains('/') {
            continue;
        }
        names.push(rest.to_string());
    }
    Ok(names)
}
```

- [ ] **Step 2: Compile-check**

Run: `cargo check -p bookkeeper-metadata --features oxia`
Expected: clean. The liboxia type names here (`DeleteOption::ExpectVersionId`, `GetResult.value`, `GetResult.version.version_id`, `ListResult.keys`) follow the 0.0.2 crate surface I read upstream; correct as needed from real compiler errors.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-metadata/src/oxia/registration.rs
git commit -m "metadata/oxia: add RegistrationManager

Ephemeral bookie registrations use liboxia's session-managed
PutOption::Ephemeral() — no dedicated tokio runtime needed on
this path."
```

## Task 3.6: `OxiaLedgerManager`

**Files:**
- Create: `crates/bookkeeper-metadata/src/oxia/ledger_manager.rs`

- [ ] **Step 1: Write the manager**

```rust
use std::sync::Arc;

use async_trait::async_trait;
use liboxia::client::{OxiaClient, DeleteOption, PutOption};
use liboxia::errors::OxiaError;

use super::errors::map_oxia_error;
use super::parents::create_parents;
use super::paths::{ancestors_of, child_range};
use crate::ledger_metadata::{LedgerMetadata, MetadataError};
use crate::metadata_paths::{ledger_path, LedgerPaths};
use crate::metadata_serde::{deserialize_metadata, serialize_metadata};
use crate::traits::LedgerManager;
use crate::versioned::Versioned;

pub struct OxiaLedgerManager {
    client: Arc<OxiaClient>,
    paths: LedgerPaths,
}

impl OxiaLedgerManager {
    pub fn new(client: Arc<OxiaClient>, paths: LedgerPaths) -> Self {
        Self { client, paths }
    }

    fn key(&self, ledger_id: i64) -> String {
        ledger_path(self.paths.root(), ledger_id)
    }
}

#[async_trait]
impl LedgerManager for OxiaLedgerManager {
    async fn create_ledger_metadata(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
        let key = self.key(ledger_id);
        create_parents(&self.client, &key).await?;
        let bytes = serialize_metadata(metadata)?;
        match self
            .client
            .put_with_options(
                key,
                bytes,
                vec![PutOption::ExpectedRecordNotExists()],
            )
            .await
        {
            Ok(res) => Ok(Versioned::new(metadata.clone(), res.version.version_id)),
            Err(OxiaError::UnexpectedVersionId) => {
                Err(MetadataError::LedgerExists(ledger_id))
            }
            Err(e) => Err(map_oxia_error(e, Some(ledger_id))),
        }
    }

    async fn read_ledger_metadata(
        &self,
        ledger_id: i64,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
        let key = self.key(ledger_id);
        match self.client.get(key).await {
            Ok(res) => {
                let bytes = res.value.ok_or(MetadataError::NotFound(ledger_id))?;
                let md = deserialize_metadata(ledger_id, &bytes)?;
                Ok(Versioned::new(md, res.version.version_id))
            }
            Err(OxiaError::KeyNotFound) => Err(MetadataError::NotFound(ledger_id)),
            Err(e) => Err(map_oxia_error(e, Some(ledger_id))),
        }
    }

    async fn write_ledger_metadata(
        &self,
        ledger_id: i64,
        metadata: &LedgerMetadata,
        current_version: i64,
    ) -> Result<Versioned<LedgerMetadata>, MetadataError> {
        let key = self.key(ledger_id);
        let bytes = serialize_metadata(metadata)?;
        match self
            .client
            .put_with_options(
                key,
                bytes,
                vec![PutOption::ExpectVersionId(current_version)],
            )
            .await
        {
            Ok(res) => Ok(Versioned::new(metadata.clone(), res.version.version_id)),
            Err(OxiaError::UnexpectedVersionId) => Err(MetadataError::VersionConflict {
                expected: current_version,
                actual: -1,
            }),
            Err(OxiaError::KeyNotFound) => Err(MetadataError::NotFound(ledger_id)),
            Err(e) => Err(map_oxia_error(e, Some(ledger_id))),
        }
    }

    async fn remove_ledger_metadata(
        &self,
        ledger_id: i64,
        version: i64,
    ) -> Result<(), MetadataError> {
        let key = self.key(ledger_id);
        match self
            .client
            .delete_with_options(
                key.clone(),
                vec![DeleteOption::ExpectVersionId(version)],
            )
            .await
        {
            Ok(_) => {
                // Best-effort ancestor cleanup — mirrors Java BK's
                // asyncDeleteFullPathOptimistic. Ignore all errors.
                for anc in ancestors_of(&key).into_iter().rev() {
                    // Stop when we reach the ledgers root.
                    if anc == self.paths.root() || !anc.starts_with(self.paths.root()) {
                        break;
                    }
                    let (start, end) = child_range(&anc);
                    let has_children = matches!(
                        self.client.list(start, end).await,
                        Ok(r) if !r.keys.is_empty()
                    );
                    if has_children {
                        break;
                    }
                    let _ = self.client.delete(anc).await;
                }
                Ok(())
            }
            Err(OxiaError::KeyNotFound) => Err(MetadataError::NotFound(ledger_id)),
            Err(OxiaError::UnexpectedVersionId) => Err(MetadataError::VersionConflict {
                expected: version,
                actual: -1,
            }),
            Err(e) => Err(map_oxia_error(e, Some(ledger_id))),
        }
    }

    async fn get_ledger_ranges(&self) -> Result<Vec<i64>, MetadataError> {
        // Range-scan over the entire ledger root and parse leaf names of
        // the form "L{NNNN}".
        let (start, end) = child_range(self.paths.root());
        let keys = self
            .client
            .list(start, end)
            .await
            .map_err(|e| map_oxia_error(e, None))?
            .keys;

        let mut ids = Vec::new();
        for key in keys {
            let Some(leaf) = key.rsplit('/').next() else { continue };
            let Some(num) = leaf.strip_prefix('L') else { continue };
            let Ok(id) = num.parse::<i64>() else { continue };
            // Reject the L-named keys that aren't actually ledger records
            // (defence-in-depth — real ones always match the
            // hierarchical path format).
            ids.push(id);
        }
        ids.sort_unstable();
        Ok(ids)
    }
}
```

Note: `list` above lists ALL keys under the root, including the internal `available/cookies/idgen/...` trees. The `strip_prefix('L')` + parse filter drops everything that isn't a ledger leaf. If this turns out to be slow in practice (tens of thousands of keys), a follow-up can narrow the scan using sharded ranges. Document as a known limitation.

- [ ] **Step 2: Compile-check**

Run: `cargo check -p bookkeeper-metadata --features oxia`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-metadata/src/oxia/ledger_manager.rs
git commit -m "metadata/oxia: add LedgerManager

Uses the shared hierarchical ledger-path helper so keys are
bit-identical to Pulsar's LegacyHierarchicalLedgerManager."
```

## Task 3.7: `OxiaLedgerIdGenerator`

**Files:**
- Create: `crates/bookkeeper-metadata/src/oxia/idgen.rs`

- [ ] **Step 1: Write the generator**

Port of Pulsar's `PulsarLedgerIdGenerator`. Key constants must match
exactly — `idgen` / `idgen-long` / `ID-` / `HOB-` / `%010d`.

```rust
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use liboxia::client::{OxiaClient, PutOption};
use liboxia::errors::OxiaError;

use super::errors::map_oxia_error;
use super::paths::child_range;
use crate::ledger_metadata::MetadataError;
use crate::metadata_paths::LedgerPaths;
use crate::traits::LedgerIdGenerator;

const SHORT_ID_PREFIX: &str = "ID-";
const HOB_PREFIX: &str = "HOB-";
const HOB_GC_KEEP: usize = 3;

pub struct OxiaLedgerIdGenerator {
    client: Arc<OxiaClient>,
    short_gen_dir: String,   // "{root}/idgen"
    long_gen_dir: String,    // "{root}/idgen-long"
    long_path_known: Mutex<Option<bool>>,
}

impl OxiaLedgerIdGenerator {
    pub fn new(client: Arc<OxiaClient>, paths: &LedgerPaths) -> Self {
        Self {
            client,
            short_gen_dir: paths.idgen().to_string(),
            long_gen_dir: paths.idgen_long().to_string(),
            long_path_known: Mutex::new(None),
        }
    }

    async fn ensure_path(&self, path: &str) -> Result<(), MetadataError> {
        match self
            .client
            .put_with_options(
                path.to_string(),
                Vec::new(),
                vec![PutOption::ExpectedRecordNotExists()],
            )
            .await
        {
            Ok(_) | Err(OxiaError::UnexpectedVersionId) => Ok(()),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }

    async fn is_long_present(&self) -> Result<bool, MetadataError> {
        if let Some(v) = *self.long_path_known.lock().unwrap() {
            return Ok(v);
        }
        let exists = match self.client.get(self.long_gen_dir.clone()).await {
            Ok(_) => true,
            Err(OxiaError::KeyNotFound) => false,
            Err(e) => return Err(map_oxia_error(e, None)),
        };
        *self.long_path_known.lock().unwrap() = Some(exists);
        Ok(exists)
    }

    /// Do one sequential-create under `parent`: put an empty value
    /// under `parent` itself, read the resulting `modifications_count`,
    /// and use it as the suffix on `child_prefix`. On success return
    /// the created key.
    async fn seq_create(
        &self,
        parent: &str,
        child_prefix: &str,
    ) -> Result<String, MetadataError> {
        // Pulsar's trick: incrementing the parent advances its
        // modifications_count, which is used as the suffix.
        let res = self
            .client
            .put(parent.to_string(), Vec::new())
            .await
            .map_err(|e| map_oxia_error(e, None))?;
        let suffix = res.version.modifications_count;
        let key = format!("{child_prefix}{:010}", suffix);
        // The record doesn't need to actually exist — Pulsar
        // creates+deletes it for parity with the ZK impl. We match
        // that to stay wire-compatible.
        let _ = self
            .client
            .put_with_options(
                key.clone(),
                Vec::new(),
                vec![PutOption::Ephemeral()],
            )
            .await;
        let _ = self.client.delete(key.clone()).await;
        Ok(key)
    }

    async fn generate_short(&self) -> Result<i64, MetadataError> {
        self.ensure_path(&self.short_gen_dir).await?;
        let prefix = format!("{}/{}", self.short_gen_dir, SHORT_ID_PREFIX);
        let key = self.seq_create(&self.short_gen_dir, &prefix).await?;
        let suffix = &key[prefix.len()..];
        let id: i64 = suffix
            .parse()
            .map_err(|e| MetadataError::SerializationError(format!("idgen parse: {e}")))?;
        Ok(id)
    }

    async fn list_hobs(&self) -> Result<Vec<i32>, MetadataError> {
        let (start, end) = child_range(&self.long_gen_dir);
        let keys = self
            .client
            .list(start.clone(), end)
            .await
            .map_err(|e| map_oxia_error(e, None))?
            .keys;
        let mut hobs: Vec<i32> = keys
            .into_iter()
            .filter_map(|k| {
                let leaf = k.rsplit('/').next()?;
                leaf.strip_prefix(HOB_PREFIX)?.parse::<i32>().ok()
            })
            .collect();
        hobs.sort_unstable();
        Ok(hobs)
    }

    async fn create_hob(&self, hob: i32) -> Result<String, MetadataError> {
        let path = format!("{}/{}{:010}", self.long_gen_dir, HOB_PREFIX, hob);
        self.ensure_path(&path).await?;
        Ok(path)
    }

    async fn gc_old_hobs(&self, hobs: &[i32]) {
        if hobs.len() <= HOB_GC_KEEP {
            return;
        }
        for &hob in &hobs[..hobs.len() - HOB_GC_KEEP] {
            let path = format!("{}/{}{:010}", self.long_gen_dir, HOB_PREFIX, hob);
            let _ = self.client.delete(path).await;
        }
    }

    async fn generate_long(&self) -> Result<i64, MetadataError> {
        for _ in 0..5 {
            let mut hobs = self.list_hobs().await?;
            let high_bits = match hobs.last() {
                Some(&m) => m,
                None => {
                    self.create_hob(1).await?;
                    hobs.push(1);
                    1
                }
            };
            let hob_path = format!("{}/{}{:010}", self.long_gen_dir, HOB_PREFIX, high_bits);
            let prefix = format!("{hob_path}/{}", SHORT_ID_PREFIX);
            let key = self.seq_create(&hob_path, &prefix).await?;
            let suffix: i64 = key[prefix.len()..]
                .parse()
                .map_err(|e| MetadataError::SerializationError(format!("idgen parse: {e}")))?;
            if suffix >= i32::MAX as i64 {
                // Low bits exhausted for this HOB — create next and retry.
                self.create_hob(high_bits + 1).await?;
                continue;
            }
            self.gc_old_hobs(&hobs).await;
            return Ok(((high_bits as i64) << 32) | suffix);
        }
        Err(MetadataError::ZkError(
            "long ID generation failed after retries".into(),
        ))
    }
}

#[async_trait]
impl LedgerIdGenerator for OxiaLedgerIdGenerator {
    async fn generate_ledger_id(&self) -> Result<i64, MetadataError> {
        if !self.is_long_present().await? {
            match self.generate_short().await {
                Ok(id) if id < i32::MAX as i64 => return Ok(id),
                Ok(_) => {
                    // Short space exhausted — create long path and fall through.
                    self.ensure_path(&self.long_gen_dir).await?;
                    *self.long_path_known.lock().unwrap() = Some(true);
                }
                Err(e) => return Err(e),
            }
        }
        self.generate_long().await
    }
}
```

Important: the `seq_create` helper does a no-op create+delete of the ephemeral `ID-{NNNN}` record for parity with Pulsar, since a reader that walks `idgen/` children expects to see them transiently. The monotonicity guarantee is carried by the parent's `modifications_count`, not by the record existing.

- [ ] **Step 2: Compile-check**

Run: `cargo check -p bookkeeper-metadata --features oxia`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-metadata/src/oxia/idgen.rs
git commit -m "metadata/oxia: add LedgerIdGenerator

Ports Pulsar's two-tier HOB scheme bit-for-bit. Since liboxia
0.0.2 has no native Sequential put option, we derive the
monotonic suffix from the parent's modifications_count — same
mechanism OxiaMetadataStore.doStorePut uses internally."
```

## Task 3.8: Wire the oxia backend into the factory

**Files:**
- Modify: `crates/bookkeeper-metadata/src/backend.rs`

- [ ] **Step 1: Add the oxia arm to `open_metadata_backend`**

Replace the `ParsedMetadataUri::Oxia { .. }` arm with:

```rust
        ParsedMetadataUri::Oxia { service_addr, namespace, ledgers_root } => {
            #[cfg(feature = "oxia")]
            {
                open_oxia_backend(service_addr, namespace, ledgers_root, session_timeout).await
            }
            #[cfg(not(feature = "oxia"))]
            {
                let _ = (service_addr, namespace, ledgers_root, session_timeout);
                Err(MetadataError::InvalidConfig(
                    "oxia backend requires the `oxia` cargo feature".into(),
                ))
            }
        }
```

- [ ] **Step 2: Add the `open_oxia_backend` helper**

At the bottom of `backend.rs`:

```rust
#[cfg(feature = "oxia")]
async fn open_oxia_backend(
    service_addr: &str,
    namespace: &str,
    ledgers_root: &str,
    session_timeout: Option<Duration>,
) -> Result<MetadataBackend, MetadataError> {
    use crate::oxia::{
        client::OxiaClientHandle, idgen::OxiaLedgerIdGenerator,
        layout::OxiaLayoutManager, ledger_manager::OxiaLedgerManager,
        registration::OxiaRegistrationManager,
    };

    let handle = OxiaClientHandle::connect(service_addr, namespace, ledgers_root, session_timeout)
        .await?;
    let paths = handle.paths.clone();

    Ok(MetadataBackend {
        ledger_manager: Arc::new(OxiaLedgerManager::new(handle.client.clone(), paths.clone())),
        id_generator: Arc::new(OxiaLedgerIdGenerator::new(handle.client.clone(), &paths)),
        layout_manager: Arc::new(OxiaLayoutManager::new(handle.client.clone(), paths.clone())),
        registration_manager: Arc::new(OxiaRegistrationManager::new(handle.client, paths.clone())),
        paths,
        zk_raw: None,
    })
}
```

- [ ] **Step 3: Compile and lint**

Run: `cargo check -p bookkeeper-metadata --features oxia`
Expected: clean.

Run: `cargo clippy -p bookkeeper-metadata --features oxia -- -D warnings`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/bookkeeper-metadata/src/backend.rs
git commit -m "metadata: wire oxia backend into open_metadata_backend"
```

## Task 3.9: Shared contract test harness

**Files:**
- Create: `crates/bookkeeper-metadata/tests/common/mod.rs`
- Create: `crates/bookkeeper-metadata/tests/common/contract.rs`

This runs the same assertions against any `MetadataBackend`. Phase 3's value depends on proving the oxia backend behaves identically to ZK, so these tests are load-bearing.

- [ ] **Step 1: Define the harness**

Create `crates/bookkeeper-metadata/tests/common/mod.rs`:

```rust
pub mod contract;
```

Create `crates/bookkeeper-metadata/tests/common/contract.rs`:

```rust
use bookkeeper_metadata::backend::MetadataBackend;
use bookkeeper_metadata::ledger_metadata::{LedgerMetadata, MetadataError};
use bookkeeper_metadata::service_info::BookieServiceInfo;
use bookkeeper_metadata::traits::LedgerLayout;

/// Run the full contract suite against `backend`. Panics on any failure.
pub async fn run_all(backend: &MetadataBackend) {
    layout_roundtrip(backend).await;
    ledger_crud(backend).await;
    ledger_cas_conflict_on_stale_write(backend).await;
    list_ledger_ids(backend).await;
    id_generator_monotonic(backend).await;
    ephemeral_registration_disappears_on_drop(backend).await;
    cookie_crud(backend).await;
}

async fn layout_roundtrip(backend: &MetadataBackend) {
    let layout = LedgerLayout {
        manager_factory_class: "org.apache.bookkeeper.meta.LegacyHierarchicalLedgerManagerFactory".into(),
        manager_version: 1,
    };
    backend.layout_manager.store_ledger_layout(&layout).await.expect("store");
    let got = backend.layout_manager.read_ledger_layout().await.expect("read")
        .expect("present");
    assert_eq!(got.manager_factory_class, layout.manager_factory_class);
    assert_eq!(got.manager_version, layout.manager_version);
    backend.layout_manager.delete_ledger_layout().await.expect("delete");
    assert!(backend.layout_manager.read_ledger_layout().await.expect("read").is_none());
}

async fn ledger_crud(backend: &MetadataBackend) {
    let md = LedgerMetadata::fake_for_test();
    let created = backend.ledger_manager.create_ledger_metadata(42, &md).await.expect("create");
    let read = backend.ledger_manager.read_ledger_metadata(42).await.expect("read");
    assert_eq!(read.version(), created.version());
    backend.ledger_manager.remove_ledger_metadata(42, read.version()).await.expect("remove");
    assert!(matches!(
        backend.ledger_manager.read_ledger_metadata(42).await,
        Err(MetadataError::NotFound(42))
    ));
}

async fn ledger_cas_conflict_on_stale_write(backend: &MetadataBackend) {
    let md = LedgerMetadata::fake_for_test();
    let created = backend.ledger_manager.create_ledger_metadata(99, &md).await.expect("create");
    // Update once to bump the version.
    let bumped = backend.ledger_manager
        .write_ledger_metadata(99, &md, created.version())
        .await
        .expect("write 1");
    // Attempt to write with the stale version — must fail.
    let err = backend.ledger_manager
        .write_ledger_metadata(99, &md, created.version())
        .await
        .expect_err("stale write should fail");
    assert!(matches!(err, MetadataError::VersionConflict { .. }));
    backend.ledger_manager.remove_ledger_metadata(99, bumped.version()).await.ok();
}

async fn list_ledger_ids(backend: &MetadataBackend) {
    let md = LedgerMetadata::fake_for_test();
    for id in [1_i64, 7, 13] {
        backend.ledger_manager.create_ledger_metadata(id, &md).await.expect("create");
    }
    let mut ids = backend.ledger_manager.get_ledger_ranges().await.expect("list");
    ids.retain(|id| [1, 7, 13].contains(id));
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 7, 13]);
    for id in [1_i64, 7, 13] {
        let v = backend.ledger_manager.read_ledger_metadata(id).await.expect("read").version();
        backend.ledger_manager.remove_ledger_metadata(id, v).await.ok();
    }
}

async fn id_generator_monotonic(backend: &MetadataBackend) {
    let a = backend.id_generator.generate_ledger_id().await.expect("gen 1");
    let b = backend.id_generator.generate_ledger_id().await.expect("gen 2");
    let c = backend.id_generator.generate_ledger_id().await.expect("gen 3");
    assert!(a < b && b < c, "generator must be monotonic: {a} < {b} < {c}");
}

async fn ephemeral_registration_disappears_on_drop(_backend: &MetadataBackend) {
    // Placeholder — this property requires dropping the backend to
    // observe, so it is exercised at the tests/oxia_container_test
    // level where a second backend connects to verify.
}

async fn cookie_crud(backend: &MetadataBackend) {
    let bookie = "bookie-test:3181";
    let data = b"cookie-v1".to_vec();
    backend.registration_manager.create_cookie(bookie, &data).await.expect("create cookie");
    let (read_data, v) = backend.registration_manager.read_cookie(bookie).await.expect("read").expect("present");
    assert_eq!(read_data, data);
    let updated = b"cookie-v2".to_vec();
    backend.registration_manager.update_cookie(bookie, &updated, v).await.expect("update");
    let (read2, v2) = backend.registration_manager.read_cookie(bookie).await.expect("read").expect("present");
    assert_eq!(read2, updated);
    assert_ne!(v, v2);
    backend.registration_manager.delete_cookie(bookie, v2).await.expect("delete");
    assert!(backend.registration_manager.read_cookie(bookie).await.expect("read").is_none());
}
```

- [ ] **Step 2: Add `LedgerMetadata::fake_for_test` if it doesn't exist**

Search for an existing test helper. If none exists, add under `#[cfg(test)]` in `crates/bookkeeper-metadata/src/ledger_metadata.rs`:

```rust
#[cfg(test)]
impl LedgerMetadata {
    pub fn fake_for_test() -> Self {
        Self {
            // fill with minimum valid values — inspect the struct
            // and set required fields (ensemble_size=1, ack_quorum=1,
            // write_quorum=1, one ensemble entry at a dummy bookie, etc).
        }
    }
}
```

If the struct is complex (multiple required fields), look in
`crates/bookkeeper-metadata/tests/zk_container_test.rs` for how it
currently builds a test `LedgerMetadata` and copy that construction.

- [ ] **Step 3: Compile-check**

Run: `cargo check -p bookkeeper-metadata --tests --features oxia`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/bookkeeper-metadata/tests/common
git commit -m "metadata/tests: shared contract harness for both backends"
```

## Task 3.10: Run the contract harness against the existing ZK test

**Files:**
- Modify: `crates/bookkeeper-metadata/tests/zk_container_test.rs`

- [ ] **Step 1: Add a single smoke test that runs the contract suite**

Append to `zk_container_test.rs`:

```rust
#[path = "common/mod.rs"]
mod common;

#[tokio::test]
#[ignore] // container-based; run with `cargo test -- --ignored`
async fn zk_backend_satisfies_contract() {
    let fixture = ZkFixture::start().await; // whatever the existing helper is called
    let uri = bookkeeper_metadata::metadata_paths::ParsedMetadataUri::Zk {
        servers: fixture.connect_string().to_string(),
        ledgers_root: "/ledgers".to_string(),
    };
    let backend = bookkeeper_metadata::backend::open_metadata_backend(&uri, None, None)
        .await
        .expect("open zk");
    backend.registration_manager
        .init_new_cluster(
            &bookkeeper_metadata::traits::LedgerLayout {
                manager_factory_class: "LegacyHierarchicalLedgerManagerFactory".into(),
                manager_version: 1,
            },
            "test-cluster",
        )
        .await
        .expect("init");
    common::contract::run_all(&backend).await;
}
```

- [ ] **Step 2: Run**

Run: `cargo test -p bookkeeper-metadata --test zk_container_test -- --ignored --nocapture`
Expected: passes. This validates the harness itself before we use it against oxia.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-metadata/tests/zk_container_test.rs
git commit -m "tests: exercise contract harness against ZK"
```

## Task 3.11: Oxia container integration test

**Files:**
- Create: `crates/bookkeeper-metadata/tests/oxia_container_test.rs`

- [ ] **Step 1: Write the test**

```rust
#![cfg(feature = "oxia")]

#[path = "common/mod.rs"]
mod common;

use testcontainers_oxia::OxiaContainer;

#[tokio::test]
#[ignore] // container-based
async fn oxia_backend_satisfies_contract() {
    let container = OxiaContainer::default().start().await.expect("start oxia");
    let host = container.get_host().await.expect("host");
    let port = container
        .get_host_port_ipv4(testcontainers_oxia::OXIA_PORT)
        .await
        .expect("port");
    let service_addr = format!("{host}:{port}");

    let uri = bookkeeper_metadata::metadata_paths::ParsedMetadataUri::Oxia {
        service_addr,
        namespace: "default".to_string(),
        ledgers_root: "/ledgers".to_string(),
    };
    let backend = bookkeeper_metadata::backend::open_metadata_backend(&uri, None, None)
        .await
        .expect("open oxia");
    backend.registration_manager
        .init_new_cluster(
            &bookkeeper_metadata::traits::LedgerLayout {
                manager_factory_class: "LegacyHierarchicalLedgerManagerFactory".into(),
                manager_version: 1,
            },
            "test-cluster",
        )
        .await
        .expect("init");
    common::contract::run_all(&backend).await;
}

#[tokio::test]
#[ignore]
async fn ephemeral_registration_cleans_up_on_backend_drop() {
    let container = OxiaContainer::default().start().await.expect("start oxia");
    let host = container.get_host().await.expect("host");
    let port = container
        .get_host_port_ipv4(testcontainers_oxia::OXIA_PORT)
        .await
        .expect("port");
    let service_addr = format!("{host}:{port}");
    let uri = bookkeeper_metadata::metadata_paths::ParsedMetadataUri::Oxia {
        service_addr: service_addr.clone(),
        namespace: "default".to_string(),
        ledgers_root: "/ledgers".to_string(),
    };

    let info = bookkeeper_metadata::service_info::BookieServiceInfo::new(
        "ephemeral-bookie:3181", "ephemeral-host", 3181,
    );

    {
        let b = bookkeeper_metadata::backend::open_metadata_backend(&uri, None, None).await.unwrap();
        b.registration_manager.init_new_cluster(
            &bookkeeper_metadata::traits::LedgerLayout {
                manager_factory_class: "LegacyHierarchicalLedgerManagerFactory".into(),
                manager_version: 1,
            },
            "test",
        ).await.unwrap();
        b.registration_manager.register_bookie("ephemeral-bookie:3181", false, &info).await.unwrap();
        let list = b.registration_manager.get_writable_bookies().await.unwrap();
        assert!(list.iter().any(|s| s == "ephemeral-bookie:3181"));
    } // backend dropped — session should expire

    // Reconnect with a different client, give the session a few
    // seconds to expire (default session timeout is 30s — we wait
    // long enough to prove the property).
    tokio::time::sleep(std::time::Duration::from_secs(35)).await;
    let b2 = bookkeeper_metadata::backend::open_metadata_backend(&uri, None, None).await.unwrap();
    let list = b2.registration_manager.get_writable_bookies().await.unwrap();
    assert!(!list.iter().any(|s| s == "ephemeral-bookie:3181"),
        "ephemeral registration should have been cleaned up, got {list:?}");
}
```

- [ ] **Step 2: Run**

Run: `cargo test -p bookkeeper-metadata --test oxia_container_test --features oxia -- --ignored --nocapture`
Expected: both tests pass. The ephemeral test takes ~35s.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-metadata/tests/oxia_container_test.rs
git commit -m "tests: oxia backend contract + ephemeral lifecycle test"
```

## Task 3.12: Bookie end-to-end test against oxia

**Files:**
- Create: `crates/bookkeeper-server/tests/oxia_e2e_test.rs`

- [ ] **Step 1: Write the test**

Copy the layout of the existing `zk_e2e_test.rs` (or equivalent) in
`bookkeeper-server/tests/`. The bookie starts, one ledger is written
and read back, the bookie stops. Everything identical to the ZK test
except:

- Start an `OxiaContainer` instead of a ZK container.
- Construct the bookie config with
  `metadata_service_uri = Some(format!("oxia://{host}:{port}/default/ledgers"))`
  and `zk_servers = None`.
- Everything after is the same — the bookie doesn't know or care.

If no such end-to-end test exists yet, this creates one. Reference
any `tokio::test` at top level that already boots a bookie for the
reference harness shape.

- [ ] **Step 2: Run**

Run: `cargo test -p bookkeeper-server --test oxia_e2e_test --features bookkeeper-metadata/oxia -- --ignored --nocapture`
Expected: pass.

- [ ] **Step 3: Commit**

```bash
git add crates/bookkeeper-server/tests/oxia_e2e_test.rs
git commit -m "tests: bookie end-to-end boot against oxia"
```

## Task 3.13: Documentation

**Files:**
- Create: `docs/metadata-backends.md`
- Modify: `README.md` if it documents `metadataServiceUri`

- [ ] **Step 1: Write `docs/metadata-backends.md`**

```markdown
# Metadata backends

bookkeeper-rust supports two metadata backends, selected by the
`metadataServiceUri` option on both server and client.

## ZooKeeper (default)

```
metadataServiceUri=zk+hierarchical://host1:2181,host2:2181/ledgers
```

Equivalent older forms `zk://...` and `zk+flat://...` are accepted
for compatibility.

## Oxia

```
metadataServiceUri=oxia://host:6648/namespace[/ledgers-root]
```

Optionally prefixed with `metadata-store:` to match Apache Pulsar's
broker config format:

```
metadataServiceUri=metadata-store:oxia://host:6648/namespace
```

The on-wire keys are the same as Apache Pulsar's
`pulsar-metadata/bookkeeper` driver produces. A bookkeeper-rust bookie
can join a Pulsar+oxia cluster that was configured for Pulsar's driver
as long as both use the same namespace and `ledgers-root` (default
`/ledgers`).
```

- [ ] **Step 2: Commit**

```bash
git add docs/metadata-backends.md README.md
git commit -m "docs: metadata backends overview"
```

## Task 3.14: Pulsar interop smoke test (optional, manual)

**Files:**
- Create: `tests/interop/pulsar_oxia.sh` (shell script)

This test needs a Java Pulsar broker and is not run in CI. Marked
optional because it's the expensive confirmation that justifies the
whole Pulsar-interop scope.

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# Boot: oxia standalone + pulsar broker (with pulsar-metadata) + one bookkeeper-rust bookie.
# Create a ledger via Pulsar's managed-ledger client.
# Assert the bookkeeper-rust bookie can read_ledger_metadata on the same id
# and returns identical bytes.
set -euo pipefail
# ...script body is left to the implementer; target environment
# dependent. Document how to run it in a comment at the top.
```

- [ ] **Step 2: Document under `docs/metadata-backends.md`**

Add a subsection at the bottom of `docs/metadata-backends.md`:

```markdown
## Verifying Pulsar interop

`tests/interop/pulsar_oxia.sh` boots a local oxia + Pulsar broker
(Java) + bookkeeper-rust bookie, creates a ledger through Pulsar's
managed-ledger client, and asserts the Rust bookie sees identical
bytes. Run before shipping a significant change to the oxia backend.
```

- [ ] **Step 3: Commit**

```bash
git add tests/interop/pulsar_oxia.sh docs/metadata-backends.md
git commit -m "docs,tests: pulsar+oxia interop smoke script"
```

---

# Done criteria

- [ ] `cargo test --workspace` passes (all three phases landed).
- [ ] `cargo test -p bookkeeper-metadata --features oxia --test oxia_container_test -- --ignored` passes.
- [ ] `cargo test -p bookkeeper-server --features bookkeeper-metadata/oxia --test oxia_e2e_test -- --ignored` passes.
- [ ] `cargo check -p bookkeeper-metadata --no-default-features` passes (the crate still builds without oxia).
- [ ] `grep -rn "ZkLedgerManager::new\|ZkRegistrationManager::new_isolated" crates --include="*.rs"` returns results only under `bookkeeper-metadata/src/zk/` and test files.
- [ ] `docs/metadata-backends.md` exists and describes both URI forms.
