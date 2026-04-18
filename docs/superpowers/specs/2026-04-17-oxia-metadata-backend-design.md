# Oxia as a Metadata Backend

Status: Draft (pending user review)
Author: bookkeeper-rust maintainers
Date: 2026-04-17

## 1. Goal

Add [Oxia](https://github.com/oxia-db/oxia) as a second metadata backend for
bookkeeper-rust, alongside the existing ZooKeeper backend.

**Interop requirement (load-bearing):** keys, values, and CAS semantics
MUST be wire-compatible with Apache Pulsar's existing
`pulsar-metadata/bookkeeper` driver (`PulsarRegistrationManager`,
`PulsarLedgerManager`, `PulsarLedgerIdGenerator`,
`PulsarLedgerUnderreplicationManager`). A bookkeeper-rust bookie pointed
at the same oxia namespace as a Pulsar broker managing BK ledgers must
read and write the same records that Pulsar expects.

Scope:

- Server (bookie), CLI, replication worker, auditor — all talk to oxia
  via the existing four metadata traits.
- Bookie registration via ephemeral records with oxia-managed sessions.
- Ledger ID generation matching Pulsar's HOB scheme bit-for-bit.

Non-goals:

- Flattening or "modernising" the Pulsar layout. Interop wins over
  elegance; oxia's native `SequenceKeyDelta` and flat list APIs are
  *not* used even though they would be simpler — Pulsar doesn't use
  them either.
- ZK → oxia migration tooling.
- Oxia notification/watch support. The current codebase does not consume
  ZK watches.
- Client-side oxia support in the Rust client crate. Same factory
  extends to it later; the server path is landed first.

## 2. Motivation

- Operational parity with StreamNative / Apache Pulsar deployments that
  already run on oxia (Pulsar 4.x default).
- A bookkeeper-rust bookie can be dropped into a Pulsar+oxia cluster
  without standing up a separate ZK ensemble.
- Oxia's sharded design removes the ZK bottleneck on ledger-metadata hot
  paths; oxia-managed sessions replace the bookie's fragile
  heartbeat-isolation workaround (`ZkRegistrationManager::new_isolated`'s
  dedicated 1-worker runtime).

## 3. Dependency

`liboxia = "0.0.2"` — the official Rust client from `oxia-db/liboxia`
(crates.io, Oct 2025, last updated Jan 2026). Surface we rely on:

- `OxiaClient::new(OxiaClientOptions)`, `put`/`get`/`delete`/`list`/
  `range_scan`/`delete_range` plus `*_with_options`.
- `PutOption::ExpectVersionId(i64)`, `ExpectedRecordNotExists()`,
  `Ephemeral()`.
- `DeleteOption::ExpectVersionId(i64)`.
- `OxiaError::{KeyNotFound, UnexpectedVersionId, SessionDoesNotExist,
  KeyLeaderNotFound, ...}`.

`PutOption::SequenceKeyDelta` is available but **deliberately unused**
(see §6.4 — we port Pulsar's HOB scheme instead for interop).

`testcontainers-oxia = "0.0.1"` (dev-dependency) for integration tests.

## 4. Trait-layer changes

### 4.1 Widen version to `i64`

Oxia's `version_id` is `i64`. Pulsar's `PulsarRegistrationManager` uses
`LongVersion` (i64). ZK `stat.version` (i32) fits losslessly.

- `Versioned<T>::version: i32 → i64` (`crates/bookkeeper-metadata/src/versioned.rs`)
- `LedgerManager::write_ledger_metadata(..., current_version: i32 → i64)`
- `RegistrationManager::{read,update,delete}_cookie` version field
  → `i64`

Mechanical across ~15 call sites (server, http_api, cli, datainteg,
tests). No on-wire format changes for ZK — the ZK backend widens
`stat.version` on read.

### 4.2 `MetadataBackend` factory

Today, server.rs and friends construct `ZkLedgerManager::new(...)` and
`ZkRegistrationManager::new_isolated(...)` directly. Replace with:

```rust
pub struct MetadataBackend {
    pub ledger_manager: Arc<dyn LedgerManager>,
    pub id_generator: Arc<dyn LedgerIdGenerator>,
    pub layout_manager: Arc<dyn LayoutManager>,
    pub registration_manager: Arc<dyn RegistrationManager>,
    // Raw-ZK escape hatch for http_api endpoints that currently reach
    // into zk::Client directly. None for oxia — those endpoints will
    // return NotSupported until we abstract them behind traits.
    pub zk_raw: Option<ZkRawHandles>,
}

pub struct ZkRawHandles {
    pub client: zookeeper_client::Client,
    pub paths: ZkPaths,
}

pub async fn open_metadata_backend(
    metadata_service_uri: &str,
    runtime: Option<tokio::runtime::Handle>,     // ZK-only
    session_timeout: Option<Duration>,           // applies to both
) -> Result<MetadataBackend, MetadataError>;
```

Selection by URI scheme:

- `zk://...`, `zk+hierarchical://...`, `zk+flat://...` → ZK backend
- `oxia://...`, `metadata-store:oxia://...` (see §5) → oxia backend
- anything else → `MetadataError::InvalidConfig`

## 5. URI scheme

Two forms supported, both parsed into the same internal representation:

```
oxia://<host1>:<port>[,<host2>:<port>...]/<namespace>[/<ledgersRootPath>]
metadata-store:oxia://<host>:<port>/<namespace>[/<ledgersRootPath>]
```

The `metadata-store:` prefix matches Pulsar's own
`metadataStoreUrl=metadata-store:oxia://...` form used in Pulsar's broker
config. We accept both so operators can copy/paste from Pulsar configs
without translation.

`<ledgersRootPath>` is the base key for all BK records. It defaults to
`/ledgers` — the same default Pulsar uses (`PulsarRegistrationManager`
is constructed with `"/ledgers"`). Must begin with `/`; embedded slashes
are allowed (so nested namespaces like `/app/bk` work).

Parser changes in `crates/bookkeeper-config/src/server.rs`:

- `parse_metadata_service_uri` returns an enum
  `ParsedMetadataUri::{Zk(String), Oxia { service_addr, namespace, ledgers_root }}`.
- Existing consumers that need "just ZK hosts" keep working via a helper
  returning `None` for non-ZK URIs.

## 6. Key layout (must match Pulsar exactly)

All keys share a `<ledgersRootPath>` prefix (default `/ledgers`). The
sub-key names and formats below are copied from
`pulsar-metadata/src/main/java/org/apache/pulsar/metadata/bookkeeper/`
and `BookKeeperConstants.java`.

| Concept                       | Oxia key                                                                      | Source of truth                        |
| ----------------------------- | ----------------------------------------------------------------------------- | -------------------------------------- |
| Cluster layout                | `<root>/LAYOUT`                                                               | `PulsarLayoutManager`, `LAYOUT_ZNODE`  |
| Cluster instance ID           | `<root>/INSTANCEID`                                                           | `BookKeeperConstants.INSTANCEID`       |
| Ledger metadata               | `<root>/<hierarchical-path>/L{NNNN}` (see §6.1)                               | `LegacyHierarchicalLedgerManager`      |
| Cookie                        | `<root>/cookies/<bookie_id>`                                                  | `COOKIE_NODE`                          |
| Writable bookie registration  | `<root>/available/<bookie_id>`                                                | `AVAILABLE_NODE`                       |
| Read-only bookie registration | `<root>/available/readonly/<bookie_id>`                                       | `AVAILABLE_NODE`/`READONLY`            |
| Short ID gen                  | `<root>/idgen/ID-{NNNNNNNNNN}`                                                | `PulsarLedgerIdGenerator.IDGEN_SHORT_NODE` |
| Long ID gen (HOB dirs)        | `<root>/idgen-long/HOB-{NNNNNNNNNN}/ID-{NNNNNNNNNN}`                          | `PulsarLedgerIdGenerator.IDGEN_NODE`   |

### 6.1 Hierarchical ledger paths

Same format as Java's `LegacyHierarchicalLedgerManager`, which is what
our existing `ZkLedgerManager` already produces via
`crate::zk_path::ledger_path`:

- ID `< i32::MAX` → `<root>/XX/XXXX/L{XXXX}` (3-level, 10 digits)
- ID `>= i32::MAX` → `<root>/XXX/XXXX/XXXX/XXXX/L{XXXX}` (5-level, 19 digits)

The `zk_path.rs` helpers (`ledger_path`, `ensure_zk_parents`, etc.) need
no behavioural change, but they should move into a shared
`metadata_paths.rs` module since both backends produce identical paths.
Backend-specific path code that remains in the zk module: only the ZK
path-ensuring (which uses `zk::CreateMode::Persistent`) — the oxia side
does its own parent creation via `put(..., IfRecordDoesNotExist)`.

### 6.2 Parent creation

Oxia has no implicit hierarchy — paths like `<root>/cookies/<id>` are
flat strings. But Pulsar's `OxiaMetadataStore.createParents` creates
explicit empty-value records at every intermediate path segment, so that
`store.getChildren` / listing-by-prefix works. We replicate:

Before `put(<root>/<a>/<b>/<leaf>)`, for each prefix path
`<root>/<a>`, `<root>/<a>/<b>`: `put(prefix, [], IfRecordDoesNotExist)`
and swallow `UnexpectedVersionId` (already created).

The `<root>` itself is assumed to exist — `init_new_cluster` creates it
alongside `LAYOUT` / `INSTANCEID`.

### 6.3 Listing children

To enumerate immediate children of `<path>` (e.g. bookies under
`<root>/available/`, or ledger directories under `<root>`), use:

```rust
client.list(format!("{path}/"), format!("{path}/\u{00ff}")).await
```

matching Pulsar's `getChildrenFromStore(path)` (`{pathWithSlash}/` to
`{pathWithSlash}//` — we use a higher-codepoint upper bound to be safe
against keys containing `/`). Then strip the `{path}/` prefix and drop
entries containing further `/` (those are grand-children — same filter
Pulsar applies).

### 6.4 Ledger ID generation

Port of `PulsarLedgerIdGenerator`, which matches `ZkLedgerIdGenerator`'s
two-tier scheme bit-for-bit:

Phase 1 (short, 31-bit):
1. `put(<root>/idgen, [], IfRecordDoesNotExist)` once (idempotent).
2. `put(<root>/idgen/ID-, [], Ephemeral + Sequential)` — the oxia
   "sequential" translation in liboxia should yield a zero-padded
   10-digit suffix derived from the parent's `modifications_count`.
3. Delete the created ephemeral record (best-effort).
4. Parse suffix; if `>= i32::MAX`, create `<root>/idgen-long` and fall
   through to phase 2.

**Open liboxia question:** at 0.0.2 I have not yet confirmed that
`liboxia` exposes a `CreateOption.Sequential` equivalent — Pulsar's
Java client does, but the Rust crate's `PutOption` enum seen in
§3 does not list it. If missing, we implement the same behaviour
client-side: read the parent's current `modifications_count` via `get`,
then `put` the child at `ID-{count:010}` with
`ExpectedRecordNotExists()`; retry on `UnexpectedVersionId` (another
bookie raced). Either way the on-wire key is identical to Pulsar's.

Phase 2 (long, 63-bit): identical to our existing
`ZkLedgerIdGenerator::generate_long_id` — list HOB directories, pick
max, generate low-bits, combine `(high << 32) | low`, GC old HOB dirs
keeping 3. The code ports nearly line-for-line from `zk/idgen.rs`;
only the "create sequential node" primitive differs.

## 7. Semantics mapping

### 7.1 CAS writes

| Operation                                              | Oxia call                                                             | Error mapping                                                            |
| ------------------------------------------------------ | --------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `create_ledger_metadata`                               | `put_with_options(key, value, [ExpectedRecordNotExists])`             | `UnexpectedVersionId` → `LedgerExists(id)`                               |
| `write_ledger_metadata(id, v, current_version)`        | `put_with_options(key, value, [ExpectVersionId(current_version)])`    | `UnexpectedVersionId` → `VersionConflict { expected, actual: -1 }`       |
| `remove_ledger_metadata(id, version)`                  | `delete_with_options(key, [ExpectVersionId(version)])`                | `UnexpectedVersionId` → `VersionConflict`, `KeyNotFound` → `NotFound(id)`|
| `read_ledger_metadata`                                 | `get(key)`                                                            | `KeyNotFound` → `NotFound(id)`; version = `GetResult.version.version_id` |
| Cookie ops                                             | as above with the cookie key                                          | same                                                                     |

### 7.2 Ephemeral registration

```rust
client.put_with_options(
    format!("{root}/available/{bookie_id}"),
    info.to_protobuf_bytes(),
    vec![PutOption::Ephemeral()],
).await?;
```

`OxiaClient` creates one session per shard and keeps it alive.
`OxiaClientOptions::session_timeout` — we default to 30s to match the
existing ZK behaviour. On bookie process death, every `Ephemeral()` key
is purged, matching Pulsar's `PulsarRegistrationManager` behaviour (it
uses `MetadataStoreExtended.registerBookie` which goes through
`OxiaMetadataStore.doStorePut` with `CreateOption.Ephemeral`).

This **eliminates** the `ZkRegistrationManager::new_isolated` dedicated
1-worker runtime. `liboxia` runs its own background tasks.

### 7.3 Error mapping

| `liboxia::OxiaError`                | `bookkeeper_metadata::MetadataError`                                       |
| ----------------------------------- | -------------------------------------------------------------------------- |
| `KeyNotFound`                       | `NotFound(id)` (id-contextual) or `NotFound(0)`                            |
| `UnexpectedVersionId`               | `VersionConflict { expected, actual: -1 }` or `LedgerExists` in create-only context |
| `SessionDoesNotExist`               | `ConnectionError(...)` — caller retries after session re-init              |
| `IllegalArgument` (when tied to encoding) | `SerializationError(...)`                                            |
| `KeyLeaderNotFound` / transport     | `ConnectionError(...)` — retriable                                         |
| `UnexpectedStatus(code)`            | `ConnectionError("oxia status {code}")`                                    |

## 8. Module layout

```
crates/bookkeeper-metadata/src/
├── lib.rs                # pub uses both backends; oxia gated on feature
├── traits.rs             # i64 widening only
├── versioned.rs          # version: i64
├── ledger_metadata.rs    # unchanged
├── metadata_serde.rs     # unchanged — v3 protobuf codec is shared
├── service_info.rs       # unchanged
├── backend.rs            # NEW — MetadataBackend, open_metadata_backend
├── metadata_paths.rs     # NEW — lifted from zk_path.rs; shared path helpers
├── zk/                   # unchanged except import paths
│   ├── mod.rs
│   ├── ledger_manager.rs
│   ├── layout.rs
│   ├── registration.rs
│   └── idgen.rs
└── oxia/                 # NEW  (cfg(feature = "oxia"))
    ├── mod.rs
    ├── client.rs         # OxiaClient wrapper (shared options, session)
    ├── paths.rs          # oxia-specific helpers (child listing upper-bound etc.)
    ├── parents.rs        # createParents equivalent
    ├── ledger_manager.rs # OxiaLedgerManager
    ├── layout.rs         # OxiaLayoutManager
    ├── registration.rs   # OxiaRegistrationManager
    └── idgen.rs          # OxiaLedgerIdGenerator (ports from zk/idgen.rs)
```

`Cargo.toml`:

```toml
[features]
default = ["oxia"]
oxia = ["dep:liboxia"]

[dependencies]
liboxia = { version = "0.0.2", optional = true }

[dev-dependencies]
testcontainers-oxia = "0.0.1"
```

Rationale for enabling-by-default: same shape as the existing
`zookeeper-client` dependency; users who don't configure `oxia://` pay
only compile cost.

## 9. Consumer call-site changes (mechanical)

1. `server.rs` — replace direct `Zk*Manager::new(...)` with
   `open_metadata_backend(uri, ...)` and unpack `MetadataBackend`.
2. `http_api.rs` — same. Endpoints that need `zk::Client` directly
   access `backend.zk_raw`; on oxia they return `501 Not Supported` for
   the ZK-specific ones (all administrative — none on the hot path).
3. `cli/admin.rs`, `cli/main.rs` — same.
4. `bookkeeper-replication` (auditor, replication worker) — same.
5. `datainteg/recovery.rs` — i64 widening only.
6. Tests:
   - Existing ZK tests untouched.
   - New `bookkeeper-metadata/tests/oxia_container_test.rs` via
     `testcontainers-oxia`, mirroring the assertions in
     `zk_container_test.rs`.
   - Shared contract harness under `bookkeeper-metadata/tests/common/`
     that runs the same assertions against both backends.
   - One end-to-end test under `bookkeeper-server/tests/` booting a
     bookie against an oxia container and exercising a full ledger.

## 10. Interop verification

Before calling this done:

1. Start a Pulsar broker (Java) pointed at oxia, create one ledger via
   Pulsar's managed-ledger path.
2. Point a bookkeeper-rust bookie at the same `metadataServiceUri`.
3. The Rust bookie must:
   - See the `LAYOUT` Pulsar wrote (same format string).
   - See the same `INSTANCEID`.
   - Appear in `PulsarRegistrationClient.getWritableBookies()` after
     registration (so Pulsar accepts it as a peer).
   - Accept a `read_ledger_metadata` call for the Pulsar-created ledger
     and return identical bytes to what Pulsar sees.

A short integration script (`tests/interop/pulsar_oxia.sh`) codifies
these checks. Gated behind a test feature because it needs the Pulsar
broker jar; skipped in CI unless explicitly opted in.

## 11. Risks and open questions

- **liboxia `Sequential` equivalent.** §6.4 — if liboxia 0.0.2 exposes
  no `Sequential` put option, we replicate the behaviour via a
  get-parent-then-CAS-put pattern. Both produce the exact Pulsar key
  format; the fallback is one extra RTT on idgen. Need to confirm by
  reading `liboxia`'s PutOption enum more carefully during
  implementation.
- **`liboxia` is at 0.0.2.** Pre-1.0; APIs may churn. We pin `=0.0.2`
  and re-qualify on upgrade. Surface used is small.
- **i64 version widening** is cross-crate but mechanical (~15 sites).
  `cargo check --workspace` catches any missed cast.
- **Pulsar layout lock-in.** Matching Pulsar exactly means we inherit
  its quirks (HOB-3-GC race, explicit-parent creation). This is the
  price of interop and explicitly in scope.
- **`open_metadata_backend` takes ZK-specific `runtime` /
  `session_timeout` today.** They're `Option`s and oxia applies the
  session timeout to `OxiaClientOptions::session_timeout`. Fine.

## 12. Rollout

Three reviewable slices, each lands independently and keeps CI green:

1. Trait-level `i64` widening. Neutral refactor.
2. Extract `metadata_paths.rs`; introduce `MetadataBackend` factory;
   migrate all call sites. Still ZK-only. Neutral refactor.
3. Oxia backend + feature flag + tests (contract + integration +
   Pulsar interop). New behaviour only under `oxia://` URIs.
