# Oxia as a Metadata Backend

Status: Draft (pending user review)
Author: bookkeeper-rust maintainers
Date: 2026-04-17

## 1. Goal

Add [Oxia](https://github.com/oxia-db/oxia) as a second metadata backend for
bookkeeper-rust, alongside the existing ZooKeeper backend. Bookies, the CLI,
the replication worker, and all integration tests must be able to run against
either backend selected via `metadataServiceUri`.

Non-goals:

- Client-side (reader/writer) oxia support — the current client already
  consumes the same trait layer; wiring it through is trivial once the
  server path works, but it is deferred to a follow-up to keep this change
  focused.
- ZK → oxia migration tooling.
- Notification/watch support. The current codebase does not consume ZK
  watches; `get_ledger_ranges` is polled. Oxia notifications stay unused.

## 2. Motivation

- Operational parity with StreamNative-hosted deployments, where Oxia is
  the default metadata store for Pulsar 4.x and newer.
- Oxia's sharded design removes the well-known ZK bottleneck on ledger
  metadata hot paths (tens of thousands of `get_ledger_metadata` calls/s)
  and the cost of maintaining a separate ZK ensemble.
- Oxia sessions + server-side sequence generation replace two of the
  bookie's most fragile ZK patterns (ephemeral znodes on a
  heartbeat-starved runtime, the two-tier HOB ID generator).

## 3. Non-requirements we are explicitly not preserving

- The ZK-specific hierarchical 2-4-4 / 3-4-4-4-4 ledger path layout. Under
  oxia the layout is flat and range-scannable.
- The ZK-specific `idgen-long/HOB-XXXXXXXXXX/ID-...` ID scheme. Oxia's
  built-in sequence keys cover the same need with an i64 counter and no
  rollover logic.

## 4. Dependency

`liboxia = "0.0.2"` — the official Rust client from `oxia-db/liboxia`,
published on crates.io (Oct 2025, last updated Jan 2026).

Relevant surface we rely on:

- `OxiaClient::new(OxiaClientOptions)` / `put`/`get`/`delete`/`list`/
  `range_scan`/`delete_range` with `*_with_options` variants.
- `PutOption::ExpectVersionId(i64)` for CAS, `ExpectedRecordNotExists()`
  for create-only semantics.
- `PutOption::Ephemeral()` — client-managed session ties the key to the
  `OxiaClient` lifetime.
- `PutOption::SequenceKeyDelta(Vec<u64>)` — server-assigned monotonic key
  suffixes (i64-valued).
- `OxiaError::{KeyNotFound, UnexpectedVersionId, SessionDoesNotExist, ...}`
  — mapped to `MetadataError` variants.

The oxia-managed session replaces the custom
`ZkRegistrationManager::new_isolated` dedicated-runtime workaround: the
client keeps the session alive internally.

## 5. Trait-layer changes

The four existing traits in `crates/bookkeeper-metadata/src/traits.rs`
(`LedgerManager`, `LedgerIdGenerator`, `LayoutManager`,
`RegistrationManager`) already abstract the right concepts. Two small
changes are needed:

### 5.1 Widen version to `i64`

Oxia's `version_id` is `i64`. ZK `stat.version` is `i32` and fits
losslessly. Change:

- `Versioned<T>::version: i32 → i64` (`crates/bookkeeper-metadata/src/versioned.rs`)
- `LedgerManager::write_ledger_metadata(..., current_version: i32)` → `i64`
- `RegistrationManager::{read,update,delete}_cookie` version field → `i64`

This touches ~15 consumer sites (server, http_api, cli, datainteg). The
change is mechanical: widen the type and remove a couple of `as i64`
casts. No on-wire format changes — the ZK backend still stores the
underlying `stat.version: i32` and widens on read.

### 5.2 New `MetadataBackendFactory`

Today, server.rs and friends construct `ZkLedgerManager::new(...)`,
`ZkRegistrationManager::new_isolated(...)`, etc. directly. Replace those
call sites with a single factory:

```rust
pub struct MetadataBackend {
    pub ledger_manager: Arc<dyn LedgerManager>,
    pub id_generator: Arc<dyn LedgerIdGenerator>,
    pub layout_manager: Arc<dyn LayoutManager>,
    pub registration_manager: Arc<dyn RegistrationManager>,
    // For existing ZK call sites that still need the raw client (http_api
    // stats endpoints, ad-hoc znode reads): exposed only by the ZK impl.
    pub zk_raw: Option<ZkRawHandles>,
}

pub async fn open_metadata_backend(
    metadata_service_uri: &str,
    // runtime is retained for the ZK heartbeat-isolation optimisation;
    // oxia ignores it (liboxia has its own internal runtime scheduling).
    runtime: Option<tokio::runtime::Handle>,
    // ZK-only; oxia uses liboxia's default.
    session_timeout: Option<Duration>,
) -> Result<MetadataBackend, MetadataError>;
```

Selection by scheme:

- `zk://...`, `zk+hierarchical://...`, `zk+flat://...` → ZK backend
- `oxia://...` → Oxia backend
- anything else → `MetadataError::InvalidConfig`

The `zk_raw` escape hatch is there to keep the http_api endpoints that
currently talk to the ZK client directly working, so that the factory
migration is mechanical. Follow-up work can fold those endpoints behind
generic traits so that the escape hatch disappears for oxia deployments
(they'll return an explicit `NotSupported` error in the meantime — the
affected endpoints are administrative, not on the hot path).

## 6. URI scheme

```
oxia://<host1>:<port>[,<host2>:<port>...]/<namespace>[/<root-prefix>]
```

- `host:port` — oxia gateway address; multiple hosts are comma-separated
  (matching the oxia Go client convention).
- `<namespace>` — required; oxia is multi-tenant.
- `<root-prefix>` — optional key prefix. Defaults to `bookkeeper` so that
  namespaces shared with other services don't collide. All keys below
  become `/{root-prefix}/<path>`.

Parser changes in
`crates/bookkeeper-config/src/server.rs::parse_metadata_service_uri`:

- Existing return type (`Option<String>` returning ZK hosts) generalises
  to an enum `ParsedMetadataUri::{Zk(String), Oxia { hosts, namespace, prefix }}`.
- Consumers that need "just ZK hosts" continue to work via a helper that
  returns `None` for non-ZK URIs.

## 7. Key layout under oxia

All keys below assume a `{root}` prefix derived from the URI (e.g.
`bookkeeper`).

| Concept                       | Oxia key                                       | Notes                                                    |
| ----------------------------- | ---------------------------------------------- | -------------------------------------------------------- |
| Cluster layout                | `{root}/LAYOUT`                                | Same text format as ZK (`{fmtVer}\n{class}:{mgrVer}`).   |
| Cluster instance ID           | `{root}/INSTANCEID`                            | UUID string.                                             |
| Ledger metadata               | `{root}/ledgers/{ledger_id:019}`               | Binary protobuf (v3), same codec as ZK backend.          |
| Cookie                        | `{root}/cookies/{bookie_id}`                   | Raw cookie bytes.                                        |
| Writable bookie registration  | `{root}/available/{bookie_id}`                 | Ephemeral. Value: `BookieServiceInfo` protobuf.          |
| Read-only bookie registration | `{root}/available/readonly/{bookie_id}`        | Ephemeral.                                               |
| Ledger ID generator counter   | `{root}/idgen/seq-`                            | Sequence-key puts; suffix == ledger ID.                  |

Key-choice rationale:

- `{ledger_id:019}` — 19 decimal digits = widest i64. Fixed-width zero
  padding makes the natural string order equal the numeric order, so
  `list()` / `range_scan()` over `ledgers/` returns IDs in order without
  an explicit sort.
- `available/` directly holding regular writable bookies and
  `available/readonly/` for read-only — matches ZK layout. `list()` over
  `{root}/available/` with proper end-bound excludes the `readonly/`
  sub-prefix.
- Ledger ID counter under its own prefix so sequence-key suffix scans don't
  mix with ledger data.

## 8. Semantics mapping

### 8.1 CAS writes

| Our operation                                          | Oxia                                                 | Error mapping                                          |
| ------------------------------------------------------ | ---------------------------------------------------- | ------------------------------------------------------ |
| `create_ledger_metadata` (must not exist)              | `put_with_options(key, value, [ExpectedRecordNotExists])` | `UnexpectedVersionId` → `LedgerExists(id)`         |
| `write_ledger_metadata(id, v, current_version)`        | `put_with_options(key, value, [ExpectVersionId(current_version)])` | `UnexpectedVersionId` → `VersionConflict`       |
| `remove_ledger_metadata(id, version)`                  | `delete_with_options(key, [ExpectVersionId(version)])` | `UnexpectedVersionId` → `VersionConflict`, `KeyNotFound` → `NotFound(id)` |
| `read_ledger_metadata`                                 | `get(key)`                                           | `KeyNotFound` → `NotFound(id)`; version = `GetResult.version.version_id` |
| Cookie CAS                                             | Same as above with cookie key                        | Same                                                   |

### 8.2 Ephemeral registration

```rust
client.put_with_options(
    format!("{root}/available/{bookie_id}"),
    info.to_protobuf_bytes(),
    vec![PutOption::Ephemeral()],
).await?;
```

`OxiaClient` creates one session per shard and keeps it alive. Session
timeout is controlled by `OxiaClientOptions::session_timeout`; we default
to 30s to match the ZK backend's behaviour. When the bookie process dies
the session expires and every `Ephemeral()` key it owned is removed —
same guarantee as ZK ephemeral znodes.

This **eliminates** the dedicated-runtime workaround documented in
`ZkRegistrationManager::new_isolated` (the 1-worker runtime that exists
purely to keep heartbeats unblocked by RPC load). `liboxia` runs its own
background tasks and does not share the bookie's request-handling
runtime.

### 8.3 Listing ledgers

```rust
client.list(format!("{root}/ledgers/"), format!("{root}/ledgers0")).await
```

`0` is the next char after `/` in ASCII, so the exclusive upper bound is
a well-defined lexicographic end of the prefix. The returned keys are
already sorted, strip the prefix, parse the 19-digit decimal → `i64`.
No hierarchical recursion, no reconstruction.

### 8.4 Ledger ID generation

```rust
let res = client.put_with_options(
    format!("{root}/idgen/seq-"),
    vec![],
    vec![PutOption::SequenceKeyDelta(vec![1])],
).await?;
// res.key == "{root}/idgen/seq-{XXXXXXXXXXXXXXXXXXXX}" (20-digit suffix)
let ledger_id: i64 = res.key.rsplit('-').next().unwrap().parse()?;
```

Oxia's sequence counter is i64; no split-namespace HOB logic needed.
Monotonic across the cluster by construction.

### 8.5 Error mapping

| `liboxia::OxiaError`                | `bookkeeper_metadata::MetadataError`          |
| ----------------------------------- | --------------------------------------------- |
| `KeyNotFound`                       | `NotFound(id)` (when id-contextual) or `NotFound(0)` |
| `UnexpectedVersionId`               | `VersionConflict { expected, actual: -1 }` or `LedgerExists` in create-only context |
| `SessionDoesNotExist`               | `ConnectionError(...)` — caller should retry after re-registration |
| `IllegalArgument`                   | `SerializationError(...)` if tied to encoding; else `InvalidConfig` |
| transport / `KeyLeaderNotFound`     | `ConnectionError(...)` (retriable)            |
| `UnexpectedStatus`                  | `ConnectionError(...)` with the wrapped code  |

## 9. Module layout

```
crates/bookkeeper-metadata/src/
├── lib.rs                # pub use both backends behind feature gates
├── traits.rs             # unchanged (modulo i64 widening)
├── versioned.rs          # version widened to i64
├── ledger_metadata.rs    # unchanged
├── metadata_serde.rs     # unchanged — protobuf codec is backend-agnostic
├── service_info.rs       # unchanged
├── backend.rs            # NEW — MetadataBackend, open_metadata_backend
├── zk/                   # unchanged
└── oxia/                 # NEW  (cfg(feature = "oxia"))
    ├── mod.rs
    ├── paths.rs          # key-building helpers analogous to zk_path.rs
    ├── client.rs         # OxiaClient wrapper with shared options + lifetime
    ├── layout.rs         # OxiaLayoutManager
    ├── registration.rs   # OxiaRegistrationManager
    ├── ledger_manager.rs # OxiaLedgerManager
    └── idgen.rs          # OxiaLedgerIdGenerator
```

Cargo:

```toml
[features]
default = ["oxia"]   # ship with oxia on by default; ZK remains always-on
oxia = ["dep:liboxia"]

[dependencies]
liboxia = { version = "0.0.2", optional = true }
```

Rationale for enabling-by-default: users who don't configure `oxia://`
pay only the compile cost. Build-time gating matters mainly for
embedded / size-sensitive builds; those can turn the feature off.

## 10. Consumer call-site changes

All changes are mechanical:

1. **server.rs** — replace direct `ZkRegistrationManager::new_isolated`
   and `ZkLedgerManager::new(...)` with `open_metadata_backend(uri, ...)`
   and unpack `MetadataBackend`.
2. **http_api.rs** — same.
3. **cli/admin.rs**, **cli/main.rs** — same.
4. **bookkeeper-replication** (auditor, replication worker) — same.
5. **datainteg/recovery.rs** — no code change beyond i64 widening.
6. **Tests**:
   - `bookkeeper-metadata/tests/zk_container_test.rs` — unchanged.
   - New `bookkeeper-metadata/tests/oxia_container_test.rs` — Testcontainers-
     based, using the `testcontainers-oxia` crate (same author, 0.0.1).
   - One new end-to-end test under `bookkeeper-server/tests/` that boots
     a bookie against an oxia container and writes/reads a ledger.

## 11. Testing

Two levels:

1. **Contract tests (new)** — a shared test harness in
   `bookkeeper-metadata/tests/common/` runs the same trait-level
   assertions against both backends. Covers: CAS conflict, not-found,
   ephemeral-on-drop, sequence monotonicity, list ordering, layout
   round-trip. Gated by the `oxia` feature.
2. **Integration (one new test)** — full bookie boot against oxia, one
   ledger created and recovered.

Existing ZK-specific tests remain untouched.

## 12. Risks and open questions

- **Oxia cluster operational model is different from ZK.** Bookie ops who
  know ZK cold won't have the same muscle memory for oxia. We mitigate by
  keeping `zk://` as the documented default in README until oxia support
  has soaked in at least one dev environment.
- **`liboxia` is at 0.0.2.** Pre-1.0 APIs may churn. We pin `=0.0.2` and
  re-qualify on upgrade. The surface we use is small (put/get/delete/list
  plus three options); breakage risk is localised.
- **i64 version widening is cross-crate.** Touches datainteg/check.rs,
  server.rs, http_api.rs, cli — but the change is purely mechanical
  (type widening, no semantic change). Pre-commit `cargo check --workspace`
  catches any missed cast.
- **`open_metadata_backend` signature takes ZK-only params
  (`runtime`, `session_timeout`).** These become optional and oxia
  ignores them. If oxia grows equivalent knobs later we add
  `oxia_session_timeout` symmetrically.

## 13. Rollout

1. Land trait-level i64 widening (neutral refactor, reviewable standalone).
2. Land `MetadataBackend` factory + migrate ZK call sites (still ZK-only,
   neutral refactor).
3. Land oxia backend + feature flag + tests.
4. Documentation: a `docs/metadata-backends.md` note covering
   `metadataServiceUri` syntax for both.
