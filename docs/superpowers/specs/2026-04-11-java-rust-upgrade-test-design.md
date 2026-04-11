# Java ↔ Rust Bookie In-Place Upgrade Test — Design Spec

## Goal

A high-confidence end-to-end test that proves the Rust bookie binary can replace a running Java bookie *in place* — same data directory, same journal directory, same cookie, same ZK identity — without losing data, and that the swap is fully reversible (Rust → Java works too).

This test exists to validate the production migration path *before* a real cluster is upgraded. It is intended to be run manually on developer laptops or CI runners as a pre-release validator, not on every commit.

## Scope

The test stresses **on-disk format compatibility** between Java BookKeeper 4.18.0 and the Rust bookie:

- Cookie file (`/bk/data/current/VERSION`)
- RocksDB column families (`locations/`, `ledgers/`)
- Entry log files (`<hex>.log`) — header + per-entry framing
- Journal files (`<hex>.txn`) — record format and `lastMark` semantics
- ZooKeeper registration paths (`/ledgers/available/<bookieId>`, `/ledgers/INSTANCEID`)

Wire-protocol compatibility (Java client ↔ Rust bookie) is **out of scope** here. It is already covered by:

- `crates/bookkeeper-client/tests/java_bookie_container_test.rs`
- `crates/bookkeeper-client/tests/java_client_compat_test.rs`
- `tests/java-compat/.../MixedClusterTest.java`

## Goals

- Prove Java-written ledgers are readable from a Rust bookie that takes over the same data dir.
- Prove Rust-written ledgers are readable from a Java bookie that takes over the same data dir (downgrade safety).
- Prove the upgraded bookie keeps its **identity** (bookieId, cluster instance ID, ZK registration path).
- Prove the auditor does not generate **phantom under-replication** events for ledgers that already meet quorum on the upgraded bookie.
- Surface a clear, actionable failure when any compat surface is broken.

## Non-goals

- Zero-downtime guarantees: with the chosen 2-bookie / `E=2 W=2 A=2` topology the swap window blocks writes by design.
- Latency or throughput regressions: not measured, not asserted.
- CI-on-every-commit: the test is `#[ignore]`-gated and runs ~6 minutes; it is for manual / scheduled use.
- Ungraceful crash injection during the swap.
- Network-partition behavior during the swap.
- Performance comparisons between Java and Rust bookies.
- 3+ bookie topologies.
- Pulsar broker integration. The workload is driven by the Rust BK client directly.
- Java BK client coverage of Rust bookies (covered by the existing wire-compat tests above).

## Success criteria (hard pass/fail)

1. **Zero ack loss across the upgrade.** Every entry that was acked by the cluster in any phase is byte-identical when read back in the final phase.
2. **Identity continuity.** Each bookie slot keeps the same `bookieId` (network alias + port) before and after every swap. The set `{bookie-1:3181, bookie-2:3181}` is invariant in `/ledgers/available/` across the entire run.
3. **No phantom under-replication.** `/ledgers/under_replication/` in ZK is empty after every swap.
4. **All Java-written entries readable from Rust** (covered by #1 — listed for clarity).
5. **All Rust-written entries readable from Java after downgrade** (covered by #1 — listed for clarity, and is the key bidirectional property).
6. **Cluster instance ID is invariant.** `/ledgers/INSTANCEID` returns the same UUID throughout the entire run. A change here would mean a bookie ran `initnewcluster` instead of joining the existing cluster — catastrophic.

Soft (warned but not failed): per-bookie disk usage should be ≤ 2× the bytes the test wrote. Catches "rewrite-everything-on-restart" regressions like the bug fixed in commit `bc4e7922`.

## Architecture

A new workspace crate `crates/bookkeeper-upgrade-test/` containing a single `#[ignore]`-gated `tokio::test`. The test orchestrates Docker containers via `testcontainers-rs` and drives the workload with the existing `bookkeeper-client` workspace crate. No Java code, no Maven, no JVM on the host, no Pulsar broker.

The test owns:

- One user-defined Docker bridge network (`bk-upgrade-test-<run-id>`)
- Four named Docker volumes (`bk-upgrade-test-<run-id>-bk{1,2}-{data,journal}`)
- All container handles, dropped on test end

A `TestEnv` guard struct manages cleanup. On success, network and volumes are removed. On failure, they are left intact and the test prints the cleanup commands the user can run after investigation.

## Container topology

| Slot | Network alias | Volumes | Initial image |
|---|---|---|---|
| zk | `zookeeper` | (ephemeral) | `zookeeper:3.8` |
| bookie 1 | `bookie-1` | `bk1-data → /bk/data`, `bk1-journal → /bk/journal` | `apache/bookkeeper:4.17.3` |
| bookie 2 | `bookie-2` | `bk2-data → /bk/data`, `bk2-journal → /bk/journal` | `apache/bookkeeper:4.17.3` |

Two bookies, configured for `E=2 W=2 A=2` (matches the existing `docker-compose.yml` topology). Both bookies advertise their network alias as `bookieId`, so the bookieId is stable across image swaps.

## Compat surfaces stressed by every swap

When a slot swaps from Java to Rust (or vice versa) on the same volumes, the new bookie must:

1. **Read the cookie file** at `/bk/data/current/VERSION` and adopt the existing `bookieHost`, `journalDir`, `ledgerDirs`, `instanceId`, `layoutVersion`.
2. **Open RocksDB column families** `locations/` and `ledgers/` that the previous bookie wrote — same schema, same key/value layout, same `LedgerData` protobuf.
3. **Read existing entry log files** (`<hex>.log`) — header format and per-entry framing must match.
4. **Replay any unflushed journal entries** since `lastMark` — journal record format must match.
5. **Register in ZK** at `/ledgers/available/<alias>:3181` with the same identity.
6. **Roll forward to a fresh entry log file** instead of reopening an existing one (the bug fixed in commit `bc4e7922`).

Each of these is a compat surface. Any one being wrong causes the swap to fail. The test treats failure of any of them as a real bug to file and fix.

## Swap mechanism

The test owns a `BookieSlot` per bookie:

```rust
struct BookieSlot {
    slot_id: u32,
    alias: String,                              // "bookie-1"
    data_volume: String,                        // "bk-upgrade-test-<run>-bk1-data"
    journal_volume: String,                     // "bk-upgrade-test-<run>-bk1-journal"
    network: String,
    config_dir: PathBuf,                        // host-side dir holding bk_server.conf
    container: Option<ContainerAsync<GenericImage>>,
}

impl BookieSlot {
    async fn start_java(&mut self, zk: &str) -> Result<()>;
    async fn start_rust(&mut self, zk: &str) -> Result<()>;
    async fn stop(&mut self) -> Result<()>;
    async fn wait_healthy(&self, timeout: Duration) -> Result<()>;
}

async fn swap_to_rust(slot: &mut BookieSlot, zk: &str, zk_client: &ZkClient) -> Result<()> {
    slot.stop().await?;
    wait_for_zk_ephemeral_to_clear(zk_client, &slot.alias).await?;
    slot.start_rust(zk).await?;
    slot.wait_healthy(Duration::from_secs(120)).await?;
    Ok(())
}
```

`start_java` and `start_rust` mount the same volumes and join the network with the same alias, so other containers (the second bookie, the test's BK client) continue to resolve `bookie-1` to the new container's IP.

### Two non-obvious details

**Stop timeout.** Docker's default container stop timeout is 10 seconds, which is shorter than a worst-case bookie shutdown flush. The fix in commit `bc4e7922` made restart-from-corrupted-entry-log impossible, but the underlying cause (SIGKILL during a flush) is still wasteful. The test sets the stop timeout to **120 seconds** explicitly so `checkpoint()` always completes cleanly.

**ZK ephemeral race.** When a bookie process exits, its ZK ephemeral node at `/ledgers/available/<alias>:3181` does not disappear instantly — ZK only removes it after the session times out (~6 s by default). If the new bookie tries to register before that, it gets `NODE_EXISTS` and fails. `wait_for_zk_ephemeral_to_clear` polls the path until it disappears or until a 30-second deadline.

### Configuring the Rust bookie image

The Rust image's entrypoint reads `/etc/bookkeeper/bk_server.conf`. The test writes a per-slot config file to a host-side temp dir and bind-mounts it to that path. Each slot's config differs only in `advertisedAddress`.

The Java image accepts environment variables (`BK_advertisedAddress`, `BK_zkServers`, etc.) which its entrypoint translates to the same config file. Both images end up with equivalent runtime configuration.

## Test phases

| Phase | Cluster state | Workload |
|---|---|---|
| **P1** | All-Java (bookie-1 + bookie-2) | Open L1 (`E=2 W=2 A=2`, CRC32C, password `b"upgrade-test"`), write 100 entries `format!("ledger=1 entry={i} java-era")`, close. Same for L2. |
| **P2** | Rolling Java → Rust | **No active writers.** Swap bookie-1 → run full per-phase verification. Swap bookie-2 → verify again. |
| **P3** | All-Rust | Open L3, write 100 entries `format!("ledger=3 entry={i} rust-era")`, close. Same for L4. |
| **P4** | Rolling Rust → Java | **No active writers.** Swap bookie-1 → verify. Swap bookie-2 → verify. |
| **P5** | All-Java again | Final read-back of L1..L4 — every entry, both bookies independently. |

Four ledgers total. Two from the Java era, two from the Rust era. The test is fully sequential: writes only happen while both bookies in the cluster are alive, never during a swap window. This is required because `W=2` with one bookie down would block writes indefinitely.

### Why 4 ledgers, not 1 each per era

Two ledgers per era guards against accidentally getting lucky on a single ledger ID (e.g. a hash collision in the bookieId placement, a stale write_cache entry that happens to be valid). Two independent ledgers per era is cheap (each is 100 entries × ~80 bytes) and gives meaningful redundancy.

### Wall-time estimate

| Phase | Time |
|---|---|
| P1 — startup of 2 Java bookies + writes | ~90 s (Java JVM startup dominates) |
| P2 — 2 swaps × ~50 s each | ~100 s |
| P3 — Rust writes only, no startup | ~10 s |
| P4 — 2 swaps × ~70 s each (Java startup is slower than Rust startup) | ~140 s |
| P5 — reads only | ~10 s |
| **Total** | **~6 minutes** |

## Per-phase assertions

After every phase boundary the test runs `verify_cluster_state(&zk, &bookies, &baseline, &closed_ledgers)`:

1. **Both bookies healthy.** HTTP `GET /heartbeat` returns 200.
2. **Identity continuity.** `zk ls /ledgers/available/` returns exactly the baseline set `{bookie-1:3181, bookie-2:3181}`.
3. **Cluster instance ID invariant.** `zk get /ledgers/INSTANCEID` matches the value first read in P1.
4. **Cookie field-equal.** `docker exec` into each bookie, read `/bk/data/current/VERSION`, parse it as a cookie record, and compare its `bookieHost`, `journalDir`, `ledgerDirs`, `instanceId`, and `layoutVersion` fields to the values captured in P1. Per-field equality, not byte-for-byte equality, because Java and Rust may serialize the file with different field ordering or whitespace. If parsing fails on either side, that itself fails the assertion.
5. **No under-replication.** `zk ls /ledgers/under_replication/` is empty. With the 2-bookie sequential design there is no in-flight catch-up, so this is enforced strictly.
6. **All closed ledgers fully readable.** For every closed ledger up to this point, open it via the Rust BK client and read every entry. Compare each payload to the deterministic format string. Mismatches fail the test loudly with the exact `(ledger, entry)` pair plus expected and actual bytes.

## Cross-phase final assertions (after P5)

These run once at end of test, comparing snapshots collected throughout:

- **Identity is one set across the whole run.** The bookieId set in `/ledgers/available/` is `{bookie-1:3181, bookie-2:3181}` in every snapshot.
- **Cluster instance ID is one value across the whole run.**
- **All 4 ledgers are intact byte-for-byte.**
- **Disk usage is reasonable.** `du -sh /bk/data /bk/journal` for each bookie is ≤ 2× the entry payload bytes written. Catches accidental rewrites.

## File layout

```
crates/bookkeeper-upgrade-test/
├── Cargo.toml
├── README.md           # prereqs, run command, what each phase asserts
├── src/
│   └── lib.rs          # empty placeholder: //! See tests/upgrade.rs
└── tests/
    └── upgrade.rs      # the integration test (~250 LoC, #[ignore]-gated)
```

`src/lib.rs` exists only because Cargo requires a non-test target on a crate. It contains nothing but a doc comment pointing to the test file. All fixture helpers (`TestEnv`, `BookieSlot`, `verify_cluster_state`, etc.) live inside `tests/upgrade.rs` itself — there is no library API to share.

## Cargo.toml

```toml
[package]
name = "bookkeeper-upgrade-test"
version.workspace = true
edition.workspace = true
license.workspace = true
publish = false

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
testcontainers = { workspace = true }
anyhow = "1"
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
bookkeeper-client = { workspace = true }
bookkeeper-protocol = { workspace = true }
zookeeper-client = { workspace = true }
bytes = { workspace = true }
bollard = "0.20"
tempfile = { workspace = true }
```

`anyhow` is added as a dev-dep — for a test crate, `anyhow::Result` is the right call (stack traces, easy `?` on heterogeneous error types). It does not need to be a workspace dep because no production crate uses it.

`bollard` 0.20 (testcontainers' underlying Docker client) is used directly for network and volume management, since `testcontainers-rs`'s public API is too restrictive for those operations.

The crate is added to the workspace `members` list. It is *not* added to the default test set because its sole test is `#[ignore]`-gated.

## How to run

```bash
# 1. Build the Rust bookie image (production target — no profiling, no debug info)
docker build --target runtime -t rust-bookie:latest .

# 2. Pull the Java BK image (pinned tag)
docker pull apache/bookkeeper:4.17.3

# 3. Run the test
cargo test -p bookkeeper-upgrade-test --test upgrade -- --ignored --nocapture
```

`--nocapture` is required so the test's `tracing::info!` per-phase progress messages stream live. `--ignored` is required because the test is gated.

## Prerequisites checking

`check_prerequisites()` runs first and fails fast with an actionable message if any prerequisite is missing:

- Docker daemon reachable? (`docker info`)
- `apache/bookkeeper:4.17.3` image present locally?
- `rust-bookie:latest` image present locally?
- ≥10 GB free in `/var/lib/docker` (best-effort warning, not a hard fail)

If any check fails, the test returns `Result::Err` (not panic) so the cargo output is clean.

## Cleanup behavior

`TestEnv` holds a guard whose `Drop` impl manages cleanup:

- **On success**: removes the network, all 4 volumes, and any leftover containers tagged with the test's run ID.
- **On failure** (`Result::Err` or panic): leaves the network, volumes, and containers in place. Prints the cleanup commands the user can run after investigation:

```
TEST FAILED. State preserved for inspection. To clean up:
  docker network rm bk-upgrade-test-<run-id>
  docker volume rm bk-upgrade-test-<run-id>-bk{1,2}-{data,journal}
```

The user can also `docker run -it --rm -v <vol>:/bk apache/bookkeeper:4.17.3 bash` to poke at any volume directly.

## Failure modes — what each test failure tells you

| Symptom | Likely root cause |
|---|---|
| Rust bookie won't start in P2 | Cookie format incompat — Rust can't parse Java's cookie file |
| Rust bookie starts but `wait_healthy` times out | RocksDB schema incompat — Rust can't open Java's column families, or ZK registration incompat |
| Java bookie won't start in P4 | Cookie format asymmetry — Rust wrote it differently from how Java expects |
| Read of L1 fails after a swap in P2 | Entry log file format incompat or entry location index format incompat |
| Read of L3 fails in P5 | Bidirectional compat broken — Java cannot read Rust's entry log files |
| `under_replication/` non-empty | ZK identity incompat — auditor sees the swapped-in bookie as a different bookie |
| Cluster instance ID changed | The new bookie ran `initnewcluster` instead of joining (catastrophic) |
| Disk size > 2× expected | Entry log roll-forward bug regression — recheck commit `bc4e7922` |

Each of these is a real compat surface a production migration would hit. The test's purpose is to find them before users do.

## Acceptance criteria for the test itself

The test is "done" when:

1. It compiles cleanly with `cargo build -p bookkeeper-upgrade-test --tests`.
2. It passes locally on macOS with Docker Desktop and on Linux with Docker Engine.
3. The cleanup path runs cleanly after both success and failure paths (manually verified by causing each).
4. The README has a copy-pastable run command and a list of prerequisites.
5. The first time it is run against current `main`, ideally it passes. If it fails, the failures it surfaces are real compat bugs to file and fix in separate PRs. The test stays as the validation gate going forward.

## Open questions / future work

- Whether to add a 3-bookie / `E=3 W=3 A=2` variant later for "rolling upgrade with continuous traffic" coverage. This requires Pulsar or a more complex Rust-client workload that handles swap-window writes.
- Whether the test should also run periodically against unstable Java BK versions (e.g. `4.19.0-SNAPSHOT`) to catch upstream changes that would break the upgrade path.
- Whether to add ungraceful-crash injection (SIGKILL during a write) as a later test variant.

These are deliberately not part of the initial test.
