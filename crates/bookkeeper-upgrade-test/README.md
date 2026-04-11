# bookkeeper-upgrade-test

End-to-end Java↔Rust bookie in-place upgrade validator. See
`docs/superpowers/specs/2026-04-11-java-rust-upgrade-test-design.md`
for the full design rationale.

## Prerequisites

```bash
docker info                                                # daemon running
docker pull apache/bookkeeper:4.17.3
docker build --target runtime -t rust-bookie:latest .      # in workspace root
docker build -t bk-upgrade-helper:latest \
    crates/bookkeeper-upgrade-test/helper                  # Java sidecar
```

All three images are verified by `check_prerequisites()` at the start
of the test; the test fails fast with an actionable message if any are
missing.

## How to run

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade \
    java_to_rust_in_place_upgrade -- --ignored --nocapture
```

`--ignored` is required because the test is gated out of the default
workspace test set. `--nocapture` is required so progress messages
stream live.

Typical wall time: **~3m30s**, dominated by Java bookie JVM startup
(5 × ~5s) and per-phase read verification (4 × 4 ledgers × ~7s).

## What it does

5-phase rolling upgrade test on a 2-bookie cluster (E=2 W=2 A=2):

| Phase | Cluster | Workload |
|---|---|---|
| P1 | all-Java | Open L1, L2; write 100 entries each (`ledger_label=N entry=i era=java-era`); close |
| P2 | rolling J→R | Swap bookie-1, verify reads; swap bookie-2, verify reads |
| P3 | all-Rust | Open L3, L4; write 100 entries each (era=rust-era); close |
| P4 | rolling R→J | Swap bookie-1, verify reads; swap bookie-2, verify reads |
| P5 | all-Java again | Final byte-for-byte read-back of L1..L4 |

Four ledgers total, 400 entries. The workload is driven by the Java
BK client inside a sidecar container (`bk-upgrade-helper:latest`) so
it can reach the bookies by their in-network DNS names on the test's
user-defined Docker bridge network.

Per-phase assertion bundle (six checks):

1. Both bookies healthy (HTTP `/heartbeat` returns 200)
2. `/ledgers/available/` in ZK contains the baseline bookieId set
3. `/ledgers/INSTANCEID` is unchanged from P1 (catches accidental rebootstrap)
4. Cookie fields unchanged (`bookieHost`, `journalDir`, `ledgerDirs`, `instanceId`, `layoutVersion`)
5. `/ledgers/underreplication/ledgers` is empty (no auditor false positives)
6. Every closed ledger is fully readable via the sidecar client

## Smoke tests

The file also contains smaller `smoke_*` tests for diagnosing each
fixture in isolation:

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade smoke_ -- --ignored --nocapture
```

| Smoke test | Exercises |
|---|---|
| `smoke_test_env` | TestEnv: network + volume lifecycle |
| `smoke_zk_container` | ZkContainer, zookeeper-client from host |
| `smoke_java_bookie` | Start a Java bookie + wait_healthy |
| `smoke_rust_bookie` | Start a Rust bookie + wait_healthy |
| `smoke_write_ledger` | Sidecar workload write + read round-trip |
| `smoke_stop_and_zk_clear` | stop() + wait_for_zk_ephemeral_to_clear |
| `smoke_swap_one_bookie` | First cross-swap compat test (write Java, read Rust) |

Each smoke test is self-contained and useful when bisecting a failure
in the main test.

## Failure modes — what each test failure tells you

| Symptom | Likely root cause |
|---|---|
| Rust bookie won't start in P2 | Cookie format incompat — Rust can't parse Java's cookie file |
| Rust bookie starts but `wait_healthy` times out | RocksDB schema incompat (can't open Java's column families) or ZK registration incompat |
| Java bookie won't start in P4 | Cookie format asymmetry — Rust wrote it differently from how Java expects |
| Read of L1 fails after a swap in P2 | Entry log file format incompat OR entry location index format incompat |
| Read of L3 fails in P5 | Bidirectional compat broken — Java cannot read Rust's entry log files |
| `underreplication/ledgers` non-empty | ZK identity incompat — auditor sees the swapped-in bookie as a different bookie |
| Cluster instance ID changed | The new bookie ran `initnewcluster` instead of joining (catastrophic) |
| `Permission denied` on volume open | UID mismatch between Java and Rust bookie images (should be fixed in-test via `with_user("0:0")`) |

## On failure

State is preserved for inspection — the network and named volumes are
left in place so you can poke at the cluster manually. Cleanup
commands are printed to stderr:

```
TEST FAILED — state preserved for inspection.
To clean up manually:
  docker network rm bk-upgrade-test-<run-id>
  docker volume rm -f bk-upgrade-test-<run-id>-bk{1,2}-{data,journal}
```

To inspect a preserved volume directly:

```bash
docker run -it --rm \
  -v bk-upgrade-test-<run-id>-bk1-data:/bk/data \
  apache/bookkeeper:4.17.3 bash
# Inside:
ls /bk/data/current/
cat /bk/data/current/VERSION
```

To run a Rust bookie manually on a preserved volume (useful for
iterative compat debugging):

```bash
CONFDIR=$(mktemp -d) && cat > $CONFDIR/bk_server.conf <<'EOF'
bookiePort=3181
httpServerPort=8080
ledgerDirectories=/bk/data
journalDirectory=/bk/journal
zkServers=bk-upgrade-zk-<run-id>:2181
metadataServiceUri=zk://bk-upgrade-zk-<run-id>:2181/ledgers
advertisedAddress=bk-upgrade-bookie-1-<run-id>
useHostNameAsBookieID=true
EOF
docker run --rm --network bk-upgrade-test-<run-id> --user 0:0 \
  -v bk-upgrade-test-<run-id>-bk1-data:/bk/data \
  -v bk-upgrade-test-<run-id>-bk1-journal:/bk/journal \
  -v $CONFDIR/bk_server.conf:/etc/bookkeeper/bk_server.conf \
  rust-bookie:latest
```
