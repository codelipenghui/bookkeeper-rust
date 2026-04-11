# bookkeeper-upgrade-test

End-to-end Java↔Rust bookie in-place upgrade validator. See
`docs/superpowers/specs/2026-04-11-java-rust-upgrade-test-design.md`
for the full design rationale.

## Prerequisites

```bash
docker info                                                # daemon running
docker pull apache/bookkeeper:4.17.3
docker build --target runtime -t rust-bookie:latest .      # in workspace root
```

## How to run

```bash
cargo test -p bookkeeper-upgrade-test --test upgrade -- --ignored --nocapture
```

`--ignored` is required because the test is gated. `--nocapture` is
required so progress messages stream live.

## What it does

5-phase rolling upgrade test on a 2-bookie cluster (E=2 W=2 A=2):

1. **P1 all-Java** — write ledgers L1, L2 (java-era data)
2. **P2 rolling J→R** — swap bookies one at a time, verify reads after each
3. **P3 all-Rust** — write ledgers L3, L4 (rust-era data)
4. **P4 rolling R→J** — swap back to Java one at a time, verify reads
5. **P5 all-Java again** — final read-back of L1..L4 byte-for-byte

Wall time: ~6 minutes, dominated by Java JVM startup.

## On failure

State is preserved for inspection. Cleanup commands are printed to stderr.
