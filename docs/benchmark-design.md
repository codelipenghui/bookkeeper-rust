# Benchmark Design for Rust Bookie

## Overview

Three benchmark levels matching Java BookKeeper's benchmark suite, plus Rust-specific microbenchmarks:

1. **`bk bench-bookie`** — Direct single-bookie benchmark (raw protocol, no ZK)
2. **`bk bench-e2e`** — End-to-end client benchmark (full quorum path via ZK)
3. **Criterion microbenchmarks** — Component-level: digest, entry encoding, storage

---

## 1. `bk bench-bookie` — Direct Bookie Benchmark

Matches Java's `BenchBookie`. Sends raw AddEntry/ReadEntry protocol messages to a single bookie.

### Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `localhost` | Bookie hostname |
| `--port` | `3181` | Bookie port |
| `--entry-size` | `1024` | Entry payload size in bytes |
| `--warmup` | `1000` | Warmup entries (not measured) |
| `--latency-count` | `5000` | Sequential entries for latency measurement |
| `--throughput-count` | `50000` | Async entries for throughput measurement |
| `--max-outstanding` | `1000` | Max concurrent in-flight requests |
| `--digest` | `crc32c` | Digest type: crc32, crc32c, hmac, dummy |

### Phases

1. **Warmup**: Send `warmup` entries sequentially, discard results
2. **Latency**: Send `latency-count` entries one-at-a-time (wait for ACK before next), record per-entry nanosecond latency
3. **Throughput**: Send `throughput-count` entries asynchronously with semaphore-based throttling (`max-outstanding`), measure wall-clock time

### Output

```
=== Write Latency (sequential, 1KB entries) ===
  Entries: 5000
  p50:    0.12 ms
  p95:    0.35 ms
  p99:    0.82 ms
  p999:   2.15 ms
  max:    5.23 ms

=== Write Throughput (async, 1KB entries) ===
  Entries:    50000
  Duration:   2.34 s
  Throughput: 21367 entries/s
  Bandwidth:  20.87 MB/s

=== Read Latency (sequential) ===
  Entries: 5000
  p50:    0.08 ms
  p95:    0.22 ms
  p99:    0.51 ms
  p999:   1.82 ms

=== Read Throughput (async) ===
  Entries:    50000
  Duration:   1.52 s
  Throughput: 32894 entries/s
  Bandwidth:  32.12 MB/s
```

### Implementation

```
1. Connect to bookie via BookieConnection::connect()
2. Build entries using EntryBuilder + DigestManager
3. Warmup: loop { send_add_entry().await; }
4. Latency: loop { t0 = Instant::now(); send_add_entry().await; record(t0.elapsed()); }
5. Throughput: semaphore(max_outstanding); loop { sem.acquire(); spawn { send_add_entry(); sem.release(); } }
6. Read latency: same pattern with send_read_entry()
7. Read throughput: same async pattern with send_read_entry()
8. Compute percentiles from sorted latency array
```

---

## 2. `bk bench-e2e` — End-to-End Client Benchmark

Matches Java's `BenchThroughputLatency`. Uses ZkBookKeeper client with full quorum writes.

### Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--zk` | `localhost:2181` | ZooKeeper connection string |
| `--ledger-root` | `/ledgers` | ZK ledger root |
| `--entry-size` | `1024` | Entry payload bytes |
| `--time` | `60` | Benchmark duration (seconds) |
| `--ensemble` | `3` | Ensemble size |
| `--write-quorum` | `2` | Write quorum |
| `--ack-quorum` | `2` | Ack quorum |
| `--ledgers` | `1` | Concurrent ledgers |
| `--max-outstanding` | `10000` | Max in-flight requests |
| `--digest` | `crc32c` | Digest type |
| `--warmup` | `10000` | Warmup entries |

### Phases

1. **Setup**: Connect to ZK, create `ledgers` ledgers with specified ensemble/quorum
2. **Warmup**: Write `warmup` entries (not measured)
3. **Benchmark**: For `time` seconds:
   - Pick random ledger (if multiple)
   - `append_async()` with semaphore throttling
   - Record per-entry latency in callback
4. **Teardown**: Close all ledgers, report results

### Output

```
=== End-to-End Write Benchmark ===
  Config:     E=3, W=2, A=2, 1KB entries, 1 ledger
  Duration:   60.0 s
  Entries:    145832
  Throughput: 2430 entries/s
  Bandwidth:  2.37 MB/s

  Latency:
    p50:    0.38 ms
    p95:    1.12 ms
    p99:    2.45 ms
    p999:   8.72 ms
```

---

## 3. Criterion Microbenchmarks

Rust-native benchmarks using the `criterion` crate for component-level perf measurement.

### Benchmarks

| Benchmark | What it measures |
|-----------|-----------------|
| `digest_crc32` | CRC32 digest compute for 1KB payload |
| `digest_crc32c` | CRC32C digest compute for 1KB payload |
| `digest_hmac` | HMAC-SHA1 digest compute for 1KB payload |
| `entry_build` | EntryBuilder: metadata + digest + payload serialization |
| `entry_parse` | parse_entry: deserialization + digest verification |
| `write_cache_put` | WriteCache.put() for 1KB entry |
| `write_cache_get` | WriteCache.get() for cached entry |
| `rocksdb_put` | EntryLocationIndex.put() |
| `rocksdb_get` | EntryLocationIndex.get() |
| `codec_encode` | BookieCodec frame encoding |
| `codec_decode` | BookieCodec frame decoding |

### Entry Size Parameterization

Each benchmark parameterized over: `[128, 1024, 4096, 65536]` bytes.

---

## 4. Standard Test Matrix

For comparing Rust vs Java bookie performance:

| Scenario | Entry Size | Ensemble | Write Quorum | Entries |
|----------|-----------|----------|-------------|---------|
| Small entries | 128 B | 1 | 1 | 100K |
| Default | 1 KB | 1 | 1 | 100K |
| Medium entries | 4 KB | 1 | 1 | 50K |
| Large entries | 64 KB | 1 | 1 | 10K |
| Quorum write | 1 KB | 3 | 2 | 50K |
| Multi-ledger | 1 KB | 1 | 1 | 100K (10 ledgers) |
| Mixed read/write | 1 KB | 1 | 1 | 50K write + 50K read |

---

## 5. Implementation Plan

### File Structure

```
crates/bookkeeper-cli/src/
  main.rs              — add bench-bookie and bench-e2e commands
  bench.rs             — benchmark implementations

benches/                 — criterion benchmarks
  digest_bench.rs
  entry_bench.rs
  storage_bench.rs
  codec_bench.rs
```

### Dependencies

```toml
# CLI (for bench commands)
hdrhistogram = "7"     # HdrHistogram for latency percentiles

# benches/
[dev-dependencies]
criterion = "0.5"
```

### Key Design Decisions

1. **HdrHistogram** for latency recording — matches industry standard, handles percentiles efficiently
2. **Semaphore-based throttling** — `tokio::sync::Semaphore` for max-outstanding control
3. **Per-entry nanotime** — `Instant::now()` captured before send, elapsed after response
4. **Warmup phase** — primes caches, JIT (irrelevant for Rust but primes OS page cache)
5. **Random payload** — fill with random bytes to avoid compression artifacts
6. **Separate read/write phases** — write first, then read back (like Java BenchBookie)
