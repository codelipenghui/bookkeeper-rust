# Comprehensive Benchmark Test Plan

## Purpose

Measure and compare the Rust bookie's performance against Java BookKeeper across all important dimensions: entry sizes, quorum configurations, concurrency levels, and read/write patterns.

---

## Test Environment Setup

```
Hardware: Same machine for both Java and Rust bookies (to eliminate network variance)
OS: Linux (or macOS for development)
Disk: SSD recommended (NVMe for production benchmarks)
ZK: Embedded ZK (for E2E tests) or standalone ZK
Build: Rust release build (--release), Java default JVM opts
Repetitions: 3 runs per scenario, report median + stddev
```

---

## Suite 1: Direct Bookie Benchmark (`bk bench-bookie`)

Single bookie, no ZK, raw protocol. Measures bookie I/O path performance.

### 1A. Entry Size Sweep (baseline)

| ID | Entry Size | Warmup | Latency | Throughput | Max Outstanding |
|----|-----------|--------|---------|-----------|-----------------|
| S1 | 128 B     | 2000   | 10000   | 100000    | 1000            |
| S2 | 512 B     | 1000   | 10000   | 100000    | 1000            |
| S3 | 1 KB      | 1000   | 10000   | 100000    | 1000            |
| S4 | 4 KB      | 1000   | 5000    | 50000     | 1000            |
| S5 | 16 KB     | 500    | 3000    | 20000     | 500             |
| S6 | 64 KB     | 200    | 1000    | 5000      | 200             |
| S7 | 256 KB    | 100    | 500     | 2000      | 100             |

**What to learn:** CPU overhead per entry, memory bandwidth limits, protocol framing cost.

### 1B. Concurrency Sweep (1KB entries)

| ID | Max Outstanding | Warmup | Throughput Count |
|----|----------------|--------|-----------------|
| C1 | 1              | 1000   | 10000           |
| C2 | 10             | 1000   | 50000           |
| C3 | 100            | 1000   | 100000          |
| C4 | 1000           | 1000   | 100000          |
| C5 | 5000           | 1000   | 200000          |
| C6 | 10000          | 1000   | 200000          |

**What to learn:** Throughput scaling with concurrency, connection pipelining efficiency.

### 1C. Digest Type Comparison (1KB entries, 1000 outstanding)

| ID | Digest | Throughput Count |
|----|--------|-----------------|
| D1 | crc32c | 100000          |
| D2 | crc32  | 100000          |
| D3 | hmac   | 100000          |
| D4 | dummy  | 100000          |

**What to learn:** Digest computation overhead, HMAC vs CRC cost.

### 1D. Sustained Load (1KB entries)

| ID | Duration | Entry Size | Max Outstanding | Purpose |
|----|----------|-----------|-----------------|---------|
| L1 | 5 min    | 1 KB      | 1000            | Steady state, cache warm |
| L2 | 30 min   | 1 KB      | 1000            | Long-running stability |
| L3 | 5 min    | 4 KB      | 1000            | Entry log rotation test |

**What to learn:** Performance stability over time, GC impact, entry log rotation.

---

## Suite 2: End-to-End Benchmark (`bk bench-e2e`)

Full client path with ZK, quorum writes, ensemble management.

### 2A. Quorum Configuration Sweep (1KB entries, 60s)

| ID | Ensemble | Write Q | Ack Q | Max Outstanding | Description |
|----|----------|---------|-------|-----------------|-------------|
| Q1 | 1        | 1       | 1     | 5000            | Single bookie baseline |
| Q2 | 3        | 2       | 2     | 5000            | Pulsar production default |
| Q3 | 3        | 3       | 2     | 5000            | Full replication, relaxed ack |
| Q4 | 3        | 3       | 3     | 5000            | Full replication, strict ack |
| Q5 | 5        | 3       | 2     | 5000            | Wide ensemble, moderate quorum |
| Q6 | 5        | 3       | 3     | 5000            | High availability |

**What to learn:** Quorum overhead, ack latency impact, ensemble size scaling.

### 2B. Multi-Ledger Scaling (E=3, W=2, A=2, 1KB, 60s)

| ID | Ledgers | Max Outstanding | Description |
|----|---------|-----------------|-------------|
| M1 | 1       | 10000           | Single ledger |
| M2 | 5       | 10000           | Few ledgers |
| M3 | 10      | 10000           | Moderate multi-topic |
| M4 | 50      | 10000           | Busy broker |
| M5 | 100     | 10000           | Large-scale deployment |

**What to learn:** Lock contention, cache efficiency, metadata overhead per ledger.

### 2C. Entry Size x Quorum (60s)

| ID | Entry Size | E | W | A | Max Outstanding |
|----|-----------|---|---|---|-----------------|
| EQ1 | 128 B    | 3 | 2 | 2 | 10000           |
| EQ2 | 1 KB     | 3 | 2 | 2 | 10000           |
| EQ3 | 4 KB     | 3 | 2 | 2 | 5000            |
| EQ4 | 16 KB    | 3 | 2 | 2 | 2000            |
| EQ5 | 64 KB    | 3 | 2 | 2 | 500             |

**What to learn:** Network bandwidth saturation point, large entry quorum overhead.

---

## Suite 3: Read Performance

### 3A. Sequential Read (after writing N entries)

| ID | Entry Size | Entries Written | Read Pattern | Description |
|----|-----------|----------------|-------------|-------------|
| R1 | 1 KB      | 100000          | Sequential   | Full ledger read |
| R2 | 4 KB      | 50000           | Sequential   | Larger entries |
| R3 | 64 KB     | 5000            | Sequential   | Large entry read |
| R4 | 1 KB      | 100000          | Last 1000    | Tail read (hot cache) |
| R5 | 1 KB      | 100000          | Random       | Cache miss pattern |

**What to learn:** Read cache hit rate, sequential vs random access, cold vs hot reads.

### 3B. Mixed Read/Write (E=3, W=2, A=2, 1KB, 60s)

| ID | Write % | Read % | Ledgers | Description |
|----|---------|--------|---------|-------------|
| RW1 | 100    | 0      | 1       | Write-only baseline |
| RW2 | 70     | 30     | 1       | DistributedLog pattern |
| RW3 | 50     | 50     | 2       | Balanced (separate ledgers) |
| RW4 | 20     | 80     | 5       | Consumer catch-up |
| RW5 | 0      | 100    | 10      | Read-only fan-out |

**What to learn:** Read/write interference, cache thrashing under mixed load.

---

## Suite 4: Stress & Limits

### 4A. Maximum Throughput Discovery

| ID | Entry Size | Start Outstanding | Step | Max | Duration per Step |
|----|-----------|-------------------|------|-----|-------------------|
| T1 | 128 B     | 100               | 2x   | 100K | 30s              |
| T2 | 1 KB      | 100               | 2x   | 100K | 30s              |
| T3 | 4 KB      | 100               | 2x   | 50K  | 30s              |

Increase `max_outstanding` exponentially until latency degrades or errors appear.

**What to learn:** Saturation point, backpressure behavior, error handling under load.

### 4B. Tail Latency Stability

| ID | Entry Size | Outstanding | Duration | Measurement Focus |
|----|-----------|-------------|----------|-------------------|
| TL1 | 1 KB     | 1000        | 10 min   | p999 stability over time |
| TL2 | 1 KB     | 5000        | 10 min   | p999 under moderate load |
| TL3 | 1 KB     | 10000       | 10 min   | p999 under heavy load |

**What to learn:** GC pauses (Java), allocation patterns (Rust), tail latency sources.

### 4C. Recovery Performance

| ID | Entries | Entry Size | Ensemble | Scenario |
|----|---------|-----------|----------|----------|
| RC1 | 10000  | 1 KB      | 1        | Fence + forward read time |
| RC2 | 100000 | 1 KB      | 3        | Full recovery with quorum |
| RC3 | 10000  | 64 KB     | 3        | Large entry recovery |

**What to learn:** Recovery protocol overhead, fencing latency.

---

## Suite 5: Component Microbenchmarks (Criterion)

### 5A. Digest Performance

| ID | Digest | Payload Size | Description |
|----|--------|-------------|-------------|
| DG1 | CRC32   | 128, 1K, 4K, 64K | CRC32 compute |
| DG2 | CRC32C  | 128, 1K, 4K, 64K | CRC32C compute |
| DG3 | HMAC    | 128, 1K, 4K, 64K | HMAC-SHA1 compute |
| DG4 | Dummy   | 128, 1K, 4K, 64K | Baseline (no digest) |

### 5B. Entry Codec

| ID | Operation | Payload Size |
|----|-----------|-------------|
| EC1 | EntryBuilder.build() | 128, 1K, 4K, 64K |
| EC2 | parse_entry() | 128, 1K, 4K, 64K |

### 5C. Storage Operations

| ID | Operation | Description |
|----|-----------|-------------|
| ST1 | WriteCache.put() | 1KB entry insert |
| ST2 | WriteCache.get() | 1KB entry lookup (hit) |
| ST3 | ReadCache.get() | 1KB entry lookup (hit) |
| ST4 | RocksDB put (entry location) | Single key-value |
| ST5 | RocksDB get (entry location) | Single key-value |
| ST6 | Full flush cycle | WriteCache → entry log + index |

### 5D. Protocol Codec

| ID | Operation | Payload Size |
|----|-----------|-------------|
| PC1 | Encode AddEntry request | 128, 1K, 4K, 64K |
| PC2 | Decode AddEntry request | 128, 1K, 4K, 64K |
| PC3 | Encode ReadEntry response | 128, 1K, 4K, 64K |

---

## Metrics to Record (per scenario)

```
Latency:
  - p50, p75, p95, p99, p999, p9999, max (microseconds)
  - Mean, standard deviation

Throughput:
  - ops/sec (entries per second)
  - MB/sec (payload bandwidth)

Resource Usage:
  - Peak RSS memory (via /proc/self/status or ps)
  - CPU utilization (user + sys)
  - Disk I/O (bytes written, IOPS)

Stability:
  - Error count / error rate
  - Latency variance over 10-second windows
```

---

## Comparison Report Format

For each scenario, side-by-side table:

```
Scenario: S3 (1KB entries, single bookie)
+------------------+-----------+-----------+--------+
| Metric           | Rust      | Java      | Δ      |
+------------------+-----------+-----------+--------+
| Write p50 (ms)   | 0.10      | 0.15      | -33%   |
| Write p99 (ms)   | 0.45      | 0.82      | -45%   |
| Write p999 (ms)  | 1.20      | 3.50      | -66%   |
| Write ops/s      | 45000     | 32000     | +41%   |
| Write MB/s       | 43.9      | 31.3      | +40%   |
| Read p50 (ms)    | 0.07      | 0.12      | -42%   |
| Read ops/s       | 65000     | 48000     | +35%   |
| RSS memory (MB)  | 85        | 450       | -81%   |
| CPU (user %)     | 35        | 55        | -36%   |
+------------------+-----------+-----------+--------+
```

---

## Execution Script

```bash
#!/bin/bash
# Run all Suite 1 benchmarks
for SIZE in 128 512 1024 4096 16384 65536; do
  echo "=== Entry size: $SIZE ==="
  bk bench-bookie --bookie 127.0.0.1:3181 \
    --entry-size $SIZE \
    --warmup 1000 \
    --latency-count 5000 \
    --throughput-count 50000 \
    --max-outstanding 1000 \
    | tee results/bench-bookie-${SIZE}B.txt
done
```
