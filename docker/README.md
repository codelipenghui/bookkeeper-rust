# Rust BookKeeper + Pulsar docker-compose playground

A 7-service compose stack that lets you run Apache Pulsar against a
3-node Rust BookKeeper ensemble on a laptop, with real replication
(E=3, W=2, A=2) and auto-recovery exercised by Pulsar's normal traffic.

```
         pulsar-broker (Java BK client)
              │
              │  BookKeeper V3 wire protocol
              ▼
   ┌──────────┼──────────┐
   ▼          ▼          ▼
rust-bk-1  rust-bk-2  rust-bk-3
   │          │          │
   └──────────┼──────────┘
              ▼
          zookeeper  (/ledgers/*, auditor election,
                       under-replicated markers, ...)
```

## Stack

| Service         | Image                       | Purpose |
|-----------------|-----------------------------|---------|
| `zookeeper`     | `zookeeper:3.8`             | Shared metadata store for BK + Pulsar |
| `bk-init`       | (built from this repo)      | One-shot: `bk initnewcluster` (idempotent) |
| `rust-bookie-1` | (built from this repo)      | Rust bookie instance 1 |
| `rust-bookie-2` | (built from this repo)      | Rust bookie instance 2 |
| `rust-bookie-3` | (built from this repo)      | Rust bookie instance 3 |
| `pulsar-init`   | `apachepulsar/pulsar:3.3.5` | One-shot: `pulsar initialize-cluster-metadata` (idempotent) |
| `pulsar-broker` | `apachepulsar/pulsar:3.3.5` | Broker only (no bundled BK/ZK) |

All three bookies use the same image and the same `bk_server.conf`;
they differ only by their `hostname`, their data volumes, and the
`--advertised-address rust-bookie-N` CLI override that tells each one
which identity to register under in ZK.

## Ports

| Service         | Container | Host  | Purpose                       |
|-----------------|-----------|-------|-------------------------------|
| `zookeeper`     | 2181      | 2181  | ZK client protocol            |
| `rust-bookie-1` | 3181      | 3181  | BK V2/V3 wire protocol        |
| `rust-bookie-1` | 8080      | 8081  | BK HTTP admin API             |
| `rust-bookie-2` | 3181      | 3182  | BK V2/V3 wire protocol        |
| `rust-bookie-2` | 8080      | 8082  | BK HTTP admin API             |
| `rust-bookie-3` | 3181      | 3183  | BK V2/V3 wire protocol        |
| `rust-bookie-3` | 8080      | 8083  | BK HTTP admin API             |
| `pulsar-broker` | 6650      | 6650  | Pulsar native binary protocol |
| `pulsar-broker` | 8080      | 8080  | Pulsar admin HTTP API         |

## Quick start

```bash
# First run: builds the rust-bookie image (~10 min for RocksDB), then
# starts everything. Subsequent runs skip the build and are much faster.
docker compose up

# Or run detached:
docker compose up -d

# Watch logs for a single service:
docker compose logs -f rust-bookie-1
docker compose logs -f pulsar-broker

# Watch all 3 bookies at once:
docker compose logs -f rust-bookie-1 rust-bookie-2 rust-bookie-3

# Tear down, keep data:
docker compose down

# Tear down + nuke all ZK / bookie / Pulsar data:
docker compose down -v
```

## Smoke tests

### Verify all 3 Rust bookies are healthy

```bash
for port in 8081 8082 8083; do
  echo "--- bookie on port $port ---"
  curl -sS http://localhost:$port/api/v1/bookie/is_ready
  curl -sS http://localhost:$port/api/v1/bookie/state | jq
done

curl -sS http://localhost:8081/metrics | head
# → Prometheus metrics from bookie-1
```

### Verify Pulsar discovered the Rust bookie

```bash
curl http://localhost:8080/admin/v2/brokers/health
# → ok

curl http://localhost:8080/admin/v2/clusters
# → ["cluster-a"]

# List bookies registered with the shared ZK:
docker compose exec pulsar-broker bin/bookkeeper shell listbookies -rw
```

### Produce and consume a message via the Pulsar client

```bash
# Easiest: use the pulsar-client tool from inside the broker container
docker compose exec pulsar-broker bin/pulsar-client produce \
  persistent://public/default/demo --messages "hello from rust bookkeeper"

docker compose exec pulsar-broker bin/pulsar-client consume \
  persistent://public/default/demo --subscription-name s1 --num-messages 0
```

You should see the message content being read back — all underlying
storage went through the Rust bookie, readable by the Java client
unchanged.

### Inspect ledgers via the Rust CLI

```bash
docker compose exec rust-bookie-1 bk listledgers --zk zookeeper:2181
docker compose exec rust-bookie-1 bk clusterinfo --zk zookeeper:2181
docker compose exec rust-bookie-1 bk whoisauditor --zk zookeeper:2181
docker compose exec rust-bookie-1 bk listbookies --zk zookeeper:2181 --rw
```

### Exercise replication + recovery (the point of running 3 bookies)

```bash
# Kill one bookie — Pulsar keeps working with W=2, A=2
docker compose stop rust-bookie-2

# Produce/consume a few messages — should continue to work
docker compose exec pulsar-broker bin/pulsar-client produce \
  persistent://public/default/demo --messages "during-outage"

# Bring it back — the auditor detects under-replicated ledgers and
# the worker copies missing fragments to the recovered bookie
docker compose start rust-bookie-2
docker compose logs -f rust-bookie-1 | grep -i "auditor\|replicat"
```

## Configuration

- **Rust bookie config:** `docker/bk_server.conf` (mounted into
  `/etc/bookkeeper/bk_server.conf`). Edit and `docker compose up -d
  rust-bookie --force-recreate` to apply.
- **Pulsar broker config:** passed via `PULSAR_PREFIX_*` environment
  variables in `docker-compose.yml`. The Pulsar image's entrypoint
  (`bin/apply-config-from-env.py`) rewrites `conf/broker.conf` at
  startup.

## Replication config

Pulsar is configured for `E=3 / W=2 / A=2`:
- **Ensemble size 3** — each managed ledger spans all 3 bookies
- **Write quorum 2** — every entry is written to 2 of the 3
- **Ack quorum 2** — the client waits for 2 acknowledgments

This tolerates one bookie failure without data loss or write
interruption. Killing a bookie triggers the auditor (running in-
process on every Rust bookie) to mark affected ledgers as
under-replicated; the replication worker then copies missing
fragments once the bookie comes back. Both code paths are exercised
by the integration tests under
`crates/bookkeeper-replication/tests/worker_e2e_test.rs`.

## Troubleshooting

### A `rust-bookie-N` keeps exiting or never becomes healthy

```bash
docker compose logs rust-bookie-1
```

Common causes:
- Docker can't reach the `zookeeper` service — check `docker compose ps`
  shows zookeeper as `healthy`.
- The bookie is stuck in ReadOnly because `readOnlyModeEnabled=true`
  in the mounted config plus a >95% full host disk. `bk_server.conf`
  disables this by default for the playground.
- Two bookies accidentally registered under the same identity (the
  same `--advertised-address`). Each service must pass a unique
  `--advertised-address rust-bookie-N`.

### Pulsar broker errors with `BKNotEnoughBookiesException`

The broker is trying to allocate a 3-bookie ensemble but fewer than 3
are reachable via ZK. Usually this means:
- One or more bookies registered with the wrong advertised address
  (e.g., `0.0.0.0:3181`, which Java clients can't connect to).
- Fewer than 3 bookies finished their ZK registration before the broker
  tried to allocate its first ledger.

Check what's registered:
```bash
docker compose exec zookeeper zkCli.sh ls /ledgers/available
```

Expected: `[rust-bookie-1, rust-bookie-2, rust-bookie-3, readonly]`

If you see `0.0.0.0:3181` in the list, the `--advertised-address` flag
didn't take effect — verify each service's `command:` in the compose
file passes the correct override.

### Port 8080 collision

Pulsar broker uses 8080 on the host. If something else is already
bound there, edit the `pulsar-broker` service's port mapping in
`docker-compose.yml`.

### Build takes forever

The first `docker compose up` builds the Rust image, which includes
compiling RocksDB from source (~5-10 min, depending on CPU). After
that, unchanged workspace code uses cached layers and rebuilds in
seconds. To force a rebuild: `docker compose build --no-cache rust-bookie`.

## What this validates

A working `docker compose up` proves that the Rust bookie:
- Binds the V2/V3 protocol correctly
- Registers with ZK using a client-reachable advertised address
- Accepts AddEntry/ReadEntry calls from the real Java BookKeeper client
  used by Pulsar
- Correctly serves fence, recovery, and LAC-tail-read operations that
  Pulsar uses on every ledger creation and close

If Pulsar's `pulsar-client produce/consume` round-trip works, you've
just done the same thing the gap-#3 Java compat test suite does, but
against a real Pulsar broker with real topic/subscription/cursor
machinery on top.
