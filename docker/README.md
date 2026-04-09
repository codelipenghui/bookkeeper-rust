# Rust BookKeeper + Pulsar docker-compose playground

A 5-service compose stack that lets you run Apache Pulsar against the
Rust BookKeeper implementation on a laptop.

```
pulsar-broker  ────┐
                   │  BookKeeper V3 wire protocol
                   ▼
              rust-bookie  ────┐
                               │  /ledgers/*
                               ▼
                           zookeeper
                               ▲
                               │  metadata URI
pulsar-broker  ────────────────┘
```

## Stack

| Service         | Image                       | Purpose |
|-----------------|-----------------------------|---------|
| `zookeeper`     | `zookeeper:3.8`             | Shared metadata store for BK + Pulsar |
| `bk-init`       | (built from this repo)      | One-shot: `bk initnewcluster` (idempotent) |
| `rust-bookie`   | (built from this repo)      | The Rust bookie under test |
| `pulsar-init`   | `apachepulsar/pulsar:3.3.5` | One-shot: `pulsar initialize-cluster-metadata` (idempotent) |
| `pulsar-broker` | `apachepulsar/pulsar:3.3.5` | Broker only (no bundled BK/ZK) |

## Ports

| Service         | Container | Host  | Purpose                       |
|-----------------|-----------|-------|-------------------------------|
| `zookeeper`     | 2181      | 2181  | ZK client protocol            |
| `rust-bookie`   | 3181      | 3181  | BK V2/V3 wire protocol        |
| `rust-bookie`   | 8080      | 8081  | BK HTTP admin API             |
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
docker compose logs -f rust-bookie
docker compose logs -f pulsar-broker

# Tear down, keep data:
docker compose down

# Tear down + nuke all ZK / bookie / Pulsar data:
docker compose down -v
```

## Smoke tests

### Verify the Rust bookie is healthy

```bash
curl http://localhost:8081/api/v1/bookie/is_ready
# → 200 OK

curl http://localhost:8081/api/v1/bookie/state | jq
# → { "running": true, "readOnly": false, ... }

curl http://localhost:8081/metrics | head
# → Prometheus metrics
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
docker compose exec rust-bookie bk listledgers --zk zookeeper:2181
docker compose exec rust-bookie bk clusterinfo --zk zookeeper:2181
docker compose exec rust-bookie bk whoisauditor --zk zookeeper:2181
```

## Configuration

- **Rust bookie config:** `docker/bk_server.conf` (mounted into
  `/etc/bookkeeper/bk_server.conf`). Edit and `docker compose up -d
  rust-bookie --force-recreate` to apply.
- **Pulsar broker config:** passed via `PULSAR_PREFIX_*` environment
  variables in `docker-compose.yml`. The Pulsar image's entrypoint
  (`bin/apply-config-from-env.py`) rewrites `conf/broker.conf` at
  startup.

## Single-bookie caveats

This playground runs exactly one bookie, so all Pulsar managed ledgers
are configured with ensemble=write=ack=1. Do NOT use this config in
production — any bookie failure loses data. For durability, run at
least 3 bookies and raise the quorum sizes.

## Troubleshooting

### `rust-bookie` keeps exiting or never becomes healthy

```bash
docker compose logs rust-bookie
```

Common causes:
- Docker can't reach the `zookeeper` service — check `docker compose ps`
  shows zookeeper as `healthy`.
- The bookie is stuck in ReadOnly because `readOnlyModeEnabled=true`
  in the mounted config plus a >95% full host disk. `bk_server.conf`
  disables this by default for the playground.

### Pulsar broker errors with `BKNotEnoughBookiesException`

The broker is trying to allocate a ledger ensemble but can't reach
the bookie. Usually this means:
- The Rust bookie's `advertisedAddress` doesn't match its docker
  service name. Should be `rust-bookie` (verify in `bk_server.conf`).
- The bookie hasn't finished registering in ZK yet — the broker's
  `depends_on` healthcheck should prevent this, but on slow laptops
  the 30s `start_period` may be too short.

Check what's registered:
```bash
docker compose exec zookeeper zkCli.sh ls /ledgers/available
```

Expected: `[rust-bookie, readonly]`

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
