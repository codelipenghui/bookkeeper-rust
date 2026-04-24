# bookkeeper-rust

A Rust implementation of [Apache BookKeeper](https://bookkeeper.apache.org/) —
the low-latency, log-structured write-ahead store that powers Apache Pulsar.

The bookie speaks the BookKeeper V2 and V3 wire protocols, so a
bookkeeper-rust bookie is a drop-in replacement for a Java bookie from
the perspective of any Java BookKeeper client — including the
BookKeeper client embedded in Apache Pulsar.

> Status: experimental. Wire-compatible with Apache BookKeeper 4.x and
> validated against Apache Pulsar 3.3 in the playground compose stack.
> Not yet recommended for production.

## Highlights

- **Wire-compatible bookie** — V2 and V3 framed protocols, RocksDB
  entry-location index, journal + entry-log storage, LAC tail-read,
  fence, and ledger recovery. The Java Pulsar broker reads and writes
  ledgers on a Rust bookie without any client-side change.
- **Two I/O engines** — a portable `std` blocking-threadpool engine and
  a Linux-only `io_uring` engine. Selected automatically at startup or
  overridden via `BOOKKEEPER_IO_ENGINE` / `--io-engine`.
- **Two metadata backends** — ZooKeeper (default, hierarchical layout
  matching Apache BK) and [Oxia](https://github.com/oxia-db/oxia).
  The Oxia keys are bit-identical to Apache Pulsar's oxia metadata
  driver, so a Rust bookie can join a Pulsar+Oxia cluster. See
  [docs/metadata-backends.md](docs/metadata-backends.md).
- **In-process auditor + replication worker** — every bookie runs the
  auditor election, under-replicated scan, and fragment re-replication
  loop, matching the Java `bookkeeper-server-autorecovery` component.
- **Admin HTTP API and Prometheus metrics** — `/api/v1/bookie/...` and
  `/metrics` on the HTTP port; `pprof` flamegraph endpoint available in
  the `debug-runtime` image / `--features profiling` build.
- **`bk` CLI** — a single binary with subcommands covering bookie
  serve, cluster init, ledger listing/reading, recovery, decommission,
  cookie management, benchmarking, and the sanity tests.

## Quick start

### Build locally

Requires Rust 1.90+, `cmake`, `clang`, `libclang`, and `protoc` (for
RocksDB and Prost):

```bash
cargo build --release -p bookkeeper-cli
./target/release/bk --help
```

Run a single-node sanity check (starts a bookie in a temp dir, writes
and reads entries, tears down):

```bash
cargo run -p bookkeeper-cli -- bookiesanity --entries 100
```

### 3-bookie cluster + Pulsar broker (docker-compose)

The fastest way to see the bookie working end-to-end with a real Java
Pulsar client is the compose playground:

```bash
docker compose up
# Then, in another shell:
docker compose exec pulsar-broker bin/pulsar-client produce \
    persistent://public/default/demo --messages "hello from rust bookkeeper"
docker compose exec pulsar-broker bin/pulsar-client consume \
    persistent://public/default/demo --subscription-name s1 --num-messages 0
```

See [docker/README.md](docker/README.md) for the full tour — 3-bookie
ensemble, smoke tests, failure-injection walkthrough, and
troubleshooting.

An Oxia-backed variant is available via
`docker compose -f docker-compose-oxia.yml up`.

## Workspace layout

```
crates/
├── bookkeeper-proto         # Generated protobuf types (BookkeeperProtocol.proto)
├── bookkeeper-common        # Shared error, type, and disk utilities
├── bookkeeper-protocol      # V2/V3 framing, digest, entry codec
├── bookkeeper-metadata      # Cookie, ledger metadata, backend trait (ZK + Oxia)
├── bookkeeper-storage       # Journal, entry log, RocksDB index, GC, LAC
├── bookkeeper-server        # Bookie server: processor, HTTP API, metrics, scrub
├── bookkeeper-client        # Ledger read/write handles, placement, recovery
├── bookkeeper-replication   # Auditor, election, worker, under-replication
├── bookkeeper-config        # bk_server.conf / client-conf parser
├── bookkeeper-cli           # `bk` binary — serve, admin, bench, sanity
├── bookkeeper-test-utils    # Shared ZK-container + cluster fixtures (dev only)
└── bookkeeper-upgrade-test  # Rolling-upgrade integration test
```

## Running the test suite

The workspace tests exercise both I/O engines, the wire protocol, and
the replication / recovery code paths against a real ZooKeeper
container. On Linux:

```bash
cargo test --workspace
```

On macOS the `io_uring` engine won't compile, so run the io_uring
paths inside Linux via docker-compose:

```bash
docker compose -f docker-compose-test.yml run --rm test-uring-only
docker compose -f docker-compose-test.yml run --rm test-all
```

A separate Java-compatibility harness
(`tests/java-compat/run-tests.sh`) builds a Maven project that uses
the official Java BookKeeper client against a running Rust bookie.

## Documentation

- [docs/metadata-backends.md](docs/metadata-backends.md) — configuring
  ZooKeeper vs. Oxia, Pulsar interop notes.
- [docs/benchmark-design.md](docs/benchmark-design.md) — benchmark
  taxonomy (direct-bookie, end-to-end, microbenchmarks).
- [docs/benchmark-test-plan.md](docs/benchmark-test-plan.md) —
  load-test matrix and methodology.
- [docker/README.md](docker/README.md) — compose playground and
  failure-injection walkthrough.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). This is an
independent Rust implementation; it is not affiliated with or endorsed
by the Apache Software Foundation or the Apache BookKeeper project.
