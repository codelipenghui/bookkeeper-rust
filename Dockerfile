# Multi-stage build for the Rust BookKeeper bookie (`bk` binary).
#
# Uses `cargo-chef` to cache dependency compilation across source edits.
# Without it, every code change invalidates the COPY layer and forces a
# full rebuild of rocksdb-sys (~60s of C++ compile). With chef, deps are
# rebuilt only when Cargo.lock or a Cargo.toml manifest changes.
#
# Two production-shaped images:
#
#   runtime        — production binary. `--release` profile, no debug
#                    info, no symbols, no `pprof` dependency. Smallest
#                    image and fastest binary, but no profiling endpoint.
#   debug-runtime  — debugging binary. `--profile release-debug` (same
#                    optimization as `release` plus line-tables-only
#                    DWARF) with `--features profiling`. The
#                    `/debug/pprof/flamegraph` endpoint is compiled in
#                    and stack traces are usable.
#
# Plus a `test` stage used by docker-compose-test.yml that runs the
# workspace test suite in dev profile.
#
# Each profile has its own cargo-chef cache, so cold builds are paid
# only once per profile and source edits never recompile rocksdb-sys.
#
# Build targets:
#   docker build --target runtime .         # production (default)
#   docker build --target debug-runtime .   # debug image with profiling
#   docker build --target test .            # test runner

FROM rust:1.90-bookworm AS chef
WORKDIR /app
# System deps:
#   libclang-dev / clang / cmake — for the RocksDB crate's C++ build
#   protobuf-compiler            — for bookkeeper-proto's prost-build
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libclang-dev \
        cmake \
        clang \
        protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Optional corporate / proxy CA bundles. If you're building behind a
# TLS-inspection proxy (Zscaler, mitmproxy, etc.) that breaks cargo's
# outbound HTTPS to crates.io, drop the proxy's root cert(s) as *.crt
# files into `docker/extra-ca-certs/` on your working copy — this step
# merges them into the builder's trust store. The directory is tracked
# via .gitkeep so this COPY always succeeds; actual certs are
# gitignored. No-op if the directory only contains .gitkeep.
COPY docker/extra-ca-certs/ /usr/local/share/ca-certificates/extra/
RUN update-ca-certificates

RUN cargo install cargo-chef --locked

# ----------------------------------------------------------------------------
# Planner: extract the dependency manifest into recipe.json. The recipe is
# computed only from Cargo.toml + Cargo.lock + each crate's manifest, so
# its content is independent of source-file edits — which means the
# downstream `cargo chef cook` layer stays cached as long as no manifest
# or lockfile changes.
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

# ----------------------------------------------------------------------------
# Release builder: production binary. Stripped, no pprof, no debug info.
# Compiles all deps in release profile (cached on recipe.json), then
# copies source and builds the workspace itself.
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p bookkeeper-cli
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY docker ./docker
RUN cargo build --release -p bookkeeper-cli

# ----------------------------------------------------------------------------
# Debug builder: same optimization as release, but with line-tables-only
# debug info AND the `profiling` feature so the pprof flamegraph endpoint
# is compiled in. Output goes to target/release-debug/bk (custom profile).
FROM chef AS debug-builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook \
        --profile release-debug \
        --features bookkeeper-cli/profiling \
        --recipe-path recipe.json \
        -p bookkeeper-cli
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY docker ./docker
RUN cargo build \
        --profile release-debug \
        --features profiling \
        -p bookkeeper-cli

# ----------------------------------------------------------------------------
# Test builder: dev profile, no features. Cached separately because dev
# differs from both release and release-debug.
FROM chef AS test-builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --recipe-path recipe.json -p bookkeeper-cli
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY docker ./docker
RUN cargo build -p bookkeeper-cli

# ----------------------------------------------------------------------------
# Test stage: runs the workspace test suite. Used by docker-compose-test.yml
# to run tests inside Linux with io_uring (Debian Bookworm has kernel 6.1).
FROM test-builder AS test
WORKDIR /app
ENV BOOKKEEPER_IO_ENGINE=uring
CMD ["cargo", "test", "--workspace", "--", \
     "--skip", "java_bookie_container", "--skip", "java_client"]

# ----------------------------------------------------------------------------
# Production runtime image: slim Debian + the release-profile binary.
FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Non-root user for the bookie process
RUN useradd --system --create-home --home-dir /bk --shell /bin/bash bookkeeper

COPY --from=builder /app/target/release/bk /usr/local/bin/bk

# Default bookie config used by docker-compose. Can be overridden by
# mounting a different file at the same path. The rocksdb subdirectory
# holds Java BK-style INI tuning files referenced from bk_server.conf.
COPY docker/bk_server.conf /etc/bookkeeper/bk_server.conf
COPY docker/rocksdb /etc/bookkeeper/rocksdb

# Data and journal live here — mount volumes for persistence
RUN mkdir -p /bk/data /bk/journal && chown -R bookkeeper:bookkeeper /bk /etc/bookkeeper

USER bookkeeper
WORKDIR /bk

# Bookie V2/V3 protocol port and HTTP admin API port
EXPOSE 3181 8080

ENTRYPOINT ["bk"]
CMD ["serve", "--conf", "/etc/bookkeeper/bk_server.conf"]

# ----------------------------------------------------------------------------
# Debug runtime image: same Debian base as `runtime`, but with the
# release-debug binary (release-level optimization + line-tables debug
# info + the `profiling` feature so /debug/pprof/flamegraph is available).
# Use this when you need to capture flamegraphs or investigate a problem
# in a production-like cluster.
FROM debian:bookworm-slim AS debug-runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl && \
    rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --home-dir /bk --shell /bin/bash bookkeeper

COPY --from=debug-builder /app/target/release-debug/bk /usr/local/bin/bk
COPY docker/bk_server.conf /etc/bookkeeper/bk_server.conf
COPY docker/rocksdb /etc/bookkeeper/rocksdb

RUN mkdir -p /bk/data /bk/journal && chown -R bookkeeper:bookkeeper /bk /etc/bookkeeper

USER bookkeeper
WORKDIR /bk

EXPOSE 3181 8080

ENTRYPOINT ["bk"]
CMD ["serve", "--conf", "/etc/bookkeeper/bk_server.conf"]
