# Multi-stage build for the Rust BookKeeper bookie (`bk` binary).
#
# Uses `cargo-chef` to cache dependency compilation across source edits.
# Without it, every code change invalidates the COPY layer and forces a
# full rebuild of rocksdb-sys (~60s of C++ compile). With chef, deps are
# rebuilt only when Cargo.lock or a Cargo.toml manifest changes.
#
# cargo-chef caches deps PER-PROFILE. We need both release deps (for the
# production runtime image) AND dev deps (for the test stage and the
# debug-runtime image), so we have parallel `*-builder` stages.
#
# Stages:
#   chef           — base image: rust + system deps + cargo-chef
#   planner        — compute recipe.json from Cargo.toml/Cargo.lock
#   builder        — release-profile cook + cargo build (used by `runtime`)
#   debug-builder  — dev-profile cook + cargo build (used by `test` and
#                    `debug-runtime`)
#   test           — runs `cargo test` against debug-builder
#   runtime        — slim production image with the release `bk` binary
#   debug-runtime  — slim image with the debug `bk` binary (assertions on)
#
# Build targets:
#   docker build --target runtime .         # production image
#   docker build --target debug-runtime .   # debug image
#   docker build --target test .            # test runner image

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
# Release builder: compiles all deps in release profile (cached on
# recipe.json), then copies source and builds the workspace itself.
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p bookkeeper-cli
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p bookkeeper-cli

# ----------------------------------------------------------------------------
# Debug builder: parallel cache for the dev profile. Used by the test stage
# (which runs `cargo test --workspace`, dev profile by default) and by the
# debug-runtime image. The cook only includes deps reachable from the cli
# binary; the test stage's full-workspace test will re-cook a few extra
# transitive deps the first time but the heavy ones (rocksdb-sys etc.)
# are already cached.
FROM chef AS debug-builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --recipe-path recipe.json -p bookkeeper-cli
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build -p bookkeeper-cli

# ----------------------------------------------------------------------------
# Test stage: reuses the debug-builder (deps + source + compiled artifacts).
# Used by docker-compose-test.yml to run the test suite inside Linux with
# io_uring support (Debian Bookworm ships kernel 6.1).
FROM debug-builder AS test
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
# mounting a different file at the same path.
COPY docker/bk_server.conf /etc/bookkeeper/bk_server.conf

# Data and journal live here — mount volumes for persistence
RUN mkdir -p /bk/data /bk/journal && chown -R bookkeeper:bookkeeper /bk /etc/bookkeeper

USER bookkeeper
WORKDIR /bk

# Bookie V2/V3 protocol port and HTTP admin API port
EXPOSE 3181 8080

ENTRYPOINT ["bk"]
CMD ["serve", "--conf", "/etc/bookkeeper/bk_server.conf"]

# ----------------------------------------------------------------------------
# Debug runtime image: same shape as `runtime`, but with the dev-profile
# binary (debug assertions enabled, faster compile time, larger binary).
# Use for in-cluster reproduction of bugs that only show up under debug
# assertions.
FROM debian:bookworm-slim AS debug-runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl && \
    rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --home-dir /bk --shell /bin/bash bookkeeper

COPY --from=debug-builder /app/target/debug/bk /usr/local/bin/bk
COPY docker/bk_server.conf /etc/bookkeeper/bk_server.conf

RUN mkdir -p /bk/data /bk/journal && chown -R bookkeeper:bookkeeper /bk /etc/bookkeeper

USER bookkeeper
WORKDIR /bk

EXPOSE 3181 8080

ENTRYPOINT ["bk"]
CMD ["serve", "--conf", "/etc/bookkeeper/bk_server.conf"]
