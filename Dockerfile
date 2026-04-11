# Multi-stage build for the Rust BookKeeper bookie (`bk` binary).
#
# Uses `cargo-chef` to cache dependency compilation across source edits.
# Without it, every code change invalidates the COPY layer and forces a
# full rebuild of rocksdb-sys (~60s of C++ compile). With chef, deps are
# rebuilt only when Cargo.lock or a Cargo.toml manifest changes.
#
# Stages:
#   chef     — base image: rust + system deps + cargo-chef
#   planner  — compute recipe.json from Cargo.toml/Cargo.lock (cache key
#              for the deps build is the recipe, not the source tree)
#   builder  — `cargo chef cook` builds and caches all deps, then copies
#              source and builds only the workspace code
#   test     — reuses builder, runs `cargo test`
#   runtime  — slim production image with just the `bk` binary

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
# Builder: compile dependencies first (cached on recipe.json), then copy
# source and build the workspace itself. Editing only Rust source skips
# straight to the final `cargo build`, with rocksdb-sys etc. already
# compiled and cached in the previous layer.
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p bookkeeper-cli
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p bookkeeper-cli

# ----------------------------------------------------------------------------
# Test stage: reuses the builder (all deps + source + compiled artifacts).
# Used by docker-compose-test.yml to run the test suite inside Linux with
# io_uring support (Debian Bookworm ships kernel 6.1).
FROM builder AS test
WORKDIR /app
ENV BOOKKEEPER_IO_ENGINE=uring
CMD ["cargo", "test", "--workspace", "--", \
     "--skip", "java_bookie_container", "--skip", "java_client"]

# ----------------------------------------------------------------------------
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
