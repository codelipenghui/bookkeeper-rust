# Multi-stage build for the Rust BookKeeper bookie (`bk` binary).
#
# Stages:
#   builder  — compile workspace with RocksDB bundled
#   test     — run cargo test with io_uring (used by docker-compose-test.yml)
#   runtime  — slim production image with just the `bk` binary

FROM rust:1.90-bookworm AS builder
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

# Copy the whole workspace. A .dockerignore keeps target/ and other bulky
# paths out of the build context.
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
