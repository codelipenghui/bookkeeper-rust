# Multi-stage build for the Rust BookKeeper bookie (`bk` binary).
#
# Stage 1: build the release `bk` binary with RocksDB bundled.
# Stage 2: copy the binary into a slim runtime image, along with a default
#          docker-compose-friendly config.

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
