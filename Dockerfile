FROM rust:1.75-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y libclang-dev cmake
COPY . .
RUN cargo build --release -p bookkeeper-cli

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/bk /usr/local/bin/bk
EXPOSE 3181 8080
ENTRYPOINT ["bk"]
CMD ["serve"]
