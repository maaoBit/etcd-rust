FROM rust:1.85-bookworm AS builder

RUN apt-get update && apt-get install -y protobuf-compiler libprotobuf-dev pkg-config && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy source
COPY . .

# Build
RUN cargo build --release

# Output binary
FROM debian:bookworm-slim
COPY --from=builder /app/target/release/mem_etcd /mem_etcd
ENTRYPOINT ["/mem_etcd"]
