# etcd-rust

A high-performance, etcd-compatible key-value store built in Rust. Derived from the [mem_etcd](https://github.com/bchess/k8s-1m) component of the [k8s-1m](https://github.com/bchess/k8s-1m) project, independently forked to pursue a different set of trade-offs.

## Design Philosophy

- **Distributed consensus** — Multi-Raft architecture for high availability and horizontal scalability, without sacrificing performance
- **Performance** — Targeting higher throughput than etcd; accepting a gap vs the original single-node mem_etcd in exchange for distributed safety
- **Compatibility** — Tracks the latest Kubernetes version's etcd API usage, not the latest etcd release
- **Durability** — Data persistence at massive scale, with acceptance of worst-case data loss (e.g., multi-node power loss affecting the latest replicated entries)

## Quick Start

### Build

```bash
cargo build --release
```

### Run (single-node)

```bash
./target/release/mem_etcd --port 2379 --metrics-port 9000
```

### Run (3-node Raft cluster)

```bash
# Node 1 (initializes cluster)
./target/release/mem_etcd \
  --port 23791 --metrics-port 9001 \
  --raft-enabled --raft-node-id 1 \
  --raft-peers "1@localhost:23791,2@localhost:23792,3@localhost:23793" \
  --raft-init

# Node 2
./target/release/mem_etcd \
  --port 23792 --metrics-port 9002 \
  --raft-enabled --raft-node-id 2 \
  --raft-peers "1@localhost:23791,2@localhost:23792,3@localhost:23793"

# Node 3
./target/release/mem_etcd \
  --port 23793 --metrics-port 9003 \
  --raft-enabled --raft-node-id 3 \
  --raft-peers "1@localhost:23791,2@localhost:23792,3@localhost:23793"
```

### Docker

```bash
docker build -t etcd-rust .
docker run -p 2379:2379 etcd-rust
```

### Connect with etcdctl

```bash
ETCDCTL_API=3 etcdctl --endpoints=http://localhost:2379 put foo bar
ETCDCTL_API=3 etcdctl --endpoints=http://localhost:2379 get foo
```

## Configuration

| Flag | Env | Default | Description |
|---|---|---|---|
| `--port` | `ETCD_PORT` | 2379 | gRPC listen port (etcd API + Raft transport) |
| `--metrics-port` | `ETCD_METRICS_PORT` | 9000 | Prometheus metrics port |
| `--wal-dir` | `ETCD_WAL_DIR` | `./wal` | WAL directory path |
| `--wal-default` | — | `buffered` | Default WAL mode (`none`, `buffered`, `fsync`) |
| `--raft-enabled` | `RAFT_ENABLED` | false | Enable Raft consensus mode |
| `--raft-node-id` | `RAFT_NODE_ID` | — | This node's Raft ID (1-based) |
| `--raft-peers` | `RAFT_PEERS` | — | Peer list: `1@host:port,2@host:port,...` |
| `--raft-init` | `RAFT_INIT` | false | Initialize the cluster (first node only) |

## Testing

```bash
# Unit tests
cargo test

# Raft integration tests
cargo test --test raft_test

# Linearizability verification (requires Go)
cd tests/porcupine-test && go run main.go
```

## Acknowledgments

- **Ben Chess** — Original mem_etcd implementation as part of [k8s-1m](https://github.com/bchess/k8s-1m)
- [openraft](https://github.com/databendlabs/openraft) — Raft consensus library
- [tonic](https://github.com/hyperium/tonic) — gRPC framework for Rust

## License

Apache License 2.0 — See [LICENSE](LICENSE).
