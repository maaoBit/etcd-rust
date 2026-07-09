# etcd-rust

A high-performance, etcd-compatible key-value store built in Rust. Designed as a drop-in replacement for [etcd](https://github.com/etcd-io/etcd) in large-scale Kubernetes clusters, with a focus on **stability**, **throughput**, and **operational simplicity**.

## Background

This project is derived from the [mem_etcd](https://github.com/bchess/k8s-1m) component of the [k8s-1m](https://github.com/bchess/k8s-1m) project, originally created by Ben Chess. It has been independently forked to pursue a different design philosophy.

### Design Philosophy

The original k8s-1m project takes an aggressive approach to Kubernetes scalability, pushing toward 1 million nodes. This project takes a more **conservative, production-oriented** path:

| | etcd | k8s-1m (mem_etcd) | etcd-rust (this project) |
|---|---|---|---|
| **Stability** | Battle-tested, stable | Experimental, unproven | Priority — every change must be verifiable |
| **Performance** | Baseline | Aggressive optimizations, accepts correctness trade-offs | Better than etcd, but never at the cost of correctness |
| **Consensus** | Multi-node Raft (etcd-raft) | Single-node only (original), Raft added later | Full Raft consensus from the start, with linearizability verification |
| **Compatibility** | Full etcd API | Subset of etcd API | Same subset, but tested for real-world K8s workloads |
| **Persistence** | BoltDB (B-tree on disk) | In-memory + optional WAL | In-memory + configurable WAL (per-prefix fsync control) |

**Key principles:**

1. **Correctness first** — All optimizations must pass linearizability testing (verified via [Porcupine](https://github.com/anishathalye/porcupine))
2. **Incremental improvement** — Each iteration is small, testable, and reversible
3. **Production-ready** — Not chasing benchmark numbers; stability under real workloads matters more
4. **etcd API compatibility** — Works with standard Kubernetes components without modification

## Architecture

### Core data structures

- **DashMap** — Concurrent hash map storing the full keyspace
- **BTreeMap per prefix** — Indexes keys within each `/registry/[$APIGROUP/]$APIKIND/[$NAMESPACE/]` prefix, enabling efficient range queries
- **BlockDeque** — Custom lock-free block-based array for O(1) revision history storage
- **Per-prefix WAL** — Each key prefix gets its own WAL file, allowing independent fsync policies

### Write path

```
Client → gRPC (tonic) → KvService → Store.set()
                                        ├── WAL write (async/sync/per-prefix config)
                                        ├── DashMap + BTreeMap update
                                        └── Watch notification (async, buffered)
```

- Writes to existing keys: **O(1)**
- Writes to new keys: **O(log n)** (where n = resources of that Kind)
- Range queries: **O(log n + limit)**

### Raft consensus

Multi-node deployment uses [openraft](https://github.com/databendlabs/openraft) v0.9:

- **Write path**: Put/DeleteRange → Raft `client_write()` → replicated → applied to StateMachine (Store)
- **Read path**: Local follower reads (no Raft round-trip for reads)
- **Leader election**: Automatic failover (~8s for new leader election)
- **Transport**: gRPC-based Raft RPC (same port as etcd API)

```
                    ┌─────────────────────────┐
  Client ──gRPC──→  │  Node 1 (Leader)        │
                    │  ├── etcd API (gRPC)     │
                    │  ├── Raft Transport      │
                    │  └── Store (in-memory)   │
                    └──────┬──────────┬────────┘
                     Raft  │          │  Raft
                  append   │          │  append
                    ┌──────▼──┐   ┌───▼───────┐
                    │ Node 2  │   │  Node 3   │
                    │ (Follower)│   │ (Follower)│
                    └─────────┘   └───────────┘
```

### WAL (Write-Ahead Log)

Per-prefix WAL files provide durability without BoltDB overhead:

| Mode | Behavior | Use case |
|---|---|---|
| `None` | No WAL, data is ephemeral | Testing, ephemeral workloads |
| `Buffered` (default) | WAL written, `fsync` deferred | Max throughput, acceptable durability |
| `Fsync` | WAL written + `fsync` before response | Strong durability guarantee |

Per-prefix configuration allows fine-grained control — e.g., `fsync` for critical prefixes while buffering high-volume ones.

## Supported etcd APIs

| API | Methods | Status |
|---|---|---|
| **KV** | Range, Put, DeleteRange, Txn, Compact | Implemented |
| **Watch** | Watch (streaming) | Implemented |
| **Lease** | Grant, Revoke, KeepAlive | Implemented |
| **Maintenance** | Status, Hash, Snapshot, Defragment | Partial |

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
| `--wal-no-write-prefix` | — | — | Prefixes to skip WAL entirely |
| `--raft-enabled` | `RAFT_ENABLED` | false | Enable Raft consensus mode |
| `--raft-node-id` | `RAFT_NODE_ID` | — | This node's Raft ID (1-based) |
| `--raft-peers` | `RAFT_PEERS` | — | Peer list: `1@host:port,2@host:port,...` |
| `--raft-init` | `RAFT_INIT` | false | Initialize the cluster (first node only) |

## Metrics

Prometheus metrics are exposed at `http://localhost:9000/metrics`:

- `etcd_revision_count` — Current revision
- `etcd_compacted_revision_count` — Last compacted revision
- `etcd_watcher_count` — Active watchers
- `etcd_in_flight_requests` — Current in-flight gRPC requests
- `tree_map_size_bytes` — Estimated memory usage of stored values
- Raft metrics (leader, term, state, etc.)

## Testing

### Unit tests

```bash
cargo test
```

### Raft integration tests

```bash
cargo test --test raft_test
```

Tests cover: leader election, write replication, delete replication, read consistency, revision monotonicity, data integrity, and leader failover.

### Linearizability verification

The `tests/porcupine-test/` directory contains a Go program that runs concurrent clients against a 3-node Docker cluster and verifies linearizability using [Porcupine](https://github.com/anishathalye/porcupine):

```bash
cd tests/porcupine-test
go run main.go
```

### Stress testing

```bash
cd stress-client
cargo run -- --help
```

## Project Structure

```
.
├── src/
│   ├── main.rs              # Entry point, CLI parsing, server setup
│   ├── lib.rs              # Public API exports
│   ├── store.rs            # Core in-memory KV store with revision tracking
│   ├── kv_service.rs       # etcd KV gRPC service (Range, Put, DeleteRange, Txn, Compact)
│   ├── watch_service.rs    # etcd Watch gRPC service (streaming)
│   ├── lease_service.rs    # etcd Lease gRPC service
│   ├── maintenance_service.rs # etcd Maintenance gRPC service
│   ├── raft.rs             # Raft consensus (openraft integration)
│   ├── wal.rs              # Per-prefix Write-Ahead Log
│   ├── block_deque.rs      # Lock-free block-based array for revision storage
│   └── metrics.rs          # Prometheus metrics
├── proto/
│   └── raft_transport.proto # Raft gRPC transport protocol
├── extern/
│   ├── etcd/api/           # etcd protobuf definitions (vendored)
│   ├── google/             # Google well-known protobuf types (vendored)
│   └── tonic-mock/         # tonic mocking utilities (vendored)
├── tests/
│   ├── kv_service_test.rs
│   ├── store_test.rs
│   ├── watch_service_test.rs
│   ├── watch_test.rs
│   ├── raft_test.rs
│   └── porcupine-test/     # Linearizability checker (Go)
├── stress-client/          # Load testing client
├── build.rs                # Protobuf code generation
├── Cargo.toml
└── Dockerfile
```

## Roadmap

- [ ] Snapshot support for Raft (currently in-memory log only)
- [ ] Persistent Raft log (survive node restart without WAL replay)
- [ ] Authentication (etcd Auth API)
- [ ] Compaction strategy improvements
- [ ] Benchmark suite against etcd under K8s-like workloads
- [ ] Multi-architecture Docker images (amd64/arm64)

## Acknowledgments

- **Ben Chess** — Original mem_etcd implementation as part of [k8s-1m](https://github.com/bchess/k8s-1m)
- [openraft](https://github.com/databendlabs/openraft) — Raft consensus library
- [tonic](https://github.com/hyperium/tonic) — gRPC framework for Rust
- [etcd](https://github.com/etcd-io/etcd) — The project we aim to be compatible with

## License

Apache License 2.0 — See [LICENSE](LICENSE).
