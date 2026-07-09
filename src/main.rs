// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
mod block_deque;
mod kv_service;
mod lease_service;
mod metrics;
mod raft;
mod store;
mod watch_service;
mod wal;
mod maintenance_service;
mod membership;
mod lease;
mod alarm;
mod election;
mod lock;

use etcdserverpb::kv_server::KvServer;
use etcdserverpb::maintenance_server::MaintenanceServer;
use etcdserverpb::lease_server::LeaseServer;
use etcdserverpb::watch_server::WatchServer;
use v3electionpb::election_server::ElectionServer;
use v3lockpb::lock_server::LockServer;

use bytes::Bytes;
use std::sync::Arc;
use std::path::PathBuf;
use clap::{Parser, ValueEnum};
use dashmap::DashSet;

use kv_service::KvService;
use lease_service::LeaseService;
use maintenance_service::MaintenanceService;
use std::net::SocketAddr;
use tonic::transport::Server;
use tower_http::metrics::InFlightRequestsLayer;
use watch_service::WatchService;
use election::ElectionService;
use lock::LockService;
use lease::lessor::Lessor;

use prometheus::{TextEncoder, Encoder};
use axum::{
    routing::get,
    Router,
};

mod authpb {
    tonic::include_proto!("authpb");
}

mod mvccpb {
    tonic::include_proto!("mvccpb");
}

mod etcdserverpb {
    tonic::include_proto!("etcdserverpb");
}

mod v3electionpb {
    tonic::include_proto!("v3electionpb");
}

mod v3lockpb {
    tonic::include_proto!("v3lockpb");
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq)]
enum CliWalMode { None, Buffered, Fsync }

impl From<CliWalMode> for crate::wal::WalMode {
    fn from(m: CliWalMode) -> Self {
        match m { CliWalMode::None => Self::None, CliWalMode::Buffered => Self::Async, CliWalMode::Fsync => Self::Sync }
    }
}

#[derive(Parser, Debug)]
#[command(name = "mem_etcd", version, about = "In-memory etcd-like server with optional Raft", long_about = None)]
struct Cli {
    /// gRPC listen port (etcd API + Raft transport)
    #[arg(long = "port", env = "ETCD_PORT", default_value_t = 2379)]
    port: u16,

    /// Metrics port
    #[arg(long = "metrics-port", env = "ETCD_METRICS_PORT", default_value_t = 9000)]
    metrics_port: u16,

    /// WAL directory path
    #[arg(long = "wal-dir", env = "ETCD_WAL_DIR", default_value = "./wal")]
    wal_dir: PathBuf,

    /// Default WAL mode for prefixes without explicit override
    #[arg(long = "wal-default", value_enum, default_value_t = CliWalMode::Buffered)]
    wal_default: CliWalMode,

    #[arg(long = "wal-no-write-prefix", value_parser, num_args = 0.., value_delimiter = ' ')]
    wal_no_write_prefixes: Vec<String>,

    // ── Raft configuration ──

    /// Enable Raft consensus mode
    #[arg(long = "raft-enabled", env = "RAFT_ENABLED", default_value_t = false)]
    raft_enabled: bool,

    /// This node's Raft ID (1-based)
    #[arg(long = "raft-node-id", env = "RAFT_NODE_ID")]
    raft_node_id: Option<u64>,

    /// Peer list: "1@host:port,2@host:port,3@host:port"
    #[arg(long = "raft-peers", env = "RAFT_PEERS")]
    raft_peers: Option<String>,

    /// Initialize the cluster (only first node should use this)
    #[arg(long = "raft-init", env = "RAFT_INIT", default_value_t = false)]
    raft_init: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli = Cli::parse();
    let addr: SocketAddr = format!("[::]:{}", cli.port).parse()?;

    let wal_settings = Some(store::WalSettings {
        wal_dir: cli.wal_dir.clone(),
        default_mode: cli.wal_default.into(),
        load_wal_dir: !cli.raft_enabled, // Don't load WAL in raft mode (raft log is the source of truth)
        prefix_modes_no_persist: {
            let set = DashSet::new();
            for prefix in cli.wal_no_write_prefixes.into_iter() {
                set.insert(prefix.as_bytes().to_vec());
            }
            set
        }
    });
    let store = Arc::new(store::Store::new(wal_settings));

    // Create the Lessor for lease management used by election and lock services
    let lessor = Arc::new(Lessor::new(store.clone()));

    // etcd initializes with rev at 1, so set a dummy key to take rev 0
    store.set(b"~".to_vec(), Some(Bytes::from(b"".to_vec())), None).await.unwrap();

    // ── Optional Raft setup ──
    let mut raft_node: Option<raft::RaftNode> = None;

    if cli.raft_enabled {
        let node_id = cli.raft_node_id.ok_or("--raft-node-id is required when --raft-enabled")?;
        let peers_str = cli.raft_peers.as_deref().ok_or("--raft-peers is required when --raft-enabled")?;
        let peers = raft::parse_peers(peers_str)?;

        let (raft, grpc_service, _sm_store) = raft::create_grpc_raft_node(node_id, peers.clone(), store.clone(), None).await?;
        raft_node = Some(raft.clone());

        // Spawn cluster initialization after gRPC server starts
        if cli.raft_init {
            let raft_clone = raft.clone();
            let peers_clone = peers.clone();
            tokio::spawn(async move {
                // Wait for gRPC server to be ready on all nodes
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                match raft::init_grpc_cluster(&raft_clone, &peers_clone).await {
                    Ok(_) => println!("Cluster initialized with {} peers", peers_clone.len()),
                    Err(e) => eprintln!("Cluster init failed: {} (will retry via election)", e),
                }
            });
        }

        // Leader election will happen after cluster init + gRPC server starts
        // Check leader status asynchronously
        {
            let raft_clone = raft.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                match raft::wait_for_leader_grpc(&raft_clone, std::time::Duration::from_secs(15)).await {
                    Ok(leader) => {
                        let metrics = raft_clone.metrics().borrow().clone();
                        println!("Raft cluster ready. Leader: node {}, term: {}", leader, metrics.current_term);
                    }
                    Err(e) => {
                        eprintln!("Warning: no leader elected: {}", e);
                    }
                }
            });
        }

        // Build gRPC server with both etcd API and Raft transport services
        let kv_service = KvServer::new(KvService::with_raft(Arc::clone(&store), raft.clone()));
        let maintenance_service = MaintenanceServer::new(MaintenanceService::new(Arc::clone(&store)));
        let watch_service = WatchServer::new(WatchService::new(Arc::clone(&store)));
        let lease_service = LeaseServer::new(LeaseService::new(Arc::clone(&store)));
        let election_service = ElectionServer::new(ElectionService::new(Arc::clone(&store), lessor.clone()));
        let lock_service = LockServer::new(LockService::new(Arc::clone(&store), lessor.clone()));
        let raft_service = grpc_service.into_server();

        let (in_flight_requests_layer, in_flight_requests_counter) = InFlightRequestsLayer::pair();
        tokio::spawn(
            in_flight_requests_counter.run_emitter(std::time::Duration::from_secs(5), |counter| async move {
                metrics::IN_FLIGHT_REQUESTS.set(counter as i64);
            }),
        );

        // Metrics server
        let metrics_store = Arc::clone(&store);
        let metrics_raft = raft.clone();
        let metrics_app = Router::new().route("/metrics", get(move || {
            let store = Arc::clone(&metrics_store);
            let raft = metrics_raft.clone();
            async move {
                metrics::REVISION_COUNT.set(store.current_revision() as i64);
                metrics::COMPACTED_REVISION_COUNT.set(store.compacted_revision() as i64);
                metrics::WATCHER_COUNT.set(store.watcher_count());
                let raft_metrics = raft.metrics().borrow().clone();
                println!("raft_state: node_id={} leader={:?} term={} state={:?}",
                    raft_metrics.id, raft_metrics.current_leader, raft_metrics.current_term, raft_metrics.state);
                let metric_families = prometheus::gather();
                let mut buf = Vec::new();
                let encoder = TextEncoder::new();
                encoder.encode(&metric_families, &mut buf).unwrap();
                String::from_utf8(buf).unwrap()
            }
        }));
        let metrics_listener = tokio::net::TcpListener::bind(format!("[::]:{}", cli.metrics_port)).await.unwrap();
        tokio::spawn(async move {
            axum::serve(metrics_listener, metrics_app).await.unwrap();
        });

        println!("Starting gRPC server on {} (Raft mode, node {})", addr, node_id);
        Server::builder()
            .max_concurrent_streams(100)
            .http2_adaptive_window(Some(true))
            .layer(in_flight_requests_layer)
            .add_service(kv_service)
            .add_service(maintenance_service)
            .add_service(lease_service)
            .add_service(election_service)
            .add_service(lock_service)
            .add_service(watch_service)
            .add_service(raft_service)
            .serve(addr)
            .await?;
    } else {
        // ── Original single-node mode ──
        let kv_service = KvServer::new(KvService::new(Arc::clone(&store)));
        let maintenance_service = MaintenanceServer::new(MaintenanceService::new(Arc::clone(&store)));
        let watch_service = WatchServer::new(WatchService::new(Arc::clone(&store)));
        let lease_service = LeaseServer::new(LeaseService::new(Arc::clone(&store)));
        let election_service = ElectionServer::new(ElectionService::new(Arc::clone(&store), lessor.clone()));
        let lock_service = LockServer::new(LockService::new(Arc::clone(&store), lessor.clone()));

        let metrics_app = Router::new().route("/metrics", get(move || {
            let store = Arc::clone(&store);
            async move {
                metrics::REVISION_COUNT.set(store.current_revision() as i64);
                metrics::COMPACTED_REVISION_COUNT.set(store.compacted_revision() as i64);
                metrics::WATCHER_COUNT.set(store.watcher_count());
                let metric_families = prometheus::gather();
                let mut buf = Vec::new();
                let encoder = TextEncoder::new();
                encoder.encode(&metric_families, &mut buf).unwrap();
                String::from_utf8(buf).unwrap()
            }
        }));
        let metrics_listener = tokio::net::TcpListener::bind(format!("[::]:{}", cli.metrics_port)).await.unwrap();
        tokio::spawn(async move {
            axum::serve(metrics_listener, metrics_app).await.unwrap();
        });

        let (in_flight_requests_layer, in_flight_requests_counter) = InFlightRequestsLayer::pair();
        tokio::spawn(
            in_flight_requests_counter.run_emitter(std::time::Duration::from_secs(5), |counter| async move {
                metrics::IN_FLIGHT_REQUESTS.set(counter as i64);
            }),
        );

        println!("Starting gRPC server on {} (single-node mode)", addr);
        Server::builder()
            .max_concurrent_streams(100)
            .http2_adaptive_window(Some(true))
            .layer(in_flight_requests_layer)
            .add_service(kv_service)
            .add_service(maintenance_service)
            .add_service(lease_service)
            .add_service(election_service)
            .add_service(lock_service)
            .add_service(watch_service)
            .serve(addr)
            .await?;
    }

    Ok(())
}
