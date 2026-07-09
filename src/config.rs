// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess

//! Configuration and bootstrap module for mem_etcd.
//!
//! Provides a structured `ServerConfig` that centralizes all configuration
//! parameters, and a `bootstrap` function that orchestrates startup of the
//! Store, WAL, and optional Raft node.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashSet;

use crate::raft::{self, RaftGrpcService, RaftNode};
use crate::store::{Store, WalSettings};
use crate::wal::WalMode;

// ── ClusterState (config-level) ────────────────────────────────────────────

/// Whether this node is joining a new cluster or an existing one.
#[derive(Clone, Debug, PartialEq)]
pub enum ClusterState {
    New,
    Existing,
}

// ── ServerConfig ───────────────────────────────────────────────────────────

/// Centralised configuration for a mem_etcd server.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Human-readable name for this member.
    pub name: String,
    /// Path to the data directory (not yet used in single-node mode).
    pub data_dir: PathBuf,
    /// Directory for write-ahead log files.
    pub wal_dir: PathBuf,
    /// WAL persistence mode: "none", "async", or "sync".
    pub wal_mode: String,
    /// Address the client-facing gRPC API listens on.
    pub listen_client_addr: String,
    /// Optional address for inter-peer Raft traffic (gRPC).
    pub listen_peer_addr: Option<String>,
    /// Initial cluster peer URLs: list of `(name, peer_url)` pairs.
    pub initial_cluster: Vec<(String, String)>,
    /// Whether this node starts a new cluster or joins an existing one.
    pub initial_cluster_state: ClusterState,
    /// Cluster name for identification.
    pub cluster_name: String,
    /// Enable Raft consensus mode.
    pub enable_raft: bool,
    /// This node's Raft ID (1-based).
    pub raft_node_id: Option<u64>,
    /// Peer list string: "1@host:port,2@host:port".
    pub raft_peers: Option<String>,
    /// If true, initialise the cluster (first node only).
    pub raft_init: bool,
    /// Number of applied entries before taking a snapshot.
    pub snapshot_count: u64,
    /// Maximum number of snapshots to retain.
    pub max_snapshots: u64,
    /// Maximum request size in bytes.
    pub max_request_bytes: i64,
    /// Backend quota in bytes.
    pub quota_backend_bytes: i64,
    /// Auto-compaction mode: "periodic" or "revision".
    pub auto_compaction_mode: String,
    /// Auto-compaction retention duration or revision count.
    pub auto_compaction_retention: String,
    /// Auth token type (e.g. "simple").
    pub auth_token: String,
    /// Enable authentication.
    pub enable_auth: bool,
    /// WAL key prefixes that should not be persisted (best-effort).
    pub wal_no_write_prefixes: Vec<String>,
}

impl ServerConfig {
    /// Returns a default configuration suitable for single-node development.
    pub fn default() -> Self {
        Self {
            name: "default".to_string(),
            data_dir: PathBuf::from("./mem_etcd.data"),
            wal_dir: PathBuf::from("./wal"),
            wal_mode: "async".to_string(),
            listen_client_addr: "[::]:2379".to_string(),
            listen_peer_addr: None,
            initial_cluster: Vec::new(),
            initial_cluster_state: ClusterState::New,
            cluster_name: "default".to_string(),
            enable_raft: false,
            raft_node_id: None,
            raft_peers: None,
            raft_init: false,
            snapshot_count: 100_000,
            max_snapshots: 5,
            max_request_bytes: 1_500_000,
            quota_backend_bytes: 2_000_000_000,
            auto_compaction_mode: "periodic".to_string(),
            auto_compaction_retention: "1h".to_string(),
            auth_token: "simple".to_string(),
            enable_auth: false,
            wal_no_write_prefixes: Vec::new(),
        }
    }

    /// Validate configuration values and return an error string if invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("name must not be empty".to_string());
        }
        match self.wal_mode.as_str() {
            "none" | "async" | "sync" => {}
            other => {
                return Err(format!(
                    "invalid wal_mode: '{}' (expected 'none', 'async', or 'sync')",
                    other
                ))
            }
        }
        if self.listen_client_addr.is_empty() {
            return Err("listen_client_addr must not be empty".to_string());
        }
        if self.initial_cluster_state == ClusterState::New && !self.initial_cluster.is_empty() {
            if !self
                .initial_cluster
                .iter()
                .any(|(name, _)| name == &self.name)
            {
                return Err(format!(
                    "node '{}' not found in initial_cluster",
                    self.name
                ));
            }
        }
        Ok(())
    }

    /// Whether this node starts as a learner (not yet supported).
    pub fn is_learner(&self) -> bool {
        false
    }

    /// Look up the peer URL for a given Raft node ID from `raft_peers`.
    pub fn peer_url_for(&self, node_id: u64) -> Option<String> {
        if let Some(ref peers) = self.raft_peers {
            for entry in peers.split(',') {
                let entry = entry.trim();
                if entry.is_empty() {
                    continue;
                }
                let parts: Vec<&str> = entry.splitn(2, '@').collect();
                if parts.len() == 2 {
                    if let Ok(id) = parts[0].trim().parse::<u64>() {
                        if id == node_id {
                            return Some(parts[1].trim().to_string());
                        }
                    }
                }
            }
        }
        None
    }
}

// ── WAL mode conversion ────────────────────────────────────────────────────

fn wal_mode_from_str(s: &str) -> Result<WalMode, String> {
    match s {
        "none" => Ok(WalMode::None),
        "async" => Ok(WalMode::Async),
        "sync" => Ok(WalMode::Sync),
        other => Err(format!("invalid wal_mode: '{}'", other)),
    }
}

// ── BootstrappedServer ─────────────────────────────────────────────────────

/// Components returned after a successful bootstrap sequence.
pub struct BootstrappedServer {
    pub store: Arc<Store>,
    pub raft_node: Option<RaftNode>,
    pub raft_grpc: Option<RaftGrpcService>,
}

// ── Bootstrap function ─────────────────────────────────────────────────────

/// Orchestrate startup: validate config, initialise WAL and Store, and
/// optionally initialise the Raft node.
///
/// Does **not** start the gRPC server — that is the caller's responsibility
/// using the returned components.
pub async fn bootstrap(config: &ServerConfig) -> Result<BootstrappedServer, Box<dyn std::error::Error>> {
    config
        .validate()
        .map_err(|e| format!("config validation failed: {}", e))?;

    // ── WAL settings ────────────────────────────────────────────────────
    let wal_mode = wal_mode_from_str(&config.wal_mode)?;
    let wal_settings = if config.wal_dir.as_os_str().is_empty() {
        None
    } else {
        let mut prefix_modes_no_persist = DashSet::new();
        for prefix in &config.wal_no_write_prefixes {
            prefix_modes_no_persist.insert(prefix.as_bytes().to_vec());
        }
        Some(WalSettings {
            wal_dir: config.wal_dir.clone(),
            default_mode: wal_mode,
            load_wal_dir: !config.enable_raft,
            prefix_modes_no_persist,
        })
    };

    // ── Store ───────────────────────────────────────────────────────────
    let store = Arc::new(Store::new(wal_settings));

    // etcd initialises with revision at 1, so set a dummy key to occupy rev 0.
    store
        .set(b"~".to_vec(), Some(Bytes::from(b"".to_vec())), None)
        .await
        .unwrap();

    // ── Optional Raft setup ─────────────────────────────────────────────
    let mut raft_node: Option<RaftNode> = None;
    let mut raft_grpc: Option<RaftGrpcService> = None;

    if config.enable_raft {
        let node_id = config
            .raft_node_id
            .ok_or("--raft-node-id is required when --raft-enabled")?;
        let peers_str = config
            .raft_peers
            .as_deref()
            .ok_or("--raft-peers is required when --raft-enabled")?;
        let peers = raft::parse_peers(peers_str)?;

        let (raft, grpc_service, _sm_store) =
            raft::create_grpc_raft_node(node_id, peers.clone(), store.clone(), None).await?;
        raft_node = Some(raft.clone());
        raft_grpc = Some(grpc_service);

        // ── Cluster initialisation ──────────────────────────────────────
        if config.raft_init {
            let raft_clone = raft.clone();
            let peers_clone = peers.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                match raft::init_grpc_cluster(&raft_clone, &peers_clone).await {
                    Ok(_) => println!("Cluster initialized with {} peers", peers_clone.len()),
                    Err(e) => {
                        eprintln!("Cluster init failed: {} (will retry via election)", e)
                    }
                }
            });
        }

        // ── Wait for leader ─────────────────────────────────────────────
        {
            let raft_clone = raft.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                match raft::wait_for_leader_grpc(&raft_clone, std::time::Duration::from_secs(15)).await
                {
                    Ok(leader) => {
                        let metrics = raft_clone.metrics().borrow().clone();
                        println!(
                            "Raft cluster ready. Leader: node {}, term: {}",
                            leader, metrics.current_term
                        );
                    }
                    Err(e) => {
                        eprintln!("Warning: no leader elected: {}", e);
                    }
                }
            });
        }
    }

    Ok(BootstrappedServer {
        store,
        raft_node,
        raft_grpc,
    })
}
