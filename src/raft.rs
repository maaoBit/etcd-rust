// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
// Single-Raft integration for mem_etcd using openraft v0.9

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{BasicNode, Config, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, Raft, RaftTypeConfig, Snapshot, SnapshotMeta, StorageError, StoredMembership, Vote};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::store::Store;

// ── Type Configuration ───────────────────────────────────────────────────

pub type NodeId = u64;

/// Request type that gets replicated through Raft.
/// Each variant maps to a Store operation.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftRequest {
    Set { key: Vec<u8>, value: Bytes },
    Delete { key: Vec<u8> },
}

/// Response from applying a RaftRequest to the state machine.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RaftResponse {
    pub revision: i64,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = RaftRequest,
        R = RaftResponse,
        NodeId = NodeId,
        Node = BasicNode,
);

pub type RaftNode = Raft<TypeConfig>;

// ── In-memory Log Store ──────────────────────────────────────────────────

#[derive(Debug, Default)]
struct LogStoreInner {
    last_purged_log_id: Option<LogId<NodeId>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
}

#[derive(Debug, Default, Clone)]
pub struct MemLogStore {
    inner: Arc<RwLock<LogStoreInner>>,
}

impl RaftLogReader<TypeConfig> for MemLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let log = self.inner.read().await;
        Ok(log.log.range(range).map(|(_, v)| v.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        let last = inner.log.iter().next_back().map(|(_, ent)| ent.log_id);
        let last_purged = inner.last_purged_log_id;
        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        Ok(inner.vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId<NodeId>>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        Ok(inner.committed)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.write().await;
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        inner.last_purged_log_id = Some(log_id);
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        Ok(())
    }
}

// ── State Machine Store (wraps mem_etcd Store) ────────────────────────────

/// Raft state machine that applies entries to the existing mem_etcd Store.
/// Also tracks Raft metadata (last_applied_log, last_membership, snapshot).
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
struct StateMachineData {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
}

/// Full snapshot data including KV store data.
#[derive(Serialize, Deserialize)]
struct FullSnapshotData {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    kv_entries: Vec<(Vec<u8>, Vec<u8>, i64, i64, i64)>, // (key, value, create_rev, mod_rev, version)
}

#[derive(Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

pub struct StateMachineStore {
    /// The mem_etcd store that actually holds KV data
    pub store: Arc<Store>,
    /// Raft metadata
    sm: RwLock<StateMachineData>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl StateMachineStore {
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            sm: RwLock::new(StateMachineData::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let sm = self.sm.read().await;

        // Export KV data from the store
        let kv_entries = self.store.export_snapshot_data();

        let snap_data = FullSnapshotData {
            last_applied_log: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
            kv_entries,
        };
        drop(sm);

        let data = serde_json::to_vec(&snap_data)
            .map_err(|e| {
                let io_err = std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string());
                StorageError::from_io_error(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                    io_err,
                )
            })?;
        let last_applied_log = snap_data.last_applied_log;
        let last_membership = snap_data.last_membership;

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        *self.current_snapshot.write().await = Some(snapshot);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>> {
        let sm = self.sm.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut results = Vec::new();
        let mut sm = self.sm.write().await;

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => RaftResponse { revision: self.store.current_revision() },
                EntryPayload::Normal(ref req) => {
                    let rev = match req {
                        RaftRequest::Set { key, value } => {
                            let val = Some(value.clone());
                            match self.store.set(key.clone(), val, None).await {
                                Ok(rev) => rev,
                                Err((rev, _)) => rev,
                            }
                        }
                        RaftRequest::Delete { key } => {
                            match self.store.delete(key.clone(), None).await {
                                Ok(rev) => rev,
                                Err((rev, _)) => rev,
                            }
                        }
                    };
                    RaftResponse { revision: rev }
                }
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    RaftResponse { revision: self.store.current_revision() }
                }
            };
            results.push(response);
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        // Restore full snapshot data (KV data + Raft metadata)
        let recovered: FullSnapshotData = serde_json::from_slice(&data)
            .map_err(|e| {
                let io_err = std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string());
                StorageError::from_io_error(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    io_err,
                )
            })?;

        // Restore KV data into the store
        self.store.import_snapshot_data(recovered.kv_entries);

        // Restore state machine metadata
        let mut sm = self.sm.write().await;
        sm.last_applied_log = recovered.last_applied_log;
        sm.last_membership = recovered.last_membership;

        *self.current_snapshot.write().await = Some(new_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(snap) => Ok(Some(Snapshot {
                meta: snap.meta.clone(),
                snapshot: Box::new(Cursor::new(snap.data.clone())),
            })),
            None => Ok(None),
        }
    }
}

// ── In-memory Network (for testing) ───────────────────────────────────────

use openraft::error::{InstallSnapshotError, RemoteError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};
use tokio::sync::{mpsc, oneshot};

/// Helper: create an Unreachable error from a string message
fn unreachable_err(msg: &str) -> RPCError<NodeId, BasicNode, RaftError<NodeId>> {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, msg);
    RPCError::Unreachable(Unreachable::new(&io_err))
}

/// RPC message envelope for the in-memory router
struct RpcMessage {
    path: String,
    data: Vec<u8>,
    response_tx: oneshot::Sender<Result<Vec<u8>, String>>,
}

/// In-memory network router. Each node registers a receiver.
/// When a Connection sends an RPC, it goes through the router to the target node.
#[derive(Clone)]
pub struct Router {
    targets: Arc<RwLock<BTreeMap<NodeId, mpsc::UnboundedSender<RpcMessage>>>>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            targets: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub async fn register(&self, id: NodeId) -> mpsc::UnboundedReceiver<RpcMessage> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.targets.write().await.insert(id, tx);
        rx
    }

    pub async fn unregister(&self, id: NodeId) {
        self.targets.write().await.remove(&id);
    }

    async fn send<Req, Resp>(
        &self,
        to: NodeId,
        path: &str,
        req: Req,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    where
        Req: Serialize,
        Result<Resp, RaftError<NodeId>>: serde::de::DeserializeOwned,
    {
        let data = serde_json::to_vec(&req).map_err(|e| {
            let io_err = std::io::Error::new(std::io::ErrorKind::Other, e.to_string());
            RPCError::Unreachable(Unreachable::new(&io_err))
        })?;

        let targets = self.targets.read().await;
        let tx = targets.get(&to).cloned().ok_or_else(|| {
            unreachable_err(&format!("node {} not found", to))
        })?;
        drop(targets);

        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(RpcMessage {
            path: path.to_string(),
            data,
            response_tx: resp_tx,
        }).map_err(|_| {
            unreachable_err(&format!("node {} channel closed", to))
        })?;

        let resp_data = match resp_rx.await {
            Ok(Ok(data)) => data,
            Ok(Err(e)) => return Err(unreachable_err(&e)),
            Err(_) => return Err(unreachable_err(&format!("node {} dropped response", to))),
        };

        let result: Result<Resp, RaftError<NodeId>> = serde_json::from_slice(&resp_data)
            .map_err(|e| {
                unreachable_err(&format!("deserialize error: {}", e))
            })?;

        match result {
            Ok(r) => Ok(r),
            Err(RaftError::APIError(_)) => {
                // For non-generic RPCs (append_entries, vote), there's no API error.
                // This branch shouldn't be hit in practice for those RPCs.
                unreachable!("APIError should not occur for non-generic RPCs")
            }
            Err(RaftError::Fatal(f)) => Err(RPCError::Unreachable(Unreachable::new(&f))),
        }
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

/// Network connection to a specific target node
pub struct Connection {
    router: Router,
    target: NodeId,
}

impl RaftNetworkFactory<TypeConfig> for Router {
    type Network = Connection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        Connection {
            router: self.clone(),
            target,
        }
    }
}

impl RaftNetwork<TypeConfig> for Connection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.router.send(self.target, "/raft/append", req).await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>> {
        let data = serde_json::to_vec(&req).map_err(|e| {
            let io_err = std::io::Error::new(std::io::ErrorKind::Other, e.to_string());
            RPCError::<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>::Unreachable(Unreachable::new(&io_err))
        })?;

        let targets = self.router.targets.read().await;
        let tx = targets.get(&self.target).cloned().ok_or_else(|| {
            let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, format!("node {} not found", self.target));
            RPCError::<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>::Unreachable(Unreachable::new(&io_err))
        })?;
        drop(targets);

        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(RpcMessage {
            path: "/raft/snapshot".to_string(),
            data,
            response_tx: resp_tx,
        }).map_err(|_| {
            let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, format!("node {} channel closed", self.target));
            RPCError::<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>::Unreachable(Unreachable::new(&io_err))
        })?;

        let resp_data = match resp_rx.await {
            Ok(Ok(data)) => data,
            Ok(Err(e)) => {
                let io_err = std::io::Error::new(std::io::ErrorKind::Other, e);
                return Err(RPCError::<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>::Unreachable(Unreachable::new(&io_err)));
            }
            Err(_) => {
                let io_err = std::io::Error::new(std::io::ErrorKind::Other, format!("node {} dropped response", self.target));
                return Err(RPCError::<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>::Unreachable(Unreachable::new(&io_err)));
            }
        };

        let result: Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>> = serde_json::from_slice(&resp_data)
            .map_err(|e| {
                let io_err = std::io::Error::new(std::io::ErrorKind::Other, format!("deserialize error: {}", e));
                RPCError::<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>::Unreachable(Unreachable::new(&io_err))
            })?;

        match result {
            Ok(r) => Ok(r),
            Err(RaftError::APIError(e)) => Err(RPCError::RemoteError(RemoteError::new(self.target, RaftError::APIError(e)))),
            Err(RaftError::Fatal(f)) => {
                let msg = f.to_string();
                let io_err = std::io::Error::new(std::io::ErrorKind::Other, msg);
                Err(RPCError::<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>::Unreachable(Unreachable::new(&io_err)))
            }
        }
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.router.send(self.target, "/raft/vote", req).await
    }
}

// ── Cluster helper: runs a Raft node's RPC event loop ─────────────────────

/// A running Raft node with its RPC event loop.
pub struct ClusterNode {
    pub id: NodeId,
    pub raft: RaftNode,
    pub store: Arc<Store>,
}

/// Spawn the RPC event loop for a Raft node.
/// This receives incoming RPC messages and dispatches them to the Raft instance.
pub async fn run_raft_rpc_loop(
    raft: RaftNode,
    mut rx: mpsc::UnboundedReceiver<RpcMessage>,
) {
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let raft = raft.clone();
            tokio::spawn(async move {
                let result = match msg.path.as_str() {
                    "/raft/append" => {
                        let req: AppendEntriesRequest<TypeConfig> = match serde_json::from_slice(&msg.data) {
                            Ok(r) => r,
                            Err(e) => {
                                let _ = msg.response_tx.send(Err(format!("deserialize error: {}", e)));
                                return;
                            }
                        };
                        let res = raft.append_entries(req).await;
                        serde_json::to_vec(&res).map_err(|e| e.to_string())
                    }
                    "/raft/vote" => {
                        let req: VoteRequest<NodeId> = match serde_json::from_slice(&msg.data) {
                            Ok(r) => r,
                            Err(e) => {
                                let _ = msg.response_tx.send(Err(format!("deserialize error: {}", e)));
                                return;
                            }
                        };
                        let res = raft.vote(req).await;
                        serde_json::to_vec(&res).map_err(|e| e.to_string())
                    }
                    "/raft/snapshot" => {
                        let req: InstallSnapshotRequest<TypeConfig> = match serde_json::from_slice(&msg.data) {
                            Ok(r) => r,
                            Err(e) => {
                                let _ = msg.response_tx.send(Err(format!("deserialize error: {}", e)));
                                return;
                            }
                        };
                        let res = raft.install_snapshot(req).await;
                        serde_json::to_vec(&res).map_err(|e| e.to_string())
                    }
                    _ => Err(format!("unknown path: {}", msg.path)),
                };

                let _ = match result {
                    Ok(data) => msg.response_tx.send(Ok(data)),
                    Err(e) => msg.response_tx.send(Err(e)),
                };
            });
        }
    });
}

/// Create a Raft cluster node with in-memory storage and network.
pub async fn create_raft_node(
    id: NodeId,
    router: &Router,
    store: Arc<Store>,
) -> Result<ClusterNode, Box<dyn std::error::Error>> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()?,
    );

    let log_store = MemLogStore::default();
    let sm_store = Arc::new(StateMachineStore::new(store.clone()));

    let rx = router.register(id).await;

    let raft = Raft::new(id, config, router.clone(), log_store, sm_store).await?;

    run_raft_rpc_loop(raft.clone(), rx).await;

    Ok(ClusterNode { id, raft, store })
}

/// Initialize a cluster with the given node IDs.
pub async fn init_cluster(
    raft: &RaftNode,
    node_ids: &[NodeId],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut nodes = BTreeMap::new();
    for &id in node_ids {
        nodes.insert(id, BasicNode { addr: format!("in-memory-{}", id) });
    }
    raft.initialize(nodes).await?;
    Ok(())
}

// ── gRPC Network Layer (for cross-process deployment) ─────────────────────

use raftpb::raft_transport_server::{RaftTransport, RaftTransportServer};
use raftpb::RaftPayload;
use tonic::transport::Channel;

/// gRPC-generated modules
pub mod raftpb {
    tonic::include_proto!("raftpb");
}

/// Parse peer list from CLI string: "1@host:port,2@host:port,3@host:port"
pub fn parse_peers(peers_str: &str) -> Result<Vec<(NodeId, String)>, Box<dyn std::error::Error>> {
    let mut peers = Vec::new();
    for entry in peers_str.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let parts: Vec<&str> = entry.splitn(2, '@').collect();
        if parts.len() != 2 {
            return Err(format!("invalid peer entry: {}", entry).into());
        }
        let id: NodeId = parts[0].trim().parse()?;
        let addr = parts[1].trim().to_string();
        peers.push((id, addr));
    }
    Ok(peers)
}

// ── gRPC Server Side ─────────────────────────────────────────────────────

/// Raft gRPC service that receives RPCs from peer nodes.
pub struct RaftGrpcService {
    raft: RaftNode,
}

impl RaftGrpcService {
    pub fn new(raft: RaftNode) -> Self {
        Self { raft }
    }

    pub fn into_server(self) -> RaftTransportServer<Self> {
        RaftTransportServer::new(self)
    }
}

#[tonic::async_trait]
impl RaftTransport for RaftGrpcService {
    async fn raft_rpc(
        &self,
        request: tonic::Request<RaftPayload>,
    ) -> Result<tonic::Response<RaftPayload>, tonic::Status> {
        let req = request.into_inner();
        let path = req.path.as_str();
        let data = &req.data;

        let result: Result<Vec<u8>, tonic::Status> = match path {
            "/raft/append" => {
                let req: AppendEntriesRequest<TypeConfig> = serde_json::from_slice(data)
                    .map_err(|e| tonic::Status::invalid_argument(format!("deserialize append_entries: {}", e)))?;
                let res = self.raft.append_entries(req).await;
                serde_json::to_vec(&res).map_err(|e| tonic::Status::internal(format!("serialize append_entries resp: {}", e)))
            }
            "/raft/vote" => {
                let req: VoteRequest<NodeId> = serde_json::from_slice(data)
                    .map_err(|e| tonic::Status::invalid_argument(format!("deserialize vote: {}", e)))?;
                let res = self.raft.vote(req).await;
                serde_json::to_vec(&res).map_err(|e| tonic::Status::internal(format!("serialize vote resp: {}", e)))
            }
            "/raft/snapshot" => {
                let req: InstallSnapshotRequest<TypeConfig> = serde_json::from_slice(data)
                    .map_err(|e| tonic::Status::invalid_argument(format!("deserialize install_snapshot: {}", e)))?;
                let res = self.raft.install_snapshot(req).await;
                serde_json::to_vec(&res).map_err(|e| tonic::Status::internal(format!("serialize install_snapshot resp: {}", e)))
            }
            _ => Err(tonic::Status::not_found(format!("unknown raft rpc path: {}", path))),
        };

        match result {
            Ok(data) => Ok(tonic::Response::new(RaftPayload {
                path: String::new(),
                data,
            })),
            Err(e) => Err(e),
        }
    }
}

// ── gRPC Client Side ─────────────────────────────────────────────────────

/// Network factory that uses gRPC (tonic) to communicate with peer nodes.
/// Holds a map of NodeId → gRPC address.
#[derive(Clone)]
pub struct GrpcRouter {
    /// NodeId → "host:port" address
    peers: Arc<RwLock<BTreeMap<NodeId, String>>>,
    /// Cached tonic channels (connection pool)
    channels: Arc<RwLock<BTreeMap<NodeId, Channel>>>,
}

impl GrpcRouter {
    pub fn new(peers: Vec<(NodeId, String)>) -> Self {
        let mut map = BTreeMap::new();
        for (id, addr) in peers {
            map.insert(id, addr);
        }
        Self {
            peers: Arc::new(RwLock::new(map)),
            channels: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    async fn get_channel(&self, target: NodeId) -> Result<Channel, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        // Check cache first
        {
            let cache = self.channels.read().await;
            if let Some(ch) = cache.get(&target) {
                return Ok(ch.clone());
            }
        }

        // Create new channel
        let addr = {
            let peers = self.peers.read().await;
            peers.get(&target).cloned().ok_or_else(|| {
                unreachable_err(&format!("node {} address not found", target))
            })?
        };

        let channel = Channel::from_shared(format!("http://{}", addr))
            .map_err(|e| unreachable_err(&format!("invalid address {}: {}", addr, e)))?
            .connect()
            .await
            .map_err(|e| unreachable_err(&format!("connect to {} failed: {}", addr, e)))?;

        self.channels.write().await.insert(target, channel.clone());
        Ok(channel)
    }

    async fn grpc_send<Req, Resp>(
        &self,
        target: NodeId,
        path: &str,
        req: Req,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    where
        Req: Serialize,
        Result<Resp, RaftError<NodeId>>: serde::de::DeserializeOwned,
    {
        let channel = self.get_channel(target).await?;
        let mut client = raftpb::raft_transport_client::RaftTransportClient::new(channel);

        let data = serde_json::to_vec(&req).map_err(|e| {
            unreachable_err(&format!("serialize request: {}", e))
        })?;

        let resp = client.raft_rpc(tonic::Request::new(RaftPayload {
            path: path.to_string(),
            data,
        }))
        .await
        .map_err(|e| unreachable_err(&format!("gRPC call to {} failed: {}", target, e)))?;

        let result: Result<Resp, RaftError<NodeId>> = serde_json::from_slice(&resp.into_inner().data)
            .map_err(|e| unreachable_err(&format!("deserialize response: {}", e)))?;

        match result {
            Ok(r) => Ok(r),
            Err(RaftError::APIError(_)) => {
                unreachable!("APIError should not occur for non-generic RPCs")
            }
            Err(RaftError::Fatal(f)) => Err(RPCError::Unreachable(Unreachable::new(&f))),
        }
    }

    /// Send install_snapshot RPC with proper error type
    async fn grpc_send_snapshot(
        &self,
        target: NodeId,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>> {
        let channel = self.get_channel(target).await
            .map_err(|e| {
                let io_err = std::io::Error::new(std::io::ErrorKind::Other, e.to_string());
                RPCError::<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>::Unreachable(Unreachable::new(&io_err))
            })?;
        let mut client = raftpb::raft_transport_client::RaftTransportClient::new(channel);

        let data = serde_json::to_vec(&req).map_err(|e| {
            let io_err = std::io::Error::new(std::io::ErrorKind::Other, e.to_string());
            RPCError::<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>::Unreachable(Unreachable::new(&io_err))
        })?;

        let resp = client.raft_rpc(tonic::Request::new(RaftPayload {
            path: "/raft/snapshot".to_string(),
            data,
        }))
        .await
        .map_err(|e| {
            let io_err = std::io::Error::new(std::io::ErrorKind::Other, format!("gRPC call to {} failed: {}", target, e));
            RPCError::<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>::Unreachable(Unreachable::new(&io_err))
        })?;

        let result: Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>> = serde_json::from_slice(&resp.into_inner().data)
            .map_err(|e| {
                let io_err = std::io::Error::new(std::io::ErrorKind::Other, format!("deserialize: {}", e));
                RPCError::<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>::Unreachable(Unreachable::new(&io_err))
            })?;

        match result {
            Ok(r) => Ok(r),
            Err(RaftError::APIError(e)) => Err(RPCError::RemoteError(RemoteError::new(target, RaftError::APIError(e)))),
            Err(RaftError::Fatal(f)) => {
                let msg = f.to_string();
                let io_err = std::io::Error::new(std::io::ErrorKind::Other, msg);
                Err(RPCError::<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>::Unreachable(Unreachable::new(&io_err)))
            }
        }
    }
}

/// gRPC network connection to a specific target node
pub struct GrpcConnection {
    router: GrpcRouter,
    target: NodeId,
}

impl RaftNetworkFactory<TypeConfig> for GrpcRouter {
    type Network = GrpcConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        GrpcConnection {
            router: self.clone(),
            target,
        }
    }
}

impl RaftNetwork<TypeConfig> for GrpcConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.router.grpc_send(self.target, "/raft/append", req).await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>> {
        self.router.grpc_send_snapshot(self.target, req).await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.router.grpc_send(self.target, "/raft/vote", req).await
    }
}

// ── gRPC deployment helper ────────────────────────────────────────────────

/// Create a Raft node with gRPC networking for production deployment.
pub async fn create_grpc_raft_node(
    id: NodeId,
    peers: Vec<(NodeId, String)>,
    store: Arc<Store>,
) -> Result<(RaftNode, RaftGrpcService), Box<dyn std::error::Error>> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 250,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            ..Default::default()
        }
        .validate()?,
    );

    let log_store = MemLogStore::default();
    let sm_store = Arc::new(StateMachineStore::new(store.clone()));
    let router = GrpcRouter::new(peers);

    let raft = Raft::new(id, config, router, log_store, sm_store).await?;
    let grpc_service = RaftGrpcService::new(raft.clone());

    Ok((raft, grpc_service))
}

/// Initialize a cluster with gRPC-addressed peers.
pub async fn init_grpc_cluster(
    raft: &RaftNode,
    peers: &[(NodeId, String)],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut nodes = BTreeMap::new();
    for (id, addr) in peers {
        nodes.insert(*id, BasicNode { addr: addr.clone() });
    }
    raft.initialize(nodes).await?;
    Ok(())
}

/// Wait for a leader to be elected, returns the leader's NodeId.
pub async fn wait_for_leader_grpc(
    raft: &RaftNode,
    timeout: std::time::Duration,
) -> Result<NodeId, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    loop {
        let metrics = raft.metrics().borrow().clone();
        if let Some(leader) = metrics.current_leader {
            return Ok(leader);
        }
        if start.elapsed() > timeout {
            return Err(format!("No leader elected within {:?}", timeout).into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
