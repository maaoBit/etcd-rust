// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
use crate::etcdserverpb;
use crate::etcdserverpb::cluster_server::Cluster as ClusterTrait;
use crate::etcdserverpb::{
    MemberAddRequest, MemberAddResponse, MemberListRequest, MemberListResponse,
    MemberRemoveRequest, MemberRemoveResponse, MemberUpdateRequest, MemberUpdateResponse,
    MemberPromoteRequest, MemberPromoteResponse, ResponseHeader,
};
use crate::raft::{RaftNode, RaftRequest};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::RwLock;
use tonic::{Request, Response, Status};

// ── Types ───────────────────────────────────────────────────────────────────

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Member {
    pub id: u64,
    pub name: String,
    pub peer_urls: Vec<String>,
    pub client_urls: Vec<String>,
    pub is_learner: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct ClusterState {
    pub cluster_id: u64,
    pub members: Vec<Member>,
    pub removed: Vec<u64>,
    pub version: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MembershipError {
    IdRemoved,
    IdNotFound,
    IdExists,
    PeerUrlExists,
    MemberNotLearner,
    TooManyLearners,
    NotEnoughStartedMembers,
}

impl std::fmt::Display for MembershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MembershipError::IdRemoved => write!(f, "member ID has been removed"),
            MembershipError::IdNotFound => write!(f, "member ID not found"),
            MembershipError::IdExists => write!(f, "member ID already exists"),
            MembershipError::PeerUrlExists => write!(f, "peer URL already exists"),
            MembershipError::MemberNotLearner => write!(f, "member is not a learner"),
            MembershipError::TooManyLearners => write!(f, "too many learners"),
            MembershipError::NotEnoughStartedMembers => write!(f, "not enough started members"),
        }
    }
}

// ── Cluster ─────────────────────────────────────────────────────────────────

pub struct Cluster {
    state: RwLock<ClusterState>,
    local_id: u64,
}

impl Cluster {
    pub fn new(local_id: u64) -> Self {
        Self {
            state: RwLock::new(ClusterState {
                cluster_id: Self::gen_id(),
                ..Default::default()
            }),
            local_id,
        }
    }

    /// Create a Cluster with a given initial state (e.g., restored from snapshot).
    pub fn with_state(local_id: u64, state: ClusterState) -> Self {
        Self {
            state: RwLock::new(state),
            local_id,
        }
    }

    pub fn add_member(&self, member: Member) -> Result<(), MembershipError> {
        let mut state = self.state.write().unwrap();
        if state.removed.contains(&member.id) {
            return Err(MembershipError::IdRemoved);
        }
        if state.members.iter().any(|m| m.id == member.id) {
            return Err(MembershipError::IdExists);
        }
        if state.members.iter().any(|m| m.peer_urls == member.peer_urls) {
            return Err(MembershipError::PeerUrlExists);
        }
        state.members.push(member);
        Ok(())
    }

    pub fn remove_member(&self, id: u64) -> Result<(), MembershipError> {
        let mut state = self.state.write().unwrap();
        let pos = state
            .members
            .iter()
            .position(|m| m.id == id)
            .ok_or(MembershipError::IdNotFound)?;
        let member = state.members.remove(pos);
        state.removed.push(member.id);
        Ok(())
    }

    pub fn update_member(
        &self,
        id: u64,
        peer_urls: Vec<String>,
        client_urls: Vec<String>,
    ) -> Result<(), MembershipError> {
        let mut state = self.state.write().unwrap();
        let member = state
            .members
            .iter_mut()
            .find(|m| m.id == id)
            .ok_or(MembershipError::IdNotFound)?;
        member.peer_urls = peer_urls;
        member.client_urls = client_urls;
        Ok(())
    }

    pub fn promote_member(&self, id: u64) -> Result<(), MembershipError> {
        let mut state = self.state.write().unwrap();
        let member = state
            .members
            .iter_mut()
            .find(|m| m.id == id)
            .ok_or(MembershipError::IdNotFound)?;
        if !member.is_learner {
            return Err(MembershipError::MemberNotLearner);
        }
        member.is_learner = false;
        Ok(())
    }

    pub fn list_members(&self) -> Vec<Member> {
        self.state.read().unwrap().members.clone()
    }

    pub fn is_removed(&self, id: u64) -> bool {
        self.state.read().unwrap().removed.contains(&id)
    }

    pub fn get_member(&self, id: u64) -> Option<Member> {
        self.state
            .read()
            .unwrap()
            .members
            .iter()
            .find(|m| m.id == id)
            .cloned()
    }

    pub fn get_cluster_id(&self) -> u64 {
        self.state.read().unwrap().cluster_id
    }

    pub fn get_state(&self) -> ClusterState {
        self.state.read().unwrap().clone()
    }

    pub fn set_state(&self, state: ClusterState) {
        *self.state.write().unwrap() = state;
    }

    /// Get a reference to the shared cluster state for Raft integration.
    pub fn state_arc(&self) -> Arc<RwLock<ClusterState>> {
        let state = self.state.read().unwrap().clone();
        Arc::new(RwLock::new(state))
    }

    /// Generate a random ID using a simple hash-based approach.
    pub fn gen_id() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        // Mix the timestamp with a pseudo-random component
        let mixed = (nanos as u64).wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        mixed ^ (mixed >> 32)
    }

    pub fn validate_config_change(&self, member: &Member) -> Result<(), MembershipError> {
        let state = self.state.read().unwrap();
        if state.removed.contains(&member.id) {
            return Err(MembershipError::IdRemoved);
        }
        if state.members.iter().any(|m| m.id == member.id) {
            return Err(MembershipError::IdExists);
        }
        Ok(())
    }
}

// ── gRPC Service ────────────────────────────────────────────────────────────

pub struct ClusterService {
    cluster: Arc<Cluster>,
    raft: Option<RaftNode>,
}

impl ClusterService {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        Self {
            cluster,
            raft: None,
        }
    }

    pub fn with_raft(cluster: Arc<Cluster>, raft: RaftNode) -> Self {
        Self {
            cluster,
            raft: Some(raft),
        }
    }

    /// Check if this node is the Raft leader. Returns error if not.
    fn ensure_leader(&self) -> Result<(), Status> {
        if let Some(ref raft) = self.raft {
            let metrics = raft.metrics().borrow().clone();
            let node_id = metrics.id;
            match metrics.current_leader {
                Some(leader) if leader == node_id => Ok(()),
                Some(leader) => Err(Status::failed_precondition(format!(
                    "not leader, current leader is node {}",
                    leader
                ))),
                None => Err(Status::unavailable("no leader elected")),
            }
        } else {
            Ok(())
        }
    }

    /// Propose a write through Raft.
    async fn raft_write(&self, req: RaftRequest) -> Result<i64, Status> {
        let raft = self.raft.as_ref().unwrap();
        let resp = raft
            .client_write(req)
            .await
            .map_err(|e| Status::internal(format!("raft write failed: {:?}", e)))?;
        Ok(resp.data.revision)
    }

    fn member_to_proto(m: &Member) -> etcdserverpb::Member {
        etcdserverpb::Member {
            id: m.id,
            name: m.name.clone(),
            peer_ur_ls: m.peer_urls.clone(),
            client_ur_ls: m.client_urls.clone(),
            is_learner: m.is_learner,
        }
    }

    fn all_members_proto(cluster: &Cluster) -> Vec<etcdserverpb::Member> {
        cluster
            .list_members()
            .iter()
            .map(Self::member_to_proto)
            .collect()
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_new() {
        let cluster = Cluster::new(1);
        assert_eq!(cluster.local_id, 1);
        assert!(cluster.list_members().is_empty());
        assert!(!cluster.is_removed(99));
        assert_ne!(cluster.get_cluster_id(), 0);
    }

    #[test]
    fn test_cluster_member_add() {
        let cluster = Cluster::new(1);
        let member = Member {
            id: 10,
            name: "node1".into(),
            peer_urls: vec!["http://127.0.0.1:2380".into()],
            client_urls: vec![],
            is_learner: false,
        };
        assert!(cluster.add_member(member).is_ok());

        let members = cluster.list_members();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].id, 10);
        assert_eq!(members[0].name, "node1");
    }

    #[test]
    fn test_cluster_member_add_duplicate() {
        let cluster = Cluster::new(1);
        let member = Member {
            id: 10,
            name: "node1".into(),
            peer_urls: vec!["http://127.0.0.1:2380".into()],
            client_urls: vec![],
            is_learner: false,
        };
        assert!(cluster.add_member(member).is_ok());

        let duplicate = Member {
            id: 10,
            name: "node1-dup".into(),
            peer_urls: vec!["http://127.0.0.1:2381".into()],
            client_urls: vec![],
            is_learner: false,
        };
        let err = cluster.add_member(duplicate).unwrap_err();
        assert_eq!(err, MembershipError::IdExists);
    }

    #[test]
    fn test_cluster_member_add_removed_id() {
        let cluster = Cluster::new(1);
        let member = Member {
            id: 10,
            name: "node1".into(),
            peer_urls: vec!["http://127.0.0.1:2380".into()],
            client_urls: vec![],
            is_learner: false,
        };
        assert!(cluster.add_member(member).is_ok());
        assert!(cluster.remove_member(10).is_ok());

        let re_add = Member {
            id: 10,
            name: "node1-again".into(),
            peer_urls: vec!["http://127.0.0.1:2381".into()],
            client_urls: vec![],
            is_learner: false,
        };
        let err = cluster.add_member(re_add).unwrap_err();
        assert_eq!(err, MembershipError::IdRemoved);
    }

    #[test]
    fn test_cluster_member_add_peer_url_exists() {
        let cluster = Cluster::new(1);
        let peer_url = vec!["http://127.0.0.1:2380".into()];
        let member1 = Member {
            id: 10,
            name: "node1".into(),
            peer_urls: peer_url.clone(),
            client_urls: vec![],
            is_learner: false,
        };
        assert!(cluster.add_member(member1).is_ok());

        let member2 = Member {
            id: 20,
            name: "node2".into(),
            peer_urls: peer_url,
            client_urls: vec![],
            is_learner: false,
        };
        let err = cluster.add_member(member2).unwrap_err();
        assert_eq!(err, MembershipError::PeerUrlExists);
    }

    #[test]
    fn test_cluster_member_remove() {
        let cluster = Cluster::new(1);
        let member = Member {
            id: 10,
            name: "node1".into(),
            peer_urls: vec!["http://127.0.0.1:2380".into()],
            client_urls: vec![],
            is_learner: false,
        };
        assert!(cluster.add_member(member).is_ok());
        assert!(cluster.remove_member(10).is_ok());

        // Member should be gone from the list
        assert!(cluster.list_members().is_empty());
        // ID should be in the removed set
        assert!(cluster.is_removed(10));
    }

    #[test]
    fn test_cluster_member_remove_not_found() {
        let cluster = Cluster::new(1);
        let err = cluster.remove_member(99).unwrap_err();
        assert_eq!(err, MembershipError::IdNotFound);
    }

    #[test]
    fn test_cluster_member_update() {
        let cluster = Cluster::new(1);
        let member = Member {
            id: 10,
            name: "node1".into(),
            peer_urls: vec!["http://127.0.0.1:2380".into()],
            client_urls: vec!["http://127.0.0.1:2379".into()],
            is_learner: false,
        };
        assert!(cluster.add_member(member).is_ok());

        let new_peer_urls = vec!["http://127.0.0.1:2381".into()];
        let new_client_urls = vec!["http://127.0.0.1:2382".into()];
        assert!(cluster.update_member(10, new_peer_urls.clone(), new_client_urls.clone()).is_ok());

        let updated = cluster.get_member(10).unwrap();
        assert_eq!(updated.peer_urls, new_peer_urls);
        assert_eq!(updated.client_urls, new_client_urls);
    }

    #[test]
    fn test_cluster_member_update_not_found() {
        let cluster = Cluster::new(1);
        let err = cluster.update_member(99, vec![], vec![]).unwrap_err();
        assert_eq!(err, MembershipError::IdNotFound);
    }

    #[test]
    fn test_cluster_member_list() {
        let cluster = Cluster::new(1);
        let members = vec![
            Member {
                id: 1,
                name: "alpha".into(),
                peer_urls: vec!["http://10.0.0.1:2380".into()],
                client_urls: vec![],
                is_learner: false,
            },
            Member {
                id: 2,
                name: "beta".into(),
                peer_urls: vec!["http://10.0.0.2:2380".into()],
                client_urls: vec![],
                is_learner: false,
            },
            Member {
                id: 3,
                name: "gamma".into(),
                peer_urls: vec!["http://10.0.0.3:2380".into()],
                client_urls: vec![],
                is_learner: true,
            },
        ];
        for m in members.clone() {
            assert!(cluster.add_member(m).is_ok());
        }

        let listed = cluster.list_members();
        assert_eq!(listed.len(), 3);

        // Verify all IDs are present (order is insertion order)
        let mut ids: Vec<_> = listed.iter().map(|m| m.id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);

        // Verify the learner flag is preserved
        let gamma = listed.iter().find(|m| m.id == 3).unwrap();
        assert!(gamma.is_learner);
    }

    #[test]
    fn test_cluster_member_promote() {
        let cluster = Cluster::new(1);
        let member = Member {
            id: 10,
            name: "learner-node".into(),
            peer_urls: vec!["http://127.0.0.1:2380".into()],
            client_urls: vec![],
            is_learner: true,
        };
        assert!(cluster.add_member(member).is_ok());
        assert!(cluster.promote_member(10).is_ok());

        let promoted = cluster.get_member(10).unwrap();
        assert!(!promoted.is_learner);
    }

    #[test]
    fn test_cluster_member_promote_not_learner() {
        let cluster = Cluster::new(1);
        let member = Member {
            id: 10,
            name: "voter-node".into(),
            peer_urls: vec!["http://127.0.0.1:2380".into()],
            client_urls: vec![],
            is_learner: false,
        };
        assert!(cluster.add_member(member).is_ok());
        let err = cluster.promote_member(10).unwrap_err();
        assert_eq!(err, MembershipError::MemberNotLearner);
    }

    #[test]
    fn test_cluster_member_promote_not_found() {
        let cluster = Cluster::new(1);
        let err = cluster.promote_member(99).unwrap_err();
        assert_eq!(err, MembershipError::IdNotFound);
    }

    #[test]
    fn test_cluster_is_removed() {
        let cluster = Cluster::new(1);
        // Fresh cluster: no IDs removed
        assert!(!cluster.is_removed(10));

        let member = Member {
            id: 10,
            name: "node1".into(),
            peer_urls: vec!["http://127.0.0.1:2380".into()],
            client_urls: vec![],
            is_learner: false,
        };
        assert!(cluster.add_member(member).is_ok());
        assert!(cluster.remove_member(10).is_ok());

        assert!(cluster.is_removed(10));
        assert!(!cluster.is_removed(20));
    }

    #[test]
    fn test_cluster_get_member() {
        let cluster = Cluster::new(1);
        // Non-existent member returns None
        assert!(cluster.get_member(99).is_none());

        let member = Member {
            id: 10,
            name: "node1".into(),
            peer_urls: vec!["http://127.0.0.1:2380".into()],
            client_urls: vec![],
            is_learner: false,
        };
        assert!(cluster.add_member(member).is_ok());

        let found = cluster.get_member(10);
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "node1");

        // Non-existent after add still returns None
        assert!(cluster.get_member(99).is_none());
    }

    #[test]
    fn test_cluster_gen_id() {
        let id1 = Cluster::gen_id();
        std::thread::sleep(std::time::Duration::from_micros(1));
        let id2 = Cluster::gen_id();
        assert_ne!(id1, id2, "consecutive gen_id calls must produce different IDs");
        assert_ne!(id1, 0);
        assert_ne!(id2, 0);
    }

    #[test]
    fn test_cluster_with_state() {
        let state = ClusterState {
            cluster_id: 42,
            members: vec![Member {
                id: 1,
                name: "restored-node".into(),
                peer_urls: vec!["http://127.0.0.1:2380".into()],
                client_urls: vec!["http://127.0.0.1:2379".into()],
                is_learner: false,
            }],
            removed: vec![99],
            version: Some("3.5".into()),
        };
        let cluster = Cluster::with_state(1, state);
        assert_eq!(cluster.get_cluster_id(), 42);
        assert_eq!(cluster.list_members().len(), 1);
        assert_eq!(cluster.list_members()[0].name, "restored-node");
        assert!(cluster.is_removed(99));
        assert!(!cluster.is_removed(1));
        assert_eq!(cluster.get_state().version, Some("3.5".into()));
    }

    #[test]
    fn test_cluster_get_state() {
        let cluster = Cluster::new(1);
        let original_id = cluster.get_cluster_id();

        let member = Member {
            id: 10,
            name: "node1".into(),
            peer_urls: vec!["http://127.0.0.1:2380".into()],
            client_urls: vec![],
            is_learner: false,
        };
        assert!(cluster.add_member(member).is_ok());

        let state = cluster.get_state();
        assert_eq!(state.cluster_id, original_id);
        assert_eq!(state.members.len(), 1);
        assert_eq!(state.members[0].id, 10);
    }

    #[test]
    fn test_cluster_set_state() {
        let cluster = Cluster::new(1);
        let new_state = ClusterState {
            cluster_id: 100,
            members: vec![Member {
                id: 7,
                name: "replaced".into(),
                peer_urls: vec!["http://127.0.0.1:2380".into()],
                client_urls: vec![],
                is_learner: false,
            }],
            removed: vec![1, 2, 3],
            version: None,
        };
        cluster.set_state(new_state);

        assert_eq!(cluster.get_cluster_id(), 100);
        assert_eq!(cluster.list_members().len(), 1);
        assert_eq!(cluster.list_members()[0].id, 7);
        assert!(cluster.is_removed(1));
        assert!(cluster.is_removed(3));
    }

    #[test]
    fn test_cluster_validate_config_change() {
        let cluster = Cluster::new(1);
        let fresh = Member {
            id: 10,
            name: "new-node".into(),
            peer_urls: vec!["http://127.0.0.1:2380".into()],
            client_urls: vec![],
            is_learner: false,
        };
        // Fresh ID should pass validation
        assert!(cluster.validate_config_change(&fresh).is_ok());

        // Add the member
        assert!(cluster.add_member(fresh).is_ok());

        // Same ID should now fail
        let dup = Member {
            id: 10,
            name: "dup".into(),
            peer_urls: vec!["http://127.0.0.1:2381".into()],
            client_urls: vec![],
            is_learner: false,
        };
        assert_eq!(
            cluster.validate_config_change(&dup).unwrap_err(),
            MembershipError::IdExists
        );

        // Remove and re-validate
        assert!(cluster.remove_member(10).is_ok());
        let removed_id = Member {
            id: 10,
            name: "removed-again".into(),
            peer_urls: vec!["http://127.0.0.1:2382".into()],
            client_urls: vec![],
            is_learner: false,
        };
        assert_eq!(
            cluster.validate_config_change(&removed_id).unwrap_err(),
            MembershipError::IdRemoved
        );
    }
}

#[tonic::async_trait]
impl ClusterTrait for ClusterService {
    async fn member_add(
        &self,
        request: Request<MemberAddRequest>,
    ) -> Result<Response<MemberAddResponse>, Status> {
        self.ensure_leader()?;
        let req = request.into_inner();

        let member_id = Cluster::gen_id(); // Cluster::gen_id is on Member too
        let member = Member {
            id: member_id,
            name: String::new(),
            peer_urls: req.peer_ur_ls.clone(),
            client_urls: vec![],
            is_learner: req.is_learner,
        };

        if self.raft.is_some() {
            self.raft_write(RaftRequest::MemberAdd {
                peer_urls: req.peer_ur_ls.clone(),
                is_learner: req.is_learner,
                member_id,
            })
            .await?;
        } else {
            self.cluster.add_member(member).map_err(|e| {
                Status::invalid_argument(format!("member add failed: {}", e))
            })?;
        }

        let members = Self::all_members_proto(&self.cluster);
        let member_proto = etcdserverpb::Member {
            id: member_id,
            ..Default::default()
        };

        Ok(Response::new(MemberAddResponse {
            header: Some(ResponseHeader {
                cluster_id: self.cluster.get_cluster_id(),
                ..Default::default()
            }),
            member: Some(member_proto),
            members,
        }))
    }

    async fn member_remove(
        &self,
        request: Request<MemberRemoveRequest>,
    ) -> Result<Response<MemberRemoveResponse>, Status> {
        self.ensure_leader()?;
        let req = request.into_inner();

        // Do not allow removing the local node
        if self.raft.is_some() {
            let metrics = self.raft.as_ref().unwrap().metrics().borrow().clone();
            if req.id == metrics.id {
                return Err(Status::invalid_argument(
                    "cannot remove the current leader node",
                ));
            }
        }

        if self.raft.is_some() {
            self.raft_write(RaftRequest::MemberRemove { id: req.id })
                .await?;
        } else {
            self.cluster.remove_member(req.id).map_err(|e| {
                Status::invalid_argument(format!("member remove failed: {}", e))
            })?;
        }

        let members = Self::all_members_proto(&self.cluster);

        Ok(Response::new(MemberRemoveResponse {
            header: Some(ResponseHeader {
                cluster_id: self.cluster.get_cluster_id(),
                ..Default::default()
            }),
            members,
        }))
    }

    async fn member_update(
        &self,
        request: Request<MemberUpdateRequest>,
    ) -> Result<Response<MemberUpdateResponse>, Status> {
        self.ensure_leader()?;
        let req = request.into_inner();

        if self.raft.is_some() {
            self.raft_write(RaftRequest::MemberUpdate {
                id: req.id,
                peer_urls: req.peer_ur_ls.clone(),
                client_urls: vec![],
            })
            .await?;
        } else {
            self.cluster
                .update_member(req.id, req.peer_ur_ls.clone(), vec![])
                .map_err(|e| Status::invalid_argument(format!("member update failed: {}", e)))?;
        }

        let members = Self::all_members_proto(&self.cluster);

        Ok(Response::new(MemberUpdateResponse {
            header: Some(ResponseHeader {
                cluster_id: self.cluster.get_cluster_id(),
                ..Default::default()
            }),
            members,
        }))
    }

    async fn member_list(
        &self,
        _request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListResponse>, Status> {
        let members = Self::all_members_proto(&self.cluster);

        Ok(Response::new(MemberListResponse {
            header: Some(ResponseHeader {
                cluster_id: self.cluster.get_cluster_id(),
                ..Default::default()
            }),
            members,
        }))
    }

    async fn member_promote(
        &self,
        request: Request<MemberPromoteRequest>,
    ) -> Result<Response<MemberPromoteResponse>, Status> {
        self.ensure_leader()?;
        let req = request.into_inner();

        if self.raft.is_some() {
            self.raft_write(RaftRequest::MemberPromote { id: req.id })
                .await?;
        } else {
            self.cluster.promote_member(req.id).map_err(|e| {
                Status::invalid_argument(format!("member promote failed: {}", e))
            })?;
        }

        let members = Self::all_members_proto(&self.cluster);

        Ok(Response::new(MemberPromoteResponse {
            header: Some(ResponseHeader {
                cluster_id: self.cluster.get_cluster_id(),
                ..Default::default()
            }),
            members,
        }))
    }
}
