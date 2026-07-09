// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess

use crate::lease::lessor::Lessor;
use crate::store::{SetRequired, Store};
use crate::v3electionpb;
use crate::v3electionpb::election_server::Election;
use crate::mvccpb::KeyValue;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tokio_stream::Stream;
use async_stream::try_stream;
use bytes::Bytes;

/// The prefix used for election keys in the store.
const ELECTION_KEY_PREFIX: &[u8] = b"\x00election\x00";

/// Build the full store key for a given election name.
fn election_key(name: &[u8]) -> Vec<u8> {
    let mut k = ELECTION_KEY_PREFIX.to_vec();
    k.extend_from_slice(name);
    k
}

/// Convert an mvccpb::KeyValue into the flat fields of a LeaderResponse.
fn kv_to_leader_response(kv: &KeyValue) -> v3electionpb::LeaderResponse {
    v3electionpb::LeaderResponse {
        kv_key: kv.key.clone(),
        kv_value: kv.value.to_vec(),
        kv_create_revision: kv.create_revision,
        kv_mod_revision: kv.mod_revision,
        kv_version: kv.version,
        kv_lease: kv.lease,
        revision: 0, // will be set by caller
    }
}

/// Convert an mvccpb::KeyValue into the flat fields of a CampaignResponse.
fn kv_to_campaign_response(kv: &KeyValue) -> v3electionpb::CampaignResponse {
    v3electionpb::CampaignResponse {
        leader_key: kv.key.clone(),
        leader_value: kv.value.to_vec(),
        leader_create_revision: kv.create_revision,
        leader_mod_revision: kv.mod_revision,
        leader_version: kv.version,
        leader_lease: kv.lease,
        revision: 0,
    }
}

pub struct ElectionService {
    store: Arc<Store>,
    lessor: Arc<Lessor>,
}

impl ElectionService {
    pub fn new(store: Arc<Store>, lessor: Arc<Lessor>) -> Self {
        ElectionService { store, lessor }
    }

    /// Fetch the current leader KV for an election key.
    fn get_leader(&self, key: &[u8]) -> Result<Option<KeyValue>, Status> {
        let range_result = self.store.range(key.to_vec(), vec![], 0, None)
            .map_err(|e| Status::internal(format!("leader range failed: {:?}", e)))?;
        Ok(range_result.kvs.into_iter().next())
    }
}

#[tonic::async_trait]
impl Election for ElectionService {
    async fn campaign(
        &self,
        request: Request<v3electionpb::CampaignRequest>,
    ) -> Result<Response<v3electionpb::CampaignResponse>, Status> {
        let req = request.into_inner();
        let key = election_key(&req.name);
        let value = req.value;
        let lease_id = req.lease;

        // Grant the lease if one was provided
        if lease_id > 0 {
            self.lessor.grant(lease_id, 60)
                .map_err(|e| Status::internal(format!("lease grant failed: {:?}", e)))?;
            self.lessor.attach(lease_id, &key)
                .map_err(|e| Status::internal(format!("lease attach failed: {:?}", e)))?;
        }

        // Try to create the leadership key (create_revision == 0 means key must not exist)
        let required = SetRequired {
            required_last_revision: Some(0),
            required_version: None,
            compare_result: 0, // EQUAL
        };

        match self.store.set(
            key.clone(),
            Some(Bytes::from(value)),
            Some(required),
        ).await {
            Ok(rev) => {
                // We are the leader
                let leader = self.get_leader(&key)?;
                let mut resp = leader.as_ref()
                    .map(|kv| kv_to_campaign_response(kv))
                    .unwrap_or_default();
                resp.revision = rev;
                Ok(Response::new(resp))
            }
            Err((_rev, current_kv)) => {
                // Failed to become leader – return current leader info
                if lease_id > 0 {
                    let _ = self.lessor.revoke(lease_id);
                }
                let mut resp = current_kv.as_ref()
                    .map(|kv| kv_to_campaign_response(kv))
                    .unwrap_or_default();
                resp.revision = self.store.current_revision();
                Ok(Response::new(resp))
            }
        }
    }

    async fn proclaim(
        &self,
        request: Request<v3electionpb::ProclaimRequest>,
    ) -> Result<Response<v3electionpb::ProclaimResponse>, Status> {
        let req = request.into_inner();
        let key = election_key(&req.name);

        // Proclaim updates the leader value. The key must already exist.
        match self.store.set(
            key.clone(),
            Some(Bytes::from(req.value)),
            None,
        ).await {
            Ok(rev) => {
                Ok(Response::new(v3electionpb::ProclaimResponse {
                    revision: rev,
                }))
            }
            Err((_rev, _)) => {
                Err(Status::failed_precondition("no leader; cannot proclaim"))
            }
        }
    }

    async fn leader(
        &self,
        request: Request<v3electionpb::LeaderRequest>,
    ) -> Result<Response<v3electionpb::LeaderResponse>, Status> {
        let req = request.into_inner();
        let key = election_key(&req.name);

        let kv = self.get_leader(&key)?;
        let mut resp = kv.as_ref()
            .map(|kv| kv_to_leader_response(kv))
            .unwrap_or_default();
        resp.revision = self.store.current_revision();
        Ok(Response::new(resp))
    }

    type ObserveStream = Pin<Box<dyn Stream<Item = Result<v3electionpb::LeaderResponse, Status>> + Send>>;

    async fn observe(
        &self,
        request: Request<v3electionpb::ObserveRequest>,
    ) -> Result<Response<Self::ObserveStream>, Status> {
        let req = request.into_inner();
        let key = election_key(&req.name);

        // Get initial leader state
        let initial_kv = self.get_leader(&key)?;

        // Set up a watch on the election key
        let watch_result = self.store.watch(key.clone(), vec![], 0, true);
        let (past_changes, watcher_id, mut rx) = match watch_result {
            Ok(r) => r,
            Err(compact_rev) => {
                return Ok(Response::new(Box::pin(tokio_stream::once(Ok(
                    v3electionpb::LeaderResponse {
                        revision: compact_rev,
                        ..Default::default()
                    }
                )))));
            }
        };

        let store = self.store.clone();
        let watch_key = key.clone();

        let stream = try_stream! {
            // Send past changes if any
            for change in &past_changes {
                let kv = change.kv.clone();
                let mut resp = kv_to_leader_response(&kv);
                resp.revision = kv.mod_revision;
                yield resp;
            }

            // If there are no past changes, send the initial state
            if past_changes.is_empty() {
                if let Some(ref kv) = initial_kv {
                    let mut resp = kv_to_leader_response(kv);
                    resp.revision = kv.mod_revision;
                    yield resp;
                } else {
                    yield v3electionpb::LeaderResponse {
                        revision: store.current_revision(),
                        ..Default::default()
                    };
                }
            }

            let mut max_rev = past_changes.last().map(|c| c.kv.mod_revision).unwrap_or(0);
            if let Some(ref kv) = initial_kv {
                max_rev = std::cmp::max(max_rev, kv.mod_revision);
            }

            loop {
                let mut read_many = Vec::with_capacity(100);
                tokio::select! {
                    biased;
                    num_read = rx.recv_many(&mut read_many, 1000) => {
                        if num_read == 0 {
                            store.unwatch(watch_key.clone(), watcher_id);
                            return;
                        }
                        for change in read_many {
                            let kv = change.kv;
                            if kv.mod_revision <= max_rev {
                                continue;
                            }
                            max_rev = kv.mod_revision;

                            let mut resp = kv_to_leader_response(&kv);
                            resp.revision = kv.mod_revision;
                            yield resp;
                        }
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream) as Self::ObserveStream))
    }

    async fn resign(
        &self,
        request: Request<v3electionpb::ResignRequest>,
    ) -> Result<Response<v3electionpb::ResignResponse>, Status> {
        let req = request.into_inner();
        let key = election_key(&req.name);

        match self.store.set(key.clone(), None, None).await {
            Ok(rev) => {
                Ok(Response::new(v3electionpb::ResignResponse {
                    revision: rev,
                }))
            }
            Err((_rev, _)) => {
                Err(Status::failed_precondition("no leader to resign"))
            }
        }
    }
}
