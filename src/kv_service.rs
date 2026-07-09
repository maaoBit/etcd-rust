// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
use crate::etcdserverpb;
use crate::etcdserverpb::kv_server::Kv;
use crate::etcdserverpb::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
    PutResponse, RangeRequest, RangeResponse, ResponseHeader, TxnRequest, TxnResponse,
};
use crate::mvccpb::KeyValue;
use crate::raft::{RaftNode, RaftRequest};
use crate::store::{SetRequired, Store};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use bytes::Bytes;

pub struct KvService {
    store: Arc<Store>,
    raft: Option<RaftNode>,
}

impl KvService {
    pub fn new(store: Arc<Store>) -> Self {
        KvService { store, raft: None }
    }

    /// Create KvService with Raft enabled. Writes go through Raft.
    pub fn with_raft(store: Arc<Store>, raft: RaftNode) -> Self {
        KvService { store, raft: Some(raft) }
    }

    /// Check if this node is the Raft leader. Returns error if not.
    fn ensure_leader(&self) -> Result<(), Status> {
        if let Some(ref raft) = self.raft {
            let metrics = raft.metrics().borrow().clone();
            let node_id = metrics.id;
            match metrics.current_leader {
                Some(leader) if leader == node_id => Ok(()),
                Some(leader) => Err(Status::failed_precondition(
                    format!("not leader, current leader is node {}", leader)
                )),
                None => Err(Status::unavailable("no leader elected")),
            }
        } else {
            Ok(())
        }
    }

    /// Propose a write through Raft and return the revision.
    async fn raft_write(&self, req: RaftRequest) -> Result<i64, Status> {
        let raft = self.raft.as_ref()
            .ok_or_else(|| Status::failed_precondition("raft not enabled"))?;
        let resp = raft.client_write(req).await
            .map_err(|e| Status::internal(format!("raft write failed: {:?}", e)))?;
        Ok(resp.data.revision)
    }

    /// Best-effort compare check for txn operations.
    /// Checks if the current key state satisfies the required conditions.
    fn check_compare(current_kv: &Option<KeyValue>, required: &Option<SetRequired>) -> bool {
        let required_last_revision = required.as_ref().and_then(|r| r.required_last_revision).unwrap_or(-1);
        let required_version = required.as_ref().and_then(|r| r.required_version).unwrap_or(-1);
        let cmp = required.as_ref().map(|r| r.compare_result).unwrap_or(0);
        let cmp_satisfied = |existing: i64, target: i64| -> bool {
            match cmp {
                1 => existing > target,    // GREATER
                2 => existing < target,     // LESS
                3 => existing != target,    // NOT_EQUAL
                _ => existing == target,   // EQUAL (default)
            }
        };
        match current_kv {
            Some(kv) => {
                if required_last_revision >= 0 && !cmp_satisfied(kv.mod_revision, required_last_revision) {
                    return false;
                }
                if required_version >= 0 && !cmp_satisfied(kv.version, required_version) {
                    return false;
                }
                true
            }
            None => {
                // Key doesn't exist
                required_last_revision <= 0 && required_version <= 0
            }
        }
    }
}

#[tonic::async_trait]
impl Kv for KvService {
    /// Implements the Range RPC method.
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let req = request.into_inner();
        let rev = req.revision;

        // Perform the range query on the store
        let limit: Option<usize>;
        if req.count_only {
            limit = Some(0);
        } else if req.limit == 0 {
            limit = None;
        } else {
            limit = Some(req.limit as usize);
        }
        let range_result = self
            .store
            .range(req.key, req.range_end, rev, limit)?;


        // Construct the RangeResponse
        let more: bool;
        if req.count_only || limit.is_none() {
            more = false;
        } else {
            more = range_result.count > limit.unwrap() as i64;
        }

        let response = RangeResponse {
            header: Some(ResponseHeader {
                cluster_id: 0,
                member_id: 0,
                raft_term: 0,
                revision: range_result.latest_rev,
            }),
            count: range_result.count,
            kvs: range_result.kvs,
            more: more,
        };

        Ok(Response::new(response))
    }

    /// Implements the Put RPC method.
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();

        // Get previous value if prev_kv is requested or ignore_value is set
        let prev_kv = if req.prev_kv || req.ignore_value {
            let range_result = self.store.range(req.key.clone(), vec![], 0, Some(1))?;
            range_result.kvs.into_iter().next()
        } else {
            None
        };

        // Handle ignore_value: keep existing value instead of using request value
        let value = if req.ignore_value {
            match &prev_kv {
                Some(kv) => Some(Bytes::from(kv.value.clone())),
                None => return Err(Status::invalid_argument("ignore_value set but key does not exist")),
            }
        } else {
            Some(Bytes::from(req.value.clone()))
        };

        // Perform the put operation
        let rev = if self.raft.is_some() {
            // Raft mode: propose through consensus
            self.ensure_leader()?;
            self.raft_write(RaftRequest::Set {
                key: req.key.clone(),
                value: value.unwrap_or_default(),
            }).await?
        } else {
            // Single-node mode: write directly
            self.store
                .set(req.key, value, None)
                .await
                .map_err(|e| Status::internal(format!("Put operation failed: {:?}", e)))?
        };

        // Construct the PutResponse
        let response = PutResponse {
            header: Some(ResponseHeader {
                cluster_id: 0,
                member_id: 0,
                raft_term: 0,
                revision: rev,
            }),
            prev_kv: if req.prev_kv { prev_kv } else { None },
        };

        Ok(Response::new(response))
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let req = request.into_inner();

        if !req.range_end.is_empty() {
            return Err(Status::invalid_argument("range end not supported in deleteRange"));
        }

        // Always check if key exists for accurate deleted count
        let existing_kv = {
            let range_result = self.store.range(req.key.clone(), vec![], 0, Some(1))?;
            range_result.kvs.into_iter().next()
        };
        let deleted = if existing_kv.is_some() { 1 } else { 0 };
        let prev_kv = if req.prev_kv { existing_kv } else { None };

        let rev = if self.raft.is_some() {
            // Raft mode: propose through consensus
            self.ensure_leader()?;
            self.raft_write(RaftRequest::Delete {
                key: req.key.clone(),
            }).await?
        } else {
            // Single-node mode
            match self.store.delete(req.key, None).await {
                Ok(rev) => rev,
                Err((rev, _)) => rev,
            }
        };

        Ok(Response::new(DeleteRangeResponse {
            prev_kvs: if req.prev_kv { prev_kv.map(|kv| vec![kv]).unwrap_or_default() } else { Vec::new() },
            deleted,
            header: Some(ResponseHeader {
                cluster_id: 0,
                member_id: 0,
                raft_term: 0,
                revision: rev,
            }),
        }))
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        /*
             Most will be of target MOD, e.g.:
                {
                  "method": "/etcdserverpb.KV/Txn",
                  "request": {
                    "compare": [
                      {
                        "key": "/registry/flowschemas/endpoint-controller",
                        "modRevision": "51",
                        "target": "MOD"
                      }
                    ],
                    "failure": [
                      {
                        "requestRange": {
                          "key": "/registry/flowschemas/endpoint-controller"
                        }
                      }
                    ],
                    "success": [
                      {
                        "requestPut": {
                          "key": "/registry/flowschemas/endpoint-controller",
                          "value": "azhzAAotCh9mbG93Y29udHJvbC5hcGlzZXJ2ZX..."
                        }
                      }
                    ]
                  },

            // We also need to support success.requestDeleteRange (with a single key, no range end)

            // Also need to support compare.key.version, wand compare.target == EQUAL
        */
        let mut req = request.into_inner();
        if req.compare.len() != 1 {
            log::debug!("Unsupported txn: {:?}", req);
            return Err(Status::invalid_argument("only one compare supported"));
        }
        let required: Option<SetRequired>;
        let target_union = match req.compare[0].target_union.as_ref() {
            Some(t) => t,
            None => return Err(Status::invalid_argument("compare target_union is required")),
        };
        match target_union {
            crate::etcdserverpb::compare::TargetUnion::ModRevision(mod_revision) => {
                required = Some(SetRequired {
                    required_last_revision: Some(*mod_revision),
                    required_version: None,
                    compare_result: req.compare[0].result,
                });
            }
            crate::etcdserverpb::compare::TargetUnion::Version(version) => {
                required = Some(SetRequired {
                    required_last_revision: None,
                    required_version: Some(*version),
                    compare_result: req.compare[0].result,
                });
            }
            _ => {
                log::debug!("Unsupported compare: {:?}", req.compare[0]);
                return Err(Status::invalid_argument("only MOD or VERSION target supported"));
            }
        }
        if req.success.len() != 1 {
            log::debug!("Unsupported txn: {:?}", req);
            return Err(Status::invalid_argument("only one success supported"));
        }
        if req.failure.len() > 1 {
            log::debug!("Unsupported txn: {:?}", req);
            return Err(Status::invalid_argument("only one failure supported"));
        }
        if !req.failure.is_empty() {
            if let Some(op) = &req.failure[0].request {
                match op {
                    crate::etcdserverpb::request_op::Request::RequestRange(range_req) => {
                        let failure_key = &range_req.key;
                        if *failure_key != req.compare[0].key {
                            log::debug!("Unsupported txn: {:?}", req);
                            return Err(Status::invalid_argument(
                                "compare key must match failure key",
                            ));
                        }
                        if !range_req.range_end.is_empty() {
                            log::debug!("Unsupported txn: {:?}", req);
                            return Err(Status::invalid_argument(
                                "range end not supported in failure",
                            ));
                        }
                    }
                    _ => return Err(Status::invalid_argument("only range supported in failure")),
                }
            }
        }

        if req.success.len() != 1 {
            return Err(Status::invalid_argument("success must be a single request"));
        }

        let success_req = req.success.pop().unwrap().request.unwrap();
        let result: Result<i64, (i64, Option<KeyValue>)>;
        let mut success_is_delete = false;
        match success_req {
            crate::etcdserverpb::request_op::Request::RequestPut(put) => {
                if put.key.is_empty() {
                    return Err(Status::invalid_argument("key required in success put"));
                }
                /*
                put.value can be an empty string, which is a valid value

                if put.value.is_empty() {
                    return Err(Status::invalid_argument("value required in success put"));
                } */
                if put.key != req.compare[0].key {
                    return Err(Status::invalid_argument("compare key must match put key"));
                }
                if self.raft.is_some() {
                    self.ensure_leader()?;
                    // Best-effort compare check locally before proposing through Raft
                    let range_result = self.store.range(put.key.clone(), vec![], 0, Some(1))?;
                    let current_kv = range_result.kvs.into_iter().next();
                    if !Self::check_compare(&current_kv, &required) {
                        result = Err((range_result.latest_rev, current_kv));
                    } else {
                        let rev = self.raft_write(RaftRequest::Set {
                            key: put.key.clone(),
                            value: Bytes::from(put.value),
                        }).await?;
                        result = Ok(rev);
                    }
                } else {
                    result = self.store.set(put.key, Some(Bytes::from(put.value)), required).await;
                }
            }
            crate::etcdserverpb::request_op::Request::RequestDeleteRange(delete_range) => {
                success_is_delete = true;
                if delete_range.key.is_empty() {
                    return Err(Status::invalid_argument(
                        "key required in success deleteRange",
                    ));
                }
                if !delete_range.range_end.is_empty() {
                    return Err(Status::invalid_argument(
                        "range end not supported in deleteRange",
                    ));
                }
                if delete_range.key != req.compare[0].key {
                    return Err(Status::invalid_argument(
                        "compare key must match deleteRange key",
                    ));
                }
                if self.raft.is_some() {
                    self.ensure_leader()?;
                    let range_result = self.store.range(delete_range.key.clone(), vec![], 0, Some(1))?;
                    let current_kv = range_result.kvs.into_iter().next();
                    if !Self::check_compare(&current_kv, &required) {
                        result = Err((range_result.latest_rev, current_kv));
                    } else {
                        let rev = self.raft_write(RaftRequest::Delete {
                            key: delete_range.key.clone(),
                        }).await?;
                        result = Ok(rev);
                    }
                } else {
                    result = self.store.set(delete_range.key, None, required).await;
                }
            }
            _ => {
                return Err(Status::invalid_argument(
                    "only put and deleteRange supported in success",
                ));
            }
        }

        match result {
            Err((rev, kv)) => {
                let kvs = kv.as_slice().to_vec();
                let responses = if req.failure.is_empty() { vec![] } else { vec![etcdserverpb::ResponseOp {
                    response: Some(etcdserverpb::response_op::Response::ResponseRange(
                        etcdserverpb::RangeResponse {
                            header: Some(etcdserverpb::ResponseHeader {
                                cluster_id: 0,
                                member_id: 0,
                                raft_term: 0,
                                revision: rev,
                            }),
                            count: 1,
                            kvs: kvs,
                            more: false,
                        },
                    )),
                }]};

                return Ok(tonic::Response::new(etcdserverpb::TxnResponse {
                    header: Some(etcdserverpb::ResponseHeader {
                        cluster_id: 0,
                        member_id: 0,
                        raft_term: 0,
                        revision: rev,
                    }),
                    responses: responses,
                    succeeded: false,
                }));
            }
            Ok(rev) => {
                let responses: Vec<etcdserverpb::ResponseOp>;
                match success_is_delete {
                    false => {
                        responses = vec![etcdserverpb::ResponseOp {
                            response: Some(etcdserverpb::response_op::Response::ResponsePut(
                                etcdserverpb::PutResponse {
                                    header: Some(etcdserverpb::ResponseHeader {
                                        revision: rev,
                                        ..Default::default()
                                    }),
                                    prev_kv: None,
                                }
                            ))
                        }];
                    }
                    true => {
                        responses = vec![etcdserverpb::ResponseOp {
                            response: Some(etcdserverpb::response_op::Response::ResponseDeleteRange (
                                etcdserverpb::DeleteRangeResponse {
                                    header: Some(etcdserverpb::ResponseHeader {
                                        revision: rev,
                                        ..Default::default()
                                    }),
                                    prev_kvs: vec![],
                                    deleted: 1,
                                }
                            ))
                        }];
                    }
                }
                return Ok(tonic::Response::new(etcdserverpb::TxnResponse {
                    header: Some(etcdserverpb::ResponseHeader {
                        cluster_id: 0,
                        member_id: 0,
                        raft_term: 0,
                        revision: rev,
                    }),
                    responses: responses,
                    succeeded: true,
                }));
            }
        }
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        self.store
            .compact(request.into_inner().revision)
            .map_err(|e| Status::internal(format!("Compaction operation failed: {:?}", e)))?;
        Ok(Response::new(CompactionResponse {
            header: Some(ResponseHeader {
                cluster_id: 0,
                member_id: 0,
                raft_term: 0,
                revision: self.store.current_revision(),
            }),
        }))
    }
}
