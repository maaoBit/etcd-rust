// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess

use crate::lease::lessor::Lessor;
use crate::store::{SetRequired, Store};
use crate::v3lockpb;
use crate::v3lockpb::lock_server::Lock;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use bytes::Bytes;

/// The prefix used for lock keys in the store.
const LOCK_KEY_PREFIX: &[u8] = b"\x00lock\x00";

/// Build the full store key for a given lock name.
fn lock_key(name: &[u8]) -> Vec<u8> {
    let mut k = LOCK_KEY_PREFIX.to_vec();
    k.extend_from_slice(name);
    k
}

/// Convert an mvccpb::KeyValue into the flat fields of a LockResponse.
fn kv_to_lock_response(kv: &crate::mvccpb::KeyValue, revision: i64) -> v3lockpb::LockResponse {
    v3lockpb::LockResponse {
        kv_key: kv.key.clone(),
        kv_value: kv.value.to_vec(),
        kv_create_revision: kv.create_revision,
        kv_mod_revision: kv.mod_revision,
        kv_version: kv.version,
        kv_lease: kv.lease,
        revision,
    }
}

pub struct LockService {
    store: Arc<Store>,
    lessor: Arc<Lessor>,
}

impl LockService {
    pub fn new(store: Arc<Store>, lessor: Arc<Lessor>) -> Self {
        LockService { store, lessor }
    }
}

#[tonic::async_trait]
impl Lock for LockService {
    async fn lock(
        &self,
        request: Request<v3lockpb::LockRequest>,
    ) -> Result<Response<v3lockpb::LockResponse>, Status> {
        let req = request.into_inner();
        let key = lock_key(&req.name);
        let lease_id = req.lease;

        // Grant the lease if one was provided
        if lease_id > 0 {
            self.lessor.grant(lease_id, 60)
                .map_err(|e| Status::internal(format!("lease grant failed: {:?}", e)))?;
            self.lessor.attach(lease_id, &key)
                .map_err(|e| Status::internal(format!("lease attach failed: {:?}", e)))?;
        }

        // Try to acquire the lock. If it fails (key already exists), watch and retry.
        loop {
            let required = SetRequired {
                required_last_revision: Some(0),
                required_version: None,
                compare_result: 0, // EQUAL
            };

            match self.store.set(
                key.clone(),
                Some(Bytes::from(b"lock".to_vec())),
                Some(required),
            ).await {
                Ok(rev) => {
                    let range_result = self.store.range(key.clone(), vec![], 0, None)
                        .map_err(|e| Status::internal(format!("lock range failed: {:?}", e)))?;
                    if let Some(kv) = range_result.kvs.into_iter().next() {
                        return Ok(Response::new(kv_to_lock_response(&kv, rev)));
                    }
                    return Ok(Response::new(v3lockpb::LockResponse {
                        revision: rev,
                        ..Default::default()
                    }));
                }
                Err((_rev, _)) => {
                    // Lock is held by someone else. Watch the key for deletion.
                    let watch_result = self.store.watch(key.clone(), vec![], 0, false);
                    match watch_result {
                        Ok((_, watcher_id, mut rx)) => {
                            loop {
                                let mut read_many = Vec::with_capacity(100);
                                tokio::select! {
                                    num_read = rx.recv_many(&mut read_many, 1000) => {
                                        if num_read == 0 {
                                            self.store.unwatch(key.clone(), watcher_id);
                                            break;
                                        }
                                        // Check for a delete event on the lock key
                                        let should_retry = read_many.iter().any(|c| {
                                            c.kv.key == key && c.kv.value.is_empty()
                                        });
                                        if should_retry {
                                            self.store.unwatch(key.clone(), watcher_id);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Err(_compact_rev) => {
                            // Compacted revision, just retry
                        }
                    }
                }
            }
        }
    }

    async fn unlock(
        &self,
        request: Request<v3lockpb::UnlockRequest>,
    ) -> Result<Response<v3lockpb::UnlockResponse>, Status> {
        let req = request.into_inner();
        let key = lock_key(&req.name);

        match self.store.set(key.clone(), None, None).await {
            Ok(rev) => {
                Ok(Response::new(v3lockpb::UnlockResponse {
                    revision: rev,
                }))
            }
            Err((_rev, _)) => {
                Err(Status::not_found("lock not held"))
            }
        }
    }
}
