// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
pub mod block_deque;
pub mod kv_service;
pub mod raft;
pub mod store;
pub mod wal;
mod metrics;
pub mod watch_service;
pub mod maintenance_service;
pub mod pkg;
pub mod lease;
pub mod compactor;
pub mod membership;
pub mod alarm;
pub mod auth;
pub mod election;
pub mod lock;
pub mod snap;
pub mod rpc;
pub mod config;

pub mod authpb {
    tonic::include_proto!("authpb");
}

pub mod mvccpb {
    tonic::include_proto!("mvccpb");
}

pub mod etcdserverpb {
    tonic::include_proto!("etcdserverpb");
}

pub mod v3electionpb {
    tonic::include_proto!("v3electionpb");
}

pub mod v3lockpb {
    tonic::include_proto!("v3lockpb");
}
