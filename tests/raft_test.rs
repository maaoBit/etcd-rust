// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
// Unit tests for single-Raft integration

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use mem_etcd::raft::{
    self, init_cluster, create_raft_node, RaftRequest, RaftResponse, Router, NodeId,
};
use mem_etcd::store::Store;

use openraft::BasicNode;

/// Wait for a leader to be elected in the cluster.
/// Returns the leader's NodeId.
async fn wait_for_leader(
    nodes: &[(&raft::RaftNode, NodeId)],
    timeout: Duration,
) -> Result<NodeId, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    loop {
        for (raft, id) in nodes {
            let metrics = raft.metrics().borrow().clone();
            if metrics.current_leader.is_some() {
                return Ok(metrics.current_leader.unwrap());
            }
        }
        if start.elapsed() > timeout {
            return Err(format!("No leader elected within {:?}", timeout).into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Find which node is the current leader.
async fn find_leader(nodes: &[(&raft::RaftNode, NodeId)]) -> Option<NodeId> {
    for (raft, _id) in nodes {
        let metrics = raft.metrics().borrow().clone();
        if metrics.current_leader.is_some() {
            return metrics.current_leader;
        }
    }
    None
}

/// Create a 3-node cluster with in-memory stores.
async fn create_3node_cluster() -> (
    Vec<(raft::RaftNode, NodeId, Arc<Store>)>,
    Router,
) {
    let router = Router::new();

    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let store = Arc::new(Store::new(None));
        // Set initial dummy key like main.rs does
        store.set(b"~".to_vec(), Some(Bytes::from(b"".to_vec())), None).await.unwrap();
        let cluster_node = create_raft_node(id, &router, store.clone()).await.unwrap();
        nodes.push((cluster_node.raft, id, store));
    }

    // Initialize the cluster with all 3 nodes
    init_cluster(&nodes[0].0, &[1, 2, 3]).await.unwrap();

    // Wait for leader election
    let raft_refs: Vec<(&raft::RaftNode, NodeId)> = nodes.iter().map(|(r, id, _)| (r, *id)).collect();
    wait_for_leader(&raft_refs, Duration::from_secs(5)).await.unwrap();

    (nodes, router)
}

/// Write a key-value pair through the Raft leader.
async fn raft_write(
    leader: &raft::RaftNode,
    key: &[u8],
    value: &[u8],
) -> Result<RaftResponse, Box<dyn std::error::Error>> {
    let resp = leader
        .client_write(RaftRequest::Set {
            key: key.to_vec(),
            value: value.to_vec(),
        })
        .await?;
    Ok(resp.data)
}

/// Read a key from a store (local read).
fn store_read(store: &Store, key: &[u8]) -> Option<Vec<u8>> {
    match store.range(key.to_vec(), vec![], 0, Some(1)) {
        Ok(result) => {
            if result.kvs.is_empty() {
                None
            } else {
                Some(result.kvs[0].value.to_vec())
            }
        }
        Err(_) => None,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_leader_election() {
    let (nodes, _router) = create_3node_cluster().await;

    let raft_refs: Vec<(&raft::RaftNode, NodeId)> = nodes.iter().map(|(r, id, _)| (r, *id)).collect();
    let leader = wait_for_leader(&raft_refs, Duration::from_secs(5)).await.unwrap();

    // Verify that exactly one leader exists
    let mut leaders = Vec::new();
    for (raft, id) in &raft_refs {
        let metrics = raft.metrics().borrow().clone();
        if metrics.current_leader.is_some() && metrics.current_leader.unwrap() == *id {
            leaders.push(*id);
        }
    }
    assert_eq!(leaders.len(), 1, "Expected exactly 1 leader, got {:?}", leaders);
    println!("Leader elected: node {}", leader);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_write_replication() {
    let (nodes, _router) = create_3node_cluster().await;

    // Find the leader
    let raft_refs: Vec<(&raft::RaftNode, NodeId)> = nodes.iter().map(|(r, id, _)| (r, *id)).collect();
    let leader_id = wait_for_leader(&raft_refs, Duration::from_secs(5)).await.unwrap();

    let leader_node = nodes.iter().find(|(_, id, _)| *id == leader_id).unwrap();
    let leader_raft = &leader_node.0;

    // Write a key through the leader
    let resp = raft_write(leader_raft, b"/test/key1", b"value1").await.unwrap();
    assert!(resp.revision > 0, "Revision should be positive");

    // Wait for replication to all nodes
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify the key exists on all nodes
    for (_, id, store) in &nodes {
        let val = store_read(store, b"/test/key1");
        assert_eq!(
            val,
            Some(b"value1".to_vec()),
            "Node {} does not have the replicated value",
            id
        );
        println!("Node {} has the value", id);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multiple_writes() {
    let (nodes, _router) = create_3node_cluster().await;

    let raft_refs: Vec<(&raft::RaftNode, NodeId)> = nodes.iter().map(|(r, id, _)| (r, *id)).collect();
    let leader_id = wait_for_leader(&raft_refs, Duration::from_secs(5)).await.unwrap();
    let leader_node = nodes.iter().find(|(_, id, _)| *id == leader_id).unwrap();
    let leader_raft = &leader_node.0;

    // Write multiple keys
    for i in 0..10 {
        let key = format!("/test/multi/key{}", i);
        let value = format!("value{}", i);
        let resp = raft_write(leader_raft, key.as_bytes(), value.as_bytes()).await.unwrap();
        assert!(resp.revision > 0, "Revision should be positive for key {}", i);
    }

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all keys on all nodes
    for (_, id, store) in &nodes {
        for i in 0..10 {
            let key = format!("/test/multi/key{}", i);
            let expected = format!("value{}", i);
            let val = store_read(store, key.as_bytes());
            assert_eq!(
                val,
                Some(expected.into_bytes()),
                "Node {} missing key {}",
                id,
                key
            );
        }
    }
    println!("All 10 keys replicated to all 3 nodes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_delete_replication() {
    let (nodes, _router) = create_3node_cluster().await;

    let raft_refs: Vec<(&raft::RaftNode, NodeId)> = nodes.iter().map(|(r, id, _)| (r, *id)).collect();
    let leader_id = wait_for_leader(&raft_refs, Duration::from_secs(5)).await.unwrap();
    let leader_node = nodes.iter().find(|(_, id, _)| *id == leader_id).unwrap();
    let leader_raft = &leader_node.0;

    // Write a key
    raft_write(leader_raft, b"/test/delkey", b"initial").await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify it exists
    for (_, _, store) in &nodes {
        assert!(store_read(store, b"/test/delkey").is_some(), "Key should exist before delete");
    }

    // Delete the key through Raft
    let resp = leader_raft
        .client_write(RaftRequest::Delete {
            key: b"/test/delkey".to_vec(),
        })
        .await
        .unwrap();
    assert!(resp.data.revision > 0, "Delete should return a valid revision");

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify the key is deleted on all nodes
    for (_, id, store) in &nodes {
        let val = store_read(store, b"/test/delkey");
        assert!(
            val.is_none() || val == Some(vec![]),
            "Node {} still has the deleted key",
            id
        );
    }
    println!("Delete replicated to all nodes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_read_consistency() {
    let (nodes, _router) = create_3node_cluster().await;

    let raft_refs: Vec<(&raft::RaftNode, NodeId)> = nodes.iter().map(|(r, id, _)| (r, *id)).collect();
    let leader_id = wait_for_leader(&raft_refs, Duration::from_secs(5)).await.unwrap();
    let leader_node = nodes.iter().find(|(_, id, _)| *id == leader_id).unwrap();
    let leader_raft = &leader_node.0;

    // Write a value
    raft_write(leader_raft, b"/test/consistency", b"consistent_value").await.unwrap();

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Read from all nodes - they should all have the same value
    let mut values = Vec::new();
    for (_, id, store) in &nodes {
        let val = store_read(store, b"/test/consistency");
        values.push(val);
    }

    // All values should be the same
    assert!(values.iter().all(|v| v == &Some(b"consistent_value".to_vec())),
        "Not all nodes have consistent value: {:?}", values);
    println!("All nodes have consistent value");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_revision_monotonic() {
    let (nodes, _router) = create_3node_cluster().await;

    let raft_refs: Vec<(&raft::RaftNode, NodeId)> = nodes.iter().map(|(r, id, _)| (r, *id)).collect();
    let leader_id = wait_for_leader(&raft_refs, Duration::from_secs(5)).await.unwrap();
    let leader_node = nodes.iter().find(|(_, id, _)| *id == leader_id).unwrap();
    let leader_raft = &leader_node.0;

    // Write multiple keys and verify revisions are monotonically increasing
    let mut revisions = Vec::new();
    for i in 0..5 {
        let key = format!("/test/mono/{}", i);
        let resp = raft_write(leader_raft, key.as_bytes(), b"val").await.unwrap();
        revisions.push(resp.revision);
    }

    // Verify revisions are strictly increasing
    for i in 1..revisions.len() {
        assert!(
            revisions[i] > revisions[i - 1],
            "Revision {} ({}) should be greater than revision {} ({})",
            i, revisions[i], i - 1, revisions[i - 1]
        );
    }
    println!("Revisions are monotonically increasing: {:?}", revisions);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster_data_integrity() {
    let (nodes, _router) = create_3node_cluster().await;

    let raft_refs: Vec<(&raft::RaftNode, NodeId)> = nodes.iter().map(|(r, id, _)| (r, *id)).collect();
    let leader_id = wait_for_leader(&raft_refs, Duration::from_secs(5)).await.unwrap();
    let leader_node = nodes.iter().find(|(_, id, _)| *id == leader_id).unwrap();
    let leader_raft = &leader_node.0;

    // Write a set of keys
    let test_data = vec![
        (b"/registry/pods/default/pod1".to_vec(), b"pod_data_1".to_vec()),
        (b"/registry/services/default/svc1".to_vec(), b"svc_data_1".to_vec()),
        (b"/registry/configmaps/default/cm1".to_vec(), b"cm_data_1".to_vec()),
        (b"/registry/secrets/default/secret1".to_vec(), b"secret_data_1".to_vec()),
    ];

    for (key, value) in &test_data {
        raft_write(leader_raft, key, value).await.unwrap();
    }

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all data is consistent across all nodes
    for (_, id, store) in &nodes {
        for (key, expected_value) in &test_data {
            let val = store_read(store, key);
            assert_eq!(
                val,
                Some(expected_value.clone()),
                "Node {} has wrong value for key {:?}",
                id,
                String::from_utf8_lossy(key)
            );
        }
    }
    println!("All K8s-style keys are consistent across all nodes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_overwrite_and_update() {
    let (nodes, _router) = create_3node_cluster().await;

    let raft_refs: Vec<(&raft::RaftNode, NodeId)> = nodes.iter().map(|(r, id, _)| (r, *id)).collect();
    let leader_id = wait_for_leader(&raft_refs, Duration::from_secs(5)).await.unwrap();
    let leader_node = nodes.iter().find(|(_, id, _)| *id == leader_id).unwrap();
    let leader_raft = &leader_node.0;

    // Write initial value
    let resp1 = raft_write(leader_raft, b"/test/overwrite", b"v1").await.unwrap();
    let rev1 = resp1.revision;

    // Overwrite with new value
    let resp2 = raft_write(leader_raft, b"/test/overwrite", b"v2").await.unwrap();
    let rev2 = resp2.revision;

    assert!(rev2 > rev1, "Revision should increase after overwrite: {} -> {}", rev1, rev2);

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // All nodes should have the latest value
    for (_, id, store) in &nodes {
        let val = store_read(store, b"/test/overwrite");
        assert_eq!(val, Some(b"v2".to_vec()), "Node {} has stale value", id);
    }
    println!("Overwrite replicated correctly, revision {} -> {}", rev1, rev2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_empty_value() {
    let (nodes, _router) = create_3node_cluster().await;

    let raft_refs: Vec<(&raft::RaftNode, NodeId)> = nodes.iter().map(|(r, id, _)| (r, *id)).collect();
    let leader_id = wait_for_leader(&raft_refs, Duration::from_secs(5)).await.unwrap();
    let leader_node = nodes.iter().find(|(_, id, _)| *id == leader_id).unwrap();
    let leader_raft = &leader_node.0;

    // Write an empty value (like etcd allows)
    let resp = leader_raft
        .client_write(RaftRequest::Set {
            key: b"/test/empty".to_vec(),
            value: vec![],
        })
        .await
        .unwrap();
    assert!(resp.data.revision > 0, "Empty value write should succeed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // All nodes should have the key with empty value
    for (_, id, store) in &nodes {
        let result = store.range(b"/test/empty".to_vec(), vec![], 0, Some(1));
        assert!(result.is_ok(), "Node {} should have the key", id);
        let kvs = result.unwrap().kvs;
        assert_eq!(kvs.len(), 1, "Node {} should have exactly one value", id);
        assert!(kvs[0].value.is_empty(), "Node {} value should be empty", id);
    }
    println!("Empty value replicated correctly");
}
