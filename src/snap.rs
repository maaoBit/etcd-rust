// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess

//! Snapshot module for mem_etcd.
//! Provides state machine snapshot serialization for Raft log compaction.

use serde::{Deserialize, Serialize};

/// Metadata for a snapshot
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SnapshotMeta {
    pub revision: i64,
    pub timestamp_secs: u64,
    pub kv_count: usize,
}

/// A single key-value entry in a snapshot
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SnapshotEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub lease: i64,
}

/// A complete snapshot of the store state
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreSnapshot {
    pub meta: SnapshotMeta,
    pub entries: Vec<SnapshotEntry>,
}

impl StoreSnapshot {
    /// Encode this snapshot as a gzip-compressed JSON blob
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        let json = serde_json::to_vec(self).map_err(|e| format!("snapshot serialize: {}", e))?;
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&json).map_err(|e| format!("snapshot compress: {}", e))?;
        encoder.finish().map_err(|e| format!("snapshot finish: {}", e))
    }

    /// Decode a gzip-compressed JSON blob into a snapshot
    pub fn decode(data: &[u8]) -> Result<Self, String> {
        use std::io::Read;
        let mut decoder = flate2::read::GzDecoder::new(data);
        let mut json = Vec::new();
        decoder.read_to_end(&mut json).map_err(|e| format!("snapshot decompress: {}", e))?;
        serde_json::from_slice(&json).map_err(|e| format!("snapshot deserialize: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let snap = StoreSnapshot {
            meta: SnapshotMeta { revision: 42, timestamp_secs: 1000, kv_count: 1 },
            entries: vec![SnapshotEntry {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
                create_revision: 1,
                mod_revision: 2,
                version: 1,
                lease: 0,
            }],
        };

        let data = snap.encode().unwrap();
        let decoded = StoreSnapshot::decode(&data).unwrap();
        assert_eq!(decoded.meta.revision, 42);
        assert_eq!(decoded.meta.kv_count, 1);
        assert_eq!(decoded.entries[0].key, b"foo");
        assert_eq!(decoded.entries[0].value, b"bar");
    }

    #[test]
    fn test_empty_snapshot() {
        let snap = StoreSnapshot {
            meta: SnapshotMeta { revision: 0, timestamp_secs: 0, kv_count: 0 },
            entries: vec![],
        };
        let data = snap.encode().unwrap();
        let decoded = StoreSnapshot::decode(&data).unwrap();
        assert_eq!(decoded.meta.revision, 0);
        assert!(decoded.entries.is_empty());
    }

    #[test]
    fn test_large_snapshot() {
        let entries: Vec<SnapshotEntry> = (0..100).map(|i| SnapshotEntry {
            key: format!("key{}", i).into_bytes(),
            value: format!("value{}", i).into_bytes(),
            create_revision: i,
            mod_revision: i + 1,
            version: 1,
            lease: 0,
        }).collect();

        let snap = StoreSnapshot {
            meta: SnapshotMeta { revision: 100, timestamp_secs: 2000, kv_count: entries.len() },
            entries,
        };

        let data = snap.encode().unwrap();
        let decoded = StoreSnapshot::decode(&data).unwrap();
        assert_eq!(decoded.entries.len(), 100);
    }
}
