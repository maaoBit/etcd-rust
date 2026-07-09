// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess

use std::time::SystemTime;
use std::fmt;

pub type LeaseId = i64;
pub const NO_LEASE: LeaseId = 0;

/// Represents a single lease with TTL tracking and attached keys.
#[derive(Clone)]
pub struct Lease {
    pub id: LeaseId,
    pub ttl: i64,
    pub remaining_ttl: f64,
    pub expiry: SystemTime,
    pub keys: Vec<Vec<u8>>,
    pub checkpoint_ttl: Option<i64>,
}

/// Errors that can occur during lease operations.
#[derive(Debug, Clone, PartialEq)]
pub enum LeaseError {
    NotFound(LeaseId),
    Exists(LeaseId),
    TTLTooLarge,
}

impl fmt::Display for LeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LeaseError::NotFound(id) => write!(f, "lease not found: {}", id),
            LeaseError::Exists(id) => write!(f, "lease already exists: {}", id),
            LeaseError::TTLTooLarge => write!(f, "ttl too large"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lease_error_display() {
        assert_eq!(
            format!("{}", LeaseError::NotFound(42)),
            "lease not found: 42"
        );
        assert_eq!(
            format!("{}", LeaseError::Exists(7)),
            "lease already exists: 7"
        );
        assert_eq!(
            format!("{}", LeaseError::TTLTooLarge),
            "ttl too large"
        );
    }

    #[test]
    fn test_lease_error_equality() {
        assert_eq!(LeaseError::NotFound(1), LeaseError::NotFound(1));
        assert_eq!(LeaseError::Exists(2), LeaseError::Exists(2));
        assert_eq!(LeaseError::TTLTooLarge, LeaseError::TTLTooLarge);
        assert_ne!(LeaseError::NotFound(1), LeaseError::NotFound(2));
        assert_ne!(LeaseError::NotFound(1), LeaseError::Exists(1));
    }

    #[test]
    fn test_lease_creation() {
        let lease = Lease {
            id: 1,
            ttl: 10,
            remaining_ttl: 10.0,
            expiry: SystemTime::now(),
            keys: vec![b"key1".to_vec(), b"key2".to_vec()],
            checkpoint_ttl: None,
        };
        assert_eq!(lease.id, 1);
        assert_eq!(lease.ttl, 10);
        assert_eq!(lease.keys.len(), 2);
    }

    #[test]
    fn test_no_lease_constant() {
        assert_eq!(NO_LEASE, 0);
    }
}
