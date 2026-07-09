// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess

//! gRPC error handling and status code mapping for etcd domain errors.
//!
//! Maps all etcd domain errors to appropriate [tonic::Status] gRPC status codes,
//! matching the conventions used in etcd's server implementation.

use crate::lease::lease::LeaseError;
use crate::membership::MembershipError;
use tonic::Status;

// ── Generic error mapping ────────────────────────────────────────────────────

/// Map any standard error to a gRPC [tonic::Status].
pub fn to_grpc_error(err: &dyn std::error::Error) -> Status {
    Status::internal(format!("etcdserver: {}", err))
}

// ── Revision errors ──────────────────────────────────────────────────────────

/// Revision has been compacted.
pub fn err_compacted(rev: i64) -> Status {
    Status::new(
        tonic::Code::OutOfRange,
        format!("etcdserver: mvcc: required revision {} has been compacted", rev),
    )
}

/// Revision is a future revision.
pub fn err_future_rev(rev: i64) -> Status {
    Status::new(
        tonic::Code::OutOfRange,
        format!("etcdserver: mvcc: required revision {} is a future revision", rev),
    )
}

// ── Leader errors ────────────────────────────────────────────────────────────

/// No leader is currently elected.
pub fn err_no_leader() -> Status {
    Status::new(
        tonic::Code::Unavailable,
        "etcdserver: no leader",
    )
}

/// This node is not the leader. `leader` is the current leader's node ID.
pub fn err_not_leader(leader: u64) -> Status {
    Status::new(
        tonic::Code::FailedPrecondition,
        format!("etcdserver: not leader, current leader is node {}", leader),
    )
}

// ── Resource limit errors ────────────────────────────────────────────────────

/// Request is too large.
pub fn err_request_too_large() -> Status {
    Status::new(
        tonic::Code::ResourceExhausted,
        "etcdserver: request is too large",
    )
}

/// No space left on the storage backend.
pub fn err_no_space() -> Status {
    Status::new(
        tonic::Code::ResourceExhausted,
        "etcdserver: no space",
    )
}

// ── Lease errors ─────────────────────────────────────────────────────────────

/// Lease not found for the given lease ID.
pub fn err_lease_not_found(id: i64) -> Status {
    Status::new(
        tonic::Code::NotFound,
        format!("etcdserver: lease not found: {}", id),
    )
}

/// Lease already exists for the given lease ID.
pub fn err_lease_exists(id: i64) -> Status {
    Status::new(
        tonic::Code::AlreadyExists,
        format!("etcdserver: lease already exists: {}", id),
    )
}

/// TTL is too large.
pub fn err_lease_ttl_too_large() -> Status {
    Status::new(
        tonic::Code::InvalidArgument,
        "etcdserver: lease TTL too large",
    )
}

// ── Key errors ───────────────────────────────────────────────────────────────

/// Key not found.
pub fn err_key_not_found() -> Status {
    Status::new(
        tonic::Code::NotFound,
        "etcdserver: key not found",
    )
}

// ── Membership error mapping ─────────────────────────────────────────────────

/// Map a [MembershipError] to a gRPC [tonic::Status].
pub fn map_membership_error(err: &MembershipError) -> Status {
    match err {
        MembershipError::IdRemoved => Status::new(
            tonic::Code::NotFound,
            format!("etcdserver: membership: {}", err),
        ),
        MembershipError::IdNotFound => Status::new(
            tonic::Code::NotFound,
            format!("etcdserver: membership: {}", err),
        ),
        MembershipError::IdExists => Status::new(
            tonic::Code::AlreadyExists,
            format!("etcdserver: membership: {}", err),
        ),
        MembershipError::PeerUrlExists => Status::new(
            tonic::Code::AlreadyExists,
            format!("etcdserver: membership: {}", err),
        ),
        MembershipError::MemberNotLearner => Status::new(
            tonic::Code::FailedPrecondition,
            format!("etcdserver: membership: {}", err),
        ),
        MembershipError::TooManyLearners => Status::new(
            tonic::Code::ResourceExhausted,
            format!("etcdserver: membership: {}", err),
        ),
        MembershipError::NotEnoughStartedMembers => Status::new(
            tonic::Code::FailedPrecondition,
            format!("etcdserver: membership: {}", err),
        ),
    }
}

// ── Lease error mapping ──────────────────────────────────────────────────────

/// Map a [LeaseError] to a gRPC [tonic::Status].
pub fn map_lease_error(err: &LeaseError) -> Status {
    match err {
        LeaseError::NotFound(id) => err_lease_not_found(*id),
        LeaseError::Exists(id) => err_lease_exists(*id),
        LeaseError::TTLTooLarge => err_lease_ttl_too_large(),
    }
}

// ── Convenience helpers ──────────────────────────────────────────────────────

/// Check if a status indicates "not leader".
pub fn is_not_leader(status: &Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status.message().contains("not leader")
}

/// Check if a status indicates "no leader".
pub fn is_no_leader(status: &Status) -> bool {
    status.code() == tonic::Code::Unavailable
        && status.message().contains("no leader")
}

/// Check if a status indicates a compacted revision error.
pub fn is_compacted(status: &Status) -> bool {
    status.code() == tonic::Code::OutOfRange
        && status.message().contains("compacted")
}

/// Check if a status indicates a future revision error.
pub fn is_future_rev(status: &Status) -> bool {
    status.code() == tonic::Code::OutOfRange
        && status.message().contains("future revision")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_err_compacted() {
        let status = err_compacted(42);
        assert_eq!(status.code(), tonic::Code::OutOfRange);
        assert!(status.message().contains("compacted"));
        assert!(status.message().contains("42"));
    }

    #[test]
    fn test_err_future_rev() {
        let status = err_future_rev(999);
        assert_eq!(status.code(), tonic::Code::OutOfRange);
        assert!(status.message().contains("future revision"));
        assert!(status.message().contains("999"));
    }

    #[test]
    fn test_err_no_leader() {
        let status = err_no_leader();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("no leader"));
    }

    #[test]
    fn test_err_not_leader() {
        let status = err_not_leader(3);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(status.message().contains("not leader"));
        assert!(status.message().contains("3"));
    }

    #[test]
    fn test_err_request_too_large() {
        let status = err_request_too_large();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    }

    #[test]
    fn test_err_no_space() {
        let status = err_no_space();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert!(status.message().contains("no space"));
    }

    #[test]
    fn test_err_lease_not_found() {
        let status = err_lease_not_found(7);
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert!(status.message().contains("7"));
    }

    #[test]
    fn test_err_lease_exists() {
        let status = err_lease_exists(7);
        assert_eq!(status.code(), tonic::Code::AlreadyExists);
        assert!(status.message().contains("7"));
    }

    #[test]
    fn test_err_key_not_found() {
        let status = err_key_not_found();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_map_membership_id_removed() {
        let status = map_membership_error(&MembershipError::IdRemoved);
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_map_membership_id_not_found() {
        let status = map_membership_error(&MembershipError::IdNotFound);
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_map_membership_id_exists() {
        let status = map_membership_error(&MembershipError::IdExists);
        assert_eq!(status.code(), tonic::Code::AlreadyExists);
    }

    #[test]
    fn test_map_membership_peer_url_exists() {
        let status = map_membership_error(&MembershipError::PeerUrlExists);
        assert_eq!(status.code(), tonic::Code::AlreadyExists);
    }

    #[test]
    fn test_map_membership_member_not_learner() {
        let status = map_membership_error(&MembershipError::MemberNotLearner);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn test_map_membership_too_many_learners() {
        let status = map_membership_error(&MembershipError::TooManyLearners);
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    }

    #[test]
    fn test_map_membership_not_enough_started() {
        let status = map_membership_error(&MembershipError::NotEnoughStartedMembers);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn test_map_lease_not_found() {
        let status = map_lease_error(&LeaseError::NotFound(42));
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_map_lease_exists() {
        let status = map_lease_error(&LeaseError::Exists(42));
        assert_eq!(status.code(), tonic::Code::AlreadyExists);
    }

    #[test]
    fn test_map_lease_ttl_too_large() {
        let status = map_lease_error(&LeaseError::TTLTooLarge);
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_is_not_leader() {
        let s = err_not_leader(1);
        assert!(is_not_leader(&s));
        assert!(!is_no_leader(&s));
    }

    #[test]
    fn test_is_no_leader() {
        let s = err_no_leader();
        assert!(is_no_leader(&s));
        assert!(!is_not_leader(&s));
    }

    #[test]
    fn test_is_compacted() {
        let s = err_compacted(5);
        assert!(is_compacted(&s));
        assert!(!is_future_rev(&s));
    }

    #[test]
    fn test_is_future_rev() {
        let s = err_future_rev(5);
        assert!(is_future_rev(&s));
        assert!(!is_compacted(&s));
    }
}
