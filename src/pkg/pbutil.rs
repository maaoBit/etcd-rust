// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
use prost::Message;

use crate::etcdserverpb::request_op::Request;
use crate::etcdserverpb::RequestOp;

/// Marshal a protobuf message into a `Vec<u8>`, panicking on error.
/// Analogous to `proto.Marshal` / `gogo protobuf` in Go etcd.
pub fn must_marshal<T: Message>(msg: &T) -> Vec<u8> {
    let mut buf = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut buf).expect("protobuf encode should not fail");
    buf
}

/// Unmarshal a protobuf message from a byte slice, panicking on error.
/// Analogous to `proto.Unmarshal` in Go etcd.
pub fn must_unmarshal<T: Message + Default>(data: &[u8]) -> T {
    T::decode(data).expect("protobuf decode should not fail")
}

/// Returns `true` if the `RequestOp` contains a nested `TxnRequest`, which may
/// need to be handled as a fragmented operation across multiple Raft entries.
pub fn is_maybe_fragmented(op: &RequestOp) -> Result<bool, String> {
    match &op.request {
        Some(Request::RequestTxn(_)) => Ok(true),
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etcdserverpb::*;
    use crate::mvccpb::KeyValue;

    #[test]
    fn test_marshal_roundtrip() {
        use bytes::Bytes;
        let kv = KeyValue {
            key: b"foo".to_vec(),
            value: Bytes::from_static(b"bar"),
            create_revision: 1,
            mod_revision: 2,
            version: 3,
            lease: 0,
        };
        let data = must_marshal(&kv);
        let decoded: KeyValue = must_unmarshal(&data);
        assert_eq!(decoded.key.as_slice(), b"foo");
        assert_eq!(decoded.value.as_ref(), b"bar");
        assert_eq!(decoded.create_revision, 1);
    }

    #[test]
    fn test_is_maybe_fragmented() {
        let range_op = RequestOp {
            request: Some(Request::RequestRange(RangeRequest::default())),
        };
        assert!(!is_maybe_fragmented(&range_op).unwrap());

        let txn_op = RequestOp {
            request: Some(Request::RequestTxn(TxnRequest::default())),
        };
        assert!(is_maybe_fragmented(&txn_op).unwrap());
    }
}
