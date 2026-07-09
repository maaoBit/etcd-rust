// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tokio::sync::oneshot;

/// A wait/notification mechanism.
///
/// `register()` creates a unique ID and returns it together with a `oneshot::Receiver`.
/// A separate call to `trigger(id, data)` sends data to the registered receiver.
pub struct Wait {
    chs: Mutex<HashMap<u64, oneshot::Sender<Vec<u8>>>>,
    next_id: AtomicU64,
}

impl Wait {
    pub fn new() -> Self {
        Self {
            chs: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Register a new waiter and return `(id, receiver)`.
    pub fn register(&self) -> (u64, oneshot::Receiver<Vec<u8>>) {
        let (tx, rx) = oneshot::channel();
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.chs.lock().unwrap().insert(id, tx);
        (id, rx)
    }

    /// Trigger the waiter identified by `id` with the given data.
    /// Returns `true` if the waiter was found and triggered.
    pub fn trigger(&self, id: u64, data: Vec<u8>) -> bool {
        if let Some(tx) = self.chs.lock().unwrap().remove(&id) {
            let _ = tx.send(data);
            true
        } else {
            false
        }
    }

    /// Check whether a waiter with the given `id` is registered.
    pub fn is_registered(&self, id: u64) -> bool {
        self.chs.lock().unwrap().contains_key(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wait_register_trigger() {
        let w = Wait::new();
        let (id, rx) = w.register();
        assert!(w.trigger(id, b"hello".to_vec()));
        assert_eq!(rx.await.unwrap(), b"hello");
    }

    #[tokio::test]
    async fn test_wait_trigger_not_found() {
        let w = Wait::new();
        assert!(!w.trigger(999, b"data".to_vec()));
    }

    #[tokio::test]
    async fn test_wait_is_registered() {
        let w = Wait::new();
        let (id, _rx) = w.register();
        assert!(w.is_registered(id));
        w.trigger(id, b"data".to_vec());
        assert!(!w.is_registered(id));
    }

    #[tokio::test]
    async fn test_wait_multiple_waiters() {
        let w = Wait::new();
        let waiters: Vec<(u64, oneshot::Receiver<Vec<u8>>)> =
            (0..5).map(|_| w.register()).collect();

        for (id, _) in &waiters {
            assert!(w.trigger(*id, b"data".to_vec()));
        }

        for (_, rx) in waiters {
            assert_eq!(rx.await.unwrap(), b"data");
        }
    }

    #[tokio::test]
    async fn test_wait_trigger_multiple_times() {
        let w = Wait::new();
        let (id, rx) = w.register();

        // First trigger should succeed
        assert!(w.trigger(id, b"first".to_vec()));
        assert_eq!(rx.await.unwrap(), b"first");

        // Second trigger should fail (already consumed)
        assert!(!w.trigger(id, b"second".to_vec()));
    }
}
