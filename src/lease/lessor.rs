// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess

use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use tokio::sync::oneshot;

use crate::lease::lease::{Lease, LeaseError, LeaseId, NO_LEASE};
use crate::store::Store;

/// Min-heap item ordered by expiry time.
/// BinaryHeap is a max-heap, so we reverse the ordering.
#[derive(Clone, Eq, PartialEq)]
struct LeaseItem {
    expiry: SystemTime,
    id: LeaseId,
}

impl Ord for LeaseItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.expiry.cmp(&self.expiry)
    }
}

impl PartialOrd for LeaseItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Maximum number of lease revocations per second.
const RATE_LIMIT_PER_SEC: u64 = 1000;

/// Lease manager that tracks leases, handles TTL expiration, and
/// manages the background expiry loop.
pub struct Lessor {
    lease_map: Arc<RwLock<BTreeMap<LeaseId, Arc<RwLock<Lease>>>>>,
    expiring_queue: Arc<Mutex<BinaryHeap<LeaseItem>>>,
    store: Arc<Store>,
    stop_ch: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    paused: Arc<AtomicBool>,
    next_id: AtomicI64,
}

impl Lessor {
    pub fn new(store: Arc<Store>) -> Self {
        let seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;

        Lessor {
            lease_map: Arc::new(RwLock::new(BTreeMap::new())),
            expiring_queue: Arc::new(Mutex::new(BinaryHeap::new())),
            store,
            stop_ch: Arc::new(Mutex::new(None)),
            paused: Arc::new(AtomicBool::new(false)),
            next_id: AtomicI64::new(seed.max(1)),
        }
    }

    /// Generate a unique lease ID using a monotonically increasing counter
    /// seeded from system time.
    fn generate_id(&self) -> LeaseId {
        self.next_id.fetch_add(1, Ordering::Relaxed).abs().max(1)
    }

    /// Grant a new lease.
    ///
    /// If `id` is NO_LEASE (0), a new ID is generated.
    /// Returns the granted lease ID on success.
    pub fn grant(&self, id: LeaseId, ttl: i64) -> Result<LeaseId, LeaseError> {
        if ttl < 0 {
            return Err(LeaseError::TTLTooLarge);
        }

        let lease_id = if id == NO_LEASE {
            // Generate a unique ID
            let mut new_id = self.generate_id();
            loop {
                let map = self.lease_map.read().unwrap();
                if !map.contains_key(&new_id) {
                    break;
                }
                new_id = self.generate_id();
            }
            new_id
        } else {
            let map = self.lease_map.read().unwrap();
            if map.contains_key(&id) {
                return Err(LeaseError::Exists(id));
            }
            id
        };

        let now = SystemTime::now();
        let ttl_abs = ttl.unsigned_abs();
        let expiry = now + Duration::from_secs(ttl_abs);

        let lease = Lease {
            id: lease_id,
            ttl,
            remaining_ttl: ttl as f64,
            expiry,
            keys: Vec::new(),
            checkpoint_ttl: None,
        };

        {
            let mut map = self.lease_map.write().unwrap();
            map.insert(lease_id, Arc::new(RwLock::new(lease)));
        }

        {
            let mut queue = self.expiring_queue.lock().unwrap();
            queue.push(LeaseItem { expiry, id: lease_id });
        }

        Ok(lease_id)
    }

    /// Revoke a lease, returning the list of attached keys.
    pub fn revoke(&self, id: LeaseId) -> Result<Vec<Vec<u8>>, LeaseError> {
        let mut map = self.lease_map.write().unwrap();
        let lease = map.remove(&id).ok_or(LeaseError::NotFound(id))?;
        let lease = lease.read().unwrap();
        Ok(lease.keys.clone())
    }

    /// Renew a lease, extending its expiry. Returns the TTL.
    pub fn renew(&self, id: LeaseId) -> Result<i64, LeaseError> {
        let map = self.lease_map.read().unwrap();
        let lease_arc = map.get(&id).ok_or(LeaseError::NotFound(id))?;
        let mut lease = lease_arc.write().unwrap();

        let now = SystemTime::now();
        let ttl_abs = lease.ttl.unsigned_abs();
        lease.expiry = now + Duration::from_secs(ttl_abs);
        lease.remaining_ttl = lease.ttl as f64;
        let ttl = lease.ttl;

        drop(lease);
        drop(lease_arc);
        drop(map);
        let mut queue = self.expiring_queue.lock().unwrap();
        queue.push(LeaseItem {
            expiry: now + Duration::from_secs(ttl_abs),
            id,
        });

        Ok(ttl)
    }

    /// Attach a key to a lease.
    pub fn attach(&self, id: LeaseId, key: &[u8]) -> Result<(), LeaseError> {
        let map = self.lease_map.read().unwrap();
        let lease_arc = map.get(&id).ok_or(LeaseError::NotFound(id))?;
        let mut lease = lease_arc.write().unwrap();
        if !lease.keys.contains(&key.to_vec()) {
            lease.keys.push(key.to_vec());
        }
        Ok(())
    }

    /// Detach a key from a lease.
    pub fn detach(&self, id: LeaseId, key: &[u8]) -> Result<(), LeaseError> {
        let map = self.lease_map.read().unwrap();
        let lease_arc = map.get(&id).ok_or(LeaseError::NotFound(id))?;
        let mut lease = lease_arc.write().unwrap();
        lease.keys.retain(|k| k.as_slice() != key);
        Ok(())
    }

    /// Look up a lease by ID.
    pub fn lookup(&self, id: LeaseId) -> Option<Arc<RwLock<Lease>>> {
        let map = self.lease_map.read().unwrap();
        map.get(&id).cloned()
    }

    /// Look up the remaining TTL for a lease.
    pub fn lookup_ttl(&self, id: LeaseId) -> Result<i64, LeaseError> {
        let map = self.lease_map.read().unwrap();
        let lease_arc = map.get(&id).ok_or(LeaseError::NotFound(id))?;
        let lease = lease_arc.read().unwrap();
        let now = SystemTime::now();
        let remaining = lease
            .expiry
            .duration_since(now)
            .unwrap_or(Duration::ZERO)
            .as_secs() as i64;
        Ok(remaining)
    }

    /// List all active lease IDs.
    pub fn get_leases(&self) -> Vec<LeaseId> {
        let map = self.lease_map.read().unwrap();
        map.keys().copied().collect()
    }

    /// Start the background expiry loop.
    ///
    /// The loop checks the expiring queue every 500ms and revokes
    /// expired leases at a rate of at most 1000/s.
    pub fn run(&self) {
        let (tx, rx) = oneshot::channel();
        {
            let mut stop = self.stop_ch.lock().unwrap();
            *stop = Some(tx);
        }

        let lease_map = self.lease_map.clone();
        let expiring_queue = self.expiring_queue.clone();
        let store = self.store.clone();
        let paused = self.paused.clone();

        tokio::spawn(async move {
            Self::expiry_loop(lease_map, expiring_queue, store, rx, paused).await;
        });
    }

    /// Stop the background expiry loop.
    pub fn stop(&self) {
        let mut stop = self.stop_ch.lock().unwrap();
        if let Some(tx) = stop.take() {
            let _ = tx.send(());
        }
    }

    /// On leader election, extend all leases by the given duration to
    /// prevent mass expiration during leader transition.
    pub fn promote(&self, extend: Duration) {
        self.paused.store(false, Ordering::Relaxed);
        let now = SystemTime::now();
        let map = self.lease_map.write().unwrap();
        for lease_arc in map.values() {
            let mut lease = lease_arc.write().unwrap();
            lease.expiry = lease.expiry.checked_add(extend).unwrap_or(lease.expiry);
            let new_expiry = lease.expiry;
            let id = lease.id;
            drop(lease);
            let mut queue = self.expiring_queue.lock().unwrap();
            queue.push(LeaseItem {
                expiry: new_expiry,
                id,
            });
        }
    }

    /// On leader step-down, pause the expiry loop.
    pub fn demote(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }

    /// The main expiry loop, running in a background task.
    async fn expiry_loop(
        lease_map: Arc<RwLock<BTreeMap<LeaseId, Arc<RwLock<Lease>>>>>,
        expiring_queue: Arc<Mutex<BinaryHeap<LeaseItem>>>,
        store: Arc<Store>,
        mut stop_rx: oneshot::Receiver<()>,
        paused: Arc<AtomicBool>,
    ) {
        let poll_interval = Duration::from_millis(500);
        let rate_limit_delay = Duration::from_micros(1_000_000 / RATE_LIMIT_PER_SEC);

        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                _ = tokio::time::sleep(poll_interval) => {}
            }

            if paused.load(Ordering::Relaxed) {
                continue;
            }

            // Collect expired leases without holding the map lock
            let to_revoke: Vec<(LeaseId, Vec<Vec<u8>>)> = {
                let now = SystemTime::now();
                let mut queue = expiring_queue.lock().unwrap();
                let map = lease_map.read().unwrap();
                let mut expired = Vec::new();

                'peek: loop {
                    let item = match queue.peek() {
                        Some(item) if item.expiry <= now => item.clone(),
                        _ => break 'peek,
                    };
                    queue.pop();

                    if let Some(lease_arc) = map.get(&item.id) {
                        let lease = lease_arc.read().unwrap();
                        if lease.expiry == item.expiry {
                            // Leases with no keys can be silently revoked
                            let keys = lease.keys.clone();
                            expired.push((item.id, keys));
                        }
                    }
                }

                expired
            };

            // Process expired leases
            for (lease_id, keys) in to_revoke {
                // Remove the lease from the map (it might already be removed by revoke())
                lease_map.write().unwrap().remove(&lease_id);

                // Delete each key from the store, which triggers watcher notifications
                for key in &keys {
                    let _ = store.set(key.clone(), None, None).await;

                    // Rate limiting: at most 1000 revocations per second
                    tokio::time::sleep(rate_limit_delay).await;
                }
            }
        }
    }
}

unsafe impl Send for Lessor {}
unsafe impl Sync for Lessor {}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (Lessor, Arc<Store>) {
        let store = Arc::new(Store::new(None));
        let lessor = Lessor::new(store.clone());
        (lessor, store)
    }

    #[test]
    fn test_lease_grant() {
        let (lessor, _store) = setup();
        let id1 = lessor.grant(NO_LEASE, 10).unwrap();
        assert!(id1 > 0);
        let id2 = lessor.grant(NO_LEASE, 20).unwrap();
        assert_ne!(id1, id2);
        assert_eq!(lessor.lookup(id1).unwrap().read().unwrap().ttl, 10);
    }

    #[test]
    fn test_lease_revoke() {
        let (lessor, _store) = setup();
        let id = lessor.grant(NO_LEASE, 10).unwrap();
        lessor.attach(id, b"test_key").unwrap();
        let keys = lessor.revoke(id).unwrap();
        assert_eq!(keys, vec![b"test_key".to_vec()]);
        assert!(lessor.lookup(id).is_none());
    }

    #[test]
    fn test_lease_renew() {
        let (lessor, _store) = setup();
        let id = lessor.grant(NO_LEASE, 10).unwrap();
        assert_eq!(lessor.renew(id).unwrap(), 10);
    }

    #[test]
    fn test_lease_attach_detach() {
        let (lessor, _store) = setup();
        let id = lessor.grant(NO_LEASE, 10).unwrap();
        lessor.attach(id, b"key1").unwrap();
        assert_eq!(lessor.lookup(id).unwrap().read().unwrap().keys, vec![b"key1".to_vec()]);
        lessor.detach(id, b"key1").unwrap();
        assert!(lessor.lookup(id).unwrap().read().unwrap().keys.is_empty());
    }

    #[test]
    fn test_lease_list() {
        let (lessor, _store) = setup();
        assert!(lessor.get_leases().is_empty());
        let id1 = lessor.grant(NO_LEASE, 10).unwrap();
        let id2 = lessor.grant(NO_LEASE, 20).unwrap();
        assert_eq!(lessor.get_leases().len(), 2);
        lessor.revoke(id1).unwrap();
        assert_eq!(lessor.get_leases().len(), 1);
        assert!(lessor.get_leases().contains(&id2));
    }

    #[test]
    fn test_lease_grant_duplicate_id() {
        let (lessor, _store) = setup();
        lessor.grant(42, 10).unwrap();
        let result = lessor.grant(42, 10);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), LeaseError::Exists(42));
    }

    #[test]
    fn test_lease_grant_negative_ttl() {
        let (lessor, _store) = setup();
        let result = lessor.grant(NO_LEASE, -1);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), LeaseError::TTLTooLarge);
    }

    #[test]
    fn test_lease_revoke_not_found() {
        let (lessor, _store) = setup();
        let result = lessor.revoke(999);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), LeaseError::NotFound(999));
    }

    #[test]
    fn test_lease_renew_not_found() {
        let (lessor, _store) = setup();
        let result = lessor.renew(999);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), LeaseError::NotFound(999));
    }

    #[test]
    fn test_lease_lookup_empty() {
        let (lessor, _store) = setup();
        assert!(lessor.lookup(999).is_none());
    }

    #[test]
    fn test_lease_lookup_after_grant() {
        let (lessor, _store) = setup();
        let id = lessor.grant(NO_LEASE, 10).unwrap();
        assert!(lessor.lookup(id).is_some());
    }

    #[test]
    fn test_lease_attach_not_found() {
        let (lessor, _store) = setup();
        let result = lessor.attach(999, b"key");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), LeaseError::NotFound(999));
    }

    #[test]
    fn test_lease_detach_not_found() {
        let (lessor, _store) = setup();
        let result = lessor.detach(999, b"key");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), LeaseError::NotFound(999));
    }

    #[test]
    fn test_lease_ttl() {
        let (lessor, _store) = setup();
        let id = lessor.grant(NO_LEASE, 60).unwrap();
        let ttl = lessor.lookup_ttl(id).unwrap();
        assert!(ttl >= 0);
        assert!(ttl <= 60);
    }

    #[test]
    fn test_lease_ttl_not_found() {
        let (lessor, _store) = setup();
        let result = lessor.lookup_ttl(999);
        assert!(result.is_err());
    }

    #[test]
    fn test_lease_comprehensive() {
        let (lessor, _store) = setup();
        // Grant
        let id = lessor.grant(NO_LEASE, 30).unwrap();
        assert!(id > 0);
        assert!(lessor.lookup(id).is_some());
        // Attach
        lessor.attach(id, b"k1").unwrap();
        lessor.attach(id, b"k2").unwrap();
        assert_eq!(lessor.lookup(id).unwrap().read().unwrap().keys.len(), 2);
        // Detach one
        lessor.detach(id, b"k1").unwrap();
        assert_eq!(lessor.lookup(id).unwrap().read().unwrap().keys.len(), 1);
        // Renew
        assert_eq!(lessor.renew(id).unwrap(), 30);
        // Revoke
        let keys = lessor.revoke(id).unwrap();
        assert_eq!(keys, vec![b"k2".to_vec()]);
        assert!(lessor.lookup(id).is_none());
    }
}
