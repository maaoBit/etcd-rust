// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Produces unique IDs within a member.
///
/// ID layout: `(member_id << 40) | (time_bits << 24) | counter`
/// - `member_id` occupies the top 24 bits
/// - `time_bits` occupies the next 16 bits (seconds since epoch, wrapping)
/// - `counter` occupies the bottom 24 bits
pub struct IdGenerator {
    member_id: u64,
    counter: AtomicU64,
}

impl IdGenerator {
    pub fn new(member_id: u64) -> Self {
        Self {
            member_id,
            counter: AtomicU64::new(0),
        }
    }

    pub fn next(&self) -> u64 {
        let counter = self.counter.fetch_add(1, Ordering::Relaxed) & 0xFF_FFFF;
        let time_bits = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            & 0xFFFF) as u64;
        (self.member_id << 40) | (time_bits << 24) | counter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_id_generator_next() {
        let gen = IdGenerator::new(1);
        let a = gen.next();
        let b = gen.next();
        assert_ne!(a, b);
    }

    #[test]
    fn test_id_generator_member_id() {
        let gen_a = IdGenerator::new(0xA);
        let gen_b = IdGenerator::new(0xB);
        let a = gen_a.next();
        let b = gen_b.next();
        // The top 24 bits should differ because member_id differs
        assert_ne!((a >> 40) & 0xFF_FFFF, (b >> 40) & 0xFF_FFFF);
    }

    #[test]
    fn test_id_generator_uniqueness() {
        let gen = IdGenerator::new(1);
        let mut ids = HashSet::new();
        for _ in 0..1000 {
            let id = gen.next();
            assert!(ids.insert(id), "ID {} was a duplicate", id);
        }
        assert_eq!(ids.len(), 1000);
    }
}
