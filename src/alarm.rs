// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Debug)]
pub enum AlarmType {
    None = 0,
    NoSpace = 1,
    Corrupt = 2,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AlarmMember {
    pub member_id: u64,
    pub alarm_type: AlarmType,
}

/// Manages active alarms for etcd cluster members.
/// Each alarm type maps to a set of member IDs that have raised that alarm.
pub struct AlarmStore {
    alarms: RwLock<HashMap<AlarmType, HashMap<u64, AlarmMember>>>,
}

impl AlarmStore {
    pub fn new() -> Self {
        AlarmStore {
            alarms: RwLock::new(HashMap::new()),
        }
    }

    /// Activate an alarm for the given member. Returns all currently active alarms of this type.
    pub fn activate(&self, member_id: u64, alarm_type: AlarmType) -> Vec<AlarmMember> {
        let mut alarms = self.alarms.write().unwrap();
        let type_alarms = alarms.entry(alarm_type).or_insert_with(HashMap::new);
        type_alarms
            .entry(member_id)
            .or_insert(AlarmMember {
                member_id,
                alarm_type,
            });
        type_alarms.values().cloned().collect()
    }

    /// Deactivate an alarm for the given member. Returns all remaining active alarms.
    pub fn deactivate(&self, member_id: u64, alarm_type: AlarmType) -> Vec<AlarmMember> {
        let mut alarms = self.alarms.write().unwrap();
        if let Some(type_alarms) = alarms.get_mut(&alarm_type) {
            type_alarms.remove(&member_id);
            if type_alarms.is_empty() {
                alarms.remove(&alarm_type);
            }
        }
        alarms
            .values()
            .flat_map(|m| m.values().cloned())
            .collect()
    }

    /// List all active alarms across all types and members.
    pub fn get_alarms(&self) -> Vec<AlarmMember> {
        let alarms = self.alarms.read().unwrap();
        alarms
            .values()
            .flat_map(|m| m.values().cloned())
            .collect()
    }

    /// Check if any member has raised the given alarm type.
    pub fn is_active(&self, alarm_type: AlarmType) -> bool {
        let alarms = self.alarms.read().unwrap();
        alarms.contains_key(&alarm_type)
    }

    /// Serialize all active alarms to JSON bytes for snapshot storage.
    pub fn serialize(&self) -> Vec<u8> {
        let alarms = self.alarms.read().unwrap();
        let flat: Vec<AlarmMember> = alarms
            .values()
            .flat_map(|m| m.values().cloned())
            .collect();
        serde_json::to_vec(&flat).unwrap_or_default()
    }

    /// Deserialize alarms from JSON bytes to restore from a snapshot.
    pub fn deserialize(data: &[u8]) -> Self {
        let flat: Vec<AlarmMember> = serde_json::from_slice(data).unwrap_or_default();
        let mut alarms: HashMap<AlarmType, HashMap<u64, AlarmMember>> = HashMap::new();
        for member in flat {
            alarms
                .entry(member.alarm_type)
                .or_insert_with(HashMap::new)
                .insert(member.member_id, member);
        }
        AlarmStore {
            alarms: RwLock::new(alarms),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_activate_and_get_alarms() {
        let store = AlarmStore::new();
        assert!(store.get_alarms().is_empty());

        let result = store.activate(1, AlarmType::NoSpace);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].member_id, 1);
        assert_eq!(result[0].alarm_type, AlarmType::NoSpace);

        let alarms = store.get_alarms();
        assert_eq!(alarms.len(), 1);
    }

    #[test]
    fn test_deactivate() {
        let store = AlarmStore::new();
        store.activate(1, AlarmType::NoSpace);
        store.activate(2, AlarmType::NoSpace);
        assert_eq!(store.get_alarms().len(), 2);

        let remaining = store.deactivate(1, AlarmType::NoSpace);
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].member_id, 2);
    }

    #[test]
    fn test_is_active() {
        let store = AlarmStore::new();
        assert!(!store.is_active(AlarmType::NoSpace));
        store.activate(1, AlarmType::NoSpace);
        assert!(store.is_active(AlarmType::NoSpace));
        assert!(!store.is_active(AlarmType::Corrupt));
    }

    #[test]
    fn test_serialize_deserialize() {
        let store = AlarmStore::new();
        store.activate(1, AlarmType::NoSpace);
        store.activate(2, AlarmType::Corrupt);

        let data = store.serialize();
        assert!(!data.is_empty());

        let restored = AlarmStore::deserialize(&data);
        let alarms = restored.get_alarms();
        assert_eq!(alarms.len(), 2);
    }

    #[test]
    fn test_multiple_alarm_types() {
        let store = AlarmStore::new();
        store.activate(1, AlarmType::NoSpace);
        store.activate(1, AlarmType::Corrupt);

        let alarms = store.get_alarms();
        assert_eq!(alarms.len(), 2);

        assert!(store.is_active(AlarmType::NoSpace));
        assert!(store.is_active(AlarmType::Corrupt));
    }
}
