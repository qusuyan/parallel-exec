/**
 * Multi-Version Storage implementation based on Block-STM.
 * The nonce corresponds to incarnation number, which is unique per transaction attempt.
 */
use super::{Storage, StorageError};
use crate::{Key, TxnID, Version, WriteOp};

use std::collections::{HashMap, HashSet};

use dashmap::DashMap;

#[derive(Clone, Debug)]
enum StorageEntry {
    Version(usize),
    Estimate,
}

pub(crate) struct MVStorage {
    storage: DashMap<Key, DashMap<TxnID, StorageEntry>>,
    last_write_set: DashMap<TxnID, HashSet<Key>>,
}

impl MVStorage {
    pub(crate) fn new() -> Self {
        Self {
            storage: DashMap::new(),
            last_write_set: DashMap::new(),
        }
    }

    #[inline]
    fn version(tid: TxnID, nonce: u32) -> usize {
        tid << 32 | nonce as usize
    }

    fn apply_write_set(&self, tid: TxnID, nonce: u32, write_set: &HashMap<Key, WriteOp>) {
        let version = Self::version(tid, nonce);
        for (key, _) in write_set {
            let entry = StorageEntry::Version(version);
            let versions = self
                .storage
                .entry(key.clone())
                .or_insert_with(|| DashMap::new());
            versions.insert(version, entry);
        }
    }

    // return if any written keys are newly written
    fn rcu_update_written_locations(&self, tid: TxnID, new_keys: HashSet<Key>) -> bool {
        let prev_keys = self.last_write_set.get(&tid);
        let ret = if let Some(prev_keys) = prev_keys {
            for key in prev_keys.difference(&new_keys) {
                if let Some(versions) = self.storage.get(key) {
                    versions.remove(&tid);
                }
            }
            new_keys.is_subset(&prev_keys.value())
        } else {
            // first time writing, all keys are new
            false
        };
        self.last_write_set.insert(tid, new_keys);
        ret
    }

    // called when finish execution - so that write set is immediately visible
    pub(crate) fn record(&self, tid: TxnID, nonce: u32, write_set: HashMap<Key, WriteOp>) -> bool {
        // write new values
        self.apply_write_set(tid, nonce, &write_set);
        let wrote_keys = write_set.keys().cloned().collect();
        // clear out old writes and return if any key is not written in the last time
        self.rcu_update_written_locations(tid, wrote_keys)
    }

    // called when transaction fails - revert writes and leave an Estimate marker
    pub(crate) fn convert_writes_to_estimates(&self, tid: TxnID) {
        if let Some(write_keys) = self.last_write_set.get(&tid) {
            for key in write_keys.iter() {
                if let Some(versions) = self.storage.get(key) {
                    if let Some(mut entry) = versions.get_mut(&tid) {
                        *entry.value_mut() = StorageEntry::Estimate;
                    }
                }
            }
        }
    }
}

impl Storage for MVStorage {
    fn get(&self, tid: TxnID, key: &Key) -> Result<Version, StorageError> {
        let version = self.storage.get(key).and_then(|versions| {
            // find the latest version less than tid
            versions
                .iter()
                .filter(|e| *e.key() < tid)
                .max_by_key(|e| *e.key())
                .map(|e| e.value().clone())
        });
        match version {
            Some(StorageEntry::Version(v)) => Ok(Some(v)),
            Some(StorageEntry::Estimate) => Err(StorageError::ReadToBeModified),
            None => Ok(None),
        }
    }

    fn validate(&self, tid: TxnID, read_set: &HashMap<Key, Version>) -> bool {
        for (key, ver) in read_set {
            match self.get(tid, key) {
                Ok(current_ver) => {
                    if current_ver != *ver {
                        return false;
                    }
                }
                Err(_) => return false,
            }
        }
        true
    }

    fn commit(&self, _tid: TxnID, _write_set: HashMap<Key, WriteOp>) {
        unreachable!(
            "MVStorage should call commit - it will speculatively publish write set and revert if validation fails"
        );
    }
}
