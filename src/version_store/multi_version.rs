/**
 * Multi-Version Storage implementation based on Block-STM.
 * The nonce corresponds to incarnation number, which is unique per transaction attempt.
 */
use super::{VersionStoreError, VersionedStore};
use crate::storage::CacheStore;
use crate::{Field, Key, StorageKey, TxnID, Version, WriteOp};

use std::collections::{HashMap, HashSet};

use dashmap::DashMap;

#[derive(Clone, Debug)]
enum StorageEntry {
    Version(usize),
    Estimate,
}

pub(crate) struct MultiVersionStorage {
    storage: CacheStore,
    version_manager: DashMap<Key, DashMap<TxnID, StorageEntry>>,
    last_write_set: DashMap<TxnID, HashSet<Key>>,
    last_read_set: DashMap<TxnID, HashMap<Key, Version>>,
}

impl MultiVersionStorage {
    pub(crate) fn new(storage: CacheStore) -> Self {
        Self {
            storage,
            version_manager: DashMap::new(),
            last_write_set: DashMap::new(),
            last_read_set: DashMap::new(),
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
                .version_manager
                .entry(key.clone())
                .or_insert_with(|| DashMap::new());
            versions.insert(tid, entry);
        }
    }

    // remove writes from previous executions that are not written this time
    // return if any key in new_keys was not written in the last time
    // |-- prev_keys --|
    //           |-- new_keys --|
    // |---------|                <= keys to be deleted
    //                 | -------| <= return true if this is empty
    fn rcu_update_written_locations(&self, tid: TxnID, new_keys: HashSet<Key>) -> bool {
        let prev_keys = self.last_write_set.get(&tid);
        let ret = if let Some(prev_keys) = prev_keys {
            for key in prev_keys.difference(&new_keys) {
                if let Some(versions) = self.version_manager.get(key) {
                    versions.remove(&tid);
                }
            }
            new_keys.is_subset(&prev_keys.value())
        } else {
            // first time writing, all keys are new
            new_keys.is_empty()
        };
        self.last_write_set.insert(tid, new_keys);
        ret
    }

    // called when finish execution - so that write set is immediately visible
    // return if any key in the write set was not written in the last time
    pub(crate) fn record(
        &self,
        tid: TxnID,
        nonce: u32,
        read_set: HashMap<Key, Version>,
        write_set: HashMap<Key, WriteOp>,
    ) -> bool {
        // write new values
        self.apply_write_set(tid, nonce, &write_set);
        let wrote_keys = write_set.keys().cloned().collect();
        // clear out old writes and return if any key is not written in the last time
        let ret = self.rcu_update_written_locations(tid, wrote_keys);
        // record read set
        self.last_read_set.insert(tid, read_set);
        ret
    }

    // called when transaction fails - revert writes and leave an Estimate marker
    pub(crate) fn convert_writes_to_estimates(&self, tid: TxnID) {
        let write_keys = self.last_write_set.get(&tid).expect(&format!(
            "Txn {:x}: converting writes to estimates before publishing write set",
            tid
        ));
        for key in write_keys.iter() {
            if let Some(versions) = self.version_manager.get(key) {
                let mut entry = versions.get_mut(&tid).expect(&format!(
                    "Txn {:x}: entry {:?} in write set is missing",
                    tid, key
                ));
                *entry.value_mut() = StorageEntry::Estimate;
            }
        }
    }

    pub(crate) fn validate_read_set(&self, tid: TxnID) -> bool {
        let read_set = self.last_read_set.get(&tid).expect(&format!(
            "Txn {}: validating read set before publishing read set",
            tid
        ));
        for (key, ver) in read_set.iter() {
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
}

impl VersionedStore for MultiVersionStorage {
    fn get(&self, tid: TxnID, key: &Key) -> Result<Version, VersionStoreError> {
        let entry = self.version_manager.get(key).and_then(|versions| {
            // find the latest version less than tid
            versions
                .iter()
                .filter(|e| *e.key() < tid)
                .max_by_key(|e| *e.key())
                .map(|e| (e.key().clone(), e.value().clone()))
        });
        match entry {
            Some((_, StorageEntry::Version(v))) => Ok(Some(v)),
            Some((prev_tid, StorageEntry::Estimate)) => {
                Err(VersionStoreError::ReadToBeModified(prev_tid))
            }
            None => {
                let storage_key = match key.field {
                    Field::State(ref state_key) => {
                        StorageKey::State(key.address.clone(), state_key.clone())
                    }
                    _ => StorageKey::Account(key.address.clone()),
                };
                self.storage.get(&storage_key);
                Ok(None)
            }
        }
    }

    fn finalize(&mut self) -> Result<HashMap<Key, TxnID>, VersionStoreError> {
        let version_manager = std::mem::take(&mut self.version_manager);
        let finalized_map = version_manager
            .into_iter()
            .filter_map(|(key, versions)| {
                // get the latest committed version
                let max_version = versions
                    .into_iter()
                    .filter_map(|(tid, version)| match version {
                        StorageEntry::Version(_) => Some(tid),
                        StorageEntry::Estimate => panic!("Estimate marks should have been removed"),
                    })
                    .max();
                max_version.map(|tid| (key, tid))
            })
            .collect();
        Ok(finalized_map)
    }

    fn get_committed_state(
        &self,
        account: &crate::AccountAddress,
        state_key: &crate::StateKey,
    ) -> Result<(), VersionStoreError> {
        let storage_key = StorageKey::State(account.clone(), state_key.clone());
        self.storage.get(&storage_key);
        Ok(())
    }
}
