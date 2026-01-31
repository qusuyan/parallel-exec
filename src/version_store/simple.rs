/**
 * A simple storage implementation based on DashMap.
 */
use super::{VersionStoreError, VersionedStore};
use crate::storage::CacheStore;
use crate::{AccountAddress, Field, Key, StateKey, StorageKey, TxnID, Version, WriteOp};

use std::collections::HashMap;

use dashmap::DashMap;

pub(crate) struct SingleVersionStorage {
    versions: DashMap<Key, TxnID>,
    storage: CacheStore,
}

impl SingleVersionStorage {
    pub(crate) fn new(storage: CacheStore) -> Self {
        Self {
            versions: DashMap::new(),
            storage,
        }
    }

    pub(crate) fn validate(&self, tid: TxnID, read_set: &HashMap<Key, Version>) -> bool {
        for (key, ver) in read_set {
            let current_ver = self.get(tid, key).unwrap();
            if current_ver != *ver {
                return false;
            }
        }
        true
    }

    pub(crate) fn commit(&self, tid: TxnID, write_set: HashMap<Key, WriteOp>) {
        for (key, _) in write_set {
            self.versions.insert(key, tid);
        }
    }
}

impl VersionedStore for SingleVersionStorage {
    fn get(&self, _: TxnID, key: &Key) -> Result<Version, VersionStoreError> {
        let version = self.versions.get(key).map(|e| *e);
        // insert storage access latency if not cached
        let storage_key = match key.field {
            Field::State(ref state_key) => {
                StorageKey::State(key.address.clone(), state_key.clone())
            }
            _ => StorageKey::Account(key.address.clone()),
        };
        self.storage.get(&storage_key);
        Ok(version)
    }

    fn finalize(&mut self) -> Result<HashMap<Key, TxnID>, VersionStoreError> {
        let versions = std::mem::take(&mut self.versions);
        Ok(versions.into_iter().collect())
    }

    fn get_committed_state(
        &self,
        account: &AccountAddress,
        state_key: &StateKey,
    ) -> Result<(), VersionStoreError> {
        let storage_key = StorageKey::State(account.clone(), state_key.clone());
        self.storage.get(&storage_key);
        Ok(())
    }
}
