/**
 * A simple storage implementation based on DashMap.
 */
use super::{VersionStoreError, VersionedStore};
use crate::storage::{CacheStore, StorageKeyType};
use crate::{AccountAddress, Key, StateKey, StorageKey, TxnID, Value, Version, WriteOp};

use std::collections::HashMap;

use dashmap::DashMap;

pub(crate) struct SingleVersionStorage {
    versions: DashMap<Key, (TxnID, Value)>,
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
            let (_, current_ver) = self.get(tid, key).unwrap();
            if current_ver != *ver {
                return false;
            }
        }
        true
    }

    pub(crate) fn commit(
        &self,
        tid: TxnID,
        write_set: HashMap<Key, WriteOp>,
    ) -> Result<(), VersionStoreError> {
        for (key, write_op) in write_set {
            let value = match write_op {
                WriteOp::Set(v) => v,
                WriteOp::Inc(..) => {
                    let orig = match self.versions.get(&key) {
                        Some(e) => {
                            let (_, orig_val) = *e;
                            orig_val
                        }
                        None => {
                            let storage_key = (&key).into();
                            let Some(orig_val) = self.storage.get(&storage_key) else {
                                return Err(VersionStoreError::IncrementNone);
                            };
                            orig_val
                        }
                    };
                    write_op.apply_to(&orig)
                }
            };
            self.versions.insert(key, (tid, value));
        }
        Ok(())
    }
}

impl VersionedStore for SingleVersionStorage {
    fn get(&self, _: TxnID, key: &Key) -> Result<(Option<Value>, Version), VersionStoreError> {
        let version = self.versions.get(key).map(|e| *e);
        if let Some((ver, val)) = version {
            Ok((Some(val), Some(ver)))
        } else {
            let storage_key = key.into();
            let val = self.storage.get(&storage_key);
            Ok((val, None))
        }
    }

    fn finalize(&mut self) -> Result<HashMap<Key, (TxnID, Value)>, VersionStoreError> {
        let versions = std::mem::take(&mut self.versions);
        Ok(versions.into_iter().collect())
    }

    fn get_committed_state(
        &self,
        account: &AccountAddress,
        state_key: &StateKey,
    ) -> Result<Option<Value>, VersionStoreError> {
        let storage_key = StorageKey {
            account: *account,
            key_type: StorageKeyType::State(*state_key),
        };
        Ok(self.storage.get(&storage_key))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::storage::{CacheStore, DummyStore};
    use crate::{AccountAddress, Field, Key, Value};

    #[test]
    fn test_single_version_storage_commit() {
        let test_key = Key {
            address: AccountAddress::from(12345u64),
            field: Field::Balance,
        };
        let storage_key = (&test_key).into();
        let data = [(storage_key, Value::from(100u64))].into_iter().collect();
        let storage = CacheStore::new(0x1000, Box::new(DummyStore::new(data)));

        let versioned_store = SingleVersionStorage::new(storage);
        // test get from underlying storage
        let (val, ver) = versioned_store.get(5, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(100u64)));
        assert_eq!(ver, None);

        // commit an increment operation
        let write_set = [(test_key.clone(), WriteOp::Inc(Value::from(50u64), true))]
            .into_iter()
            .collect();
        versioned_store.commit(0, write_set).unwrap();
        let (val, ver) = versioned_store.get(5, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(150u64)));
        assert_eq!(ver, Some(0));

        // commit a set operation
        let write_set = [(test_key.clone(), WriteOp::Set(Value::from(200u64)))]
            .into_iter()
            .collect();
        versioned_store.commit(1, write_set).unwrap();
        let (val, ver) = versioned_store.get(5, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(200u64)));
        assert_eq!(ver, Some(1));

        // commit another increment operation
        let write_set = [(test_key.clone(), WriteOp::Inc(Value::from(50u64), true))]
            .into_iter()
            .collect();
        versioned_store.commit(2, write_set).unwrap();
        let (val, ver) = versioned_store.get(5, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(250u64)));
        assert_eq!(ver, Some(2));
    }
}
