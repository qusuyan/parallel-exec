/**
 * Multi-Version Storage implementation based on Block-STM.
 * The nonce corresponds to incarnation number, which is unique per transaction attempt.
 */
use super::{VersionStoreError, VersionedStore};
use crate::storage::{CacheStore, StorageKeyType};
use crate::{Key, StorageKey, TxnID, Value, Version, WriteOp};

use dashmap::DashMap;
use primitive_types::U256;
use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;

#[derive(Clone, Debug)]
enum StorageEntry {
    Version(u32, WriteOp),
    Estimate,
}

pub(crate) struct MultiVersionStorage {
    storage: CacheStore,
    version_manager: DashMap<Key, RwLock<Vec<(TxnID, StorageEntry)>>>,
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
        for (key, op) in write_set {
            let entry = StorageEntry::Version(nonce, op.clone());
            let versions = self
                .version_manager
                .entry(key.clone())
                .or_insert_with(|| RwLock::new(Vec::new()));
            let mut versions_mut = loop {
                if let Ok(guard) = versions.try_write() {
                    break guard;
                }
            };
            // increment nonce of all increments applied by a later txn until a Set or an ESTIMATE is found
            let mut insert_idx = versions_mut.len();
            for (idx, (other_tid, storage_entry)) in versions_mut.iter_mut().enumerate() {
                if *other_tid < tid {
                    continue;
                }
                if idx < insert_idx {
                    insert_idx = idx;
                }
                if let StorageEntry::Version(nonce, WriteOp::Inc(..)) = storage_entry {
                    *nonce += 1
                } else {
                    break;
                }
            }
            // insert new version entry in ascending order of TxnID
            versions_mut.insert(insert_idx, (tid, entry));
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
                    let mut versions_mut = loop {
                        if let Ok(guard) = versions.try_write() {
                            break guard;
                        }
                    };
                    // increment nonce of all increments applied by a later txn until a Set or an ESTIMATE is found
                    let mut remove_idx = None;
                    for (idx, (other_tid, storage_entry)) in versions_mut.iter_mut().enumerate() {
                        if *other_tid < tid {
                            continue;
                        } else if *other_tid == tid {
                            remove_idx = Some(idx);
                            continue;
                        } else {
                            if remove_idx.is_none() {
                                break;
                            }
                            if let StorageEntry::Version(nonce, WriteOp::Inc(..)) = storage_entry {
                                *nonce += 1
                            } else {
                                break;
                            }
                        }
                    }
                    if let Some(idx) = remove_idx {
                        versions_mut.remove(idx);
                    }
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

    // called when transaction fails - revert writes and leave an Estimate marker, return the highest nonce
    pub(crate) fn convert_writes_to_estimates(&self, tid: TxnID) -> u32 {
        let write_keys = self.last_write_set.get(&tid).expect(&format!(
            "Txn {:x}: converting writes to estimates before publishing write set",
            tid
        ));
        let mut highest_nonce = 0;
        for key in write_keys.iter() {
            if let Some(versions) = self.version_manager.get(key) {
                let mut versions_mut = loop {
                    if let Ok(guard) = versions.try_write() {
                        break guard;
                    }
                };
                for (version_tid, storage_entry) in versions_mut.iter_mut() {
                    if *version_tid == tid {
                        // remove the version entry for this txn
                        if let StorageEntry::Version(nonce, _) = storage_entry {
                            if *nonce > highest_nonce {
                                highest_nonce = *nonce;
                            }
                        }
                        *storage_entry = StorageEntry::Estimate;
                        break;
                    } else if *version_tid > tid {
                        panic!("Txn {tid:x}: entry {key:?} in write set is missing");
                    }
                }
            }
        }
        highest_nonce
    }

    pub(crate) fn validate_read_set(&self, tid: TxnID) -> bool {
        let read_set = self.last_read_set.get(&tid).expect(&format!(
            "Txn {tid}: validating read set before publishing read set",
        ));
        for (key, ver) in read_set.iter() {
            match self.get(tid, key) {
                Ok((_, current_ver)) => {
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
    fn get(&self, tid: TxnID, key: &Key) -> Result<(Option<Value>, Version), VersionStoreError> {
        let agg_op = match self.version_manager.get(key) {
            Some(writes) => {
                let writes_guard = loop {
                    if let Ok(w) = writes.try_read() {
                        break w;
                    }
                };
                if writes_guard.is_empty() {
                    // no versions written, return base value
                    None
                } else {
                    let mut agg_op = WriteOp::Inc(U256::zero(), true);
                    let mut latest_version = None;
                    let mut estimate_mark = None; // if this read depends on a pending txn
                    for (prev_tid, storage_entry) in writes_guard.iter() {
                        // stop when reaching versions written by later txns
                        if prev_tid >= &tid {
                            break;
                        }
                        match storage_entry {
                            StorageEntry::Version(nonce, op) => {
                                latest_version = Some(Self::version(*prev_tid, *nonce));
                                if let WriteOp::Set(..) = op {
                                    // Set overrides all previous writes
                                    agg_op = op.clone();
                                    estimate_mark = None;
                                } else {
                                    // accumulate increments
                                    agg_op = agg_op - *op;
                                }
                            }
                            StorageEntry::Estimate => {
                                // mark prev_tid as a dependency
                                estimate_mark = Some(*prev_tid);
                            }
                        }
                    }
                    if let Some(pending_txn) = estimate_mark {
                        // agg_op depends on a pending txn
                        return Err(VersionStoreError::ReadToBeModified(pending_txn));
                    }
                    match agg_op {
                        WriteOp::Set(v) => return Ok((Some(v), latest_version)), // directly return Set value
                        WriteOp::Inc(..) => Some((agg_op, latest_version)), // continue to apply increments
                    }
                }
            }
            None => None, // no versions written, return base value
        };
        // at this point, we either have no writes, or we need to apply increments to base value
        let base_value = self.storage.get(&key.into());
        if let Some((agg_op, version)) = agg_op {
            let final_value = agg_op.apply_to(&base_value.unwrap_or_else(U256::zero));
            Ok((Some(final_value), version))
        } else {
            Ok((base_value, None))
        }
    }

    fn finalize(&mut self) -> Result<HashMap<Key, (TxnID, Value)>, VersionStoreError> {
        let version_manager = std::mem::take(&mut self.version_manager);
        let kv_pairs = version_manager.into_iter().filter_map(|(key, versions)| {
            // get the latest txn that writes the kv pair, and the aggregated write op
            versions
                .into_inner()
                .into_iter()
                .map(|(tid, entry)| match entry {
                    StorageEntry::Version(_, op) => (tid, op),
                    StorageEntry::Estimate => panic!("Estimate marks should have been removed"),
                })
                .reduce(|(_, op1), (tid2, op2)| (tid2, op1 - op2))
                .map(|entry| (key, entry))
        });
        let mut finalized_map = HashMap::new();
        for (key, (tid, agg_op)) in kv_pairs {
            match agg_op {
                WriteOp::Set(v) => {
                    finalized_map.insert(key, (tid, v));
                }
                WriteOp::Inc(..) => {
                    let Some(base_value) = self.storage.get(&(&key).into()) else {
                        return Err(VersionStoreError::IncrementNone);
                    };
                    finalized_map.insert(key, (tid, agg_op.apply_to(&base_value)));
                }
            }
        }
        Ok(finalized_map)
    }

    fn get_committed_state(
        &self,
        account: &crate::AccountAddress,
        state_key: &crate::StateKey,
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
    use crate::{AccountAddress, Field};

    #[test]
    fn test_multi_version_storage_get_no_reexec() {
        let test_key = Key {
            address: AccountAddress::from(12345u64),
            field: Field::Balance,
        };
        let storage_key = (&test_key).into();
        let data = [(storage_key, Value::from(100u64))].into_iter().collect();
        let storage = CacheStore::new(0x1000, Box::new(DummyStore::new(data)));

        let mv_storage = MultiVersionStorage::new(storage);
        // Txn 1 writes +50
        let write_set = [(test_key.clone(), WriteOp::Inc(Value::from(50u64), true))]
            .into_iter()
            .collect();
        mv_storage.record(1, 0, HashMap::new(), write_set);
        // Txn 5 writes set to 200
        let write_set = [(test_key.clone(), WriteOp::Set(Value::from(200u64)))]
            .into_iter()
            .collect();
        mv_storage.record(5, 0, HashMap::new(), write_set);
        // Txn 7 writes +30
        let write_set = [(test_key.clone(), WriteOp::Inc(Value::from(30u64), true))]
            .into_iter()
            .collect();
        mv_storage.record(7, 0, HashMap::new(), write_set);

        // txn 0 reads base value (100)
        let (val, ver) = mv_storage.get(0, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(100u64)));
        assert_eq!(ver, None);
        // txn 2 reads 150
        let (val, ver) = mv_storage.get(2, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(150u64)));
        assert_eq!(ver, Some(MultiVersionStorage::version(1, 0)));
        // txn 4 reads 150
        let (val, ver) = mv_storage.get(4, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(150u64)));
        assert_eq!(ver, Some(MultiVersionStorage::version(1, 0)));
        // txn 6 reads 200
        let (val, ver) = mv_storage.get(6, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(200u64)));
        assert_eq!(ver, Some(MultiVersionStorage::version(5, 0)));
        // txn 8 reads 230
        let (val, ver) = mv_storage.get(8, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(230u64)));
        assert_eq!(ver, Some(MultiVersionStorage::version(7, 0)));

        // txn 3 writes +20
        let write_set = [(test_key.clone(), WriteOp::Inc(Value::from(20u64), true))]
            .into_iter()
            .collect();
        mv_storage.record(3, 0, HashMap::new(), write_set);
        // txn 0 reads base value (100)
        let (val, ver) = mv_storage.get(0, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(100u64)));
        assert_eq!(ver, None);
        // txn 2 reads 150
        let (val, ver) = mv_storage.get(2, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(150u64)));
        assert_eq!(ver, Some(MultiVersionStorage::version(1, 0)));
        // txn 4 reads 170
        let (val, ver) = mv_storage.get(4, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(170u64)));
        assert_eq!(ver, Some(MultiVersionStorage::version(3, 0)));
        // txn 6 reads 200
        let (val, ver) = mv_storage.get(6, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(200u64)));
        assert_eq!(ver, Some(MultiVersionStorage::version(5, 0)));
        // txn 8 reads 230
        let (val, ver) = mv_storage.get(8, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(230u64)));
        assert_eq!(ver, Some(MultiVersionStorage::version(7, 0)));
    }

    #[test]
    fn test_multi_version_storage_get_with_reexec() {
        let test_key = Key {
            address: AccountAddress::from(12345u64),
            field: Field::Balance,
        };
        let storage_key = (&test_key).into();
        let data = [(storage_key, Value::from(100u64))].into_iter().collect();
        let storage = CacheStore::new(0x1000, Box::new(DummyStore::new(data)));

        let mv_storage = MultiVersionStorage::new(storage);
        // Txn 1 writes +50
        let write_set = [(test_key.clone(), WriteOp::Inc(Value::from(50u64), true))]
            .into_iter()
            .collect();
        mv_storage.record(1, 0, HashMap::new(), write_set);
        // txn 3 writes +20
        let write_set = [(test_key.clone(), WriteOp::Inc(Value::from(20u64), true))]
            .into_iter()
            .collect();
        mv_storage.record(3, 0, HashMap::new(), write_set);
        // Txn 5 writes set to 200
        let write_set = [(test_key.clone(), WriteOp::Set(Value::from(200u64)))]
            .into_iter()
            .collect();
        mv_storage.record(5, 0, HashMap::new(), write_set);
        // Txn 1 fails validation and gets reverted
        mv_storage.convert_writes_to_estimates(1);

        // txn 0 reads base value (100)
        let (val, ver) = mv_storage.get(0, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(100u64)));
        assert_eq!(ver, None);
        // txn 2 reads estimate - depends on txn 1
        let err = mv_storage.get(2, &test_key).unwrap_err();
        assert!(matches!(
            err,
            VersionStoreError::ReadToBeModified(dependency) if dependency == 1
        ));
        // txn 4 reads estimate - depends on txn 1
        let err = mv_storage.get(4, &test_key).unwrap_err();
        assert!(matches!(
            err,
            VersionStoreError::ReadToBeModified(dependency) if dependency == 1
        ));
        // txn 6 reads 200
        let (val, ver) = mv_storage.get(6, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(200u64)));
        assert_eq!(ver, Some(MultiVersionStorage::version(5, 0)));

        // txn 1 re-executes and does not write the field
        let write_set = HashMap::new();
        mv_storage.record(1, 1, HashMap::new(), write_set);

        // txn 0 reads base value (100)
        let (val, ver) = mv_storage.get(0, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(100u64)));
        assert_eq!(ver, None);
        // txn 2 reads base value
        let (val, ver) = mv_storage.get(2, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(100u64)));
        assert_eq!(ver, None);
        // txn 4 reads 120
        let (val, ver) = mv_storage.get(4, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(120u64)));
        assert_eq!(ver, Some(MultiVersionStorage::version(3, 1)));
        // txn 6 reads 200
        let (val, ver) = mv_storage.get(6, &test_key).unwrap();
        assert_eq!(val, Some(Value::from(200u64)));
        assert_eq!(ver, Some(MultiVersionStorage::version(5, 0)));

        // assert that the highest nonce for txn 3 is 1
        let prev_nonce = mv_storage.convert_writes_to_estimates(3);
        assert_eq!(prev_nonce, 1);
    }
}
