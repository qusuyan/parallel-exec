/**
 * A simple storage implementation based on DashMap.
 */
use super::{Storage, StorageError};
use crate::{Key, TxnID, Version, WriteOp};

use std::collections::HashMap;

use dashmap::DashMap;

pub(crate) struct SimpleStorage {
    store: DashMap<Key, TxnID>,
}

impl SimpleStorage {
    pub fn new() -> Self {
        Self {
            store: DashMap::new(),
        }
    }
}

impl Storage for SimpleStorage {
    fn get(&self, _: TxnID, key: &Key) -> Result<Version, StorageError> {
        Ok(self.store.get(key).map(|e| *e))
    }

    fn validate(&self, tid: TxnID, read_set: &HashMap<Key, Version>) -> bool {
        for (key, ver) in read_set {
            let current_ver = self.get(tid, key).unwrap();
            if current_ver != *ver {
                return false;
            }
        }
        true
    }

    fn commit(&self, tid: TxnID, write_set: HashMap<Key, WriteOp>) {
        for (key, _) in write_set {
            self.store.insert(key, tid);
        }
    }
}
