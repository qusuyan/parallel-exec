use std::collections::HashMap;

use crate::{Storage, StorageKey, Value};

pub struct DummyStore {
    data: HashMap<StorageKey, Value>,
}

impl DummyStore {
    pub fn new(data: HashMap<StorageKey, Value>) -> Self {
        Self { data }
    }
}

impl Storage for DummyStore {
    fn get(&self, key: &StorageKey) -> Option<Value> {
        self.data.get(key).cloned()
    }
}
