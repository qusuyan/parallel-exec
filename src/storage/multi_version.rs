use super::Storage;
use crate::{Key, Version, WriteOp};

use std::collections::HashMap;

pub(crate) struct MVStorage {}

impl MVStorage {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl Storage for MVStorage {
    fn get(&self, version: Version, key: &Key) -> Version {
        // Placeholder implementation
        0
    }

    fn validate(&self, version: Version, read_set: &HashMap<Key, Version>) -> bool {
        // Placeholder implementation
        true
    }

    fn commit(&self, version: Version, write_set: HashMap<Key, WriteOp>) {
        // Placeholder implementation
    }
}
