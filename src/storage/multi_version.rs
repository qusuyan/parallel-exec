use super::{Storage, StorageError};
use crate::{Key, TxnID, Version, WriteOp};

use std::collections::HashMap;

pub(crate) struct MVStorage {}

impl MVStorage {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl Storage for MVStorage {
    fn get(&self, tid: TxnID, key: &Key) -> Result<Version, StorageError> {
        // Placeholder implementation
        todo!()
    }

    fn validate(&self, tid: TxnID, read_set: &HashMap<Key, Version>) -> bool {
        // Placeholder implementation
        todo!()
    }

    fn commit(&self, _tid: TxnID, _write_set: HashMap<Key, WriteOp>) {
        unreachable!(
            "MVStorage should call commit - it will speculatively publish write set and revert if validation fails"
        );
    }
}
