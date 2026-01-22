/**
 * Sequential VM will execute and validate transactions in a block one by one.
 */
use super::{VM, VMError};
use crate::statedb::StateDBType;
use crate::storage::{SimpleStorage, Storage};
use crate::{Block, Key, TxnID};

use std::collections::HashMap;

pub struct SequentialVM {
    statedb_type: StateDBType,
    storage: SimpleStorage,
}

impl SequentialVM {
    pub fn new(statedb_type: StateDBType) -> Self {
        Self {
            statedb_type,
            storage: SimpleStorage::new(),
        }
    }
}

#[async_trait::async_trait]
impl VM for SequentialVM {
    async fn process(&mut self, block: Block) -> Result<(), VMError> {
        for (i, txn) in block.transactions.into_iter().enumerate() {
            let result = txn
                .execute(&self.storage, self.statedb_type.clone())
                .await
                .map_err(|error| -> VMError { VMError::TxnError { txn_id: i, error } })?;
            self.storage.commit(i, result.write_set);
            log::debug!("Committed txn {:x}", i);
        }
        Ok(())
    }

    async fn commit(&mut self) -> Result<HashMap<Key, TxnID>, VMError> {
        let mut storage = SimpleStorage::new();
        std::mem::swap(&mut self.storage, &mut storage);
        Ok(storage.finalized()?)
    }
}
