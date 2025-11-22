/**
 * Sequential VM will execute and validate transactions in a block one by one.
 */
use super::{VM, VMError};
use crate::statedb::StateDBType;
use crate::storage::{SimpleStorage, Storage};

pub struct SequentialVM {
    revision_strategy: StateDBType,
    storage: SimpleStorage,
}

impl SequentialVM {
    pub fn new(revision_strategy: StateDBType) -> Self {
        Self {
            revision_strategy,
            storage: SimpleStorage::new(),
        }
    }
}

#[async_trait::async_trait]
impl VM for SequentialVM {
    async fn process(&self, block: crate::Block) -> Result<(), VMError> {
        for (i, txn) in block.transactions.into_iter().enumerate() {
            let result = txn
                .execute(&self.storage, self.revision_strategy.clone())
                .await
                .map_err(|error| -> VMError {
                    VMError::TxnError {
                        txn_id: i as u64,
                        error,
                    }
                })?;
            self.storage.commit(i, result.write_set);
            log::debug!("Committed txn {}", i);
        }
        Ok(())
    }
}
