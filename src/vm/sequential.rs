/**
 * Sequential VM will execute and validate transactions in a block one by one.
 */
use super::{VM, VMError};
use crate::Block;
use crate::statedb::StateDBType;
use crate::storage::SimpleStorage;

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
    async fn process(&self, block: Block) -> Result<(), VMError> {
        for (i, txn) in block.transactions.into_iter().enumerate() {
            let result = txn
                .execute(&self.storage, self.statedb_type.clone())
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
