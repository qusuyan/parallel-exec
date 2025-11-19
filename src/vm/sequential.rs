use super::{VM, VMError};
use crate::statedb::{RevisionStrategy, StateDB};

pub struct SequentialVM {
    revision_strategy: RevisionStrategy,
    statedb: StateDB,
}

impl SequentialVM {
    pub fn new(revision_strategy: RevisionStrategy) -> Self {
        Self {
            revision_strategy,
            statedb: StateDB::new(),
        }
    }
}

#[async_trait::async_trait]
impl VM for SequentialVM {
    async fn process(&self, block: crate::Block) -> Result<(), VMError> {
        for (i, txn) in block.transactions.into_iter().enumerate() {
            let result = txn
                .execute(&self.statedb, self.revision_strategy.clone())
                .await
                .map_err(|error| -> VMError {
                    VMError::TxnError {
                        txn_id: i as u64,
                        error,
                    }
                })?;
            for (key, _) in result.write_set {
                self.statedb.insert(key, i as u64);
            }
        }
        Ok(())
    }
}
