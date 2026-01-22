/**
 * Sequential Validation VM will execute transactions in parallel but validate them in order.
 * Since we only know if re-execution is needed after validation, we have to re-execute failed
 * transactions sequentially.
 */
use super::{VM, VMError};
use crate::statedb::{StateDBError, StateDBType};
use crate::storage::{SimpleStorage, Storage};
use crate::txn::TxnError;
use crate::{Block, Key, TxnID, WriteOp};

use std::collections::HashMap;
use std::sync::Arc;

pub struct SequentialValidationVM {
    statedb_type: StateDBType,
    storage: Arc<SimpleStorage>,
}

impl SequentialValidationVM {
    pub fn new(statedb_type: StateDBType) -> Self {
        Self {
            statedb_type,
            storage: Arc::new(SimpleStorage::new()),
        }
    }
}

#[async_trait::async_trait]
impl VM for SequentialValidationVM {
    async fn process(&mut self, block: Block) -> Result<(), VMError> {
        let mut reexec_count = 0;
        let mut resps = Vec::new();
        for txn in block.transactions.iter() {
            let storage_ref = self.storage.clone();
            let txn_ref = txn.clone();
            let statedb_type = self.statedb_type.clone();
            let handle =
                tokio::spawn(
                    async move { txn_ref.execute(storage_ref.as_ref(), statedb_type).await },
                );
            resps.push(handle);
        }

        // validate transactions in order
        for (i, resp) in resps.into_iter().enumerate() {
            let exec_result = resp.await.unwrap();
            let result = match exec_result {
                Ok(result) => {
                    if self.storage.validate(i, &result.read_set) {
                        Some(result.write_set)
                    } else {
                        None
                    }
                }
                Err(TxnError::StateDBError(StateDBError::ReadInconsistentVersions)) => None,
                Err(error) => {
                    return Err(VMError::TxnError { txn_id: i, error });
                }
            };

            // re-execute if validation failed
            let write_set_raw = match result {
                Some(ws) => ws,
                None => {
                    reexec_count += 1;
                    // since all previous transactions are validated and committed, we don't need to check read set again
                    let exec_result = block.transactions[i]
                        .reexecute(self.storage.as_ref(), self.statedb_type.clone())
                        .await
                        .map_err(|error| VMError::TxnError { txn_id: i, error })?;
                    exec_result.write_set
                }
            };

            // skip writes with zero increment
            let write_set = write_set_raw
                .into_iter()
                .filter(|(_, write_op)| !matches!(write_op, WriteOp::Inc(val, _) if val.is_zero()))
                .collect();
            self.storage.commit(i, write_set);
            log::debug!("Committed txn {:x}", i);
        }

        log::info!("Re-executed {} transactions", reexec_count);

        Ok(())
    }

    async fn commit(&mut self) -> Result<HashMap<Key, TxnID>, VMError> {
        let mut storage = Arc::new(SimpleStorage::new());
        std::mem::swap(&mut self.storage, &mut storage);
        let storage = Arc::try_unwrap(storage).map_err(|_| VMError::CommitWhileProcessing)?;
        Ok(storage.finalized()?)
    }
}
