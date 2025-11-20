use super::{VM, VMError};
use crate::statedb::{RevisionStrategy, StateDB};
use crate::{Block, WriteOp};

use std::sync::Arc;

pub struct SequentialValidationVM {
    revision_strategy: RevisionStrategy,
    statedb: Arc<StateDB>,
}

impl SequentialValidationVM {
    pub fn new(revision_strategy: RevisionStrategy) -> Self {
        Self {
            revision_strategy,
            statedb: Arc::new(StateDB::new()),
        }
    }
}

#[async_trait::async_trait]
impl VM for SequentialValidationVM {
    async fn process(&self, block: Block) -> Result<(), VMError> {
        let mut reexec_count = 0;
        let mut resps = Vec::new();
        for txn in block.transactions.iter() {
            let statedb_ref = self.statedb.clone();
            let txn_ref = txn.clone();
            let revision_strategy = self.revision_strategy.clone();
            let handle =
                tokio::spawn(async move { txn_ref.execute(&statedb_ref, revision_strategy).await });
            resps.push(handle);
        }

        // validate transactions in order
        for (i, resp) in resps.into_iter().enumerate() {
            let exec_result = resp.await.unwrap().map_err(|error| VMError::TxnError {
                txn_id: i as u64,
                error,
            })?;
            let mut success = true;
            for (key, version) in exec_result.read_set {
                let v = match self.statedb.get(&key) {
                    Some(e) => *e,
                    None => 0,
                };
                if v != version {
                    success = false;
                    break;
                }
            }

            // re-execute if validation failed
            if !success {
                reexec_count += 1;
                // since all previous transactions are validated and committed, we don't need to check read set again
                block.transactions[i]
                    .execute(&self.statedb, self.revision_strategy.clone())
                    .await
                    .map_err(|error| VMError::TxnError {
                        txn_id: i as u64,
                        error,
                    })?;
            }

            // commit write set
            for (key, op) in exec_result.write_set {
                if let WriteOp::Inc(val, _) = op {
                    if val.is_zero() {
                        continue;
                    }
                }
                self.statedb.insert(key, i as u64);
            }
        }

        println!("Re-executed {} transactions", reexec_count);

        Ok(())
    }
}
