use super::VM;
use crate::{Block, StateDB};

use std::sync::Arc;

pub struct SequentialValidationVM {
    statedb: Arc<StateDB>,
}

impl SequentialValidationVM {
    pub fn new() -> Self {
        Self {
            statedb: Arc::new(StateDB::new()),
        }
    }
}

#[async_trait::async_trait]
impl VM for SequentialValidationVM {
    async fn process(&self, block: Block) {
        let mut resps = Vec::new();
        for txn in block.transactions.iter() {
            let statedb_ref = self.statedb.clone();
            let txn_ref = txn.clone();
            let handle = tokio::spawn(async move { txn_ref.execute(&statedb_ref).await });
            resps.push(handle);
        }

        // validate transactions in order
        for (i, resp) in resps.into_iter().enumerate() {
            let exec_result = resp.await.unwrap();
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
                // since all previous transactions are validated and committed, we don't need to check read set again
                block.transactions[i].execute(&self.statedb).await;
            }

            // commit write set
            for (key, _) in exec_result.write_set {
                self.statedb.insert(key, i as u64);
            }
        }
    }
}
