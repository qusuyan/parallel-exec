/**
 * Sequential Validation Priority VM is similar to Sequential Validation VM, but it prioritizes
 * the execution of transactions based on their estimated execution time.
 */
use super::{VM, VMError};
use crate::statedb::StateDBType;
use crate::storage::CacheStore;
use crate::vm::seq_val::SequentialValidationVM;
use crate::{Block, Key, TxnID, Value};

use std::cmp::Reverse;
use std::collections::{HashMap, VecDeque};

pub struct SequentialValidationPriorityVM {
    inner: SequentialValidationVM,
}

impl SequentialValidationPriorityVM {
    pub fn new(statedb_type: StateDBType, storage: CacheStore) -> Self {
        Self {
            inner: SequentialValidationVM::new(statedb_type, storage),
        }
    }
}

#[async_trait::async_trait]
impl VM for SequentialValidationPriorityVM {
    async fn process(&mut self, block: Block) -> Result<(), VMError> {
        // determine priority by execution time estimate
        let threshold1 = 8_000_000;
        let threshold2 = 1_000_000;

        let mut high_priority_queue = Vec::with_capacity(block.transactions.len());
        let mut medium_priority_queue = Vec::with_capacity(block.transactions.len());
        let mut low_priority_queue = Vec::with_capacity(block.transactions.len());

        for (i, txn) in block.transactions.into_iter().enumerate() {
            if txn.exec_time_est() >= threshold1 {
                // high priority
                high_priority_queue.push((i, txn));
            } else if txn.exec_time_est() >= threshold2 {
                // medium priority
                medium_priority_queue.push((i, txn));
            } else {
                // low priority
                low_priority_queue.push((i, txn));
            }
        }

        // sort high priority queue by execution time estimate in descending order
        high_priority_queue.sort_by_key(|(_, txn)| Reverse(txn.exec_time_est()));

        let exec_queue: VecDeque<_> = high_priority_queue
            .into_iter()
            .chain(medium_priority_queue.into_iter())
            .chain(low_priority_queue.into_iter())
            .collect();

        self.inner.process_with_exec_queue(exec_queue).await
    }

    async fn commit(&mut self) -> Result<HashMap<Key, (TxnID, Value)>, VMError> {
        self.inner.commit().await
    }
}
