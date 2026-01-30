/**
 * Sequential Validation VM will execute transactions in parallel but validate them in order.
 * Since we only know if re-execution is needed after validation, we have to re-execute failed
 * transactions sequentially.
 */
use super::{VM, VMError};
use crate::statedb::{StateDBError, StateDBType, ValidationSets};
use crate::txn::TxnError;
use crate::version_store::{SingleVersionStorage, VersionedStore};
use crate::{Block, Key, Transaction, TxnID, WriteOp};

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::runtime::Handle;
use tokio::sync::Mutex;

pub(super) enum Task {
    Exec {
        txn_id: TxnID,
        txn: Arc<dyn Transaction + Send + Sync>,
    },
    Validate {
        txn_id: TxnID,
        txn: Arc<dyn Transaction + Send + Sync>,
        result: Result<ValidationSets, TxnError>,
    },
}

pub(super) struct TaskQueue {
    exec_queue: Mutex<VecDeque<(TxnID, Arc<dyn Transaction + Send + Sync>)>>, // min-heap
    pending_validations: Mutex<
        HashMap<
            TxnID,
            (
                Arc<dyn Transaction + Send + Sync>,
                Result<ValidationSets, TxnError>,
            ),
        >,
    >,
    next_to_validate: AtomicUsize,
}

impl TaskQueue {
    pub fn new(exec_queue: VecDeque<(TxnID, Arc<dyn Transaction + Send + Sync>)>) -> Self {
        Self {
            exec_queue: Mutex::new(exec_queue),
            pending_validations: Mutex::new(HashMap::new()),
            next_to_validate: AtomicUsize::new(0),
        }
    }

    pub fn add_validation_task(
        &self,
        txn_id: TxnID,
        txn: Arc<dyn Transaction + Send + Sync>,
        result: Result<ValidationSets, TxnError>,
    ) {
        let mut pending_validations = loop {
            if let Ok(guard) = self.pending_validations.try_lock() {
                break guard;
            }
        };
        pending_validations.insert(txn_id, (txn, result));
    }

    pub fn advance_validation(&self) {
        self.next_to_validate.fetch_add(1, Ordering::Release);
    }

    pub fn pop(&self) -> Option<Task> {
        let next_to_validate = self.next_to_validate.load(Ordering::Acquire);
        let validate_task = {
            let mut pending_validations = loop {
                if let Ok(guard) = self.pending_validations.try_lock() {
                    break guard;
                }
            };
            pending_validations.remove(&next_to_validate)
        };
        if let Some((txn, result)) = validate_task {
            Some(Task::Validate {
                txn_id: next_to_validate,
                txn,
                result,
            })
        } else {
            let mut exec_queue = loop {
                if let Ok(guard) = self.exec_queue.try_lock() {
                    break guard;
                }
            };
            exec_queue
                .pop_front()
                .map(|(txn_id, txn)| Task::Exec { txn_id, txn })
        }
    }
}

pub struct SequentialValidationVM {
    statedb_type: StateDBType,
    storage: Arc<SingleVersionStorage>,
}

impl SequentialValidationVM {
    pub fn new(statedb_type: StateDBType, cache_size: usize) -> Self {
        Self {
            statedb_type,
            storage: Arc::new(SingleVersionStorage::new(cache_size)),
        }
    }

    pub async fn process_with_exec_queue(
        &mut self,
        exec_queue: VecDeque<(TxnID, Arc<dyn Transaction + Send + Sync>)>,
    ) -> Result<(), VMError> {
        let metrics = Handle::current().metrics();
        let num_threads = metrics.num_workers();
        let task_queue = Arc::new(TaskQueue::new(exec_queue));
        let reexec_count = Arc::new(AtomicUsize::new(0));

        log::debug!("Spawning {num_threads} worker threads");
        let handler: Vec<_> = (0..num_threads)
            .map(|id| {
                tokio::spawn(Self::handler(
                    id,
                    task_queue.clone(),
                    self.storage.clone(),
                    self.statedb_type.clone(),
                    reexec_count.clone(),
                ))
            })
            .collect();

        for handle in handler.into_iter() {
            handle.await??;
        }

        log::info!(
            "SequentialValidationVM: total re-executions = {}",
            reexec_count.load(std::sync::atomic::Ordering::Relaxed),
        );

        Ok(())
    }
}

#[async_trait::async_trait]
impl VM for SequentialValidationVM {
    async fn process(&mut self, block: Block) -> Result<(), VMError> {
        let exec_queue = block.transactions.into_iter().enumerate().collect();
        self.process_with_exec_queue(exec_queue).await
    }

    async fn commit(&mut self) -> Result<HashMap<Key, TxnID>, VMError> {
        let storage = Arc::get_mut(&mut self.storage).ok_or(VMError::CommitWhileProcessing)?;
        let finalized_map = storage.finalize()?;
        Ok(finalized_map)
    }
}

impl SequentialValidationVM {
    async fn handler(
        id: usize,
        task_queue: Arc<TaskQueue>,
        storage: Arc<SingleVersionStorage>,
        statedb_type: StateDBType,
        reexec_count: Arc<AtomicUsize>,
    ) -> Result<(), VMError> {
        log::debug!("handler {id}: started");
        loop {
            let Some(task) = task_queue.pop() else {
                log::debug!("handler {id}: no more tasks, exiting");
                break;
            };

            match task {
                Task::Exec { txn_id, txn } => {
                    log::debug!("handler {id}: executing txn {:x}", txn_id);
                    let exec_result = txn.execute(storage.as_ref(), statedb_type.clone()).await;
                    task_queue.add_validation_task(txn_id, txn, exec_result);
                }
                Task::Validate {
                    txn_id,
                    txn,
                    result: exec_result,
                } => {
                    log::debug!("handler {id}: validating txn {:x}", txn_id);
                    let result = match exec_result {
                        Ok(result) => {
                            if storage.validate(txn_id, &result.read_set) {
                                Some(result.write_set)
                            } else {
                                None
                            }
                        }
                        Err(TxnError::StateDBError(StateDBError::ReadInconsistentVersions)) => None,
                        Err(error) => {
                            return Err(VMError::TxnError { txn_id, error });
                        }
                    };

                    // re-execute if validation failed
                    let write_set_raw = match result {
                        Some(ws) => ws,
                        None => {
                            reexec_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let exec_result = txn
                                .execute(storage.as_ref(), statedb_type.clone())
                                .await
                                .map_err(|error| VMError::TxnError { txn_id, error })?;
                            exec_result.write_set
                        }
                    };

                    // skip writes with zero increment
                    let write_set = write_set_raw
                        .into_iter()
                        .filter(|(_, write_op)| !matches!(write_op, WriteOp::Inc(val, _) if val.is_zero()))
                        .collect();

                    // commit the write set
                    storage.commit(txn_id, write_set);
                    log::debug!("handler {id}: committed txn {:x}", txn_id);

                    // advance the next_to_validate pointer
                    task_queue.advance_validation();
                }
            }
        }

        Ok(())
    }
}
