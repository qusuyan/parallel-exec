use super::{VM, VMError};
use crate::statedb::{StateDBError, ValidationSets};
use crate::storage::{MVStorage, StorageError};
use crate::txn::TxnError;
use crate::{Block, StateDBType, Transaction, TxnID};

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::runtime::Handle;

#[derive(Debug)]
enum TxnStatus {
    Ready,
    Executing,
    Executed,
    Aborting,
}

enum Task {
    Execute { txn_id: TxnID, incarnate: u32 },
    Validate { txn_id: TxnID, incarnate: u32 },
}

struct TxnEntry {
    txn: Arc<dyn Transaction + Send + Sync + 'static>,
    status: Mutex<(u32, TxnStatus, HashSet<TxnID>)>,
}

impl Debug for TxnEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnData")
            .field("status", &self.status)
            .finish()
    }
}

#[derive(Debug)]
struct Scheduler {
    execution_idx: AtomicUsize,
    validation_idx: AtomicUsize,
    decrease_count: AtomicUsize, // TODO: use it to read consistent execution_idx and validation_idx during check_done
    num_active_tasks: AtomicUsize, // TODO: keep track of active tasks
    done_marking: AtomicBool,
    txn_info: Box<[TxnEntry]>,
}

impl Scheduler {
    fn new(txns: Vec<Arc<dyn Transaction + Send + Sync + 'static>>) -> Scheduler {
        Scheduler {
            execution_idx: AtomicUsize::new(0),
            validation_idx: AtomicUsize::new(txns.len()),
            decrease_count: AtomicUsize::new(0), // ensure atomic reading execution_idx and validation_idx
            num_active_tasks: AtomicUsize::new(0),
            done_marking: AtomicBool::new(false),
            txn_info: txns
                .into_iter()
                .map(|txn| TxnEntry {
                    txn,
                    status: Mutex::new((0, TxnStatus::Ready, HashSet::new())),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    #[inline]
    fn decrease_execution_idx(&self, target_idx: TxnID) {
        self.execution_idx.fetch_min(target_idx, Ordering::Relaxed);
        self.decrease_count.fetch_add(1, Ordering::Release); // prevent reordering of execution_idx update
    }

    #[inline]
    fn decrease_validation_idx(&self, target_idx: TxnID) {
        self.validation_idx.fetch_min(target_idx, Ordering::Relaxed);
        self.decrease_count.fetch_add(1, Ordering::Release);
    }

    fn check_done(&self) {
        // Use acquire to ensure we read everything in order specified:
        // decrease_count -> execution_idx/validation_idx -> num_active_tasks -> decrease_count again
        let observed_decrease_count = self.decrease_count.load(Ordering::Acquire);
        if self.execution_idx.load(Ordering::Acquire) >= self.txn_info.len()
            && self.validation_idx.load(Ordering::Acquire) >= self.txn_info.len()
            && self.num_active_tasks.load(Ordering::Acquire) == 0
            && observed_decrease_count == self.decrease_count.load(Ordering::Acquire)
        {
            self.done_marking.store(true, Ordering::Relaxed);
        }
    }

    // return a new incarnate number if the requested txn is ready to execute
    fn try_exec_incarnate(&self, txn_id: TxnID) -> Option<u32> {
        if let Some(entry) = self.txn_info.get(txn_id) {
            let mut guard = entry.status.lock().expect("poisoned lock");
            let (ref incarnate_count, ref mut status, _) = *guard;
            if matches!(*status, TxnStatus::Ready) {
                *status = TxnStatus::Executing;
                return Some(*incarnate_count);
            }
        }
        None
    }

    // return a new incarnate number if the requested txn is ready to be validated
    fn try_validate_incarnate(&self, txn_id: TxnID) -> Option<u32> {
        if let Some(entry) = self.txn_info.get(txn_id) {
            let guard = entry.status.lock().expect("poisoned lock");
            let (ref incarnate_count, ref status, _) = *guard;
            if matches!(*status, TxnStatus::Executed) {
                return Some(*incarnate_count);
            }
        }
        None
    }

    // try getting the next txn to execute
    // TODO: this can be optimized to a channel of tasks - instead of scanning through table
    async fn next_version_to_execute(&self) -> Option<(TxnID, u32)> {
        loop {
            if self.done_marking.load(Ordering::Relaxed) {
                return None;
            }
            if self.execution_idx.load(Ordering::Relaxed) >= self.txn_info.len() {
                // we have checked all txns, none is ready to execute
                self.check_done();
                tokio::task::yield_now().await; // if done, next time wakeup it will see the done_marking flag
                continue;
            }
            self.num_active_tasks.fetch_add(1, Ordering::Acquire); // ensure that next_idx fetch_add happens after this line
            let next_idx = self.execution_idx.fetch_add(1, Ordering::Relaxed);
            if let Some(inc_num) = self.try_exec_incarnate(next_idx) {
                // found a txn to execute
                return Some((next_idx, inc_num));
            }
            self.num_active_tasks.fetch_sub(1, Ordering::Release); // ensure that next_idx fetch_add happens before this line
        }
    }

    // try getting the next txn to validate. We scan through table here since when a txn finishes,
    // all subsequent txns need to be validated.
    async fn next_version_to_validate(&self) -> Option<(TxnID, u32)> {
        loop {
            if self.done_marking.load(Ordering::Relaxed) {
                return None;
            }
            if self.validation_idx.load(Ordering::Relaxed) >= self.txn_info.len() {
                // we have checked all txns, none is ready to validate
                self.check_done();
                tokio::task::yield_now().await;
                continue;
            }
            self.num_active_tasks.fetch_add(1, Ordering::Acquire);
            let next_idx = self.validation_idx.fetch_add(1, Ordering::Relaxed);
            if let Some(inc_num) = self.try_validate_incarnate(next_idx) {
                // found a txn to validate
                return Some((next_idx, inc_num));
            }
            self.num_active_tasks.fetch_sub(1, Ordering::Release);
        }
    }

    // add txn_id as a dependent of blocking_txn_id so that when blocking_txn_id finishes,
    // it can notify txn_id - return false if the blocking txn has finished execution
    fn add_dependency(&self, txn_id: TxnID, blocking_txn_id: TxnID) -> bool {
        assert!(blocking_txn_id < txn_id); // avoid deadlock
        // we need to hold the lock for blocking_txn for the entire function so that:
        // 1. either we see the blocking txn has already executed, or
        // 2. when the blocking txn issues re-execution for this txn, it is in ABORTING state
        let mut blocking_txn_guard = self.txn_info[blocking_txn_id]
            .status
            .lock()
            .expect("poisoned lock");
        let (_, ref blocking_txn_status, ref mut deps) = *blocking_txn_guard;
        if matches!(*blocking_txn_status, TxnStatus::Executed) {
            // the blocking txn has already executed
            return false;
        }
        // mark the current txn abort so that it will not be re-executed until blocking_txn_id finishes
        let mut guard = self.txn_info[blocking_txn_id]
            .status
            .lock()
            .expect("poisoned lock");
        let (_, ref mut status, _) = *guard;
        *status = TxnStatus::Aborting;
        // add txn_id as a dependent so that it will be re-executed when blocking_txn_id finishes
        deps.insert(txn_id);
        self.num_active_tasks.fetch_sub(1, Ordering::Release);
        true
    }

    // called when finish execution. It will mark the txn as executed, resume dependencies, and start validation tasks
    fn finish_execution(
        &self,
        txn_id: TxnID,
        incarnation: u32,
        did_not_write_new_keys: bool,
    ) -> Option<Task> {
        let mut guard = self.txn_info[txn_id].status.lock().expect("poisoned lock");
        let (ref inc, ref mut status, ref mut deps) = *guard;
        assert!(*inc == incarnation);
        *status = TxnStatus::Executed;
        // resume dependencies
        let dependencies = std::mem::take(deps);
        for dep_txn_id in dependencies.iter() {
            let mut dep_guard = self.txn_info[*dep_txn_id]
                .status
                .lock()
                .expect("poisoned lock");
            let (ref mut dep_incarnate_number, ref mut dep_status, _) = *dep_guard;
            *dep_incarnate_number += 1;
            *dep_status = TxnStatus::Ready;
        }
        let min_dep = dependencies.into_iter().min();
        if let Some(min_dep_txn_id) = min_dep {
            self.decrease_execution_idx(min_dep_txn_id);
        }
        // check which txns need to be validated
        if self.validation_idx.load(Ordering::Relaxed) > txn_id {
            if did_not_write_new_keys {
                // only re-validate this transaction
                return Some(Task::Validate {
                    txn_id,
                    incarnate: incarnation,
                });
            } else {
                // we need to re-validate subsequent transactions
                self.decrease_validation_idx(txn_id);
            }
        }
        self.num_active_tasks.fetch_sub(1, Ordering::Release);
        None
    }

    // mark the txn as aborting - return true if the incarnation number matches and the txn is in Executed state
    fn try_validation_abort(&self, txn_id: TxnID, incarnation: u32) -> bool {
        let mut guard = self.txn_info[txn_id].status.lock().expect("poisoned lock");
        let (ref inc, ref mut status, _) = *guard;
        if *inc == incarnation && matches!(*status, TxnStatus::Executed) {
            *status = TxnStatus::Aborting;
            return true;
        }
        false
    }

    fn finish_validation(&self, txn_id: TxnID, aborted: bool) -> Option<Task> {
        if aborted {
            // mark the txn ready for re-execution
            let mut guard = self.txn_info[txn_id].status.lock().expect("poisoned lock");
            let (ref mut inc, ref mut status, _) = *guard;
            *inc += 1;
            *status = TxnStatus::Ready;
            self.decrease_validation_idx(txn_id + 1); // TODO: why is this line needed?
            if self.execution_idx.load(Ordering::Relaxed) > txn_id {
                return self
                    .try_exec_incarnate(txn_id)
                    .map(|incarnate| Task::Execute { txn_id, incarnate });
            }
        }
        self.num_active_tasks.fetch_sub(1, Ordering::Release);
        None
    }
}

pub struct BlockSTMVM {
    storage: Arc<MVStorage>,
    statedb_type: StateDBType,
}

impl BlockSTMVM {
    pub fn new(statedb_type: StateDBType) -> Self {
        Self {
            storage: Arc::new(MVStorage::new()),
            statedb_type,
        }
    }
}

#[async_trait::async_trait]
impl VM for BlockSTMVM {
    async fn process(&self, block: Block) -> Result<(), VMError> {
        // Initialize scheduler
        let sched = Arc::new(Scheduler::new(block.transactions));
        // Spawn worker threads
        let num_threads = Handle::current().metrics().num_workers();
        let handlers = (0..num_threads)
            .map(|_| {
                tokio::spawn(Self::handler(
                    sched.clone(),
                    self.storage.clone(),
                    self.statedb_type,
                ))
            })
            .collect::<Vec<_>>();

        // Wait for all workers to finish
        for handle in handlers {
            handle.await??;
        }
        Ok(())
    }
}

impl BlockSTMVM {
    async fn handler(
        sched: Arc<Scheduler>,
        mv_memory: Arc<MVStorage>,
        statedb_type: StateDBType,
    ) -> Result<(), VMError> {
        let mut current_task = None;
        loop {
            if current_task.is_none() {
                current_task = tokio::select! {
                    Some((txn_id, incarnate)) = sched.next_version_to_execute() => Some(Task::Execute { txn_id, incarnate }),
                    Some((txn_id, incarnate)) = sched.next_version_to_validate() => Some(Task::Validate { txn_id, incarnate }),
                    else => {
                        // No more transactions to process
                        return Ok(());
                    }
                }
            }

            match current_task.take().unwrap() {
                Task::Execute { txn_id, incarnate } => {
                    match sched.txn_info[txn_id]
                        .txn
                        .execute(mv_memory.as_ref(), statedb_type.clone())
                        .await
                    {
                        Ok(ValidationSets {
                            read_set,
                            write_set,
                        }) => {
                            // publish read and write sets
                            let did_not_write_new_keys =
                                mv_memory.record(txn_id, incarnate, read_set, write_set);
                            // finish execution
                            current_task =
                                sched.finish_execution(txn_id, incarnate, did_not_write_new_keys);
                        }
                        Err(TxnError::StateDBError(StateDBError::StorageError(
                            StorageError::ReadToBeModified(prev_tid),
                        ))) => {
                            if !sched.add_dependency(txn_id, prev_tid) {
                                // prev_tid has already executed, rerun the transaction
                                current_task = Some(Task::Execute { txn_id, incarnate });
                            }
                        }
                    }
                }
                Task::Validate { txn_id, incarnate } => {
                    let valid = mv_memory.validate_read_set(txn_id);
                    let aborted = !valid && sched.try_validation_abort(txn_id, incarnate);
                    if aborted {
                        mv_memory.convert_writes_to_estimates(txn_id);
                    }
                    // finish validation
                    current_task = sched.finish_validation(txn_id, aborted);
                }
            }
        }
    }
}
