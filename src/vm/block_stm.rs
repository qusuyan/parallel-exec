use super::{VM, VMError};
use crate::statedb::{StateDBError, ValidationSets};
use crate::storage::{MVStorage, Storage, StorageError};
use crate::txn::TxnError;
use crate::{Block, Key, StateDBType, Transaction, TxnID};

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::runtime::Handle;
use tokio::sync::{Mutex, MutexGuard};

#[derive(Debug)]
enum TxnStatus {
    Ready,
    Executing,
    Executed,
    Aborting,
}

#[derive(Debug)]
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
    decrease_count: AtomicUsize,
    num_active_tasks: AtomicUsize,
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
            let guard = loop {
                if let Ok(guard) = entry.status.try_lock() {
                    break guard;
                }
            };
            return self.try_exec_incarnate_unsafe(guard);
        }
        None
    }

    fn try_exec_incarnate_unsafe(
        &self,
        mut guard: MutexGuard<'_, (u32, TxnStatus, HashSet<TxnID>)>,
    ) -> Option<u32> {
        let (ref incarnate_count, ref mut status, _) = *guard;
        if matches!(*status, TxnStatus::Ready) {
            *status = TxnStatus::Executing;
            return Some(*incarnate_count);
        }
        None
    }

    // return a new incarnate number if the requested txn is ready to be validated
    fn try_validate_incarnate(&self, txn_id: TxnID) -> Option<u32> {
        if let Some(entry) = self.txn_info.get(txn_id) {
            let guard = loop {
                if let Ok(guard) = entry.status.try_lock() {
                    break guard;
                }
            };
            let (ref incarnate_count, ref status, _) = *guard;
            if matches!(*status, TxnStatus::Executed) {
                return Some(*incarnate_count);
            }
        }
        None
    }

    // try getting the next txn to execute
    // TODO: this can be optimized to a channel of tasks - instead of scanning through table
    fn next_version_to_execute(&self) -> (TxnID, Option<u32>) {
        let current_execution_idx = self.execution_idx.load(Ordering::Relaxed);
        if current_execution_idx >= self.txn_info.len() {
            // we have checked all txns, none is ready to execute
            self.check_done();
            return (current_execution_idx, None);
        }

        self.num_active_tasks.fetch_add(1, Ordering::Acquire); // ensure that next_idx fetch_add happens after this line
        let next_idx = self.execution_idx.fetch_add(1, Ordering::Relaxed);

        let inc_num = self.try_exec_incarnate(next_idx);
        if inc_num.is_some() {
            (next_idx, inc_num)
        } else {
            self.num_active_tasks.fetch_sub(1, Ordering::Release);
            (next_idx, None)
        }
    }

    // try getting the next txn to validate
    fn next_version_to_validate(&self) -> (TxnID, Option<u32>) {
        let current_validation_idx = self.validation_idx.load(Ordering::Relaxed);
        if current_validation_idx >= self.txn_info.len() {
            // we have checked all txns, none is ready to validate
            self.check_done();
            return (current_validation_idx, None);
        }

        self.num_active_tasks.fetch_add(1, Ordering::Acquire); // ensure that next_idx fetch_add happens after this line
        let next_idx = self.validation_idx.fetch_add(1, Ordering::Relaxed);
        let inc_num = self.try_validate_incarnate(next_idx);
        if inc_num.is_some() {
            (next_idx, inc_num)
        } else {
            self.num_active_tasks.fetch_sub(1, Ordering::Release);
            (next_idx, None)
        }
    }

    fn next_task(&self) -> Option<Task> {
        loop {
            // prioritize validation tasks
            let (val_txn_id, inc) = self.next_version_to_validate();
            if let Some(incarnate) = inc {
                return Some(Task::Validate {
                    txn_id: val_txn_id,
                    incarnate,
                });
            }
            let (exec_txn_id, inc) = self.next_version_to_execute();
            if let Some(incarnate) = inc {
                return Some(Task::Execute {
                    txn_id: exec_txn_id,
                    incarnate,
                });
            }
            // we are done
            if self.done_marking.load(Ordering::Relaxed) {
                return None;
            }
            // we are not done but no task is available right now - yield to allow other tasks to proceed
            // if val_txn_id >= self.txn_info.len() && exec_txn_id >= self.txn_info.len() {
            //     tokio::task::yield_now().await;
            // }
        }
    }

    // add txn_id as a dependent of blocking_txn_id so that when blocking_txn_id finishes,
    // it can notify txn_id - return false if the blocking txn has finished execution
    fn add_dependency(&self, txn_id: TxnID, blocking_txn_id: TxnID) -> bool {
        assert!(blocking_txn_id < txn_id); // avoid deadlock
        // we need to hold the lock for blocking_txn for the entire function so that:
        // 1. either we see the blocking txn has already executed, or
        // 2. when the blocking txn issues re-execution for this txn, it is in ABORTING state
        let mut blocking_txn_guard = loop {
            if let Ok(guard) = self.txn_info[blocking_txn_id].status.try_lock() {
                break guard;
            }
        };
        let (_, ref blocking_txn_status, ref mut deps) = *blocking_txn_guard;
        if matches!(*blocking_txn_status, TxnStatus::Executed) {
            // the blocking txn has already executed
            return false;
        }
        // mark the current txn abort so that it will not be re-executed until blocking_txn_id finishes
        let mut guard = loop {
            if let Ok(guard) = self.txn_info[txn_id].status.try_lock() {
                break guard;
            }
        };
        let (_, ref mut status, _) = *guard;
        *status = TxnStatus::Aborting;
        // add txn_id as a dependent so that it will be re-executed when blocking_txn_id finishes
        deps.insert(txn_id);
        true
    }

    // called when finish execution. It will mark the txn as executed, resume dependencies, and start validation tasks
    fn finish_execution(
        &self,
        txn_id: TxnID,
        incarnation: u32,
        did_not_write_new_keys: bool,
    ) -> Option<Task> {
        let mut guard = loop {
            if let Ok(guard) = self.txn_info[txn_id].status.try_lock() {
                break guard;
            }
        };
        let (ref inc, ref mut status, ref mut deps) = *guard;
        assert!(*inc == incarnation);
        *status = TxnStatus::Executed;
        // resume dependencies
        let dependencies = std::mem::take(deps);
        for dep_txn_id in dependencies.iter() {
            let mut dep_guard = loop {
                if let Ok(guard) = self.txn_info[*dep_txn_id].status.try_lock() {
                    break guard;
                }
            };
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
        None
    }

    // mark the txn as aborting - return true if the incarnation number matches and the txn is in Executed state
    fn try_validation_abort(&self, txn_id: TxnID, incarnation: u32) -> bool {
        let mut guard = loop {
            if let Ok(guard) = self.txn_info[txn_id].status.try_lock() {
                break guard;
            }
        };
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
            let mut guard = loop {
                if let Ok(guard) = self.txn_info[txn_id].status.try_lock() {
                    break guard;
                }
            };
            let (ref mut inc, ref mut status, _) = *guard;
            *inc += 1;
            *status = TxnStatus::Ready;
            self.decrease_validation_idx(txn_id + 1); // TODO: why is this line needed?
            if self.execution_idx.load(Ordering::Relaxed) > txn_id {
                return self
                    .try_exec_incarnate_unsafe(guard)
                    .map(|incarnate| Task::Execute { txn_id, incarnate });
            }
        }
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
    async fn process(&mut self, block: Block) -> Result<(), VMError> {
        // Initialize scheduler
        let sched = Arc::new(Scheduler::new(block.transactions));
        // Spawn worker threads
        let metrics = Handle::current().metrics();
        let num_threads = metrics.num_workers();
        log::debug!("Spawning {num_threads} worker threads");
        let handlers = (0..num_threads)
            .map(|id| {
                tokio::spawn(Self::handler(
                    id,
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

    async fn commit(&mut self) -> Result<HashMap<Key, TxnID>, VMError> {
        let mut storage = Arc::new(MVStorage::new());
        std::mem::swap(&mut self.storage, &mut storage);
        let storage = Arc::try_unwrap(storage).map_err(|_| VMError::CommitWhileProcessing)?;
        Ok(storage.finalized()?)
    }
}

impl BlockSTMVM {
    async fn handler(
        id: usize,
        sched: Arc<Scheduler>,
        mv_memory: Arc<MVStorage>,
        statedb_type: StateDBType,
    ) -> Result<(), VMError> {
        let mut next_task = None;
        let mut exe_time = Duration::ZERO;
        let mut val_time = Duration::ZERO;
        log::debug!("handler {id}: started");
        loop {
            let current_task = match next_task.take() {
                Some(task) => task,
                None => match sched.next_task() {
                    Some(task) => task,
                    None => {
                        // we are done
                        break;
                    }
                },
            };

            // log::debug!("handler {}: processing {:?}", id, current_task);

            match current_task {
                Task::Execute { txn_id, incarnate } => {
                    log::debug!("handler {id}: executing Txn {txn_id:x} (incarnate {incarnate})");
                    let start = Instant::now();
                    let txn = &sched.txn_info[txn_id].txn;
                    let result = if incarnate == 0 {
                        txn.execute(mv_memory.as_ref(), statedb_type.clone()).await
                    } else {
                        txn.reexecute(mv_memory.as_ref(), statedb_type.clone())
                            .await
                    };
                    match result {
                        Ok(ValidationSets {
                            read_set,
                            write_set,
                        }) => {
                            // publish read and write sets
                            let did_not_write_new_keys =
                                mv_memory.record(txn_id, incarnate, read_set, write_set);
                            // finish execution
                            next_task =
                                sched.finish_execution(txn_id, incarnate, did_not_write_new_keys);
                        }
                        Err(TxnError::StateDBError(StateDBError::StorageError(
                            StorageError::ReadToBeModified(prev_tid),
                        ))) => {
                            if !sched.add_dependency(txn_id, prev_tid) {
                                // prev_tid has already executed, rerun the transaction
                                next_task = Some(Task::Execute { txn_id, incarnate });
                            }
                        }
                        Err(TxnError::StateDBError(StateDBError::ReadInconsistentVersions)) => {
                            // we read a value that is later written by a previous txn - abort and retry
                            next_task = Some(Task::Execute { txn_id, incarnate });
                        }
                    }
                    exe_time += Instant::now() - start;
                }
                Task::Validate { txn_id, incarnate } => {
                    log::debug!("handler {id}: validating Txn {txn_id:x} (incarnate {incarnate})");
                    let start = Instant::now();
                    let valid = mv_memory.validate_read_set(txn_id);
                    let aborted = !valid && sched.try_validation_abort(txn_id, incarnate);
                    if aborted {
                        mv_memory.convert_writes_to_estimates(txn_id);
                    }
                    // finish validation
                    next_task = sched.finish_validation(txn_id, aborted);
                    val_time += Instant::now() - start;
                }
            }

            if next_task.is_none() {
                // done with current task, decrement the number of active tasks
                sched.num_active_tasks.fetch_sub(1, Ordering::Release);
            }
        }

        log::debug!(
            "handler {id}: execution time: {:},  validation time: {:}",
            exe_time.as_secs_f64() * 1000.0,
            val_time.as_secs_f64() * 1000.0
        );

        Ok(())
    }
}
