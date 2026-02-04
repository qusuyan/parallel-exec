use super::{VM, VMError};
use crate::statedb::{StateDBError, ValidationSets};
use crate::storage::CacheStore;
use crate::txn::TxnError;
use crate::version_store::{MultiVersionStorage, VersionStoreError, VersionedStore};
use crate::{Block, Key, StateDBType, Transaction, TxnID, Value};

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

    fn finish_validation_abort(&self, txn_id: TxnID, prev_incarnation: u32) -> Option<Task> {
        // mark the txn ready for re-execution
        let mut guard = loop {
            if let Ok(guard) = self.txn_info[txn_id].status.try_lock() {
                break guard;
            }
        };
        let (ref mut inc, ref mut status, _) = *guard;
        *inc = std::cmp::max(*inc, prev_incarnation) + 1;
        *status = TxnStatus::Ready;
        self.decrease_validation_idx(txn_id + 1); // TODO: why is this line needed?
        if self.execution_idx.load(Ordering::Relaxed) > txn_id {
            return self
                .try_exec_incarnate_unsafe(guard)
                .map(|incarnate| Task::Execute { txn_id, incarnate });
        }
        None
    }
}

pub struct BlockSTMVM {
    storage: Arc<MultiVersionStorage>,
    statedb_type: StateDBType,
}

impl BlockSTMVM {
    pub fn new(statedb_type: StateDBType, storage: CacheStore) -> Self {
        Self {
            storage: Arc::new(MultiVersionStorage::new(storage)),
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

    async fn commit(&mut self) -> Result<HashMap<Key, (TxnID, Value)>, VMError> {
        let storage = Arc::get_mut(&mut self.storage).ok_or(VMError::CommitWhileProcessing)?;
        let finalized_map = storage.finalize()?;
        Ok(finalized_map)
    }
}

impl BlockSTMVM {
    async fn handler(
        id: usize,
        sched: Arc<Scheduler>,
        mv_memory: Arc<MultiVersionStorage>,
        statedb_type: StateDBType,
    ) -> Result<(), VMError> {
        let mut next_task = None;
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
                    let txn = &sched.txn_info[txn_id].txn;
                    let result = txn
                        .execute(txn_id, mv_memory.as_ref(), statedb_type.clone())
                        .await;
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
                            VersionStoreError::ReadToBeModified(prev_tid),
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
                        Err(TxnError::StateDBError(StateDBError::StorageError(
                            VersionStoreError::IncrementNone,
                        ))) => {
                            unreachable!()
                        }
                        Err(_) => {
                            // txn is not recoverable - get the read set for valiation
                            todo!()
                        }
                    }
                }
                Task::Validate { txn_id, incarnate } => {
                    let valid = mv_memory.validate_read_set(txn_id);
                    let aborted = !valid && sched.try_validation_abort(txn_id, incarnate);
                    next_task = if aborted {
                        let prev_incarnation = mv_memory.convert_writes_to_estimates(txn_id);
                        sched.finish_validation_abort(txn_id, prev_incarnation)
                    } else {
                        None
                    }
                }
            }

            if next_task.is_none() {
                // done with current task, decrement the number of active tasks
                sched.num_active_tasks.fetch_sub(1, Ordering::Release);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use primitive_types::U256;
    use std::time::Duration;

    use super::*;
    use crate::storage::{CacheStore, DummyStore};
    use crate::{
        AccountAddress, Block, Field, MockTransaction, StateDBOp, StateDBOpType, StateKey,
        StorageKey, StorageKeyType,
    };

    #[test]
    fn test_parallel_exec() {
        let txn0 = Arc::new(MockTransaction::new(Duration::from_secs(1), vec![]));
        let txn1 = Arc::new(MockTransaction::new(Duration::from_secs(1), vec![]));
        let txn2 = Arc::new(MockTransaction::new(Duration::from_secs(1), vec![]));
        let txn3 = Arc::new(MockTransaction::new(Duration::from_secs(1), vec![]));
        let txn4 = Arc::new(MockTransaction::new(Duration::from_secs(1), vec![]));
        let storage = Box::new(DummyStore::new(HashMap::new()));

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let block1 = Block {
            id: 0,
            transactions: vec![txn0.clone(), txn1.clone(), txn2.clone(), txn3.clone()],
        };
        rt.block_on(async {
            let mut vm =
                BlockSTMVM::new(StateDBType::Basic, CacheStore::new(0x1000, storage.clone()));
            let start = std::time::Instant::now();
            vm.process(block1).await.unwrap();
            let duration = start.elapsed();
            println!("Total execution time: {:?}", duration);
            assert!(duration < Duration::from_secs_f64(1.5));
        });

        let block2 = Block {
            id: 1,
            transactions: vec![
                txn0.clone(),
                txn1.clone(),
                txn2.clone(),
                txn3.clone(),
                txn4.clone(),
            ],
        };
        rt.block_on(async {
            let mut vm =
                BlockSTMVM::new(StateDBType::Basic, CacheStore::new(0x1000, storage.clone()));
            let start = std::time::Instant::now();
            vm.process(block2).await.unwrap();
            let duration = start.elapsed();
            println!("Total execution time: {:?}", duration);
            assert!(duration > Duration::from_secs_f64(1.5));
            assert!(duration < Duration::from_secs_f64(2.5));
        });
    }

    #[test]
    fn test_dependency_reexecute() {
        let test_account = AccountAddress::from(13579u64);
        let test_state_key = StateKey::from(24680u64);
        let txn0 = Arc::new(MockTransaction::new(
            Duration::from_secs(1),
            vec![StateDBOp {
                op_type: StateDBOpType::AddBalance(U256::from(340)),
                account: test_account,
                start_time: Duration::from_millis(200),
                storage_time: Duration::from_millis(0),
            }],
        ));
        let txn1 = Arc::new(MockTransaction::new(
            Duration::from_secs(1),
            vec![StateDBOp {
                op_type: StateDBOpType::GetBalance,
                account: test_account,
                start_time: Duration::from_millis(700),
                storage_time: Duration::from_millis(0),
            }],
        ));
        let txn2 = Arc::new(MockTransaction::new(
            Duration::from_secs(1),
            vec![StateDBOp {
                op_type: StateDBOpType::SetState(test_state_key),
                account: test_account,
                start_time: Duration::from_millis(100),
                storage_time: Duration::from_millis(0),
            }],
        ));
        let txn3 = Arc::new(MockTransaction::new(
            Duration::from_secs(1),
            vec![StateDBOp {
                op_type: StateDBOpType::GetState(test_state_key),
                account: test_account,
                start_time: Duration::from_millis(800),
                storage_time: Duration::from_millis(0),
            }],
        ));

        let account_key = StorageKey {
            account: test_account,
            key_type: StorageKeyType::Account,
        };
        let state_key = StorageKey {
            account: test_account,
            key_type: StorageKeyType::State(test_state_key),
        };
        let storage = Box::new(DummyStore::new(
            [
                (account_key, Value::from(120u64)),
                (state_key, Value::from(30u64)),
            ]
            .into_iter()
            .collect(),
        ));

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let block = Block {
            id: 0,
            transactions: vec![txn0.clone(), txn1.clone(), txn2.clone(), txn3.clone()],
        };
        rt.block_on(async {
            let mut vm =
                BlockSTMVM::new(StateDBType::Basic, CacheStore::new(0x1000, storage.clone()));
            let start = std::time::Instant::now();
            vm.process(block).await.unwrap();
            let duration = start.elapsed();
            println!("Total execution time: {:?}", duration);
            let blk_write_set = vm.commit().await.unwrap();
            assert_eq!(
                blk_write_set,
                [
                    (
                        Key {
                            address: test_account,
                            field: Field::Balance,
                        },
                        (0, Value::from(460u64)),
                    ),
                    (
                        Key {
                            address: test_account,
                            field: Field::State(test_state_key),
                        },
                        (2, Value::zero()),
                    ),
                ]
                .into_iter()
                .collect()
            );
            assert!(duration > Duration::from_secs_f64(2.0));
            assert!(duration < Duration::from_secs_f64(2.5));
        });
    }

    /*
     * This test looks like
     * Txn 0: |----------------------|
     * Txn 1: |---------------|      |----------------| <-- reexecute due to conflict with Txn 0
     * Txn 2: |------------------------------|        |-------------------------------| <-- reads an estimate mark and reexecutes after Txn 1
     */
    #[test]
    fn test_read_estimate() {
        let test_account = AccountAddress::from(13579u64);
        let txn0 = Arc::new(MockTransaction::new(
            Duration::from_secs(1),
            vec![StateDBOp {
                op_type: StateDBOpType::AddBalance(U256::from(340)),
                account: test_account,
                start_time: Duration::from_millis(200),
                storage_time: Duration::from_millis(0),
            }],
        ));
        let txn1 = Arc::new(MockTransaction::new(
            Duration::from_millis(600),
            vec![
                StateDBOp {
                    op_type: StateDBOpType::GetBalance,
                    account: test_account,
                    start_time: Duration::from_millis(400),
                    storage_time: Duration::from_millis(0),
                },
                StateDBOp {
                    op_type: StateDBOpType::SubBalance(U256::from(40)),
                    account: test_account,
                    start_time: Duration::from_millis(500),
                    storage_time: Duration::from_millis(0),
                },
            ],
        ));
        let txn2 = Arc::new(MockTransaction::new(
            Duration::from_millis(1500),
            vec![StateDBOp {
                op_type: StateDBOpType::GetBalance,
                account: test_account,
                start_time: Duration::from_millis(1200),
                storage_time: Duration::from_millis(0),
            }],
        ));

        let account_key = StorageKey {
            account: test_account,
            key_type: StorageKeyType::Account,
        };
        let storage = Box::new(DummyStore::new(
            [(account_key, Value::from(120u64))].into_iter().collect(),
        ));

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let block = Block {
            id: 0,
            transactions: vec![txn0.clone(), txn1.clone(), txn2.clone()],
        };
        rt.block_on(async {
            let mut vm =
                BlockSTMVM::new(StateDBType::Basic, CacheStore::new(0x1000, storage.clone()));
            let start = std::time::Instant::now();
            vm.process(block).await.unwrap();
            let duration = start.elapsed();
            println!("Total execution time: {:?}", duration);
            let blk_write_set = vm.commit().await.unwrap();
            assert_eq!(
                blk_write_set,
                [(
                    Key {
                        address: test_account,
                        field: Field::Balance,
                    },
                    (1, Value::from(420u64)),
                ),]
                .into_iter()
                .collect()
            );
            assert!(duration >= Duration::from_secs_f64(3.0));
            assert!(duration < Duration::from_secs_f64(3.2));
        });
    }
}
