use super::{VM, VMError};
use crate::storage::MVStorage;
use crate::{Block, TxnID};

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64};

use dashmap::DashSet;
use tokio::runtime::Handle;

#[derive(Debug)]
enum TxnStatus {
    Ready,
    Executing,
    Executed,
    Aborting,
}

#[derive(Debug)]
struct Scheduler {
    execution_id: AtomicU64,
    validation_id: AtomicU64,
    decrease_count: AtomicU64,
    num_active_tasks: AtomicU64,
    done_marker: AtomicBool,
    txn_dependency: Box<[DashSet<TxnID>]>,
    txn_status: Box<[Mutex<(u32, TxnStatus)>]>,
}

impl Scheduler {
    fn new(num_txns: usize) -> Scheduler {
        Scheduler {
            execution_id: AtomicU64::new(0),
            validation_id: AtomicU64::new(0),
            decrease_count: AtomicU64::new(0),
            num_active_tasks: AtomicU64::new(0),
            done_marker: AtomicBool::new(false),
            txn_dependency: (0..num_txns)
                .map(|_| DashSet::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            txn_status: (0..num_txns)
                .map(|_| Mutex::new((0, TxnStatus::Ready)))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }
}

pub struct BlockSTMVM {
    storage: MVStorage,
}

impl BlockSTMVM {
    pub fn new() -> Self {
        Self {
            storage: MVStorage::new(),
        }
    }
}

#[async_trait::async_trait]
impl VM for BlockSTMVM {
    async fn process(&self, block: Block) -> Result<(), VMError> {
        // Spawn worker threads
        let num_threads = Handle::current().metrics().num_workers();
        let handlers = (0..num_threads)
            .map(|_| tokio::spawn(Self::handler()))
            .collect::<Vec<_>>();

        // Implementation of BlockSTM processing logic goes here
        // This is a placeholder for the actual STM logic
        unimplemented!();
    }
}

impl BlockSTMVM {
    async fn handler() {}
}
