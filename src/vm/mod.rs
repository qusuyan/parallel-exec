/**
 * Virtual Machine (VM) module. It specifies how the VM processes a block of transactions.
 */
mod sequential;
use sequential::SequentialVM;

mod block_stm;
use block_stm::BlockSTMVM;

mod seq_val;
use seq_val::SequentialValidationVM;

mod seq_val_priority;
use seq_val_priority::SequentialValidationPriorityVM;

use crate::statedb::StateDBType;
use crate::storage::CacheStore;
use crate::txn::TxnError;
use crate::version_store::VersionStoreError;
use crate::{Block, Key, Storage, TxnID, Value};

use async_trait::async_trait;
use std::collections::HashMap;
use tokio::task::JoinError;

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum VMType {
    Sequential,
    SequentialValidation,
    SequentialValidationPriority,
    BlockSTM,
}

impl VMType {
    pub fn new(
        &self,
        statedb_type: StateDBType,
        cache_size: usize,
        storage: Box<dyn Storage>,
    ) -> Box<dyn VM> {
        let cache_store = CacheStore::new(cache_size, storage);
        match self {
            VMType::Sequential => Box::new(SequentialVM::new(statedb_type, cache_store)),
            VMType::SequentialValidation => {
                Box::new(SequentialValidationVM::new(statedb_type, cache_store))
            }
            VMType::SequentialValidationPriority => Box::new(SequentialValidationPriorityVM::new(
                statedb_type,
                cache_store,
            )),
            VMType::BlockSTM => Box::new(BlockSTMVM::new(statedb_type, cache_store)),
        }
    }
}

#[derive(Debug)]
pub enum VMError {
    TxnError { txn_id: TxnID, error: TxnError },
    StorageError(VersionStoreError),
    TokioRuntimeError(JoinError),
    CommitWhileProcessing,
}

impl From<JoinError> for VMError {
    fn from(err: JoinError) -> Self {
        VMError::TokioRuntimeError(err)
    }
}

impl From<VersionStoreError> for VMError {
    fn from(err: VersionStoreError) -> Self {
        VMError::StorageError(err)
    }
}

#[async_trait]
pub trait VM {
    // use mut to avoid currently processing multiple blocks
    async fn process(&mut self, block: Block) -> Result<(), VMError>;
    async fn commit(&mut self) -> Result<HashMap<Key, (TxnID, Value)>, VMError>;
}
