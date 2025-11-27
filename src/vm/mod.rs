/**
 * Virtual Machine (VM) module. It specifies how the VM processes a block of transactions.
 */
mod sequential;
use std::collections::HashMap;

pub use sequential::SequentialVM;

mod block_stm;

mod seq_val;
pub use seq_val::SequentialValidationVM;

use crate::statedb::StateDBType;
use crate::storage::StorageError;
use crate::txn::TxnError;
use crate::{Block, Key, VersionRaw};

use async_trait::async_trait;
use tokio::task::JoinError;

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum VMType {
    Sequential,
    SequentialValidation,
    BlockSTM,
}

impl VMType {
    pub fn new(&self, statedb_type: StateDBType) -> Box<dyn VM> {
        match self {
            VMType::Sequential => Box::new(SequentialVM::new(statedb_type)),
            VMType::SequentialValidation => Box::new(SequentialValidationVM::new(statedb_type)),
            VMType::BlockSTM => Box::new(block_stm::BlockSTMVM::new(statedb_type)),
        }
    }
}

#[derive(Debug)]
pub enum VMError {
    TxnError { txn_id: u64, error: TxnError },
    StorageError(StorageError),
    TokioRuntimeError(JoinError),
    CommitWhileProcessing,
}

impl From<JoinError> for VMError {
    fn from(err: JoinError) -> Self {
        VMError::TokioRuntimeError(err)
    }
}

impl From<StorageError> for VMError {
    fn from(err: StorageError) -> Self {
        VMError::StorageError(err)
    }
}

#[async_trait]
pub trait VM {
    // use mut to avoid currently processing multiple blocks
    async fn process(&mut self, block: Block) -> Result<(), VMError>;
    async fn commit(&mut self) -> Result<HashMap<Key, VersionRaw>, VMError>;
}
