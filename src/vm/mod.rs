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
use crate::txn::TxnError;
use crate::version_manager::VersionManagerError;
use crate::{Block, Key, TxnID};

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
    pub fn new(&self, statedb_type: StateDBType) -> Box<dyn VM> {
        match self {
            VMType::Sequential => Box::new(SequentialVM::new(statedb_type)),
            VMType::SequentialValidation => Box::new(SequentialValidationVM::new(statedb_type)),
            VMType::SequentialValidationPriority => {
                Box::new(SequentialValidationPriorityVM::new(statedb_type))
            }
            VMType::BlockSTM => Box::new(BlockSTMVM::new(statedb_type)),
        }
    }
}

#[derive(Debug)]
pub enum VMError {
    TxnError { txn_id: TxnID, error: TxnError },
    StorageError(VersionManagerError),
    TokioRuntimeError(JoinError),
    CommitWhileProcessing,
}

impl From<JoinError> for VMError {
    fn from(err: JoinError) -> Self {
        VMError::TokioRuntimeError(err)
    }
}

impl From<VersionManagerError> for VMError {
    fn from(err: VersionManagerError) -> Self {
        VMError::StorageError(err)
    }
}

#[async_trait]
pub trait VM {
    // use mut to avoid currently processing multiple blocks
    async fn process(&mut self, block: Block) -> Result<(), VMError>;
    async fn commit(&mut self) -> Result<HashMap<Key, TxnID>, VMError>;
}
