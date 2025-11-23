/**
 * Virtual Machine (VM) module. It specifies how the VM processes a block of transactions.
 */
mod sequential;
pub use sequential::SequentialVM;

mod block_stm;

mod seq_val;
pub use seq_val::SequentialValidationVM;

use crate::Block;
use crate::statedb::StateDBType;
use crate::txn::TxnError;

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
    TokioRuntimeError(JoinError),
}

impl From<JoinError> for VMError {
    fn from(err: JoinError) -> Self {
        VMError::TokioRuntimeError(err)
    }
}

#[async_trait]
pub trait VM {
    async fn process(&self, block: Block) -> Result<(), VMError>;
}
