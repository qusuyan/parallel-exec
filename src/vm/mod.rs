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

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum VMType {
    Sequential,
    SequentialValidation,
}

impl VMType {
    pub fn new(&self, revision_strategy: StateDBType) -> Box<dyn VM> {
        match self {
            VMType::Sequential => Box::new(SequentialVM::new(revision_strategy)),
            VMType::SequentialValidation => {
                Box::new(SequentialValidationVM::new(revision_strategy))
            }
        }
    }
}

#[derive(Debug)]
pub enum VMError {
    TxnError { txn_id: u64, error: TxnError },
}

#[async_trait]
pub trait VM {
    async fn process(&self, block: Block) -> Result<(), VMError>;
}
