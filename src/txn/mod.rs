mod mock;
pub use mock::MockTransaction;

use crate::Value;
use crate::statedb::{RevisionStrategy, StateDB, StateDBError, ValidationSets};

use async_trait::async_trait;

#[derive(Clone, Debug)]
pub enum WriteOp {
    Set(Value),
    Inc(Value, bool), // bool indicates increment or decrement
}

#[derive(Debug)]
pub enum TxnError {
    StateDBError(StateDBError),
}

impl From<StateDBError> for TxnError {
    fn from(err: StateDBError) -> Self {
        TxnError::StateDBError(err)
    }
}

#[async_trait]
pub trait Transaction {
    fn id(&self) -> u64;
    async fn execute(
        &self,
        statedb: &StateDB,
        revision_strategy: RevisionStrategy,
    ) -> Result<ValidationSets, TxnError>;
}
