/**
 * Transaction abstraction. It takes the current storage state, creates a StateDB instance,
 * and run the transaction logic.
 */
mod mock;
pub use mock::MockTransaction;

use crate::statedb::{StateDBError, StateDBType, ValidationSets};
use crate::version_manager::VersionManager;
use crate::{TxnID, Value};

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
    fn id(&self) -> TxnID;
    fn exec_time_est(&self) -> u128; // estimate how long txn will take to execute - it can be gas fee or any other metric

    async fn execute(
        &self,
        statedb: &dyn VersionManager,
        statedb_type: StateDBType,
    ) -> Result<ValidationSets, TxnError>;

    async fn reexecute(
        &self,
        statedb: &dyn VersionManager,
        statedb_type: StateDBType,
    ) -> Result<ValidationSets, TxnError> {
        self.execute(statedb, statedb_type).await
    }
}
