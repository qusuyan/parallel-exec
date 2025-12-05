/**
 * MockTransaction is a mock implementation of the Transaction trait for testing purposes.
 * We collect a series of StateDB operations to be performed during execution, and simulate
 * execution time by busy spin for a specified duration.
 */
use async_trait::async_trait;
use tokio::time::{Duration, Instant};

use super::{Transaction, TxnError};
use crate::TxnID;
use crate::statedb::{StateDBOp, StateDBType, ValidationSets};
use crate::storage::Storage;

pub struct MockTransaction {
    id: TxnID,
    exec_time: Duration,
    ops: Vec<StateDBOp>,
}

impl MockTransaction {
    pub fn new(id: TxnID, exec_time: Duration, ops: Vec<StateDBOp>) -> Self {
        Self { id, exec_time, ops }
    }
}

#[async_trait]
impl Transaction for MockTransaction {
    fn id(&self) -> TxnID {
        self.id
    }

    async fn execute(
        &self,
        storage: &dyn Storage,
        statedb_type: StateDBType,
    ) -> Result<ValidationSets, TxnError> {
        log::debug!(
            "{:?} Executing MockTransaction {}",
            std::thread::current().id(),
            self.id
        );
        let start = Instant::now();
        let end = start + self.exec_time;

        let mut statedb = statedb_type.new(self.id, storage);

        for op in &self.ops {
            match op {
                StateDBOp::Exist(address) => {
                    statedb.exist(*address)?;
                }
                StateDBOp::Empty(address) => {
                    statedb.empty(*address)?;
                }
                StateDBOp::GetBalance(address) => {
                    statedb.get_balance(*address)?;
                }
                StateDBOp::GetNonce(address) => {
                    statedb.get_nonce(*address)?;
                }
                StateDBOp::GetCode(address) => {
                    statedb.get_code(*address)?;
                }
                StateDBOp::GetState(address, state_key) => {
                    statedb.get_state(*address, *state_key)?;
                }
                StateDBOp::GetCommittedState(address, state_key) => {
                    statedb.get_committed_state(*address, *state_key)?;
                }
                StateDBOp::HasSelfDestructed(address) => {
                    statedb.has_self_destructed(*address)?;
                }
                StateDBOp::CheckEnoughBalance(address, amount) => {
                    statedb.check_enough_balance(*address, *amount)?;
                }
                StateDBOp::AddBalance(address, amount) => {
                    statedb.add_balance(*address, *amount)?;
                }
                StateDBOp::SubBalance(address, amount) => {
                    statedb.sub_balance(*address, *amount)?;
                }
                StateDBOp::SetBalance(address, amount) => {
                    statedb.set_balance(*address, *amount)?;
                }
                StateDBOp::SetNonce(address) => {
                    statedb.set_nonce(*address)?;
                }
                StateDBOp::SetCode(address) => {
                    statedb.set_code(*address)?;
                }
                StateDBOp::SetState(address, state_key) => {
                    statedb.set_state(*address, *state_key)?;
                }
                StateDBOp::SelfDestruct(address) => {
                    statedb.self_destruct(*address)?;
                }
                StateDBOp::CreateAccount(address) => {
                    statedb.create_account(*address)?;
                }
            }
        }
        // mock execution time - do not deschedule the task
        while Instant::now() < end {}

        Ok(statedb.into_validation_sets())
    }
}
