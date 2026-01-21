/**
 * MockTransaction is a mock implementation of the Transaction trait for testing purposes.
 * We collect a series of StateDB operations to be performed during execution, and simulate
 * execution time by busy spin for a specified duration.
 */
use async_trait::async_trait;
use tokio::time::{Duration, Instant};

use super::{Transaction, TxnError};
use crate::TxnID;
use crate::statedb::{StateDBOp, StateDBOpType, StateDBType, ValidationSets};
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

    fn exec_time_est(&self) -> u128 {
        self.exec_time.as_nanos()
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

        for StateDBOp {
            account,
            op_type,
            start_time,
            ..
        } in &self.ops
        {
            let start_time = start + *start_time;
            // busy spin until start_time
            while Instant::now() < start_time {}
            match op_type {
                StateDBOpType::Exist => {
                    statedb.exist(*account)?;
                }
                StateDBOpType::Empty => {
                    statedb.empty(*account)?;
                }
                StateDBOpType::GetBalance => {
                    statedb.get_balance(*account)?;
                }
                StateDBOpType::GetNonce => {
                    statedb.get_nonce(*account)?;
                }
                StateDBOpType::GetCode => {
                    statedb.get_code(*account)?;
                }
                StateDBOpType::GetState(state_key) => {
                    statedb.get_state(*account, *state_key)?;
                }
                StateDBOpType::GetCommittedState(state_key) => {
                    statedb.get_committed_state(*account, *state_key)?;
                }
                StateDBOpType::HasSelfDestructed => {
                    statedb.has_self_destructed(*account)?;
                }
                StateDBOpType::CheckEnoughBalance(amount) => {
                    statedb.check_enough_balance(*account, *amount)?;
                }
                StateDBOpType::AddBalance(amount) => {
                    statedb.add_balance(*account, *amount)?;
                }
                StateDBOpType::SubBalance(amount) => {
                    statedb.sub_balance(*account, *amount)?;
                }
                StateDBOpType::SetBalance(amount) => {
                    statedb.set_balance(*account, *amount)?;
                }
                StateDBOpType::SetNonce => {
                    statedb.set_nonce(*account)?;
                }
                StateDBOpType::SetCode => {
                    statedb.set_code(*account)?;
                }
                StateDBOpType::SetState(state_key) => {
                    statedb.set_state(*account, *state_key)?;
                }
                StateDBOpType::SelfDestruct => {
                    statedb.self_destruct(*account)?;
                }
                StateDBOpType::CreateAccount => {
                    statedb.create_account(*account)?;
                }
            }
        }

        // busy spin for the rest of the execution time
        while Instant::now() < end {}

        Ok(statedb.into_validation_sets())
    }

    // During re-execution, we skip storage access time since it is brought into cache during initial execution.
    async fn reexecute(
        &self,
        storage: &dyn Storage,
        statedb_type: StateDBType,
    ) -> Result<ValidationSets, TxnError> {
        log::debug!(
            "{:?} Re-executing MockTransaction {}",
            std::thread::current().id(),
            self.id
        );
        let start = Instant::now();
        let mut cumulative_storage_time = Duration::from_secs(0);

        let mut statedb = statedb_type.new(self.id, storage);

        for StateDBOp {
            account,
            op_type,
            start_time,
            storage_time,
        } in &self.ops
        {
            let start_time = start + *start_time - cumulative_storage_time;
            // busy spin until start_time
            while Instant::now() < start_time {}
            match op_type {
                StateDBOpType::Exist => {
                    statedb.exist(*account)?;
                }
                StateDBOpType::Empty => {
                    statedb.empty(*account)?;
                }
                StateDBOpType::GetBalance => {
                    statedb.get_balance(*account)?;
                }
                StateDBOpType::GetNonce => {
                    statedb.get_nonce(*account)?;
                }
                StateDBOpType::GetCode => {
                    statedb.get_code(*account)?;
                }
                StateDBOpType::GetState(state_key) => {
                    statedb.get_state(*account, *state_key)?;
                }
                StateDBOpType::GetCommittedState(state_key) => {
                    statedb.get_committed_state(*account, *state_key)?;
                }
                StateDBOpType::HasSelfDestructed => {
                    statedb.has_self_destructed(*account)?;
                }
                StateDBOpType::CheckEnoughBalance(amount) => {
                    statedb.check_enough_balance(*account, *amount)?;
                }
                StateDBOpType::AddBalance(amount) => {
                    statedb.add_balance(*account, *amount)?;
                }
                StateDBOpType::SubBalance(amount) => {
                    statedb.sub_balance(*account, *amount)?;
                }
                StateDBOpType::SetBalance(amount) => {
                    statedb.set_balance(*account, *amount)?;
                }
                StateDBOpType::SetNonce => {
                    statedb.set_nonce(*account)?;
                }
                StateDBOpType::SetCode => {
                    statedb.set_code(*account)?;
                }
                StateDBOpType::SetState(state_key) => {
                    statedb.set_state(*account, *state_key)?;
                }
                StateDBOpType::SelfDestruct => {
                    statedb.self_destruct(*account)?;
                }
                StateDBOpType::CreateAccount => {
                    statedb.create_account(*account)?;
                }
            }
            cumulative_storage_time += *storage_time;
        }

        // busy spin for the rest of the execution time
        let end = start + self.exec_time - cumulative_storage_time;
        while Instant::now() < end {}

        Ok(statedb.into_validation_sets())
    }
}
