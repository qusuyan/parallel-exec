use async_trait::async_trait;
use tokio::time::{Duration, Instant};

use super::{Transaction, TxnError};
use crate::statedb::{RevisionStrategy, StateDB, StateDBOp, ValidationSets};

pub struct MockTransaction {
    id: u64,
    exec_time: Duration,
    ops: Vec<StateDBOp>,
}

impl MockTransaction {
    pub fn new(id: u64, exec_time: Duration, ops: Vec<StateDBOp>) -> Self {
        Self { id, exec_time, ops }
    }
}

#[async_trait]
impl Transaction for MockTransaction {
    fn id(&self) -> u64 {
        self.id
    }

    async fn execute(
        &self,
        statedb: &StateDB,
        revision_strategy: RevisionStrategy,
    ) -> Result<ValidationSets, TxnError> {
        let start = Instant::now();
        let end = start + self.exec_time;

        let mut revision = revision_strategy.new(statedb);

        for op in &self.ops {
            match op {
                StateDBOp::Exist(address) => {
                    revision.exist(*address)?;
                }
                StateDBOp::Empty(address) => {
                    revision.empty(*address)?;
                }
                StateDBOp::GetBalance(address) => {
                    revision.get_balance(*address)?;
                }
                StateDBOp::GetNonce(address) => {
                    revision.get_nonce(*address)?;
                }
                StateDBOp::GetCode(address) => {
                    revision.get_code(*address)?;
                }
                StateDBOp::GetState(address, state_key) => {
                    revision.get_state(*address, *state_key)?;
                }
                StateDBOp::GetCommittedState(address, state_key) => {
                    revision.get_committed_state(*address, *state_key)?;
                }
                StateDBOp::HasSelfDestructed(address) => {
                    revision.has_self_destructed(*address)?;
                }
                StateDBOp::CheckEnoughBalance(address, amount) => {
                    revision.check_enough_balance(*address, *amount)?;
                }
                StateDBOp::AddBalance(address, amount) => {
                    revision.add_balance(*address, *amount)?;
                }
                StateDBOp::SubBalance(address, amount) => {
                    revision.sub_balance(*address, *amount)?;
                }
                StateDBOp::SetBalance(address, amount) => {
                    revision.set_balance(*address, *amount)?;
                }
                StateDBOp::SetNonce(address) => {
                    revision.set_nonce(*address)?;
                }
                StateDBOp::SetCode(address) => {
                    revision.set_code(*address)?;
                }
                StateDBOp::SetState(address, state_key) => {
                    revision.set_state(*address, *state_key)?;
                }
                StateDBOp::SelfDestruct(address) => {
                    revision.self_destruct(*address)?;
                }
                StateDBOp::CreateAccount(address) => {
                    revision.create_account(*address)?;
                }
            }
        }
        // mock execution time - do not deschedule the task
        while Instant::now() < end {}

        Ok(revision.into_validation_sets())
    }
}
