/**
 * MockTransaction is a mock implementation of the Transaction trait for testing purposes.
 * We collect a series of StateDB operations to be performed during execution, and simulate
 * execution time by busy spin for a specified duration.
 */
use async_trait::async_trait;
use tokio::time::{Duration, Instant};

use super::{Transaction, TxnError};
use crate::statedb::{StateDBError, StateDBOp, StateDBOpType, StateDBType, ValidationSets};
use crate::version_store::VersionedStore;
use crate::{TxnID, Value};

const COMP_RESOLUTION: Duration = Duration::from_nanos(150);
// const COMP_RESOLUTION: Duration = Duration::ZERO;

#[derive(Debug)]
pub struct MockTransaction {
    exec_time: Duration,
    ops: Vec<StateDBOp>,
}

impl MockTransaction {
    pub fn new(exec_time: Duration, ops: Vec<StateDBOp>) -> Self {
        Self { exec_time, ops }
    }
}

#[async_trait]
impl Transaction for MockTransaction {
    fn exec_time_est(&self) -> u128 {
        self.exec_time.as_nanos()
    }

    async fn execute(
        &self,
        id: TxnID,
        storage: &dyn VersionedStore,
        statedb_type: StateDBType,
        early_stop: bool, // whether to stop execution early if we encounter a ReadInconsistentVersions error
    ) -> Result<ValidationSets, TxnError> {
        log::debug!(
            "{:?} Executing MockTransaction {id:x}",
            std::thread::current().id(),
        );

        // let mut start: Instant = Instant::now();
        // let mut op_count = 0;

        let mut statedb = statedb_type.new(id, storage);

        // do not simulate storage access time here - the CachedStore will do it
        let mut prev_comp_start = Duration::ZERO;

        for StateDBOp {
            account,
            op_type,
            start_time,
            storage_time,
        } in &self.ops
        {
            // busy spin until previous op's compute time is reached
            let emu_start = Instant::now();
            let prev_compute = *start_time - prev_comp_start;
            let end = emu_start + prev_compute - COMP_RESOLUTION;
            prev_comp_start = *start_time + *storage_time;
            while Instant::now() < end {}

            // if id == 0 {
            //     op_count += 1;
            //     let elapsed = Instant::now() - start;
            //     let (err, err_sign) = if elapsed > *start_time {
            //         (elapsed - *start_time, "")
            //     } else {
            //         (*start_time - elapsed, "-")
            //     };
            //     println!(
            //         "{elapsed:?} - executing op {op_count}:{op_type:?}, expected start time is {start_time:?} (err: {err_sign}{err:?}, account: {account:x}, expected_storage_time: {storage_time:?})",
            //     );
            //     start = Instant::now() - elapsed;
            // }

            let op_err = match op_type {
                StateDBOpType::Exist => statedb.exist(*account).err(),
                StateDBOpType::Empty => statedb.empty(*account).err(),
                StateDBOpType::GetBalance => statedb.get_balance(*account).err(),
                StateDBOpType::GetNonce => statedb.get_nonce(*account).err(),
                StateDBOpType::GetCodeHash => statedb.get_code_hash(*account).err(),
                StateDBOpType::GetCode => statedb.get_code(*account).err(),
                StateDBOpType::GetState(state_key) => statedb.get_state(*account, *state_key).err(),
                StateDBOpType::GetCommittedState(state_key) => {
                    statedb.get_committed_state(*account, *state_key).err()
                }
                StateDBOpType::HasSelfDestructed => statedb.has_self_destructed(*account).err(),
                StateDBOpType::CheckEnoughBalance(amount) => {
                    statedb.check_enough_balance(*account, *amount).err()
                }
                StateDBOpType::AddBalance(amount) => statedb.add_balance(*account, *amount).err(),
                StateDBOpType::SubBalance(amount) => statedb.sub_balance(*account, *amount).err(),
                StateDBOpType::SetBalance(amount) => statedb.set_balance(*account, *amount).err(),
                StateDBOpType::SetNonce => statedb.set_nonce(*account, Value::zero()).err(),
                StateDBOpType::SetCode => statedb.set_code(*account, Value::zero()).err(),
                StateDBOpType::SetState(state_key) => statedb.set_state(*account, *state_key).err(),
                StateDBOpType::SelfDestruct => statedb.self_destruct(*account).err(),
                StateDBOpType::CreateAccount => statedb.create_account(*account).err(),
            };

            match op_err {
                None => {}
                Some(StateDBError::ReadInconsistentVersions) => {
                    if early_stop {
                        return Err(StateDBError::ReadInconsistentVersions.into());
                    }
                }
                Some(e) => {
                    return Err(e.into());
                }
            }
        }

        // busy spin for the rest of the execution time
        let end = Instant::now() - COMP_RESOLUTION
            + if self.exec_time > prev_comp_start {
                self.exec_time - prev_comp_start
            } else {
                Duration::ZERO
            };
        while Instant::now() < end {}

        // {
        //     let elapsed = Instant::now() - start;
        //     let (err, sign) = if elapsed > self.exec_time {
        //         (elapsed - self.exec_time, "")
        //     } else {
        //         (self.exec_time - elapsed, "-")
        //     };
        //     println!(
        //         "Txn {} with {} ops took {:?}, expect {:?} (err: {}{:?})",
        //         id,
        //         self.ops.len(),
        //         elapsed,
        //         self.exec_time,
        //         sign,
        //         err
        //     );
        // }
        Ok(statedb.into_validation_sets())
    }
}

#[cfg(test)]
mod test {

    use tokio::time::Instant;

    #[tokio::test]
    async fn test_time_resolution() {
        let loops = 100;
        let mut resolution = Vec::with_capacity(loops);
        let mut prev = Instant::now();
        for _ in 0..loops {
            let cur = Instant::now();
            resolution.push(cur.duration_since(prev).as_nanos());
            prev = cur;
        }

        // mostly < 150ns
        println!("{:?}", resolution);
    }
}
