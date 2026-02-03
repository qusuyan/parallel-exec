/**
 * Transaction abstraction. It takes the current storage state, creates a StateDB instance,
 * and run the transaction logic.
 */
mod mock;
use std::ops::Sub;

pub use mock::MockTransaction;

use crate::statedb::{StateDBError, StateDBType, ValidationSets};
use crate::version_store::VersionedStore;
use crate::{TxnID, Value};

use async_trait::async_trait;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteOp {
    Set(Value),
    Inc(Value, bool), // bool indicates increment or decrement
}

// Aggregating two write operations - using sub since it is not symmetric (and - looks like chaining write ops)
impl Sub for WriteOp {
    type Output = WriteOp;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (_, WriteOp::Set(v2)) => WriteOp::Set(v2),
            (WriteOp::Set(v), WriteOp::Inc(delta, true)) => WriteOp::Set(v.saturating_add(delta)),
            (WriteOp::Set(v), WriteOp::Inc(delta, false)) => WriteOp::Set(v.saturating_sub(delta)),
            (WriteOp::Inc(delta1, sign1), WriteOp::Inc(delta2, sign2)) => {
                if sign1 == sign2 {
                    WriteOp::Inc(delta1.saturating_add(delta2), sign1)
                } else {
                    if delta1 >= delta2 {
                        WriteOp::Inc(delta1 - delta2, sign1)
                    } else {
                        WriteOp::Inc(delta2 - delta1, sign2)
                    }
                }
            }
        }
    }
}

impl WriteOp {
    pub fn apply_to(&self, base: &Value) -> Value {
        match self {
            WriteOp::Set(v) => *v,
            WriteOp::Inc(delta, true) => base.saturating_add(*delta),
            WriteOp::Inc(delta, false) => base.saturating_sub(*delta),
        }
    }
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
        statedb: &dyn VersionedStore,
        statedb_type: StateDBType,
    ) -> Result<ValidationSets, TxnError>;
}

#[cfg(test)]
mod test {
    use primitive_types::U256;

    use crate::WriteOp;

    #[test]
    fn test_write_op_chaining() {
        assert!(
            WriteOp::Set(U256::from(100)) - WriteOp::Set(U256::from(200))
                == WriteOp::Set(U256::from(200))
        );
        assert!(
            WriteOp::Set(U256::from(100)) - WriteOp::Inc(U256::from(50), true)
                == WriteOp::Set(U256::from(150))
        );
        assert!(
            WriteOp::Set(U256::from(100)) - WriteOp::Inc(U256::from(50), false)
                == WriteOp::Set(U256::from(50))
        );
        assert!(
            WriteOp::Inc(U256::from(50), true) - WriteOp::Inc(U256::from(30), true)
                == WriteOp::Inc(U256::from(80), true)
        );
        assert!(
            WriteOp::Inc(U256::from(50), false) - WriteOp::Inc(U256::from(30), false)
                == WriteOp::Inc(U256::from(80), false)
        );
        assert!(
            WriteOp::Inc(U256::from(50), true) - WriteOp::Inc(U256::from(30), false)
                == WriteOp::Inc(U256::from(20), true)
        );
        assert!(
            WriteOp::Inc(U256::from(50), false) - WriteOp::Inc(U256::from(30), true)
                == WriteOp::Inc(U256::from(20), false)
        );
        assert!(
            WriteOp::Inc(U256::from(50), true) - WriteOp::Inc(U256::from(70), false)
                == WriteOp::Inc(U256::from(20), false)
        );
        assert!(
            WriteOp::Inc(U256::from(50), false) - WriteOp::Inc(U256::from(70), true)
                == WriteOp::Inc(U256::from(20), true)
        );
    }
}
