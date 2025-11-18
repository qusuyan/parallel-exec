mod txn;
pub use txn::{MockTransaction, StateDBOp, Transaction, WriteOp};

mod statedb;
use statedb::StateDB;

mod vm;
pub use vm::{SequentialVM, VM};

use primitive_types::U256;

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub enum Field {
    Exist,
    Balance,
    EnoughBalance(U256),
    Nonce,
    Code,
    State(U256),
}
pub type AccountAddress = U256;
pub type StateKey = U256;

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct Key {
    pub address: AccountAddress,
    pub field: Field,
}
pub type Value = U256;

// Txn ID, or 0 if it is the committed state
pub type Version = u64;

pub struct Block {
    pub id: u64,
    pub transactions: Vec<Box<dyn Transaction + Send + Sync>>,
}
