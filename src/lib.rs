mod txn;
use std::sync::Arc;

pub use txn::{MockTransaction, Transaction, WriteOp};

mod statedb;
pub use statedb::{RevisionStrategy, StateDBOp};

mod vm;
pub use vm::{SequentialVM, SequentialValidationVM, VM, VMType};

use primitive_types::U256;

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub enum Field {
    Exist,
    Balance,
    Nonce,
    Code,
    State(U256),
}
pub type AccountAddress = U256;
pub type StateKey = U256;

impl PartialOrd for Field {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Field {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use Field::*;
        match (self, other) {
            (Exist, Exist) => std::cmp::Ordering::Equal,
            (Exist, _) => std::cmp::Ordering::Less,
            (_, Exist) => std::cmp::Ordering::Greater,
            (Balance, Balance) => std::cmp::Ordering::Equal,
            (Balance, _) => std::cmp::Ordering::Less,
            (_, Balance) => std::cmp::Ordering::Greater,
            (Nonce, Nonce) => std::cmp::Ordering::Equal,
            (Nonce, _) => std::cmp::Ordering::Less,
            (_, Nonce) => std::cmp::Ordering::Greater,
            (Code, Code) => std::cmp::Ordering::Equal,
            (Code, _) => std::cmp::Ordering::Less,
            (_, Code) => std::cmp::Ordering::Greater,
            (State(a), State(b)) => a.cmp(b),
        }
    }
}

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct Key {
    pub address: AccountAddress,
    pub field: Field,
}
pub type Value = U256;

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.address.cmp(&other.address) {
            std::cmp::Ordering::Equal => self.field.cmp(&other.field),
            ord => ord,
        }
    }
}

// Txn ID, or 0 if it is the committed state
pub type Version = u64;

pub struct Block {
    pub id: u64,
    pub transactions: Vec<Arc<dyn Transaction + Send + Sync>>,
}
