mod mock;
pub use mock::{MockTransaction, StateDBOp};

use async_trait::async_trait;
use std::collections::HashMap;

use crate::{Key, StateDB, Value, Version};

#[derive(Clone, Debug)]
pub enum WriteOp {
    Set(Value),
    Inc(Value, bool), // bool indicates increment or decrement
}

pub struct ExecResult {
    pub read_set: HashMap<Key, Version>,
    pub write_set: HashMap<Key, WriteOp>,
}

#[async_trait]
pub trait Transaction {
    fn id(&self) -> u64;
    async fn execute(&self, statedb: &StateDB) -> ExecResult;
}
