use super::VM;
use crate::{StateDB, Value, WriteOp};

pub struct SequentialVM {
    statedb: StateDB,
}

impl SequentialVM {
    pub fn new() -> Self {
        Self {
            statedb: StateDB::new(),
        }
    }
}

#[async_trait::async_trait]
impl VM for SequentialVM {
    async fn process(&self, block: crate::Block) {
        for txn in block.transactions {
            let result = txn.execute(&self.statedb).await;
            for (key, write_op) in result.write_set {
                match write_op {
                    WriteOp::Set(value) => {
                        self.statedb.insert(key, (value, txn.id()));
                    }
                    WriteOp::Inc(amount, is_increment) => {
                        let mut entry = self.statedb.entry(key).or_insert((Value::zero(), 0));
                        let new_value = if is_increment {
                            entry.0 + amount
                        } else {
                            entry.0.saturating_sub(amount) // assume sufficient balance
                        };
                        *entry = (new_value, txn.id());
                    }
                }
            }
        }
    }
}
