use super::VM;
use crate::StateDB;

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
        for (i, txn) in block.transactions.into_iter().enumerate() {
            let result = txn.execute(&self.statedb).await;
            for (key, _) in result.write_set {
                self.statedb.insert(key, i as u64);
            }
        }
    }
}
