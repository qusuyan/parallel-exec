use super::VM;
use crate::{Block, StateDB};

use tokio::task::JoinHandle;

pub struct BlockSTMVMConfig {
    pub num_workers: usize,
}

pub struct BlockSTMVM {
    statedb: StateDB,
    handlers: Vec<JoinHandle<()>>,
}

impl BlockSTMVM {
    pub fn new(config: BlockSTMVMConfig) -> Self {
        let handlers = (0..config.num_workers)
            .map(|_| tokio::spawn(Self::handler()))
            .collect();

        Self {
            statedb: StateDB::new(),
            handlers,
        }
    }
}

impl Drop for BlockSTMVM {
    fn drop(&mut self) {
        for handler in self.handlers.drain(..) {
            handler.abort();
        }
    }
}

#[async_trait::async_trait]
impl VM for BlockSTMVM {
    async fn process(&self, block: Block) {
        // Implementation of BlockSTM processing logic goes here
        // This is a placeholder for the actual STM logic
        unimplemented!();
    }
}

impl BlockSTMVM {
    async fn handler() {}
}
