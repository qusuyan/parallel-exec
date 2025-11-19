mod sequential;
pub use sequential::SequentialVM;

mod block_stm;

mod seq_val;
pub use seq_val::SequentialValidationVM;

use crate::Block;

use async_trait::async_trait;

#[async_trait]
pub trait VM {
    async fn process(&self, block: Block);
}
