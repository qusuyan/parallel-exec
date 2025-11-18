mod sequential;
pub use sequential::SequentialVM;

use crate::Block;

use async_trait::async_trait;

#[async_trait]
pub trait VM {
    async fn process(&self, block: Block);
}
