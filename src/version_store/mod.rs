/**
 * Versioned storage module for the entire block execution.
 * Since the exact value does not matter from concurrency control perspective, we only store version numbers.
 */
mod simple;
pub(crate) use simple::SingleVersionStorage;

mod multi_version;
pub(crate) use multi_version::MultiVersionStorage;

use crate::{AccountAddress, Key, StateKey, TxnID, Value, Version};

use std::collections::HashMap;

#[derive(Debug)]
pub enum VersionStoreError {
    ReadToBeModified(TxnID),
    IncrementNone,
}

pub trait VersionedStore: Sync + Send {
    fn get(&self, tid: TxnID, key: &Key) -> Result<(Option<Value>, Version), VersionStoreError>;
    fn get_committed_state(
        &self,
        account: &AccountAddress,
        state_key: &StateKey,
    ) -> Result<Option<Value>, VersionStoreError>;
    fn finalize(&mut self) -> Result<HashMap<Key, (TxnID, Value)>, VersionStoreError>;
}
