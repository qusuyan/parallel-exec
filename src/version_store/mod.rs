/**
 * Versioned storage module for the entire block execution.
 * Since the exact value does not matter from concurrency control perspective, we only store version numbers.
 */
mod simple;
pub(crate) use simple::SingleVersionStorage;

mod multi_version;
pub(crate) use multi_version::MultiVersionStorage;

mod storage;

use crate::{Key, TxnID, Version};

use std::collections::HashMap;

#[derive(Debug)]
pub enum VersionStoreError {
    ReadToBeModified(TxnID),
}

pub trait VersionedStore: Sync + Send {
    fn get(&self, tid: TxnID, key: &Key) -> Result<Version, VersionStoreError>;
    fn finalize(&mut self) -> Result<HashMap<Key, TxnID>, VersionStoreError>;
}
