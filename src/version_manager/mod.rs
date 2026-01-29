/**
 * Versioned storage module for the entire block execution.
 * Since the exact value does not matter from concurrency control perspective, we only store version numbers.
 */
mod simple;
pub(crate) use simple::SingleVersionManager;

mod multi_version;
pub(crate) use multi_version::MultiVersionManager;

use crate::{Key, TxnID, Version};

use std::collections::HashMap;

#[derive(Debug)]
pub enum VersionManagerError {
    ReadToBeModified(TxnID),
}

pub trait VersionManager: Sync + Send {
    fn get(&self, tid: TxnID, key: &Key) -> Result<Version, VersionManagerError>;
    fn finalized(self) -> Result<HashMap<Key, TxnID>, VersionManagerError>;
}
