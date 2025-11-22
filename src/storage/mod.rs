/**
 * Versioned storage module for the entire block execution.
 * Since the exact value does not matter from concurrency control perspective, we only store version numbers.
 */
mod simple;
pub(crate) use simple::SimpleStorage;

mod multi_version;
pub(crate) use multi_version::MVStorage;

use crate::{Key, TxnID, Version, WriteOp};

use std::collections::HashMap;

#[derive(Debug)]
pub enum StorageError {
    ReadToBeModified,
}

pub trait Storage: Sync + Send {
    fn get(&self, tid: TxnID, key: &Key) -> Result<Version, StorageError>;
    fn validate(&self, tid: TxnID, read_set: &HashMap<Key, Version>) -> bool;
    fn commit(&self, tid: TxnID, write_set: HashMap<Key, WriteOp>);
}
