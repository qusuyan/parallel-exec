/**
 * Versioned storage module for the entire block execution.
 * When a txn finishes execution, its write set is committed to Storage.
 */
mod simple;
pub(crate) use simple::SimpleStorage;

mod multi_version;
pub(crate) use multi_version::MVStorage;

use crate::{Key, TxnID, Version, WriteOp};

use std::collections::HashMap;

pub trait Storage: Sync + Send {
    fn get(&self, tid: TxnID, key: &Key) -> Version;
    fn validate(&self, tid: TxnID, read_set: &HashMap<Key, Version>) -> bool;
    fn commit(&self, tid: TxnID, write_set: HashMap<Key, WriteOp>);
}
