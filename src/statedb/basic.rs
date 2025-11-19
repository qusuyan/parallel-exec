use super::{Revision, RevisionBase, StateDB, StateDBError, ValidationSets, Value, Version};
use crate::{Key, WriteOp};

use std::collections::HashMap;

pub(crate) struct BasicRevision<'a> {
    statedb: &'a StateDB,
    read_set: HashMap<Key, Version>,
    write_set: HashMap<Key, WriteOp>,
}

impl<'a> BasicRevision<'a> {
    pub fn new(statedb: &'a StateDB) -> Self {
        Self {
            statedb,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
        }
    }
}

impl<'a> RevisionBase for BasicRevision<'a> {
    fn write(&mut self, key: Key, write_op: WriteOp) -> Result<(), StateDBError> {
        self.write_set.insert(key, write_op);
        Ok(())
    }

    fn read(&mut self, key: Key) -> Result<Value, StateDBError> {
        match self.write_set.get_mut(&key) {
            Some(write_op) => match write_op {
                WriteOp::Set(val) => {
                    return Ok(*val); // read your own write
                }
                WriteOp::Inc(inc, _) => {
                    // get value from statedb (later) and apply increment/decrement
                    *write_op = WriteOp::Set(*inc);
                }
            },
            None => {}
        }

        // read from statedb
        let version = self.statedb.get(&key).map(|e| *e).unwrap_or(0);
        self.read_set.insert(key, version);
        Ok(Value::one())
    }
}

impl<'a> Revision for BasicRevision<'a> {
    fn into_validation_sets(self: Box<Self>) -> ValidationSets {
        ValidationSets {
            read_set: self.read_set,
            write_set: self.write_set,
        }
    }
}
