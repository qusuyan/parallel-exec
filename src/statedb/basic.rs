/**
 * Basic StateDB implementation. It treats all operations as intended without any optimization.
 */
use super::{StateDB, StateDBBase, StateDBError, ValidationSets, Value, Version};
use crate::storage::Storage;
use crate::{Key, TxnID, WriteOp};

use std::collections::HashMap;

pub(crate) struct BasicStateDB<'a> {
    id: TxnID,
    storage: &'a dyn Storage,
    read_set: HashMap<Key, Version>,
    write_set: HashMap<Key, WriteOp>,
}

impl<'a> BasicStateDB<'a> {
    pub fn new(id: TxnID, storage: &'a dyn Storage) -> Self {
        Self {
            id,
            storage,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
        }
    }
}

impl<'a> StateDBBase for BasicStateDB<'a> {
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
        let version = self.storage.get(self.id, &key)?;
        self.read_set.insert(key, version);
        Ok(Value::one())
    }
}

impl<'a> StateDB for BasicStateDB<'a> {
    fn into_validation_sets(self: Box<Self>) -> ValidationSets {
        ValidationSets {
            read_set: self.read_set,
            write_set: self.write_set,
        }
    }
}
