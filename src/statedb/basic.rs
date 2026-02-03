/**
 * Basic StateDB implementation. It treats all operations as intended without any optimization.
 */
use super::{StateDB, StateDBBase, StateDBError, ValidationSets, Value, Version};
use crate::version_store::VersionedStore;
use crate::{AccountAddress, Field, Key, StateKey, TxnID, WriteOp};

use std::collections::HashMap;

pub(crate) struct BasicStateDB<'a> {
    id: TxnID,
    storage: &'a dyn VersionedStore,
    read_set: HashMap<Key, Version>,
    write_set: HashMap<Key, WriteOp>,
}

impl<'a> BasicStateDB<'a> {
    pub fn new(id: TxnID, storage: &'a dyn VersionedStore) -> Self {
        Self {
            id,
            storage,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
        }
    }
}

impl<'a> StateDBBase for BasicStateDB<'a> {
    fn write(
        &mut self,
        address: AccountAddress,
        field: Field,
        write_op: WriteOp,
    ) -> Result<(), StateDBError> {
        self.write_set.insert(Key { address, field }, write_op);
        Ok(())
    }

    fn read(
        &mut self,
        address: AccountAddress,
        field: Field,
    ) -> Result<Option<Value>, StateDBError> {
        let key = Key { address, field };
        match self.write_set.get_mut(&key) {
            Some(write_op) => match write_op {
                WriteOp::Set(val) => {
                    return Ok(Some(*val)); // read your own write
                }
                WriteOp::Inc(..) => {
                    unreachable!()
                }
            },
            None => {}
        }

        // read from storage
        let (val, ver) = self.storage.get(self.id, &key)?;
        if let Some(orig_v) = self.read_set.insert(key, ver) {
            if orig_v != ver {
                return Err(StateDBError::ReadInconsistentVersions);
            }
        }

        Ok(val)
    }
}

impl<'a> StateDB for BasicStateDB<'a> {
    fn into_validation_sets(self: Box<Self>) -> ValidationSets {
        ValidationSets {
            read_set: self.read_set,
            write_set: self.write_set,
        }
    }

    fn get_committed_state(
        &mut self,
        address: AccountAddress,
        state_key: StateKey,
    ) -> Result<Value, StateDBError> {
        self.storage.get_committed_state(&address, &state_key)?;
        Ok(Value::zero())
    }
}
