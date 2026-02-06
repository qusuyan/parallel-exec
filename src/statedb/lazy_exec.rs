/**
 * LazyExecution lazily executes write operations since their effects do not have external impact.
 * This allows us to treat AddBalance and SubBalance as writes instead of read-modify-write,
 * reducing conflicts and improving parallelism.
 */
use super::{StateDB, StateDBBase, StateDBError, ValidationSets, Value, Version};
use crate::version_store::VersionedStore;
use crate::{AccountAddress, Field, Key, TxnID, WriteOp};

use std::collections::HashMap;
use std::collections::hash_map::Entry;

pub(crate) struct LazyExecStateDB<'a> {
    id: TxnID,
    storage: &'a dyn VersionedStore,
    read_set: HashMap<Key, Version>,
    write_set: HashMap<Key, WriteOp>,
}

impl<'a> LazyExecStateDB<'a> {
    pub fn new(id: TxnID, storage: &'a dyn VersionedStore) -> Self {
        Self {
            id,
            storage,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
        }
    }
}

impl<'a> StateDBBase for LazyExecStateDB<'a> {
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
                    let (Some(orig_val), orig_ver) = self.storage.get(self.id, &key)? else {
                        return Err(StateDBError::IncrementNone);
                    };
                    self.read_set.insert(key.clone(), orig_ver);
                    let new_val = write_op.apply_to(&orig_val);
                    *write_op = WriteOp::Set(new_val);
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

impl<'a> StateDB for LazyExecStateDB<'a> {
    fn add_balance(&mut self, address: AccountAddress, amount: Value) -> Result<(), StateDBError> {
        let key = Key {
            address,
            field: Field::Balance,
        };
        match self.write_set.entry(key) {
            Entry::Occupied(mut entry) => {
                entry.insert(*entry.get() - WriteOp::Inc(amount, true));
            }
            Entry::Vacant(entry) => {
                entry.insert(WriteOp::Inc(amount, true));
            }
        }
        Ok(())
    }

    fn sub_balance(
        &mut self,
        address: AccountAddress,
        amount: primitive_types::U256,
    ) -> Result<(), StateDBError> {
        let key = Key {
            address,
            field: Field::Balance,
        };
        match self.write_set.entry(key) {
            Entry::Occupied(mut entry) => {
                entry.insert(*entry.get() - WriteOp::Inc(amount, false));
            }
            Entry::Vacant(entry) => {
                entry.insert(WriteOp::Inc(amount, false));
            }
        }
        Ok(())
    }

    fn into_validation_sets(self: Box<Self>) -> ValidationSets {
        ValidationSets {
            read_set: self.read_set,
            write_set: self.write_set,
        }
    }

    fn get_committed_state(
        &mut self,
        address: AccountAddress,
        state_key: crate::StateKey,
    ) -> Result<Value, StateDBError> {
        self.storage.get_committed_state(&address, &state_key)?;
        Ok(Value::zero())
    }
}
