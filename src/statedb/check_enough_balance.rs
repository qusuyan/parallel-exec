use super::{Revision, RevisionBase, StateDB, StateDBError, ValidationSets, Value, Version};
use crate::{AccountAddress, Field, Key, WriteOp};

use std::collections::HashMap;
use std::collections::hash_map::Entry;

pub(crate) struct CheckEnoughBalanceRevision<'a> {
    statedb: &'a StateDB,
    read_set: HashMap<Key, Version>,
    write_set: HashMap<Key, WriteOp>,
}

impl<'a> CheckEnoughBalanceRevision<'a> {
    pub fn new(statedb: &'a StateDB) -> Self {
        Self {
            statedb,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
        }
    }
}

impl<'a> RevisionBase for CheckEnoughBalanceRevision<'a> {
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

impl<'a> Revision for CheckEnoughBalanceRevision<'a> {
    fn add_balance(&mut self, address: AccountAddress, amount: Value) -> Result<(), StateDBError> {
        let key = Key {
            address,
            field: Field::Balance,
        };
        match self.write_set.entry(key) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                WriteOp::Set(current_val) => {
                    *current_val += amount;
                }
                WriteOp::Inc(inc, sign) => {
                    if *sign {
                        // adding more balance
                        *inc += amount;
                    } else {
                        if *inc > amount {
                            // subtracted more balance than adding
                            *inc -= amount;
                        } else if *inc < amount {
                            // subtracted less balance than adding
                            *sign = true;
                            *inc = amount - *inc;
                        } else {
                            entry.remove();
                        }
                    }
                }
            },
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
            Entry::Occupied(mut entry) => match entry.get_mut() {
                WriteOp::Set(current_val) => {
                    *current_val += amount;
                }
                WriteOp::Inc(inc, sign) => {
                    if !*sign {
                        // subtracting more balance
                        *inc += amount;
                    } else {
                        if *inc > amount {
                            // added more balance than subtracting
                            *inc -= amount;
                        } else if *inc < amount {
                            // added less balance than subtracting
                            *sign = false;
                            *inc = amount - *inc;
                        } else {
                            entry.remove();
                        }
                    }
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(WriteOp::Inc(amount, false));
            }
        }
        Ok(())
    }

    fn check_enough_balance(
        &mut self,
        address: AccountAddress,
        amount: primitive_types::U256,
    ) -> Result<bool, StateDBError> {
        let key = Key {
            address: address,
            field: Field::Balance,
        };
        match self.write_set.get_mut(&key) {
            Some(write_op) => match write_op {
                WriteOp::Set(val) => {
                    return Ok(*val >= amount);
                } // have enough balance
                WriteOp::Inc(inc, sign) => {
                    if *sign && *inc >= amount {
                        // enough balance, no need to read actual balance
                        return Ok(true);
                    } else {
                        // for simplicity - since the actual value stored will not matter here
                        *write_op = WriteOp::Set(amount);
                    }
                }
            },
            None => {}
        }

        let value = self.read(key)?;
        return Ok(value >= amount);
    }

    fn into_validation_sets(self: Box<Self>) -> ValidationSets {
        ValidationSets {
            read_set: self.read_set,
            write_set: self.write_set,
        }
    }
}
