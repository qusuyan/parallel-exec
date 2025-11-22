/**
 * CheckEnoughBalanceStateDB introduces the new operation `check_enough_balance`. The foundamental
 * observation is that in most cases GetBalance is used to check if an account has enough balance.
 * If there is an AddBalance on the same account for a value greater than or equal to the checked
 * amount, we can safely skip reading the actual balance from storage. This optimization can
 * reduce read-write conflicts and improve parallelism in several cases.
 */
use super::{StateDB, StateDBBase, StateDBError, ValidationSets, Value, Version};
use crate::storage::Storage;
use crate::{AccountAddress, Field, Key, TxnID, WriteOp};

use std::collections::HashMap;
use std::collections::hash_map::Entry;

pub(crate) struct CheckEnoughBalanceStateDB<'a> {
    id: TxnID,
    storage: &'a dyn Storage,
    read_set: HashMap<Key, Version>,
    write_set: HashMap<Key, WriteOp>,
}

impl<'a> CheckEnoughBalanceStateDB<'a> {
    pub fn new(id: TxnID, storage: &'a dyn Storage) -> Self {
        Self {
            id,
            storage,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
        }
    }
}

impl<'a> StateDBBase for CheckEnoughBalanceStateDB<'a> {
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
        let version = self.storage.get(self.id, &key);
        self.read_set.insert(key, version);
        Ok(Value::one())
    }
}

impl<'a> StateDB for CheckEnoughBalanceStateDB<'a> {
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
                        } else {
                            // subtracted less balance than adding
                            *sign = true;
                            *inc = amount - *inc;
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
                WriteOp::Set(current_val) => *current_val = current_val.saturating_sub(amount),
                WriteOp::Inc(inc, sign) => {
                    if !*sign {
                        // subtracting more balance
                        *inc += amount;
                    } else {
                        if *inc > amount {
                            // added more balance than subtracting
                            *inc -= amount;
                        } else {
                            // added less balance than subtracting
                            *sign = false;
                            *inc = amount - *inc;
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
                    // we know we have enough balance due to recent set
                    return Ok(*val >= amount);
                }
                WriteOp::Inc(inc, sign) => {
                    if *sign && *inc >= amount {
                        // enough balance, no need to read actual balance
                        return Ok(true);
                    }
                }
            },
            None => {}
        }

        let _ = self.get_balance(address)?;
        return Ok(true);
    }

    fn into_validation_sets(self: Box<Self>) -> ValidationSets {
        ValidationSets {
            read_set: self.read_set,
            write_set: self.write_set,
        }
    }
}
