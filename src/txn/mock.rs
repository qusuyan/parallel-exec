use core::panic;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

use async_trait::async_trait;
use primitive_types::U256;
use tokio::time::{Duration, Instant};

use super::{ExecResult, Transaction, WriteOp};
use crate::{AccountAddress, Field, Key, StateDB, StateKey, Value, Version};

pub enum StateDBOp {
    Exist(AccountAddress),
    Empty(AccountAddress),
    GetBalance(AccountAddress),
    GetNonce(AccountAddress),
    GetCode(AccountAddress),
    GetState(AccountAddress, StateKey),
    GetCommittedState(AccountAddress, StateKey),
    HasSelfDestructed(AccountAddress),
    CheckEnoughBalance(AccountAddress, U256),
    AddBalance(AccountAddress, U256),
    SubBalance(AccountAddress, U256),
    SetBalance(AccountAddress, U256),
    SetNonce(AccountAddress),
    SetCode(AccountAddress),
    SetState(AccountAddress, StateKey),
    SelfDestruct(AccountAddress),
    CreateAccount(AccountAddress),
}

pub struct MockTransaction {
    id: u64,
    exec_time: Duration,
    ops: Vec<StateDBOp>,
}

impl MockTransaction {
    pub fn new(id: u64, exec_time: Duration, ops: Vec<StateDBOp>) -> Self {
        Self { id, exec_time, ops }
    }

    fn nonbalance_read(
        statedb: &StateDB,
        key: Key,
        read_set: &mut HashMap<Key, Version>,
        write_set: &HashMap<Key, WriteOp>,
    ) {
        match write_set.get(&key) {
            Some(write_op) => {
                // If there is a Set in the write set, use that value instead
                match write_op {
                    WriteOp::Set(_) => {} // read own write - not reading actual value
                    WriteOp::Inc(..) => {
                        panic!("Field cannot be incremented");
                    }
                }
            }
            None => {
                let version = statedb.get(&key).map(|e| *e).unwrap_or(0);
                read_set.insert(key, version);
            }
        }
    }

    fn balance_read(
        statedb: &StateDB,
        key: Key,
        read_set: &mut HashMap<Key, Version>,
        write_set: &mut HashMap<Key, WriteOp>,
    ) {
        assert!(matches!(key.field, Field::Balance));
        match write_set.get_mut(&key) {
            Some(write_op) => match write_op {
                WriteOp::Set(_) => {}
                WriteOp::Inc(inc, _) => {
                    let version = statedb.get(&key).map(|e| *e).unwrap_or(0);
                    *write_op = WriteOp::Set(*inc);
                    read_set.insert(key.clone(), version);
                }
            },
            None => {
                let version = statedb.get(&key).map(|e| *e).unwrap_or(0);
                read_set.insert(key, version);
            }
        }
    }
}

#[async_trait]
impl Transaction for MockTransaction {
    fn id(&self) -> u64 {
        self.id
    }

    async fn execute(&self, statedb: &StateDB) -> ExecResult {
        let start = Instant::now();
        let end = start + self.exec_time;
        let mut read_set = HashMap::new();
        let mut write_set = HashMap::new();
        for op in &self.ops {
            match op {
                StateDBOp::Exist(address) => {
                    let key = Key {
                        address: *address,
                        field: Field::Exist,
                    };
                    Self::nonbalance_read(statedb, key, &mut read_set, &write_set);
                }
                StateDBOp::Empty(address) => {
                    let balance_key = Key {
                        address: *address,
                        field: Field::Balance,
                    };
                    Self::balance_read(statedb, balance_key, &mut read_set, &mut write_set);

                    let nonce_key = Key {
                        address: *address,
                        field: Field::Nonce,
                    };
                    Self::nonbalance_read(statedb, nonce_key, &mut read_set, &write_set);

                    let code_key = Key {
                        address: *address,
                        field: Field::Code,
                    };
                    Self::nonbalance_read(statedb, code_key, &mut read_set, &write_set);
                }
                StateDBOp::GetBalance(address) => {
                    let key = Key {
                        address: *address,
                        field: Field::Balance,
                    };
                    Self::balance_read(statedb, key, &mut read_set, &mut write_set);
                }
                StateDBOp::GetNonce(address) => {
                    let key = Key {
                        address: *address,
                        field: Field::Nonce,
                    };
                    Self::nonbalance_read(statedb, key, &mut read_set, &write_set);
                }
                StateDBOp::GetCode(address) => {
                    let key = Key {
                        address: *address,
                        field: Field::Code,
                    };
                    Self::nonbalance_read(statedb, key, &mut read_set, &write_set);
                }
                StateDBOp::GetState(address, state_key) => {
                    let key = Key {
                        address: *address,
                        field: Field::State(*state_key),
                    };
                    Self::nonbalance_read(statedb, key, &mut read_set, &write_set);
                }
                StateDBOp::GetCommittedState(..) => {
                    // Do nothing since it won't conflict with other txns
                }
                StateDBOp::HasSelfDestructed(address) => {
                    let key = Key {
                        address: *address,
                        field: Field::Exist,
                    };
                    Self::nonbalance_read(statedb, key, &mut read_set, &write_set);
                }
                StateDBOp::CheckEnoughBalance(address, amount) => {
                    let key = Key {
                        address: *address,
                        field: Field::Balance,
                    };
                    match write_set.get_mut(&key) {
                        Some(write_op) => match write_op {
                            WriteOp::Set(_) => {}
                            WriteOp::Inc(inc, sign) => {
                                if *sign && *inc >= *amount {
                                    // enough balance, no need to read actual balance
                                } else {
                                    let version = statedb.get(&key).map(|e| *e).unwrap_or(0);
                                    *write_op = WriteOp::Set(*inc);
                                    read_set.insert(key.clone(), version);
                                }
                            }
                        },
                        None => {
                            let version = statedb.get(&key).map(|e| *e).unwrap_or(0);
                            read_set.insert(key, version);
                        }
                    }
                }
                StateDBOp::AddBalance(address, amount) => {
                    let key = Key {
                        address: *address,
                        field: Field::Balance,
                    };
                    match write_set.entry(key) {
                        Entry::Occupied(mut entry) => match entry.get_mut() {
                            WriteOp::Set(current_val) => {
                                *current_val += *amount;
                            }
                            WriteOp::Inc(inc, sign) => {
                                if *sign {
                                    // adding more balance
                                    *inc += *amount;
                                } else {
                                    if *inc > *amount {
                                        // subtracted more balance than adding
                                        *inc -= *amount;
                                    } else if *inc < *amount {
                                        // subtracted less balance than adding
                                        *sign = true;
                                        *inc = *amount - *inc;
                                    } else {
                                        entry.remove();
                                    }
                                }
                            }
                        },
                        Entry::Vacant(entry) => {
                            entry.insert(WriteOp::Inc(*amount, true));
                        }
                    }
                }
                StateDBOp::SubBalance(address, amount) => {
                    let key = Key {
                        address: *address,
                        field: Field::Balance,
                    };
                    match write_set.entry(key) {
                        Entry::Occupied(mut entry) => match entry.get_mut() {
                            WriteOp::Set(current_val) => {
                                *current_val += *amount;
                            }
                            WriteOp::Inc(inc, sign) => {
                                if !*sign {
                                    // subtracting more balance
                                    *inc += *amount;
                                } else {
                                    if *inc > *amount {
                                        // added more balance than subtracting
                                        *inc -= *amount;
                                    } else if *inc < *amount {
                                        // added less balance than subtracting
                                        *sign = false;
                                        *inc = *amount - *inc;
                                    } else {
                                        entry.remove();
                                    }
                                }
                            }
                        },
                        Entry::Vacant(entry) => {
                            entry.insert(WriteOp::Inc(*amount, false));
                        }
                    }
                }
                StateDBOp::SetBalance(address, amount) => {
                    let key = Key {
                        address: *address,
                        field: Field::Balance,
                    };
                    write_set.insert(key, WriteOp::Set(*amount));
                }
                StateDBOp::SetNonce(address) => {
                    let key = Key {
                        address: *address,
                        field: Field::Nonce,
                    };
                    write_set.insert(key, WriteOp::Set(Value::from(self.id)));
                }
                StateDBOp::SetCode(address) => {
                    let key = Key {
                        address: *address,
                        field: Field::Code,
                    };
                    write_set.insert(key, WriteOp::Set(Value::from(self.id)));
                }
                StateDBOp::SetState(address, state_key) => {
                    let key = Key {
                        address: *address,
                        field: Field::State(*state_key),
                    };
                    write_set.insert(key, WriteOp::Set(Value::from(self.id)));
                }
                StateDBOp::SelfDestruct(address) => {
                    let key = Key {
                        address: *address,
                        field: Field::Exist,
                    };
                    write_set.insert(key, WriteOp::Set(Value::from(0)));
                }
                StateDBOp::CreateAccount(address) => {
                    let key = Key {
                        address: *address,
                        field: Field::Exist,
                    };
                    write_set.insert(key, WriteOp::Set(Value::from(1)));
                }
            }
        }

        // mock execution time - do not deschedule the task
        while Instant::now() < end {}

        ExecResult {
            read_set,
            write_set,
        }
    }
}
