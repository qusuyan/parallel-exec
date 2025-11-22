/**
 * StateDB wraps Storage to provide higher level state operations.
 * It additionally maintains the read and write set so each transaction should
 * have its own StateDB instance.
 */
mod basic;
use basic::BasicStateDB;

mod lazy_exec;
use lazy_exec::LazyExecStateDB;

mod check_enough_balance;
use check_enough_balance::CheckEnoughBalanceStateDB;

use std::result::Result;

use crate::storage::Storage;
use crate::{AccountAddress, Field, Key, StateKey, Value, Version, WriteOp};

use primitive_types::U256;
use std::collections::HashMap;

#[derive(Debug)]
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

#[derive(Debug)]
pub enum StateDBError {}

#[derive(Debug)]
pub struct ValidationSets {
    pub read_set: HashMap<Key, Version>,
    pub write_set: HashMap<Key, WriteOp>,
}

#[derive(clap::ValueEnum, Clone, Debug, Copy)]
pub enum StateDBType {
    Basic,
    LazyExec,
    CheckEnoughBalance,
}

impl StateDBType {
    pub(crate) fn new<'a>(&self, id: Version, storage: &'a dyn Storage) -> Box<dyn StateDB + 'a> {
        match self {
            StateDBType::Basic => Box::new(BasicStateDB::new(id, storage)),
            StateDBType::LazyExec => Box::new(LazyExecStateDB::new(id, storage)),
            StateDBType::CheckEnoughBalance => {
                Box::new(CheckEnoughBalanceStateDB::new(id, storage))
            }
        }
    }
}

pub(crate) trait StateDBBase {
    fn write(&mut self, key: Key, write_op: WriteOp) -> Result<(), StateDBError>;
    fn read(&mut self, key: Key) -> Result<Value, StateDBError>;
}

pub(crate) trait StateDB: StateDBBase {
    fn exist(&mut self, address: AccountAddress) -> Result<bool, StateDBError> {
        let key = Key {
            address,
            field: Field::Exist,
        };
        self.read(key).map(|val| val == Value::one())
    }

    fn empty(&mut self, address: AccountAddress) -> Result<bool, StateDBError> {
        let balance = self.get_balance(address)?;
        let nonce = self.get_nonce(address)?;
        let code = self.get_code(address)?;
        Ok(balance.is_zero() && nonce.is_zero() && code.is_zero())
    }

    fn get_balance(&mut self, address: AccountAddress) -> Result<Value, StateDBError> {
        let key = Key {
            address,
            field: Field::Balance,
        };
        self.read(key)
    }

    fn get_nonce(&mut self, address: AccountAddress) -> Result<Value, StateDBError> {
        let key = Key {
            address,
            field: Field::Nonce,
        };
        self.read(key)
    }

    fn get_code(&mut self, address: AccountAddress) -> Result<Value, StateDBError> {
        let key = Key {
            address,
            field: Field::Code,
        };
        self.read(key)
    }

    fn get_state(
        &mut self,
        address: AccountAddress,
        state_key: StateKey,
    ) -> Result<Value, StateDBError> {
        let key = Key {
            address,
            field: Field::State(state_key),
        };
        self.read(key)
    }

    fn get_committed_state(
        &mut self,
        _address: AccountAddress,
        _state_key: StateKey,
    ) -> Result<Value, StateDBError> {
        // For simplicity, we assume committed state is always zero in this mock implementation.
        Ok(Value::zero())
    }

    fn has_self_destructed(&mut self, address: AccountAddress) -> Result<bool, StateDBError> {
        self.exist(address).map(|exist| !exist) // has_self_destructed = !exist
    }

    fn check_enough_balance(
        &mut self,
        address: AccountAddress,
        amount: U256,
    ) -> Result<bool, StateDBError> {
        self.get_balance(address).map(|val| val >= amount)
    }

    fn add_balance(&mut self, address: AccountAddress, amount: U256) -> Result<(), StateDBError> {
        self.get_balance(address)?;
        let key = Key {
            address,
            field: Field::Balance,
        };
        self.write(key, WriteOp::Set(amount))
    }

    fn sub_balance(&mut self, address: AccountAddress, amount: U256) -> Result<(), StateDBError> {
        self.get_balance(address)?;
        let key = Key {
            address,
            field: Field::Balance,
        };
        self.write(key, WriteOp::Set(amount))
    }

    fn set_balance(&mut self, address: AccountAddress, amount: U256) -> Result<(), StateDBError> {
        let key = Key {
            address,
            field: Field::Balance,
        };
        self.write(key, WriteOp::Set(amount))
    }

    fn set_nonce(&mut self, address: AccountAddress) -> Result<(), StateDBError> {
        let key = Key {
            address,
            field: Field::Nonce,
        };
        self.write(key, WriteOp::Set(Value::zero()))
    }

    fn set_code(&mut self, address: AccountAddress) -> Result<(), StateDBError> {
        let key = Key {
            address,
            field: Field::Code,
        };
        self.write(key, WriteOp::Set(Value::zero()))
    }

    fn set_state(
        &mut self,
        address: AccountAddress,
        state_key: StateKey,
    ) -> Result<(), StateDBError> {
        let key = Key {
            address,
            field: Field::State(state_key),
        };
        self.write(key, WriteOp::Set(Value::zero()))
    }

    fn self_destruct(&mut self, address: AccountAddress) -> Result<(), StateDBError> {
        let key = Key {
            address,
            field: Field::Exist,
        };
        self.write(key, WriteOp::Set(Value::zero()))
    }

    fn create_account(&mut self, address: AccountAddress) -> Result<(), StateDBError> {
        let key = Key {
            address,
            field: Field::Exist,
        };
        self.write(key, WriteOp::Set(Value::one()))
    }

    fn into_validation_sets(self: Box<Self>) -> ValidationSets;
}
