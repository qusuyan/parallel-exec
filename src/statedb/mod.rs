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
use std::time::Duration;

use crate::version_store::{VersionStoreError, VersionedStore};
use crate::{AccountAddress, Field, Key, StateKey, TxnID, Value, Version, WriteOp};

use primitive_types::U256;
use std::collections::HashMap;

#[derive(Debug)]
pub struct StateDBOp {
    pub op_type: StateDBOpType,
    pub account: AccountAddress,
    pub start_time: Duration,
    pub storage_time: Duration,
}

#[derive(Debug)]
pub enum StateDBOpType {
    Exist,
    Empty,
    GetBalance,
    GetNonce,
    GetCode,
    GetCodeHash,
    GetState(StateKey),
    GetCommittedState(StateKey),
    HasSelfDestructed,
    CheckEnoughBalance(U256),
    AddBalance(U256),
    SubBalance(U256),
    SetBalance(U256),
    SetNonce,
    SetCode,
    SetState(StateKey),
    SelfDestruct,
    CreateAccount,
}

#[derive(Debug)]
pub enum StateDBError {
    ReadUndefined,
    IncrementNone,
    ReadInconsistentVersions,
    StorageError(VersionStoreError),
}

impl From<VersionStoreError> for StateDBError {
    fn from(err: VersionStoreError) -> Self {
        StateDBError::StorageError(err)
    }
}

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
    pub(crate) fn new<'a>(
        &self,
        id: TxnID,
        storage: &'a dyn VersionedStore,
    ) -> Box<dyn StateDB + 'a> {
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
    fn write(
        &mut self,
        account: AccountAddress,
        field: Field,
        write_op: WriteOp,
    ) -> Result<(), StateDBError>;
    fn read(
        &mut self,
        account: AccountAddress,
        field: Field,
    ) -> Result<Option<Value>, StateDBError>;
}

pub(crate) trait StateDB: StateDBBase {
    fn exist(&mut self, address: AccountAddress) -> Result<bool, StateDBError> {
        self.read(address, Field::Exist).map(|val| val.is_some())
    }

    fn empty(&mut self, address: AccountAddress) -> Result<bool, StateDBError> {
        if !self.exist(address)? {
            return Ok(true);
        }
        let balance = self
            .read(address, Field::Balance)?
            .ok_or(StateDBError::ReadUndefined)?;
        let nonce = self
            .read(address, Field::Nonce)?
            .ok_or(StateDBError::ReadUndefined)?;
        let code = self
            .read(address, Field::CodeHash)?
            .ok_or(StateDBError::ReadUndefined)?;
        Ok(balance.is_zero() && nonce.is_zero() && code.is_zero())
    }

    fn get_balance(&mut self, address: AccountAddress) -> Result<Value, StateDBError> {
        if !self.exist(address)? {
            return Ok(Value::zero());
        }
        self.read(address, Field::Balance)?
            .ok_or(StateDBError::ReadUndefined)
    }

    fn get_nonce(&mut self, address: AccountAddress) -> Result<Value, StateDBError> {
        if !self.exist(address)? {
            return Ok(Value::zero());
        }
        self.read(address, Field::Nonce)?
            .ok_or(StateDBError::ReadUndefined)
    }

    fn get_code(&mut self, address: AccountAddress) -> Result<Value, StateDBError> {
        if !self.exist(address)? {
            return Ok(Value::zero());
        }
        self.read(address, Field::Code)?
            .ok_or(StateDBError::ReadUndefined)
    }

    fn get_code_hash(&mut self, address: AccountAddress) -> Result<Value, StateDBError> {
        if !self.exist(address)? {
            return Ok(Value::zero());
        }
        self.read(address, Field::CodeHash)?
            .ok_or(StateDBError::ReadUndefined)
    }

    fn get_state(
        &mut self,
        address: AccountAddress,
        state_key: StateKey,
    ) -> Result<Value, StateDBError> {
        if !self.exist(address)? {
            return Ok(Value::zero());
        }
        self.read(address, Field::State(state_key))?
            .ok_or(StateDBError::ReadUndefined)
    }

    fn get_committed_state(
        &mut self,
        address: AccountAddress,
        state_key: StateKey,
    ) -> Result<Value, StateDBError>;

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
        let orig_balance = self.get_balance(address)?;
        self.write(
            address,
            Field::Balance,
            WriteOp::Set(orig_balance.saturating_add(amount)),
        )
    }

    fn sub_balance(&mut self, address: AccountAddress, amount: U256) -> Result<(), StateDBError> {
        let orig_balance = self.get_balance(address)?;
        self.write(
            address,
            Field::Balance,
            WriteOp::Set(orig_balance.saturating_sub(amount)),
        )
    }

    fn set_balance(&mut self, address: AccountAddress, amount: U256) -> Result<(), StateDBError> {
        if !self.exist(address)? {
            return Ok(());
        }
        self.write(address, Field::Balance, WriteOp::Set(amount))
    }

    fn set_nonce(&mut self, address: AccountAddress, nonce: Value) -> Result<(), StateDBError> {
        if !self.exist(address)? {
            return Ok(());
        }
        self.write(address, Field::Nonce, WriteOp::Set(nonce))
    }

    fn set_code(&mut self, address: AccountAddress, code: Value) -> Result<(), StateDBError> {
        if !self.exist(address)? {
            return Ok(());
        }
        self.write(address, Field::Code, WriteOp::Set(code))?;
        self.write(address, Field::CodeHash, WriteOp::Set(code))
    }

    fn set_state(
        &mut self,
        address: AccountAddress,
        state_key: StateKey,
    ) -> Result<(), StateDBError> {
        if !self.exist(address)? {
            return Ok(());
        }
        self.write(
            address,
            Field::State(state_key),
            WriteOp::Set(Value::zero()),
        )
    }

    fn self_destruct(&mut self, address: AccountAddress) -> Result<(), StateDBError> {
        self.write(address, Field::Exist, WriteOp::Set(Value::zero()))
    }

    fn create_account(&mut self, address: AccountAddress) -> Result<(), StateDBError> {
        if self.exist(address)? {
            return Ok(());
        }
        self.write(address, Field::Exist, WriteOp::Set(Value::one()))?;
        self.write(address, Field::Balance, WriteOp::Set(Value::zero()))?;
        self.write(address, Field::Nonce, WriteOp::Set(Value::zero()))?;
        // self.write(address, Field::CodeHash, WriteOp::Set(Value::zero()))?;
        Ok(())
    }

    fn into_validation_sets(self: Box<Self>) -> ValidationSets;
}
