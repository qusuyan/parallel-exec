use crate::{StateDBOp, StateDBOpType, Storage, storage::StorageKey};

use std::collections::{HashMap, HashSet};
use tokio::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct MockStore {
    read_times: HashMap<StorageKey, Duration>,
    in_cache: HashSet<StorageKey>,
}

impl MockStore {
    pub fn new() -> Self {
        MockStore {
            read_times: HashMap::new(),
            in_cache: HashSet::new(),
        }
    }

    pub fn record_op(&mut self, op: &StateDBOp) {
        let StateDBOp {
            op_type,
            account,
            storage_time,
            ..
        } = op;
        match op_type {
            StateDBOpType::GetBalance
            | StateDBOpType::GetNonce
            | StateDBOpType::GetCode
            | StateDBOpType::Exist
            | StateDBOpType::Empty
            | StateDBOpType::HasSelfDestructed
            | StateDBOpType::CheckEnoughBalance(_)
            | StateDBOpType::AddBalance(_)  // treat read-modify-write as read
            | StateDBOpType::SubBalance(_) => {
                let storage_key = StorageKey::Account(*account);
                if storage_time > &Duration::ZERO {
                    if self
                        .read_times
                        .insert(storage_key.clone(), *storage_time)
                        .is_some()
                    {
                        log::warn!("Account {:x} read time overwritten", account);
                    }
                    self.in_cache.insert(storage_key);
                } else {
                    #[cfg(debug_assertions)]
                    if !self.in_cache(&storage_key) {
                        log::warn!(
                            "Account {:x} read time is zero even though it was not read or written before",
                            account
                        );
                    }
                }
            }
            StateDBOpType::GetState(state_key) | StateDBOpType::GetCommittedState(state_key) => {
                let storage_key = StorageKey::State(*account, *state_key);
                if storage_time > &Duration::ZERO {
                    if self.read_times.insert(storage_key.clone(), *storage_time).is_some() {
                        log::warn!("State {:x}-{:x} read time overwritten", account, state_key);
                    }
                    self.in_cache.insert(storage_key);
                } else {
                    #[cfg(debug_assertions)]
                    if !self.in_cache(&storage_key) {
                        log::warn!(
                            "State {:x}-{:x} read time is zero even though it was not read or written before",
                            account,
                            state_key
                        );
                    }
                }
            }
            StateDBOpType::SetBalance(_)
            | StateDBOpType::SetNonce
            | StateDBOpType::SetCode
            | StateDBOpType::SelfDestruct
            | StateDBOpType::CreateAccount => {
                let storage_key = StorageKey::Account(*account);
                self.in_cache.insert(storage_key);
            }
            StateDBOpType::SetState(state_key) => {
                let storage_key = StorageKey::State(*account, *state_key);
                self.in_cache.insert(storage_key);
            }
        }
    }

    #[inline]
    pub fn in_cache(&self, storage_key: &StorageKey) -> bool {
        self.in_cache.contains(storage_key)
    }
}

impl Storage for MockStore {
    fn get(&self, key: &StorageKey) {
        let start_time = Instant::now();
        let read_duration = self.read_times.get(key).unwrap_or_else(|| {
            #[cfg(debug_assertions)]
            if !self.in_cache.contains(key) {
                log::warn!("Storage Key {:x} not found in EmuStore", key);
            }
            &Duration::ZERO
        });
        let end_time = start_time + *read_duration;
        while Instant::now() < end_time {}
    }
}

impl Default for MockStore {
    fn default() -> Self {
        Self::new()
    }
}
