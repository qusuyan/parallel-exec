use crate::storage::StorageKeyType;
use crate::{AccountAddress, Value};
use crate::{StateDBOp, StateDBOpType, Storage, storage::StorageKey};

use std::collections::HashMap;
use tokio::time::{Duration, Instant};

const STORAGE_RESOLUTION: Duration = Duration::from_nanos(4500);
// const STORAGE_RESOLUTION: Duration = Duration::ZERO;

#[derive(Clone, Debug)]
pub struct MockStoreBuilder {
    read_times: HashMap<StorageKey, Vec<Duration>>,
    created: HashMap<AccountAddress, usize>,
}

impl MockStoreBuilder {
    pub fn new() -> Self {
        MockStoreBuilder {
            read_times: HashMap::new(),
            created: HashMap::new(),
        }
    }

    pub fn record_op(&mut self, op: &StateDBOp) {
        let StateDBOp {
            op_type,
            account,
            storage_time,
            ..
        } = op;
        // add read time for the account itself
        let account_key = StorageKey {
            account: *account,
            key_type: StorageKeyType::Account,
        };
        let read_times = self.read_times.entry(account_key.clone()).or_default();
        if storage_time > &Duration::ZERO {
            read_times.push(*storage_time);
        }

        match op_type {
            StateDBOpType::CreateAccount => {
                self.created.insert(*account, read_times.len());
                // reading code before the account is created are only reading the account
                // the same goes for state but this never happens in practice
                let code_key = StorageKey {
                    account: *account,
                    key_type: StorageKeyType::Code,
                };
                self.read_times.remove(&code_key);
            }
            StateDBOpType::GetCode => {
                let storage_key = StorageKey {
                    account: *account,
                    key_type: StorageKeyType::Code,
                };
                if storage_time > &Duration::ZERO {
                    let read_times = self.read_times.entry(storage_key.clone()).or_default();
                    read_times.push(*storage_time);
                }
            }
            StateDBOpType::GetState(state_key) | StateDBOpType::GetCommittedState(state_key) => {
                let storage_key = StorageKey {
                    account: *account,
                    key_type: StorageKeyType::State(*state_key),
                };
                if storage_time > &Duration::ZERO {
                    let read_times = self.read_times.entry(storage_key.clone()).or_default();
                    read_times.push(*storage_time);
                }
            }
            _ => {}
        }
    }

    pub fn build(self) -> MockStore {
        let entries = self
            .read_times
            .into_iter()
            .map(|(key, dur)| {
                if let Some(created_time) = self.created.get(&key.account) {
                    if key.key_type == StorageKeyType::Account {
                        let avg_dur = dur.into_iter().take(*created_time).sum::<Duration>()
                            / *created_time as u32;
                        return (key, (avg_dur, None));
                    }
                }

                // assuming the value is already created
                if dur.len() > 1 {
                    log::warn!("Multiple access to the existing field {key:x} hit storage");
                }
                (key, (*dur.first().unwrap(), Some(Value::zero())))
            })
            .collect();
        MockStore { entries }
    }
}

impl Default for MockStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct MockStore {
    entries: HashMap<StorageKey, (Duration, Option<Value>)>,
}

impl Storage for MockStore {
    fn get(&self, key: &StorageKey) -> Option<Value> {
        let start_time = Instant::now();
        let (read_duration, val) = self.entries.get(key).cloned().unwrap_or_else(|| {
            #[cfg(debug_assertions)]
            log::warn!("Storage Key {key:x} not found in EmuStore");
            (Duration::ZERO, Some(Value::zero()))
        });
        let end_time = start_time + read_duration - STORAGE_RESOLUTION;
        while Instant::now() < end_time {}
        // println!("Get {key:x} with time {read_duration:?} and value {val:?}");
        val
    }
}
