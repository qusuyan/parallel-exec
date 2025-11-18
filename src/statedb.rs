use crate::{Key, Value, Version};
use dashmap::DashMap;

pub(crate) type StateDB = DashMap<Key, (Value, Version)>;
