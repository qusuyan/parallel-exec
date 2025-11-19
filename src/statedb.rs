use crate::{Key, Version};
use dashmap::DashMap;

pub(crate) type StateDB = DashMap<Key, Version>;
