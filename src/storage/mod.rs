mod mock;
pub use mock::MockStore;
use primitive_types::U256;

use crate::AccountAddress;

use dashmap::DashMap;
use lru::LruCache;
use std::num::NonZeroUsize;
use tokio::sync::Mutex;

pub trait Storage: Sync + Send {
    fn get(&self, key: &StorageKey);
}

#[derive(Clone, Copy, Debug)]
enum CacheEntry {
    Ready,
    Pending,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum StorageKey {
    Account(AccountAddress),
    State(AccountAddress, U256),
}

impl std::fmt::LowerHex for StorageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageKey::Account(addr) => write!(f, "{:x}", addr),
            StorageKey::State(addr, state_key) => write!(f, "{:x}-{:x}", addr, state_key),
        }
    }
}

/// An in‑memory storage emulator with a small async cache and in‑flight
/// coordination so only one fetch happens per cache miss.
///
/// Behaviour:
/// - Cache hit: return immediately.
/// - Cache miss while another task is loading the same key: wait for it.
/// - Cache miss with no ongoing load: emulate storage latency, load, then cache.
pub struct CacheStore {
    cache: DashMap<StorageKey, CacheEntry>,
    evict_queue: Mutex<LruCache<StorageKey, ()>>,
    storage: Box<dyn Storage>,
}

impl CacheStore {
    /// Create a storage emulator with an LRU cache of the given capacity
    /// (must be > 0).
    pub fn new(cache_capacity: usize, storage: Box<dyn Storage>) -> Self {
        let cap =
            NonZeroUsize::new(cache_capacity).expect("cache capacity must be greater than zero");
        Self {
            cache: DashMap::new(),
            evict_queue: Mutex::new(LruCache::new(cap)),
            storage,
        }
    }

    /// Get a value. Supply a loader that produces the value after the
    /// (emulated) storage latency.
    pub fn get(&self, key: &StorageKey) {
        // check cache entry first
        let mut is_loader = false;
        let mut cache_entry = self.cache.entry(key.clone()).or_insert_with(|| {
            // we are the loader
            is_loader = true;
            CacheEntry::Pending
        });

        match *cache_entry {
            CacheEntry::Ready => {
                // cache hit
                return;
            }
            CacheEntry::Pending => {
                if is_loader {
                    // delegate storage access to the underlying storage
                    self.storage.get(&key);

                    // update LRU
                    let evict_key = {
                        let mut evict_queue = loop {
                            if let Ok(guard) = self.evict_queue.try_lock() {
                                break guard;
                            }
                        };
                        match evict_queue.push(key.clone(), ()) {
                            Some((evicted_key, _)) if evicted_key == *key => None,
                            Some((evicted_key, _)) => Some(evicted_key),
                            None => None,
                        }
                    };

                    // remove evicted key and insert new key into cache
                    if let Some(evict_key) = evict_key {
                        self.cache.remove(&evict_key);
                    }
                    *cache_entry = CacheEntry::Ready;
                } else {
                    // we are a waiter, spin until ready
                    loop {
                        if let Some(entry) = self.cache.get(&key) {
                            if matches!(*entry, CacheEntry::Ready) {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}
