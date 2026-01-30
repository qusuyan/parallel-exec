use std::num::NonZeroUsize;

use dashmap::DashMap;
use lru::LruCache;
use tokio::sync::Mutex;

use crate::Key;

#[derive(Clone, Copy, Debug)]
enum CacheEntry {
    Ready,
    Pending,
}

/// An in‑memory storage emulator with a small async cache and in‑flight
/// coordination so only one fetch happens per cache miss.
///
/// Behaviour:
/// - Cache hit: return immediately.
/// - Cache miss while another task is loading the same key: wait for it.
/// - Cache miss with no ongoing load: emulate storage latency, load, then cache.
pub struct CacheStore {
    cache: DashMap<Key, CacheEntry>,
    evict_queue: Mutex<LruCache<Key, ()>>,
}

impl CacheStore {
    /// Create a storage emulator with an LRU cache of the given capacity
    /// (must be > 0).
    pub fn new(cache_capacity: usize) -> Self {
        let cap =
            NonZeroUsize::new(cache_capacity).expect("cache capacity must be greater than zero");
        Self {
            cache: DashMap::new(),
            evict_queue: Mutex::new(LruCache::new(cap)),
        }
    }

    /// Read a value. Supply a loader that produces the value after the
    /// (emulated) storage latency.
    pub fn read(&self, key: &Key) {
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
                    // TODO: emulate storage latency here

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
                        if let Some(entry) = self.cache.get(key) {
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
