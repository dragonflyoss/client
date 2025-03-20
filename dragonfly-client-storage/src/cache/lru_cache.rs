/*
 *     Copyright 2025 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::{borrow::Borrow, collections::HashMap, hash::Hash, hash::Hasher};

/// KeyRef is a reference to the key.
#[derive(Debug, Clone, Copy)]
struct KeyRef<K> {
    k: *const K,
}

/// KeyRef implements Hash for KeyRef.
impl<K: Hash> Hash for KeyRef<K> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        unsafe {
            let key = &*self.k;
            key.hash(state)
        }
    }
}

/// KeyRef implements PartialEq for KeyRef.
impl<K: PartialEq> PartialEq for KeyRef<K> {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            let key1 = &*self.k;
            let key2 = &*other.k;
            key1.eq(key2)
        }
    }
}

/// KeyRef implements Eq for KeyRef.
impl<K: Eq> Eq for KeyRef<K> {}

/// KeyWrapper is a wrapper for the key.
#[repr(transparent)]
struct KeyWrapper<K: ?Sized>(K);

/// KeyWrapper implements reference conversion.
impl<K: ?Sized> KeyWrapper<K> {
    /// from_ref creates a new KeyWrapper from a reference to the key.
    fn from_ref(key: &K) -> &Self {
        unsafe { &*(key as *const K as *const KeyWrapper<K>) }
    }
}

/// KeyWrapper implements Hash for KeyWrapper.
impl<K: ?Sized + Hash> Hash for KeyWrapper<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

/// KeyWrapper implements PartialEq for KeyWrapper.
impl<K: ?Sized + PartialEq> PartialEq for KeyWrapper<K> {
    #![allow(unknown_lints)]
    #[allow(clippy::unconditional_recursion)]
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

/// KeyWrapper implements Eq for KeyWrapper.
impl<K: ?Sized + Eq> Eq for KeyWrapper<K> {}

/// KeyWrapper implements Borrow for KeyWrapper.
impl<K, Q> Borrow<KeyWrapper<Q>> for KeyRef<K>
where
    K: Borrow<Q>,
    Q: ?Sized,
{
    /// borrow borrows the key.
    fn borrow(&self) -> &KeyWrapper<Q> {
        unsafe {
            let key = &*self.k;
            KeyWrapper::from_ref(key.borrow())
        }
    }
}

/// Entry is a cache entry.
struct Entry<K, V> {
    key: K,
    value: V,
    prev: Option<*mut Entry<K, V>>,
    next: Option<*mut Entry<K, V>>,
}

/// Entry implements Drop for Entry.
impl<K, V> Entry<K, V> {
    /// new creates a new Entry.
    fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            prev: None,
            next: None,
        }
    }
}

/// LruCache is a least recently used cache.
pub struct LruCache<K, V> {
    capacity: usize,
    map: HashMap<KeyRef<K>, Box<Entry<K, V>>>,
    head: Option<*mut Entry<K, V>>,
    tail: Option<*mut Entry<K, V>>,
    _marker: std::marker::PhantomData<K>,
}

/// LruCache implements LruCache.
impl<K: Hash + Eq, V> LruCache<K, V> {
    /// new creates a new LruCache.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::new(),
            head: None,
            tail: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// get gets the value of the key.
    pub fn get<'a, Q>(&'a mut self, k: &Q) -> Option<&'a V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some(entry) = self.map.get_mut(KeyWrapper::from_ref(k)) {
            let entry_ptr: *mut Entry<K, V> = &mut **entry;

            self.detach(entry_ptr);
            self.attach(entry_ptr);
            Some(&unsafe { &*entry_ptr }.value)
        } else {
            None
        }
    }

    /// put puts the key and value into the cache.
    pub fn put(&mut self, key: K, mut value: V) -> Option<V> {
        if let Some(existing_entry) = self.map.get_mut(&KeyRef { k: &key }) {
            let entry = existing_entry.as_mut();
            std::mem::swap(&mut entry.value, &mut value);

            let entry_ptr: *mut Entry<K, V> = entry;
            self.detach(entry_ptr);
            self.attach(entry_ptr);
            return Some(value);
        }

        let mut evicted_value = None;
        if self.map.len() >= self.capacity {
            if let Some(tail) = self.tail {
                self.detach(tail);

                unsafe {
                    if let Some(entry) = self.map.remove(&KeyRef { k: &(*tail).key }) {
                        evicted_value = Some(entry.value);
                    }
                }
            }
        }

        let new_entry = Box::new(Entry::new(key, value));
        let key_ptr: *const K = &new_entry.key;
        let entry_ptr = Box::into_raw(new_entry);

        unsafe {
            self.attach(entry_ptr);
            self.map
                .insert(KeyRef { k: key_ptr }, Box::from_raw(entry_ptr));
        }

        evicted_value
    }

    /// detach detaches the entry from the cache.
    fn detach(&mut self, entry: *mut Entry<K, V>) {
        unsafe {
            let prev = (*entry).prev;
            let next = (*entry).next;

            match prev {
                Some(prev) => (*prev).next = next,
                None => self.head = next,
            }

            match next {
                Some(next) => (*next).prev = prev,
                None => self.tail = prev,
            }

            (*entry).prev = None;
            (*entry).next = None;
        }
    }

    /// attach attaches the entry to the cache.
    fn attach(&mut self, entry: *mut Entry<K, V>) {
        match self.head {
            Some(head) => {
                unsafe {
                    (*entry).next = Some(head);
                    (*head).prev = Some(entry);
                }

                self.head = Some(entry);
            }
            None => {
                self.head = Some(entry);
                self.tail = Some(entry);
            }
        }
    }

    /// contains checks whether the key exists in the cache.
    pub fn contains<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.contains_key(KeyWrapper::from_ref(k))
    }

    /// peek peeks the value of the key. It does not move the key to the front of the cache.
    pub fn peek<'a, Q>(&'a self, k: &Q) -> Option<&'a V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map
            .get(KeyWrapper::from_ref(k))
            .map(|entry| &entry.value)
    }

    /// pop_lru pops the least recently used value from the cache.
    pub fn pop_lru(&mut self) -> Option<(K, V)> {
        if self.is_empty() {
            return None;
        }

        let tail = self.tail?;
        self.detach(tail);

        unsafe {
            self.map
                .remove(&KeyRef { k: &(*tail).key })
                .map(|entry| (entry.key, entry.value))
        }
    }

    /// is_empty checks whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

unsafe impl<K: Send, V: Send> Send for LruCache<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for LruCache<K, V> {}

impl<K, V> Drop for LruCache<K, V> {
    fn drop(&mut self) {
        self.map.clear();
        self.head = None;
        self.tail = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        // Define test cases: (capacity, expected_capacity)
        let test_cases = vec![
            // Normal capacity
            (5, 5),
            // Minimum meaningful capacity
            (1, 1),
            // Zero capacity
            (0, 0),
            // Maximum capacity
            (usize::MAX, usize::MAX),
        ];

        for (capacity, expected_capacity) in test_cases {
            let cache: LruCache<String, i32> = LruCache::new(capacity);
            assert!(cache.is_empty());
            assert_eq!(cache.capacity, expected_capacity);
        }
    }

    #[test]
    fn test_put() {
        // Test basic insertion and eviction with capacity 3
        let mut cache = LruCache::new(3);

        // Define test cases: (key, value, expected_previous_value)
        let test_cases = vec![
            // Initial insertions
            ("key1", 1, None),
            ("key2", 2, None),
            ("key3", 3, None),
            // Update existing key
            ("key2", 22, Some(2)),
            // Eviction of oldest key
            ("key4", 4, Some(1)),
        ];

        for (key, value, expected_result) in test_cases {
            let result = cache.put(key.to_string(), value);
            assert_eq!(result, expected_result);
        }

        // Verify final state
        assert_eq!(cache.get(&"key1".to_string()), None);
        assert_eq!(cache.get(&"key2".to_string()).copied(), Some(22));
        assert_eq!(cache.get(&"key3".to_string()).copied(), Some(3));
        assert_eq!(cache.get(&"key4".to_string()).copied(), Some(4));

        // Test edge case with capacity 1
        let mut cache = LruCache::new(1);

        // Define test cases: (key, value, expected_previous_value)
        let test_cases = vec![
            // Initial insertion
            ("key1", 1, None),
            // Eviction of previous key
            ("key2", 2, Some(1)),
        ];

        for (key, value, expected_result) in test_cases {
            let result = cache.put(key.to_string(), value);
            assert_eq!(result, expected_result);
        }

        // Verify final state
        assert_eq!(cache.get(&"key1".to_string()), None);
        assert_eq!(cache.get(&"key2".to_string()).copied(), Some(2));
    }

    #[test]
    fn test_get() {
        let mut cache = LruCache::new(3);

        // Test get on empty cache
        assert_eq!(cache.get(&"nonexistent".to_string()), None);

        // Prepare cache with initial values
        for (key, value) in [("key1", 1), ("key2", 2), ("key3", 3)] {
            cache.put(key.to_string(), value);
        }

        // Define test cases: (key, expected_value)
        let test_cases = vec![
            // Get first key - should move to front
            ("key1", Some(1)),
            // Get non-existent key
            ("nonexistent", None),
            // Get first key again
            ("key1", Some(1)),
            // Get third key - should move to front
            ("key3", Some(3)),
        ];

        for (key, expected_value) in test_cases {
            assert_eq!(cache.get(&key.to_string()).copied(), expected_value);
        }

        // Test eviction after getting (key1 and key3 should be at front)
        cache.put("key4".to_string(), 4);

        // Define post-eviction test cases: (key, expected_value)
        let post_eviction_tests = vec![
            // key1 should still exist
            ("key1", Some(1)),
            // key2 should be evicted
            ("key2", None),
            // key3 should still exist
            ("key3", Some(3)),
            // key4 should be added
            ("key4", Some(4)),
        ];

        for (key, expected_value) in post_eviction_tests {
            assert_eq!(cache.get(&key.to_string()).copied(), expected_value);
        }

        // Test edge case with capacity of 1
        let mut cache = LruCache::new(1);
        cache.put("key1".to_string(), 1);
        assert_eq!(cache.get(&"key1".to_string()).copied(), Some(1));
        cache.put("key2".to_string(), 2);
        assert_eq!(cache.get(&"key1".to_string()), None);
    }

    #[test]
    fn test_peek() {
        let mut cache = LruCache::new(3);

        // Prepare cache with initial values
        for (key, value) in [("key1", 1), ("key2", 2), ("key3", 3)] {
            cache.put(key.to_string(), value);
        }

        // Define test cases: (key, expected_value)
        let test_cases = vec![
            // Peek at non-existent key
            ("nonexistent", None),
            // Peek at first key - shouldn't affect LRU order
            ("key1", Some(1)),
            // Peek at second key - shouldn't affect LRU order
            ("key2", Some(2)),
            // Peek at third key - shouldn't affect LRU order
            ("key3", Some(3)),
        ];

        for (key, expected_value) in test_cases {
            assert_eq!(cache.peek(&key.to_string()).copied(), expected_value);
        }

        // Test eviction after peeking (original LRU order should be preserved)
        cache.put("key4".to_string(), 4);

        // Define post-eviction test cases: (key, expected_value)
        let post_eviction_tests = vec![
            // key1 should be evicted
            ("key1", None),
            // key2 should still exist
            ("key2", Some(2)),
            // key3 should still exist
            ("key3", Some(3)),
            // key4 should be added
            ("key4", Some(4)),
        ];

        for (key, expected_value) in post_eviction_tests {
            assert_eq!(cache.peek(&key.to_string()).copied(), expected_value);
        }

        // Test edge case with capacity of 1
        let mut cache = LruCache::new(1);
        cache.put("key1".to_string(), 1);
        assert_eq!(cache.peek(&"key1".to_string()).copied(), Some(1));
        cache.put("key2".to_string(), 2);
        assert_eq!(cache.peek(&"key1".to_string()), None);
    }

    #[test]
    fn test_contains() {
        let mut cache = LruCache::new(3);

        // Test contains on empty cache
        assert!(!cache.contains(&"nonexistent".to_string()));

        // Prepare cache with initial values
        for (key, value) in [("key1", 1), ("key2", 2), ("key3", 3)] {
            cache.put(key.to_string(), value);
        }

        // Define test cases: (key, expected_result)
        let test_cases = vec![
            // Check non-existent key
            ("nonexistent", false),
            // Check first key - shouldn't affect LRU order
            ("key1", true),
            // Check second key - shouldn't affect LRU order
            ("key2", true),
            // Check third key - shouldn't affect LRU order
            ("key3", true),
        ];

        for (key, expected_result) in test_cases {
            assert_eq!(cache.contains(&key.to_string()), expected_result);
        }

        // Test eviction after contains (original LRU order should be preserved)
        cache.put("key4".to_string(), 4);

        // Define post-eviction test cases: (key, expected_result)
        let post_eviction_tests = vec![
            // key1 should be evicted
            ("key1", false),
            // key2 should still exist
            ("key2", true),
            // key3 should still exist
            ("key3", true),
            // key4 should be added
            ("key4", true),
        ];

        for (key, expected_result) in post_eviction_tests {
            assert_eq!(cache.contains(&key.to_string()), expected_result);
        }

        // Test edge case with capacity of 1
        let mut cache = LruCache::new(1);
        cache.put("key1".to_string(), 1);
        assert!(cache.contains(&"key1".to_string()));
        cache.put("key2".to_string(), 2);
        assert!(!cache.contains(&"key1".to_string()));
        assert!(cache.contains(&"key2".to_string()));
    }

    #[test]
    fn test_pop_lru() {
        // Test case 1: Standard pop_lru operations
        let mut cache = LruCache::new(3);

        // Test pop_lru on empty cache
        assert_eq!(cache.pop_lru(), None);

        // Fill cache and test pop operations
        // (key, value, expected_pop_result)
        let test_cases = vec![
            // Fill operations
            ("key1", 1, None),
            ("key2", 2, None),
            ("key3", 3, None),
            // Pop operations with expected results
            // Pop least recently used (key1)
            ("", 0, Some(("key1".to_string(), 1))),
            // Pop next least recently used (key2)
            ("", 0, Some(("key2".to_string(), 2))),
            // Pop last item (key3)
            ("", 0, Some(("key3".to_string(), 3))),
            // Pop from empty cache
            ("", 0, None),
        ];

        for (key, value, expected_result) in test_cases {
            if !key.is_empty() {
                // Fill operation
                cache.put(key.to_string(), value);
            } else {
                // Pop operation
                assert_eq!(cache.pop_lru(), expected_result);
            }
        }

        // Cache should be empty after all pops
        assert!(cache.is_empty());

        // Test case 2: Edge case with capacity 1
        let mut cache = LruCache::new(1);

        // (operation, key, value, expected_result)
        let test_cases = vec![
            // Put key1
            ("put", "key1", 1, None),
            // Pop the only entry
            ("pop", "", 0, Some(("key1".to_string(), 1))),
            // Pop from empty cache
            ("pop", "", 0, None),
        ];

        for (operation, key, value, expected_result) in test_cases {
            match operation {
                "put" => {
                    cache.put(key.to_string(), value);
                }
                "pop" => {
                    assert_eq!(cache.pop_lru(), expected_result);
                }
                _ => panic!("Unknown operation"),
            }
        }

        assert!(cache.is_empty());
    }
}
