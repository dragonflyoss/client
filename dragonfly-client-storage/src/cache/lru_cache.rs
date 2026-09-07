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

/// A reference to the key.
#[derive(Debug, Clone, Copy)]
struct KeyRef<K> {
    k: *const K,
}

/// Implements Hash for KeyRef.
impl<K: Hash> Hash for KeyRef<K> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        unsafe {
            let key = &*self.k;
            key.hash(state)
        }
    }
}

/// Implements PartialEq for KeyRef.
impl<K: PartialEq> PartialEq for KeyRef<K> {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            let key1 = &*self.k;
            let key2 = &*other.k;
            key1.eq(key2)
        }
    }
}

/// Implements Eq for KeyRef.
impl<K: Eq> Eq for KeyRef<K> {}

/// A wrapper for the key.
#[repr(transparent)]
struct KeyWrapper<K: ?Sized>(K);

/// Implements reference conversion.
impl<K: ?Sized> KeyWrapper<K> {
    /// Creates a new KeyWrapper from a reference to the key.
    fn from_ref(key: &K) -> &Self {
        unsafe { &*(key as *const K as *const KeyWrapper<K>) }
    }
}

/// Implements Hash for KeyWrapper.
impl<K: ?Sized + Hash> Hash for KeyWrapper<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

/// Implements PartialEq for KeyWrapper.
impl<K: ?Sized + PartialEq> PartialEq for KeyWrapper<K> {
    #![allow(unknown_lints)]
    #[allow(clippy::unconditional_recursion)]
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

/// Implements Eq for KeyWrapper.
impl<K: ?Sized + Eq> Eq for KeyWrapper<K> {}

/// Implements Borrow for KeyWrapper.
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

/// A cache entry.
struct Entry<K, V> {
    key: K,
    value: V,
    prev: Option<*mut Entry<K, V>>,
    next: Option<*mut Entry<K, V>>,
}

/// Implements Drop for Entry.
impl<K, V> Entry<K, V> {
    /// Creates a new Entry.
    fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            prev: None,
            next: None,
        }
    }
}

/// A least recently used cache.
pub struct LruCache<K, V> {
    capacity: usize,
    map: HashMap<KeyRef<K>, Box<Entry<K, V>>>,
    head: Option<*mut Entry<K, V>>,
    tail: Option<*mut Entry<K, V>>,
    _marker: std::marker::PhantomData<K>,
}

/// Implements LruCache.
impl<K: Hash + Eq, V> LruCache<K, V> {
    /// Creates a new LruCache.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::new(),
            head: None,
            tail: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Gets the value of the key.
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

    /// Puts the key and value into the cache.
    pub fn put(&mut self, key: K, mut value: V) -> Option<V> {
        if let Some(existing_entry) = self.map.get_mut(KeyWrapper::from_ref(&key)) {
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
                    if let Some(entry) = self.map.remove(KeyWrapper::from_ref(&(*tail).key)) {
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

    /// Checks whether the key exists in the cache.
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

    /// Pops the least recently used value from the cache.
    pub fn pop_lru(&mut self) -> Option<(K, V)> {
        if self.is_empty() {
            return None;
        }

        let tail = self.tail?;
        self.detach(tail);

        unsafe {
            self.map
                .remove(KeyWrapper::from_ref(&(*tail).key))
                .map(|entry| (entry.key, entry.value))
        }
    }

    /// Removes and returns the value for a given key, if it does not exist, it returns None.
    pub fn pop<Q>(&mut self, k: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.map.remove(KeyWrapper::from_ref(k)) {
            None => None,
            Some(entry) => {
                let entry_ptr = Box::into_raw(entry);
                self.detach(entry_ptr);

                unsafe {
                    let entry = Box::from_raw(entry_ptr);
                    Some((entry.key, entry.value))
                }
            }
        }
    }

    /// Checks whether the cache is empty.
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

    type Access = fn(&mut LruCache<String, i32>, &str);
    type Entries = Vec<(&'static str, Option<i32>)>;

    fn cache(capacity: usize, entries: &[(&str, i32)]) -> LruCache<String, i32> {
        let mut cache = LruCache::new(capacity);
        for (key, value) in entries {
            cache.put(key.to_string(), *value);
        }
        cache
    }

    #[test]
    fn new_sets_capacity_and_starts_empty() {
        let test_cases = vec![5, 1, 0, usize::MAX];

        for capacity in test_cases {
            let cache: LruCache<String, i32> = LruCache::new(capacity);
            assert_eq!(cache.capacity, capacity);
            assert!(cache.is_empty());
        }
    }

    #[test]
    fn put_returns_replaced_or_evicted_value() {
        let test_cases = vec![
            ("key1", 1, None),
            ("key2", 2, None),
            ("key3", 3, None),
            ("key2", 22, Some(2)),
            ("key4", 4, Some(1)),
            ("key5", 5, Some(3)),
            ("key4", 44, Some(4)),
        ];

        let mut cache = cache(3, &[]);
        for (key, value, expected) in test_cases {
            let replaced = cache.put(key.to_string(), value);
            assert_eq!(replaced, expected);
        }

        let expected_entries = vec![
            ("key1", None),
            ("key2", Some(22)),
            ("key3", None),
            ("key4", Some(44)),
            ("key5", Some(5)),
        ];
        for (key, expected) in expected_entries {
            assert_eq!(cache.peek(key).copied(), expected);
        }
    }

    #[test]
    fn get_peek_and_contains_find_only_present_keys() {
        let test_cases = vec![
            (vec![], "key1", None),
            (
                vec![("key1", 1), ("key2", 2), ("key3", 3)],
                "nonexistent",
                None,
            ),
            (vec![("key1", 1), ("key2", 2), ("key3", 3)], "key1", Some(1)),
            (vec![("key1", 1), ("key2", 2), ("key3", 3)], "key2", Some(2)),
            (vec![("key1", 1), ("key2", 2), ("key3", 3)], "key3", Some(3)),
        ];

        for (entries, key, expected) in test_cases {
            let mut cache = cache(3, &entries);
            assert_eq!(cache.peek(key).copied(), expected);
            assert_eq!(cache.contains(key), expected.is_some());
            assert_eq!(cache.get(key).copied(), expected);
        }
    }

    #[test]
    fn get_and_put_promote_key_peek_and_contains_do_not() {
        let test_cases: Vec<(Access, Entries)> = vec![
            (
                |cache, key| {
                    cache.get(key);
                },
                vec![
                    ("key1", Some(1)),
                    ("key2", None),
                    ("key3", Some(3)),
                    ("key4", Some(4)),
                ],
            ),
            (
                |cache, key| {
                    cache.put(key.to_string(), 11);
                },
                vec![
                    ("key1", Some(11)),
                    ("key2", None),
                    ("key3", Some(3)),
                    ("key4", Some(4)),
                ],
            ),
            (
                |cache, key| {
                    cache.peek(key);
                },
                vec![
                    ("key1", None),
                    ("key2", Some(2)),
                    ("key3", Some(3)),
                    ("key4", Some(4)),
                ],
            ),
            (
                |cache, key| {
                    cache.contains(key);
                },
                vec![
                    ("key1", None),
                    ("key2", Some(2)),
                    ("key3", Some(3)),
                    ("key4", Some(4)),
                ],
            ),
        ];

        for (access, expected_entries) in test_cases {
            let mut cache = cache(3, &[("key1", 1), ("key2", 2), ("key3", 3)]);
            access(&mut cache, "key1");
            cache.put("key4".to_string(), 4);

            for (key, expected) in expected_entries {
                assert_eq!(cache.peek(key).copied(), expected);
            }
        }
    }

    #[test]
    fn pop_lru_drains_entries_from_least_recently_used() {
        let test_cases = vec![
            (vec![], vec![]),
            (
                vec![("key1", 1), ("key2", 2), ("key3", 3)],
                vec![("key1", 1), ("key2", 2), ("key3", 3)],
            ),
            (
                vec![("key1", 1), ("key2", 2), ("key1", 11)],
                vec![("key2", 2), ("key1", 11)],
            ),
        ];

        for (entries, expected_order) in test_cases {
            let mut cache = cache(3, &entries);
            for (key, value) in expected_order {
                assert_eq!(cache.pop_lru(), Some((key.to_string(), value)));
            }

            assert_eq!(cache.pop_lru(), None);
            assert!(cache.is_empty());
        }
    }

    #[test]
    fn pop_removes_only_present_keys() {
        let test_cases = vec![
            ("key3", Some(("key3".to_string(), 3))),
            ("nonexistent", None),
            ("key1", Some(("key1".to_string(), 1))),
            ("key3", None),
            ("key2", Some(("key2".to_string(), 2))),
        ];

        let mut cache = cache(3, &[("key1", 1), ("key2", 2), ("key3", 3)]);
        for (key, expected) in test_cases {
            assert_eq!(cache.pop(key), expected);
            assert!(!cache.contains(key));
        }

        assert!(cache.is_empty());
    }
}
