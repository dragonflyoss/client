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
            map: HashMap::with_capacity(capacity),
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
    use bytes::Bytes;

    #[test]
    fn test_put() {
        let mut cache = LruCache::new(3);

        assert_eq!(cache.put("key1".to_string(), "value1".to_string()), None);
        assert_eq!(cache.put("key2".to_string(), "value2".to_string()), None);
        assert_eq!(cache.put("key3".to_string(), "value3".to_string()), None);

        assert_eq!(
            cache.put("key2".to_string(), "value2_updated".to_string()),
            Some("value2".to_string())
        );

        assert_eq!(cache.get("key2"), Some(&"value2_updated".to_string()));

        assert_eq!(
            cache.put("key4".to_string(), "value4".to_string()),
            Some("value1".to_string())
        );

        assert_eq!(cache.get("key1"), None);

        assert!(cache.contains("key2"));
        assert!(cache.contains("key3"));
        assert!(cache.contains("key4"));
    }

    #[test]
    fn test_get() {
        let mut cache = LruCache::new(3);

        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());
        cache.put("key3".to_string(), "value3".to_string());

        assert_eq!(cache.get("key1"), Some(&"value1".to_string()));
        assert_eq!(cache.get("key2"), Some(&"value2".to_string()));
        assert_eq!(cache.get("key3"), Some(&"value3".to_string()));

        assert_eq!(cache.get("key4"), None);

        cache.put("key4".to_string(), "value4".to_string());

        assert_eq!(cache.get("key1"), None);
        assert_eq!(cache.get("key2"), Some(&"value2".to_string()));
        assert_eq!(cache.get("key3"), Some(&"value3".to_string()));
        assert_eq!(cache.get("key4"), Some(&"value4".to_string()));
    }

    #[test]
    fn test_peek() {
        let mut cache = LruCache::new(3);

        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());
        cache.put("key3".to_string(), "value3".to_string());

        assert_eq!(cache.peek("key1"), Some(&"value1".to_string()));
        assert_eq!(cache.peek("key2"), Some(&"value2".to_string()));
        assert_eq!(cache.peek("key3"), Some(&"value3".to_string()));

        assert_eq!(cache.peek("key4"), None);

        cache.put("key4".to_string(), "value4".to_string());

        assert_eq!(cache.peek("key1"), None);
        assert_eq!(cache.peek("key2"), Some(&"value2".to_string()));
        assert_eq!(cache.peek("key3"), Some(&"value3".to_string()));
        assert_eq!(cache.peek("key4"), Some(&"value4".to_string()));

        let mut cache = LruCache::new(2);
        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());

        cache.peek("key1");
        cache.put("key3".to_string(), "value3".to_string());
        assert_eq!(cache.peek("key1"), None);

        let mut cache = LruCache::new(2);
        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());

        cache.get("key1");
        cache.put("key3".to_string(), "value3".to_string());
        assert_eq!(cache.peek("key2"), None);
        assert_eq!(cache.peek("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_contains() {
        let mut cache = LruCache::new(5);

        let test_cases = vec![
            ("piece_1", Bytes::from("data 1"), false),
            ("piece_2", Bytes::from("data 2"), true),
            ("piece_3", Bytes::from("data 3"), false),
            ("piece_4", Bytes::from("data 4"), true),
            ("piece_5", Bytes::from("data 5"), true),
            ("piece_6", Bytes::from("data 6"), true),
            ("piece_7", Bytes::from("data 7"), true),
        ];

        for (piece_id, piece_content, _) in test_cases[0..6].iter() {
            cache.put(piece_id.to_string(), piece_content.clone());
        }

        let _ = cache.get("piece_2");

        let (piece_id, piece_content, _) = &test_cases[6];
        cache.put(piece_id.to_string(), piece_content.clone());

        for (piece_id, _, expected_existence) in test_cases {
            let exists = cache.contains(piece_id);
            assert_eq!(exists, expected_existence);
        }
    }
}
