use std::{borrow::Borrow, collections::HashMap, hash::Hash, hash::Hasher, num::NonZeroUsize, ptr};

#[derive(Debug, Clone, Copy)]
/// KeyRef is a reference to the key.
struct KeyRef<K> {
    k: *const K,
}

impl<K: Hash> Hash for KeyRef<K> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        unsafe {
            let key = &*self.k;
            key.hash(state)
        }
    }
}

impl<K: PartialEq> PartialEq for KeyRef<K> {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            let key1 = &*self.k;
            let key2 = &*other.k;
            key1.eq(key2)
        }
    }
}

impl<K: Eq> Eq for KeyRef<K> {}

#[repr(transparent)]
/// KeyWrapper is a wrapper for the key.
struct KeyWrapper<K: ?Sized>(K);

impl<K: ?Sized> KeyWrapper<K> {
    /// from_ref creates a new KeyWrapper from a reference to the key.
    fn from_ref(key: &K) -> &Self {
        unsafe { &*(key as *const K as *const KeyWrapper<K>) }
    }
}

impl<K: ?Sized + Hash> Hash for KeyWrapper<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl<K: ?Sized + PartialEq> PartialEq for KeyWrapper<K> {
    #![allow(unknown_lints)]
    #[allow(clippy::unconditional_recursion)]
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<K: ?Sized + Eq> Eq for KeyWrapper<K> {}

impl<K, Q> Borrow<KeyWrapper<Q>> for KeyRef<K>
where
    K: Borrow<Q>,
    Q: ?Sized,
{
    fn borrow(&self) -> &KeyWrapper<Q> {
        unsafe {
            let key = &*self.k;
            KeyWrapper::from_ref(key.borrow())
        }
    }
}

struct Entry<K, V> {
    key: K,
    value: V,
    prev: Option<*mut Entry<K, V>>,
    next: Option<*mut Entry<K, V>>,
}

impl<K, V> Entry<K, V> {
    fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            prev: None,
            next: None,
        }
    }
}

pub struct LruCache<K, V> {
    cap: NonZeroUsize,
    map: HashMap<KeyRef<K>, Box<Entry<K, V>>>,
    head: Option<*mut Entry<K, V>>,
    tail: Option<*mut Entry<K, V>>,
    _marker: std::marker::PhantomData<K>,
}

impl<K: Hash + Eq, V> LruCache<K, V> {
    /// new creates a new LruCache.
    pub fn new(cap: NonZeroUsize) -> Self {
        Self {
            cap,
            map: HashMap::with_capacity(cap.get()),
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

            unsafe {
                self.detach(entry_ptr);
                self.attach(entry_ptr);
            }

            Some(&unsafe { &*entry_ptr }.value)
        } else {
            None
        }
    }

    /// put puts the key and value into the cache.
    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        let key_ref = KeyRef { k: &key };

        if let Some(existing_entry) = self.map.get_mut(&key_ref) {
            let entry_ptr = existing_entry.as_mut() as *mut Entry<K, V>;

            let old_value = unsafe {
                let old_value = ptr::read(&(*entry_ptr).value);
                ptr::write(&mut (*entry_ptr).value, value);
                old_value
            };

            unsafe {
                self.detach(entry_ptr);
                self.attach(entry_ptr);
            }

            return Some(old_value);
        }

        let mut evicted_value = None;
        if self.map.len() >= self.cap.get() {
            if let Some(tail) = self.tail {
                unsafe {
                    let tail_key_ref = KeyRef { k: &(*tail).key };

                    self.detach(tail);

                    if let Some(entry) = self.map.remove(&tail_key_ref) {
                        evicted_value = Some(entry.value);
                    }
                }
            }
        }

        let new_entry = Box::new(Entry::new(key, value));
        let key_ptr = &new_entry.key as *const K;
        let entry_ptr = Box::into_raw(new_entry);

        unsafe {
            self.attach(entry_ptr);
            self.map
                .insert(KeyRef { k: key_ptr }, Box::from_raw(entry_ptr));
        }

        evicted_value
    }

    /// detach detaches the entry from the cache.
    unsafe fn detach(&mut self, entry: *mut Entry<K, V>) {
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

    /// attach attaches the entry to the cache.
    unsafe fn attach(&mut self, entry: *mut Entry<K, V>) {
        match self.head {
            Some(old_head) => {
                (*entry).next = Some(old_head);
                (*old_head).prev = Some(entry);
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

        unsafe {
            self.detach(tail);

            let tail_key_ref = KeyRef { k: &(*tail).key };
            self.map
                .remove(&tail_key_ref)
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
        let mut cache = LruCache::new(NonZeroUsize::new(3).unwrap());

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
        let mut cache = LruCache::new(NonZeroUsize::new(3).unwrap());

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
        let mut cache = LruCache::new(NonZeroUsize::new(3).unwrap());

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

        let mut cache = LruCache::new(NonZeroUsize::new(2).unwrap());
        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());

        cache.peek("key1");
        cache.put("key3".to_string(), "value3".to_string());
        assert_eq!(cache.peek("key1"), None);

        let mut cache = LruCache::new(NonZeroUsize::new(2).unwrap());
        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());

        cache.get("key1");
        cache.put("key3".to_string(), "value3".to_string());
        assert_eq!(cache.peek("key2"), None);
        assert_eq!(cache.peek("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_contains() {
        let mut cache = LruCache::new(NonZeroUsize::new(5).unwrap());

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
