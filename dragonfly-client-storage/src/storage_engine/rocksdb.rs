/*
 *     Copyright 2024 The Dragonfly Authors
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

use crate::storage_engine::{DatabaseObject, Operations, StorageEngine};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use rocksdb::WriteOptions;
use std::{
    ops::Deref,
    path::{Path, PathBuf},
};
use tracing::{info, warn};

/// RocksdbStorageEngine is a storage engine based on rocksdb.
pub struct RocksdbStorageEngine {
    // inner is the inner rocksdb DB.
    inner: rocksdb::DB,
}

/// RocksdbStorageEngine implements deref of the storage engine.
impl Deref for RocksdbStorageEngine {
    /// Target is the inner rocksdb DB.
    type Target = rocksdb::DB;

    /// deref returns the inner rocksdb DB.
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// RocksdbStorageEngine implements the storage engine of the rocksdb.
impl RocksdbStorageEngine {
    /// DEFAULT_DIR_NAME is the default directory name to store metadata.
    const DEFAULT_DIR_NAME: &'static str = "metadata";

    /// DEFAULT_MEMTABLE_MEMORY_BUDGET is the default memory budget for memtable, default is 512MB.
    const DEFAULT_MEMTABLE_MEMORY_BUDGET: usize = 512 * 1024 * 1024;

    // DEFAULT_MAX_BACKGROUND_JOBS is the default max background jobs for rocksdb, default is 2.
    const DEFAULT_MAX_BACKGROUND_JOBS: i32 = 2;

    /// DEFAULT_BLOCK_SIZE is the default block size for rocksdb, default is 64KB.
    const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;

    /// DEFAULT_CACHE_SIZE is the default cache size for rocksdb, default is 1GB.
    const DEFAULT_CACHE_SIZE: usize = 1024 * 1024 * 1024;

    /// DEFAULT_LOG_MAX_SIZE is the default max log size for rocksdb, default is 64MB.
    const DEFAULT_LOG_MAX_SIZE: usize = 64 * 1024 * 1024;

    /// DEFAULT_LOG_MAX_FILES is the default max log files for rocksdb.
    const DEFAULT_LOG_MAX_FILES: usize = 10;

    /// open opens a rocksdb storage engine with the given directory and column families.
    pub fn open(dir: &Path, log_dir: &PathBuf, cf_names: &[&str], keep: bool) -> Result<Self> {
        info!("initializing metadata directory: {:?} {:?}", dir, cf_names);
        // Initialize rocksdb options.
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Optimize compression.
        options.set_compression_type(rocksdb::DBCompressionType::Lz4);
        options.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);

        // Improved parallelism.
        options.increase_parallelism(num_cpus::get() as i32);
        options.set_max_background_jobs(std::cmp::max(
            num_cpus::get() as i32,
            Self::DEFAULT_MAX_BACKGROUND_JOBS,
        ));

        // Set rocksdb log options.
        options.set_db_log_dir(log_dir);
        options.set_log_level(rocksdb::LogLevel::Info);
        options.set_max_log_file_size(Self::DEFAULT_LOG_MAX_SIZE);
        options.set_keep_log_file_num(Self::DEFAULT_LOG_MAX_FILES);

        // Initialize rocksdb block based table options.
        let mut block_options = rocksdb::BlockBasedOptions::default();
        block_options.set_block_cache(&rocksdb::Cache::new_lru_cache(Self::DEFAULT_CACHE_SIZE));
        block_options.set_block_size(Self::DEFAULT_BLOCK_SIZE);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
        options.set_block_based_table_factory(&block_options);

        // Initialize column family options.
        let mut cf_options = rocksdb::Options::default();
        // TODO
        cf_options.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(64));
        cf_options.set_memtable_prefix_bloom_ratio(0.25);
        cf_options.optimize_level_style_compaction(Self::DEFAULT_MEMTABLE_MEMORY_BUDGET);

        // Initialize column families.
        let cfs = cf_names
            .iter()
            .map(|name| (name.to_string(), cf_options.clone()))
            .collect::<Vec<_>>();

        // Initialize rocksdb directory.
        let dir = dir.join(Self::DEFAULT_DIR_NAME);

        // If the storage is kept, open the db and drop the unused column families.
        // Otherwise, destroy the db.
        if !keep {
            rocksdb::DB::destroy(&options, &dir).unwrap_or_else(|err| {
                warn!("destroy {:?} failed: {}", dir, err);
            });
        }

        // Open rocksdb.
        let db =
            rocksdb::DB::open_cf_with_opts(&options, &dir, cfs).or_err(ErrorType::StorageError)?;
        info!("metadata initialized directory: {:?}", dir);

        Ok(Self { inner: db })
    }
}

/// RocksdbStorageEngine implements the storage engine operations.
impl Operations for RocksdbStorageEngine {
    /// get gets the object by key.
    fn get<O: DatabaseObject>(&self, key: &[u8]) -> Result<Option<O>> {
        let cf = cf_handle::<O>(self)?;
        let value = self.get_cf(cf, key).or_err(ErrorType::StorageError)?;
        match value {
            Some(value) => Ok(Some(O::deserialize_from(&value)?)),
            None => Ok(None),
        }
    }

    /// is_exist checks if the object exists by key.
    fn is_exist<O: DatabaseObject>(&self, key: &[u8]) -> Result<bool> {
        let cf = cf_handle::<O>(self)?;
        Ok(self
            .get_cf(cf, key)
            .or_err(ErrorType::StorageError)?
            .is_some())
    }

    /// put puts the object by key.
    fn put<O: DatabaseObject>(&self, key: &[u8], value: &O) -> Result<()> {
        let cf = cf_handle::<O>(self)?;
        self.put_cf(cf, key, value.serialized()?)
            .or_err(ErrorType::StorageError)?;
        Ok(())
    }

    /// delete deletes the object by key.
    fn delete<O: DatabaseObject>(&self, key: &[u8]) -> Result<()> {
        let cf = cf_handle::<O>(self)?;
        let mut options = WriteOptions::default();
        options.set_sync(true);

        self.delete_cf_opt(cf, key, &options)
            .or_err(ErrorType::StorageError)?;
        Ok(())
    }

    /// iter iterates all objects.
    fn iter<O: DatabaseObject>(&self) -> Result<impl Iterator<Item = Result<(Box<[u8]>, O)>>> {
        let cf = cf_handle::<O>(self)?;
        let iter = self.iterator_cf(cf, rocksdb::IteratorMode::Start);
        Ok(iter.map(|ele| {
            let (key, value) = ele.or_err(ErrorType::StorageError)?;
            Ok((key, O::deserialize_from(&value)?))
        }))
    }

    /// iter_raw iterates all objects without serialization.
    fn iter_raw<O: DatabaseObject>(
        &self,
    ) -> Result<impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>)>>> {
        let cf = cf_handle::<O>(self)?;
        Ok(self
            .iterator_cf(cf, rocksdb::IteratorMode::Start)
            .map(|ele| {
                let (key, value) = ele.or_err(ErrorType::StorageError)?;
                Ok((key, value))
            }))
    }

    /// prefix_iter iterates all objects with prefix.
    fn prefix_iter<O: DatabaseObject>(
        &self,
        prefix: &[u8],
    ) -> Result<impl Iterator<Item = Result<(Box<[u8]>, O)>>> {
        let cf = cf_handle::<O>(self)?;
        let iter = self.prefix_iterator_cf(cf, prefix);
        Ok(iter.map(|ele| {
            let (key, value) = ele.or_err(ErrorType::StorageError)?;
            Ok((key, O::deserialize_from(&value)?))
        }))
    }

    /// prefix_iter_raw iterates all objects with prefix without serialization.
    fn prefix_iter_raw<O: DatabaseObject>(
        &self,
        prefix: &[u8],
    ) -> Result<impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>)>>> {
        let cf = cf_handle::<O>(self)?;
        // prefix should not shorter than `set_prefix_extractor`
        // assert!(prefix.len() >= 64);
        Ok(self.prefix_iterator_cf(cf, prefix).map(|ele| {
            let (key, value) = ele.or_err(ErrorType::StorageError)?;
            Ok((key, value))
        }))
    }

    /// batch_delete deletes objects by keys.
    fn batch_delete<O: DatabaseObject>(&self, keys: Vec<&[u8]>) -> Result<()> {
        let cf = cf_handle::<O>(self)?;
        let mut batch = rocksdb::WriteBatch::default();
        for key in keys {
            batch.delete_cf(cf, key);
        }

        let mut options = WriteOptions::default();
        options.set_sync(true);
        Ok(self
            .write_opt(batch, &options)
            .or_err(ErrorType::StorageError)?)
    }
}

/// RocksdbStorageEngine implements the rocksdb of the storage engine.
impl StorageEngine<'_> for RocksdbStorageEngine {}

/// cf_handle returns the column family handle for the given object.
fn cf_handle<T>(db: &rocksdb::DB) -> Result<&rocksdb::ColumnFamily>
where
    T: DatabaseObject,
{
    let cf_name = T::NAMESPACE;
    db.cf_handle(cf_name)
        .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
    struct Object {
        id: String,
        value: i32,
    }

    impl DatabaseObject for Object {
        const NAMESPACE: &'static str = "object";
    }

    fn create_test_engine() -> RocksdbStorageEngine {
        let temp_dir = tempdir().unwrap();
        let log_dir = temp_dir.path().to_path_buf();
        RocksdbStorageEngine::open(temp_dir.path(), &log_dir, &[Object::NAMESPACE], false).unwrap()
    }

    #[test]
    fn test_put_and_get() {
        let engine = create_test_engine();

        let object = Object {
            id: "1".to_string(),
            value: 42,
        };

        engine.put::<Object>(object.id.as_bytes(), &object).unwrap();
        let retrieved_object = engine.get::<Object>(object.id.as_bytes()).unwrap().unwrap();
        assert_eq!(object, retrieved_object);
    }

    #[test]
    fn test_is_exist() {
        let engine = create_test_engine();

        let object = Object {
            id: "2".to_string(),
            value: 100,
        };

        assert!(!engine.is_exist::<Object>(object.id.as_bytes()).unwrap());
        engine.put::<Object>(object.id.as_bytes(), &object).unwrap();
        assert!(engine.is_exist::<Object>(object.id.as_bytes()).unwrap());
    }

    #[test]
    fn test_delete() {
        let engine = create_test_engine();

        let object = Object {
            id: "3".to_string(),
            value: 200,
        };

        engine.put::<Object>(object.id.as_bytes(), &object).unwrap();
        assert!(engine.is_exist::<Object>(object.id.as_bytes()).unwrap());

        engine.delete::<Object>(object.id.as_bytes()).unwrap();
        assert!(!engine.is_exist::<Object>(object.id.as_bytes()).unwrap());
    }

    #[test]
    fn test_batch_delete() {
        let engine = create_test_engine();

        let objects = vec![
            Object {
                id: "1".to_string(),
                value: 1,
            },
            Object {
                id: "2".to_string(),
                value: 2,
            },
            Object {
                id: "3".to_string(),
                value: 3,
            },
        ];

        for object in &objects {
            engine.put::<Object>(object.id.as_bytes(), object).unwrap();
            assert!(engine.is_exist::<Object>(object.id.as_bytes()).unwrap());
        }

        let ids: Vec<&[u8]> = objects.iter().map(|object| object.id.as_bytes()).collect();
        engine.batch_delete::<Object>(ids).unwrap();

        for object in &objects {
            assert!(!engine.is_exist::<Object>(object.id.as_bytes()).unwrap());
        }
    }

    #[test]
    fn test_iter() {
        let engine = create_test_engine();

        let objects = vec![
            Object {
                id: "1".to_string(),
                value: 10,
            },
            Object {
                id: "2".to_string(),
                value: 20,
            },
            Object {
                id: "3".to_string(),
                value: 30,
            },
        ];

        for object in &objects {
            engine.put::<Object>(object.id.as_bytes(), object).unwrap();
        }

        let retrieved_objects = engine
            .iter::<Object>()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(retrieved_objects.len(), objects.len());
        for object in &objects {
            let found = retrieved_objects
                .iter()
                .any(|(_, v)| v.id == object.id && v.value == object.value);
            assert!(found, "could not find object with id {:?}", object.id);
        }
    }

    #[test]
    fn test_prefix_iter() {
        let engine = create_test_engine();

        // RocksDB prefix extractor is configured with fixed_prefix(64) in the open method.
        let prefix_a = [b'a'; 64];
        let prefix_b = [b'b'; 64];

        // Create test keys with 64-byte identical prefixes.
        let key_a1 = [&prefix_a[..], b"_suffix1"].concat();
        let key_a2 = [&prefix_a[..], b"_suffix2"].concat();

        let key_b1 = [&prefix_b[..], b"_suffix1"].concat();
        let key_b2 = [&prefix_b[..], b"_suffix2"].concat();

        let objects_with_prefix_a = vec![
            (
                key_a1.clone(),
                Object {
                    id: "prefix_id_a1".to_string(),
                    value: 100,
                },
            ),
            (
                key_a2.clone(),
                Object {
                    id: "prefix_id_a2".to_string(),
                    value: 200,
                },
            ),
        ];

        let objects_with_prefix_b = vec![
            (
                key_b1.clone(),
                Object {
                    id: "prefix_id_b1".to_string(),
                    value: 300,
                },
            ),
            (
                key_b2.clone(),
                Object {
                    id: "prefix_id_b2".to_string(),
                    value: 400,
                },
            ),
        ];

        for (key, obj) in &objects_with_prefix_a {
            engine.put::<Object>(key, obj).unwrap();
        }

        for (key, obj) in &objects_with_prefix_b {
            engine.put::<Object>(key, obj).unwrap();
        }

        let retrieved_objects = engine
            .prefix_iter::<Object>(&prefix_a)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(
            retrieved_objects.len(),
            objects_with_prefix_a.len(),
            "expected {} objects with prefix 'a', but got {}",
            objects_with_prefix_a.len(),
            retrieved_objects.len()
        );

        // Verify each object with prefix is correctly retrieved.
        for (key, object) in &objects_with_prefix_a {
            let found = retrieved_objects
                .iter()
                .any(|(_, v)| v.id == object.id && v.value == object.value);
            assert!(found, "could not find object with key {:?}", key);
        }

        // Verify objects with different prefix are not retrieved.
        for (key, object) in &objects_with_prefix_b {
            let found = retrieved_objects
                .iter()
                .any(|(_, v)| v.id == object.id && v.value == object.value);
            assert!(!found, "found object with different prefix: {:?}", key);
        }
    }

    #[test]
    fn test_iter_raw() {
        let engine = create_test_engine();

        let objects = vec![
            Object {
                id: "1".to_string(),
                value: 10,
            },
            Object {
                id: "2".to_string(),
                value: 20,
            },
            Object {
                id: "3".to_string(),
                value: 30,
            },
        ];

        for object in &objects {
            engine.put::<Object>(object.id.as_bytes(), object).unwrap();
        }

        let retrieved_objects = engine
            .iter_raw::<Object>()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(retrieved_objects.len(), objects.len());

        // Verify each object can be deserialized from the raw bytes.
        for object in &objects {
            let found = retrieved_objects
                .iter()
                .any(|(_, v)| match Object::deserialize_from(v) {
                    Ok(deserialized) => {
                        deserialized.id == object.id && deserialized.value == object.value
                    }
                    Err(_) => false,
                });

            assert!(
                found,
                "could not find or deserialize object with key {:?}",
                object.id
            );
        }
    }

    #[test]
    fn test_prefix_iter_raw() {
        let engine = create_test_engine();

        // RocksDB prefix extractor is configured with fixed_prefix(64) in the open method.
        let prefix_a = [b'a'; 64];
        let prefix_b = [b'b'; 64];

        // Create test keys with 64-byte identical prefixes.
        let key_a1 = [&prefix_a[..], b"_raw_suffix1"].concat();
        let key_a2 = [&prefix_a[..], b"_raw_suffix2"].concat();

        let key_b1 = [&prefix_b[..], b"_raw_suffix1"].concat();
        let key_b2 = [&prefix_b[..], b"_raw_suffix2"].concat();

        let objects_with_prefix_a = vec![
            (
                key_a1.clone(),
                Object {
                    id: "raw_prefix_id_a1".to_string(),
                    value: 100,
                },
            ),
            (
                key_a2.clone(),
                Object {
                    id: "raw_prefix_id_a2".to_string(),
                    value: 200,
                },
            ),
        ];

        let objects_with_prefix_b = vec![
            (
                key_b1.clone(),
                Object {
                    id: "raw_prefix_id_b1".to_string(),
                    value: 300,
                },
            ),
            (
                key_b2.clone(),
                Object {
                    id: "raw_prefix_id_b2".to_string(),
                    value: 400,
                },
            ),
        ];

        for (key, obj) in &objects_with_prefix_a {
            engine.put::<Object>(key, obj).unwrap();
        }

        for (key, obj) in &objects_with_prefix_b {
            engine.put::<Object>(key, obj).unwrap();
        }

        let retrieved_objects = engine
            .prefix_iter_raw::<Object>(&prefix_a)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(
            retrieved_objects.len(),
            objects_with_prefix_a.len(),
            "expected {} raw objects with prefix 'a', but got {}",
            objects_with_prefix_a.len(),
            retrieved_objects.len()
        );

        // Verify each object with prefix can be deserialized from raw bytes.
        for (_, object) in &objects_with_prefix_a {
            let found = retrieved_objects
                .iter()
                .any(|(_, v)| match Object::deserialize_from(v) {
                    Ok(deserialized) => {
                        deserialized.id == object.id && deserialized.value == object.value
                    }
                    Err(_) => false,
                });

            assert!(
                found,
                "could not find or deserialize object with key {:?}",
                object.id
            );
        }

        // Verify objects with different prefix are not retrieved.
        for (key, _) in &objects_with_prefix_b {
            let found = retrieved_objects
                .iter()
                .any(|(k, _)| k.as_ref() == key.as_slice());
            assert!(!found, "found object with different prefix: {:?}", key);
        }
    }

    #[test]
    fn test_column_family_not_found() {
        let engine = create_test_engine();

        // Define a new type with a different namespace that hasn't been registered.
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct UnregisteredObject {
            data: String,
        }

        impl DatabaseObject for UnregisteredObject {
            const NAMESPACE: &'static str = "unregistered";
        }

        let key = b"unregistered";
        let result = engine.get::<UnregisteredObject>(key);

        assert!(result.is_err());
        if let Err(err) = result {
            assert!(format!("{:?}", err).contains("ColumnFamilyNotFound"));
        }
    }

    #[test]
    /// copied from `test_prefix_iter_raw`
    fn test_prefix_iter_raw_shorter_key_should_fail() {
        let engine = create_test_engine();

        // RocksDB prefix extractor is configured with fixed_prefix(64) in the open method.
        let prefix_a = [b'a'; 64];
        let prefix_b = [b'b'; 64];


        // ADD shorter key test
        let prefix_a_shorter: [u8; 10] = prefix_a[..10].try_into().unwrap();

        println!("prefix_a address: {:p}", &prefix_a);
        println!("prefix_a_shorter address: {:p}", &prefix_a_shorter);
        println!("shoter(len: {}): {:#?}",prefix_a_shorter.len(), prefix_a_shorter);
        // ADD shorter key test

        // Create test keys with 64-byte identical prefixes.
        let key_a1 = [&prefix_a[..], b"_raw_suffix1"].concat();
        let key_a2 = [&prefix_a[..], b"_raw_suffix2"].concat();

        let key_b1 = [&prefix_b[..], b"_raw_suffix1"].concat();
        let key_b2 = [&prefix_b[..], b"_raw_suffix2"].concat();

        let objects_with_prefix_a = vec![
            (
                key_a1.clone(),
                Object {
                    id: "raw_prefix_id_a1".to_string(),
                    value: 100,
                },
            ),
            (
                key_a2.clone(),
                Object {
                    id: "raw_prefix_id_a2".to_string(),
                    value: 200,
                },
            ),
        ];

        let objects_with_prefix_b = vec![
            (
                key_b1.clone(),
                Object {
                    id: "raw_prefix_id_b1".to_string(),
                    value: 300,
                },
            ),
            (
                key_b2.clone(),
                Object {
                    id: "raw_prefix_id_b2".to_string(),
                    value: 400,
                },
            ),
        ];

        for (key, obj) in &objects_with_prefix_a {
            engine.put::<Object>(key, obj).unwrap();
        }

        for (key, obj) in &objects_with_prefix_b {
            engine.put::<Object>(key, obj).unwrap();
        }

        let retrieved_objects = engine
            // .prefix_iter_raw::<Object>(&prefix_a[..10])
            // .prefix_iter_raw::<Object>(&prefix_a)
            .prefix_iter_raw::<Object>(&prefix_a_shorter)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        // Can not seek value
        assert_eq!(
            retrieved_objects.len(),
            objects_with_prefix_a.len(),
            "expected {} raw objects with prefix 'a', but got {}",
            objects_with_prefix_a.len(),
            retrieved_objects.len()
        );

        // println!("Retrieved objects count: {}", retrieved_objects.len());
        // for (i, (key, value)) in retrieved_objects.iter().enumerate() {
        //     println!("Object {}: key={:?}, value_len={}", i, key, value.len());
        //     if let Ok(obj) = Object::deserialize_from(value) {
        //         println!("  -> deserialized: id={}, value={}", obj.id, obj.value);
        //     } else {
        //         println!("  -> failed to deserialize");
        //     }
        // }

        // // Verify each object with prefix can be deserialized from raw bytes.
        // for (_, object) in &objects_with_prefix_a {
        //     let found = retrieved_objects
        //         .iter()
        //         .any(|(_, v)| match Object::deserialize_from(v) {
        //             Ok(deserialized) => {
        //                 deserialized.id == object.id && deserialized.value == object.value
        //             }
        //             Err(_) => false,
        //         });

        //     assert!(
        //         found,
        //         "could not find or deserialize object with key {:?}",
        //         object.id
        //     );
        // }

        // // Verify objects with different prefix are not retrieved.
        // for (key, _) in &objects_with_prefix_b {
        //     let found = retrieved_objects
        //         .iter()
        //         .any(|(k, _)| k.as_ref() == key.as_slice());
        //     assert!(!found, "found object with different prefix: {:?}", key);
        // }
    }
}
