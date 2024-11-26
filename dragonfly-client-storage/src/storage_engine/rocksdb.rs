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
use tracing::{info, instrument, warn};

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
    #[instrument(skip_all)]
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
        if keep {
            drop_unused_cfs(&dir);
        } else {
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
    #[instrument(skip_all)]
    fn get<O: DatabaseObject>(&self, key: &[u8]) -> Result<Option<O>> {
        let cf = cf_handle::<O>(self)?;
        let value = self.get_cf(cf, key).or_err(ErrorType::StorageError)?;
        match value {
            Some(value) => Ok(Some(O::deserialize_from(&value)?)),
            None => Ok(None),
        }
    }

    /// is_exist checks if the object exists by key.
    #[instrument(skip_all)]
    fn is_exist<O: DatabaseObject>(&self, key: &[u8]) -> Result<bool> {
        let cf = cf_handle::<O>(self)?;
        Ok(self
            .get_cf(cf, key)
            .or_err(ErrorType::StorageError)?
            .is_some())
    }

    /// put puts the object by key.
    #[instrument(skip_all)]
    fn put<O: DatabaseObject>(&self, key: &[u8], value: &O) -> Result<()> {
        let cf = cf_handle::<O>(self)?;
        self.put_cf(cf, key, value.serialized()?)
            .or_err(ErrorType::StorageError)?;
        Ok(())
    }

    /// delete deletes the object by key.
    #[instrument(skip_all)]
    fn delete<O: DatabaseObject>(&self, key: &[u8]) -> Result<()> {
        let cf = cf_handle::<O>(self)?;
        let mut options = WriteOptions::default();
        options.set_sync(true);

        self.delete_cf_opt(cf, key, &options)
            .or_err(ErrorType::StorageError)?;
        Ok(())
    }

    /// iter iterates all objects.
    #[instrument(skip_all)]
    fn iter<O: DatabaseObject>(&self) -> Result<impl Iterator<Item = Result<(Box<[u8]>, O)>>> {
        let cf = cf_handle::<O>(self)?;
        let iter = self.iterator_cf(cf, rocksdb::IteratorMode::Start);
        Ok(iter.map(|ele| {
            let (key, value) = ele.or_err(ErrorType::StorageError)?;
            Ok((key, O::deserialize_from(&value)?))
        }))
    }

    /// iter_raw iterates all objects without serialization.
    #[instrument(skip_all)]
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
    #[instrument(skip_all)]
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
    #[instrument(skip_all)]
    fn prefix_iter_raw<O: DatabaseObject>(
        &self,
        prefix: &[u8],
    ) -> Result<impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>)>>> {
        let cf = cf_handle::<O>(self)?;
        Ok(self.prefix_iterator_cf(cf, prefix).map(|ele| {
            let (key, value) = ele.or_err(ErrorType::StorageError)?;
            Ok((key, value))
        }))
    }

    /// batch_delete deletes objects by keys.
    #[instrument(skip_all)]
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
impl<'db> StorageEngine<'db> for RocksdbStorageEngine {}

/// cf_handle returns the column family handle for the given object.
fn cf_handle<T>(db: &rocksdb::DB) -> Result<&rocksdb::ColumnFamily>
where
    T: DatabaseObject,
{
    let cf_name = T::NAMESPACE;
    db.cf_handle(cf_name)
        .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.to_string()))
}

/// drop_unused_cfs drops the unused column families.
fn drop_unused_cfs(dir: &Path) {
    let old_cf_names = vec!["task", "piece", "cache_task"];
    let unused_cf_names = vec!["cache_task"];

    let mut db = match rocksdb::DB::open_cf(&rocksdb::Options::default(), dir, old_cf_names) {
        Ok(db) => db,
        Err(err) => {
            warn!("open cf failed: {}", err);
            return;
        }
    };

    for cf_name in unused_cf_names {
        match db.drop_cf(cf_name) {
            Ok(_) => info!("drop cf [{}] success", cf_name),
            Err(err) => warn!("drop cf [{}] failed: {}", cf_name, err),
        }
    }
}
