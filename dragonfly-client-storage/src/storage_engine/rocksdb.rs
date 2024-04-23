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

use crate::storage_engine::{DatabaseObject, Operations, StorageEngine, Transaction};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use std::{ops::Deref, path::Path};
use tracing::info;

/// RocksdbStorageEngine is a storage engine based on rocksdb.
pub struct RocksdbStorageEngine {
    // inner is the inner rocksdb transaction db.
    inner: rocksdb::TransactionDB,
}

// RocksdbStorageEngine implements deref of the storage engine.
impl Deref for RocksdbStorageEngine {
    // Target is the inner rocksdb transaction db.
    type Target = rocksdb::TransactionDB;

    // deref returns the inner rocksdb transaction db.
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// RocksdbStorageEngine implements the storage engine of the rocksdb.
impl RocksdbStorageEngine {
    /// DEFAULT_DIR_NAME is the default directory name to store metadata.
    const DEFAULT_DIR_NAME: &'static str = "metadata";

    /// DEFAULT_MEMTABLE_MEMORY_BUDGET is the default memory budget for memtable, default is 64MB.
    const DEFAULT_MEMTABLE_MEMORY_BUDGET: usize = 64 * 1024 * 1024;

    /// DEFAULT_MAX_OPEN_FILES is the default max open files for rocksdb.
    const DEFAULT_MAX_OPEN_FILES: i32 = 10_000;

    /// DEFAULT_BLOCK_SIZE is the default block size for rocksdb.
    const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;

    /// DEFAULT_CACHE_SIZE is the default cache size for rocksdb.
    const DEFAULT_CACHE_SIZE: usize = 16 * 1024 * 1024;

    /// open opens a rocksdb storage engine with the given directory and column families.
    pub fn open(dir: &Path, cf_names: &[&str]) -> Result<Self> {
        // Initialize rocksdb options.
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.optimize_level_style_compaction(Self::DEFAULT_MEMTABLE_MEMORY_BUDGET);
        options.increase_parallelism(num_cpus::get() as i32);
        options.set_max_open_files(Self::DEFAULT_MAX_OPEN_FILES);
        // Set prefix extractor to reduce the memory usage of bloom filter and length of task id is 64.
        options.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(64));
        options.set_memtable_prefix_bloom_ratio(0.2);

        // Initialize rocksdb block based table options.
        let mut block_options = rocksdb::BlockBasedOptions::default();
        block_options.set_block_cache(&rocksdb::Cache::new_lru_cache(Self::DEFAULT_CACHE_SIZE));
        block_options.set_block_size(Self::DEFAULT_BLOCK_SIZE);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_options.set_bloom_filter(10.0, false);
        options.set_block_based_table_factory(&block_options);

        // Open rocksdb.
        let dir = dir.join(Self::DEFAULT_DIR_NAME);
        let db = rocksdb::TransactionDB::open_cf(
            &options,
            &rocksdb::TransactionDBOptions::default(),
            &dir,
            cf_names,
        )
        .or_err(ErrorType::StorageError)?;
        info!("metadata initialized directory: {:?}", dir);

        Ok(Self { inner: db })
    }
}

// RocksdbStorageEngine implements the storage engine operations.
impl Operations for RocksdbStorageEngine {
    // get gets the object by key.
    fn get<O: DatabaseObject>(&self, key: &[u8]) -> Result<Option<O>> {
        let cf = cf_handle::<O>(self)?;
        let value = self.get_cf(cf, key).or_err(ErrorType::StorageError)?;
        match value {
            Some(value) => Ok(Some(O::deserialize_from(&value)?)),
            None => Ok(None),
        }
    }

    // put puts the object by key.
    fn put<O: DatabaseObject>(&self, key: &[u8], value: &O) -> Result<()> {
        let cf = cf_handle::<O>(self)?;
        let serialized = value.serialized()?;
        self.put_cf(cf, key, serialized)
            .or_err(ErrorType::StorageError)?;
        Ok(())
    }

    // delete deletes the object by key.
    fn delete<O: DatabaseObject>(&self, key: &[u8]) -> Result<()> {
        let cf = cf_handle::<O>(self)?;
        self.delete_cf(cf, key).or_err(ErrorType::StorageError)?;
        Ok(())
    }

    // iter iterates all objects.
    fn iter<O: DatabaseObject>(&self) -> Result<impl Iterator<Item = Result<(Box<[u8]>, O)>>> {
        let cf = cf_handle::<O>(self)?;
        let iter = self.iterator_cf(cf, rocksdb::IteratorMode::Start);
        Ok(iter.map(|ele| {
            let (key, value) = ele.or_err(ErrorType::StorageError)?;
            Ok((key, O::deserialize_from(&value)?))
        }))
    }

    // prefix_iter iterates all objects with prefix.
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
}

// RocksdbStorageEngine implements the transaction of the storage engine.
impl<'db> StorageEngine<'db> for RocksdbStorageEngine {
    // Txn is the transaction type.
    type Txn = RocksdbTransaction<'db>;

    // start_transaction starts a transaction.
    fn start_transaction(&'db self) -> RocksdbTransaction<'db> {
        let txn = self.transaction();
        RocksdbTransaction { txn, db: self }
    }
}

/// RocksdbTransaction wraps a rocksdb transaction.
pub struct RocksdbTransaction<'db> {
    // txn is the inner rocksdb transaction.
    txn: rocksdb::Transaction<'db, rocksdb::TransactionDB>,

    // db is the rocksdb storage engine.
    db: &'db rocksdb::TransactionDB,
}

// RocksdbTransaction implements the transaction operations.
impl Operations for RocksdbTransaction<'_> {
    // get gets the object by key.
    fn get<O: DatabaseObject>(&self, key: &[u8]) -> Result<Option<O>> {
        let cf = cf_handle::<O>(self.db)?;
        let value = self.txn.get_cf(cf, key).or_err(ErrorType::StorageError)?;
        match value {
            Some(value) => Ok(Some(O::deserialize_from(&value)?)),
            None => Ok(None),
        }
    }

    // put puts the object by key.
    fn put<O: DatabaseObject>(&self, key: &[u8], value: &O) -> Result<()> {
        let cf = cf_handle::<O>(self.db)?;
        let serialized = value.serialized()?;
        self.txn
            .put_cf(cf, key, serialized)
            .or_err(ErrorType::StorageError)?;
        Ok(())
    }

    // delete deletes the object by key.
    fn delete<O: DatabaseObject>(&self, key: &[u8]) -> Result<()> {
        let cf = cf_handle::<O>(self.db)?;
        self.txn
            .delete_cf(cf, key)
            .or_err(ErrorType::StorageError)?;
        Ok(())
    }

    // iter iterates all objects.
    fn iter<O: DatabaseObject>(&self) -> Result<impl Iterator<Item = Result<(Box<[u8]>, O)>>> {
        let cf = cf_handle::<O>(self.db)?;
        let iter = self.txn.iterator_cf(cf, rocksdb::IteratorMode::Start);
        Ok(iter.map(|ele| {
            let (key, value) = ele.or_err(ErrorType::StorageError)?;
            Ok((key, O::deserialize_from(&value)?))
        }))
    }

    // prefix_iter iterates all objects with prefix.
    fn prefix_iter<O: DatabaseObject>(
        &self,
        prefix: &[u8],
    ) -> Result<impl Iterator<Item = Result<(Box<[u8]>, O)>>> {
        let cf = cf_handle::<O>(self.db)?;
        let iter = self.txn.prefix_iterator_cf(cf, prefix);
        Ok(iter.map(|ele| {
            let (key, value) = ele.or_err(ErrorType::StorageError)?;
            Ok((key, O::deserialize_from(&value)?))
        }))
    }
}

// RocksdbTransaction implements the transaction operations.
impl Transaction for RocksdbTransaction<'_> {
    // get_for_update gets the object for update.
    fn get_for_update<O: DatabaseObject>(&self, key: &[u8]) -> Result<Option<O>> {
        let cf = cf_handle::<O>(self.db)?;
        let value = self
            .txn
            .get_for_update_cf(cf, key, true)
            .or_err(ErrorType::StorageError)?;
        match value {
            Some(value) => Ok(Some(O::deserialize_from(&value)?)),
            None => Ok(None),
        }
    }

    // commit commits the transaction.
    fn commit(self) -> Result<()> {
        self.txn.commit().or_err(ErrorType::StorageError)?;
        Ok(())
    }

    // rollback rolls back the transaction.
    fn rollback(&self) -> Result<()> {
        self.txn.rollback().or_err(ErrorType::StorageError)?;
        Ok(())
    }
}

/// cf_handle returns the column family handle for the given object.
fn cf_handle<T>(db: &rocksdb::TransactionDB) -> Result<&rocksdb::ColumnFamily>
where
    T: DatabaseObject,
{
    let cf_name = T::NAMESPACE;
    db.cf_handle(cf_name)
        .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.to_string()))
}
