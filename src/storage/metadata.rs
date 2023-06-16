/*
 *     Copyright 2023 The Dragonfly Authors
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

use crate::config::NAME;
use chrono::NaiveDateTime;
use rocksdb::{BlockBasedOptions, Cache, ColumnFamily, Options, DB};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// DEFAULT_DIR_NAME is the default directory name to store metadata.
const DEFAULT_DIR_NAME: &str = "metadata";

// DEFAULT_MEMTABLE_MEMORY_BUDGET is the default memory budget for memtable.
const DEFAULT_MEMTABLE_MEMORY_BUDGET: usize = 32 * 1024 * 1024;

// DEFAULT_MAX_OPEN_FILES is the default max open files for rocksdb.
const DEFAULT_MAX_OPEN_FILES: i32 = 10_000;

// DEFAULT_BLOCK_SIZE is the default block size for rocksdb.
const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;

// DEFAULT_CACHE_SIZE is the default cache size for rocksdb.
const DEFAULT_CACHE_SIZE: usize = 16 * 1024 * 1024;

// TASK_CF_NAME is the column family name of task.
const TASK_CF_NAME: &str = "task";

// PIECE_CF_NAME is the column family name of piece.
const PIECE_CF_NAME: &str = "piece";

// Task is the metadata of the task.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Task {
    // id is the task id.
    id: String,

    // piece_length is the length of the piece.
    piece_length: u64,

    // updated_at is the time when the task metadata is updated. If the task is downloaded
    // by other peers, it will also update updated_at.
    updated_at: NaiveDateTime,

    // created_at is the time when the task metadata is created.
    created_at: NaiveDateTime,
}

// PieceState is the state of the piece.
#[derive(Debug, Clone, Default, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum PieceState {
    // Running is the state of the piece which is running,
    // piece is being downloaded from source or other peers.
    #[default]
    Running,

    // Succeeded is the state of the piece which is succeeded, piece downloaded successfully.
    Succeeded,

    // Failed is the state of the piece which is failed. When the enableBackToSource is true in dfdaemon configuration,
    // it means that peer back-to-source downloads failed, and when the enableBackToSource is false
    // in dfdaemon configuration, it means that piece downloads from other peers failed.
    Failed,
}

// Piece is the metadata of the piece.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Piece {
    // number is the piece number.
    number: u32,

    // offset is the offset of the piece in the task.
    offset: u64,

    // length is the length of the piece.
    length: u64,

    // digest is the digest of the piece.
    digest: String,

    // state is the state of the piece.
    state: PieceState,

    // created_at is the time when the piece metadata is created.
    created_at: NaiveDateTime,
}

// Error is the error for Metadata.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // RocksDB is the error for rocksdb.
    #[error(transparent)]
    RocksDB(#[from] rocksdb::Error),

    // JSON is the error for serde_json.
    #[error(transparent)]
    JSON(#[from] serde_json::Error),

    // Other is the other error.
    #[error("{0}")]
    Other(String),
}

// Result is the result for Metadata.
pub type Result<T> = std::result::Result<T, Error>;

// Metadata is the metadata of the task.
pub struct Metadata {
    // db is the rocksdb instance.
    db: DB,
}

impl Metadata {
    // NewMetadata returns a new metadata.
    pub fn new(data_dir: PathBuf) -> Result<Metadata> {
        // Initialize rocksdb options.
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.optimize_level_style_compaction(DEFAULT_MEMTABLE_MEMORY_BUDGET);
        options.increase_parallelism(num_cpus::get() as i32);
        options.set_max_open_files(DEFAULT_MAX_OPEN_FILES);

        // Initialize rocksdb block based table options.
        let mut block_options = BlockBasedOptions::default();
        block_options.set_block_cache(&Cache::new_lru_cache(DEFAULT_CACHE_SIZE));
        block_options.set_block_size(DEFAULT_BLOCK_SIZE);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_options.set_bloom_filter(10.0, false);
        options.set_block_based_table_factory(&block_options);

        // Open rocksdb.
        let db = DB::open_cf(
            &options,
            data_dir.join(NAME).join(DEFAULT_DIR_NAME),
            [TASK_CF_NAME, PIECE_CF_NAME],
        )?;

        Ok(Metadata { db })
    }

    // put_task puts the task metadata.
    pub fn put_task(self, id: &str, task: &Task) -> Result<()> {
        let handle = self.cf_handle(TASK_CF_NAME)?;
        let json = serde_json::to_string(&task)?;
        self.db.put_cf(handle, id.as_bytes(), json.as_bytes())?;
        Ok(())
    }

    // get_task gets the task metadata.
    pub fn get_task(self, id: &str) -> Result<Task> {
        let handle = self.cf_handle(TASK_CF_NAME)?;
        match self.db.get_cf(handle, id)? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Err(Error::Other(format!("task {} not found", id))),
        }
    }

    // delete_task deletes the task metadata.
    pub fn delete_task(self, id: &str) -> Result<()> {
        let handle = self.cf_handle(TASK_CF_NAME)?;
        self.db.delete_cf(handle, id)?;
        Ok(())
    }

    // put_piece puts the piece metadata.
    pub fn put_piece(self, task_id: &str, piece: &Piece) -> Result<()> {
        let handle = self.cf_handle(PIECE_CF_NAME)?;
        let json = serde_json::to_string(&piece)?;
        self.db.put_cf(
            handle,
            self.piece_id(task_id, piece.number).as_bytes(),
            json.as_bytes(),
        )?;
        Ok(())
    }

    // get_piece gets the piece metadata.
    pub fn get_piece(self, task_id: &str, number: u32) -> Result<Piece> {
        let handle = self.cf_handle(PIECE_CF_NAME)?;
        match self
            .db
            .get_cf(handle, self.piece_id(task_id, number).as_bytes())?
        {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Err(Error::Other(format!(
                "piece {} not found in task {}",
                number, task_id
            ))),
        }
    }

    // delete_piece deletes the piece metadata.
    pub fn delete_piece(self, task_id: &str, number: u32) -> Result<()> {
        let handle = self.cf_handle(PIECE_CF_NAME)?;
        self.db
            .delete_cf(handle, self.piece_id(task_id, number).as_bytes())?;
        Ok(())
    }

    // piece_id returns the piece id.
    fn piece_id(&self, task_id: &str, number: u32) -> String {
        format!("{}-{}", task_id, number)
    }

    // cf_handle returns the column family handle.
    fn cf_handle(&self, cf_name: &str) -> Result<&ColumnFamily> {
        self.db
            .cf_handle(cf_name)
            .ok_or_else(|| Error::Other(format!("column family {} not found", cf_name)))
    }
}
