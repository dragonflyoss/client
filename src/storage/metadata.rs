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

use crate::config;
use crate::{Error, Result};
use chrono::{NaiveDateTime, Utc};
use rocksdb::{BlockBasedOptions, Cache, ColumnFamily, Options, DB};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::info;

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
    pub id: String,

    // piece_length is the length of the piece.
    pub piece_length: i32,

    // uploaded_count is the count of the task uploaded by other peers.
    pub uploaded_count: u64,

    // content_length is the length of the task.
    pub content_length: i64,

    // updated_at is the time when the task metadata is updated. If the task is downloaded
    // by other peers, it will also update updated_at.
    pub updated_at: NaiveDateTime,

    // created_at is the time when the task metadata is created.
    pub created_at: NaiveDateTime,

    // finished_at is the time when the task downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

// Task implements the task metadata.
impl Task {
    // is_started returns whether the task downloads started.
    pub fn is_started(&self) -> bool {
        self.finished_at.is_none()
    }

    // is_finished returns whether the task downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }
}

// Piece is the metadata of the piece.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Piece {
    // number is the piece number.
    pub number: i32,

    // offset is the offset of the piece in the task.
    pub offset: u64,

    // length is the length of the piece.
    pub length: u64,

    // digest is the digest of the piece.
    pub digest: String,

    // uploaded_count is the count of the piece uploaded by other peers.
    pub uploaded_count: u64,

    // updated_at is the time when the piece metadata is updated. If the piece is downloaded
    // by other peers, it will also update updated_at.
    pub updated_at: NaiveDateTime,

    // created_at is the time when the piece metadata is created.
    pub created_at: NaiveDateTime,

    // finished_at is the time when the piece downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

// Piece implements the piece metadata.
impl Piece {
    // is_started returns whether the piece downloads started.
    pub fn is_started(&self) -> bool {
        self.finished_at.is_none()
    }

    // is_finished returns whether the piece downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }
}

// Metadata is the metadata of the task.
pub struct Metadata {
    // db is the rocksdb instance.
    db: DB,
}

// Metadata implements the metadata storage.
impl Metadata {
    // new returns a new metadata.
    pub fn new(data_dir: &Path) -> Result<Metadata> {
        // Initialize rocksdb options.
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.optimize_level_style_compaction(DEFAULT_MEMTABLE_MEMORY_BUDGET);
        options.increase_parallelism(num_cpus::get() as i32);
        options.set_max_open_files(DEFAULT_MAX_OPEN_FILES);
        // Set prefix extractor to reduce the memory usage of bloom filter and length of task id is 64.
        options.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(64));
        options.set_memtable_prefix_bloom_ratio(0.2);

        // Initialize rocksdb block based table options.
        let mut block_options = BlockBasedOptions::default();
        block_options.set_block_cache(&Cache::new_lru_cache(DEFAULT_CACHE_SIZE));
        block_options.set_block_size(DEFAULT_BLOCK_SIZE);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_options.set_bloom_filter(10.0, false);
        options.set_block_based_table_factory(&block_options);

        // Open rocksdb.
        let dir = data_dir.join(config::NAME).join(DEFAULT_DIR_NAME);
        let cf_names = [TASK_CF_NAME, PIECE_CF_NAME];
        let db = DB::open_cf_with_opts(
            &options,
            &dir,
            cf_names
                .iter()
                .map(|name| (name.to_string(), options.clone()))
                .collect::<Vec<_>>(),
        )?;
        info!("create metadata directory: {:?}", dir);

        Ok(Metadata { db })
    }

    // download_task_started updates the metadata of the task when the task downloads started.
    pub fn download_task_started(&self, id: &str, piece_length: i32) -> Result<()> {
        let task = match self.get_task(id)? {
            // If the task exists, update the updated_at.
            Some(mut task) => {
                task.updated_at = Utc::now().naive_utc();
                task
            }
            // If the task does not exist, create a new task.
            None => Task {
                id: id.to_string(),
                piece_length,
                updated_at: Utc::now().naive_utc(),
                created_at: Utc::now().naive_utc(),
                ..Default::default()
            },
        };

        self.put_task(id, &task)
    }

    // upload_task_finished updates the metadata of the task when task uploads finished.
    pub fn upload_task_finished(&self, id: &str) -> Result<()> {
        match self.get_task(id)? {
            Some(mut task) => {
                task.uploaded_count += 1;
                task.updated_at = Utc::now().naive_utc();
                self.put_task(id, &task)
            }
            None => Err(Error::TaskNotFound(id.to_string())),
        }
    }

    // download_task_failed updates the metadata of the task when the task downloads failed.
    pub fn download_task_failed(&self, id: &str) -> Result<()> {
        match self.get_task(id)? {
            Some(_piece) => self.delete_task(id),
            None => Err(Error::TaskNotFound(id.to_string())),
        }
    }

    // get_task gets the task metadata.
    pub fn get_task(&self, id: &str) -> Result<Option<Task>> {
        let handle = self.cf_handle(TASK_CF_NAME)?;
        match self.db.get_cf(handle, id)? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    // put_task puts the task metadata.
    fn put_task(&self, id: &str, task: &Task) -> Result<()> {
        let handle = self.cf_handle(TASK_CF_NAME)?;
        let json = serde_json::to_string(&task)?;
        self.db.put_cf(handle, id.as_bytes(), json.as_bytes())?;
        Ok(())
    }

    // delete_task deletes the task metadata.
    fn delete_task(&self, id: &str) -> Result<()> {
        let handle = self.cf_handle(TASK_CF_NAME)?;
        self.db.delete_cf(handle, id.as_bytes())?;
        Ok(())
    }

    // download_piece_started updates the metadata of the piece when the piece downloads started.
    pub fn download_piece_started(&self, task_id: &str, number: i32) -> Result<()> {
        self.put_piece(
            task_id,
            &Piece {
                number,
                updated_at: Utc::now().naive_utc(),
                created_at: Utc::now().naive_utc(),
                ..Default::default()
            },
        )
    }

    // download_piece_finished updates the metadata of the piece when the piece downloads finished.
    pub fn download_piece_finished(
        &self,
        task_id: &str,
        number: i32,
        offset: u64,
        length: u64,
        digest: &str,
    ) -> Result<()> {
        match self.get_piece(task_id, number)? {
            Some(mut piece) => {
                piece.offset = offset;
                piece.length = length;
                piece.digest = digest.to_string();
                piece.updated_at = Utc::now().naive_utc();
                piece.finished_at = Some(Utc::now().naive_utc());
                self.put_piece(task_id, &piece)
            }
            None => Err(Error::PieceNotFound(self.piece_id(task_id, number))),
        }
    }

    // download_piece_failed updates the metadata of the piece when the piece downloads failed.
    pub fn download_piece_failed(&self, task_id: &str, number: i32) -> Result<()> {
        match self.get_piece(task_id, number)? {
            Some(_piece) => self.delete_piece(task_id, number),
            None => Err(Error::PieceNotFound(self.piece_id(task_id, number))),
        }
    }

    // upload_piece_finished updates the metadata of the piece when piece uploads finished.
    pub fn upload_piece_finished(&self, task_id: &str, number: i32) -> Result<()> {
        match self.get_piece(task_id, number)? {
            Some(mut piece) => {
                piece.uploaded_count += 1;
                piece.updated_at = Utc::now().naive_utc();
                self.put_piece(task_id, &piece)
            }
            None => Err(Error::PieceNotFound(self.piece_id(task_id, number))),
        }
    }

    // get_piece gets the piece metadata.
    pub fn get_piece(&self, task_id: &str, number: i32) -> Result<Option<Piece>> {
        let id = self.piece_id(task_id, number);
        let handle = self.cf_handle(PIECE_CF_NAME)?;
        match self.db.get_cf(handle, id.as_bytes())? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    // get_pieces gets the pieces metadata.
    pub fn get_pieces(&self, task_id: &str) -> Result<Vec<Piece>> {
        let handle = self.cf_handle(PIECE_CF_NAME)?;
        let iter = self.db.prefix_iterator_cf(handle, task_id.as_bytes());

        // Iterate the pieces metadata.
        let mut pieces = Vec::new();
        for ele in iter {
            let (_, value) = ele?;
            let piece: Piece = serde_json::from_slice(&value)?;
            pieces.push(piece);
        }

        Ok(pieces)
    }

    // put_piece puts the piece metadata.
    fn put_piece(&self, task_id: &str, piece: &Piece) -> Result<()> {
        let id = self.piece_id(task_id, piece.number);
        let handle = self.cf_handle(PIECE_CF_NAME)?;
        let json = serde_json::to_string(&piece)?;
        self.db.put_cf(handle, id.as_bytes(), json.as_bytes())?;
        Ok(())
    }

    // delete_piece deletes the piece metadata.
    fn delete_piece(&self, task_id: &str, number: i32) -> Result<()> {
        let id = self.piece_id(task_id, number);
        let handle = self.cf_handle(PIECE_CF_NAME)?;
        self.db.delete_cf(handle, id.as_bytes())?;
        Ok(())
    }

    // piece_id returns the piece id.
    pub fn piece_id(&self, task_id: &str, number: i32) -> String {
        format!("{}-{}", task_id, number)
    }

    // cf_handle returns the column family handle.
    fn cf_handle(&self, cf_name: &str) -> Result<&ColumnFamily> {
        self.db
            .cf_handle(cf_name)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.to_string()))
    }
}
