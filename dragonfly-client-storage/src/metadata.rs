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

use chrono::{NaiveDateTime, Utc};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use dragonfly_client_util::http::reqwest_headermap_to_hashmap;
use reqwest::header::{self, HeaderMap};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, IteratorMode, Options, TransactionDB,
    TransactionDBOptions,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tracing::{error, info};

// DEFAULT_DIR_NAME is the default directory name to store metadata.
const DEFAULT_DIR_NAME: &str = "metadata";

// DEFAULT_MEMTABLE_MEMORY_BUDGET is the default memory budget for memtable, default is 64MB.
const DEFAULT_MEMTABLE_MEMORY_BUDGET: usize = 64 * 1024 * 1024;

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
    pub piece_length: u64,

    // header is the header of the response.
    pub response_header: HashMap<String, String>,

    // uploading_count is the count of the task being uploaded by other peers.
    pub uploading_count: u64,

    // uploaded_count is the count of the task has been uploaded by other peers.
    pub uploaded_count: u64,

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

    // is_downloading returns whether the task is downloading.
    pub fn is_uploading(&self) -> bool {
        self.uploading_count > 0
    }

    // is_expired returns whether the task is expired.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at + ttl < Utc::now().naive_utc()
    }

    // is_finished returns whether the task downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }

    // is_empty returns whether the task is empty.
    pub fn is_empty(&self) -> bool {
        if let Some(content_length) = self.content_length() {
            if content_length == 0 {
                return true;
            }
        }

        false
    }

    // content_length returns the content length of the task.
    pub fn content_length(&self) -> Option<u64> {
        match self.response_header.get(header::CONTENT_LENGTH.as_str()) {
            Some(content_length) => match content_length.parse::<u64>() {
                Ok(content_length) => Some(content_length),
                Err(err) => {
                    error!("parse content length error: {:?}", err);
                    None
                }
            },
            None => None,
        }
    }
}

// Piece is the metadata of the piece.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Piece {
    // number is the piece number.
    pub number: u32,

    // offset is the offset of the piece in the task.
    pub offset: u64,

    // length is the length of the piece.
    pub length: u64,

    // digest is the digest of the piece.
    pub digest: String,

    // parent_id is the parent id of the piece.
    pub parent_id: Option<String>,

    // uploading_count is the count of the piece being uploaded by other peers.
    pub uploading_count: u64,

    // uploaded_count is the count of the piece has been uploaded by other peers.
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

    // cost returns the cost of the piece downloaded.
    pub fn cost(&self) -> Option<Duration> {
        match self
            .finished_at
            .map(|finished_at| finished_at - self.created_at)
        {
            Some(cost) => match cost.to_std() {
                Ok(cost) => Some(cost),
                Err(err) => {
                    error!("convert cost error: {:?}", err);
                    None
                }
            },
            None => None,
        }
    }

    // prost_cost returns the prost cost of the piece downloaded.
    pub fn prost_cost(&self) -> Option<prost_wkt_types::Duration> {
        match self.cost() {
            Some(cost) => match prost_wkt_types::Duration::try_from(cost) {
                Ok(cost) => Some(cost),
                Err(err) => {
                    error!("convert cost error: {:?}", err);
                    None
                }
            },
            None => None,
        }
    }
}

// Metadata is the metadata of the task.
pub struct Metadata {
    // db is the rocksdb instance.
    db: TransactionDB,
}

// Metadata implements the metadata storage.
impl Metadata {
    // new returns a new metadata.
    pub fn new(dir: &Path) -> Result<Metadata> {
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
        let dir = dir.join(DEFAULT_DIR_NAME);
        let cf_names = [TASK_CF_NAME, PIECE_CF_NAME];
        let db = TransactionDB::open_cf(&options, &TransactionDBOptions::default(), &dir, cf_names)
            .or_err(ErrorType::StorageError)?;
        info!("metadata initialized directory: {:?}", dir);

        Ok(Metadata { db })
    }

    // download_task_started updates the metadata of the task when the task downloads started.
    pub fn download_task_started(
        &self,
        id: &str,
        piece_length: u64,
        response_header: Option<HeaderMap>,
    ) -> Result<Task> {
        // Get the column family handle of task.
        let handle = self.cf_handle(TASK_CF_NAME)?;

        // Convert the response header to hashmap.
        let response_header = match response_header {
            Some(response_header) => reqwest_headermap_to_hashmap(&response_header),
            None => HashMap::new(),
        };

        // Transaction is used to update the task metadata.
        let txn = self.db.transaction();
        let task = match txn
            .get_for_update_cf(handle, id, true)
            .or_err(ErrorType::StorageError)?
        {
            Some(bytes) => {
                // If the task exists, update the task metadata.
                let mut task: Task =
                    serde_json::from_slice(&bytes).or_err(ErrorType::SerializeError)?;
                task.updated_at = Utc::now().naive_utc();

                // If the task has the response header, the response header
                // will not be covered.
                if task.response_header.is_empty() {
                    task.response_header = response_header;
                }
                task
            }
            // If the task does not exist, create a new task metadata.
            None => Task {
                id: id.to_string(),
                piece_length,
                response_header,
                updated_at: Utc::now().naive_utc(),
                created_at: Utc::now().naive_utc(),
                ..Default::default()
            },
        };

        // Put the task metadata.
        let json = serde_json::to_string(&task).or_err(ErrorType::SerializeError)?;
        txn.put_cf(handle, id.as_bytes(), json.as_bytes())
            .or_err(ErrorType::StorageError)?;
        txn.commit().or_err(ErrorType::StorageError)?;
        Ok(task)
    }

    // download_task_finished updates the metadata of the task when the task downloads finished.
    pub fn download_task_finished(&self, id: &str) -> Result<Task> {
        // Get the column family handle of task.
        let handle = self.cf_handle(TASK_CF_NAME)?;

        // Transaction is used to update the task metadata.
        let txn = self.db.transaction();
        let task = match txn
            .get_for_update_cf(handle, id, true)
            .or_err(ErrorType::StorageError)?
        {
            Some(bytes) => {
                //    If the task exists, update the task metadata.
                let mut task: Task =
                    serde_json::from_slice(&bytes).or_err(ErrorType::SerializeError)?;
                task.updated_at = Utc::now().naive_utc();
                task.finished_at = Some(Utc::now().naive_utc());
                task
            }
            // If the task does not exist, return error.
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        // Put the task metadata.
        let json = serde_json::to_string(&task).or_err(ErrorType::SerializeError)?;
        txn.put_cf(handle, id.as_bytes(), json.as_bytes())
            .or_err(ErrorType::StorageError)?;
        txn.commit().or_err(ErrorType::StorageError)?;
        Ok(task)
    }

    // upload_task_started updates the metadata of the task when task uploads started.
    pub fn upload_task_started(&self, id: &str) -> Result<Task> {
        // Get the column family handle of task.
        let handle = self.cf_handle(TASK_CF_NAME)?;

        // Transaction is used to update the task metadata.
        let txn = self.db.transaction();
        let task = match txn
            .get_for_update_cf(handle, id, true)
            .or_err(ErrorType::StorageError)?
        {
            Some(bytes) => {
                // If the task exists, update the task metadata.
                let mut task: Task =
                    serde_json::from_slice(&bytes).or_err(ErrorType::SerializeError)?;
                task.uploading_count += 1;
                task.updated_at = Utc::now().naive_utc();
                task
            }
            // If the task does not exist, return error.
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        // Put the task metadata.
        let json = serde_json::to_string(&task).or_err(ErrorType::SerializeError)?;
        txn.put_cf(handle, id.as_bytes(), json.as_bytes())
            .or_err(ErrorType::StorageError)?;
        txn.commit().or_err(ErrorType::StorageError)?;
        Ok(task)
    }

    // upload_task_finished updates the metadata of the task when task uploads finished.
    pub fn upload_task_finished(&self, id: &str) -> Result<Task> {
        // Get the column family handle of task.
        let handle = self.cf_handle(TASK_CF_NAME)?;

        // Transaction is used to update the task metadata.
        let txn = self.db.transaction();
        let task = match txn
            .get_for_update_cf(handle, id, true)
            .or_err(ErrorType::StorageError)?
        {
            Some(bytes) => {
                // If the task exists, update the task metadata.
                let mut task: Task =
                    serde_json::from_slice(&bytes).or_err(ErrorType::SerializeError)?;
                task.uploading_count -= 1;
                task.uploaded_count += 1;
                task.updated_at = Utc::now().naive_utc();
                task
            }
            // If the task does not exist, return error.
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        // Put the task metadata.
        let json = serde_json::to_string(&task).or_err(ErrorType::SerializeError)?;
        txn.put_cf(handle, id.as_bytes(), json.as_bytes())
            .or_err(ErrorType::StorageError)?;
        txn.commit().or_err(ErrorType::StorageError)?;
        Ok(task)
    }

    // upload_task_failed updates the metadata of the task when the task uploads failed.
    pub fn upload_task_failed(&self, id: &str) -> Result<Task> {
        // Get the column family handle of task.
        let handle = self.cf_handle(TASK_CF_NAME)?;

        // Transaction is used to update the task metadata.
        let txn = self.db.transaction();
        let task = match txn
            .get_for_update_cf(handle, id, true)
            .or_err(ErrorType::StorageError)?
        {
            Some(bytes) => {
                // If the task exists, update the task metadata.
                let mut task: Task =
                    serde_json::from_slice(&bytes).or_err(ErrorType::SerializeError)?;
                task.uploading_count -= 1;
                task.updated_at = Utc::now().naive_utc();
                task
            }
            // If the task does not exist, return error.
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        // Put the task metadata.
        let json = serde_json::to_string(&task).or_err(ErrorType::SerializeError)?;
        txn.put_cf(handle, id.as_bytes(), json.as_bytes())
            .or_err(ErrorType::StorageError)?;
        txn.commit().or_err(ErrorType::StorageError)?;
        Ok(task)
    }

    // get_task gets the task metadata.
    pub fn get_task(&self, id: &str) -> Result<Option<Task>> {
        // Get the column family handle of task.
        let handle = self.cf_handle(TASK_CF_NAME)?;
        match self.db.get_cf(handle, id).or_err(ErrorType::StorageError)? {
            Some(bytes) => Ok(Some(
                serde_json::from_slice(&bytes).or_err(ErrorType::SerializeError)?,
            )),
            None => Ok(None),
        }
    }

    // get_tasks gets the task metadatas.
    pub fn get_tasks(&self) -> Result<Vec<Task>> {
        // Get the column family handle of task.
        let handle = self.cf_handle(TASK_CF_NAME)?;

        // Transaction is used to get the task metadatas.
        let txn = self.db.transaction();
        let iter = txn.iterator_cf(handle, IteratorMode::Start);

        // Iterate the task metadatas.
        let mut tasks = Vec::new();
        for ele in iter {
            let (_, value) = ele.or_err(ErrorType::StorageError)?;
            let task: Task = serde_json::from_slice(&value).or_err(ErrorType::SerializeError)?;
            tasks.push(task);
        }

        Ok(tasks)
    }

    // delete_task deletes the task metadata.
    pub fn delete_task(&self, task_id: &str) -> Result<()> {
        // Get the column family handle of task.
        let handle = self.cf_handle(TASK_CF_NAME)?;

        // Transaction is used to delete the task metadata.
        let txn = self.db.transaction();
        txn.delete_cf(handle, task_id)
            .or_err(ErrorType::SerializeError)?;
        txn.commit().or_err(ErrorType::SerializeError)?;
        Ok(())
    }

    // download_piece_started updates the metadata of the piece when the piece downloads started.
    pub fn download_piece_started(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Get the column family handle of piece.
        let handle = self.cf_handle(PIECE_CF_NAME)?;

        // Get the piece id.
        let id = self.piece_id(task_id, number);

        // Construct the piece metadata.
        let piece = Piece {
            number,
            updated_at: Utc::now().naive_utc(),
            created_at: Utc::now().naive_utc(),
            ..Default::default()
        };

        // Transaction is used to update the piece metadata.
        let txn = self.db.transaction();

        // Put the piece metadata.
        let json = serde_json::to_string(&piece).or_err(ErrorType::SerializeError)?;
        txn.put_cf(handle, id.as_bytes(), json.as_bytes())
            .or_err(ErrorType::StorageError)?;
        txn.commit().or_err(ErrorType::StorageError)?;
        Ok(piece)
    }

    // download_piece_finished updates the metadata of the piece when the piece downloads finished.
    pub fn download_piece_finished(
        &self,
        task_id: &str,
        number: u32,
        offset: u64,
        length: u64,
        digest: &str,
        parent_id: Option<String>,
    ) -> Result<Piece> {
        // Get the column family handle of piece.
        let handle = self.cf_handle(PIECE_CF_NAME)?;

        // Get the piece id.
        let id = self.piece_id(task_id, number);

        // Transaction is used to update the piece metadata.
        let txn = self.db.transaction();
        let piece = match txn
            .get_for_update_cf(handle, id.as_bytes(), true)
            .or_err(ErrorType::StorageError)?
        {
            Some(bytes) => {
                // If the piece exists, update the piece metadata.
                let mut piece: Piece =
                    serde_json::from_slice(&bytes).or_err(ErrorType::SerializeError)?;
                piece.offset = offset;
                piece.length = length;
                piece.digest = digest.to_string();
                piece.parent_id = parent_id;
                piece.updated_at = Utc::now().naive_utc();
                piece.finished_at = Some(Utc::now().naive_utc());
                piece
            }
            // If the piece does not exist, return error.
            None => return Err(Error::PieceNotFound(id)),
        };

        // Put the piece metadata.
        let json = serde_json::to_string(&piece).or_err(ErrorType::SerializeError)?;
        txn.put_cf(handle, id.as_bytes(), json.as_bytes())
            .or_err(ErrorType::StorageError)?;
        txn.commit().or_err(ErrorType::StorageError)?;
        Ok(piece)
    }

    // download_piece_failed updates the metadata of the piece when the piece downloads failed.
    pub fn download_piece_failed(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Get the column family handle of piece.
        let handle = self.cf_handle(PIECE_CF_NAME)?;

        // Get the piece id.
        let id = self.piece_id(task_id, number);

        // Transaction is used to update the piece metadata.
        let txn = self.db.transaction();
        let piece = match txn
            .get_for_update_cf(handle, id.as_bytes(), true)
            .or_err(ErrorType::StorageError)?
        {
            // If the piece exists, delete the piece metadata.
            Some(bytes) => {
                txn.delete_cf(handle, id.as_bytes())
                    .or_err(ErrorType::StorageError)?;
                let piece: Piece =
                    serde_json::from_slice(&bytes).or_err(ErrorType::SerializeError)?;
                piece
            }
            // If the piece does not exist, return error.
            None => return Err(Error::PieceNotFound(id)),
        };

        // Commit the transaction.
        txn.commit().or_err(ErrorType::StorageError)?;
        Ok(piece)
    }

    // upload_piece_started updates the metadata of the piece when piece uploads started.
    pub fn upload_piece_started(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Get the column family handle of piece.
        let handle = self.cf_handle(PIECE_CF_NAME)?;

        // Get the piece id.
        let id = self.piece_id(task_id, number);

        // Transaction is used to update the piece metadata.
        let txn = self.db.transaction();
        let piece = match txn
            .get_for_update_cf(handle, id.as_bytes(), true)
            .or_err(ErrorType::StorageError)?
        {
            Some(bytes) => {
                // If the piece exists, update the piece metadata.
                let mut piece: Piece =
                    serde_json::from_slice(&bytes).or_err(ErrorType::SerializeError)?;
                piece.uploading_count += 1;
                piece.updated_at = Utc::now().naive_utc();
                piece
            }
            // If the piece does not exist, return error.
            None => return Err(Error::PieceNotFound(id)),
        };

        // Put the piece metadata.
        let json = serde_json::to_string(&piece).or_err(ErrorType::SerializeError)?;
        txn.put_cf(handle, id.as_bytes(), json.as_bytes())
            .or_err(ErrorType::StorageError)?;
        txn.commit().or_err(ErrorType::StorageError)?;
        Ok(piece)
    }

    // upload_piece_finished updates the metadata of the piece when piece uploads finished.
    pub fn upload_piece_finished(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Get the column family handle of piece.
        let handle = self.cf_handle(PIECE_CF_NAME)?;

        // Get the piece id.
        let id = self.piece_id(task_id, number);

        // Transaction is used to update the piece metadata.
        let txn = self.db.transaction();
        let piece = match txn
            .get_for_update_cf(handle, id.as_bytes(), true)
            .or_err(ErrorType::StorageError)?
        {
            Some(bytes) => {
                // If the piece exists, update the piece metadata.
                let mut piece: Piece =
                    serde_json::from_slice(&bytes).or_err(ErrorType::SerializeError)?;
                piece.uploading_count -= 1;
                piece.uploaded_count += 1;
                piece.updated_at = Utc::now().naive_utc();
                piece
            }
            // If the piece does not exist, return error.
            None => return Err(Error::PieceNotFound(id)),
        };

        // Put the piece metadata.
        let json = serde_json::to_string(&piece).or_err(ErrorType::SerializeError)?;
        txn.put_cf(handle, id.as_bytes(), json.as_bytes())
            .or_err(ErrorType::StorageError)?;
        txn.commit().or_err(ErrorType::StorageError)?;
        Ok(piece)
    }

    // upload_piece_failed updates the metadata of the piece when the piece uploads failed.
    pub fn upload_piece_failed(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Get the column family handle of piece.
        let handle = self.cf_handle(PIECE_CF_NAME)?;

        // Get the piece id.
        let id = self.piece_id(task_id, number);

        // Transaction is used to update the piece metadata.
        let txn = self.db.transaction();
        let piece = match txn
            .get_for_update_cf(handle, id.as_bytes(), true)
            .or_err(ErrorType::StorageError)?
        {
            Some(bytes) => {
                // If the piece exists, update the piece metadata.
                let mut piece: Piece =
                    serde_json::from_slice(&bytes).or_err(ErrorType::SerializeError)?;
                piece.uploading_count -= 1;
                piece.updated_at = Utc::now().naive_utc();
                piece
            }
            // If the piece does not exist, return error.
            None => return Err(Error::PieceNotFound(id)),
        };

        // Put the piece metadata.
        let json = serde_json::to_string(&piece).or_err(ErrorType::SerializeError)?;
        txn.put_cf(handle, id.as_bytes(), json.as_bytes())
            .or_err(ErrorType::StorageError)?;
        txn.commit().or_err(ErrorType::StorageError)?;
        Ok(piece)
    }

    // get_piece gets the piece metadata.
    pub fn get_piece(&self, task_id: &str, number: u32) -> Result<Option<Piece>> {
        let id = self.piece_id(task_id, number);
        let handle = self.cf_handle(PIECE_CF_NAME)?;
        match self
            .db
            .get_cf(handle, id.as_bytes())
            .or_err(ErrorType::StorageError)?
        {
            Some(bytes) => Ok(Some(
                serde_json::from_slice(&bytes).or_err(ErrorType::SerializeError)?,
            )),
            None => Ok(None),
        }
    }

    // get_pieces gets the piece metadatas.
    pub fn get_pieces(&self, task_id: &str) -> Result<Vec<Piece>> {
        // Get the column family handle of piece.
        let handle = self.cf_handle(PIECE_CF_NAME)?;

        // Transaction is used to get the piece metadatas.
        let txn = self.db.transaction();
        let iter = txn.prefix_iterator_cf(handle, task_id.as_bytes());

        // Iterate the piece metadatas.
        let mut pieces = Vec::new();
        for ele in iter {
            let (_, value) = ele.or_err(ErrorType::StorageError)?;
            let piece: Piece = serde_json::from_slice(&value).or_err(ErrorType::SerializeError)?;
            pieces.push(piece);
        }

        Ok(pieces)
    }

    // delete_pieces deletes the piece metadatas.
    pub fn delete_pieces(&self, task_id: &str) -> Result<()> {
        // Get the column family handle of piece.
        let handle = self.cf_handle(PIECE_CF_NAME)?;

        // Transaction is used to delete the piece metadatas.
        let txn = self.db.transaction();
        let iter = txn.prefix_iterator_cf(handle, task_id.as_bytes());

        // Iterate the piece metadatas.
        for ele in iter {
            let (key, _) = ele.or_err(ErrorType::StorageError)?;
            txn.delete_cf(handle, key).or_err(ErrorType::StorageError)?;
        }

        // Commit the transaction.
        txn.commit().or_err(ErrorType::StorageError)?;
        Ok(())
    }

    // piece_id returns the piece id.
    pub fn piece_id(&self, task_id: &str, number: u32) -> String {
        format!("{}-{}", task_id, number)
    }

    // cf_handle returns the column family handle.
    fn cf_handle(&self, cf_name: &str) -> Result<&ColumnFamily> {
        self.db
            .cf_handle(cf_name)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.to_string()))
    }
}
