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
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::http::reqwest_headermap_to_hashmap;
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

use crate::storage_engine::{rocksdb::RocksdbStorageEngine, DatabaseObject, StorageEngineOwned};

// Task is the metadata of the task.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    // id is the task id.
    pub id: String,

    // piece_length is the length of the piece.
    pub piece_length: u64,

    // content_length is the length of the content.
    pub content_length: Option<u64>,

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

    // prefetched_at is the time when the task prefetched.
    pub prefetched_at: Option<NaiveDateTime>,

    // failed_at is the time when the task downloads failed.
    pub failed_at: Option<NaiveDateTime>,

    // finished_at is the time when the task downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

// Task implements the task database object.
impl DatabaseObject for Task {
    // NAMESPACE is the namespace of [Task] objects.
    const NAMESPACE: &'static str = "task";
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
        self.updated_at + ttl < Utc::now().naive_utc()
    }

    // is_prefetched returns whether the task is prefetched.
    pub fn is_prefetched(&self) -> bool {
        self.prefetched_at.is_some()
    }

    // is_failed returns whether the task downloads failed.
    pub fn is_failed(&self) -> bool {
        self.failed_at.is_some()
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
        self.content_length
    }
}

// CacheTask is the metadata of the cache task.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheTask {
    // id is the task id.
    pub id: String,

    // persistent represents whether the cache task is persistent.
    // If the cache task is persistent, the cache peer will
    // not be deleted when dfdamon runs garbage collection.
    pub persistent: bool,

    // digests is the digests of the cache task.
    pub digest: String,

    // piece_length is the length of the piece.
    pub piece_length: u64,

    // content_length is the length of the content.
    pub content_length: u64,

    // uploading_count is the count of the task being uploaded by other peers.
    pub uploading_count: u64,

    // uploaded_count is the count of the task has been uploaded by other peers.
    pub uploaded_count: u64,

    // updated_at is the time when the task metadata is updated. If the task is downloaded
    // by other peers, it will also update updated_at.
    pub updated_at: NaiveDateTime,

    // created_at is the time when the task metadata is created.
    pub created_at: NaiveDateTime,

    // failed_at is the time when the task downloads failed.
    pub failed_at: Option<NaiveDateTime>,

    // finished_at is the time when the task downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

// CacheTask implements the task database object.
impl DatabaseObject for CacheTask {
    // NAMESPACE is the namespace of [CacheTask] objects.
    const NAMESPACE: &'static str = "cache_task";
}

// CacheTask implements the cache task metadata.
impl CacheTask {
    // is_started returns whether the cache task downloads started.
    pub fn is_started(&self) -> bool {
        self.finished_at.is_none()
    }

    // is_downloading returns whether the cache task is downloading.
    pub fn is_uploading(&self) -> bool {
        self.uploading_count > 0
    }

    // is_failed returns whether the cache task downloads failed.
    pub fn is_failed(&self) -> bool {
        self.failed_at.is_some()
    }

    // is_finished returns whether the cache task downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }

    // is_empty returns whether the cache task is empty.
    pub fn is_empty(&self) -> bool {
        if self.content_length == 0 {
            return true;
        }

        false
    }

    // is_persistent returns whether the cache task is persistent.
    pub fn is_persistent(&self) -> bool {
        self.persistent
    }

    // content_length returns the content length of the cache task.
    pub fn content_length(&self) -> u64 {
        self.content_length
    }
}

// Piece is the metadata of the piece.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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

// Piece implements the piece database object.
impl DatabaseObject for Piece {
    // NAMESPACE is the namespace of [Piece] objects.
    const NAMESPACE: &'static str = "piece";
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

// Metadata manages the metadata of [Task] and [Piece].
pub struct Metadata<E = RocksdbStorageEngine>
where
    E: StorageEngineOwned,
{
    // db is the underlying storage engine instance.
    db: E,
}

impl<E: StorageEngineOwned> Metadata<E> {
    // download_task_started updates the metadata of the task when the task downloads started.
    pub fn download_task_started(
        &self,
        id: &str,
        piece_length: u64,
        content_length: Option<u64>,
        response_header: Option<HeaderMap>,
    ) -> Result<Task> {
        // Convert the response header to hashmap.
        let response_header = response_header
            .as_ref()
            .map(reqwest_headermap_to_hashmap)
            .unwrap_or_default();

        let task = match self.db.get::<Task>(id.as_bytes())? {
            Some(mut task) => {
                // If the task exists, update the task metadata.
                task.updated_at = Utc::now().naive_utc();
                task.failed_at = None;

                // Protect content length to be overwritten by None.
                if content_length.is_some() {
                    task.content_length = content_length;
                }

                // If the task has the response header, the response header
                // will not be covered.
                if task.response_header.is_empty() {
                    task.response_header = response_header;
                }

                task
            }
            None => Task {
                id: id.to_string(),
                piece_length,
                content_length,
                response_header,
                updated_at: Utc::now().naive_utc(),
                created_at: Utc::now().naive_utc(),
                ..Default::default()
            },
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    // download_task_finished updates the metadata of the task when the task downloads finished.
    pub fn download_task_finished(&self, id: &str) -> Result<Task> {
        let task = match self.db.get::<Task>(id.as_bytes())? {
            Some(mut task) => {
                task.updated_at = Utc::now().naive_utc();
                task.failed_at = None;
                task.finished_at = Some(Utc::now().naive_utc());
                task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    // download_task_failed updates the metadata of the task when the task downloads failed.
    pub fn download_task_failed(&self, id: &str) -> Result<Task> {
        let task = match self.db.get::<Task>(id.as_bytes())? {
            Some(mut task) => {
                task.updated_at = Utc::now().naive_utc();
                task.failed_at = Some(Utc::now().naive_utc());
                task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    // prefetch_task_started updates the metadata of the task when the task prefetch started.
    pub fn prefetch_task_started(&self, id: &str) -> Result<Task> {
        let task = match self.db.get::<Task>(id.as_bytes())? {
            Some(mut task) => {
                // If the task is prefetched, return an error.
                if task.is_prefetched() {
                    return Err(Error::InvalidState("prefetched".to_string()));
                }

                task.updated_at = Utc::now().naive_utc();
                task.prefetched_at = Some(Utc::now().naive_utc());
                task.failed_at = None;
                task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    // prefetch_task_failed updates the metadata of the task when the task prefetch failed.
    pub fn prefetch_task_failed(&self, id: &str) -> Result<Task> {
        let task = match self.db.get::<Task>(id.as_bytes())? {
            Some(mut task) => {
                task.updated_at = Utc::now().naive_utc();
                task.prefetched_at = None;
                task.failed_at = Some(Utc::now().naive_utc());
                task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    // upload_task_started updates the metadata of the task when task uploads started.
    pub fn upload_task_started(&self, id: &str) -> Result<Task> {
        let task = match self.db.get::<Task>(id.as_bytes())? {
            Some(mut task) => {
                task.uploading_count += 1;
                task.updated_at = Utc::now().naive_utc();
                task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    // upload_task_finished updates the metadata of the task when task uploads finished.
    pub fn upload_task_finished(&self, id: &str) -> Result<Task> {
        let task = match self.db.get::<Task>(id.as_bytes())? {
            Some(mut task) => {
                task.uploading_count -= 1;
                task.uploaded_count += 1;
                task.updated_at = Utc::now().naive_utc();
                task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    // upload_task_failed updates the metadata of the task when the task uploads failed.
    pub fn upload_task_failed(&self, id: &str) -> Result<Task> {
        let task = match self.db.get::<Task>(id.as_bytes())? {
            Some(mut task) => {
                task.uploading_count -= 1;
                task.updated_at = Utc::now().naive_utc();
                task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    // get_task gets the task metadata.
    pub fn get_task(&self, id: &str) -> Result<Option<Task>> {
        self.db.get(id.as_bytes())
    }

    // get_tasks gets the task metadatas.
    pub fn get_tasks(&self) -> Result<Vec<Task>> {
        let iter = self.db.iter::<Task>()?;
        iter.map(|ele| ele.map(|(_, task)| task)).collect()
    }

    // delete_task deletes the task metadata.
    pub fn delete_task(&self, id: &str) -> Result<()> {
        info!("delete task metadata {}", id);
        self.db.delete::<Task>(id.as_bytes())
    }

    // create_persistent_cache_task creates a new persistent cache task.
    // If the cache task imports the content to the dfdaemon finished,
    // the dfdaemon will create a persistent cache task metadata.
    pub fn create_persistent_cache_task(
        &self,
        id: &str,
        piece_length: u64,
        content_length: u64,
        digest: &str,
    ) -> Result<CacheTask> {
        let cache_task = CacheTask {
            id: id.to_string(),
            persistent: true,
            piece_length,
            content_length,
            digest: digest.to_string(),
            updated_at: Utc::now().naive_utc(),
            created_at: Utc::now().naive_utc(),
            finished_at: Some(Utc::now().naive_utc()),
            ..Default::default()
        };

        self.db.put(id.as_bytes(), &cache_task)?;
        Ok(cache_task)
    }

    // download_cache_task_started updates the metadata of the cache task when
    // the cache task downloads started. If the cache task downloaded by scheduler
    // to create persistent cache task, the persistent should be set to true.
    pub fn download_cache_task_started(
        &self,
        id: &str,
        persistent: bool,
        piece_length: u64,
        content_length: u64,
    ) -> Result<CacheTask> {
        let cache_task = match self.db.get::<CacheTask>(id.as_bytes())? {
            Some(mut cache_task) => {
                // If the task exists, update the task metadata.
                cache_task.updated_at = Utc::now().naive_utc();
                cache_task.failed_at = None;
                cache_task
            }
            None => CacheTask {
                id: id.to_string(),
                persistent,
                piece_length,
                content_length,
                updated_at: Utc::now().naive_utc(),
                created_at: Utc::now().naive_utc(),
                ..Default::default()
            },
        };

        self.db.put(id.as_bytes(), &cache_task)?;
        Ok(cache_task)
    }

    // download_cache_task_finished updates the metadata of the cache task when the cache task downloads finished.
    pub fn download_cache_task_finished(&self, id: &str) -> Result<CacheTask> {
        let cache_task = match self.db.get::<CacheTask>(id.as_bytes())? {
            Some(mut cache_task) => {
                cache_task.updated_at = Utc::now().naive_utc();
                cache_task.failed_at = None;

                // If the cache task is created by user, the finished_at has been set.
                if cache_task.finished_at.is_none() {
                    cache_task.finished_at = Some(Utc::now().naive_utc());
                }

                cache_task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &cache_task)?;
        Ok(cache_task)
    }

    // download_cache_task_failed updates the metadata of the cache task when the cache task downloads failed.
    pub fn download_cache_task_failed(&self, id: &str) -> Result<CacheTask> {
        let cache_task = match self.db.get::<CacheTask>(id.as_bytes())? {
            Some(mut cache_task) => {
                cache_task.updated_at = Utc::now().naive_utc();
                cache_task.failed_at = Some(Utc::now().naive_utc());
                cache_task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &cache_task)?;
        Ok(cache_task)
    }

    // upload_cache_task_started updates the metadata of the cache task when cache task uploads started.
    pub fn upload_cache_task_started(&self, id: &str) -> Result<CacheTask> {
        let cache_task = match self.db.get::<CacheTask>(id.as_bytes())? {
            Some(mut cache_task) => {
                cache_task.uploading_count += 1;
                cache_task.updated_at = Utc::now().naive_utc();
                cache_task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &cache_task)?;
        Ok(cache_task)
    }

    // upload_cache_task_finished updates the metadata of the cache task when cache task uploads finished.
    pub fn upload_cache_task_finished(&self, id: &str) -> Result<CacheTask> {
        let cache_task = match self.db.get::<CacheTask>(id.as_bytes())? {
            Some(mut cache_task) => {
                cache_task.uploading_count -= 1;
                cache_task.uploaded_count += 1;
                cache_task.updated_at = Utc::now().naive_utc();
                cache_task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &cache_task)?;
        Ok(cache_task)
    }

    // upload_cache_task_failed updates the metadata of the cache task when the cache task uploads failed.
    pub fn upload_cache_task_failed(&self, id: &str) -> Result<CacheTask> {
        let cache_task = match self.db.get::<CacheTask>(id.as_bytes())? {
            Some(mut cache_task) => {
                cache_task.uploading_count -= 1;
                cache_task.updated_at = Utc::now().naive_utc();
                cache_task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &cache_task)?;
        Ok(cache_task)
    }

    // get_cache_task gets the cache task metadata.
    pub fn get_cache_task(&self, id: &str) -> Result<Option<CacheTask>> {
        self.db.get(id.as_bytes())
    }

    // get_cache_tasks gets the cache task metadatas.
    pub fn get_cache_tasks(&self) -> Result<Vec<CacheTask>> {
        let iter = self.db.iter::<CacheTask>()?;
        iter.map(|ele| ele.map(|(_, cache_task)| cache_task))
            .collect()
    }

    // delete_cache_task deletes the cache task metadata.
    pub fn delete_cache_task(&self, id: &str) -> Result<()> {
        info!("delete cache task metadata {}", id);
        self.db.delete::<CacheTask>(id.as_bytes())
    }

    // download_piece_started updates the metadata of the piece when the piece downloads started.
    pub fn download_piece_started(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Construct the piece metadata.
        let piece = Piece {
            number,
            updated_at: Utc::now().naive_utc(),
            created_at: Utc::now().naive_utc(),
            ..Default::default()
        };

        // Put the piece metadata.
        self.db
            .put(self.piece_id(task_id, number).as_bytes(), &piece)?;
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
        // Get the piece id.
        let id = self.piece_id(task_id, number);
        let piece = match self.db.get::<Piece>(id.as_bytes())? {
            Some(mut piece) => {
                piece.offset = offset;
                piece.length = length;
                piece.digest = digest.to_string();
                piece.parent_id = parent_id;
                piece.updated_at = Utc::now().naive_utc();
                piece.finished_at = Some(Utc::now().naive_utc());
                piece
            }
            None => return Err(Error::PieceNotFound(id)),
        };

        self.db.put(id.as_bytes(), &piece)?;
        Ok(piece)
    }

    // download_piece_failed updates the metadata of the piece when the piece downloads failed.
    pub fn download_piece_failed(&self, task_id: &str, number: u32) -> Result<()> {
        self.delete_piece(task_id, number)
    }

    // wait_for_piece_finished_failed waits for the piece to be finished or failed.
    pub fn wait_for_piece_finished_failed(&self, task_id: &str, number: u32) -> Result<()> {
        self.delete_piece(task_id, number)
    }

    // upload_piece_started updates the metadata of the piece when piece uploads started.
    pub fn upload_piece_started(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Get the piece id.
        let id = self.piece_id(task_id, number);
        let piece = match self.db.get::<Piece>(id.as_bytes())? {
            Some(mut piece) => {
                piece.uploading_count += 1;
                piece.updated_at = Utc::now().naive_utc();
                piece
            }
            None => return Err(Error::PieceNotFound(id)),
        };

        self.db.put(id.as_bytes(), &piece)?;
        Ok(piece)
    }

    // upload_piece_finished updates the metadata of the piece when piece uploads finished.
    pub fn upload_piece_finished(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Get the piece id.
        let id = self.piece_id(task_id, number);
        let piece = match self.db.get::<Piece>(id.as_bytes())? {
            Some(mut piece) => {
                piece.uploading_count -= 1;
                piece.uploaded_count += 1;
                piece.updated_at = Utc::now().naive_utc();
                piece
            }
            None => return Err(Error::PieceNotFound(id)),
        };

        self.db.put(id.as_bytes(), &piece)?;
        Ok(piece)
    }

    // upload_piece_failed updates the metadata of the piece when the piece uploads failed.
    pub fn upload_piece_failed(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Get the piece id.
        let id = self.piece_id(task_id, number);
        let piece = match self.db.get::<Piece>(id.as_bytes())? {
            Some(mut piece) => {
                piece.uploading_count -= 1;
                piece.updated_at = Utc::now().naive_utc();
                piece
            }
            None => return Err(Error::PieceNotFound(id)),
        };

        self.db.put(id.as_bytes(), &piece)?;
        Ok(piece)
    }

    // get_piece gets the piece metadata.
    pub fn get_piece(&self, task_id: &str, number: u32) -> Result<Option<Piece>> {
        self.db.get(self.piece_id(task_id, number).as_bytes())
    }

    // get_pieces gets the piece metadatas.
    pub fn get_pieces(&self, task_id: &str) -> Result<Vec<Piece>> {
        let iter = self.db.prefix_iter::<Piece>(task_id.as_bytes())?;
        iter.map(|ele| ele.map(|(_, piece)| piece)).collect()
    }

    // delete_piece deletes the piece metadata.
    pub fn delete_piece(&self, task_id: &str, number: u32) -> Result<()> {
        info!("delete piece metadata {}", self.piece_id(task_id, number));
        self.db
            .delete::<Piece>(self.piece_id(task_id, number).as_bytes())
    }

    // delete_pieces deletes the piece metadatas.
    pub fn delete_pieces(&self, task_id: &str) -> Result<()> {
        let iter = self.db.prefix_iter::<Piece>(task_id.as_bytes())?;
        for ele in iter {
            let (key, _) = ele?;
            self.db.delete::<Piece>(&key)?;
        }

        Ok(())
    }

    // piece_id returns the piece id.
    pub fn piece_id(&self, task_id: &str, number: u32) -> String {
        format!("{}-{}", task_id, number)
    }
}

impl Metadata<RocksdbStorageEngine> {
    // new creates a new metadata instance.
    pub fn new(config: Arc<Config>, dir: &Path) -> Result<Metadata<RocksdbStorageEngine>> {
        let db = RocksdbStorageEngine::open(
            dir,
            &[Task::NAMESPACE, Piece::NAMESPACE],
            config.storage.keep,
        )?;

        Ok(Metadata { db })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempdir::TempDir;

    #[test]
    fn should_create_metadata_db() {
        let dir = TempDir::new("metadata_db").unwrap();
        let metadata = Metadata::new(Arc::new(Config::default()), dir.path()).unwrap();
        assert!(metadata.get_tasks().unwrap().is_empty());
        assert!(metadata.get_pieces("task").unwrap().is_empty());
    }

    #[test]
    fn test_task_lifecycle() {
        let dir = TempDir::new("metadata_db").unwrap();
        let metadata = Metadata::new(Arc::new(Config::default()), dir.path()).unwrap();

        let task_id = "task1";

        // Test download_task_started.
        metadata
            .download_task_started(task_id, 1024, Some(1024), None)
            .unwrap();
        let task = metadata
            .get_task(task_id)
            .unwrap()
            .expect("task should exist after download_task_started");
        assert_eq!(task.id, task_id);
        assert_eq!(task.piece_length, 1024);
        assert_eq!(task.content_length, Some(1024));
        assert!(task.response_header.is_empty());
        assert_eq!(task.uploading_count, 0);
        assert_eq!(task.uploaded_count, 0);

        // Test download_task_finished.
        metadata.download_task_finished(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert!(
            task.is_finished(),
            "task should be finished after download_task_finished"
        );

        // Test upload_task_started.
        metadata.upload_task_started(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert_eq!(
            task.uploading_count, 1,
            "uploading_count should be increased by 1 after upload_task_started"
        );

        // Test upload_task_finished.
        metadata.upload_task_finished(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert_eq!(
            task.uploading_count, 0,
            "uploading_count should be decreased by 1 after upload_task_finished"
        );
        assert_eq!(
            task.uploaded_count, 1,
            "uploaded_count should be increased by 1 after upload_task_finished"
        );

        // Test upload_task_failed.
        let task = metadata.upload_task_started(task_id).unwrap();
        assert_eq!(task.uploading_count, 1);
        let task = metadata.upload_task_failed(task_id).unwrap();
        assert_eq!(
            task.uploading_count, 0,
            "uploading_count should be decreased by 1 after upload_task_failed"
        );
        assert_eq!(
            task.uploaded_count, 1,
            "uploaded_count should not be changed after upload_task_failed"
        );

        // Test get_tasks.
        let task_id = "task2";

        metadata
            .download_task_started(task_id, 1024, None, None)
            .unwrap();
        let tasks = metadata.get_tasks().unwrap();
        assert_eq!(tasks.len(), 2, "should get 2 tasks in total");

        // Test delete_task.
        metadata.delete_task(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap();
        assert!(task.is_none(), "task should be deleted after delete_task");
    }

    #[test]
    fn test_piece_lifecycle() {
        let dir = TempDir::new("metadata_db").unwrap();
        let metadata = Metadata::new(Arc::new(Config::default()), dir.path()).unwrap();
        let task_id = "task3";

        // Test download_piece_started.
        metadata.download_piece_started(task_id, 1).unwrap();
        let piece = metadata.get_piece(task_id, 1).unwrap().unwrap();
        assert_eq!(
            piece.number, 1,
            "should get newly created piece with number specified"
        );

        // Test download_piece_finished.
        metadata
            .download_piece_finished(task_id, 1, 0, 1024, "digest1", None)
            .unwrap();
        let piece = metadata.get_piece(task_id, 1).unwrap().unwrap();
        assert_eq!(
            piece.length, 1024,
            "piece should be updated after download_piece_finished"
        );
        assert_eq!(
            piece.digest, "digest1",
            "piece should be updated after download_piece_finished"
        );

        // Test get_pieces.
        metadata.download_piece_started(task_id, 2).unwrap();
        metadata.download_piece_started(task_id, 3).unwrap();
        let pieces = metadata.get_pieces(task_id).unwrap();
        assert_eq!(pieces.len(), 3, "should get 3 pieces in total");

        // Test download_piece_failed.
        metadata.download_piece_failed(task_id, 2).unwrap();
        let piece = metadata.get_piece(task_id, 2).unwrap();
        assert!(
            piece.is_none(),
            "piece should be deleted after download_piece_failed"
        );

        // Test upload_piece_started.
        metadata.upload_piece_started(task_id, 3).unwrap();
        let piece = metadata.get_piece(task_id, 3).unwrap().unwrap();
        assert_eq!(
            piece.uploading_count, 1,
            "piece should be updated after upload_piece_started"
        );

        // Test upload_piece_finished.
        metadata.upload_piece_finished(task_id, 3).unwrap();
        let piece = metadata.get_piece(task_id, 3).unwrap().unwrap();
        assert_eq!(
            piece.uploading_count, 0,
            "piece should be updated after upload_piece_finished"
        );
        assert_eq!(
            piece.uploaded_count, 1,
            "piece should be updated after upload_piece_finished"
        );

        // Test upload_piece_failed.
        metadata.upload_piece_started(task_id, 3).unwrap();
        metadata.upload_piece_failed(task_id, 3).unwrap();
        let piece = metadata.get_piece(task_id, 3).unwrap().unwrap();
        assert_eq!(
            piece.uploading_count, 0,
            "piece should be updated after upload_piece_failed"
        );

        // Test delete_pieces.
        metadata.delete_pieces(task_id).unwrap();
        let pieces = metadata.get_pieces(task_id).unwrap();
        assert!(pieces.is_empty(), "should get 0 pieces after delete_pieces");
    }
}
