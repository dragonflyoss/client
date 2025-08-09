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
use dragonfly_client_util::{digest, http::headermap_to_hashmap};
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, instrument};

use crate::storage_engine::{rocksdb::RocksdbStorageEngine, DatabaseObject, StorageEngineOwned};

/// Task is the metadata of the task.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    /// id is the task id.
    pub id: String,

    /// piece_length is the length of the piece.
    pub piece_length: Option<u64>,

    /// content_length is the length of the content.
    pub content_length: Option<u64>,

    /// header is the header of the response.
    pub response_header: HashMap<String, String>,

    /// uploading_count is the count of the task being uploaded by other peers.
    pub uploading_count: i64,

    /// uploaded_count is the count of the task has been uploaded by other peers.
    pub uploaded_count: u64,

    /// updated_at is the time when the task metadata is updated. If the task is downloaded
    /// by other peers, it will also update updated_at.
    pub updated_at: NaiveDateTime,

    /// created_at is the time when the task metadata is created.
    pub created_at: NaiveDateTime,

    /// prefetched_at is the time when the task prefetched.
    pub prefetched_at: Option<NaiveDateTime>,

    /// failed_at is the time when the task downloads failed.
    pub failed_at: Option<NaiveDateTime>,

    /// finished_at is the time when the task downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

/// Task implements the task database object.
impl DatabaseObject for Task {
    /// NAMESPACE is the namespace of [Task] objects.
    const NAMESPACE: &'static str = "task";
}

/// Task implements the task metadata.
impl Task {
    /// is_started returns whether the task downloads started.
    pub fn is_started(&self) -> bool {
        self.finished_at.is_none()
    }

    /// is_uploading returns whether the task is uploading.
    pub fn is_uploading(&self) -> bool {
        self.uploading_count > 0
    }

    /// is_expired returns whether the task is expired.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.updated_at + ttl < Utc::now().naive_utc()
    }

    /// is_prefetched returns whether the task is prefetched.
    pub fn is_prefetched(&self) -> bool {
        self.prefetched_at.is_some()
    }

    /// is_failed returns whether the task downloads failed.
    pub fn is_failed(&self) -> bool {
        self.failed_at.is_some()
    }

    /// is_finished returns whether the task downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }

    /// is_empty returns whether the task is empty.
    pub fn is_empty(&self) -> bool {
        match self.content_length() {
            Some(content_length) => content_length == 0,
            None => false,
        }
    }

    /// piece_length returns the piece length of the task.
    pub fn piece_length(&self) -> Option<u64> {
        self.piece_length
    }

    /// content_length returns the content length of the task.
    pub fn content_length(&self) -> Option<u64> {
        self.content_length
    }
}

/// PersistentCacheTask is the metadata of the persistent cache task.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentCacheTask {
    /// id is the task id.
    pub id: String,

    /// persistent represents whether the persistent cache task is persistent.
    /// If the persistent cache task is persistent, the persistent cache peer will
    /// not be deleted when dfdamon runs garbage collection.
    pub persistent: bool,

    /// ttl is the time to live of the persistent cache task.
    pub ttl: Duration,

    /// piece_length is the length of the piece.
    pub piece_length: u64,

    /// content_length is the length of the content.
    pub content_length: u64,

    /// uploading_count is the count of the task being uploaded by other peers.
    pub uploading_count: u64,

    /// uploaded_count is the count of the task has been uploaded by other peers.
    pub uploaded_count: u64,

    /// updated_at is the time when the task metadata is updated. If the task is downloaded
    /// by other peers, it will also update updated_at.
    pub updated_at: NaiveDateTime,

    /// created_at is the time when the task metadata is created.
    pub created_at: NaiveDateTime,

    /// failed_at is the time when the task downloads failed.
    pub failed_at: Option<NaiveDateTime>,

    /// finished_at is the time when the task downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

/// PersistentCacheTask implements the persistent cache task database object.
impl DatabaseObject for PersistentCacheTask {
    /// NAMESPACE is the namespace of [PersistentCacheTask] objects.
    const NAMESPACE: &'static str = "persistent_cache_task";
}

/// PersistentCacheTask implements the persistent cache task metadata.
impl PersistentCacheTask {
    /// is_started returns whether the persistent cache task downloads started.
    pub fn is_started(&self) -> bool {
        self.finished_at.is_none()
    }

    /// is_uploading returns whether the persistent cache task is uploading.
    pub fn is_uploading(&self) -> bool {
        self.uploading_count > 0
    }

    /// is_expired returns whether the persistent cache task is expired.
    pub fn is_expired(&self) -> bool {
        self.created_at + self.ttl < Utc::now().naive_utc()
    }

    /// is_failed returns whether the persistent cache task downloads failed.
    pub fn is_failed(&self) -> bool {
        self.failed_at.is_some()
    }

    /// is_finished returns whether the persistent cache task downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }

    /// is_empty returns whether the persistent cache task is empty.
    pub fn is_empty(&self) -> bool {
        self.content_length == 0
    }

    /// is_persistent returns whether the persistent cache task is persistent.
    pub fn is_persistent(&self) -> bool {
        self.persistent
    }

    /// piece_length returns the piece length of the persistent cache task.
    pub fn piece_length(&self) -> u64 {
        self.piece_length
    }

    /// content_length returns the content length of the persistent cache task.
    pub fn content_length(&self) -> u64 {
        self.content_length
    }
}

/// Piece is the metadata of the piece.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Piece {
    /// number is the piece number.
    pub number: u32,

    /// offset is the offset of the piece in the task.
    pub offset: u64,

    /// length is the length of the piece.
    pub length: u64,

    /// digest is the digest of the piece.
    pub digest: String,

    /// parent_id is the parent id of the piece.
    pub parent_id: Option<String>,

    /// DEPRECATED: uploading_count is the count of the piece being uploaded by other peers.
    pub uploading_count: i64,

    /// DEPRECATED: uploaded_count is the count of the piece has been uploaded by other peers.
    pub uploaded_count: u64,

    /// updated_at is the time when the piece metadata is updated.
    pub updated_at: NaiveDateTime,

    /// created_at is the time when the piece metadata is created.
    pub created_at: NaiveDateTime,

    /// finished_at is the time when the piece downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

/// Piece implements the piece database object.
impl DatabaseObject for Piece {
    /// NAMESPACE is the namespace of [Piece] objects.
    const NAMESPACE: &'static str = "piece";
}

/// Piece implements the piece metadata.
impl Piece {
    /// is_started returns whether the piece downloads started.
    pub fn is_started(&self) -> bool {
        self.finished_at.is_none()
    }

    /// is_finished returns whether the piece downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }

    /// cost returns the cost of the piece downloaded.
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

    /// prost_cost returns the prost cost of the piece downloaded.
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

    /// calculate_digest return the digest of the piece metadata, including the piece number,
    /// offset, length and content digest. The digest is used to check the integrity of the
    /// piece metadata.
    pub fn calculate_digest(&self) -> String {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.number.to_be_bytes());
        hasher.update(&self.offset.to_be_bytes());
        hasher.update(&self.length.to_be_bytes());
        hasher.update(self.digest.as_bytes());

        let encoded = hasher.finalize().to_string();
        digest::Digest::new(digest::Algorithm::Crc32, encoded).to_string()
    }
}

/// Metadata manages the metadata of [Task], [Piece] and [PersistentCacheTask].
pub struct Metadata<E = RocksdbStorageEngine>
where
    E: StorageEngineOwned,
{
    /// db is the underlying storage engine instance.
    db: E,
}

impl<E: StorageEngineOwned> Metadata<E> {
    /// download_task_started updates the metadata of the task when the task downloads started.
    #[instrument(skip_all)]
    pub fn download_task_started(
        &self,
        id: &str,
        piece_length: Option<u64>,
        content_length: Option<u64>,
        response_header: Option<HeaderMap>,
    ) -> Result<Task> {
        // Convert the response header to hashmap.
        let response_header = response_header
            .as_ref()
            .map(headermap_to_hashmap)
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

                // Protect piece length to be overwritten by None.
                if piece_length.is_some() {
                    task.piece_length = piece_length;
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

    /// download_task_finished updates the metadata of the task when the task downloads finished.
    #[instrument(skip_all)]
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

    /// download_task_failed updates the metadata of the task when the task downloads failed.
    #[instrument(skip_all)]
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

    /// prefetch_task_started updates the metadata of the task when the task prefetch started.
    #[instrument(skip_all)]
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

    /// prefetch_task_failed updates the metadata of the task when the task prefetch failed.
    #[instrument(skip_all)]
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

    /// upload_task_started updates the metadata of the task when task uploads started.
    #[instrument(skip_all)]
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

    /// upload_task_finished updates the metadata of the task when task uploads finished.
    #[instrument(skip_all)]
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

    /// upload_task_failed updates the metadata of the task when the task uploads failed.
    #[instrument(skip_all)]
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

    /// get_task gets the task metadata.
    #[instrument(skip_all)]
    pub fn get_task(&self, id: &str) -> Result<Option<Task>> {
        self.db.get(id.as_bytes())
    }

    /// is_task_exists checks if the task exists.
    #[instrument(skip_all)]
    pub fn is_task_exists(&self, id: &str) -> Result<bool> {
        self.db.is_exist::<Task>(id.as_bytes())
    }

    /// get_tasks gets the task metadatas.
    #[instrument(skip_all)]
    pub fn get_tasks(&self) -> Result<Vec<Task>> {
        let tasks = self
            .db
            .iter_raw::<Task>()?
            .map(|ele| {
                let (_, value) = ele?;
                Ok(value)
            })
            .collect::<Result<Vec<Box<[u8]>>>>()?;

        tasks
            .iter()
            .map(|task| Task::deserialize_from(task))
            .collect()
    }

    /// delete_task deletes the task metadata.
    #[instrument(skip_all)]
    pub fn delete_task(&self, id: &str) -> Result<()> {
        info!("delete task metadata {}", id);
        self.db.delete::<Task>(id.as_bytes())
    }

    /// create_persistent_cache_task creates a new persistent cache task.
    #[instrument(skip_all)]
    pub fn create_persistent_cache_task_started(
        &self,
        id: &str,
        ttl: Duration,
        piece_length: u64,
        content_length: u64,
    ) -> Result<PersistentCacheTask> {
        let task = PersistentCacheTask {
            id: id.to_string(),
            persistent: true,
            ttl,
            piece_length,
            content_length,
            updated_at: Utc::now().naive_utc(),
            created_at: Utc::now().naive_utc(),
            ..Default::default()
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    /// create_persistent_cache_task_finished updates the metadata of the persistent cache task
    /// when the persistent cache task finished.
    #[instrument(skip_all)]
    pub fn create_persistent_cache_task_finished(&self, id: &str) -> Result<PersistentCacheTask> {
        let task = match self.db.get::<PersistentCacheTask>(id.as_bytes())? {
            Some(mut task) => {
                task.updated_at = Utc::now().naive_utc();
                task.failed_at = None;

                // If the persistent cache task is created by user, the finished_at has been set.
                if task.finished_at.is_none() {
                    task.finished_at = Some(Utc::now().naive_utc());
                }

                task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    /// download_persistent_cache_task_started updates the metadata of the persistent cache task when
    /// the persistent cache task downloads started. If the persistent cache task downloaded by scheduler
    /// to create persistent cache task, the persistent should be set to true.
    #[instrument(skip_all)]
    pub fn download_persistent_cache_task_started(
        &self,
        id: &str,
        ttl: Duration,
        persistent: bool,
        piece_length: u64,
        content_length: u64,
        created_at: NaiveDateTime,
    ) -> Result<PersistentCacheTask> {
        let task = match self.db.get::<PersistentCacheTask>(id.as_bytes())? {
            Some(mut task) => {
                // If the task exists, update the task metadata.
                task.ttl = ttl;
                task.persistent = persistent;
                task.piece_length = piece_length;
                task.updated_at = Utc::now().naive_utc();
                task.failed_at = None;
                task
            }
            None => PersistentCacheTask {
                id: id.to_string(),
                persistent,
                ttl,
                piece_length,
                content_length,
                updated_at: Utc::now().naive_utc(),
                created_at,
                ..Default::default()
            },
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    /// download_persistent_cache_task_finished updates the metadata of the persistent cache task when the persistent cache task downloads finished.
    #[instrument(skip_all)]
    pub fn download_persistent_cache_task_finished(&self, id: &str) -> Result<PersistentCacheTask> {
        let task = match self.db.get::<PersistentCacheTask>(id.as_bytes())? {
            Some(mut task) => {
                task.updated_at = Utc::now().naive_utc();
                task.failed_at = None;

                // If the persistent cache task is created by user, the finished_at has been set.
                if task.finished_at.is_none() {
                    task.finished_at = Some(Utc::now().naive_utc());
                }

                task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    /// download_persistent_cache_task_failed updates the metadata of the persistent cache task when the persistent cache task downloads failed.
    #[instrument(skip_all)]
    pub fn download_persistent_cache_task_failed(&self, id: &str) -> Result<PersistentCacheTask> {
        let task = match self.db.get::<PersistentCacheTask>(id.as_bytes())? {
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

    /// upload_persistent_cache_task_started updates the metadata of the persistent cache task when persistent cache task uploads started.
    #[instrument(skip_all)]
    pub fn upload_persistent_cache_task_started(&self, id: &str) -> Result<PersistentCacheTask> {
        let task = match self.db.get::<PersistentCacheTask>(id.as_bytes())? {
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

    /// upload_persistent_cache_task_finished updates the metadata of the persistent cache task when persistent cache task uploads finished.
    #[instrument(skip_all)]
    pub fn upload_persistent_cache_task_finished(&self, id: &str) -> Result<PersistentCacheTask> {
        let task = match self.db.get::<PersistentCacheTask>(id.as_bytes())? {
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

    /// upload_persistent_cache_task_failed updates the metadata of the persistent cache task when the persistent cache task uploads failed.
    #[instrument(skip_all)]
    pub fn upload_persistent_cache_task_failed(&self, id: &str) -> Result<PersistentCacheTask> {
        let task = match self.db.get::<PersistentCacheTask>(id.as_bytes())? {
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

    /// persist_persistent_cache_task persists the persistent cache task metadata.
    #[instrument(skip_all)]
    pub fn persist_persistent_cache_task(&self, id: &str) -> Result<PersistentCacheTask> {
        let task = match self.db.get::<PersistentCacheTask>(id.as_bytes())? {
            Some(mut task) => {
                task.persistent = true;
                task.updated_at = Utc::now().naive_utc();
                task
            }
            None => return Err(Error::TaskNotFound(id.to_string())),
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    /// get_persistent_cache_task gets the persistent cache task metadata.
    #[instrument(skip_all)]
    pub fn get_persistent_cache_task(&self, id: &str) -> Result<Option<PersistentCacheTask>> {
        self.db.get(id.as_bytes())
    }

    /// is_persistent_cache_task_exists checks if the persistent cache task exists.
    #[instrument(skip_all)]
    pub fn is_persistent_cache_task_exists(&self, id: &str) -> Result<bool> {
        self.db.is_exist::<PersistentCacheTask>(id.as_bytes())
    }

    /// get_persistent_cache_tasks gets the persistent cache task metadatas.
    #[instrument(skip_all)]
    pub fn get_persistent_cache_tasks(&self) -> Result<Vec<PersistentCacheTask>> {
        let iter = self.db.iter::<PersistentCacheTask>()?;
        iter.map(|ele| ele.map(|(_, task)| task)).collect()
    }

    /// delete_persistent_cache_task deletes the persistent cache task metadata.
    #[instrument(skip_all)]
    pub fn delete_persistent_cache_task(&self, id: &str) -> Result<()> {
        info!("delete persistent cache task metadata {}", id);
        self.db.delete::<PersistentCacheTask>(id.as_bytes())
    }

    /// create_persistent_cache_piece creates a new persistent cache piece, which is imported by
    /// local.
    #[instrument(skip_all)]
    pub fn create_persistent_cache_piece(
        &self,
        piece_id: &str,
        number: u32,
        offset: u64,
        length: u64,
        digest: &str,
    ) -> Result<Piece> {
        // Construct the piece metadata.
        let piece = Piece {
            number,
            offset,
            length,
            digest: digest.to_string(),
            // Persistent cache piece does not have parent id, because the piece content is
            // imported by local.
            parent_id: None,
            updated_at: Utc::now().naive_utc(),
            created_at: Utc::now().naive_utc(),
            finished_at: Some(Utc::now().naive_utc()),
            ..Default::default()
        };

        // Put the piece metadata.
        self.db.put(piece_id.as_bytes(), &piece)?;
        Ok(piece)
    }

    /// download_piece_started updates the metadata of the piece when the piece downloads started.
    #[instrument(skip_all)]
    pub fn download_piece_started(&self, piece_id: &str, number: u32) -> Result<Piece> {
        // Construct the piece metadata.
        let piece = Piece {
            number,
            updated_at: Utc::now().naive_utc(),
            created_at: Utc::now().naive_utc(),
            ..Default::default()
        };

        // Put the piece metadata.
        self.db.put(piece_id.as_bytes(), &piece)?;
        Ok(piece)
    }

    /// download_piece_finished updates the metadata of the piece when the piece downloads finished.
    #[instrument(skip_all)]
    pub fn download_piece_finished(
        &self,
        piece_id: &str,
        offset: u64,
        length: u64,
        digest: &str,
        parent_id: Option<String>,
    ) -> Result<Piece> {
        let piece = match self.db.get::<Piece>(piece_id.as_bytes())? {
            Some(mut piece) => {
                piece.offset = offset;
                piece.length = length;
                piece.digest = digest.to_string();
                piece.parent_id = parent_id;
                piece.updated_at = Utc::now().naive_utc();
                piece.finished_at = Some(Utc::now().naive_utc());
                piece
            }
            None => return Err(Error::PieceNotFound(piece_id.to_string())),
        };

        self.db.put(piece_id.as_bytes(), &piece)?;
        Ok(piece)
    }

    /// download_piece_failed updates the metadata of the piece when the piece downloads failed.
    #[instrument(skip_all)]
    pub fn download_piece_failed(&self, piece_id: &str) -> Result<()> {
        self.delete_piece(piece_id)
    }

    /// wait_for_piece_finished_failed waits for the piece to be finished or failed.
    #[instrument(skip_all)]
    pub fn wait_for_piece_finished_failed(&self, piece_id: &str) -> Result<()> {
        self.delete_piece(piece_id)
    }

    /// get_piece gets the piece metadata.
    pub fn get_piece(&self, piece_id: &str) -> Result<Option<Piece>> {
        self.db.get(piece_id.as_bytes())
    }

    /// is_piece_exists checks if the piece exists.
    #[instrument(skip_all)]
    pub fn is_piece_exists(&self, piece_id: &str) -> Result<bool> {
        self.db.is_exist::<Piece>(piece_id.as_bytes())
    }

    /// get_pieces gets the piece metadatas.
    #[instrument(skip_all)]
    pub fn get_pieces(&self, task_id: &str) -> Result<Vec<Piece>> {
        let pieces = self
            .db
            .prefix_iter_raw::<Piece>(task_id.as_bytes())?
            .map(|ele| {
                let (_, value) = ele?;
                Ok(value)
            })
            .collect::<Result<Vec<Box<[u8]>>>>()?;

        info!("query for task [{}] found {} pieces", task_id, pieces.len());

        pieces
            .iter()
            .map(|piece| Piece::deserialize_from(piece))
            .collect()
    }

    /// delete_piece deletes the piece metadata.
    #[instrument(skip_all)]
    pub fn delete_piece(&self, piece_id: &str) -> Result<()> {
        info!("delete piece metadata {}", piece_id);
        self.db.delete::<Piece>(piece_id.as_bytes())
    }

    /// delete_pieces deletes the piece metadatas.
    #[instrument(skip_all)]
    pub fn delete_pieces(&self, task_id: &str) -> Result<()> {
        let piece_ids = self
            .db
            .prefix_iter_raw::<Piece>(task_id.as_bytes())?
            .map(|ele| {
                let (key, _) = ele?;
                Ok(key)
            })
            .collect::<Result<Vec<Box<[u8]>>>>()?;

        let piece_ids_refs = piece_ids
            .iter()
            .map(|id| {
                let id_ref = id.as_ref();
                info!(
                    "delete piece metadata {} in batch",
                    std::str::from_utf8(id_ref).unwrap_or_default(),
                );

                id_ref
            })
            .collect::<Vec<&[u8]>>();

        self.db.batch_delete::<Piece>(piece_ids_refs)?;
        Ok(())
    }

    /// piece_id returns the piece id.
    #[inline]
    pub fn piece_id(&self, task_id: &str, number: u32) -> String {
        format!("{}-{}", task_id, number)
    }
}

/// Metadata implements the metadata of the storage engine.
impl Metadata<RocksdbStorageEngine> {
    /// new creates a new metadata instance.
    #[instrument(skip_all)]
    pub fn new(
        config: Arc<Config>,
        dir: &Path,
        log_dir: &PathBuf,
    ) -> Result<Metadata<RocksdbStorageEngine>> {
        let db = RocksdbStorageEngine::open(
            dir,
            log_dir,
            &[
                Task::NAMESPACE,
                Piece::NAMESPACE,
                PersistentCacheTask::NAMESPACE,
            ],
            config.storage.keep,
        )?;

        Ok(Metadata { db })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_calculate_digest() {
        let piece = Piece {
            number: 1,
            offset: 0,
            length: 1024,
            digest: "crc32:1929153120".to_string(),
            ..Default::default()
        };

        let digest = piece.calculate_digest();
        assert_eq!(digest, "crc32:3299754941");
    }

    #[test]
    fn should_create_metadata() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path().join("log");
        let metadata = Metadata::new(Arc::new(Config::default()), dir.path(), &log_dir).unwrap();
        assert!(metadata.get_tasks().unwrap().is_empty());
        assert!(metadata
            .get_pieces("d3c4e940ad06c47fc36ac67801e6f8e36cb400e2391708620bc7e865b102062c")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_task_lifecycle() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path().join("log");
        let metadata = Metadata::new(Arc::new(Config::default()), dir.path(), &log_dir).unwrap();
        let task_id = "d3c4e940ad06c47fc36ac67801e6f8e36cb400e2391708620bc7e865b102062c";

        // Test download_task_started.
        metadata
            .download_task_started(task_id, Some(1024), Some(1024), None)
            .unwrap();
        let task = metadata
            .get_task(task_id)
            .unwrap()
            .expect("task should exist after download_task_started");
        assert_eq!(task.id, task_id);
        assert_eq!(task.piece_length, Some(1024));
        assert_eq!(task.content_length, Some(1024));
        assert!(task.response_header.is_empty());
        assert_eq!(task.uploading_count, 0);
        assert_eq!(task.uploaded_count, 0);
        assert!(!task.is_finished());

        // Test download_task_finished.
        metadata.download_task_finished(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert!(task.is_finished());

        // Test upload_task_started.
        metadata.upload_task_started(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert_eq!(task.uploading_count, 1);

        // Test upload_task_finished.
        metadata.upload_task_finished(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert_eq!(task.uploading_count, 0);
        assert_eq!(task.uploaded_count, 1);

        // Test upload_task_failed.
        let task = metadata.upload_task_started(task_id).unwrap();
        assert_eq!(task.uploading_count, 1);
        let task = metadata.upload_task_failed(task_id).unwrap();
        assert_eq!(task.uploading_count, 0);
        assert_eq!(task.uploaded_count, 1);

        // Test get_tasks.
        let task_id = "a535b115f18d96870f0422ac891f91dd162f2f391e4778fb84279701fcd02dd1";
        metadata
            .download_task_started(task_id, Some(1024), None, None)
            .unwrap();
        let tasks = metadata.get_tasks().unwrap();
        assert_eq!(tasks.len(), 2);

        // Test delete_task.
        metadata.delete_task(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap();
        assert!(task.is_none());
    }

    #[test]
    fn test_piece_lifecycle() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path().join("log");
        let metadata = Metadata::new(Arc::new(Config::default()), dir.path(), &log_dir).unwrap();
        let task_id = "d3c4e940ad06c47fc36ac67801e6f8e36cb400e2391708620bc7e865b102062c";
        let piece_id = metadata.piece_id(task_id, 1);

        // Test download_piece_started.
        metadata
            .download_piece_started(piece_id.as_str(), 1)
            .unwrap();
        let piece = metadata.get_piece(piece_id.as_str()).unwrap().unwrap();
        assert_eq!(piece.number, 1);

        // Test download_piece_finished.
        metadata
            .download_piece_finished(piece_id.as_str(), 0, 1024, "digest1", None)
            .unwrap();
        let piece = metadata.get_piece(piece_id.as_str()).unwrap().unwrap();
        assert_eq!(piece.length, 1024);
        assert_eq!(piece.digest, "digest1");

        // Test get_pieces.
        metadata
            .download_piece_started(metadata.piece_id(task_id, 2).as_str(), 2)
            .unwrap();
        metadata
            .download_piece_started(metadata.piece_id(task_id, 3).as_str(), 3)
            .unwrap();
        let pieces = metadata.get_pieces(task_id).unwrap();
        assert_eq!(pieces.len(), 3);

        // Test download_piece_failed.
        let piece_id = metadata.piece_id(task_id, 2);
        metadata
            .download_piece_started(piece_id.as_str(), 2)
            .unwrap();
        metadata
            .download_piece_started(metadata.piece_id(task_id, 3).as_str(), 3)
            .unwrap();
        metadata.download_piece_failed(piece_id.as_str()).unwrap();
        let piece = metadata.get_piece(piece_id.as_str()).unwrap();
        assert!(piece.is_none());

        // Test delete_pieces.
        metadata.delete_pieces(task_id).unwrap();
        let pieces = metadata.get_pieces(task_id).unwrap();
        assert!(pieces.is_empty());
    }
}
