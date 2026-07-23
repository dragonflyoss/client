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
use dashmap::DashMap;
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

/// The metadata of the task.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    /// The task id.
    pub id: String,

    /// The length of the piece.
    pub piece_length: Option<u64>,

    /// The length of the content.
    pub content_length: Option<u64>,

    /// The header of the response.
    pub response_header: HashMap<String, String>,

    /// The count of the task being uploaded by other peers.
    pub uploading_count: i64,

    /// The count of the task has been uploaded by other peers.
    pub uploaded_count: u64,

    /// The time when the task metadata is updated. If the task is downloaded
    /// by other peers, it will also update updated_at.
    pub updated_at: NaiveDateTime,

    /// The time when the task metadata is created.
    pub created_at: NaiveDateTime,

    /// The time when the task prefetched.
    pub prefetched_at: Option<NaiveDateTime>,

    /// The time when the task downloads failed.
    pub failed_at: Option<NaiveDateTime>,

    /// The time when the task downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

/// Implements the task database object.
impl DatabaseObject for Task {
    /// The namespace of [Task] objects.
    const NAMESPACE: &'static str = "task";
}

/// Implements the task metadata.
impl Task {
    /// Returns whether the task downloads started.
    pub fn is_started(&self) -> bool {
        self.finished_at.is_none()
    }

    /// Returns whether the task is uploading.
    pub fn is_uploading(&self) -> bool {
        self.uploading_count > 0
    }

    /// Returns whether the task is expired.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.updated_at + ttl < Utc::now().naive_utc()
    }

    /// Returns whether the task is prefetched.
    pub fn is_prefetched(&self) -> bool {
        self.prefetched_at.is_some()
    }

    /// Returns whether the task downloads failed.
    pub fn is_failed(&self) -> bool {
        self.failed_at.is_some()
    }

    /// Returns whether the task downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }

    /// Returns whether the task is empty.
    pub fn is_empty(&self) -> bool {
        match self.content_length() {
            Some(content_length) => content_length == 0,
            None => false,
        }
    }

    /// Returns the piece length of the task.
    pub fn piece_length(&self) -> Option<u64> {
        self.piece_length
    }

    /// Returns the content length of the task.
    pub fn content_length(&self) -> Option<u64> {
        self.content_length
    }

    /// Returns the piece count of the task.
    pub fn piece_count(&self) -> Option<u64> {
        self.content_length()
            .zip(self.piece_length())
            .map(|(content_length, piece_length)| content_length.div_ceil(piece_length))
    }
}

/// The metadata of the persistent task.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentTask {
    /// The task id.
    pub id: String,

    /// Represents whether the persistent task is persistent.
    /// If the persistent task is persistent, the persistent peer will
    /// not be deleted when dfdamon runs garbage collection.
    pub persistent: bool,

    /// The time to live of the persistent task.
    pub ttl: Duration,

    /// The length of the piece.
    pub piece_length: u64,

    /// The length of the content.
    pub content_length: u64,

    /// The count of the task being uploaded by other peers.
    pub uploading_count: i64,

    /// The count of the task has been uploaded by other peers.
    pub uploaded_count: u64,

    /// The time when the task metadata is updated. If the task is downloaded
    /// by other peers, it will also update updated_at.
    pub updated_at: NaiveDateTime,

    /// The time when the task metadata is created.
    pub created_at: NaiveDateTime,

    /// The time when the task downloads failed.
    pub failed_at: Option<NaiveDateTime>,

    /// The time when the task downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

/// Implements the persistent task database object.
impl DatabaseObject for PersistentTask {
    /// The namespace of [PersistentTask] objects.
    const NAMESPACE: &'static str = "persistent_task";
}

/// Implements the persistent task metadata.
impl PersistentTask {
    /// Returns whether the persistent task downloads started.
    pub fn is_started(&self) -> bool {
        self.finished_at.is_none()
    }

    /// Returns whether the persistent task is uploading.
    pub fn is_uploading(&self) -> bool {
        self.uploading_count > 0
    }

    /// Returns whether the persistent task is expired.
    pub fn is_expired(&self) -> bool {
        self.created_at + self.ttl < Utc::now().naive_utc()
    }

    /// Returns whether the persistent task downloads failed.
    pub fn is_failed(&self) -> bool {
        self.failed_at.is_some()
    }

    /// Returns whether the persistent task downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }

    /// Returns whether the persistent task is empty.
    pub fn is_empty(&self) -> bool {
        self.content_length == 0
    }

    /// Returns whether the persistent task is persistent.
    pub fn is_persistent(&self) -> bool {
        self.persistent
    }

    /// Returns the piece length of the persistent task.
    pub fn piece_length(&self) -> u64 {
        self.piece_length
    }

    /// Returns the content length of the persistent task.
    pub fn content_length(&self) -> u64 {
        self.content_length
    }

    /// Returns the piece count of the persistent task.
    pub fn piece_count(&self) -> u64 {
        self.content_length.div_ceil(self.piece_length)
    }
}

/// The metadata of the persistent cache task.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentCacheTask {
    /// The task id.
    pub id: String,

    /// Represents whether the persistent cache task is persistent.
    /// If the persistent cache task is persistent, the persistent cache peer will
    /// not be deleted when dfdamon runs garbage collection.
    pub persistent: bool,

    /// The time to live of the persistent cache task.
    pub ttl: Duration,

    /// The length of the piece.
    pub piece_length: u64,

    /// The length of the content.
    pub content_length: u64,

    /// The count of the task being uploaded by other peers.
    pub uploading_count: i64,

    /// The count of the task has been uploaded by other peers.
    pub uploaded_count: u64,

    /// The time when the task metadata is updated. If the task is downloaded
    /// by other peers, it will also update updated_at.
    pub updated_at: NaiveDateTime,

    /// The time when the task metadata is created.
    pub created_at: NaiveDateTime,

    /// The time when the task downloads failed.
    pub failed_at: Option<NaiveDateTime>,

    /// The time when the task downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

/// Implements the persistent cache task database object.
impl DatabaseObject for PersistentCacheTask {
    /// The namespace of [PersistentCacheTask] objects.
    const NAMESPACE: &'static str = "persistent_cache_task";
}

/// Implements the persistent cache task metadata.
impl PersistentCacheTask {
    /// Returns whether the persistent cache task downloads started.
    pub fn is_started(&self) -> bool {
        self.finished_at.is_none()
    }

    /// Returns whether the persistent cache task is uploading.
    pub fn is_uploading(&self) -> bool {
        self.uploading_count > 0
    }

    /// Returns whether the persistent cache task is expired.
    pub fn is_expired(&self) -> bool {
        self.created_at + self.ttl < Utc::now().naive_utc()
    }

    /// Returns whether the persistent cache task downloads failed.
    pub fn is_failed(&self) -> bool {
        self.failed_at.is_some()
    }

    /// Returns whether the persistent cache task downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }

    /// Returns whether the persistent cache task is empty.
    pub fn is_empty(&self) -> bool {
        self.content_length == 0
    }

    /// Returns whether the persistent cache task is persistent.
    pub fn is_persistent(&self) -> bool {
        self.persistent
    }

    /// Returns the piece length of the persistent cache task.
    pub fn piece_length(&self) -> u64 {
        self.piece_length
    }

    /// Returns the content length of the persistent cache task.
    pub fn content_length(&self) -> u64 {
        self.content_length
    }

    /// Returns the piece count of the persistent cache task.
    pub fn piece_count(&self) -> u64 {
        self.content_length.div_ceil(self.piece_length)
    }
}

/// The metadata of the cache task.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheTask {
    /// The task id.
    pub id: String,

    /// The length of the piece.
    pub piece_length: Option<u64>,

    /// The length of the content.
    pub content_length: Option<u64>,

    /// The header of the response.
    pub response_header: HashMap<String, String>,

    /// The count of the task being uploaded by other peers.
    pub uploading_count: i64,

    /// The count of the task has been uploaded by other peers.
    pub uploaded_count: u64,

    /// The time when the task metadata is updated. If the task is downloaded
    /// by other peers, it will also update updated_at.
    pub updated_at: NaiveDateTime,

    /// The time when the task metadata is created.
    pub created_at: NaiveDateTime,

    /// The time when the task downloads failed.
    pub failed_at: Option<NaiveDateTime>,

    /// The time when the task downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

/// Implements the cache task database object.
impl DatabaseObject for CacheTask {
    /// The namespace of [CacheTask] objects.
    const NAMESPACE: &'static str = "cache_task";
}

/// Implements the cache task metadata.
impl CacheTask {
    /// Returns whether the cache task downloads started.
    pub fn is_started(&self) -> bool {
        self.finished_at.is_none()
    }

    /// Returns whether the cache task is uploading.
    pub fn is_uploading(&self) -> bool {
        self.uploading_count > 0
    }

    /// Returns whether the cache task is expired.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.updated_at + ttl < Utc::now().naive_utc()
    }

    /// Returns whether the cache task downloads failed.
    pub fn is_failed(&self) -> bool {
        self.failed_at.is_some()
    }

    /// Returns whether the cache task downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }

    /// Returns whether the cache task is empty.
    pub fn is_empty(&self) -> bool {
        match self.content_length() {
            Some(content_length) => content_length == 0,
            None => false,
        }
    }

    /// Returns the piece length of the cache task.
    pub fn piece_length(&self) -> Option<u64> {
        self.piece_length
    }

    /// Returns the content length of the cache task.
    pub fn content_length(&self) -> Option<u64> {
        self.content_length
    }

    /// Returns the piece count of the cache task.
    pub fn piece_count(&self) -> Option<u64> {
        self.content_length()
            .zip(self.piece_length())
            .map(|(content_length, piece_length)| content_length.div_ceil(piece_length))
    }
}

/// The metadata of the piece.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Piece {
    /// The piece number.
    pub number: u32,

    /// The offset of the piece in the task.
    pub offset: u64,

    /// The length of the piece.
    pub length: u64,

    /// The digest of the piece.
    pub digest: String,

    /// The parent id of the piece.
    pub parent_id: Option<String>,

    /// DEPRECATED: The count of the piece being uploaded by other peers.
    pub uploading_count: u64,

    /// DEPRECATED: The count of the piece has been uploaded by other peers.
    pub uploaded_count: u64,

    /// The time when the piece metadata is updated.
    pub updated_at: NaiveDateTime,

    /// The time when the piece metadata is created.
    pub created_at: NaiveDateTime,

    /// The time when the piece downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

/// Implements the piece database object.
impl DatabaseObject for Piece {
    /// The namespace of [Piece] objects.
    const NAMESPACE: &'static str = "piece";
}

/// Implements the piece metadata.
impl Piece {
    /// Returns whether the piece downloads started.
    pub fn is_started(&self) -> bool {
        self.finished_at.is_none()
    }

    /// Returns whether the piece downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }

    /// Returns the cost of the piece downloaded.
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

    /// Returns the prost cost of the piece downloaded.
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

    /// Returns the digest of the piece metadata, including the piece number,
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

/// UploadStats is the in-memory upload statistics of a task, tracked outside of
/// the storage engine to keep uploads from writing to it. It is filled into the
/// task metadata when the task is read.
#[derive(Clone, Default)]
struct UploadStats {
    /// The count of the task being uploaded by other peers.
    pub uploading_count: i64,

    /// The count of the task has been uploaded by other peers.
    pub uploaded_count: u64,
}

/// Metadata manages the metadata of Task, Piece, PersistentCacheTask, etc.
pub struct Metadata<E = RocksdbStorageEngine>
where
    E: StorageEngineOwned,
{
    /// The underlying storage engine instance.
    db: E,

    /// The in-memory upload statistics of the tasks.
    upload_stats: DashMap<String, UploadStats>,

    /// The in-memory upload statistics of the persistent tasks.
    persistent_task_upload_stats: DashMap<String, UploadStats>,

    /// The in-memory upload statistics of the persistent cache tasks.
    persistent_cache_task_upload_stats: DashMap<String, UploadStats>,
}

impl<E: StorageEngineOwned> Metadata<E> {
    /// Prepares the metadata of the download task.
    #[instrument(level = "debug", skip_all)]
    pub fn prepare_download_task(&self, id: &str) -> Result<(Task, bool)> {
        let task = match self.db.get::<Task>(id.as_bytes())? {
            Some(mut task) => {
                // Reuse existing task if all conditions are met:
                // 1. Content length is defined.
                // 2. Piece length is defined.
                // 3. Task status is not failed.
                if task.content_length().is_some()
                    && task.piece_length().is_some()
                    && !task.is_failed()
                {
                    return Ok((task, true));
                } else {
                    // If reuse conditions are not met, update metadata and retry with HEAD request.
                    task.updated_at = Utc::now().naive_utc();
                    task.failed_at = None;
                    task
                }
            }
            None => {
                // If the task does not exist, create a new task.
                Task {
                    id: id.to_string(),
                    updated_at: Utc::now().naive_utc(),
                    created_at: Utc::now().naive_utc(),
                    ..Default::default()
                }
            }
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok((task, false))
    }

    /// Updates the metadata of the task when the task downloads started.
    #[instrument(level = "debug", skip_all)]
    pub fn download_task_started(
        &self,
        id: &str,
        piece_length: u64,
        content_length: u64,
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
                task.content_length = Some(content_length);
                task.piece_length = Some(piece_length);
                task.response_header = response_header;
                task
            }
            None => Task {
                id: id.to_string(),
                piece_length: Some(piece_length),
                content_length: Some(content_length),
                response_header,
                updated_at: Utc::now().naive_utc(),
                created_at: Utc::now().naive_utc(),
                ..Default::default()
            },
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    /// Updates the metadata of the task when the task downloads finished.
    #[instrument(level = "debug", skip_all)]
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

    /// Updates the metadata of the task when the task downloads failed.
    #[instrument(level = "debug", skip_all)]
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

    /// Updates the metadata of the task when the task prefetch started.
    #[instrument(level = "debug", skip_all)]
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

    /// Updates the metadata of the task when the task prefetch failed.
    #[instrument(level = "debug", skip_all)]
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

    /// Updates the in-memory upload statistics of the task when task uploads started.
    #[instrument(level = "debug", skip_all)]
    pub fn upload_task_started(&self, id: &str) {
        self.upload_stats
            .entry(id.to_string())
            .or_default()
            .uploading_count += 1;
    }

    /// Updates the in-memory upload statistics of the task when task uploads finished.
    #[instrument(level = "debug", skip_all)]
    pub fn upload_task_finished(&self, id: &str) {
        let mut stats = self.upload_stats.entry(id.to_string()).or_default();
        stats.uploading_count = stats.uploading_count.saturating_sub(1);
        stats.uploaded_count += 1;
    }

    /// Updates the in-memory upload statistics of the task when the task uploads failed.
    #[instrument(level = "debug", skip_all)]
    pub fn upload_task_failed(&self, id: &str) {
        let mut stats = self.upload_stats.entry(id.to_string()).or_default();
        stats.uploading_count = stats.uploading_count.saturating_sub(1);
    }

    /// Fills the in-memory upload statistics of the task into the metadata.
    fn fill_upload_stats(&self, task: &mut Task) {
        if let Some(stats) = self.upload_stats.get(&task.id) {
            task.uploading_count = stats.uploading_count;
            task.uploaded_count += stats.uploaded_count;
        }
    }

    /// Gets the task metadata.
    #[instrument(level = "debug", skip_all)]
    pub fn get_task(&self, id: &str) -> Result<Option<Task>> {
        Ok(self.db.get::<Task>(id.as_bytes())?.map(|mut task| {
            self.fill_upload_stats(&mut task);
            task
        }))
    }

    /// Checks if the task exists.
    #[instrument(level = "debug", skip_all)]
    pub fn is_task_exists(&self, id: &str) -> Result<bool> {
        self.db.exists::<Task>(id.as_bytes())
    }

    /// Gets the task metadatas.
    #[instrument(level = "debug", skip_all)]
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
            .map(|task| {
                let mut task = Task::deserialize_from(task)?;
                self.fill_upload_stats(&mut task);
                Ok(task)
            })
            .collect()
    }

    /// Deletes the task metadata.
    #[instrument(level = "debug", skip_all)]
    pub fn delete_task(&self, id: &str) -> Result<()> {
        info!("delete task metadata {}", id);
        self.upload_stats.remove(id);
        self.db.delete::<Task>(id.as_bytes())
    }

    /// Creates a new persistent task.
    #[instrument(level = "debug", skip_all)]
    pub fn create_persistent_task_started(
        &self,
        id: &str,
        ttl: Duration,
        piece_length: u64,
        content_length: u64,
    ) -> Result<PersistentTask> {
        let task = PersistentTask {
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

    /// Updates the metadata of the persistent task
    /// when the persistent task finished.
    #[instrument(level = "debug", skip_all)]
    pub fn create_persistent_task_finished(&self, id: &str) -> Result<PersistentTask> {
        let task = match self.db.get::<PersistentTask>(id.as_bytes())? {
            Some(mut task) => {
                task.updated_at = Utc::now().naive_utc();
                task.failed_at = None;

                // If the persistent task is created by user, the finished_at has been set.
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

    /// Updates the metadata of the persistent task when
    /// the persistent task downloads started. If the persistent task downloaded by scheduler
    /// to create persistent task, the persistent should be set to true.
    #[instrument(level = "debug", skip_all)]
    pub fn download_persistent_task_started(
        &self,
        id: &str,
        ttl: Duration,
        persistent: bool,
        piece_length: u64,
        content_length: u64,
        created_at: NaiveDateTime,
    ) -> Result<PersistentTask> {
        let task = match self.db.get::<PersistentTask>(id.as_bytes())? {
            Some(mut task) => {
                // If the task exists, update the task metadata.
                task.ttl = ttl;
                task.persistent = persistent;
                task.piece_length = piece_length;
                task.updated_at = Utc::now().naive_utc();
                task.failed_at = None;
                task
            }
            None => PersistentTask {
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

    /// Updates the metadata of the persistent task when the persistent task downloads finished.
    #[instrument(level = "debug", skip_all)]
    pub fn download_persistent_task_finished(&self, id: &str) -> Result<PersistentTask> {
        let task = match self.db.get::<PersistentTask>(id.as_bytes())? {
            Some(mut task) => {
                task.updated_at = Utc::now().naive_utc();
                task.failed_at = None;

                // If the persistent task is created by user, the finished_at has been set.
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

    /// Updates the metadata of the persistent task when the persistent task downloads failed.
    #[instrument(level = "debug", skip_all)]
    pub fn download_persistent_task_failed(&self, id: &str) -> Result<PersistentTask> {
        let task = match self.db.get::<PersistentTask>(id.as_bytes())? {
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

    /// Updates the in-memory upload statistics of the persistent task when persistent task uploads started.
    #[instrument(level = "debug", skip_all)]
    pub fn upload_persistent_task_started(&self, id: &str) {
        self.persistent_task_upload_stats
            .entry(id.to_string())
            .or_default()
            .uploading_count += 1;
    }

    /// Updates the in-memory upload statistics of the persistent task when persistent task uploads finished.
    #[instrument(level = "debug", skip_all)]
    pub fn upload_persistent_task_finished(&self, id: &str) {
        let mut stats = self
            .persistent_task_upload_stats
            .entry(id.to_string())
            .or_default();
        stats.uploading_count = stats.uploading_count.saturating_sub(1);
        stats.uploaded_count += 1;
    }

    /// Updates the in-memory upload statistics of the persistent task when the persistent task uploads failed.
    #[instrument(level = "debug", skip_all)]
    pub fn upload_persistent_task_failed(&self, id: &str) {
        let mut stats = self
            .persistent_task_upload_stats
            .entry(id.to_string())
            .or_default();
        stats.uploading_count = stats.uploading_count.saturating_sub(1);
    }

    /// Fills the in-memory upload statistics of the persistent task into the metadata.
    fn fill_persistent_task_upload_stats(&self, task: &mut PersistentTask) {
        if let Some(stats) = self.persistent_task_upload_stats.get(&task.id) {
            task.uploading_count = stats.uploading_count;
            task.uploaded_count += stats.uploaded_count;
        }
    }

    /// Persists the persistent task metadata.
    #[instrument(level = "debug", skip_all)]
    pub fn persist_persistent_task(&self, id: &str) -> Result<PersistentTask> {
        let task = match self.db.get::<PersistentTask>(id.as_bytes())? {
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

    /// Gets the persistent task metadata.
    #[instrument(level = "debug", skip_all)]
    pub fn get_persistent_task(&self, id: &str) -> Result<Option<PersistentTask>> {
        Ok(self
            .db
            .get::<PersistentTask>(id.as_bytes())?
            .map(|mut task| {
                self.fill_persistent_task_upload_stats(&mut task);
                task
            }))
    }

    /// Checks if the persistent task exists.
    #[instrument(level = "debug", skip_all)]
    pub fn is_persistent_task_exists(&self, id: &str) -> Result<bool> {
        self.db.exists::<PersistentTask>(id.as_bytes())
    }

    /// Gets the persistent task metadatas.
    #[instrument(level = "debug", skip_all)]
    pub fn get_persistent_tasks(&self) -> Result<Vec<PersistentTask>> {
        let iter = self.db.iter::<PersistentTask>()?;
        iter.map(|ele| {
            ele.map(|(_, mut task)| {
                self.fill_persistent_task_upload_stats(&mut task);
                task
            })
        })
        .collect()
    }

    /// Deletes the persistent task metadata.
    #[instrument(level = "debug", skip_all)]
    pub fn delete_persistent_task(&self, id: &str) -> Result<()> {
        info!("delete persistent task metadata {}", id);
        self.persistent_task_upload_stats.remove(id);
        self.db.delete::<PersistentTask>(id.as_bytes())
    }

    /// Creates a new persistent cache task.
    #[instrument(level = "debug", skip_all)]
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

    /// Updates the metadata of the persistent cache task
    /// when the persistent cache task finished.
    #[instrument(level = "debug", skip_all)]
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

    /// Updates the metadata of the persistent cache task when
    /// the persistent cache task downloads started. If the persistent cache task downloaded by scheduler
    /// to create persistent cache task, the persistent should be set to true.
    #[instrument(level = "debug", skip_all)]
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

    /// Updates the metadata of the persistent cache task when the persistent cache task downloads finished.
    #[instrument(level = "debug", skip_all)]
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

    /// Updates the metadata of the persistent cache task when the persistent cache task downloads failed.
    #[instrument(level = "debug", skip_all)]
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

    /// Updates the in-memory upload statistics of the persistent cache task when persistent cache task uploads started.
    #[instrument(level = "debug", skip_all)]
    pub fn upload_persistent_cache_task_started(&self, id: &str) {
        self.persistent_cache_task_upload_stats
            .entry(id.to_string())
            .or_default()
            .uploading_count += 1;
    }

    /// Updates the in-memory upload statistics of the persistent cache task when persistent cache task uploads finished.
    #[instrument(level = "debug", skip_all)]
    pub fn upload_persistent_cache_task_finished(&self, id: &str) {
        let mut stats = self
            .persistent_cache_task_upload_stats
            .entry(id.to_string())
            .or_default();
        stats.uploading_count = stats.uploading_count.saturating_sub(1);
        stats.uploaded_count += 1;
    }

    /// Updates the in-memory upload statistics of the persistent cache task when the persistent cache task uploads failed.
    #[instrument(level = "debug", skip_all)]
    pub fn upload_persistent_cache_task_failed(&self, id: &str) {
        let mut stats = self
            .persistent_cache_task_upload_stats
            .entry(id.to_string())
            .or_default();
        stats.uploading_count = stats.uploading_count.saturating_sub(1);
    }

    /// Fills the in-memory upload statistics of the persistent cache task into the metadata.
    fn fill_persistent_cache_task_upload_stats(&self, task: &mut PersistentCacheTask) {
        if let Some(stats) = self.persistent_cache_task_upload_stats.get(&task.id) {
            task.uploading_count = stats.uploading_count;
            task.uploaded_count += stats.uploaded_count;
        }
    }

    /// Persists the persistent cache task metadata.
    #[instrument(level = "debug", skip_all)]
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

    /// Gets the persistent cache task metadata.
    #[instrument(level = "debug", skip_all)]
    pub fn get_persistent_cache_task(&self, id: &str) -> Result<Option<PersistentCacheTask>> {
        Ok(self
            .db
            .get::<PersistentCacheTask>(id.as_bytes())?
            .map(|mut task| {
                self.fill_persistent_cache_task_upload_stats(&mut task);
                task
            }))
    }

    /// Checks if the persistent cache task exists.
    #[instrument(level = "debug", skip_all)]
    pub fn is_persistent_cache_task_exists(&self, id: &str) -> Result<bool> {
        self.db.exists::<PersistentCacheTask>(id.as_bytes())
    }

    /// Gets the persistent cache task metadatas.
    #[instrument(level = "debug", skip_all)]
    pub fn get_persistent_cache_tasks(&self) -> Result<Vec<PersistentCacheTask>> {
        let iter = self.db.iter::<PersistentCacheTask>()?;
        iter.map(|ele| {
            ele.map(|(_, mut task)| {
                self.fill_persistent_cache_task_upload_stats(&mut task);
                task
            })
        })
        .collect()
    }

    /// Deletes the persistent cache task metadata.
    #[instrument(level = "debug", skip_all)]
    pub fn delete_persistent_cache_task(&self, id: &str) -> Result<()> {
        info!("delete persistent cache task metadata {}", id);
        self.persistent_cache_task_upload_stats.remove(id);
        self.db.delete::<PersistentCacheTask>(id.as_bytes())
    }

    /// Updates the metadata of the cache task when the cache task downloads started.
    #[instrument(level = "debug", skip_all)]
    pub fn download_cache_task_started(
        &self,
        id: &str,
        piece_length: u64,
        content_length: u64,
        response_header: Option<HeaderMap>,
    ) -> Result<CacheTask> {
        // Convert the response header to hashmap.
        let response_header = response_header
            .as_ref()
            .map(headermap_to_hashmap)
            .unwrap_or_default();

        let task = match self.db.get::<CacheTask>(id.as_bytes())? {
            Some(mut task) => {
                // If the task exists, update the task metadata.
                task.updated_at = Utc::now().naive_utc();
                task.failed_at = None;
                task.content_length = Some(content_length);
                task.piece_length = Some(piece_length);
                task.response_header = response_header;
                task
            }
            None => CacheTask {
                id: id.to_string(),
                piece_length: Some(piece_length),
                content_length: Some(content_length),
                response_header,
                updated_at: Utc::now().naive_utc(),
                created_at: Utc::now().naive_utc(),
                ..Default::default()
            },
        };

        self.db.put(id.as_bytes(), &task)?;
        Ok(task)
    }

    /// Updates the metadata of the cache task when the cache task downloads finished.
    #[instrument(level = "debug", skip_all)]
    pub fn download_cache_task_finished(&self, id: &str) -> Result<CacheTask> {
        let task = match self.db.get::<CacheTask>(id.as_bytes())? {
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

    /// Updates the metadata of the cache task when the cache task downloads failed.
    #[instrument(level = "debug", skip_all)]
    pub fn download_cache_task_failed(&self, id: &str) -> Result<CacheTask> {
        let task = match self.db.get::<CacheTask>(id.as_bytes())? {
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

    /// Updates the metadata of the cache task when the cache task uploads started.
    #[instrument(level = "debug", skip_all)]
    pub fn upload_cache_task_started(&self, id: &str) -> Result<CacheTask> {
        let task = match self.db.get::<CacheTask>(id.as_bytes())? {
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

    /// Updates the metadata of the cache task when the cache task uploads finished.
    #[instrument(level = "debug", skip_all)]
    pub fn upload_cache_task_finished(&self, id: &str) -> Result<CacheTask> {
        let task = match self.db.get::<CacheTask>(id.as_bytes())? {
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

    /// Updates the metadata of the cache task when the cache task uploads failed.
    #[instrument(level = "debug", skip_all)]
    pub fn upload_cache_task_failed(&self, id: &str) -> Result<CacheTask> {
        let task = match self.db.get::<CacheTask>(id.as_bytes())? {
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

    /// Gets the cache task metadata.
    #[instrument(level = "debug", skip_all)]
    pub fn get_cache_task(&self, id: &str) -> Result<Option<CacheTask>> {
        self.db.get(id.as_bytes())
    }

    /// Checks if the cache task exists.
    #[instrument(level = "debug", skip_all)]
    pub fn is_cache_task_exists(&self, id: &str) -> Result<bool> {
        self.db.exists::<CacheTask>(id.as_bytes())
    }

    /// Gets the cache task metadatas.
    #[instrument(level = "debug", skip_all)]
    pub fn get_cache_tasks(&self) -> Result<Vec<CacheTask>> {
        let tasks = self
            .db
            .iter_raw::<CacheTask>()?
            .map(|ele| {
                let (_, value) = ele?;
                Ok(value)
            })
            .collect::<Result<Vec<Box<[u8]>>>>()?;

        tasks
            .iter()
            .map(|task| CacheTask::deserialize_from(task))
            .collect()
    }

    /// Deletes the cache task metadata.
    #[instrument(level = "debug", skip_all)]
    pub fn delete_cache_task(&self, id: &str) -> Result<()> {
        info!("delete cache task metadata {}", id);
        self.db.delete::<CacheTask>(id.as_bytes())
    }

    /// Creates a new persistent piece, which is imported by
    /// local.
    #[instrument(level = "debug", skip_all)]
    pub fn create_persistent_piece(
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
            // Persistent piece does not have parent id, because the piece content is
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

    /// Creates a new persistent cache piece, which is imported by
    /// local.
    #[instrument(level = "debug", skip_all)]
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

    /// Updates the metadata of the piece when the piece downloads started.
    #[instrument(level = "debug", skip_all)]
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

    /// Updates the metadata of the piece when the piece downloads finished.
    #[instrument(level = "debug", skip_all)]
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

    /// Updates the metadata of the piece when the piece downloads failed.
    #[instrument(level = "debug", skip_all)]
    pub fn download_piece_failed(&self, piece_id: &str) -> Result<()> {
        // A failed download must never erase a piece another download completed,
        // e.g. a duplicate downloader failing after the winner finished the piece.
        if let Some(piece) = self.get_piece(piece_id)? {
            if piece.is_finished() {
                return Ok(());
            }
        }

        self.delete_piece(piece_id)
    }

    /// Waits for the piece to be finished or failed.
    #[instrument(level = "debug", skip_all)]
    pub fn wait_for_piece_finished_failed(&self, piece_id: &str) -> Result<()> {
        // A timed-out or stale-detecting waiter must never erase a piece that
        // has just been finished by its downloader.
        if let Some(piece) = self.get_piece(piece_id)? {
            if piece.is_finished() {
                return Ok(());
            }
        }

        self.delete_piece(piece_id)
    }

    /// Gets the piece metadata.
    pub fn get_piece(&self, piece_id: &str) -> Result<Option<Piece>> {
        self.db.get(piece_id.as_bytes())
    }

    /// Gets the metadata of the pieces by the piece ids, returning the pieces
    /// in the order of the ids.
    pub fn get_pieces_by_ids(&self, piece_ids: &[&str]) -> Result<Vec<Option<Piece>>> {
        let keys: Vec<&[u8]> = piece_ids
            .iter()
            .map(|piece_id| piece_id.as_bytes())
            .collect();

        self.db.multi_get(&keys)
    }

    /// Checks if the piece exists.
    #[instrument(level = "debug", skip_all)]
    pub fn is_piece_exists(&self, piece_id: &str) -> Result<bool> {
        self.db.exists::<Piece>(piece_id.as_bytes())
    }

    /// Gets the piece metadatas.
    #[instrument(level = "debug", skip_all)]
    pub fn get_pieces(&self, task_id: &str) -> Result<Vec<Piece>> {
        let pieces = self
            .db
            .prefix_iter_raw::<Piece>(task_id.as_bytes())?
            .map(|ele| {
                let (_, value) = ele?;
                Ok(value)
            })
            .collect::<Result<Vec<Box<[u8]>>>>()?;

        pieces
            .iter()
            .map(|piece| Piece::deserialize_from(piece))
            .collect()
    }

    /// Deletes the piece metadata.
    #[instrument(level = "debug", skip_all)]
    pub fn delete_piece(&self, piece_id: &str) -> Result<()> {
        info!("delete piece metadata {}", piece_id);
        self.db.delete::<Piece>(piece_id.as_bytes())
    }

    /// Deletes the piece metadatas.
    #[instrument(level = "debug", skip_all)]
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

    /// Returns the piece id.
    #[inline]
    pub fn piece_id(&self, task_id: &str, number: u32) -> String {
        format!("{task_id}-{number}")
    }
}

/// Implements the metadata of the storage engine.
impl Metadata<RocksdbStorageEngine> {
    /// Creates a new metadata instance.
    #[instrument(level = "debug", skip_all)]
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
                PersistentTask::NAMESPACE,
                PersistentCacheTask::NAMESPACE,
                CacheTask::NAMESPACE,
            ],
            config.storage.keep,
        )?;

        Ok(Metadata {
            db,
            upload_stats: DashMap::new(),
            persistent_task_upload_stats: DashMap::new(),
            persistent_cache_task_upload_stats: DashMap::new(),
        })
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

        metadata
            .download_task_started(task_id, 1024, 1024, None)
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

        metadata.download_task_finished(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert!(task.is_finished());

        metadata.upload_task_started(task_id);
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert_eq!(task.uploading_count, 1);

        metadata.upload_task_finished(task_id);
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert_eq!(task.uploading_count, 0);
        assert_eq!(task.uploaded_count, 1);

        metadata.upload_task_started(task_id);
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert_eq!(task.uploading_count, 1);
        metadata.upload_task_failed(task_id);
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert_eq!(task.uploading_count, 0);
        assert_eq!(task.uploaded_count, 1);

        let task_id = "a535b115f18d96870f0422ac891f91dd162f2f391e4778fb84279701fcd02dd1";
        metadata
            .download_task_started(task_id, 1024, 0, None)
            .unwrap();
        let tasks = metadata.get_tasks().unwrap();
        assert_eq!(tasks.len(), 2);

        metadata.delete_task(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap();
        assert!(task.is_none());
    }

    #[test]
    fn test_cache_task_lifecycle() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path().join("log");
        let metadata = Metadata::new(Arc::new(Config::default()), dir.path(), &log_dir).unwrap();
        let task_id = "d3c4e940ad06c47fc36ac67801e6f8e36cb400e2391708620bc7e865b102062c";

        metadata
            .download_cache_task_started(task_id, 1024, 1024, None)
            .unwrap();
        let task = metadata
            .get_cache_task(task_id)
            .unwrap()
            .expect("task should exist after download_cache_task_started");
        assert_eq!(task.id, task_id);
        assert_eq!(task.piece_length, Some(1024));
        assert_eq!(task.content_length, Some(1024));
        assert!(task.response_header.is_empty());
        assert_eq!(task.uploading_count, 0);
        assert_eq!(task.uploaded_count, 0);
        assert!(!task.is_finished());

        metadata.download_cache_task_finished(task_id).unwrap();
        let task = metadata.get_cache_task(task_id).unwrap().unwrap();
        assert!(task.is_finished());

        metadata.upload_cache_task_started(task_id).unwrap();
        let task = metadata.get_cache_task(task_id).unwrap().unwrap();
        assert_eq!(task.uploading_count, 1);

        metadata.upload_cache_task_finished(task_id).unwrap();
        let task = metadata.get_cache_task(task_id).unwrap().unwrap();
        assert_eq!(task.uploading_count, 0);
        assert_eq!(task.uploaded_count, 1);

        let task = metadata.upload_cache_task_started(task_id).unwrap();
        assert_eq!(task.uploading_count, 1);
        let task = metadata.upload_cache_task_failed(task_id).unwrap();
        assert_eq!(task.uploading_count, 0);
        assert_eq!(task.uploaded_count, 1);

        let task_id = "a535b115f18d96870f0422ac891f91dd162f2f391e4778fb84279701fcd02dd1";
        metadata
            .download_cache_task_started(task_id, 1024, 0, None)
            .unwrap();
        let tasks = metadata.get_cache_tasks().unwrap();
        assert_eq!(tasks.len(), 2);

        metadata.delete_cache_task(task_id).unwrap();
        let task = metadata.get_cache_task(task_id).unwrap();
        assert!(task.is_none());
    }

    #[test]
    fn test_piece_lifecycle() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path().join("log");
        let metadata = Metadata::new(Arc::new(Config::default()), dir.path(), &log_dir).unwrap();
        let task_id = "d3c4e940ad06c47fc36ac67801e6f8e36cb400e2391708620bc7e865b102062c";
        let piece_id = metadata.piece_id(task_id, 1);

        metadata
            .download_piece_started(piece_id.as_str(), 1)
            .unwrap();
        let piece = metadata.get_piece(piece_id.as_str()).unwrap().unwrap();
        assert_eq!(piece.number, 1);

        metadata
            .download_piece_finished(piece_id.as_str(), 0, 1024, "digest1", None)
            .unwrap();
        let piece = metadata.get_piece(piece_id.as_str()).unwrap().unwrap();
        assert_eq!(piece.length, 1024);
        assert_eq!(piece.digest, "digest1");

        metadata
            .download_piece_started(metadata.piece_id(task_id, 2).as_str(), 2)
            .unwrap();
        metadata
            .download_piece_started(metadata.piece_id(task_id, 3).as_str(), 3)
            .unwrap();
        let pieces = metadata.get_pieces(task_id).unwrap();
        assert_eq!(pieces.len(), 3);

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

        metadata.delete_pieces(task_id).unwrap();
        let pieces = metadata.get_pieces(task_id).unwrap();
        assert!(pieces.is_empty());
    }

    #[test]
    fn test_download_piece_failed_keeps_finished_piece() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path().join("log");
        let metadata = Metadata::new(Arc::new(Config::default()), dir.path(), &log_dir).unwrap();
        let task_id = "e4c4e940ad06c47fc36ac67801e6f8e36cb400e2391708620bc7e865b102062d";
        let piece_id = metadata.piece_id(task_id, 1);

        metadata
            .download_piece_started(piece_id.as_str(), 1)
            .unwrap();
        metadata
            .download_piece_finished(piece_id.as_str(), 0, 1024, "digest1", None)
            .unwrap();

        metadata.download_piece_failed(piece_id.as_str()).unwrap();
        let piece = metadata.get_piece(piece_id.as_str()).unwrap().unwrap();
        assert!(piece.is_finished());

        metadata
            .wait_for_piece_finished_failed(piece_id.as_str())
            .unwrap();
        let piece = metadata.get_piece(piece_id.as_str()).unwrap().unwrap();
        assert!(piece.is_finished());
    }
}
