/*
 *     Copyright 2025 The Dragonfly Authors
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

use bytes::Bytes;
use dragonfly_api::common::v2::Range;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use lru_cache::LruCache;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::RwLock;
use tracing::{info, warn};
mod lru_cache;

/// Task is the task content in the cache.
#[derive(Clone, Debug)]
struct Task {
    /// id is the id of the task.
    id: String,

    /// content_length is the length of the task content.
    content_length: u64,

    /// pieces is the pieces content of the task.
    pieces: Arc<RwLock<HashMap<String, Bytes>>>,
}

/// Task implements the task content in the cache.
impl Task {
    /// new creates a new task.
    fn new(id: String, content_length: u64) -> Self {
        Self {
            id,
            content_length,
            pieces: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// write_piece writes the piece content to the task.
    async fn write_piece(&self, id: &str, piece: Bytes) {
        let mut pieces = self.pieces.write().await;
        pieces.insert(id.to_string(), piece);
    }

    /// read_piece reads the piece content from the task.
    async fn read_piece(&self, id: &str) -> Option<Bytes> {
        let pieces = self.pieces.read().await;
        pieces.get(id).cloned()
    }

    /// contains checks whether the piece exists in the task.
    async fn contains(&self, id: &str) -> bool {
        let pieces = self.pieces.read().await;
        pieces.contains_key(id)
    }

    /// content_length returns the content length of the task.
    fn content_length(&self) -> u64 {
        self.content_length
    }
}

/// Cache is the cache for storing piece content by LRU algorithm.
///
/// Cache storage:
/// 1. Users can create preheating jobs and preheat tasks to memory and disk by setting `load_to_cache` to `true`.
///    For more details, refer to https://github.com/dragonflyoss/api/blob/main/proto/common.proto#L443.
/// 2. If the download hits the memory cache, it will be faster than reading from the disk, because there is no
///    page cache for the first read.
/// ```
///
///     1.Preheat
///         |
///         |
/// +--------------------------------------------------+
/// |       |              Peer                        |
/// |       |                   +-----------+          |
/// |       |     -- Partial -->|   Cache   |          |
/// |       |     |             +-----------+          |
/// |       v     |                |    |              |
/// |   Download  |              Miss   |              |             
/// |     Task -->|                |    --- Hit ------>|<-- 2.Download
/// |             |                |               ^   |              
/// |             |                v               |   |
/// |             |          +-----------+         |   |
/// |             -- Full -->|   Disk    |----------   |
/// |                        +-----------+             |
/// |                                                  |
/// +--------------------------------------------------+
/// ```
/// Task is the metadata of the task.
#[derive(Clone)]
pub struct Cache {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// size is the size of the cache in bytes.
    size: u64,

    /// capacity is the maximum capacity of the cache in bytes.
    capacity: u64,

    /// tasks stores the tasks with their task id.
    tasks: Arc<RwLock<LruCache<String, Task>>>,
}

/// Cache implements the cache for storing piece content by LRU algorithm.
impl Cache {
    /// new creates a new cache with the specified capacity.
    pub fn new(config: Arc<Config>) -> Result<Self> {
        Ok(Cache {
            config: config.clone(),
            size: 0,
            capacity: config.storage.cache_capacity.as_u64(),
            // LRU cache capacity is set to usize::MAX to avoid evicting tasks. LRU cache will evict tasks
            // by cache capacity(cache size) itself, and used pop_lru to evict the least recently
            // used task.
            tasks: Arc::new(RwLock::new(LruCache::new(usize::MAX))),
        })
    }

    /// read_piece reads the piece from the cache.
    pub async fn read_piece(
        &self,
        task_id: &str,
        piece_id: &str,
        piece: super::metadata::Piece,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        let mut tasks = self.tasks.write().await;
        let Some(task) = tasks.get(task_id) else {
            return Err(Error::TaskNotFound(task_id.to_string()));
        };

        let Some(piece_content) = task.read_piece(piece_id).await else {
            return Err(Error::PieceNotFound(piece_id.to_string()));
        };
        drop(tasks);

        // Calculate the range of bytes to return based on the range provided.
        let (target_offset, target_length) = if let Some(range) = range {
            let target_offset = max(piece.offset, range.start) - piece.offset;
            let target_length = min(
                piece.offset + piece.length - 1,
                range.start + range.length - 1,
            ) - target_offset
                - piece.offset
                + 1;
            (target_offset as usize, target_length as usize)
        } else {
            (0, piece.length as usize)
        };

        // Check if the target range is valid.
        let begin = target_offset;
        let end = target_offset + target_length;
        if begin >= piece_content.len() || end > piece_content.len() {
            return Err(Error::InvalidParameter);
        }

        let content = piece_content.slice(begin..end);
        let reader =
            BufReader::with_capacity(self.config.storage.read_buffer_size, Cursor::new(content));
        Ok(reader)
    }

    /// write_piece writes the piece content to the cache.
    pub async fn write_piece<R: AsyncRead + Unpin + ?Sized>(
        &self,
        task_id: &str,
        piece_id: &str,
        reader: &mut R,
        length: u64,
    ) -> Result<()> {
        let mut tasks = self.tasks.write().await;
        let Some(task) = tasks.get(task_id) else {
            return Err(Error::TaskNotFound(task_id.to_string()));
        };

        if task.contains(piece_id).await {
            warn!("piece {} already exists in cache", piece_id);
            return Ok(());
        }

        let mut buffer = bytes::BytesMut::with_capacity(length as usize);
        match reader.read_buf(&mut buffer).await {
            Ok(_) => {
                task.write_piece(piece_id, buffer.freeze()).await;
                Ok(())
            }
            Err(err) => Err(Error::Unknown(format!(
                "failed to read piece data for {}: {}",
                piece_id, err
            ))),
        }
    }

    /// put_task puts a new task into the cache, constrained by the capacity of the cache.
    pub async fn put_task(&mut self, task_id: &str, content_length: u64) {
        // If the content length is 0, we don't cache the task.
        if content_length == 0 {
            return;
        }

        // If the content length is larger than the cache capacity, we don't cache the task.
        if content_length >= self.capacity {
            info!(
                "task {} is too large to cache, size: {}",
                task_id, content_length
            );

            return;
        }

        let mut tasks = self.tasks.write().await;
        while self.size + content_length > self.capacity {
            match tasks.pop_lru() {
                Some((_, task)) => {
                    self.size -= task.content_length();
                    info!("evicted task in cache: {}", task.id);
                }
                None => {
                    break;
                }
            }
        }

        let task = Task::new(task_id.to_string(), content_length);
        tasks.put(task_id.to_string(), task);
        self.size += content_length;
    }

    /// contains_task checks whether the task exists in the cache.
    pub async fn contains_task(&self, id: &str) -> bool {
        let tasks = self.tasks.read().await;
        tasks.contains(id)
    }

    /// contains_piece checks whether the piece exists in the specified task.
    pub async fn contains_piece(&self, task_id: &str, piece_id: &str) -> bool {
        let tasks = self.tasks.read().await;
        if let Some(task) = tasks.peek(task_id) {
            task.contains(piece_id).await
        } else {
            false
        }
    }
}
