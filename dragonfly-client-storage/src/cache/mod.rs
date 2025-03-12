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
use dragonfly_client_core::{Error, Result};
use lru_cache::LruCache;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::RwLock;
use tracing::info;
mod lru_cache;

#[derive(Clone, Debug)]
/// Task is the task content in the cache.
struct Task {
    /// id is the id of the task.
    id: String,

    /// content_length is the length of the task content.
    content_length: u64,

    /// pieces is the pieces content of the task.
    pieces: Arc<RwLock<HashMap<String, Bytes>>>,
}

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
    async fn write_piece(&self, piece_id: &str, piece: Bytes) {
        let mut pieces = self.pieces.write().await;
        pieces.insert(piece_id.to_string(), piece);
    }

    /// read_piece reads the piece content from the task.
    async fn read_piece(&self, piece_id: &str) -> Option<Bytes> {
        let pieces = self.pieces.read().await;
        pieces.get(piece_id).cloned()
    }

    /// contains checks whether the piece exists in the task.
    async fn contains(&self, piece_id: &str) -> bool {
        let pieces = self.pieces.read().await;
        pieces.contains_key(piece_id)
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
    /// size is the size of the cache.
    size: u64,

    /// capacity_bytes is the maximum capacity of the cache in bytes.
    capacity_bytes: u64,

    /// tasks stores the tasks with their task id.
    tasks: Arc<RwLock<LruCache<String, Task>>>,
}

/// Cache implements the cache for storing piece content by LRU algorithm.
impl Cache {
    /// new creates a new cache with the specified capacity.
    pub fn new(capacity: usize) -> Result<Self> {
        let capacity = NonZeroUsize::new(capacity).ok_or(Error::InvalidParameter)?;
        let tasks = Arc::new(RwLock::new(LruCache::new(capacity)));

        Ok(Cache {
            size: 0,
            capacity_bytes: 10000000,
            tasks,
        })
    }

    /// read_piece reads the piece from the cache.
    pub async fn read_piece(
        &self,
        task_id: &str,
        piece_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        // Try to get the piece content from the task.
        let mut tasks = self.tasks.write().await;
        let Some(task) = tasks.get(task_id) else {
            return Err(Error::TaskNotFound(task_id.to_string()));
        };
        let Some(piece_content) = task.read_piece(piece_id).await else {
            return Err(Error::PieceNotFound(piece_id.to_string()));
        };

        // Calculate the range of bytes to return based on the range provided.
        let (target_offset, target_length) = if let Some(range) = range {
            let target_offset = max(offset, range.start) - offset;
            let target_length =
                min(offset + length - 1, range.start + range.length - 1) - target_offset - offset
                    + 1;
            (target_offset as usize, target_length as usize)
        } else {
            (0, length as usize)
        };

        // Check if the target range is valid.
        let begin = target_offset;
        let end = target_offset + target_length;
        if begin >= piece_content.len() || end > piece_content.len() {
            return Err(Error::InvalidParameter);
        }

        let reader = BufReader::new(Cursor::new(piece_content.slice(begin..end)));
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

        // The piece already exists in the cache.
        if task.contains(piece_id).await {
            return Err(Error::Unknown(format!(
                "overwrite existing piece {}",
                piece_id
            )));
        }

        let mut buffer = Vec::with_capacity(length as usize);
        match reader.read_to_end(&mut buffer).await {
            Ok(_) => {
                task.write_piece(piece_id, Bytes::from(buffer)).await;
                Ok(())
            }
            Err(err) => Err(Error::Unknown(format!(
                "Failed to read piece data for {}: {}",
                piece_id, err
            ))),
        }
    }

    /// put_task puts a new task into the cache, constrained by the capacity of the cache.
    pub async fn put_task(&mut self, task_id: &str, content_length: u64) {
        if content_length == 0 {
            return;
        }

        let mut tasks = self.tasks.write().await;
        let new_task = Task::new(task_id.to_string(), content_length);
        while self.size + content_length > self.capacity_bytes {
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
        tasks.put(task_id.to_string(), new_task);
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
