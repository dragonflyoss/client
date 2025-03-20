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

#[cfg(test)]
mod tests {
    use super::super::metadata::Piece;
    use super::*;
    use bytesize::ByteSize;
    use dragonfly_api::common::v2::Range;
    use dragonfly_client_config::dfdaemon::Storage;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_new() {
        // Test initialization with default configuration
        // Initial cache size should be 0 and default capacity should be 128MB
        let config = Config::default();
        let cache = Cache::new(Arc::new(config)).unwrap();
        assert_eq!(cache.size, 0);
        assert_eq!(cache.capacity, ByteSize::mb(128).as_u64());

        // Test initialization with custom configuration
        // Initial cache size should be 0 and cache capacity should match configuration (100MB)
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mb(100),
                ..Default::default()
            },
            ..Default::default()
        };
        let cache = Cache::new(Arc::new(config)).unwrap();
        assert_eq!(cache.size, 0);
        assert_eq!(cache.capacity, ByteSize::mb(100).as_u64());

        // Test initialization with zero capacity
        // Both initial cache size and capacity should be 0
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::b(0),
                ..Default::default()
            },
            ..Default::default()
        };
        let cache = Cache::new(Arc::new(config)).unwrap();
        assert_eq!(cache.size, 0);
        assert_eq!(cache.capacity, 0);
    }

    #[tokio::test]
    async fn test_contains_task() {
        // Initialize cache with 10MiB capacity
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let cache = Cache::new(Arc::new(config)).unwrap();

        // Test non-existent task
        // Should return false for task that hasn't been added
        assert!(!cache.contains_task("non_existent").await);

        // Test task after adding
        // Should return true after task is added
        let task = Task::new("task1".to_string(), ByteSize::mib(1).as_u64());
        cache.tasks.write().await.put("task1".to_string(), task);
        assert!(cache.contains_task("task1").await);

        // Test task after removing
        // Should return false after task is removed
        cache.tasks.write().await.pop_lru();
        assert!(!cache.contains_task("task1").await);

        // Test multiple tasks
        // Should correctly track existence of different tasks
        let task1 = Task::new("task1".to_string(), ByteSize::mib(1).as_u64());
        let task2 = Task::new("task2".to_string(), ByteSize::mib(2).as_u64());
        let mut tasks = cache.tasks.write().await;
        tasks.put("task1".to_string(), task1);
        tasks.put("task2".to_string(), task2);
        drop(tasks);

        assert!(cache.contains_task("task1").await);
        assert!(cache.contains_task("task2").await);
        assert!(!cache.contains_task("task3").await);
    }

    #[tokio::test]
    async fn test_put_task() {
        // Initialize cache configuration with 10MiB capacity
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();
        let cache_capacity = ByteSize::mib(10).as_u64();

        // Test boundary conditions
        // Empty task should not be cached
        cache.put_task("empty_task", 0).await;
        assert!(!cache.contains_task("empty_task").await);

        // Task equal to capacity should not be cached
        cache.put_task("equal_capacity", cache_capacity).await;
        assert!(!cache.contains_task("equal_capacity").await);

        // Task exceeding capacity should not be cached
        cache.put_task("exceed_capacity", cache_capacity + 1).await;
        assert!(!cache.contains_task("exceed_capacity").await);

        // Test normal caching and size calculation
        // Normal sized task should be cached and size should be updated correctly
        let task_size = ByteSize::mib(1).as_u64();
        cache.put_task("normal_task", task_size).await;
        assert!(cache.contains_task("normal_task").await);
        assert_eq!(cache.size, task_size);

        // Test LRU eviction mechanism
        // Add multiple tasks until eviction is triggered
        let task_size = ByteSize::mib(2).as_u64();
        for i in 0..5 {
            cache.put_task(&format!("lru_task_{}", i), task_size).await;
        }

        // Earliest task should be evicted and most recent task should exist
        assert!(!cache.contains_task("normal_task").await);
        assert!(cache.contains_task("lru_task_4").await);

        // Cache size should not exceed capacity
        assert!(cache.size <= cache_capacity);

        // Test updating existing task
        // Updated task should still exist and cache size should reflect the update
        let new_task_size = ByteSize::mib(3).as_u64();
        let old_cache_size = cache.size;
        cache.put_task("lru_task_5", new_task_size).await;
        assert!(cache.contains_task("lru_task_5").await);
        assert_eq!(cache.size, old_cache_size - task_size * 2 + new_task_size);
    }

    #[tokio::test]
    async fn test_contains_piece() {
        // Initialize cache with 10MiB capacity and 8KB read buffer
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Should return false when checking non-existent task
        assert!(!cache.contains_piece("non_existent", "piece1").await);

        // Should return false when checking piece in non-existent task with empty piece ID
        assert!(!cache.contains_piece("non_existent", "").await);

        // Add task and verify empty task behavior
        cache.put_task("task1", 1000).await;
        assert!(!cache.contains_piece("task1", "piece1").await);

        // Add piece and verify existence
        let mut content = Cursor::new("test data");
        cache
            .write_piece("task1", "piece1", &mut content, 9)
            .await
            .unwrap();
        assert!(cache.contains_piece("task1", "piece1").await);

        // Should return false for empty piece ID in existing task
        assert!(!cache.contains_piece("task1", "").await);

        // Should return false for non-existent piece in existing task
        assert!(!cache.contains_piece("task1", "non_existent_piece").await);

        // Test piece ID with special characters
        let mut content = Cursor::new("test data");
        cache
            .write_piece("task1", "piece#$%^&*", &mut content, 9)
            .await
            .unwrap();
        assert!(cache.contains_piece("task1", "piece#$%^&*").await);

        // Test concurrent read access to the same piece
        let cache_arc = Arc::new(cache);
        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache_clone = cache_arc.clone();
            let handle =
                tokio::spawn(async move { cache_clone.contains_piece("task1", "piece1").await });
            handles.push(handle);
        }

        // All concurrent reads should succeed
        for handle in handles {
            assert!(handle.await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_contains_piece_with_eviction() {
        // Initialize cache with 10MiB capacity
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Add initial task with 1MiB size
        cache.put_task("task1", ByteSize::mib(2).as_u64()).await;
        let mut content = Cursor::new("test data");
        cache
            .write_piece("task1", "piece1", &mut content, 9)
            .await
            .unwrap();
        assert!(cache.contains_piece("task1", "piece1").await);

        // Add a larger task that forces eviction of the first task
        cache
            .put_task("large_task", ByteSize::mib(9).as_u64())
            .await;

        // Verify first task's piece is no longer accessible
        assert!(!cache.contains_piece("task1", "piece1").await);

        // Verify the new task exists
        assert!(cache.contains_task("large_task").await);
    }

    #[tokio::test]
    async fn test_write_piece() {
        // Initialize cache with 10MiB capacity and 8KB read buffer
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                read_buffer_size: 8192,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Test writing to non-existent task
        let mut content = Cursor::new("test data");
        let result = cache
            .write_piece("non_existent", "piece1", &mut content, 9)
            .await;
        assert!(matches!(result, Err(Error::TaskNotFound(_))));

        // Add task and write piece
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;
        let mut content = Cursor::new("test data");
        let result = cache.write_piece("task1", "piece1", &mut content, 9).await;
        assert!(result.is_ok());
        assert!(cache.contains_piece("task1", "piece1").await);

        // Test writing same piece again (should not error)
        let mut content = Cursor::new("new data");
        let result = cache.write_piece("task1", "piece1", &mut content, 8).await;
        assert!(result.is_ok());

        // Test writing piece with different ID
        let test_content = b"another piece";
        let mut content = Cursor::new(test_content);
        let result = cache
            .write_piece("task1", "piece2", &mut content, test_content.len() as u64)
            .await;
        assert!(result.is_ok());
        assert!(cache.contains_piece("task1", "piece2").await);

        // Verify piece content
        let piece = Piece {
            number: 0,
            offset: 0,
            length: test_content.len() as u64,
            digest: "".to_string(),
            parent_id: None,
            uploading_count: 0,
            uploaded_count: 0,
            updated_at: chrono::Utc::now().naive_utc(),
            created_at: chrono::Utc::now().naive_utc(),
            finished_at: None,
        };
        let mut reader = cache
            .read_piece("task1", "piece2", piece, None)
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, test_content);

        // Test concurrent writes to different pieces
        let cache_arc = Arc::new(cache);
        let mut handles = Vec::new();
        for i in 0..3 {
            let cache_clone = cache_arc.clone();
            let piece_id = format!("concurrent_piece_{}", i);
            let content = format!("concurrent data {}", i);
            let handle = tokio::spawn(async move {
                let mut cursor = Cursor::new(content.as_bytes());
                cache_clone
                    .write_piece("task1", &piece_id, &mut cursor, content.len() as u64)
                    .await
            });
            handles.push(handle);
        }

        // All concurrent writes should succeed
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        // Verify all concurrent pieces exist
        for i in 0..3 {
            assert!(
                cache_arc
                    .contains_piece("task1", &format!("concurrent_piece_{}", i))
                    .await
            );
        }

        // Test concurrent writes to the same piece
        let mut handles = Vec::new();
        let same_piece_content = b"same piece data";
        for _ in 0..3 {
            let cache_clone = cache_arc.clone();
            let content = same_piece_content;
            let handle = tokio::spawn(async move {
                let mut cursor = Cursor::new(content);
                cache_clone
                    .write_piece("task1", "same_piece", &mut cursor, content.len() as u64)
                    .await
            });
            handles.push(handle);
        }

        // All concurrent writes to same piece should succeed (due to early return on exists)
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        // Verify the piece exists
        assert!(cache_arc.contains_piece("task1", "same_piece").await);
    }

    #[tokio::test]
    async fn test_read_piece() {
        // Initialize cache with 10MiB capacity and 8KB read buffer
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                read_buffer_size: 8192,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Add task
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;

        // Define test pieces with corresponding metadata
        let test_pieces = vec![
            (
                "piece1",
                b"hello world".to_vec(),
                Piece {
                    number: 0,
                    offset: 0,
                    length: 11,
                    digest: "".to_string(),
                    parent_id: None,
                    uploading_count: 0,
                    uploaded_count: 0,
                    updated_at: chrono::Utc::now().naive_utc(),
                    created_at: chrono::Utc::now().naive_utc(),
                    finished_at: None,
                },
            ),
            (
                "piece2",
                b"rust lang".to_vec(),
                Piece {
                    number: 1,
                    offset: 11,
                    length: 9,
                    digest: "".to_string(),
                    parent_id: None,
                    uploading_count: 0,
                    uploaded_count: 0,
                    updated_at: chrono::Utc::now().naive_utc(),
                    created_at: chrono::Utc::now().naive_utc(),
                    finished_at: None,
                },
            ),
            (
                "piece3",
                b"unit test".to_vec(),
                Piece {
                    number: 2,
                    offset: 20,
                    length: 9,
                    digest: "".to_string(),
                    parent_id: None,
                    uploading_count: 0,
                    uploaded_count: 0,
                    updated_at: chrono::Utc::now().naive_utc(),
                    created_at: chrono::Utc::now().naive_utc(),
                    finished_at: None,
                },
            ),
            (
                "piece4",
                b"dragonfly".to_vec(),
                Piece {
                    number: 3,
                    offset: 29,
                    length: 9,
                    digest: "".to_string(),
                    parent_id: None,
                    uploading_count: 0,
                    uploaded_count: 0,
                    updated_at: chrono::Utc::now().naive_utc(),
                    created_at: chrono::Utc::now().naive_utc(),
                    finished_at: None,
                },
            ),
            (
                "piece5",
                b"cache test".to_vec(),
                Piece {
                    number: 4,
                    offset: 38,
                    length: 10,
                    digest: "".to_string(),
                    parent_id: None,
                    uploading_count: 0,
                    uploaded_count: 0,
                    updated_at: chrono::Utc::now().naive_utc(),
                    created_at: chrono::Utc::now().naive_utc(),
                    finished_at: None,
                },
            ),
        ];

        // Write all pieces
        for (id, content, _) in &test_pieces {
            let mut cursor = Cursor::new(content);
            cache
                .write_piece("task1", id, &mut cursor, content.len() as u64)
                .await
                .unwrap();
            assert!(cache.contains_piece("task1", id).await);
        }

        // Test reading from non-existent task
        let result = cache
            .read_piece("non_existent", "piece1", test_pieces[0].2.clone(), None)
            .await;
        assert!(matches!(result, Err(Error::TaskNotFound(_))));

        // Test reading non-existent piece
        let result = cache
            .read_piece("task1", "non_existent", test_pieces[0].2.clone(), None)
            .await;
        assert!(matches!(result, Err(Error::PieceNotFound(_))));

        // Test full read for each piece
        for (id, content, meta) in &test_pieces {
            let mut reader = cache
                .read_piece("task1", id, meta.clone(), None)
                .await
                .unwrap();
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, *content);
        }

        // Test reading first 5 bytes of each piece
        for (id, content, meta) in &test_pieces {
            let range = Range {
                start: meta.offset,
                length: 5,
            };
            let mut reader = cache
                .read_piece("task1", id, meta.clone(), Some(range))
                .await
                .unwrap();
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, content[..5].to_vec());
        }

        // Test concurrent reads on all pieces
        let cache_arc = Arc::new(cache);
        let mut handles = Vec::new();
        for (id, content, meta) in &test_pieces {
            let cache_clone = cache_arc.clone();
            let piece_id = id.to_string();
            let expected_content = content.clone();
            let piece_meta = meta.clone();
            let handle = tokio::spawn(async move {
                let mut reader = cache_clone
                    .read_piece("task1", &piece_id, piece_meta, None)
                    .await
                    .unwrap();
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).await.unwrap();
                assert_eq!(buffer, expected_content);
                buffer
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_concurrent() {
        // Initialize cache with 10MiB capacity and 8KB read buffer
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                read_buffer_size: 8192,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Add task before wrapping cache in Arc for concurrent testing
        cache
            .put_task("concurrent_task1", ByteSize::mib(1).as_u64())
            .await;

        // Wrap cache in Arc for concurrent access
        let cache_arc = Arc::new(cache);

        // Test concurrent write operations to different pieces
        // This tests the ability to handle multiple concurrent write operations to different pieces
        // within the same task, verifying thread safety and proper synchronization.
        let (tx, mut rx) = mpsc::unbounded_channel();
        let tx = Arc::new(tx);

        let num_writers = 10;
        let mut handles = Vec::new();

        for i in 0..num_writers {
            let cache_clone = cache_arc.clone();
            let tx_clone = tx.clone();
            let piece_id = format!("piece_{}", i);
            let piece_content = format!("content_{}", i);

            // Spawn concurrent writers
            handles.push(tokio::spawn(async move {
                let mut cursor = Cursor::new(piece_content.as_bytes());
                let result = cache_clone
                    .write_piece(
                        "concurrent_task1",
                        &piece_id,
                        &mut cursor,
                        piece_content.len() as u64,
                    )
                    .await;
                assert!(result.is_ok());
                tx_clone.send(()).unwrap();
            }));
        }

        // Wait for all writers to complete
        for _ in 0..num_writers {
            rx.recv().await.unwrap();
        }

        // Verify all pieces were written correctly
        for i in 0..num_writers {
            let piece_id = format!("piece_{}", i);
            assert!(
                cache_arc
                    .contains_piece("concurrent_task1", &piece_id)
                    .await
            );
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Test concurrent reads of the same piece
        // This tests the ability to handle multiple concurrent read operations to the same piece,
        // verifying read concurrency and data consistency.
        let piece_id = "shared_piece";
        let piece_content = b"shared content for concurrent reading";
        let mut cursor = Cursor::new(piece_content);

        // Write the shared piece
        cache_arc
            .write_piece(
                "concurrent_task1",
                piece_id,
                &mut cursor,
                piece_content.len() as u64,
            )
            .await
            .unwrap();

        // Create piece metadata
        let piece_meta = Piece {
            number: 0,
            offset: 0,
            length: piece_content.len() as u64,
            digest: "".to_string(),
            parent_id: None,
            uploading_count: 0,
            uploaded_count: 0,
            updated_at: chrono::Utc::now().naive_utc(),
            created_at: chrono::Utc::now().naive_utc(),
            finished_at: None,
        };

        // Create a channel to track read completion
        let (tx, mut rx) = mpsc::unbounded_channel();
        let tx = Arc::new(tx);

        // Prepare concurrent read operations on the same piece
        let num_readers = 50;
        let mut handles = Vec::new();

        for _ in 0..num_readers {
            let cache_clone = cache_arc.clone();
            let tx_clone = tx.clone();
            let piece_meta_clone = piece_meta.clone();

            // Spawn concurrent readers
            handles.push(tokio::spawn(async move {
                let mut reader = cache_clone
                    .read_piece("concurrent_task1", piece_id, piece_meta_clone, None)
                    .await
                    .unwrap();

                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).await.unwrap();
                assert_eq!(buffer, piece_content);
                tx_clone.send(()).unwrap();
            }));
        }

        // Wait for all readers to complete
        for _ in 0..num_readers {
            rx.recv().await.unwrap();
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Test simultaneous reads and writes to the same piece
        // This tests a real-world scenario where the same piece is being read and written concurrently,
        // verifying the cache's ability to handle read-write contention without data corruption.
        let (tx, mut rx) = mpsc::unbounded_channel();
        let tx = Arc::new(tx);

        // Prepare piece data
        let piece_id = "rw_piece";
        let original_content = b"original content";
        let new_content = b"new content for the piece";

        // Write original piece content
        let mut cursor = Cursor::new(original_content);
        cache_arc
            .write_piece(
                "concurrent_task1",
                piece_id,
                &mut cursor,
                original_content.len() as u64,
            )
            .await
            .unwrap();

        // Create piece metadata
        let piece_meta = Piece {
            number: 1,
            offset: 100,
            length: original_content.len() as u64,
            digest: "".to_string(),
            parent_id: None,
            uploading_count: 0,
            uploaded_count: 0,
            updated_at: chrono::Utc::now().naive_utc(),
            created_at: chrono::Utc::now().naive_utc(),
            finished_at: None,
        };

        let num_ops = 20; // 10 readers and 10 writers
        let mut handles = Vec::new();

        // Create concurrent readers
        for _ in 0..num_ops / 2 {
            let cache_clone = cache_arc.clone();
            let tx_clone = tx.clone();
            let piece_meta_clone = piece_meta.clone();

            handles.push(tokio::spawn(async move {
                let mut reader = cache_clone
                    .read_piece("concurrent_task1", piece_id, piece_meta_clone, None)
                    .await
                    .unwrap();

                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).await.unwrap();
                // Content could be either original or new, just check it's one of them
                assert!(buffer == original_content || buffer == new_content);
                tx_clone.send(()).unwrap();
            }));
        }

        // Create concurrent writers
        for _ in 0..num_ops / 2 {
            let cache_clone = cache_arc.clone();
            let tx_clone = tx.clone();

            handles.push(tokio::spawn(async move {
                let mut cursor = Cursor::new(new_content);
                let result = cache_clone
                    .write_piece(
                        "concurrent_task1",
                        piece_id,
                        &mut cursor,
                        new_content.len() as u64,
                    )
                    .await;
                // Write may succeed or early return because piece exists
                assert!(result.is_ok());
                tx_clone.send(()).unwrap();
            }));
        }

        // Wait for all operations to complete
        for _ in 0..num_ops {
            rx.recv().await.unwrap();
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Verify the piece still exists
        assert!(cache_arc.contains_piece("concurrent_task1", piece_id).await);
    }
}
