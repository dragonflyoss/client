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

    #[tokio::test]
    async fn test_new() {
        // Define test cases: (config, expected_size, expected_capacity)
        let test_cases = vec![
            // Test with default configuration - should have 0 size and 128MB capacity
            (Config::default(), 0, ByteSize::mb(128).as_u64()),
            // Test with custom configuration - should have 0 size and 100MB capacity
            (
                Config {
                    storage: Storage {
                        cache_capacity: ByteSize::mb(100),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                0,
                ByteSize::mb(100).as_u64(),
            ),
            // Test with zero capacity - should have 0 size and 0 capacity
            (
                Config {
                    storage: Storage {
                        cache_capacity: ByteSize::b(0),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                0,
                0,
            ),
        ];

        // Execute test cases
        for (config, expected_size, expected_capacity) in test_cases {
            let cache = Cache::new(Arc::new(config)).unwrap();
            assert_eq!(cache.size, expected_size);
            assert_eq!(cache.capacity, expected_capacity);
        }
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

        // Define test cases: (operation, task_id, task_content_length, expected_result)
        // Operations: "check", "add", "remove"
        let test_cases = vec![
            // Test non-existent task
            ("check", "non_existent", 0, false),
            // Add task and check
            ("add", "task1", ByteSize::mib(1).as_u64(), true),
            ("check", "task1", 0, true),
            // Remove task and check
            ("remove", "task1", 0, false),
            ("check", "task1", 0, false),
            // Test multiple tasks
            ("add", "task1", ByteSize::mib(1).as_u64(), true),
            ("add", "task2", ByteSize::mib(2).as_u64(), true),
            ("check", "task1", 0, true),
            ("check", "task2", 0, true),
            ("check", "task3", 0, false),
        ];

        // Execute test cases
        for (operation, task_id, content_length, expected_result) in test_cases {
            match operation {
                "check" => {
                    assert_eq!(cache.contains_task(task_id).await, expected_result);
                }
                "add" => {
                    let task = Task::new(task_id.to_string(), content_length);
                    cache.tasks.write().await.put(task_id.to_string(), task);
                    assert_eq!(cache.contains_task(task_id).await, expected_result);
                }
                "remove" => {
                    cache.tasks.write().await.pop_lru();
                    assert_eq!(cache.contains_task(task_id).await, expected_result);
                }
                _ => panic!("Unknown operation"),
            }
        }
    }

    #[tokio::test]
    async fn test_put_task() {
        // Test basic boundary conditions with 10MiB capacity
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Test cases for basic conditions: (task_id, size, should_exist)
        let test_cases = vec![
            // Empty task should not be cached
            ("empty_task", 0, false),
            // Task equal to capacity should not be cached
            ("equal_capacity", ByteSize::mib(10).as_u64(), false),
            // Task exceeding capacity should not be cached
            ("exceed_capacity", ByteSize::mib(10).as_u64() + 1, false),
            // Normal sized task should be cached
            ("normal_task", ByteSize::mib(1).as_u64(), true),
        ];

        // Execute basic condition tests
        for (task_id, size, should_exist) in test_cases {
            if size > 0 {
                cache.put_task(task_id, size).await;
            }
            assert_eq!(cache.contains_task(task_id).await, should_exist);
        }

        // Test LRU eviction with 5MiB capacity
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(5),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Test cases for LRU eviction: (task_id, size, should_exist)
        let test_cases = vec![
            // Add multiple tasks until eviction is triggered
            ("lru_task_1", ByteSize::mib(2).as_u64(), true),
            ("lru_task_2", ByteSize::mib(2).as_u64(), true),
            // This should cause the first task to be evicted
            ("lru_task_3", ByteSize::mib(2).as_u64(), true),
            // Verify first task is evicted and recent tasks exist
            ("lru_task_1", 0, false),
            ("lru_task_2", 0, true),
            ("lru_task_3", 0, true),
        ];

        // Execute LRU eviction tests
        for (task_id, size, should_exist) in test_cases {
            if size > 0 {
                cache.put_task(task_id, size).await;
            }
            assert_eq!(cache.contains_task(task_id).await, should_exist);
        }
    }

    #[tokio::test]
    async fn test_contains_piece() {
        // Initialize cache with 10MiB capacity
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Define test cases: (operation, task_id, piece_id, content, expected_result)
        let test_cases = vec![
            // Check non-existent task
            ("check", "non_existent", "piece1", "", false),
            // Check empty piece ID in non-existent task
            ("check", "non_existent", "", "", false),
            // Add task and verify empty task behavior
            ("add_task", "task1", "", "", true),
            ("check", "task1", "piece1", "", false),
            // Add piece and verify existence
            ("add_piece", "task1", "piece1", "test data", true),
            ("check", "task1", "piece1", "", true),
            // Check empty piece ID in existing task
            ("check", "task1", "", "", false),
            // Check non-existent piece in existing task
            ("check", "task1", "non_existent_piece", "", false),
            // Test piece ID with special characters
            ("add_piece", "task1", "piece#$%^&*", "test data", true),
            ("check", "task1", "piece#$%^&*", "", true),
        ];

        // Execute test cases
        for (operation, task_id, piece_id, content, expected_result) in test_cases {
            match operation {
                "check" => {
                    assert_eq!(
                        cache.contains_piece(task_id, piece_id).await,
                        expected_result
                    );
                }
                "add_task" => {
                    cache.put_task(task_id, 1000).await;
                    assert!(cache.contains_task(task_id).await);
                }
                "add_piece" => {
                    let mut cursor = Cursor::new(content);
                    cache
                        .write_piece(task_id, piece_id, &mut cursor, content.len() as u64)
                        .await
                        .unwrap();
                    assert_eq!(
                        cache.contains_piece(task_id, piece_id).await,
                        expected_result
                    );
                }
                _ => panic!("Unknown operation"),
            }
        }

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
    async fn test_write_piece() {
        // Initialize cache with 10MiB capacity
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Test writing to non-existent task
        let test_data = b"test data".to_vec();
        let mut cursor = Cursor::new(&test_data);
        let result = cache
            .write_piece(
                "non_existent",
                "piece1",
                &mut cursor,
                test_data.len() as u64,
            )
            .await;
        assert!(matches!(result, Err(Error::TaskNotFound(_))));

        // Create a task for testing
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;
        assert!(cache.contains_task("task1").await);

        // Test cases: (piece_id, content)
        let test_cases = vec![
            ("piece1", b"hello world".to_vec()),
            ("piece2", b"rust programming".to_vec()),
            ("piece3", b"dragonfly cache".to_vec()),
            ("piece4", b"unit testing".to_vec()),
            ("piece5", b"async await".to_vec()),
            ("piece6", b"error handling".to_vec()),
            ("piece7", vec![0u8; 1024]),
            ("piece8", vec![1u8; 2048]),
        ];

        // Write all pieces and verify
        for (piece_id, content) in &test_cases {
            let mut cursor = Cursor::new(content);
            let result = cache
                .write_piece("task1", piece_id, &mut cursor, content.len() as u64)
                .await;
            assert!(result.is_ok());
            assert!(cache.contains_piece("task1", piece_id).await);

            // Verify content with correct piece metadata
            let piece = Piece {
                number: 0,
                offset: 0,
                length: content.len() as u64,
                digest: "".to_string(),
                parent_id: None,
                uploading_count: 0,
                uploaded_count: 0,
                updated_at: chrono::Utc::now().naive_utc(),
                created_at: chrono::Utc::now().naive_utc(),
                finished_at: None,
            };

            let mut reader = cache
                .read_piece("task1", piece_id, piece, None)
                .await
                .unwrap();

            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, *content);
        }

        // Test attempting to overwrite existing pieces
        // The write should succeed (return Ok) but content should not change
        for (piece_id, original_content) in &test_cases {
            let new_content = format!("updated content for {}", piece_id).into_bytes();
            let mut cursor = Cursor::new(&new_content);
            let result = cache
                .write_piece("task1", piece_id, &mut cursor, new_content.len() as u64)
                .await;
            assert!(result.is_ok());

            // Verify content remains unchanged
            let piece = Piece {
                number: 0,
                offset: 0,
                length: original_content.len() as u64,
                digest: "".to_string(),
                parent_id: None,
                uploading_count: 0,
                uploaded_count: 0,
                updated_at: chrono::Utc::now().naive_utc(),
                created_at: chrono::Utc::now().naive_utc(),
                finished_at: None,
            };

            let mut reader = cache
                .read_piece("task1", piece_id, piece, None)
                .await
                .unwrap();

            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, *original_content);
        }

        // Test concurrent writes to new pieces
        let cache_arc = Arc::new(cache);
        let mut handles = Vec::new();

        for i in 0..10 {
            let cache_clone = cache_arc.clone();
            let piece_id = format!("concurrent_piece_{}", i);
            let content = format!("concurrent data {}", i).into_bytes();

            let handle = tokio::spawn(async move {
                let mut cursor = Cursor::new(&content);
                let result = cache_clone
                    .write_piece("task1", &piece_id, &mut cursor, content.len() as u64)
                    .await;
                (piece_id, content, result)
            });
            handles.push(handle);
        }

        // Verify concurrent writes
        for handle in handles {
            let (piece_id, content, result) = handle.await.unwrap();
            assert!(result.is_ok());
            assert!(cache_arc.contains_piece("task1", &piece_id).await);

            // Verify content
            let piece = Piece {
                number: 0,
                offset: 0,
                length: content.len() as u64,
                digest: "".to_string(),
                parent_id: None,
                uploading_count: 0,
                uploaded_count: 0,
                updated_at: chrono::Utc::now().naive_utc(),
                created_at: chrono::Utc::now().naive_utc(),
                finished_at: None,
            };

            let mut reader = cache_arc
                .read_piece("task1", &piece_id, piece, None)
                .await
                .unwrap();

            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, content);
        }

        // Test concurrent writes to same piece
        let mut handles = Vec::new();
        let piece_id = "concurrent_same_piece";
        let original_content = b"original content".to_vec();

        // First write to create the piece
        let mut cursor = Cursor::new(&original_content);
        let result = cache_arc
            .write_piece(
                "task1",
                piece_id,
                &mut cursor,
                original_content.len() as u64,
            )
            .await;
        assert!(result.is_ok());

        // Try concurrent writes to the same piece
        for i in 0..10 {
            let cache_clone = cache_arc.clone();
            let content = format!("concurrent overwrite attempt {}", i).into_bytes();

            let handle = tokio::spawn(async move {
                let mut cursor = Cursor::new(&content);
                let result = cache_clone
                    .write_piece("task1", piece_id, &mut cursor, content.len() as u64)
                    .await;
                (content, result)
            });
            handles.push(handle);
        }

        // Verify all concurrent writes return Ok but content remains unchanged
        for handle in handles {
            let (_, result) = handle.await.unwrap();
            assert!(result.is_ok());
        }

        // Verify the content remains unchanged
        let piece = Piece {
            number: 0,
            offset: 0,
            length: original_content.len() as u64,
            digest: "".to_string(),
            parent_id: None,
            uploading_count: 0,
            uploaded_count: 0,
            updated_at: chrono::Utc::now().naive_utc(),
            created_at: chrono::Utc::now().naive_utc(),
            finished_at: None,
        };

        let mut reader = cache_arc
            .read_piece("task1", piece_id, piece, None)
            .await
            .unwrap();

        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, original_content);
    }

    #[tokio::test]
    async fn test_read_piece() {
        // Initialize cache with 10MiB capacity
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
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
        let (_, _, meta) = &test_pieces[0];
        let result = cache
            .read_piece("non_existent", "piece1", meta.clone(), None)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::TaskNotFound(_))));

        // Test reading non-existent piece
        let result = cache
            .read_piece("task1", "non_existent", test_pieces[0].2.clone(), None)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::PieceNotFound(_))));

        // Test full read of piece1
        let (id, content, meta) = &test_pieces[0];
        let mut reader = cache
            .read_piece("task1", id, meta.clone(), None)
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, *content);
        assert_eq!(buffer.len(), 11);

        // Test full read of piece2
        let (id, content, meta) = &test_pieces[1];
        let mut reader = cache
            .read_piece("task1", id, meta.clone(), None)
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, *content);
        assert_eq!(buffer.len(), 9);

        // Test partial read of piece1 (first 5 bytes)
        let (id, content, meta) = &test_pieces[0];
        let range = Range {
            start: 0,
            length: 5,
        };
        let mut reader = cache
            .read_piece("task1", id, meta.clone(), Some(range))
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        let start_offset = (range.start - meta.offset) as usize;
        let expected_content = &content[start_offset..(start_offset + range.length as usize)];
        assert_eq!(buffer, expected_content);
        assert_eq!(buffer.len(), 5);

        // Test partial read of piece2 (first 5 bytes)
        let (id, content, meta) = &test_pieces[1];
        let range = Range {
            start: 11,
            length: 5,
        };
        let mut reader = cache
            .read_piece("task1", id, meta.clone(), Some(range))
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        let start_offset = (range.start - meta.offset) as usize;
        let expected_content = &content[start_offset..(start_offset + range.length as usize)];
        assert_eq!(buffer, expected_content);
        assert_eq!(buffer.len(), 5);

        // Test concurrent reads
        let cache_arc = Arc::new(cache);
        let mut handles = Vec::new();

        // Launch concurrent reads for all pieces
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

        // Verify all concurrent reads succeeded
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_concurrent_read() {
        // Initialize cache with 10MiB capacity
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Create a task and write test piece
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;

        // Write test pieces with different sizes
        let test_pieces = vec![
            ("piece1", b"small content".to_vec()),
            ("piece2", vec![1u8; 1024]), // 1KB
            ("piece3", vec![2u8; 4096]), // 4KB
        ];

        for (id, content) in &test_pieces {
            let mut cursor = Cursor::new(content);
            cache
                .write_piece("task1", id, &mut cursor, content.len() as u64)
                .await
                .unwrap();
        }

        let cache_arc = Arc::new(cache);
        let mut join_set = tokio::task::JoinSet::new();

        // Spawn concurrent readers
        for reader_id in 0..50 {
            let cache_clone = cache_arc.clone();
            let test_pieces = test_pieces.clone();

            join_set.spawn(async move {
                for i in 0..20 {
                    // Randomly select a piece to read
                    let piece_idx = i % test_pieces.len();
                    let (piece_id, expected_content) = &test_pieces[piece_idx];

                    let piece = Piece {
                        number: 0,
                        offset: 0,
                        length: expected_content.len() as u64,
                        digest: "".to_string(),
                        parent_id: None,
                        uploading_count: 0,
                        uploaded_count: 0,
                        updated_at: chrono::Utc::now().naive_utc(),
                        created_at: chrono::Utc::now().naive_utc(),
                        finished_at: None,
                    };

                    // Randomly choose between full read and partial read
                    let range = if i % 2 == 0 {
                        None
                    } else {
                        Some(Range {
                            start: 0,
                            length: std::cmp::min(5, expected_content.len() as u64),
                        })
                    };

                    let mut reader = cache_clone
                        .read_piece("task1", piece_id, piece, range)
                        .await
                        .unwrap_or_else(|e| {
                            panic!("Reader {} failed at iteration {}: {:?}", reader_id, i, e)
                        });

                    let mut buffer = Vec::new();
                    reader.read_to_end(&mut buffer).await.unwrap();

                    let expected = if let Some(range) = range {
                        expected_content[0..range.length as usize].to_vec()
                    } else {
                        expected_content.clone()
                    };

                    assert_eq!(buffer, expected);
                }
                Ok::<_, Error>(())
            });
        }

        // Wait for all readers to complete
        while let Some(result) = join_set.join_next().await {
            result.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn test_concurrent_write() {
        // Initialize cache with 10MiB capacity
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;
        cache.put_task("task2", ByteSize::mib(1).as_u64()).await;

        let cache_arc = Arc::new(cache);
        let mut join_set = tokio::task::JoinSet::new();

        // Test 1: Multiple writers writing to different pieces
        for writer_id in 0..50 {
            let cache_clone = cache_arc.clone();

            join_set.spawn(async move {
                for i in 0..20 {
                    let piece_id = format!("piece_{}_{}", writer_id, i);
                    let content =
                        format!("content from writer {} iteration {}", writer_id, i).into_bytes();

                    let mut cursor = Cursor::new(&content);
                    let result = cache_clone
                        .write_piece("task1", &piece_id, &mut cursor, content.len() as u64)
                        .await;

                    assert!(result.is_ok());

                    // Verify write succeeded and content is correct
                    let piece = Piece {
                        number: 0,
                        offset: 0,
                        length: content.len() as u64,
                        digest: "".to_string(),
                        parent_id: None,
                        uploading_count: 0,
                        uploaded_count: 0,
                        updated_at: chrono::Utc::now().naive_utc(),
                        created_at: chrono::Utc::now().naive_utc(),
                        finished_at: None,
                    };

                    let mut reader = cache_clone
                        .read_piece("task1", &piece_id, piece, None)
                        .await
                        .unwrap();

                    let mut buffer = Vec::new();
                    reader.read_to_end(&mut buffer).await.unwrap();
                    assert_eq!(buffer, content);
                }
                Ok::<_, Error>(())
            });
        }

        // Wait for all writers to complete
        while let Some(result) = join_set.join_next().await {
            result.unwrap().unwrap();
        }

        // Test 2: Multiple writers attempting to write to the same piece
        let mut join_set = tokio::task::JoinSet::new();
        let shared_piece = "shared_piece";
        let original_content = b"original content".to_vec();

        // First write to create the piece
        let mut cursor = Cursor::new(&original_content);
        cache_arc
            .write_piece(
                "task2",
                shared_piece,
                &mut cursor,
                original_content.len() as u64,
            )
            .await
            .unwrap();

        // Spawn multiple writers trying to overwrite the same piece
        for writer_id in 0..50 {
            let cache_clone = cache_arc.clone();

            join_set.spawn(async move {
                for i in 0..20 {
                    let content =
                        format!("overwrite attempt {} from writer {}", i, writer_id).into_bytes();
                    let mut cursor = Cursor::new(&content);
                    let result = cache_clone
                        .write_piece("task2", shared_piece, &mut cursor, content.len() as u64)
                        .await;

                    assert!(result.is_ok());
                }
                Ok::<_, Error>(())
            });
        }

        // Wait for all writers to complete
        while let Some(result) = join_set.join_next().await {
            result.unwrap().unwrap();
        }

        // Verify the content remains unchanged
        let piece = Piece {
            number: 0,
            offset: 0,
            length: original_content.len() as u64,
            digest: "".to_string(),
            parent_id: None,
            uploading_count: 0,
            uploaded_count: 0,
            updated_at: chrono::Utc::now().naive_utc(),
            created_at: chrono::Utc::now().naive_utc(),
            finished_at: None,
        };

        let mut reader = cache_arc
            .read_piece("task2", shared_piece, piece, None)
            .await
            .unwrap();

        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, original_content);
    }
}
