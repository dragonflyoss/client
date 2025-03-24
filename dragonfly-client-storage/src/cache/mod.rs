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
    /// Tests cache creation with different configurations.
    async fn test_new() {
        let test_cases = vec![
            // Default configuration with 128MB capacity.
            (Config::default(), 0, ByteSize::mb(128).as_u64()),
            // Custom configuration with 100MB capacity.
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
            // Zero capacity configuration.
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

        // Execute test cases.
        for (config, expected_size, expected_capacity) in test_cases {
            let cache = Cache::new(Arc::new(config)).unwrap();
            assert_eq!(cache.size, expected_size);
            assert_eq!(cache.capacity, expected_capacity);
        }
    }

    #[tokio::test]
    /// Tests task existence checks in cache.
    async fn test_contains_task() {
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let cache = Cache::new(Arc::new(config)).unwrap();

        let test_cases = vec![
            // Test non-existent task.
            ("check", "non_existent", 0, false),
            // Add and verify task.
            ("add", "task1", ByteSize::mib(1).as_u64(), true),
            ("check", "task1", 0, true),
            // Remove and verify task.
            ("remove", "task1", 0, false),
            ("check", "task1", 0, false),
            // Test multiple tasks.
            ("add", "task1", ByteSize::mib(1).as_u64(), true),
            ("add", "task2", ByteSize::mib(2).as_u64(), true),
            ("check", "task1", 0, true),
            ("check", "task2", 0, true),
            ("check", "task3", 0, false),
        ];

        // Execute test cases.
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
                _ => panic!("Unknown operation."),
            }
        }
    }

    #[tokio::test]
    /// Tests task addition boundary conditions.
    async fn test_put_task() {
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Test boundary cases.
        let test_cases = vec![
            // Empty task should not be cached.
            ("empty_task", 0, false),
            // Task equal to capacity should not be cached.
            ("equal_capacity", ByteSize::mib(10).as_u64(), false),
            // Task exceeding capacity should not be cached.
            ("exceed_capacity", ByteSize::mib(10).as_u64() + 1, false),
            // Normal sized task should be cached.
            ("normal_task", ByteSize::mib(1).as_u64(), true),
        ];

        // Execute test cases.
        for (task_id, size, should_exist) in test_cases {
            if size > 0 {
                cache.put_task(task_id, size).await;
            }
            assert_eq!(cache.contains_task(task_id).await, should_exist);
        }
    }

    #[tokio::test]
    /// Tests LRU cache eviction mechanism.
    async fn test_put_task_lru() {
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(5),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Test LRU eviction.
        let test_cases = vec![
            // Add tasks until eviction triggers.
            ("lru_task_1", ByteSize::mib(2).as_u64(), true),
            ("lru_task_2", ByteSize::mib(2).as_u64(), true),
            // Third task triggers eviction.
            ("lru_task_3", ByteSize::mib(2).as_u64(), true),
            // Verify eviction results.
            ("lru_task_1", 0, false),
            ("lru_task_2", 0, true),
            ("lru_task_3", 0, true),
        ];

        // Execute test cases.
        for (task_id, size, should_exist) in test_cases {
            if size > 0 {
                cache.put_task(task_id, size).await;
            }
            assert_eq!(cache.contains_task(task_id).await, should_exist);
        }
    }

    #[tokio::test]
    /// Tests piece existence checks in cache.
    async fn test_contains_piece() {
        // Initialize cache with 10MiB capacity.
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Define test cases: (operation, task_id, piece_id, content, expected_result).
        let test_cases = vec![
            // Check non-existent task.
            ("check", "non_existent", "piece1", "", false),
            // Check empty piece ID in non-existent task.
            ("check", "non_existent", "", "", false),
            // Add task and verify empty task behavior.
            ("add_task", "task1", "", "", true),
            ("check", "task1", "piece1", "", false),
            // Add piece and verify existence.
            ("add_piece", "task1", "piece1", "test data", true),
            ("check", "task1", "piece1", "", true),
            // Check empty piece ID in existing task.
            ("check", "task1", "", "", false),
            // Check non-existent piece in existing task.
            ("check", "task1", "non_existent_piece", "", false),
            // Test piece ID with special characters.
            ("add_piece", "task1", "piece#$%^&*", "test data", true),
            ("check", "task1", "piece#$%^&*", "", true),
        ];

        // Execute test cases.
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
                _ => panic!("Unknown operation."),
            }
        }
    }

    #[tokio::test]
    /// Tests piece writing to cache.
    async fn test_write_piece() {
        // Initialize cache with 10MiB capacity.
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Test writing to non-existent task.
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

        // Create a task for testing.
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;
        assert!(cache.contains_task("task1").await);

        // Test cases: (piece_id, content).
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

        // Write all pieces and verify.
        for (piece_id, content) in &test_cases {
            let mut cursor = Cursor::new(content);
            let result = cache
                .write_piece("task1", piece_id, &mut cursor, content.len() as u64)
                .await;
            assert!(result.is_ok());
            assert!(cache.contains_piece("task1", piece_id).await);

            // Verify content with correct piece metadata.
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

        // Test attempting to overwrite existing pieces.
        // The write should succeed (return Ok) but content should not change.
        for (piece_id, original_content) in &test_cases {
            let new_content = format!("updated content for {}", piece_id).into_bytes();
            let mut cursor = Cursor::new(&new_content);
            let result = cache
                .write_piece("task1", piece_id, &mut cursor, new_content.len() as u64)
                .await;
            assert!(result.is_ok());

            // Verify content remains unchanged.
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
    }

    #[tokio::test]
    /// Tests piece reading from cache.
    async fn test_read_piece() {
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(100),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();

        // Test error cases first.
        let piece = Piece {
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
        };

        let result = cache
            .read_piece("non_existent", "piece1", piece.clone(), None)
            .await;
        assert!(matches!(result, Err(Error::TaskNotFound(_))));

        // Create task for testing.
        cache.put_task("task1", ByteSize::mib(50).as_u64()).await;

        let result = cache
            .read_piece("task1", "non_existent", piece.clone(), None)
            .await;
        assert!(matches!(result, Err(Error::PieceNotFound(_))));

        // Define test pieces with corresponding metadata and read ranges.
        let test_pieces = vec![
            // Small pieces for basic functionality testing
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
                vec![
                    (None, b"hello world".to_vec()),
                    (
                        Some(Range {
                            start: 0,
                            length: 5,
                        }),
                        b"hello".to_vec(),
                    ),
                ],
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
                vec![
                    (None, b"rust lang".to_vec()),
                    (
                        Some(Range {
                            start: 11,
                            length: 4,
                        }),
                        b"rust".to_vec(),
                    ),
                ],
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
                vec![
                    (None, b"unit test".to_vec()),
                    (
                        Some(Range {
                            start: 20,
                            length: 4,
                        }),
                        b"unit".to_vec(),
                    ),
                ],
            ),
            // Large piece for boundary testing
            (
                "large_piece",
                {
                    let size = ByteSize::mib(50).as_u64();
                    (0..size).map(|i| (i % 256) as u8).collect()
                },
                Piece {
                    number: 2,
                    offset: 0,
                    length: ByteSize::mib(50).as_u64(),
                    digest: "".to_string(),
                    parent_id: None,
                    uploading_count: 0,
                    uploaded_count: 0,
                    updated_at: chrono::Utc::now().naive_utc(),
                    created_at: chrono::Utc::now().naive_utc(),
                    finished_at: None,
                },
                vec![
                    // Full read
                    (
                        None,
                        (0..ByteSize::mib(50).as_u64())
                            .map(|i| (i % 256) as u8)
                            .collect(),
                    ),
                    // Read first 1MiB
                    (
                        Some(Range {
                            start: 0,
                            length: ByteSize::mib(1).as_u64(),
                        }),
                        (0..ByteSize::mib(1).as_u64())
                            .map(|i| (i % 256) as u8)
                            .collect(),
                    ),
                    // Read last 1MiB
                    (
                        Some(Range {
                            start: ByteSize::mib(49).as_u64(),
                            length: ByteSize::mib(1).as_u64(),
                        }),
                        (ByteSize::mib(49).as_u64()..ByteSize::mib(50).as_u64())
                            .map(|i| (i % 256) as u8)
                            .collect(),
                    ),
                ],
            ),
        ];

        // Write all pieces
        for (id, content, _, _) in &test_pieces {
            let mut cursor = Cursor::new(content);
            cache
                .write_piece("task1", id, &mut cursor, content.len() as u64)
                .await
                .unwrap();
        }

        // Test all pieces with their read ranges
        for (id, _, piece, ranges) in &test_pieces {
            for (range, expected_content) in ranges {
                let mut reader = cache
                    .read_piece("task1", id, piece.clone(), *range)
                    .await
                    .unwrap();
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).await.unwrap();
                assert_eq!(&buffer, expected_content);
            }
        }
    }

    #[tokio::test]
    /// Tests concurrent reads of the same piece.
    async fn test_concurrent_read_same_piece() {
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;

        // Write test data.
        let content = b"test data for concurrent read".to_vec();
        let mut cursor = Cursor::new(&content);
        cache
            .write_piece("task1", "piece1", &mut cursor, content.len() as u64)
            .await
            .unwrap();

        let cache_arc = Arc::new(cache);
        let mut join_set = tokio::task::JoinSet::new();

        // Spawn concurrent readers.
        for i in 0..50 {
            let cache_clone = cache_arc.clone();
            let expected_content = content.clone();

            join_set.spawn(async move {
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

                // Alternate between full and partial reads.
                let range = if i % 2 == 0 {
                    None
                } else {
                    Some(Range {
                        start: 0,
                        length: 5,
                    })
                };

                let mut reader = cache_clone
                    .read_piece("task1", "piece1", piece, range)
                    .await
                    .unwrap_or_else(|e| panic!("Reader {} failed: {:?}.", i, e));

                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).await.unwrap();

                if let Some(range) = range {
                    assert_eq!(buffer, &expected_content[..range.length as usize]);
                } else {
                    assert_eq!(buffer, expected_content);
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    /// Tests concurrent writes to different pieces.
    async fn test_concurrent_write_different_pieces() {
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;

        let cache_arc = Arc::new(cache);
        let mut join_set = tokio::task::JoinSet::new();

        // Spawn concurrent writers.
        for i in 0..50 {
            let cache_clone = cache_arc.clone();
            let content = format!("content for piece {}", i).into_bytes();

            join_set.spawn(async move {
                let mut cursor = Cursor::new(&content);
                let piece_id = format!("piece{}", i);
                let result = cache_clone
                    .write_piece("task1", &piece_id, &mut cursor, content.len() as u64)
                    .await;
                assert!(result.is_ok());

                // Verify written content.
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
            });
        }

        while let Some(result) = join_set.join_next().await {
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    /// Tests concurrent writes to the same piece.
    async fn test_concurrent_write_same_piece() {
        let config = Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut cache = Cache::new(Arc::new(config)).unwrap();
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;

        // Write initial content.
        let original_content = b"original content".to_vec();
        let mut cursor = Cursor::new(&original_content);
        cache
            .write_piece(
                "task1",
                "piece1",
                &mut cursor,
                original_content.len() as u64,
            )
            .await
            .unwrap();

        let cache_arc = Arc::new(cache);
        let mut join_set = tokio::task::JoinSet::new();

        // Spawn concurrent writers.
        for i in 0..50 {
            let cache_clone = cache_arc.clone();
            let new_content = format!("new content from writer {}", i).into_bytes();

            join_set.spawn(async move {
                let mut cursor = Cursor::new(&new_content);
                let result = cache_clone
                    .write_piece("task1", "piece1", &mut cursor, new_content.len() as u64)
                    .await;
                assert!(result.is_ok());
            });
        }

        while let Some(result) = join_set.join_next().await {
            assert!(result.is_ok());
        }

        // Verify content remains unchanged.
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
            .read_piece("task1", "piece1", piece, None)
            .await
            .unwrap();

        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, original_content);
    }
}
