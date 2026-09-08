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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncBufRead;
use tokio::sync::RwLock;
use tracing::{error, info};

pub mod lru_cache;

/// The task content in the cache.
#[derive(Clone, Debug)]
struct Task {
    /// The length of the task content.
    content_length: u64,

    /// The pieces content of the task.
    pieces: Arc<RwLock<HashMap<String, Bytes>>>,
}

/// Implements the task content in the cache.
impl Task {
    /// Creates a new task.
    fn new(content_length: u64) -> Self {
        Self {
            content_length,
            pieces: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Writes the piece content to the task.
    async fn write_piece(&self, id: &str, piece: Bytes) {
        let mut pieces = self.pieces.write().await;
        pieces.insert(id.to_string(), piece);
    }

    /// Reads the piece content from the task.
    async fn read_piece(&self, id: &str) -> Option<Bytes> {
        let pieces = self.pieces.read().await;
        pieces.get(id).cloned()
    }

    /// Checks whether the piece exists in the task.
    async fn contains(&self, id: &str) -> bool {
        let pieces = self.pieces.read().await;
        pieces.contains_key(id)
    }

    /// Returns the content length of the task.
    fn content_length(&self) -> u64 {
        self.content_length
    }
}

/// The cache for storing piece content by LRU algorithm.
///
/// Cache storage:
/// 1. Users can preheat task by caching to memory (via CacheTask) or to disk (via Task).
///    For more details, refer to https://github.com/dragonflyoss/api/blob/main/proto/dfdaemon.proto#L174.
/// 2. If the download hits the memory cache, it will be faster than reading from the disk, because there is no
///    page cache for the first read.
///
///```text
///                    +--------+
///                    │ Source │
///                    +--------+
///                       ^ ^                Preheat
///                       │ │                   |
/// +-----------------+   │ │    +----------------------------+
/// │   Other Peers   │   │ │    │  Peer        |             │
/// │                 │   │ │    │              v             │
/// │  +----------+   │   │ │    │        +----------+        │
/// │  │  Cache   |<--|----------|<-Miss--|  Cache   |--Hit-->|<----Download CacheTask
/// │  +----------+   │     │    │        +----------+        │
/// │                 │     │    │                            │
/// │  +----------+   │     │    │        +----------+        │
/// │  │   Disk   |<--|----------|<-Miss--|   Disk   |--Hit-->|<----Download Task
/// │  +----------+   │          │        +----------+        │
/// │                 │          │              ^             │
/// │                 │          │              |             │
/// +-----------------+          +----------------------------+
///                                             |
///                                          Preheat
///```    
/// Task is the metadata of the task.
#[derive(Clone)]
pub struct Cache {
    /// The size of the cache in bytes.
    size: Arc<AtomicU64>,

    /// The maximum capacity of the cache in bytes.
    capacity: u64,

    /// Stores the tasks with their task id.
    tasks: Arc<RwLock<LruCache<String, Task>>>,
}

/// Implements the cache for storing piece content by LRU algorithm.
impl Cache {
    /// Creates a new cache with the specified capacity.
    pub fn new(config: Arc<Config>) -> Self {
        Cache {
            size: Arc::new(AtomicU64::new(0)),
            capacity: config.storage.cache_capacity.as_u64(),
            // LRU cache capacity is set to usize::MAX to avoid evicting tasks. LRU cache will evict tasks
            // by cache capacity(cache size) itself, and used pop_lru to evict the least recently
            // used task.
            tasks: Arc::new(RwLock::new(LruCache::new(usize::MAX))),
        }
    }

    /// Reads the piece from the cache.
    pub async fn read_piece(
        &self,
        task_id: &str,
        piece_id: &str,
        piece: super::metadata::Piece,
        range: Option<Range>,
    ) -> Result<impl AsyncBufRead> {
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
            error!(
                "invalid range for piece {} in task {}: begin {}, end {}, piece length {}",
                piece_id,
                task_id,
                begin,
                end,
                piece_content.len()
            );

            return Err(Error::InvalidParameter);
        }

        let content = piece_content.slice(begin..end);
        Ok(Cursor::new(content))
    }

    /// Writes the piece content to the cache.
    pub async fn write_piece(&self, task_id: &str, piece_id: &str, content: Bytes) -> Result<()> {
        let mut tasks = self.tasks.write().await;
        let Some(task) = tasks.get(task_id) else {
            return Err(Error::TaskNotFound(task_id.to_string()));
        };

        if task.contains(piece_id).await {
            return Ok(());
        }

        task.write_piece(piece_id, content).await;
        Ok(())
    }

    /// Puts a new task into the cache, constrained by the capacity of the cache.
    pub async fn put_task(&mut self, task_id: &str, content_length: u64) {
        // If the content length is 0, we don't cache the task.
        if content_length == 0 {
            return;
        }

        // If the content length is larger than the cache capacity and the task cannot be cached.
        if content_length > self.capacity {
            info!(
                "task {} is too large and cannot be cached: {}",
                task_id, content_length
            );

            return;
        }

        let mut tasks = self.tasks.write().await;
        while self.size.load(Ordering::Relaxed) + content_length > self.capacity {
            match tasks.pop_lru() {
                Some((_, task)) => {
                    self.size
                        .fetch_sub(task.content_length(), Ordering::Relaxed);
                }
                None => {
                    break;
                }
            }
        }

        let task = Task::new(content_length);
        tasks.put(task_id.to_string(), task);
        self.size.fetch_add(content_length, Ordering::Relaxed);
    }

    pub async fn delete_task(&mut self, task_id: &str) -> Result<()> {
        let mut tasks = self.tasks.write().await;
        let Some((_, task)) = tasks.pop(task_id) else {
            return Err(Error::TaskNotFound(task_id.to_string()));
        };

        self.size
            .fetch_sub(task.content_length(), Ordering::Relaxed);
        Ok(())
    }

    /// Checks whether the task exists in the cache.
    pub async fn contains_task(&self, id: &str) -> bool {
        let tasks = self.tasks.read().await;
        tasks.contains(id)
    }

    /// Checks whether the piece exists in the specified task.
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
    #![allow(clippy::type_complexity)]

    use super::super::metadata::Piece;
    use super::*;
    use bytesize::ByteSize;
    use dragonfly_client_config::dfdaemon::Storage;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn new_uses_configured_cache_capacity() {
        let test_cases = vec![
            (Config::default(), ByteSize::mib(64).as_u64()),
            (
                Config {
                    storage: Storage {
                        cache_capacity: ByteSize::mib(100),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                ByteSize::mib(100).as_u64(),
            ),
            (
                Config {
                    storage: Storage {
                        cache_capacity: ByteSize::b(0),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                0,
            ),
        ];

        for (config, expected_capacity) in test_cases {
            let cache = Cache::new(Arc::new(config));
            assert_eq!(cache.size.load(Ordering::Relaxed), 0);
            assert_eq!(cache.capacity, expected_capacity);
        }
    }

    #[tokio::test]
    async fn put_task_skips_empty_or_oversized_tasks() {
        let test_cases = vec![
            (0, 0),
            (ByteSize::mib(1).as_u64(), ByteSize::mib(1).as_u64()),
            (ByteSize::mib(10).as_u64(), ByteSize::mib(10).as_u64()),
            (ByteSize::mib(10).as_u64() + 1, 0),
        ];

        for (content_length, expected_size) in test_cases {
            let mut cache = Cache::new(Arc::new(Config {
                storage: Storage {
                    cache_capacity: ByteSize::mib(10),
                    ..Default::default()
                },
                ..Default::default()
            }));
            cache.put_task("task1", content_length).await;
            assert_eq!(cache.contains_task("task1").await, expected_size > 0);
            assert_eq!(cache.size.load(Ordering::Relaxed), expected_size);
        }
    }

    #[tokio::test]
    async fn put_task_evicts_least_recently_used_tasks_to_fit() {
        let test_cases = vec![
            (
                vec![("task1", 2), ("task2", 2), ("task3", 1)],
                vec!["task1", "task2", "task3"],
                5,
            ),
            (
                vec![("task1", 2), ("task2", 2), ("task3", 2)],
                vec!["task2", "task3"],
                4,
            ),
            (
                vec![("task1", 2), ("task2", 2), ("task3", 5)],
                vec!["task3"],
                5,
            ),
        ];

        for (tasks, expected_tasks, expected_size) in test_cases {
            let mut cache = Cache::new(Arc::new(Config {
                storage: Storage {
                    cache_capacity: ByteSize::mib(5),
                    ..Default::default()
                },
                ..Default::default()
            }));
            for (task_id, content_length) in &tasks {
                cache
                    .put_task(task_id, ByteSize::mib(*content_length).as_u64())
                    .await;
            }

            for (task_id, _) in &tasks {
                assert_eq!(
                    cache.contains_task(task_id).await,
                    expected_tasks.contains(task_id)
                );
            }

            assert_eq!(
                cache.size.load(Ordering::Relaxed),
                ByteSize::mib(expected_size).as_u64()
            );
        }
    }

    #[tokio::test]
    async fn contains_task_finds_only_present_tasks() {
        let test_cases = vec![
            (vec![], "task1", false),
            (vec!["task1"], "task1", true),
            (vec!["task1", "task2"], "task2", true),
            (vec!["task1", "task2"], "task3", false),
        ];

        for (put_tasks, task_id, expected) in test_cases {
            let mut cache = Cache::new(Arc::new(Config {
                storage: Storage {
                    cache_capacity: ByteSize::mib(10),
                    ..Default::default()
                },
                ..Default::default()
            }));
            for put_task in &put_tasks {
                cache.put_task(put_task, ByteSize::mib(1).as_u64()).await;
            }

            assert_eq!(cache.contains_task(task_id).await, expected);
        }
    }

    #[tokio::test]
    async fn delete_task_removes_present_tasks_and_releases_size() {
        let test_cases: Vec<(&str, u64, fn(Result<()>))> = vec![
            ("task1", 2, |result| assert!(result.is_ok())),
            ("task2", 1, |result| assert!(result.is_ok())),
            ("task3", 0, |result| assert!(result.is_ok())),
            ("nonexistent", 0, |result| {
                assert!(matches!(result, Err(Error::TaskNotFound(_))));
            }),
            ("", 0, |result| {
                assert!(matches!(result, Err(Error::TaskNotFound(_))));
            }),
            ("large_task", 0, |result| {
                assert!(matches!(result, Err(Error::TaskNotFound(_))));
            }),
        ];

        let mut cache = Cache::new(Arc::new(Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        }));
        for task_id in ["task1", "task2", "task3"] {
            cache.put_task(task_id, ByteSize::mib(1).as_u64()).await;
        }

        for (task_id, expected_size, expect) in test_cases {
            expect(cache.delete_task(task_id).await);
            assert!(!cache.contains_task(task_id).await);
            assert_eq!(
                cache.size.load(Ordering::Relaxed),
                ByteSize::mib(expected_size).as_u64()
            );
        }
    }

    #[tokio::test]
    async fn contains_piece_requires_task_and_piece() {
        let test_cases = vec![
            ("non_existent", vec![], "piece1", false),
            ("non_existent", vec![], "", false),
            ("task1", vec![], "piece1", false),
            ("task1", vec!["piece1"], "piece1", true),
            ("task1", vec!["piece1"], "", false),
            ("task1", vec!["piece1"], "non_existent_piece", false),
            ("task1", vec!["piece#$%^&*"], "piece#$%^&*", true),
        ];

        for (task_id, written_pieces, piece_id, expected) in test_cases {
            let mut cache = Cache::new(Arc::new(Config {
                storage: Storage {
                    cache_capacity: ByteSize::mib(10),
                    ..Default::default()
                },
                ..Default::default()
            }));
            cache.put_task("task1", 1000).await;

            for written_piece in &written_pieces {
                cache
                    .write_piece("task1", written_piece, Bytes::from("test data"))
                    .await
                    .unwrap();
            }

            assert_eq!(cache.contains_piece(task_id, piece_id).await, expected);
        }
    }

    #[tokio::test]
    async fn write_piece_fails_without_task() {
        let cache = Cache::new(Arc::new(Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        }));
        let result = cache
            .write_piece("non_existent", "piece1", Bytes::from("test data"))
            .await;
        assert!(matches!(result, Err(Error::TaskNotFound(_))));
    }

    #[tokio::test]
    async fn write_piece_stores_content_once_per_piece() {
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

        let mut cache = Cache::new(Arc::new(Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        }));
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;

        for (piece_id, content) in test_cases {
            let piece = Piece {
                offset: 0,
                length: content.len() as u64,
                ..Default::default()
            };
            cache
                .write_piece("task1", piece_id, Bytes::copy_from_slice(&content))
                .await
                .unwrap();
            assert!(cache.contains_piece("task1", piece_id).await);

            let mut reader = cache
                .read_piece("task1", piece_id, piece.clone(), None)
                .await
                .unwrap();
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, content);

            cache
                .write_piece(
                    "task1",
                    piece_id,
                    Bytes::from(format!("updated content for {piece_id}")),
                )
                .await
                .unwrap();
            let mut reader = cache
                .read_piece("task1", piece_id, piece, None)
                .await
                .unwrap();
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, content);
        }
    }

    #[tokio::test]
    async fn read_piece_returns_content_within_range() {
        let large_piece_length = ByteSize::mib(50).as_u64();
        let large_piece_content: Vec<u8> =
            (0..large_piece_length).map(|i| (i % 256) as u8).collect();

        let test_cases = vec![
            ("piece1", 0, 11, None, b"hello world".to_vec()),
            (
                "piece1",
                0,
                11,
                Some(Range {
                    start: 0,
                    length: 5,
                }),
                b"hello".to_vec(),
            ),
            (
                "piece1",
                0,
                11,
                Some(Range {
                    start: 6,
                    length: 100,
                }),
                b"world".to_vec(),
            ),
            ("piece2", 11, 9, None, b"rust lang".to_vec()),
            (
                "piece2",
                11,
                9,
                Some(Range {
                    start: 11,
                    length: 4,
                }),
                b"rust".to_vec(),
            ),
            (
                "piece2",
                11,
                9,
                Some(Range {
                    start: 5,
                    length: 10,
                }),
                b"rust".to_vec(),
            ),
            ("piece3", 20, 9, None, b"unit test".to_vec()),
            (
                "piece3",
                20,
                9,
                Some(Range {
                    start: 20,
                    length: 4,
                }),
                b"unit".to_vec(),
            ),
            (
                "large_piece",
                0,
                large_piece_length,
                None,
                large_piece_content.clone(),
            ),
            (
                "large_piece",
                0,
                large_piece_length,
                Some(Range {
                    start: 0,
                    length: ByteSize::mib(1).as_u64(),
                }),
                large_piece_content[..ByteSize::mib(1).as_u64() as usize].to_vec(),
            ),
            (
                "large_piece",
                0,
                large_piece_length,
                Some(Range {
                    start: ByteSize::mib(49).as_u64(),
                    length: ByteSize::mib(1).as_u64(),
                }),
                large_piece_content[ByteSize::mib(49).as_u64() as usize..].to_vec(),
            ),
        ];

        let mut cache = Cache::new(Arc::new(Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(100),
                ..Default::default()
            },
            ..Default::default()
        }));
        cache.put_task("task1", large_piece_length).await;
        let pieces = vec![
            ("piece1", b"hello world".to_vec()),
            ("piece2", b"rust lang".to_vec()),
            ("piece3", b"unit test".to_vec()),
            ("large_piece", large_piece_content),
        ];
        for (piece_id, content) in pieces {
            cache
                .write_piece("task1", piece_id, Bytes::from(content))
                .await
                .unwrap();
        }

        for (piece_id, offset, length, range, expected) in test_cases {
            let piece = Piece {
                offset,
                length,
                ..Default::default()
            };
            let mut reader = cache
                .read_piece("task1", piece_id, piece, range)
                .await
                .unwrap();
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, expected);
        }
    }

    #[tokio::test]
    async fn read_piece_fails_on_missing_task_piece_or_invalid_range() {
        let test_cases: Vec<(&str, &str, u64, u64, Option<Range>, fn(Result<()>))> = vec![
            ("non_existent", "piece1", 0, 11, None, |result| {
                assert!(matches!(result, Err(Error::TaskNotFound(_))));
            }),
            ("task1", "non_existent", 0, 11, None, |result| {
                assert!(matches!(result, Err(Error::PieceNotFound(_))));
            }),
            ("task1", "piece1", 0, 12, None, |result| {
                assert!(matches!(result, Err(Error::InvalidParameter)));
            }),
            (
                "task1",
                "piece1",
                0,
                20,
                Some(Range {
                    start: 11,
                    length: 5,
                }),
                |result| {
                    assert!(matches!(result, Err(Error::InvalidParameter)));
                },
            ),
        ];

        let mut cache = Cache::new(Arc::new(Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        }));
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;
        cache
            .write_piece("task1", "piece1", Bytes::from("hello world"))
            .await
            .unwrap();

        for (task_id, piece_id, offset, length, range, expect) in test_cases {
            let piece = Piece {
                offset,
                length,
                ..Default::default()
            };
            expect(
                cache
                    .read_piece(task_id, piece_id, piece, range)
                    .await
                    .map(|_| ()),
            );
        }
    }

    #[tokio::test]
    async fn read_piece_marks_task_recently_used() {
        let mut cache = Cache::new(Arc::new(Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(5),
                ..Default::default()
            },
            ..Default::default()
        }));
        cache.put_task("task1", ByteSize::mib(2).as_u64()).await;
        cache
            .write_piece("task1", "piece1", Bytes::from("hello world"))
            .await
            .unwrap();
        cache.put_task("task2", ByteSize::mib(2).as_u64()).await;

        let mut reader = cache
            .read_piece(
                "task1",
                "piece1",
                Piece {
                    offset: 0,
                    length: 11,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        cache.put_task("task3", ByteSize::mib(2).as_u64()).await;

        assert!(cache.contains_task("task1").await);
        assert!(!cache.contains_task("task2").await);
        assert!(cache.contains_task("task3").await);
    }

    #[tokio::test]
    async fn concurrent_reads_of_one_piece_return_its_content() {
        let mut cache = Cache::new(Arc::new(Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        }));
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;

        let content = b"test data for concurrent read".to_vec();
        cache
            .write_piece("task1", "piece1", Bytes::from(content.clone()))
            .await
            .unwrap();

        let cache = Arc::new(cache);
        let mut join_set = tokio::task::JoinSet::new();
        for i in 0..50 {
            let cache = cache.clone();
            let content = content.clone();
            join_set.spawn(async move {
                let (read_range, expected) = if i % 2 == 0 {
                    (None, content.clone())
                } else {
                    (
                        Some(Range {
                            start: 0,
                            length: 5,
                        }),
                        content[..5].to_vec(),
                    )
                };

                let mut reader = cache
                    .read_piece(
                        "task1",
                        "piece1",
                        Piece {
                            offset: 0,
                            length: content.len() as u64,
                            ..Default::default()
                        },
                        read_range,
                    )
                    .await
                    .unwrap();
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).await.unwrap();
                assert_eq!(buffer, expected);
            });
        }

        while let Some(result) = join_set.join_next().await {
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn concurrent_writes_of_different_pieces_store_each() {
        let mut cache = Cache::new(Arc::new(Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        }));
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;

        let cache = Arc::new(cache);
        let mut join_set = tokio::task::JoinSet::new();
        for i in 0..50 {
            let cache = cache.clone();
            join_set.spawn(async move {
                let piece_id = format!("piece{i}");
                let content = format!("content for piece {i}").into_bytes();
                cache
                    .write_piece("task1", &piece_id, Bytes::from(content.clone()))
                    .await
                    .unwrap();

                let mut reader = cache
                    .read_piece(
                        "task1",
                        &piece_id,
                        Piece {
                            offset: 0,
                            length: content.len() as u64,
                            ..Default::default()
                        },
                        None,
                    )
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
    async fn concurrent_writes_of_one_piece_keep_first_content() {
        let mut cache = Cache::new(Arc::new(Config {
            storage: Storage {
                cache_capacity: ByteSize::mib(10),
                ..Default::default()
            },
            ..Default::default()
        }));
        cache.put_task("task1", ByteSize::mib(1).as_u64()).await;

        let original_content = b"original content".to_vec();
        cache
            .write_piece("task1", "piece1", Bytes::from(original_content.clone()))
            .await
            .unwrap();

        let cache = Arc::new(cache);
        let mut join_set = tokio::task::JoinSet::new();
        for i in 0..50 {
            let cache = cache.clone();
            join_set.spawn(async move {
                let new_content = format!("new content from writer {i}").into_bytes();
                cache
                    .write_piece("task1", "piece1", Bytes::from(new_content))
                    .await
                    .unwrap();
            });
        }

        while let Some(result) = join_set.join_next().await {
            assert!(result.is_ok());
        }

        let mut reader = cache
            .read_piece(
                "task1",
                "piece1",
                Piece {
                    offset: 0,
                    length: original_content.len() as u64,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, original_content);
    }
}
