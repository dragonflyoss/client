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

use bytesize::ByteSize;
use criterion::{black_box, BenchmarkId, Criterion};
use dragonfly_client_config::dfdaemon::{Config, Storage};
use dragonfly_client_storage::{cache::Cache, metadata::Piece};
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

/// Benchmark single task operations with different data sizes.
pub fn bench_single_task_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("single_task_operations");

    // Define test data sizes.
    let sizes = vec![
        ("10MB", ByteSize::mb(10)),
        ("100MB", ByteSize::mb(100)),
        ("1GB", ByteSize::gb(1)),
    ];

    // Configure cache with 2GB capacity.
    let config = Config {
        storage: Storage {
            cache_capacity: ByteSize::gb(2),
            ..Default::default()
        },
        ..Default::default()
    };

    for (size_name, size) in sizes {
        let data = vec![1u8; size.as_u64() as usize];

        // Benchmark write operations.
        group.bench_with_input(BenchmarkId::new("write", size_name), &data, |b, data| {
            b.iter_batched(
                || Cache::new(Arc::new(config.clone())).unwrap(),
                |mut cache| {
                    rt.block_on(async {
                        cache.put_task("task1", black_box(data.len() as u64)).await;
                        let mut cursor = Cursor::new(data);
                        cache
                            .write_piece("task1", "piece1", &mut cursor, data.len() as u64)
                            .await
                            .unwrap();
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Benchmark read operations.
        group.bench_with_input(BenchmarkId::new("read", size_name), &data, |b, data| {
            b.iter_batched(
                || {
                    let mut cache = Cache::new(Arc::new(config.clone())).unwrap();
                    rt.block_on(async {
                        cache.put_task("task1", data.len() as u64).await;
                        let mut cursor = Cursor::new(data);
                        cache
                            .write_piece("task1", "piece1", &mut cursor, data.len() as u64)
                            .await
                            .unwrap();
                    });
                    cache
                },
                |cache| {
                    rt.block_on(async {
                        let piece = Piece {
                            number: 0,
                            offset: 0,
                            length: data.len() as u64,
                            digest: "".to_string(),
                            parent_id: None,
                            uploading_count: 0,
                            uploaded_count: 0,
                            updated_at: chrono::Utc::now().naive_utc(),
                            created_at: chrono::Utc::now().naive_utc(),
                            finished_at: None,
                        };

                        let mut reader = cache
                            .read_piece("task1", "piece1", piece, None)
                            .await
                            .unwrap();
                        let mut buffer = Vec::new();
                        reader.read_to_end(&mut buffer).await.unwrap();
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark concurrent operations with different concurrency levels.
pub fn bench_concurrent_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrent_operations");

    // Define test parameters.
    let data_size = ByteSize::mb(1).as_u64() as usize;
    let task_count = 10;
    let piece_count = 10;

    // Configure cache with 200MB capacity.
    let config = Config {
        storage: Storage {
            cache_capacity: ByteSize::mb(200),
            ..Default::default()
        },
        ..Default::default()
    };

    // Benchmark concurrent write operations for multiple tasks.
    group.bench_function("concurrent_task_write", |b| {
        b.iter_batched(
            || Cache::new(Arc::new(config.clone())).unwrap(),
            |cache| {
                rt.block_on(async {
                    let mut join_set = JoinSet::new();
                    let data = vec![1u8; data_size];

                    for task_id in 0..task_count {
                        let mut cache = cache.clone();
                        let data = data.clone();

                        join_set.spawn(async move {
                            let task_id = format!("task{}", task_id);
                            cache.put_task(&task_id, data_size as u64).await;
                            let mut cursor = Cursor::new(data);
                            cache
                                .write_piece(&task_id, "piece1", &mut cursor, data_size as u64)
                                .await
                                .unwrap();
                        });
                    }

                    while let Some(result) = join_set.join_next().await {
                        result.unwrap();
                    }
                });
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Benchmark concurrent write operations for multiple pieces in a single task.
    group.bench_function("concurrent_piece_write", |b| {
        b.iter_batched(
            || Cache::new(Arc::new(config.clone())).unwrap(),
            |mut cache| {
                rt.block_on(async {
                    let mut join_set = JoinSet::new();
                    let data = vec![1u8; data_size];
                    let task_id = "task1";

                    // Create task first.
                    cache
                        .put_task(task_id, (data_size * piece_count) as u64)
                        .await;

                    for piece_id in 0..piece_count {
                        let data = data.clone();
                        let cache = cache.clone();

                        join_set.spawn(async move {
                            let piece_id = format!("piece{}", piece_id);
                            let mut cursor = Cursor::new(data);
                            cache
                                .write_piece(task_id, &piece_id, &mut cursor, data_size as u64)
                                .await
                                .unwrap();
                        });
                    }

                    while let Some(result) = join_set.join_next().await {
                        result.unwrap();
                    }
                });
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Benchmark concurrent read operations for multiple tasks.
    group.bench_function("concurrent_task_read", |b| {
        b.iter_batched(
            || {
                let mut cache = Cache::new(Arc::new(config.clone())).unwrap();
                let data = vec![1u8; data_size];

                rt.block_on(async {
                    for task_id in 0..task_count {
                        let task_id = format!("task{}", task_id);
                        cache.put_task(&task_id, data_size as u64).await;
                        let mut cursor = Cursor::new(data.clone());
                        cache
                            .write_piece(&task_id, "piece1", &mut cursor, data_size as u64)
                            .await
                            .unwrap();
                    }
                });
                cache
            },
            |cache| {
                rt.block_on(async {
                    let mut join_set = JoinSet::new();

                    for task_id in 0..task_count {
                        let cache = cache.clone();

                        join_set.spawn(async move {
                            let task_id = format!("task{}", task_id);
                            let piece = Piece {
                                number: 0,
                                offset: 0,
                                length: data_size as u64,
                                digest: "".to_string(),
                                parent_id: None,
                                uploading_count: 0,
                                uploaded_count: 0,
                                updated_at: chrono::Utc::now().naive_utc(),
                                created_at: chrono::Utc::now().naive_utc(),
                                finished_at: None,
                            };

                            let mut reader = cache
                                .read_piece(&task_id, "piece1", piece, None)
                                .await
                                .unwrap();
                            let mut buffer = Vec::new();
                            reader.read_to_end(&mut buffer).await.unwrap();
                        });
                    }

                    while let Some(result) = join_set.join_next().await {
                        result.unwrap();
                    }
                });
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Benchmark concurrent read operations for multiple pieces in a single task.
    group.bench_function("concurrent_piece_read", |b| {
        b.iter_batched(
            || {
                let mut cache = Cache::new(Arc::new(config.clone())).unwrap();
                let data = vec![1u8; data_size];
                let task_id = "task1";

                rt.block_on(async {
                    // Create task and write all pieces first.
                    cache
                        .put_task(task_id, (data_size * piece_count) as u64)
                        .await;
                    for piece_id in 0..piece_count {
                        let piece_id = piece_id.to_string();
                        let mut cursor = Cursor::new(data.clone());
                        cache
                            .write_piece(task_id, &piece_id, &mut cursor, data_size as u64)
                            .await
                            .unwrap();
                    }
                });
                cache
            },
            |cache| {
                rt.block_on(async {
                    let mut join_set = JoinSet::new();
                    let task_id = "task1";

                    for piece_id in 0..piece_count {
                        let cache = cache.clone();

                        join_set.spawn(async move {
                            let piece_id = piece_id.to_string();
                            let piece = Piece {
                                number: piece_id.parse::<u32>().unwrap(),
                                offset: (piece_id.parse::<u32>().unwrap() * data_size as u32)
                                    as u64,
                                length: data_size as u64,
                                digest: "".to_string(),
                                parent_id: None,
                                uploading_count: 0,
                                uploaded_count: 0,
                                updated_at: chrono::Utc::now().naive_utc(),
                                created_at: chrono::Utc::now().naive_utc(),
                                finished_at: None,
                            };

                            let mut reader = cache
                                .read_piece(task_id, &piece_id, piece, None)
                                .await
                                .unwrap();
                            let mut buffer = Vec::new();
                            reader.read_to_end(&mut buffer).await.unwrap();
                        });
                    }

                    while let Some(result) = join_set.join_next().await {
                        result.unwrap();
                    }
                });
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}
