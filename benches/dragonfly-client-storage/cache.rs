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

use criterion::{black_box, Criterion, BenchmarkId};
use dragonfly_client_config::dfdaemon::{Config, Storage};
use dragonfly_client_storage::{cache::Cache, metadata::Piece};
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use bytesize::ByteSize;

pub fn bench_single_task_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("single_task_operations");

    // 测试不同大小的数据
    let sizes = vec![
        ("1KB", ByteSize::kb(1)),
        ("1MB", ByteSize::mb(1)),
        ("10MB", ByteSize::mb(10)),
        ("50MB", ByteSize::mb(50)),
    ];

    // 缓存配置
    let config = Config {
        storage: Storage {
            cache_capacity: ByteSize::mb(200),
            ..Default::default()
        },
        ..Default::default()
    };

    for (size_name, size) in sizes {
        let data = vec![1u8; size.as_u64() as usize];
        
        // 测试写入
        group.bench_with_input(BenchmarkId::new("write", size_name), &data, |b, data| {
            b.iter_batched(
                // 每次迭代前创建新的缓存实例
                || Cache::new(Arc::new(config.clone())).unwrap(),
                |mut cache| {
                    rt.block_on(async {
                        cache.put_task("task1", black_box(data.len() as u64)).await;
                        let mut cursor = Cursor::new(data);
                        cache.write_piece("task1", "piece1", &mut cursor, data.len() as u64)
                            .await
                            .unwrap();
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // 测试读取
        group.bench_with_input(BenchmarkId::new("read", size_name), &data, |b, data| {
            b.iter_batched(
                // 每次迭代前创建并初始化新的缓存实例
                || {
                    let mut cache = Cache::new(Arc::new(config.clone())).unwrap();
                    rt.block_on(async {
                        cache.put_task("task1", data.len() as u64).await;
                        let mut cursor = Cursor::new(data);
                        cache.write_piece("task1", "piece1", &mut cursor, data.len() as u64)
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

                        let mut reader = cache.read_piece("task1", "piece1", piece, None)
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

pub fn bench_concurrent_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrent_operations");

    // 并发配置
    let concurrency_levels = vec![4, 8, 16, 32];
    let data_size = ByteSize::mb(1).as_u64() as usize;
    let task_count = 10;

    let config = Config {
        storage: Storage {
            cache_capacity: ByteSize::mb(200),
            ..Default::default()
        },
        ..Default::default()
    };

    for &concurrency in &concurrency_levels {
        // 测试并发写入
        group.bench_with_input(
            BenchmarkId::new("concurrent_write", concurrency),
            &concurrency,
            |b, &concurrency| {
                b.iter_batched(
                    // 每次迭代前创建新的缓存实例
                    || Cache::new(Arc::new(config.clone())).unwrap(),
                    |cache| {
                        rt.block_on(async {
                            let cache = Arc::new(Mutex::new(cache));
                            let semaphore = Arc::new(Semaphore::new(concurrency));
                            let mut join_set = JoinSet::new();
                            let data = vec![1u8; data_size];

                            for task_id in 0..task_count {
                                let cache = cache.clone();
                                let semaphore = semaphore.clone();
                                let data = data.clone();

                                join_set.spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    let task_id = format!("task{}", task_id);
                                    let mut cache = cache.lock().await;
                                    cache.put_task(&task_id, data_size as u64).await;
                                    
                                    let mut cursor = Cursor::new(data);
                                    cache.write_piece(&task_id, "piece1", &mut cursor, data_size as u64)
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
            },
        );

        // 测试并发读取
        group.bench_with_input(
            BenchmarkId::new("concurrent_read", concurrency),
            &concurrency,
            |b, &concurrency| {
                b.iter_batched(
                    // 每次迭代前创建并初始化新的缓存实例
                    || {
                        let mut cache = Cache::new(Arc::new(config.clone())).unwrap();
                        let data = vec![1u8; data_size];
                        rt.block_on(async {
                            for task_id in 0..task_count {
                                let task_id = format!("task{}", task_id);
                                cache.put_task(&task_id, data_size as u64).await;
                                let mut cursor = Cursor::new(data.clone());
                                cache.write_piece(&task_id, "piece1", &mut cursor, data_size as u64)
                                    .await
                                    .unwrap();
                            }
                        });
                        cache
                    },
                    |cache| {
                        rt.block_on(async {
                            let cache = Arc::new(Mutex::new(cache));
                            let semaphore = Arc::new(Semaphore::new(concurrency));
                            let mut join_set = JoinSet::new();

                            // 并发读取
                            for task_id in 0..task_count {
                                let cache = cache.clone();
                                let semaphore = semaphore.clone();

                                join_set.spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    let task_id = format!("task{}", task_id);
                                    let cache = cache.lock().await;
                                    
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

                                    let mut reader = cache.read_piece(&task_id, "piece1", piece, None)
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
            },
        );
    }

    group.finish();
} 