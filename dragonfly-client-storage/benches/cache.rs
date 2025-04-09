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
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use dragonfly_client_config::dfdaemon::{Config, Storage};
use dragonfly_client_storage::{cache::Cache, metadata::Piece};
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;

// Number of pieces to write/read in each benchmark.
const PIECE_COUNT: usize = 100;

fn create_config(capacity: ByteSize) -> Config {
    Config {
        storage: Storage {
            cache_capacity: capacity,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn create_piece(length: u64) -> Piece {
    Piece {
        number: 0,
        offset: 0,
        length,
        digest: String::new(),
        parent_id: None,
        uploading_count: 0,
        uploaded_count: 0,
        updated_at: chrono::Utc::now().naive_utc(),
        created_at: chrono::Utc::now().naive_utc(),
        finished_at: None,
    }
}

pub fn put_task(c: &mut Criterion) {
    let rt: Runtime = Runtime::new().unwrap();
    let mut group = c.benchmark_group("Put Task");

    group.bench_with_input(
        BenchmarkId::new("Put Task", "10MB"),
        &ByteSize::mb(10),
        |b, size| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        Cache::new(Arc::new(create_config(ByteSize::gb(2)))).unwrap()
                    })
                },
                |mut cache| {
                    rt.block_on(async {
                        cache.put_task("task", black_box(size.as_u64())).await;
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Put Task", "100MB"),
        &ByteSize::mb(100),
        |b, size| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        Cache::new(Arc::new(create_config(ByteSize::gb(2)))).unwrap()
                    })
                },
                |mut cache| {
                    rt.block_on(async {
                        cache.put_task("task", black_box(size.as_u64())).await;
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Put Task", "1GB"),
        &ByteSize::gb(1),
        |b, size| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        Cache::new(Arc::new(create_config(ByteSize::gb(2)))).unwrap()
                    })
                },
                |mut cache| {
                    rt.block_on(async {
                        cache.put_task("task", black_box(size.as_u64())).await;
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

pub fn delete_task(c: &mut Criterion) {
    let rt: Runtime = Runtime::new().unwrap();
    let mut group = c.benchmark_group("Delete Task");

    group.bench_with_input(
        BenchmarkId::new("Delete Task", "10MB"),
        &ByteSize::mb(10),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = rt.block_on(async {
                        Cache::new(Arc::new(create_config(ByteSize::gb(2)))).unwrap()
                    });
                    rt.block_on(async {
                        cache.put_task("task", black_box(size.as_u64())).await;
                    });
                    cache
                },
                |mut cache| {
                    rt.block_on(async {
                        cache.delete_task("task").await.unwrap();
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Delete Task", "100MB"),
        &ByteSize::mb(100),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = rt.block_on(async {
                        Cache::new(Arc::new(create_config(ByteSize::gb(2)))).unwrap()
                    });
                    rt.block_on(async {
                        cache.put_task("task", black_box(size.as_u64())).await;
                    });
                    cache
                },
                |mut cache| {
                    rt.block_on(async {
                        cache.delete_task("task").await.unwrap();
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Delete Task", "1GB"),
        &ByteSize::gb(1),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = rt.block_on(async {
                        Cache::new(Arc::new(create_config(ByteSize::gb(2)))).unwrap()
                    });
                    rt.block_on(async {
                        cache.put_task("task", black_box(size.as_u64())).await;
                    });
                    cache
                },
                |mut cache| {
                    rt.block_on(async {
                        cache.delete_task("task").await.unwrap();
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

pub fn write_piece(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("Write Piece");

    group.bench_with_input(
        BenchmarkId::new("Write Piece", "4MB"),
        &vec![1u8; ByteSize::mb(4).as_u64() as usize],
        |b, data| {
            b.iter_batched(
                || {
                    let mut cache = rt.block_on(async {
                        Cache::new(Arc::new(create_config(
                            ByteSize::mb(4) * PIECE_COUNT as u64 + 1u64,
                        )))
                        .unwrap()
                    });

                    rt.block_on(async {
                        cache
                            .put_task("task", (ByteSize::mb(4) * PIECE_COUNT as u64).as_u64())
                            .await;
                    });
                    cache
                },
                |cache| {
                    rt.block_on(async {
                        for i in 0..PIECE_COUNT {
                            let mut cursor = Cursor::new(data);
                            cache
                                .write_piece(
                                    "task",
                                    &format!("piece{}", i),
                                    &mut cursor,
                                    data.len() as u64,
                                )
                                .await
                                .unwrap();
                        }
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Write Piece", "10MB"),
        &vec![1u8; ByteSize::mb(10).as_u64() as usize],
        |b, data| {
            b.iter_batched(
                || {
                    let mut cache = rt.block_on(async {
                        Cache::new(Arc::new(create_config(
                            ByteSize::mb(10) * PIECE_COUNT as u64 + 1u64,
                        )))
                        .unwrap()
                    });

                    rt.block_on(async {
                        cache
                            .put_task("task", (ByteSize::mb(10) * PIECE_COUNT as u64).as_u64())
                            .await;
                    });
                    cache
                },
                |cache| {
                    rt.block_on(async {
                        for i in 0..PIECE_COUNT {
                            let mut cursor = Cursor::new(data);
                            cache
                                .write_piece(
                                    "task",
                                    &format!("piece{}", i),
                                    &mut cursor,
                                    data.len() as u64,
                                )
                                .await
                                .unwrap();
                        }
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Write Piece", "16MB"),
        &vec![1u8; ByteSize::mb(16).as_u64() as usize],
        |b, data| {
            b.iter_batched(
                || {
                    let mut cache = rt.block_on(async {
                        Cache::new(Arc::new(create_config(
                            ByteSize::mb(16) * PIECE_COUNT as u64 + 1u64,
                        )))
                        .unwrap()
                    });

                    rt.block_on(async {
                        cache
                            .put_task("task", (ByteSize::mb(16) * PIECE_COUNT as u64).as_u64())
                            .await;
                    });
                    cache
                },
                |cache| {
                    rt.block_on(async {
                        for i in 0..PIECE_COUNT {
                            let mut cursor = Cursor::new(data);
                            cache
                                .write_piece(
                                    "task",
                                    &format!("piece{}", i),
                                    &mut cursor,
                                    data.len() as u64,
                                )
                                .await
                                .unwrap();
                        }
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

pub fn read_piece(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("Read Piece");

    group.bench_with_input(
        BenchmarkId::new("Read Piece", "4MB"),
        &vec![1u8; ByteSize::mb(4).as_u64() as usize],
        |b, data| {
            b.iter_batched(
                || {
                    let mut cache = rt.block_on(async {
                        Cache::new(Arc::new(create_config(
                            ByteSize::mb(4) * PIECE_COUNT as u64 + 1u64,
                        )))
                        .unwrap()
                    });

                    rt.block_on(async {
                        cache
                            .put_task("task", (ByteSize::mb(4) * PIECE_COUNT as u64).as_u64())
                            .await;
                        for i in 0..PIECE_COUNT {
                            let mut cursor = Cursor::new(data);
                            cache
                                .write_piece(
                                    "task",
                                    &format!("piece{}", i),
                                    &mut cursor,
                                    data.len() as u64,
                                )
                                .await
                                .unwrap();
                        }
                    });
                    cache
                },
                |cache| {
                    rt.block_on(async {
                        for i in 0..PIECE_COUNT {
                            let mut reader = cache
                                .read_piece(
                                    "task",
                                    &format!("piece{}", i),
                                    create_piece(data.len() as u64),
                                    None,
                                )
                                .await
                                .unwrap();
                            let mut buffer = Vec::new();
                            reader.read_to_end(&mut buffer).await.unwrap();
                        }
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Read Piece", "10MB"),
        &vec![1u8; ByteSize::mb(10).as_u64() as usize],
        |b, data| {
            b.iter_batched(
                || {
                    let mut cache = rt.block_on(async {
                        Cache::new(Arc::new(create_config(
                            ByteSize::mb(10) * PIECE_COUNT as u64 + 1u64,
                        )))
                        .unwrap()
                    });

                    rt.block_on(async {
                        cache
                            .put_task("task", (ByteSize::mb(10) * PIECE_COUNT as u64).as_u64())
                            .await;
                        for i in 0..PIECE_COUNT {
                            let mut cursor = Cursor::new(data);
                            cache
                                .write_piece(
                                    "task",
                                    &format!("piece{}", i),
                                    &mut cursor,
                                    data.len() as u64,
                                )
                                .await
                                .unwrap();
                        }
                    });
                    cache
                },
                |cache| {
                    rt.block_on(async {
                        for i in 0..PIECE_COUNT {
                            let mut reader = cache
                                .read_piece(
                                    "task",
                                    &format!("piece{}", i),
                                    create_piece(data.len() as u64),
                                    None,
                                )
                                .await
                                .unwrap();
                            let mut buffer = Vec::new();
                            reader.read_to_end(&mut buffer).await.unwrap();
                        }
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Read Piece", "16MB"),
        &vec![1u8; ByteSize::mb(16).as_u64() as usize],
        |b, data| {
            b.iter_batched(
                || {
                    let mut cache = rt.block_on(async {
                        Cache::new(Arc::new(create_config(
                            ByteSize::mb(16) * PIECE_COUNT as u64 + 1u64,
                        )))
                        .unwrap()
                    });

                    rt.block_on(async {
                        cache
                            .put_task("task", (ByteSize::mb(16) * PIECE_COUNT as u64).as_u64())
                            .await;
                        for i in 0..PIECE_COUNT {
                            let mut cursor = Cursor::new(data);
                            cache
                                .write_piece(
                                    "task",
                                    &format!("piece{}", i),
                                    &mut cursor,
                                    data.len() as u64,
                                )
                                .await
                                .unwrap();
                        }
                    });
                    cache
                },
                |cache| {
                    rt.block_on(async {
                        for i in 0..PIECE_COUNT {
                            let mut reader = cache
                                .read_piece(
                                    "task",
                                    &format!("piece{}", i),
                                    create_piece(data.len() as u64),
                                    None,
                                )
                                .await
                                .unwrap();
                            let mut buffer = Vec::new();
                            reader.read_to_end(&mut buffer).await.unwrap();
                        }
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

criterion_group!(benches, put_task, delete_task, write_piece, read_piece,);

criterion_main!(benches);
