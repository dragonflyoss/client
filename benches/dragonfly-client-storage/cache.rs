use bytes::Bytes;
use criterion::Criterion;
use dragonfly_api::common::v2::Range;
use dragonfly_client_config::dfdaemon::{Config, Storage};
use dragonfly_client_storage::cache::Cache;
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;
use bytesize::ByteSize;

pub fn bench_cache_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // 创建一个基准测试组
    let mut group = c.benchmark_group("cache_operations");

    // 设置缓存配置
    let config = Config {
        storage: Storage {
            cache_capacity: ByteSize::mib(100),
            ..Default::default()
        },
        ..Default::default()
    };

    // 准备测试数据
    let small_data = vec![0u8; 1024]; // 1KB
    let medium_data = vec![1u8; 1024 * 1024]; // 1MB
    let large_data = vec![2u8; 10 * 1024 * 1024]; // 10MB

    // 测试写入小文件
    group.bench_function("write_small_piece", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut cache = Cache::new(Arc::new(config.clone())).unwrap();
                cache.put_task("task1", small_data.len() as u64).await;
                let mut cursor = Cursor::new(&small_data);
                cache
                    .write_piece("task1", "piece1", &mut cursor, small_data.len() as u64)
                    .await
                    .unwrap();
            });
        })
    });

    // 测试写入中等大小文件
    group.bench_function("write_medium_piece", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut cache = Cache::new(Arc::new(config.clone())).unwrap();
                cache.put_task("task1", medium_data.len() as u64).await;
                let mut cursor = Cursor::new(&medium_data);
                cache
                    .write_piece("task1", "piece1", &mut cursor, medium_data.len() as u64)
                    .await
                    .unwrap();
            });
        })
    });

    // 测试写入大文件
    group.bench_function("write_large_piece", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut cache = Cache::new(Arc::new(config.clone())).unwrap();
                cache.put_task("task1", large_data.len() as u64).await;
                let mut cursor = Cursor::new(&large_data);
                cache
                    .write_piece("task1", "piece1", &mut cursor, large_data.len() as u64)
                    .await
                    .unwrap();
            });
        })
    });

    // 测试读取操作
    group.bench_function("read_operations", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut cache = Cache::new(Arc::new(config.clone())).unwrap();
                cache.put_task("task1", medium_data.len() as u64).await;
                
                // 写入数据
                let mut cursor = Cursor::new(&medium_data);
                cache
                    .write_piece("task1", "piece1", &mut cursor, medium_data.len() as u64)
                    .await
                    .unwrap();

                // 完整读取
                let piece = dragonfly_client_storage::metadata::Piece {
                    number: 0,
                    offset: 0,
                    length: medium_data.len() as u64,
                    digest: "".to_string(),
                    parent_id: None,
                    uploading_count: 0,
                    uploaded_count: 0,
                    updated_at: chrono::Utc::now().naive_utc(),
                    created_at: chrono::Utc::now().naive_utc(),
                    finished_at: None,
                };

                let mut reader = cache
                    .read_piece("task1", "piece1", piece.clone(), None)
                    .await
                    .unwrap();

                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).await.unwrap();
                assert_eq!(buffer.len(), medium_data.len());
            });
        })
    });

    // 测试并发读取
    group.bench_function("concurrent_read", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut cache = Cache::new(Arc::new(config.clone())).unwrap();
                cache.put_task("task1", medium_data.len() as u64).await;
                
                // 写入数据
                let mut cursor = Cursor::new(&medium_data);
                cache
                    .write_piece("task1", "piece1", &mut cursor, medium_data.len() as u64)
                    .await
                    .unwrap();

                let cache_arc = Arc::new(cache);
                let mut handles = Vec::new();

                // 创建10个并发读取任务
                for _ in 0..10 {
                    let cache_clone = cache_arc.clone();
                    let piece = dragonfly_client_storage::metadata::Piece {
                        number: 0,
                        offset: 0,
                        length: medium_data.len() as u64,
                        digest: "".to_string(),
                        parent_id: None,
                        uploading_count: 0,
                        uploaded_count: 0,
                        updated_at: chrono::Utc::now().naive_utc(),
                        created_at: chrono::Utc::now().naive_utc(),
                        finished_at: None,
                    };

                    handles.push(tokio::spawn(async move {
                        let mut reader = cache_clone
                            .read_piece("task1", "piece1", piece, None)
                            .await
                            .unwrap();

                        let mut buffer = Vec::new();
                        reader.read_to_end(&mut buffer).await.unwrap();
                        assert_eq!(buffer.len(), medium_data.len());
                    }));
                }

                // 等待所有读取任务完成
                for handle in handles {
                    handle.await.unwrap();
                }
            });
        })
    });

    // 测试缓存驱逐
    group.bench_function("cache_eviction", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut cache = Cache::new(Arc::new(config.clone())).unwrap();
                
                // 添加多个任务直到触发驱逐
                for i in 0..5 {
                    cache.put_task(&format!("task{}", i), medium_data.len() as u64).await;
                    let mut cursor = Cursor::new(&medium_data);
                    cache
                        .write_piece(&format!("task{}", i), "piece1", &mut cursor, medium_data.len() as u64)
                        .await
                        .unwrap();
                }
            });
        })
    });

    group.finish();
} 