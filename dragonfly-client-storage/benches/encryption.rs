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

//! Benchmarks of the encryption at rest: the raw AES-256-GCM chunk kernel, and
//! the encrypted piece write and read paths against their plaintext
//! counterparts on a tmpfs file, so the cost of the pipeline around the cipher
//! is visible separately from the cost of the cipher itself.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dragonfly_client_storage::encryption::kms::FileKeyProvider;
use dragonfly_client_storage::encryption::{
    write_encrypted_range, DataKey, EncryptedRangeReader, Encryptor, Layout, TaskCipher,
    DEFAULT_CHUNK_SIZE,
};
use dragonfly_client_storage::io::{write_range, RangeReader};
use dragonfly_client_util::buffer_pool::BufferPool;
use dragonfly_client_util::fs::fd::WriteFD;
use std::fs::File;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncReadExt;

const PIECE_LENGTH: usize = 4 * 1024 * 1024;
const BUFFER_SIZE: usize = 2 * 1024 * 1024;

fn data_key(byte: u8) -> DataKey {
    DataKey::new([byte; 32])
}

fn pattern(length: usize) -> Vec<u8> {
    (0..length).map(|i| (i % 251) as u8).collect()
}

fn bench_dir() -> PathBuf {
    let base = if std::path::Path::new("/dev/shm").is_dir() {
        PathBuf::from("/dev/shm")
    } else {
        std::env::temp_dir()
    };
    let dir = base.join(format!("dfly-enc-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Writes the benchmark piece into the encrypted file once, so the read
/// benchmarks never depend on the write benchmarks having run.
fn populate(
    rt: &tokio::runtime::Runtime,
    fd: &Arc<File>,
    cipher: &Arc<TaskCipher>,
    pool: &BufferPool,
    content: &[u8],
) {
    rt.block_on(async {
        let mut reader = Cursor::new(content);
        write_encrypted_range(
            &WriteFD::new(fd.clone()),
            cipher.clone(),
            0,
            PIECE_LENGTH as u64,
            BUFFER_SIZE,
            &mut reader,
            pool,
        )
        .await
        .unwrap();
    });
}

/// Simulates the transport work of a consumer sending `bytes` at `gib_per_sec`
/// by spinning, like a TLS sender encrypting and writing to the socket would.
fn pace(bytes: usize, gib_per_sec: f64) {
    let nanos = (bytes as f64 / (gib_per_sec * 1_073_741_824.0) * 1e9) as u128;
    let start = std::time::Instant::now();
    while start.elapsed().as_nanos() < nanos {
        std::hint::spin_loop();
    }
}

/// The raw cipher: seal and open one chunk in place, for several chunk sizes.
fn bench_chunk_kernel(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_kernel");
    for &chunk_size in &[16 * 1024u64, 64 * 1024, 256 * 1024, 1024 * 1024] {
        let layout = Layout::new(chunk_size, PIECE_LENGTH as u64, PIECE_LENGTH as u64).unwrap();
        let cipher = TaskCipher::new(&data_key(1), layout).unwrap();
        let plaintext = pattern(chunk_size as usize);
        group.throughput(Throughput::Bytes(chunk_size));

        group.bench_with_input(BenchmarkId::new("seal", chunk_size), &chunk_size, |b, _| {
            let mut buffer = plaintext.clone();
            b.iter(|| {
                buffer.copy_from_slice(&plaintext);
                cipher.seal_chunk(0, &mut buffer).unwrap()
            })
        });

        let mut sealed = plaintext.clone();
        let (nonce, tag) = cipher.seal_chunk(0, &mut sealed).unwrap();
        sealed.extend_from_slice(tag.as_ref());
        let nonce: [u8; 12] = nonce.as_ref()[..12].try_into().unwrap();
        group.bench_with_input(BenchmarkId::new("open", chunk_size), &chunk_size, |b, _| {
            let mut buffer = sealed.clone();
            b.iter(|| {
                buffer.copy_from_slice(&sealed);
                cipher.open_chunk(0, &nonce, &mut buffer).unwrap().len()
            })
        });
    }
    group.finish();
}

/// The cost of the random nonce: TaskCipher (RandomizedNonceKey) against a
/// LessSafeKey sealing the same 64 KiB chunk with a caller-provided nonce.
fn bench_nonce_cost(c: &mut Criterion) {
    use aws_lc_rs::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
    let chunk_size = 64 * 1024u64;
    let plaintext = pattern(chunk_size as usize);
    let mut group = c.benchmark_group("nonce_cost");
    group.throughput(Throughput::Bytes(chunk_size));

    let key = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &[1u8; 32]).unwrap());
    group.bench_function("less_safe_key_counter_nonce", |b| {
        let mut buffer = plaintext.clone();
        let mut counter: u64 = 0;
        b.iter(|| {
            buffer.copy_from_slice(&plaintext);
            counter += 1;
            let mut nonce = [0u8; 12];
            nonce[4..].copy_from_slice(&counter.to_be_bytes());
            key.seal_in_place_separate_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(0u64.to_be_bytes()),
                &mut buffer,
            )
            .unwrap()
        })
    });

    let layout = Layout::new(chunk_size, PIECE_LENGTH as u64, PIECE_LENGTH as u64).unwrap();
    let cipher = TaskCipher::new(&data_key(1), layout).unwrap();
    group.bench_function("randomized_nonce_key", |b| {
        let mut buffer = plaintext.clone();
        b.iter(|| {
            buffer.copy_from_slice(&plaintext);
            cipher.seal_chunk(0, &mut buffer).unwrap()
        })
    });
    group.finish();
}

/// Writing a 4 MiB piece: plaintext write_range against write_encrypted_range.
fn bench_write_piece(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dir = bench_dir();
    let pool = BufferPool::new(256 * 1024 * 1024);
    let content = Arc::new(pattern(PIECE_LENGTH));

    let mut group = c.benchmark_group("write_piece");
    group.throughput(Throughput::Bytes(PIECE_LENGTH as u64));
    group.sample_size(30);

    // Plaintext.
    let plain_path = dir.join("plain");
    {
        let f = File::create(&plain_path).unwrap();
        f.set_len(PIECE_LENGTH as u64).unwrap();
    }
    let plain_fd = Arc::new(
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&plain_path)
            .unwrap(),
    );
    let plain_fd = WriteFD::new(plain_fd);
    group.bench_function("plaintext", |b| {
        b.iter(|| {
            let fd = plain_fd.clone();
            let content = content.clone();
            let pool = pool.clone();
            rt.block_on(async move {
                let mut reader = Cursor::new(content.as_slice());
                write_range(&fd, 0, PIECE_LENGTH as u64, BUFFER_SIZE, &mut reader, &pool)
                    .await
                    .unwrap()
            })
        })
    });

    // Encrypted, with the default chunk size.
    let encryptor = Encryptor::new(Arc::new(
        FileKeyProvider::from_keys(&[data_key(1)]).unwrap(),
    ));
    let enc_path = dir.join("encrypted");
    rt.block_on(encryptor.create_file(&enc_path, PIECE_LENGTH as u64, PIECE_LENGTH as u64))
        .unwrap();
    let cipher = rt.block_on(encryptor.cipher(&enc_path)).unwrap();
    let enc_fd = Arc::new(
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&enc_path)
            .unwrap(),
    );
    populate(&rt, &enc_fd, &cipher, &pool, &content);
    let enc_fd = WriteFD::new(enc_fd);
    group.bench_function(BenchmarkId::new("encrypted", DEFAULT_CHUNK_SIZE), |b| {
        b.iter(|| {
            let fd = enc_fd.clone();
            let cipher = cipher.clone();
            let content = content.clone();
            let pool = pool.clone();
            rt.block_on(async move {
                let mut reader = Cursor::new(content.as_slice());
                write_encrypted_range(
                    &fd,
                    cipher,
                    0,
                    PIECE_LENGTH as u64,
                    BUFFER_SIZE,
                    &mut reader,
                    &pool,
                )
                .await
                .unwrap()
            })
        })
    });
    for &chunk_size in &[256 * 1024u64, 1024 * 1024] {
        let layout = Layout::new(chunk_size, PIECE_LENGTH as u64, PIECE_LENGTH as u64).unwrap();
        let cipher = Arc::new(TaskCipher::new(&data_key(1), layout).unwrap());
        let path = dir.join(format!("encrypted-{chunk_size}"));
        {
            let f = File::create(&path).unwrap();
            f.set_len(layout.file_length()).unwrap();
        }
        let fd = Arc::new(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );
        populate(&rt, &fd, &cipher, &pool, &content);
        let fd = WriteFD::new(fd);
        group.bench_function(BenchmarkId::new("encrypted", chunk_size), |b| {
            b.iter(|| {
                let fd = fd.clone();
                let cipher = cipher.clone();
                let content = content.clone();
                let pool = pool.clone();
                rt.block_on(async move {
                    let mut reader = Cursor::new(content.as_slice());
                    write_encrypted_range(
                        &fd,
                        cipher,
                        0,
                        PIECE_LENGTH as u64,
                        BUFFER_SIZE,
                        &mut reader,
                        &pool,
                    )
                    .await
                    .unwrap()
                })
            })
        });
    }
    group.finish();

    // Reading the piece back: plaintext RangeReader against EncryptedRangeReader.
    let mut group = c.benchmark_group("read_piece");
    group.throughput(Throughput::Bytes(PIECE_LENGTH as u64));
    group.sample_size(30);
    let plain_fd = WriteFD::new(plain_fd);
    group.bench_function("plaintext", |b| {
        b.iter(|| {
            let fd = plain_fd.clone();
            let pool = pool.clone();
            rt.block_on(async move {
                let mut reader = RangeReader::new(fd, 0, PIECE_LENGTH as u64, BUFFER_SIZE, pool);
                let mut sink = Vec::with_capacity(PIECE_LENGTH);
                reader.read_to_end(&mut sink).await.unwrap()
            })
        })
    });
    group.bench_function(BenchmarkId::new("encrypted", DEFAULT_CHUNK_SIZE), |b| {
        b.iter(|| {
            let fd = enc_fd.clone();
            let cipher = cipher.clone();
            let pool = pool.clone();
            rt.block_on(async move {
                let mut reader = EncryptedRangeReader::new(
                    fd,
                    cipher,
                    0,
                    PIECE_LENGTH as u64,
                    0,
                    PIECE_LENGTH as u64,
                    BUFFER_SIZE,
                    pool,
                )
                .unwrap();
                let mut sink = Vec::with_capacity(PIECE_LENGTH);
                reader.read_to_end(&mut sink).await.unwrap()
            })
        })
    });
    for &chunk_size in &[256 * 1024u64, 1024 * 1024] {
        let layout = Layout::new(chunk_size, PIECE_LENGTH as u64, PIECE_LENGTH as u64).unwrap();
        let cipher = Arc::new(TaskCipher::new(&data_key(1), layout).unwrap());
        let path = dir.join(format!("encrypted-{chunk_size}"));
        let fd = Arc::new(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );
        group.bench_function(BenchmarkId::new("encrypted", chunk_size), |b| {
            b.iter(|| {
                let fd = fd.clone();
                let cipher = cipher.clone();
                let pool = pool.clone();
                rt.block_on(async move {
                    let mut reader = EncryptedRangeReader::new(
                        fd,
                        cipher,
                        0,
                        PIECE_LENGTH as u64,
                        0,
                        PIECE_LENGTH as u64,
                        BUFFER_SIZE,
                        pool,
                    )
                    .unwrap();
                    let mut sink = Vec::with_capacity(PIECE_LENGTH);
                    reader.read_to_end(&mut sink).await.unwrap()
                })
            })
        });
    }
    group.finish();

    // Reading the piece back to a consumer that spends time per chunk, like a
    // network sender: the overlap of the decryption with the consumer matters
    // here, not the speed of a memcpy consumer.
    for &rate in &[3.0f64, 1.0] {
        let mut group = c.benchmark_group(format!("read_piece_paced_{rate}gib"));
        group.throughput(Throughput::Bytes(PIECE_LENGTH as u64));
        group.sample_size(20);
        let plain_fd = WriteFD::new(plain_fd);
        group.bench_function("plaintext", |b| {
            b.iter(|| {
                let fd = plain_fd.clone();
                let pool = pool.clone();
                rt.block_on(async move {
                    let mut reader =
                        RangeReader::new(fd, 0, PIECE_LENGTH as u64, BUFFER_SIZE, pool);
                    let mut total = 0;
                    loop {
                        let chunk = reader.read_chunk().await.unwrap();
                        if chunk.is_empty() {
                            break;
                        }
                        pace(chunk.len(), rate);
                        total += chunk.len();
                    }
                    total
                })
            })
        });
        group.bench_function(BenchmarkId::new("encrypted", DEFAULT_CHUNK_SIZE), |b| {
            b.iter(|| {
                let fd = enc_fd.clone();
                let cipher = cipher.clone();
                let pool = pool.clone();
                rt.block_on(async move {
                    let mut reader = EncryptedRangeReader::new(
                        fd,
                        cipher,
                        0,
                        PIECE_LENGTH as u64,
                        0,
                        PIECE_LENGTH as u64,
                        BUFFER_SIZE,
                        pool,
                    )
                    .unwrap();
                    let mut total = 0;
                    loop {
                        let chunk = reader.read_chunk().await.unwrap();
                        if chunk.is_empty() {
                            break;
                        }
                        pace(chunk.len(), rate);
                        total += chunk.len();
                    }
                    total
                })
            })
        });
        group.finish();
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// The lookup of the cipher of a task, done once per piece read and write: the
/// bounded sharded LRU of the encryptor against a plain concurrent map, from one
/// thread and from four, 1000 lookups per iteration.
fn bench_cipher_lookup(c: &mut Criterion) {
    use dashmap::DashMap;
    use std::hint::black_box;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let dir = bench_dir();
    let tasks = dir.join("tasks").join("abc");
    std::fs::create_dir_all(&tasks).unwrap();

    let encryptor = Arc::new(Encryptor::new(Arc::new(
        FileKeyProvider::from_keys(&[data_key(1)]).unwrap(),
    )));
    let paths: Arc<Vec<PathBuf>> =
        Arc::new((0..256).map(|i| tasks.join(format!("id{i:04}"))).collect());
    let reference: Arc<DashMap<PathBuf, Arc<TaskCipher>>> = Arc::new(DashMap::new());
    for path in paths.iter() {
        rt.block_on(encryptor.create_file(path, PIECE_LENGTH as u64, PIECE_LENGTH as u64))
            .unwrap();
        reference.insert(path.clone(), rt.block_on(encryptor.cipher(path)).unwrap());
    }

    let mut group = c.benchmark_group("cipher_lookup_x1000");
    group.bench_function("lru_sharded/1thread", |b| {
        b.iter(|| {
            let encryptor = encryptor.clone();
            let paths = paths.clone();
            rt.block_on(async move {
                for i in 0..1000 {
                    black_box(encryptor.cipher(&paths[i % 256]).await.unwrap());
                }
            })
        })
    });
    group.bench_function("dashmap/1thread", |b| {
        b.iter(|| {
            for i in 0..1000 {
                black_box(reference.get(&paths[i % 256]).unwrap().clone());
            }
        })
    });
    group.bench_function("lru_sharded/4threads", |b| {
        b.iter(|| {
            let encryptor = encryptor.clone();
            let paths = paths.clone();
            rt.block_on(async move {
                let mut handles = Vec::new();
                for t in 0..4 {
                    let encryptor = encryptor.clone();
                    let paths = paths.clone();
                    handles.push(tokio::spawn(async move {
                        for i in 0..1000 {
                            black_box(encryptor.cipher(&paths[(i * 7 + t) % 256]).await.unwrap());
                        }
                    }));
                }
                for handle in handles {
                    handle.await.unwrap();
                }
            })
        })
    });
    group.bench_function("dashmap/4threads", |b| {
        b.iter(|| {
            let reference = reference.clone();
            let paths = paths.clone();
            rt.block_on(async move {
                let mut handles = Vec::new();
                for t in 0..4 {
                    let reference = reference.clone();
                    let paths = paths.clone();
                    handles.push(tokio::spawn(async move {
                        for i in 0..1000 {
                            black_box(reference.get(&paths[(i * 7 + t) % 256]).unwrap().clone());
                        }
                    }));
                }
                for handle in handles {
                    handle.await.unwrap();
                }
            })
        })
    });
    group.finish();
    let _ = std::fs::remove_dir_all(&dir);
}

criterion_group!(
    benches,
    bench_chunk_kernel,
    bench_nonce_cost,
    bench_cipher_lookup,
    bench_write_piece
);
criterion_main!(benches);
