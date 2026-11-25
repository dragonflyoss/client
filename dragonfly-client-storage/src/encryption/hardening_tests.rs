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

//! Tests of the hardening of the encryption at rest: known-answer vectors of an
//! independent AES-256-GCM implementation, randomized geometries of the write
//! and read paths with trickled reads, concurrent creation and writes, the
//! bounds of the header read from the disk, and the cache of the data keys.

use super::*;
use crate::encryption::kms::FileKeyProvider;
use crate::encryption::layout::{MAX_CHUNK_SIZE, NONCE_LEN};
use std::fs::File;
use std::io::Cursor;
use std::pin::Pin;
use std::task::{Context, Poll};
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, ReadBuf};

fn data_key(byte: u8) -> DataKey {
    DataKey::new([byte; kms::KEY_LEN])
}

fn encryptor() -> Encryptor {
    Encryptor::new(Arc::new(
        FileKeyProvider::from_keys(&[data_key(1)]).unwrap(),
    ))
}

fn buffer_pool() -> BufferPool {
    BufferPool::new(64 * 1024 * 1024)
}

fn pattern(length: usize) -> Vec<u8> {
    (0..length).map(|i| (i % 251) as u8).collect()
}

/// A small deterministic generator (xorshift64*), so the randomized tests are
/// reproducible without a dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// A value in `low..=high`.
    fn range(&mut self, low: u64, high: u64) -> u64 {
        low + self.next() % (high - low + 1)
    }

    fn bool(&mut self) -> bool {
        self.next() & 1 == 1
    }

    fn fill(&mut self, bytes: &mut [u8]) {
        for byte in bytes {
            *byte = self.next() as u8;
        }
    }
}

/// An AsyncRead returning its data in random short reads, like a network body.
struct Trickle {
    data: Vec<u8>,
    pos: usize,
    rng: Rng,
    max: u64,
}

impl AsyncRead for Trickle {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let me = &mut *self;
        let remaining = me.data.len() - me.pos;
        let n = (me.rng.range(1, me.max) as usize)
            .min(remaining)
            .min(buf.remaining());
        buf.put_slice(&me.data[me.pos..me.pos + n]);
        me.pos += n;
        Poll::Ready(Ok(()))
    }
}

fn open_rw(path: &Path) -> Arc<File> {
    Arc::new(
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap(),
    )
}

/// The key, and then the (nonce, plaintext offset, plaintext, ciphertext || tag)
/// vectors generated with the Python `cryptography` package, an AES-256-GCM
/// implementation independent of this code base, with the big-endian plaintext
/// offset as the associated data, which is the format of a chunk.
const KAT_KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
const KAT_VECTORS: &[(&str, u64, &str, &str)] = &[
    ("a0a1a2a3a4a5a6a7a8a9aaab", 0, "",
     "e6c2eecd0b0d3459b022513f45d62d84"),
    ("b0b1b2b3b4b5b6b7b8b9babb", 0, "030a11181f262d343b424950575e656c73",
     "9a5f4bb3f3eb966b7cbadef29a03edaef7ea1679e662bfe45db3087fd8119ad02d"),
    ("c0c1c2c3c4c5c6c7c8c9cacb", 65536, "010e1b2835424f5c697683909daab7c4d1deebf805121f2c394653606d7a8794a1aebbc8d5e2effc091623303d4a5764717e8b98a5b2bfccd9e6f3000d1a2734414e5b6875828f9ca9b6c3d0ddeaf704111e2b3845525f6c798693a0adbac7d4e1eefb0815222f3c495663707d8a97a4b1becbd8e5f2ff0c192633404d5a6774818e9ba8b5c2cfdce9f603101d2a3744515e6b7885929facb9c6d3e0edfa0714212e3b4855626f7c8996a3b0bdcad7e4f1fe0b1825323f4c596673808d9aa7b4c1cedbe8f5020f1c293643505d6a7784919eabb8c5d2dfecf90613202d3a4754616e7b8895a2afbcc9d6e3f0fd0a1724313e4b5865727f8c99a6b3c0cddae7f4010e1b2835424f5c697683909daab7c4d1deebf805121f2c394653606d7a8794a1aebbc8d5e2effc09162330",
     "01463c4f423ad902f1af7e32af065f929dad08c739f000961385186a5a07f8221d382c6fa739aaffc15b7729c48e32edfc1b54e2777f1fd55dc3ddd0effd1c294c62d0559c44f330cb23dbcc29bde88254588197835f7ae1daa827b31bf6d416216f8baf532ee494485df17f2558816d6be94eaf92134cb30a0d633ce7618062bafa90ebec41d51f998436777135cc2efe2741cf56d514c4f4618dda88f0fda54d70d07834da8f1c29d18dafd6656a4def3b067cbbf86f94ebec3b20cbc5c1566d35ac4eaaaea2b56c43e701d4113b404b2b7e20ae988b9d7bcb50d0950b4bd1415a4f62c2229b7eb529bea12b328b2a98cbcb0556bf87216472dae59ea3bc7b5dfa0fc9b39efac912a9fcf5ce4f9bb0b41939a0fc0e47c682e1e251f8dfda9f1231df3ca76e4217000abf5d1cc8afe1e0086ec8ffa319a2e9661dfc"),
    ("d0d1d2d3d4d5d6d7d8d9dadb", 1099511627781, "001d3a577491aecbe805223f5c7996b3d0ed0a2744617e9bb8d5f20f2c496683a0bddaf714314e6b88a5c2dffc193653708daac7e4011e3b587592afcce90623",
     "2cbadc391c365f6ab05a0a80aaaf11521fe757f204fcc6dd8b5fbc99237a48447caabbfbb62f7102a901ee24851fb25a7f91d8c770fea76574d810e03236dffb78f91cfc3df6d3884b8b7b5df2df6f0d"),
];

#[test]
fn test_known_answer_vectors_of_an_independent_implementation() {
    let key = DataKey::new(hex::decode(KAT_KEY).unwrap().try_into().unwrap());
    let layout = Layout::new(DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE).unwrap();
    let cipher = TaskCipher::new(&key, layout).unwrap();

    for (nonce, offset, plaintext, sealed) in KAT_VECTORS {
        let nonce: [u8; NONCE_LEN as usize] = hex::decode(nonce).unwrap().try_into().unwrap();
        let plaintext = hex::decode(plaintext).unwrap();
        let sealed = hex::decode(sealed).unwrap();

        // A chunk sealed by the independent implementation opens to its plaintext.
        let mut in_out = sealed.clone();
        let opened = cipher.open_chunk(*offset, &nonce, &mut in_out).unwrap();
        assert_eq!(opened, plaintext.as_slice(), "offset {offset}");

        // It does not open under another offset, i.e. another associated data.
        let mut moved = sealed.clone();
        assert!(cipher.open_chunk(offset + 1, &nonce, &mut moved).is_err());

        // It does not open after a flipped bit of the ciphertext or of the tag.
        for position in [0, sealed.len() - 1] {
            let mut flipped = sealed.clone();
            flipped[position] ^= 0x80;
            assert!(cipher.open_chunk(*offset, &nonce, &mut flipped).is_err());
        }

        // It does not open under another nonce.
        let mut other_nonce = nonce;
        other_nonce[0] ^= 1;
        let mut in_out = sealed.clone();
        assert!(cipher
            .open_chunk(*offset, &other_nonce, &mut in_out)
            .is_err());
    }
}

#[test]
fn test_self_test() {
    let rate = self_test().unwrap();
    assert!(rate > 0.0 && rate.is_finite());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_create_file_agree_on_one_data_key() {
    let dir = tempdir().unwrap();
    let encryptor = Arc::new(encryptor());
    let tasks = dir.path().join("tasks").join("abc");
    fs::create_dir_all(&tasks).await.unwrap();

    let piece_length = 4096u64;
    let content = pattern(3 * piece_length as usize + 17);
    for round in 0..40 {
        let path = tasks.join(format!("abcdef{round}"));

        let mut handles = Vec::new();
        for _ in 0..16 {
            let encryptor = encryptor.clone();
            let path = path.clone();
            let length = content.len() as u64;
            handles.push(tokio::spawn(async move {
                encryptor.create_file(&path, piece_length, length).await
            }));
        }
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        // Whatever cipher is cached must be the one the header on disk
        // unwraps to: a piece written with it reads back after the cache is
        // dropped and the cipher is loaded again from the header.
        let cipher = encryptor.cipher(&path).await.unwrap();
        let fd = open_rw(&path);
        write_encrypted_range(
            &dragonfly_client_util::fs::fd::WriteFD::new(fd.clone()),
            cipher,
            0,
            piece_length,
            1024,
            &mut Cursor::new(content[..piece_length as usize].to_vec()),
            &buffer_pool(),
        )
        .await
        .unwrap();

        encryptor.evict(&path);
        let reloaded = encryptor.cipher(&path).await.unwrap();
        let mut reader = EncryptedRangeReader::new(
            fd,
            reloaded,
            0,
            piece_length,
            0,
            piece_length,
            1024,
            buffer_pool(),
        )
        .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, &content[..piece_length as usize], "round {round}");

        // No temporary file is left behind.
        let leftovers = std::fs::read_dir(&tasks)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
            .count();
        assert_eq!(leftovers, 0, "round {round}");
    }
}

#[tokio::test]
async fn test_create_file_is_idempotent() {
    let dir = tempdir().unwrap();
    let encryptor = encryptor();
    let path = dir.path().join("tasks").join("abc").join("abcdef");
    fs::create_dir_all(path.parent().unwrap()).await.unwrap();

    encryptor.create_file(&path, 1000, 2500).await.unwrap();
    let before = fs::read(&path).await.unwrap()[..HEADER_LEN as usize].to_vec();

    // A second creation keeps the file, and so its data key.
    encryptor.create_file(&path, 1000, 2500).await.unwrap();
    let after = fs::read(&path).await.unwrap()[..HEADER_LEN as usize].to_vec();
    assert_eq!(before, after);
    assert!(encryptor.is_readable(&path).await.unwrap());
}

#[tokio::test]
async fn test_create_file_replaces_an_unreadable_leftover() {
    let dir = tempdir().unwrap();
    let encryptor = encryptor();
    let tasks = dir.path().join("tasks").join("abc");
    fs::create_dir_all(&tasks).await.unwrap();

    // A plaintext leftover of a deleted task is replaced.
    let plaintext = tasks.join("abcplain");
    fs::write(&plaintext, b"stale plaintext").await.unwrap();
    encryptor.create_file(&plaintext, 1000, 2500).await.unwrap();
    assert!(Encryptor::is_encrypted_file(&plaintext).await.unwrap());
    assert!(encryptor.is_readable(&plaintext).await.unwrap());

    // A file of a key encryption key that is gone is replaced too.
    let other = Encryptor::new(Arc::new(
        FileKeyProvider::from_keys(&[data_key(2)]).unwrap(),
    ));
    let rotated = tasks.join("abcrotated");
    other.create_file(&rotated, 1000, 2500).await.unwrap();
    assert!(!encryptor.is_readable(&rotated).await.unwrap());
    encryptor.create_file(&rotated, 1000, 2500).await.unwrap();
    assert!(encryptor.is_readable(&rotated).await.unwrap());

    // The other encryptor still caches the cipher of the file it created, so
    // forget it, as the file was replaced behind its back by another instance.
    other.evict(&rotated);
    assert!(!other.is_readable(&rotated).await.unwrap());
}

#[tokio::test]
async fn test_cipher_cache_is_bounded() {
    let dir = tempdir().unwrap();
    let encryptor = Encryptor::with_cipher_cache_capacity(
        Arc::new(FileKeyProvider::from_keys(&[data_key(1)]).unwrap()),
        32,
    );
    let tasks = dir.path().join("tasks").join("abc");
    fs::create_dir_all(&tasks).await.unwrap();

    let paths: Vec<PathBuf> = (0..256).map(|i| tasks.join(format!("id{i:04}"))).collect();
    for path in &paths {
        encryptor.create_file(path, 1000, 2500).await.unwrap();
    }
    assert!(
        encryptor.cached_ciphers() <= 32,
        "{}",
        encryptor.cached_ciphers()
    );

    // Every file stays readable after its cipher was evicted, as the cipher is
    // unwrapped again from the header.
    for path in &paths {
        encryptor.cipher(path).await.unwrap();
    }
    assert!(
        encryptor.cached_ciphers() <= 32,
        "{}",
        encryptor.cached_ciphers()
    );

    // A forgotten cipher is gone from the cache.
    encryptor.evict(&paths[0]);
    assert!(!encryptor.ciphers.contains(&paths[0]));
}

/// Serializes a header the way `Header::encode` does, without validating it.
fn raw_header_bytes(header: &Header) -> Vec<u8> {
    let body = bincode::serialize(header).unwrap();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&body);
    bytes.resize(HEADER_LEN as usize, 0);
    bytes
}

fn valid_header() -> Header {
    Header {
        version: FORMAT_VERSION,
        chunk_size: DEFAULT_CHUNK_SIZE,
        piece_length: 100,
        content_length: 250,
        key_id: "k".repeat(64),
        wrapped_key: vec![1u8; 60],
    }
}

#[test]
fn test_header_bounds() {
    let valid = valid_header();
    assert!(valid.encode().is_ok());
    assert_eq!(
        Header::decode(&raw_header_bytes(&valid)).unwrap(),
        Some(valid.clone())
    );

    let mut long_id = valid.clone();
    long_id.key_id = "k".repeat(MAX_KEY_ID_LEN + 1);
    let mut control = valid.clone();
    control.key_id = "bad\nid".to_string();
    let mut escape = valid.clone();
    escape.key_id = "bad\u{1b}[31mid".to_string();
    let mut empty_key = valid.clone();
    empty_key.wrapped_key = Vec::new();
    let mut long_key = valid.clone();
    long_key.wrapped_key = vec![1u8; MAX_WRAPPED_KEY_LEN + 1];

    for (name, header) in [
        ("long key id", long_id),
        ("control character", control),
        ("escape sequence", escape),
        ("empty wrapped key", empty_key),
        ("long wrapped key", long_key),
    ] {
        assert!(header.encode().is_err(), "{name} must not encode");
        // Decoding rejects it as well, as the bytes come from the disk.
        let raw = raw_header_bytes(&header);
        assert!(Header::decode(&raw).is_err(), "{name} must not decode");
    }
}

#[test]
fn test_header_decode_never_panics() {
    let valid = valid_header().encode().unwrap();

    // Every single byte of a valid header flipped in several ways.
    for position in 0..valid.len() {
        for mask in [0x01u8, 0x80, 0xff] {
            let mut mutated = valid.clone();
            mutated[position] ^= mask;
            let _ = Header::decode(&mutated);
        }
    }

    // Random bytes behind the magic, of every length up to the header.
    let mut rng = Rng(0x00d7_d7d7);
    for length in 0..=HEADER_LEN as usize {
        let mut bytes = vec![0u8; length];
        rng.fill(&mut bytes);
        if length >= MAGIC.len() {
            bytes[..MAGIC.len()].copy_from_slice(MAGIC);
        }
        let _ = Header::decode(&bytes);
    }

    // A length prefix that points far past the header.
    let mut huge = valid.clone();
    huge[MAGIC.len()..HEADER_PREFIX_LEN].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(Header::decode(&huge).is_err());
}

#[test]
fn test_layout_bounds_of_a_header_are_checked_on_use() {
    // A header claiming an absurd content length yields no layout.
    let mut header = valid_header();
    header.content_length = u64::MAX;
    header.piece_length = 4 * 1024 * 1024;
    assert!(header.layout().is_err());

    let mut header = valid_header();
    header.chunk_size = MAX_CHUNK_SIZE + 1;
    assert!(header.layout().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_random_geometries_roundtrip() {
    let dir = tempdir().unwrap();
    let mut rng = Rng(0x0dfd_0001);

    for case in 0..250 {
        let chunk_size = [1u64, 3, 16, 64, 4096][rng.range(0, 4) as usize];
        let piece_length = rng.range(1, 5 * chunk_size + 3);
        let pieces = rng.range(1, 5);
        let content_length = (pieces - 1) * piece_length + rng.range(1, piece_length);
        let layout = Layout::new(chunk_size, piece_length, content_length).unwrap();

        let mut content = vec![0u8; content_length as usize];
        rng.fill(&mut content);

        let path = dir.path().join(format!("case-{case}"));
        {
            let f = File::create(&path).unwrap();
            f.set_len(layout.file_length()).unwrap();
        }
        let fd = open_rw(&path);
        let mut key = DataKey::new([0u8; kms::KEY_LEN]);
        rng.fill(key.as_mut());
        let cipher = Arc::new(TaskCipher::new(&key, layout).unwrap());
        let pool = buffer_pool();
        let max_buffer = 3 * chunk_size + 5;

        // Write the pieces in a random order, with random buffer sizes, from a
        // reader that returns random short reads and has bytes after the piece.
        let mut order: Vec<u64> = (0..layout.piece_count()).collect();
        for i in (1..order.len()).rev() {
            order.swap(i, rng.range(0, i as u64) as usize);
        }
        for number in order {
            let offset = number * piece_length;
            let length = piece_length.min(content_length - offset);
            let mut data = content[offset as usize..(offset + length) as usize].to_vec();
            data.extend_from_slice(b"bytes after the piece");
            let mut reader = Trickle {
                data,
                pos: 0,
                rng: Rng(rng.next() | 1),
                max: rng.range(1, 2 * chunk_size + 3),
            };
            let response = write_encrypted_range(
                &dragonfly_client_util::fs::fd::WriteFD::new(fd.clone()),
                cipher.clone(),
                offset,
                length,
                rng.range(1, max_buffer) as usize,
                &mut reader,
                &pool,
            )
            .await
            .unwrap();
            assert_eq!(response.length, length, "case {case}");
            assert_eq!(
                response.hash,
                crc32fast::hash(&content[offset as usize..(offset + length) as usize]).to_string(),
                "case {case}"
            );
        }

        // Read random ranges back, through the buffered and the chunk interface.
        for _ in 0..8 {
            let number = rng.range(0, layout.piece_count() - 1);
            let piece_offset = number * piece_length;
            let piece_len = piece_length.min(content_length - piece_offset);
            let start = rng.range(0, piece_len);
            let length = rng.range(0, piece_len - start);
            let mut reader = EncryptedRangeReader::new(
                fd.clone(),
                cipher.clone(),
                piece_offset,
                piece_len,
                piece_offset + start,
                length,
                rng.range(1, max_buffer) as usize,
                pool.clone(),
            )
            .unwrap();

            let mut buffer = Vec::new();
            if rng.bool() {
                reader.read_to_end(&mut buffer).await.unwrap();
            } else {
                loop {
                    let chunk = reader.read_chunk().await.unwrap();
                    if chunk.is_empty() {
                        break;
                    }
                    buffer.extend_from_slice(&chunk);
                }
            }
            let from = (piece_offset + start) as usize;
            assert_eq!(
                buffer,
                &content[from..from + length as usize],
                "case {case}: chunk {chunk_size} piece {piece_length} content {content_length} \
                 range {start}+{length}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_duplicate_concurrent_piece_writes_stay_readable() {
    let dir = tempdir().unwrap();
    let encryptor = Arc::new(encryptor());
    let path = dir.path().join("tasks").join("abc").join("abcdef");
    fs::create_dir_all(path.parent().unwrap()).await.unwrap();

    // Several writers write the same pieces at the same time, in batches of two
    // chunks that interleave, like duplicate downloads of one piece would.
    let chunk = DEFAULT_CHUNK_SIZE as usize;
    let piece_length = 16 * chunk as u64;
    let content = Arc::new(pattern(2 * piece_length as usize));
    encryptor
        .create_file(&path, piece_length, content.len() as u64)
        .await
        .unwrap();
    let cipher = encryptor.cipher(&path).await.unwrap();
    let fd = open_rw(&path);

    let mut handles = Vec::new();
    for _ in 0..4 {
        for number in 0..2u64 {
            let fd = fd.clone();
            let cipher = cipher.clone();
            let content = content.clone();
            handles.push(tokio::spawn(async move {
                let offset = number * piece_length;
                let piece = content[offset as usize..(offset + piece_length) as usize].to_vec();
                for _ in 0..10 {
                    write_encrypted_range(
                        &dragonfly_client_util::fs::fd::WriteFD::new(fd.clone()),
                        cipher.clone(),
                        offset,
                        piece_length,
                        2 * chunk,
                        &mut Cursor::new(piece.clone()),
                        &buffer_pool(),
                    )
                    .await
                    .unwrap();
                }
            }));
        }
    }
    for handle in handles {
        handle.await.unwrap();
    }

    // No chunk is torn: the whole content decrypts to the original.
    let to = dir.path().join("plaintext");
    let copied = encryptor
        .copy_file(&path, &to, 1024 * 1024, &buffer_pool())
        .await
        .unwrap();
    assert_eq!(copied, content.len() as u64);
    assert_eq!(fs::read(&to).await.unwrap(), *content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dropping_a_reader_with_a_prefetch_in_flight() {
    let dir = tempdir().unwrap();
    let encryptor = encryptor();
    let path = dir.path().join("tasks").join("abc").join("abcdef");
    fs::create_dir_all(path.parent().unwrap()).await.unwrap();

    let chunk = DEFAULT_CHUNK_SIZE as usize;
    let piece_length = (16 * chunk) as u64;
    let content = pattern(piece_length as usize);
    encryptor
        .create_file(&path, piece_length, piece_length)
        .await
        .unwrap();
    let cipher = encryptor.cipher(&path).await.unwrap();
    let fd = open_rw(&path);
    write_encrypted_range(
        &dragonfly_client_util::fs::fd::WriteFD::new(fd.clone()),
        cipher.clone(),
        0,
        piece_length,
        2 * chunk,
        &mut Cursor::new(content.clone()),
        &buffer_pool(),
    )
    .await
    .unwrap();

    // Drop readers right after their first fill, which starts the prefetch of
    // the next batch.
    let pool = buffer_pool();
    for _ in 0..200 {
        let mut reader = EncryptedRangeReader::new(
            fd.clone(),
            cipher.clone(),
            0,
            piece_length,
            0,
            piece_length,
            2 * chunk,
            pool.clone(),
        )
        .unwrap();
        assert!(!reader.fill_buf().await.unwrap().is_empty());
        drop(reader);
    }

    // The pool and the file are intact: a full read still matches.
    let mut reader = EncryptedRangeReader::new(
        fd,
        cipher,
        0,
        piece_length,
        0,
        piece_length,
        2 * chunk,
        pool,
    )
    .unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    assert_eq!(buffer, content);
}
