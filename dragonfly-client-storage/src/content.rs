/*
 *     Copyright 2024 The Dragonfly Authors
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

use crate::cache::lru_cache::LruCache;
use dragonfly_api::common::v2::Range;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Result;
use std::cmp::{max, min};
use std::fs::File;
use std::future::Future;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{ready, Context, Poll};
use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};
use tokio::task::JoinHandle;

#[cfg(target_os = "linux")]
pub type Content = super::content_linux::Content;

#[cfg(target_os = "macos")]
pub type Content = super::content_macos::Content;

/// The default directory for store content.
pub const DEFAULT_CONTENT_DIR: &str = "content";

/// The default directory for store task.
pub const DEFAULT_TASK_DIR: &str = "tasks";

/// The default directory for store persistent task.
pub const DEFAULT_PERSISTENT_TASK_DIR: &str = "persistent-tasks";

/// The default directory for store persistent cache task.
pub const DEFAULT_PERSISTENT_CACHE_TASK_DIR: &str = "persistent-cache-tasks";

/// The default capacity of the file descriptor cache.
pub const DEFAULT_FD_CACHE_CAPACITY: usize = 1024;

/// The response of writing a piece.
pub struct WritePieceResponse {
    /// The length of the piece.
    pub length: u64,

    /// The hash of the piece.
    pub hash: String,
}

/// The response of writing a persistent task.
pub struct WritePersistentTaskResponse {
    /// The length of the persistent task.
    pub length: u64,

    /// The hash of the persistent task.
    pub hash: String,
}

/// The response of writing a persistent cache task.
pub struct WritePersistentCacheTaskResponse {
    /// The length of the persistent cache task.
    pub length: u64,

    /// The hash of the persistent cache task.
    pub hash: String,
}

/// Creates a new Content instance to support linux and macos.
pub async fn new_content(config: Arc<Config>, dir: &Path) -> Result<Content> {
    Content::new(config, dir).await
}

/// Calculates the target offset and length based on the piece range and
/// request range.
pub fn calculate_piece_range(offset: u64, length: u64, range: Option<Range>) -> (u64, u64) {
    if let Some(range) = range {
        let target_offset = max(offset, range.start);
        let target_length =
            min(offset + length - 1, range.start + range.length - 1) - target_offset + 1;
        (target_offset, target_length)
    } else {
        (offset, length)
    }
}

/// FDCache caches opened file descriptors by path with LRU eviction, so repeated
/// piece reads of the same task reuse one descriptor instead of reopening the
/// file every time. The cached descriptors are only used for positional reads,
/// which never move the file cursor, so one descriptor is safely shared by
/// concurrent readers.
pub struct FDCache {
    /// The opened file descriptors by path.
    fds: Mutex<LruCache<PathBuf, Arc<File>>>,
}

/// Implements the file descriptor cache.
impl FDCache {
    /// Creates a new FDCache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            fds: Mutex::new(LruCache::new(capacity)),
        }
    }

    /// Returns the cached file descriptor of the path, opening and caching it if
    /// it is absent. Concurrent misses may open the file more than once, the last
    /// inserted descriptor wins and the others are closed when their readers finish.
    pub async fn open(&self, path: &Path) -> io::Result<Arc<File>> {
        if let Some(fd) = self.fds.lock().unwrap().get(path) {
            return Ok(fd.clone());
        }

        let path = path.to_path_buf();
        let fd = Arc::new(
            tokio::task::spawn_blocking({
                let path = path.clone();
                move || File::open(path)
            })
            .await
            .map_err(io::Error::other)??,
        );

        self.fds.lock().unwrap().put(path, fd.clone());
        Ok(fd)
    }

    /// Removes the cached file descriptor of the path. It needs to be called when
    /// the content is deleted, so a recreated task will not read the stale file.
    pub fn remove(&self, path: &Path) {
        self.fds.lock().unwrap().pop(path);
    }
}

/// The state of the in-flight read of the RangeReader.
enum RangeReaderState {
    /// No read is in flight, the buffer may hold unread data.
    Idle,

    /// A positional read is running on the blocking thread pool, owning the
    /// buffer until it completes.
    Reading(JoinHandle<io::Result<(Vec<u8>, usize)>>),
}

/// RangeReader reads a fixed range of a file with positional reads on a shared
/// file descriptor. It never seeks the descriptor and fills a single reusable
/// buffer sized to the range, instead of allocating a full read buffer and
/// seeking for every piece.
pub struct RangeReader {
    /// The shared file descriptor to read from.
    fd: Arc<File>,

    /// The offset of the next positional read.
    offset: u64,

    /// The remaining length of the range.
    remaining: u64,

    /// The reusable read buffer, moved into the blocking task while reading.
    buf: Vec<u8>,

    /// The consumed position of the buffer.
    pos: usize,

    /// The filled length of the buffer.
    filled: usize,

    /// The state of the in-flight read.
    state: RangeReaderState,
}

/// Implements the range reader.
impl RangeReader {
    /// Creates a new RangeReader reading `length` bytes starting at `offset`.
    pub fn new(fd: Arc<File>, offset: u64, length: u64, buffer_size: usize) -> Self {
        let capacity = min(max(buffer_size, 1) as u64, length) as usize;
        Self {
            fd,
            offset,
            remaining: length,
            buf: vec![0u8; capacity],
            pos: 0,
            filled: 0,
            state: RangeReaderState::Idle,
        }
    }
}

/// Implements the buffered read for the RangeReader.
impl AsyncBufRead for RangeReader {
    /// Polls the buffer to fill it with more data, returning a slice of the filled
    /// buffer. The buffer is filled with a positional read on the blocking thread pool, and the
    /// buffer is reused for subsequent reads.
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                RangeReaderState::Idle => {
                    if this.pos < this.filled || this.remaining == 0 {
                        break;
                    }

                    let len = min(this.buf.len() as u64, this.remaining) as usize;
                    let mut buf = std::mem::take(&mut this.buf);
                    let fd = this.fd.clone();
                    let offset = this.offset;
                    this.state =
                        RangeReaderState::Reading(tokio::task::spawn_blocking(move || {
                            let n = fd.read_at(&mut buf[..len], offset)?;
                            Ok((buf, n))
                        }));
                }
                RangeReaderState::Reading(handle) => {
                    let (buf, n) =
                        ready!(Pin::new(handle).poll(cx)).map_err(io::Error::other)??;
                    this.buf = buf;
                    this.pos = 0;
                    this.filled = n;
                    this.offset += n as u64;

                    // Stop at the end of the file even if the range is longer.
                    this.remaining = if n == 0 { 0 } else { this.remaining - n as u64 };
                    this.state = RangeReaderState::Idle;
                }
            }
        }

        Poll::Ready(Ok(&this.buf[this.pos..this.filled]))
    }

    /// Consumes `amt` bytes from the buffer, advancing the position.
    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();
        this.pos = min(this.pos + amt, this.filled);
    }
}

/// Implements the read for the RangeReader.
impl AsyncRead for RangeReader {
    /// Polls the read to fill the provided buffer with data from the range. It uses
    /// the buffered read to fill the buffer, and consumes the filled bytes.
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let inner = ready!(self.as_mut().poll_fill_buf(cx))?;
        let amt = min(inner.len(), buf.remaining());
        buf.put_slice(&inner[..amt]);
        self.consume(amt);
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::tempdir;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};

    fn pattern(length: usize) -> Vec<u8> {
        (0..length).map(|i| (i % 251) as u8).collect()
    }

    #[tokio::test]
    async fn test_fd_cache() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();

        let cache = FDCache::new(DEFAULT_FD_CACHE_CAPACITY);
        let first = cache.open(&path).await.unwrap();
        let second = cache.open(&path).await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));

        cache.remove(&path);
        let third = cache.open(&path).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &third));

        cache.remove(&path);
        tokio::fs::remove_file(&path).await.unwrap();
        assert!(cache.open(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_range_reader() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();
        let fd = Arc::new(File::open(&path).unwrap());

        let mut reader = RangeReader::new(fd.clone(), 0, 13, 512);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, b"hello, world!");

        let mut reader = RangeReader::new(fd.clone(), 7, 5, 2);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, b"world");

        let mut reader = RangeReader::new(fd.clone(), 7, 100, 512);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, b"world!");

        let mut reader = RangeReader::new(fd, 0, 0, 512);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert!(buffer.is_empty());
    }

    #[tokio::test]
    async fn test_range_reader_multiple_fills() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        let data = pattern(256 * 1024);
        tokio::fs::write(&path, &data).await.unwrap();
        let fd = Arc::new(File::open(&path).unwrap());

        let mut reader = RangeReader::new(fd.clone(), 0, data.len() as u64, 4 * 1024);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, data);

        let mut reader = RangeReader::new(fd, 12_345, 30_000, 4 * 1024);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, &data[12_345..42_345]);
    }

    #[tokio::test]
    async fn test_range_reader_fill_buf_and_consume() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();
        let fd = Arc::new(File::open(&path).unwrap());

        let mut reader = RangeReader::new(fd, 0, 13, 5);
        assert_eq!(reader.fill_buf().await.unwrap(), b"hello");

        reader.consume(2);
        assert_eq!(reader.fill_buf().await.unwrap(), b"llo");

        reader.consume(3);
        assert_eq!(reader.fill_buf().await.unwrap(), b", wor");

        reader.consume(5);
        assert_eq!(reader.fill_buf().await.unwrap(), b"ld!");

        reader.consume(100);
        assert!(reader.fill_buf().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_range_reader_copy_buf() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        let data = pattern(64 * 1024);
        tokio::fs::write(&path, &data).await.unwrap();
        let fd = Arc::new(File::open(&path).unwrap());

        let mut reader = RangeReader::new(fd, 1_000, 50_000, 8 * 1024);
        let mut writer = Cursor::new(Vec::new());
        let copied = tokio::io::copy_buf(&mut reader, &mut writer).await.unwrap();
        assert_eq!(copied, 50_000);
        assert_eq!(writer.into_inner(), &data[1_000..51_000]);
    }

    #[tokio::test]
    async fn test_range_reader_concurrent_readers() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        let data = pattern(64 * 1024);
        tokio::fs::write(&path, &data).await.unwrap();
        let fd = Arc::new(File::open(&path).unwrap());

        let range_length: u64 = 8 * 1024;
        let handles: Vec<_> = (0..8u64)
            .map(|i| {
                let fd = fd.clone();
                tokio::spawn(async move {
                    let mut reader = RangeReader::new(fd, i * range_length, range_length, 1024);
                    let mut buffer = Vec::new();
                    reader.read_to_end(&mut buffer).await.unwrap();
                    (i, buffer)
                })
            })
            .collect();

        for handle in handles {
            let (i, buffer) = handle.await.unwrap();
            let start = (i * range_length) as usize;
            assert_eq!(buffer, &data[start..start + range_length as usize]);
        }
    }

    #[tokio::test]
    async fn test_range_reader_buffer_sizes() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();
        let fd = Arc::new(File::open(&path).unwrap());

        for buffer_size in [13, 512, 1, 0] {
            let mut reader = RangeReader::new(fd.clone(), 0, 13, buffer_size);
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, b"hello, world!");
        }
    }

    #[tokio::test]
    async fn test_range_reader_offset_beyond_eof() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();
        let fd = Arc::new(File::open(&path).unwrap());

        let mut reader = RangeReader::new(fd.clone(), 13, 5, 512);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert!(buffer.is_empty());

        let mut reader = RangeReader::new(fd, 100, 5, 512);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert!(buffer.is_empty());
    }

    #[tokio::test]
    async fn test_range_reader_small_chunk_reads() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();
        let fd = Arc::new(File::open(&path).unwrap());

        let mut reader = RangeReader::new(fd, 0, 13, 512);
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 3];
        loop {
            let n = reader.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..n]);
        }

        assert_eq!(buffer, b"hello, world!");
    }

    #[tokio::test]
    async fn test_calculate_piece_range() {
        let test_cases = vec![
            (1, 4, None, 1, 4),
            (
                1,
                4,
                Some(Range {
                    start: 1,
                    length: 4,
                }),
                1,
                4,
            ),
            (
                1,
                4,
                Some(Range {
                    start: 2,
                    length: 1,
                }),
                2,
                1,
            ),
            (
                1,
                4,
                Some(Range {
                    start: 1,
                    length: 1,
                }),
                1,
                1,
            ),
            (
                1,
                4,
                Some(Range {
                    start: 4,
                    length: 1,
                }),
                4,
                1,
            ),
            (
                1,
                4,
                Some(Range {
                    start: 0,
                    length: 2,
                }),
                1,
                1,
            ),
            (
                1,
                4,
                Some(Range {
                    start: 4,
                    length: 3,
                }),
                4,
                1,
            ),
        ];

        for (piece_offset, piece_length, range, expected_offset, expected_length) in test_cases {
            let (target_offset, target_length) =
                calculate_piece_range(piece_offset, piece_length, range);
            assert_eq!(target_offset, expected_offset);
            assert_eq!(target_length, expected_length);
        }
    }
}
