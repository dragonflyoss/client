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
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{ready, Context, Poll};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf};
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
/// piece reads and writes of the same task reuse one descriptor instead of
/// reopening the file every time. The read-only and write-only descriptors are
/// cached separately, so a write open failure surfaces at open time instead of
/// falling back to a cached read-only descriptor. The cached descriptors are
/// only used for positional reads and writes, which never move the file cursor,
/// so one descriptor is safely shared by concurrent readers and writers.
pub struct FDCache {
    /// The opened file descriptors for reading by path.
    read_fds: Mutex<LruCache<PathBuf, Arc<File>>>,

    /// The opened file descriptors for writing by path.
    write_fds: Mutex<LruCache<PathBuf, Arc<File>>>,
}

/// Implements the file descriptor cache.
impl FDCache {
    /// Creates a new FDCache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            read_fds: Mutex::new(LruCache::new(capacity)),
            write_fds: Mutex::new(LruCache::new(capacity)),
        }
    }

    /// Returns the cached read-only file descriptor of the path, opening and
    /// caching it if it is absent. Concurrent misses may open the file more than
    /// once, the last inserted descriptor wins and the others are closed when
    /// their readers finish.
    pub async fn open(&self, path: &Path) -> Result<Arc<File>> {
        if let Some(fd) = self.read_fds.lock()?.get(path) {
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

        self.read_fds.lock()?.put(path, fd.clone());
        Ok(fd)
    }

    /// Returns the cached write-only file descriptor of the path, opening and
    /// caching it if it is absent. The open error surfaces to the caller and
    /// nothing is cached on failure, so a file that becomes writable later is
    /// opened again on the next write.
    pub async fn open_write(&self, path: &Path) -> Result<Arc<File>> {
        if let Some(fd) = self.write_fds.lock()?.get(path) {
            return Ok(fd.clone());
        }

        let path = path.to_path_buf();
        let fd = Arc::new(
            tokio::task::spawn_blocking({
                let path = path.clone();
                move || {
                    OpenOptions::new()
                        .truncate(false)
                        .write(true)
                        .open(path.as_path())
                }
            })
            .await
            .map_err(io::Error::other)??,
        );

        self.write_fds.lock()?.put(path, fd.clone());
        Ok(fd)
    }

    /// Removes the cached file descriptors of the path. It needs to be called when
    /// the content is deleted, so a recreated task will not read the stale file.
    pub fn remove(&self, path: &Path) -> Result<()> {
        self.read_fds.lock()?.pop(path);
        self.write_fds.lock()?.pop(path);
        Ok(())
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

    /// Consumes the reader and returns the shared file descriptor, the offset
    /// of the next unconsumed byte, and the number of bytes left in the range,
    /// so zero-copy senders like sendfile can transmit the rest of the range
    /// directly from the file descriptor.
    pub fn into_parts(self) -> (Arc<File>, u64, u64) {
        let buffered = (self.filled - self.pos) as u64;
        (self.fd, self.offset - buffered, self.remaining + buffered)
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
                    let result = ready!(Pin::new(handle).poll(cx));
                    this.state = RangeReaderState::Idle;
                    let (buf, n) = result.map_err(io::Error::other)??;
                    this.buf = buf;
                    this.pos = 0;
                    this.filled = n;
                    this.offset += n as u64;

                    // Stop at the end of the file even if the range is longer.
                    this.remaining = if n == 0 { 0 } else { this.remaining - n as u64 };
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

/// The state of the in-flight write of the RangeWriter.
enum RangeWriterState {
    /// No write is in flight, the buffer may hold buffered data.
    Idle,

    /// A positional write is running on the blocking thread pool, owning the
    /// buffer until it completes.
    Writing(JoinHandle<io::Result<Vec<u8>>>),
}

/// RangeWriter writes sequential data starting at a fixed offset of a file
/// with positional writes on a shared file descriptor. It never seeks the
/// descriptor and buffers the data into a single reusable buffer, instead of
/// reopening and seeking the file for every piece.
pub struct RangeWriter {
    /// The shared file descriptor to write to.
    fd: Arc<File>,

    /// The offset of the next positional write.
    offset: u64,

    /// The reusable write buffer, moved into the blocking task while writing.
    buf: Vec<u8>,

    /// The capacity of the write buffer.
    capacity: usize,

    /// The state of the in-flight write.
    state: RangeWriterState,
}

/// Implements the range writer.
impl RangeWriter {
    /// Creates a new RangeWriter writing at `offset` with a reusable buffer of
    /// `buffer_size` bytes.
    pub fn new(fd: Arc<File>, offset: u64, buffer_size: usize) -> Self {
        let capacity = max(buffer_size, 1);
        Self {
            fd,
            offset,
            buf: Vec::with_capacity(capacity),
            capacity,
            state: RangeWriterState::Idle,
        }
    }

    /// Moves the buffered data into a positional write on the blocking thread
    /// pool and advances the offset.
    fn dispatch(&mut self) {
        let buf = std::mem::take(&mut self.buf);
        let fd = self.fd.clone();
        let offset = self.offset;
        self.offset += buf.len() as u64;
        self.state = RangeWriterState::Writing(tokio::task::spawn_blocking(move || {
            fd.write_all_at(&buf, offset)?;
            Ok(buf)
        }));
    }

    /// Polls the in-flight write to complete, restoring the buffer for reuse.
    fn poll_idle(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.state {
            RangeWriterState::Idle => Poll::Ready(Ok(())),
            RangeWriterState::Writing(handle) => {
                let result = ready!(Pin::new(handle).poll(cx));
                self.state = RangeWriterState::Idle;
                self.buf = result.map_err(io::Error::other)??;
                self.buf.clear();
                Poll::Ready(Ok(()))
            }
        }
    }
}

/// Implements the buffered write for the RangeWriter.
impl AsyncWrite for RangeWriter {
    /// Polls the write to buffer the data, dispatching a positional write on the
    /// blocking thread pool when the buffer is full. The buffer is reused for
    /// subsequent writes.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        ready!(this.poll_idle(cx))?;

        let n = min(this.capacity - this.buf.len(), data.len());
        this.buf.extend_from_slice(&data[..n]);
        if this.buf.len() == this.capacity {
            this.dispatch();
        }

        Poll::Ready(Ok(n))
    }

    /// Polls the flush to dispatch the buffered data and wait for the writes
    /// to complete.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_idle(cx))?;
        if !this.buf.is_empty() {
            this.dispatch();
            ready!(this.poll_idle(cx))?;
        }

        Poll::Ready(Ok(()))
    }

    /// Polls the shutdown to flush the buffered data.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::tempdir;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

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

        let first_write = cache.open_write(&path).await.unwrap();
        let second_write = cache.open_write(&path).await.unwrap();
        assert!(Arc::ptr_eq(&first_write, &second_write));
        assert!(!Arc::ptr_eq(&first, &first_write));

        let _ = cache.remove(&path);
        let third = cache.open(&path).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &third));
        let third_write = cache.open_write(&path).await.unwrap();
        assert!(!Arc::ptr_eq(&first_write, &third_write));

        let _ = cache.remove(&path);
        tokio::fs::remove_file(&path).await.unwrap();
        assert!(cache.open(&path).await.is_err());
        assert!(cache.open_write(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_fd_cache_open_write_read_only_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();

        let mut permissions = tokio::fs::metadata(&path).await.unwrap().permissions();
        permissions.set_readonly(true);
        tokio::fs::set_permissions(&path, permissions)
            .await
            .unwrap();

        let cache = FDCache::new(DEFAULT_FD_CACHE_CAPACITY);
        let fd = cache.open(&path).await.unwrap();
        let mut buffer = vec![0u8; 13];
        fd.read_at(&mut buffer, 0).unwrap();
        assert_eq!(buffer, b"hello, world!");
        assert!(cache.open_write(&path).await.is_err());

        tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .await
            .unwrap();

        let fd = cache.open_write(&path).await.unwrap();
        fd.write_all_at(b"HELLO", 0).unwrap();
        assert_eq!(&tokio::fs::read(&path).await.unwrap()[..5], b"HELLO");
    }

    #[tokio::test]
    async fn test_range_reader_poll_after_read_error() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();
        let fd = Arc::new(
            OpenOptions::new()
                .truncate(false)
                .write(true)
                .open(&path)
                .unwrap(),
        );

        let mut reader = RangeReader::new(fd, 0, 13, 4);
        let mut buffer = Vec::new();
        assert!(reader.read_to_end(&mut buffer).await.is_err());

        // Polling again after the error must not panic.
        let mut buffer = Vec::new();
        let _ = reader.read_to_end(&mut buffer).await;
    }

    #[tokio::test]
    async fn test_range_writer_poll_after_write_error() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();
        let fd = Arc::new(File::open(&path).unwrap());

        let mut writer = RangeWriter::new(fd, 0, 4);
        assert!(writer.write_all(b"data!").await.is_err());

        // Polling again after the error must not panic.
        let _ = writer.write_all(b"more").await;
        let _ = writer.flush().await;
    }

    #[tokio::test]
    async fn test_range_writer_then_range_reader_shared_cache() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"").await.unwrap();

        let cache = FDCache::new(DEFAULT_FD_CACHE_CAPACITY);

        let data = pattern(16 * 1024);
        let mut writer = RangeWriter::new(cache.open_write(&path).await.unwrap(), 0, 4 * 1024);
        writer.write_all(&data).await.unwrap();
        writer.flush().await.unwrap();

        let mut reader = RangeReader::new(
            cache.open(&path).await.unwrap(),
            0,
            data.len() as u64,
            4 * 1024,
        );
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, data);
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
    async fn test_range_reader_into_parts() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();
        let fd = Arc::new(File::open(&path).unwrap());

        // A fresh reader returns the range unchanged.
        let reader = RangeReader::new(fd.clone(), 2, 11, 5);
        let (parts_fd, offset, remaining) = reader.into_parts();
        assert!(Arc::ptr_eq(&parts_fd, &fd));
        assert_eq!(offset, 2);
        assert_eq!(remaining, 11);

        // A partially consumed reader accounts for the buffered data.
        let mut reader = RangeReader::new(fd, 2, 11, 5);
        assert_eq!(reader.fill_buf().await.unwrap(), b"llo, ");
        reader.consume(2);
        let (_, offset, remaining) = reader.into_parts();
        assert_eq!(offset, 4);
        assert_eq!(remaining, 9);
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
    async fn test_range_writer() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"").await.unwrap();
        let fd = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );

        let mut writer = RangeWriter::new(fd, 0, 512);
        writer.write_all(b"hello, world!").await.unwrap();
        writer.flush().await.unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"hello, world!");
    }

    #[tokio::test]
    async fn test_range_writer_multiple_fills() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"").await.unwrap();
        let fd = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );

        let data = pattern(256 * 1024);
        let mut writer = RangeWriter::new(fd, 0, 4 * 1024);
        writer.write_all(&data).await.unwrap();
        writer.flush().await.unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_range_writer_offset() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, vec![0u8; 64]).await.unwrap();
        let fd = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );

        let mut writer = RangeWriter::new(fd, 7, 2);
        writer.write_all(b"world").await.unwrap();
        writer.flush().await.unwrap();

        let content = tokio::fs::read(&path).await.unwrap();
        assert_eq!(&content[7..12], b"world");
        assert_eq!(content.len(), 64);
    }

    #[tokio::test]
    async fn test_range_writer_small_chunk_writes() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"").await.unwrap();
        let fd = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );

        let data = pattern(1024);
        let mut writer = RangeWriter::new(fd, 0, 16);
        for chunk in data.chunks(3) {
            writer.write_all(chunk).await.unwrap();
        }
        writer.flush().await.unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_range_writer_buffer_sizes() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");

        for buffer_size in [13, 512, 1, 0] {
            tokio::fs::write(&path, b"").await.unwrap();
            let fd = Arc::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .unwrap(),
            );

            let mut writer = RangeWriter::new(fd, 0, buffer_size);
            writer.write_all(b"hello, world!").await.unwrap();
            writer.flush().await.unwrap();
            assert_eq!(tokio::fs::read(&path).await.unwrap(), b"hello, world!");
        }
    }

    #[tokio::test]
    async fn test_range_writer_concurrent_writers() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        let data = pattern(64 * 1024);
        tokio::fs::write(&path, vec![0u8; data.len()])
            .await
            .unwrap();
        let fd = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );

        let range_length: usize = 8 * 1024;
        let handles: Vec<_> = (0..8usize)
            .map(|i| {
                let fd = fd.clone();
                let piece = data[i * range_length..(i + 1) * range_length].to_vec();
                tokio::spawn(async move {
                    let mut writer = RangeWriter::new(fd, (i * range_length) as u64, 1024);
                    writer.write_all(&piece).await.unwrap();
                    writer.flush().await.unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
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
