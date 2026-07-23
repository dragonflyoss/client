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

use bytes::{Bytes, BytesMut};
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::buffer_pool::BufferPool;
use futures::{Stream, TryStreamExt};
use std::cmp::{max, min};
use std::fs::File;
use std::future::Future;
use std::io;
use std::os::unix::fs::FileExt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, ReadBuf};
use tokio::task::JoinHandle;

/// The response of writing a range.
pub struct WriteRangeResponse {
    /// The length of the written range.
    pub length: u64,

    /// The CRC32 hash of the written range.
    pub hash: String,
}

/// The state of the in-flight read of the RangeReader.
enum RangeReaderState {
    /// No read is in flight, the buffer may hold unread data.
    Idle,

    /// A positional read is running on the blocking thread pool, owning the
    /// buffer until it completes.
    Reading(JoinHandle<io::Result<(BytesMut, usize)>>),
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

    /// The capacity of the read buffer.
    capacity: usize,

    /// The reusable read buffer, allocated on the first buffered read and
    /// returned to the buffer pool on drop.
    buffer: BytesMut,

    /// The consumed position of the buffer.
    pos: usize,

    /// The filled length of the buffer.
    filled: usize,

    /// The state of the in-flight read.
    state: RangeReaderState,

    /// The buffer pool the read buffers are checked out from.
    buffer_pool: BufferPool,
}

/// Implements the range reader.
impl RangeReader {
    /// Creates a new RangeReader reading `length` bytes starting at `offset`.
    pub fn new(
        fd: Arc<File>,
        offset: u64,
        length: u64,
        buffer_size: usize,
        buffer_pool: BufferPool,
    ) -> Self {
        let capacity = min(max(buffer_size, 1) as u64, length) as usize;
        Self {
            fd,
            offset,
            remaining: length,
            capacity,
            buffer: BytesMut::new(),
            pos: 0,
            filled: 0,
            state: RangeReaderState::Idle,
            buffer_pool,
        }
    }

    /// Consumes the reader and returns the shared file descriptor, the offset
    /// of the next unconsumed byte, and the number of bytes left in the range,
    /// so zero-copy senders like sendfile can transmit the rest of the range
    /// directly from the file descriptor.
    pub fn into_parts(self) -> (Arc<File>, u64, u64) {
        let buffered = (self.filled - self.pos) as u64;
        (
            self.fd.clone(),
            self.offset - buffered,
            self.remaining + buffered,
        )
    }

    /// Reads the next chunk of the range with a positional read on the
    /// blocking thread pool, directly into the owned buffer it returns, so
    /// callers that send the chunk downstream get the bytes without copying
    /// them out of a reusable buffer.
    pub async fn read_chunk(&mut self) -> Result<Bytes> {
        // Drain the data buffered by the buffered read interface first.
        if self.pos < self.filled {
            let chunk = Bytes::copy_from_slice(&self.buffer[self.pos..self.filled]);
            self.pos = self.filled;
            return Ok(chunk);
        }

        if self.remaining == 0 {
            return Ok(Bytes::new());
        }

        let len = min(self.capacity as u64, self.remaining) as usize;
        let mut buffer = self.buffer_pool.checkout_for_read(len);
        let fd = self.fd.clone();
        let offset = self.offset;
        let (mut buffer, n) = tokio::task::spawn_blocking(move || {
            let n = fd.read_at(&mut buffer, offset)?;
            Ok::<_, io::Error>((buffer, n))
        })
        .await
        .map_err(io::Error::other)??;

        self.offset += n as u64;
        self.remaining = if n == 0 { 0 } else { self.remaining - n as u64 };

        buffer.truncate(n);
        Ok(self.buffer_pool.freeze(buffer))
    }
}

/// Implements the drop of the range reader.
impl Drop for RangeReader {
    /// Returns the read buffer to the buffer pool. The buffer is empty
    /// when no buffered read happened or a read is still in flight, and is
    /// skipped in that case.
    fn drop(&mut self) {
        let buffer = std::mem::take(&mut self.buffer);
        if buffer.capacity() > 0 {
            self.buffer_pool.give_back(buffer);
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

                    let len = min(this.capacity as u64, this.remaining) as usize;
                    let mut buffer = std::mem::take(&mut this.buffer);
                    if buffer.capacity() == 0 {
                        buffer = this.buffer_pool.checkout_for_read(len);
                    } else {
                        buffer.truncate(len);
                    }

                    let fd = this.fd.clone();
                    let offset = this.offset;
                    this.state =
                        RangeReaderState::Reading(tokio::task::spawn_blocking(move || {
                            let n = fd.read_at(&mut buffer, offset)?;
                            Ok((buffer, n))
                        }));
                }
                RangeReaderState::Reading(handle) => {
                    let result = ready!(Pin::new(handle).poll(cx));
                    this.state = RangeReaderState::Idle;
                    let (buffer, n) = result.map_err(io::Error::other)??;
                    this.buffer = buffer;
                    this.pos = 0;
                    this.filled = n;
                    this.offset += n as u64;

                    // Stop at the end of the file even if the range is longer.
                    this.remaining = if n == 0 { 0 } else { this.remaining - n as u64 };
                }
            }
        }

        Poll::Ready(Ok(&this.buffer[this.pos..this.filled]))
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
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let inner = ready!(self.as_mut().poll_fill_buf(cx))?;
        let amt = min(inner.len(), buffer.remaining());
        buffer.put_slice(&inner[..amt]);
        self.consume(amt);
        Poll::Ready(Ok(()))
    }
}

/// Writes exactly `expected_length` bytes from the reader to the file with
/// positional writes starting at `offset`, and calculates the CRC32 hash of
/// the written content. The reader fills one buffer on the runtime and hashes
/// it while the data is still hot in the cache of the filling core, then
/// hands it to the blocking thread pool for the positional write, swapping
/// the recycled buffer of the completed write back for the next fill instead
/// of copying, so the read and the hash overlap the write.
pub async fn write_range<R: AsyncRead + Unpin + ?Sized>(
    fd: Arc<File>,
    mut offset: u64,
    expected_length: u64,
    buffer_size: usize,
    reader: &mut R,
    buffer_pool: &BufferPool,
) -> Result<WriteRangeResponse> {
    let buffer_size = max(buffer_size, 1);
    let mut reader = reader.take(expected_length);

    // The buffer being filled from the reader, swapped with the recycled
    // buffer of the completed write. Both staging buffers are checked out
    // from the buffer pool and returned to it at the end.
    let mut buffer = buffer_pool.checkout(buffer_size);

    // The in-flight write on the blocking thread pool, owning the other
    // buffer until it completes.
    let mut in_flight: Option<JoinHandle<io::Result<BytesMut>>> = None;

    let mut hasher = crc32fast::Hasher::new();
    let mut length: u64 = 0;
    loop {
        // Fill the buffer until it is full or the reader reaches EOF.
        while buffer.len() < buffer_size {
            if reader.read_buf(&mut buffer).await? == 0 {
                break;
            }
        }

        if buffer.is_empty() {
            break;
        }

        // Hash the buffer while its data is still hot in the cache of the
        // core that filled it.
        hasher.update(&buffer);

        // Wait for the previous write to take back its buffer.
        let recycled = match in_flight.take() {
            Some(handle) => handle.await.map_err(io::Error::other)??,
            None => buffer_pool.checkout(buffer_size),
        };

        let filled = std::mem::replace(&mut buffer, recycled);
        length += filled.len() as u64;

        let fd = fd.clone();
        let filled_offset = offset;
        offset += filled.len() as u64;
        in_flight = Some(tokio::task::spawn_blocking(move || {
            fd.write_all_at(&filled, filled_offset)?;

            let mut recycled = filled;
            recycled.clear();
            Ok(recycled)
        }));
    }

    // Wait for the last write to complete and return the staging buffers to
    // the pool.
    if let Some(handle) = in_flight.take() {
        buffer_pool.give_back(handle.await.map_err(io::Error::other)??);
    }
    buffer_pool.give_back(buffer);

    if length != expected_length {
        return Err(Error::Unknown(format!(
            "expected length {expected_length} but got {length}"
        )));
    }

    Ok(WriteRangeResponse {
        length,
        hash: hasher.finalize().to_string(),
    })
}

/// Writes all the chunks to the file with vectored positional writes
/// (`pwritev`) starting at `offset`, submitting up to `MAX_WRITE_IOVECS`
/// chunks per syscall instead of one write per chunk, and resuming after
/// partial writes.
fn write_all_vectored_at(fd: &File, chunks: &[Bytes], mut offset: u64) -> io::Result<()> {
    // The maximum number of chunks of a single pwritev call, matching the
    // IOV_MAX of Linux and macOS.
    const MAX_WRITE_IOVECS: usize = 1024;

    // The first chunk not fully written and the bytes of it already consumed
    // by a partial write.
    let mut index = 0;
    let mut written = 0;
    let mut buffers = Vec::with_capacity(min(chunks.len(), MAX_WRITE_IOVECS));
    while index < chunks.len() {
        buffers.clear();
        buffers.push(io::IoSlice::new(&chunks[index][written..]));
        for chunk in chunks[index + 1..].iter().take(MAX_WRITE_IOVECS - 1) {
            buffers.push(io::IoSlice::new(chunk));
        }

        let mut n = match rustix::io::pwritev(fd, &buffers, offset) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ))
            }
            Ok(n) => n,
            Err(rustix::io::Errno::INTR) => continue,
            Err(err) => return Err(err.into()),
        };

        // Advance past the fully written chunks into the partially written
        // one.
        offset += n as u64;
        while index < chunks.len() {
            let remaining = chunks[index].len() - written;
            if n < remaining {
                written += n;
                break;
            }

            n -= remaining;
            written = 0;
            index += 1;
        }
    }

    Ok(())
}

/// Writes exactly `expected_length` bytes from the stream of bytes chunks to
/// the file with positional writes starting at `offset`, and calculates the
/// CRC32 hash of the written content. Each chunk is hashed on the runtime
/// while its data is still hot in the cache of the polling core and staged
/// without copying, then the staged chunks are handed to the blocking thread
/// pool for a vectored positional write once they hold `buffer_size` bytes,
/// swapping the emptied vector of the completed write back for the next
/// batch, so the receive and the hash overlap the write and the chunks are
/// dropped off the runtime.
pub async fn write_range_from_stream<S>(
    fd: Arc<File>,
    mut offset: u64,
    expected_length: u64,
    buffer_size: usize,
    stream: &mut S,
) -> Result<WriteRangeResponse>
where
    S: Stream<Item = io::Result<Bytes>> + Unpin + ?Sized,
{
    let buffer_size = max(buffer_size, 1);

    // The chunks staged for the next write, swapped with the emptied vector
    // of the completed write.
    let mut batch: Vec<Bytes> = Vec::new();
    let mut batch_size: usize = 0;

    // The in-flight write on the blocking thread pool, owning the staged
    // chunks until it completes.
    let mut in_flight: Option<JoinHandle<io::Result<Vec<Bytes>>>> = None;
    let mut hasher = crc32fast::Hasher::new();
    let mut length: u64 = 0;
    let mut eof = false;
    loop {
        // Stage the chunks until they hold buffer_size bytes, the expected
        // length is reached or the stream ends.
        while !eof && batch_size < buffer_size && length < expected_length {
            match stream.try_next().await? {
                Some(mut chunk) => {
                    if chunk.is_empty() {
                        continue;
                    }

                    // Cap the consumed bytes at the expected length, the rest
                    // of the stream is ignored.
                    let remaining = expected_length - length;
                    if chunk.len() as u64 > remaining {
                        chunk.truncate(remaining as usize);
                    }

                    // Hash the chunk while its data is still hot in the cache
                    // of the polling core.
                    hasher.update(&chunk);
                    length += chunk.len() as u64;
                    batch_size += chunk.len();
                    batch.push(chunk);
                }
                None => eof = true,
            }
        }

        if batch.is_empty() {
            break;
        }

        // Wait for the previous write to take back its vector.
        let recycled = match in_flight.take() {
            Some(handle) => handle.await.map_err(io::Error::other)??,
            None => Vec::new(),
        };

        let filled = std::mem::replace(&mut batch, recycled);
        let filled_offset = offset;
        offset += batch_size as u64;
        batch_size = 0;

        let fd = fd.clone();
        in_flight = Some(tokio::task::spawn_blocking(move || {
            let mut chunks = filled;
            write_all_vectored_at(&fd, &chunks, filled_offset)?;

            // Drop the chunks on the blocking thread and hand the emptied
            // vector back for the next batch.
            chunks.clear();
            Ok(chunks)
        }));
    }

    // Wait for the last write to complete.
    if let Some(handle) = in_flight.take() {
        handle.await.map_err(io::Error::other)??;
    }

    if length != expected_length {
        return Err(Error::Unknown(format!(
            "expected length {expected_length} but got {length}"
        )));
    }

    Ok(WriteRangeResponse {
        length,
        hash: hasher.finalize().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_client_util::fs::fd::{FDCache, DEFAULT_FD_CACHE_CAPACITY};
    use std::fs::OpenOptions;
    use std::io::Cursor;
    use tempfile::tempdir;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};
    use tokio_util::io::StreamReader;

    fn pattern(length: usize) -> Vec<u8> {
        (0..length).map(|i| (i % 251) as u8).collect()
    }

    fn buffer_pool() -> BufferPool {
        BufferPool::new(64 * 1024 * 1024)
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

        let mut reader = RangeReader::new(fd, 0, 13, 4, buffer_pool());
        let mut buffer = Vec::new();
        assert!(reader.read_to_end(&mut buffer).await.is_err());

        // Polling again after the error must not panic.
        let mut buffer = Vec::new();
        let _ = reader.read_to_end(&mut buffer).await;
    }

    #[tokio::test]
    async fn test_write_range_write_error() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();

        let fd = Arc::new(File::open(&path).unwrap());
        let data = pattern(16 * 1024);
        assert!(write_range(
            fd.clone(),
            0,
            data.len() as u64,
            4,
            &mut data.as_slice(),
            &buffer_pool()
        )
        .await
        .is_err());

        let data = b"hello, world!";
        assert!(write_range(
            fd,
            0,
            data.len() as u64,
            512,
            &mut data.as_slice(),
            &buffer_pool()
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn test_write_range_then_range_reader_shared_cache() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"").await.unwrap();

        let cache = FDCache::new(DEFAULT_FD_CACHE_CAPACITY);

        let data = pattern(16 * 1024);
        write_range(
            cache.open_write(&path).await.unwrap(),
            0,
            data.len() as u64,
            4 * 1024,
            &mut data.as_slice(),
            &buffer_pool(),
        )
        .await
        .unwrap();

        let mut reader = RangeReader::new(
            cache.open(&path).await.unwrap(),
            0,
            data.len() as u64,
            4 * 1024,
            buffer_pool(),
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

        let mut reader = RangeReader::new(fd.clone(), 0, 13, 512, buffer_pool());
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, b"hello, world!");

        let mut reader = RangeReader::new(fd.clone(), 7, 5, 2, buffer_pool());
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, b"world");

        let mut reader = RangeReader::new(fd.clone(), 7, 100, 512, buffer_pool());
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, b"world!");

        let mut reader = RangeReader::new(fd, 0, 0, 512, buffer_pool());
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

        let mut reader =
            RangeReader::new(fd.clone(), 0, data.len() as u64, 4 * 1024, buffer_pool());
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, data);

        let mut reader = RangeReader::new(fd, 12_345, 30_000, 4 * 1024, buffer_pool());
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
        let reader = RangeReader::new(fd.clone(), 2, 11, 5, buffer_pool());
        let (parts_fd, offset, remaining) = reader.into_parts();
        assert!(Arc::ptr_eq(&parts_fd, &fd));
        assert_eq!(offset, 2);
        assert_eq!(remaining, 11);

        // A partially consumed reader accounts for the buffered data.
        let mut reader = RangeReader::new(fd, 2, 11, 5, buffer_pool());
        assert_eq!(reader.fill_buf().await.unwrap(), b"llo, ");
        reader.consume(2);
        let (_, offset, remaining) = reader.into_parts();
        assert_eq!(offset, 4);
        assert_eq!(remaining, 9);
    }

    #[tokio::test]
    async fn test_range_reader_read_chunk() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        let data = pattern(64 * 1024);
        tokio::fs::write(&path, &data).await.unwrap();
        let fd = Arc::new(File::open(&path).unwrap());

        // Read the full range in owned chunks.
        let mut reader =
            RangeReader::new(fd.clone(), 0, data.len() as u64, 4 * 1024, buffer_pool());
        let mut buffer = Vec::new();
        loop {
            let chunk = reader.read_chunk().await.unwrap();
            if chunk.is_empty() {
                break;
            }
            buffer.extend_from_slice(&chunk);
        }
        assert_eq!(buffer, data);

        // Stop at the end of the file even if the range is longer.
        let mut reader =
            RangeReader::new(fd.clone(), 60 * 1024, 100 * 1024, 4 * 1024, buffer_pool());
        let mut buffer = Vec::new();
        loop {
            let chunk = reader.read_chunk().await.unwrap();
            if chunk.is_empty() {
                break;
            }
            buffer.extend_from_slice(&chunk);
        }
        assert_eq!(buffer, &data[60 * 1024..]);

        // A zero-length range returns an empty chunk.
        let mut reader = RangeReader::new(fd, 0, 0, 4 * 1024, buffer_pool());
        assert!(reader.read_chunk().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_range_reader_read_chunk_after_fill_buf() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();
        let fd = Arc::new(File::open(&path).unwrap());

        // The data buffered by the buffered read interface is drained first.
        let mut reader = RangeReader::new(fd, 0, 13, 5, buffer_pool());
        assert_eq!(reader.fill_buf().await.unwrap(), b"hello");
        reader.consume(2);
        assert_eq!(&reader.read_chunk().await.unwrap()[..], b"llo");
        assert_eq!(&reader.read_chunk().await.unwrap()[..], b", wor");
        assert_eq!(&reader.read_chunk().await.unwrap()[..], b"ld!");
        assert!(reader.read_chunk().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_range_reader_read_chunk_error() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();

        // Reading from a write-only file descriptor must surface the error.
        let fd = Arc::new(
            OpenOptions::new()
                .truncate(false)
                .write(true)
                .open(&path)
                .unwrap(),
        );
        let mut reader = RangeReader::new(fd, 0, 13, 4, buffer_pool());
        assert!(reader.read_chunk().await.is_err());
    }

    #[tokio::test]
    async fn test_range_reader_fill_buf_and_consume() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();
        let fd = Arc::new(File::open(&path).unwrap());

        let mut reader = RangeReader::new(fd, 0, 13, 5, buffer_pool());
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

        let mut reader = RangeReader::new(fd, 1_000, 50_000, 8 * 1024, buffer_pool());
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
                    let mut reader =
                        RangeReader::new(fd, i * range_length, range_length, 1024, buffer_pool());
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
            let mut reader = RangeReader::new(fd.clone(), 0, 13, buffer_size, buffer_pool());
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

        let mut reader = RangeReader::new(fd.clone(), 13, 5, 512, buffer_pool());
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert!(buffer.is_empty());

        let mut reader = RangeReader::new(fd, 100, 5, 512, buffer_pool());
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

        let mut reader = RangeReader::new(fd, 0, 13, 512, buffer_pool());
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
    async fn test_write_range() {
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

        let data = b"hello, world!";
        let response = write_range(
            fd,
            0,
            data.len() as u64,
            512,
            &mut data.as_slice(),
            &buffer_pool(),
        )
        .await
        .unwrap();
        assert_eq!(response.length, data.len() as u64);
        assert_eq!(response.hash, crc32fast::hash(data).to_string());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_write_range_multiple_fills() {
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
        let response = write_range(
            fd,
            0,
            data.len() as u64,
            4 * 1024,
            &mut data.as_slice(),
            &buffer_pool(),
        )
        .await
        .unwrap();
        assert_eq!(response.length, data.len() as u64);
        assert_eq!(response.hash, crc32fast::hash(&data).to_string());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_write_range_offset() {
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

        write_range(fd, 7, 5, 2, &mut b"world".as_slice(), &buffer_pool())
            .await
            .unwrap();

        let content = tokio::fs::read(&path).await.unwrap();
        assert_eq!(&content[7..12], b"world");
        assert_eq!(content.len(), 64);
    }

    #[tokio::test]
    async fn test_write_range_length_mismatch() {
        let temp_dir = tempdir().unwrap();
        let data = pattern(1024);

        // The reader is shorter than the expected length.
        let path = temp_dir.path().join("short");
        tokio::fs::write(&path, b"").await.unwrap();
        let fd = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );
        assert!(
            write_range(fd, 0, 2048, 512, &mut data.as_slice(), &buffer_pool())
                .await
                .is_err()
        );

        let path = temp_dir.path().join("long");
        tokio::fs::write(&path, b"").await.unwrap();
        let fd = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );
        let response = write_range(fd, 0, 512, 512, &mut data.as_slice(), &buffer_pool())
            .await
            .unwrap();
        assert_eq!(response.length, 512);
        assert_eq!(response.hash, crc32fast::hash(&data[..512]).to_string());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), &data[..512]);
    }

    #[tokio::test]
    async fn test_write_range_reader_error() {
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

        let mut reader = StreamReader::new(futures::stream::iter(vec![
            Ok(Bytes::from_static(b"hello")),
            Err(io::Error::other("reader failed")),
        ]));
        assert!(write_range(fd, 0, 10, 512, &mut reader, &buffer_pool())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_write_range_buffer_sizes() {
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

            let data = b"hello, world!";
            let response = write_range(
                fd,
                0,
                data.len() as u64,
                buffer_size,
                &mut data.as_slice(),
                &buffer_pool(),
            )
            .await
            .unwrap();
            assert_eq!(response.hash, crc32fast::hash(data).to_string());
            assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
        }
    }

    #[tokio::test]
    async fn test_write_range_zero_expected_length() {
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

        let data = b"hello, world!";
        let response = write_range(fd, 0, 0, 512, &mut data.as_slice(), &buffer_pool())
            .await
            .unwrap();
        assert_eq!(response.length, 0);
        assert_eq!(response.hash, crc32fast::hash(b"").to_string());
        assert!(tokio::fs::read(&path).await.unwrap().is_empty());
    }

    fn chunk_stream(
        data: Vec<u8>,
        chunk_size: usize,
    ) -> impl Stream<Item = io::Result<Bytes>> + Unpin {
        futures::stream::iter(
            data.chunks(chunk_size)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                .collect::<Vec<_>>(),
        )
    }

    #[tokio::test]
    async fn test_write_range_from_stream() {
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

        let data = b"hello, world!";
        let mut stream = chunk_stream(data.to_vec(), 5);
        let response = write_range_from_stream(fd, 0, data.len() as u64, 512, &mut stream)
            .await
            .unwrap();
        assert_eq!(response.length, data.len() as u64);
        assert_eq!(response.hash, crc32fast::hash(data).to_string());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_write_range_from_stream_multiple_batches() {
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

        // The chunk size is not aligned to the batch size.
        let data = pattern(256 * 1024);
        let mut stream = chunk_stream(data.clone(), 1000);
        let response = write_range_from_stream(fd, 0, data.len() as u64, 4 * 1024, &mut stream)
            .await
            .unwrap();
        assert_eq!(response.length, data.len() as u64);
        assert_eq!(response.hash, crc32fast::hash(&data).to_string());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_write_range_from_stream_offset() {
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

        let mut stream = chunk_stream(b"world".to_vec(), 2);
        write_range_from_stream(fd, 7, 5, 2, &mut stream)
            .await
            .unwrap();

        let content = tokio::fs::read(&path).await.unwrap();
        assert_eq!(&content[7..12], b"world");
        assert_eq!(content.len(), 64);
    }

    #[tokio::test]
    async fn test_write_range_from_stream_length_mismatch() {
        let temp_dir = tempdir().unwrap();
        let data = pattern(1024);
        let path = temp_dir.path().join("short");
        tokio::fs::write(&path, b"").await.unwrap();
        let fd = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );
        let mut stream = chunk_stream(data.clone(), 100);
        assert!(write_range_from_stream(fd, 0, 2048, 512, &mut stream)
            .await
            .is_err());

        let path = temp_dir.path().join("long");
        tokio::fs::write(&path, b"").await.unwrap();
        let fd = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );
        let mut stream = chunk_stream(data.clone(), 100);
        let response = write_range_from_stream(fd, 0, 512, 512, &mut stream)
            .await
            .unwrap();
        assert_eq!(response.length, 512);
        assert_eq!(response.hash, crc32fast::hash(&data[..512]).to_string());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), &data[..512]);
    }

    #[tokio::test]
    async fn test_write_range_from_stream_empty_chunks() {
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

        let data = b"hello, world!";
        let mut stream = futures::stream::iter(vec![
            Ok(Bytes::new()),
            Ok(Bytes::from_static(b"hello")),
            Ok(Bytes::new()),
            Ok(Bytes::from_static(b", world!")),
        ]);
        let response = write_range_from_stream(fd, 0, data.len() as u64, 512, &mut stream)
            .await
            .unwrap();
        assert_eq!(response.length, data.len() as u64);
        assert_eq!(response.hash, crc32fast::hash(data).to_string());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_write_range_from_stream_exceeds_max_iovecs() {
        const MAX_WRITE_IOVECS: usize = 1024;

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

        let data = pattern(2 * MAX_WRITE_IOVECS + 500);
        let mut stream = chunk_stream(data.clone(), 1);
        let response = write_range_from_stream(fd, 0, data.len() as u64, data.len(), &mut stream)
            .await
            .unwrap();
        assert_eq!(response.length, data.len() as u64);
        assert_eq!(response.hash, crc32fast::hash(&data).to_string());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_write_range_from_stream_stream_error() {
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

        let mut stream = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"hello")),
            Err(io::Error::other("stream failed")),
        ]);
        assert!(write_range_from_stream(fd, 0, 10, 512, &mut stream)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_write_range_from_stream_stream_error_with_in_flight_write() {
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

        let mut stream = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"hello")),
            Err(io::Error::other("stream failed")),
        ]);
        assert!(write_range_from_stream(fd, 0, 10, 4, &mut stream)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_write_range_from_stream_write_error() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();

        let fd = Arc::new(File::open(&path).unwrap());
        let data = pattern(16 * 1024);
        let mut stream = chunk_stream(data.clone(), 512);
        assert!(
            write_range_from_stream(fd.clone(), 0, data.len() as u64, 4, &mut stream)
                .await
                .is_err()
        );

        let data = b"hello, world!";
        let mut stream = chunk_stream(data.to_vec(), 5);
        assert!(
            write_range_from_stream(fd, 0, data.len() as u64, 512, &mut stream)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_write_range_from_stream_buffer_sizes() {
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

            let data = b"hello, world!";
            let mut stream = chunk_stream(data.to_vec(), 3);
            let response =
                write_range_from_stream(fd, 0, data.len() as u64, buffer_size, &mut stream)
                    .await
                    .unwrap();
            assert_eq!(response.hash, crc32fast::hash(data).to_string());
            assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
        }
    }

    #[tokio::test]
    async fn test_write_range_from_stream_zero_expected_length() {
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

        let mut stream = chunk_stream(b"hello, world!".to_vec(), 5);
        let response = write_range_from_stream(fd, 0, 0, 512, &mut stream)
            .await
            .unwrap();
        assert_eq!(response.length, 0);
        assert_eq!(response.hash, crc32fast::hash(b"").to_string());
        assert!(tokio::fs::read(&path).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_write_range_concurrent_writers() {
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
                    write_range(
                        fd,
                        (i * range_length) as u64,
                        piece.len() as u64,
                        1024,
                        &mut piece.as_slice(),
                        &buffer_pool(),
                    )
                    .await
                    .unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
    }
}
