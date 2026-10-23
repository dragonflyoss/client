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

use super::layout::{PieceLayout, CHUNK_OVERHEAD, NONCE_LEN};
use super::TaskCipher;
use crate::io::WriteRangeResponse;
use bytes::{Buf, Bytes, BytesMut};
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::buffer_pool::BufferPool;
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

/// The state of the in-flight read of the EncryptedRangeReader.
enum EncryptedRangeReaderState {
    /// No read is in flight, the buffer may hold unread plaintext.
    Idle,

    /// A positional read and the decryption of the chunks are running on the
    /// blocking thread pool, owning the buffer until they complete.
    Reading(JoinHandle<io::Result<(BytesMut, usize)>>),
}

/// EncryptedRangeReader reads a plaintext range of a piece of an encrypted
/// task with positional reads on a shared file descriptor, decrypting the
/// chunks covering the range a batch at a time on the blocking thread pool.
/// It never seeks the descriptor and fills a single reusable buffer, like the
/// plaintext range reader.
pub struct EncryptedRangeReader {
    /// The shared file descriptor to read from.
    fd: Arc<File>,

    /// The cipher of the task.
    cipher: Arc<TaskCipher>,

    /// The piece the range belongs to.
    piece: PieceLayout,

    /// The index of the next chunk of the piece to decrypt.
    next_chunk: u64,

    /// The index of the chunk after the last chunk covering the range.
    end_chunk: u64,

    /// The number of bytes of the first decrypted chunk before the start of
    /// the range, skipped once.
    skip: usize,

    /// The start of the range in the plaintext of the content.
    start: u64,

    /// The end of the range in the plaintext of the content.
    end: u64,

    /// The number of chunks decrypted per fill.
    chunks_per_fill: u64,

    /// The reusable buffer, holding the ciphertext of the chunks of a fill and
    /// then their plaintext, allocated on the first fill and returned to the
    /// buffer pool on drop.
    buffer: BytesMut,

    /// The consumed position of the buffer.
    pos: usize,

    /// The filled length of the buffer.
    filled: usize,

    /// The state of the in-flight read.
    state: EncryptedRangeReaderState,

    /// The buffer pool the buffers are checked out from.
    buffer_pool: BufferPool,
}

/// Implements the encrypted range reader.
impl EncryptedRangeReader {
    /// Creates a new EncryptedRangeReader reading the plaintext range
    /// `[offset, offset + length)` of the piece at `piece_offset` with
    /// `piece_length` bytes.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fd: Arc<File>,
        cipher: Arc<TaskCipher>,
        piece_offset: u64,
        piece_length: u64,
        offset: u64,
        length: u64,
        buffer_size: usize,
        buffer_pool: BufferPool,
    ) -> Result<Self> {
        let layout = *cipher.layout();
        let piece = layout.piece(piece_offset, piece_length)?;
        let end = offset.checked_add(length).ok_or(Error::InvalidParameter)?;
        if offset < piece.offset || end > piece.offset + piece.length {
            return Err(Error::InvalidParameter);
        }

        let (first_chunk, end_chunk, skip) = if length == 0 {
            (0, 0, 0)
        } else {
            let first_chunk = (offset - piece.offset) / layout.chunk_size;
            let end_chunk = (end - 1 - piece.offset) / layout.chunk_size + 1;
            let skip = offset - layout.chunk_plaintext_offset(&piece, first_chunk);
            (first_chunk, end_chunk, skip as usize)
        };

        Ok(Self {
            fd,
            cipher,
            piece,
            next_chunk: first_chunk,
            end_chunk,
            skip,
            start: offset,
            end,
            chunks_per_fill: max(buffer_size as u64 / layout.chunk_size, 1),
            buffer: BytesMut::new(),
            pos: 0,
            filled: 0,
            state: EncryptedRangeReaderState::Idle,
            buffer_pool,
        })
    }

    /// Returns the offset and the length on disk of the ciphertext covering the
    /// rest of the range, e.g. to queue readahead of it.
    pub fn ciphertext_span(&self) -> (u64, u64) {
        if self.next_chunk >= self.end_chunk {
            return (0, 0);
        }

        self.cipher.layout().chunks_span(
            &self.piece,
            self.next_chunk,
            self.end_chunk - self.next_chunk,
        )
    }

    /// Returns the number of bytes left in the range: the buffered plaintext
    /// not consumed yet and the plaintext of the chunks not decrypted yet.
    pub fn remaining(&self) -> u64 {
        let buffered = (self.filled - self.pos) as u64;
        if self.is_exhausted() {
            return buffered;
        }

        let next = self
            .cipher
            .layout()
            .chunk_plaintext_offset(&self.piece, self.next_chunk)
            .max(self.start);
        buffered + self.end.saturating_sub(next)
    }

    /// Returns whether all the chunks of the range are decrypted.
    fn is_exhausted(&self) -> bool {
        self.next_chunk >= self.end_chunk
    }

    /// Starts reading and decrypting the next batch of chunks on the blocking
    /// thread pool, reusing `buffer` when it has a capacity.
    fn start_fill(&self, buffer: BytesMut) -> JoinHandle<io::Result<(BytesMut, usize)>> {
        let count = min(self.chunks_per_fill, self.end_chunk - self.next_chunk);
        let layout = *self.cipher.layout();
        let (offset, length) = layout.chunks_span(&self.piece, self.next_chunk, count);

        let mut buffer = if buffer.capacity() >= length as usize {
            buffer
        } else {
            self.buffer_pool.checkout(length as usize)
        };
        buffer.resize(length as usize, 0);

        let fd = self.fd.clone();
        let cipher = self.cipher.clone();
        let piece = self.piece;
        let first = self.next_chunk;
        tokio::task::spawn_blocking(move || {
            fd.read_exact_at(&mut buffer, offset)?;

            // Decrypt the chunks in place and compact their plaintext to the
            // front of the buffer, the plaintext of a chunk never overtakes
            // the ciphertext of the next one.
            let mut in_pos = 0;
            let mut out_pos = 0;
            for index in first..first + count {
                let plaintext_length = layout.chunk_length(piece.length, index) as usize;
                let plaintext_offset = layout.chunk_plaintext_offset(&piece, index);
                let nonce_end = in_pos + NONCE_LEN as usize;
                let chunk_end = in_pos + plaintext_length + CHUNK_OVERHEAD as usize;

                let nonce: [u8; NONCE_LEN as usize] = buffer[in_pos..nonce_end]
                    .try_into()
                    .map_err(io::Error::other)?;
                cipher
                    .open_chunk(plaintext_offset, &nonce, &mut buffer[nonce_end..chunk_end])
                    .map_err(io::Error::other)?;

                buffer.copy_within(nonce_end..nonce_end + plaintext_length, out_pos);
                in_pos = chunk_end;
                out_pos += plaintext_length;
            }

            Ok((buffer, out_pos))
        })
    }

    /// Takes the decrypted batch of chunks as the buffer, skipping the bytes
    /// before the start of the range and dropping the bytes after its end.
    fn finish_fill(&mut self, buffer: BytesMut, plaintext_length: usize) {
        let layout = self.cipher.layout();
        let count = min(self.chunks_per_fill, self.end_chunk - self.next_chunk);
        let start = layout.chunk_plaintext_offset(&self.piece, self.next_chunk);
        self.next_chunk += count;

        self.buffer = buffer;
        self.pos = min(self.skip, plaintext_length);
        self.filled = min(plaintext_length, self.end.saturating_sub(start) as usize);
        self.skip = 0;
    }

    /// Reads the next chunk of plaintext of the range with a positional read
    /// and the decryption on the blocking thread pool, directly into the owned
    /// buffer it returns.
    pub async fn read_chunk(&mut self) -> Result<Bytes> {
        // Drain the data buffered by the buffered read interface first.
        if self.pos < self.filled {
            let chunk = Bytes::copy_from_slice(&self.buffer[self.pos..self.filled]);
            self.pos = self.filled;
            return Ok(chunk);
        }

        if self.is_exhausted() {
            return Ok(Bytes::new());
        }

        let buffer = std::mem::take(&mut self.buffer);
        let (buffer, plaintext_length) =
            self.start_fill(buffer).await.map_err(io::Error::other)??;
        self.finish_fill(buffer, plaintext_length);

        let mut buffer = std::mem::take(&mut self.buffer);
        buffer.truncate(self.filled);
        buffer.advance(self.pos);
        self.pos = self.filled;
        Ok(self.buffer_pool.freeze(buffer))
    }
}

/// Implements the drop of the encrypted range reader.
impl Drop for EncryptedRangeReader {
    /// Returns the buffer to the buffer pool. The buffer is empty when no
    /// fill happened or a fill is still in flight, and is skipped in that case.
    fn drop(&mut self) {
        let buffer = std::mem::take(&mut self.buffer);
        if buffer.capacity() > 0 {
            self.buffer_pool.give_back(buffer);
        }
    }
}

/// Implements the buffered read for the encrypted range reader.
impl AsyncBufRead for EncryptedRangeReader {
    /// Polls the buffer to fill it with the plaintext of the next chunks,
    /// returning a slice of the filled buffer.
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                EncryptedRangeReaderState::Idle => {
                    if this.pos < this.filled || this.is_exhausted() {
                        break;
                    }

                    let buffer = std::mem::take(&mut this.buffer);
                    this.state = EncryptedRangeReaderState::Reading(this.start_fill(buffer));
                }
                EncryptedRangeReaderState::Reading(handle) => {
                    let result = ready!(Pin::new(handle).poll(cx));
                    this.state = EncryptedRangeReaderState::Idle;
                    let (buffer, plaintext_length) = result.map_err(io::Error::other)??;
                    this.finish_fill(buffer, plaintext_length);
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

/// Implements the read for the encrypted range reader.
impl AsyncRead for EncryptedRangeReader {
    /// Polls the read to fill the provided buffer with the plaintext of the
    /// range.
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

/// Writes exactly `expected_length` bytes of plaintext from the reader as the
/// piece at `offset` of the encrypted task, and calculates the CRC32 hash of
/// the plaintext. The reader fills a batch of chunks on the runtime and hashes
/// it while the data is still hot in the cache of the filling core, then hands
/// it to the blocking thread pool, which encrypts the chunks and writes their
/// ciphertext with one positional write, swapping the recycled buffer of the
/// completed batch back for the next fill, so the read and the hash overlap the
/// encryption and the write.
pub async fn write_encrypted_range<R: AsyncRead + Unpin + ?Sized>(
    fd: Arc<File>,
    cipher: Arc<TaskCipher>,
    offset: u64,
    expected_length: u64,
    buffer_size: usize,
    reader: &mut R,
    buffer_pool: &BufferPool,
) -> Result<WriteRangeResponse> {
    let layout = *cipher.layout();
    let piece = layout.piece(offset, expected_length)?;
    let chunk_size = layout.chunk_size as usize;
    let batch_size = max(buffer_size / chunk_size, 1) * chunk_size;
    let mut reader = reader.take(expected_length);

    // The buffer being filled from the reader, swapped with the recycled
    // buffer of the completed batch.
    let mut buffer = buffer_pool.checkout(batch_size);

    // The in-flight encryption and write on the blocking thread pool, owning
    // the other buffer until it completes.
    let mut in_flight: Option<JoinHandle<io::Result<BytesMut>>> = None;

    let mut hasher = crc32fast::Hasher::new();
    let mut length: u64 = 0;
    let mut next_chunk: u64 = 0;
    loop {
        // Fill the buffer until it is full or the reader reaches EOF.
        while buffer.len() < batch_size {
            if reader.read_buf(&mut buffer).await? == 0 {
                break;
            }
        }

        if buffer.is_empty() {
            break;
        }

        // Hash the plaintext while its data is still hot in the cache of the
        // core that filled it.
        hasher.update(&buffer);

        // Wait for the previous batch to take back its buffer.
        let recycled = match in_flight.take() {
            Some(handle) => handle.await.map_err(io::Error::other)??,
            None => buffer_pool.checkout(batch_size),
        };

        let filled = std::mem::replace(&mut buffer, recycled);
        length += filled.len() as u64;

        let first_chunk = next_chunk;
        let count = filled.len().div_ceil(chunk_size) as u64;
        next_chunk += count;

        let fd = fd.clone();
        let cipher = cipher.clone();
        let pool = buffer_pool.clone();
        in_flight = Some(tokio::task::spawn_blocking(move || {
            let (ciphertext_offset, ciphertext_length) =
                layout.chunks_span(&piece, first_chunk, count);

            // Encrypt the chunks into the ciphertext buffer, each one as
            // `nonce || ciphertext || tag`.
            let mut ciphertext = pool.checkout(ciphertext_length as usize);
            for (index, chunk) in filled.chunks(chunk_size).enumerate() {
                let plaintext_offset =
                    layout.chunk_plaintext_offset(&piece, first_chunk + index as u64);
                let nonce_start = ciphertext.len();
                let chunk_start = nonce_start + NONCE_LEN as usize;
                ciphertext.resize(chunk_start, 0);
                ciphertext.extend_from_slice(chunk);

                let (nonce, tag) = cipher
                    .seal_chunk(plaintext_offset, &mut ciphertext[chunk_start..])
                    .map_err(io::Error::other)?;
                ciphertext[nonce_start..chunk_start].copy_from_slice(nonce.as_ref());
                ciphertext.extend_from_slice(tag.as_ref());
            }

            fd.write_all_at(&ciphertext, ciphertext_offset)?;
            pool.give_back(ciphertext);

            let mut recycled = filled;
            recycled.clear();
            Ok(recycled)
        }));
    }

    // Wait for the last batch to complete and return the staging buffers to
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
