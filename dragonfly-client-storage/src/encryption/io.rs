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

use super::layout::{PieceLayout, CHUNK_OVERHEAD, NONCE_LEN, TAG_LEN};
use super::TaskCipher;
use crate::io::WriteRangeResponse;
use bytes::{Buf, Bytes, BytesMut};
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::buffer_pool::BufferPool;
use dragonfly_client_util::fs::fd::WriteFD;
use std::cmp::{max, min};
use std::fs::File;
use std::future::Future;
use std::io;
use std::os::unix::fs::FileExt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, ReadBuf};
use tokio::sync::oneshot;
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

    /// The read and the decryption of the batch after the one in the buffer,
    /// started as soon as that batch is delivered, so they overlap the
    /// consumption of it. It owns its own buffer until it completes, which
    /// bounds the memory of a reader to two buffers.
    prefetch: Option<JoinHandle<io::Result<(BytesMut, usize)>>>,

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
            prefetch: None,
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
    /// thread pool into a buffer checked out of the pool.
    fn start_fill(&self) -> JoinHandle<io::Result<(BytesMut, usize)>> {
        let count = min(self.chunks_per_fill, self.end_chunk - self.next_chunk);
        let layout = *self.cipher.layout();
        let (offset, length) = layout.chunks_span(&self.piece, self.next_chunk, count);

        // Check out a buffer with `length` readable bytes without zeroing them,
        // the positional read overwrites all of them.
        let mut buffer = self.buffer_pool.checkout_for_read(length as usize);

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

        // The replaced buffer is fully consumed, return it to the pool for the
        // next prefetch.
        let consumed = std::mem::replace(&mut self.buffer, buffer);
        if consumed.capacity() > 0 {
            self.buffer_pool.give_back(consumed);
        }

        self.pos = min(self.skip, plaintext_length);
        self.filled = min(plaintext_length, self.end.saturating_sub(start) as usize);
        self.skip = 0;
    }

    /// Takes the prefetched batch, or starts reading the next one when there is
    /// none.
    fn take_fill(&mut self) -> JoinHandle<io::Result<(BytesMut, usize)>> {
        match self.prefetch.take() {
            Some(handle) => handle,
            None => self.start_fill(),
        }
    }

    /// Starts reading and decrypting the batch after the one in the buffer, so
    /// it overlaps the consumption of that one.
    fn start_prefetch(&mut self) {
        if self.prefetch.is_none() && !self.is_exhausted() {
            self.prefetch = Some(self.start_fill());
        }
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

        let handle = self.take_fill();
        let (buffer, plaintext_length) = handle.await.map_err(io::Error::other)??;
        self.finish_fill(buffer, plaintext_length);
        self.start_prefetch();

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
        // Abort the fills that have not started, a running one finishes in the
        // background and drops its buffer.
        if let Some(handle) = self.prefetch.take() {
            handle.abort();
        }
        if let EncryptedRangeReaderState::Reading(handle) = &self.state {
            handle.abort();
        }

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

                    let handle = this.take_fill();
                    this.state = EncryptedRangeReaderState::Reading(handle);
                }
                EncryptedRangeReaderState::Reading(handle) => {
                    let result = ready!(Pin::new(handle).poll(cx));
                    this.state = EncryptedRangeReaderState::Idle;
                    let (buffer, plaintext_length) = result.map_err(io::Error::other)??;
                    this.finish_fill(buffer, plaintext_length);
                    this.start_prefetch();
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

/// Reads into `dst` until it is full or the reader reaches EOF, returning the
/// number of bytes read.
async fn read_full<R: AsyncRead + Unpin + ?Sized>(
    reader: &mut R,
    dst: &mut [u8],
) -> io::Result<usize> {
    let mut filled = 0;
    while filled < dst.len() {
        let n = reader.read(&mut dst[filled..]).await?;
        if n == 0 {
            break;
        }

        filled += n;
    }

    Ok(filled)
}

/// Writes exactly `expected_length` bytes of plaintext from the reader as the
/// piece at `offset` of the encrypted task, and calculates the CRC32 hash of
/// the plaintext.
///
/// The staging buffer of a batch of chunks has the layout of the ciphertext on
/// disk, a slot of `nonce || plaintext || tag` per chunk. The reader fills the
/// plaintext of each slot on the runtime and hashes it while the data is still
/// hot in the cache of the filling core, then the batch is handed to the
/// blocking thread pool, which seals every chunk in place and writes the whole
/// batch with one positional write. The plaintext is never copied and no
/// second ciphertext buffer is needed, and the fill and the hash of the next
/// batch overlap the encryption and the write of the previous one.
pub async fn write_encrypted_range<R: AsyncRead + Unpin + ?Sized>(
    fd: &WriteFD,
    cipher: Arc<TaskCipher>,
    offset: u64,
    expected_length: u64,
    buffer_size: usize,
    reader: &mut R,
    buffer_pool: &BufferPool,
) -> Result<WriteRangeResponse> {
    let layout = *cipher.layout();
    let piece = layout.piece(offset, expected_length)?;
    let slot_size = (layout.chunk_size + CHUNK_OVERHEAD) as usize;
    let nonce_len = NONCE_LEN as usize;
    let batch_chunks = max(buffer_size as u64 / layout.chunk_size, 1);
    let total_chunks = layout.chunk_count(expected_length);
    let mut reader = reader.take(expected_length);

    // The in-flight encryption and write on the blocking thread pool, which
    // owns its staging buffer until it completes and returns it to the pool.
    // At most one batch is in flight, which bounds the memory of a writer to
    // two staging buffers.
    let mut in_flight: Option<oneshot::Receiver<io::Result<()>>> = None;

    let mut hasher = crc32fast::Hasher::new();
    let mut length: u64 = 0;
    let mut next_chunk: u64 = 0;
    let result: Result<()> = async {
        while next_chunk < total_chunks {
            let count = min(batch_chunks, total_chunks - next_chunk);
            let (disk_offset, disk_length) = layout.chunks_span(&piece, next_chunk, count);
            let mut buffer = buffer_pool.checkout_for_read(disk_length as usize);

            // Fill the plaintext of every slot and hash it while it is hot.
            for index in 0..count {
                let plaintext_length =
                    layout.chunk_length(piece.length, next_chunk + index) as usize;
                let start = index as usize * slot_size + nonce_len;
                let plaintext = &mut buffer[start..start + plaintext_length];
                let read = read_full(&mut reader, plaintext).await?;
                hasher.update(&plaintext[..read]);
                length += read as u64;
                if read < plaintext_length {
                    buffer_pool.give_back(buffer);
                    return Ok(());
                }
            }

            // Wait for the previous batch before starting the next one.
            if let Some(handle) = in_flight.take() {
                handle.await.map_err(io::Error::other)??;
            }

            let cipher = cipher.clone();
            let pool = buffer_pool.clone();
            let first_chunk = next_chunk;
            in_flight = Some(fd.spawn_blocking(move |fd| {
                // Seal every chunk in place, storing its random nonce in front
                // of it and its tag after it.
                for index in 0..count {
                    let plaintext_length =
                        layout.chunk_length(piece.length, first_chunk + index) as usize;
                    let plaintext_offset =
                        layout.chunk_plaintext_offset(&piece, first_chunk + index);
                    let slot = index as usize * slot_size;
                    let tag_start = slot + nonce_len + plaintext_length;

                    let (nonce, tag) = cipher
                        .seal_chunk(plaintext_offset, &mut buffer[slot + nonce_len..tag_start])
                        .map_err(io::Error::other)?;
                    buffer[slot..slot + nonce_len].copy_from_slice(nonce.as_ref());
                    buffer[tag_start..tag_start + TAG_LEN as usize].copy_from_slice(tag.as_ref());
                }

                fd.write_all_at(&buffer, disk_offset)?;
                pool.give_back(buffer);
                Ok(())
            })?);
            next_chunk += count;
        }

        Ok(())
    }
    .await;

    // Always wait for the last batch, so no write of this call is still in
    // flight when it returns, e.g. racing the retry of the same piece.
    if let Some(handle) = in_flight.take() {
        handle.await.map_err(io::Error::other)??;
    }
    result?;

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
