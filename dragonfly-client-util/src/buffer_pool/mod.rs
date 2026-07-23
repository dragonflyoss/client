/*
 *     Copyright 2026 The Dragonfly Authors
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
use std::sync::Mutex;

/// BufferPool is a bounded pool of staging buffers shared by I/O hot paths,
/// so they reuse a few large buffers instead of allocating (and zeroing, on
/// the read side) a fresh one per chunk.
///
/// Invariant: every buffer created by the pool has its full capacity initialized
/// (it is born with [`BytesMut::zeroed`]), and the callers only overwrite the
/// buffers in place and never grow them past their capacity, so the
/// initialization holds across reuses. This is what makes the unchecked
/// `set_len` in [`BufferPool::checkout_for_read`] sound.
pub struct BufferPool {
    /// The idle buffers and their total capacity in bytes.
    idle: Mutex<(Vec<BytesMut>, usize)>,

    /// The maximum total capacity in bytes of the idle buffers.
    max_idle_bytes: usize,
}

/// Implements the buffer pool.
impl BufferPool {
    /// Creates a new buffer pool retaining at most `max_idle_bytes` of idle
    /// buffer capacity.
    pub const fn new(max_idle_bytes: usize) -> Self {
        Self {
            idle: Mutex::new((Vec::new(), 0)),
            max_idle_bytes,
        }
    }

    /// Checks out an empty buffer with at least the given capacity, reusing an
    /// idle buffer when one is available.
    pub fn checkout(&self, capacity: usize) -> BytesMut {
        let mut idle = self.idle.lock().unwrap();
        while let Some(buffer) = idle.0.pop() {
            idle.1 -= buffer.capacity();
            if buffer.capacity() >= capacity {
                return buffer;
            }

            // Drop the undersized buffer, so the pool converges on the largest
            // buffer size in use.
        }
        drop(idle);

        // Zero the new buffer to fully initialize its capacity, upholding the pool
        // invariant. The cost is paid once per buffer creation instead of once per
        // chunk.
        let mut buffer = BytesMut::zeroed(capacity);
        buffer.clear();
        buffer
    }

    /// Checks out a buffer with `len` readable bytes for a positional read to
    /// overwrite, without zeroing them.
    pub fn checkout_for_read(&self, len: usize) -> BytesMut {
        let mut buffer = self.checkout(len);
        // SAFETY: the pool invariant guarantees the full capacity of the buffer is
        // initialized, and `len` is not greater than the capacity.
        unsafe { buffer.set_len(len) };
        buffer
    }

    /// Returns a buffer to the pool, dropping it when the pool is full.
    pub fn give_back(&self, mut buffer: BytesMut) {
        buffer.clear();
        let mut idle = self.idle.lock().unwrap();
        if idle.1 + buffer.capacity() <= self.max_idle_bytes {
            idle.1 += buffer.capacity();
            idle.0.push(buffer);
        }
    }

    /// Freezes a checked-out buffer into a [`Bytes`] handle that returns the
    /// buffer to this pool when the last reference to it drops.
    pub fn freeze(&'static self, buffer: BytesMut) -> Bytes {
        Bytes::from_owner(PooledChunk { pool: self, buffer })
    }
}

/// PooledChunk owns a pooled buffer inside a [`Bytes`] handle created by
/// [`Bytes::from_owner`], returning the buffer to the pool when the last
/// reference to the chunk drops.
struct PooledChunk {
    /// The pool the buffer is returned to on drop.
    pool: &'static BufferPool,

    /// The pooled buffer holding the chunk.
    buffer: BytesMut,
}

/// Implements the byte view of the pooled chunk.
impl AsRef<[u8]> for PooledChunk {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}

/// Implements the drop of the pooled chunk.
impl Drop for PooledChunk {
    fn drop(&mut self) {
        self.pool.give_back(std::mem::take(&mut self.buffer));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_reuse() {
        let pool = BufferPool::new(4096);

        let buffer = pool.checkout(1024);
        assert_eq!(buffer.capacity(), 1024);
        assert!(buffer.is_empty());

        let ptr = buffer.as_ptr();
        pool.give_back(buffer);

        // A smaller request reuses the idle buffer.
        let buffer = pool.checkout(512);
        assert_eq!(buffer.as_ptr(), ptr);
        assert_eq!(buffer.capacity(), 1024);
        pool.give_back(buffer);

        // An undersized idle buffer is dropped and a new one is created.
        let buffer = pool.checkout(2048);
        assert_eq!(buffer.capacity(), 2048);
        assert!(pool.idle.lock().unwrap().0.is_empty());
        pool.give_back(buffer);

        // Buffers beyond the idle capacity are dropped.
        pool.give_back(BytesMut::zeroed(4096));
        let idle = pool.idle.lock().unwrap();
        assert_eq!(idle.0.len(), 1);
        assert_eq!(idle.1, 2048);
    }

    #[test]
    fn test_buffer_pool_checkout_for_read() {
        let pool = BufferPool::new(4096);

        let mut buffer = pool.checkout_for_read(256);
        assert_eq!(buffer.len(), 256);
        buffer.fill(0xAB);
        pool.give_back(buffer);

        // The reused buffer keeps its full capacity initialized and readable.
        let buffer = pool.checkout_for_read(128);
        assert_eq!(buffer.len(), 128);
        assert!(buffer.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn test_buffer_pool_freeze() {
        static POOL: BufferPool = BufferPool::new(4096);

        let mut buffer = POOL.checkout(1024);
        buffer.extend_from_slice(b"hello, world!");
        let ptr = buffer.as_ptr();

        let bytes = POOL.freeze(buffer);
        assert_eq!(&bytes[..], b"hello, world!");

        // Dropping the last reference returns the buffer to the pool.
        drop(bytes);
        let buffer = POOL.checkout(1024);
        assert_eq!(buffer.as_ptr(), ptr);
    }
}
