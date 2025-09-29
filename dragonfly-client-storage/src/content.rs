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

use dragonfly_api::common::v2::Range;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Result;
use std::alloc::{alloc, Layout};
use std::cmp::{max, min};
use std::io::{self, ErrorKind};
use std::path::Path;
use std::sync::Arc;
use tokio_uring::buf::{IoBuf, IoBufMut};

#[cfg(target_os = "linux")]
pub type Content = super::content_linux::Content;

#[cfg(target_os = "macos")]
pub type Content = super::content_macos::Content;

/// DEFAULT_CONTENT_DIR is the default directory for store content.
pub const DEFAULT_CONTENT_DIR: &str = "content";

/// DEFAULT_TASK_DIR is the default directory for store task.
pub const DEFAULT_TASK_DIR: &str = "tasks";

/// DEFAULT_PERSISTENT_CACHE_TASK_DIR is the default directory for store persistent cache task.
pub const DEFAULT_PERSISTENT_CACHE_TASK_DIR: &str = "persistent-cache-tasks";

/// WritePieceResponse is the response of writing a piece.
pub struct WritePieceResponse {
    /// length is the length of the piece.
    pub length: u64,

    /// hash is the hash of the piece.
    pub hash: String,
}

/// WritePersistentCacheTaskResponse is the response of writing a persistent cache task.
pub struct WritePersistentCacheTaskResponse {
    /// length is the length of the persistent cache task.
    pub length: u64,

    /// hash is the hash of the persistent cache task.
    pub hash: String,
}

/// new_content creates a new Content instance to support linux and macos.
pub async fn new_content(config: Arc<Config>, dir: &Path) -> Result<Content> {
    Content::new(config, dir).await
}

/// calculate_piece_range calculates the target offset and length based on the piece range and
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

/// A struct to hold aligned memory
pub struct AlignedBuffer {
    ptr: *mut u8,
    size: usize,
    init: usize, // Tracks initialized length
}

impl AlignedBuffer {
    /// Create a new aligned buffer
    pub fn new(size: usize, alignment: usize) -> io::Result<Self> {
        // Ensure alignment is a power of two
        if !alignment.is_power_of_two() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Alignment must be a power of two",
            ));
        }

        // Allocate aligned memory
        unsafe {
            let layout = Layout::from_size_align(size, alignment).map_err(|_| {
                io::Error::new(ErrorKind::InvalidInput, "Invalid size or alignment")
            })?;
            let ptr = alloc(layout);

            if ptr.is_null() {
                return Err(io::Error::new(
                    ErrorKind::OutOfMemory,
                    "Failed to allocate memory",
                ));
            }

            Ok(Self { ptr, size, init: 0 })
        }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            let layout = Layout::from_size_align(self.size, 512).unwrap();
            std::alloc::dealloc(self.ptr, layout);
        }
    }
}

impl std::ops::Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }
}

impl std::ops::DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }
}

/// Implement `IoBuf` for `AlignedBuffer`
unsafe impl IoBuf for AlignedBuffer {
    /// Return the pointer to the start of the buffer
    fn stable_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// Return the number of initialized bytes
    fn bytes_init(&self) -> usize {
        self.init
    }

    /// Return the total size of the buffer
    fn bytes_total(&self) -> usize {
        self.size
    }
}

/// Implement `IoBufMut` for `AlignedBuffer`
unsafe impl IoBufMut for AlignedBuffer {
    /// Return the pointer to the start of the buffer
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    /// Set the initialized length of the buffer
    unsafe fn set_init(&mut self, init: usize) {
        assert!(init <= self.size, "init length exceeds buffer size");
        self.init = init;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
