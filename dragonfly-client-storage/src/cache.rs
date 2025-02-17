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

use bytes::Bytes;
use dragonfly_api::common::v2::Range;
use dragonfly_client_core::{Error, Result};
use lru::LruCache;
use std::cmp::{max, min};
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::RwLock;

/// Cache is the cache for storing piece content by LRU algorithm.
///
/// Cache storage:
/// 1. Users can create preheating jobs and preheat tasks to memory and disk by setting `load_to_cache` to `true`.
///    For more details, refer to https://github.com/dragonflyoss/api/blob/main/proto/common.proto#L443.
/// 2. If the download hits the memory cache, it will be faster than reading from the disk, because there is no
///    page cache for the first read.
/// ```
///
///     1.Preheat
///         |
///         |
/// +--------------------------------------------------+
/// |       |              Peer                        |
/// |       |                   +-----------+          |
/// |       |     -- Partial -->|   Cache   |          |
/// |       |     |             +-----------+          |
/// |       v     |                |    |              |
/// |   Download  |              Miss   |              |             
/// |     Task -->|                |    --- Hit ------>|<-- 2.Download
/// |             |                |               ^   |              
/// |             |                v               |   |
/// |             |          +-----------+         |   |
/// |             -- Full -->|   Disk    |----------   |
/// |                        +-----------+             |
/// |                                                  |
/// +--------------------------------------------------+
/// ```
#[derive(Clone)]
pub struct Cache {
    /// pieces stores the pieces with their piece id and content.
    pieces: Arc<RwLock<LruCache<String, bytes::Bytes>>>,
}

/// Cache implements the cache for storing piece content by LRU algorithm.
impl Cache {
    /// new creates a new cache with the specified capacity.
    pub fn new(capacity: usize) -> Result<Self> {
        let capacity = NonZeroUsize::new(capacity).ok_or(Error::InvalidParameter)?;
        let pieces = Arc::new(RwLock::new(LruCache::new(capacity)));

        Ok(Cache { pieces })
    }

    /// read_piece reads the piece from the cache.
    pub async fn read_piece(
        &self,
        piece_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        // Try to get the piece content from the cache.
        let mut pieces = self.pieces.write().await;
        let Some(piece_content) = pieces.get(piece_id).cloned() else {
            return Err(Error::PieceNotFound(piece_id.to_string()));
        };

        // Calculate the range of bytes to return based on the range provided.
        let (target_offset, target_length) = if let Some(range) = range {
            let target_offset = max(offset, range.start) - offset;
            let target_length =
                min(offset + length - 1, range.start + range.length - 1) - target_offset - offset
                    + 1;
            (target_offset as usize, target_length as usize)
        } else {
            (0, length as usize)
        };

        // Check if the target range is valid.
        let begin = target_offset;
        let end = target_offset + target_length;
        if begin >= piece_content.len() || end > piece_content.len() {
            return Err(Error::InvalidParameter);
        }

        let reader = BufReader::new(Cursor::new(piece_content.slice(begin..end)));
        Ok(reader)
    }

    /// write_piece writes the piece content to the cache.
    pub async fn write_piece<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        reader: &mut R,
        length: u64,
    ) -> Result<()> {
        let mut pieces = self.pieces.write().await;

        // The piece already exists in the cache.
        if pieces.contains(piece_id) {
            return Err(Error::Unknown(format!(
                "overwrite existing piece {}",
                piece_id
            )));
        }

        let mut buffer = Vec::with_capacity(length as usize);
        reader
            .read_to_end(&mut buffer)
            .await
            .map_err(|_err| Error::Unknown(piece_id.to_string()))?;

        pieces.put(piece_id.to_string(), Bytes::from(buffer));
        Ok(())
    }

    /// contains_piece checks whether the piece exists in the cache.
    pub async fn contains_piece(&self, id: &str) -> bool {
        let pieces = self.pieces.read().await;
        pieces.contains(id)
    }
}
