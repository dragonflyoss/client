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

use dragonfly_client_core::{Error, Result};
use lru::LruCache;
use std::cmp::{max, min};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

/// Cache is the cache for storing piece content by LRU algorithm.
#[derive(Clone)]
pub struct Cache {
    /// pieces stores the piece cache data with piece id and value.
    pieces: Arc<Mutex<LruCache<String, bytes::Bytes>>>,
}

/// Cache implements the cache for storing http response by LRU algorithm.
impl Cache {
    /// new creates a new cache with the specified capacity.
    pub fn new(capacity: usize) -> Result<Self> {
        let capacity = NonZeroUsize::new(capacity).ok_or(Error::InvalidParameter)?;
        let pieces = Arc::new(Mutex::new(LruCache::new(capacity)));
        Ok(Cache { pieces })
    }

    pub async fn upload_piece_from_cache(
        &self,
        piece_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range<u64>>,
    ) -> Result<Option<Vec<u8>>> {
        // Try to get the piece content from the cache
        let Some(piece_content) = self.get_piece(piece_id) else {
            return Ok(None);
        };
    
        // Calculate the range of bytes to return based on the `range` provided
        let (target_offset, target_length) = if let Some(range) = range {
            // If `range` is specified, calculate the target offset and length within the range
            let target_offset = max(offset, range.start);
            let target_length = min(offset + length - 1, range.start + range.length - 1) - target_offset + 1;
            (target_offset, target_length)
        } else {
            // Otherwise, just use the given offset and length
            (offset, length)
        };
    
        // Slice the content to match the required range and return it as a Vec<u8>
        let content_slice = &piece_content[target_offset as usize..(target_offset + target_length) as usize];
        Ok(Some(content_slice.to_vec())) // Convert the slice to a Vec<u8> and wrap it in Some
    }

    /// get gets the piece content from the cache.
    pub fn get_piece(&self, id: &str) -> Option<bytes::Bytes> {
        let mut pieces = self.pieces.lock().unwrap();
        pieces.get(id).cloned()
    }

    /// add create the piece content into the cache, if the key already exists, no operation will
    /// be performed.
    pub fn add_piece(&self, id: &str, content: bytes::Bytes) {
        let mut pieces = self.pieces.lock().unwrap();
        if !pieces.contains(id) {
            pieces.put(id.to_string(), content);
        }
    }
}
