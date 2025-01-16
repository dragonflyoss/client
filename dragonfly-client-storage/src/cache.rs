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

use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use dragonfly_api::common::v2::Range;
use lru::LruCache;
use std::cmp::{max, min};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use std::io::Cursor;

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
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,
    
    /// pieces stores the pieces with their piece id and content.
    pieces: Arc<Mutex<LruCache<String, bytes::Bytes>>>,
}

/// Cache implements the cache for storing piece content by LRU algorithm.
impl Cache {
    /// new creates a new cache with the specified capacity.
    pub fn new(config: Arc<Config>) -> Result<Self> {
        let capacity = NonZeroUsize::new(config.storage.cache_capacity)
                                                    .ok_or(Error::InvalidParameter)?;
        let pieces = Arc::new(Mutex::new(LruCache::new(capacity)));
        
        Ok(Cache { config, pieces })
    }

    /// read_piece reads the piece from the cache.
    pub async fn read_piece(
        &self,
        piece_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        // Try to get the piece content from the cache
        let Some(piece_content) = self.get_piece(piece_id).await else {
            return Err(Error::PieceNotFound(piece_id.to_string()));
        };
    
        // Calculate the range of bytes to return based on the range provided
        let (target_offset, target_length) = if let Some(range) = range {
            let target_offset = max(offset, range.start) - offset; 
            let target_length = 
                min(offset + length - 1, range.start + range.length - 1) - target_offset - offset + 1;
            (target_offset as usize, target_length as usize)
        } else {
            (0, length as usize)
        };

        let begin = target_offset;
        let end = target_offset + target_length;
        if begin >= piece_content.len() || end > piece_content.len() {
            return Err(Error::InvalidParameter);
        }
    
        let piece_content = piece_content.slice(begin..end).to_vec();
        Ok(Cursor::new(piece_content))
    }

    /// write_piece writes the piece content to the cache.
    pub async fn write_piece<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        reader: &mut R,
    ) {
        let content = match self.copy_piece_content(reader).await {
            Ok(piece_content) => piece_content,
            Err(_err) => {
                eprintln!("Failed to write piece content: {}", piece_id);
                return;
            }
        };

        self.add_piece(piece_id, content.clone().into()).await;
    }

    /// copy_piece_content reads the content from the provided asynchronous reader into a vector.
    /// 
    /// ### Purpose
    /// This method is designed to handle scenarios where downloading from source returns 
    /// a stream instead of the piece content directly when downloading from parent peer.
    pub async fn copy_piece_content<R: AsyncRead + Unpin + ?Sized>(
        &self,
        reader: &mut R,
    ) -> Result<Vec<u8>> {
        let mut content = Vec::new();
        let mut buffer = vec![0; self.config.storage.write_buffer_size];
        let mut reader = BufReader::with_capacity(self.config.storage.write_buffer_size, reader);

        loop {
            match reader.read(&mut buffer).await {
                Ok(n) => {
                    if n == 0 {
                        break;
                    }
                    content.extend_from_slice(&buffer[..n]);
                }
                Err(e) => {
                    eprintln!("Failed to read from reader: {}", e);
                }
            }
        }
        Ok(content)
    }

    /// get_piece gets the piece content from the cache.
    pub async fn get_piece(&self, id: &str) -> Option<bytes::Bytes> {
        let mut pieces = self.pieces.lock().unwrap();
        pieces.get(id).cloned()
    }

    /// add_piece add the piece content into the cache, if the key already exists, no operation will
    /// be performed.
    pub async fn add_piece(&self, id: &str, content: bytes::Bytes) {
        let mut pieces = self.pieces.lock().unwrap();
        if !pieces.contains(id) {
            pieces.put(id.to_string(), content);
        }
    }

    /// is_empty checks if the cache is empty.
    pub fn is_empty(&self) -> bool {
        let pieces = self.pieces.lock().unwrap();
        pieces.is_empty()
    }

    /// contains_piece checks whether the piece exists in the cache.
    pub fn contains_piece(&self, id: &str) -> bool {
        let pieces = self.pieces.lock().unwrap();
        pieces.contains(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use dragonfly_client_config::dfdaemon::Config;
    use std::sync::Arc;
    use std::io::Cursor;

    fn create_test_config() -> Arc<Config> {
        Arc::new(Config {
            storage: dragonfly_client_config::dfdaemon::Storage {
                cache_capacity: 10,
                write_buffer_size: 1024,
                ..Default::default()
            },
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn test_cache_creation() {
        let config = create_test_config();
        let cache = Cache::new(config.clone()).expect("Failed to create cache");
        assert!(cache.is_empty(), "Cache should be empty upon creation");
    }

    #[tokio::test]
    async fn test_write_and_read_piece() {
        let config = create_test_config();
        let cache = Cache::new(config.clone()).expect("Failed to create cache");

        let piece_id = "test_piece";
        let content = b"Hello, Dragonfly!".to_vec();
        let mut reader = Cursor::new(content.clone());

        // Write the piece to the cache
        cache.write_piece(piece_id, &mut reader).await;

        // Read the piece from the cache
        let result = cache.read_piece(piece_id, 0, content.len() as u64, None).await;

        assert!(
            result.is_ok(),
            "Failed to read the piece: {:?}",
            result.err()
        );

        let mut read_buffer = Vec::new();
        result.unwrap().read_to_end(&mut read_buffer).await.unwrap();

        assert_eq!(read_buffer, content, "The read content does not match");
    }

    #[tokio::test]
    async fn test_read_piece_with_range() {
        let config = create_test_config();
        let cache = Cache::new(config.clone()).expect("Failed to create cache");

        let piece_id = "test_piece";
        let content = b"0123456789".to_vec();
        let mut reader = Cursor::new(content.clone());

        // Write the piece to the cache
        cache.write_piece(piece_id, &mut reader).await;

        // Read a range from the piece
        let range = Some(Range {
            start: 2,
            length: 5,
        });

        let result = cache.read_piece(piece_id, 0, content.len() as u64, range).await;

        assert!(
            result.is_ok(),
            "Failed to read the piece with range: {:?}",
            result.err()
        );

        let mut read_buffer = Vec::new();
        result.unwrap().read_to_end(&mut read_buffer).await.unwrap();

        assert_eq!(read_buffer, b"23456", "The range read content is incorrect");
    }

    #[tokio::test]
    async fn test_get_and_add_piece() {
        let config = create_test_config();
        let cache = Cache::new(config.clone()).expect("Failed to create cache");

        let piece_id = "test_piece";
        let content = Bytes::from("Test content");

        // Add a piece directly
        cache.add_piece(piece_id, content.clone()).await;

        // Retrieve the piece
        let cached_piece = cache.get_piece(piece_id).await;

        assert!(
            cached_piece.is_some(),
            "The piece should exist in the cache"
        );
        assert_eq!(
            cached_piece.unwrap(),
            content,
            "The cached content does not match"
        );
    }

    #[tokio::test]
    async fn test_cache_eviction() {
        let config = Arc::new(Config {
            storage: dragonfly_client_config::dfdaemon::Storage {
                cache_capacity: 2,
                write_buffer_size: 1024,
                ..Default::default()
            },
            ..Default::default()
        });
    
        let cache = Cache::new(config.clone()).expect("Failed to create cache");
    
        cache.add_piece("piece1", Bytes::from("Content 1")).await;
        cache.add_piece("piece2", Bytes::from("Content 2")).await;
    
        assert!(
            cache.get_piece("piece1").await.is_some(),
            "Piece1 should still exist"
        );
        assert!(
            cache.get_piece("piece2").await.is_some(),
            "Piece2 should still exist"
        );
    
        cache.add_piece("piece3", Bytes::from("Content 3")).await;
    
        assert!(
            cache.get_piece("piece1").await.is_none(),
            "Piece1 should have been evicted"
        );
        assert!(
            cache.get_piece("piece2").await.is_some(),
            "Piece2 should still exist"
        );
        assert!(
            cache.get_piece("piece3").await.is_some(),
            "Piece3 should exist in the cache"
        );
    }    

    #[tokio::test]
async fn test_contains_piece() {
    let config = Arc::new(Config {
        storage: dragonfly_client_config::dfdaemon::Storage {
            cache_capacity: 3,
            write_buffer_size: 1024,
            ..Default::default()
        },
        ..Default::default()
    });

    let cache = Cache::new(config.clone()).expect("Failed to create cache");

    let piece_id1 = "piece1";
    let piece_id2 = "piece2";

    assert!(
        !cache.contains_piece(piece_id1),
        "Cache should not contain piece1 initially"
    );


    cache.add_piece(piece_id1, Bytes::from("Content 1")).await;
    assert!(
        cache.contains_piece(piece_id1),
        "Cache should contain piece1 after it is added"
    );

    assert!(
        !cache.contains_piece(piece_id2),
        "Cache should not contain piece2"
    );


    cache.add_piece(piece_id2, Bytes::from("Content 2")).await;
    assert!(
        cache.contains_piece(piece_id1),
        "Cache should still contain piece1"
    );
    assert!(
        cache.contains_piece(piece_id2),
        "Cache should contain piece2 after it is added"
    );
}

}
