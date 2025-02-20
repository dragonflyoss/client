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
        match reader.read_to_end(&mut buffer).await {
            Ok(_) => {
                pieces.push(piece_id.to_string(), Bytes::from(buffer));
                Ok(())
            }
            Err(err) => Err(Error::Unknown(format!(
                "Failed to read piece data for {}: {}",
                piece_id, err
            ))),
        }
    }

    /// contains_piece checks whether the piece exists in the cache.
    pub async fn contains_piece(&self, id: &str) -> bool {
        let pieces = self.pieces.read().await;
        pieces.contains(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new() {
        // Test case which is a tuple consists of the capacity and the expected result.
        let test_cases = vec![
            (1, Ok(1)),
            (10, Ok(10)),
            (1000, Ok(1000)),
            (0, Err(Error::InvalidParameter)),
        ];

        for (capacity, expected_result) in test_cases {
            let result = Cache::new(capacity);

            match expected_result {
                Ok(expected_capacity) => {
                    assert!(result.is_ok());
                    if let Ok(cache) = result {
                        let pieces = cache.pieces.read().await;
                        assert_eq!(pieces.cap().get(), expected_capacity);
                    }
                }
                Err(_err) => {
                    assert!(result.is_err());

                    if let Err(err) = result {
                        assert!(matches!(err, Error::InvalidParameter));
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_contains_piece() {
        let capacity = 5;
        let cache = Cache::new(capacity).unwrap();

        // Test case which is a tuple consists of piece id, piece content and expected result.
        let test_cases = vec![
            ("piece_1", Bytes::from("data 1"), false),
            ("piece_2", Bytes::from("data 2"), true),
            ("piece_3", Bytes::from("data 3"), false),
            ("piece_4", Bytes::from("data 4"), true),
            ("piece_5", Bytes::from("data 5"), true),
            ("piece_6", Bytes::from("data 6"), true),
            ("piece_7", Bytes::from("data 7"), true),
        ];

        // Insert six pieces into a cache with a capacity of 5, and piece_1 will be evicted due to LRU,
        // lru list: [piece_6, piece_5, piece_4, piece_3, piece_2].
        for (piece_id, piece_content, _) in test_cases[0..6].iter() {
            cache
                .pieces
                .write()
                .await
                .push(piece_id.to_string(), piece_content.clone());
        }

        // Read piece_2, lru list: [piece_2, piece_6, piece_5, piece_4, piece_3].
        let _ = cache.pieces.write().await.get("piece_2");

        // Write piece_7, and piece_3 will be evicted due to LRU,
        // lru list: [piece_7, piece_2, piece_6, piece_5, piece_4].
        let (piece_id, piece_content, _) = &test_cases[6];
        cache
            .pieces
            .write()
            .await
            .push(piece_id.to_string(), piece_content.clone());

        for (piece_id, _, expected_existence) in test_cases {
            let exists = cache.contains_piece(piece_id).await;
            assert_eq!(exists, expected_existence);
        }
    }

    #[tokio::test]
    async fn test_read_piece() {
        let capacity = 5;
        let cache = Cache::new(capacity).unwrap();

        cache
            .pieces
            .write()
            .await
            .push("piece_1".to_string(), Bytes::from("data 1"));

        // Test case which is a tuple consists of piece id, offset, length, range and expected result.
        let test_cases = vec![
            // Case 1: No range, read the whole piece
            ("piece_1", 0, 4, None, Ok(Bytes::from("data"))),
            // Case 2: Range that matches the whole piece
            (
                "piece_1",
                0,
                4,
                Some(Range {
                    start: 0,
                    length: 4,
                }),
                Ok(Bytes::from("data")),
            ),
            // Case 3: Range starting at position 2, length = 1
            (
                "piece_1",
                0,
                4,
                Some(Range {
                    start: 2,
                    length: 1,
                }),
                Ok(Bytes::from("t")),
            ),
            // Case 4: Range starts at 0 and length is 2
            (
                "piece_1",
                0,
                4,
                Some(Range {
                    start: 0,
                    length: 2,
                }),
                Ok(Bytes::from("da")),
            ),
            // Case 5: Range starts at the end, length = 1
            (
                "piece_1",
                0,
                4,
                Some(Range {
                    start: 3,
                    length: 1,
                }),
                Ok(Bytes::from("a")),
            ),
            // Case 6: Range length exceeding piece bounds
            (
                "piece_1",
                0,
                4,
                Some(Range {
                    start: 2,
                    length: 3,
                }),
                Ok(Bytes::from("ta")),
            ),
            // Case 7: Piece not found in cache
            (
                "piece_2",
                0,
                4,
                None,
                Err(Error::PieceNotFound("piece_2".to_string())),
            ),
        ];

        for (piece_id, offset, length, range, expected_result) in test_cases {
            let result = cache.read_piece(piece_id, offset, length, range).await;

            match expected_result {
                Ok(expected_data) => {
                    assert!(result.is_ok());
                    let mut reader = result.unwrap();
                    let mut buf = Vec::new();
                    reader.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, expected_data);
                }
                Err(expected_error) => {
                    assert!(result.is_err(), "Expected error, but got success");
                    if let Err(err) = result {
                        match expected_error {
                            Error::PieceNotFound(_id) => {
                                assert!(matches!(err, Error::PieceNotFound(_piece_id)));
                            }
                            Error::InvalidParameter => {
                                assert!(matches!(err, Error::InvalidParameter));
                            }
                            _ => panic!("Unexpected error type"),
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_write_piece() {
        let capacity = 5;
        let cache = Cache::new(capacity).unwrap();

        // Test case which is a tuple consists of piece id, piece content, length, range and expected result.
        let test_cases = vec![
            // Case 1: Write a new piece
            (
                "piece_1",
                "test data",
                9,
                Ok(()),
                Some(Bytes::from("test data")),
            ),
            // Case 2: Try to overwrite an existing piece
            (
                "piece_1",
                "new data",
                8,
                Err(Error::Unknown(
                    "overwrite existing piece piece_1".to_string(),
                )),
                None,
            ),
        ];

        for (piece_id, content, length, expected_result, expected_data) in test_cases {
            let result = {
                let reader = &mut Cursor::new(Bytes::from(content));
                cache.write_piece(piece_id, reader, length).await
            };

            match expected_result {
                Ok(_) => {
                    assert!(result.is_ok());
                    let pieces = cache.pieces.read().await;
                    assert!(pieces.contains(piece_id));

                    if let Some(expected_data) = expected_data {
                        let actual_data = pieces.peek(piece_id).unwrap();
                        assert_eq!(&*actual_data, &expected_data,);
                    }
                }
                Err(expected_error) => {
                    assert!(result.is_err());
                    if let Err(err) = result {
                        match expected_error {
                            Error::Unknown(_id) => {
                                assert!(matches!(err, Error::Unknown(_)));
                            }
                            _ => panic!("Unexpected error type"),
                        }
                    }
                }
            }
        }
    }
}
