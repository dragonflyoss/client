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

use crate::resource::task::Task;
use dragonfly_api::dfdaemon::v2::DownloadTaskRequest;
use dragonfly_client_core::{Error, Result};
use lru::LruCache;
use std::cmp::{max, min};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

/// Cache is the cache for storing http response by LRU algorithm.
#[derive(Clone)]
pub struct Cache {
    /// pieces stores the piece cache data with piece id and value.
    pieces: Arc<Mutex<LruCache<String, bytes::Bytes>>>,

    /// task is the task manager.
    task: Arc<Task>,
}

/// Cache implements the cache for storing http response by LRU algorithm.
impl Cache {
    /// new creates a new cache with the specified capacity.
    pub fn new(capacity: usize, task: Arc<Task>) -> Result<Self> {
        let capacity = NonZeroUsize::new(capacity).ok_or(Error::InvalidParameter)?;
        let pieces = Arc::new(Mutex::new(LruCache::new(capacity)));
        Ok(Cache { pieces, task })
    }

    /// get_by_request gets the content from the cache by the request.
    pub async fn get_by_request(
        &self,
        request: &DownloadTaskRequest,
    ) -> Result<Option<bytes::Bytes>> {
        let Some(download) = &request.download else {
            return Err(Error::InvalidParameter);
        };

        let task_id = self.task.id_generator.task_id(
            &download.url,
            download.tag.as_deref(),
            download.application.as_deref(),
            download.filtered_query_params.clone(),
        )?;

        let Some(task) = self.task.get(&task_id).await? else {
            return Ok(None);
        };

        let (Some(content_length), Some(piece_length)) =
            (task.content_length(), task.piece_length())
        else {
            return Ok(None);
        };

        let range = download.range;
        let interested_pieces =
            self.task
                .piece
                .calculate_interested(piece_length, content_length, range)?;

        // Calculate the content capacity based on the interested pieces and push the content into
        // the bytes.
        let content_capacity = interested_pieces.len() * piece_length as usize;
        let mut content = bytes::BytesMut::with_capacity(content_capacity);
        for interested_piece in interested_pieces {
            let piece_id = self.task.piece.id(&task_id, interested_piece.number);
            let Some(piece_content) = self.get_piece(&piece_id) else {
                return Ok(None);
            };

            // Calculate the target offset and length based on the range.
            let (target_offset, target_length) = if let Some(range) = range {
                let target_offset =
                    max(interested_piece.offset, range.start) - interested_piece.offset;
                let target_length = min(
                    interested_piece.offset + interested_piece.length - 1,
                    range.start + range.length - 1,
                ) - target_offset
                    + 1;
                (target_offset as usize, target_length as usize)
            } else {
                (0, interested_piece.length as usize)
            };

            let piece_content = piece_content.slice(target_offset..target_offset + target_length);
            content.extend_from_slice(&piece_content);
        }

        Ok(Some(content.freeze()))
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
