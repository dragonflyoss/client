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
use dragonfly_client_config::dfdaemon::{Config, WritebackMode};
use dragonfly_client_core::Result;
use dragonfly_client_util::fs::sync_file_range;
use std::cmp::{max, min};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{trace, warn};

#[cfg(target_os = "linux")]
pub type Content = super::content_linux::Content;

#[cfg(target_os = "macos")]
pub type Content = super::content_macos::Content;

/// The default directory for store content.
pub const DEFAULT_CONTENT_DIR: &str = "content";

/// The default directory for store task.
pub const DEFAULT_TASK_DIR: &str = "tasks";

/// The default directory for store persistent task.
pub const DEFAULT_PERSISTENT_TASK_DIR: &str = "persistent-tasks";

/// The default directory for store persistent cache task.
pub const DEFAULT_PERSISTENT_CACHE_TASK_DIR: &str = "persistent-cache-tasks";

/// The maximum number of idle buffers retained by the buffer pool of the
/// content, multiplied by the largest configured buffer size to size the pool.
pub const MAX_BUFFER_POOL_IDLE_BUFFERS: usize = 128;

/// The capacity of the background writeback queue, roughly the ranges a
/// congested disk drains within the kernel dirty expire window. A full
/// queue drops further ranges and the kernel writeback covers them.
const WRITEBACK_QUEUE_CAPACITY: usize = 1024;

/// Writeback initiates writeback of written piece ranges according to the
/// storage.writebackMode configuration.
pub enum Writeback {
    /// Awaits sync_file_range on the write path.
    Sync,

    /// Sends written ranges to the background writeback task.
    Async(mpsc::Sender<(Arc<File>, u64, u64)>),

    /// Leaves it to the kernel writeback.
    Off,
}

/// Implements the writeback.
impl Writeback {
    /// Creates a new writeback. In async mode it spawns the background task,
    /// which drains the queue and exits when the last sender drops.
    pub fn new(mode: WritebackMode) -> Self {
        match mode {
            WritebackMode::Sync => Writeback::Sync,
            WritebackMode::Async => {
                let (tx, mut rx) = mpsc::channel::<(Arc<File>, u64, u64)>(WRITEBACK_QUEUE_CAPACITY);
                tokio::spawn(async move {
                    while let Some((fd, offset, length)) = rx.recv().await {
                        sync_file_range(&fd, offset, length)
                            .await
                            .unwrap_or_else(|err| warn!("sync_file_range failed: {}", err));
                    }
                });

                Writeback::Async(tx)
            }
            WritebackMode::Off => Writeback::Off,
        }
    }

    /// Triggers writeback of the written range per the configured mode.
    pub async fn trigger(&self, fd: &Arc<File>, offset: u64, length: u64) {
        match self {
            Writeback::Sync => {
                sync_file_range(fd, offset, length)
                    .await
                    .unwrap_or_else(|err| warn!("sync_file_range failed: {}", err));
            }
            Writeback::Async(tx) => {
                if let Err(err) = tx.try_send((fd.clone(), offset, length)) {
                    trace!("dropped writeback range: {}", err);
                }
            }
            Writeback::Off => {}
        }
    }
}

/// Creates a new Content instance to support linux and macos.
pub async fn new_content(config: Arc<Config>, dir: &Path) -> Result<Content> {
    Content::new(config, dir).await
}

/// Calculates the target offset and length based on the piece range and
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
