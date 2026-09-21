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

use dragonfly_client_util::container::is_running_in_container;
use dragonfly_client_util::fs::fadvise_dontneed_range;
use dragonfly_client_util::sysinfo::memory::Memory;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, error, info, trace, warn};

/// The cgroup memory usage percent above which the downloaded pieces are
/// dropped, leaving the room above it for the writes between two samples.
const DEFAULT_MEMORY_THRESHOLD_PERCENT: u8 = 80;

/// The maximum number of pieces dropped concurrently, bounding the blocking
/// threads it takes.
const MAX_CONCURRENT_DROP_COUNT: usize = 16;

/// The capacity of the queue of the downloaded pieces waiting for the background task.
const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// Piece is a downloaded piece awaiting its drop from the page cache.
struct Piece {
    /// The piece id.
    id: String,

    /// The path of the task content.
    path: PathBuf,

    /// The offset of the piece in the task content.
    offset: u64,

    /// The length of the piece.
    length: u64,
}

/// PageCache drops the downloaded pieces in download order once the cgroup memory
/// nears its limit, keeping the writers out of the kernel direct reclaim.
pub struct PageCache {
    /// Hands the downloaded pieces to the background task.
    tx: mpsc::Sender<Piece>,

    /// Whether each queued piece was read since its download, granting it a
    /// second chance. Mirrors the queue, so it holds nothing else.
    referenced: Mutex<HashMap<String, bool>>,

    /// Memory sampler used to read the cgroup memory usage and limit.
    memory: Memory,

    /// PID of the current process, used to find its cgroup.
    pid: u32,

    /// Whether the process is running inside a container.
    is_running_in_container: bool,
}

/// Implements the page cache.
impl PageCache {
    /// Creates a new page cache and spawns its background task.
    pub fn new() -> Arc<Self> {
        let (tx, rx) = mpsc::channel(DEFAULT_QUEUE_CAPACITY);
        let page_cache = Arc::new(Self {
            tx,
            referenced: Mutex::new(HashMap::new()),
            memory: Memory::default(),
            pid: std::process::id(),
            is_running_in_container: is_running_in_container(),
        });

        let page_cache_clone = page_cache.clone();
        tokio::spawn(async move {
            page_cache_clone.run(rx).await;
        });

        page_cache
    }

    /// Queues the downloaded piece for dropping.
    pub fn download_piece_finished(&self, id: &str, path: PathBuf, offset: u64, length: u64) {
        if self.tx.is_closed() {
            return;
        }

        // Track the piece before it is readable, so no read of it is missed.
        if let Ok(mut referenced) = self.referenced.lock() {
            referenced.insert(id.to_string(), false);
        }

        let piece = Piece {
            id: id.to_string(),
            path,
            offset,
            length,
        };

        if let Err(err) = self.tx.try_send(piece) {
            trace!("dropped downloaded piece: {}", err);
            if let Ok(mut referenced) = self.referenced.lock() {
                referenced.remove(id);
            }
        }
    }

    /// Marks the queued piece read, granting it a second chance.
    pub fn upload_piece_started(&self, id: &str) {
        if self.tx.is_closed() {
            return;
        }

        if let Ok(mut referenced) = self.referenced.lock() {
            if let Some(referenced) = referenced.get_mut(id) {
                *referenced = true;
            }
        }
    }

    /// Runs the background task until the last sender drops.
    async fn run(&self, mut rx: mpsc::Receiver<Piece>) {
        if !self.is_running_in_container {
            info!("page cache drop disabled outside container");
            return;
        }

        // The cgroup v1 reports an unlimited memory as a huge limit.
        let total = self.memory.get_stats().total;
        let mut limit = match self.memory.get_cgroup_stats(self.pid) {
            Some(stats) if stats.limit > 0 && (stats.limit as u64) < total => stats.limit as u64,
            _ => {
                info!("page cache drop disabled without cgroup memory limit");
                return;
            }
        };

        let mut pieces: VecDeque<Piece> = VecDeque::new();
        let mut pieces_length: u64 = 0;
        let mut downloaded_length: u64 = 0;
        while let Some(piece) = rx.recv().await {
            pieces_length += piece.length;
            downloaded_length += piece.length;
            pieces.push_back(piece);

            // The pieces beyond the limit are reclaimed by the kernel already.
            while pieces_length > limit {
                let Some(piece) = pieces.pop_front() else {
                    break;
                };

                pieces_length -= piece.length;
                if let Ok(mut referenced) = self.referenced.lock() {
                    referenced.remove(&piece.id);
                }
            }

            // Sample every half of the room above the threshold, keeping the
            // downloads between two samples below the limit.
            let threshold = limit / 100 * DEFAULT_MEMORY_THRESHOLD_PERCENT as u64;
            if downloaded_length < (limit - threshold) / 2 {
                continue;
            }

            // Measure, drop and measure again until the usage falls below the
            // threshold, since the queue may hold pieces reclaimed by the kernel already.
            downloaded_length = 0;
            loop {
                let current = match self.memory.get_cgroup_stats(self.pid) {
                    Some(stats) if stats.limit > 0 && (stats.limit as u64) < total => {
                        limit = stats.limit as u64;
                        stats.current
                    }
                    _ => {
                        debug!("cgroup memory stats unavailable, skip dropping page cache");
                        break;
                    }
                };

                // Drop what exceeds the threshold, but keep the room above it in
                // pieces when other memory fills the limit, e.g. the page cache of
                // a previous run, which is older than the pieces and left to the kernel.
                let threshold = limit / 100 * DEFAULT_MEMORY_THRESHOLD_PERCENT as u64;
                let need_drop_length = current
                    .saturating_sub(threshold)
                    .min(pieces_length.saturating_sub(limit - threshold));
                let drop_pieces =
                    Self::select_drop_pieces(&mut pieces, &self.referenced, need_drop_length);
                if drop_pieces.is_empty() {
                    break;
                }

                let mut join_set = JoinSet::new();
                let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_DROP_COUNT));
                for piece in drop_pieces {
                    pieces_length -= piece.length;
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    join_set.spawn(async move {
                        let _permit = permit;
                        Self::drop_piece(piece).await;
                    });
                }

                while let Some(result) = join_set.join_next().await {
                    if let Err(err) = result {
                        error!("drop page cache failed: {}", err);
                    }
                }
            }
        }
    }

    /// Selects the oldest pieces adding up to the length to drop. The pieces read
    /// since their download move to the back for a second chance. The lock is taken
    /// per piece, so the readers never wait for a whole scan.
    fn select_drop_pieces(
        pieces: &mut VecDeque<Piece>,
        referenced: &Mutex<HashMap<String, bool>>,
        need_drop_length: u64,
    ) -> Vec<Piece> {
        let mut drop_pieces = Vec::new();
        let mut drop_length = 0;
        for _ in 0..pieces.len() {
            if drop_length >= need_drop_length {
                break;
            }

            let Ok(mut referenced) = referenced.lock() else {
                break;
            };

            let Some(piece) = pieces.pop_front() else {
                break;
            };

            match referenced.get_mut(&piece.id) {
                Some(is_referenced) if *is_referenced => {
                    *is_referenced = false;
                    drop(referenced);
                    pieces.push_back(piece);
                }
                _ => {
                    referenced.remove(&piece.id);
                    drop(referenced);
                    drop_length += piece.length;
                    drop_pieces.push(piece);
                }
            }
        }

        drop_pieces
    }

    /// Drops the pages of the piece, unless its task is gone. The kernel writes
    /// back the dirty pages of the range first and skips them.
    async fn drop_piece(piece: Piece) {
        let Ok(f) = tokio::fs::File::open(&piece.path).await else {
            return;
        };

        let f = f.into_std().await;
        if let Err(err) = fadvise_dontneed_range(&f, piece.offset, piece.length).await {
            warn!("fadvise_dontneed failed: {}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pieces_of(ids: &[&str]) -> VecDeque<Piece> {
        ids.iter()
            .map(|id| Piece {
                id: id.to_string(),
                path: PathBuf::new(),
                offset: 0,
                length: 4,
            })
            .collect()
    }

    fn referenced_of(ids: &[&str], read: &[&str]) -> Mutex<HashMap<String, bool>> {
        Mutex::new(
            ids.iter()
                .map(|id| (id.to_string(), read.contains(id)))
                .collect(),
        )
    }

    fn ids_of(pieces: &[Piece]) -> Vec<&str> {
        pieces.iter().map(|piece| piece.id.as_str()).collect()
    }

    #[test]
    fn select_drop_pieces_takes_the_oldest_unreferenced_pieces() {
        let test_cases = vec![
            (
                &["a", "b", "c"][..],
                &[][..],
                0,
                vec![],
                vec!["a", "b", "c"],
            ),
            (&["a", "b", "c"], &[], 5, vec!["a", "b"], vec!["c"]),
            (&["a", "b", "c"], &[], 100, vec!["a", "b", "c"], vec![]),
            (&["a", "b", "c"], &["a"], 4, vec!["b"], vec!["c", "a"]),
            (
                &["a", "b", "c"],
                &["a", "b", "c"],
                100,
                vec![],
                vec!["a", "b", "c"],
            ),
            (&["a", "b", "c"], &["b"], 8, vec!["a", "c"], vec!["b"]),
            (&[], &[], 100, vec![], vec![]),
        ];

        for (ids, read, need_drop_length, expected_drops, expected_pieces) in test_cases {
            let mut pieces = pieces_of(ids);
            let referenced = referenced_of(ids, read);
            let drop_pieces =
                PageCache::select_drop_pieces(&mut pieces, &referenced, need_drop_length);
            assert_eq!(ids_of(&drop_pieces), expected_drops);
            assert_eq!(ids_of(pieces.make_contiguous()), expected_pieces);

            let referenced = referenced.lock().unwrap();
            let mut tracked: Vec<&str> = referenced.keys().map(String::as_str).collect();
            tracked.sort_unstable();
            let mut remaining = ids_of(pieces.make_contiguous());
            remaining.sort_unstable();
            assert_eq!(tracked, remaining);
            assert!(referenced.values().all(|is_referenced| !is_referenced));
        }
    }
}
