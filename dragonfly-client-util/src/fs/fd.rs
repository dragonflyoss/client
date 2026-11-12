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

use dragonfly_client_core::Result;
use lru::LruCache;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io;
use std::num::NonZeroUsize;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tracing::{error, trace, warn};

use super::fadvise_sequential;

/// The default capacity of the file descriptor cache.
pub const DEFAULT_FD_CACHE_CAPACITY: usize = 1024;

/// The maximum times the high priority writes can starve a low priority write.
const MAX_STARVED_COUNT: usize = 10;

/// The blocking write queued on a write-only file descriptor.
type QueuedWrite = Box<dyn FnOnce(&File) + Send>;

/// The priority of a write queued on a write-only file descriptor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WritePriority {
    /// Runs before the low priority writes.
    #[default]
    High,

    /// Runs after the high priority writes.
    Low,
}

/// The queued writes of a write-only file descriptor, at most one per writer.
#[derive(Default)]
struct WriteQueue {
    /// The queued high priority writes in submission order.
    high: VecDeque<QueuedWrite>,

    /// The queued low priority writes in submission order.
    low: VecDeque<QueuedWrite>,

    /// The times the high priority writes have starved a low priority write.
    starved: usize,

    /// Whether a blocking thread is draining the queue.
    draining: bool,
}

/// Implements the write queue.
impl WriteQueue {
    /// Pushes the write to the queue of its priority.
    fn push(&mut self, priority: WritePriority, write: QueuedWrite) {
        match priority {
            WritePriority::High => self.high.push_back(write),
            WritePriority::Low => self.low.push_back(write),
        }
    }

    /// Pops the next write to run. The high priority writes run first, until
    /// they have starved a low priority write [MAX_STARVED_COUNT] times.
    fn pop(&mut self) -> Option<QueuedWrite> {
        if self.low.is_empty() {
            return self.high.pop_front();
        }

        if self.starved < MAX_STARVED_COUNT {
            if let Some(write) = self.high.pop_front() {
                self.starved += 1;
                return Some(write);
            }
        }

        self.starved = 0;
        self.low.pop_front()
    }
}

/// The write-only file descriptor of a file. Its writes run one at a time, so
/// concurrent writers queue here instead of on the inode lock.
#[derive(Clone)]
pub struct WriteFD {
    /// The opened file descriptor.
    fd: Arc<File>,

    /// The queued writes of the file descriptor.
    queue: Arc<Mutex<WriteQueue>>,

    /// The priority of the writes.
    priority: WritePriority,
}

/// Implements the write-only file descriptor.
impl WriteFD {
    /// Creates a new WriteFD of the opened file descriptor.
    pub fn new(fd: Arc<File>) -> Self {
        Self {
            fd,
            queue: Arc::default(),
            priority: WritePriority::default(),
        }
    }

    /// Sets the priority of the writes.
    pub fn set_priority(&mut self, priority: WritePriority) {
        self.priority = priority;
    }

    /// Returns the opened file descriptor.
    pub fn get_ref(&self) -> &Arc<File> {
        &self.fd
    }

    /// Queues the write and returns the receiver of its result. The write is
    /// skipped once its receiver is dropped.
    pub fn spawn_blocking<F, T>(&self, write: F) -> Result<oneshot::Receiver<T>>
    where
        F: FnOnce(&File) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let write: QueuedWrite = Box::new(move |fd| {
            // Skip the write of a dropped writer.
            if tx.is_closed() {
                return;
            }

            if tx.send(write(fd)).is_err() {
                trace!("dropped the result of a write without a receiver");
            }
        });

        let draining = {
            let mut queue = self.queue.lock()?;
            queue.push(self.priority, write);
            std::mem::replace(&mut queue.draining, true)
        };

        // Drain the queue on one blocking thread.
        if !draining {
            let fd = self.clone();
            tokio::task::spawn_blocking(move || fd.drain());
        }

        Ok(rx)
    }

    /// Runs the queued writes one at a time until the queue is empty.
    fn drain(&self) {
        loop {
            let write = {
                let Ok(mut queue) = self.queue.lock() else {
                    return;
                };

                let Some(write) = queue.pop() else {
                    queue.draining = false;
                    return;
                };

                write
            };

            // Keep draining after a panicking write.
            if panic::catch_unwind(AssertUnwindSafe(|| write(&self.fd))).is_err() {
                error!("queued write panicked");
            }
        }
    }
}

/// FDCache caches opened file descriptors by path with LRU eviction, so repeated
/// reads and writes of the same file reuse one descriptor instead of reopening
/// the file every time. The read-only and write-only descriptors are cached
/// separately, so a write open failure surfaces at open time instead of falling
/// back to a cached read-only descriptor. The cached descriptors are only used
/// for positional reads and writes, which never move the file cursor, so one
/// descriptor is safely shared by concurrent readers and writers.
pub struct FDCache {
    /// The opened file descriptors for reading by path.
    read_fds: Mutex<LruCache<PathBuf, Arc<File>>>,

    /// The opened file descriptors for writing by path.
    write_fds: Mutex<LruCache<PathBuf, WriteFD>>,
}

/// Implements the file descriptor cache.
impl FDCache {
    /// Creates a new FDCache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            read_fds: Mutex::new(LruCache::new(capacity)),
            write_fds: Mutex::new(LruCache::new(capacity)),
        }
    }

    /// Returns the cached read-only file descriptor of the path, opening and
    /// caching it if it is absent. Concurrent misses may open the file more than
    /// once, the last inserted descriptor wins and the others are closed when
    /// their readers finish.
    ///
    /// The descriptor is advised for sequential reads, doubling its readahead
    /// cap over the device read_ahead_kb: 128KiB -> 256KiB on the NVMe/SSD/HDD
    /// default, 4MiB -> 8MiB on md/RAID commonly tuned by distributions. With
    /// a 128KiB cap, one 512KiB positional read is served by several small
    /// readahead I/Os. At 256KiB each I/O is larger and the async readahead
    /// marker is placed earlier, keeping the disk ahead of the sequential
    /// in-piece reads and sendfile. The cap only bounds detected sequential
    /// streams, so it costs nothing on random reads.
    pub async fn open(&self, path: &Path) -> Result<Arc<File>> {
        if let Some(fd) = self.read_fds.lock()?.get(path) {
            return Ok(fd.clone());
        }

        let path = path.to_path_buf();
        let fd = Arc::new(
            tokio::task::spawn_blocking({
                let path = path.clone();
                move || -> io::Result<File> {
                    let f = File::open(path)?;
                    fadvise_sequential(&f).unwrap_or_else(|err| {
                        warn!("fadvise_sequential failed: {}", err);
                    });

                    Ok(f)
                }
            })
            .await
            .map_err(io::Error::other)??,
        );

        self.read_fds.lock()?.put(path, fd.clone());
        Ok(fd)
    }

    /// Returns the cached write-only file descriptor of the path, opening and
    /// caching it if it is absent. The open error surfaces to the caller and
    /// nothing is cached on failure, so a file that becomes writable later is
    /// opened again on the next write. Concurrent misses share one descriptor
    /// and its write queue.
    pub async fn open_write(&self, path: &Path) -> Result<WriteFD> {
        if let Some(fd) = self.write_fds.lock()?.get(path) {
            return Ok(fd.clone());
        }

        let path = path.to_path_buf();
        let fd = WriteFD::new(Arc::new(
            tokio::task::spawn_blocking({
                let path = path.clone();
                move || {
                    OpenOptions::new()
                        .truncate(false)
                        .write(true)
                        .open(path.as_path())
                }
            })
            .await
            .map_err(io::Error::other)??,
        ));

        Ok(self.write_fds.lock()?.get_or_insert(path, || fd).clone())
    }

    /// Removes the cached file descriptors of the path. It needs to be called when
    /// the file is deleted, so a recreated file will not be read through the stale
    /// descriptor.
    pub fn remove(&self, path: &Path) -> Result<()> {
        self.read_fds.lock()?.pop(path);
        self.write_fds.lock()?.pop(path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{FileExt, PermissionsExt};
    use tempfile::tempdir;

    #[tokio::test]
    async fn open_caches_one_fd_per_path_and_mode() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();

        let cache = FDCache::new(DEFAULT_FD_CACHE_CAPACITY);
        let first = cache.open(&path).await.unwrap();
        let second = cache.open(&path).await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));

        let first_write = cache.open_write(&path).await.unwrap();
        let second_write = cache.open_write(&path).await.unwrap();
        assert!(Arc::ptr_eq(first_write.get_ref(), second_write.get_ref()));
        assert!(!Arc::ptr_eq(&first, first_write.get_ref()));

        cache.remove(&path).unwrap();
        let third = cache.open(&path).await.unwrap();
        let third_write = cache.open_write(&path).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &third));
        assert!(!Arc::ptr_eq(first_write.get_ref(), third_write.get_ref()));

        cache.remove(&path).unwrap();
        tokio::fs::remove_file(&path).await.unwrap();
        assert!(cache.open(&path).await.is_err());
        assert!(cache.open_write(&path).await.is_err());
    }

    #[tokio::test]
    async fn open_evicts_the_least_recently_used_fd() {
        let temp_dir = tempdir().unwrap();
        let first_path = temp_dir.path().join("first");
        let second_path = temp_dir.path().join("second");
        tokio::fs::write(&first_path, b"first").await.unwrap();
        tokio::fs::write(&second_path, b"second").await.unwrap();

        let cache = FDCache::new(0);
        let first = cache.open(&first_path).await.unwrap();
        let second = cache.open(&second_path).await.unwrap();
        let reopened = cache.open(&first_path).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &reopened));
        assert!(!Arc::ptr_eq(&second, &reopened));
    }

    #[tokio::test]
    async fn open_write_fails_on_a_read_only_file_until_it_is_writable() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();

        let mut permissions = tokio::fs::metadata(&path).await.unwrap().permissions();
        permissions.set_readonly(true);
        tokio::fs::set_permissions(&path, permissions)
            .await
            .unwrap();

        let cache = FDCache::new(DEFAULT_FD_CACHE_CAPACITY);
        let fd = cache.open(&path).await.unwrap();
        let mut buffer = vec![0u8; 13];
        fd.read_at(&mut buffer, 0).unwrap();
        assert_eq!(buffer, b"hello, world!");
        assert!(cache.open_write(&path).await.is_err());

        tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .await
            .unwrap();
        let fd = cache.open_write(&path).await.unwrap();
        fd.get_ref().write_all_at(b"HELLO", 0).unwrap();
        assert_eq!(&tokio::fs::read(&path).await.unwrap()[..5], b"HELLO");
    }

    #[tokio::test]
    async fn open_write_shares_one_fd_between_concurrent_misses() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"").await.unwrap();

        let cache = FDCache::new(DEFAULT_FD_CACHE_CAPACITY);
        let (first, second) = tokio::join!(cache.open_write(&path), cache.open_write(&path));
        let (first, second) = (first.unwrap(), second.unwrap());
        assert!(Arc::ptr_eq(first.get_ref(), second.get_ref()));
        assert!(Arc::ptr_eq(&first.queue, &second.queue));
    }

    #[tokio::test]
    async fn spawn_blocking_runs_the_writes_one_at_a_time_in_order() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        let cache = FDCache::new(DEFAULT_FD_CACHE_CAPACITY);

        let test_cases = vec![1u8, 2, 64];

        for count in test_cases {
            tokio::fs::write(&path, b"").await.unwrap();
            let fd = cache.open_write(&path).await.unwrap();
            let receivers: Vec<_> = (0..count)
                .map(|i| {
                    fd.spawn_blocking(move |fd| {
                        let length = fd.metadata()?.len();
                        fd.write_all_at(&[i], length)
                    })
                    .unwrap()
                })
                .collect();

            for rx in receivers {
                rx.await.unwrap().unwrap();
            }

            assert_eq!(
                tokio::fs::read(&path).await.unwrap(),
                (0..count).collect::<Vec<_>>()
            );
        }
    }

    #[tokio::test]
    async fn spawn_blocking_runs_the_low_priority_writes_after_the_high_priority_writes() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        let cache = FDCache::new(DEFAULT_FD_CACHE_CAPACITY);

        let (high, low) = (WritePriority::High, WritePriority::Low);
        let test_cases: Vec<(Vec<WritePriority>, Vec<u8>)> = vec![
            (vec![high, high, high], vec![0, 1, 2]),
            (vec![low, low, low], vec![0, 1, 2]),
            (vec![low, high, high], vec![1, 2, 0]),
            (vec![low, high, low, high], vec![1, 3, 0, 2]),
            (
                [vec![low], vec![high; 12]].concat(),
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 11, 12],
            ),
        ];

        for (priorities, expected) in test_cases {
            tokio::fs::write(&path, b"").await.unwrap();
            let fd = cache.open_write(&path).await.unwrap();
            let (started_tx, started_rx) = oneshot::channel();
            let (blocking_tx, blocking_rx) = std::sync::mpsc::channel::<()>();
            let blocking = fd
                .spawn_blocking(move |_| {
                    started_tx.send(()).unwrap();
                    blocking_rx.recv()
                })
                .unwrap();
            started_rx.await.unwrap();
            let receivers: Vec<_> = priorities
                .into_iter()
                .zip(0u8..)
                .map(|(priority, i)| {
                    let mut fd = fd.clone();
                    fd.set_priority(priority);
                    fd.spawn_blocking(move |fd| {
                        let length = fd.metadata()?.len();
                        fd.write_all_at(&[i], length)
                    })
                    .unwrap()
                })
                .collect();

            blocking_tx.send(()).unwrap();
            blocking.await.unwrap().unwrap();
            for rx in receivers {
                rx.await.unwrap().unwrap();
            }

            assert_eq!(tokio::fs::read(&path).await.unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn spawn_blocking_keeps_draining_after_a_panicking_write() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"").await.unwrap();

        let cache = FDCache::new(DEFAULT_FD_CACHE_CAPACITY);
        let fd = cache.open_write(&path).await.unwrap();
        let panicked = fd
            .spawn_blocking(|_| -> io::Result<()> { panic!("write panicked") })
            .unwrap();
        let kept = fd.spawn_blocking(|fd| fd.write_all_at(b"kept", 0)).unwrap();

        assert!(panicked.await.is_err());
        kept.await.unwrap().unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"kept");
    }

    #[tokio::test]
    async fn spawn_blocking_skips_the_write_of_a_dropped_receiver() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        let cache = FDCache::new(DEFAULT_FD_CACHE_CAPACITY);

        let test_cases: Vec<(bool, &[u8])> = vec![(true, b"kept"), (false, b"keptped")];

        for (dropped, expected) in test_cases {
            tokio::fs::write(&path, b"").await.unwrap();
            let fd = cache.open_write(&path).await.unwrap();
            let (blocking_tx, blocking_rx) = std::sync::mpsc::channel::<()>();
            let blocking = fd.spawn_blocking(move |_| blocking_rx.recv()).unwrap();
            let rx = fd
                .spawn_blocking(|fd| fd.write_all_at(b"dropped", 0))
                .unwrap();
            let _rx = (!dropped).then_some(rx);
            let kept = fd.spawn_blocking(|fd| fd.write_all_at(b"kept", 0)).unwrap();

            blocking_tx.send(()).unwrap();
            blocking.await.unwrap().unwrap();
            kept.await.unwrap().unwrap();
            assert_eq!(tokio::fs::read(&path).await.unwrap(), expected);
        }
    }
}
