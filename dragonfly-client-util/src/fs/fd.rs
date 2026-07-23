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
use std::fs::{File, OpenOptions};
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// The default capacity of the file descriptor cache.
pub const DEFAULT_FD_CACHE_CAPACITY: usize = 1024;

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
    write_fds: Mutex<LruCache<PathBuf, Arc<File>>>,
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
    pub async fn open(&self, path: &Path) -> Result<Arc<File>> {
        if let Some(fd) = self
            .read_fds
            .lock()
            .map_err(|err| dragonfly_client_core::Error::MutexPoisoned(err.to_string()))?
            .get(path)
        {
        }

        let path = path.to_path_buf();
        let fd = Arc::new(
            tokio::task::spawn_blocking({
                let path = path.clone();
                move || File::open(path)
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
    /// opened again on the next write.
    pub async fn open_write(&self, path: &Path) -> Result<Arc<File>> {
        if let Some(fd) = self.write_fds.lock()?.get(path) {
            return Ok(fd.clone());
        }

        let path = path.to_path_buf();
        let fd = Arc::new(
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
        );

        self.write_fds.lock()?.put(path, fd.clone());
        Ok(fd)
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
    use std::os::unix::fs::FileExt;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_fd_cache() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("task");
        tokio::fs::write(&path, b"hello, world!").await.unwrap();

        let cache = FDCache::new(DEFAULT_FD_CACHE_CAPACITY);
        let first = cache.open(&path).await.unwrap();
        let second = cache.open(&path).await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));

        let first_write = cache.open_write(&path).await.unwrap();
        let second_write = cache.open_write(&path).await.unwrap();
        assert!(Arc::ptr_eq(&first_write, &second_write));
        assert!(!Arc::ptr_eq(&first, &first_write));

        let _ = cache.remove(&path);
        let third = cache.open(&path).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &third));
        let third_write = cache.open_write(&path).await.unwrap();
        assert!(!Arc::ptr_eq(&first_write, &third_write));

        let _ = cache.remove(&path);
        tokio::fs::remove_file(&path).await.unwrap();
        assert!(cache.open(&path).await.is_err());
        assert!(cache.open_write(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_fd_cache_open_write_read_only_file() {
        use std::os::unix::fs::PermissionsExt;

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
        fd.write_all_at(b"HELLO", 0).unwrap();
        assert_eq!(&tokio::fs::read(&path).await.unwrap()[..5], b"HELLO");
    }
}
