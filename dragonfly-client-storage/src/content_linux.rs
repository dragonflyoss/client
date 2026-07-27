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
use bytesize::ByteSize;
use dragonfly_api::common::v2::Range;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::buffer_pool::BufferPool;
use dragonfly_client_util::fs::fallocate;
use dragonfly_client_util::fs::fd::{FDCache, DEFAULT_FD_CACHE_CAPACITY};
use futures::Stream;
use std::cmp::max;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncRead;
use tracing::{error, info, instrument, warn};
use walkdir::WalkDir;

/// The content of a piece.
pub struct Content {
    /// The configuration of the dfdaemon.
    pub config: Arc<Config>,

    /// The directory to store content.
    pub dir: PathBuf,

    /// The cache of the opened file descriptors for reading pieces.
    fd_cache: FDCache,

    /// The pool of the staging buffers for reading and writing pieces.
    buffer_pool: BufferPool,
}

/// Implements the content storage.
impl Content {
    /// Returns a new content.
    pub async fn new(config: Arc<Config>, dir: &Path) -> Result<Content> {
        let dir = dir.join(super::content::DEFAULT_CONTENT_DIR);

        // If the storage is not kept, remove the directory.
        if !config.storage.keep {
            fs::remove_dir_all(&dir).await.unwrap_or_else(|err| {
                warn!("remove {:?} failed: {}", dir, err);
            });
        }

        fs::create_dir_all(&dir.join(super::content::DEFAULT_TASK_DIR)).await?;
        fs::create_dir_all(&dir.join(super::content::DEFAULT_PERSISTENT_TASK_DIR)).await?;
        fs::create_dir_all(&dir.join(super::content::DEFAULT_PERSISTENT_CACHE_TASK_DIR)).await?;
        info!("content initialized directory: {:?}", dir);
        Ok(Content {
            buffer_pool: BufferPool::new(
                super::content::MAX_BUFFER_POOL_IDLE_BUFFERS
                    * max(
                        config.storage.write_buffer_size,
                        config.storage.read_buffer_size,
                    ),
            ),
            config,
            dir,
            fd_cache: FDCache::new(DEFAULT_FD_CACHE_CAPACITY),
        })
    }

    /// Returns the available space of the disk.
    pub fn available_space(&self) -> Result<u64> {
        let disk_threshold = self.config.gc.policy.disk_threshold;
        if disk_threshold != ByteSize::default() {
            let usage_space = WalkDir::new(&self.dir)
                .into_iter()
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| entry.metadata().ok())
                .filter(|metadata| metadata.is_file())
                .fold(0, |acc, m| acc + m.len());

            if usage_space >= disk_threshold.as_u64() {
                warn!(
                    "usage space {} is greater than disk threshold {}, no need to calculate available space",
                    usage_space, disk_threshold
                );

                return Ok(0);
            }

            return Ok(disk_threshold.as_u64() - usage_space);
        }

        let stat = fs2::statvfs(&self.dir)?;
        Ok(stat.available_space())
    }

    /// Returns the total space of the disk.
    pub fn total_space(&self) -> Result<u64> {
        // If the disk_threshold is set, return it directly.
        let disk_threshold = self.config.gc.policy.disk_threshold;
        if disk_threshold != ByteSize::default() {
            return Ok(disk_threshold.as_u64());
        }

        let stat = fs2::statvfs(&self.dir)?;
        Ok(stat.total_space())
    }

    /// Checks if the storage has enough space to store the content.
    pub fn has_enough_space(&self, content_length: u64) -> Result<bool> {
        let available_space = self.available_space()?;
        if available_space < content_length {
            warn!(
                "not enough space to store the task: available_space={}, content_length={}",
                available_space, content_length
            );

            return Ok(false);
        }

        Ok(true)
    }

    /// Checks if the source and target are the same device and inode.
    async fn is_same_dev_inode<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        source: P,
        target: Q,
    ) -> Result<bool> {
        let source_metadata = fs::metadata(source).await?;
        let target_metadata = fs::metadata(target).await?;

        Ok(source_metadata.dev() == target_metadata.dev()
            && source_metadata.ino() == target_metadata.ino())
    }

    /// Checks if the task and target are the same device and inode.
    pub async fn is_same_dev_inode_as_task(&self, task_id: &str, to: &Path) -> Result<bool> {
        let task_path = self.get_task_path(task_id);
        self.is_same_dev_inode(&task_path, to).await
    }

    /// Creates a new task content.
    ///
    /// Behavior of `create_task`:
    /// 1. If the task already exists, return the task path.
    /// 2. If the task does not exist, create the task directory and file.
    #[instrument(level = "debug", skip_all)]
    pub async fn create_task(&self, task_id: &str, length: u64) -> Result<PathBuf> {
        let task_path = self.get_task_path(task_id);
        if task_path.exists() {
            return Ok(task_path);
        }

        let task_dir = self
            .dir
            .join(super::content::DEFAULT_TASK_DIR)
            .join(&task_id[..3]);
        fs::create_dir_all(&task_dir).await.inspect_err(|err| {
            error!("create {:?} failed: {}", task_dir, err);
        })?;

        let f = fs::File::create(task_dir.join(task_id))
            .await
            .inspect_err(|err| {
                error!("create {:?} failed: {}", task_dir, err);
            })?;

        fallocate(&f, length).await.inspect_err(|err| {
            error!("fallocate {:?} failed: {}", task_dir, err);
        })?;

        Ok(task_dir.join(task_id))
    }

    /// Hard links the task content to the destination.
    ///
    /// Behavior of `hard_link_task`:
    /// 1. If the destination exists:
    ///    1.1. If the source and destination share the same device and inode, return immediately.
    ///    1.2. Otherwise, return an error.
    /// 2. If the destination does not exist:
    ///    2.1. If the hard link succeeds, return immediately.
    ///    2.2. If the hard link fails, copy the task content to the destination once the task is finished, then return immediately.
    #[instrument(level = "debug", skip_all)]
    pub async fn hard_link_task(&self, task_id: &str, to: &Path) -> Result<()> {
        let task_path = self.get_task_path(task_id);
        if let Err(err) = fs::hard_link(task_path.clone(), to).await {
            if err.kind() == std::io::ErrorKind::AlreadyExists {
                if let Ok(true) = self.is_same_dev_inode(&task_path, to).await {
                    info!("hard already exists, no need to operate");
                    return Ok(());
                }
            }

            warn!("hard link {:?} to {:?} failed: {}", task_path, to, err);
            return Err(Error::IO(err));
        }

        info!("hard link {:?} to {:?} success", task_path, to);
        Ok(())
    }

    /// Copies the task content to the destination.
    #[instrument(level = "debug", skip_all)]
    pub async fn copy_task(&self, task_id: &str, to: &Path) -> Result<()> {
        fs::copy(self.get_task_path(task_id), to).await?;
        info!("copy to {:?} success", to);
        Ok(())
    }

    /// Deletes the task content.
    pub async fn delete_task(&self, task_id: &str) -> Result<()> {
        info!("delete task content: {}", task_id);
        let task_path = self.get_task_path(task_id);

        self.fd_cache.remove(&task_path).unwrap_or_else(|err| {
            error!("remove {:?} from fd_cache failed: {}", task_path, err);
        });

        fs::remove_file(task_path.as_path())
            .await
            .inspect_err(|err| {
                error!("remove {:?} failed: {}", task_path, err);
            })?;
        Ok(())
    }

    /// Reads the piece from the content.
    #[instrument(level = "debug", skip_all)]
    pub async fn read_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<super::io::RangeReader> {
        let task_path = self.get_task_path(task_id);

        // Calculate the target offset and length based on the range.
        let (target_offset, target_length) =
            super::content::calculate_piece_range(offset, length, range);

        let fd = self.fd_cache.open(&task_path).await.inspect_err(|err| {
            error!("open {:?} failed: {}", task_path, err);
        })?;

        Ok(super::io::RangeReader::new(
            fd,
            target_offset,
            target_length,
            self.config.storage.read_buffer_size,
            self.buffer_pool.clone(),
        ))
    }

    /// map_piece memory-maps the finished piece bytes on disk. The mapping covers exactly the
    /// piece range so callers can copy into RDMA send windows without an intermediate reader.
    #[instrument(skip_all)]
    pub async fn map_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
    ) -> Result<super::content::MappedPiece> {
        self.map_path_range(self.get_task_path(task_id), offset, length)
            .await
    }

    /// map_persistent_piece memory-maps finished persistent piece bytes on disk.
    #[instrument(skip_all)]
    pub async fn map_persistent_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
    ) -> Result<super::content::MappedPiece> {
        self.map_path_range(self.get_persistent_task_path(task_id), offset, length)
            .await
    }

    /// map_persistent_cache_piece memory-maps finished persistent cache piece bytes on disk.
    #[instrument(skip_all)]
    pub async fn map_persistent_cache_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
    ) -> Result<super::content::MappedPiece> {
        self.map_path_range(self.get_persistent_cache_task_path(task_id), offset, length)
            .await
    }

    /// map_path_range memory-maps `[offset, offset+length)` of a content file.
    async fn map_path_range(
        &self,
        path: PathBuf,
        offset: u64,
        length: u64,
    ) -> Result<super::content::MappedPiece> {
        if length == 0 {
            return Err(Error::InvalidParameter);
        }
        let mapped = tokio::task::spawn_blocking(move || -> Result<super::content::MappedPiece> {
            let file = std::fs::File::open(&path).inspect_err(|err| {
                error!("open {:?} failed: {}", path, err);
            })?;
            let metadata = file.metadata().inspect_err(|err| {
                error!("stat {:?} failed: {}", path, err);
            })?;
            let end = offset.checked_add(length).ok_or(Error::InvalidParameter)?;
            if end > metadata.len() {
                return Err(Error::Unknown(format!(
                    "piece range [{}, {}) exceeds content length {}",
                    offset,
                    end,
                    metadata.len()
                )));
            }
            // Safety: the file remains open while the mapping is constructed; MappedPiece owns
            // the resulting pages for the piece lifetime.
            let mmap = unsafe {
                memmap2::MmapOptions::new()
                    .offset(offset)
                    .len(length as usize)
                    .map(&file)
            }
            .inspect_err(|err| {
                error!(
                    "mmap {:?} offset {} length {} failed: {}",
                    path, offset, length, err
                );
            })?;
            // Fault pages in so later window copies do not block the fabric send path on major
            // page faults under memory pressure.
            mmap.advise(memmap2::Advice::Sequential).ok();
            mmap.advise(memmap2::Advice::WillNeed).ok();
            Ok(super::content::MappedPiece::new(mmap))
        })
        .await
        .map_err(|err| Error::Unknown(format!("mmap piece task join failed: {err}")))??;
        Ok(mapped)
    }

    /// Writes the piece from the stream of bytes chunks to the content and
    /// calculates the hash of the piece by crc32.
    #[instrument(level = "debug", skip_all)]
    pub async fn write_piece_from_stream<S>(
        &self,
        task_id: &str,
        offset: u64,
        expected_length: u64,
        stream: &mut S,
    ) -> Result<super::io::WriteRangeResponse>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Unpin + ?Sized,
    {
        let task_path = self.get_task_path(task_id);
        let fd = self
            .fd_cache
            .open_write(&task_path)
            .await
            .inspect_err(|err| {
                error!("open {:?} failed: {}", task_path, err);
            })?;

        super::io::write_range_from_stream(
            fd,
            offset,
            expected_length,
            self.config.storage.write_buffer_size,
            stream,
        )
        .await
        .inspect_err(|err| {
            error!("write {:?} failed: {}", task_path, err);
        })
    }

    /// write_piece_from_rdma_stream writes completed RDMA receive windows straight from registered
    /// memory into the task file, hashing each window in place. Unlike
    /// [`Self::write_piece_from_stream`] there is no staging buffer between the fabric and the
    /// file.
    #[cfg(feature = "rdma")]
    #[instrument(skip_all)]
    pub async fn write_piece_from_rdma_stream(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        expected_length: u64,
        reader: &mut crate::client::rdma::RDMAStreamReader,
        window_timeout: std::time::Duration,
    ) -> Result<super::io::WriteRangeResponse> {
        use std::os::unix::fs::FileExt;
        use tokio::time::timeout;

        let task_path = self.get_task_path(task_id);

        // The cached descriptor is a std::fs handle, so each window reaches the file in a single
        // pwrite straight out of registered memory. Copying the window into a buffer first costs
        // about a third of the achievable goodput on a memory filesystem.
        let file = self
            .fd_cache
            .open_write(&task_path)
            .await
            .inspect_err(|err| {
                error!("open {:?} failed: {}", task_path, err);
            })?;

        let mut hasher = crc32fast::Hasher::new();
        let mut length = 0u64;
        loop {
            // Only the wait for the next window is bounded, and the caller must not wrap this loop
            // in a cancelling timeout either. The digest and the pwrite below run on blocking
            // threads that cannot be aborted, so abandoning them between spawn and join would
            // leave a write outstanding while the caller falls back to TCP and rewrites the same
            // range, and the late write would then contradict the digest recorded for the piece.
            let window = match timeout(window_timeout, reader.next_window()).await {
                Ok(window) => window?,
                Err(_) => return Err(Error::DownloadPieceFinishedTimeout(piece_id.to_string())),
            };
            let Some(window) = window else {
                break;
            };

            let window_length = window.bytes().len() as u64;

            // Bound the write like write_range_from_stream does: a parent that streams more than
            // the piece length must not overwrite the pieces that follow it in the task file.
            if window_length > expected_length - length {
                return Err(Error::Unknown(format!(
                    "rdma stream exceeded expected length {expected_length}"
                )));
            }

            // Digest and write both only read the window, so they run on separate blocking threads
            // instead of in series, and the fabric receives the next window while they do.
            let window = Arc::new(window);
            let position = offset + length;
            let digest = {
                let window = window.clone();
                tokio::task::spawn_blocking(move || {
                    hasher.update(window.bytes());
                    hasher
                })
            };
            let write = {
                let window = window.clone();
                let file = file.clone();
                tokio::task::spawn_blocking(move || file.write_all_at(window.bytes(), position))
            };

            let (digest, write) = tokio::join!(digest, write);
            hasher = digest.map_err(|err| Error::Unknown(format!("digest panicked: {err}")))?;
            write
                .map_err(|err| Error::Unknown(format!("write piece panicked: {err}")))?
                .inspect_err(|err| {
                    error!("write {:?} failed: {}", task_path, err);
                })?;

            length += window_length;
        }

        if length != expected_length {
            return Err(Error::Unknown(format!(
                "expected length {expected_length} but got {length}"
            )));
        }

        Ok(super::io::WriteRangeResponse {
            length,
            hash: hasher.finalize().to_string(),
        })
    }

    /// Returns the task path by task id.
    fn get_task_path(&self, task_id: &str) -> PathBuf {
        // The task needs split by the first 3 characters of task id(sha256) to
        // avoid too many files in one directory.
        let sub_dir = &task_id[..3];
        self.dir
            .join(super::content::DEFAULT_TASK_DIR)
            .join(sub_dir)
            .join(task_id)
    }

    /// Checks if the persistent task and target
    /// are the same device and inode.
    pub async fn is_same_dev_inode_as_persistent_task(
        &self,
        task_id: &str,
        to: &Path,
    ) -> Result<bool> {
        let task_path = self.get_persistent_task_path(task_id);
        self.is_same_dev_inode(&task_path, to).await
    }

    /// Creates a new persistent task content.
    ///
    /// Behavior of `create_persistent_task`:
    /// 1. If the persistent task already exists, return the persistent task path.
    /// 2. If the persistent task does not exist, create the persistent task directory and file.
    #[instrument(level = "debug", skip_all)]
    pub async fn create_persistent_task(&self, task_id: &str, length: u64) -> Result<PathBuf> {
        let task_path = self.get_persistent_task_path(task_id);
        if task_path.exists() {
            return Ok(task_path);
        }

        let task_dir = self
            .dir
            .join(super::content::DEFAULT_PERSISTENT_TASK_DIR)
            .join(&task_id[..3]);
        fs::create_dir_all(&task_dir).await.inspect_err(|err| {
            error!("create {:?} failed: {}", task_dir, err);
        })?;

        let f = fs::File::create(task_dir.join(task_id))
            .await
            .inspect_err(|err| {
                error!("create {:?} failed: {}", task_dir, err);
            })?;

        fallocate(&f, length).await.inspect_err(|err| {
            error!("fallocate {:?} failed: {}", task_dir, err);
        })?;

        Ok(task_dir.join(task_id))
    }

    /// Creates only the directory for the persistent task.
    #[instrument(level = "debug", skip_all)]
    pub async fn create_persistent_task_dir(&self, task_id: &str) -> Result<PathBuf> {
        let task_path = self.get_persistent_task_path(task_id);
        if task_path.exists() {
            return Ok(task_path);
        }

        let task_dir = self
            .dir
            .join(super::content::DEFAULT_PERSISTENT_TASK_DIR)
            .join(&task_id[..3]);
        fs::create_dir_all(&task_dir).await.inspect_err(|err| {
            error!("create {:?} failed: {}", task_dir, err);
        })?;

        Ok(task_dir)
    }

    /// Hard links the persistent task content to the destination.
    ///
    /// Behavior of `hard_link_persistent_task`:
    /// 1. If the destination exists:
    ///    1.1. If the source and destination share the same device and inode, return immediately.
    ///    1.2. Otherwise, return an error.
    /// 2. If the destination does not exist:
    ///    2.1. If the hard link succeeds, return immediately.
    ///    2.2. If the hard link fails, copy the persistent task content to the destination once the task is finished, then return immediately.
    #[instrument(level = "debug", skip_all)]
    pub async fn hard_link_persistent_task(&self, task_id: &str, to: &Path) -> Result<()> {
        let task_path = self.get_persistent_task_path(task_id);
        if let Err(err) = fs::hard_link(task_path.clone(), to).await {
            if err.kind() == std::io::ErrorKind::AlreadyExists {
                if let Ok(true) = self.is_same_dev_inode(&task_path, to).await {
                    info!("hard already exists, no need to operate");
                    return Ok(());
                }
            }

            warn!("hard link {:?} to {:?} failed: {}", task_path, to, err);
            return Err(Error::IO(err));
        }

        info!("hard link {:?} to {:?} success", task_path, to);
        Ok(())
    }

    /// Hard links a source file to the persistent task content path.
    ///
    /// Behavior:
    /// 1. If the task path exists:
    ///    1.1. If source and task share the same inode, return success.
    ///    1.2. Otherwise, return an error (task content already exists).
    /// 2. If the task path does not exist:
    ///    2.1. Create hard link from source to task path.
    ///    2.2. If hard link fails, return an error.
    #[instrument(level = "debug", skip_all)]
    pub async fn hard_link_to_persistent_task(&self, from: &Path, task_id: &str) -> Result<()> {
        let task_path = self.get_persistent_task_path(task_id);
        if let Err(err) = fs::hard_link(from, &task_path).await {
            if err.kind() == std::io::ErrorKind::AlreadyExists {
                if let Ok(true) = self.is_same_dev_inode(from, &task_path).await {
                    info!("hard already exists, no need to operate");
                    return Ok(());
                }
            }

            warn!("hard link {:?} to {:?} failed: {}", task_path, from, err);
            return Err(Error::IO(err));
        }

        info!("hard link {:?} to {:?} success", from, task_path);
        Ok(())
    }

    /// Copies the persistent task content to the destination.
    #[instrument(level = "debug", skip_all)]
    pub async fn copy_persistent_task(&self, task_id: &str, to: &Path) -> Result<()> {
        fs::copy(self.get_persistent_task_path(task_id), to).await?;
        info!("copy to {:?} success", to);
        Ok(())
    }

    /// Reads the persistent piece from the content.
    #[instrument(level = "debug", skip_all)]
    pub async fn read_persistent_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<super::io::RangeReader> {
        let task_path = self.get_persistent_task_path(task_id);

        // Calculate the target offset and length based on the range.
        let (target_offset, target_length) =
            super::content::calculate_piece_range(offset, length, range);

        let fd = self.fd_cache.open(&task_path).await.inspect_err(|err| {
            error!("open {:?} failed: {}", task_path, err);
        })?;

        Ok(super::io::RangeReader::new(
            fd,
            target_offset,
            target_length,
            self.config.storage.read_buffer_size,
            self.buffer_pool.clone(),
        ))
    }

    /// Writes the persistent piece to the content and
    /// calculates the hash of the piece by crc32.
    #[instrument(level = "debug", skip_all)]
    pub async fn write_persistent_piece<R: AsyncRead + Unpin + ?Sized>(
        &self,
        task_id: &str,
        offset: u64,
        expected_length: u64,
        reader: &mut R,
    ) -> Result<super::io::WriteRangeResponse> {
        let task_path = self.get_persistent_task_path(task_id);
        let fd = self
            .fd_cache
            .open_write(&task_path)
            .await
            .inspect_err(|err| {
                error!("open {:?} failed: {}", task_path, err);
            })?;

        super::io::write_range(
            fd,
            offset,
            expected_length,
            self.config.storage.write_buffer_size,
            reader,
            &self.buffer_pool,
        )
        .await
        .inspect_err(|err| {
            error!("write {:?} failed: {}", task_path, err);
        })
    }

    /// Writes the persistent piece from the stream of bytes chunks to the
    /// content and calculates the hash of the piece by crc32.
    #[instrument(level = "debug", skip_all)]
    pub async fn write_persistent_piece_from_stream<S>(
        &self,
        task_id: &str,
        offset: u64,
        expected_length: u64,
        stream: &mut S,
    ) -> Result<super::io::WriteRangeResponse>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Unpin + ?Sized,
    {
        let task_path = self.get_persistent_task_path(task_id);
        let fd = self
            .fd_cache
            .open_write(&task_path)
            .await
            .inspect_err(|err| {
                error!("open {:?} failed: {}", task_path, err);
            })?;

        super::io::write_range_from_stream(
            fd,
            offset,
            expected_length,
            self.config.storage.write_buffer_size,
            stream,
        )
        .await
        .inspect_err(|err| {
            error!("write {:?} failed: {}", task_path, err);
        })
    }

    /// Deletes the persistent task content.
    pub async fn delete_persistent_task(&self, task_id: &str) -> Result<()> {
        info!("delete persistent task content: {}", task_id);
        let persistent_task_path = self.get_persistent_task_path(task_id);

        self.fd_cache
            .remove(&persistent_task_path)
            .unwrap_or_else(|err| {
                error!(
                    "remove {:?} from fd_cache failed: {}",
                    persistent_task_path, err
                );
            });

        fs::remove_file(persistent_task_path.as_path())
            .await
            .inspect_err(|err| {
                error!("remove {:?} failed: {}", persistent_task_path, err);
            })?;
        Ok(())
    }

    /// Returns the persistent task path by task id.
    fn get_persistent_task_path(&self, task_id: &str) -> PathBuf {
        // The persistent task needs split by the first 3 characters of task id(sha256) to
        // avoid too many files in one directory.
        self.dir
            .join(super::content::DEFAULT_PERSISTENT_TASK_DIR)
            .join(&task_id[..3])
            .join(task_id)
    }

    /// Checks if the persistent cache task and target
    /// are the same device and inode.
    pub async fn is_same_dev_inode_as_persistent_cache_task(
        &self,
        task_id: &str,
        to: &Path,
    ) -> Result<bool> {
        let task_path = self.get_persistent_cache_task_path(task_id);
        self.is_same_dev_inode(&task_path, to).await
    }

    /// Creates a new persistent cache task content.
    ///
    /// Behavior of `create_persistent_cache_task`:
    /// 1. If the persistent cache task already exists, return the persistent cache task path.
    /// 2. If the persistent cache task does not exist, create the persistent cache task directory and file.
    #[instrument(level = "debug", skip_all)]
    pub async fn create_persistent_cache_task(
        &self,
        task_id: &str,
        length: u64,
    ) -> Result<PathBuf> {
        let task_path = self.get_persistent_cache_task_path(task_id);
        if task_path.exists() {
            return Ok(task_path);
        }

        let task_dir = self
            .dir
            .join(super::content::DEFAULT_PERSISTENT_CACHE_TASK_DIR)
            .join(&task_id[..3]);
        fs::create_dir_all(&task_dir).await.inspect_err(|err| {
            error!("create {:?} failed: {}", task_dir, err);
        })?;

        let f = fs::File::create(task_dir.join(task_id))
            .await
            .inspect_err(|err| {
                error!("create {:?} failed: {}", task_dir, err);
            })?;

        fallocate(&f, length).await.inspect_err(|err| {
            error!("fallocate {:?} failed: {}", task_dir, err);
        })?;

        Ok(task_dir.join(task_id))
    }

    /// Creates only the directory for the persistent cache task.
    #[instrument(level = "debug", skip_all)]
    pub async fn create_persistent_cache_task_dir(&self, task_id: &str) -> Result<PathBuf> {
        let task_path = self.get_persistent_cache_task_path(task_id);
        if task_path.exists() {
            return Ok(task_path);
        }

        let task_dir = self
            .dir
            .join(super::content::DEFAULT_PERSISTENT_CACHE_TASK_DIR)
            .join(&task_id[..3]);
        fs::create_dir_all(&task_dir).await.inspect_err(|err| {
            error!("create {:?} failed: {}", task_dir, err);
        })?;

        Ok(task_dir)
    }

    /// Hard links the persistent cache task content to the destination.
    ///
    /// Behavior of `hard_link_persistent_cache_task`:
    /// 1. If the destination exists:
    ///    1.1. If the source and destination share the same device and inode, return immediately.
    ///    1.2. Otherwise, return an error.
    /// 2. If the destination does not exist:
    ///    2.1. If the hard link succeeds, return immediately.
    ///    2.2. If the hard link fails, copy the persistent cache task content to the destination once the task is finished, then return immediately.
    #[instrument(level = "debug", skip_all)]
    pub async fn hard_link_persistent_cache_task(&self, task_id: &str, to: &Path) -> Result<()> {
        let task_path = self.get_persistent_cache_task_path(task_id);
        if let Err(err) = fs::hard_link(task_path.clone(), to).await {
            if err.kind() == std::io::ErrorKind::AlreadyExists {
                if let Ok(true) = self.is_same_dev_inode(&task_path, to).await {
                    info!("hard already exists, no need to operate");
                    return Ok(());
                }
            }

            warn!("hard link {:?} to {:?} failed: {}", task_path, to, err);
            return Err(Error::IO(err));
        }

        info!("hard link {:?} to {:?} success", task_path, to);
        Ok(())
    }

    /// Hard links a source file to the persistent cache task content path.
    ///
    /// Behavior:
    /// 1. If the task path exists:
    ///    1.1. If source and task share the same inode, return success.
    ///    1.2. Otherwise, return an error (task content already exists).
    /// 2. If the task path does not exist:
    ///    2.1. Create hard link from source to task path.
    ///    2.2. If hard link fails, return an error.
    #[instrument(level = "debug", skip_all)]
    pub async fn hard_link_to_persistent_cache_task(
        &self,
        from: &Path,
        task_id: &str,
    ) -> Result<()> {
        let task_path = self.get_persistent_cache_task_path(task_id);
        if let Err(err) = fs::hard_link(from, &task_path).await {
            if err.kind() == std::io::ErrorKind::AlreadyExists {
                if let Ok(true) = self.is_same_dev_inode(from, &task_path).await {
                    info!("hard already exists, no need to operate");
                    return Ok(());
                }
            }

            warn!("hard link {:?} to {:?} failed: {}", task_path, from, err);
            return Err(Error::IO(err));
        }

        info!("hard link {:?} to {:?} success", from, task_path);
        Ok(())
    }

    /// Copies the persistent cache task content to the destination.
    #[instrument(level = "debug", skip_all)]
    pub async fn copy_persistent_cache_task(&self, task_id: &str, to: &Path) -> Result<()> {
        fs::copy(self.get_persistent_cache_task_path(task_id), to).await?;
        info!("copy to {:?} success", to);
        Ok(())
    }

    /// Reads the persistent cache piece from the content.
    #[instrument(level = "debug", skip_all)]
    pub async fn read_persistent_cache_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<super::io::RangeReader> {
        let task_path = self.get_persistent_cache_task_path(task_id);

        // Calculate the target offset and length based on the range.
        let (target_offset, target_length) =
            super::content::calculate_piece_range(offset, length, range);

        let fd = self.fd_cache.open(&task_path).await.inspect_err(|err| {
            error!("open {:?} failed: {}", task_path, err);
        })?;

        Ok(super::io::RangeReader::new(
            fd,
            target_offset,
            target_length,
            self.config.storage.read_buffer_size,
            self.buffer_pool.clone(),
        ))
    }

    /// Writes the persistent cache piece to the content and
    /// calculates the hash of the piece by crc32.
    #[instrument(level = "debug", skip_all)]
    pub async fn write_persistent_cache_piece<R: AsyncRead + Unpin + ?Sized>(
        &self,
        task_id: &str,
        offset: u64,
        expected_length: u64,
        reader: &mut R,
    ) -> Result<super::io::WriteRangeResponse> {
        let task_path = self.get_persistent_cache_task_path(task_id);
        let fd = self
            .fd_cache
            .open_write(&task_path)
            .await
            .inspect_err(|err| {
                error!("open {:?} failed: {}", task_path, err);
            })?;

        super::io::write_range(
            fd,
            offset,
            expected_length,
            self.config.storage.write_buffer_size,
            reader,
            &self.buffer_pool,
        )
        .await
        .inspect_err(|err| {
            error!("write {:?} failed: {}", task_path, err);
        })
    }

    /// Writes the persistent cache piece from the stream of bytes chunks to
    /// the content and calculates the hash of the piece by crc32, without
    /// copying the chunks.
    #[instrument(level = "debug", skip_all)]
    pub async fn write_persistent_cache_piece_from_stream<S>(
        &self,
        task_id: &str,
        offset: u64,
        expected_length: u64,
        stream: &mut S,
    ) -> Result<super::io::WriteRangeResponse>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Unpin + ?Sized,
    {
        let task_path = self.get_persistent_cache_task_path(task_id);
        let fd = self
            .fd_cache
            .open_write(&task_path)
            .await
            .inspect_err(|err| {
                error!("open {:?} failed: {}", task_path, err);
            })?;

        super::io::write_range_from_stream(
            fd,
            offset,
            expected_length,
            self.config.storage.write_buffer_size,
            stream,
        )
        .await
        .inspect_err(|err| {
            error!("write {:?} failed: {}", task_path, err);
        })
    }

    /// Deletes the persistent cache task content.
    pub async fn delete_persistent_cache_task(&self, task_id: &str) -> Result<()> {
        info!("delete persistent cache task content: {}", task_id);
        let persistent_cache_task_path = self.get_persistent_cache_task_path(task_id);

        self.fd_cache
            .remove(&persistent_cache_task_path)
            .unwrap_or_else(|err| {
                error!(
                    "remove {:?} from fd_cache failed: {}",
                    persistent_cache_task_path, err
                );
            });

        fs::remove_file(persistent_cache_task_path.as_path())
            .await
            .inspect_err(|err| {
                error!("remove {:?} failed: {}", persistent_cache_task_path, err);
            })?;
        Ok(())
    }

    /// Returns the persistent cache task path by task id.
    fn get_persistent_cache_task_path(&self, task_id: &str) -> PathBuf {
        // The persistent cache task needs split by the first 3 characters of task id(sha256) to
        // avoid too many files in one directory.
        self.dir
            .join(super::content::DEFAULT_PERSISTENT_CACHE_TASK_DIR)
            .join(&task_id[..3])
            .join(task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content;
    use std::io::Cursor;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_create_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "60409bd0ec44160f44c53c39b3fe1c5fdfb23faded0228c68bee83bc15a200e3";
        let task_path = content.create_task(task_id, 0).await.unwrap();
        assert!(task_path.exists());
        assert_eq!(task_path, temp_dir.path().join("content/tasks/604/60409bd0ec44160f44c53c39b3fe1c5fdfb23faded0228c68bee83bc15a200e3"));

        let task_path_exists = content.create_task(task_id, 0).await.unwrap();
        assert_eq!(task_path, task_path_exists);
    }

    #[tokio::test]
    async fn test_hard_link_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4";
        content.create_task(task_id, 0).await.unwrap();

        let to = temp_dir
            .path()
            .join("c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4");
        content.hard_link_task(task_id, &to).await.unwrap();
        assert!(to.exists());

        content.hard_link_task(task_id, &to).await.unwrap();
    }

    #[tokio::test]
    async fn test_copy_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "bfd3c02fb31a7373e25b405fd5fd3082987ccfbaf210889153af9e65bbf13002";
        content.create_task(task_id, 64).await.unwrap();

        let to = temp_dir
            .path()
            .join("bfd3c02fb31a7373e25b405fd5fd3082987ccfbaf210889153af9e65bbf13002");
        content.copy_task(task_id, &to).await.unwrap();
        assert!(to.exists());
    }

    #[tokio::test]
    async fn test_delete_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "4e19f03b0fceb38f23ff4f657681472a53ef335db3660ae5494912570b7a2bb7";
        let task_path = content.create_task(task_id, 0).await.unwrap();
        assert!(task_path.exists());

        content.delete_task(task_id).await.unwrap();
        assert!(!task_path.exists());
    }

    #[tokio::test]
    async fn test_read_piece() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "c794a3bbae81e06d1c8d362509bdd42a7c105b0fb28d80ffe27f94b8f04fc845";
        content.create_task(task_id, 13).await.unwrap();

        let data = b"hello, world!";
        let mut stream = futures::stream::iter([Ok(Bytes::from_static(data))]);
        content
            .write_piece_from_stream(task_id, 0, 13, &mut stream)
            .await
            .unwrap();

        let mut reader = content.read_piece(task_id, 0, 13, None).await.unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, data);

        let mut reader = content
            .read_piece(
                task_id,
                0,
                13,
                Some(Range {
                    start: 0,
                    length: 5,
                }),
            )
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, b"hello");
    }

    #[tokio::test]
    async fn test_write_piece() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "60b48845606946cea72084f14ed5cce61ec96e69f80a30f891a6963dccfd5b4f";
        content.create_task(task_id, 4).await.unwrap();

        let data = b"test";
        let mut stream = futures::stream::iter([Ok(Bytes::from_static(data))]);
        let response = content
            .write_piece_from_stream(task_id, 0, 4, &mut stream)
            .await
            .unwrap();
        assert_eq!(response.length, 4);
        assert!(!response.hash.is_empty());
    }

    #[tokio::test]
    async fn test_create_persistent_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "c4f108ab1d2b8cfdffe89ea9676af35123fa02e3c25167d62538f630d5d44745";
        let task_path = content.create_persistent_task(task_id, 0).await.unwrap();
        assert!(task_path.exists());
        assert_eq!(task_path, temp_dir.path().join("content/persistent-tasks/c4f/c4f108ab1d2b8cfdffe89ea9676af35123fa02e3c25167d62538f630d5d44745"));

        let task_path_exists = content.create_persistent_task(task_id, 0).await.unwrap();
        assert_eq!(task_path, task_path_exists);
    }

    #[tokio::test]
    async fn test_hard_link_persistent_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "5e81970eb2b048910cc84cab026b951f2ceac0a09c72c0717193bb6e466e11cd";
        content.create_persistent_task(task_id, 0).await.unwrap();

        let to = temp_dir
            .path()
            .join("5e81970eb2b048910cc84cab026b951f2ceac0a09c72c0717193bb6e466e11cd");
        content
            .hard_link_persistent_task(task_id, &to)
            .await
            .unwrap();
        assert!(to.exists());

        content
            .hard_link_persistent_task(task_id, &to)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_copy_persistent_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "194b9c2018429689fb4e596a506c7e9db564c187b9709b55b33b96881dfb6dd5";
        content.create_persistent_task(task_id, 64).await.unwrap();

        let to = temp_dir
            .path()
            .join("194b9c2018429689fb4e596a506c7e9db564c187b9709b55b33b96881dfb6dd5");
        content.copy_persistent_task(task_id, &to).await.unwrap();
        assert!(to.exists());
    }

    #[tokio::test]
    async fn test_delete_persistent_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "17430ba545c3ce82790e9c9f77e64dca44bb6d6a0c9e18be175037c16c73713d";
        let task_path = content.create_persistent_task(task_id, 0).await.unwrap();
        assert!(task_path.exists());

        content.delete_persistent_task(task_id).await.unwrap();
        assert!(!task_path.exists());
    }

    #[tokio::test]
    async fn test_read_persistent_piece() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "9cb27a4af09aee4eb9f904170217659683f4a0ea7cd55e1a9fbcb99ddced659a";
        content.create_persistent_task(task_id, 13).await.unwrap();

        let data = b"hello, world!";
        let mut reader = Cursor::new(data);
        content
            .write_persistent_piece(task_id, 0, 13, &mut reader)
            .await
            .unwrap();

        let mut reader = content
            .read_persistent_piece(task_id, 0, 13, None)
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, data);

        let mut reader = content
            .read_persistent_piece(
                task_id,
                0,
                13,
                Some(Range {
                    start: 0,
                    length: 5,
                }),
            )
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, b"hello");
    }

    #[tokio::test]
    async fn test_write_persistent_piece() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "ca1afaf856e8a667fbd48093ca3ca1b8eeb4bf735912fbe551676bc5817a720a";
        content.create_persistent_task(task_id, 4).await.unwrap();

        let data = b"test";
        let mut reader = Cursor::new(data);
        let response = content
            .write_persistent_piece(task_id, 0, 4, &mut reader)
            .await
            .unwrap();
        assert_eq!(response.length, 4);
        assert!(!response.hash.is_empty());
    }

    #[tokio::test]
    async fn test_create_persistent_cache_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "c4f108ab1d2b8cfdffe89ea9676af35123fa02e3c25167d62538f630d5d44745";
        let task_path = content
            .create_persistent_cache_task(task_id, 0)
            .await
            .unwrap();
        assert!(task_path.exists());
        assert_eq!(task_path, temp_dir.path().join("content/persistent-cache-tasks/c4f/c4f108ab1d2b8cfdffe89ea9676af35123fa02e3c25167d62538f630d5d44745"));

        let task_path_exists = content
            .create_persistent_cache_task(task_id, 0)
            .await
            .unwrap();
        assert_eq!(task_path, task_path_exists);
    }

    #[tokio::test]
    async fn test_hard_link_persistent_cache_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "5e81970eb2b048910cc84cab026b951f2ceac0a09c72c0717193bb6e466e11cd";
        content
            .create_persistent_cache_task(task_id, 0)
            .await
            .unwrap();

        let to = temp_dir
            .path()
            .join("5e81970eb2b048910cc84cab026b951f2ceac0a09c72c0717193bb6e466e11cd");
        content
            .hard_link_persistent_cache_task(task_id, &to)
            .await
            .unwrap();
        assert!(to.exists());

        content
            .hard_link_persistent_cache_task(task_id, &to)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_copy_persistent_cache_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "194b9c2018429689fb4e596a506c7e9db564c187b9709b55b33b96881dfb6dd5";
        content
            .create_persistent_cache_task(task_id, 64)
            .await
            .unwrap();

        let to = temp_dir
            .path()
            .join("194b9c2018429689fb4e596a506c7e9db564c187b9709b55b33b96881dfb6dd5");
        content
            .copy_persistent_cache_task(task_id, &to)
            .await
            .unwrap();
        assert!(to.exists());
    }

    #[tokio::test]
    async fn test_delete_persistent_cache_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "17430ba545c3ce82790e9c9f77e64dca44bb6d6a0c9e18be175037c16c73713d";
        let task_path = content
            .create_persistent_cache_task(task_id, 0)
            .await
            .unwrap();
        assert!(task_path.exists());

        content.delete_persistent_cache_task(task_id).await.unwrap();
        assert!(!task_path.exists());
    }

    #[tokio::test]
    async fn test_read_persistent_cache_piece() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "9cb27a4af09aee4eb9f904170217659683f4a0ea7cd55e1a9fbcb99ddced659a";
        content
            .create_persistent_cache_task(task_id, 13)
            .await
            .unwrap();

        let data = b"hello, world!";
        let mut reader = Cursor::new(data);
        content
            .write_persistent_cache_piece(task_id, 0, 13, &mut reader)
            .await
            .unwrap();

        let mut reader = content
            .read_persistent_cache_piece(task_id, 0, 13, None)
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, data);

        let mut reader = content
            .read_persistent_cache_piece(
                task_id,
                0,
                13,
                Some(Range {
                    start: 0,
                    length: 5,
                }),
            )
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, b"hello");
    }

    #[tokio::test]
    async fn test_write_persistent_cache_piece() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "ca1afaf856e8a667fbd48093ca3ca1b8eeb4bf735912fbe551676bc5817a720a";
        content
            .create_persistent_cache_task(task_id, 4)
            .await
            .unwrap();

        let data = b"test";
        let mut reader = Cursor::new(data);
        let response = content
            .write_persistent_cache_piece(task_id, 0, 4, &mut reader)
            .await
            .unwrap();
        assert_eq!(response.length, 4);
        assert!(!response.hash.is_empty());
    }

    #[tokio::test]
    async fn test_has_enough_space() {
        let config = Arc::new(Config::default());
        let temp_dir = tempdir().unwrap();
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let has_space = content.has_enough_space(1).unwrap();
        assert!(has_space);

        let has_space = content.has_enough_space(u64::MAX).unwrap();
        assert!(!has_space);

        let mut config = Config::default();
        config.gc.policy.disk_threshold = ByteSize::mib(10);
        let config = Arc::new(config);
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let file_path = Path::new(temp_dir.path())
            .join(content::DEFAULT_CONTENT_DIR)
            .join(content::DEFAULT_TASK_DIR)
            .join("1mib");
        let mut file = fs::File::create(&file_path).await.unwrap();
        let buffer = vec![0u8; ByteSize::mib(1).as_u64() as usize];
        file.write_all(&buffer).await.unwrap();
        file.flush().await.unwrap();

        let has_space = content
            .has_enough_space(ByteSize::mib(9).as_u64() + 1)
            .unwrap();
        assert!(!has_space);

        let has_space = content.has_enough_space(ByteSize::mib(9).as_u64()).unwrap();
        assert!(has_space);
    }

    /// TEST_WINDOW_TIMEOUT is generous because these windows are already queued; the tests are
    /// about the length and digest checks, not about the wait.
    #[cfg(feature = "rdma")]
    const TEST_WINDOW_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    /// rdma_stream_reader hands the content layer windows that are already "received".
    #[cfg(feature = "rdma")]
    async fn rdma_stream_reader(payloads: &[&[u8]]) -> crate::client::rdma::RDMAStreamReader {
        let fabric = crate::rdma::fabric::Fabric::new(None, None, 1024 * 1024, true)
            .expect("libfabric endpoint");

        let mut windows = Vec::new();
        for payload in payloads {
            let mut window = fabric.acquire_buffer(payload.len()).await.unwrap();
            // Safety: this lease has not been posted.
            unsafe { window.as_mut_slice() }.copy_from_slice(payload);
            windows.push(window);
        }

        crate::client::rdma::RDMAStreamReader::from_windows(windows)
    }

    #[cfg(feature = "rdma")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_piece_from_rdma_stream() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "8ab7e2a2c1b7a5b19a4b3f7f2a9e6c1d3f5a7b9c1e3d5f7a9b1c3e5d7f9a1b3c";
        content.create_task(task_id, 9).await.unwrap();

        let mut reader = rdma_stream_reader(&[b"rdma", b"-win"]).await;
        let response = content
            .write_piece_from_rdma_stream("piece", task_id, 1, 8, &mut reader, TEST_WINDOW_TIMEOUT)
            .await
            .unwrap();

        assert_eq!(response.length, 8);
        assert_eq!(
            response.hash,
            crc32fast::hash(b"rdma-win").to_string(),
            "hash must cover the windows in order"
        );

        // The stream started at offset 1, so byte 0 must be untouched.
        let written = tokio::fs::read(content.get_task_path(task_id))
            .await
            .unwrap();
        assert_eq!(&written[1..9], b"rdma-win");
    }

    /// A parent that streams more than the piece length must be rejected before it can overwrite
    /// the pieces that follow it in the task file.
    #[cfg(feature = "rdma")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_piece_from_rdma_stream_rejects_overlong_stream() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "1cd7e2a2c1b7a5b19a4b3f7f2a9e6c1d3f5a7b9c1e3d5f7a9b1c3e5d7f9a1b3c";
        content.create_task(task_id, 8).await.unwrap();

        let mut reader = rdma_stream_reader(&[b"0123", b"4567"]).await;
        let Err(err) = content
            .write_piece_from_rdma_stream("piece", task_id, 0, 6, &mut reader, TEST_WINDOW_TIMEOUT)
            .await
        else {
            panic!("stream longer than the piece must fail");
        };
        assert!(
            err.to_string().contains("exceeded expected length"),
            "unexpected error: {err}"
        );

        // Only the first window may have landed; the tail of the task file is still zeroed.
        let written = tokio::fs::read(content.get_task_path(task_id))
            .await
            .unwrap();
        assert_eq!(&written[4..], &[0u8; 4]);
    }

    #[cfg(feature = "rdma")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_piece_from_rdma_stream_rejects_short_stream() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let task_id = "2ed7e2a2c1b7a5b19a4b3f7f2a9e6c1d3f5a7b9c1e3d5f7a9b1c3e5d7f9a1b3c";
        content.create_task(task_id, 8).await.unwrap();

        let mut reader = rdma_stream_reader(&[b"0123"]).await;
        let Err(err) = content
            .write_piece_from_rdma_stream("piece", task_id, 0, 8, &mut reader, TEST_WINDOW_TIMEOUT)
            .await
        else {
            panic!("stream shorter than the piece must fail");
        };
        assert!(
            err.to_string().contains("expected length 8 but got 4"),
            "unexpected error: {err}"
        );
    }
}
