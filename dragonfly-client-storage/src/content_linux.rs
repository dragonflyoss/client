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

use crate::encryption::{write_encrypted_range, EncryptedRangeReader, Encryptor};
use crate::io::{RangeReader, WriteRangeResponse};
use bytes::Bytes;
use bytesize::ByteSize;
use dragonfly_api::common::v2::Range;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_config::MIN_PIECE_LENGTH;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::buffer_pool::BufferPool;
use dragonfly_client_util::fs::fd::{FDCache, DEFAULT_FD_CACHE_CAPACITY};
use dragonfly_client_util::fs::{fadvise_dontneed, fadvise_willneed, fallocate};
use futures::Stream;
use std::cmp::max;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;
use tracing::{debug, error, info, instrument, warn};
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

    /// Initiates writeback of written piece ranges per storage.writebackMode.
    writeback: super::content::Writeback,

    /// The encryptor of the content at rest, when enabled.
    encryptor: Option<Arc<Encryptor>>,
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

        let encryptor = Encryptor::from_config(&config).await?.map(Arc::new);
        if encryptor.is_some() {
            info!("content encryption at rest enabled");
        }

        Ok(Content {
            encryptor,
            buffer_pool: BufferPool::new(
                super::content::MAX_BUFFER_POOL_IDLE_BUFFERS
                    * max(
                        config.storage.write_buffer_size,
                        config.storage.read_buffer_size,
                    ),
            ),
            writeback: super::content::Writeback::new(config.storage.writeback_mode),
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

    /// Returns whether the content is encrypted at rest.
    pub fn is_encrypted(&self) -> bool {
        self.encryptor.is_some()
    }

    /// Checks whether the content file is usable under the current encryption
    /// configuration: a plaintext file when the encryption is disabled, or an
    /// encrypted file whose data key the key provider unwraps when it is
    /// enabled.
    async fn is_encryption_compatible(&self, path: &Path) -> Result<bool> {
        match &self.encryptor {
            Some(encryptor) => encryptor.is_readable(path).await,
            None => Ok(!Encryptor::is_encrypted_file(path).await?),
        }
    }

    /// Checks whether the task content is usable under the current encryption
    /// configuration.
    pub async fn is_task_encryption_compatible(&self, task_id: &str) -> Result<bool> {
        self.is_encryption_compatible(&self.get_task_path(task_id))
            .await
    }

    /// Checks whether the persistent task content is usable under the current
    /// encryption configuration.
    pub async fn is_persistent_task_encryption_compatible(&self, task_id: &str) -> Result<bool> {
        self.is_encryption_compatible(&self.get_persistent_task_path(task_id))
            .await
    }

    /// Checks whether the persistent cache task content is usable under the
    /// current encryption configuration.
    pub async fn is_persistent_cache_task_encryption_compatible(
        &self,
        task_id: &str,
    ) -> Result<bool> {
        self.is_encryption_compatible(&self.get_persistent_cache_task_path(task_id))
            .await
    }

    /// Returns the range on disk holding the piece at `offset` with `length`
    /// bytes of the content file: the plaintext range itself, or the
    /// ciphertext of its chunks when the content is encrypted at rest. It
    /// falls back to the plaintext range when the layout is unavailable, as
    /// the callers only advise the kernel about it.
    async fn piece_disk_range(&self, path: &Path, offset: u64, length: u64) -> (u64, u64) {
        if let Some(encryptor) = &self.encryptor {
            if let Ok(cipher) = encryptor.cipher(path).await {
                if let Ok(range) = cipher.layout().piece_disk_range(offset, length) {
                    return range;
                }
            }
        }

        (offset, length)
    }

    /// Returns the range on disk holding the piece of the task content.
    pub async fn task_piece_disk_range(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
    ) -> (u64, u64) {
        self.piece_disk_range(&self.get_task_path(task_id), offset, length)
            .await
    }

    /// Returns the range on disk holding the piece of the persistent task
    /// content.
    pub async fn persistent_task_piece_disk_range(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
    ) -> (u64, u64) {
        self.piece_disk_range(&self.get_persistent_task_path(task_id), offset, length)
            .await
    }

    /// Returns the range on disk holding the piece of the persistent cache
    /// task content.
    pub async fn persistent_cache_task_piece_disk_range(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
    ) -> (u64, u64) {
        self.piece_disk_range(
            &self.get_persistent_cache_task_path(task_id),
            offset,
            length,
        )
        .await
    }

    /// Creates the content file of a task, preallocated to the length of the
    /// content, and encrypted when the encryption at rest is enabled.
    async fn create_file(&self, path: &Path, piece_length: u64, length: u64) -> Result<()> {
        if let Some(encryptor) = &self.encryptor {
            return encryptor.create_file(path, piece_length, length).await;
        }

        let f = fs::File::create(path).await.inspect_err(|err| {
            error!("create {:?} failed: {}", path, err);
        })?;

        fallocate(&f, length).await.inspect_err(|err| {
            error!("fallocate {:?} failed: {}", path, err);
        })?;

        Ok(())
    }

    /// Refuses the hard links of the content encrypted at rest, since a hard
    /// link would expose the ciphertext or import a plaintext file into the
    /// encrypted storage. The callers fall back to copying the content.
    fn check_hard_link(&self) -> Result<()> {
        if self.encryptor.is_some() {
            debug!("hard link is not supported for the content encrypted at rest");
            return Err(Error::Encryption(
                "hard link is not supported for the content encrypted at rest".to_string(),
            ));
        }

        Ok(())
    }

    /// Copies the content file to the destination, decrypting it when the
    /// encryption at rest is enabled.
    async fn copy_file(&self, path: &Path, to: &Path) -> Result<()> {
        let length = match &self.encryptor {
            Some(encryptor) => {
                encryptor
                    .copy_file(
                        path,
                        to,
                        self.config.storage.read_buffer_size,
                        &self.buffer_pool,
                    )
                    .await?
            }
            None => fs::copy(path, to).await?,
        };

        // Triggers writeback of the copied content per storage.writebackMode.
        if let Ok(f) = fs::File::open(to).await {
            self.writeback
                .trigger(&Arc::new(f.into_std().await), 0, length)
                .await;
        }

        info!("copy to {:?} success", to);
        Ok(())
    }

    /// Returns the reader of the plaintext range `[offset, offset + length)` of
    /// the piece at `piece_offset` with `piece_length` bytes of the content
    /// file, decrypting it when the encryption at rest is enabled.
    async fn read_range(
        &self,
        path: &Path,
        piece_offset: u64,
        piece_length: u64,
        offset: u64,
        length: u64,
    ) -> Result<RangeReader> {
        let fd = self.fd_cache.open(path).await.inspect_err(|err| {
            error!("open {:?} failed: {}", path, err);
        })?;

        if let Some(encryptor) = &self.encryptor {
            let cipher = encryptor.cipher(path).await?;
            let reader = EncryptedRangeReader::new(
                fd.clone(),
                cipher,
                piece_offset,
                piece_length,
                offset,
                length,
                self.config.storage.read_buffer_size,
                self.buffer_pool.clone(),
            )?;

            // Queue readahead of the ciphertext of the range explicitly, see
            // the plaintext case below.
            if length >= MIN_PIECE_LENGTH {
                let (ciphertext_offset, ciphertext_length) = reader.ciphertext_span();
                fadvise_willneed(&fd, ciphertext_offset, ciphertext_length)
                    .await
                    .unwrap_or_else(|err| warn!("fadvise_willneed failed: {}", err));
            }

            return Ok(RangeReader::Encrypted(Box::new(reader)));
        }

        // Queue readahead of the range explicitly, since interleaved uploads
        // on the shared descriptor break the sequential detection. Skip the
        // small ranges, which the kernel readahead covers quickly.
        if length >= MIN_PIECE_LENGTH {
            fadvise_willneed(&fd, offset, length)
                .await
                .unwrap_or_else(|err| warn!("fadvise_willneed failed: {}", err));
        }

        Ok(RangeReader::new(
            fd,
            offset,
            length,
            self.config.storage.read_buffer_size,
            self.buffer_pool.clone(),
        ))
    }

    /// Writes the piece at `offset` from the reader to the content file and
    /// calculates the hash of the piece by crc32, encrypting it when the
    /// encryption at rest is enabled, and triggers writeback of the written
    /// range per storage.writebackMode.
    async fn write_range<R: AsyncRead + Unpin + ?Sized>(
        &self,
        path: &Path,
        offset: u64,
        expected_length: u64,
        reader: &mut R,
    ) -> Result<WriteRangeResponse> {
        let fd = self.fd_cache.open_write(path).await.inspect_err(|err| {
            error!("open {:?} failed: {}", path, err);
        })?;

        if let Some(encryptor) = &self.encryptor {
            let cipher = encryptor.cipher(path).await?;
            let response = write_encrypted_range(
                fd.clone(),
                cipher.clone(),
                offset,
                expected_length,
                self.config.storage.write_buffer_size,
                reader,
                &self.buffer_pool,
            )
            .await
            .inspect_err(|err| {
                error!("write {:?} failed: {}", path, err);
            })?;

            let (disk_offset, disk_length) =
                cipher.layout().piece_disk_range(offset, response.length)?;
            self.writeback.trigger(&fd, disk_offset, disk_length).await;
            return Ok(response);
        }

        let response = super::io::write_range(
            fd.clone(),
            offset,
            expected_length,
            self.config.storage.write_buffer_size,
            reader,
            &self.buffer_pool,
        )
        .await
        .inspect_err(|err| {
            error!("write {:?} failed: {}", path, err);
        })?;

        self.writeback.trigger(&fd, offset, response.length).await;
        Ok(response)
    }

    /// Writes the piece at `offset` from the stream of bytes chunks to the
    /// content file and calculates the hash of the piece by crc32, encrypting
    /// it when the encryption at rest is enabled, and triggers writeback of
    /// the written range per storage.writebackMode.
    async fn write_range_from_stream<S>(
        &self,
        path: &Path,
        offset: u64,
        expected_length: u64,
        stream: &mut S,
    ) -> Result<WriteRangeResponse>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Unpin + ?Sized,
    {
        if self.encryptor.is_some() {
            let mut reader = StreamReader::new(&mut *stream);
            return self
                .write_range(path, offset, expected_length, &mut reader)
                .await;
        }

        let fd = self.fd_cache.open_write(path).await.inspect_err(|err| {
            error!("open {:?} failed: {}", path, err);
        })?;

        let response = super::io::write_range_from_stream(
            fd.clone(),
            offset,
            expected_length,
            self.config.storage.write_buffer_size,
            stream,
        )
        .await
        .inspect_err(|err| {
            error!("write {:?} failed: {}", path, err);
        })?;

        self.writeback.trigger(&fd, offset, response.length).await;
        Ok(response)
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
    pub async fn create_task(
        &self,
        task_id: &str,
        piece_length: u64,
        length: u64,
    ) -> Result<PathBuf> {
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

        self.create_file(&task_path, piece_length, length).await?;
        Ok(task_path)
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
        self.check_hard_link()?;

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
        self.copy_file(&self.get_task_path(task_id), to).await
    }

    /// Deletes the task content.
    pub async fn delete_task(&self, task_id: &str) -> Result<()> {
        info!("delete task content: {}", task_id);
        let task_path = self.get_task_path(task_id);

        self.fd_cache.remove(&task_path).unwrap_or_else(|err| {
            error!("remove {:?} from fd_cache failed: {}", task_path, err);
        });

        if let Some(encryptor) = &self.encryptor {
            encryptor.evict(&task_path);
        }

        fs::remove_file(task_path.as_path())
            .await
            .inspect_err(|err| {
                error!("remove {:?} failed: {}", task_path, err);
            })?;
        Ok(())
    }

    /// Drops the cached pages of the task content.
    #[instrument(level = "debug", skip_all)]
    pub async fn fadvise_dontneed_task(&self, task_id: &str) -> Result<()> {
        let f = fs::File::open(self.get_task_path(task_id)).await?;
        fadvise_dontneed(&f).await
    }

    /// Reads the piece from the content.
    #[instrument(level = "debug", skip_all)]
    pub async fn read_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<RangeReader> {
        let task_path = self.get_task_path(task_id);

        // Calculate the target offset and length based on the range.
        let (target_offset, target_length) =
            super::content::calculate_piece_range(offset, length, range);

        self.read_range(&task_path, offset, length, target_offset, target_length)
            .await
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
    ) -> Result<WriteRangeResponse>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Unpin + ?Sized,
    {
        let task_path = self.get_task_path(task_id);
        self.write_range_from_stream(&task_path, offset, expected_length, stream)
            .await
    }

    /// Returns the task path by task id.
    pub(crate) fn get_task_path(&self, task_id: &str) -> PathBuf {
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
    pub async fn create_persistent_task(
        &self,
        task_id: &str,
        piece_length: u64,
        length: u64,
    ) -> Result<PathBuf> {
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

        self.create_file(&task_path, piece_length, length).await?;
        Ok(task_path)
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
        self.check_hard_link()?;

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
        self.check_hard_link()?;

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
        self.copy_file(&self.get_persistent_task_path(task_id), to)
            .await
    }

    /// Reads the persistent piece from the content.
    #[instrument(level = "debug", skip_all)]
    pub async fn read_persistent_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<RangeReader> {
        let task_path = self.get_persistent_task_path(task_id);

        // Calculate the target offset and length based on the range.
        let (target_offset, target_length) =
            super::content::calculate_piece_range(offset, length, range);

        self.read_range(&task_path, offset, length, target_offset, target_length)
            .await
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
    ) -> Result<WriteRangeResponse> {
        let task_path = self.get_persistent_task_path(task_id);
        self.write_range(&task_path, offset, expected_length, reader)
            .await
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
    ) -> Result<WriteRangeResponse>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Unpin + ?Sized,
    {
        let task_path = self.get_persistent_task_path(task_id);
        self.write_range_from_stream(&task_path, offset, expected_length, stream)
            .await
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

        if let Some(encryptor) = &self.encryptor {
            encryptor.evict(&persistent_task_path);
        }

        fs::remove_file(persistent_task_path.as_path())
            .await
            .inspect_err(|err| {
                error!("remove {:?} failed: {}", persistent_task_path, err);
            })?;
        Ok(())
    }

    /// Drops the cached pages of the persistent task content.
    #[instrument(level = "debug", skip_all)]
    pub async fn fadvise_dontneed_persistent_task(&self, task_id: &str) -> Result<()> {
        let f = fs::File::open(self.get_persistent_task_path(task_id)).await?;
        fadvise_dontneed(&f).await
    }

    /// Returns the persistent task path by task id.
    pub(crate) fn get_persistent_task_path(&self, task_id: &str) -> PathBuf {
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
        piece_length: u64,
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

        self.create_file(&task_path, piece_length, length).await?;
        Ok(task_path)
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
        self.check_hard_link()?;

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
        self.check_hard_link()?;

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
        self.copy_file(&self.get_persistent_cache_task_path(task_id), to)
            .await
    }

    /// Reads the persistent cache piece from the content.
    #[instrument(level = "debug", skip_all)]
    pub async fn read_persistent_cache_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<RangeReader> {
        let task_path = self.get_persistent_cache_task_path(task_id);

        // Calculate the target offset and length based on the range.
        let (target_offset, target_length) =
            super::content::calculate_piece_range(offset, length, range);

        self.read_range(&task_path, offset, length, target_offset, target_length)
            .await
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
    ) -> Result<WriteRangeResponse> {
        let task_path = self.get_persistent_cache_task_path(task_id);
        self.write_range(&task_path, offset, expected_length, reader)
            .await
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
    ) -> Result<WriteRangeResponse>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Unpin + ?Sized,
    {
        let task_path = self.get_persistent_cache_task_path(task_id);
        self.write_range_from_stream(&task_path, offset, expected_length, stream)
            .await
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

        if let Some(encryptor) = &self.encryptor {
            encryptor.evict(&persistent_cache_task_path);
        }

        fs::remove_file(persistent_cache_task_path.as_path())
            .await
            .inspect_err(|err| {
                error!("remove {:?} failed: {}", persistent_cache_task_path, err);
            })?;
        Ok(())
    }

    /// Drops the cached pages of the persistent cache task content.
    #[instrument(level = "debug", skip_all)]
    pub async fn fadvise_dontneed_persistent_cache_task(&self, task_id: &str) -> Result<()> {
        let f = fs::File::open(self.get_persistent_cache_task_path(task_id)).await?;
        fadvise_dontneed(&f).await
    }

    /// Returns the persistent cache task path by task id.
    pub(crate) fn get_persistent_cache_task_path(&self, task_id: &str) -> PathBuf {
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
    use crate::content::DEFAULT_TASK_DIR;
    use dragonfly_client_config::dfdaemon::WritebackMode;
    use std::io::Cursor;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn content(config: Config, dir: &Path) -> Content {
        Content::new(Arc::new(config), dir).await.unwrap()
    }

    #[tokio::test]
    async fn new_wipes_the_content_dir_unless_keep_is_set() {
        let test_cases = vec![(false, false), (true, true)];

        for (keep, expected_exists) in test_cases {
            let temp_dir = tempdir().unwrap();
            let task_id = "60409bd0ec44160f44c53c39b3fe1c5fdfb23faded0228c68bee83bc15a200e3";
            let task_path = content(Config::default(), temp_dir.path())
                .await
                .create_task(task_id, 0, 0)
                .await
                .unwrap();
            assert!(task_path.exists());

            let mut config = Config::default();
            config.storage.keep = keep;
            content(config, temp_dir.path()).await;
            assert_eq!(task_path.exists(), expected_exists);
        }
    }

    #[tokio::test]
    async fn create_task_reuses_the_existing_path() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "60409bd0ec44160f44c53c39b3fe1c5fdfb23faded0228c68bee83bc15a200e3";
        let task_path = content.create_task(task_id, 0, 0).await.unwrap();
        assert!(task_path.exists());
        assert_eq!(task_path, temp_dir.path().join("content/tasks/604/60409bd0ec44160f44c53c39b3fe1c5fdfb23faded0228c68bee83bc15a200e3"));

        let task_path_exists = content.create_task(task_id, 0, 0).await.unwrap();
        assert_eq!(task_path, task_path_exists);
    }

    #[tokio::test]
    async fn hard_link_task_links_and_accepts_the_existing_link() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4";
        content.create_task(task_id, 0, 0).await.unwrap();

        let to = temp_dir
            .path()
            .join("c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4");
        content.hard_link_task(task_id, &to).await.unwrap();
        assert!(to.exists());
        assert!(content
            .is_same_dev_inode_as_task(task_id, &to)
            .await
            .unwrap());

        content.hard_link_task(task_id, &to).await.unwrap();
    }

    #[tokio::test]
    async fn copy_task_copies_the_content_to_the_destination() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "bfd3c02fb31a7373e25b405fd5fd3082987ccfbaf210889153af9e65bbf13002";
        content.create_task(task_id, 64, 64).await.unwrap();

        let to = temp_dir
            .path()
            .join("bfd3c02fb31a7373e25b405fd5fd3082987ccfbaf210889153af9e65bbf13002");
        content.copy_task(task_id, &to).await.unwrap();
        assert!(to.exists());
    }

    #[tokio::test]
    async fn delete_task_removes_the_content() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "4e19f03b0fceb38f23ff4f657681472a53ef335db3660ae5494912570b7a2bb7";
        let task_path = content.create_task(task_id, 0, 0).await.unwrap();
        assert!(task_path.exists());

        content.delete_task(task_id).await.unwrap();
        assert!(!task_path.exists());
    }

    #[tokio::test]
    async fn write_piece_from_stream_reads_back_in_every_writeback_mode() {
        let test_cases = vec![
            WritebackMode::Sync,
            WritebackMode::Async,
            WritebackMode::Off,
        ];

        for writeback_mode in test_cases {
            let temp_dir = tempdir().unwrap();
            let mut config = Config::default();
            config.storage.writeback_mode = writeback_mode;
            let content = content(config, temp_dir.path()).await;

            let task_id = "60409bd0ec44160f44c53c39b3fe1c5fdfb23faded0228c68bee83bc15a200e3";
            content.create_task(task_id, 13, 13).await.unwrap();

            let data = b"hello, world!";
            let response = content
                .write_piece_from_stream(
                    task_id,
                    0,
                    13,
                    &mut futures::stream::iter([Ok(Bytes::from_static(data))]),
                )
                .await
                .unwrap();
            assert_eq!(response.length, 13);

            if writeback_mode == WritebackMode::Async {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }

            let mut reader = content.read_piece(task_id, 0, 13, None).await.unwrap();
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, data);
        }
    }

    #[tokio::test]
    async fn fadvise_dontneed_task_keeps_content_and_rejects_missing_task() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
        content.create_task(task_id, 13, 13).await.unwrap();

        let data = b"hello, world!";
        content
            .write_piece_from_stream(
                task_id,
                0,
                13,
                &mut futures::stream::iter([Ok(Bytes::from_static(data))]),
            )
            .await
            .unwrap();

        let test_cases = vec![
            (task_id, true),
            (
                "aaa6963dccfd5b4f60b48845606946cea72084f14ed5cce61ec96e69f80a30f8",
                false,
            ),
        ];

        for (task_id, expected_ok) in test_cases {
            assert_eq!(
                content.fadvise_dontneed_task(task_id).await.is_ok(),
                expected_ok
            );
        }

        let mut reader = content.read_piece(task_id, 0, 13, None).await.unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, data);
    }

    #[tokio::test]
    async fn fadvise_dontneed_persistent_task_requires_the_content() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        content
            .create_persistent_task(task_id, 13, 13)
            .await
            .unwrap();

        let test_cases = vec![
            (task_id, true),
            (
                "aaa6963dccfd5b4f60b48845606946cea72084f14ed5cce61ec96e69f80a30f8",
                false,
            ),
        ];

        for (task_id, expected_ok) in test_cases {
            assert_eq!(
                content
                    .fadvise_dontneed_persistent_task(task_id)
                    .await
                    .is_ok(),
                expected_ok
            );
        }
    }

    #[tokio::test]
    async fn fadvise_dontneed_persistent_cache_task_requires_the_content() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        content
            .create_persistent_cache_task(task_id, 13, 13)
            .await
            .unwrap();

        let test_cases = vec![
            (task_id, true),
            (
                "aaa6963dccfd5b4f60b48845606946cea72084f14ed5cce61ec96e69f80a30f8",
                false,
            ),
        ];

        for (task_id, expected_ok) in test_cases {
            assert_eq!(
                content
                    .fadvise_dontneed_persistent_cache_task(task_id)
                    .await
                    .is_ok(),
                expected_ok
            );
        }
    }

    #[tokio::test]
    async fn read_piece_reads_the_requested_range() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "c794a3bbae81e06d1c8d362509bdd42a7c105b0fb28d80ffe27f94b8f04fc845";
        content.create_task(task_id, 13, 13).await.unwrap();
        content
            .write_piece_from_stream(
                task_id,
                0,
                13,
                &mut futures::stream::iter([Ok(Bytes::from_static(b"hello, world!"))]),
            )
            .await
            .unwrap();

        let test_cases = vec![
            (None, &b"hello, world!"[..]),
            (
                Some(Range {
                    start: 0,
                    length: 5,
                }),
                &b"hello"[..],
            ),
            (
                Some(Range {
                    start: 7,
                    length: 6,
                }),
                &b"world!"[..],
            ),
        ];

        for (range, expected) in test_cases {
            let mut reader = content.read_piece(task_id, 0, 13, range).await.unwrap();
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, expected);
        }
    }

    #[tokio::test]
    async fn write_piece_from_stream_returns_length_and_crc32() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "60b48845606946cea72084f14ed5cce61ec96e69f80a30f891a6963dccfd5b4f";
        content.create_task(task_id, 4, 4).await.unwrap();

        let response = content
            .write_piece_from_stream(
                task_id,
                0,
                4,
                &mut futures::stream::iter([Ok(Bytes::from_static(b"test"))]),
            )
            .await
            .unwrap();
        assert_eq!(response.length, 4);
        assert_eq!(response.hash, "3632233996");
    }

    #[tokio::test]
    async fn create_persistent_task_reuses_the_existing_path() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "c4f108ab1d2b8cfdffe89ea9676af35123fa02e3c25167d62538f630d5d44745";
        let task_path = content.create_persistent_task(task_id, 0, 0).await.unwrap();
        assert!(task_path.exists());
        assert_eq!(task_path, temp_dir.path().join("content/persistent-tasks/c4f/c4f108ab1d2b8cfdffe89ea9676af35123fa02e3c25167d62538f630d5d44745"));

        let task_path_exists = content.create_persistent_task(task_id, 0, 0).await.unwrap();
        assert_eq!(task_path, task_path_exists);
    }

    #[tokio::test]
    async fn hard_link_persistent_task_links_and_accepts_the_existing_link() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "5e81970eb2b048910cc84cab026b951f2ceac0a09c72c0717193bb6e466e11cd";
        content.create_persistent_task(task_id, 0, 0).await.unwrap();

        let to = temp_dir
            .path()
            .join("5e81970eb2b048910cc84cab026b951f2ceac0a09c72c0717193bb6e466e11cd");
        content
            .hard_link_persistent_task(task_id, &to)
            .await
            .unwrap();
        assert!(to.exists());
        assert!(content
            .is_same_dev_inode_as_persistent_task(task_id, &to)
            .await
            .unwrap());

        content
            .hard_link_persistent_task(task_id, &to)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn hard_link_to_persistent_task_links_a_single_source() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592";
        let task_dir = content.create_persistent_task_dir(task_id).await.unwrap();
        assert_eq!(
            task_dir,
            temp_dir.path().join("content/persistent-tasks/d7a")
        );

        let from = temp_dir.path().join("source");
        fs::write(&from, b"hello, world!").await.unwrap();
        content
            .hard_link_to_persistent_task(&from, task_id)
            .await
            .unwrap();
        assert!(content
            .is_same_dev_inode_as_persistent_task(task_id, &from)
            .await
            .unwrap());

        content
            .hard_link_to_persistent_task(&from, task_id)
            .await
            .unwrap();

        let other_source = temp_dir.path().join("other-source");
        fs::write(&other_source, b"other").await.unwrap();
        assert!(content
            .hard_link_to_persistent_task(&other_source, task_id)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn copy_persistent_task_copies_the_content_to_the_destination() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "194b9c2018429689fb4e596a506c7e9db564c187b9709b55b33b96881dfb6dd5";
        content
            .create_persistent_task(task_id, 64, 64)
            .await
            .unwrap();

        let to = temp_dir
            .path()
            .join("194b9c2018429689fb4e596a506c7e9db564c187b9709b55b33b96881dfb6dd5");
        content.copy_persistent_task(task_id, &to).await.unwrap();
        assert!(to.exists());
    }

    #[tokio::test]
    async fn delete_persistent_task_removes_the_content() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "17430ba545c3ce82790e9c9f77e64dca44bb6d6a0c9e18be175037c16c73713d";
        let task_path = content.create_persistent_task(task_id, 0, 0).await.unwrap();
        assert!(task_path.exists());

        content.delete_persistent_task(task_id).await.unwrap();
        assert!(!task_path.exists());
    }

    #[tokio::test]
    async fn read_persistent_piece_reads_the_requested_range() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "9cb27a4af09aee4eb9f904170217659683f4a0ea7cd55e1a9fbcb99ddced659a";
        content
            .create_persistent_task(task_id, 13, 13)
            .await
            .unwrap();
        content
            .write_persistent_piece(task_id, 0, 13, &mut Cursor::new(b"hello, world!"))
            .await
            .unwrap();

        let test_cases = vec![
            (None, &b"hello, world!"[..]),
            (
                Some(Range {
                    start: 0,
                    length: 5,
                }),
                &b"hello"[..],
            ),
            (
                Some(Range {
                    start: 7,
                    length: 6,
                }),
                &b"world!"[..],
            ),
        ];

        for (range, expected) in test_cases {
            let mut reader = content
                .read_persistent_piece(task_id, 0, 13, range)
                .await
                .unwrap();
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, expected);
        }
    }

    #[tokio::test]
    async fn write_persistent_piece_hashes_reader_and_stream_input() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "ca1afaf856e8a667fbd48093ca3ca1b8eeb4bf735912fbe551676bc5817a720a";
        content.create_persistent_task(task_id, 4, 4).await.unwrap();

        let response = content
            .write_persistent_piece(task_id, 0, 4, &mut Cursor::new(b"test"))
            .await
            .unwrap();
        assert_eq!(response.length, 4);
        assert_eq!(response.hash, "3632233996");

        let response = content
            .write_persistent_piece_from_stream(
                task_id,
                0,
                4,
                &mut futures::stream::iter([Ok(Bytes::from_static(b"test"))]),
            )
            .await
            .unwrap();
        assert_eq!(response.length, 4);
        assert_eq!(response.hash, "3632233996");
    }

    #[tokio::test]
    async fn create_persistent_cache_task_reuses_the_existing_path() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "c4f108ab1d2b8cfdffe89ea9676af35123fa02e3c25167d62538f630d5d44745";
        let task_path = content
            .create_persistent_cache_task(task_id, 0, 0)
            .await
            .unwrap();
        assert!(task_path.exists());
        assert_eq!(task_path, temp_dir.path().join("content/persistent-cache-tasks/c4f/c4f108ab1d2b8cfdffe89ea9676af35123fa02e3c25167d62538f630d5d44745"));

        let task_path_exists = content
            .create_persistent_cache_task(task_id, 0, 0)
            .await
            .unwrap();
        assert_eq!(task_path, task_path_exists);
    }

    #[tokio::test]
    async fn hard_link_persistent_cache_task_links_and_accepts_the_existing_link() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "5e81970eb2b048910cc84cab026b951f2ceac0a09c72c0717193bb6e466e11cd";
        content
            .create_persistent_cache_task(task_id, 0, 0)
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
        assert!(content
            .is_same_dev_inode_as_persistent_cache_task(task_id, &to)
            .await
            .unwrap());

        content
            .hard_link_persistent_cache_task(task_id, &to)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn hard_link_to_persistent_cache_task_links_a_single_source() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "ef537f25c895bfa782526529a9b63d97aa631564d5d789c2b765448c8635fb6c";
        let task_dir = content
            .create_persistent_cache_task_dir(task_id)
            .await
            .unwrap();
        assert_eq!(
            task_dir,
            temp_dir.path().join("content/persistent-cache-tasks/ef5")
        );

        let from = temp_dir.path().join("source");
        fs::write(&from, b"hello, world!").await.unwrap();
        content
            .hard_link_to_persistent_cache_task(&from, task_id)
            .await
            .unwrap();
        assert!(content
            .is_same_dev_inode_as_persistent_cache_task(task_id, &from)
            .await
            .unwrap());

        content
            .hard_link_to_persistent_cache_task(&from, task_id)
            .await
            .unwrap();

        let other_source = temp_dir.path().join("other-source");
        fs::write(&other_source, b"other").await.unwrap();
        assert!(content
            .hard_link_to_persistent_cache_task(&other_source, task_id)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn copy_persistent_cache_task_copies_the_content_to_the_destination() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "194b9c2018429689fb4e596a506c7e9db564c187b9709b55b33b96881dfb6dd5";
        content
            .create_persistent_cache_task(task_id, 64, 64)
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
    async fn delete_persistent_cache_task_removes_the_content() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "17430ba545c3ce82790e9c9f77e64dca44bb6d6a0c9e18be175037c16c73713d";
        let task_path = content
            .create_persistent_cache_task(task_id, 0, 0)
            .await
            .unwrap();
        assert!(task_path.exists());

        content.delete_persistent_cache_task(task_id).await.unwrap();
        assert!(!task_path.exists());
    }

    #[tokio::test]
    async fn read_persistent_cache_piece_reads_the_requested_range() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "9cb27a4af09aee4eb9f904170217659683f4a0ea7cd55e1a9fbcb99ddced659a";
        content
            .create_persistent_cache_task(task_id, 13, 13)
            .await
            .unwrap();
        content
            .write_persistent_cache_piece(task_id, 0, 13, &mut Cursor::new(b"hello, world!"))
            .await
            .unwrap();

        let test_cases = vec![
            (None, &b"hello, world!"[..]),
            (
                Some(Range {
                    start: 0,
                    length: 5,
                }),
                &b"hello"[..],
            ),
            (
                Some(Range {
                    start: 7,
                    length: 6,
                }),
                &b"world!"[..],
            ),
        ];

        for (range, expected) in test_cases {
            let mut reader = content
                .read_persistent_cache_piece(task_id, 0, 13, range)
                .await
                .unwrap();
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, expected);
        }
    }

    #[tokio::test]
    async fn write_persistent_cache_piece_hashes_reader_and_stream_input() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let task_id = "ca1afaf856e8a667fbd48093ca3ca1b8eeb4bf735912fbe551676bc5817a720a";
        content
            .create_persistent_cache_task(task_id, 4, 4)
            .await
            .unwrap();

        let response = content
            .write_persistent_cache_piece(task_id, 0, 4, &mut Cursor::new(b"test"))
            .await
            .unwrap();
        assert_eq!(response.length, 4);
        assert_eq!(response.hash, "3632233996");

        let response = content
            .write_persistent_cache_piece_from_stream(
                task_id,
                0,
                4,
                &mut futures::stream::iter([Ok(Bytes::from_static(b"test"))]),
            )
            .await
            .unwrap();
        assert_eq!(response.length, 4);
        assert_eq!(response.hash, "3632233996");
    }

    #[tokio::test]
    async fn has_enough_space_compares_against_the_free_space() {
        let temp_dir = tempdir().unwrap();
        let content = content(Config::default(), temp_dir.path()).await;

        let test_cases = vec![(1, true), (u64::MAX, false)];

        for (content_length, expected) in test_cases {
            assert_eq!(content.has_enough_space(content_length).unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn space_accounting_uses_disk_threshold_minus_usage() {
        let test_cases = vec![
            (
                ByteSize::mib(10),
                ByteSize::mib(9).as_u64() + 1,
                ByteSize::mib(9).as_u64(),
                false,
            ),
            (
                ByteSize::mib(10),
                ByteSize::mib(9).as_u64(),
                ByteSize::mib(9).as_u64(),
                true,
            ),
            (ByteSize::mib(1), 1, 0, false),
        ];

        for (disk_threshold, content_length, expected_available_space, expected_enough) in
            test_cases
        {
            let temp_dir = tempdir().unwrap();
            let mut config = Config::default();
            config.gc.policy.disk_threshold = disk_threshold;
            let content = content(config, temp_dir.path()).await;

            let mut file = fs::File::create(content.dir.join(DEFAULT_TASK_DIR).join("1mib"))
                .await
                .unwrap();
            file.write_all(&vec![0u8; ByteSize::mib(1).as_u64() as usize])
                .await
                .unwrap();
            file.flush().await.unwrap();

            assert_eq!(content.total_space().unwrap(), disk_threshold.as_u64());
            assert_eq!(content.available_space().unwrap(), expected_available_space);
            assert_eq!(
                content.has_enough_space(content_length).unwrap(),
                expected_enough
            );
        }
    }

    /// Returns a configuration with the encryption at rest enabled by a key
    /// encryption key written to the directory.
    async fn encrypted_config(dir: &Path) -> Arc<Config> {
        let key_file = dir.join("kek.key");
        fs::write(&key_file, [1u8; 32]).await.unwrap();

        let mut config = Config::default();
        config.storage.encryption.enable = true;
        config.storage.encryption.kms.file.key_file = Some(key_file);
        Arc::new(config)
    }

    fn pattern(length: usize) -> Vec<u8> {
        (0..length).map(|i| (i % 251) as u8).collect()
    }

    #[tokio::test]
    async fn test_encrypted_content_requires_key_file() {
        let temp_dir = tempdir().unwrap();
        let mut config = Config::default();
        config.storage.encryption.enable = true;
        assert!(Content::new(Arc::new(config), temp_dir.path())
            .await
            .is_err());

        let mut config = Config::default();
        config.storage.encryption.enable = true;
        config.storage.encryption.kms.file.key_file = Some(temp_dir.path().join("missing"));
        assert!(Content::new(Arc::new(config), temp_dir.path())
            .await
            .is_err());

        let config = encrypted_config(temp_dir.path()).await;
        let content = Content::new(config, temp_dir.path()).await.unwrap();
        assert!(content.is_encrypted());
    }

    #[tokio::test]
    async fn test_encrypted_task_write_read_copy() {
        let temp_dir = tempdir().unwrap();
        let config = encrypted_config(temp_dir.path()).await;
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        // Three pieces, the last one short, written out of order.
        let piece_length = 100_000u64;
        let data = pattern(250_000);
        let task_id = "a3f8c2d1e5b7490612fd3e9a8c7b6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c";
        let task_path = content
            .create_task(task_id, piece_length, data.len() as u64)
            .await
            .unwrap();
        assert!(task_path.exists());
        assert!(Encryptor::is_encrypted_file(&task_path).await.unwrap());
        assert!(content
            .is_task_encryption_compatible(task_id)
            .await
            .unwrap());

        for number in [2u64, 0, 1] {
            let offset = number * piece_length;
            let piece = &data
                [offset as usize..std::cmp::min(offset + piece_length, data.len() as u64) as usize];
            let mut stream = futures::stream::iter([
                Ok(Bytes::copy_from_slice(&piece[..piece.len() / 2])),
                Ok(Bytes::copy_from_slice(&piece[piece.len() / 2..])),
            ]);
            let response = content
                .write_piece_from_stream(task_id, offset, piece.len() as u64, &mut stream)
                .await
                .unwrap();
            assert_eq!(response.length, piece.len() as u64);
            assert_eq!(response.hash, crc32fast::hash(piece).to_string());
        }

        // The plaintext never reaches the disk.
        let on_disk = fs::read(&task_path).await.unwrap();
        assert!(!on_disk.windows(32).any(|window| window == &data[..32]));

        // Whole pieces and ranges within them read back as plaintext.
        for number in 0..3u64 {
            let offset = number * piece_length;
            let length = std::cmp::min(piece_length, data.len() as u64 - offset);
            let mut reader = content
                .read_piece(task_id, offset, length, None)
                .await
                .unwrap();
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.unwrap();
            assert_eq!(buffer, &data[offset as usize..(offset + length) as usize]);
        }

        let mut reader = content
            .read_piece(
                task_id,
                piece_length,
                piece_length,
                Some(Range {
                    start: piece_length + 65_000,
                    length: 2_000,
                }),
            )
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, &data[165_000..167_000]);

        // The reader has no file descriptor to send from.
        let reader = content
            .read_piece(task_id, 0, piece_length, None)
            .await
            .unwrap();
        assert!(reader.into_parts().is_err());

        // Hard links are refused and the copy decrypts.
        let to = temp_dir.path().join("output");
        assert!(content.hard_link_task(task_id, &to).await.is_err());
        assert!(!to.exists());
        content.copy_task(task_id, &to).await.unwrap();
        assert_eq!(fs::read(&to).await.unwrap(), data);
        assert!(!content
            .is_same_dev_inode_as_task(task_id, &to)
            .await
            .unwrap());

        content.fadvise_dontneed_task(task_id).await.unwrap();
        content.delete_task(task_id).await.unwrap();
        assert!(!task_path.exists());
    }

    #[tokio::test]
    async fn test_encrypted_task_survives_restart() {
        let temp_dir = tempdir().unwrap();
        let config = encrypted_config(temp_dir.path()).await;
        let mut kept = (*config).clone();
        kept.storage.keep = true;
        let config = Arc::new(kept);

        let data = pattern(70_000);
        let task_id = "b4e9d3c2f6a8501723ae4f0b9d8c7e6f5a4b3c2d1e0f9a8b7c6d5e4f3a2b1c0d";
        {
            let content = Content::new(config.clone(), temp_dir.path()).await.unwrap();
            content
                .create_task(task_id, data.len() as u64, data.len() as u64)
                .await
                .unwrap();
            let mut stream = futures::stream::iter([Ok(Bytes::copy_from_slice(&data))]);
            content
                .write_piece_from_stream(task_id, 0, data.len() as u64, &mut stream)
                .await
                .unwrap();
        }

        // A new content with the same key encryption key reads the task.
        let content = Content::new(config, temp_dir.path()).await.unwrap();
        assert!(content
            .is_task_encryption_compatible(task_id)
            .await
            .unwrap());
        let mut reader = content
            .read_piece(task_id, 0, data.len() as u64, None)
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, data);

        // A content with another key encryption key cannot read the task, and
        // a content without the encryption does not accept it either.
        let mut other = (*encrypted_config(temp_dir.path()).await).clone();
        other.storage.keep = true;
        fs::write(
            other.storage.encryption.kms.file.key_file.as_ref().unwrap(),
            [2u8; 32],
        )
        .await
        .unwrap();
        let content = Content::new(Arc::new(other), temp_dir.path())
            .await
            .unwrap();
        assert!(!content
            .is_task_encryption_compatible(task_id)
            .await
            .unwrap());
        assert!(content
            .read_piece(task_id, 0, data.len() as u64, None)
            .await
            .is_err());

        let mut plaintext = Config::default();
        plaintext.storage.keep = true;
        let content = Content::new(Arc::new(plaintext), temp_dir.path())
            .await
            .unwrap();
        assert!(!content
            .is_task_encryption_compatible(task_id)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_plaintext_task_is_incompatible_with_encryption() {
        let temp_dir = tempdir().unwrap();
        let mut plaintext = Config::default();
        plaintext.storage.keep = true;

        let task_id = "c5fa0e4d3b7962834bf5a1c0e9d8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0";
        {
            let content = Content::new(Arc::new(plaintext.clone()), temp_dir.path())
                .await
                .unwrap();
            content.create_task(task_id, 13, 13).await.unwrap();
            let mut stream = futures::stream::iter([Ok(Bytes::from_static(b"hello, world!"))]);
            content
                .write_piece_from_stream(task_id, 0, 13, &mut stream)
                .await
                .unwrap();
            assert!(content
                .is_task_encryption_compatible(task_id)
                .await
                .unwrap());

            // A missing task is an error rather than incompatible.
            assert!(content
                .is_task_encryption_compatible(
                    "aaa6963dccfd5b4f60b48845606946cea72084f14ed5cce61ec96e69f80a30f8"
                )
                .await
                .is_err());
        }

        let mut encrypted = (*encrypted_config(temp_dir.path()).await).clone();
        encrypted.storage.keep = true;
        let content = Content::new(Arc::new(encrypted), temp_dir.path())
            .await
            .unwrap();
        assert!(!content
            .is_task_encryption_compatible(task_id)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_encrypted_persistent_tasks() {
        let temp_dir = tempdir().unwrap();
        let config = encrypted_config(temp_dir.path()).await;
        let content = Content::new(config, temp_dir.path()).await.unwrap();

        let data = pattern(150_000);
        let piece_length = 100_000u64;

        // Importing a local file by hard link is refused, so the pieces are
        // written through the encrypting writer.
        let source = temp_dir.path().join("source");
        fs::write(&source, &data).await.unwrap();

        let task_id = "d6ab1f5e4c8a73945cf6b2d1f0e9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1";
        assert!(content
            .hard_link_to_persistent_task(&source, task_id)
            .await
            .is_err());
        content
            .create_persistent_task(task_id, piece_length, data.len() as u64)
            .await
            .unwrap();
        for (number, piece) in data.chunks(piece_length as usize).enumerate() {
            let offset = number as u64 * piece_length;
            let response = content
                .write_persistent_piece(
                    task_id,
                    offset,
                    piece.len() as u64,
                    &mut Cursor::new(piece.to_vec()),
                )
                .await
                .unwrap();
            assert_eq!(response.hash, crc32fast::hash(piece).to_string());
        }
        assert!(content
            .is_persistent_task_encryption_compatible(task_id)
            .await
            .unwrap());

        let mut reader = content
            .read_persistent_piece(
                task_id,
                piece_length,
                50_000,
                Some(Range {
                    start: piece_length + 10,
                    length: 100,
                }),
            )
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, &data[100_010..100_110]);

        let to = temp_dir.path().join("persistent-output");
        assert!(content
            .hard_link_persistent_task(task_id, &to)
            .await
            .is_err());
        content.copy_persistent_task(task_id, &to).await.unwrap();
        assert_eq!(fs::read(&to).await.unwrap(), data);
        content.delete_persistent_task(task_id).await.unwrap();

        // The persistent cache tasks behave the same.
        let task_id = "e7bc2a6f5d9b84a56df7c3e2a1f0b9c8d7e6f5a4b3c2d1e0f9a8b7c6d5e4f3a2";
        assert!(content
            .hard_link_to_persistent_cache_task(&source, task_id)
            .await
            .is_err());
        content
            .create_persistent_cache_task(task_id, piece_length, data.len() as u64)
            .await
            .unwrap();
        for (number, piece) in data.chunks(piece_length as usize).enumerate() {
            let offset = number as u64 * piece_length;
            let mut stream = futures::stream::iter([Ok(Bytes::copy_from_slice(piece))]);
            content
                .write_persistent_cache_piece_from_stream(
                    task_id,
                    offset,
                    piece.len() as u64,
                    &mut stream,
                )
                .await
                .unwrap();
        }
        assert!(content
            .is_persistent_cache_task_encryption_compatible(task_id)
            .await
            .unwrap());

        let mut reader = content
            .read_persistent_cache_piece(task_id, 0, piece_length, None)
            .await
            .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, &data[..piece_length as usize]);

        let to = temp_dir.path().join("persistent-cache-output");
        assert!(content
            .hard_link_persistent_cache_task(task_id, &to)
            .await
            .is_err());
        content
            .copy_persistent_cache_task(task_id, &to)
            .await
            .unwrap();
        assert_eq!(fs::read(&to).await.unwrap(), data);
        content.delete_persistent_cache_task(task_id).await.unwrap();
    }
}
