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

use bytesize::ByteSize;
use dragonfly_api::common::v2::Range;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use crate::encrypt::{DecryptReader, EncryptReader};
use dragonfly_client_util::fs::fallocate;
use tokio_util::either::Either;
use std::cmp::{max, min};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{
    self, AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom,
};
use tokio_util::io::InspectReader;
use tracing::{error, info, instrument, warn};
use walkdir::WalkDir;

/// DEFAULT_CONTENT_DIR is the default directory for store content.
pub const DEFAULT_CONTENT_DIR: &str = "content";

/// DEFAULT_TASK_DIR is the default directory for store task.
pub const DEFAULT_TASK_DIR: &str = "tasks";

/// DEFAULT_PERSISTENT_CACHE_TASK_DIR is the default directory for store persistent cache task.
pub const DEFAULT_PERSISTENT_CACHE_TASK_DIR: &str = "persistent-cache-tasks";

/// Content is the content of a piece.
pub struct Content {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// dir is the directory to store content.
    dir: PathBuf,

    /// key is the encryption key
    key: Option<Vec<u8>>,
}

/// WritePieceResponse is the response of writing a piece.
pub struct WritePieceResponse {
    /// length is the length of the piece.
    pub length: u64,

    /// hash is the hash of the piece.
    pub hash: String,
}

/// WritePersistentCacheTaskResponse is the response of writing a persistent cache task.
pub struct WritePersistentCacheTaskResponse {
    /// length is the length of the persistent cache task.
    pub length: u64,

    /// hash is the hash of the persistent cache task.
    pub hash: String,
}

/// Content implements the content storage.
impl Content {
    /// new returns a new content.
    pub async fn new(config: Arc<Config>, dir: &Path, key: Option<Vec<u8>>) -> Result<Content> {
        let dir = dir.join(DEFAULT_CONTENT_DIR);

        // If the storage is not kept, remove the directory.
        if !config.storage.keep {
            fs::remove_dir_all(&dir).await.unwrap_or_else(|err| {
                warn!("remove {:?} failed: {}", dir, err);
            });
        }

        fs::create_dir_all(&dir.join(DEFAULT_TASK_DIR)).await?;
        fs::create_dir_all(&dir.join(DEFAULT_PERSISTENT_CACHE_TASK_DIR)).await?;
        info!("content initialized directory: {:?}", dir);
        Ok(Content { config, dir, key })
    }

    /// available_space returns the available space of the disk.
    pub fn available_space(&self) -> Result<u64> {
        let dist_threshold = self.config.gc.policy.dist_threshold;
        if dist_threshold != ByteSize::default() {
            let usage_space = WalkDir::new(&self.dir)
                .into_iter()
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| entry.metadata().ok())
                .filter(|metadata| metadata.is_file())
                .fold(0, |acc, m| acc + m.len());

            if usage_space >= dist_threshold.as_u64() {
                warn!(
                    "usage space {} is greater than dist threshold {}, no need to calculate available space",
                    usage_space, dist_threshold
                );

                return Ok(0);
            }

            return Ok(dist_threshold.as_u64() - usage_space);
        }

        let stat = fs2::statvfs(&self.dir)?;
        Ok(stat.available_space())
    }

    /// total_space returns the total space of the disk.
    pub fn total_space(&self) -> Result<u64> {
        // If the dist_threshold is set, return it directly.
        let dist_threshold = self.config.gc.policy.dist_threshold;
        if dist_threshold != ByteSize::default() {
            return Ok(dist_threshold.as_u64());
        }

        let stat = fs2::statvfs(&self.dir)?;
        Ok(stat.total_space())
    }

    /// has_enough_space checks if the storage has enough space to store the content.
    pub fn has_enough_space(&self, content_length: u64) -> Result<bool> {
        let available_space = self.available_space()?;
        if available_space < content_length {
            warn!(
                "not enough space to store the persistent cache task: available_space={}, content_length={}",
                available_space, content_length
            );

            return Ok(false);
        }

        Ok(true)
    }

    /// is_same_dev_inode checks if the source and target are the same device and inode.
    async fn is_same_dev_inode<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        source: P,
        target: Q,
    ) -> Result<bool> {
        let source_metadata = fs::metadata(source).await?;
        let target_metadata = fs::metadata(target).await?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(source_metadata.dev() == target_metadata.dev()
                && source_metadata.ino() == target_metadata.ino())
        }

        #[cfg(not(unix))]
        {
            Err(Error::IO(io::Error::new(
                io::ErrorKind::Unsupported,
                "platform not supported",
            )))
        }
    }

    /// is_same_dev_inode_as_task checks if the task and target are the same device and inode.
    pub async fn is_same_dev_inode_as_task(&self, task_id: &str, to: &Path) -> Result<bool> {
        let task_path = self.get_task_path(task_id);
        self.is_same_dev_inode(&task_path, to).await
    }

    /// create_task creates a new task content.
    ///
    /// Behavior of `create_task`:
    /// 1. If the task already exists, return the task path.
    /// 2. If the task does not exist, create the task directory and file.
    #[instrument(skip_all)]
    pub async fn create_task(&self, task_id: &str, length: u64) -> Result<PathBuf> {
        let task_path = self.get_task_path(task_id);
        if task_path.exists() {
            return Ok(task_path);
        }

        let task_dir = self.dir.join(DEFAULT_TASK_DIR).join(&task_id[..3]);
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
    #[instrument(skip_all)]
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

    /// copy_task copies the task content to the destination.
    #[instrument(skip_all)]
    pub async fn copy_task(&self, task_id: &str, to: &Path) -> Result<()> {
        fs::copy(self.get_task_path(task_id), to).await?;
        info!("copy to {:?} success", to);
        Ok(())
    }

    /// copy_task_by_range copies the task content to the destination by range.
    #[instrument(skip_all)]
    async fn copy_task_by_range(&self, task_id: &str, to: &Path, range: Range) -> Result<()> {
        // Ensure the parent directory of the destination exists.
        if let Some(parent) = to.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).await.inspect_err(|err| {
                    error!("failed to create directory {:?}: {}", parent, err);
                })?;
            }
        }

        let mut from_f = File::open(self.get_task_path(task_id)).await?;
        from_f.seek(SeekFrom::Start(range.start)).await?;
        let range_reader = from_f.take(range.length);

        // Use a buffer to read the range.
        let mut range_reader =
            BufReader::with_capacity(self.config.storage.read_buffer_size, range_reader);

        let mut to_f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(to.as_os_str())
            .await?;

        io::copy(&mut range_reader, &mut to_f).await?;
        Ok(())
    }

    /// delete_task deletes the task content.
    pub async fn delete_task(&self, task_id: &str) -> Result<()> {
        info!("delete task content: {}", task_id);
        let task_path = self.get_task_path(task_id);
        fs::remove_file(task_path.as_path())
            .await
            .inspect_err(|err| {
                error!("remove {:?} failed: {}", task_path, err);
            })?;
        Ok(())
    }

    /// read_piece reads the piece from the content.
    #[instrument(skip_all)]
    pub async fn read_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        let task_path = self.get_task_path(task_id);

        // Calculate the target offset and length based on the range.
        let (target_offset, target_length) = calculate_piece_range(offset, length, range);

        let f = File::open(task_path.as_path()).await.inspect_err(|err| {
            error!("open {:?} failed: {}", task_path, err);
        })?;
        let mut f_reader = BufReader::with_capacity(self.config.storage.read_buffer_size, f);

        f_reader
            .seek(SeekFrom::Start(target_offset))
            .await
            .inspect_err(|err| {
                error!("seek {:?} failed: {}", task_path, err);
            })?;

        Ok(f_reader.take(target_length))
    }

    /// read_piece_with_dual_read return two readers, one is the range reader, and the other is the
    /// full reader of the piece. It is used for cache the piece content to the proxy cache.
    #[instrument(skip_all)]
    pub async fn read_piece_with_dual_read(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<(impl AsyncRead, impl AsyncRead)> {
        let task_path = self.get_task_path(task_id);

        // Calculate the target offset and length based on the range.
        let (target_offset, target_length) = calculate_piece_range(offset, length, range);

        let f = File::open(task_path.as_path()).await.inspect_err(|err| {
            error!("open {:?} failed: {}", task_path, err);
        })?;
        let mut f_range_reader = BufReader::with_capacity(self.config.storage.read_buffer_size, f);

        f_range_reader
            .seek(SeekFrom::Start(target_offset))
            .await
            .inspect_err(|err| {
                error!("seek {:?} failed: {}", task_path, err);
            })?;
        let range_reader = f_range_reader.take(target_length);

        // Create full reader of the piece.
        let f = File::open(task_path.as_path()).await.inspect_err(|err| {
            error!("open {:?} failed: {}", task_path, err);
        })?;
        let mut f_reader = BufReader::with_capacity(self.config.storage.read_buffer_size, f);

        f_reader
            .seek(SeekFrom::Start(offset))
            .await
            .inspect_err(|err| {
                error!("seek {:?} failed: {}", task_path, err);
            })?;
        let reader = f_reader.take(length);

        Ok((range_reader, reader))
    }

    /// write_piece writes the piece to the content and calculates the hash of the piece by crc32.
    #[instrument(skip_all)]
    pub async fn write_piece<R: AsyncRead + Unpin + ?Sized>(
        &self,
        task_id: &str,
        offset: u64,
        expected_length: u64,
        reader: &mut R,
    ) -> Result<WritePieceResponse> {
        // Open the file and seek to the offset.
        let task_path = self.get_task_path(task_id);
        let mut f = OpenOptions::new()
            .truncate(false)
            .write(true)
            .open(task_path.as_path())
            .await
            .inspect_err(|err| {
                error!("open {:?} failed: {}", task_path, err);
            })?;

        f.seek(SeekFrom::Start(offset)).await.inspect_err(|err| {
            error!("seek {:?} failed: {}", task_path, err);
        })?;

        let reader = BufReader::with_capacity(self.config.storage.write_buffer_size, reader);
        let mut writer = BufWriter::with_capacity(self.config.storage.write_buffer_size, f);

        // Copy the piece to the file while updating the CRC32 value.
        let mut hasher = crc32fast::Hasher::new();
        let mut tee = InspectReader::new(reader, |bytes| {
            hasher.update(bytes);
        });

        let length = io::copy(&mut tee, &mut writer).await.inspect_err(|err| {
            error!("copy {:?} failed: {}", task_path, err);
        })?;

        writer.flush().await.inspect_err(|err| {
            error!("flush {:?} failed: {}", task_path, err);
        })?;

        if length != expected_length {
            return Err(Error::Unknown(format!(
                "expected length {} but got {}",
                expected_length, length
            )));
        }

        // Calculate the hash of the piece.
        Ok(WritePieceResponse {
            length,
            hash: hasher.finalize().to_string(),
        })
    }

    /// get_task_path returns the task path by task id.
    fn get_task_path(&self, task_id: &str) -> PathBuf {
        // The task needs split by the first 3 characters of task id(sha256) to
        // avoid too many files in one directory.
        let sub_dir = &task_id[..3];
        self.dir.join(DEFAULT_TASK_DIR).join(sub_dir).join(task_id)
    }

    /// is_same_dev_inode_as_persistent_cache_task checks if the persistent cache task and target
    /// are the same device and inode.
    pub async fn is_same_dev_inode_as_persistent_cache_task(
        &self,
        task_id: &str,
        to: &Path,
    ) -> Result<bool> {
        let task_path = self.get_persistent_cache_task_path(task_id);
        self.is_same_dev_inode(&task_path, to).await
    }

    /// create_persistent_cache_task creates a new persistent cache task content.
    ///
    /// Behavior of `create_persistent_cache_task`:
    /// 1. If the persistent cache task already exists, return the persistent cache task path.
    /// 2. If the persistent cache task does not exist, create the persistent cache task directory and file.
    #[instrument(skip_all)]
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
            .join(DEFAULT_PERSISTENT_CACHE_TASK_DIR)
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

    /// Hard links the persistent cache task content to the destination.
    ///
    /// Behavior of `hard_link_persistent_cache_task`:
    /// 1. If the destination exists:
    ///    1.1. If the source and destination share the same device and inode, return immediately.
    ///    1.2. Otherwise, return an error.
    /// 2. If the destination does not exist:
    ///    2.1. If the hard link succeeds, return immediately.
    ///    2.2. If the hard link fails, copy the persistent cache task content to the destination once the task is finished, then return immediately.
    #[instrument(skip_all)]
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

    /// copy_persistent_cache_task copies the persistent cache task content to the destination.
    #[instrument(skip_all)]
    pub async fn copy_persistent_cache_task(&self, task_id: &str, to: &Path) -> Result<()> {
        fs::copy(self.get_persistent_cache_task_path(task_id), to).await?;
        info!("copy to {:?} success", to);
        Ok(())
    }

    /// read_persistent_cache_piece reads the persistent cache piece from the content.
    #[instrument(skip_all)]
    pub async fn read_persistent_cache_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
        // ADDED
        // encrypt: bool,
        piece_id: &str,
    ) -> Result<impl AsyncRead> {
        let task_path = self.get_persistent_cache_task_path(task_id);

        // Calculate the target offset and length based on the range.
        let (target_offset, target_length) = calculate_piece_range(offset, length, range);

        let f = File::open(task_path.as_path()).await.inspect_err(|err| {
            error!("open {:?} failed: {}", task_path, err);
        })?;
        let mut f_reader = BufReader::with_capacity(self.config.storage.read_buffer_size, f);

        f_reader
            .seek(SeekFrom::Start(target_offset))
            .await
            .inspect_err(|err| {
                error!("seek {:?} failed: {}", task_path, err);
            })?;

        if self.config.storage.encryption.enable {
            let key = self.key.as_ref().expect("should have key when encryption enabled");
                
            let limited_reader = f_reader.take(target_length);
            let decrypt_reader = DecryptReader::new(
                limited_reader, 
                key, 
                piece_id
            );

            return Ok(Either::Left(decrypt_reader));
        }
        
        Ok(Either::Right(f_reader.take(target_length)))
    }

    // TODO encryption?
    /// read_persistent_cache_piece_with_dual_read return two readers, one is the range reader, and the other is the
    /// full reader of the persistent cache piece. It is used for cache the piece content to the proxy cache.
    #[instrument(skip_all)]
    pub async fn read_persistent_cache_piece_with_dual_read(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<(impl AsyncRead, impl AsyncRead)> {
        let task_path = self.get_persistent_cache_task_path(task_id);

        // Calculate the target offset and length based on the range.
        let (target_offset, target_length) = calculate_piece_range(offset, length, range);

        let f = File::open(task_path.as_path()).await.inspect_err(|err| {
            error!("open {:?} failed: {}", task_path, err);
        })?;
        let mut f_range_reader = BufReader::with_capacity(self.config.storage.read_buffer_size, f);

        f_range_reader
            .seek(SeekFrom::Start(target_offset))
            .await
            .inspect_err(|err| {
                error!("seek {:?} failed: {}", task_path, err);
            })?;
        let range_reader = f_range_reader.take(target_length);

        // Create full reader of the piece.
        let f = File::open(task_path.as_path()).await.inspect_err(|err| {
            error!("open {:?} failed: {}", task_path, err);
        })?;
        let mut f_reader = BufReader::with_capacity(self.config.storage.read_buffer_size, f);

        f_reader
            .seek(SeekFrom::Start(offset))
            .await
            .inspect_err(|err| {
                error!("seek {:?} failed: {}", task_path, err);
            })?;
        let reader = f_reader.take(length);

        Ok((range_reader, reader))
    }

    /// write_persistent_cache_piece writes the persistent cache piece to the content and
    /// calculates the hash of the piece by crc32.
    #[instrument(skip_all)]
    pub async fn write_persistent_cache_piece<R: AsyncRead + Unpin + ?Sized>(
        &self,
        task_id: &str,
        offset: u64,
        expected_length: u64,
        reader: &mut R,
        // ADDED
        // encrypt: bool,
        piece_id: &str,
    ) -> Result<WritePieceResponse> {
        // Open the file and seek to the offset.
        let task_path = self.get_persistent_cache_task_path(task_id);
        let mut f = OpenOptions::new()
            .truncate(false)
            .write(true)
            .open(task_path.as_path())
            .await
            .inspect_err(|err| {
                error!("open {:?} failed: {}", task_path, err);
            })?;

        f.seek(SeekFrom::Start(offset)).await.inspect_err(|err| {
            error!("seek {:?} failed: {}", task_path, err);
        })?;

        let reader = BufReader::with_capacity(self.config.storage.write_buffer_size, reader);
        let mut writer = BufWriter::with_capacity(self.config.storage.write_buffer_size, f);

        // Copy the piece to the file while updating the CRC32 value.
        let mut hasher = crc32fast::Hasher::new();
        let tee = InspectReader::new(reader, |bytes| {
            hasher.update(bytes);
        });

        let mut tee_warpper = if self.config.storage.encryption.enable {
            let key = self.key.as_ref().expect("should have key when encryption enabled");

            let encrypt_reader = EncryptReader::new(
                tee, 
                key, 
                piece_id
            );
            Either::Left(encrypt_reader)
        } else {
            Either::Right(tee)
        };

        let length = io::copy(&mut tee_warpper, &mut writer).await.inspect_err(|err| {
            error!("copy {:?} failed: {}", task_path, err);
        })?;

        writer.flush().await.inspect_err(|err| {
            error!("flush {:?} failed: {}", task_path, err);
        })?;

        if length != expected_length {
            return Err(Error::Unknown(format!(
                "expected length {} but got {}",
                expected_length, length
            )));
        }

        // Calculate the hash of the piece.
        Ok(WritePieceResponse {
            length,
            hash: hasher.finalize().to_string(),
        })
    }

    /// delete_task deletes the persistent cache task content.
    pub async fn delete_persistent_cache_task(&self, task_id: &str) -> Result<()> {
        info!("delete persistent cache task content: {}", task_id);
        let persistent_cache_task_path = self.get_persistent_cache_task_path(task_id);
        fs::remove_file(persistent_cache_task_path.as_path())
            .await
            .inspect_err(|err| {
                error!("remove {:?} failed: {}", persistent_cache_task_path, err);
            })?;
        Ok(())
    }

    /// get_persistent_cache_task_path returns the persistent cache task path by task id.
    fn get_persistent_cache_task_path(&self, task_id: &str) -> PathBuf {
        // The persistent cache task needs split by the first 3 characters of task id(sha256) to
        // avoid too many files in one directory.
        self.dir
            .join(DEFAULT_PERSISTENT_CACHE_TASK_DIR)
            .join(&task_id[..3])
            .join(task_id)
    }
}

/// calculate_piece_range calculates the target offset and length based on the piece range and
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
    use std::io::Cursor;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_create_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

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
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

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
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

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
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

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
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

        let task_id = "c794a3bbae81e06d1c8d362509bdd42a7c105b0fb28d80ffe27f94b8f04fc845";
        content.create_task(task_id, 13).await.unwrap();

        let data = b"hello, world!";
        let mut reader = Cursor::new(data);
        content
            .write_piece(task_id, 0, 13, &mut reader)
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
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

        let task_id = "60b48845606946cea72084f14ed5cce61ec96e69f80a30f891a6963dccfd5b4f";
        content.create_task(task_id, 4).await.unwrap();

        let data = b"test";
        let mut reader = Cursor::new(data);
        let response = content
            .write_piece(task_id, 0, 4, &mut reader)
            .await
            .unwrap();
        assert_eq!(response.length, 4);
        assert!(!response.hash.is_empty());
    }

    #[tokio::test]
    async fn test_create_persistent_task() {
        let temp_dir = tempdir().unwrap();
        let config = Arc::new(Config::default());
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

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
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

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
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

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
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

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
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

        let task_id = "9cb27a4af09aee4eb9f904170217659683f4a0ea7cd55e1a9fbcb99ddced659a";
        content
            .create_persistent_cache_task(task_id, 13)
            .await
            .unwrap();

        let data = b"hello, world!";
        let mut reader = Cursor::new(data);
        content
            .write_persistent_cache_piece(task_id, 0, 13, &mut reader, "")
            .await
            .unwrap();

        let mut reader = content
            .read_persistent_cache_piece(task_id, 0, 13, None, "")
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
                "",
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
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

        let task_id = "ca1afaf856e8a667fbd48093ca3ca1b8eeb4bf735912fbe551676bc5817a720a";
        content
            .create_persistent_cache_task(task_id, 4)
            .await
            .unwrap();

        let data = b"test";
        let mut reader = Cursor::new(data);
        let response = content
            .write_persistent_cache_piece(task_id, 0, 4, &mut reader, "")
            .await
            .unwrap();
        assert_eq!(response.length, 4);
        assert!(!response.hash.is_empty());
    }

    #[tokio::test]
    async fn test_has_enough_space() {
        let config = Arc::new(Config::default());
        let temp_dir = tempdir().unwrap();
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

        let has_space = content.has_enough_space(1).unwrap();
        assert!(has_space);

        let has_space = content.has_enough_space(u64::MAX).unwrap();
        assert!(!has_space);

        let mut config = Config::default();
        config.gc.policy.dist_threshold = ByteSize::mib(10);
        let config = Arc::new(config);
        let content = Content::new(config, temp_dir.path(), None).await.unwrap();

        let file_path = Path::new(temp_dir.path())
            .join(DEFAULT_CONTENT_DIR)
            .join(DEFAULT_TASK_DIR)
            .join("1mib");
        let mut file = File::create(&file_path).await.unwrap();
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

    #[tokio::test]
    async fn test_encryption_write_read_persistent_cache_piece() {
        // cargo test --package dragonfly-client-storage --lib 
        // \ -- content::tests::test_encrypt_write_persistent_cache_piece --exact --show-output
        use base64;

        let temp_dir = tempdir().unwrap();
        let mut config = Config::default();
        config.storage.encryption.enable = true;
        
        // base64 key
        let base64_key = "jqe8buWT8rsfBMYt8mpwSbnjy44WNy/5v1gN1JfFsNk=";
        let key = base64::decode(base64_key).expect("Failed to decode base64 key");
        println!("key: {:#x?}", key);
        
        let config = Arc::new(config);
        let content = Content::new(config, temp_dir.path(), Some(key)).await.unwrap();

        let task_id = "ca1afaf856e8a667fbd48093ca3ca1b8eeb4bf735912fbe551676bc5817a720a";
        let piece_id = "ca1afaf856e8a667fbd48093ca3ca1b8eeb4bf735912fbe551676bc5817a720a-1";
        content
            .create_persistent_cache_task(task_id, 4)
            .await
            .unwrap();

        let data = b"data";
        
        // cal CRC
        let mut plaintext_hasher = crc32fast::Hasher::new();
        plaintext_hasher.update(data);
        let plaintext_crc = plaintext_hasher.finalize();

        // encrypted write
        let mut reader = Cursor::new(data);
        let response = content
            .write_persistent_cache_piece(task_id, 0, data.len() as u64, &mut reader, piece_id)
            .await
            .unwrap();

        // check len
        assert_eq!(response.length, data.len() as u64);
        
        // check with plaintext crc
        let response_crc = response.hash.parse::<u32>().expect("Failed to parse CRC");
        assert_eq!(response_crc, plaintext_crc, "Encrypted CRC should match plaintext CRC");
        
        // decrypted read
        let mut decrypted_reader = content
            .read_persistent_cache_piece(task_id, 0, data.len() as u64, None, piece_id)
            .await
            .unwrap();
        
        let mut decrypted_data = Vec::new();
        decrypted_reader.read_to_end(&mut decrypted_data).await.unwrap();
        
        assert_eq!(decrypted_data, data, "Decrypted data should match original data");
    }
}