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
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use std::cmp::{max, min};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{
    self, AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom,
};
use tokio_util::io::InspectReader;
use tracing::{error, info, instrument, warn};

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
    #[instrument(skip_all)]
    pub async fn new(config: Arc<Config>, dir: &Path) -> Result<Content> {
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
        Ok(Content { config, dir })
    }

    /// available_space returns the available space of the disk.
    pub fn available_space(&self) -> Result<u64> {
        let stat = fs2::statvfs(&self.dir)?;
        Ok(stat.available_space())
    }

    /// total_space returns the total space of the disk.
    pub fn total_space(&self) -> Result<u64> {
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

    /// hard_link_or_copy_task hard links or copies the task content to the destination.
    ///
    /// 1. Destination exists:
    ///     1.1. If the source and destination are the same device and inode, return directly.
    ///     1.2. If the source and destination are not the same device and inode, return an error.
    ///       Because the destination already exists, it is not allowed to overwrite the
    ///       destination.
    /// 2. Destination does not exist:
    ///     2.1. Hard link the task content to the destination.
    ///     2.2. If the hard link fails, copy the task content to the destination.
    #[instrument(skip_all)]
    pub async fn hard_link_or_copy_task(
        &self,
        task: &crate::metadata::Task,
        to: &Path,
    ) -> Result<()> {
        let task_path = self.get_task_path(task.id.as_str());

        // If the destination does not exist, hard link the task content to the destination.
        // If the hard link fails, copy the task content to the destination.
        if let Err(err) = fs::hard_link(task_path.clone(), to).await {
            warn!("hard link {:?} to {:?} failed: {}", task_path, to, err);

            // If the destination exists, check if the source and destination are the same device and
            // inode. If they are the same, return directly. If not, return an error.
            if err.kind() == std::io::ErrorKind::AlreadyExists {
                return match self.is_same_dev_inode(&task_path, to).await {
                    Ok(true) => {
                        info!("hard already exists, no need to operate");
                        Ok(())
                    }
                    Ok(false) => Err(Error::IO(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!("{:?} already exists", to),
                    ))),
                    Err(err) => Err(err),
                };
            }

            // If the task is empty, no need to copy. Need to open the file to
            // ensure the file exists.
            if task.is_empty() {
                info!("task is empty, no need to copy");
                File::create(to).await.inspect_err(|err| {
                    error!("create {:?} failed: {}", to, err);
                })?;

                return Ok(());
            }

            self.copy_task(task.id.as_str(), to)
                .await
                .inspect_err(|err| {
                    error!("copy {:?} to {:?} failed: {}", task_path, to, err);
                })?;

            info!("copy {:?} to {:?} success", task_path, to);
            return Ok(());
        }

        info!("hard link {:?} to {:?} success", task_path, to);
        Ok(())
    }

    /// copy_task copies the task content to the destination.
    #[instrument(skip_all)]
    async fn copy_task(&self, task_id: &str, to: &Path) -> Result<()> {
        // Ensure the parent directory of the destination exists.
        if let Some(parent) = to.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).await.inspect_err(|err| {
                    error!("failed to create directory {:?}: {}", parent, err);
                })?;
            }
        }

        fs::copy(self.get_task_path(task_id), to).await?;
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
    #[instrument(skip_all)]
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
        reader: &mut R,
    ) -> Result<WritePieceResponse> {
        // Open the file and seek to the offset.
        let task_path = self.create_or_get_task_path(task_id).await?;
        let mut f = OpenOptions::new()
            .create(true)
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

        // Calculate the hash of the piece.
        Ok(WritePieceResponse {
            length,
            hash: hasher.finalize().to_string(),
        })
    }

    /// get_task_path returns the task path by task id.
    #[instrument(skip_all)]
    fn get_task_path(&self, task_id: &str) -> PathBuf {
        // The task needs split by the first 3 characters of task id(sha256) to
        // avoid too many files in one directory.
        let sub_dir = &task_id[..3];
        self.dir.join(DEFAULT_TASK_DIR).join(sub_dir).join(task_id)
    }

    /// create_or_get_task_path creates parent directories or returns the task path by task id.
    #[instrument(skip_all)]
    async fn create_or_get_task_path(&self, task_id: &str) -> Result<PathBuf> {
        let task_dir = self.dir.join(DEFAULT_TASK_DIR).join(&task_id[..3]);
        fs::create_dir_all(&task_dir).await.inspect_err(|err| {
            error!("create {:?} failed: {}", task_dir, err);
        })?;

        Ok(task_dir.join(task_id))
    }

    /// hard_link_or_copy_persistent_cache_task hard links or copies the task content to the destination.
    ///
    /// 1. Destination exists:
    ///     1.1. If the source and destination are the same device and inode, return directly.
    ///     1.2. If the source and destination are not the same device and inode, return an error.
    ///       Because the destination already exists, it is not allowed to overwrite the
    ///       destination.
    /// 2. Destination does not exist:
    ///     2.1. Hard link the task content to the destination.
    ///     2.2. If the hard link fails, copy the task content to the destination.
    #[instrument(skip_all)]
    pub async fn hard_link_or_copy_persistent_cache_task(
        &self,
        task: &crate::metadata::PersistentCacheTask,
        to: &Path,
    ) -> Result<()> {
        // Get the persistent cache task path.
        let task_path = self.get_persistent_cache_task_path(task.id.as_str());

        // If the destination does not exist, hard link the task content to the destination.
        // If the hard link fails, copy the task content to the destination.
        if let Err(err) = fs::hard_link(task_path.clone(), to).await {
            warn!("hard link {:?} to {:?} failed: {}", task_path, to, err);

            // If the destination exists, check if the source and destination are the same device and
            // inode. If they are the same, return directly. If not, return an error.
            if err.kind() == std::io::ErrorKind::AlreadyExists {
                return match self.is_same_dev_inode(&task_path, to).await {
                    Ok(true) => {
                        info!("hard already exists, no need to operate");
                        Ok(())
                    }
                    Ok(false) => Err(Error::IO(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!("{:?} already exists", to),
                    ))),
                    Err(err) => Err(err),
                };
            }

            // If the persistent cache task is empty, no need to copy. Need to open the file to
            // ensure the file exists.
            if task.is_empty() {
                info!("persistent cache task is empty, no need to copy");
                File::create(to).await.inspect_err(|err| {
                    error!("create {:?} failed: {}", to, err);
                })?;

                return Ok(());
            }

            self.copy_persistent_cache_task(task.id.as_str(), to)
                .await
                .inspect_err(|err| {
                    error!("copy {:?} to {:?} failed: {}", task_path, to, err);
                })?;

            info!("copy {:?} to {:?} success", task_path, to);
            return Ok(());
        }

        info!("hard link {:?} to {:?} success", task_path, to);
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

        Ok(f_reader.take(target_length))
    }

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
        reader: &mut R,
    ) -> Result<WritePieceResponse> {
        // Open the file and seek to the offset.
        let task_path = self
            .create_or_get_persistent_cache_task_path(task_id)
            .await?;
        let mut f = OpenOptions::new()
            .create(true)
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

        // Calculate the hash of the piece.
        Ok(WritePieceResponse {
            length,
            hash: hasher.finalize().to_string(),
        })
    }

    /// copy_persistent_cache_task copies the persistent cache task content to the destination.
    #[instrument(skip_all)]
    async fn copy_persistent_cache_task(&self, task_id: &str, to: &Path) -> Result<()> {
        // Ensure the parent directory of the destination exists.
        if let Some(parent) = to.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).await.inspect_err(|err| {
                    error!("failed to create directory {:?}: {}", parent, err);
                })?;
            }
        }

        fs::copy(self.get_persistent_cache_task_path(task_id), to).await?;
        Ok(())
    }

    /// delete_task deletes the persistent cache task content.
    #[instrument(skip_all)]
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
    #[instrument(skip_all)]
    fn get_persistent_cache_task_path(&self, task_id: &str) -> PathBuf {
        // The persistent cache task needs split by the first 3 characters of task id(sha256) to
        // avoid too many files in one directory.
        self.dir
            .join(DEFAULT_PERSISTENT_CACHE_TASK_DIR)
            .join(&task_id[..3])
            .join(task_id)
    }

    /// create_or_get_persistent_cache_task_path creates parent directories or returns the persistent cache task path by task id.
    #[instrument(skip_all)]
    async fn create_or_get_persistent_cache_task_path(&self, task_id: &str) -> Result<PathBuf> {
        let task_dir = self
            .dir
            .join(DEFAULT_PERSISTENT_CACHE_TASK_DIR)
            .join(&task_id[..3]);

        fs::create_dir_all(&task_dir).await.inspect_err(|err| {
            error!("create {:?} failed: {}", task_dir, err);
        })?;

        Ok(task_dir.join(task_id))
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
    async fn test_has_enough_space() {
        let config = Arc::new(Config::default());
        let dir = PathBuf::from("/tmp/dragonfly_test");
        let content = Content::new(config, &dir).await.unwrap();

        let has_space = content.has_enough_space(1).unwrap();
        assert!(has_space);

        let has_space = content.has_enough_space(u64::MAX).unwrap();
        assert!(!has_space);
    }
}
