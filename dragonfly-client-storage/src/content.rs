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
use dragonfly_client_core::Result;
use std::cmp::{max, min};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncSeekExt, BufReader, SeekFrom};
use tokio_util::io::InspectReader;
use tracing::{error, info, warn};

// DEFAULT_DIR_NAME is the default directory name to store content.
const DEFAULT_DIR_NAME: &str = "content";

// Content is the content of a piece.
pub struct Content {
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

    // dir is the directory to store content.
    dir: PathBuf,
}

// WritePieceResponse is the response of writing a piece.
pub struct WritePieceResponse {
    // length is the length of the piece.
    pub length: u64,

    // hash is the hash of the piece.
    pub hash: String,
}

// WriteCacheTaskResponse is the response of writing a cache task.
pub struct WriteCacheTaskResponse {
    // length is the length of the cache task.
    pub length: u64,

    // hash is the hash of the cache task.
    pub hash: String,
}

// Content implements the content storage.
impl Content {
    // new returns a new content.
    pub async fn new(config: Arc<Config>, dir: &Path) -> Result<Content> {
        let dir = dir.join(DEFAULT_DIR_NAME);

        // If the storage is not kept, remove the directory.
        if !config.storage.keep {
            fs::remove_dir_all(&dir).await.unwrap_or_else(|err| {
                warn!("remove {:?} failed: {}", dir, err);
            });
        }

        fs::create_dir_all(&dir).await?;
        info!("content initialized directory: {:?}", dir);

        Ok(Content { config, dir })
    }

    // hard_link_or_copy_task hard links or copies the task content to the destination.
    pub async fn hard_link_or_copy_task(
        &self,
        task: crate::metadata::Task,
        to: &Path,
        range: Option<Range>,
    ) -> Result<()> {
        let task_path = self.dir.join(task.id.as_str());

        // Copy the task content to the destination by range
        // if the range is specified.
        if let Some(range) = range {
            // If the range length is 0, no need to copy. Need to open the file to
            // ensure the file exists.
            if range.length == 0 {
                info!("range length is 0, no need to copy");
                File::create(to).await.map_err(|err| {
                    error!("create {:?} failed: {}", to, err);
                    err
                })?;

                return Ok(());
            }

            self.copy_task_by_range(task.id.as_str(), to, range)
                .await
                .map_err(|err| {
                    error!("copy range {:?} to {:?} failed: {}", task_path, to, err);
                    err
                })?;

            info!("copy range {:?} to {:?} success", task_path, to);
            return Ok(());
        }

        // If the hard link fails, copy the task content to the destination.
        fs::remove_file(to).await.unwrap_or_else(|err| {
            info!("remove {:?} failed: {}", to, err);
        });

        if let Err(err) = self.hard_link_task(task.id.as_str(), to).await {
            warn!("hard link {:?} to {:?} failed: {}", task_path, to, err);

            // If the task is empty, no need to copy. Need to open the file to
            // ensure the file exists.
            if task.is_empty() {
                info!("task is empty, no need to copy");
                File::create(to).await.map_err(|err| {
                    error!("create {:?} failed: {}", to, err);
                    err
                })?;

                return Ok(());
            }

            self.copy_task(task.id.as_str(), to).await.map_err(|err| {
                error!("copy {:?} to {:?} failed: {}", task_path, to, err);
                err
            })?;

            info!("copy {:?} to {:?} success", task_path, to);
            return Ok(());
        }

        info!("hard link {:?} to {:?} success", task_path, to);
        Ok(())
    }

    // hard_link_task hard links the task content.
    async fn hard_link_task(&self, task_id: &str, link: &Path) -> Result<()> {
        fs::hard_link(self.dir.join(task_id), link).await?;
        Ok(())
    }

    // copy_task copies the task content to the destination.
    async fn copy_task(&self, task_id: &str, to: &Path) -> Result<()> {
        fs::copy(self.dir.join(task_id), to).await?;
        Ok(())
    }

    // copy_task_by_range copies the task content to the destination by range.
    async fn copy_task_by_range(&self, task_id: &str, to: &Path, range: Range) -> Result<()> {
        let mut from_f = File::open(self.dir.join(task_id)).await?;
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

    // read_task reads the task content by range.
    pub async fn read_task_by_range(&self, task_id: &str, range: Range) -> Result<impl AsyncRead> {
        let task_path = self.dir.join(task_id);

        let mut from_f = File::open(task_path.as_path()).await.map_err(|err| {
            error!("open {:?} failed: {}", task_path, err);
            err
        })?;

        from_f
            .seek(SeekFrom::Start(range.start))
            .await
            .map_err(|err| {
                error!("seek {:?} failed: {}", task_path, err);
                err
            })?;

        let range_reader = from_f.take(range.length);
        Ok(range_reader)
    }

    // delete_task deletes the task content.
    pub async fn delete_task(&self, task_id: &str) -> Result<()> {
        let task_path = self.dir.join(task_id);
        fs::remove_file(task_path.as_path()).await.map_err(|err| {
            error!("remove {:?} failed: {}", task_path, err);
            err
        })?;
        Ok(())
    }

    // read_piece reads the piece from the content.
    pub async fn read_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        let task_path = self.dir.join(task_id);

        if let Some(range) = range {
            let target_offset = max(offset, range.start);
            let target_length =
                min(offset + length - 1, range.start + range.length - 1) - target_offset + 1;

            let mut f = File::open(task_path.as_path()).await.map_err(|err| {
                error!("open {:?} failed: {}", task_path, err);
                err
            })?;

            f.seek(SeekFrom::Start(target_offset))
                .await
                .map_err(|err| {
                    error!("seek {:?} failed: {}", task_path, err);
                    err
                })?;
            return Ok(f.take(target_length));
        }

        let mut f = File::open(task_path.as_path()).await.map_err(|err| {
            error!("open {:?} failed: {}", task_path, err);
            err
        })?;

        f.seek(SeekFrom::Start(offset)).await.map_err(|err| {
            error!("seek {:?} failed: {}", task_path, err);
            err
        })?;
        Ok(f.take(length))
    }

    // write_piece writes the piece to the content.
    pub async fn write_piece<R: AsyncRead + Unpin + ?Sized>(
        &self,
        task_id: &str,
        offset: u64,
        reader: &mut R,
    ) -> Result<WritePieceResponse> {
        let task_path = self.dir.join(task_id);

        // Use a buffer to read the piece.
        let reader = BufReader::with_capacity(self.config.storage.write_buffer_size, reader);

        // Crc32 is used to calculate the hash of the piece.
        let mut hasher = crc32fast::Hasher::new();

        // InspectReader is used to calculate the hash of the piece.
        let mut tee = InspectReader::new(reader, |bytes| {
            hasher.update(bytes);
        });

        // Open the file and seek to the offset.
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(task_path.as_path())
            .await
            .map_err(|err| {
                error!("open {:?} failed: {}", task_path, err);
                err
            })?;

        f.seek(SeekFrom::Start(offset)).await.map_err(|err| {
            error!("seek {:?} failed: {}", task_path, err);
            err
        })?;

        // Copy the piece to the file.
        let length = io::copy(&mut tee, &mut f).await.map_err(|err| {
            error!("copy {:?} failed: {}", task_path, err);
            err
        })?;

        // Calculate the hash of the piece.
        let hash = hasher.finalize();
        Ok(WritePieceResponse {
            length,
            hash: base16ct::lower::encode_string(&hash.to_be_bytes()),
        })
    }

    // hard_link_or_copy_cache_task hard links or copies the task content to the destination.
    pub async fn hard_link_or_copy_cache_task(
        &self,
        task: crate::metadata::CacheTask,
        to: &Path,
    ) -> Result<()> {
        let task_path = self.dir.join(task.id.as_str());

        // If the hard link fails, copy the task content to the destination.
        fs::remove_file(to).await.unwrap_or_else(|err| {
            info!("remove {:?} failed: {}", to, err);
        });

        if let Err(err) = self.hard_link_task(task.id.as_str(), to).await {
            warn!("hard link {:?} to {:?} failed: {}", task_path, to, err);

            // If the cache task is empty, no need to copy. Need to open the file to
            // ensure the file exists.
            if task.is_empty() {
                info!("cache task is empty, no need to copy");
                File::create(to).await.map_err(|err| {
                    error!("create {:?} failed: {}", to, err);
                    err
                })?;

                return Ok(());
            }

            self.copy_task(task.id.as_str(), to).await.map_err(|err| {
                error!("copy {:?} to {:?} failed: {}", task_path, to, err);
                err
            })?;

            info!("copy {:?} to {:?} success", task_path, to);
            return Ok(());
        }

        info!("hard link {:?} to {:?} success", task_path, to);
        Ok(())
    }

    // copy_cache_task copies the cache task content to the destination.
    pub async fn write_cache_task(
        &self,
        task_id: &str,
        from: &Path,
    ) -> Result<WriteCacheTaskResponse> {
        // Open the file to copy the content.
        let from_f = File::open(from).await?;

        // Use a buffer to read the content.
        let reader = BufReader::with_capacity(self.config.storage.write_buffer_size, from_f);

        // Crc32 is used to calculate the hash of the content.
        let mut hasher = crc32fast::Hasher::new();

        // InspectReader is used to calculate the hash of the content.
        let mut tee = InspectReader::new(reader, |bytes| {
            hasher.update(bytes);
        });

        let task_path = self.dir.join(task_id);
        let mut to_f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(task_path.as_path())
            .await
            .map_err(|err| {
                error!("open {:?} failed: {}", task_path, err);
                err
            })?;

        // Copy the content to the file.
        let length = io::copy(&mut tee, &mut to_f).await.map_err(|err| {
            error!("copy {:?} failed: {}", task_path, err);
            err
        })?;

        // Calculate the hash of the content.
        let hash = hasher.finalize();
        Ok(WriteCacheTaskResponse {
            length,
            hash: base16ct::lower::encode_string(&hash.to_be_bytes()),
        })
    }

    // delete_task deletes the cache task content.
    pub async fn delete_cache_task(&self, cache_task_id: &str) -> Result<()> {
        let cache_task_path = self.dir.join(cache_task_id);
        fs::remove_file(cache_task_path.as_path())
            .await
            .map_err(|err| {
                error!("remove {:?} failed: {}", cache_task_path, err);
                err
            })?;
        Ok(())
    }
}
