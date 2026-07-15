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

use bytes::BytesMut;
use chrono::NaiveDateTime;
use dragonfly_api::common::v2::Range;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::digest::{Algorithm, Digest};
use reqwest::header::HeaderMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::{
    fs,
    io::{AsyncRead, AsyncReadExt},
    time::sleep,
};
use tokio_util::either::Either;
use tokio_util::io::InspectReader;
use tracing::{debug, error, info, instrument, warn};

#[cfg(target_os = "linux")]
mod content_linux;

#[cfg(target_os = "macos")]
mod content_macos;

pub mod cache;
pub mod client;
pub mod content;
pub mod metadata;
pub mod server;
pub mod storage_engine;

/// The default interval for waiting for the piece to be finished.
pub const DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL: Duration = Duration::from_millis(100);

/// Default temporary directory name for output operations.
///
/// When users need to hardlink files from the client DaemonSet Pod's cache to the output
/// directory within a user's Pod (to avoid the time overhead of copying), they can use this
/// tmp directory as the output location. This works around a Kubernetes limitation:
/// when ephemeral-storage limits are set, Kubernetes assigns project quota ID to
/// hostPath mount point, which prevents hardlinks from being created across different
/// quota contexts.
pub const DEFAULT_TMP_DIR: &str = "tmp";

/// The storage of the task.
pub struct Storage {
    /// The configuration of the dfdaemon.
    config: Arc<Config>,

    /// Implements the metadata storage.
    metadata: metadata::Metadata,

    /// Implements the content storage.
    content: content::Content,

    /// Implements the cache storage.
    cache: cache::Cache,
}

/// Implements the storage.
impl Storage {
    /// Returns a new storage.
    pub async fn new(config: Arc<Config>, dir: &Path, log_dir: PathBuf) -> Result<Self> {
        let metadata = metadata::Metadata::new(config.clone(), dir, &log_dir)?;
        let content = content::new_content(config.clone(), dir).await?;
        let cache = cache::Cache::new(config.clone());

        // Create temporary directory for output operations.
        fs::create_dir_all(&dir.join(DEFAULT_TMP_DIR)).await?;
        Ok(Storage {
            config,
            metadata,
            content,
            cache,
        })
    }

    /// Returns the total space of the disk.
    pub fn total_space(&self) -> Result<u64> {
        self.content.total_space()
    }

    /// Returns the available space of the disk.
    pub fn available_space(&self) -> Result<u64> {
        self.content.available_space()
    }

    /// Checks if the storage has enough space to store the content.
    pub fn has_enough_space(&self, content_length: u64) -> Result<bool> {
        self.content.has_enough_space(content_length)
    }

    /// Hard links the task content to the destination.
    #[instrument(skip_all)]
    pub async fn hard_link_task(&self, task_id: &str, to: &Path) -> Result<()> {
        self.content.hard_link_task(task_id, to).await
    }

    /// Copies the task content to the destination.
    #[instrument(skip_all)]
    pub async fn copy_task(&self, id: &str, to: &Path) -> Result<()> {
        self.content.copy_task(id, to).await
    }

    /// Checks if the task content is on the same device inode as the
    /// destination.
    pub async fn is_same_dev_inode_as_task(&self, id: &str, to: &Path) -> Result<bool> {
        self.content.is_same_dev_inode_as_task(id, to).await
    }

    /// Prepares the metadata of the task when the task downloads
    /// started.
    pub fn prepare_download_task(&self, id: &str) -> Result<(metadata::Task, bool)> {
        self.metadata.prepare_download_task(id)
    }

    /// Updates the metadata of the task and create task content
    /// when the task downloads started.
    #[instrument(skip_all)]
    pub async fn download_task_started(
        &self,
        id: &str,
        piece_length: u64,
        content_length: u64,
        response_header: Option<HeaderMap>,
    ) -> Result<metadata::Task> {
        self.content.create_task(id, content_length).await?;

        self.metadata
            .download_task_started(id, piece_length, content_length, response_header)
    }

    /// Updates the metadata of the task when the task downloads finished.
    #[instrument(skip_all)]
    pub fn download_task_finished(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.download_task_finished(id)
    }

    /// Updates the metadata of the task when the task downloads failed.
    #[instrument(skip_all)]
    pub async fn download_task_failed(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.download_task_failed(id)
    }

    /// Updates the metadata of the task when the task prefetches started.
    #[instrument(skip_all)]
    pub async fn prefetch_task_started(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.prefetch_task_started(id)
    }

    /// Updates the metadata of the task when the task prefetches failed.
    #[instrument(skip_all)]
    pub async fn prefetch_task_failed(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.prefetch_task_failed(id)
    }

    /// Updates the metadata of the task when task uploads finished.
    #[instrument(skip_all)]
    pub fn upload_task_finished(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.upload_task_finished(id)
    }

    /// Returns the task metadata.
    #[instrument(skip_all)]
    pub fn get_task(&self, id: &str) -> Result<Option<metadata::Task>> {
        self.metadata.get_task(id)
    }

    /// Returns whether the task exists.
    #[instrument(skip_all)]
    pub fn is_task_exists(&self, id: &str) -> Result<bool> {
        self.metadata.is_task_exists(id)
    }

    /// Returns the task metadatas.
    #[instrument(skip_all)]
    pub fn get_tasks(&self) -> Result<Vec<metadata::Task>> {
        self.metadata.get_tasks()
    }

    /// Deletes the task metadatas, task content and piece metadatas.
    #[instrument(skip_all)]
    pub async fn delete_task(&self, id: &str) {
        self.metadata
            .delete_task(id)
            .unwrap_or_else(|err| error!("delete task metadata failed: {}", err));

        self.metadata.delete_pieces(id).unwrap_or_else(|err| {
            error!("delete piece metadatas failed: {}", err);
        });

        self.content.delete_task(id).await.unwrap_or_else(|err| {
            error!("delete task content failed: {}", err);
        });

        let mut cache = self.cache.clone();
        cache.delete_task(id).await.unwrap_or_else(|err| {
            info!("delete task from cache failed: {}", err);
        });
    }

    /// Hard links the persistent task content to the destination.
    #[instrument(skip_all)]
    pub async fn hard_link_persistent_task(&self, task_id: &str, to: &Path) -> Result<()> {
        self.content.hard_link_persistent_task(task_id, to).await
    }

    /// Hard links the source file to the persistent task content.
    #[instrument(skip_all)]
    pub async fn hard_link_to_persistent_task(&self, from: &Path, task_id: &str) -> Result<()> {
        self.content
            .hard_link_to_persistent_task(from, task_id)
            .await
    }

    /// Copies the persistent task content to the destination.
    #[instrument(skip_all)]
    pub async fn copy_persistent_task(&self, id: &str, to: &Path) -> Result<()> {
        self.content.copy_persistent_task(id, to).await
    }

    /// Checks if the persistent task content is on the same device inode as the
    /// destination.
    pub async fn is_same_dev_inode_as_persistent_task(&self, id: &str, to: &Path) -> Result<bool> {
        self.content
            .is_same_dev_inode_as_persistent_task(id, to)
            .await
    }

    /// Prepares the metadata of the persistent task
    /// and create directory for the persistent task.
    #[instrument(skip_all)]
    pub async fn create_persistent_task_started(
        &self,
        id: &str,
        ttl: Duration,
        piece_length: u64,
        content_length: u64,
    ) -> Result<metadata::PersistentTask> {
        let metadata =
            self.metadata
                .create_persistent_task_started(id, ttl, piece_length, content_length)?;

        self.content.create_persistent_task_dir(id).await?;
        return Ok(metadata);
    }

    /// Creates and fallocates the persistent task content.
    #[instrument(skip_all)]
    pub async fn create_persistent_task(&self, id: &str, content_length: u64) -> Result<()> {
        self.content
            .create_persistent_task(id, content_length)
            .await
            .map(|_| ())
    }

    /// Updates the metadata of the persistent task
    /// when the persistent task creates finished.
    #[instrument(skip_all)]
    pub async fn create_persistent_task_finished(
        &self,
        id: &str,
    ) -> Result<metadata::PersistentTask> {
        self.metadata.create_persistent_task_finished(id)
    }

    /// Deletes the persistent task when
    /// the persistent task creates failed.
    #[instrument(skip_all)]
    pub async fn create_persistent_task_failed(&self, id: &str) {
        self.delete_persistent_task(id).await;
    }

    /// Updates the metadata of the persistent task
    /// and creates the persistent task content when the persistent task downloads started.
    #[instrument(skip_all)]
    pub async fn download_persistent_task_started(
        &self,
        id: &str,
        ttl: Duration,
        persistent: bool,
        piece_length: u64,
        content_length: u64,
        created_at: NaiveDateTime,
    ) -> Result<metadata::PersistentTask> {
        let metadata = self.metadata.download_persistent_task_started(
            id,
            ttl,
            persistent,
            piece_length,
            content_length,
            created_at,
        )?;

        self.content
            .create_persistent_task(id, content_length)
            .await?;
        Ok(metadata)
    }

    /// Updates the metadata of the persistent task when the persistent task downloads finished.
    #[instrument(skip_all)]
    pub fn download_persistent_task_finished(&self, id: &str) -> Result<metadata::PersistentTask> {
        self.metadata.download_persistent_task_finished(id)
    }

    /// Updates the metadata of the persistent task when the persistent task downloads failed.
    #[instrument(skip_all)]
    pub async fn download_persistent_task_failed(
        &self,
        id: &str,
    ) -> Result<metadata::PersistentTask> {
        self.metadata.download_persistent_task_failed(id)
    }

    /// Updates the metadata of the cahce task when persistent task uploads finished.
    #[instrument(skip_all)]
    pub fn upload_persistent_task_finished(&self, id: &str) -> Result<metadata::PersistentTask> {
        self.metadata.upload_persistent_task_finished(id)
    }

    /// Returns the persistent task metadata.
    #[instrument(skip_all)]
    pub fn get_persistent_task(&self, id: &str) -> Result<Option<metadata::PersistentTask>> {
        self.metadata.get_persistent_task(id)
    }

    /// Persists the persistent task metadata.
    #[instrument(skip_all)]
    pub fn persist_persistent_task(&self, id: &str) -> Result<metadata::PersistentTask> {
        self.metadata.persist_persistent_task(id)
    }

    /// Returns whether the persistent task exists.
    #[instrument(skip_all)]
    pub fn is_persistent_task_exists(&self, id: &str) -> Result<bool> {
        self.metadata.is_persistent_task_exists(id)
    }

    /// Returns the task metadatas.
    #[instrument(skip_all)]
    pub fn get_persistent_tasks(&self) -> Result<Vec<metadata::PersistentTask>> {
        self.metadata.get_persistent_tasks()
    }

    /// Deletes the persistent task metadatas, persistent task content and piece metadatas.
    #[instrument(skip_all)]
    pub async fn delete_persistent_task(&self, id: &str) {
        self.metadata
            .delete_persistent_task(id)
            .unwrap_or_else(|err| {
                error!("delete persistent task metadata failed: {}", err);
            });

        self.metadata.delete_pieces(id).unwrap_or_else(|err| {
            error!("delete persistent piece metadatas failed: {}", err);
        });

        self.content
            .delete_persistent_task(id)
            .await
            .unwrap_or_else(|err| {
                error!("delete persistent task content failed: {}", err);
            });
    }

    /// Hard links the persistent cache task content to the destination.
    #[instrument(skip_all)]
    pub async fn hard_link_persistent_cache_task(&self, task_id: &str, to: &Path) -> Result<()> {
        self.content
            .hard_link_persistent_cache_task(task_id, to)
            .await
    }

    /// Hard links the source file to the persistent cache task content.
    #[instrument(skip_all)]
    pub async fn hard_link_to_persistent_cache_task(
        &self,
        from: &Path,
        task_id: &str,
    ) -> Result<()> {
        self.content
            .hard_link_to_persistent_cache_task(from, task_id)
            .await
    }

    /// Copies the persistent cache task content to the destination.
    #[instrument(skip_all)]
    pub async fn copy_persistent_cache_task(&self, id: &str, to: &Path) -> Result<()> {
        self.content.copy_persistent_cache_task(id, to).await
    }

    /// Checks if the persistent cache task content is on the same device inode as the
    /// destination.
    pub async fn is_same_dev_inode_as_persistent_cache_task(
        &self,
        id: &str,
        to: &Path,
    ) -> Result<bool> {
        self.content
            .is_same_dev_inode_as_persistent_cache_task(id, to)
            .await
    }

    /// Prepares the metadata of the persistent cache task
    /// and create directory for the persistent cache task.
    #[instrument(skip_all)]
    pub async fn create_persistent_cache_task_started(
        &self,
        id: &str,
        ttl: Duration,
        piece_length: u64,
        content_length: u64,
    ) -> Result<metadata::PersistentCacheTask> {
        let metadata = self.metadata.create_persistent_cache_task_started(
            id,
            ttl,
            piece_length,
            content_length,
        )?;

        self.content.create_persistent_cache_task_dir(id).await?;
        return Ok(metadata);
    }

    /// Creates and fallocates the persistent cache task content.
    #[instrument(skip_all)]
    pub async fn create_persistent_cache_task(&self, id: &str, content_length: u64) -> Result<()> {
        self.content
            .create_persistent_cache_task(id, content_length)
            .await
            .map(|_| ())
    }

    /// Updates the metadata of the persistent cache task
    /// when the persistent cache task creates finished.
    #[instrument(skip_all)]
    pub async fn create_persistent_cache_task_finished(
        &self,
        id: &str,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata.create_persistent_cache_task_finished(id)
    }

    /// Deletes the persistent cache task when
    /// the persistent cache task creates failed.
    #[instrument(skip_all)]
    pub async fn create_persistent_cache_task_failed(&self, id: &str) {
        self.delete_persistent_cache_task(id).await;
    }

    /// Updates the metadata of the persistent cache task
    /// and creates the persistent cache task content when the persistent cache task downloads started.
    #[instrument(skip_all)]
    pub async fn download_persistent_cache_task_started(
        &self,
        id: &str,
        ttl: Duration,
        persistent: bool,
        piece_length: u64,
        content_length: u64,
        created_at: NaiveDateTime,
    ) -> Result<metadata::PersistentCacheTask> {
        let metadata = self.metadata.download_persistent_cache_task_started(
            id,
            ttl,
            persistent,
            piece_length,
            content_length,
            created_at,
        )?;

        self.content
            .create_persistent_cache_task(id, content_length)
            .await?;
        Ok(metadata)
    }

    /// Updates the metadata of the persistent cache task when the persistent cache task downloads finished.
    #[instrument(skip_all)]
    pub fn download_persistent_cache_task_finished(
        &self,
        id: &str,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata.download_persistent_cache_task_finished(id)
    }

    /// Updates the metadata of the persistent cache task when the persistent cache task downloads failed.
    #[instrument(skip_all)]
    pub async fn download_persistent_cache_task_failed(
        &self,
        id: &str,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata.download_persistent_cache_task_failed(id)
    }

    /// Updates the metadata of the cahce task when persistent cache task uploads finished.
    #[instrument(skip_all)]
    pub fn upload_persistent_cache_task_finished(
        &self,
        id: &str,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata.upload_persistent_cache_task_finished(id)
    }

    /// Returns the persistent cache task metadata.
    #[instrument(skip_all)]
    pub fn get_persistent_cache_task(
        &self,
        id: &str,
    ) -> Result<Option<metadata::PersistentCacheTask>> {
        self.metadata.get_persistent_cache_task(id)
    }

    /// Persists the persistent cache task metadata.
    #[instrument(skip_all)]
    pub fn persist_persistent_cache_task(&self, id: &str) -> Result<metadata::PersistentCacheTask> {
        self.metadata.persist_persistent_cache_task(id)
    }

    /// Returns whether the persistent cache task exists.
    #[instrument(skip_all)]
    pub fn is_persistent_cache_task_exists(&self, id: &str) -> Result<bool> {
        self.metadata.is_persistent_cache_task_exists(id)
    }

    /// Returns the task metadatas.
    #[instrument(skip_all)]
    pub fn get_persistent_cache_tasks(&self) -> Result<Vec<metadata::PersistentCacheTask>> {
        self.metadata.get_persistent_cache_tasks()
    }

    /// Deletes the persistent cache task metadatas, persistent cache task content and piece metadatas.
    #[instrument(skip_all)]
    pub async fn delete_persistent_cache_task(&self, id: &str) {
        self.metadata
            .delete_persistent_cache_task(id)
            .unwrap_or_else(|err| {
                error!("delete persistent cache task metadata failed: {}", err);
            });

        self.metadata.delete_pieces(id).unwrap_or_else(|err| {
            error!("delete persistent cache piece metadatas failed: {}", err);
        });

        self.content
            .delete_persistent_cache_task(id)
            .await
            .unwrap_or_else(|err| {
                error!("delete persistent cache task content failed: {}", err);
            });
    }

    /// Updates the metadata of the cache task and create cache task content
    /// when the cache task downloads started.
    #[instrument(skip_all)]
    pub async fn download_cache_task_started(
        &self,
        id: &str,
        piece_length: u64,
        content_length: u64,
        response_header: Option<HeaderMap>,
    ) -> Result<metadata::CacheTask> {
        let mut cache = self.cache.clone();
        cache.put_task(id, content_length).await;

        self.metadata
            .download_cache_task_started(id, piece_length, content_length, response_header)
    }

    /// Updates the metadata of the cache task when the cache task downloads finished.
    #[instrument(skip_all)]
    pub fn download_cache_task_finished(&self, id: &str) -> Result<metadata::CacheTask> {
        self.metadata.download_cache_task_finished(id)
    }

    /// Updates the metadata of the cache task when the cache task downloads failed.
    #[instrument(skip_all)]
    pub async fn download_cache_task_failed(&self, id: &str) -> Result<metadata::CacheTask> {
        self.metadata.download_cache_task_failed(id)
    }

    /// Updates the metadata of the cache task when the cache task uploads finished.
    #[instrument(skip_all)]
    pub fn upload_cache_task_finished(&self, id: &str) -> Result<metadata::CacheTask> {
        self.metadata.upload_cache_task_finished(id)
    }

    /// Returns the cache task metadata.
    #[instrument(skip_all)]
    pub fn get_cache_task(&self, id: &str) -> Result<Option<metadata::CacheTask>> {
        self.metadata.get_cache_task(id)
    }

    /// Returns whether the cache task exists.
    #[instrument(skip_all)]
    pub fn is_cache_task_exists(&self, id: &str) -> Result<bool> {
        self.metadata.is_cache_task_exists(id)
    }

    /// Returns the cache task metadatas.
    #[instrument(skip_all)]
    pub fn get_cache_tasks(&self) -> Result<Vec<metadata::CacheTask>> {
        self.metadata.get_cache_tasks()
    }

    /// Deletes the cache task metadatas, cache task content and piece metadatas.
    #[instrument(skip_all)]
    pub async fn delete_cache_task(&self, id: &str) {
        self.metadata
            .delete_cache_task(id)
            .unwrap_or_else(|err| error!("delete cache task metadata failed: {}", err));

        self.metadata.delete_pieces(id).unwrap_or_else(|err| {
            error!("delete cache piece metadatas failed: {}", err);
        });

        let mut cache = self.cache.clone();
        cache.delete_task(id).await.unwrap_or_else(|err| {
            info!("delete cache task from cache failed: {}", err);
        });
    }

    /// Creates a new persistent piece.
    #[instrument(level = "debug", skip_all)]
    pub async fn create_persistent_piece<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        number: u32,
        offset: u64,
        length: u64,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self
            .content
            .write_persistent_piece(task_id, offset, length, reader)
            .await?;
        let digest = Digest::new(Algorithm::Crc32, response.hash);

        self.metadata.create_persistent_piece(
            piece_id,
            number,
            offset,
            length,
            digest.to_string().as_str(),
        )
    }

    /// Registers a persistent piece without calculating its digest.
    /// Used when creating a hardlink from persistent to storage. Since the piece
    /// content is accessed via hardlink, digest calculation is deferred until another
    /// peer downloads the piece.
    #[instrument(level = "debug", skip_all)]
    pub fn register_persistent_piece(
        &self,
        piece_id: &str,
        number: u32,
        offset: u64,
        length: u64,
    ) -> Result<metadata::Piece> {
        self.metadata.create_persistent_piece(
            piece_id,
            number,
            offset,
            length,
            "".to_string().as_str(),
        )
    }

    /// Creates a new persistent cache piece.
    #[instrument(level = "debug", skip_all)]
    pub async fn create_persistent_cache_piece<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        number: u32,
        offset: u64,
        length: u64,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self
            .content
            .write_persistent_cache_piece(task_id, offset, length, reader)
            .await?;
        let digest = Digest::new(Algorithm::Crc32, response.hash);

        self.metadata.create_persistent_cache_piece(
            piece_id,
            number,
            offset,
            length,
            digest.to_string().as_str(),
        )
    }

    /// Registers a persistent cache piece without calculating its digest.
    /// Used when creating a hardlink from persistent cache to storage. Since the piece
    /// content is accessed via hardlink, digest calculation is deferred until another
    /// peer downloads the piece.
    #[instrument(level = "debug", skip_all)]
    pub fn register_persistent_cache_piece(
        &self,
        piece_id: &str,
        number: u32,
        offset: u64,
        length: u64,
    ) -> Result<metadata::Piece> {
        self.metadata.create_persistent_cache_piece(
            piece_id,
            number,
            offset,
            length,
            "".to_string().as_str(),
        )
    }

    /// Updates the metadata of the piece and writes
    /// the data of piece to file when the piece downloads started.
    #[instrument(level = "debug", skip_all)]
    pub async fn download_piece_started(
        &self,
        piece_id: &str,
        number: u32,
    ) -> Result<metadata::Piece> {
        // Wait for the piece to be finished.
        match self.wait_for_piece_finished(piece_id).await {
            Ok(piece) => Ok(piece),
            // If piece is not found or wait timeout, create piece metadata.
            Err(_) => self.metadata.download_piece_started(piece_id, number),
        }
    }

    /// Used for downloading piece from source.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all)]
    pub async fn download_piece_from_source_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        reader: &mut R,
        timeout: Duration,
    ) -> Result<metadata::Piece> {
        tokio::select! {
            piece = self.handle_downloaded_from_source_finished(piece_id, task_id, offset, length, reader) => {
                piece
            }
            _ = sleep(timeout) => {
                Err(Error::DownloadPieceFinishedTimeout(piece_id.to_string()))
            }
        }
    }

    // handle_downloaded_from_source_finished handles the downloaded piece from source.
    #[instrument(skip_all)]
    async fn handle_downloaded_from_source_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self
            .content
            .write_piece(task_id, offset, length, reader)
            .await?;

        let digest = Digest::new(Algorithm::Crc32, response.hash);
        self.metadata.download_piece_finished(
            piece_id,
            offset,
            length,
            digest.to_string().as_str(),
            None,
        )
    }

    /// Used for downloading piece from parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all)]
    pub async fn download_piece_from_parent_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        expected_digest: &str,
        parent_id: &str,
        reader: &mut R,
        timeout: Duration,
    ) -> Result<metadata::Piece> {
        tokio::select! {
            piece = self.handle_downloaded_piece_from_parent_finished(piece_id, task_id, offset, length, expected_digest, parent_id, reader) => {
                piece
            }
            _ = sleep(timeout) => {
                Err(Error::DownloadPieceFinishedTimeout(piece_id.to_string()))
            }
        }
    }

    // handle_downloaded_piece_from_parent_finished handles the downloaded piece from parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all)]
    async fn handle_downloaded_piece_from_parent_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        expected_digest: &str,
        parent_id: &str,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self
            .content
            .write_piece(task_id, offset, length, reader)
            .await?;

        let length = response.length;
        let digest = Digest::new(Algorithm::Crc32, response.hash);

        // Check the digest of the piece.
        if expected_digest.is_empty() {
            warn!(
                "expected digest is empty for piece {} downloaded from parent {}",
                piece_id, parent_id
            );
        } else if expected_digest != digest.to_string() {
            return Err(Error::DigestMismatch(
                expected_digest.to_string(),
                digest.to_string(),
            ));
        }

        self.metadata.download_piece_finished(
            piece_id,
            offset,
            length,
            digest.to_string().as_str(),
            Some(parent_id.to_string()),
        )
    }

    /// Updates the metadata of the piece when the piece downloads failed.
    #[instrument(level = "debug", skip_all)]
    pub fn download_piece_failed(&self, piece_id: &str) -> Result<()> {
        self.metadata.download_piece_failed(piece_id)
    }

    /// Updates the metadata of the piece and
    /// returns the data of the piece.
    #[instrument(level = "debug", skip_all)]
    pub async fn upload_piece(
        &self,
        piece_id: &str,
        task_id: &str,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        // Wait for the piece to be finished.
        self.wait_for_piece_finished(piece_id).await?;

        // Start uploading the task.
        self.metadata.upload_task_started(task_id)?;

        // Get the piece metadata and return the content of the piece.
        match self.metadata.get_piece(piece_id) {
            Ok(Some(piece)) => {
                if self.cache.contains_piece(task_id, piece_id).await {
                    match self
                        .cache
                        .read_piece(task_id, piece_id, piece.clone(), range)
                        .await
                    {
                        Ok(reader) => {
                            // Finish uploading the task.
                            self.metadata.upload_task_finished(task_id)?;
                            debug!("get piece from cache: {}", piece_id);
                            return Ok(Either::Left(reader));
                        }
                        Err(err) => {
                            return Err(err);
                        }
                    }
                }

                match self
                    .content
                    .read_piece(task_id, piece.offset, piece.length, range)
                    .await
                {
                    Ok(reader) => {
                        // Finish uploading the task.
                        self.metadata.upload_task_finished(task_id)?;
                        Ok(Either::Right(reader))
                    }
                    Err(err) => {
                        // Failed uploading the task.
                        self.metadata.upload_task_failed(task_id)?;
                        Err(err)
                    }
                }
            }
            Ok(None) => {
                // Failed uploading the task.
                self.metadata.upload_task_failed(task_id)?;
                Err(Error::PieceNotFound(piece_id.to_string()))
            }
            Err(err) => {
                // Failed uploading the task.
                self.metadata.upload_task_failed(task_id)?;
                Err(err)
            }
        }
    }

    /// Returns the piece metadata.
    pub fn get_piece(&self, piece_id: &str) -> Result<Option<metadata::Piece>> {
        self.metadata.get_piece(piece_id)
    }

    /// Returns whether the piece exists.
    #[instrument(level = "debug", skip_all)]
    pub fn is_piece_exists(&self, piece_id: &str) -> Result<bool> {
        self.metadata.is_piece_exists(piece_id)
    }

    /// Returns the piece metadatas.
    #[instrument(level = "debug", skip_all)]
    pub fn get_pieces(&self, task_id: &str) -> Result<Vec<metadata::Piece>> {
        self.metadata.get_pieces(task_id)
    }

    /// Returns the piece id.
    #[inline]
    pub fn piece_id(&self, task_id: &str, number: u32) -> String {
        self.metadata.piece_id(task_id, number)
    }

    /// Updates the metadata of the persistent piece and writes
    /// the data of piece to file when the persistent piece downloads started.
    #[instrument(level = "debug", skip_all)]
    pub async fn download_persistent_piece_started(
        &self,
        piece_id: &str,
        number: u32,
    ) -> Result<metadata::Piece> {
        // Wait for the piece to be finished.
        match self.wait_for_persistent_piece_finished(piece_id).await {
            Ok(piece) => Ok(piece),
            // If piece is not found or wait timeout, create piece metadata.
            Err(_) => self.metadata.download_piece_started(piece_id, number),
        }
    }

    /// Used for downloading persistent piece from parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all)]
    pub async fn download_persistent_piece_from_parent_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        expected_digest: &str,
        parent_id: &str,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self
            .content
            .write_persistent_piece(task_id, offset, length, reader)
            .await?;

        let length = response.length;
        let digest = Digest::new(Algorithm::Crc32, response.hash);

        // Check the digest of the piece.
        if expected_digest.is_empty() {
            warn!(
                "expected digest is empty for piece {} downloaded from parent {}",
                piece_id, parent_id
            );
        } else if expected_digest != digest.to_string() {
            return Err(Error::DigestMismatch(
                expected_digest.to_string(),
                digest.to_string(),
            ));
        }

        self.metadata.download_piece_finished(
            piece_id,
            offset,
            length,
            digest.to_string().as_str(),
            Some(parent_id.to_string()),
        )
    }

    /// Used for downloading piece from source.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all)]
    pub async fn download_persistent_piece_from_source_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        reader: &mut R,
        timeout: Duration,
    ) -> Result<metadata::Piece> {
        tokio::select! {
            piece = self.handle_persistent_downloaded_from_source_finished(piece_id, task_id, offset, length, reader) => {
                piece
            }
            _ = sleep(timeout) => {
                Err(Error::DownloadPieceFinishedTimeout(piece_id.to_string()))
            }
        }
    }

    // handle_persistent_downloaded_from_source_finished handles the downloaded persistent piece from source.
    #[instrument(skip_all)]
    async fn handle_persistent_downloaded_from_source_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self
            .content
            .write_persistent_piece(task_id, offset, length, reader)
            .await?;

        let digest = Digest::new(Algorithm::Crc32, response.hash);
        self.metadata.download_piece_finished(
            piece_id,
            offset,
            length,
            digest.to_string().as_str(),
            None,
        )
    }

    /// Updates the metadata of the persistent piece when the persistent piece downloads failed.
    #[instrument(level = "debug", skip_all)]
    pub fn download_persistent_piece_failed(&self, piece_id: &str) -> Result<()> {
        self.metadata.download_piece_failed(piece_id)
    }

    /// Updates the metadata of the piece and_then
    /// returns the data of the piece.
    #[instrument(level = "debug", skip_all)]
    pub async fn upload_persistent_piece(
        &self,
        piece_id: &str,
        task_id: &str,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        // Wait for the persistent piece to be finished.
        self.wait_for_persistent_piece_finished(piece_id).await?;

        // Start uploading the persistent task.
        self.metadata.upload_persistent_task_started(task_id)?;

        // Get the persistent piece metadata and return the content of the persistent piece.
        match self.metadata.get_piece(piece_id) {
            Ok(Some(piece)) => {
                match self
                    .content
                    .read_persistent_piece(task_id, piece.offset, piece.length, range)
                    .await
                {
                    Ok(reader) => {
                        // Finish uploading the persistent task.
                        self.metadata.upload_persistent_task_finished(task_id)?;
                        Ok(reader)
                    }
                    Err(err) => {
                        // Failed uploading the persistent task.
                        self.metadata.upload_persistent_task_failed(task_id)?;
                        Err(err)
                    }
                }
            }
            Ok(None) => {
                // Failed uploading the persistent task.
                self.metadata.upload_persistent_task_failed(task_id)?;
                Err(Error::PieceNotFound(piece_id.to_string()))
            }
            Err(err) => {
                // Failed uploading the persistent task.
                self.metadata.upload_persistent_task_failed(task_id)?;
                Err(err)
            }
        }
    }

    /// Returns the persistent piece metadata.
    #[instrument(level = "debug", skip_all)]
    pub fn get_persistent_piece(&self, piece_id: &str) -> Result<Option<metadata::Piece>> {
        self.metadata.get_piece(piece_id)
    }

    /// Returns whether the persistent piece exists.
    #[instrument(level = "debug", skip_all)]
    pub fn is_persistent_piece_exists(&self, piece_id: &str) -> Result<bool> {
        self.metadata.is_piece_exists(piece_id)
    }

    /// Returns the persistent piece metadatas.
    pub fn get_persistent_pieces(&self, task_id: &str) -> Result<Vec<metadata::Piece>> {
        self.metadata.get_pieces(task_id)
    }

    /// Returns the persistent piece id.
    #[inline]
    pub fn persistent_piece_id(&self, task_id: &str, number: u32) -> String {
        self.metadata.piece_id(task_id, number)
    }

    /// Updates the metadata of the persistent cache piece and writes
    /// the data of piece to file when the persistent cache piece downloads started.
    #[instrument(level = "debug", skip_all)]
    pub async fn download_persistent_cache_piece_started(
        &self,
        piece_id: &str,
        number: u32,
    ) -> Result<metadata::Piece> {
        // Wait for the piece to be finished.
        match self
            .wait_for_persistent_cache_piece_finished(piece_id)
            .await
        {
            Ok(piece) => Ok(piece),
            // If piece is not found or wait timeout, create piece metadata.
            Err(_) => self.metadata.download_piece_started(piece_id, number),
        }
    }

    /// Used for downloading persistent cache piece from parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all)]
    pub async fn download_persistent_cache_piece_from_parent_finished<
        R: AsyncRead + Unpin + ?Sized,
    >(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        expected_digest: &str,
        parent_id: &str,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self
            .content
            .write_persistent_cache_piece(task_id, offset, length, reader)
            .await?;

        let length = response.length;
        let digest = Digest::new(Algorithm::Crc32, response.hash);

        // Check the digest of the piece.
        if expected_digest.is_empty() {
            warn!(
                "expected digest is empty for piece {} downloaded from parent {}",
                piece_id, parent_id
            );
        } else if expected_digest != digest.to_string() {
            return Err(Error::DigestMismatch(
                expected_digest.to_string(),
                digest.to_string(),
            ));
        }

        self.metadata.download_piece_finished(
            piece_id,
            offset,
            length,
            digest.to_string().as_str(),
            Some(parent_id.to_string()),
        )
    }

    /// Updates the metadata of the persistent cache piece when the persistent cache piece downloads failed.
    #[instrument(level = "debug", skip_all)]
    pub fn download_persistent_cache_piece_failed(&self, piece_id: &str) -> Result<()> {
        self.metadata.download_piece_failed(piece_id)
    }

    /// Updates the metadata of the piece and_then
    /// returns the data of the piece.
    #[instrument(level = "debug", skip_all)]
    pub async fn upload_persistent_cache_piece(
        &self,
        piece_id: &str,
        task_id: &str,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        // Wait for the persistent cache piece to be finished.
        self.wait_for_persistent_cache_piece_finished(piece_id)
            .await?;

        // Start uploading the persistent cache task.
        self.metadata
            .upload_persistent_cache_task_started(task_id)?;

        // Get the persistent cache piece metadata and return the content of the persistent cache piece.
        match self.metadata.get_piece(piece_id) {
            Ok(Some(piece)) => {
                match self
                    .content
                    .read_persistent_cache_piece(task_id, piece.offset, piece.length, range)
                    .await
                {
                    Ok(reader) => {
                        // Finish uploading the persistent cache task.
                        self.metadata
                            .upload_persistent_cache_task_finished(task_id)?;
                        Ok(reader)
                    }
                    Err(err) => {
                        // Failed uploading the persistent cache task.
                        self.metadata.upload_persistent_cache_task_failed(task_id)?;
                        Err(err)
                    }
                }
            }
            Ok(None) => {
                // Failed uploading the persistent cache task.
                self.metadata.upload_persistent_cache_task_failed(task_id)?;
                Err(Error::PieceNotFound(piece_id.to_string()))
            }
            Err(err) => {
                // Failed uploading the persistent cache task.
                self.metadata.upload_persistent_cache_task_failed(task_id)?;
                Err(err)
            }
        }
    }

    /// Returns the persistent cache piece metadata.
    #[instrument(level = "debug", skip_all)]
    pub fn get_persistent_cache_piece(&self, piece_id: &str) -> Result<Option<metadata::Piece>> {
        self.metadata.get_piece(piece_id)
    }

    /// Returns whether the persistent cache piece exists.
    #[instrument(level = "debug", skip_all)]
    pub fn is_persistent_cache_piece_exists(&self, piece_id: &str) -> Result<bool> {
        self.metadata.is_piece_exists(piece_id)
    }

    /// Returns the persistent cache piece metadatas.
    pub fn get_persistent_cache_pieces(&self, task_id: &str) -> Result<Vec<metadata::Piece>> {
        self.metadata.get_pieces(task_id)
    }

    /// Returns the persistent cache piece id.
    #[inline]
    pub fn persistent_cache_piece_id(&self, task_id: &str, number: u32) -> String {
        self.metadata.piece_id(task_id, number)
    }

    /// Waits for the piece to be finished.
    #[instrument(level = "debug", skip_all)]
    async fn wait_for_piece_finished(&self, piece_id: &str) -> Result<metadata::Piece> {
        // Total timeout for downloading a piece, combining the download time and the time to write to storage.
        let wait_timeout = tokio::time::sleep(
            self.config.download.piece_timeout + self.config.storage.write_piece_timeout,
        );
        tokio::pin!(wait_timeout);

        let mut interval = tokio::time::interval(DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let piece = self
                        .get_piece(piece_id)?
                        .ok_or_else(|| Error::PieceNotFound(piece_id.to_string()))?;

                    // If the piece is finished, return.
                    if piece.is_finished() {
                        debug!("wait piece finished success");
                        return Ok(piece);
                    }
                }
                _ = &mut wait_timeout => {
                    self.metadata.wait_for_piece_finished_failed(piece_id).unwrap_or_else(|err| error!("delete piece metadata failed: {}", err));
                    return Err(Error::WaitForPieceFinishedTimeout(piece_id.to_string()));
                }
            }
        }
    }

    /// Waits for the persistent piece to be finished.
    #[instrument(level = "debug", skip_all)]
    async fn wait_for_persistent_piece_finished(&self, piece_id: &str) -> Result<metadata::Piece> {
        // Total timeout for downloading a piece, combining the download time and the time to write to storage.
        let wait_timeout = tokio::time::sleep(
            self.config.download.piece_timeout + self.config.storage.write_piece_timeout,
        );
        tokio::pin!(wait_timeout);

        let mut interval = tokio::time::interval(DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let piece = self
                        .get_persistent_piece(piece_id)?
                        .ok_or_else(|| Error::PieceNotFound(piece_id.to_string()))?;

                    // If the piece is finished, return.
                    if piece.is_finished() {
                        debug!("wait piece finished success");
                        return Ok(piece);
                    }
                }
                _ = &mut wait_timeout => {
                    self.metadata.wait_for_piece_finished_failed(piece_id).unwrap_or_else(|err| error!("delete piece metadata failed: {}", err));
                    return Err(Error::WaitForPieceFinishedTimeout(piece_id.to_string()));
                }
            }
        }
    }

    /// Waits for the persistent cache piece to be finished.
    #[instrument(level = "debug", skip_all)]
    async fn wait_for_persistent_cache_piece_finished(
        &self,
        piece_id: &str,
    ) -> Result<metadata::Piece> {
        // Total timeout for downloading a piece, combining the download time and the time to write to storage.
        let wait_timeout = tokio::time::sleep(
            self.config.download.piece_timeout + self.config.storage.write_piece_timeout,
        );
        tokio::pin!(wait_timeout);

        let mut interval = tokio::time::interval(DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let piece = self
                        .get_persistent_cache_piece(piece_id)?
                        .ok_or_else(|| Error::PieceNotFound(piece_id.to_string()))?;

                    // If the piece is finished, return.
                    if piece.is_finished() {
                        debug!("wait piece finished success");
                        return Ok(piece);
                    }
                }
                _ = &mut wait_timeout => {
                    self.metadata.wait_for_piece_finished_failed(piece_id).unwrap_or_else(|err| error!("delete piece metadata failed: {}", err));
                    return Err(Error::WaitForPieceFinishedTimeout(piece_id.to_string()));
                }
            }
        }
    }

    /// Updates the metadata of the cache piece and writes
    /// the data of cache piece to file when the cache piece downloads started.
    #[instrument(level = "debug", skip_all)]
    pub async fn download_cache_piece_started(
        &self,
        piece_id: &str,
        number: u32,
    ) -> Result<metadata::Piece> {
        // Wait for the piece to be finished.
        match self.wait_for_cache_piece_finished(piece_id).await {
            Ok(piece) => Ok(piece),
            // If piece is not found or wait timeout, create piece metadata.
            Err(_) => self.metadata.download_piece_started(piece_id, number),
        }
    }

    /// Used for downloading cache piece from source.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all)]
    pub async fn download_cache_piece_from_source_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        reader: &mut R,
        timeout: Duration,
    ) -> Result<metadata::Piece> {
        tokio::select! {
            piece = self.handle_downloaded_cache_piece_from_source_finished(piece_id, task_id, offset, length, reader) => {
                piece
            }
            _ = sleep(timeout) => {
                Err(Error::DownloadPieceFinishedTimeout(piece_id.to_string()))
            }
        }
    }

    // handle_downloaded_cache_piece_from_source_finished handles the downloaded cache piece from source.
    #[instrument(level = "debug", skip_all)]
    async fn handle_downloaded_cache_piece_from_source_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let mut hasher = crc32fast::Hasher::new();
        let mut content = BytesMut::with_capacity(length as usize);
        let mut tee = InspectReader::new(reader, |bytes| {
            hasher.update(bytes);
        });

        // Keep reading until EOF to avoid truncating the piece content.
        while tee.read_buf(&mut content).await? > 0 {}

        self.cache
            .write_piece(task_id, piece_id, content.freeze())
            .await?;

        let hash = hasher.finalize().to_string();
        let digest = Digest::new(Algorithm::Crc32, hash);
        self.metadata.download_piece_finished(
            piece_id,
            offset,
            length,
            digest.to_string().as_str(),
            None,
        )
    }

    /// Used for downloading cache piece from parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all)]
    pub async fn download_cache_piece_from_parent_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        expected_digest: &str,
        parent_id: &str,
        reader: &mut R,
        timeout: Duration,
    ) -> Result<metadata::Piece> {
        tokio::select! {
            piece = self.handle_downloaded_cache_piece_from_parent_finished(piece_id, task_id, offset, length, expected_digest, parent_id, reader) => {
                piece
            }
            _ = sleep(timeout) => {
                Err(Error::DownloadPieceFinishedTimeout(piece_id.to_string()))
            }
        }
    }

    // handle_downloaded_cache_piece_from_parent_finished handles the downloaded cache piece from parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all)]
    async fn handle_downloaded_cache_piece_from_parent_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        expected_digest: &str,
        parent_id: &str,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let mut hasher = crc32fast::Hasher::new();
        let mut content = BytesMut::with_capacity(length as usize);
        let mut tee = InspectReader::new(reader, |bytes| {
            hasher.update(bytes);
        });

        // Keep reading until EOF to avoid truncating the piece content.
        while tee.read_buf(&mut content).await? > 0 {}

        self.cache
            .write_piece(task_id, piece_id, content.freeze())
            .await?;

        let hash = hasher.finalize().to_string();
        let digest = Digest::new(Algorithm::Crc32, hash);

        // Check the digest of the piece.
        if expected_digest.is_empty() {
            warn!(
                "expected digest is empty for piece {} downloaded from parent {}",
                piece_id, parent_id
            );
        } else if expected_digest != digest.to_string() {
            return Err(Error::DigestMismatch(
                expected_digest.to_string(),
                digest.to_string(),
            ));
        }

        self.metadata.download_piece_finished(
            piece_id,
            offset,
            length,
            digest.to_string().as_str(),
            Some(parent_id.to_string()),
        )
    }
    /// Updates the metadata of the cache piece when the cache piece downloads failed.
    #[instrument(level = "debug", skip_all)]
    pub fn download_cache_piece_failed(&self, piece_id: &str) -> Result<()> {
        self.metadata.download_piece_failed(piece_id)
    }

    /// Updates the metadata of the piece and
    /// returns the data of the piece.
    #[instrument(level = "debug", skip_all)]
    pub async fn upload_cache_piece(
        &self,
        piece_id: &str,
        task_id: &str,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        // Wait for the cache piece to be finished.
        self.wait_for_cache_piece_finished(piece_id).await?;

        // Start uploading the task.
        self.metadata.upload_cache_task_started(task_id)?;

        // Get the piece metadata and return the content of the piece.
        match self.metadata.get_piece(piece_id) {
            Ok(Some(piece)) => {
                if self.cache.contains_piece(task_id, piece_id).await {
                    match self
                        .cache
                        .read_piece(task_id, piece_id, piece.clone(), range)
                        .await
                    {
                        Ok(reader) => {
                            // Finish uploading the task.
                            self.metadata.upload_cache_task_finished(task_id)?;
                            debug!("get piece from cache: {}", piece_id);
                            Ok(reader)
                        }
                        Err(err) => {
                            // Failed uploading the cache task.
                            self.metadata.upload_cache_task_failed(task_id)?;
                            Err(err)
                        }
                    }
                } else {
                    // Failed uploading the cache task.
                    self.metadata.upload_cache_task_failed(task_id)?;
                    Err(Error::PieceNotFound(piece_id.to_string()))
                }
            }
            Ok(None) => {
                // Failed uploading the cache task.
                self.metadata.upload_cache_task_failed(task_id)?;
                Err(Error::PieceNotFound(piece_id.to_string()))
            }
            Err(err) => {
                // Failed uploading the cache task.
                self.metadata.upload_cache_task_failed(task_id)?;
                Err(err)
            }
        }
    }

    /// Returns the cache piece metadata.
    pub fn get_cache_piece(&self, piece_id: &str) -> Result<Option<metadata::Piece>> {
        self.metadata.get_piece(piece_id)
    }

    /// Returns whether the cache piece exists.
    #[instrument(level = "debug", skip_all)]
    pub fn is_cache_piece_exists(&self, piece_id: &str) -> Result<bool> {
        self.metadata.is_piece_exists(piece_id)
    }

    /// Returns the cache piece metadatas.
    #[instrument(level = "debug", skip_all)]
    pub fn get_cache_pieces(&self, task_id: &str) -> Result<Vec<metadata::Piece>> {
        self.metadata.get_pieces(task_id)
    }

    /// Returns the cache piece id.
    #[inline]
    pub fn cache_piece_id(&self, task_id: &str, number: u32) -> String {
        self.metadata.piece_id(task_id, number)
    }

    /// Waits for the cache piece to be finished.
    #[instrument(level = "debug", skip_all)]
    async fn wait_for_cache_piece_finished(&self, piece_id: &str) -> Result<metadata::Piece> {
        // Total timeout for downloading a piece, combining the download time and the time to write to storage.
        let wait_timeout = tokio::time::sleep(
            self.config.download.piece_timeout + self.config.storage.write_piece_timeout,
        );
        tokio::pin!(wait_timeout);

        let mut interval = tokio::time::interval(DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let piece = self
                        .get_cache_piece(piece_id)?
                        .ok_or_else(|| Error::PieceNotFound(piece_id.to_string()))?;

                    // If the piece is finished, return.
                    if piece.is_finished() {
                        debug!("wait piece finished success");
                        return Ok(piece);
                    }
                }
                _ = &mut wait_timeout => {
                    self.metadata.wait_for_piece_finished_failed(piece_id).unwrap_or_else(|err| error!("delete piece metadata failed: {}", err));
                    return Err(Error::WaitForPieceFinishedTimeout(piece_id.to_string()));
                }
            }
        }
    }
}
