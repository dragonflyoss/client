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
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use dragonfly_client_util::digest::{Algorithm, Digest};
use reqwest::header::HeaderMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncRead;
use tracing::{debug, error, instrument};

pub mod content;
pub mod metadata;
pub mod storage_engine;

/// DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL is the default interval for waiting for the piece to be finished.
pub const DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL: Duration = Duration::from_millis(100);

/// Storage is the storage of the task.
pub struct Storage {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// metadata implements the metadata storage.
    metadata: metadata::Metadata,

    /// content implements the content storage.
    content: content::Content,
}

/// Storage implements the storage.
impl Storage {
    /// new returns a new storage.
    #[instrument(skip_all)]
    pub async fn new(config: Arc<Config>, dir: &Path, log_dir: PathBuf) -> Result<Self> {
        let metadata = metadata::Metadata::new(config.clone(), dir, &log_dir)?;
        let content = content::Content::new(config.clone(), dir).await?;
        Ok(Storage {
            config,
            metadata,
            content,
        })
    }

    /// hard_link_or_copy_task hard links or copies the task content to the destination.
    #[instrument(skip_all)]
    pub async fn hard_link_or_copy_task(
        &self,
        task: &metadata::Task,
        to: &Path,
        range: Option<Range>,
    ) -> Result<()> {
        self.content.hard_link_or_copy_task(task, to, range).await
    }

    /// read_task_by_range returns the reader of the task by range.
    #[instrument(skip_all)]
    pub async fn read_task_by_range(
        &self,
        task_id: &str,
        range: Range,
    ) -> Result<impl AsyncRead + 'static> {
        self.content.read_task_by_range(task_id, range).await
    }

    /// download_task_started updates the metadata of the task when the task downloads started.
    #[instrument(skip_all)]
    pub fn download_task_started(
        &self,
        id: &str,
        piece_length: Option<u64>,
        content_length: Option<u64>,
        response_header: Option<HeaderMap>,
    ) -> Result<metadata::Task> {
        self.metadata
            .download_task_started(id, piece_length, content_length, response_header)
    }

    /// download_task_finished updates the metadata of the task when the task downloads finished.
    #[instrument(skip_all)]
    pub fn download_task_finished(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.download_task_finished(id)
    }

    /// download_task_failed updates the metadata of the task when the task downloads failed.
    #[instrument(skip_all)]
    pub async fn download_task_failed(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.download_task_failed(id)
    }

    /// prefetch_task_started updates the metadata of the task when the task prefetches started.
    #[instrument(skip_all)]
    pub async fn prefetch_task_started(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.prefetch_task_started(id)
    }

    /// prefetch_task_failed updates the metadata of the task when the task prefetches failed.
    #[instrument(skip_all)]
    pub async fn prefetch_task_failed(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.prefetch_task_failed(id)
    }

    /// upload_task_finished updates the metadata of the task when task uploads finished.
    #[instrument(skip_all)]
    pub fn upload_task_finished(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.upload_task_finished(id)
    }

    /// get_task returns the task metadata.
    #[instrument(skip_all)]
    pub fn get_task(&self, id: &str) -> Result<Option<metadata::Task>> {
        self.metadata.get_task(id)
    }

    /// is_task_exists returns whether the task exists.
    #[instrument(skip_all)]
    pub fn is_task_exists(&self, id: &str) -> Result<bool> {
        self.metadata.is_task_exists(id)
    }

    /// get_tasks returns the task metadatas.
    #[instrument(skip_all)]
    pub fn get_tasks(&self) -> Result<Vec<metadata::Task>> {
        self.metadata.get_tasks()
    }

    /// delete_task deletes the task metadatas, task content and piece metadatas.
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
    }

    /// hard_link_or_copy_persistent_cache_task hard links or copies the persistent cache task content to the destination.
    #[instrument(skip_all)]
    pub async fn hard_link_or_copy_persistent_cache_task(
        &self,
        task: &metadata::PersistentCacheTask,
        to: &Path,
    ) -> Result<()> {
        self.content
            .hard_link_or_copy_persistent_cache_task(task, to)
            .await
    }

    /// create_persistent_persistent_cache_task creates a new persistent cache task.
    #[instrument(skip_all)]
    pub async fn create_persistent_persistent_cache_task(
        &self,
        id: &str,
        ttl: Duration,
        path: &Path,
        piece_length: u64,
        content_length: u64,
        expected_digest: &str,
    ) -> Result<metadata::PersistentCacheTask> {
        let response = self.content.write_persistent_cache_task(id, path).await?;
        let digest = Digest::new(Algorithm::Crc32, response.hash);
        if expected_digest != digest.to_string() {
            return Err(Error::DigestMismatch(
                expected_digest.to_string(),
                digest.to_string(),
            ));
        }

        self.metadata.create_persistent_persistent_cache_task(
            id,
            ttl,
            piece_length,
            content_length,
            digest.to_string().as_str(),
        )
    }

    /// download_persistent_cache_task_started updates the metadata of the persistent cache task when the persistent cache task downloads started.
    #[instrument(skip_all)]
    pub fn download_persistent_cache_task_started(
        &self,
        id: &str,
        ttl: Duration,
        persistent: bool,
        piece_length: u64,
        content_length: u64,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata.download_persistent_cache_task_started(
            id,
            ttl,
            persistent,
            piece_length,
            content_length,
        )
    }

    /// download_persistent_cache_task_finished updates the metadata of the persistent cache task when the persistent cache task downloads finished.
    #[instrument(skip_all)]
    pub fn download_persistent_cache_task_finished(
        &self,
        id: &str,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata.download_persistent_cache_task_finished(id)
    }

    /// download_persistent_cache_task_failed updates the metadata of the persistent cache task when the persistent cache task downloads failed.
    #[instrument(skip_all)]
    pub async fn download_persistent_cache_task_failed(
        &self,
        id: &str,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata.download_persistent_cache_task_failed(id)
    }

    /// upload_persistent_cache_task_finished updates the metadata of the cahce task when persistent cache task uploads finished.
    #[instrument(skip_all)]
    pub fn upload_persistent_cache_task_finished(
        &self,
        id: &str,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata.upload_persistent_cache_task_finished(id)
    }

    /// get_persistent_cache_task returns the persistent cache task metadata.
    #[instrument(skip_all)]
    pub fn get_persistent_cache_task(
        &self,
        id: &str,
    ) -> Result<Option<metadata::PersistentCacheTask>> {
        self.metadata.get_persistent_cache_task(id)
    }

    /// is_persistent_cache_task_exists returns whether the persistent cache task exists.
    #[instrument(skip_all)]
    pub fn is_persistent_cache_task_exists(&self, id: &str) -> Result<bool> {
        self.metadata.is_persistent_cache_task_exists(id)
    }

    /// get_tasks returns the task metadatas.
    #[instrument(skip_all)]
    pub fn get_persistent_cache_tasks(&self) -> Result<Vec<metadata::PersistentCacheTask>> {
        self.metadata.get_persistent_cache_tasks()
    }

    /// delete_persistent_cache_task deletes the persistent cache task metadatas, persistent cache task content and piece metadatas.
    #[instrument(skip_all)]
    pub async fn delete_persistent_cache_task(&self, id: &str) {
        self.metadata
            .delete_persistent_cache_task(id)
            .unwrap_or_else(|err| {
                error!("delete persistent cache task metadata failed: {}", err);
            });

        self.content
            .delete_persistent_cache_task(id)
            .await
            .unwrap_or_else(|err| {
                error!("delete persistent cache task content failed: {}", err);
            });
    }

    /// download_piece_started updates the metadata of the piece and writes
    /// the data of piece to file when the piece downloads started.
    #[instrument(skip_all)]
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

    /// download_piece_from_source_finished is used for downloading piece from source.
    #[instrument(skip_all)]
    pub async fn download_piece_from_source_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self
            .content
            .write_piece_with_crc32_castagnoli(task_id, offset, reader)
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

    /// download_piece_from_parent_finished is used for downloading piece from parent.
    #[instrument(skip_all)]
    pub async fn download_piece_from_parent_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        expected_digest: &str,
        parent_id: &str,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let digest: Digest = expected_digest.parse().or_err(ErrorType::ParseError)?;
        let response = if digest.is_crc32_iso3309() {
            // Compatible with the old version.
            self.content
                .write_piece_with_crc32_iso3309(task_id, offset, reader)
                .await?
        } else {
            self.content
                .write_piece_with_crc32_castagnoli(task_id, offset, reader)
                .await?
        };

        let length = response.length;
        let digest = Digest::new(Algorithm::Crc32, response.hash);

        // Check the digest of the piece.
        if expected_digest != digest.to_string() {
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

    /// download_piece_failed updates the metadata of the piece when the piece downloads failed.
    #[instrument(skip_all)]
    pub fn download_piece_failed(&self, piece_id: &str) -> Result<()> {
        self.metadata.download_piece_failed(piece_id)
    }

    /// upload_piece updates the metadata of the piece and
    /// returns the data of the piece.
    #[instrument(skip_all)]
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
                match self
                    .content
                    .read_piece(task_id, piece.offset, piece.length, range)
                    .await
                {
                    Ok(reader) => {
                        // Finish uploading the task.
                        self.metadata.upload_task_finished(task_id)?;
                        Ok(reader)
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

    /// get_piece returns the piece metadata.
    #[instrument(skip_all)]
    pub fn get_piece(&self, piece_id: &str) -> Result<Option<metadata::Piece>> {
        self.metadata.get_piece(piece_id)
    }

    /// is_piece_exists returns whether the piece exists.
    #[instrument(skip_all)]
    pub fn is_piece_exists(&self, piece_id: &str) -> Result<bool> {
        self.metadata.is_piece_exists(piece_id)
    }

    /// get_pieces returns the piece metadatas.
    pub fn get_pieces(&self, task_id: &str) -> Result<Vec<metadata::Piece>> {
        self.metadata.get_pieces(task_id)
    }

    /// piece_id returns the piece id.
    #[inline]
    #[instrument(skip_all)]
    pub fn piece_id(&self, task_id: &str, number: u32) -> String {
        self.metadata.piece_id(task_id, number)
    }

    /// wait_for_piece_finished waits for the piece to be finished.
    #[instrument(skip_all)]
    async fn wait_for_piece_finished(&self, piece_id: &str) -> Result<metadata::Piece> {
        // Initialize the timeout of piece.
        let piece_timeout = tokio::time::sleep(self.config.download.piece_timeout);
        tokio::pin!(piece_timeout);

        // Initialize the interval of piece.
        let mut wait_for_piece_count = 0;
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

                    if wait_for_piece_count > 0 {
                        debug!("wait piece finished");
                    }
                    wait_for_piece_count += 1;
                }
                _ = &mut piece_timeout => {
                    self.metadata.wait_for_piece_finished_failed(piece_id).unwrap_or_else(|err| error!("delete piece metadata failed: {}", err));
                    return Err(Error::WaitForPieceFinishedTimeout(piece_id.to_string()));
                }
            }
        }
    }
}
