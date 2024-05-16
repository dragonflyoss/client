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
use dragonfly_client_util::digest::{Algorithm, Digest};
use reqwest::header::HeaderMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncRead;

pub mod content;
pub mod metadata;
pub mod storage_engine;

// DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL is the default interval for waiting for the piece to be finished.
pub const DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL: Duration = Duration::from_millis(500);

// Storage is the storage of the task.
pub struct Storage {
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

    // metadata implements the metadata storage.
    metadata: metadata::Metadata,

    // content implements the content storage.
    content: content::Content,
}

// Storage implements the storage.
impl Storage {
    // new returns a new storage.
    pub async fn new(config: Arc<Config>, dir: &Path) -> Result<Self> {
        let metadata = metadata::Metadata::new(dir)?;
        let content = content::Content::new(config.clone(), dir).await?;
        Ok(Storage {
            config,
            metadata,
            content,
        })
    }

    // hard_link_or_copy_task hard links or copies the task content to the destination.
    pub async fn hard_link_or_copy_task(
        &self,
        task: metadata::Task,
        to: &Path,
        range: Option<Range>,
    ) -> Result<()> {
        self.content.hard_link_or_copy_task(task, to, range).await
    }

    // read_task_by_range returns the reader of the task by range.
    pub async fn read_task_by_range(
        &self,
        task_id: &str,
        range: Range,
    ) -> Result<impl AsyncRead + 'static> {
        self.content.read_task_by_range(task_id, range).await
    }

    // download_task_started updates the metadata of the task when the task downloads started.
    pub fn download_task_started(
        &self,
        id: &str,
        peer_id: &str,
        piece_length: u64,
        response_header: Option<HeaderMap>,
    ) -> Result<metadata::Task> {
        self.metadata
            .download_task_started(id, peer_id, piece_length, response_header)
    }

    // download_task_finished updates the metadata of the task when the task downloads finished.
    pub fn download_task_finished(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.download_task_finished(id)
    }

    // download_task_failed updates the metadata of the task when the task downloads failed.
    pub async fn download_task_failed(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.download_task_failed(id)
    }

    // prefetch_task_started updates the metadata of the task when the task prefetches started.
    pub async fn prefetch_task_started(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.prefetch_task_started(id)
    }

    // prefetch_task_failed updates the metadata of the task when the task prefetches failed.
    pub async fn prefetch_task_failed(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.prefetch_task_failed(id)
    }

    // upload_task_finished updates the metadata of the task when task uploads finished.
    pub fn upload_task_finished(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.upload_task_finished(id)
    }

    // get_task returns the task metadata.
    pub fn get_task(&self, id: &str) -> Result<Option<metadata::Task>> {
        self.metadata.get_task(id)
    }

    // get_tasks returns the task metadatas.
    pub fn get_tasks(&self) -> Result<Vec<metadata::Task>> {
        self.metadata.get_tasks()
    }

    // delete_task deletes the task metadatas, task content and piece metadatas.
    pub async fn delete_task(&self, id: &str) -> Result<()> {
        self.metadata.delete_task(id)?;
        self.metadata.delete_pieces(id)?;
        self.content.delete_task(id).await?;
        Ok(())
    }

    // download_piece_started updates the metadata of the piece and writes
    // the data of piece to file when the piece downloads started.
    pub async fn download_piece_started(
        &self,
        task_id: &str,
        number: u32,
    ) -> Result<metadata::Piece> {
        // Wait for the piece to be finished.
        match self.wait_for_piece_finished(task_id, number).await {
            Ok(piece) => Ok(piece),
            // If piece is not found or wait timeout, create piece metadata.
            Err(_) => self.metadata.download_piece_started(task_id, number),
        }
    }

    // download_piece_from_source_finished is used for downloading piece from source.
    pub async fn download_piece_from_source_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        task_id: &str,
        number: u32,
        offset: u64,
        length: u64,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self.content.write_piece(task_id, offset, reader).await?;
        let digest = Digest::new(Algorithm::Blake3, response.hash);

        self.metadata.download_piece_finished(
            task_id,
            number,
            offset,
            length,
            digest.to_string().as_str(),
            None,
        )
    }

    // download_piece_from_remote_peer_finished is used for downloading piece from remote peer.
    pub async fn download_piece_from_remote_peer_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        task_id: &str,
        number: u32,
        offset: u64,
        expected_digest: &str,
        parent_id: &str,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self.content.write_piece(task_id, offset, reader).await?;
        let length = response.length;
        let digest = Digest::new(Algorithm::Blake3, response.hash);

        // Check the digest of the piece.
        if expected_digest != digest.to_string() {
            return Err(Error::PieceDigestMismatch);
        }

        self.metadata.download_piece_finished(
            task_id,
            number,
            offset,
            length,
            digest.to_string().as_str(),
            Some(parent_id.to_string()),
        )
    }

    // download_piece_failed updates the metadata of the piece when the piece downloads failed.
    pub fn download_piece_failed(&self, task_id: &str, number: u32) -> Result<()> {
        self.metadata.download_piece_failed(task_id, number)
    }

    // upload_piece updates the metadata of the piece and
    // returns the data of the piece.
    pub async fn upload_piece(
        &self,
        task_id: &str,
        number: u32,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        // Start uploading the task.
        self.metadata.upload_task_started(task_id)?;

        // Start uploading the piece.
        if let Err(err) = self.metadata.upload_piece_started(task_id, number) {
            // Failed uploading the task.
            self.metadata.upload_task_failed(task_id)?;
            return Err(err);
        }

        // Wait for the piece to be finished.
        if let Err(err) = self.wait_for_piece_finished(task_id, number).await {
            // Failed uploading the task.
            self.metadata.upload_task_failed(task_id)?;

            // Failed uploading the piece.
            self.metadata.upload_piece_failed(task_id, number)?;
            return Err(err);
        }

        // Get the piece metadata and return the content of the piece.
        match self.metadata.get_piece(task_id, number)? {
            Some(piece) => {
                match self
                    .content
                    .read_piece(task_id, piece.offset, piece.length, range)
                    .await
                {
                    Ok(reader) => {
                        // Finish uploading the task.
                        self.metadata.upload_task_finished(task_id)?;

                        // Finish uploading the piece.
                        self.metadata.upload_piece_finished(task_id, number)?;
                        Ok(reader)
                    }
                    Err(err) => {
                        // Failed uploading the task.
                        self.metadata.upload_task_failed(task_id)?;

                        // Failed uploading the piece.
                        self.metadata.upload_piece_failed(task_id, number)?;
                        Err(err)
                    }
                }
            }
            None => {
                // Failed uploading the task.
                self.metadata.upload_task_failed(task_id)?;

                // Failed uploading the piece.
                self.metadata.upload_piece_failed(task_id, number)?;
                Err(Error::PieceNotFound(self.piece_id(task_id, number)))
            }
        }
    }

    // get_piece returns the piece metadata.
    pub fn get_piece(&self, task_id: &str, number: u32) -> Result<Option<metadata::Piece>> {
        self.metadata.get_piece(task_id, number)
    }

    // get_pieces returns the piece metadatas.
    pub fn get_pieces(&self, task_id: &str) -> Result<Vec<metadata::Piece>> {
        self.metadata.get_pieces(task_id)
    }

    // piece_id returns the piece id.
    pub fn piece_id(&self, task_id: &str, number: u32) -> String {
        self.metadata.piece_id(task_id, number)
    }

    // wait_for_piece_finished waits for the piece to be finished.
    async fn wait_for_piece_finished(&self, task_id: &str, number: u32) -> Result<metadata::Piece> {
        // Initialize the timeout of piece.
        let piece_timeout = tokio::time::sleep(self.config.download.piece_timeout);
        tokio::pin!(piece_timeout);

        // Initialize the interval of piece.
        let mut interval = tokio::time::interval(DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let piece = self
                        .get_piece(task_id, number)?
                        .ok_or_else(|| Error::PieceNotFound(self.piece_id(task_id, number)))?;

                    // If the piece is finished, return.
                    if piece.is_finished() {
                        return Ok(piece);
                    }
                }
                _ = &mut piece_timeout => {
                    return Err(Error::WaitForPieceFinishedTimeout(self.piece_id(task_id, number)));
                }
            }
        }
    }
}
