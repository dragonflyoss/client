/*
 *     Copyright 2023 The Dragonfly Authors
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

use crate::utils::digest::{Algorithm, Digest};
use crate::{Error, Result};
use std::path::Path;
use tokio::io::AsyncRead;

pub mod content;
pub mod metadata;

// Storage is the storage of the task.
pub struct Storage {
    // metadata implements the metadata storage.
    metadata: metadata::Metadata,

    // content implements the content storage.
    content: content::Content,
}

// Storage implements the storage.
impl Storage {
    // new returns a new storage.
    pub fn new(data_dir: &Path) -> Result<Self> {
        let metadata = metadata::Metadata::new(data_dir)?;
        let content = content::Content::new(data_dir)?;
        Ok(Storage { metadata, content })
    }

    // download_task_started updates the metadata of the task when the task downloads started.
    pub fn download_task_started(&self, id: &str, piece_length: i32) -> Result<()> {
        self.metadata.download_task_started(id, piece_length)
    }

    // download_task_failed updates the metadata of the task when the task downloads failed.
    pub fn download_task_failed(&self, id: &str) -> Result<()> {
        self.metadata.download_task_failed(id)
    }

    // upload_task_finished updates the metadata of the task when task uploads finished.
    pub fn upload_task_finished(&self, id: &str) -> Result<()> {
        self.metadata.upload_task_finished(id)
    }

    // get_task returns the task metadata.
    pub fn get_task(&self, id: &str) -> Result<Option<metadata::Task>> {
        let task = self.metadata.get_task(id)?;
        Ok(task)
    }

    // download_piece_started updates the metadata of the piece and writes
    // the data of piece to file when the piece downloads started.
    pub fn download_piece_started(&self, task_id: &str, number: i32) -> Result<()> {
        self.metadata.download_piece_started(task_id, number)
    }

    // download_piece_from_source_finished is used for downloading piece from source.
    pub async fn download_piece_from_source_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        task_id: &str,
        number: i32,
        offset: u64,
        length: u64,
        reader: &mut R,
    ) -> Result<u64> {
        let response = self.content.write_piece(task_id, offset, reader).await?;
        let digest = Digest::new(Algorithm::Sha256, response.hash);

        self.metadata.download_piece_finished(
            task_id,
            number,
            offset,
            length,
            digest.to_string().as_str(),
        )?;
        Ok(length)
    }

    // download_piece_from_remote_peer_finished is used for downloading piece from remote peer.
    pub async fn download_piece_from_remote_peer_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        task_id: &str,
        number: i32,
        offset: u64,
        expected_digest: String,
        reader: &mut R,
    ) -> Result<u64> {
        let response = self.content.write_piece(task_id, offset, reader).await?;
        let length = response.length;
        let digest = Digest::new(Algorithm::Sha256, response.hash);

        // Check the digest of the piece.
        if digest.to_string() != expected_digest {
            return Err(Error::PieceDigestMismatch(
                self.metadata.piece_id(task_id, number),
            ));
        }

        self.metadata.download_piece_finished(
            task_id,
            number,
            offset,
            length,
            digest.to_string().as_str(),
        )?;
        Ok(length)
    }

    // download_piece_failed updates the metadata of the piece when the piece downloads failed.
    pub fn download_piece_failed(&self, task_id: &str, number: i32) -> Result<()> {
        self.metadata.download_piece_failed(task_id, number)
    }

    // upload_piece updates the metadata of the piece and
    // returns the data of the piece.
    pub async fn upload_piece(&self, task_id: &str, number: i32) -> Result<impl AsyncRead> {
        match self.metadata.get_piece(task_id, number)? {
            Some(piece) => {
                let reader = self
                    .content
                    .read_piece(task_id, piece.offset, piece.length)
                    .await?;
                self.metadata.upload_piece_finished(task_id, number)?;
                Ok(reader)
            }
            None => Err(Error::PieceNotFound(
                self.metadata.piece_id(task_id, number),
            )),
        }
    }

    // get_piece returns the piece metadata.
    pub fn get_piece(&self, task_id: &str, number: i32) -> Result<Option<metadata::Piece>> {
        let piece = self.metadata.get_piece(task_id, number)?;
        Ok(piece)
    }

    // get_pieces returns the pieces metadata.
    pub fn get_pieces(&self, task_id: &str) -> Result<Vec<metadata::Piece>> {
        self.metadata.get_pieces(task_id)
    }
}
