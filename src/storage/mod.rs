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

use crate::{Error, Result};
use std::path::Path;
use tokio::io::AsyncRead;

mod content;
mod metadata;

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
    pub fn download_task_started(&self, id: &str, piece_length: u64) -> Result<()> {
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
        self.metadata.get_task(id)
    }

    // download_piece_started updates the metadata of the piece and writes
    // the data of piece to file when the piece downloads started.
    pub fn download_piece_started(&self, task_id: &str, number: u32) -> Result<()> {
        self.metadata.download_piece_started(task_id, number)
    }

    // download_piece_finished updates the metadata of the piece and writes the data of piece to file.
    pub async fn download_piece_finished<R: AsyncRead + Unpin>(
        &self,
        task_id: &str,
        number: u32,
        offset: u64,
        digest: &str,
        reader: &mut R,
    ) -> Result<u64> {
        let length = self.content.write_piece(task_id, offset, reader).await?;
        self.metadata
            .download_piece_finished(task_id, number, offset, length, digest)?;
        Ok(length)
    }

    // download_piece_failed updates the metadata of the piece when the piece downloads failed.
    pub fn download_piece_failed(&self, task_id: &str, number: u32) -> Result<()> {
        self.metadata.download_piece_failed(task_id, number)
    }

    // upload_piece updates the metadata of the piece and
    // returns the data of the piece.
    pub async fn upload_piece(&self, task_id: &str, number: u32) -> Result<impl AsyncRead> {
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
}
