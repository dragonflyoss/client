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

use std::path::Path;

mod content;
mod metadata;

// Error is the error for Storage.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // RocksDB is the error for rocksdb.
    #[error(transparent)]
    RocksDB(#[from] rocksdb::Error),

    // JSON is the error for serde_json.
    #[error(transparent)]
    JSON(#[from] serde_json::Error),

    // IO is the error for IO operation.
    #[error(transparent)]
    IO(#[from] std::io::Error),

    // Other is the other error.
    #[error("{0}")]
    Other(String),
}

// Result is the result for Storage.
pub type Result<T> = std::result::Result<T, Error>;

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
    pub fn download_task_started(&self, id: &str, piece_length: u64) -> Result<metadata::Task> {
        self.metadata.download_task_started(id, piece_length)
    }

    // upload_task_finished updates the metadata of the task when task uploads finished.
    pub fn upload_task_finished(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.upload_task_finished(id)
    }

    // get_task returns the task metadata.
    pub fn get_task(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.get_task(id)
    }

    // TODO implement feature.
    // reuse_piece reuses the piece.
    // pub async fn reuse_piece(&self, task_id: &str, number: u32) -> Result<Option<Vec<u8>>> {
    // Ok(None)
    // }

    // download_piece_started updates the metadata of the piece and writes the data of piece to file when the piece downloads started.
    pub fn download_piece_started(&self, task_id: &str, number: u32) -> Result<()> {
        self.metadata.download_piece_started(task_id, number)?;
        Ok(())
    }

    // download_piece_succeeded updates the metadata of the piece when the piece downloads succeeded.
    pub fn download_piece_succeeded(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        digest: &str,
        data: &[u8],
    ) -> Result<()> {
        self.content.write_piece(task_id, offset, data)?;
        self.metadata
            .download_piece_succeeded(task_id, offset, length, digest)?;
        Ok(())
    }

    // download_piece_failed updates the metadata of the piece when the piece downloads failed.
    pub fn download_piece_failed(&self, task_id: &str, number: u32) -> Result<()> {
        self.metadata
            .download_piece_failed(self.metadata.piece_id(task_id, number).as_str())?;
        Ok(())
    }

    // upload_piece updates the metadata of the piece when the piece uploads finished and
    // returns the data of the piece.
    pub fn upload_piece(&self, task_id: &str, number: u32) -> Result<Vec<u8>> {
        let piece_id = self.metadata.piece_id(task_id, number);
        let piece = self.metadata.get_piece(&piece_id)?;
        let data = self
            .content
            .read_piece(task_id, piece.offset, piece.length)?;
        self.metadata.upload_piece_finished(&piece_id)?;
        Ok(data)
    }

    // get_piece returns the piece metadata.
    pub fn get_piece(&self, task_id: &str, number: u32) -> Result<metadata::Piece> {
        self.metadata
            .get_piece(self.metadata.piece_id(task_id, number).as_str())
    }

    // get_piece_state returns the piece state.
    pub fn get_piece_state(&self, task_id: &str, number: u32) -> Result<metadata::PieceState> {
        self.metadata
            .get_piece_state(self.metadata.piece_id(task_id, number).as_str())
    }
}
