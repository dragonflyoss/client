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

    // TaskNotFound is the error when the task is not found.
    #[error{"task {0} not found"}]
    TaskNotFound(String),

    // PieceNotFound is the error when the piece is not found.
    #[error{"piece {0} not found"}]
    PieceNotFound(String),

    // PieceStateIsFailed is the error when the piece state is failed.
    #[error{"piece {0} state is failed"}]
    PieceStateIsFailed(String),

    // ColumnFamilyNotFound is the error when the column family is not found.
    #[error{"column family {0} not found"}]
    ColumnFamilyNotFound(String),

    // InvalidStateTransition is the error when the state transition is invalid.
    #[error{"can not transit from {0} to {1}"}]
    InvalidStateTransition(String, String),

    // InvalidState is the error when the state is invalid.
    #[error{"invalid state {0}"}]
    InvalidState(String),
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
    pub fn download_task_started(&self, id: &str, piece_length: u64) -> Result<()> {
        self.metadata.download_task_started(id, piece_length)
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
    pub fn download_piece_finished(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        digest: &str,
        data: &[u8],
    ) -> Result<()> {
        self.content.write_piece(task_id, offset, data)?;
        self.metadata
            .download_piece_finished(task_id, offset, length, digest)
    }

    // upload_piece_finished updates the metadata of the piece when the piece uploads finished and
    // returns the data of the piece.
    pub fn upload_piece_finished(&self, task_id: &str, number: u32) -> Result<Vec<u8>> {
        let id = self.metadata.piece_id(task_id, number);
        match self.metadata.get_piece(&id)? {
            Some(piece) => {
                let data = self
                    .content
                    .read_piece(task_id, piece.offset, piece.length)?;
                self.metadata.upload_piece_finished(&id)?;
                Ok(data)
            }
            None => Err(Error::PieceNotFound(id)),
        }
    }

    // get_piece returns the piece metadata.
    pub fn get_piece(&self, task_id: &str, number: u32) -> Result<Option<metadata::Piece>> {
        self.metadata
            .get_piece(self.metadata.piece_id(task_id, number).as_str())
    }
}
