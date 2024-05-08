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

use chrono::{NaiveDateTime, Utc};
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::http::reqwest_headermap_to_hashmap;
use reqwest::header::{self, HeaderMap};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tracing::error;

use crate::storage_engine::{
    rocksdb::RocksdbStorageEngine, DatabaseObject, Operations, StorageEngine, StorageEngineOwned,
    Transaction,
};

// Task is the metadata of the task.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    // id is the task id.
    pub id: String,

    // piece_length is the length of the piece.
    pub piece_length: u64,

    // header is the header of the response.
    pub response_header: HashMap<String, String>,

    // uploading_count is the count of the task being uploaded by other peers.
    pub uploading_count: u64,

    // uploaded_count is the count of the task has been uploaded by other peers.
    pub uploaded_count: u64,

    // updated_at is the time when the task metadata is updated. If the task is downloaded
    // by other peers, it will also update updated_at.
    pub updated_at: NaiveDateTime,

    // created_at is the time when the task metadata is created.
    pub created_at: NaiveDateTime,

    // finished_at is the time when the task downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

// Task implements the task database object.
impl DatabaseObject for Task {
    // NAMESPACE is the namespace of [Task] objects.
    const NAMESPACE: &'static str = "task";
}

// Task implements the task metadata.
impl Task {
    // is_started returns whether the task downloads started.
    pub fn is_started(&self) -> bool {
        self.finished_at.is_none()
    }

    // is_downloading returns whether the task is downloading.
    pub fn is_uploading(&self) -> bool {
        self.uploading_count > 0
    }

    // is_expired returns whether the task is expired.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at + ttl < Utc::now().naive_utc()
    }

    // is_finished returns whether the task downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }

    // is_empty returns whether the task is empty.
    pub fn is_empty(&self) -> bool {
        if let Some(content_length) = self.content_length() {
            if content_length == 0 {
                return true;
            }
        }

        false
    }

    // content_length returns the content length of the task.
    pub fn content_length(&self) -> Option<u64> {
        match self.response_header.get(header::CONTENT_LENGTH.as_str()) {
            Some(content_length) => match content_length.parse::<u64>() {
                Ok(content_length) => Some(content_length),
                Err(err) => {
                    error!("parse content length error: {:?}", err);
                    None
                }
            },
            None => None,
        }
    }
}

// Piece is the metadata of the piece.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Piece {
    // number is the piece number.
    pub number: u32,

    // offset is the offset of the piece in the task.
    pub offset: u64,

    // length is the length of the piece.
    pub length: u64,

    // digest is the digest of the piece.
    pub digest: String,

    // parent_id is the parent id of the piece.
    pub parent_id: Option<String>,

    // uploading_count is the count of the piece being uploaded by other peers.
    pub uploading_count: u64,

    // uploaded_count is the count of the piece has been uploaded by other peers.
    pub uploaded_count: u64,

    // updated_at is the time when the piece metadata is updated. If the piece is downloaded
    // by other peers, it will also update updated_at.
    pub updated_at: NaiveDateTime,

    // created_at is the time when the piece metadata is created.
    pub created_at: NaiveDateTime,

    // finished_at is the time when the piece downloads finished.
    pub finished_at: Option<NaiveDateTime>,
}

// Piece implements the piece database object.
impl DatabaseObject for Piece {
    // NAMESPACE is the namespace of [Piece] objects.
    const NAMESPACE: &'static str = "piece";
}

// Piece implements the piece metadata.
impl Piece {
    // is_started returns whether the piece downloads started.
    pub fn is_started(&self) -> bool {
        self.finished_at.is_none()
    }

    // is_finished returns whether the piece downloads finished.
    pub fn is_finished(&self) -> bool {
        self.finished_at.is_some()
    }

    // cost returns the cost of the piece downloaded.
    pub fn cost(&self) -> Option<Duration> {
        match self
            .finished_at
            .map(|finished_at| finished_at - self.created_at)
        {
            Some(cost) => match cost.to_std() {
                Ok(cost) => Some(cost),
                Err(err) => {
                    error!("convert cost error: {:?}", err);
                    None
                }
            },
            None => None,
        }
    }

    // prost_cost returns the prost cost of the piece downloaded.
    pub fn prost_cost(&self) -> Option<prost_wkt_types::Duration> {
        match self.cost() {
            Some(cost) => match prost_wkt_types::Duration::try_from(cost) {
                Ok(cost) => Some(cost),
                Err(err) => {
                    error!("convert cost error: {:?}", err);
                    None
                }
            },
            None => None,
        }
    }
}

/// Metadata manages the metadata of [Task] and [Piece].
pub struct Metadata<E = RocksdbStorageEngine>
where
    E: StorageEngineOwned,
{
    /// db is the underlying storage engine instance.
    db: E,
}

impl<E: StorageEngineOwned> Metadata<E> {
    /// with_txn executes the enclosed closure within a transaction.
    fn with_txn<T>(&self, f: impl FnOnce(&<E as StorageEngine>::Txn) -> Result<T>) -> Result<T> {
        let txn = self.db.start_transaction();
        let result = f(&txn)?;
        txn.commit()?;
        Ok(result)
    }

    /// transactional_update_or_else gets the object from the database and updates
    /// it within a transaction.
    /// If the object does not exist, execute the `or_else` closure.
    /// `or_else` can either return an new object, which means that a new object
    /// will be created; or return an error to abort the transaction.
    fn transactional_update_or_else<T>(
        &self,
        key: &str,
        update: impl FnOnce(T) -> Result<T>,
        or_else: impl FnOnce() -> Result<T>,
    ) -> Result<T>
    where
        T: DatabaseObject,
    {
        self.with_txn(|txn| {
            let object: T = match txn.get_for_update(key.as_bytes())? {
                Some(object) => object,
                None => or_else()?,
            };
            let object = update(object)?;
            txn.put(key.as_bytes(), &object)?;
            Ok(object)
        })
    }

    /// download_task_started updates the metadata of the task when the task downloads started.
    pub fn download_task_started(
        &self,
        id: &str,
        piece_length: u64,
        response_header: Option<HeaderMap>,
    ) -> Result<Task> {
        // Convert the response header to hashmap.
        let get_response_header = || {
            response_header
                .as_ref()
                .map(reqwest_headermap_to_hashmap)
                .unwrap_or_default()
        };

        self.transactional_update_or_else(
            id,
            |mut task: Task| {
                // If the task exists, update the task metadata.
                task.updated_at = Utc::now().naive_utc();

                // If the task has the response header, the response header
                // will not be covered.
                if task.response_header.is_empty() {
                    task.response_header = get_response_header();
                }

                Ok(task)
            },
            || {
                Ok(Task {
                    id: id.to_string(),
                    piece_length,
                    response_header: get_response_header(),
                    updated_at: Utc::now().naive_utc(),
                    created_at: Utc::now().naive_utc(),
                    ..Default::default()
                })
            },
        )
    }

    /// download_task_finished updates the metadata of the task when the task downloads finished.
    pub fn download_task_finished(&self, id: &str) -> Result<Task> {
        self.transactional_update_or_else(
            id,
            |mut task: Task| {
                task.updated_at = Utc::now().naive_utc();
                task.finished_at = Some(Utc::now().naive_utc());
                Ok(task)
            },
            || Err(Error::TaskNotFound(id.to_string())),
        )
    }

    /// upload_task_started updates the metadata of the task when task uploads started.
    pub fn upload_task_started(&self, id: &str) -> Result<Task> {
        self.transactional_update_or_else(
            id,
            |mut task: Task| {
                task.uploading_count += 1;
                task.updated_at = Utc::now().naive_utc();
                Ok(task)
            },
            || Err(Error::TaskNotFound(id.to_string())),
        )
    }

    /// upload_task_finished updates the metadata of the task when task uploads finished.
    pub fn upload_task_finished(&self, id: &str) -> Result<Task> {
        self.transactional_update_or_else(
            id,
            |mut task: Task| {
                task.uploading_count -= 1;
                task.uploaded_count += 1;
                task.updated_at = Utc::now().naive_utc();
                Ok(task)
            },
            || Err(Error::TaskNotFound(id.to_string())),
        )
    }

    /// upload_task_failed updates the metadata of the task when the task uploads failed.
    pub fn upload_task_failed(&self, id: &str) -> Result<Task> {
        self.transactional_update_or_else(
            id,
            |mut task: Task| {
                task.uploading_count -= 1;
                task.updated_at = Utc::now().naive_utc();
                Ok(task)
            },
            || Err(Error::TaskNotFound(id.to_string())),
        )
    }

    /// get_task gets the task metadata.
    pub fn get_task(&self, id: &str) -> Result<Option<Task>> {
        self.db.get(id.as_bytes())
    }

    /// get_tasks gets the task metadatas.
    pub fn get_tasks(&self) -> Result<Vec<Task>> {
        self.with_txn(|txn| txn.iter()?.map(|ele| ele.map(|(_, task)| task)).collect())
    }

    /// delete_task deletes the task metadata.
    pub fn delete_task(&self, task_id: &str) -> Result<()> {
        self.with_txn(|txn| {
            txn.delete::<Task>(task_id.as_bytes())?;
            Ok(())
        })
    }

    /// download_piece_started updates the metadata of the piece when the piece downloads started.
    pub fn download_piece_started(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Get the piece id.
        let id = self.piece_id(task_id, number);

        // Construct the piece metadata.
        let piece = Piece {
            number,
            updated_at: Utc::now().naive_utc(),
            created_at: Utc::now().naive_utc(),
            ..Default::default()
        };

        self.with_txn(|txn| {
            // Put the piece metadata.
            txn.put(id.as_bytes(), &piece)?;
            Ok(piece)
        })
    }

    /// download_piece_finished updates the metadata of the piece when the piece downloads finished.
    pub fn download_piece_finished(
        &self,
        task_id: &str,
        number: u32,
        offset: u64,
        length: u64,
        digest: &str,
        parent_id: Option<String>,
    ) -> Result<Piece> {
        // Get the piece id.
        let id = self.piece_id(task_id, number);

        self.transactional_update_or_else(
            id.as_str(),
            |mut piece: Piece| {
                piece.offset = offset;
                piece.length = length;
                piece.digest = digest.to_string();
                piece.parent_id = parent_id;
                piece.updated_at = Utc::now().naive_utc();
                piece.finished_at = Some(Utc::now().naive_utc());
                Ok(piece)
            },
            || Err(Error::PieceNotFound(id.to_string())),
        )
    }

    /// download_piece_failed updates the metadata of the piece when the piece downloads failed.
    pub fn download_piece_failed(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Get the piece id.
        let id = self.piece_id(task_id, number);

        self.with_txn(|txn| {
            let piece = match txn.get_for_update(id.as_bytes())? {
                // If the piece exists, delete the piece metadata.
                Some(piece) => {
                    txn.delete::<Piece>(id.as_bytes())?;
                    piece
                }
                // If the piece does not exist, return error.
                None => return Err(Error::PieceNotFound(id)),
            };

            Ok(piece)
        })
    }

    /// upload_piece_started updates the metadata of the piece when piece uploads started.
    pub fn upload_piece_started(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Get the piece id.
        let id = self.piece_id(task_id, number);

        self.transactional_update_or_else(
            id.as_str(),
            |mut piece: Piece| {
                piece.uploading_count += 1;
                piece.updated_at = Utc::now().naive_utc();
                Ok(piece)
            },
            || Err(Error::PieceNotFound(id.to_string())),
        )
    }

    /// upload_piece_finished updates the metadata of the piece when piece uploads finished.
    pub fn upload_piece_finished(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Get the piece id.
        let id = self.piece_id(task_id, number);

        self.transactional_update_or_else(
            id.as_str(),
            |mut piece: Piece| {
                piece.uploading_count -= 1;
                piece.uploaded_count += 1;
                piece.updated_at = Utc::now().naive_utc();
                Ok(piece)
            },
            || Err(Error::PieceNotFound(id.to_string())),
        )
    }

    /// upload_piece_failed updates the metadata of the piece when the piece uploads failed.
    pub fn upload_piece_failed(&self, task_id: &str, number: u32) -> Result<Piece> {
        // Get the piece id.
        let id = self.piece_id(task_id, number);

        self.transactional_update_or_else(
            id.as_str(),
            |mut piece: Piece| {
                piece.uploading_count -= 1;
                piece.updated_at = Utc::now().naive_utc();
                Ok(piece)
            },
            || Err(Error::PieceNotFound(id.to_string())),
        )
    }

    /// get_piece gets the piece metadata.
    pub fn get_piece(&self, task_id: &str, number: u32) -> Result<Option<Piece>> {
        let id = self.piece_id(task_id, number);
        self.db.get(id.as_bytes())
    }

    /// get_pieces gets the piece metadatas.
    pub fn get_pieces(&self, task_id: &str) -> Result<Vec<Piece>> {
        self.with_txn(|txn| {
            // Iterate the piece metadatas.
            txn.prefix_iter(task_id.as_bytes())?
                .map(|ele| ele.map(|(_, piece)| piece))
                .collect()
        })
    }

    /// delete_pieces deletes the piece metadatas.
    pub fn delete_pieces(&self, task_id: &str) -> Result<()> {
        self.with_txn(|txn| {
            let iter = txn.prefix_iter::<Piece>(task_id.as_bytes())?;

            // Iterate the piece metadatas.
            for ele in iter {
                let (key, _) = ele?;
                txn.delete::<Piece>(&key)?;
            }
            Ok(())
        })
    }

    /// piece_id returns the piece id.
    pub fn piece_id(&self, task_id: &str, number: u32) -> String {
        format!("{}-{}", task_id, number)
    }
}

impl Metadata<RocksdbStorageEngine> {
    /// new creates a new metadata instance.
    pub fn new(dir: &Path) -> Result<Metadata<RocksdbStorageEngine>> {
        let db = RocksdbStorageEngine::open(dir, &[Task::NAMESPACE, Piece::NAMESPACE])?;

        Ok(Metadata { db })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempdir::TempDir;

    #[test]
    fn should_create_metadata_db() {
        let dir = TempDir::new("metadata_db").unwrap();
        let metadata = Metadata::new(dir.path()).unwrap();
        assert!(metadata.get_tasks().unwrap().is_empty());
        assert!(metadata.get_pieces("task").unwrap().is_empty());
    }

    #[test]
    fn test_task_lifecycle() {
        let dir = TempDir::new("metadata_db").unwrap();
        let metadata = Metadata::new(dir.path()).unwrap();

        let task_id = "task1";

        // Test download_task_started
        metadata.download_task_started(task_id, 1024, None).unwrap();
        let task = metadata
            .get_task(task_id)
            .unwrap()
            .expect("task should exist after download_task_started");
        assert_eq!(task.id, task_id);
        assert_eq!(task.piece_length, 1024);
        assert!(task.response_header.is_empty());
        assert_eq!(task.uploading_count, 0);
        assert_eq!(task.uploaded_count, 0);

        // Test download_task_finished
        metadata.download_task_finished(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert!(
            task.is_finished(),
            "task should be finished after download_task_finished"
        );

        // Test upload_task_started
        metadata.upload_task_started(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert_eq!(
            task.uploading_count, 1,
            "uploading_count should be increased by 1 after upload_task_started"
        );

        // Test upload_task_finished
        metadata.upload_task_finished(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap().unwrap();
        assert_eq!(
            task.uploading_count, 0,
            "uploading_count should be decreased by 1 after upload_task_finished"
        );
        assert_eq!(
            task.uploaded_count, 1,
            "uploaded_count should be increased by 1 after upload_task_finished"
        );

        // Test upload_task_failed
        let task = metadata.upload_task_started(task_id).unwrap();
        assert_eq!(task.uploading_count, 1);
        let task = metadata.upload_task_failed(task_id).unwrap();
        assert_eq!(
            task.uploading_count, 0,
            "uploading_count should be decreased by 1 after upload_task_failed"
        );
        assert_eq!(
            task.uploaded_count, 1,
            "uploaded_count should not be changed after upload_task_failed"
        );

        // Test get_tasks
        let task_id = "task2";
        metadata.download_task_started(task_id, 1024, None).unwrap();
        let tasks = metadata.get_tasks().unwrap();
        assert_eq!(tasks.len(), 2, "should get 2 tasks in total");

        // Test delete_task
        metadata.delete_task(task_id).unwrap();
        let task = metadata.get_task(task_id).unwrap();
        assert!(task.is_none(), "task should be deleted after delete_task");
    }

    #[test]
    fn test_piece_lifecycle() {
        let dir = TempDir::new("metadata_db").unwrap();
        let metadata = Metadata::new(dir.path()).unwrap();
        let task_id = "task3";

        // Test download_piece_started
        metadata.download_piece_started(task_id, 1).unwrap();
        let piece = metadata.get_piece(task_id, 1).unwrap().unwrap();
        assert_eq!(
            piece.number, 1,
            "should get newly created piece with number specified"
        );

        // Test download_piece_finished
        metadata
            .download_piece_finished(task_id, 1, 0, 1024, "digest1", None)
            .unwrap();
        let piece = metadata.get_piece(task_id, 1).unwrap().unwrap();
        assert_eq!(
            piece.length, 1024,
            "piece should be updated after download_piece_finished"
        );
        assert_eq!(
            piece.digest, "digest1",
            "piece should be updated after download_piece_finished"
        );

        // Test get_pieces
        metadata.download_piece_started(task_id, 2).unwrap();
        metadata.download_piece_started(task_id, 3).unwrap();
        let pieces = metadata.get_pieces(task_id).unwrap();
        assert_eq!(pieces.len(), 3, "should get 3 pieces in total");

        // Test download_piece_failed
        metadata.download_piece_failed(task_id, 2).unwrap();
        let piece = metadata.get_piece(task_id, 2).unwrap();
        assert!(
            piece.is_none(),
            "piece should be deleted after download_piece_failed"
        );

        // Test upload_piece_started
        metadata.upload_piece_started(task_id, 3).unwrap();
        let piece = metadata.get_piece(task_id, 3).unwrap().unwrap();
        assert_eq!(
            piece.uploading_count, 1,
            "piece should be updated after upload_piece_started"
        );

        // Test upload_piece_finished
        metadata.upload_piece_finished(task_id, 3).unwrap();
        let piece = metadata.get_piece(task_id, 3).unwrap().unwrap();
        assert_eq!(
            piece.uploading_count, 0,
            "piece should be updated after upload_piece_finished"
        );
        assert_eq!(
            piece.uploaded_count, 1,
            "piece should be updated after upload_piece_finished"
        );

        // Test upload_piece_failed
        metadata.upload_piece_started(task_id, 3).unwrap();
        metadata.upload_piece_failed(task_id, 3).unwrap();
        let piece = metadata.get_piece(task_id, 3).unwrap().unwrap();
        assert_eq!(
            piece.uploading_count, 0,
            "piece should be updated after upload_piece_failed"
        );

        // Test delete_pieces
        metadata.delete_pieces(task_id).unwrap();
        let pieces = metadata.get_pieces(task_id).unwrap();
        assert!(pieces.is_empty(), "should get 0 pieces after delete_pieces");
    }
}
