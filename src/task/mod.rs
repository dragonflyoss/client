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

use crate::backend::http::HTTP;
use crate::grpc::scheduler::SchedulerClient;
use crate::storage::{metadata, Storage};
use crate::utils::digest::{Algorithm, Digest as UtilsDigest};
use crate::utils::id_generator::IDGenerator;
use crate::{Error, Result};
use dragonfly_api::common::v2::Download;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::{
    fs::{self, OpenOptions},
    io::{self, AsyncSeekExt, SeekFrom},
};
use tokio_util::io::InspectReader;

pub mod piece;

// Task represents a task manager.
pub struct Task {
    // id_generator is the id generator.
    id_generator: Arc<IDGenerator>,

    // manager_client is the grpc client of the manager.
    storage: Arc<Storage>,

    // scheduler_client is the grpc client of the scheduler.
    pub scheduler_client: Arc<SchedulerClient>,

    // piece is the piece manager.
    pub piece: piece::Piece,
}

// Task implements the task manager.
impl Task {
    // new returns a new Task.
    pub fn new(
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        scheduler_client: Arc<SchedulerClient>,
        http_client: Arc<HTTP>,
    ) -> Self {
        Self {
            id_generator,
            storage: storage.clone(),
            scheduler_client: scheduler_client.clone(),
            piece: piece::Piece::new(
                storage.clone(),
                scheduler_client.clone(),
                http_client.clone(),
            ),
        }
    }

    pub fn get(&self, task_id: &str) -> Result<Option<metadata::Task>> {
        self.storage.get_task(task_id)
    }

    // download_into_file downloads a task into a file.
    pub async fn download_into_file(&self, download: Download) -> Result<fs::File> {
        // Get the output path.
        let output_path = download.output_path.ok_or(Error::InvalidParameter())?;

        // Generate the task id.
        let task_id = self.id_generator.task_id(
            download.url,
            download.digest,
            download.tag,
            download.application,
            download.piece_length,
            download.filters,
        )?;

        // Open the file.
        let mut f = OpenOptions::new()
            .write(true)
            .open(output_path.as_str())
            .await?;

        // Get the task from the local storage.
        let task = self.get(task_id.as_str())?;
        match task {
            Some(task) => {
                // If the task is finished, return the file.
                if task.is_finished() {
                    let pieces = self.piece.get_all(task_id.as_str())?;
                    for piece in pieces {
                        // Seek to the offset of the piece.
                        f.seek(SeekFrom::Start(piece.offset)).await?;

                        // Download the piece from the local peer.
                        let reader = self
                            .piece
                            .download_from_local_peer(task_id.as_str(), piece.number)
                            .await?;

                        // Sha256 is used to calculate the hash of the piece.
                        let mut hasher = Sha256::new();

                        // InspectReader is used to calculate the hash of the piece.
                        let mut tee = InspectReader::new(reader, |bytes| hasher.update(bytes));

                        // Copy the piece to the file.
                        io::copy(&mut tee, &mut f).await?;

                        // Calculate the hash of the piece.
                        let hash = hasher.finalize();

                        // Calculate the digest of the piece.
                        let digest = UtilsDigest::new(
                            Algorithm::Sha256,
                            base16ct::lower::encode_string(&hash),
                        );

                        // Check the digest of the piece.
                        if digest.to_string() != piece.digest {
                            return Err(Error::PieceDigestMismatch(piece.number.to_string()));
                        }
                    }

                    return Ok(f);
                }

                // If the task is not finished, download the task.
                Err(Error::Unimplemented())
            }
            None => {
                // If the task is not found, create a new task.
                Err(Error::Unimplemented())
            }
        }
    }

    // download_into_async_read downloads a task into an AsyncRead.
    // pub async fn download_into_async_read() -> Result<impl AsyncRead> {
    // Err(Error::Unimplemented())
    // }
}
