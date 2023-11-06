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

use crate::backend::http::{Request, HTTP};
use crate::grpc::{dfdaemon::DfdaemonClient, scheduler::SchedulerClient};
use crate::storage::{metadata, Storage};
use crate::{Error, HttpError, Result};
use dragonfly_api::common::v2::Peer;
use dragonfly_api::dfdaemon::v2::{
    sync_pieces_request, sync_pieces_response, InterestedPiecesRequest, InterestedPiecesResponse,
    SyncPiecesRequest,
};
use reqwest::header::HeaderMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::error;

// Task represents a task manager.
pub struct Task {
    // manager_client is the grpc client of the manager.
    storage: Arc<Storage>,

    // scheduler_client is the grpc client of the scheduler.
    pub scheduler_client: Arc<SchedulerClient>,

    // http_client is the http client.
    http_client: Arc<HTTP>,
}

// Task implements the task manager.
impl Task {
    // new returns a new Task.
    pub fn new(
        storage: Arc<Storage>,
        scheduler_client: Arc<SchedulerClient>,
        http_client: Arc<HTTP>,
    ) -> Self {
        Self {
            storage,
            scheduler_client,
            http_client,
        }
    }

    // get_task gets a task from the local storage.
    pub fn get_task(&self, task_id: &str) -> Result<metadata::Task> {
        self.storage.get_task(task_id)
    }

    // get_piece gets a piece from the local storage.
    pub fn get_piece(&self, task_id: &str, number: i32) -> Result<metadata::Piece> {
        self.storage.get_piece(task_id, number)
    }

    // get_pieces gets pieces from the local storage.
    pub fn get_pieces(&self, task_id: &str) -> Result<Vec<metadata::Piece>> {
        self.storage.get_pieces(task_id)
    }

    // download_piece_from_local_peer downloads a piece from a local peer.
    pub async fn download_piece_from_local_peer(
        &self,
        task_id: &str,
        number: i32,
    ) -> Result<impl AsyncRead> {
        self.storage.upload_piece(task_id, number).await
    }

    // download_piece_from_remote_peer downloads a piece from a remote peer.
    pub async fn download_piece_from_remote_peer(
        &self,
        task_id: &str,
        number: i32,
        remote_peer: Peer,
    ) -> Result<impl AsyncRead> {
        // Create a dfdaemon client.
        let host = remote_peer
            .host
            .clone()
            .ok_or(Error::InvalidPeer(remote_peer.id))?;
        let dfdaemon_client =
            DfdaemonClient::new(format!("http://{}:{}", host.ip, host.port)).await?;

        // Record the start of downloading piece.
        self.storage.download_piece_started(task_id, number)?;

        // Construct the interested pieces request.
        let in_stream = tokio_stream::once(SyncPiecesRequest {
            task_id: task_id.to_string(),
            request: Some(sync_pieces_request::Request::InterestedPiecesRequest(
                InterestedPiecesRequest {
                    piece_numbers: vec![number],
                },
            )),
        });

        // Send the interested pieces request.
        let response = dfdaemon_client.sync_pieces(in_stream).await?;
        let mut resp_stream = response.into_inner();
        if let Some(message) = resp_stream.message().await? {
            if let Some(sync_pieces_response::Response::InterestedPiecesResponse(
                InterestedPiecesResponse { piece },
            )) = message.response
            {
                if let Some(piece) = piece {
                    // Record the finish of downloading piece.
                    self.storage
                        .download_piece_from_remote_peer_finished(
                            task_id,
                            number,
                            piece.offset,
                            piece.digest.clone(),
                            &mut piece.content.as_slice(),
                        )
                        .await
                        .map_err(|err| {
                            // Record the failure of downloading piece,
                            // If storage fails to record piece.
                            error!("download piece finished: {}", err);
                            if let Some(err) =
                                self.storage.download_piece_failed(task_id, number).err()
                            {
                                error!("download piece failed: {}", err)
                            };
                            err
                        })?;

                    // Return reader of the piece.
                    return self.storage.upload_piece(task_id, number).await;
                }

                // Record the failure of downloading piece,
                // if the piece is not found.
                error!("piece not found");
                self.storage.download_piece_failed(task_id, number)?;
                return Err(Error::UnexpectedResponse());
            }

            // Record the failure of downloading piece,
            // if the response is not found.
            error!("response not found");
            self.storage.download_piece_failed(task_id, number)?;
            return Err(Error::UnexpectedResponse());
        }

        // Record the failure of downloading piece,
        // if the message is not found.
        error!("message not found");
        self.storage.download_piece_failed(task_id, number)?;
        Err(Error::UnexpectedResponse())
    }

    // download_piece_from_source downloads a piece from the source.
    pub async fn download_piece_from_source(
        &self,
        task_id: &str,
        number: i32,
        url: &str,
        offset: u64,
        header: HeaderMap,
        timeout: Duration,
    ) -> Result<impl AsyncRead> {
        // Record the start of downloading piece.
        self.storage.download_piece_started(task_id, number)?;

        // Download the piece from the source.
        let mut response = self
            .http_client
            .get(Request {
                url: url.to_string(),
                header,
                timeout: Some(timeout),
            })
            .await
            .map_err(|err| {
                // Record the failure of downloading piece,
                // if the request is failed.
                error!("http error: {}", err);
                if let Some(err) = self.storage.download_piece_failed(task_id, number).err() {
                    error!("download piece failed: {}", err)
                };
                err
            })?;

        // HTTP status code is not OK, handle the error.
        if !response.status_code.is_success() {
            // Record the failure of downloading piece,
            // if the status code is not OK.
            self.storage.download_piece_failed(task_id, number)?;

            let mut buffer = String::new();
            response.reader.read_to_string(&mut buffer).await?;
            error!("http error {}: {}", response.status_code, buffer.as_str());
            return Err(Error::HTTP(HttpError {
                status_code: response.status_code,
                header: response.header,
                body: buffer,
            }));
        }

        // Record the finish of downloading piece.
        self.storage
            .download_piece_from_source_finished(task_id, number, offset, &mut response.reader)
            .await
            .map_err(|err| {
                // Record the failure of downloading piece,
                // If storage fails to record piece.
                error!("download piece finished: {}", err);
                if let Some(err) = self.storage.download_piece_failed(task_id, number).err() {
                    error!("download piece failed: {}", err)
                };
                err
            })?;

        // Return reader of the piece.
        self.storage.upload_piece(task_id, number).await
    }
}
