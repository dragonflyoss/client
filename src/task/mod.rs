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

use crate::grpc::dfdaemon::DfdaemonClient;
use crate::storage::Storage;
use crate::{Error, Result};
use dragonfly_api::dfdaemon::v2::{
    sync_pieces_request, sync_pieces_response, InterestedPiecesRequest, InterestedPiecesResponse,
    SyncPiecesRequest,
};
use std::sync::Arc;
use tokio::io::AsyncRead;
use tracing::error;

// Task represents a task manager.
pub struct Task {
    // manager_client is the grpc client of the manager.
    dfdaemon_client: Arc<DfdaemonClient>,

    // manager_client is the grpc client of the manager.
    storage: Arc<Storage>,
}

// NewTask returns a new Task.
impl Task {
    // new returns a new Task.
    pub fn new(dfdaemon_client: Arc<DfdaemonClient>, storage: Arc<Storage>) -> Self {
        Self {
            dfdaemon_client,
            storage,
        }
    }

    // download_piece_from_remote_peer downloads a piece from a remote peer.
    pub async fn download_piece_from_remote_peer<'a>(
        &self,
        task_id: &str,
        number: u32,
    ) -> Result<impl AsyncRead> {
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
        let response = self.dfdaemon_client.sync_pieces(in_stream).await?;
        let mut resp_stream = response.into_inner();
        if let Some(message) = resp_stream.message().await? {
            if let Some(response) = message.response {
                match response {
                    sync_pieces_response::Response::InterestedPiecesResponse(
                        InterestedPiecesResponse { pieces },
                    ) => {
                        if let Some(piece) = pieces.first() {
                            // Record the finish of downloading piece.
                            self.storage
                                .download_piece_finished(
                                    task_id,
                                    number,
                                    piece.offset,
                                    &piece.digest,
                                    &mut piece.content.as_slice(),
                                )
                                .await?;

                            // Return reader of the piece.
                            return self.storage.upload_piece(task_id, number).await;
                        }

                        // Record the failure of downloading piece,
                        // if the piece is not found.
                        error!("piece not found");
                        self.storage.download_piece_failed(task_id, number)?;
                        return Err(Error::UnexpectedResponse());
                    }
                };
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

    // download_piece_from_local_peer downloads a piece from a local peer.
    pub async fn download_piece_from_local_peer(
        &self,
        task_id: &str,
        number: u32,
    ) -> Result<impl AsyncRead> {
        self.storage.upload_piece(task_id, number).await
    }

    // download_piece_from_source downloads a piece from the source.
    pub fn download_piece_from_source(&self) -> Result<()> {
        unimplemented!()
    }
}
