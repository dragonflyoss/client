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

use crate::backend::http::{Request as HTTPRequest, HTTP};
use crate::grpc::scheduler::SchedulerClient;
use crate::storage::{metadata, Storage};
use crate::utils::http::headermap_to_hashmap;
use crate::utils::id_generator::IDGenerator;
use crate::{Error, Result as ClientResult};
use dragonfly_api::common::v2::{Download, Piece, TrafficType};
use dragonfly_api::dfdaemon::v2::DownloadTaskResponse;
use dragonfly_api::scheduler::v2::{
    announce_peer_request, announce_peer_response, download_piece_back_to_source_failed_request,
    AnnouncePeerRequest, DownloadPeerBackToSourceStartedRequest, DownloadPeerFinishedRequest,
    DownloadPeerStartedRequest, DownloadPieceBackToSourceFailedRequest, DownloadPieceFailedRequest,
    DownloadPieceFinishedRequest, HttpResponse, RegisterPeerRequest,
};
use mpsc::Sender;
use reqwest::header::{self, HeaderMap};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncSeekExt, SeekFrom},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::Status;
use tracing::{error, info};

pub mod piece;

// Task represents a task manager.
pub struct Task {
    // id_generator is the id generator.
    pub id_generator: Arc<IDGenerator>,

    // manager_client is the grpc client of the manager.
    storage: Arc<Storage>,

    // scheduler_client is the grpc client of the scheduler.
    pub scheduler_client: Arc<SchedulerClient>,

    // http_client is the http client.
    http_client: Arc<HTTP>,

    // piece is the piece manager.
    pub piece: Arc<piece::Piece>,
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
        let piece = piece::Piece::new(
            storage.clone(),
            scheduler_client.clone(),
            http_client.clone(),
        );
        let piece = Arc::new(piece);

        Self {
            id_generator,
            storage: storage.clone(),
            scheduler_client: scheduler_client.clone(),
            http_client: http_client.clone(),
            piece: piece.clone(),
        }
    }

    // get gets a task metadata.
    pub fn get(&self, task_id: &str) -> ClientResult<Option<metadata::Task>> {
        self.storage.get_task(task_id)
    }

    // download_task_started updates the metadata of the task when the task downloads started.
    pub fn download_task_started(&self, id: &str, piece_length: u64) -> ClientResult<()> {
        self.storage.download_task_started(id, piece_length)
    }

    // download_task_finished updates the metadata of the task when the task downloads finished.
    pub fn download_task_finished(&self, id: &str) -> ClientResult<()> {
        self.storage.download_task_finished(id)
    }

    // download_task_failed updates the metadata of the task when the task downloads failed.
    pub fn download_task_failed(&self, id: &str) -> ClientResult<()> {
        self.storage.download_task_failed(id)
    }

    // download_into_file downloads a task into a file.
    #[allow(clippy::too_many_arguments)]
    pub async fn download_into_file(
        &self,
        task_id: &str,
        host_id: &str,
        peer_id: &str,
        content_length: u64,
        download: Download,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<()> {
        // Convert the timeout.
        let timeout: Option<Duration> = match download.timeout.clone() {
            Some(timeout) => match Duration::try_from(timeout) {
                Ok(timeout) => Some(timeout),
                Err(err) => {
                    error!("convert timeout error: {:?}", err);
                    download_progress_tx
                        .send(Err(Status::invalid_argument("invalid timeout")))
                        .await
                        .unwrap_or_else(|err| error!("send download progress error: {:?}", err));

                    return Err(Error::InvalidParameter());
                }
            },
            None => None,
        };

        // Open the file.
        let mut f = match OpenOptions::new()
            .create(true)
            .write(true)
            .open(download.output_path.as_str())
            .await
        {
            Ok(f) => f,
            Err(err) => {
                error!("open file error: {:?}", err);
                download_progress_tx
                    .send(Err(Status::internal("open file error")))
                    .await
                    .unwrap_or_else(|err| error!("send download progress error: {:?}", err));

                return Err(Error::IO(err));
            }
        };

        // Calculate the interested pieces to download.
        let interested_pieces = match self.piece.calculate_interested(
            download.piece_length,
            content_length,
            download.range.clone(),
        ) {
            Ok(interested_pieces) => interested_pieces,
            Err(err) => {
                error!("calculate interested pieces error: {:?}", err);
                download_progress_tx
                    .send(Err(Status::invalid_argument(
                        "calculate interested pieces error",
                    )))
                    .await
                    .unwrap_or_else(|err| error!("send download progress error: {:?}", err));

                return Err(err);
            }
        };
        info!("interested pieces: {:?}", interested_pieces);

        // Get the task from the local storage.
        let task = match self.get(task_id) {
            Ok(Some(task)) => task,
            Ok(None) => {
                error!("task not found");
                download_progress_tx
                    .send(Err(Status::not_found("task not found")))
                    .await
                    .unwrap_or_else(|err| error!("send download progress error: {:?}", err));

                return Err(Error::TaskNotFound(task_id.to_string()));
            }
            Err(err) => {
                error!("get task error: {:?}", err);
                download_progress_tx
                    .send(Err(Status::internal("get task error")))
                    .await
                    .unwrap_or_else(|err| error!("send download progress error: {:?}", err));

                return Err(err);
            }
        };

        // If the task is finished, return the file.
        if task.is_finished() {
            info!("task is finished, download the pieces from the local peer");

            // Download the pieces from the local peer.
            if let Err(err) = self
                .download_partial_from_local_peer_into_file(
                    &mut f,
                    task_id,
                    interested_pieces.clone(),
                    content_length,
                    download_progress_tx.clone(),
                )
                .await
            {
                error!("download from local peer error: {:?}", err);
                download_progress_tx
                    .send(Err(Status::internal("download from local peer error")))
                    .await
                    .unwrap_or_else(|err| error!("send download progress error: {:?}", err));

                return Err(err);
            };

            info!("all pieces are downloaded from local peer");
            return Ok(());
        }

        info!("download the pieces from local peer");

        // Download the pieces from the local peer.
        let finished_pieces = match self
            .download_partial_from_local_peer_into_file(
                &mut f,
                task_id,
                interested_pieces.clone(),
                content_length,
                download_progress_tx.clone(),
            )
            .await
        {
            Ok(finished_pieces) => finished_pieces,
            Err(err) => {
                error!("download from local peer error: {:?}", err);
                download_progress_tx
                    .send(Err(Status::internal(err.to_string())))
                    .await
                    .unwrap_or_else(|err| error!("send download progress error: {:?}", err));

                return Err(err);
            }
        };

        // Remove the finished pieces from the pieces.
        let interested_pieces = self
            .piece
            .remove_finished_from_interested(finished_pieces, interested_pieces);
        info!(
            "interested pieces after removing the finished piece: {:?}",
            interested_pieces
        );

        // Check if all pieces are downloaded.
        if interested_pieces.is_empty() {
            info!("all pieces are downloaded from local peer");
            return Ok(());
        };

        info!("download the pieces with scheduler");

        // Download the pieces with scheduler.
        if let Err(err) = self
            .download_partial_with_scheduler_into_file(
                &mut f,
                task_id,
                host_id,
                peer_id,
                interested_pieces.clone(),
                content_length,
                download.clone(),
                download_progress_tx.clone(),
            )
            .await
        {
            error!("download with scheduler error: {:?}", err);

            // Download the pieces from the source.
            if let Err(err) = self
                .download_partial_from_source_into_file(
                    &mut f,
                    task_id,
                    interested_pieces.clone(),
                    download.url.clone(),
                    download.header.clone(),
                    content_length,
                    timeout,
                    download_progress_tx.clone(),
                )
                .await
            {
                error!("download from source error: {:?}", err);
                download_progress_tx
                    .send(Err(Status::internal("download from source error")))
                    .await
                    .unwrap_or_else(|err| error!("send download progress error: {:?}", err));

                return Err(err);
            }

            info!("all pieces are downloaded from source");
            return Ok(());
        };

        info!("all pieces are downloaded with scheduler");
        Ok(())
    }

    // download_partial_with_scheduler_into_file downloads a partial task with scheduler into a file.
    #[allow(clippy::too_many_arguments)]
    async fn download_partial_with_scheduler_into_file(
        &self,
        f: &mut fs::File,
        task_id: &str,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        content_length: u64,
        download: Download,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Initialize stream channel.
        let (in_stream_tx, in_stream_rx) = mpsc::channel(128);

        // Send the register peer request.
        in_stream_tx
            .send(AnnouncePeerRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
                peer_id: peer_id.to_string(),
                request: Some(announce_peer_request::Request::RegisterPeerRequest(
                    RegisterPeerRequest {
                        download: Some(download.clone()),
                    },
                )),
            })
            .await?;

        // Send the download peer started request.
        in_stream_tx
            .send(AnnouncePeerRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
                peer_id: peer_id.to_string(),
                request: Some(announce_peer_request::Request::DownloadPeerStartedRequest(
                    DownloadPeerStartedRequest {},
                )),
            })
            .await?;

        // Initialize the stream.
        let in_stream = ReceiverStream::new(in_stream_rx);
        let response = self
            .scheduler_client
            .announce_peer(Request::new(in_stream))
            .await?;

        let mut out_stream = response.into_inner();
        while let Some(message) = out_stream.message().await? {
            let response = message.response.ok_or(Error::UnexpectedResponse())?;
            match response {
                announce_peer_response::Response::EmptyTaskResponse(response) => {
                    // If the task is empty, return an empty vector.
                    info!("empty task response: {:?}", response);

                    // Send the download peer finished request.
                    in_stream_tx
                        .send(AnnouncePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_peer_request::Request::DownloadPeerFinishedRequest(
                                    DownloadPeerFinishedRequest {
                                        content_length: 0,
                                        piece_count: 0,
                                    },
                                ),
                            ),
                        })
                        .await?;

                    drop(in_stream_tx);
                    return Ok(Vec::new());
                }
                announce_peer_response::Response::NormalTaskResponse(response) => {
                    // If the task is normal, download the pieces from the remote peer.
                    info!("normal task response: {:?}", response);
                    let collect_interested_pieces = self
                        .piece
                        .collect_interested_from_remote_peer(
                            task_id,
                            interested_pieces.clone(),
                            response.candidate_parents,
                        )
                        .await;

                    // Download the pieces from the remote peer.
                    finished_pieces = self
                        .download_partial_with_scheduler_from_remote_peer_into_file(
                            f,
                            task_id,
                            host_id,
                            peer_id,
                            collect_interested_pieces,
                            content_length,
                            download_progress_tx.clone(),
                            in_stream_tx.clone(),
                        )
                        .await?;

                    // Check if all pieces are downloaded.
                    if finished_pieces.len() == interested_pieces.len() {
                        // Send the download peer finished request.
                        in_stream_tx
                            .send(AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPeerFinishedRequest(
                                        DownloadPeerFinishedRequest {
                                            content_length,
                                            piece_count: interested_pieces.len() as u32,
                                        },
                                    ),
                                ),
                            })
                            .await?;

                        drop(in_stream_tx);
                        return Ok(finished_pieces);
                    }
                }
                announce_peer_response::Response::NeedBackToSourceResponse(response) => {
                    // If the task need back to source, download the pieces from the source.
                    info!("need back to source response: {:?}", response);

                    // Send the download peer back-to-source request.
                    in_stream_tx
                            .send(AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPeerBackToSourceStartedRequest(
                                        DownloadPeerBackToSourceStartedRequest {
                                            description: None,
                                        },
                                    ),
                                ),
                            })
                            .await?;

                    // Remove the finished pieces from the pieces.
                    let remaining_interested_pieces = self.piece.remove_finished_from_interested(
                        finished_pieces.clone(),
                        interested_pieces.clone(),
                    );

                    // Download the pieces from the source.
                    let finished_pieces = self
                        .download_partial_with_scheduler_from_source_into_file(
                            f,
                            task_id,
                            host_id,
                            peer_id,
                            remaining_interested_pieces.clone(),
                            download.url.clone(),
                            download.header.clone(),
                            content_length,
                            download_progress_tx.clone(),
                            in_stream_tx.clone(),
                        )
                        .await?;

                    if finished_pieces.len() == remaining_interested_pieces.len() {
                        // Send the download peer finished request.
                        in_stream_tx
                            .send(AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPeerFinishedRequest(
                                        DownloadPeerFinishedRequest {
                                            content_length,
                                            piece_count: interested_pieces.len() as u32,
                                        },
                                    ),
                                ),
                            })
                            .await?;

                        drop(in_stream_tx);
                        return Ok(finished_pieces);
                    }
                }
            }
        }

        // If not all pieces are downloaded, return an error.
        Err(Error::Unknown(
            "not all pieces are downloaded with scheduler".to_string(),
        ))
    }

    // download_partial_with_scheduler_from_remote_peer_into_file downloads a partial task with scheduler from a remote peer into a file.
    #[allow(clippy::too_many_arguments)]
    async fn download_partial_with_scheduler_from_remote_peer_into_file(
        &self,
        f: &mut fs::File,
        task_id: &str,
        host_id: &str,
        peer_id: &str,
        collect_interested_pieces: Vec<piece::CollectPiece>,
        content_length: u64,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
        in_stream_tx: Sender<AnnouncePeerRequest>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        for collect_interested_piece in collect_interested_pieces {
            let mut reader = match self
                .piece
                .download_from_remote_peer(
                    task_id,
                    collect_interested_piece.number,
                    collect_interested_piece.parent.clone(),
                )
                .await
            {
                Ok(reader) => reader,
                Err(err) => {
                    error!(
                        "download piece {} from remote peer error: {:?}",
                        collect_interested_piece.number, err
                    );

                    // Send the download piece failed request.
                    in_stream_tx
                        .send(AnnouncePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_peer_request::Request::DownloadPieceFailedRequest(
                                    DownloadPieceFailedRequest {
                                        piece_number: collect_interested_piece.number,
                                        parent_id: collect_interested_piece.parent.id.clone(),
                                        temporary: true,
                                    },
                                ),
                            ),
                        })
                        .await
                        .unwrap_or_else(|err| {
                            error!("send download piece failed request error: {:?}", err)
                        });

                    continue;
                }
            };

            // Get the piece metadata from the local storage.
            let metadata = self
                .piece
                .get(task_id, collect_interested_piece.number)?
                .ok_or(Error::PieceNotFound(
                    collect_interested_piece.number.to_string(),
                ))?;

            // Write the piece into the file.
            self.piece
                .write_into_file_and_verify(&mut reader, f, metadata.digest.as_str())
                .await?;

            info!(
                "finished piece {} from remote peer {}",
                collect_interested_piece.parent.id, metadata.number
            );

            // Construct the piece.
            let piece = Piece {
                number: metadata.number,
                parent_id: Some(collect_interested_piece.parent.id.clone()),
                offset: metadata.offset,
                length: metadata.length,
                digest: metadata.digest.clone(),
                content: None,
                traffic_type: Some(TrafficType::RemotePeer as i32),
                cost: metadata.prost_cost(),
                created_at: Some(prost_wkt_types::Timestamp::from(metadata.created_at)),
            };

            // Send the download piece finished request.
            in_stream_tx
                .send(AnnouncePeerRequest {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    peer_id: peer_id.to_string(),
                    request: Some(
                        announce_peer_request::Request::DownloadPieceFinishedRequest(
                            DownloadPieceFinishedRequest {
                                piece: Some(piece.clone()),
                            },
                        ),
                    ),
                })
                .await?;

            // Send the download progress.
            download_progress_tx
                .send(Ok(DownloadTaskResponse {
                    content_length,
                    piece: Some(piece.clone()),
                }))
                .await?;

            // Store the finished piece.
            finished_pieces.push(metadata.clone());
        }

        Ok(finished_pieces)
    }

    // download_partial_with_scheduler_from_source_into_file downloads a partial task with scheduler from the source into a file.
    #[allow(clippy::too_many_arguments)]
    async fn download_partial_with_scheduler_from_source_into_file(
        &self,
        f: &mut fs::File,
        task_id: &str,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        url: String,
        header: HashMap<String, String>,
        content_length: u64,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
        in_stream_tx: Sender<AnnouncePeerRequest>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Convert the header.
        let header: HeaderMap = (&header).try_into()?;

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        for interested_piece in interested_pieces.clone() {
            // Seek to the offset of the piece.
            if let Err(err) = f.seek(SeekFrom::Start(interested_piece.offset)).await {
                error!("seek error: {:?}", err);
                continue;
            }

            // Download the piece from the local peer.
            let mut reader = match self
                .piece
                .download_from_source(
                    task_id,
                    interested_piece.number,
                    url.as_str(),
                    interested_piece.offset,
                    interested_piece.length,
                    header.clone(),
                    None,
                )
                .await
            {
                Ok(reader) => reader,
                Err(Error::HTTP(err)) => {
                    error!(
                        "download piece {} from source error: {:?}",
                        interested_piece.number, err
                    );

                    // Send the download piece failed request.
                    in_stream_tx.send(AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
                                            DownloadPieceBackToSourceFailedRequest{
                                                piece_number: interested_piece.number,
                                                response: Some(download_piece_back_to_source_failed_request::Response::HttpResponse(
                                                        HttpResponse{
                                                            header: headermap_to_hashmap(&err.header),
                                                            status_code: err.status_code.as_u16() as i32,
                                                            status: err.status_code.canonical_reason().unwrap_or("").to_string(),
                                                        }
                                                )),
                                            }
                                    )),
                                })
                                .await
                                .unwrap_or_else(|err| error!("send download piece failed request error: {:?}", err));

                    return Err(Error::HTTP(err));
                }
                Err(err) => {
                    error!("download from source error: {:?}", err);

                    // Send the download piece failed request.
                    in_stream_tx.send(AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
                                            DownloadPieceBackToSourceFailedRequest{
                                                piece_number: interested_piece.number,
                                                response: None,
                                            }
                                    )),
                                })
                                .await
                                .unwrap_or_else(|err| error!("send download piece failed request error: {:?}", err));

                    return Err(err);
                }
            };

            // Get the piece metadata from the local storage.
            let metadata = self
                .piece
                .get(task_id, interested_piece.number)?
                .ok_or(Error::PieceNotFound(interested_piece.number.to_string()))?;

            // Write the piece into the file.
            self.piece
                .write_into_file_and_verify(&mut reader, f, metadata.digest.as_str())
                .await?;

            info!("finished piece {} from source", metadata.number);

            // Construct the piece.
            let piece = Piece {
                number: metadata.number,
                parent_id: None,
                offset: metadata.offset,
                length: metadata.length,
                digest: metadata.digest.clone(),
                content: None,
                traffic_type: Some(TrafficType::BackToSource as i32),
                cost: metadata.prost_cost(),
                created_at: Some(prost_wkt_types::Timestamp::from(metadata.created_at)),
            };

            // Send the download piece finished request.
            in_stream_tx
                .send(AnnouncePeerRequest {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    peer_id: peer_id.to_string(),
                    request: Some(
                        announce_peer_request::Request::DownloadPieceFinishedRequest(
                            DownloadPieceFinishedRequest {
                                piece: Some(piece.clone()),
                            },
                        ),
                    ),
                })
                .await?;

            // Send the download progress.
            download_progress_tx
                .send(Ok(DownloadTaskResponse {
                    content_length,
                    piece: Some(piece.clone()),
                }))
                .await?;

            // Store the finished piece.
            finished_pieces.push(metadata.clone());
        }

        Ok(finished_pieces)
    }

    // download_partial_from_local_peer_into_file downloads a partial task from a local peer into a file.
    async fn download_partial_from_local_peer_into_file(
        &self,
        f: &mut fs::File,
        task_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        content_length: u64,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        for interested_piece in interested_pieces {
            // Seek to the offset of the piece.
            if let Err(err) = f.seek(SeekFrom::Start(interested_piece.offset)).await {
                error!("seek error: {:?}", err);
                continue;
            }

            // Download the piece from the local peer.
            let mut reader = match self
                .piece
                .download_from_local_peer(task_id, interested_piece.number)
                .await
            {
                Ok(reader) => reader,
                Err(err) => {
                    error!(
                        "download piece {} from local peer error: {:?}",
                        interested_piece.number, err
                    );
                    continue;
                }
            };

            // Get the piece metadata from the local storage.
            let metadata = self
                .piece
                .get(task_id, interested_piece.number)?
                .ok_or(Error::PieceNotFound(interested_piece.number.to_string()))?;

            // Write the piece into the file.
            self.piece
                .write_into_file_and_verify(&mut reader, f, metadata.digest.as_str())
                .await?;

            info!("finished piece {} from local peer", metadata.number);

            // Construct the piece.
            let piece = Piece {
                number: metadata.number,
                parent_id: None,
                offset: metadata.offset,
                length: metadata.length,
                digest: metadata.digest.clone(),
                content: None,
                traffic_type: Some(TrafficType::LocalPeer as i32),
                cost: metadata.prost_cost(),
                created_at: Some(prost_wkt_types::Timestamp::from(metadata.created_at)),
            };

            // Send the download progress.
            download_progress_tx
                .send(Ok(DownloadTaskResponse {
                    content_length,
                    piece: Some(piece.clone()),
                }))
                .await?;

            // Store the finished piece.
            finished_pieces.push(interested_piece.clone());
        }

        Ok(finished_pieces)
    }

    // download_partial_from_source_into_file downloads a partial task from the source into a file.
    #[allow(clippy::too_many_arguments)]
    async fn download_partial_from_source_into_file(
        &self,
        f: &mut fs::File,
        task_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        url: String,
        header: HashMap<String, String>,
        content_length: u64,
        timeout: Option<Duration>,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Convert the header.
        let header: HeaderMap = (&header).try_into()?;

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Download the pieces.
        for interested_piece in &interested_pieces {
            // Download the piece from the source.
            let mut reader = self
                .piece
                .download_from_source(
                    task_id,
                    interested_piece.number,
                    url.as_str(),
                    interested_piece.offset,
                    interested_piece.length,
                    header.clone(),
                    timeout,
                )
                .await
                .map_err(|err| {
                    error!(
                        "download piece {} from source error: {:?}",
                        interested_piece.number, err
                    );
                    err
                })?;

            // Get the piece metadata from the local storage.
            let metadata = self
                .piece
                .get(task_id, interested_piece.number)?
                .ok_or(Error::PieceNotFound(interested_piece.number.to_string()))?;

            // Write the piece into the file.
            self.piece
                .write_into_file_and_verify(&mut reader, f, metadata.digest.as_str())
                .await?;

            info!("finished piece {} from source", metadata.number);

            // Construct the piece.
            let piece = Piece {
                number: metadata.number,
                parent_id: None,
                offset: metadata.offset,
                length: metadata.length,
                digest: metadata.digest.clone(),
                content: None,
                traffic_type: Some(TrafficType::LocalPeer as i32),
                cost: metadata.prost_cost(),
                created_at: Some(prost_wkt_types::Timestamp::from(metadata.created_at)),
            };

            // Send the download progress.
            download_progress_tx
                .send(Ok(DownloadTaskResponse {
                    content_length,
                    piece: Some(piece.clone()),
                }))
                .await?;

            // Store the finished piece.
            finished_pieces.push(metadata.clone());
        }

        // Check if all pieces are downloaded.
        if finished_pieces.len() == interested_pieces.len() {
            return Ok(finished_pieces);
        }

        // If not all pieces are downloaded, return an error.
        Err(Error::Unknown(
            "not all pieces are downloaded from source".to_string(),
        ))
    }

    // get_content_length gets the content length of the task.
    pub async fn get_content_length(
        &self,
        task_id: &str,
        url: &str,
        header: HeaderMap,
        timeout: Option<Duration>,
    ) -> ClientResult<u64> {
        let task = self
            .storage
            .get_task(task_id)?
            .ok_or(Error::TaskNotFound(task_id.to_string()))?;

        if let Some(content_length) = task.content_length {
            return Ok(content_length);
        }

        // Head the url to get the content length.
        let response = self
            .http_client
            .head(HTTPRequest {
                url: url.to_string(),
                header,
                timeout,
            })
            .await?;

        // Get the content length from the response.
        let content_length = response
            .header
            .get(header::CONTENT_LENGTH)
            .ok_or(Error::InvalidContentLength())?
            .to_str()
            .map_err(|_| Error::InvalidContentLength())?
            .parse::<u64>()
            .map_err(|_| Error::InvalidContentLength())?;

        // Set the content length of the task.
        self.storage
            .set_task_content_length(task_id, content_length)?;

        Ok(content_length)
    }

    // download_into_async_read downloads a task into an AsyncRead.
    // pub async fn download_into_async_read() -> Result<impl AsyncRead> {
    // Err(Error::Unimplemented())
    // }
}
