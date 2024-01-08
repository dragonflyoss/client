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
use crate::config::dfdaemon::Config;
use crate::grpc::{scheduler::SchedulerClient, REQUEST_TIMEOUT};
use crate::storage::{metadata, Storage};
use crate::utils::http::headermap_to_hashmap;
use crate::utils::id_generator::IDGenerator;
use crate::{DownloadFromRemotePeerFailed, Error, HTTPError, Result as ClientResult};
use dragonfly_api::common::v2::{Download, Peer, Piece, TrafficType};
use dragonfly_api::dfdaemon::v2::DownloadTaskResponse;
use dragonfly_api::scheduler::v2::{
    announce_peer_request, announce_peer_response, download_piece_back_to_source_failed_request,
    AnnouncePeerRequest, DownloadPeerBackToSourceFailedRequest,
    DownloadPeerBackToSourceFinishedRequest, DownloadPeerBackToSourceStartedRequest,
    DownloadPeerFailedRequest, DownloadPeerFinishedRequest, DownloadPeerStartedRequest,
    DownloadPieceBackToSourceFailedRequest, DownloadPieceBackToSourceFinishedRequest,
    DownloadPieceFailedRequest, DownloadPieceFinishedRequest, HttpResponse, RegisterPeerRequest,
};
use reqwest::header::{self, HeaderMap};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{self, OpenOptions};
use tokio::sync::{
    mpsc::{self, Sender},
    Semaphore,
};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::Request;
use tonic::Status;
use tracing::{error, info};

pub mod piece;
pub mod piece_collector;

// Task represents a task manager.
pub struct Task {
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

    // id_generator is the id generator.
    pub id_generator: Arc<IDGenerator>,

    // storage is the local storage.
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
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        scheduler_client: Arc<SchedulerClient>,
        http_client: Arc<HTTP>,
    ) -> Self {
        let piece = piece::Piece::new(config.clone(), storage.clone(), http_client.clone());
        let piece = Arc::new(piece);

        Self {
            config,
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
                    .send_timeout(
                        Err(Status::internal(format!("open file error: {:?}", err))),
                        REQUEST_TIMEOUT,
                    )
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
                    .send_timeout(
                        Err(Status::invalid_argument(format!(
                            "calculate interested pieces error: {:?}",
                            err
                        ))),
                        REQUEST_TIMEOUT,
                    )
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
                    .send_timeout(Err(Status::not_found("task not found")), REQUEST_TIMEOUT)
                    .await
                    .unwrap_or_else(|err| error!("send download progress error: {:?}", err));

                return Err(Error::TaskNotFound(task_id.to_string()));
            }
            Err(err) => {
                error!("get task error: {:?}", err);
                download_progress_tx
                    .send_timeout(
                        Err(Status::internal(format!("get task error: {:?}", err))),
                        REQUEST_TIMEOUT,
                    )
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
                .download_from_local_peer_into_file(
                    &mut f,
                    host_id,
                    task_id,
                    peer_id,
                    interested_pieces.clone(),
                    content_length,
                    download_progress_tx.clone(),
                )
                .await
            {
                error!("download from local peer error: {:?}", err);
                download_progress_tx
                    .send_timeout(
                        Err(Status::internal(format!(
                            "download from local peer error: {:?}",
                            err
                        ))),
                        REQUEST_TIMEOUT,
                    )
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
                host_id,
                task_id,
                peer_id,
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
                    .send_timeout(Err(Status::internal(err.to_string())), REQUEST_TIMEOUT)
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
                    host_id,
                    task_id,
                    peer_id,
                    interested_pieces.clone(),
                    download.url.clone(),
                    download.header.clone(),
                    content_length,
                    download_progress_tx.clone(),
                )
                .await
            {
                error!("download from source error: {:?}", err);
                download_progress_tx
                    .send_timeout(
                        Err(Status::internal(format!(
                            "download from source error: {}",
                            err
                        ))),
                        REQUEST_TIMEOUT,
                    )
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
        // Initialize the schedule count.
        let mut schedule_count = 0;

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Initialize stream channel.
        let (in_stream_tx, in_stream_rx) = mpsc::channel(1024);

        // Send the register peer request.
        in_stream_tx
            .send_timeout(
                AnnouncePeerRequest {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    peer_id: peer_id.to_string(),
                    request: Some(announce_peer_request::Request::RegisterPeerRequest(
                        RegisterPeerRequest {
                            download: Some(download.clone()),
                        },
                    )),
                },
                REQUEST_TIMEOUT,
            )
            .await?;
        info!("sent RegisterPeerRequest");

        // Initialize the stream.
        let in_stream = ReceiverStream::new(in_stream_rx);
        let response = self
            .scheduler_client
            .announce_peer(task_id, Request::new(in_stream))
            .await?;

        let mut out_stream = response.into_inner();
        while let Some(message) = out_stream.try_next().await? {
            // Check if the schedule count is exceeded.
            schedule_count += 1;
            if schedule_count >= self.config.scheduler.max_schedule_count {
                in_stream_tx
                    .send_timeout(
                        AnnouncePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_peer_request::Request::DownloadPeerFailedRequest(
                                    DownloadPeerFailedRequest {
                                        description: Some(
                                            "max schedule count exceeded".to_string(),
                                        ),
                                    },
                                ),
                            ),
                        },
                        REQUEST_TIMEOUT,
                    )
                    .await
                    .unwrap_or_else(|err| {
                        error!("send DownloadPeerFailedRequest failed: {:?}", err)
                    });
                info!("sent DownloadPeerFailedRequest");

                // Wait for the latest message to be sent.
                sleep(Duration::from_millis(1)).await;
                return Err(Error::MaxScheduleCountExceeded(
                    self.config.scheduler.max_schedule_count,
                ));
            }

            let response = message.response.ok_or(Error::UnexpectedResponse())?;
            match response {
                announce_peer_response::Response::EmptyTaskResponse(response) => {
                    // If the task is empty, return an empty vector.
                    info!("empty task response: {:?}", response);

                    // Send the download peer started request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPeerStartedRequest(
                                        DownloadPeerStartedRequest {},
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await?;
                    info!("sent DownloadPeerStartedRequest");

                    // Send the download peer finished request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
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
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await?;
                    info!("sent DownloadPeerFinishedRequest");

                    // Wait for the latest message to be sent.
                    sleep(Duration::from_millis(1)).await;
                    return Ok(Vec::new());
                }
                announce_peer_response::Response::NormalTaskResponse(response) => {
                    // If the task is normal, download the pieces from the remote peer.
                    info!("normal task response: {:?}", response);

                    // Send the download peer started request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPeerStartedRequest(
                                        DownloadPeerStartedRequest {},
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await?;
                    info!("sent DownloadPeerStartedRequest");

                    // Download the pieces from the remote peer.
                    finished_pieces = self
                        .download_partial_with_scheduler_from_remote_peer_into_file(
                            f,
                            task_id,
                            host_id,
                            peer_id,
                            response.candidate_parents.clone(),
                            interested_pieces.clone(),
                            content_length,
                            download_progress_tx.clone(),
                            in_stream_tx.clone(),
                        )
                        .await?;

                    // Check if all pieces are downloaded.
                    if finished_pieces.len() == interested_pieces.len() {
                        // Send the download peer finished request.
                        in_stream_tx
                            .send_timeout(
                                AnnouncePeerRequest {
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
                                },
                                REQUEST_TIMEOUT,
                            )
                            .await?;
                        info!("sent DownloadPeerFinishedRequest");

                        // Wait for the latest message to be sent.
                        sleep(Duration::from_millis(1)).await;
                        return Ok(finished_pieces);
                    }
                }
                announce_peer_response::Response::NeedBackToSourceResponse(response) => {
                    // If the task need back to source, download the pieces from the source.
                    info!("need back to source response: {:?}", response);

                    // Send the download peer back-to-source request.
                    in_stream_tx
                        .send_timeout(AnnouncePeerRequest {
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
                        }, REQUEST_TIMEOUT)
                        .await?;
                    info!("sent DownloadPeerBackToSourceStartedRequest");

                    // Remove the finished pieces from the pieces.
                    let remaining_interested_pieces = self.piece.remove_finished_from_interested(
                        finished_pieces.clone(),
                        interested_pieces.clone(),
                    );

                    // Download the pieces from the source.
                    let finished_pieces = match self
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
                        .await
                    {
                        Ok(finished_pieces) => finished_pieces,
                        Err(err) => {
                            in_stream_tx
                                .send_timeout(AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(
                                        announce_peer_request::Request::DownloadPeerBackToSourceFailedRequest(
                                            DownloadPeerBackToSourceFailedRequest {
                                                description: Some(err.to_string()),
                                            },
                                        ),
                                    ),
                                }, REQUEST_TIMEOUT)
                                .await
                                .unwrap_or_else(|err| {
                                    error!("send DownloadPeerBackToSourceFailedRequest failed: {:?}", err)
                                });
                            info!("sent DownloadPeerBackToSourceFailedRequest");

                            // Wait for the latest message to be sent.
                            sleep(Duration::from_millis(1)).await;
                            return Err(err);
                        }
                    };

                    if finished_pieces.len() == remaining_interested_pieces.len() {
                        // Send the download peer finished request.
                        in_stream_tx
                            .send_timeout(
                                AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(
                                        announce_peer_request::Request::DownloadPeerBackToSourceFinishedRequest(
                                            DownloadPeerBackToSourceFinishedRequest {
                                                content_length,
                                                piece_count: interested_pieces.len() as u32,
                                            },
                                        ),
                                    ),
                                },
                                REQUEST_TIMEOUT,
                            )
                            .await?;
                        info!("sent DownloadPeerBackToSourceFinishedRequest");

                        // Wait for the latest message to be sent.
                        sleep(Duration::from_millis(1)).await;
                        return Ok(finished_pieces);
                    }

                    in_stream_tx
                        .send_timeout(AnnouncePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_peer_request::Request::DownloadPeerBackToSourceFailedRequest(
                                    DownloadPeerBackToSourceFailedRequest {
                                        description: Some("not all pieces are downloaded from source".to_string()),
                                    },
                                ),
                            ),
                        }, REQUEST_TIMEOUT)
                        .await?;
                    info!("sent DownloadPeerBackToSourceFailedRequest");

                    // Wait for the latest message to be sent.
                    sleep(Duration::from_millis(1)).await;
                    return Err(Error::Unknown(
                        "not all pieces are downloaded from source".to_string(),
                    ));
                }
            }
        }

        // If the stream is finished abnormally, return an error.
        Err(Error::Unknown("stream is finished abnormally".to_string()))
    }

    // download_partial_with_scheduler_from_remote_peer_into_file downloads a partial task with scheduler from a remote peer into a file.
    #[allow(clippy::too_many_arguments)]
    async fn download_partial_with_scheduler_from_remote_peer_into_file(
        &self,
        f: &mut fs::File,
        task_id: &str,
        host_id: &str,
        peer_id: &str,
        parents: Vec<Peer>,
        interested_pieces: Vec<metadata::Piece>,
        content_length: u64,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
        in_stream_tx: Sender<AnnouncePeerRequest>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Initialize the piece collector.
        let piece_collector = piece_collector::PieceCollector::new(
            self.config.clone(),
            task_id,
            interested_pieces.clone(),
            parents.clone(),
        );
        let mut piece_collector_rx = piece_collector.run().await;

        // Initialize the join set.
        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(
            self.config.download.concurrent_piece_count as usize,
        ));

        // Download the pieces from the remote peers.
        while let Some(collect_piece) = piece_collector_rx.recv().await {
            let semaphore = semaphore.clone();
            async fn download_from_remote_peer(
                task_id: String,
                number: u32,
                parent: Peer,
                piece: Arc<piece::Piece>,
                semaphore: Arc<Semaphore>,
            ) -> ClientResult<metadata::Piece> {
                let _permit = semaphore.acquire().await.map_err(|err| {
                    error!("acquire semaphore error: {:?}", err);
                    Error::DownloadFromRemotePeerFailed(DownloadFromRemotePeerFailed {
                        piece_number: number,
                        parent_id: parent.id.clone(),
                    })
                })?;

                let metadata = piece
                    .download_from_remote_peer(task_id.as_str(), number, parent.clone())
                    .await
                    .map_err(|err| {
                        error!(
                            "download piece {} from remote peer {:?} error: {:?}",
                            number,
                            parent.id.clone(),
                            err
                        );
                        Error::DownloadFromRemotePeerFailed(DownloadFromRemotePeerFailed {
                            piece_number: number,
                            parent_id: parent.id.clone(),
                        })
                    })?;

                Ok(metadata)
            }

            join_set.spawn(download_from_remote_peer(
                task_id.to_string(),
                collect_piece.number,
                collect_piece.parent.clone(),
                self.piece.clone(),
                semaphore.clone(),
            ));
        }

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Wait for the pieces to be downloaded.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok(Ok(metadata)) => {
                    // Get the piece content from the local storage.
                    let mut reader = self.storage.upload_piece(task_id, metadata.number).await?;

                    // Write the piece into the file.
                    self.piece
                        .write_into_file_and_verify(
                            &mut reader,
                            f,
                            metadata.offset,
                            metadata.digest.as_str(),
                        )
                        .await?;

                    info!(
                        "finished piece {} from remote peer {:?}",
                        metadata.number, metadata.parent_id
                    );

                    // Construct the piece.
                    let piece = Piece {
                        number: metadata.number,
                        parent_id: metadata.parent_id.clone(),
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
                        .send_timeout(
                            AnnouncePeerRequest {
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
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await?;

                    // Send the download progress.
                    download_progress_tx
                        .send_timeout(
                            Ok(DownloadTaskResponse {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                content_length,
                                piece: Some(piece.clone()),
                            }),
                            REQUEST_TIMEOUT,
                        )
                        .await?;

                    // Store the finished piece.
                    finished_pieces.push(metadata.clone());
                }
                Ok(Err(Error::DownloadFromRemotePeerFailed(err))) => {
                    error!(
                        "download piece {} from remote peer {} error: {:?}",
                        err.piece_number, err.parent_id, err
                    );

                    // Send the download piece failed request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPieceFailedRequest(
                                        DownloadPieceFailedRequest {
                                            piece_number: Some(err.piece_number),
                                            parent_id: err.parent_id,
                                            temporary: true,
                                        },
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                        .unwrap_or_else(|err| {
                            error!("send download piece failed request error: {:?}", err)
                        });

                    continue;
                }
                Ok(Err(err)) => {
                    error!("download from remote peer error: {:?}", err);
                    continue;
                }
                Err(err) => {
                    return Err(Error::JoinError(err));
                }
            }
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

        // Download the piece from the local peer.
        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(
            self.config.download.concurrent_piece_count as usize,
        ));
        for interested_piece in interested_pieces {
            let semaphore = semaphore.clone();
            async fn download_from_source(
                task_id: String,
                number: u32,
                url: String,
                offset: u64,
                length: u64,
                header: HeaderMap,
                piece: Arc<piece::Piece>,
                semaphore: Arc<Semaphore>,
            ) -> ClientResult<metadata::Piece> {
                let _permit = semaphore.acquire().await?;

                let metadata = piece
                    .download_from_source(
                        task_id.as_str(),
                        number,
                        url.as_str(),
                        offset,
                        length,
                        header,
                    )
                    .await?;

                Ok(metadata)
            }

            join_set.spawn(download_from_source(
                task_id.to_string(),
                interested_piece.number,
                url.clone(),
                interested_piece.offset,
                interested_piece.length,
                header.clone(),
                self.piece.clone(),
                semaphore.clone(),
            ));
        }

        // Wait for the pieces to be downloaded.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok(Ok(metadata)) => {
                    // Get the piece content from the local storage.
                    let mut reader = self.storage.upload_piece(task_id, metadata.number).await?;

                    // Write the piece into the file.
                    self.piece
                        .write_into_file_and_verify(
                            &mut reader,
                            f,
                            metadata.offset,
                            metadata.digest.as_str(),
                        )
                        .await?;

                    info!("finished piece {} from source", metadata.number);

                    // Construct the piece.
                    let piece = Piece {
                        number: metadata.number,
                        parent_id: metadata.parent_id.clone(),
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
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPieceBackToSourceFinishedRequest(
                                        DownloadPieceBackToSourceFinishedRequest {
                                            piece: Some(piece.clone()),
                                        },
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await?;

                    // Send the download progress.
                    download_progress_tx
                        .send_timeout(
                            Ok(DownloadTaskResponse {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                content_length,
                                piece: Some(piece.clone()),
                            }),
                            REQUEST_TIMEOUT,
                        )
                        .await?;

                    // Store the finished piece.
                    finished_pieces.push(metadata.clone());
                }
                Ok(Err(Error::HTTP(err))) => {
                    // Send the download piece http failed request.
                    in_stream_tx.send_timeout(AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
                                            DownloadPieceBackToSourceFailedRequest{
                                                piece_number: None,
                                                response: Some(download_piece_back_to_source_failed_request::Response::HttpResponse(
                                                        HttpResponse{
                                                            header: headermap_to_hashmap(&err.header),
                                                            status_code: err.status_code.as_u16() as i32,
                                                            status: err.status_code.canonical_reason().unwrap_or("").to_string(),
                                                        }
                                                )),
                                            }
                                    )),
                                }, REQUEST_TIMEOUT)
                                .await
                                .unwrap_or_else(|err| error!("send download piece failed request error: {:?}", err));

                    join_set.abort_all();
                    return Err(Error::HTTP(err));
                }
                Ok(Err(err)) => {
                    // Send the download piece failed request.
                    in_stream_tx.send_timeout(AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
                                            DownloadPieceBackToSourceFailedRequest{
                                                piece_number: None,
                                                response: None,
                                            }
                                    )),
                                }, REQUEST_TIMEOUT)
                                .await
                                .unwrap_or_else(|err| error!("send download piece failed request error: {:?}", err));

                    return Err(err);
                }
                Err(err) => {
                    return Err(Error::JoinError(err));
                }
            }
        }

        Ok(finished_pieces)
    }

    // download_from_local_peer_into_file downloads a task from a local peer into a file.
    #[allow(clippy::too_many_arguments)]
    async fn download_from_local_peer_into_file(
        &self,
        f: &mut fs::File,
        host_id: &str,
        task_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        content_length: u64,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<()> {
        let finished_pieces = self
            .download_partial_from_local_peer_into_file(
                f,
                host_id,
                task_id,
                peer_id,
                interested_pieces.clone(),
                content_length,
                download_progress_tx.clone(),
            )
            .await?;

        // Check if all pieces are downloaded.
        if finished_pieces.len() == interested_pieces.len() {
            return Ok(());
        }

        // Send the download progress.
        download_progress_tx
            .send_timeout(
                Err(Status::internal("not all pieces are downloaded")),
                REQUEST_TIMEOUT,
            )
            .await?;

        // If not all pieces are downloaded, return an error.
        Err(Error::Unknown(
            "not all pieces are downloaded from source".to_string(),
        ))
    }

    // download_partial_from_local_peer_into_file downloads a partial task from a local peer into a file.
    #[allow(clippy::too_many_arguments)]
    async fn download_partial_from_local_peer_into_file(
        &self,
        f: &mut fs::File,
        host_id: &str,
        task_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        content_length: u64,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Download the piece from the local peer.
        for interested_piece in interested_pieces {
            let mut reader = match self
                .piece
                .download_from_local_peer_into_async_read(task_id, interested_piece.number)
                .await
            {
                Ok(reader) => reader,
                Err(err) => {
                    info!(
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
                .write_into_file_and_verify(
                    &mut reader,
                    f,
                    metadata.offset,
                    metadata.digest.as_str(),
                )
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
                .send_timeout(
                    Ok(DownloadTaskResponse {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                        peer_id: peer_id.to_string(),
                        content_length,
                        piece: Some(piece.clone()),
                    }),
                    REQUEST_TIMEOUT,
                )
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
        host_id: &str,
        task_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        url: String,
        header: HashMap<String, String>,
        content_length: u64,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Convert the header.
        let header: HeaderMap = (&header).try_into()?;

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Download the pieces.
        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(
            self.config.download.concurrent_piece_count as usize,
        ));
        for interested_piece in &interested_pieces {
            let semaphore = semaphore.clone();
            async fn download_from_source(
                task_id: String,
                number: u32,
                url: String,
                offset: u64,
                length: u64,
                header: HeaderMap,
                piece: Arc<piece::Piece>,
                semaphore: Arc<Semaphore>,
            ) -> ClientResult<metadata::Piece> {
                let _permit = semaphore.acquire().await?;

                let metadata = piece
                    .download_from_source(
                        task_id.as_str(),
                        number,
                        url.as_str(),
                        offset,
                        length,
                        header,
                    )
                    .await?;

                Ok(metadata)
            }

            join_set.spawn(download_from_source(
                task_id.to_string(),
                interested_piece.number,
                url.clone(),
                interested_piece.offset,
                interested_piece.length,
                header.clone(),
                self.piece.clone(),
                semaphore.clone(),
            ));
        }

        // Wait for the pieces to be downloaded.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok(Ok(metadata)) => {
                    // Get the piece content from the local storage.
                    let mut reader = self.storage.upload_piece(task_id, metadata.number).await?;

                    // Write the piece into the file.
                    self.piece
                        .write_into_file_and_verify(
                            &mut reader,
                            f,
                            metadata.offset,
                            metadata.digest.as_str(),
                        )
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

                    // Send the download progress.
                    download_progress_tx
                        .send_timeout(
                            Ok(DownloadTaskResponse {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                content_length,
                                piece: Some(piece.clone()),
                            }),
                            REQUEST_TIMEOUT,
                        )
                        .await?;

                    // Store the finished piece.
                    finished_pieces.push(metadata.clone());
                }
                Ok(Err(err)) => {
                    join_set.abort_all();
                    return Err(err);
                }
                Err(err) => {
                    return Err(Error::JoinError(err));
                }
            }
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
                timeout: self.config.download.piece_timeout,
            })
            .await?;

        // Check if the status code is success.
        if !response.status_code.is_success() {
            return Err(Error::HTTP(HTTPError {
                status_code: response.status_code,
                header: response.header,
            }));
        }

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
}
