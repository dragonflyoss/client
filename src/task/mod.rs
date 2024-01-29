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
use crate::utils::http::reqwest_headermap_to_hashmap;
use crate::utils::id_generator::IDGenerator;
use crate::{DownloadFromRemotePeerFailed, Error, HTTPError, Result as ClientResult};
use dragonfly_api::common::v2::Range;
use dragonfly_api::common::v2::{Download, Peer, Piece, TrafficType};
use dragonfly_api::dfdaemon::{
    self,
    v2::{download_task_response, DownloadTaskResponse},
};
use dragonfly_api::scheduler::v2::{
    announce_peer_request, announce_peer_response, download_piece_back_to_source_failed_request,
    AnnouncePeerRequest, DownloadPeerBackToSourceFailedRequest,
    DownloadPeerBackToSourceFinishedRequest, DownloadPeerBackToSourceStartedRequest,
    DownloadPeerFailedRequest, DownloadPeerFinishedRequest, DownloadPeerStartedRequest,
    DownloadPieceBackToSourceFailedRequest, DownloadPieceBackToSourceFinishedRequest,
    DownloadPieceFailedRequest, DownloadPieceFinishedRequest, HttpResponse, RegisterPeerRequest,
    RescheduleRequest,
};
use reqwest::header::HeaderMap;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{
    mpsc::{self, Sender},
    Semaphore,
};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::Request;
use tonic::Status;
use tracing::{error, info, Instrument};

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

    // download_started updates the metadata of the task when the task downloads started.
    pub async fn download_started(
        &self,
        id: &str,
        piece_length: u64,
        url: &str,
        mut request_header: HeaderMap,
    ) -> ClientResult<metadata::Task> {
        let task = self.storage.download_task_started(id, piece_length, None)?;

        if !task.response_header.is_empty() {
            return Ok(task);
        }

        // Remove the range header to prevent the server from
        // returning a 206 partial content and returning
        // a 200 full content.
        request_header.remove(reqwest::header::RANGE);

        // Head the url to get the content length.
        let response = self
            .http_client
            .head(HTTPRequest {
                url: url.to_string(),
                header: request_header,
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

        self.storage
            .download_task_started(id, piece_length, Some(response.header))
    }

    // download_finished updates the metadata of the task when the task downloads finished.
    pub fn download_finished(&self, id: &str) -> ClientResult<metadata::Task> {
        self.storage.download_task_finished(id)
    }

    // download_failed updates the metadata of the task when the task downloads failed.
    pub async fn download_failed(&self, id: &str) -> ClientResult<()> {
        self.storage.download_task_failed(id).await
    }

    // hard_link_or_copy hard links or copies the task content to the destination.
    pub async fn hard_link_or_copy(
        &self,
        task_id: &str,
        to: &Path,
        range: Option<Range>,
    ) -> ClientResult<()> {
        self.storage
            .hard_link_or_copy_task(task_id, to, range)
            .await
    }

    // download downloads a task.
    #[allow(clippy::too_many_arguments)]
    pub async fn download(
        &self,
        task: metadata::Task,
        host_id: &str,
        peer_id: &str,
        download: Download,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<()> {
        // Send the download task started request.
        download_progress_tx
            .send_timeout(
                Ok(DownloadTaskResponse {
                    host_id: host_id.to_string(),
                    task_id: task.id.clone(),
                    peer_id: peer_id.to_string(),
                    request: Some(download_task_response::Request::DownloadTaskStartedRequest(
                        dfdaemon::v2::DownloadTaskStartedRequest {
                            response_header: task.response_header.clone(),
                        },
                    )),
                }),
                REQUEST_TIMEOUT,
            )
            .await?;

        // Get the content length from the task.
        let Some(content_length) = task.content_length() else {
            error!("content length not found");
            download_progress_tx
                .send_timeout(
                    Err(Status::internal("content length not found")),
                    REQUEST_TIMEOUT,
                )
                .await
                .unwrap_or_else(|err| error!("send download progress error: {:?}", err));

            return Err(Error::InvalidContentLength());
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

        // If the task is finished, return the file.
        if task.is_finished() {
            info!("task is finished, download the pieces from the local peer");

            // Download the pieces from the local peer.
            if let Err(err) = self
                .download_from_local_peer(
                    task.clone(),
                    host_id,
                    peer_id,
                    interested_pieces.clone(),
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

            // Send the download task finished request.
            download_progress_tx
                .send_timeout(
                    Ok(DownloadTaskResponse {
                        host_id: host_id.to_string(),
                        task_id: task.id.clone(),
                        peer_id: peer_id.to_string(),
                        request: Some(
                            download_task_response::Request::DownloadTaskFinishedRequest(
                                dfdaemon::v2::DownloadTaskFinishedRequest {},
                            ),
                        ),
                    }),
                    REQUEST_TIMEOUT,
                )
                .await?;

            info!("all pieces are downloaded from local peer");
            return Ok(());
        }
        info!("download the pieces from local peer");

        // Download the pieces from the local peer.
        let finished_pieces = match self
            .download_partial_from_local_peer(
                task.clone(),
                host_id,
                peer_id,
                interested_pieces.clone(),
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
            // Send the download task finished request.
            download_progress_tx
                .send_timeout(
                    Ok(DownloadTaskResponse {
                        host_id: host_id.to_string(),
                        task_id: task.id.clone(),
                        peer_id: peer_id.to_string(),
                        request: Some(
                            download_task_response::Request::DownloadTaskFinishedRequest(
                                dfdaemon::v2::DownloadTaskFinishedRequest {},
                            ),
                        ),
                    }),
                    REQUEST_TIMEOUT,
                )
                .await?;

            info!("all pieces are downloaded from local peer");
            return Ok(());
        };
        info!("download the pieces with scheduler");

        // Download the pieces with scheduler.
        if let Err(err) = self
            .download_partial_with_scheduler(
                task.clone(),
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
                .download_partial_from_source(
                    task.clone(),
                    host_id,
                    peer_id,
                    interested_pieces.clone(),
                    download.url.clone(),
                    download.request_header.clone(),
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

            // Send the download task finished request.
            download_progress_tx
                .send_timeout(
                    Ok(DownloadTaskResponse {
                        host_id: host_id.to_string(),
                        task_id: task.id.clone(),
                        peer_id: peer_id.to_string(),
                        request: Some(
                            download_task_response::Request::DownloadTaskFinishedRequest(
                                dfdaemon::v2::DownloadTaskFinishedRequest {},
                            ),
                        ),
                    }),
                    REQUEST_TIMEOUT,
                )
                .await?;

            info!("all pieces are downloaded from source");
            return Ok(());
        };

        // Send the download task finished request.
        download_progress_tx
            .send_timeout(
                Ok(DownloadTaskResponse {
                    host_id: host_id.to_string(),
                    task_id: task.id.clone(),
                    peer_id: peer_id.to_string(),
                    request: Some(
                        download_task_response::Request::DownloadTaskFinishedRequest(
                            dfdaemon::v2::DownloadTaskFinishedRequest {},
                        ),
                    ),
                }),
                REQUEST_TIMEOUT,
            )
            .await?;

        info!("all pieces are downloaded with scheduler");
        Ok(())
    }

    // download_partial_with_scheduler downloads a partial task with scheduler.
    #[allow(clippy::too_many_arguments)]
    async fn download_partial_with_scheduler(
        &self,
        task: metadata::Task,
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
                    task_id: task.id.clone(),
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
            .announce_peer(task.id.as_str(), Request::new(in_stream))
            .await?;

        let out_stream = response
            .into_inner()
            .timeout(self.config.scheduler.schedule_timeout);
        tokio::pin!(out_stream);

        while let Some(message) = out_stream.try_next().await? {
            // Check if the schedule count is exceeded.
            schedule_count += 1;
            if schedule_count >= self.config.scheduler.max_schedule_count {
                in_stream_tx
                    .send_timeout(
                        AnnouncePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task.id.clone(),
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

            let response = message?.response.ok_or(Error::UnexpectedResponse())?;
            match response {
                announce_peer_response::Response::EmptyTaskResponse(response) => {
                    // If the task is empty, return an empty vector.
                    info!("empty task response: {:?}", response);

                    // Send the download peer started request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task.id.clone(),
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
                                task_id: task.id.clone(),
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
                                task_id: task.id.clone(),
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
                        .download_partial_with_scheduler_from_remote_peer(
                            task.clone(),
                            host_id,
                            peer_id,
                            response.candidate_parents.clone(),
                            interested_pieces.clone(),
                            download_progress_tx.clone(),
                            in_stream_tx.clone(),
                        )
                        .await?;
                    info!(
                        "schedule {} finished {} pieces from remote peer",
                        schedule_count,
                        finished_pieces.len()
                    );

                    // Check if all pieces are downloaded.
                    if finished_pieces.len() == interested_pieces.len() {
                        // Send the download peer finished request.
                        in_stream_tx
                            .send_timeout(
                                AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task.id.clone(),
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

                    // If not all pieces are downloaded, send the reschedule request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task.id.clone(),
                                peer_id: peer_id.to_string(),
                                request: Some(announce_peer_request::Request::RescheduleRequest(
                                    RescheduleRequest {
                                        candidate_parents: response.candidate_parents,
                                        description: Some(
                                            "not all pieces are downloaded from remote peer"
                                                .to_string(),
                                        ),
                                    },
                                )),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await?;
                    info!("sent RescheduleRequest");
                }
                announce_peer_response::Response::NeedBackToSourceResponse(response) => {
                    // If the task need back to source, download the pieces from the source.
                    info!("need back to source response: {:?}", response);

                    // Send the download peer back-to-source request.
                    in_stream_tx
                        .send_timeout(AnnouncePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task.id.clone(),
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
                        .download_partial_with_scheduler_from_source(
                            task.clone(),
                            host_id,
                            peer_id,
                            remaining_interested_pieces.clone(),
                            download.url.clone(),
                            download.request_header.clone(),
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
                                    task_id: task.id.clone(),
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
                                    task_id: task.id.clone(),
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
                            task_id: task.id,
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

    // download_partial_with_scheduler_from_remote_peer downloads a partial task with scheduler from a remote peer.
    #[allow(clippy::too_many_arguments)]
    async fn download_partial_with_scheduler_from_remote_peer(
        &self,
        task: metadata::Task,
        host_id: &str,
        peer_id: &str,
        parents: Vec<Peer>,
        interested_pieces: Vec<metadata::Piece>,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
        in_stream_tx: Sender<AnnouncePeerRequest>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Initialize the piece collector.
        let piece_collector = piece_collector::PieceCollector::new(
            self.config.clone(),
            task.id.as_str(),
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
                length: u64,
                parent: Peer,
                piece_manager: Arc<piece::Piece>,
                storage: Arc<Storage>,
                semaphore: Arc<Semaphore>,
            ) -> ClientResult<metadata::Piece> {
                info!(
                    "start to download piece {} from remote peer {:?}",
                    storage.piece_id(task_id.as_str(), number),
                    parent.id.clone()
                );

                let _permit = semaphore.acquire().await.map_err(|err| {
                    error!("acquire semaphore error: {:?}", err);
                    Error::DownloadFromRemotePeerFailed(DownloadFromRemotePeerFailed {
                        piece_number: number,
                        parent_id: parent.id.clone(),
                    })
                })?;

                piece_manager
                    .download_from_remote_peer(task_id.as_str(), number, length, parent.clone())
                    .await
                    .map_err(|err| {
                        error!(
                            "download piece {} from remote peer {:?} error: {:?}",
                            storage.piece_id(task_id.as_str(), number),
                            parent.id.clone(),
                            err
                        );
                        Error::DownloadFromRemotePeerFailed(DownloadFromRemotePeerFailed {
                            piece_number: number,
                            parent_id: parent.id.clone(),
                        })
                    })
            }

            join_set.spawn(
                download_from_remote_peer(
                    task.id.clone(),
                    collect_piece.number,
                    collect_piece.length,
                    collect_piece.parent.clone(),
                    self.piece.clone(),
                    self.storage.clone(),
                    semaphore.clone(),
                )
                .in_current_span(),
            );
        }

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Wait for the pieces to be downloaded.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok(Ok(metadata)) => {
                    info!(
                        "finished piece {} from remote peer {:?}",
                        self.storage.piece_id(task.id.as_str(), metadata.number),
                        metadata.parent_id
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
                                task_id: task.id.clone(),
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
                                task_id: task.id.clone(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    download_task_response::Request::DownloadPieceFinishedRequest(
                                        dfdaemon::v2::DownloadPieceFinishedRequest {
                                            piece: Some(piece.clone()),
                                        },
                                    ),
                                ),
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
                        self.storage.piece_id(task.id.as_str(), err.piece_number),
                        err.parent_id,
                        err
                    );

                    // Send the download piece failed request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task.id.clone(),
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

    // download_partial_with_scheduler_from_source downloads a partial task with scheduler from the source.
    #[allow(clippy::too_many_arguments)]
    async fn download_partial_with_scheduler_from_source(
        &self,
        task: metadata::Task,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        url: String,
        request_header: HashMap<String, String>,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
        in_stream_tx: Sender<AnnouncePeerRequest>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Convert the header.
        let request_header: HeaderMap = (&request_header).try_into()?;

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
                request_header: HeaderMap,
                piece_manager: Arc<piece::Piece>,
                storage: Arc<Storage>,
                semaphore: Arc<Semaphore>,
            ) -> ClientResult<metadata::Piece> {
                info!(
                    "start to download piece {} from source",
                    storage.piece_id(task_id.as_str(), number)
                );

                let _permit = semaphore.acquire().await?;
                piece_manager
                    .download_from_source(
                        task_id.as_str(),
                        number,
                        url.as_str(),
                        offset,
                        length,
                        request_header,
                    )
                    .await
            }

            join_set.spawn(
                download_from_source(
                    task.id.clone(),
                    interested_piece.number,
                    url.clone(),
                    interested_piece.offset,
                    interested_piece.length,
                    request_header.clone(),
                    self.piece.clone(),
                    self.storage.clone(),
                    semaphore.clone(),
                )
                .in_current_span(),
            );
        }

        // Wait for the pieces to be downloaded.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok(Ok(metadata)) => {
                    info!(
                        "finished piece {} from source",
                        self.storage.piece_id(task.id.as_str(), metadata.number)
                    );

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
                                task_id: task.id.clone(),
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
                                task_id: task.id.clone(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    download_task_response::Request::DownloadPieceFinishedRequest(
                                        dfdaemon::v2::DownloadPieceFinishedRequest {
                                            piece: Some(piece.clone()),
                                        },
                                    ),
                                ),
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
                                    task_id: task.id.clone(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
                                            DownloadPieceBackToSourceFailedRequest{
                                                piece_number: None,
                                                response: Some(download_piece_back_to_source_failed_request::Response::HttpResponse(
                                                        HttpResponse{
                                                            header: reqwest_headermap_to_hashmap(&err.header),
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
                                    task_id: task.id,
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

    // download_from_local_peer downloads a task from a local peer.
    #[allow(clippy::too_many_arguments)]
    async fn download_from_local_peer(
        &self,
        task: metadata::Task,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<()> {
        let finished_pieces = self
            .download_partial_from_local_peer(
                task,
                host_id,
                peer_id,
                interested_pieces.clone(),
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

    // download_partial_from_local_peer downloads a partial task from a local peer.
    #[allow(clippy::too_many_arguments)]
    async fn download_partial_from_local_peer(
        &self,
        task: metadata::Task,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Download the piece from the local peer.
        for interested_piece in interested_pieces {
            // Get the piece metadata from the local storage.
            let piece = match self.piece.get(task.id.as_str(), interested_piece.number) {
                Ok(Some(piece)) => piece,
                Ok(None) => {
                    info!(
                        "piece {} not found in local storage",
                        self.storage
                            .piece_id(task.id.as_str(), interested_piece.number)
                    );
                    continue;
                }
                Err(err) => {
                    error!(
                        "get piece {} from local storage error: {:?}",
                        self.storage
                            .piece_id(task.id.as_str(), interested_piece.number),
                        err
                    );
                    continue;
                }
            };

            info!(
                "finished piece {} from local peer",
                self.storage.piece_id(task.id.as_str(), piece.number)
            );

            // Construct the piece.
            let piece = Piece {
                number: piece.number,
                parent_id: None,
                offset: piece.offset,
                length: piece.length,
                digest: piece.digest.clone(),
                content: None,
                traffic_type: Some(TrafficType::LocalPeer as i32),
                cost: piece.prost_cost(),
                created_at: Some(prost_wkt_types::Timestamp::from(piece.created_at)),
            };

            // Send the download progress.
            download_progress_tx
                .send_timeout(
                    Ok(DownloadTaskResponse {
                        host_id: host_id.to_string(),
                        task_id: task.id.clone(),
                        peer_id: peer_id.to_string(),
                        request: Some(
                            download_task_response::Request::DownloadPieceFinishedRequest(
                                dfdaemon::v2::DownloadPieceFinishedRequest {
                                    piece: Some(piece.clone()),
                                },
                            ),
                        ),
                    }),
                    REQUEST_TIMEOUT,
                )
                .await?;

            // Store the finished piece.
            finished_pieces.push(interested_piece.clone());
        }

        Ok(finished_pieces)
    }

    // download_partial_from_source downloads a partial task from the source.
    #[allow(clippy::too_many_arguments)]
    async fn download_partial_from_source(
        &self,
        task: metadata::Task,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        url: String,
        request_header: HashMap<String, String>,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Convert the header.
        let request_header: HeaderMap = (&request_header).try_into()?;

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
                request_header: HeaderMap,
                piece_manager: Arc<piece::Piece>,
                storage: Arc<Storage>,
                semaphore: Arc<Semaphore>,
            ) -> ClientResult<metadata::Piece> {
                info!(
                    "start to download piece {} from source",
                    storage.piece_id(task_id.as_str(), number)
                );

                let _permit = semaphore.acquire().await?;
                piece_manager
                    .download_from_source(
                        task_id.as_str(),
                        number,
                        url.as_str(),
                        offset,
                        length,
                        request_header,
                    )
                    .await
            }

            join_set.spawn(
                download_from_source(
                    task.id.clone(),
                    interested_piece.number,
                    url.clone(),
                    interested_piece.offset,
                    interested_piece.length,
                    request_header.clone(),
                    self.piece.clone(),
                    self.storage.clone(),
                    semaphore.clone(),
                )
                .in_current_span(),
            );
        }

        // Wait for the pieces to be downloaded.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok(Ok(metadata)) => {
                    info!(
                        "finished piece {} from source",
                        self.storage.piece_id(task.id.as_str(), metadata.number)
                    );

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
                                task_id: task.id.clone(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    download_task_response::Request::DownloadPieceFinishedRequest(
                                        dfdaemon::v2::DownloadPieceFinishedRequest {
                                            piece: Some(piece.clone()),
                                        },
                                    ),
                                ),
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
}
