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

use crate::grpc::{scheduler::SchedulerClient, REQUEST_TIMEOUT};
use dragonfly_api::common::v2::{
    PersistentCachePeer, PersistentCacheTask as CommonPersistentCacheTask, Piece, TrafficType,
};
use dragonfly_api::dfdaemon::{
    self,
    v2::{
        download_persistent_cache_task_response, DownloadPersistentCacheTaskRequest,
        DownloadPersistentCacheTaskResponse, UploadPersistentCacheTaskRequest,
    },
};
use dragonfly_api::scheduler::v2::{
    announce_persistent_cache_peer_request, announce_persistent_cache_peer_response,
    AnnouncePersistentCachePeerRequest, DownloadPersistentCachePeerFailedRequest,
    DownloadPersistentCachePeerFinishedRequest, DownloadPersistentCachePeerStartedRequest,
    DownloadPieceFailedRequest, DownloadPieceFinishedRequest, RegisterPersistentCachePeerRequest,
    ReschedulePersistentCachePeerRequest, StatPersistentCacheTaskRequest,
    UploadPersistentCacheTaskFailedRequest, UploadPersistentCacheTaskFinishedRequest,
    UploadPersistentCacheTaskStartedRequest,
};
use dragonfly_client_backend::BackendFactory;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{error::DownloadFromRemotePeerFailed, Error};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Result as ClientResult,
};
use dragonfly_client_storage::{metadata, Storage};
use dragonfly_client_util::id_generator::IDGenerator;
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
use tonic::{Request, Status};
use tracing::{debug, error, info, instrument, Instrument};

use super::*;

/// PersistentCacheTask represents a persistent cache task manager.
pub struct PersistentCacheTask {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// id_generator is the id generator.
    pub id_generator: Arc<IDGenerator>,

    /// storage is the local storage.
    storage: Arc<Storage>,

    /// scheduler_client is the grpc client of the scheduler.
    pub scheduler_client: Arc<SchedulerClient>,

    /// piece is the piece manager.
    pub piece: Arc<piece::Piece>,
}

/// PersistentCacheTask is the implementation of PersistentCacheTask.
impl PersistentCacheTask {
    /// new creates a new PersistentCacheTask.
    #[instrument(skip_all)]
    pub fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        scheduler_client: Arc<SchedulerClient>,
        backend_factory: Arc<BackendFactory>,
    ) -> ClientResult<Self> {
        let piece = piece::Piece::new(
            config.clone(),
            id_generator.clone(),
            storage.clone(),
            backend_factory.clone(),
        )?;
        let piece = Arc::new(piece);

        Ok(Self {
            config,
            id_generator,
            storage,
            scheduler_client,
            piece: piece.clone(),
        })
    }

    /// create_persistent creates a persistent cache task from local.
    #[instrument(skip_all)]
    pub async fn create_persistent(
        &self,
        task_id: &str,
        host_id: &str,
        peer_id: &str,
        path: &Path,
        digest: &str,
        request: UploadPersistentCacheTaskRequest,
    ) -> ClientResult<CommonPersistentCacheTask> {
        // Convert prost_wkt_types::Duration to std::time::Duration.
        let ttl = Duration::try_from(request.ttl.ok_or(Error::UnexpectedResponse)?)
            .or_err(ErrorType::ParseError)?;

        // Get the content length of the file.
        let content_length = std::fs::metadata(path)
            .map_err(|err| {
                error!("get file metadata error: {}", err);
                err
            })?
            .len();

        // Get the piece length of the file.
        let piece_length = self.piece.calculate_piece_length(
            piece::PieceLengthStrategy::OptimizeByFileLength,
            content_length,
        );

        // Notify the scheduler that the persistent cache task is started.
        self.scheduler_client
            .upload_persistent_cache_task_started(UploadPersistentCacheTaskStartedRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
                peer_id: peer_id.to_string(),
                persistent_replica_count: request.persistent_replica_count,
                digest: digest.to_string(),
                tag: request.tag.clone(),
                application: request.application.clone(),
                piece_length,
                content_length,
                piece_count: self
                    .piece
                    .calculate_piece_count(piece_length, content_length),
                ttl: request.ttl,
            })
            .await
            .map_err(|err| {
                error!("upload persistent cache task started: {}", err);
                err
            })?;

        // Create the persistent cache task.
        match self
            .storage
            .create_persistent_persistent_cache_task(
                task_id,
                ttl,
                path,
                piece_length,
                content_length,
                digest,
            )
            .await
        {
            Ok(metadata) => {
                let response = match self
                    .scheduler_client
                    .upload_persistent_cache_task_finished(
                        UploadPersistentCacheTaskFinishedRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                        },
                    )
                    .await
                {
                    Ok(response) => response,
                    Err(err) => {
                        // Notify the scheduler that the persistent cache task is failed.
                        self.scheduler_client
                            .upload_persistent_cache_task_failed(
                                UploadPersistentCacheTaskFailedRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    description: Some(err.to_string()),
                                },
                            )
                            .await
                            .map_err(|err| {
                                error!("upload persistent cache task failed: {}", err);
                                err
                            })?;

                        // Delete the persistent cache task.
                        self.storage.delete_persistent_cache_task(task_id).await;
                        return Err(err);
                    }
                };

                Ok(CommonPersistentCacheTask {
                    id: task_id.to_string(),
                    persistent_replica_count: request.persistent_replica_count,
                    current_persistent_replica_count: response.current_persistent_replica_count,
                    current_replica_count: response.current_replica_count,
                    digest: digest.to_string(),
                    tag: request.tag,
                    application: request.application,
                    piece_length: metadata.piece_length,
                    content_length: metadata.content_length,
                    piece_count: response.piece_count,
                    state: response.state,
                    ttl: request.ttl,
                    created_at: response.created_at,
                    updated_at: response.updated_at,
                })
            }
            Err(err) => {
                // Notify the scheduler that the persistent cache task is failed.
                self.scheduler_client
                    .upload_persistent_cache_task_failed(UploadPersistentCacheTaskFailedRequest {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                        peer_id: peer_id.to_string(),
                        description: Some(err.to_string()),
                    })
                    .await
                    .map_err(|err| {
                        error!("upload persistent cache task failed: {}", err);
                        err
                    })?;

                // Delete the persistent cache task.
                self.storage.delete_persistent_cache_task(task_id).await;
                Err(err)
            }
        }
    }

    /// download_started updates the metadata of the persistent cache task when the persistent cache task downloads started.
    #[instrument(skip_all)]
    pub async fn download_started(
        &self,
        task_id: &str,
        host_id: &str,
        request: DownloadPersistentCacheTaskRequest,
    ) -> ClientResult<metadata::PersistentCacheTask> {
        let response = self
            .scheduler_client
            .stat_persistent_cache_task(StatPersistentCacheTaskRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
            })
            .await?;

        // Convert prost_wkt_types::Duration to std::time::Duration.
        let ttl = Duration::try_from(response.ttl.ok_or(Error::InvalidParameter)?)
            .or_err(ErrorType::ParseError)?;

        self.storage.download_persistent_cache_task_started(
            task_id,
            ttl,
            request.persistent,
            response.piece_length,
            response.content_length,
        )
    }

    /// download_finished updates the metadata of the persistent cache task when the task downloads finished.
    #[instrument(skip_all)]
    pub fn download_finished(&self, id: &str) -> ClientResult<metadata::PersistentCacheTask> {
        self.storage.download_persistent_cache_task_finished(id)
    }

    /// download_failed updates the metadata of the persistent cache task when the task downloads failed.
    #[instrument(skip_all)]
    pub async fn download_failed(&self, id: &str) -> ClientResult<()> {
        let _ = self
            .storage
            .download_persistent_cache_task_failed(id)
            .await?;
        Ok(())
    }

    /// hard_link_or_copy hard links or copies the persistent cache task content to the destination.
    #[instrument(skip_all)]
    pub async fn hard_link_or_copy(
        &self,
        task: &metadata::PersistentCacheTask,
        to: &Path,
    ) -> ClientResult<()> {
        self.storage
            .hard_link_or_copy_persistent_cache_task(task, to)
            .await
    }

    /// download downloads a persistent cache task.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub async fn download(
        &self,
        task: &metadata::PersistentCacheTask,
        host_id: &str,
        peer_id: &str,
        request: DownloadPersistentCacheTaskRequest,
        download_progress_tx: Sender<Result<DownloadPersistentCacheTaskResponse, Status>>,
    ) -> ClientResult<()> {
        // Calculate the interested pieces to download.
        let interested_pieces =
            match self
                .piece
                .calculate_interested(task.piece_length, task.content_length, None)
            {
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
        info!(
            "interested pieces: {:?}",
            interested_pieces
                .iter()
                .map(|p| p.number)
                .collect::<Vec<u32>>()
        );

        // Construct the pieces for the download persistent cache task started response.
        let mut pieces = Vec::new();
        for interested_piece in interested_pieces.clone() {
            pieces.push(Piece {
                number: interested_piece.number,
                parent_id: interested_piece.parent_id.clone(),
                offset: interested_piece.offset,
                length: interested_piece.length,
                digest: interested_piece.digest.clone(),
                content: None,
                traffic_type: None,
                cost: interested_piece.prost_cost(),
                created_at: Some(prost_wkt_types::Timestamp::from(
                    interested_piece.created_at,
                )),
            });
        }

        // Send the download persistent cache task started request.
        download_progress_tx
            .send_timeout(
                Ok(DownloadPersistentCacheTaskResponse {
                    host_id: host_id.to_string(),
                    task_id: task.id.clone(),
                    peer_id: peer_id.to_string(),
                    response: Some(
                        download_persistent_cache_task_response::Response::DownloadPersistentCacheTaskStartedResponse(
                            dfdaemon::v2::DownloadPersistentCacheTaskStartedResponse {
                                content_length: task.content_length,
                            },
                        ),
                    ),
                }),
                REQUEST_TIMEOUT,
            )
            .await
            .map_err(|err| {
                error!("send DownloadPersistentCacheTaskStartedResponse failed: {:?}", err);
                err
            })?;

        // Download the pieces from the local peer.
        debug!("download the pieces from local peer");
        let finished_pieces = match self
            .download_partial_from_local_peer(
                task,
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
                .iter()
                .map(|p| p.number)
                .collect::<Vec<u32>>()
        );

        // Check if all pieces are downloaded.
        if interested_pieces.is_empty() {
            info!("all pieces are downloaded from local peer");
            return Ok(());
        };
        debug!("download the pieces with scheduler");

        // Download the pieces with scheduler.
        let finished_pieces = match self
            .download_partial_with_scheduler(
                task,
                host_id,
                peer_id,
                interested_pieces.clone(),
                request.clone(),
                download_progress_tx.clone(),
            )
            .await
        {
            Ok(finished_pieces) => finished_pieces,
            Err(err) => {
                error!("download with scheduler error: {:?}", err);
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
                .iter()
                .map(|p| p.number)
                .collect::<Vec<u32>>()
        );

        // Check if all pieces are downloaded.
        if !interested_pieces.is_empty() {
            error!("not all pieces are downloaded with scheduler");
            download_progress_tx
                .send_timeout(
                    Err(Status::internal(
                        "not all pieces are downloaded with scheduler",
                    )),
                    REQUEST_TIMEOUT,
                )
                .await
                .unwrap_or_else(|err| error!("send download progress error: {:?}", err));

            return Err(Error::Unknown(
                "not all pieces are downloaded with scheduler".to_string(),
            ));
        };

        info!("all pieces are downloaded with scheduler");
        Ok(())
    }

    /// download_partial_with_scheduler downloads a partial persistent cache task with scheduler.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_with_scheduler(
        &self,
        task: &metadata::PersistentCacheTask,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        request: DownloadPersistentCacheTaskRequest,
        download_progress_tx: Sender<Result<DownloadPersistentCacheTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Initialize the schedule count.
        let mut schedule_count = 0;

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Initialize stream channel.
        let (in_stream_tx, in_stream_rx) = mpsc::channel(10 * 1024);

        // Send the register peer request.
        in_stream_tx
            .send_timeout(
                AnnouncePersistentCachePeerRequest {
                    host_id: host_id.to_string(),
                    task_id: task.id.clone(),
                    peer_id: peer_id.to_string(),
                    request: Some(
                        announce_persistent_cache_peer_request::Request::RegisterPersistentCachePeerRequest(
                            RegisterPersistentCachePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task.id.clone(),
                                tag: request.tag.clone(),
                                application: request.application.clone(),
                                piece_length: task.piece_length,
                                output_path: request.output_path.clone(),
                                timeout: request.timeout,
                            },
                        ),
                    ),
                },
                REQUEST_TIMEOUT,
            )
            .await
            .map_err(|err| {
                error!("send RegisterPersistentCachePeerRequest failed: {:?}", err);
                err
            })?;
        info!("sent RegisterPersistentCachePeerRequest");

        // Initialize the stream.
        let in_stream = ReceiverStream::new(in_stream_rx);
        let request_stream = Request::new(in_stream);
        let response = self
            .scheduler_client
            .announce_persistent_cache_peer(task.id.as_str(), peer_id, request_stream)
            .await
            .map_err(|err| {
                error!("announce peer failed: {:?}", err);
                err
            })?;
        info!("announced peer has been connected");

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
                        AnnouncePersistentCachePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task.id.clone(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_persistent_cache_peer_request::Request::DownloadPersistentCachePeerFailedRequest(
                                    DownloadPersistentCachePeerFailedRequest {
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
                        error!("send DownloadPersistentCachePeerFailedRequest failed: {:?}", err)
                    });
                info!("sent DownloadPersistentCachePeerFailedRequest");

                // Wait for the latest message to be sent.
                sleep(Duration::from_millis(1)).await;
                return Ok(finished_pieces);
            }

            let response = message?.response.ok_or(Error::UnexpectedResponse)?;
            match response {
                announce_persistent_cache_peer_response::Response::EmptyPersistentCacheTaskResponse(
                    response,
                ) => {
                    // If the persistent cache task is empty, return an empty vector.
                    info!("empty persistent cache task response: {:?}", response);

                    // Send the download peer started request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePersistentCachePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task.id.clone(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_cache_peer_request::Request::DownloadPersistentCachePeerStartedRequest(
                                        DownloadPersistentCachePeerStartedRequest {},
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                        .map_err(|err| {
                            error!("send DownloadPersistentCachePeerStartedRequest failed: {:?}", err);
                            err
                        })?;
                    info!("sent DownloadPersistentCachePeerStartedRequest");

                    // Send the download peer finished request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePersistentCachePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task.id.clone(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_cache_peer_request::Request::DownloadPersistentCachePeerFinishedRequest(
                                        DownloadPersistentCachePeerFinishedRequest {
                                            piece_count: 0,
                                        },
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                        .map_err(|err| {
                            error!("send DownloadPersistentCachePeerFinishedRequest failed: {:?}", err);
                            err
                        })?;
                    info!("sent DownloadPersistentCachePeerFinishedRequest");

                    // Wait for the latest message to be sent.
                    sleep(Duration::from_millis(1)).await;
                    return Ok(Vec::new());
                }
                announce_persistent_cache_peer_response::Response::NormalPersistentCacheTaskResponse(
                    response,
                ) => {
                    // If the persistent cache task is normal, download the pieces from the remote peer.
                    info!(
                        "normal persistent cache task response: {:?}",
                        response
                            .candidate_cache_parents
                            .iter()
                            .map(|p| p.id.clone())
                            .collect::<Vec<String>>()
                    );

                    // Send the download peer started request.
                    match in_stream_tx
                        .send_timeout(
                            AnnouncePersistentCachePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task.id.clone(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_cache_peer_request::Request::DownloadPersistentCachePeerStartedRequest(
                                        DownloadPersistentCachePeerStartedRequest {},
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                    {
                        Ok(_) => info!("sent DownloadPersistentCachePeerStartedRequest"),
                        Err(err) => {
                            error!("send DownloadPersistentCachePeerStartedRequest failed: {:?}", err);
                            return Ok(finished_pieces);
                        }
                    };

                    // Remove the finished pieces from the pieces.
                    let remaining_interested_pieces = self.piece.remove_finished_from_interested(
                        finished_pieces.clone(),
                        interested_pieces.clone(),
                    );

                    // Download the pieces from the remote peer.
                    let partial_finished_pieces = match self
                        .download_partial_with_scheduler_from_remote_peer(
                            task,
                            host_id,
                            peer_id,
                            response.candidate_cache_parents.clone(),
                            remaining_interested_pieces.clone(),
                            download_progress_tx.clone(),
                            in_stream_tx.clone(),
                        )
                        .await
                    {
                        Ok(partial_finished_pieces) => {
                            info!(
                                "schedule {} finished {} pieces from remote peer",
                                schedule_count,
                                partial_finished_pieces.len()
                            );

                            partial_finished_pieces
                        }
                        Err(err) => {
                            error!("download from remote peer error: {:?}", err);
                            return Ok(finished_pieces);
                        }
                    };

                    // Merge the finished pieces.
                    finished_pieces = self.piece.merge_finished_pieces(
                        finished_pieces.clone(),
                        partial_finished_pieces.clone(),
                    );

                    // Check if all pieces are downloaded.
                    if finished_pieces.len() == interested_pieces.len() {
                        // Send the download peer finished request.
                        match in_stream_tx
                            .send_timeout(
                                AnnouncePersistentCachePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task.id.clone(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(
                                        announce_persistent_cache_peer_request::Request::DownloadPersistentCachePeerFinishedRequest(
                                            DownloadPersistentCachePeerFinishedRequest {
                                                piece_count: interested_pieces.len() as u32,
                                            },
                                        ),
                                    ),
                                },
                                REQUEST_TIMEOUT,
                            )
                            .await
                        {
                            Ok(_) => info!("sent DownloadPersistentCachePeerFinishedRequest"),
                            Err(err) => {
                                error!("send DownloadPersistentCachePeerFinishedRequest failed: {:?}", err);
                            }
                        }

                        // Wait for the latest message to be sent.
                        sleep(Duration::from_millis(1)).await;
                        return Ok(finished_pieces);
                    }

                    // If not all pieces are downloaded, send the reschedule request.
                    match in_stream_tx
                        .send_timeout(
                            AnnouncePersistentCachePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task.id.clone(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_cache_peer_request::Request::ReschedulePersistentCachePeerRequest(
                                        ReschedulePersistentCachePeerRequest {
                                            candidate_parents: response.candidate_cache_parents,
                                            description: Some(
                                                "not all pieces are downloaded from remote peer"
                                                    .to_string(),
                                            ),
                                        },
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                    {
                        Ok(_) => info!("sent ReschedulePersistentCachePeerRequest"),
                        Err(err) => {
                            error!("send ReschedulePersistentCachePeerRequest failed: {:?}", err);
                            return Ok(finished_pieces);
                        }
                    };
                }
            }
        }

        // If the stream is finished abnormally, return an error.
        error!("stream is finished abnormally");
        Ok(finished_pieces)
    }

    /// download_partial_with_scheduler_from_remote_peer downloads a partial persistent cache task with scheduler from a remote peer.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_with_scheduler_from_remote_peer(
        &self,
        task: &metadata::PersistentCacheTask,
        host_id: &str,
        peer_id: &str,
        parents: Vec<PersistentCachePeer>,
        interested_pieces: Vec<metadata::Piece>,
        download_progress_tx: Sender<Result<DownloadPersistentCacheTaskResponse, Status>>,
        in_stream_tx: Sender<AnnouncePersistentCachePeerRequest>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Initialize the piece collector.
        let piece_collector = piece_collector::PieceCollector::new(
            self.config.clone(),
            host_id,
            task.id.as_str(),
            interested_pieces.clone(),
            parents
                .clone()
                .into_iter()
                .map(|peer| piece_collector::CollectedParent {
                    id: peer.id.clone(),
                    host: peer.host.clone(),
                })
                .collect(),
        );
        let mut piece_collector_rx = piece_collector.run().await;

        // Initialize the join set.
        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(
            self.config.download.concurrent_piece_count as usize,
        ));

        // Download the pieces from the remote peers.
        while let Some(collect_piece) = piece_collector_rx.recv().await {
            async fn download_from_remote_peer(
                task_id: String,
                host_id: String,
                peer_id: String,
                number: u32,
                length: u64,
                parent: piece_collector::CollectedParent,
                piece_manager: Arc<super::piece::Piece>,
                storage: Arc<Storage>,
                semaphore: Arc<Semaphore>,
                download_progress_tx: Sender<Result<DownloadPersistentCacheTaskResponse, Status>>,
                in_stream_tx: Sender<AnnouncePersistentCachePeerRequest>,
            ) -> ClientResult<metadata::Piece> {
                // Limit the concurrent download count.
                let _permit = semaphore.acquire().await.unwrap();

                let piece_id = storage.piece_id(task_id.as_str(), number);
                info!(
                    "start to download piece {} from remote peer {:?}",
                    piece_id,
                    parent.id.clone()
                );

                let metadata = piece_manager
                    .download_from_remote_peer(
                        peer_id.as_str(),
                        host_id.as_str(),
                        task_id.as_str(),
                        number,
                        length,
                        parent.clone(),
                        false,
                    )
                    .await
                    .map_err(|err| {
                        error!(
                            "download piece {} from remote peer {:?} error: {:?}",
                            piece_id,
                            parent.id.clone(),
                            err
                        );
                        Error::DownloadFromRemotePeerFailed(DownloadFromRemotePeerFailed {
                            piece_number: number,
                            parent_id: parent.id.clone(),
                        })
                    })?;

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
                        AnnouncePersistentCachePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.clone(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_persistent_cache_peer_request::Request::DownloadPieceFinishedRequest(
                                    DownloadPieceFinishedRequest {
                                        piece: Some(piece.clone()),
                                    },
                                ),
                            ),
                        },
                        REQUEST_TIMEOUT,
                    )
                    .await
                    .map_err(|err| {
                        error!("send DownloadPieceFinishedRequest failed: {:?}", err);
                        err
                    })?;

                // Send the download progress.
                download_progress_tx
                    .send_timeout(
                        Ok(DownloadPersistentCacheTaskResponse {
                            host_id: host_id.to_string(),
                            task_id: task_id.clone(),
                            peer_id: peer_id.to_string(),
                            response: Some(
                                download_persistent_cache_task_response::Response::DownloadPieceFinishedResponse(
                                    dfdaemon::v2::DownloadPieceFinishedResponse {
                                        piece: Some(piece.clone()),
                                    },
                                ),
                            ),
                        }),
                        REQUEST_TIMEOUT,
                    )
                    .await
                    .map_err(|err| {
                        error!("send DownloadPieceFinishedResponse failed: {:?}", err);
                        err
                    })?;

                info!(
                    "finished piece {} from remote peer {:?}",
                    piece_id, metadata.parent_id
                );

                Ok(metadata)
            }

            join_set.spawn(
                download_from_remote_peer(
                    task.id.clone(),
                    host_id.to_string(),
                    peer_id.to_string(),
                    collect_piece.number,
                    collect_piece.length,
                    collect_piece.parent.clone(),
                    self.piece.clone(),
                    self.storage.clone(),
                    semaphore.clone(),
                    download_progress_tx.clone(),
                    in_stream_tx.clone(),
                )
                .in_current_span(),
            );
        }

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Wait for the pieces to be downloaded.
        while let Some(message) = join_set
            .join_next()
            .await
            .transpose()
            .or_err(ErrorType::AsyncRuntimeError)?
        {
            match message {
                Ok(metadata) => {
                    // Store the finished piece.
                    finished_pieces.push(metadata.clone());
                }
                Err(Error::DownloadFromRemotePeerFailed(err)) => {
                    error!(
                        "download piece {} from remote peer {} error: {:?}",
                        self.storage.piece_id(task.id.as_str(), err.piece_number),
                        err.parent_id,
                        err
                    );

                    // Send the download piece failed request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePersistentCachePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task.id.clone(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_cache_peer_request::Request::DownloadPieceFailedRequest(
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
                            error!("send DownloadPieceFailedRequest failed: {:?}", err)
                        });

                    continue;
                }
                Err(err) => {
                    error!("download from remote peer error: {:?}", err);
                    continue;
                }
            }
        }
        Ok(finished_pieces)
    }

    /// download_partial_from_local_peer downloads a partial persistent cache task from a local peer.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_from_local_peer(
        &self,
        task: &metadata::PersistentCacheTask,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        download_progress_tx: Sender<Result<DownloadPersistentCacheTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Download the piece from the local peer.
        for interested_piece in interested_pieces {
            let piece_id = self
                .storage
                .piece_id(task.id.as_str(), interested_piece.number);

            // Get the piece metadata from the local storage.
            let piece = match self.piece.get(piece_id.as_str()) {
                Ok(Some(piece)) => piece,
                Ok(None) => {
                    info!("piece {} not found in local storage", piece_id);
                    continue;
                }
                Err(err) => {
                    error!("get piece {} from local storage error: {:?}", piece_id, err);
                    continue;
                }
            };

            // Fake the download from the local peer.
            self.piece
                .download_from_local_peer(task.id.as_str(), piece.length);
            info!("finished piece {} from local peer", piece_id);

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
                    Ok(DownloadPersistentCacheTaskResponse {
                        host_id: host_id.to_string(),
                        task_id: task.id.clone(),
                        peer_id: peer_id.to_string(),
                        response: Some(
                            download_persistent_cache_task_response::Response::DownloadPieceFinishedResponse(
                                dfdaemon::v2::DownloadPieceFinishedResponse {
                                    piece: Some(piece.clone()),
                                },
                            ),
                        ),
                    }),
                    REQUEST_TIMEOUT,
                )
                .await
                .map_err(|err| {
                    error!("send DownloadPieceFinishedResponse failed: {:?}", err);
                    err
                })?;

            // Store the finished piece.
            finished_pieces.push(interested_piece.clone());
        }

        Ok(finished_pieces)
    }

    /// stat stats the persistent cache task from the scheduler.
    #[instrument(skip_all)]
    pub async fn stat(
        &self,
        task_id: &str,
        host_id: &str,
    ) -> ClientResult<CommonPersistentCacheTask> {
        self.scheduler_client
            .stat_persistent_cache_task(StatPersistentCacheTaskRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
            })
            .await
    }

    /// delete_persistent_cache_task deletes a persistent cache task.
    #[instrument(skip_all)]
    pub async fn delete(&self, task_id: &str) {
        self.storage.delete_persistent_cache_task(task_id).await
    }
}
