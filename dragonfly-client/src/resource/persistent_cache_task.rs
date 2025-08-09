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
use chrono::DateTime;
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
use dragonfly_client_core::{error::DownloadFromParentFailed, Error};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Result as ClientResult,
};
use dragonfly_client_storage::{metadata, Storage};
use dragonfly_client_util::id_generator::IDGenerator;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader, SeekFrom, BufWriter, AsyncWriteExt};
use tokio::sync::{
    mpsc::{self, Sender},
    Semaphore,
};
use tokio::task::JoinSet;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Status};
use tracing::{debug, error, info, instrument, warn, Instrument};

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
            piece,
        })
    }

    /// get gets a persistent cache task from local.
    #[instrument(skip_all)]
    pub fn get(&self, task_id: &str) -> ClientResult<Option<metadata::PersistentCacheTask>> {
        self.storage.get_persistent_cache_task(task_id)
    }

    /// create_persistent creates a persistent cache task from local.
    #[instrument(skip_all)]
    pub async fn create_persistent(
        &self,
        task_id: &str,
        host_id: &str,
        peer_id: &str,
        path: PathBuf,
        request: UploadPersistentCacheTaskRequest,
    ) -> ClientResult<CommonPersistentCacheTask> {
        // Convert prost_wkt_types::Duration to std::time::Duration.
        let ttl = Duration::try_from(request.ttl.ok_or(Error::UnexpectedResponse)?)
            .or_err(ErrorType::ParseError)?;

        // Get the content length of the file asynchronously.
        let content_length = tokio::fs::metadata(path.as_path())
            .await
            .inspect_err(|err| {
                error!("get file metadata error: {}", err);
            })?
            .len();

        // Get the piece length of the file.
        let piece_length = match request.piece_length {
            Some(piece_length) => self
                .piece
                .calculate_piece_length(piece::PieceLengthStrategy::FixedPieceLength(piece_length)),
            None => {
                self.piece
                    .calculate_piece_length(piece::PieceLengthStrategy::OptimizeByFileLength(
                        content_length,
                    ))
            }
        };

        // Notify the scheduler that the persistent cache task is started.
        self.scheduler_client
            .upload_persistent_cache_task_started(UploadPersistentCacheTaskStartedRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
                peer_id: peer_id.to_string(),
                persistent_replica_count: request.persistent_replica_count,
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
            .inspect_err(|err| error!("upload persistent cache task started: {}", err))?;

        // Check if the storage has enough space to store the persistent cache task.
        let has_enough_space = self.storage.has_enough_space(content_length)?;
        if !has_enough_space {
            return Err(Error::NoSpace(format!(
                "not enough space to store the persistent cache task: content_length={}",
                content_length
            )));
        }

        self.storage
            .create_persistent_cache_task_started(task_id, ttl, piece_length, content_length)
            .await?;

        info!("upload persistent cache task started");

        // Calculate the interested pieces to import.
        let interested_pieces =
            match self
                .piece
                .calculate_interested(piece_length, content_length, None)
            {
                Ok(interested_pieces) => interested_pieces,
                Err(err) => {
                    error!(
                        "calculate interested persistent cache pieces error: {:?}",
                        err
                    );
                    return Err(err);
                }
            };

        // Initialize the join set.
        let mut join_set = JoinSet::new();

        // Import the pieces from the local.
        for interested_piece in interested_pieces.clone() {
            async fn create_persistent_cache_piece(
                task_id: String,
                path: PathBuf,
                piece: metadata::Piece,
                piece_manager: Arc<piece::Piece>,
                read_buffer_size: usize,
            ) -> ClientResult<metadata::Piece> {
                let piece_id = piece_manager.persistent_cache_id(task_id.as_str(), piece.number);
                debug!("start to write persistent cache piece {}", piece_id,);

                let f = File::open(path.as_path()).await.inspect_err(|err| {
                    error!("open {:?} failed: {}", path, err);
                })?;
                let mut f_reader = BufReader::with_capacity(read_buffer_size, f);

                f_reader
                    .seek(SeekFrom::Start(piece.offset))
                    .await
                    .inspect_err(|err| {
                        error!("seek {:?} failed: {}", path, err);
                    })?;

                let metadata = piece_manager
                    .create_persistent_cache(
                        piece_id.as_str(),
                        task_id.as_str(),
                        piece.number,
                        piece.offset,
                        piece.length,
                        &mut f_reader.take(piece.length),
                    )
                    .await
                    .inspect_err(|err| {
                        error!("write {:?} failed: {}", piece_id, err);
                    })?;

                debug!("finished persistent cache piece {}", piece_id);
                Ok(metadata)
            }

            join_set.spawn(
                create_persistent_cache_piece(
                    task_id.to_string(),
                    path.clone(),
                    interested_piece,
                    self.piece.clone(),
                    self.config.storage.read_buffer_size,
                )
                .in_current_span(),
            );
        }

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Wait for the pieces to be created.
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
                Err(err) => {
                    join_set.detach_all();

                    // Delete the persistent cache task.
                    self.storage
                        .create_persistent_cache_task_failed(task_id)
                        .await;

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
                        .inspect_err(|err| {
                            error!("upload persistent cache task failed: {}", err);
                        })?;

                    return Err(err);
                }
            }
        }

        if finished_pieces.len() != interested_pieces.len() {
            // Delete the persistent cache task.
            self.storage
                .create_persistent_cache_task_failed(task_id)
                .await;

            // Notify the scheduler that the persistent cache task is failed.
            self.scheduler_client
                .upload_persistent_cache_task_failed(UploadPersistentCacheTaskFailedRequest {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    peer_id: peer_id.to_string(),
                    description: Some("not all persistent cache pieces are imported".to_string()),
                })
                .await
                .inspect_err(|err| {
                    error!("upload persistent cache task failed: {}", err);
                })?;

            return Err(Error::Unknown(
                "not all persistent cache pieces are imported".to_string(),
            ));
        }

        // Create the persistent cache task.
        match self
            .storage
            .create_persistent_cache_task_finished(task_id)
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
                        error!("upload persistent cache task finished: {}", err);

                        // Delete the persistent cache task.
                        self.storage
                            .create_persistent_cache_task_failed(task_id)
                            .await;

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
                            .inspect_err(|err| {
                                error!("upload persistent cache task failed: {}", err);
                            })?;

                        return Err(err);
                    }
                };

                info!("upload persistent cache task finished");
                Ok(CommonPersistentCacheTask {
                    id: task_id.to_string(),
                    persistent_replica_count: request.persistent_replica_count,
                    current_persistent_replica_count: response.current_persistent_replica_count,
                    current_replica_count: response.current_replica_count,
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
                error!("create persistent cache task finished: {}", err);

                // Delete the persistent cache task.
                self.storage
                    .create_persistent_cache_task_failed(task_id)
                    .await;

                // Notify the scheduler that the persistent cache task is failed.
                self.scheduler_client
                    .upload_persistent_cache_task_failed(UploadPersistentCacheTaskFailedRequest {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                        peer_id: peer_id.to_string(),
                        description: Some(err.to_string()),
                    })
                    .await
                    .inspect_err(|err| {
                        error!("upload persistent cache task failed: {}", err);
                    })?;

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

        // Convert prost_wkt_types::Timestamp to chrono::DateTime.
        let created_at = response.created_at.ok_or(Error::InvalidParameter)?;
        let created_at = DateTime::from_timestamp(created_at.seconds, created_at.nanos as u32)
            .ok_or(Error::InvalidParameter)?;

        // If the persistent cache task is not found, check if the storage has enough space to
        // store the persistent cache task.
        if let Ok(None) = self.get(task_id) {
            let has_enough_space = self.storage.has_enough_space(response.content_length)?;
            if !has_enough_space {
                return Err(Error::NoSpace(format!(
                    "not enough space to store the persistent cache task: content_length={}",
                    response.content_length
                )));
            }
        }

        let task = self
            .storage
            .download_persistent_cache_task_started(
                task_id,
                ttl,
                request.persistent,
                response.piece_length,
                response.content_length,
                created_at.naive_utc(),
            )
            .await?;
        
        // When enable encryption, copy encrypted file instead of create hard-link to source file
        if self.config.storage.encryption.enable {
            info!("Omit HARD-LINK when encryption is enabled");
            return Ok(task);
        }

        // Attempt to create a hard link from the task file to the output path.
        //
        // Behavior based on force_hard_link setting:
        // 1. force_hard_link is true:
        //    - Success: Continue processing
        //    - Failure: Return error immediately
        // 2. force_hard_link is false:
        //    - Success: Continue processing
        //    - Failure: Fall back to copying the file instead
        if let Some(output_path) = &request.output_path {
            if let Err(err) = self
                .storage
                .hard_link_persistent_cache_task(task_id, Path::new(output_path.as_str()))
                .await
            {
                if request.force_hard_link {
                    return Err(err);
                }
            }
        }

        Ok(task)
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

    /// is_same_dev_inode checks if the persistent cache task is on the same device inode as the given path.
    pub async fn is_same_dev_inode(&self, id: &str, to: &Path) -> ClientResult<bool> {
        self.storage
            .is_same_dev_inode_as_persistent_cache_task(id, to)
            .await
    }

    //// copy_task copies the persistent cache task content to the destination.
    #[instrument(skip_all)]
    pub async fn copy_task(&self, id: &str, to: &Path) -> ClientResult<()> {
        self.storage.copy_persistent_cache_task(id, to).await
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
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Calculate the interested pieces to download.
        let interested_pieces =
            match self
                .piece
                .calculate_interested(task.piece_length, task.content_length, None)
            {
                Ok(interested_pieces) => interested_pieces,
                Err(err) => {
                    error!(
                        "calculate interested persistent cache pieces error: {:?}",
                        err
                    );
                    return Err(err);
                }
            };
        debug!(
            "interested persistent cache pieces: {:?}",
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
                    task_id: task_id.to_string(),
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
            .inspect_err(|err| {
                error!("send DownloadPersistentCacheTaskStartedResponse failed: {:?}", err);
            })?;

        // Download the pieces from the local.
        debug!("download the persistent cache pieces from local");
        let finished_pieces = match self
            .download_partial_from_local(
                task,
                host_id,
                peer_id,
                request.need_piece_content,
                interested_pieces.clone(),
                download_progress_tx.clone(),
            )
            .await
        {
            Ok(finished_pieces) => finished_pieces,
            Err(err) => {
                error!("download from persistent cache local error: {:?}", err);
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
            info!("all persistent cache pieces are downloaded from local");
            return Ok(());
        };
        debug!("download the persistent cache pieces with scheduler");

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
            "interested persistent cache pieces after removing the finished piece: {:?}",
            interested_pieces
                .iter()
                .map(|p| p.number)
                .collect::<Vec<u32>>()
        );

        // Check if all pieces are downloaded.
        if !interested_pieces.is_empty() {
            error!("not all persistent cache pieces are downloaded with scheduler");
            return Err(Error::Unknown(
                "not all persistent cache pieces are downloaded with scheduler".to_string(),
            ));
        };

        info!("all persistent cache pieces are downloaded with scheduler");
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
        // Get the id of the task.
        let task_id = task.id.as_str();

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
                    task_id: task_id.to_string(),
                    peer_id: peer_id.to_string(),
                    request: Some(
                        announce_persistent_cache_peer_request::Request::RegisterPersistentCachePeerRequest(
                            RegisterPersistentCachePeerRequest {
                                persistent: request.persistent,
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
            .inspect_err(|err| {
                error!("send RegisterPersistentCachePeerRequest failed: {:?}", err);
            })?;
        info!("sent RegisterPersistentCachePeerRequest");

        // Initialize the stream.
        let in_stream = ReceiverStream::new(in_stream_rx);
        let request_stream = Request::new(in_stream);
        let response = self
            .scheduler_client
            .announce_persistent_cache_peer(task.id.as_str(), peer_id, request_stream)
            .await
            .inspect_err(|err| {
                error!("announce persistent cache peer failed: {:?}", err);
            })?;
        info!("announced persistent cache peer has been connected");

        let out_stream = response
            .into_inner()
            .timeout(self.config.scheduler.schedule_timeout);
        tokio::pin!(out_stream);

        while let Some(message) = out_stream.try_next().await.inspect_err(|err| {
            error!("receive message from scheduler failed: {:?}", err);
        })? {
            // Check if the schedule count is exceeded.
            schedule_count += 1;
            if schedule_count > self.config.scheduler.max_schedule_count {
                in_stream_tx
                    .send_timeout(
                        AnnouncePersistentCachePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
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
                in_stream_tx.closed().await;
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
                                task_id: task_id.to_string(),
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
                        .inspect_err(|err| {
                            error!("send DownloadPersistentCachePeerStartedRequest failed: {:?}", err);
                        })?;
                    info!("sent DownloadPersistentCachePeerStartedRequest");

                    // Send the download peer finished request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePersistentCachePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
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
                        .inspect_err(|err| {
                            error!("send DownloadPersistentCachePeerFinishedRequest failed: {:?}", err);
                        })?;
                    info!("sent DownloadPersistentCachePeerFinishedRequest");

                    // Wait for the latest message to be sent.
                    in_stream_tx.closed().await;
                    return Ok(Vec::new());
                }
                announce_persistent_cache_peer_response::Response::NormalPersistentCacheTaskResponse(
                    response,
                ) => {
                    // If the persistent cache task is normal, download the pieces from the parent.
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
                                task_id: task_id.to_string(),
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

                    // // Remove the finished pieces from the pieces.
                    // let remaining_interested_pieces = self.piece.remove_finished_from_interested(
                    //     finished_pieces.clone(),
                    //     interested_pieces.clone(),
                    // );

                    // TODO: The remove function useless? Cause `finished_pieces` MUST be empty here
                    let remaining_interested_pieces = interested_pieces.clone();

                    // Download the pieces from the parent.
                    let partial_finished_pieces = match self
                        .download_partial_with_scheduler_from_parent(
                            task,
                            host_id,
                            peer_id,
                            response.candidate_cache_parents.clone(),
                            request.need_piece_content,
                            remaining_interested_pieces.clone(),
                            download_progress_tx.clone(),
                            in_stream_tx.clone(),
                        )
                        .await
                    {
                        Ok(partial_finished_pieces) => {
                            info!(
                                "schedule {} finished {} persistent cache pieces from parent",
                                schedule_count,
                                partial_finished_pieces.len()
                            );

                            partial_finished_pieces
                        }
                        Err(err) => {
                            error!("download from parent error: {:?}", err);
                            Vec::new()
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
                                    task_id: task_id.to_string(),
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
                        in_stream_tx.closed().await;
                        return Ok(finished_pieces);
                    }

                    // If not all pieces are downloaded, send the reschedule request.
                    match in_stream_tx
                        .send_timeout(
                            AnnouncePersistentCachePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_cache_peer_request::Request::ReschedulePersistentCachePeerRequest(
                                        ReschedulePersistentCachePeerRequest {
                                            candidate_parents: response.candidate_cache_parents,
                                            description: Some(
                                                "not all persistent cache pieces are downloaded from parent"
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

    /// download_partial_with_scheduler_from_parent downloads a partial persistent cache task with scheduler from a parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_with_scheduler_from_parent(
        &self,
        task: &metadata::PersistentCacheTask,
        host_id: &str,
        peer_id: &str,
        parents: Vec<PersistentCachePeer>,
        need_piece_content: bool,
        interested_pieces: Vec<metadata::Piece>,
        download_progress_tx: Sender<Result<DownloadPersistentCacheTaskResponse, Status>>,
        in_stream_tx: Sender<AnnouncePersistentCachePeerRequest>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Initialize the piece collector.
        let piece_collector = piece_collector::PersistentCachePieceCollector::new(
            self.config.clone(),
            host_id,
            task_id,
            interested_pieces.clone(),
            parents
                .into_iter()
                .map(|peer| piece_collector::CollectedParent {
                    id: peer.id,
                    host: peer.host,
                })
                .collect(),
        )
        .await;
        let mut piece_collector_rx = piece_collector.run().await;

        // Initialize the interrupt. If download from parent failed with scheduler or download
        // progress, interrupt the collector and return the error.
        let interrupt = Arc::new(AtomicBool::new(false));

        // Initialize the finished pieces.
        let finished_pieces = Arc::new(Mutex::new(Vec::new()));

        // Initialize the join set.
        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(
            self.config.download.concurrent_piece_count as usize,
        ));

        // Download the pieces from the parents.
        while let Some(collect_piece) = piece_collector_rx.recv().await {
            if interrupt.load(Ordering::SeqCst) {
                // If the interrupt is true, break the collector loop.
                debug!("interrupt the piece collector");
                drop(piece_collector_rx);
                break;
            }

            async fn download_from_parent(
                task_id: String,
                host_id: String,
                peer_id: String,
                number: u32,
                length: u64,
                need_piece_content: bool,
                parent: piece_collector::CollectedParent,
                piece_manager: Arc<super::piece::Piece>,
                semaphore: Arc<Semaphore>,
                download_progress_tx: Sender<Result<DownloadPersistentCacheTaskResponse, Status>>,
                in_stream_tx: Sender<AnnouncePersistentCachePeerRequest>,
                interrupt: Arc<AtomicBool>,
                finished_pieces: Arc<Mutex<Vec<metadata::Piece>>>,
            ) -> ClientResult<metadata::Piece> {
                // Limit the concurrent download count.
                let _permit = semaphore.acquire().await.unwrap();

                let piece_id = piece_manager.persistent_cache_id(task_id.as_str(), number);
                info!(
                    "start to download persistent cache piece {} from parent {:?}",
                    piece_id,
                    parent.id.clone()
                );

                let metadata = piece_manager
                    .download_persistent_cache_from_parent(
                        piece_id.as_str(),
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
                            "download persistent cache piece {} from parent {:?} error: {:?}",
                            piece_id,
                            parent.id.clone(),
                            err
                        );
                        Error::DownloadFromParentFailed(DownloadFromParentFailed {
                            piece_number: number,
                            parent_id: parent.id.clone(),
                        })
                    })?;

                // Construct the piece.
                let mut piece = Piece {
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

                // If need_piece_content is true, read the piece content from the local.
                if need_piece_content {
                    let mut reader = piece_manager
                        .download_persistent_cache_from_local_into_async_read(
                            piece_id.as_str(),
                            task_id.as_str(),
                            metadata.length,
                            None,
                            true,
                            false,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("read persistent cache piece {} failed: {:?}", piece_id, err);
                            interrupt.store(true, Ordering::SeqCst);
                        })?;

                    let mut content = vec![0; metadata.length as usize];
                    reader.read_exact(&mut content).await.inspect_err(|err| {
                        error!("read persistent cache piece {} failed: {:?}", piece_id, err);
                        interrupt.store(true, Ordering::SeqCst);
                    })?;

                    piece.content = Some(content);
                }

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
                    .unwrap_or_else(|err| {
                        error!(
                            "send DownloadPieceFinishedRequest for piece {} failed: {:?}",
                            piece_id, err
                        );
                        interrupt.store(true, Ordering::SeqCst);
                    });

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
                    .unwrap_or_else(|err| {
                        error!(
                            "send DownloadPieceFinishedResponse for piece {} failed: {:?}",
                            piece_id, err
                        );
                        interrupt.store(true, Ordering::SeqCst);
                    });

                info!(
                    "finished persistent cache piece {} from parent {:?}",
                    piece_id, metadata.parent_id
                );

                let mut finished_pieces = finished_pieces.lock().unwrap();
                finished_pieces.push(metadata.clone());

                Ok(metadata)
            }

            join_set.spawn(
                download_from_parent(
                    task.id.clone(),
                    host_id.to_string(),
                    peer_id.to_string(),
                    collect_piece.number,
                    collect_piece.length,
                    need_piece_content,
                    collect_piece.parent.clone(),
                    self.piece.clone(),
                    semaphore.clone(),
                    download_progress_tx.clone(),
                    in_stream_tx.clone(),
                    interrupt.clone(),
                    finished_pieces.clone(),
                )
                .in_current_span(),
            );
        }

        // Wait for the pieces to be downloaded.
        while let Some(message) = join_set
            .join_next()
            .await
            .transpose()
            .or_err(ErrorType::AsyncRuntimeError)?
        {
            match message {
                Ok(_) => {}
                Err(Error::DownloadFromParentFailed(err)) => {
                    join_set.detach_all();

                    // Send the download piece failed request.
                    let (piece_number, parent_id) = (err.piece_number, err.parent_id.clone());
                    in_stream_tx
                        .send_timeout(
                            AnnouncePersistentCachePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task.id.clone(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_cache_peer_request::Request::DownloadPieceFailedRequest(
                                        DownloadPieceFailedRequest {
                                            piece_number: Some(piece_number),
                                            parent_id,
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

                    return Err(Error::DownloadFromParentFailed(err));
                }
                Err(Error::SendTimeout) => {
                    join_set.detach_all();

                    // If the send timeout with scheduler or download progress, return the error
                    // and interrupt the collector.
                    return Err(Error::SendTimeout);
                }
                Err(err) => {
                    join_set.detach_all();
                    error!("download from parent error: {:?}", err);
                    return Err(err);
                }
            }
        }

        let finished_pieces = finished_pieces.lock().unwrap().clone();
        Ok(finished_pieces)
    }

    /// download_partial_from_local downloads a partial persistent cache task from a local.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_from_local(
        &self,
        task: &metadata::PersistentCacheTask,
        host_id: &str,
        peer_id: &str,
        need_piece_content: bool,
        interested_pieces: Vec<metadata::Piece>,
        download_progress_tx: Sender<Result<DownloadPersistentCacheTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Download the piece from the local.
        for interested_piece in interested_pieces {
            let piece_id = self
                .piece
                .persistent_cache_id(task_id, interested_piece.number);

            // Get the piece metadata from the local storage.
            let piece = match self.piece.get_persistent_cache(piece_id.as_str()) {
                Ok(Some(piece)) => piece,
                Ok(None) => {
                    info!(
                        "persistent cache piece {} not found in local storage",
                        piece_id
                    );
                    continue;
                }
                Err(err) => {
                    error!(
                        "get persistent cache piece {} from local storage error: {:?}",
                        piece_id, err
                    );
                    continue;
                }
            };

            // Fake the download from the local.
            self.piece
                .download_persistent_cache_from_local(task.id.as_str(), piece.length);
            info!("finished persistent cache piece {} from local", piece_id);

            // Construct the piece.
            let mut piece = Piece {
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

            // If need_piece_content is true, read the piece content from the local.
            if need_piece_content {
                let mut reader = self
                    .piece
                    .download_persistent_cache_from_local_into_async_read(
                        piece_id.as_str(),
                        task_id,
                        piece.length,
                        None,
                        true,
                        false,
                    )
                    .await
                    .inspect_err(|err| {
                        error!("read persistent cache piece {} failed: {:?}", piece_id, err);
                    })?;

                let mut content = vec![0; piece.length as usize];
                reader.read_exact(&mut content).await.inspect_err(|err| {
                    error!("read persistent cache piece {} failed: {:?}", piece_id, err);
                })?;

                piece.content = Some(content);
            }

            // Send the download progress.
            download_progress_tx
                .send_timeout(
                    Ok(DownloadPersistentCacheTaskResponse {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
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
                .unwrap_or_else(|err| {
                    error!(
                        "send DownloadPieceFinishedResponse for piece {} failed: {:?}",
                        piece_id, err
                    );
                });

            // Store the finished piece.
            finished_pieces.push(interested_piece.clone());
        }

        Ok(finished_pieces)
    }

    /// persist persists the persistent cache task.
    pub fn persist(&self, task_id: &str) -> ClientResult<metadata::PersistentCacheTask> {
        self.storage.persist_persistent_cache_task(task_id)
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
    
    /// whether the encrytion is enabled
    pub fn encrypted(&self) -> bool {
        self.config.storage.encryption.enable
    }

    /// export encryption file to path
    pub async fn export_encryption_file(&self, task_id: &str, to: &Path) -> ClientResult<()> {
        let task = self.storage.get_persistent_cache_task(task_id)?
            .ok_or_else(|| Error::TaskNotFound(task_id.to_string()))?;

        if !task.is_finished() {
            // TODO errors
            return Err(Error::Unknown(format!("Task {} is not finished", task_id)));
        }

        // get all pieces
        let mut pieces = self.storage.get_persistent_cache_pieces(task_id)?;
        // TODO: need sort?
        pieces.sort_by_key(|p| p.number);

        if pieces.is_empty() {
            return Err(Error::Unknown(format!("No pieces found for task {}", task_id)));
        }

        info!("Exporting encrypted file for task {} with {} pieces to {:?}", 
              task_id, pieces.len(), to);

        let output_file = tokio::fs::File::create(to).await
            // .or_err(ErrorType::StorageError)?;
            .map_err(|e| Error::Unknown(format!("Failed to create output file: {}", e)))?;
        let mut writer = BufWriter::new(output_file);

        // process every piece
        for piece in pieces {
            if !piece.is_finished() {
                // warn!("Piece {} is not finished, skipping", piece.number);
                // continue;
                panic!("Piece {} is not finished", piece.number);
            }

            debug!("Processing piece {} (offset: {}, length: {})", 
                  piece.number, piece.offset, piece.length);

            // read and decrypt piece
            let piece_id = self.piece.persistent_cache_id(task_id, piece.number);
            let mut reader = self.storage.upload_persistent_cache_piece(
                piece_id.as_str(),
                task_id,
                None, // read whole piece
            ).await?;

            // write decrypted content to file
            let bytes_copied = tokio::io::copy(&mut reader, &mut writer).await
                // .or_err(ErrorType::StorageError)?;
                .map_err(|e| Error::Unknown(format!("Failed to copy piece {}: {}", piece.number, e)))?;

            if bytes_copied != piece.length {
                return Err(Error::Unknown(format!(
                    "Piece {} length mismatch: expected {}, got {}", 
                    piece.number, piece.length, bytes_copied
                )));
            }

            debug!("Successfully processed piece {} ({} bytes)", piece.number, bytes_copied);
        }

        writer.flush().await
            // .or_err(ErrorType::StorageError)?;
            .map_err(|e| Error::Unknown(format!("Failed to flush output file: {}", e)))?;

        info!("Successfully exported encrypted file for task {} to {:?}", task_id, to);
        Ok(())
    }
}