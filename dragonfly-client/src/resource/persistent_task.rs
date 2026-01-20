/*
 *     Copyright 2025 The Dragonfly Authors
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
use crate::resource::parent_selector::PersistentParentSelector;
use chrono::DateTime;
use dragonfly_api::common::v2::{
    Hdfs, ObjectStorage, PersistentPeer, PersistentTask as CommonPersistentTask, Piece, TrafficType,
};
use dragonfly_api::dfdaemon::{
    self,
    v2::{
        download_persistent_task_response, DownloadPersistentTaskRequest,
        DownloadPersistentTaskResponse, UploadPersistentTaskRequest,
    },
};
use dragonfly_api::errordetails::v2::{Backend, Unknown};
use dragonfly_api::scheduler::v2::{
    announce_persistent_peer_request, announce_persistent_peer_response,
    download_piece_back_to_source_failed_request, AnnouncePersistentPeerRequest,
    DownloadPersistentPeerBackToSourceFailedRequest,
    DownloadPersistentPeerBackToSourceFinishedRequest,
    DownloadPersistentPeerBackToSourceStartedRequest, DownloadPersistentPeerFailedRequest,
    DownloadPersistentPeerFinishedRequest, DownloadPersistentPeerStartedRequest,
    DownloadPieceBackToSourceFailedRequest, DownloadPieceBackToSourceFinishedRequest,
    DownloadPieceFailedRequest, DownloadPieceFinishedRequest, RegisterPersistentPeerRequest,
    ReschedulePersistentPeerRequest, StatPersistentTaskRequest, UploadPersistentTaskFailedRequest,
    UploadPersistentTaskFinishedRequest, UploadPersistentTaskStartedRequest,
};
use dragonfly_client_backend::{
    BackendFactory, ExistsRequest, PutRequest, StatRequest, StatResponse,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    error::{BackendError, DownloadFromParentFailed, ErrorType, OrErr},
    Error, Result as ClientResult,
};
use dragonfly_client_metric::{
    collect_backend_request_failure_metrics, collect_backend_request_finished_metrics,
    collect_backend_request_started_metrics,
};
use dragonfly_client_storage::{metadata, Storage};
use dragonfly_client_util::{http::headermap_to_hashmap, id_generator::IDGenerator, shutdown};
use leaky_bucket::RateLimiter;
use reqwest::header::HeaderMap;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader, SeekFrom};
use tokio::sync::{
    mpsc::{self, Sender},
    Mutex, Semaphore,
};
use tokio::task::JoinSet;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Status};
use tracing::{debug, error, info, instrument, warn, Instrument};

use super::*;

/// PersistentTask represents a persistent task manager.
pub struct PersistentTask {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// id_generator is the id generator.
    pub id_generator: Arc<IDGenerator>,

    /// storage is the local storage.
    storage: Arc<Storage>,

    /// backend_factory is the backend factory.
    backend_factory: Arc<BackendFactory>,

    /// scheduler_client is the grpc client of the scheduler.
    pub scheduler_client: Arc<SchedulerClient>,

    /// piece is the piece manager.
    pub piece: Arc<piece::Piece>,

    /// parent_selector is the parent selector.
    pub parent_selector: Arc<PersistentParentSelector>,
}

/// PersistentTask is the implementation of PersistentTask.
impl PersistentTask {
    /// Creates a new PersistentTask.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        scheduler_client: Arc<SchedulerClient>,
        backend_factory: Arc<BackendFactory>,
        download_rate_limiter: Arc<RateLimiter>,
        upload_rate_limiter: Arc<RateLimiter>,
        prefetch_rate_limiter: Arc<RateLimiter>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> ClientResult<Self> {
        Ok(Self {
            config: config.clone(),
            id_generator: id_generator.clone(),
            storage: storage.clone(),
            backend_factory: backend_factory.clone(),
            scheduler_client,
            piece: Arc::new(piece::Piece::new(
                config.clone(),
                storage.clone(),
                backend_factory.clone(),
                download_rate_limiter,
                upload_rate_limiter,
                prefetch_rate_limiter,
            )?),
            parent_selector: Arc::new(PersistentParentSelector::new(
                config.clone(),
                id_generator.clone(),
                shutdown.clone(),
                shutdown_complete_tx.clone(),
            )),
        })
    }

    /// Gets a persistent task from local.
    #[instrument(skip_all)]
    pub fn get(&self, task_id: &str) -> ClientResult<Option<metadata::PersistentTask>> {
        self.storage.get_persistent_task(task_id)
    }

    /// Creates a persistent task from local.
    #[instrument(skip_all)]
    pub async fn upload(
        &self,
        task_id: &str,
        host_id: &str,
        peer_id: &str,
        path: PathBuf,
        request: UploadPersistentTaskRequest,
    ) -> ClientResult<CommonPersistentTask> {
        let ttl = match request.ttl {
            Some(ttl) => Duration::try_from(ttl).or_err(ErrorType::ParseError)?,
            None => self.config.gc.policy.persistent_cache_task_ttl,
        };

        // Get the content length of the file asynchronously.
        let content_length = tokio::fs::metadata(path.as_path())
            .await
            .inspect_err(|err| {
                error!("get file metadata error: {}", err);
            })?
            .len();

        // Get the piece length of the file.
        let piece_length =
            self.piece
                .calculate_piece_length(piece::PieceLengthStrategy::OptimizeByFileLength(
                    content_length,
                ));

        // Notify the scheduler that the persistent task is started.
        self.scheduler_client
            .upload_persistent_task_started(UploadPersistentTaskStartedRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
                peer_id: peer_id.to_string(),
                url: request.url.clone(),
                object_storage: request.object_storage.clone(),
                persistent_replica_count: request.persistent_replica_count,
                content_length,
                piece_count: self
                    .piece
                    .calculate_piece_count(piece_length, content_length),
                ttl: Some(prost_wkt_types::Duration::try_from(ttl).or_err(ErrorType::ParseError)?),
            })
            .await
            .inspect_err(|err| error!("upload persistent task started: {}", err))?;

        // Check if the storage has enough space to store the persistent task.
        let has_enough_space = self.storage.has_enough_space(content_length)?;
        if !has_enough_space {
            return Err(Error::NoSpace(format!(
                "not enough space to store the persistent task: content_length={}",
                content_length
            )));
        }

        self.storage
            .create_persistent_task_started(task_id, ttl, piece_length, content_length)
            .await?;

        info!("upload persistent task started");
        match self
            .upload_content(
                task_id,
                &request.url,
                piece_length,
                content_length,
                path,
                request.object_storage,
            )
            .await
        {
            Ok(_) => {
                // Create the persistent task.
                match self.storage.create_persistent_task_finished(task_id).await {
                    Ok(metadata) => {
                        let response = match self
                            .scheduler_client
                            .upload_persistent_task_finished(UploadPersistentTaskFinishedRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                            })
                            .await
                        {
                            Ok(response) => response,
                            Err(err) => {
                                error!("upload persistent task failed: {}", err);

                                // Delete the persistent task.
                                self.storage.create_persistent_task_failed(task_id).await;

                                // Notify the scheduler that the persistent task is failed.
                                self.scheduler_client
                                    .upload_persistent_task_failed(
                                        UploadPersistentTaskFailedRequest {
                                            host_id: host_id.to_string(),
                                            task_id: task_id.to_string(),
                                            peer_id: peer_id.to_string(),
                                            description: Some(err.to_string()),
                                        },
                                    )
                                    .await
                                    .inspect_err(|err| {
                                        error!("upload persistent task failed: {}", err);
                                    })?;

                                return Err(err);
                            }
                        };

                        info!("upload persistent task finished");
                        Ok(CommonPersistentTask {
                            id: task_id.to_string(),
                            persistent_replica_count: request.persistent_replica_count,
                            current_persistent_replica_count: response
                                .current_persistent_replica_count,
                            current_replica_count: response.current_replica_count,
                            content_length: metadata.content_length,
                            piece_count: response.piece_count,
                            state: response.state,
                            ttl: Some(
                                prost_wkt_types::Duration::try_from(ttl)
                                    .or_err(ErrorType::ParseError)?,
                            ),
                            created_at: response.created_at,
                            updated_at: response.updated_at,
                        })
                    }
                    Err(err) => {
                        error!("upload persistent task failed: {}", err);

                        // Delete the persistent task.
                        self.storage.create_persistent_task_failed(task_id).await;

                        // Notify the scheduler that the persistent task is failed.
                        self.scheduler_client
                            .upload_persistent_task_failed(UploadPersistentTaskFailedRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                description: Some(err.to_string()),
                            })
                            .await
                            .inspect_err(|err| {
                                error!("upload persistent task failed: {}", err);
                            })?;

                        Err(err)
                    }
                }
            }
            Err(err) => {
                error!("upload persistent task failed: {}", err);

                // Delete the persistent task.
                self.storage.create_persistent_task_failed(task_id).await;

                // Notify the scheduler that the persistent task is failed.
                self.scheduler_client
                    .upload_persistent_task_failed(UploadPersistentTaskFailedRequest {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                        peer_id: peer_id.to_string(),
                        description: Some(err.to_string()),
                    })
                    .await
                    .inspect_err(|err| {
                        error!("upload persistent task failed: {}", err);
                    })?;

                return Err(err);
            }
        }
    }

    /// Orchestrates uploading content for a task by first persisting the local file
    /// into the local piece-based storage, then uploading the same file to the
    /// configured source storage backend.
    #[instrument(skip_all)]
    async fn upload_content(
        &self,
        task_id: &str,
        url: &str,
        piece_length: u64,
        content_length: u64,
        path: PathBuf,
        object_storage: Option<ObjectStorage>,
    ) -> ClientResult<()> {
        tokio::try_join!(
            self.upload_content_to_local(task_id, piece_length, content_length, path.clone()),
            self.upload_content_to_source(task_id, url, path, object_storage)
        )?;

        Ok(())
    }

    /// Persists a local file into the piece-based local storage for a task by
    /// computing the required pieces, then either registering them via a hard link
    /// when possible or, if linking fails, concurrently reading the file by piece
    /// and writing each piece into persistent storage, ensuring all targeted pieces
    /// are successfully imported.
    #[instrument(skip_all)]
    async fn upload_content_to_local(
        &self,
        task_id: &str,
        piece_length: u64,
        content_length: u64,
        path: PathBuf,
    ) -> ClientResult<()> {
        // Record the start time.
        let start_time = Instant::now();

        // Hard link creation failed. Fall back to writing pieces to content storage.
        let interested_pieces =
            match self
                .piece
                .calculate_interested(piece_length, content_length, None)
            {
                Ok(interested_pieces) => interested_pieces,
                Err(err) => {
                    error!("calculate interested persistent pieces error: {:?}", err);
                    return Err(err);
                }
            };

        // Attempt to create a hard link from the task file to the output path.
        // If successful, return immediately as no further processing is needed.
        if self
            .storage
            .hard_link_to_persistent_task(&path, task_id)
            .await
            .is_ok()
        {
            // Initialize the join set.
            let mut join_set = JoinSet::new();

            // Import the pieces from the local.
            for interested_piece in interested_pieces.clone() {
                async fn register_persistent_piece(
                    task_id: String,
                    piece: metadata::Piece,
                    piece_manager: Arc<piece::Piece>,
                ) -> ClientResult<metadata::Piece> {
                    let piece_id = piece_manager.persistent_id(task_id.as_str(), piece.number);
                    debug!("start to register persistent piece {}", piece_id,);

                    let metadata = piece_manager
                        .register_persistent(
                            piece_id.as_str(),
                            piece.number,
                            piece.offset,
                            piece.length,
                        )
                        .inspect_err(|err| {
                            error!("write {:?} failed: {}", piece_id, err);
                        })?;

                    debug!("finished persistent piece {}", piece_id);
                    Ok(metadata)
                }

                join_set.spawn(
                    register_persistent_piece(
                        task_id.to_string(),
                        interested_piece,
                        self.piece.clone(),
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
                        join_set.shutdown().await;
                        return Err(err);
                    }
                }
            }

            if finished_pieces.len() != interested_pieces.len() {
                return Err(Error::Unknown(
                    "not all persistent pieces are imported".to_string(),
                ));
            }

            info!(
                "upload persistent task to local, cost: {:?}, size: {} bytes",
                start_time.elapsed(),
                content_length
            );
            return Ok(());
        }

        // Create and fallocate the persistent task in storage.
        self.storage
            .create_persistent_task(task_id, content_length)
            .await?;

        // Initialize the join set.
        let mut join_set = JoinSet::new();

        // Import the pieces from the local.
        for interested_piece in interested_pieces.clone() {
            async fn create_persistent_piece(
                task_id: String,
                path: PathBuf,
                piece: metadata::Piece,
                piece_manager: Arc<piece::Piece>,
                read_buffer_size: usize,
            ) -> ClientResult<metadata::Piece> {
                let piece_id = piece_manager.persistent_id(task_id.as_str(), piece.number);
                debug!("start to write persistent piece {}", piece_id,);

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
                    .create_persistent(
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

                debug!("finished persistent piece {}", piece_id);
                Ok(metadata)
            }

            join_set.spawn(
                create_persistent_piece(
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
                    join_set.shutdown().await;
                    return Err(err);
                }
            }
        }

        if finished_pieces.len() != interested_pieces.len() {
            return Err(Error::Unknown(
                "not all persistent pieces are imported".to_string(),
            ));
        }

        info!(
            "upload persistent task to local, cost: {:?}, size: {} bytes",
            start_time.elapsed(),
            content_length
        );
        Ok(())
    }

    /// Uploads a locally persisted task file to the remote source storage backend
    /// using a PUT request created from the backend factory, while recording metrics
    /// for request start, failure, and completion, and converting non‑successful
    /// backend responses into structured backend errors.
    #[instrument(skip_all)]
    async fn upload_content_to_source(
        &self,
        task_id: &str,
        url: &str,
        path: PathBuf,
        object_storage: Option<ObjectStorage>,
    ) -> ClientResult<()> {
        // Record the start time.
        let start_time = Instant::now();

        let backend = self.backend_factory.build(url)?;

        // Collect the backend request started metrics.
        collect_backend_request_started_metrics(
            backend.scheme().as_str(),
            http::Method::PUT.as_str(),
        );
        let response = backend
            .put(PutRequest {
                task_id: task_id.to_string(),
                url: url.to_string(),
                path,
                http_header: None,
                timeout: self.config.backend.put_timeout,
                client_cert: None,
                object_storage,
                hdfs: None,
            })
            .await
            .inspect_err(|err| {
                error!("upload persistent task to source failed: {}", err);
            })?;

        if !response.success {
            error!(
                "upload persistent task to source failed: {:?}",
                response.error_message
            );

            // Collect the backend request failure metrics.
            collect_backend_request_failure_metrics(
                backend.scheme().as_str(),
                http::Method::PUT.as_str(),
            );

            return Err(Error::BackendError(Box::new(BackendError {
                message: response.error_message.unwrap_or_default(),
                status_code: response.http_status_code,
                header: response.http_header,
            })));
        }

        // Collect the backend request finished metrics.
        collect_backend_request_finished_metrics(
            backend.scheme().as_str(),
            http::Method::PUT.as_str(),
            start_time.elapsed(),
        );

        info!(
            "upload persistent task to source, cost: {:?}, size: {:?} bytes",
            start_time.elapsed(),
            response.content_length
        );
        Ok(())
    }

    /// Updates the metadata of the persistent task when the persistent task downloads started.
    #[instrument(skip_all)]
    pub async fn download_started(
        &self,
        task_id: &str,
        host_id: &str,
        content_length: u64,
        request: DownloadPersistentTaskRequest,
    ) -> ClientResult<metadata::PersistentTask> {
        let (ttl, created_at) = match self
            .scheduler_client
            .stat_persistent_task(StatPersistentTaskRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
            })
            .await
        {
            Ok(response) => {
                // Convert prost_wkt_types::Duration to std::time::Duration.
                let ttl = response
                    .ttl
                    .ok_or(Error::InvalidParameter)
                    .inspect_err(|_err| {
                        error!("persistent task ttl is missing");
                    })?;

                let ttl = Duration::try_from(ttl).or_err(ErrorType::ParseError)?;

                // Convert prost_wkt_types::Timestamp to chrono::DateTime.
                let created_at = response
                    .created_at
                    .ok_or(Error::InvalidParameter)
                    .inspect_err(|_err| {
                        error!("persistent task created_at is missing");
                    })?;

                let created_at =
                    DateTime::from_timestamp(created_at.seconds, created_at.nanos as u32)
                        .ok_or(Error::InvalidParameter)
                        .inspect_err(|_err| {
                            error!("invalid created_at: {}", created_at);
                        })?;

                (ttl, created_at)
            }
            Err(Error::TonicStatus(status)) if status.code() == tonic::Code::NotFound => {
                info!(
                    "persistent task {} not found in scheduler, proceed to download",
                    task_id
                );

                (
                    self.config.gc.policy.persistent_cache_task_ttl,
                    chrono::Utc::now(),
                )
            }
            Err(err) => {
                error!("stat persistent task failed: {}", err);
                return Err(err);
            }
        };

        // If the persistent task is not found, check if the storage has enough space to
        // store the persistent task.
        if let Ok(None) = self.get(task_id) {
            let has_enough_space = self.storage.has_enough_space(content_length)?;
            if !has_enough_space {
                return Err(Error::NoSpace(format!(
                    "not enough space to store the persistent task: content_length={}",
                    content_length
                )));
            }
        }

        let piece_length =
            self.piece
                .calculate_piece_length(piece::PieceLengthStrategy::OptimizeByFileLength(
                    content_length,
                ));

        let task = self
            .storage
            .download_persistent_task_started(
                task_id,
                ttl,
                request.persistent,
                piece_length,
                content_length,
                created_at.naive_utc(),
            )
            .await?;

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
                .hard_link_persistent_task(task_id, Path::new(output_path.as_str()))
                .await
            {
                if request.force_hard_link {
                    return Err(err);
                }
            }
        }

        Ok(task)
    }

    /// download_started_for_replication updates the metadata of the persistent task when the
    /// persistent task downloads started for replication.
    #[instrument(skip_all)]
    pub async fn download_started_for_replication(
        &self,
        task_id: &str,
        content_length: u64,
        ttl: Duration,
        created_at: DateTime<chrono::Utc>,
        request: DownloadPersistentTaskRequest,
    ) -> ClientResult<metadata::PersistentTask> {
        // If the persistent task is not found, check if the storage has enough space to
        // store the persistent task.
        if let Ok(None) = self.get(task_id) {
            let has_enough_space = self.storage.has_enough_space(content_length)?;
            if !has_enough_space {
                return Err(Error::NoSpace(format!(
                    "not enough space to store the persistent task: content_length={}",
                    content_length
                )));
            }
        }

        let piece_length =
            self.piece
                .calculate_piece_length(piece::PieceLengthStrategy::OptimizeByFileLength(
                    content_length,
                ));

        let task = self
            .storage
            .download_persistent_task_started(
                task_id,
                ttl,
                request.persistent,
                piece_length,
                content_length,
                created_at.naive_utc(),
            )
            .await?;

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
                .hard_link_persistent_task(task_id, Path::new(output_path.as_str()))
                .await
            {
                if request.force_hard_link {
                    return Err(err);
                }
            }
        }

        Ok(task)
    }

    /// Updates the metadata of the persistent task when the task downloads finished.
    #[instrument(skip_all)]
    pub fn download_finished(&self, id: &str) -> ClientResult<metadata::PersistentTask> {
        self.storage.download_persistent_task_finished(id)
    }

    /// Updates the metadata of the persistent task when the task downloads failed.
    #[instrument(skip_all)]
    pub async fn download_failed(&self, id: &str) -> ClientResult<()> {
        let _ = self.storage.download_persistent_task_failed(id).await?;
        Ok(())
    }

    /// Checks if the persistent task is on the same device inode as the given path.
    pub async fn is_same_dev_inode(&self, id: &str, to: &Path) -> ClientResult<bool> {
        self.storage
            .is_same_dev_inode_as_persistent_task(id, to)
            .await
    }

    //// copy_task copies the persistent task content to the destination.
    #[instrument(skip_all)]
    pub async fn copy_task(&self, id: &str, to: &Path) -> ClientResult<()> {
        self.storage.copy_persistent_task(id, to).await
    }

    /// download_for_replication downloads a persistent task for replication.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub async fn download_for_replication(
        &self,
        task: &metadata::PersistentTask,
        host_id: &str,
        peer_id: &str,
        request: DownloadPersistentTaskRequest,
        download_progress_tx: Sender<Result<DownloadPersistentTaskResponse, Status>>,
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
                    error!("calculate interested pieces error: {:?}", err);
                    return Err(err);
                }
            };
        debug!(
            "interested pieces: {:?}",
            interested_pieces
                .iter()
                .map(|p| p.number)
                .collect::<Vec<u32>>()
        );

        // Construct the pieces for the download task started response.
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

        // Send the download task started request.
        download_progress_tx
            .send_timeout(
                Ok(DownloadPersistentTaskResponse {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    peer_id: peer_id.to_string(),
                    response: Some(
                        download_persistent_task_response::Response::DownloadPersistentTaskStartedResponse(
                            dfdaemon::v2::DownloadPersistentTaskStartedResponse {
                                content_length: task.content_length,
                            },
                        ),
                    ),
                }),
                REQUEST_TIMEOUT,
            )
            .await
            .inspect_err(|err| {
                error!("send DownloadPersistentTaskStartedResponse failed: {:?}", err);
            })?;

        // Download the pieces from the local.
        debug!("download the pieces from local");
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
                error!("download from local error: {:?}", err);
                return Err(err);
            }
        };

        // Remove the finished pieces from the pieces.
        let interested_pieces = self
            .piece
            .remove_finished_from_interested(finished_pieces, interested_pieces);
        info!(
            "interested pieces after downloading from local: {:?}",
            interested_pieces
                .iter()
                .map(|p| p.number)
                .collect::<Vec<u32>>()
        );

        // Check if all pieces are downloaded.
        if interested_pieces.is_empty() {
            info!("all pieces are downloaded from local");
            return Ok(());
        };
        debug!("download the pieces with scheduler");

        // Download the pieces with scheduler.
        let finished_pieces = match self
            .download_partial_for_replication_with_scheduler(
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
            "interested pieces after downloading from scheduler: {:?}",
            interested_pieces
                .iter()
                .map(|p| p.number)
                .collect::<Vec<u32>>()
        );

        // Check if all pieces are downloaded.
        if !interested_pieces.is_empty() {
            error!("not all persistent pieces are downloaded with scheduler");
            return Err(Error::Unknown(
                "not all persistent pieces are downloaded with scheduler".to_string(),
            ));
        };

        info!("all persistent pieces are downloaded with scheduler");
        Ok(())
    }

    /// download_partial_for_replication_with_scheduler downloads a partial task for replication
    /// with scheduler.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_for_replication_with_scheduler(
        &self,
        task: &metadata::PersistentTask,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        request: DownloadPersistentTaskRequest,
        download_progress_tx: Sender<Result<DownloadPersistentTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Initialize the schedule count.
        let mut schedule_count = 0;

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Initialize stream channel.
        let (in_stream_tx, in_stream_rx) = mpsc::channel(4);

        // Send the register peer request.
        in_stream_tx
            .send_timeout(
                AnnouncePersistentPeerRequest {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    peer_id: peer_id.to_string(),
                    request: Some(
                        announce_persistent_peer_request::Request::RegisterPersistentPeerRequest(
                            RegisterPersistentPeerRequest {
                                url: request.url.clone(),
                                object_storage: request.object_storage.clone(),
                                persistent: request.persistent,
                                output_path: request.output_path.clone(),
                                concurrent_piece_count: Some(
                                    self.config.download.concurrent_piece_count,
                                ),
                                piece_count: task.piece_count(),
                                need_back_to_source: false,
                            },
                        ),
                    ),
                },
                REQUEST_TIMEOUT,
            )
            .await
            .inspect_err(|err| {
                error!("send RegisterPersistentPeerRequest failed: {:?}", err);
            })?;
        debug!("sent RegisterPersistentPeerRequest");

        // Initialize the stream.
        let in_stream = ReceiverStream::new(in_stream_rx);
        let request_stream = Request::new(in_stream);
        let response = self
            .scheduler_client
            .announce_persistent_peer(task_id, peer_id, request_stream)
            .await
            .inspect_err(|err| {
                error!("announce persistent peer failed: {:?}", err);
            })?;
        debug!("announced persistent peer has been connected");

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
                        AnnouncePersistentPeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_persistent_peer_request::Request::DownloadPersistentPeerFailedRequest(
                                    DownloadPersistentPeerFailedRequest {
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
                        error!("send DownloadPersistentPeerFailedRequest failed: {:?}", err)
                    });
                debug!("sent DownloadPersistentPeerFailedRequest");

                // Wait for the latest message to be sent.
                in_stream_tx.closed().await;
                return Ok(finished_pieces);
            }

            let response = message?.response.ok_or(Error::UnexpectedResponse)?;
            match response {
                announce_persistent_peer_response::Response::EmptyPersistentTaskResponse(
                    response,
                ) => {
                    // If the task is empty, return an empty vector.
                    info!("empty persistent task response: {:?}", response);

                    // Send the download peer started request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePersistentPeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_peer_request::Request::DownloadPersistentPeerFinishedRequest(
                                        DownloadPersistentPeerFinishedRequest {},
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("send DownloadPersistentPeerFinishedRequest failed: {:?}", err);
                        })?;
                    debug!("sent DownloadPersistentPeerFinishedRequest");

                    // Send the download peer finished request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePersistentPeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_peer_request::Request::DownloadPersistentPeerStartedRequest(
                                        DownloadPersistentPeerStartedRequest {},
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("send DownloadPersistentPeerStartedRequest failed: {:?}", err);
                        })?;
                    debug!("sent DownloadPersistentPeerStartedRequest");

                    // Wait for the latest message to be sent.
                    in_stream_tx.closed().await;
                    return Ok(Vec::new());
                }
                announce_persistent_peer_response::Response::NormalPersistentTaskResponse(
                    response,
                ) => {
                    // If the task is normal, download the pieces from the parent.
                    info!(
                        "normal persistent task response: {:?}",
                        response
                            .candidate_parents
                            .iter()
                            .map(|p| p.id.clone())
                            .collect::<Vec<String>>()
                    );

                    // Send the download peer started request.
                    match in_stream_tx
                        .send_timeout(
                            AnnouncePersistentPeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_peer_request::Request::DownloadPersistentPeerStartedRequest(
                                        DownloadPersistentPeerStartedRequest {},
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                    {
                        Ok(_) => debug!("sent DownloadPersistentPeerStartedRequest"),
                        Err(err) => {
                            error!("send DownloadPersistentPeerStartedRequest failed: {:?}", err);
                            return Ok(finished_pieces);
                        }
                    };

                    // Remove the finished pieces from the pieces.
                    let remaining_interested_pieces = self.piece.remove_finished_from_interested(
                        finished_pieces.clone(),
                        interested_pieces.clone(),
                    );

                    // Download the pieces from the parent.
                    let partial_finished_pieces = match self
                        .download_partial_with_scheduler_from_parent(
                            task,
                            host_id,
                            peer_id,
                            response.candidate_parents.clone(),
                            remaining_interested_pieces.clone(),
                            request.need_piece_content,
                            download_progress_tx.clone(),
                            in_stream_tx.clone(),
                        )
                        .await
                    {
                        Ok(partial_finished_pieces) => {
                            debug!(
                                "schedule {} finished {} pieces from parent",
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
                                AnnouncePersistentPeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(
                                        announce_persistent_peer_request::Request::DownloadPersistentPeerFinishedRequest(
                                            DownloadPersistentPeerFinishedRequest {},
                                        ),
                                    ),
                                },
                                REQUEST_TIMEOUT,
                            )
                            .await
                        {
                            Ok(_) => debug!("sent DownloadPersistentPeerFinishedRequest"),
                            Err(err) => {
                                error!("send DownloadPersistentPeerFinishedRequest failed: {:?}", err);
                            }
                        }

                        // Wait for the latest message to be sent.
                        in_stream_tx.closed().await;
                        return Ok(finished_pieces);
                    }

                    // If not all pieces are downloaded, send the reschedule request.
                    match in_stream_tx
                        .send_timeout(
                            AnnouncePersistentPeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_peer_request::Request::ReschedulePersistentPeerRequest(
                                        ReschedulePersistentPeerRequest {
                                            candidate_parents: response.candidate_parents,
                                            description: Some(
                                                "not all pieces are downloaded from parent"
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
                        Ok(_) => debug!("sent ReschedulePersistentPeerRequest"),
                        Err(err) => {
                            error!("send ReschedulePersistentPeerRequest failed: {:?}", err);
                            return Ok(finished_pieces);
                        }
                    };
                }
                announce_persistent_peer_response::Response::NeedBackToSourceResponse(
                    _response,
                ) => {
                    // If the task need back to source, download the pieces from the source.
                    info!("replication cannot download from source");
                    match in_stream_tx
                        .send_timeout(AnnouncePersistentPeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_persistent_peer_request::Request::DownloadPersistentPeerBackToSourceFailedRequest(
                                    DownloadPersistentPeerBackToSourceFailedRequest {
                                        description: Some("replication cannot download from source".to_string()),
                                    },
                                ),
                            ),
                        }, REQUEST_TIMEOUT)
                    .await {
                        Ok(_) => debug!("sent DownloadPersistentPeerBackToSourceFailedRequest"),
                        Err(err) => {
                            error!("send DownloadPersistentPeerBackToSourceFailedRequest failed: {:?}", err);
                        }
                    }

                    // Wait for the latest message to be sent.
                    in_stream_tx.closed().await;
                    return Ok(finished_pieces);
                }
            }
        }

        // If the stream is finished abnormally, return an error.
        error!("stream is finished abnormally");
        Ok(finished_pieces)
    }

    /// download downloads a persistent task.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub async fn download(
        &self,
        task: &metadata::PersistentTask,
        host_id: &str,
        peer_id: &str,
        request: DownloadPersistentTaskRequest,
        download_progress_tx: Sender<Result<DownloadPersistentTaskResponse, Status>>,
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
                    error!("calculate interested pieces error: {:?}", err);
                    return Err(err);
                }
            };
        debug!(
            "interested pieces: {:?}",
            interested_pieces
                .iter()
                .map(|p| p.number)
                .collect::<Vec<u32>>()
        );

        // Construct the pieces for the download task started response.
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

        // Send the download task started request.
        download_progress_tx
            .send_timeout(
                Ok(DownloadPersistentTaskResponse {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    peer_id: peer_id.to_string(),
                    response: Some(
                        download_persistent_task_response::Response::DownloadPersistentTaskStartedResponse(
                            dfdaemon::v2::DownloadPersistentTaskStartedResponse {
                                content_length: task.content_length,
                            },
                        ),
                    ),
                }),
                REQUEST_TIMEOUT,
            )
            .await
            .inspect_err(|err| {
                error!("send DownloadPersistentTaskStartedResponse failed: {:?}", err);
            })?;

        // Download the pieces from the local.
        debug!("download the pieces from local");
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
                error!("download from local error: {:?}", err);
                return Err(err);
            }
        };

        // Remove the finished pieces from the pieces.
        let interested_pieces = self
            .piece
            .remove_finished_from_interested(finished_pieces, interested_pieces);
        info!(
            "interested pieces after downloading from local: {:?}",
            interested_pieces
                .iter()
                .map(|p| p.number)
                .collect::<Vec<u32>>()
        );

        // Check if all pieces are downloaded.
        if interested_pieces.is_empty() {
            info!("all pieces are downloaded from local");
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

                // Download the pieces from the source.
                if let Err(err) = self
                    .download_partial_from_source(
                        task,
                        host_id,
                        peer_id,
                        interested_pieces.clone(),
                        request.clone(),
                        download_progress_tx.clone(),
                    )
                    .await
                {
                    error!("download from source error: {:?}", err);
                    return Err(err);
                }

                info!("all pieces are downloaded from source");
                return Ok(());
            }
        };

        // Remove the finished pieces from the pieces.
        let interested_pieces = self
            .piece
            .remove_finished_from_interested(finished_pieces, interested_pieces);
        info!(
            "interested pieces after downloading from scheduler: {:?}",
            interested_pieces
                .iter()
                .map(|p| p.number)
                .collect::<Vec<u32>>()
        );

        // Check if all pieces are downloaded.
        if interested_pieces.is_empty() {
            info!("all pieces are downloaded with scheduler");
            return Ok(());
        };

        // Download the pieces from the source.
        if let Err(err) = self
            .download_partial_from_source(
                task,
                host_id,
                peer_id,
                interested_pieces.clone(),
                request.clone(),
                download_progress_tx.clone(),
            )
            .await
        {
            error!("download from source error: {:?}", err);
            return Err(err);
        }

        info!("all pieces are downloaded from source");
        Ok(())
    }

    /// download_partial_with_scheduler downloads a partial task with scheduler.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_with_scheduler(
        &self,
        task: &metadata::PersistentTask,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        request: DownloadPersistentTaskRequest,
        download_progress_tx: Sender<Result<DownloadPersistentTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Initialize the schedule count.
        let mut schedule_count = 0;

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Initialize stream channel.
        let (in_stream_tx, in_stream_rx) = mpsc::channel(4);

        // Send the register peer request.
        in_stream_tx
            .send_timeout(
                AnnouncePersistentPeerRequest {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    peer_id: peer_id.to_string(),
                    request: Some(
                        announce_persistent_peer_request::Request::RegisterPersistentPeerRequest(
                            RegisterPersistentPeerRequest {
                                url: request.url.clone(),
                                object_storage: request.object_storage.clone(),
                                persistent: request.persistent,
                                output_path: request.output_path.clone(),
                                concurrent_piece_count: Some(
                                    self.config.download.concurrent_piece_count,
                                ),
                                piece_count: task.piece_count(),
                                need_back_to_source: false,
                            },
                        ),
                    ),
                },
                REQUEST_TIMEOUT,
            )
            .await
            .inspect_err(|err| {
                error!("send RegisterPersistentPeerRequest failed: {:?}", err);
            })?;
        debug!("sent RegisterPersistentPeerRequest");

        // Initialize the stream.
        let in_stream = ReceiverStream::new(in_stream_rx);
        let request_stream = Request::new(in_stream);
        let response = self
            .scheduler_client
            .announce_persistent_peer(task_id, peer_id, request_stream)
            .await
            .inspect_err(|err| {
                error!("announce persistent peer failed: {:?}", err);
            })?;
        debug!("announced persistent peer has been connected");

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
                        AnnouncePersistentPeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_persistent_peer_request::Request::DownloadPersistentPeerFailedRequest(
                                    DownloadPersistentPeerFailedRequest {
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
                        error!("send DownloadPersistentPeerFailedRequest failed: {:?}", err)
                    });
                debug!("sent DownloadPersistentPeerFailedRequest");

                // Wait for the latest message to be sent.
                in_stream_tx.closed().await;
                return Ok(finished_pieces);
            }

            let response = message?.response.ok_or(Error::UnexpectedResponse)?;
            match response {
                announce_persistent_peer_response::Response::EmptyPersistentTaskResponse(
                    response,
                ) => {
                    // If the task is empty, return an empty vector.
                    info!("empty persistent task response: {:?}", response);

                    // Send the download peer started request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePersistentPeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_peer_request::Request::DownloadPersistentPeerFinishedRequest(
                                        DownloadPersistentPeerFinishedRequest {},
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("send DownloadPersistentPeerFinishedRequest failed: {:?}", err);
                        })?;
                    debug!("sent DownloadPersistentPeerFinishedRequest");

                    // Send the download peer finished request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePersistentPeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_peer_request::Request::DownloadPersistentPeerStartedRequest(
                                        DownloadPersistentPeerStartedRequest {},
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("send DownloadPersistentPeerStartedRequest failed: {:?}", err);
                        })?;
                    debug!("sent DownloadPersistentPeerStartedRequest");

                    // Wait for the latest message to be sent.
                    in_stream_tx.closed().await;
                    return Ok(Vec::new());
                }
                announce_persistent_peer_response::Response::NormalPersistentTaskResponse(
                    response,
                ) => {
                    // If the task is normal, download the pieces from the parent.
                    info!(
                        "normal persistent task response: {:?}",
                        response
                            .candidate_parents
                            .iter()
                            .map(|p| p.id.clone())
                            .collect::<Vec<String>>()
                    );

                    // Send the download peer started request.
                    match in_stream_tx
                        .send_timeout(
                            AnnouncePersistentPeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_peer_request::Request::DownloadPersistentPeerStartedRequest(
                                        DownloadPersistentPeerStartedRequest {},
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                    {
                        Ok(_) => debug!("sent DownloadPersistentPeerStartedRequest"),
                        Err(err) => {
                            error!("send DownloadPersistentPeerStartedRequest failed: {:?}", err);
                            return Ok(finished_pieces);
                        }
                    };

                    // Remove the finished pieces from the pieces.
                    let remaining_interested_pieces = self.piece.remove_finished_from_interested(
                        finished_pieces.clone(),
                        interested_pieces.clone(),
                    );

                    // Download the pieces from the parent.
                    let partial_finished_pieces = match self
                        .download_partial_with_scheduler_from_parent(
                            task,
                            host_id,
                            peer_id,
                            response.candidate_parents.clone(),
                            remaining_interested_pieces.clone(),
                            request.need_piece_content,
                            download_progress_tx.clone(),
                            in_stream_tx.clone(),
                        )
                        .await
                    {
                        Ok(partial_finished_pieces) => {
                            debug!(
                                "schedule {} finished {} pieces from parent",
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
                                AnnouncePersistentPeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(
                                        announce_persistent_peer_request::Request::DownloadPersistentPeerFinishedRequest(
                                            DownloadPersistentPeerFinishedRequest {},
                                        ),
                                    ),
                                },
                                REQUEST_TIMEOUT,
                            )
                            .await
                        {
                            Ok(_) => debug!("sent DownloadPersistentPeerFinishedRequest"),
                            Err(err) => {
                                error!("send DownloadPersistentPeerFinishedRequest failed: {:?}", err);
                            }
                        }

                        // Wait for the latest message to be sent.
                        in_stream_tx.closed().await;
                        return Ok(finished_pieces);
                    }

                    // If not all pieces are downloaded, send the reschedule request.
                    match in_stream_tx
                        .send_timeout(
                            AnnouncePersistentPeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_peer_request::Request::ReschedulePersistentPeerRequest(
                                        ReschedulePersistentPeerRequest {
                                            candidate_parents: response.candidate_parents,
                                            description: Some(
                                                "not all pieces are downloaded from parent"
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
                        Ok(_) => debug!("sent ReschedulePersistentPeerRequest"),
                        Err(err) => {
                            error!("send ReschedulePersistentPeerRequest failed: {:?}", err);
                            return Ok(finished_pieces);
                        }
                    };
                }
                announce_persistent_peer_response::Response::NeedBackToSourceResponse(response) => {
                    // If the task need back to source, download the pieces from the source.
                    info!("need back to source response: {:?}", response);

                    // Send the download peer back-to-source request.
                    match in_stream_tx
                        .send_timeout(AnnouncePersistentPeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_persistent_peer_request::Request::DownloadPersistentPeerBackToSourceStartedRequest(
                                    DownloadPersistentPeerBackToSourceStartedRequest {
                                        description: None,
                                    },
                                ),
                            ),
                        }, REQUEST_TIMEOUT)
                    .await {
                        Ok(_) => debug!("sent DownloadPersistentPeerBackToSourceStartedRequest"),
                        Err(err) => {
                            error!("send DownloadPersistentPeerBackToSourceStartedRequest failed: {:?}", err);
                            return Ok(finished_pieces);
                        }
                    };

                    // Remove the finished pieces from the pieces.
                    let remaining_interested_pieces = self.piece.remove_finished_from_interested(
                        finished_pieces.clone(),
                        interested_pieces.clone(),
                    );

                    // Download the pieces from the source.
                    let partial_finished_pieces = match self
                        .download_partial_with_scheduler_from_source(
                            task,
                            host_id,
                            peer_id,
                            remaining_interested_pieces.clone(),
                            request.clone(),
                            download_progress_tx.clone(),
                            in_stream_tx.clone(),
                        )
                        .await
                    {
                        Ok(finished_pieces) => finished_pieces,
                        Err(err) => {
                            in_stream_tx
                                .send_timeout(AnnouncePersistentPeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(
                                        announce_persistent_peer_request::Request::DownloadPersistentPeerBackToSourceFailedRequest(
                                            DownloadPersistentPeerBackToSourceFailedRequest {
                                                description: Some(err.to_string()),
                                            },
                                        ),
                                    ),
                                }, REQUEST_TIMEOUT)
                                .await
                                .unwrap_or_else(|err| {
                                    error!("send DownloadPersistentPeerBackToSourceFailedRequest failed: {:?}", err)
                                });
                            debug!("sent DownloadPersistentPeerBackToSourceFailedRequest");

                            // Wait for the latest message to be sent.
                            in_stream_tx.closed().await;
                            return Ok(finished_pieces);
                        }
                    };

                    // Merge the finished pieces.
                    finished_pieces = self.piece.merge_finished_pieces(
                        finished_pieces.clone(),
                        partial_finished_pieces.clone(),
                    );

                    if partial_finished_pieces.len() == remaining_interested_pieces.len() {
                        // Send the download peer finished request.
                        match in_stream_tx
                            .send_timeout(
                                AnnouncePersistentPeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(
                                        announce_persistent_peer_request::Request::DownloadPersistentPeerBackToSourceFinishedRequest(
                                            DownloadPersistentPeerBackToSourceFinishedRequest {},
                                        ),
                                    ),
                                },
                                REQUEST_TIMEOUT,
                            )
                            .await
                        {
                            Ok(_) => debug!("sent DownloadPersistentPeerBackToSourceFinishedRequest"),
                            Err(err) => {
                                error!("send DownloadPersistentPeerBackToSourceFinishedRequest failed: {:?}", err);
                            }
                        }

                        // Wait for the latest message to be sent.
                        in_stream_tx.closed().await;
                        return Ok(finished_pieces);
                    }

                    match in_stream_tx
                        .send_timeout(AnnouncePersistentPeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_persistent_peer_request::Request::DownloadPersistentPeerBackToSourceFailedRequest(
                                    DownloadPersistentPeerBackToSourceFailedRequest {
                                        description: Some("not all pieces are downloaded from source".to_string()),
                                    },
                                ),
                            ),
                        }, REQUEST_TIMEOUT)
                    .await {
                        Ok(_) => debug!("sent DownloadPersistentPeerBackToSourceFailedRequest"),
                        Err(err) => {
                            error!("send DownloadPersistentPeerBackToSourceFailedRequest failed: {:?}", err);
                        }
                    }

                    // Wait for the latest message to be sent.
                    in_stream_tx.closed().await;
                    return Ok(finished_pieces);
                }
            }
        }

        // If the stream is finished abnormally, return an error.
        error!("stream is finished abnormally");
        Ok(finished_pieces)
    }

    /// download_partial_with_scheduler_from_parent downloads a partial task with scheduler from a parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_with_scheduler_from_parent(
        &self,
        task: &metadata::PersistentTask,
        host_id: &str,
        peer_id: &str,
        parents: Vec<PersistentPeer>,
        interested_pieces: Vec<metadata::Piece>,
        need_piece_content: bool,
        download_progress_tx: Sender<Result<DownloadPersistentTaskResponse, Status>>,
        in_stream_tx: Sender<AnnouncePersistentPeerRequest>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Register the parents for syncing host info.
        self.parent_selector
            .register(&parents)
            .await
            .inspect_err(|err| {
                error!("register parents for syncing host info failed: {:?}", err);
            })?;

        // Clean up the parents in parent selector when the function returns.
        let parents_clone = parents.clone();
        let _guard = scopeguard::guard((), |_| {
            self.parent_selector.unregister(&parents_clone);
        });

        // Initialize the piece collector.
        let piece_collector = piece_collector::PersistentPieceCollector::new(
            self.config.clone(),
            host_id,
            task_id,
            interested_pieces.clone(),
            parents
                .into_iter()
                .map(|peer| piece_collector::CollectedParent {
                    id: peer.id,
                    host: peer.host,
                    download_ip: None,
                    download_tcp_port: None,
                    download_quic_port: None,
                })
                .collect(),
        )
        .await;
        let mut piece_collector_rx = piece_collector.run().await;

        // Initialize the interrupt. If download from parent failed with scheduler or download
        // progress, interrupt the collector and return the finished pieces.
        let interrupt = Arc::new(AtomicBool::new(false));

        // Initialize the finished pieces.
        let finished_pieces = Arc::new(Mutex::new(Vec::new()));

        // Initialize the join set.
        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(
            self.config.download.concurrent_piece_count as usize,
        ));
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
                parents: Vec<piece_collector::CollectedParent>,
                piece_manager: Arc<super::piece::Piece>,
                download_progress_tx: Sender<Result<DownloadPersistentTaskResponse, Status>>,
                in_stream_tx: Sender<AnnouncePersistentPeerRequest>,
                interrupt: Arc<AtomicBool>,
                finished_pieces: Arc<Mutex<Vec<metadata::Piece>>>,
                protocol: String,
                parent_selector: Arc<PersistentParentSelector>,
            ) -> ClientResult<metadata::Piece> {
                let piece_id = piece_manager.id(task_id.as_str(), number);
                let parent = parent_selector.select(parents);

                info!(
                    "start to download persistent piece {} from parent {:?}",
                    piece_id,
                    parent.id.clone()
                );

                let metadata = piece_manager
                    .download_persistent_from_parent(
                        piece_id.as_str(),
                        host_id.as_str(),
                        task_id.as_str(),
                        number,
                        length,
                        parent.clone(),
                    )
                    .await
                    .map_err(|err| {
                        error!(
                            "download persistent piece {} from parent {:?} error: {:?}",
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

                // If need_piece_content is true, read the piece content from the local.
                let mut response_piece = piece.clone();
                if need_piece_content {
                    let mut reader = piece_manager
                        .download_persistent_from_local_into_async_read(
                            piece_id.as_str(),
                            task_id.as_str(),
                            metadata.length,
                            None,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("read persistent piece {} failed: {:?}", piece_id, err);
                            interrupt.store(true, Ordering::SeqCst);
                        })?;

                    let mut content = vec![0; metadata.length as usize];
                    reader.read_exact(&mut content).await.inspect_err(|err| {
                        error!("read persistent piece {} failed: {:?}", piece_id, err);
                        interrupt.store(true, Ordering::SeqCst);
                    })?;

                    response_piece.content = Some(content);
                }

                // Send the download progress.
                download_progress_tx
                    .send_timeout(
                        Ok(DownloadPersistentTaskResponse {
                            host_id: host_id.to_string(),
                            task_id: task_id.clone(),
                            peer_id: peer_id.to_string(),
                            response: Some(
                                download_persistent_task_response::Response::DownloadPieceFinishedResponse(
                                    dfdaemon::v2::DownloadPieceFinishedResponse {
                                        piece: Some(response_piece),
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

                // Send the download piece finished request.
                in_stream_tx
                    .send_timeout(
                        AnnouncePersistentPeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.clone(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_persistent_peer_request::Request::DownloadPieceFinishedRequest(
                                    DownloadPieceFinishedRequest {
                                        piece: Some(piece),
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

                info!(
                    "finished persistent piece {} from parent {:?} using protocol {}",
                    piece_id, metadata.parent_id, protocol,
                );

                let mut finished_pieces = finished_pieces.lock().await;
                finished_pieces.push(metadata.clone());

                Ok(metadata)
            }

            let task_id = task_id.to_string();
            let host_id = host_id.to_string();
            let peer_id = peer_id.to_string();
            let piece_manager = self.piece.clone();
            let download_progress_tx = download_progress_tx.clone();
            let in_stream_tx = in_stream_tx.clone();
            let interrupt = interrupt.clone();
            let finished_pieces = finished_pieces.clone();
            let protocol = self.config.download.protocol.clone();
            let parent_selector = self.parent_selector.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            join_set.spawn(
                async move {
                    let _permit = permit;
                    download_from_parent(
                        task_id,
                        host_id,
                        peer_id,
                        collect_piece.number,
                        collect_piece.length,
                        need_piece_content,
                        collect_piece.parents,
                        piece_manager,
                        download_progress_tx,
                        in_stream_tx,
                        interrupt,
                        finished_pieces,
                        protocol,
                        parent_selector,
                    )
                    .await
                }
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
                    let (piece_number, parent_id) = (err.piece_number, err.parent_id);

                    // Send the download piece failed request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePersistentPeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task.id.clone(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_peer_request::Request::DownloadPieceFailedRequest(
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
                            error!(
                                "send DownloadPieceFailedRequest for piece {} failed: {:?}",
                                self.piece.id(task_id, piece_number),
                                err
                            )
                        });

                    // If the download failed from the parent, continue to download the next
                    // piece and ignore the error.
                    continue;
                }
                Err(Error::SendTimeout) => {
                    join_set.shutdown().await;

                    // If the send timeout with scheduler or download progress, return the finished pieces.
                    // It will stop the download from the parent with scheduler
                    // and download from the source directly from middle.
                    let finished_pieces = finished_pieces.lock().await.clone();
                    return Ok(finished_pieces);
                }
                Err(err) => {
                    error!("download from parent error: {:?}", err);

                    // If the unknown error occurred, continue to download the next piece and
                    // ignore the error.
                    continue;
                }
            }
        }

        let finished_pieces = finished_pieces.lock().await.clone();
        Ok(finished_pieces)
    }

    /// download_partial_with_scheduler_from_source downloads a partial task with scheduler from the source.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_with_scheduler_from_source(
        &self,
        task: &metadata::PersistentTask,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        request: DownloadPersistentTaskRequest,
        download_progress_tx: Sender<Result<DownloadPersistentTaskResponse, Status>>,
        in_stream_tx: Sender<AnnouncePersistentPeerRequest>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Download the piece from the local.
        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(
            self.config.download.concurrent_piece_count as usize,
        ));
        for interested_piece in interested_pieces {
            async fn download_from_source(
                task_id: String,
                host_id: String,
                peer_id: String,
                number: u32,
                url: String,
                offset: u64,
                length: u64,
                request_header: HeaderMap,
                need_piece_content: bool,
                piece_manager: Arc<piece::Piece>,
                download_progress_tx: Sender<Result<DownloadPersistentTaskResponse, Status>>,
                in_stream_tx: Sender<AnnouncePersistentPeerRequest>,
                object_storage: Option<ObjectStorage>,
                hdfs: Option<Hdfs>,
            ) -> ClientResult<metadata::Piece> {
                let piece_id = piece_manager.id(task_id.as_str(), number);
                info!("start to download piece {} from source", piece_id);

                let metadata = piece_manager
                    .download_persistent_from_source(
                        piece_id.as_str(),
                        task_id.as_str(),
                        number,
                        url.as_str(),
                        offset,
                        length,
                        request_header,
                        object_storage,
                        hdfs,
                    )
                    .await?;

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

                // If need_piece_content is true, read the piece content from the local.
                let mut response_piece = piece.clone();
                if need_piece_content {
                    let mut reader = piece_manager
                        .download_from_local_into_async_read(
                            piece_id.as_str(),
                            task_id.as_str(),
                            metadata.length,
                            None,
                            true,
                            false,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("read piece {} failed: {:?}", piece_id, err);
                        })?;

                    let mut content = vec![0; metadata.length as usize];
                    reader.read_exact(&mut content).await.inspect_err(|err| {
                        error!("read piece {} failed: {:?}", piece_id, err);
                    })?;

                    response_piece.content = Some(content);
                }

                // Send the download progress.
                download_progress_tx
                    .send_timeout(
                        Ok(DownloadPersistentTaskResponse {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            response: Some(
                                download_persistent_task_response::Response::DownloadPieceFinishedResponse(
                                    dfdaemon::v2::DownloadPieceFinishedResponse {
                                        piece: Some(response_piece),
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

                // Send the download piece finished request.
                in_stream_tx
                        .send_timeout(
                            AnnouncePersistentPeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_persistent_peer_request::Request::DownloadPieceBackToSourceFinishedRequest(
                                        DownloadPieceBackToSourceFinishedRequest {
                                            piece: Some(piece),
                                        },
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await.unwrap_or_else(|err| {
                            error!("send DownloadPieceBackToSourceFinishedRequest for piece {} failed: {:?}", piece_id, err);
                        });

                info!("finished piece {} from source", piece_id);
                Ok(metadata)
            }

            let task_id = task_id.to_string();
            let host_id = host_id.to_string();
            let peer_id = peer_id.to_string();
            let url = request.url.clone();
            let piece_manager = self.piece.clone();
            let download_progress_tx = download_progress_tx.clone();
            let in_stream_tx = in_stream_tx.clone();
            let object_storage = request.object_storage.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            join_set.spawn(
                async move {
                    let _permit = permit;
                    download_from_source(
                        task_id,
                        host_id,
                        peer_id,
                        interested_piece.number,
                        url,
                        interested_piece.offset,
                        interested_piece.length,
                        HeaderMap::new(),
                        request.need_piece_content,
                        piece_manager,
                        download_progress_tx,
                        in_stream_tx,
                        object_storage,
                        None,
                    )
                    .await
                }
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
                Ok(metadata) => {
                    // Store the finished piece.
                    finished_pieces.push(metadata.clone());
                }
                Err(Error::BackendError(err)) => {
                    join_set.shutdown().await;

                    // Send the download piece http failed request.
                    in_stream_tx.send_timeout(AnnouncePersistentPeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_persistent_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
                                            DownloadPieceBackToSourceFailedRequest{
                                                piece_number: None,
                                                response: Some(download_piece_back_to_source_failed_request::Response::Backend(
                                                        Backend{
                                                            message: err.message.clone(),
                                                            header: headermap_to_hashmap(&err.header.clone().unwrap_or_default()),
                                                            status_code: err.status_code.map(|code| code.as_u16() as i32),
                                                        }
                                                )),
                                            }
                                    )),
                                }, REQUEST_TIMEOUT)
                                .await
                                .unwrap_or_else(|err| error!("send DownloadPieceBackToSourceFailedRequest error: {:?}", err));

                    // If the backend error with source, return the error.
                    // It will stop the download from the source with scheduler
                    // and download from the source directly from beginning.
                    return Err(Error::BackendError(err));
                }
                Err(Error::SendTimeout) => {
                    join_set.shutdown().await;

                    // Send the download piece failed request.
                    in_stream_tx.send_timeout(AnnouncePersistentPeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_persistent_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
                                            DownloadPieceBackToSourceFailedRequest{
                                                piece_number: None,
                                                response: Some(download_piece_back_to_source_failed_request::Response::Unknown(
                                                        Unknown{
                                                            message: Some("send timeout".to_string()),
                                                        }
                                                )),
                                            }
                                    )),
                                }, REQUEST_TIMEOUT)
                                .await
                                .unwrap_or_else(|err| error!("send DownloadPieceBackToSourceFailedRequest error: {:?}", err));

                    // If the send timeout with scheduler or download progress, return
                    // the finished pieces. It will stop the download from the source with
                    // scheduler and download from the source directly from middle.
                    return Ok(finished_pieces);
                }
                Err(err) => {
                    join_set.shutdown().await;

                    // Send the download piece failed request.
                    in_stream_tx.send_timeout(AnnouncePersistentPeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_persistent_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
                                            DownloadPieceBackToSourceFailedRequest{
                                                piece_number: None,
                                                response: Some(download_piece_back_to_source_failed_request::Response::Unknown(
                                                        Unknown{
                                                            message: Some(err.to_string()),
                                                        }
                                                )),
                                            }
                                    )),
                                }, REQUEST_TIMEOUT)
                                .await
                                .unwrap_or_else(|err| error!("send DownloadPieceBackToSourceFailedRequest error: {:?}", err));

                    // If the unknown error, return the error.
                    // It will stop the download from the source with scheduler
                    // and download from the source directly from beginning.
                    return Err(err);
                }
            }
        }

        Ok(finished_pieces)
    }

    /// download_partial_from_local downloads a partial task from a local.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_from_local(
        &self,
        task: &metadata::PersistentTask,
        host_id: &str,
        peer_id: &str,
        need_piece_content: bool,
        interested_pieces: Vec<metadata::Piece>,
        download_progress_tx: Sender<Result<DownloadPersistentTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Download the piece from the local.
        for interested_piece in interested_pieces {
            let piece_id = self.piece.id(task_id, interested_piece.number);

            // Get the piece metadata from the local storage.
            let piece = match self.piece.get(piece_id.as_str()) {
                Ok(Some(piece)) => piece,
                Ok(None) => {
                    debug!("piece {} not found in local storage", piece_id);
                    continue;
                }
                Err(err) => {
                    error!("get piece {} from local storage error: {:?}", piece_id, err);
                    continue;
                }
            };

            if !piece.is_finished() {
                debug!("piece {} is not finished, skip it", piece_id);
                continue;
            }

            // Fake the download from the local.
            self.piece.download_persistent_from_local(piece.length);
            info!("finished piece {} from local", piece_id,);

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
                    .download_persistent_from_local_into_async_read(
                        piece_id.as_str(),
                        task_id,
                        piece.length,
                        None,
                    )
                    .await
                    .inspect_err(|err| {
                        error!("read piece {} failed: {:?}", piece_id, err);
                    })?;

                let mut content = vec![0; piece.length as usize];
                reader.read_exact(&mut content).await.inspect_err(|err| {
                    error!("read piece {} failed: {:?}", piece_id, err);
                })?;

                piece.content = Some(content);
            }

            // Send the download progress.
            download_progress_tx
                .send_timeout(
                    Ok(DownloadPersistentTaskResponse {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                        peer_id: peer_id.to_string(),
                        response: Some(
                            download_persistent_task_response::Response::DownloadPieceFinishedResponse(
                                dfdaemon::v2::DownloadPieceFinishedResponse {
                                    piece: Some(piece),
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

    /// download_partial_from_source downloads a partial task from the source.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_from_source(
        &self,
        task: &metadata::PersistentTask,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        request: DownloadPersistentTaskRequest,
        download_progress_tx: Sender<Result<DownloadPersistentTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Download the pieces.
        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(
            self.config.download.concurrent_piece_count as usize,
        ));
        for interested_piece in interested_pieces.clone() {
            async fn download_from_source(
                task_id: String,
                host_id: String,
                peer_id: String,
                number: u32,
                url: String,
                offset: u64,
                length: u64,
                request_header: HeaderMap,
                need_piece_content: bool,
                piece_manager: Arc<piece::Piece>,
                download_progress_tx: Sender<Result<DownloadPersistentTaskResponse, Status>>,
                object_storage: Option<ObjectStorage>,
                hdfs: Option<Hdfs>,
            ) -> ClientResult<metadata::Piece> {
                let piece_id = piece_manager.id(task_id.as_str(), number);
                info!("start to download piece {} from source", piece_id);

                let metadata = piece_manager
                    .download_persistent_from_source(
                        piece_id.as_str(),
                        task_id.as_str(),
                        number,
                        url.as_str(),
                        offset,
                        length,
                        request_header,
                        object_storage,
                        hdfs,
                    )
                    .await?;

                // Construct the piece.
                let mut piece = Piece {
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

                // If need_piece_content is true, read the piece content from the local.
                if need_piece_content {
                    let mut reader = piece_manager
                        .download_persistent_from_local_into_async_read(
                            piece_id.as_str(),
                            task_id.as_str(),
                            piece.length,
                            None,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("read piece {} failed: {:?}", piece_id, err);
                        })?;

                    let mut content = vec![0; piece.length as usize];
                    reader.read_exact(&mut content).await.inspect_err(|err| {
                        error!("read piece {} failed: {:?}", piece_id, err);
                    })?;

                    piece.content = Some(content);
                }

                // Send the download progress.
                download_progress_tx
                    .send_timeout(
                        Ok(DownloadPersistentTaskResponse {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            response: Some(
                                download_persistent_task_response::Response::DownloadPieceFinishedResponse(
                                    dfdaemon::v2::DownloadPieceFinishedResponse {
                                        piece: Some(piece),
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

                info!("finished piece {} from source", piece_id);
                Ok(metadata)
            }

            let task_id = task_id.to_string();
            let host_id = host_id.to_string();
            let peer_id = peer_id.to_string();
            let url = request.url.clone();
            let piece_manager = self.piece.clone();
            let download_progress_tx = download_progress_tx.clone();
            let object_storage = request.object_storage.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            join_set.spawn(
                async move {
                    let _permit = permit;
                    download_from_source(
                        task_id,
                        host_id,
                        peer_id,
                        interested_piece.number,
                        url,
                        interested_piece.offset,
                        interested_piece.length,
                        HeaderMap::new(),
                        request.need_piece_content,
                        piece_manager,
                        download_progress_tx,
                        object_storage,
                        None,
                    )
                    .await
                }
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
                Ok(metadata) => {
                    // Store the finished piece.
                    finished_pieces.push(metadata.clone());
                }
                Err(err) => {
                    join_set.shutdown().await;

                    // If the download failed from the source, return the error.
                    // It will stop the download from the source.
                    return Err(err);
                }
            }
        }

        // Check if all pieces are downloaded.
        if finished_pieces.len() != interested_pieces.len() {
            // If not all pieces are downloaded, return an error.
            return Err(Error::Unknown(
                "not all pieces are downloaded from source".to_string(),
            ));
        }

        return Ok(finished_pieces);
    }

    /// persist persists the persistent task.
    pub fn persist(&self, task_id: &str) -> ClientResult<metadata::PersistentTask> {
        self.storage.persist_persistent_task(task_id)
    }

    /// exists checks if the persistent task exists in the source.
    #[instrument(skip_all)]
    pub async fn exists(
        &self,
        task_id: &str,
        url: &str,
        object_storage: Option<ObjectStorage>,
    ) -> ClientResult<bool> {
        self.backend_factory
            .build(url)?
            .exists(ExistsRequest {
                task_id: task_id.to_string(),
                url: url.to_string(),
                http_header: None,
                timeout: self.config.backend.put_timeout,
                client_cert: None,
                object_storage,
                hdfs: None,
            })
            .await
            .inspect_err(|err| {
                error!("exists persistent task in source failed: {}", err);
            })
    }

    /// stat_source stats the persistent task in the source.
    #[instrument(skip_all)]
    pub async fn stat_source(
        &self,
        task_id: &str,
        url: &str,
        object_storage: Option<ObjectStorage>,
    ) -> ClientResult<StatResponse> {
        self.backend_factory
            .build(url)?
            .stat(StatRequest {
                task_id: task_id.to_string(),
                url: url.to_string(),
                http_header: None,
                timeout: self.config.backend.put_timeout,
                client_cert: None,
                object_storage,
                hdfs: None,
            })
            .await
            .inspect_err(|err| {
                error!("stat persistent task in source failed: {}", err);
            })
    }

    /// stat stats the persistent task from the scheduler.
    #[instrument(skip_all)]
    pub async fn stat(&self, task_id: &str, host_id: &str) -> ClientResult<CommonPersistentTask> {
        self.scheduler_client
            .stat_persistent_task(StatPersistentTaskRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
            })
            .await
    }

    /// delete_persistent_task deletes a persistent task.
    #[instrument(skip_all)]
    pub async fn delete(&self, task_id: &str) {
        self.storage.delete_persistent_task(task_id).await
    }
}
