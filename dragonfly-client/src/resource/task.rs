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

use crate::dynconfig::Dynconfig;
use crate::grpc::block_list::{BlockList, DownloadBlockListCheckParams};
use crate::grpc::{
    prefetch_task, scheduler::SchedulerClient, DOWNLOAD_STREAM_BUFFER_SIZE, REQUEST_TIMEOUT,
};
use crate::resource::parent_selector::ParentSelector;
use dragonfly_api::common::v2::{
    Download, Hdfs, HuggingFace, ModelScope, ObjectStorage, Peer, Piece, Task as CommonTask,
    TaskType, TrafficType,
};
use dragonfly_api::dfdaemon::{
    self,
    v2::{
        download_task_response, DownloadTaskRequest, DownloadTaskResponse, ListLocalTasksResponse,
        StatLocalTaskResponse,
    },
};
use dragonfly_api::errordetails::v2::{Backend, Unknown};
use dragonfly_api::scheduler::v2::{
    announce_peer_request, announce_peer_response, download_piece_back_to_source_failed_request,
    AnnouncePeerRequest, DeleteTaskRequest, DownloadPeerBackToSourceFailedRequest,
    DownloadPeerBackToSourceFinishedRequest, DownloadPeerBackToSourceStartedRequest,
    DownloadPeerFailedRequest, DownloadPeerFinishedRequest, DownloadPeerStartedRequest,
    DownloadPieceBackToSourceFailedRequest, DownloadPieceBackToSourceFinishedRequest,
    DownloadPieceFailedRequest, DownloadPieceFinishedRequest, RegisterPeerRequest,
    ReschedulePeerRequest, StatTaskRequest,
};
use dragonfly_client_backend::{BackendFactory, StatRequest};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    error::{BackendError, DownloadFromParentFailed, ErrorType, OrErr},
    Error, Result as ClientResult,
};
use dragonfly_client_metric::{
    collect_backend_request_failure_metrics, collect_backend_request_finished_metrics,
    collect_backend_request_started_metrics, collect_download_task_blocked_metrics,
    collect_download_task_failure_metrics, collect_download_task_finished_metrics,
    collect_download_task_started_metrics,
};
use dragonfly_client_storage::{metadata, Storage};
use dragonfly_client_util::{
    digest::{is_blob_url, verify_file_digest, Digest},
    http::{get_range, hashmap_to_headermap, headermap_to_hashmap},
    id_generator::{IDGenerator, TaskIDParameter},
    shutdown,
    types::redacted::RedactedDownload,
};
use leaky_bucket::RateLimiter;
use reqwest::header::HeaderMap;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tokio::sync::{
    mpsc::{self, Sender},
    Mutex, Semaphore,
};
use tokio::task::JoinSet;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Code, Request, Status};
use tracing::{debug, error, info, instrument, warn, Instrument, Span};

use super::*;

/// Task represents a task manager.
pub struct Task {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// block_list is the block list for download tasks.
    block_list: BlockList,

    /// id_generator is the id generator.
    pub id_generator: Arc<IDGenerator>,

    /// storage is the local storage.
    storage: Arc<Storage>,

    /// scheduler_client is the grpc client of the scheduler.
    pub scheduler_client: Arc<SchedulerClient>,

    /// backend_factory is the backend factory.
    pub backend_factory: Arc<BackendFactory>,

    /// piece is the piece manager.
    pub piece: Arc<piece::Piece>,

    /// parent_selector is the parent selector.
    pub parent_selector: Arc<ParentSelector>,
}

/// Task implements the task manager.
impl Task {
    /// new returns a new Task.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<Config>,
        dynconfig: Arc<Dynconfig>,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        scheduler_client: Arc<SchedulerClient>,
        backend_factory: Arc<BackendFactory>,
        download_bandwidth_limiter: Arc<RateLimiter>,
        prefetch_bandwidth_limiter: Arc<RateLimiter>,
        back_to_source_bandwidth_limiter: Arc<RateLimiter>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> ClientResult<Self> {
        Ok(Self {
            config: config.clone(),
            block_list: BlockList::new(config.clone(), dynconfig),
            id_generator: id_generator.clone(),
            storage: storage.clone(),
            scheduler_client: scheduler_client.clone(),
            backend_factory: backend_factory.clone(),
            piece: Arc::new(piece::Piece::new(
                config.clone(),
                storage.clone(),
                backend_factory.clone(),
                download_bandwidth_limiter,
                prefetch_bandwidth_limiter,
                back_to_source_bandwidth_limiter,
            )?),
            parent_selector: Arc::new(ParentSelector::new(
                config.clone(),
                id_generator.clone(),
                shutdown.clone(),
                shutdown_complete_tx.clone(),
            )),
        })
    }

    /// download_task downloads the task from remote peers or the source and
    /// returns the stream receiver of the download progress. It is shared by
    /// the dfdaemon download gRPC service and the proxy server, so that the
    /// proxy server can download tasks by direct method calls without the
    /// gRPC overhead.
    #[instrument(
        skip_all,
        fields(host_id, task_id, peer_id, url, remote_ip, content_length)
    )]
    pub async fn download_task(
        self: Arc<Self>,
        request: DownloadTaskRequest,
    ) -> Result<mpsc::Receiver<Result<DownloadTaskResponse, Status>>, Status> {
        // Record the start time.
        let start_time = Instant::now();

        // Check whether the download is empty.
        let mut download = request.download.ok_or_else(|| {
            error!("missing download");
            Status::invalid_argument("missing download")
        })?;

        // Check whether rejected by blocklist policy.
        let check_params = DownloadBlockListCheckParams {
            application: download.application.clone(),
            url: Some(download.url.clone()),
            tag: download.tag.clone(),
            priority: Some(download.priority),
        };
        if self
            .block_list
            .is_task_download_blocked(&check_params)
            .await
        {
            warn!("download rejected by blocklist policy: {:?}", check_params);
            collect_download_task_blocked_metrics(TaskType::Standard as i32);
            return Err(Status::permission_denied(
                "download rejected by blocklist policy",
            ));
        }

        // If concurrent_piece_count is not set in the request, use the default value in the config.
        download.concurrent_piece_count = Some(self.config.download.concurrent_piece_count);

        // Generate the task id.
        let task_id = self
            .id_generator
            .task_id(
                if let Some(content) = download.content_for_calculating_task_id.clone() {
                    TaskIDParameter::Content(content)
                } else if download.enable_task_id_based_blob_digest && is_blob_url(&download.url) {
                    TaskIDParameter::BlobDigestBased(download.url.clone())
                } else {
                    let revision = download
                        .hugging_face
                        .as_ref()
                        .map(|hf| hf.revision.clone())
                        .or_else(|| download.model_scope.as_ref().map(|ms| ms.revision.clone()));

                    TaskIDParameter::URLBased {
                        url: download.url.clone(),
                        piece_length: download.piece_length,
                        tag: download.tag.clone(),
                        application: download.application.clone(),
                        filtered_query_params: download.filtered_query_params.clone(),
                        revision,
                    }
                },
            )
            .map_err(|e| {
                error!("generate task id: {}", e);
                Status::invalid_argument(e.to_string())
            })?;

        // Generate the host id.
        let host_id = self.id_generator.host_id();

        // Generate the peer id.
        let peer_id = self.id_generator.peer_id();

        // Span record the host id, task id and peer id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record("peer_id", peer_id.as_str());
        Span::current().record("url", download.url.clone());
        Span::current().record(
            "remote_ip",
            download.remote_ip.clone().unwrap_or_default().as_str(),
        );

        // Download task started.
        info!("download task started: {:?}", RedactedDownload(&download));
        let task = match self
            .download_started(task_id.as_str(), download.clone())
            .await
        {
            Err(Error::BackendError(err)) => {
                error!("download started failed by error: {}", err);
                self.download_failed(task_id.as_str())
                    .await
                    .unwrap_or_else(|err| error!("download task failed: {}", err));

                match serde_json::to_vec::<Backend>(&Backend {
                    message: err.message.clone(),
                    header: headermap_to_hashmap(&err.header.clone().unwrap_or_default()),
                    status_code: err.status_code.map(|code| code.as_u16() as i32),
                }) {
                    Ok(json) => {
                        return Err(Status::with_details(
                            Code::Internal,
                            err.to_string(),
                            json.into(),
                        ));
                    }
                    Err(err) => {
                        error!("serialize error: {}", err);
                        return Err(Status::internal(err.to_string()));
                    }
                }
            }
            Err(err) => {
                error!("download started failed: {}", err);
                return Err(Status::internal(err.to_string()));
            }
            Ok(task) => {
                // Collect download task started metrics.
                collect_download_task_started_metrics(
                    download.r#type,
                    download.tag.clone().unwrap_or_default().as_str(),
                    download.application.clone().unwrap_or_default().as_str(),
                    download.priority.to_string().as_str(),
                );

                task
            }
        };
        Span::current().record("content_length", task.content_length().unwrap_or_default());

        // Update the actual content length, actual piece length and actual
        // piece count of the download.
        download.actual_content_length = task.content_length();
        download.actual_piece_length = task.piece_length();
        download.actual_piece_count = task.piece_count();

        // Download's range priority is higher than the request header's range.
        // If download protocol is http, use the range of the request header.
        // If download protocol is not http, use the range of the download.
        if download.range.is_none() {
            // Convert the header.
            let request_header = match hashmap_to_headermap(&download.request_header) {
                Ok(header) => header,
                Err(err) => {
                    // Download task failed.
                    self.download_failed(task_id.as_str())
                        .await
                        .unwrap_or_else(|err| error!("download task failed: {}", err));

                    // Collect download task failure metrics.
                    collect_download_task_failure_metrics(
                        download.r#type,
                        download.tag.clone().unwrap_or_default().as_str(),
                        download.application.clone().unwrap_or_default().as_str(),
                        download.priority.to_string().as_str(),
                    );

                    error!("convert header: {}", err);
                    return Err(Status::invalid_argument(err.to_string()));
                }
            };

            download.range =
                match get_range(&request_header, task.content_length().unwrap_or_default()) {
                    Ok(range) => range,
                    Err(err) => {
                        // Download task failed.
                        self.download_failed(task_id.as_str())
                            .await
                            .unwrap_or_else(|err| error!("download task failed: {}", err));

                        // Collect download task failure metrics.
                        collect_download_task_failure_metrics(
                            download.r#type,
                            download.tag.clone().unwrap_or_default().as_str(),
                            download.application.clone().unwrap_or_default().as_str(),
                            download.priority.to_string().as_str(),
                        );

                        error!("get range failed: {}", err);
                        return Err(Status::failed_precondition(err.to_string()));
                    }
                };
        }

        // Initialize stream channel.
        let download_clone = download.clone();
        let task_manager_clone = self.clone();
        let task_clone = task.clone();
        let (out_stream_tx, out_stream_rx) = mpsc::channel(DOWNLOAD_STREAM_BUFFER_SIZE);

        // Define the error handler to send the error to the stream.
        async fn handle_error(
            out_stream_tx: &Sender<Result<DownloadTaskResponse, Status>>,
            err: impl std::error::Error,
        ) {
            out_stream_tx
                .send_timeout(Err(Status::internal(err.to_string())), REQUEST_TIMEOUT)
                .await
                .unwrap_or_else(|err| error!("send download progress error: {:?}", err));
        }

        // Define the backend error handler to send the error to the stream.
        async fn handle_backend_error(
            out_stream_tx: &Sender<Result<DownloadTaskResponse, Status>>,
            err: Status,
        ) {
            out_stream_tx
                .send_timeout(Err(err), REQUEST_TIMEOUT)
                .await
                .unwrap_or_else(|err| error!("send download progress error: {:?}", err));
        }

        tokio::spawn(
            async move {
                match task_manager_clone
                    .download(
                        &task_clone,
                        host_id.as_str(),
                        peer_id.as_str(),
                        download_clone.clone(),
                        out_stream_tx.clone(),
                    )
                    .await
                {
                    Ok(_) => {
                        // Collect download task finished metrics.
                        collect_download_task_finished_metrics(
                            download_clone.r#type,
                            download_clone.tag.clone().unwrap_or_default().as_str(),
                            download_clone
                                .application
                                .clone()
                                .unwrap_or_default()
                                .as_str(),
                            download_clone.priority.to_string().as_str(),
                            task_clone.content_length().unwrap_or_default(),
                            download_clone.range,
                            start_time.elapsed(),
                        );

                        // Download task succeeded.
                        info!("download task succeeded");
                        if let Err(err) =
                            task_manager_clone.download_finished(task_clone.id.as_str())
                        {
                            error!("download task finished: {}", err);
                            handle_error(&out_stream_tx, err).await;
                            return;
                        }

                        if download_clone.range.is_none() {
                            if let Some(output_path) = &download_clone.output_path {
                                if !download_clone.force_hard_link {
                                    let output_path = Path::new(output_path.as_str());
                                    if output_path.exists() {
                                        match task_manager_clone
                                            .is_same_dev_inode(task_clone.id.as_str(), output_path)
                                            .await
                                        {
                                            Ok(true) => {}
                                            Ok(false) => {
                                                if download_clone.overwrite {
                                                    if let Err(err) = task_manager_clone
                                                        .copy_task(
                                                            task_clone.id.as_str(),
                                                            output_path,
                                                        )
                                                        .await
                                                    {
                                                        error!("copy task: {}", err);
                                                        handle_error(&out_stream_tx, err).await;
                                                        return;
                                                    };

                                                    return;
                                                }

                                                error!(
                                                    "output path {} is already exists",
                                                    output_path.display()
                                                );

                                                handle_error(
                                                    &out_stream_tx,
                                                    Status::internal(format!(
                                                        "output path {} is already exists",
                                                        output_path.display()
                                                    )),
                                                )
                                                .await;
                                                return;
                                            }
                                            Err(err) => {
                                                error!("check output path: {}", err);
                                                handle_error(&out_stream_tx, err).await;
                                                return;
                                            }
                                        }
                                    } else if let Err(err) = task_manager_clone
                                        .copy_task(task_clone.id.as_str(), output_path)
                                        .await
                                    {
                                        error!("copy task: {}", err);
                                        handle_error(&out_stream_tx, err).await;
                                        return;
                                    }
                                }

                                // Verify the file digest if it is provided.
                                if let Some(raw_digest) = &download_clone.digest {
                                    let digest = match raw_digest.parse::<Digest>() {
                                        Ok(digest) => digest,
                                        Err(err) => {
                                            error!("parse digest: {}", err);
                                            handle_error(
                                                &out_stream_tx,
                                                Status::invalid_argument(format!(
                                                    "invalid digest({raw_digest}): {err}"
                                                )),
                                            )
                                            .await;
                                            return;
                                        }
                                    };

                                    if let Err(err) =
                                        verify_file_digest(digest, Path::new(output_path.as_str()))
                                    {
                                        error!("verify file digest: {}", err);
                                        handle_error(&out_stream_tx, err).await;
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Err(Error::BackendError(err)) => {
                        // Collect download task failure metrics.
                        collect_download_task_failure_metrics(
                            download_clone.r#type,
                            download_clone.tag.clone().unwrap_or_default().as_str(),
                            download_clone
                                .application
                                .clone()
                                .unwrap_or_default()
                                .as_str(),
                            download_clone.priority.to_string().as_str(),
                        );

                        task_manager_clone
                            .download_failed(task_clone.id.as_str())
                            .await
                            .unwrap_or_else(|err| error!("download task failed: {}", err));

                        match serde_json::to_vec::<Backend>(&Backend {
                            message: err.message.clone(),
                            header: headermap_to_hashmap(&err.header.clone().unwrap_or_default()),
                            status_code: err.status_code.map(|code| code.as_u16() as i32),
                        }) {
                            Ok(json) => {
                                handle_backend_error(
                                    &out_stream_tx,
                                    Status::with_details(
                                        Code::Internal,
                                        err.to_string(),
                                        json.into(),
                                    ),
                                )
                                .await;
                            }
                            Err(err) => {
                                error!("serialize error: {}", err);
                                handle_error(&out_stream_tx, err).await;
                            }
                        }
                    }
                    Err(err) => {
                        error!("download failed: {}", err);

                        // Collect download task failure metrics.
                        collect_download_task_failure_metrics(
                            download_clone.r#type,
                            download_clone.tag.clone().unwrap_or_default().as_str(),
                            download_clone
                                .application
                                .clone()
                                .unwrap_or_default()
                                .as_str(),
                            download_clone.priority.to_string().as_str(),
                        );

                        // Download task failed.
                        task_manager_clone
                            .download_failed(task_clone.id.as_str())
                            .await
                            .unwrap_or_else(|err| error!("download task failed: {}", err));

                        handle_error(&out_stream_tx, err).await;
                    }
                }

                drop(out_stream_tx);
            }
            .in_current_span(),
        );

        // If prefetch flag is true, prefetch the full task.
        if download.prefetch {
            match self.prefetch_task_started(task_id.as_str()).await {
                Ok(_) => {
                    info!("prefetch task started");
                    let socket_path = self.config.download.server.socket_path.clone();
                    let task_manager_clone = self.clone();
                    tokio::spawn(
                        async move {
                            if let Err(err) = prefetch_task(
                                socket_path.clone(),
                                Request::new(DownloadTaskRequest {
                                    download: Some(download.clone()),
                                }),
                            )
                            .await
                            {
                                match task_manager_clone
                                    .prefetch_task_failed(task_id.clone().as_str())
                                    .await
                                {
                                    Ok(_) => {
                                        error!("prefetch task failed: {}", err);
                                    }
                                    Err(err) => {
                                        error!(
                                            "prefetch succeeded, but failed to update metadata: {}",
                                            err
                                        );
                                    }
                                }
                            }
                        }
                        .in_current_span(),
                    );
                }
                // If the task is already prefetched, ignore the error.
                Err(Error::InvalidState(_)) => info!("task is already prefetched"),
                Err(err) => {
                    error!("prefetch task started: {}", err);
                }
            }
        }

        Ok(out_stream_rx)
    }

    /// get gets the metadata of the task.
    #[instrument(skip_all)]
    pub fn get(&self, id: &str) -> ClientResult<Option<metadata::Task>> {
        self.storage.get_task(id)
    }

    /// download_started updates the metadata of the task when the task downloads started.
    #[instrument(skip_all)]
    pub async fn download_started(
        &self,
        id: &str,
        request: Download,
    ) -> ClientResult<metadata::Task> {
        let (task, reused) = self.storage.prepare_download_task(id)?;
        if reused {
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
                    .hard_link_task(id, Path::new(output_path.as_str()))
                    .await
                {
                    if request.force_hard_link {
                        return Err(err);
                    }
                }
            }

            return Ok(task);
        }

        // Handle the request header.
        let mut request_header =
            hashmap_to_headermap(&request.request_header).inspect_err(|err| {
                error!("convert header: {}", err);
            })?;

        // Remove the range header to prevent the server from
        // returning a 206 partial content and returning
        // a 200 full content.
        request_header.remove(reqwest::header::RANGE);

        // Head the url to get the content length.
        let backend = self.backend_factory.build(request.url.as_str())?;

        // Record the start time.
        let start_time = Instant::now();

        // Collect the backend request started metrics.
        collect_backend_request_started_metrics(
            backend.scheme().as_str(),
            http::Method::HEAD.as_str(),
        );
        let response = backend
            .stat(StatRequest {
                task_id: id.to_string(),
                url: request.url,
                http_header: Some(request_header),
                timeout: self.config.download.piece_timeout,
                client_cert: None,
                object_storage: request.object_storage,
                hdfs: request.hdfs,
                hugging_face: request.hugging_face,
                model_scope: request.model_scope,
            })
            .await
            .inspect_err(|_err| {
                // Collect the backend request failure metrics.
                collect_backend_request_failure_metrics(
                    backend.scheme().as_str(),
                    http::Method::HEAD.as_str(),
                );
            })?;

        // Check if the status code is success.
        if !response.success {
            // Collect the backend request failure metrics.
            collect_backend_request_failure_metrics(
                backend.scheme().as_str(),
                http::Method::HEAD.as_str(),
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
            http::Method::HEAD.as_str(),
            start_time.elapsed(),
        );

        let content_length = match response.content_length {
            Some(content_length) => content_length,
            None => return Err(Error::InvalidContentLength),
        };

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

        // If the task is not finished, check if the storage has enough space to
        // store the task.
        if !task.is_finished() && !self.storage.has_enough_space(content_length)? {
            return Err(Error::NoSpace(format!(
                "not enough space to store the task: content_length={content_length}"
            )));
        }

        let task = self
            .storage
            .download_task_started(id, piece_length, content_length, response.http_header)
            .await;

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
                .hard_link_task(id, Path::new(output_path.as_str()))
                .await
            {
                if request.force_hard_link {
                    return Err(err);
                }
            }
        }

        task
    }

    /// download_finished updates the metadata of the task when the task downloads finished.
    #[instrument(skip_all)]
    pub fn download_finished(&self, id: &str) -> ClientResult<metadata::Task> {
        self.storage.download_task_finished(id)
    }

    /// download_failed updates the metadata of the task when the task downloads failed.
    #[instrument(skip_all)]
    pub async fn download_failed(&self, id: &str) -> ClientResult<()> {
        self.storage.download_task_failed(id).await.map(|_| ())
    }

    /// prefetch_task_started updates the metadata of the task when the task prefetch started.
    #[instrument(skip_all)]
    pub async fn prefetch_task_started(&self, id: &str) -> ClientResult<metadata::Task> {
        self.storage.prefetch_task_started(id).await
    }

    /// prefetch_task_failed updates the metadata of the task when the task prefetch failed.
    #[instrument(skip_all)]
    pub async fn prefetch_task_failed(&self, id: &str) -> ClientResult<metadata::Task> {
        self.storage.prefetch_task_failed(id).await
    }

    /// is_same_dev_inode checks if the task is on the same device inode as the given path.
    pub async fn is_same_dev_inode(&self, id: &str, to: &Path) -> ClientResult<bool> {
        self.storage.is_same_dev_inode_as_task(id, to).await
    }

    //// copy_task copies the task content to the destination.
    #[instrument(skip_all)]
    pub async fn copy_task(&self, id: &str, to: &Path) -> ClientResult<()> {
        self.storage.copy_task(id, to).await
    }

    /// download downloads a task.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub async fn download(
        &self,
        task: &metadata::Task,
        host_id: &str,
        peer_id: &str,
        request: Download,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<()> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Get the content length from the task.
        let Some(content_length) = task.content_length() else {
            error!("content length not found");
            return Err(Error::InvalidContentLength);
        };

        // Get the piece length from the task.
        let Some(piece_length) = task.piece_length() else {
            error!("piece length not found");
            return Err(Error::InvalidPieceLength);
        };

        // Calculate the interested pieces to download.
        let interested_pieces =
            match self
                .piece
                .calculate_interested(piece_length, content_length, request.range)
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
                Ok(DownloadTaskResponse {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    peer_id: peer_id.to_string(),
                    response: Some(
                        download_task_response::Response::DownloadTaskStartedResponse(
                            dfdaemon::v2::DownloadTaskStartedResponse {
                                content_length,
                                range: request.range,
                                response_header: task.response_header.clone(),
                                pieces,
                                is_finished: task.is_finished(),
                            },
                        ),
                    ),
                }),
                REQUEST_TIMEOUT,
            )
            .await
            .inspect_err(|err| {
                error!("send DownloadTaskStartedResponse failed: {:?}", err);
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

                // If disable back-to-source is true, return an error directly.
                if request.disable_back_to_source {
                    error!(
                        "download back-to-source is disabled, download with scheduler error: {:?}",
                        err
                    );
                    return Err(Error::Unknown("download failed".to_string()));
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

        // If disable back-to-source is true, return an error directly.
        if request.disable_back_to_source {
            error!("download back-to-source is disabled");
            return Err(Error::Unknown("download failed".to_string()));
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
        task: &metadata::Task,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        request: Download,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
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
                AnnouncePeerRequest {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    peer_id: peer_id.to_string(),
                    request: Some(announce_peer_request::Request::RegisterPeerRequest(
                        RegisterPeerRequest {
                            download: Some(request.clone()),
                        },
                    )),
                },
                REQUEST_TIMEOUT,
            )
            .await
            .inspect_err(|err| {
                error!("send RegisterPeerRequest failed: {:?}", err);
            })?;
        debug!("sent RegisterPeerRequest");

        // Initialize the stream.
        let in_stream = ReceiverStream::new(in_stream_rx);
        let request_stream = Request::new(in_stream);
        let response = self
            .scheduler_client
            .announce_peer(task_id, peer_id, request_stream)
            .await
            .inspect_err(|err| {
                error!("announce peer failed: {:?}", err);
            })?;
        debug!("announced peer has been connected");

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
                debug!("sent DownloadPeerFailedRequest");

                // Wait for the latest message to be sent.
                in_stream_tx.closed().await;
                return Ok(finished_pieces);
            }

            let response = message?.response.ok_or(Error::UnexpectedResponse)?;
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
                        .await
                        .inspect_err(|err| {
                            error!("send DownloadPeerStartedRequest failed: {:?}", err);
                        })?;
                    debug!("sent DownloadPeerStartedRequest");

                    // Send the download peer finished request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPeerFinishedRequest(
                                        DownloadPeerFinishedRequest {},
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("send DownloadPeerFinishedRequest failed: {:?}", err);
                        })?;
                    debug!("sent DownloadPeerFinishedRequest");

                    // Wait for the latest message to be sent.
                    in_stream_tx.closed().await;
                    return Ok(Vec::new());
                }
                announce_peer_response::Response::NormalTaskResponse(response) => {
                    // If the task is normal, download the pieces from the parent.
                    info!(
                        "normal task response: {:?}",
                        response
                            .candidate_parents
                            .iter()
                            .map(|p| p.id.clone())
                            .collect::<Vec<String>>()
                    );

                    // Send the download peer started request.
                    match in_stream_tx
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
                        .await
                    {
                        Ok(_) => debug!("sent DownloadPeerStartedRequest"),
                        Err(err) => {
                            error!("send DownloadPeerStartedRequest failed: {:?}", err);
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
                            request.is_prefetch,
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
                                AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(
                                        announce_peer_request::Request::DownloadPeerFinishedRequest(
                                            DownloadPeerFinishedRequest {},
                                        ),
                                    ),
                                },
                                REQUEST_TIMEOUT,
                            )
                            .await
                        {
                            Ok(_) => debug!("sent DownloadPeerFinishedRequest"),
                            Err(err) => {
                                error!("send DownloadPeerFinishedRequest failed: {:?}", err);
                            }
                        }

                        // Wait for the latest message to be sent.
                        in_stream_tx.closed().await;
                        return Ok(finished_pieces);
                    }

                    // If not all pieces are downloaded, send the reschedule request.
                    match in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::ReschedulePeerRequest(
                                        ReschedulePeerRequest {
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
                        Ok(_) => debug!("sent ReschedulePeerRequest"),
                        Err(err) => {
                            error!("send ReschedulePeerRequest failed: {:?}", err);
                            return Ok(finished_pieces);
                        }
                    };
                }
                announce_peer_response::Response::NeedBackToSourceResponse(response) => {
                    // If the task need back to source, download the pieces from the source.
                    info!("need back to source response: {:?}", response);

                    // Send the download peer back-to-source request.
                    match in_stream_tx
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
                    .await {
                        Ok(_) => debug!("sent DownloadPeerBackToSourceStartedRequest"),
                        Err(err) => {
                            error!("send DownloadPeerBackToSourceStartedRequest failed: {:?}", err);
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
                            debug!("sent DownloadPeerBackToSourceFailedRequest");

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
                                AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(
                                        announce_peer_request::Request::DownloadPeerBackToSourceFinishedRequest(
                                            DownloadPeerBackToSourceFinishedRequest {},
                                        ),
                                    ),
                                },
                                REQUEST_TIMEOUT,
                            )
                            .await
                        {
                            Ok(_) => debug!("sent DownloadPeerBackToSourceFinishedRequest"),
                            Err(err) => {
                                error!("send DownloadPeerBackToSourceFinishedRequest failed: {:?}", err);
                            }
                        }

                        // Wait for the latest message to be sent.
                        in_stream_tx.closed().await;
                        return Ok(finished_pieces);
                    }

                    match in_stream_tx
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
                    .await {
                        Ok(_) => debug!("sent DownloadPeerBackToSourceFailedRequest"),
                        Err(err) => {
                            error!("send DownloadPeerBackToSourceFailedRequest failed: {:?}", err);
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
        task: &metadata::Task,
        host_id: &str,
        peer_id: &str,
        parents: Vec<Peer>,
        interested_pieces: Vec<metadata::Piece>,
        is_prefetch: bool,
        need_piece_content: bool,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
        in_stream_tx: Sender<AnnouncePeerRequest>,
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
        let piece_collector = piece_collector::PieceCollector::new(
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
                parents: Vec<piece_collector::CollectedParent>,
                piece_manager: Arc<piece::Piece>,
                download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
                in_stream_tx: Sender<AnnouncePeerRequest>,
                interrupt: Arc<AtomicBool>,
                finished_pieces: Arc<Mutex<Vec<metadata::Piece>>>,
                is_prefetch: bool,
                need_piece_content: bool,
                protocol: String,
                parent_selector: Arc<ParentSelector>,
            ) -> ClientResult<metadata::Piece> {
                let piece_id = piece_manager.id(task_id.as_str(), number);
                let parent = parent_selector.select(parents);

                info!(
                    "start to download piece {} from parent {:?}",
                    piece_id,
                    parent.id.clone()
                );

                let metadata = piece_manager
                    .download_from_parent(
                        piece_id.as_str(),
                        host_id.as_str(),
                        task_id.as_str(),
                        number,
                        length,
                        parent.clone(),
                        is_prefetch,
                    )
                    .await
                    .map_err(|err| {
                        error!(
                            "download piece {} from parent {:?} error: {:?}",
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
                if need_piece_content {
                    let mut reader = piece_manager
                        .download_from_local_into_async_read(
                            piece_id.as_str(),
                            task_id.as_str(),
                            metadata.length,
                            None,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("read piece {} failed: {:?}", piece_id, err);
                            interrupt.store(true, Ordering::SeqCst);
                        })?;

                    let mut content = vec![0; metadata.length as usize];
                    reader.read_exact(&mut content).await.inspect_err(|err| {
                        error!("read piece {} failed: {:?}", piece_id, err);
                        interrupt.store(true, Ordering::SeqCst);
                    })?;

                    let piece = Piece {
                        content: Some(content),
                        ..piece.clone()
                    };

                    // Piece content must be delivered to dfget: the piece is already marked as
                    // finished and will not be re-sent, so propagate any send failure to abort
                    // the download instead of silently losing the content.
                    download_progress_tx
                        .send(Ok(DownloadTaskResponse {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            response: Some(
                                download_task_response::Response::DownloadPieceFinishedResponse(
                                    dfdaemon::v2::DownloadPieceFinishedResponse {
                                        piece: Some(piece),
                                    },
                                ),
                            ),
                        }))
                        .await
                        .map_err(|err| {
                            error!(
                                "send DownloadPieceFinishedResponse for piece {} failed: {:?}",
                                piece_id, err
                            );

                            interrupt.store(true, Ordering::SeqCst);
                            err
                        })?;
                } else {
                    download_progress_tx
                        .send_timeout(
                            Ok(DownloadTaskResponse {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                response: Some(
                                    download_task_response::Response::DownloadPieceFinishedResponse(
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
                }

                // Send the download piece finished request.
                in_stream_tx
                    .send_timeout(
                        AnnouncePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_peer_request::Request::DownloadPieceFinishedRequest(
                                    DownloadPieceFinishedRequest { piece: Some(piece) },
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
                    "finished piece {} from parent {:?} using protocol {}",
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
                        collect_piece.parents,
                        piece_manager,
                        download_progress_tx,
                        in_stream_tx,
                        interrupt,
                        finished_pieces,
                        is_prefetch,
                        need_piece_content,
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
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPieceFailedRequest(
                                        DownloadPieceFailedRequest {
                                            piece_number: Some(err.piece_number),
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
        task: &metadata::Task,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        request: Download,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
        in_stream_tx: Sender<AnnouncePeerRequest>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Convert the header.
        let request_header: HeaderMap = (&request.request_header)
            .try_into()
            .or_err(ErrorType::ParseError)?;

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
                is_prefetch: bool,
                need_piece_content: bool,
                piece_manager: Arc<piece::Piece>,
                download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
                in_stream_tx: Sender<AnnouncePeerRequest>,
                object_storage: Option<ObjectStorage>,
                hdfs: Option<Hdfs>,
                hugging_face: Option<HuggingFace>,
                model_scope: Option<ModelScope>,
            ) -> ClientResult<metadata::Piece> {
                let piece_id = piece_manager.id(task_id.as_str(), number);
                info!("start to download piece {} from source", piece_id);

                let metadata = piece_manager
                    .download_from_source(
                        piece_id.as_str(),
                        task_id.as_str(),
                        number,
                        url.as_str(),
                        offset,
                        length,
                        request_header,
                        is_prefetch,
                        object_storage,
                        hdfs,
                        hugging_face,
                        model_scope,
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
                if need_piece_content {
                    let mut reader = piece_manager
                        .download_from_local_into_async_read(
                            piece_id.as_str(),
                            task_id.as_str(),
                            metadata.length,
                            None,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("read piece {} failed: {:?}", piece_id, err);
                        })?;

                    let mut content = vec![0; metadata.length as usize];
                    reader.read_exact(&mut content).await.inspect_err(|err| {
                        error!("read piece {} failed: {:?}", piece_id, err);
                    })?;

                    let piece = Piece {
                        content: Some(content),
                        ..piece.clone()
                    };

                    // Piece content must be delivered to dfget: the piece is already marked as
                    // finished and will not be re-sent, so propagate any send failure to abort
                    // the download instead of silently losing the content.
                    download_progress_tx
                        .send(Ok(DownloadTaskResponse {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            response: Some(
                                download_task_response::Response::DownloadPieceFinishedResponse(
                                    dfdaemon::v2::DownloadPieceFinishedResponse {
                                        piece: Some(piece),
                                    },
                                ),
                            ),
                        }))
                        .await
                        .map_err(|err| {
                            error!(
                                "send DownloadPieceFinishedResponse for piece {} failed: {:?}",
                                piece_id, err
                            );

                            err
                        })?;
                } else {
                    download_progress_tx
                        .send_timeout(
                            Ok(DownloadTaskResponse {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                response: Some(
                                    download_task_response::Response::DownloadPieceFinishedResponse(
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
                }

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
            let request_header = request_header.clone();
            let piece_manager = self.piece.clone();
            let download_progress_tx = download_progress_tx.clone();
            let in_stream_tx = in_stream_tx.clone();
            let object_storage = request.object_storage.clone();
            let hdfs = request.hdfs.clone();
            let hugging_face = request.hugging_face.clone();
            let model_scope = request.model_scope.clone();
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
                        request_header,
                        request.is_prefetch,
                        request.need_piece_content,
                        piece_manager,
                        download_progress_tx,
                        in_stream_tx,
                        object_storage,
                        hdfs,
                        hugging_face,
                        model_scope,
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
                    in_stream_tx.send_timeout(AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
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
                    in_stream_tx.send_timeout(AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
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
                    in_stream_tx.send_timeout(AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
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
        task: &metadata::Task,
        host_id: &str,
        peer_id: &str,
        need_piece_content: bool,
        interested_pieces: Vec<metadata::Piece>,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
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
            self.piece.download_from_local(piece.length);
            info!("finished piece {} from local", piece_id,);

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

            // If need_piece_content is true, read the piece content from the local.
            if need_piece_content {
                let mut reader = self
                    .piece
                    .download_from_local_into_async_read(
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

                let piece = Piece {
                    content: Some(content),
                    ..piece.clone()
                };

                // Piece content must be delivered to dfget: the piece is already marked as
                // finished and will not be re-sent, so propagate any send failure to abort
                // the download instead of silently losing the content.
                download_progress_tx
                    .send(Ok(DownloadTaskResponse {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                        peer_id: peer_id.to_string(),
                        response: Some(
                            download_task_response::Response::DownloadPieceFinishedResponse(
                                dfdaemon::v2::DownloadPieceFinishedResponse { piece: Some(piece) },
                            ),
                        ),
                    }))
                    .await
                    .map_err(|err| {
                        error!(
                            "send DownloadPieceFinishedResponse for piece {} failed: {:?}",
                            piece_id, err
                        );

                        err
                    })?;
            } else {
                download_progress_tx
                    .send_timeout(
                        Ok(DownloadTaskResponse {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            response: Some(
                                download_task_response::Response::DownloadPieceFinishedResponse(
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
            }

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
        task: &metadata::Task,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        request: Download,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Convert the header.
        let request_header: HeaderMap = (&request.request_header)
            .try_into()
            .or_err(ErrorType::ParseError)?;

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
                is_prefetch: bool,
                need_piece_content: bool,
                piece_manager: Arc<piece::Piece>,
                download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
                object_storage: Option<ObjectStorage>,
                hdfs: Option<Hdfs>,
                hugging_face: Option<HuggingFace>,
                model_scope: Option<ModelScope>,
            ) -> ClientResult<metadata::Piece> {
                let piece_id = piece_manager.id(task_id.as_str(), number);
                info!("start to download piece {} from source", piece_id);

                let metadata = piece_manager
                    .download_from_source(
                        piece_id.as_str(),
                        task_id.as_str(),
                        number,
                        url.as_str(),
                        offset,
                        length,
                        request_header,
                        is_prefetch,
                        object_storage,
                        hdfs,
                        hugging_face,
                        model_scope,
                    )
                    .await?;

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

                // If need_piece_content is true, read the piece content from the local.
                if need_piece_content {
                    let mut reader = piece_manager
                        .download_from_local_into_async_read(
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

                    let piece = Piece {
                        content: Some(content),
                        ..piece.clone()
                    };

                    // Piece content must be delivered to dfget: the piece is already marked as
                    // finished and will not be re-sent, so propagate any send failure to abort
                    // the download instead of silently losing the content.
                    download_progress_tx
                        .send(Ok(DownloadTaskResponse {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            response: Some(
                                download_task_response::Response::DownloadPieceFinishedResponse(
                                    dfdaemon::v2::DownloadPieceFinishedResponse {
                                        piece: Some(piece),
                                    },
                                ),
                            ),
                        }))
                        .await
                        .map_err(|err| {
                            error!(
                                "send DownloadPieceFinishedResponse for piece {} failed: {:?}",
                                piece_id, err
                            );

                            err
                        })?;
                } else {
                    download_progress_tx
                        .send_timeout(
                            Ok(DownloadTaskResponse {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                response: Some(
                                    download_task_response::Response::DownloadPieceFinishedResponse(
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
                }

                info!("finished piece {} from source", piece_id);
                Ok(metadata)
            }

            let task_id = task_id.to_string();
            let host_id = host_id.to_string();
            let peer_id = peer_id.to_string();
            let url = request.url.clone();
            let request_header = request_header.clone();
            let piece_manager = self.piece.clone();
            let download_progress_tx = download_progress_tx.clone();
            let object_storage = request.object_storage.clone();
            let hdfs = request.hdfs.clone();
            let hugging_face = request.hugging_face.clone();
            let model_scope = request.model_scope.clone();
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
                        request_header,
                        request.is_prefetch,
                        request.need_piece_content,
                        piece_manager,
                        download_progress_tx,
                        object_storage,
                        hdfs,
                        hugging_face,
                        model_scope,
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

    /// stat_task returns the task metadata from scheduler.
    #[instrument(skip_all)]
    pub async fn stat(&self, task_id: &str, host_id: &str) -> ClientResult<CommonTask> {
        self.scheduler_client
            .stat_task(StatTaskRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
            })
            .await
            .inspect_err(|err| {
                error!("stat task failed: {}", err);
            })
    }

    /// stat_local returns the task metadata from local storage.
    #[instrument(skip_all)]
    pub async fn stat_local(&self, task_id: &str) -> ClientResult<StatLocalTaskResponse> {
        let Some(task) = self.get(task_id).inspect_err(|err| {
            error!("get task {} from local storage error: {:?}", task_id, err);
        })?
        else {
            return Err(Error::TaskNotFound(task_id.to_owned()));
        };

        Ok(StatLocalTaskResponse {
            task_id: task.id,
            piece_length: task.piece_length,
            content_length: task.content_length,
            response_header: task.response_header,
            uploading_count: task.uploading_count as u64,
            uploaded_count: task.uploaded_count,
            created_at: Some(task.created_at.into()),
            updated_at: Some(task.updated_at.into()),
            prefetched_at: task.prefetched_at.map(Into::into),
            failed_at: task.failed_at.map(Into::into),
            finished_at: task.finished_at.map(Into::into),
        })
    }

    /// list_local returns the tasks from local storage.
    #[instrument(skip_all)]
    pub async fn list_local(&self) -> ClientResult<ListLocalTasksResponse> {
        let tasks = self.storage.get_tasks().inspect_err(|err| {
            error!("list tasks from local storage error: {:?}", err);
        })?;

        Ok(ListLocalTasksResponse {
            tasks: tasks
                .into_iter()
                .map(|task| StatLocalTaskResponse {
                    task_id: task.id,
                    piece_length: task.piece_length,
                    content_length: task.content_length,
                    response_header: task.response_header,
                    uploading_count: task.uploading_count as u64,
                    uploaded_count: task.uploaded_count,
                    created_at: Some(task.created_at.into()),
                    updated_at: Some(task.updated_at.into()),
                    prefetched_at: task.prefetched_at.map(Into::into),
                    failed_at: task.failed_at.map(Into::into),
                    finished_at: task.finished_at.map(Into::into),
                })
                .collect(),
        })
    }

    /// Delete a task and reclaim local storage.
    #[instrument(skip_all)]
    pub async fn delete(&self, task_id: &str, host_id: &str) -> ClientResult<()> {
        let task = self.get(task_id).inspect_err(|err| {
            error!("get task {} from local storage error: {:?}", task_id, err);
        })?;

        match task {
            Some(task) => {
                self.storage.delete_task(task.id.as_str()).await;

                self.scheduler_client
                    .delete_task(DeleteTaskRequest {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                    })
                    .await
                    .inspect_err(|err| {
                        error!("delete task {} failed from scheduler: {:?}", task_id, err);
                    })?;

                info!("delete task {} from local storage", task.id);
                Ok(())
            }
            None => {
                error!("delete task {} not found", task_id);
                Err(Error::TaskNotFound(task_id.to_owned()))
            }
        }
    }

    /// Delete a local task and reclaim local storage.
    #[instrument(skip_all)]
    pub async fn delete_local(&self, task_id: &str) -> ClientResult<()> {
        let task = self.get(task_id).inspect_err(|err| {
            error!("get task {} from local storage error: {:?}", task_id, err);
        })?;

        match task {
            Some(task) => {
                self.storage.delete_task(task.id.as_str()).await;
                info!("delete task {} from local storage", task.id);
                Ok(())
            }
            None => {
                error!("delete task task {} not found", task_id);
                Err(Error::TaskNotFound(task_id.to_owned()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    // test_delete_task_not_found tests the Task.delete method when the task does not exist.
    #[tokio::test]
    async fn test_delete_task_not_found() {
        // Create a temporary directory for testing.
        let temp_dir = tempdir().unwrap();
        let log_dir = temp_dir.path().join("log");
        std::fs::create_dir_all(&log_dir).unwrap();

        // Create configuration.
        let config = Config::default();
        let config = Arc::new(config);

        // Create storage.
        let storage = Storage::new(config.clone(), temp_dir.path(), log_dir)
            .await
            .unwrap();
        let storage = Arc::new(storage);

        // Test Storage.get_task and Error::TaskNotFound.
        let task_id = "non-existent-task-id";

        // Verify that non-existent tasks return None.
        let task = storage.get_task(task_id).unwrap();
        assert!(task.is_none(), "non-existent tasks should return None");

        // Create a task and save it to storage.
        let task_id = "test-task-id";
        storage
            .download_task_started(task_id, 1024, 4096, None)
            .await
            .unwrap();

        // Verify that the task exists.
        let task = storage.get_task(task_id).unwrap();
        assert!(task.is_some(), "task should exist");

        // Delete the task from storage.
        storage.delete_task(task_id).await;

        // Verify that the task has been deleted.
        let task = storage.get_task(task_id).unwrap();
        assert!(task.is_none(), "task should be deleted");
    }
}
