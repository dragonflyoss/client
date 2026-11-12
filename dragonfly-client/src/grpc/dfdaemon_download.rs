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

use self::dfdaemon_download_content_client::DfdaemonDownloadContentClient as DfdaemonDownloadContentGRPCClient;
use self::dfdaemon_download_content_server::{
    DfdaemonDownloadContent, DfdaemonDownloadContentServer as DfdaemonDownloadContentGRPCServer,
};
use crate::dynconfig::block_list::{DownloadBlockListCheckParams, UploadBlockListCheckParams};
use crate::dynconfig::Dynconfig;
use crate::resource::{persistent_cache_task, persistent_task, task};
use bytes::Bytes;
use dragonfly_api::common::v2::{
    CacheTask, PersistentCacheTask, PersistentTask, Priority, Task, TaskType,
};
use dragonfly_api::dfdaemon::v2::{
    dfdaemon_download_client::DfdaemonDownloadClient as DfdaemonDownloadGRPCClient,
    dfdaemon_download_server::{
        DfdaemonDownload, DfdaemonDownloadServer as DfdaemonDownloadGRPCServer,
    },
    DeleteCacheTaskRequest, DeleteImageRequest, DeleteImageResponse, DeleteLocalTaskRequest,
    DeleteTaskRequest, DownloadCacheTaskRequest, DownloadCacheTaskResponse,
    DownloadPersistentCacheTaskRequest, DownloadPersistentCacheTaskResponse,
    DownloadPersistentTaskRequest, DownloadPersistentTaskResponse, DownloadTaskRequest,
    DownloadTaskResponse, Entry, ListLocalPersistentCacheTasksRequest,
    ListLocalPersistentCacheTasksResponse, ListLocalPersistentTasksRequest,
    ListLocalPersistentTasksResponse, ListLocalTasksRequest, ListLocalTasksResponse,
    ListTaskEntriesRequest, ListTaskEntriesResponse,
    StatCacheTaskRequest as DfdaemonStatCacheTaskRequest, StatLocalPersistentCacheTaskRequest,
    StatLocalPersistentCacheTaskResponse, StatLocalPersistentTaskRequest,
    StatLocalPersistentTaskResponse, StatLocalTaskRequest, StatLocalTaskResponse,
    StatPersistentCacheTaskRequest, StatPersistentTaskRequest,
    StatTaskRequest as DfdaemonStatTaskRequest, UploadPersistentCacheTaskRequest,
    UploadPersistentTaskRequest,
};
use dragonfly_api::errordetails::v2::Backend;
use dragonfly_api::scheduler::v2::DeleteHostRequest as SchedulerDeleteHostRequest;
use dragonfly_client_backend::StatRequest;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error as ClientError, Result as ClientResult,
};
use dragonfly_client_metric::{
    collect_delete_host_failure_metrics, collect_delete_host_started_metrics,
    collect_delete_local_task_failure_metrics, collect_delete_local_task_started_metrics,
    collect_delete_task_failure_metrics, collect_delete_task_started_metrics,
    collect_download_task_blocked_metrics, collect_download_task_failure_metrics,
    collect_download_task_finished_metrics, collect_download_task_started_metrics,
    collect_list_local_tasks_failure_metrics, collect_list_local_tasks_started_metrics,
    collect_list_task_entries_failure_metrics, collect_list_task_entries_started_metrics,
    collect_stat_local_task_failure_metrics, collect_stat_local_task_started_metrics,
    collect_stat_task_failure_metrics, collect_stat_task_started_metrics,
    collect_upload_task_blocked_metrics, collect_upload_task_failure_metrics,
    collect_upload_task_finished_metrics, collect_upload_task_started_metrics,
};
use dragonfly_client_storage::DEFAULT_TMP_DIR;
use dragonfly_client_util::{
    digest::{is_blob_url, is_manifest_digest_url, verify_file_digest, Digest},
    http::{hashmap_to_headermap, headermap_to_hashmap, parse_range_header},
    id_generator::{
        repository_revision, ImageIDParameter, PersistentCacheTaskIDParameter,
        PersistentTaskIDParameter, TaskIDParameter,
    },
    ratelimiter::bbr::BBR,
    shutdown,
    types::redacted::{RedactedDownload, RedactedDownloadPersistentTaskRequest},
};
use futures::{Stream, StreamExt};
use hyper_util::rt::TokioIo;
use opentelemetry::Context;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::Barrier;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::service::interceptor::InterceptedService;
use tonic::{
    transport::{Channel, Endpoint, Server, Uri},
    Code, Request, Response, Status,
};
use tower::service_fn;
use tracing::{debug, error, info, instrument, warn, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use url::Url;
use uuid::Uuid;

use super::interceptor::{ExtractTracingInterceptor, InjectTracingInterceptor};
use super::middleware::rate_limit;

/// Maximum size of a single streamed persistent cache import chunk.
pub const UPLOAD_PERSISTENT_CACHE_TASK_CONTENT_CHUNK_SIZE: usize = 1024 * 1024;

/// Request for streaming persistent cache content to dfdaemon. The first message must contain
/// metadata; subsequent messages must contain non-empty chunks up to the declared content length.
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UploadPersistentCacheTaskContentRequest {
    #[prost(
        oneof = "upload_persistent_cache_task_content_request::Request",
        tags = "1, 2"
    )]
    pub request: Option<upload_persistent_cache_task_content_request::Request>,
}

pub mod upload_persistent_cache_task_content_request {
    #[derive(Clone, PartialEq, Eq, Hash, ::prost::Oneof)]
    pub enum Request {
        #[prost(message, tag = "1")]
        Metadata(super::UploadPersistentCacheTaskContentMetadata),
        #[prost(bytes = "bytes", tag = "2")]
        Chunk(::bytes::Bytes),
    }
}

/// Metadata for a streamed persistent cache import. No client path is transmitted, so paths that
/// are not valid UTF-8 or are invisible in dfdaemon's mount namespace are supported.
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UploadPersistentCacheTaskContentMetadata {
    #[prost(string, optional, tag = "1")]
    pub content_for_calculating_task_id: Option<String>,
    #[prost(uint64, tag = "2")]
    pub content_length: u64,
    #[prost(uint64, tag = "3")]
    pub persistent_replica_count: u64,
    #[prost(string, optional, tag = "4")]
    pub tag: Option<String>,
    #[prost(string, optional, tag = "5")]
    pub application: Option<String>,
    #[prost(uint64, optional, tag = "6")]
    pub piece_length: Option<u64>,
    #[prost(message, optional, tag = "7")]
    pub ttl: Option<prost_wkt_types::Duration>,
    #[prost(message, optional, tag = "8")]
    pub timeout: Option<prost_wkt_types::Duration>,
    #[prost(string, optional, tag = "9")]
    pub remote_ip: Option<String>,
}

impl UploadPersistentCacheTaskContentRequest {
    pub fn metadata(metadata: UploadPersistentCacheTaskContentMetadata) -> Self {
        Self {
            request: Some(
                upload_persistent_cache_task_content_request::Request::Metadata(metadata),
            ),
        }
    }

    pub fn chunk(chunk: Bytes) -> Self {
        Self {
            request: Some(upload_persistent_cache_task_content_request::Request::Chunk(chunk)),
        }
    }
}

impl UploadPersistentCacheTaskContentMetadata {
    fn into_upload_request(self, path: PathBuf) -> UploadPersistentCacheTaskRequest {
        UploadPersistentCacheTaskRequest {
            content_for_calculating_task_id: self.content_for_calculating_task_id,
            path: path.to_string_lossy().to_string(),
            persistent_replica_count: self.persistent_replica_count,
            tag: self.tag,
            application: self.application,
            piece_length: self.piece_length,
            ttl: self.ttl,
            timeout: self.timeout,
            remote_ip: self.remote_ip,
        }
    }
}

/// gRPC Unix server for download operations.
pub struct DfdaemonDownloadServer {
    /// Configuration of the dfdaemon.
    config: Arc<Config>,

    /// Dynamic configuration of the dfdaemon.
    dynconfig: Arc<Dynconfig>,

    /// Path of the Unix domain socket.
    socket_path: PathBuf,

    /// Task manager.
    task: Arc<task::Task>,

    /// Persistent cache task manager.
    persistent_task: Arc<persistent_task::PersistentTask>,

    /// Persistent cache task manager.
    persistent_cache_task: Arc<persistent_cache_task::PersistentCacheTask>,

    /// BBR rate limiter middleware for adaptive rate limiting based on system load.
    bbr: Option<Arc<BBR>>,

    /// Used to shut down the gRPC server.
    shutdown: shutdown::Shutdown,

    /// Used to notify that the gRPC server shutdown is complete.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// Dfdaemon download server implements the grpc server of the download.
impl DfdaemonDownloadServer {
    /// Creates a new download server.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<Config>,
        dynconfig: Arc<Dynconfig>,
        socket_path: PathBuf,
        task: Arc<task::Task>,
        persistent_task: Arc<persistent_task::PersistentTask>,
        persistent_cache_task: Arc<persistent_cache_task::PersistentCacheTask>,
        bbr: Option<Arc<BBR>>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            config,
            dynconfig,
            socket_path,
            task,
            persistent_task,
            persistent_cache_task,
            bbr,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// Starts the download server with Unix domain socket.
    pub async fn run(&mut self, grpc_server_started_barrier: Arc<Barrier>) -> ClientResult<()> {
        // Initialize the grpc service.
        let service = DfdaemonDownloadGRPCServer::with_interceptor(
            DfdaemonDownloadServerHandler {
                config: self.config.clone(),
                dynconfig: self.dynconfig.clone(),
                socket_path: self.socket_path.clone(),
                task: self.task.clone(),
                persistent_task: self.persistent_task.clone(),
                persistent_cache_task: self.persistent_cache_task.clone(),
            },
            ExtractTracingInterceptor,
        );

        let content_service = DfdaemonDownloadContentGRPCServer::with_interceptor(
            DfdaemonDownloadServerHandler {
                config: self.config.clone(),
                dynconfig: self.dynconfig.clone(),
                socket_path: self.socket_path.clone(),
                task: self.task.clone(),
                persistent_task: self.persistent_task.clone(),
                persistent_cache_task: self.persistent_cache_task.clone(),
            },
            ExtractTracingInterceptor,
        );

        // Register the reflection service.
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(dragonfly_api::FILE_DESCRIPTOR_SET)
            .build_v1()?;

        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Initialize health reporter.
        let (health_reporter, health_service) = tonic_health::server::health_reporter();

        // Start download grpc server with unix domain socket.
        fs::create_dir_all(self.socket_path.parent().unwrap()).await?;
        fs::remove_file(self.socket_path.clone())
            .await
            .unwrap_or_else(|err| {
                info!("remove {:?} failed: {}", self.socket_path, err);
            });

        // Bind the unix domain socket and set the permissions for the socket.
        let uds = UnixListener::bind(&self.socket_path)?;
        let perms = std::fs::Permissions::from_mode(0o777);
        fs::set_permissions(&self.socket_path, perms).await?;

        let uds_stream = UnixListenerStream::new(uds);
        let server = Server::builder()
            .max_frame_size(super::MAX_FRAME_SIZE)
            .tcp_keepalive(Some(super::TCP_KEEPALIVE))
            .http2_keepalive_interval(Some(super::HTTP2_KEEP_ALIVE_INTERVAL))
            .http2_keepalive_timeout(Some(super::HTTP2_KEEP_ALIVE_TIMEOUT))
            .initial_stream_window_size(super::INITIAL_WINDOW_SIZE)
            .initial_connection_window_size(super::INITIAL_CONNECTION_WINDOW_SIZE)
            .add_service(reflection)
            .add_service(health_service)
            .add_service(rate_limit(
                content_service,
                self.config.download.server.request_rate_limit,
                self.config.download.server.request_buffer_size,
                self.bbr.clone(),
            ))
            .add_service(rate_limit(
                service,
                self.config.download.server.request_rate_limit,
                self.config.download.server.request_buffer_size,
                self.bbr.clone(),
            ))
            .serve_with_incoming_shutdown(uds_stream, async move {
                // When the grpc server is started, notify the barrier. If the shutdown signal is received
                // before barrier is waited successfully, the server will shutdown immediately.
                tokio::select! {
                    // Notify the download grpc server is started.
                    _ = grpc_server_started_barrier.wait() => {
                        info!("download server is ready to start");

                        health_reporter
                            .set_serving::<DfdaemonDownloadGRPCServer<DfdaemonDownloadServerHandler>>()
                            .await;
                        health_reporter
                            .set_serving::<DfdaemonDownloadContentGRPCServer<DfdaemonDownloadServerHandler>>()
                            .await;

                        info!("download server's health status set to serving");
                    }
                    // Wait for shutdown signal.
                    _ = shutdown.recv() => {
                        info!("download grpc server stop to wait");
                    }
                }

                // Wait for the shutdown signal to shutdown the upload grpc server,
                // when server is started.
                let _ = shutdown.recv().await;
                info!("download grpc server shutting down");
            });

        // Wait for the download grpc server to shutdown.
        info!(
            "download server listening on {}",
            self.socket_path.display()
        );
        server.await?;

        // Remove the unix domain socket file.
        fs::remove_file(&self.socket_path).await?;
        info!("remove the unix domain socket file of the download server");
        Ok(())
    }
}

/// Handler for the dfdaemon download gRPC service.
pub struct DfdaemonDownloadServerHandler {
    /// Configuration of the dfdaemon.
    config: Arc<Config>,

    /// Dynamic configuration of the dfdaemon.
    dynconfig: Arc<Dynconfig>,

    /// Path of the Unix domain socket.
    socket_path: PathBuf,

    /// Task manager.
    task: Arc<task::Task>,

    /// Persistent task manager.
    persistent_task: Arc<persistent_task::PersistentTask>,

    /// Persistent cache task manager.
    persistent_cache_task: Arc<persistent_cache_task::PersistentCacheTask>,
}

/// Dfdaemon download server handler implements the dfdaemon download grpc service.
#[async_trait::async_trait]
impl DfdaemonDownload for DfdaemonDownloadServerHandler {
    /// Stream of download task responses.
    type DownloadTaskStream = ReceiverStream<Result<DownloadTaskResponse, Status>>;

    /// Download the task.
    #[instrument(
        skip_all,
        fields(host_id, task_id, peer_id, url, remote_ip, content_length)
    )]
    async fn download_task(
        &self,
        request: Request<DownloadTaskRequest>,
    ) -> Result<Response<Self::DownloadTaskStream>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Record the start time.
        let start_time = Instant::now();

        // Clone the request.
        let request = request.into_inner();

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
            .dynconfig
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
            .task
            .id_generator
            .task_id(
                if let Some(content) = download.content_for_calculating_task_id.clone() {
                    TaskIDParameter::Content(content)
                } else if download.enable_task_id_based_blob_digest && is_blob_url(&download.url) {
                    TaskIDParameter::BlobDigestBased(download.url.clone())
                } else if download.enable_task_id_based_blob_digest
                    && is_manifest_digest_url(&download.url)
                {
                    TaskIDParameter::ManifestDigestBased(download.url.clone())
                } else {
                    TaskIDParameter::URLBased {
                        url: download.url.clone(),
                        piece_length: download.piece_length,
                        tag: download.tag.clone(),
                        application: download.application.clone(),
                        filtered_query_params: download.filtered_query_params.clone(),
                        revision: repository_revision(&download),
                    }
                },
            )
            .map_err(|e| {
                error!("generate task id: {}", e);
                Status::invalid_argument(e.to_string())
            })?;

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Generate the peer id.
        let peer_id = self.task.id_generator.peer_id();

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
            .task
            .download_started(task_id.as_str(), download.clone())
            .await
        {
            Err(ClientError::BackendError(err)) => {
                error!("download started failed by error: {}", err);
                self.task
                    .download_failed(task_id.as_str())
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
            Err(err @ ClientError::ValidationError(_)) => {
                error!("download started failed by validation: {}", err);
                return Err(Status::invalid_argument(err.to_string()));
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
            // Look up the range header directly instead of converting the whole
            // request header hashmap into a HeaderMap.
            let range_header = download
                .request_header
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(reqwest::header::RANGE.as_str()))
                .map(|(_, value)| value.as_str());

            download.range = match range_header {
                Some(range_header) => {
                    match parse_range_header(
                        range_header,
                        task.content_length().unwrap_or_default(),
                    ) {
                        Ok(range) => Some(range),
                        Err(err) => {
                            // Download task failed.
                            self.task
                                .download_failed(task_id.as_str())
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
                    }
                }
                None => None,
            };
        }

        // Initialize stream channel.
        let download_clone = download.clone();
        let task_manager_clone = self.task.clone();
        let task_clone = task.clone();
        let (out_stream_tx, out_stream_rx) = mpsc::channel(super::DOWNLOAD_STREAM_BUFFER_SIZE);

        // Define the error handler to send the error to the stream.
        async fn handle_error(
            out_stream_tx: &Sender<Result<DownloadTaskResponse, Status>>,
            err: impl std::error::Error,
        ) {
            out_stream_tx
                .send(Err(Status::internal(err.to_string())))
                .await
                .unwrap_or_else(|err| error!("send download progress error: {:?}", err));
        }

        // Define the backend error handler to send the error to the stream.
        async fn handle_backend_error(
            out_stream_tx: &Sender<Result<DownloadTaskResponse, Status>>,
            err: Status,
        ) {
            out_stream_tx
                .send(Err(err))
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
                        debug!("download task succeeded");
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
                    Err(ClientError::BackendError(err)) => {
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
            match self.task.prefetch_task_started(task_id.as_str()).await {
                Ok(_) => {
                    let socket_path = self.socket_path.clone();
                    let task_manager_clone = self.task.clone();
                    tokio::spawn(
                        async move {
                            if let Err(err) = super::prefetch_task(
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
                Err(err @ ClientError::InvalidState(_)) => {
                    debug!("task is already prefetched: {}", err)
                }
                Err(err) => {
                    error!("prefetch task started: {}", err);
                }
            }
        }

        Ok(Response::new(ReceiverStream::new(out_stream_rx)))
    }

    /// Stats the task metadata.
    #[instrument(skip_all, fields(host_id, task_id, remote_ip))]
    async fn stat_task(
        &self,
        request: Request<DfdaemonStatTaskRequest>,
    ) -> Result<Response<Task>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("stat task in download server");

        // Collect the stat task metrics.
        collect_stat_task_started_metrics(TaskType::Standard as i32);
        match self.task.stat(task_id.as_str(), host_id.as_str()).await {
            Ok(task) => Ok(Response::new(task)),
            Err(err) => {
                // Collect the stat task failure metrics.
                collect_stat_task_failure_metrics(TaskType::Standard as i32);

                // Log the error with detailed context.
                error!("stat task failed: {}", err);

                // Map the error to an appropriate gRPC status.
                Err(match err {
                    ClientError::TaskNotFound(id) => {
                        Status::not_found(format!("task not found: {id}"))
                    }
                    _ => Status::internal(err.to_string()),
                })
            }
        }
    }

    /// Stats the local task metadata.
    #[instrument(skip_all, fields(host_id, task_id, remote_ip))]
    async fn stat_local_task(
        &self,
        request: Request<StatLocalTaskRequest>,
    ) -> Result<Response<StatLocalTaskResponse>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("stat local task in download server");

        // Collect the local stat task metrics.
        collect_stat_local_task_started_metrics(TaskType::Standard as i32);

        match self.task.stat_local(task_id.as_str()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => {
                // Collect the local stat task failure metrics.
                collect_stat_local_task_failure_metrics(TaskType::Standard as i32);

                // Log the error with detailed context.
                error!("stat local task failed: {}", err);

                // Map the error to an appropriate gRPC status.
                Err(match err {
                    ClientError::TaskNotFound(id) => {
                        Status::not_found(format!("local task not found: {id}"))
                    }
                    _ => Status::internal(err.to_string()),
                })
            }
        }
    }

    /// List local tasks metadata.
    #[instrument(skip_all, fields(host_id, remote_ip))]
    async fn list_local_tasks(
        &self,
        request: Request<ListLocalTasksRequest>,
    ) -> Result<Response<ListLocalTasksResponse>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Span record the remote ip.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("list local tasks in download server");

        // Collect the list local tasks metrics.
        collect_list_local_tasks_started_metrics(TaskType::Standard as i32);

        match self.task.list_local().await {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => {
                // Collect the list local tasks failure metrics.
                collect_list_local_tasks_failure_metrics(TaskType::Standard as i32);

                // Log the error with detailed context.
                error!("list local tasks failed: {}", err);

                // Map the error to an appropriate gRPC status.
                Err(Status::internal(err.to_string()))
            }
        }
    }

    /// List task entries in the task.
    #[instrument(skip_all, fields(task_id, url, remote_ip))]
    async fn list_task_entries(
        &self,
        request: Request<ListTaskEntriesRequest>,
    ) -> Result<Response<ListTaskEntriesResponse>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Clone the request.
        let request = request.into_inner();

        // Span record the task id and url.
        Span::current().record("task_id", request.task_id.as_str());
        Span::current().record("url", request.url.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("list tasks in download server");

        // Collect the list tasks started metrics.
        collect_list_task_entries_started_metrics(TaskType::Standard as i32);

        // Build the backend.
        let backend = self
            .task
            .backend_factory
            .build(request.url.as_str())
            .map_err(|err| {
                // Collect the list tasks failure metrics.
                collect_list_task_entries_failure_metrics(TaskType::Standard as i32);

                error!("build backend: {}", err);
                Status::internal(err.to_string())
            })?;

        // Head the task entries.
        let response = backend
            .stat(StatRequest {
                task_id: request.task_id.clone(),
                url: request.url.clone(),
                http_header: Some(hashmap_to_headermap(&request.request_header).map_err(
                    |err| {
                        error!("parse request header: {}", err);
                        Status::internal(err.to_string())
                    },
                )?),
                timeout: self.config.download.piece_timeout,
                client_cert: None,
                object_storage: request.object_storage.clone(),
                hdfs: request.hdfs.clone(),
                hugging_face: request.hugging_face.clone(),
                model_scope: request.model_scope.clone(),
                open_csg: request.open_csg.clone(),
            })
            .await
            .map_err(|err| {
                // Collect the list tasks failure metrics.
                collect_list_task_entries_failure_metrics(TaskType::Standard as i32);

                error!("list task entries: {}", err);
                Status::internal(err.to_string())
            })?;

        Ok(Response::new(ListTaskEntriesResponse {
            content_length: response.content_length.unwrap_or_default(),
            response_header: headermap_to_hashmap(&response.http_header.unwrap_or_default()),
            status_code: response.http_status_code.map(|code| code.as_u16().into()),
            entries: response
                .entries
                .into_iter()
                .map(|dir_entry| Entry {
                    url: dir_entry.url,
                    content_length: dir_entry.content_length as u64,
                    is_dir: dir_entry.is_dir,
                })
                .collect(),
        }))
    }

    /// Deletes the task's content and metadata from local storage, and removes
    /// the associated peer metadata from the scheduler.
    #[instrument(skip_all, fields(host_id, task_id, remote_ip))]
    async fn delete_task(
        &self,
        request: Request<DeleteTaskRequest>,
    ) -> Result<Response<()>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("delete task in download server");

        // Collect the delete task started metrics.
        collect_delete_task_started_metrics(TaskType::Standard as i32);

        // Delete the task from the scheduler.
        self.task
            .delete(task_id.as_str(), host_id.as_str())
            .await
            .map_err(|err| {
                // Collect the delete task failure metrics.
                collect_delete_task_failure_metrics(TaskType::Standard as i32);

                error!("delete task: {}", err);
                match err {
                    ClientError::TaskNotFound(id) => {
                        Status::not_found(format!("task not found: {id}"))
                    }
                    _ => Status::internal(err.to_string()),
                }
            })?;

        Ok(Response::new(()))
    }

    /// Deletes task content and metadata in the local storage.
    #[instrument(skip_all, fields(host_id, task_id, remote_ip))]
    async fn delete_local_task(
        &self,
        request: Request<DeleteLocalTaskRequest>,
    ) -> Result<Response<()>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("delete local task in download server");

        // Collect the delete local task started metrics.
        collect_delete_local_task_started_metrics(TaskType::Standard as i32);

        // Delete the task from the local storage.
        self.task
            .delete_local(task_id.as_str())
            .await
            .map_err(|err| {
                // Collect the delete local task failure metrics.
                collect_delete_local_task_failure_metrics(TaskType::Standard as i32);

                error!("delete local task: {}", err);
                match err {
                    ClientError::TaskNotFound(id) => {
                        Status::not_found(format!("local task not found: {id}"))
                    }
                    _ => Status::internal(err.to_string()),
                }
            })?;

        Ok(Response::new(()))
    }

    /// Deletes the tasks of the image which are not referenced by other images in
    /// the peer, and returns the deleted and retained task ids.
    #[instrument(skip_all, fields(host_id, image_id, remote_ip))]
    async fn delete_image(
        &self,
        request: Request<DeleteImageRequest>,
    ) -> Result<Response<DeleteImageResponse>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Generate the image id by the manifest url.
        let image_id = self
            .task
            .id_generator
            .image_id(ImageIDParameter::ManifestURLBased(request.url.clone()))
            .map_err(|e| {
                error!("generate image id: {}", e);
                Status::invalid_argument(e.to_string())
            })?;

        // Span record the host id, image id and remote ip.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("image_id", image_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );

        // Delete all the platforms of the image if the platform is not specified or empty.
        let platform = request
            .platform
            .as_deref()
            .filter(|platform| !platform.is_empty());
        info!(
            "delete image {} {:?} in download server",
            request.url, platform
        );

        // Delete the image from the local storage.
        let response = self
            .task
            .delete_image(&image_id, platform, host_id.as_str())
            .await
            .map_err(|err| {
                error!("delete image: {}", err);
                Status::internal(err.to_string())
            })?;

        Ok(Response::new(response))
    }

    /// Deletes the host from the scheduler, and the scheduler will delete all peers associated
    /// with the host.
    #[instrument(skip_all, fields(host_id))]
    async fn delete_host(&self, request: Request<()>) -> Result<Response<()>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Span record the host id.
        Span::current().record("host_id", host_id.as_str());
        info!("delete host in download server");

        // Collect the delete host started metrics.
        collect_delete_host_started_metrics();

        self.task
            .scheduler_client
            .delete_host(SchedulerDeleteHostRequest { host_id })
            .await
            .map_err(|e| {
                // Collect the delete host failure metrics.
                collect_delete_host_failure_metrics();

                error!("delete host: {}", e);
                Status::internal(e.to_string())
            })?;

        Ok(Response::new(()))
    }

    /// The stream of the download persistent task response.
    type DownloadPersistentTaskStream =
        ReceiverStream<Result<DownloadPersistentTaskResponse, Status>>;

    /// Downloads the persistent task for downloading by user.
    /// Note: This request include object storage credentials and should be used
    /// for downloading from object storage and peers.
    #[instrument(skip_all, fields(host_id, task_id, peer_id, remote_ip, content_length))]
    async fn download_persistent_task(
        &self,
        request: Request<DownloadPersistentTaskRequest>,
    ) -> Result<Response<Self::DownloadPersistentTaskStream>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Record the start time.
        let start_time = Instant::now();

        // Clone the request.
        let request = request.into_inner();

        // Check whether rejected by blocklist policy.
        let check_params = &DownloadBlockListCheckParams {
            application: None,
            url: Some(request.url.clone()),
            tag: None,
            priority: None,
        };
        if self
            .dynconfig
            .block_list
            .is_persistent_task_download_blocked(check_params)
            .await
        {
            warn!("download rejected by blocklist policy: {:?}", check_params);
            collect_download_task_blocked_metrics(TaskType::Persistent as i32);
            return Err(Status::permission_denied(
                "download rejected by blocklist policy",
            ));
        }

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Generate the peer id.
        let peer_id = self.task.id_generator.peer_id();

        // Generate the task type and task priority.
        let task_type = TaskType::Persistent as i32;
        let task_priority = Priority::Level0 as i32;

        // Get the object storage from the request.
        let object_storage = request.object_storage.clone().ok_or_else(|| {
            error!("missing object storage");
            Status::invalid_argument("missing object storage")
        })?;

        // Generate the task id.
        let task_id = self
            .task
            .id_generator
            .persistent_task_id(PersistentTaskIDParameter::FileContentBased {
                url: request.url.clone(),
                region: object_storage.region.ok_or_else(|| {
                    error!("missing object storage region");
                    Status::invalid_argument("missing object storage region")
                })?,
                endpoint: object_storage.endpoint.ok_or_else(|| {
                    error!("missing object storage endpoint");
                    Status::invalid_argument("missing object storage endpoint")
                })?,
            })
            .map_err(|err| {
                error!("generate persistent task id: {}", err);
                Status::invalid_argument(err.to_string())
            })?;

        // Span record the host id, task id and peer id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record("peer_id", peer_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );

        // Check whether the persistent task already exists.
        let content_length = match self
            .persistent_task
            .stat_source(&task_id, &request.url, request.object_storage.clone())
            .await
        {
            Ok(response) => response.content_length.ok_or_else(|| {
                error!("missing content length in persistent task");
                Status::internal("missing content length in persistent task")
            })?,
            Err(ClientError::OpenDALError(err)) if err.kind() == opendal::ErrorKind::NotFound => {
                error!("persistent task not found: {}", err);
                return Err(Status::not_found(err.to_string()));
            }
            Err(err) => {
                error!("check persistent task exists: {}", err);
                return Err(Status::internal(err.to_string()));
            }
        };

        // Download task started.
        info!(
            "download persistent task started: {:?}",
            RedactedDownloadPersistentTaskRequest(&request)
        );
        let task = match self
            .persistent_task
            .download_started(
                task_id.as_str(),
                host_id.as_str(),
                content_length,
                request.clone(),
            )
            .await
        {
            Err(ClientError::BackendError(err)) => {
                error!("download persistent task started failed by error: {}", err);
                self.persistent_task
                    .download_failed(task_id.as_str())
                    .await
                    .unwrap_or_else(|err| error!("download persistent task failed: {}", err));

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
                error!("download persistent task started failed: {}", err);
                return Err(Status::internal(err.to_string()));
            }
            Ok(task) => {
                // Collect download persistent task started metrics.
                collect_download_task_started_metrics(
                    task_type,
                    "",
                    "",
                    task_priority.to_string().as_str(),
                );

                task
            }
        };
        Span::current().record("content_length", task.content_length());

        // Initialize stream channel.
        let request_clone = request.clone();
        let task_manager_clone = self.persistent_task.clone();
        let task_clone = task.clone();
        let (out_stream_tx, out_stream_rx) = mpsc::channel(super::DOWNLOAD_STREAM_BUFFER_SIZE);

        // Define the error handler to send the error to the stream.
        async fn handle_error(
            out_stream_tx: &Sender<Result<DownloadPersistentTaskResponse, Status>>,
            err: impl std::error::Error,
        ) {
            out_stream_tx
                .send(Err(Status::internal(err.to_string())))
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
                        request_clone.clone(),
                        out_stream_tx.clone(),
                    )
                    .await
                {
                    Ok(_) => {
                        // Collect download task finished metrics.
                        collect_download_task_finished_metrics(
                            task_type,
                            "",
                            "",
                            task_priority.to_string().as_str(),
                            task_clone.content_length(),
                            None,
                            start_time.elapsed(),
                        );

                        // Download persistent task succeeded.
                        debug!("download persistent task succeeded");
                        if let Err(err) =
                            task_manager_clone.download_finished(task_clone.id.as_str())
                        {
                            error!("download persistent task finished: {}", err);
                            handle_error(&out_stream_tx, err).await;
                            return;
                        }

                        if let Some(output_path) = &request_clone.output_path {
                            if !request_clone.force_hard_link {
                                let output_path = Path::new(output_path.as_str());
                                if output_path.exists() {
                                    match task_manager_clone
                                        .is_same_dev_inode(task_clone.id.as_str(), output_path)
                                        .await
                                    {
                                        Ok(true) => {}
                                        Ok(false) => {
                                            if request_clone.overwrite {
                                                if let Err(err) = task_manager_clone
                                                    .copy_task(task_clone.id.as_str(), output_path)
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
                            if let Some(raw_digest) = &request_clone.digest {
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
                    Err(err) => {
                        error!("download persistent task failed: {}", err);

                        // Collect download task failure metrics.
                        collect_download_task_failure_metrics(
                            task_type,
                            "",
                            "",
                            task_priority.to_string().as_str(),
                        );

                        // Download task failed.
                        task_manager_clone
                            .download_failed(task_clone.id.as_str())
                            .await
                            .unwrap_or_else(|err| {
                                error!("download persistent task failed: {}", err)
                            });

                        handle_error(&out_stream_tx, err).await;
                    }
                }

                drop(out_stream_tx);
            }
            .in_current_span(),
        );

        Ok(Response::new(ReceiverStream::new(out_stream_rx)))
    }

    /// Uploads the persistent task.
    #[instrument(skip_all, fields(host_id, task_id, peer_id, remote_ip))]
    async fn upload_persistent_task(
        &self,
        request: Request<UploadPersistentTaskRequest>,
    ) -> Result<Response<PersistentTask>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Record the start time.
        let start_time = Instant::now();

        // Clone the request.
        let request = request.into_inner();

        // Check whether rejected by blocklist policy.
        let check_params = &UploadBlockListCheckParams {
            application: None,
            url: Some(request.url.clone()),
            tag: None,
        };
        if self
            .dynconfig
            .block_list
            .is_persistent_task_upload_blocked(check_params)
            .await
        {
            warn!("upload rejected by blocklist policy: {:?}", check_params);
            collect_upload_task_blocked_metrics(TaskType::Persistent as i32);
            return Err(Status::permission_denied(
                "upload rejected by blocklist policy",
            ));
        }

        // Get the path from the request.
        let path = Path::new(request.path.as_str());
        info!("upload persistent task {:?}", request);

        // Get the object storage from the request.
        let object_storage = request.object_storage.clone().ok_or_else(|| {
            error!("missing object storage");
            Status::invalid_argument("missing object storage")
        })?;

        // Generate the task id.
        let task_id = self
            .task
            .id_generator
            .persistent_task_id(PersistentTaskIDParameter::FileContentBased {
                url: request.url.clone(),
                region: object_storage.region.ok_or_else(|| {
                    error!("missing object storage region");
                    Status::invalid_argument("missing object storage region")
                })?,
                endpoint: object_storage.endpoint.ok_or_else(|| {
                    error!("missing object storage endpoint");
                    Status::invalid_argument("missing object storage endpoint")
                })?,
            })
            .map_err(|err| {
                error!("generate persistent task id: {}", err);
                Status::invalid_argument(err.to_string())
            })?;

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Generate the peer id.
        let peer_id = self.task.id_generator.peer_id();

        // Span record the host id, task id and peer id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record("peer_id", peer_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("upload persistent task in download server");

        // Check whether the persistent task already exists.
        match self
            .persistent_task
            .exists(&task_id, &request.url, request.object_storage.clone())
            .await
        {
            Ok(true) => {
                info!("persistent task already exists, skip upload");
                return Err(Status::already_exists("persistent task already exists"));
            }
            Ok(false) => {}
            Err(err) => {
                error!("check persistent task exists: {}", err);
                return Err(Status::internal(err.to_string()));
            }
        };

        // Collect upload task started metrics.
        collect_upload_task_started_metrics(TaskType::Persistent as i32, "", "");

        // Create the persistent task to local storage.
        let task = match self
            .persistent_task
            .upload(
                task_id.as_str(),
                host_id.as_str(),
                peer_id.as_str(),
                path.to_path_buf(),
                request.clone(),
            )
            .await
        {
            Ok(task) => {
                // Collect upload task finished metrics.
                collect_upload_task_finished_metrics(
                    TaskType::Persistent as i32,
                    "",
                    "",
                    task.content_length,
                    start_time.elapsed(),
                );

                task
            }
            Err(err) => {
                // Collect upload task failure metrics.
                collect_upload_task_failure_metrics(TaskType::Persistent as i32, "", "");

                error!("create persistent task: {}", err);
                return Err(Status::internal(err.to_string()));
            }
        };

        Ok(Response::new(task))
    }

    /// Stats the persistent task metadata.
    #[instrument(skip_all, fields(host_id, task_id, remote_ip))]
    async fn stat_persistent_task(
        &self,
        request: Request<StatPersistentTaskRequest>,
    ) -> Result<Response<PersistentTask>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("stat persistent task in download server");

        // Collect the stat persistent task metrics.
        collect_stat_task_started_metrics(TaskType::Persistent as i32);
        match self
            .persistent_task
            .stat(task_id.as_str(), host_id.as_str())
            .await
        {
            Ok(task) => Ok(Response::new(task)),
            Err(err) => {
                // Collect the stat persistent task failure metrics.
                collect_stat_task_failure_metrics(TaskType::Persistent as i32);

                // Log the error with detailed context.
                error!("stat persistent task failed: {}", err);

                // Map the error to an appropriate gRPC status.
                Err(Status::internal(err.to_string()))
            }
        }
    }

    /// Stats the local persistent task metadata.
    #[instrument(skip_all, fields(host_id, task_id, remote_ip))]
    async fn stat_local_persistent_task(
        &self,
        request: Request<StatLocalPersistentTaskRequest>,
    ) -> Result<Response<StatLocalPersistentTaskResponse>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("stat local persistent task in download server");

        // Collect the stat persistent task metrics.
        collect_stat_task_started_metrics(TaskType::Persistent as i32);
        match self.persistent_task.stat_local(task_id.as_str()).await {
            Ok(task) => Ok(Response::new(task)),
            Err(err) => {
                // Collect the stat persistent task failure metrics.
                collect_stat_task_failure_metrics(TaskType::Persistent as i32);

                // Log the error with detailed context.
                error!("stat local persistent task failed: {}", err);

                // Map the error to an appropriate gRPC status.
                Err(Status::internal(err.to_string()))
            }
        }
    }

    /// Lists the local persistent tasks.
    #[instrument(skip_all, fields(host_id, remote_ip))]
    async fn list_local_persistent_tasks(
        &self,
        request: Request<ListLocalPersistentTasksRequest>,
    ) -> Result<Response<ListLocalPersistentTasksResponse>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("list local persistent tasks in download server");

        // Collect the list local persistent task metrics.
        collect_list_local_tasks_started_metrics(TaskType::Persistent as i32);
        match self.persistent_task.list_local().await {
            Ok(task) => Ok(Response::new(task)),
            Err(err) => {
                // Collect the stat persistent task failure metrics.
                collect_list_local_tasks_failure_metrics(TaskType::Persistent as i32);

                // Log the error with detailed context.
                error!("list local persistent tasks failed: {}", err);

                // Map the error to an appropriate gRPC status.
                Err(Status::internal(err.to_string()))
            }
        }
    }

    /// The stream of the download persistent cache task response.
    type DownloadPersistentCacheTaskStream =
        ReceiverStream<Result<DownloadPersistentCacheTaskResponse, Status>>;

    /// Downloads the persistent cache task.
    #[instrument(skip_all, fields(host_id, task_id, peer_id, remote_ip, content_length))]
    async fn download_persistent_cache_task(
        &self,
        request: Request<DownloadPersistentCacheTaskRequest>,
    ) -> Result<Response<Self::DownloadPersistentCacheTaskStream>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Record the start time.
        let start_time = Instant::now();

        // Clone the request.
        let request = request.into_inner();

        // Check whether rejected by blocklist policy.
        let check_params = &DownloadBlockListCheckParams {
            application: request.application.clone(),
            url: None,
            tag: request.tag.clone(),
            priority: None,
        };
        if self
            .dynconfig
            .block_list
            .is_persistent_cache_task_download_blocked(check_params)
            .await
        {
            warn!("download rejected by blocklist policy: {:?}", check_params);
            collect_download_task_blocked_metrics(TaskType::PersistentCache as i32);
            return Err(Status::permission_denied("download blocked by block list"));
        }

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Clone the task id.
        let task_id = request.task_id.clone();

        // Generate the peer id.
        let peer_id = self.task.id_generator.peer_id();

        // Generate the task type and task priority.
        let task_type = TaskType::PersistentCache as i32;
        let task_priority = Priority::Level0 as i32;

        // Span record the host id, task id and peer id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record("peer_id", peer_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );

        // Download task started.
        info!("download persistent cache task started: {:?}", request);
        let task = match self
            .persistent_cache_task
            .download_started(task_id.as_str(), host_id.as_str(), request.clone())
            .await
        {
            Err(ClientError::BackendError(err)) => {
                error!(
                    "download persistent cache task started failed by error: {}",
                    err
                );
                self.persistent_cache_task
                    .download_failed(task_id.as_str())
                    .await
                    .unwrap_or_else(|err| error!("download persistent cache task failed: {}", err));

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
                error!("download persistent cache task started failed: {}", err);
                return Err(Status::internal(err.to_string()));
            }
            Ok(task) => {
                // Collect download persistent cache task started metrics.
                collect_download_task_started_metrics(
                    task_type,
                    request.tag.clone().unwrap_or_default().as_str(),
                    request.application.clone().unwrap_or_default().as_str(),
                    task_priority.to_string().as_str(),
                );

                task
            }
        };
        Span::current().record("content_length", task.content_length());

        // Initialize stream channel.
        let request_clone = request.clone();
        let task_manager_clone = self.persistent_cache_task.clone();
        let task_clone = task.clone();
        let (out_stream_tx, out_stream_rx) = mpsc::channel(super::DOWNLOAD_STREAM_BUFFER_SIZE);

        // Define the error handler to send the error to the stream.
        async fn handle_error(
            out_stream_tx: &Sender<Result<DownloadPersistentCacheTaskResponse, Status>>,
            err: impl std::error::Error,
        ) {
            out_stream_tx
                .send(Err(Status::internal(err.to_string())))
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
                        request_clone.clone(),
                        out_stream_tx.clone(),
                    )
                    .await
                {
                    Ok(_) => {
                        // Collect download task finished metrics.
                        collect_download_task_finished_metrics(
                            task_type,
                            request_clone.tag.clone().unwrap_or_default().as_str(),
                            request_clone
                                .application
                                .clone()
                                .unwrap_or_default()
                                .as_str(),
                            task_priority.to_string().as_str(),
                            task_clone.content_length(),
                            None,
                            start_time.elapsed(),
                        );

                        // Download persistent cache task succeeded.
                        debug!("download persistent cache task succeeded");
                        if let Err(err) =
                            task_manager_clone.download_finished(task_clone.id.as_str())
                        {
                            error!("download persistent cache task finished: {}", err);
                            handle_error(&out_stream_tx, err).await;
                            return;
                        }

                        if let Some(output_path) = &request_clone.output_path {
                            if !request_clone.force_hard_link {
                                let output_path = Path::new(output_path.as_str());
                                if output_path.exists() {
                                    match task_manager_clone
                                        .is_same_dev_inode(task_clone.id.as_str(), output_path)
                                        .await
                                    {
                                        Ok(true) => {}
                                        Ok(false) => {
                                            if request_clone.overwrite {
                                                if let Err(err) = task_manager_clone
                                                    .copy_task(task_clone.id.as_str(), output_path)
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
                            if let Some(raw_digest) = &request_clone.digest {
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
                    Err(err) => {
                        error!("download persistent cache task failed: {}", err);

                        // Collect download task failure metrics.
                        collect_download_task_failure_metrics(
                            task_type,
                            request.tag.clone().unwrap_or_default().as_str(),
                            request.application.clone().unwrap_or_default().as_str(),
                            task_priority.to_string().as_str(),
                        );

                        // Download task failed.
                        task_manager_clone
                            .download_failed(task_clone.id.as_str())
                            .await
                            .unwrap_or_else(|err| {
                                error!("download persistent cache task failed: {}", err)
                            });

                        handle_error(&out_stream_tx, err).await;
                    }
                }

                drop(out_stream_tx);
            }
            .in_current_span(),
        );

        Ok(Response::new(ReceiverStream::new(out_stream_rx)))
    }

    /// Uploads the persistent cache task.
    #[instrument(skip_all, fields(host_id, task_id, peer_id, remote_ip))]
    async fn upload_persistent_cache_task(
        &self,
        request: Request<UploadPersistentCacheTaskRequest>,
    ) -> Result<Response<PersistentCacheTask>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Record the start time.
        let start_time = Instant::now();

        // Clone the request.
        let request = request.into_inner();

        // Check whether rejected by blocklist policy.
        let check_params = &UploadBlockListCheckParams {
            application: request.application.clone(),
            url: None,
            tag: request.tag.clone(),
        };
        if self
            .dynconfig
            .block_list
            .is_persistent_cache_task_upload_blocked(check_params)
            .await
        {
            warn!("upload rejected by blocklist policy: {:?}", check_params);
            collect_upload_task_blocked_metrics(TaskType::PersistentCache as i32);
            return Err(Status::permission_denied(
                "upload rejected by blocklist policy",
            ));
        }

        // Get the path from the request.
        let path = Path::new(request.path.as_str());
        info!("upload persistent cache task {:?}", request);

        // Generate the task id.
        let task_id = self
            .task
            .id_generator
            .persistent_cache_task_id(match request.content_for_calculating_task_id.clone() {
                Some(content) => PersistentCacheTaskIDParameter::Content(content),
                None => PersistentCacheTaskIDParameter::FileContentBased {
                    path: path.to_path_buf(),
                    piece_length: request.piece_length,
                    tag: request.tag.clone(),
                    application: request.application.clone(),
                },
            })
            .map_err(|err| {
                error!("generate persistent cache task id: {}", err);
                Status::invalid_argument(err.to_string())
            })?;

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Generate the peer id.
        let peer_id = self.task.id_generator.peer_id();

        // Span record the host id, task id and peer id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record("peer_id", peer_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("upload persistent cache task in download server");

        // Collect upload task started metrics.
        collect_upload_task_started_metrics(
            TaskType::PersistentCache as i32,
            request.tag.clone().unwrap_or_default().as_str(),
            request.application.clone().unwrap_or_default().as_str(),
        );

        // Create the persistent cache task to local storage.
        let task = match self
            .persistent_cache_task
            .upload(
                task_id.as_str(),
                host_id.as_str(),
                peer_id.as_str(),
                path.to_path_buf(),
                request.clone(),
            )
            .await
        {
            Ok(task) => {
                // Collect upload task finished metrics.
                collect_upload_task_finished_metrics(
                    TaskType::PersistentCache as i32,
                    request.tag.clone().unwrap_or_default().as_str(),
                    request.application.clone().unwrap_or_default().as_str(),
                    task.content_length,
                    start_time.elapsed(),
                );

                task
            }
            Err(err) => {
                // Collect upload task failure metrics.
                collect_upload_task_failure_metrics(
                    TaskType::PersistentCache as i32,
                    request.tag.clone().unwrap_or_default().as_str(),
                    request.application.clone().unwrap_or_default().as_str(),
                );

                error!("create persistent cache task: {}", err);
                return Err(Status::internal(err.to_string()));
            }
        };

        Ok(Response::new(task))
    }

    /// Stats the persistent cache task.
    #[instrument(skip_all, fields(host_id, task_id, remote_ip))]
    async fn stat_persistent_cache_task(
        &self,
        request: Request<StatPersistentCacheTaskRequest>,
    ) -> Result<Response<PersistentCacheTask>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("stat persistent cache task in download server");

        // Collect the stat persistent cache task started metrics.
        collect_stat_task_started_metrics(TaskType::PersistentCache as i32);

        let task = self
            .persistent_cache_task
            .stat(task_id.as_str(), host_id.as_str())
            .await
            .map_err(|err| {
                // Collect the stat persistent cache task failure metrics.
                collect_stat_task_failure_metrics(TaskType::PersistentCache as i32);

                error!("stat persistent cache task: {}", err);
                Status::internal(err.to_string())
            })?;

        Ok(Response::new(task))
    }

    /// Stats the local persistent cache task.
    #[instrument(skip_all, fields(host_id, task_id, remote_ip))]
    async fn stat_local_persistent_cache_task(
        &self,
        request: Request<StatLocalPersistentCacheTaskRequest>,
    ) -> Result<Response<StatLocalPersistentCacheTaskResponse>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("stat local persistent cache task in download server");

        // Collect the stat persistent cache task metrics.
        collect_stat_task_started_metrics(TaskType::PersistentCache as i32);
        match self
            .persistent_cache_task
            .stat_local(task_id.as_str())
            .await
        {
            Ok(task) => Ok(Response::new(task)),
            Err(err) => {
                // Collect the stat persistent cache task failure metrics.
                collect_stat_task_failure_metrics(TaskType::PersistentCache as i32);

                // Log the error with detailed context.
                error!("stat local persistent cache task failed: {}", err);

                // Map the error to an appropriate gRPC status.
                Err(Status::internal(err.to_string()))
            }
        }
    }

    /// Lists the local persistent cache tasks.
    #[instrument(skip_all, fields(host_id, remote_ip))]
    async fn list_local_persistent_cache_tasks(
        &self,
        request: Request<ListLocalPersistentCacheTasksRequest>,
    ) -> Result<Response<ListLocalPersistentCacheTasksResponse>, Status> {
        // If the parent context is set, use it as the parent context for the span.
        if let Some(parent_ctx) = request.extensions().get::<Context>() {
            let _ = Span::current().set_parent(parent_ctx.clone());
        };

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record(
            "remote_ip",
            request.remote_ip.clone().unwrap_or_default().as_str(),
        );
        info!("list local persistent cache tasks in download server");

        // Collect the list local persistent cache task metrics.
        collect_list_local_tasks_started_metrics(TaskType::PersistentCache as i32);
        match self.persistent_cache_task.list_local().await {
            Ok(task) => Ok(Response::new(task)),
            Err(err) => {
                // Collect the stat persistent cache task failure metrics.
                collect_list_local_tasks_failure_metrics(TaskType::PersistentCache as i32);

                // Log the error with detailed context.
                error!("list local persistent cache tasks failed: {}", err);

                // Map the error to an appropriate gRPC status.
                Err(Status::internal(err.to_string()))
            }
        }
    }

    /// The stream of the download cache task response.
    type DownloadCacheTaskStream = ReceiverStream<Result<DownloadCacheTaskResponse, Status>>;

    /// Downloads the cache task.
    #[instrument(
        skip_all,
        fields(host_id, task_id, peer_id, url, remote_ip, content_length)
    )]
    async fn download_cache_task(
        &self,
        _request: Request<DownloadCacheTaskRequest>,
    ) -> Result<Response<Self::DownloadCacheTaskStream>, Status> {
        Err(Status::unimplemented("download_cache_task"))
    }

    /// Stats the cache task metadata.
    #[instrument(skip_all, fields(host_id, task_id, remote_ip))]
    async fn stat_cache_task(
        &self,
        _request: Request<DfdaemonStatCacheTaskRequest>,
    ) -> Result<Response<CacheTask>, Status> {
        Err(Status::unimplemented("stat_cache_task"))
    }

    /// Deletes the cache task.
    #[instrument(skip_all, fields(host_id, task_id, remote_ip))]
    async fn delete_cache_task(
        &self,
        _request: Request<DeleteCacheTaskRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("delete_cache_task"))
    }
}

#[async_trait::async_trait]
impl DfdaemonDownloadContent for DfdaemonDownloadServerHandler {
    #[instrument(skip_all)]
    async fn upload_persistent_cache_task_content(
        &self,
        request: Request<tonic::Streaming<UploadPersistentCacheTaskContentRequest>>,
    ) -> Result<Response<PersistentCacheTask>, Status> {
        let parent_ctx = request.extensions().get::<Context>().cloned();
        let staged_file = StagedFile::create(self.config.storage.dir.join(DEFAULT_TMP_DIR)).await?;
        let (staged_file, metadata) = staged_file.receive(request.into_inner()).await?;
        let path = staged_file.path.clone();
        let mut upload_request = Request::new(metadata.into_upload_request(path));
        if let Some(parent_ctx) = parent_ctx {
            upload_request.extensions_mut().insert(parent_ctx);
        }

        let response = DfdaemonDownload::upload_persistent_cache_task(self, upload_request).await;
        drop(staged_file);
        response
    }
}

struct StagedFile {
    path: PathBuf,
    file: File,
}

impl StagedFile {
    async fn create(directory: PathBuf) -> Result<Self, Status> {
        fs::create_dir_all(&directory)
            .await
            .map_err(|err| Status::internal(format!("create import staging directory: {err}")))?;
        let path = directory.join(format!("dfcache-import-{}", Uuid::new_v4()));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
            .map_err(|err| Status::internal(format!("create staged import file: {err}")))?;
        Ok(Self { path, file })
    }

    async fn receive<S>(
        mut self,
        stream: S,
    ) -> Result<(Self, UploadPersistentCacheTaskContentMetadata), Status>
    where
        S: Stream<Item = Result<UploadPersistentCacheTaskContentRequest, Status>> + Unpin,
    {
        let metadata = receive_persistent_cache_task_content(stream, &mut self.file).await?;
        Ok((self, metadata))
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        if let Err(err) = std::fs::remove_file(&self.path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    "remove staged import file {} failed: {}",
                    self.path.display(),
                    err
                );
            }
        }
    }
}

async fn receive_persistent_cache_task_content<S, W>(
    mut stream: S,
    writer: &mut W,
) -> Result<UploadPersistentCacheTaskContentMetadata, Status>
where
    S: Stream<Item = Result<UploadPersistentCacheTaskContentRequest, Status>> + Unpin,
    W: AsyncWrite + Unpin,
{
    use upload_persistent_cache_task_content_request::Request as ContentRequest;

    let metadata = match stream.next().await {
        Some(Ok(UploadPersistentCacheTaskContentRequest {
            request: Some(ContentRequest::Metadata(metadata)),
        })) => metadata,
        Some(Ok(_)) => {
            return Err(Status::invalid_argument(
                "first streamed import message must contain metadata",
            ));
        }
        Some(Err(err)) => return Err(err),
        None => return Err(Status::invalid_argument("missing streamed import metadata")),
    };

    let mut received_length = 0u64;
    while let Some(message) = stream.next().await {
        let chunk = match message? {
            UploadPersistentCacheTaskContentRequest {
                request: Some(ContentRequest::Chunk(chunk)),
            } => chunk,
            UploadPersistentCacheTaskContentRequest {
                request: Some(ContentRequest::Metadata(_)),
            } => {
                return Err(Status::invalid_argument(
                    "streamed import metadata must only appear once",
                ));
            }
            _ => return Err(Status::invalid_argument("missing streamed import content")),
        };

        if chunk.is_empty() {
            return Err(Status::invalid_argument("streamed import chunk is empty"));
        }
        if chunk.len() > UPLOAD_PERSISTENT_CACHE_TASK_CONTENT_CHUNK_SIZE {
            return Err(Status::invalid_argument(format!(
                "streamed import chunk exceeds {UPLOAD_PERSISTENT_CACHE_TASK_CONTENT_CHUNK_SIZE} bytes"
            )));
        }
        if chunk.len() as u64 > metadata.content_length.saturating_sub(received_length) {
            return Err(Status::invalid_argument(
                "streamed import content exceeds declared content length",
            ));
        }

        writer.write_all(&chunk).await.map_err(|err| {
            Status::internal(format!("write staged persistent cache content: {err}"))
        })?;
        received_length += chunk.len() as u64;
    }

    if received_length != metadata.content_length {
        return Err(Status::invalid_argument(format!(
            "streamed import ended at {received_length} bytes, expected {}",
            metadata.content_length
        )));
    }
    writer
        .flush()
        .await
        .map_err(|err| Status::internal(format!("flush staged persistent cache content: {err}")))?;
    Ok(metadata)
}

/// A wrapper of DfdaemonDownloadGRPCClient.
#[derive(Clone)]
pub struct DfdaemonDownloadClient {
    /// The grpc client of the dfdaemon.
    pub client: DfdaemonDownloadGRPCClient<InterceptedService<Channel, InjectTracingInterceptor>>,

    content_client:
        DfdaemonDownloadContentGRPCClient<InterceptedService<Channel, InjectTracingInterceptor>>,
}

/// Dfdaemon download client implements the grpc client of the dfdaemon download.
impl DfdaemonDownloadClient {
    /// Creates a new DfdaemonDownloadClient.
    pub async fn new(config: Arc<Config>, addr: String) -> ClientResult<Self> {
        let domain_name = Url::parse(addr.as_str())?
            .host_str()
            .ok_or(ClientError::InvalidParameter)
            .inspect_err(|_err| {
                error!("invalid address: {}", addr);
            })?
            .to_string();

        let channel = match config
            .upload
            .client
            .load_client_tls_config(domain_name.as_str())
            .await?
        {
            Some(client_tls_config) => Channel::from_shared(addr.clone())
                .inspect_err(|err| {
                    error!("invalid address {}: {}", addr, err);
                })
                .or_err(ErrorType::ParseError)?
                .tls_config(client_tls_config)?
                .buffer_size(super::BUFFER_SIZE)
                .connect_timeout(super::CONNECT_TIMEOUT)
                .timeout(super::REQUEST_TIMEOUT)
                .tcp_keepalive(Some(super::TCP_KEEPALIVE))
                .http2_keep_alive_interval(super::HTTP2_KEEP_ALIVE_INTERVAL)
                .keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT)
                .connect()
                .await
                .inspect_err(|err| {
                    error!("connect to {} failed: {}", addr, err);
                })
                .or_err(ErrorType::ConnectError)?,
            None => Channel::from_shared(addr.clone())
                .inspect_err(|err| {
                    error!("invalid address {}: {}", addr, err);
                })
                .or_err(ErrorType::ParseError)?
                .buffer_size(super::BUFFER_SIZE)
                .connect_timeout(super::CONNECT_TIMEOUT)
                .timeout(super::REQUEST_TIMEOUT)
                .tcp_keepalive(Some(super::TCP_KEEPALIVE))
                .http2_keep_alive_interval(super::HTTP2_KEEP_ALIVE_INTERVAL)
                .keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT)
                .connect()
                .await
                .inspect_err(|err| {
                    error!("connect to {} failed: {}", addr, err);
                })
                .or_err(ErrorType::ConnectError)?,
        };

        let content_client = DfdaemonDownloadContentGRPCClient::with_interceptor(
            channel.clone(),
            InjectTracingInterceptor,
        )
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);
        let client =
            DfdaemonDownloadGRPCClient::with_interceptor(channel, InjectTracingInterceptor)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX);
        Ok(Self {
            client,
            content_client,
        })
    }

    /// Creates a new DfdaemonDownloadClient with unix domain socket.
    pub async fn new_unix(socket_path: PathBuf) -> ClientResult<Self> {
        // Ignore the uri because it is not used.
        let channel = Endpoint::try_from("http://[::]:50051")
            .unwrap()
            .buffer_size(super::BUFFER_SIZE)
            .connect_timeout(super::CONNECT_TIMEOUT)
            .initial_stream_window_size(super::INITIAL_WINDOW_SIZE)
            .initial_connection_window_size(super::INITIAL_CONNECTION_WINDOW_SIZE)
            .tcp_keepalive(Some(super::TCP_KEEPALIVE))
            .http2_keep_alive_interval(super::HTTP2_KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT)
            .keep_alive_while_idle(true)
            .connect_with_connector(service_fn(move |_: Uri| {
                let socket_path = socket_path.clone();
                async move {
                    Ok::<_, std::io::Error>(TokioIo::new(
                        UnixStream::connect(socket_path.clone()).await?,
                    ))
                }
            }))
            .await
            .inspect_err(|err| {
                error!("connect failed: {}", err);
            })
            .or_err(ErrorType::ConnectError)?;

        let content_client = DfdaemonDownloadContentGRPCClient::with_interceptor(
            channel.clone(),
            InjectTracingInterceptor,
        )
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);
        let client =
            DfdaemonDownloadGRPCClient::with_interceptor(channel, InjectTracingInterceptor)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX);
        Ok(Self {
            client,
            content_client,
        })
    }

    /// Download the task.
    #[instrument(skip_all)]
    pub async fn download_task(
        &self,
        request: DownloadTaskRequest,
    ) -> ClientResult<tonic::Response<tonic::codec::Streaming<DownloadTaskResponse>>> {
        // Get the download from the request.
        let download = request.clone().download.ok_or_else(|| {
            tonic::Status::invalid_argument("missing download in download task request")
        })?;

        // Initialize the request.
        let mut request = tonic::Request::new(request);

        // Set the timeout to the request.
        if let Some(timeout) = download.timeout {
            request.set_timeout(
                Duration::try_from(timeout)
                    .map_err(|_| tonic::Status::invalid_argument("invalid timeout"))?,
            );
        }

        let response = self.client.clone().download_task(request).await?;
        Ok(response)
    }

    /// Stat the task metadata.
    #[instrument(skip_all)]
    pub async fn stat_task(&self, request: DfdaemonStatTaskRequest) -> ClientResult<Task> {
        let request = Self::make_request(request);
        let response = self.client.clone().stat_task(request).await?;
        Ok(response.into_inner())
    }

    /// List local tasks metadata.
    #[instrument(skip_all)]
    pub async fn list_local_tasks(
        &self,
        request: ListLocalTasksRequest,
    ) -> ClientResult<ListLocalTasksResponse> {
        let request = Self::make_request(request);
        let response = self.client.clone().list_local_tasks(request).await?;
        Ok(response.into_inner())
    }

    /// List task entries in the task.
    #[instrument(skip_all)]
    pub async fn list_task_entries(
        &self,
        request: ListTaskEntriesRequest,
    ) -> ClientResult<ListTaskEntriesResponse> {
        let request = Self::make_request(request);
        let response = self.client.clone().list_task_entries(request).await?;
        Ok(response.into_inner())
    }

    /// Deletes the task's content and metadata from local storage, and removes
    /// the associated peer metadata from the scheduler.
    #[instrument(skip_all)]
    pub async fn delete_task(&self, request: DeleteTaskRequest) -> ClientResult<()> {
        let request = Self::make_request(request);
        self.client.clone().delete_task(request).await?;
        Ok(())
    }

    /// Deletes task content and metadata in the local storage.
    #[instrument(skip_all)]
    pub async fn delete_local_task(&self, request: DeleteLocalTaskRequest) -> ClientResult<()> {
        let request = Self::make_request(request);
        self.client.clone().delete_local_task(request).await?;
        Ok(())
    }

    /// Deletes the tasks of the image which are not referenced by other images in
    /// the local storage.
    #[instrument(skip_all)]
    pub async fn delete_image(
        &self,
        request: DeleteImageRequest,
    ) -> ClientResult<DeleteImageResponse> {
        let request = Self::make_request(request);
        let response = self.client.clone().delete_image(request).await?;
        Ok(response.into_inner())
    }

    /// Downloads the persistent task for downloading by user.
    /// Note: This request include object storage credentials and should be used
    /// for downloading from object storage and peers.
    #[instrument(skip_all)]
    pub async fn download_persistent_task(
        &self,
        request: DownloadPersistentTaskRequest,
    ) -> ClientResult<tonic::Response<tonic::codec::Streaming<DownloadPersistentTaskResponse>>>
    {
        // Clone the request.
        let request_clone = request.clone();

        // Initialize the request.
        let mut request = tonic::Request::new(request);

        // Set the timeout to the request.
        if let Some(timeout) = request_clone.timeout {
            request.set_timeout(
                Duration::try_from(timeout)
                    .map_err(|_| tonic::Status::invalid_argument("invalid timeout"))?,
            );
        }

        let response = self
            .client
            .clone()
            .download_persistent_task(request)
            .await?;
        Ok(response)
    }

    /// Uploads the persistent task.
    #[instrument(skip_all)]
    pub async fn upload_persistent_task(
        &self,
        request: UploadPersistentTaskRequest,
    ) -> ClientResult<PersistentTask> {
        // Clone the request.
        let request_clone = request.clone();

        // Initialize the request.
        let mut request = tonic::Request::new(request);

        // Set the timeout to the request.
        if let Some(timeout) = request_clone.timeout {
            request.set_timeout(
                Duration::try_from(timeout)
                    .map_err(|_| tonic::Status::invalid_argument("invalid timeout"))?,
            );
        }

        let response = self.client.clone().upload_persistent_task(request).await?;
        Ok(response.into_inner())
    }

    /// Stats the persistent task metadata.
    #[instrument(skip_all)]
    pub async fn stat_persistent_task(
        &self,
        request: StatPersistentTaskRequest,
    ) -> ClientResult<PersistentTask> {
        let request = Self::make_request(request);
        let response = self.client.clone().stat_persistent_task(request).await?;
        Ok(response.into_inner())
    }

    /// Stats the local persistent task metadata.
    #[instrument(skip_all)]
    pub async fn stat_local_persistent_task(
        &self,
        request: StatLocalPersistentTaskRequest,
    ) -> ClientResult<StatLocalPersistentTaskResponse> {
        let request = Self::make_request(request);
        let response = self
            .client
            .clone()
            .stat_local_persistent_task(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Lists the local persistent tasks.
    #[instrument(skip_all)]
    pub async fn list_local_persistent_tasks(
        &self,
        request: ListLocalPersistentTasksRequest,
    ) -> ClientResult<ListLocalPersistentTasksResponse> {
        let request = Self::make_request(request);
        let response = self
            .client
            .clone()
            .list_local_persistent_tasks(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Downloads the persistent cache task.
    #[instrument(skip_all)]
    pub async fn download_persistent_cache_task(
        &self,
        request: DownloadPersistentCacheTaskRequest,
    ) -> ClientResult<tonic::Response<tonic::codec::Streaming<DownloadPersistentCacheTaskResponse>>>
    {
        // Clone the request.
        let request_clone = request.clone();

        // Initialize the request.
        let mut request = tonic::Request::new(request);

        // Set the timeout to the request.
        if let Some(timeout) = request_clone.timeout {
            request.set_timeout(
                Duration::try_from(timeout)
                    .map_err(|_| tonic::Status::invalid_argument("invalid timeout"))?,
            );
        }

        let response = self
            .client
            .clone()
            .download_persistent_cache_task(request)
            .await?;
        Ok(response)
    }

    /// Uploads the persistent cache task.
    #[instrument(skip_all)]
    pub async fn upload_persistent_cache_task(
        &self,
        request: UploadPersistentCacheTaskRequest,
    ) -> ClientResult<PersistentCacheTask> {
        // Clone the request.
        let request_clone = request.clone();

        // Initialize the request.
        let mut request = tonic::Request::new(request);

        // Set the timeout to the request.
        if let Some(timeout) = request_clone.timeout {
            request.set_timeout(
                Duration::try_from(timeout)
                    .map_err(|_| tonic::Status::invalid_argument("invalid timeout"))?,
            );
        }

        let response = self
            .client
            .clone()
            .upload_persistent_cache_task(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Uploads persistent cache content when the source path is only visible to dfcache.
    #[instrument(skip_all)]
    pub async fn upload_persistent_cache_task_content<S>(
        &self,
        metadata: UploadPersistentCacheTaskContentMetadata,
        content: S,
    ) -> ClientResult<PersistentCacheTask>
    where
        S: Stream<Item = UploadPersistentCacheTaskContentRequest> + Send + 'static,
    {
        let timeout = metadata.timeout;
        let stream =
            tokio_stream::once(UploadPersistentCacheTaskContentRequest::metadata(metadata))
                .chain(content);
        let mut request = Request::new(stream);
        if let Some(timeout) = timeout {
            request.set_timeout(
                Duration::try_from(timeout)
                    .map_err(|_| Status::invalid_argument("invalid timeout"))?,
            );
        }

        let response = self
            .content_client
            .clone()
            .upload_persistent_cache_task_content(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Stats the persistent cache task.
    #[instrument(skip_all)]
    pub async fn stat_persistent_cache_task(
        &self,
        request: StatPersistentCacheTaskRequest,
    ) -> ClientResult<PersistentCacheTask> {
        let request = Self::make_request(request);
        let response = self
            .client
            .clone()
            .stat_persistent_cache_task(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Stats the local persistent cache task.
    #[instrument(skip_all)]
    pub async fn stat_local_persistent_cache_task(
        &self,
        request: StatLocalPersistentCacheTaskRequest,
    ) -> ClientResult<StatLocalPersistentCacheTaskResponse> {
        let request = Self::make_request(request);
        let response = self
            .client
            .clone()
            .stat_local_persistent_cache_task(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Lists the local persistent cache tasks.
    #[instrument(skip_all)]
    pub async fn list_local_persistent_cache_tasks(
        &self,
        request: ListLocalPersistentCacheTasksRequest,
    ) -> ClientResult<ListLocalPersistentCacheTasksResponse> {
        let request = Self::make_request(request);
        let response = self
            .client
            .clone()
            .list_local_persistent_cache_tasks(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Creates a new request with timeout.
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}

pub mod dfdaemon_download_content_client {
    #![allow(missing_docs, clippy::wildcard_imports)]

    use tonic::codegen::http::Uri;
    use tonic::codegen::*;

    #[derive(Debug, Clone)]
    pub struct DfdaemonDownloadContentClient<T> {
        inner: tonic::client::Grpc<T>,
    }

    impl<T> DfdaemonDownloadContentClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            Self {
                inner: tonic::client::Grpc::new(inner),
            }
        }

        pub fn with_origin(inner: T, origin: Uri) -> Self {
            Self {
                inner: tonic::client::Grpc::with_origin(inner, origin),
            }
        }

        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> DfdaemonDownloadContentClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: Service<
                http::Request<tonic::body::Body>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::Body>>::ResponseBody,
                >,
            >,
            <T as Service<http::Request<tonic::body::Body>>>::Error: Into<StdError> + Send + Sync,
        {
            DfdaemonDownloadContentClient::new(InterceptedService::new(inner, interceptor))
        }

        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }

        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }

        pub async fn upload_persistent_cache_task_content(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::UploadPersistentCacheTaskContentRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<dragonfly_api::common::v2::PersistentCacheTask>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|err| {
                tonic::Status::unknown(format!("Service was not ready: {}", err.into()))
            })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/dfdaemon.v2.DfdaemonDownloadContent/UploadPersistentCacheTaskContent",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut().insert(GrpcMethod::new(
                "dfdaemon.v2.DfdaemonDownloadContent",
                "UploadPersistentCacheTaskContent",
            ));
            self.inner.client_streaming(req, path, codec).await
        }
    }
}

pub mod dfdaemon_download_content_server {
    #![allow(missing_docs, clippy::wildcard_imports)]

    use tonic::codegen::*;

    #[async_trait]
    pub trait DfdaemonDownloadContent: Send + Sync + 'static {
        async fn upload_persistent_cache_task_content(
            &self,
            request: tonic::Request<
                tonic::Streaming<super::UploadPersistentCacheTaskContentRequest>,
            >,
        ) -> std::result::Result<
            tonic::Response<dragonfly_api::common::v2::PersistentCacheTask>,
            tonic::Status,
        >;
    }

    #[derive(Debug)]
    pub struct DfdaemonDownloadContentServer<T> {
        inner: Arc<T>,
    }

    impl<T> DfdaemonDownloadContentServer<T> {
        pub fn new(inner: T) -> Self {
            Self {
                inner: Arc::new(inner),
            }
        }

        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }

    impl<T, B> Service<http::Request<B>> for DfdaemonDownloadContentServer<T>
    where
        T: DfdaemonDownloadContent,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match req.uri().path() {
                "/dfdaemon.v2.DfdaemonDownloadContent/UploadPersistentCacheTaskContent" => {
                    #[allow(non_camel_case_types)]
                    struct UploadPersistentCacheTaskContentSvc<T: DfdaemonDownloadContent>(
                        pub Arc<T>,
                    );

                    impl<T: DfdaemonDownloadContent>
                        tonic::server::ClientStreamingService<
                            super::UploadPersistentCacheTaskContentRequest,
                        > for UploadPersistentCacheTaskContentSvc<T>
                    {
                        type Response = dragonfly_api::common::v2::PersistentCacheTask;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::UploadPersistentCacheTaskContentRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as DfdaemonDownloadContent>::upload_persistent_cache_task_content(
                                    &inner, request,
                                )
                                .await
                            };
                            Box::pin(fut)
                        }
                    }

                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = UploadPersistentCacheTaskContentSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec);
                        let res = grpc.client_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    let mut response = http::Response::new(tonic::body::Body::default());
                    let headers = response.headers_mut();
                    headers.insert(
                        tonic::Status::GRPC_STATUS,
                        (tonic::Code::Unimplemented as i32).into(),
                    );
                    headers.insert(
                        http::header::CONTENT_TYPE,
                        tonic::metadata::GRPC_CONTENT_TYPE,
                    );
                    Ok(response)
                }),
            }
        }
    }

    impl<T> Clone for DfdaemonDownloadContentServer<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }

    pub const SERVICE_NAME: &str = "dfdaemon.v2.DfdaemonDownloadContent";

    impl<T> tonic::server::NamedService for DfdaemonDownloadContentServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context as TaskContext, Poll};
    use tempfile::tempdir;

    fn metadata(content_length: u64) -> UploadPersistentCacheTaskContentMetadata {
        UploadPersistentCacheTaskContentMetadata {
            content_for_calculating_task_id: Some("test-content".to_string()),
            content_length,
            persistent_replica_count: 1,
            tag: Some("test-tag".to_string()),
            application: Some("test-application".to_string()),
            piece_length: Some(4 * 1024 * 1024),
            ttl: Some(prost_wkt_types::Duration {
                seconds: 300,
                nanos: 0,
            }),
            timeout: Some(prost_wkt_types::Duration {
                seconds: 30,
                nanos: 0,
            }),
            remote_ip: Some("127.0.0.1".to_string()),
        }
    }

    fn requests(
        metadata: UploadPersistentCacheTaskContentMetadata,
        chunks: &[&[u8]],
    ) -> Vec<Result<UploadPersistentCacheTaskContentRequest, Status>> {
        let mut requests = vec![Ok(UploadPersistentCacheTaskContentRequest::metadata(
            metadata,
        ))];
        requests.extend(chunks.iter().map(|chunk| {
            Ok(UploadPersistentCacheTaskContentRequest::chunk(
                Bytes::copy_from_slice(chunk),
            ))
        }));
        requests
    }

    #[test]
    fn content_protocol_round_trip() {
        let metadata_request = UploadPersistentCacheTaskContentRequest::metadata(metadata(42));
        let encoded = metadata_request.encode_to_vec();
        assert_eq!(
            UploadPersistentCacheTaskContentRequest::decode(encoded.as_slice()).unwrap(),
            metadata_request
        );

        let chunk_request = UploadPersistentCacheTaskContentRequest::chunk(Bytes::from_static(&[
            0x00, 0xff, 0x80, 0x41,
        ]));
        let encoded = chunk_request.encode_to_vec();
        assert_eq!(
            UploadPersistentCacheTaskContentRequest::decode(encoded.as_slice()).unwrap(),
            chunk_request
        );
    }

    #[tokio::test]
    async fn receive_content_handles_empty_and_chunked_files() {
        let test_cases: Vec<(Vec<u8>, Vec<Vec<u8>>)> = vec![
            (Vec::new(), Vec::new()),
            (b"single chunk".to_vec(), vec![b"single chunk".to_vec()]),
            (
                (0u8..=255).collect(),
                vec![vec![0, 1, 2], (3u8..=255).collect()],
            ),
        ];

        for (expected, chunks) in test_cases {
            let chunk_refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
            let mut output = Vec::new();
            let received_metadata = receive_persistent_cache_task_content(
                tokio_stream::iter(requests(
                    metadata(expected.len() as u64),
                    chunk_refs.as_slice(),
                )),
                &mut output,
            )
            .await
            .unwrap();
            assert_eq!(received_metadata.content_length, expected.len() as u64);
            assert_eq!(output, expected);
        }
    }

    #[tokio::test]
    async fn receive_content_rejects_invalid_protocol_and_lengths() {
        let oversized_chunk = vec![0; UPLOAD_PERSISTENT_CACHE_TASK_CONTENT_CHUNK_SIZE + 1];
        let test_cases = vec![
            Vec::new(),
            vec![Ok(UploadPersistentCacheTaskContentRequest::chunk(
                Bytes::from_static(b"chunk before metadata"),
            ))],
            vec![Ok(UploadPersistentCacheTaskContentRequest {
                request: None,
            })],
            requests(metadata(4), &[b"ab", b"cdef"]),
            requests(metadata(1), &[b""]),
            vec![
                Ok(UploadPersistentCacheTaskContentRequest::metadata(metadata(
                    0,
                ))),
                Ok(UploadPersistentCacheTaskContentRequest::metadata(metadata(
                    0,
                ))),
            ],
            requests(metadata(2), &[b"a"]),
            requests(
                metadata(UPLOAD_PERSISTENT_CACHE_TASK_CONTENT_CHUNK_SIZE as u64 + 1),
                &[oversized_chunk.as_slice()],
            ),
        ];

        for stream in test_cases {
            let mut output = Vec::new();
            let err =
                receive_persistent_cache_task_content(tokio_stream::iter(stream), &mut output)
                    .await
                    .unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn receive_content_preserves_peer_interruption() {
        let mut stream = requests(metadata(4), &[b"ab"]);
        stream.push(Err(Status::cancelled("peer interrupted")));
        let mut output = Vec::new();
        let err = receive_persistent_cache_task_content(tokio_stream::iter(stream), &mut output)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Cancelled);
        assert_eq!(output, b"ab");
    }

    #[tokio::test]
    async fn staged_file_is_removed_after_short_stream() {
        let tempdir = tempdir().unwrap();
        let staged_file = StagedFile::create(tempdir.path().to_path_buf())
            .await
            .unwrap();
        let path = staged_file.path.clone();
        let err = staged_file
            .receive(tokio_stream::iter(requests(metadata(2), &[b"a"])))
            .await
            .err()
            .expect("short stream must fail");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(!path.exists());
    }

    struct FailingWriter {
        remaining: usize,
    }

    impl AsyncWrite for FailingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            if this.remaining == 0 {
                return Poll::Ready(Err(io::Error::other("injected storage write failure")));
            }

            let written = this.remaining.min(buf.len());
            this.remaining -= written;
            Poll::Ready(Ok(written))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn receive_content_reports_storage_write_failure() {
        let mut writer = FailingWriter { remaining: 2 };
        let err = receive_persistent_cache_task_content(
            tokio_stream::iter(requests(metadata(8), &[b"abcd", b"efgh"])),
            &mut writer,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), Code::Internal);
        assert!(err.message().contains("injected storage write failure"));
    }

    #[tokio::test]
    async fn unix_client_reports_missing_socket() {
        let tempdir = tempdir().unwrap();
        let result = DfdaemonDownloadClient::new_unix(tempdir.path().join("missing.sock")).await;
        assert!(result.is_err());
    }

    struct TestContentService;

    #[async_trait::async_trait]
    impl DfdaemonDownloadContent for TestContentService {
        async fn upload_persistent_cache_task_content(
            &self,
            request: Request<tonic::Streaming<UploadPersistentCacheTaskContentRequest>>,
        ) -> Result<Response<PersistentCacheTask>, Status> {
            let mut output = Vec::new();
            let metadata =
                receive_persistent_cache_task_content(request.into_inner(), &mut output).await?;
            assert_eq!(output, b"content over UDS");
            Ok(Response::new(PersistentCacheTask {
                id: "streamed-task".to_string(),
                content_length: metadata.content_length,
                ..Default::default()
            }))
        }
    }

    #[tokio::test]
    async fn content_client_streams_over_unix_socket() {
        let tempdir = tempdir().unwrap();
        let socket_path = tempdir.path().join("dfdaemon.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(
            Server::builder()
                .add_service(DfdaemonDownloadContentGRPCServer::new(TestContentService))
                .serve_with_incoming(UnixListenerStream::new(listener)),
        );

        let client = DfdaemonDownloadClient::new_unix(socket_path).await.unwrap();
        let content = b"content over UDS";
        let task = client
            .upload_persistent_cache_task_content(
                metadata(content.len() as u64),
                tokio_stream::iter(vec![
                    UploadPersistentCacheTaskContentRequest::chunk(Bytes::from_static(b"content ")),
                    UploadPersistentCacheTaskContentRequest::chunk(Bytes::from_static(b"over UDS")),
                ]),
            )
            .await
            .unwrap();
        assert_eq!(task.id, "streamed-task");
        assert_eq!(task.content_length, content.len() as u64);
        server.abort();
    }
}
