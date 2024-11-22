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

use crate::metrics::{
    collect_delete_host_failure_metrics, collect_delete_host_started_metrics,
    collect_delete_task_failure_metrics, collect_delete_task_started_metrics,
    collect_download_task_failure_metrics, collect_download_task_finished_metrics,
    collect_download_task_started_metrics, collect_stat_task_failure_metrics,
    collect_stat_task_started_metrics, collect_upload_task_failure_metrics,
    collect_upload_task_finished_metrics, collect_upload_task_started_metrics,
};
use crate::resource::{persistent_cache_task, task};
use crate::shutdown;
use dragonfly_api::common::v2::{PersistentCacheTask, Priority, Task, TaskType};
use dragonfly_api::dfdaemon::v2::{
    dfdaemon_download_client::DfdaemonDownloadClient as DfdaemonDownloadGRPCClient,
    dfdaemon_download_server::{
        DfdaemonDownload, DfdaemonDownloadServer as DfdaemonDownloadGRPCServer,
    },
    DeletePersistentCacheTaskRequest, DeleteTaskRequest, DownloadPersistentCacheTaskRequest,
    DownloadPersistentCacheTaskResponse, DownloadTaskRequest, DownloadTaskResponse,
    StatPersistentCacheTaskRequest, StatTaskRequest as DfdaemonStatTaskRequest,
    UploadPersistentCacheTaskRequest,
};
use dragonfly_api::errordetails::v2::Backend;
use dragonfly_api::scheduler::v2::DeleteHostRequest as SchedulerDeleteHostRequest;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error as ClientError, Result as ClientResult,
};
use dragonfly_client_util::{
    digest::{calculate_file_hash, Algorithm},
    http::{get_range, hashmap_to_headermap, headermap_to_hashmap},
};
use hyper_util::rt::TokioIo;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio::sync::Barrier;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{
    transport::{Channel, Endpoint, Server, Uri},
    Code, Request, Response, Status,
};
use tower::service_fn;
use tracing::{debug, error, info, instrument, Instrument, Span};

/// DfdaemonDownloadServer is the grpc unix server of the download.
pub struct DfdaemonDownloadServer {
    /// socket_path is the path of the unix domain socket.
    socket_path: PathBuf,

    /// service is the grpc service of the dfdaemon.
    service: DfdaemonDownloadGRPCServer<DfdaemonDownloadServerHandler>,

    /// shutdown is used to shutdown the grpc server.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the grpc server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// DfdaemonDownloadServer implements the grpc server of the download.
impl DfdaemonDownloadServer {
    /// new creates a new DfdaemonServer.
    #[instrument(skip_all)]
    pub fn new(
        socket_path: PathBuf,
        task: Arc<task::Task>,
        persistent_cache_task: Arc<persistent_cache_task::PersistentCacheTask>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        // Initialize the grpc service.
        let service = DfdaemonDownloadGRPCServer::new(DfdaemonDownloadServerHandler {
            socket_path: socket_path.clone(),
            task,
            persistent_cache_task,
        })
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);

        Self {
            socket_path,
            service,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// run starts the download server with unix domain socket.
    #[instrument(skip_all)]
    pub async fn run(&mut self, grpc_server_started_barrier: Arc<Barrier>) -> ClientResult<()> {
        // Register the reflection service.
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(dragonfly_api::FILE_DESCRIPTOR_SET)
            .build_v1()?;

        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Initialize health reporter.
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

        // Set the serving status of the download grpc server.
        health_reporter
            .set_serving::<DfdaemonDownloadGRPCServer<DfdaemonDownloadServerHandler>>()
            .await;

        // Start download grpc server with unix domain socket.
        fs::create_dir_all(self.socket_path.parent().unwrap()).await?;
        let uds = UnixListener::bind(&self.socket_path)?;
        let uds_stream = UnixListenerStream::new(uds);

        let server = Server::builder()
            .max_frame_size(super::MAX_FRAME_SIZE)
            .tcp_keepalive(Some(super::TCP_KEEPALIVE))
            .http2_keepalive_interval(Some(super::HTTP2_KEEP_ALIVE_INTERVAL))
            .http2_keepalive_timeout(Some(super::HTTP2_KEEP_ALIVE_TIMEOUT))
            .add_service(reflection.clone())
            .add_service(health_service)
            .add_service(self.service.clone())
            .serve_with_incoming_shutdown(uds_stream, async move {
                // Download grpc server shutting down with signals.
                let _ = shutdown.recv().await;
                info!("download grpc server shutting down");
            });

        // Notify the grpc server is started.
        grpc_server_started_barrier.wait().await;

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

/// DfdaemonDownloadServerHandler is the handler of the dfdaemon download grpc service.
pub struct DfdaemonDownloadServerHandler {
    /// socket_path is the path of the unix domain socket.
    socket_path: PathBuf,

    /// task is the task manager.
    task: Arc<task::Task>,

    /// persistent_cache_task is the persistent cache task manager.
    persistent_cache_task: Arc<persistent_cache_task::PersistentCacheTask>,
}

/// DfdaemonDownloadServerHandler implements the dfdaemon download grpc service.
#[tonic::async_trait]
impl DfdaemonDownload for DfdaemonDownloadServerHandler {
    /// DownloadTaskStream is the stream of the download task response.
    type DownloadTaskStream = ReceiverStream<Result<DownloadTaskResponse, Status>>;

    /// download_task tells the dfdaemon to download the task.
    #[instrument(skip_all, fields(host_id, task_id, peer_id))]
    async fn download_task(
        &self,
        request: Request<DownloadTaskRequest>,
    ) -> Result<Response<Self::DownloadTaskStream>, Status> {
        debug!("download task in download server");

        // Record the start time.
        let start_time = Instant::now();

        // Clone the request.
        let request = request.into_inner();

        // Check whether the download is empty.
        let mut download = request.download.ok_or_else(|| {
            error!("missing download");
            Status::invalid_argument("missing download")
        })?;

        // Generate the task id.
        let task_id = self
            .task
            .id_generator
            .task_id(
                download.url.as_str(),
                download.tag.as_deref(),
                download.application.as_deref(),
                download.filtered_query_params.clone(),
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

        // Download task started.
        info!("download task started: {:?}", download);
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

        // Clone the task.
        let task_manager = self.task.clone();

        // Check whether the content length is empty.
        let Some(content_length) = task.content_length() else {
            // Download task failed.
            task_manager
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

            error!("missing content length in the response");
            return Err(Status::internal("missing content length in the response"));
        };
        info!(
            "content length {}, piece length {}",
            content_length,
            task.piece_length().unwrap_or_default()
        );

        // Download's range priority is higher than the request header's range.
        // If download protocol is http, use the range of the request header.
        // If download protocol is not http, use the range of the download.
        if download.range.is_none() {
            // Convert the header.
            let request_header = match hashmap_to_headermap(&download.request_header) {
                Ok(header) => header,
                Err(e) => {
                    // Download task failed.
                    task_manager
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

                    error!("convert header: {}", e);
                    return Err(Status::invalid_argument(e.to_string()));
                }
            };

            download.range = match get_range(&request_header, content_length) {
                Ok(range) => range,
                Err(e) => {
                    // Download task failed.
                    task_manager
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

                    error!("get range failed: {}", e);
                    return Err(Status::failed_precondition(e.to_string()));
                }
            };
        }

        // Initialize stream channel.
        let download_clone = download.clone();
        let task_manager_clone = task_manager.clone();
        let task_clone = task.clone();
        let (out_stream_tx, out_stream_rx) = mpsc::channel(10);
        tokio::spawn(
            async move {
                match task_manager_clone
                    .download(
                        task_clone.clone(),
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
                        if download_clone.range.is_none() {
                            if let Err(err) =
                                task_manager_clone.download_finished(task_clone.id.as_str())
                            {
                                error!("download task finished: {}", err);
                            }
                        }

                        // Check whether the output path is empty. If output path is empty,
                        // should not hard link or copy the task content to the destination.
                        if let Some(output_path) = download_clone.output_path.clone() {
                            // Hard link or copy the task content to the destination.
                            if let Err(err) = task_manager_clone
                                .hard_link_or_copy(
                                    task_clone,
                                    Path::new(output_path.as_str()),
                                    download_clone.range,
                                )
                                .await
                            {
                                error!("hard link or copy task: {}", err);
                                out_stream_tx
                                    .send(Err(Status::internal(err.to_string())))
                                    .await
                                    .unwrap_or_else(|err| {
                                        error!("send download progress error: {:?}", err)
                                    });
                            };
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
                                out_stream_tx
                                    .send(Err(Status::with_details(
                                        Code::Internal,
                                        err.to_string(),
                                        json.into(),
                                    )))
                                    .await
                                    .unwrap_or_else(|err| {
                                        error!("send download progress error: {:?}", err)
                                    });
                            }
                            Err(err) => {
                                out_stream_tx
                                    .send(Err(Status::internal(err.to_string())))
                                    .await
                                    .unwrap_or_else(|err| {
                                        error!("send download progress error: {:?}", err)
                                    });
                                error!("serialize error: {}", err);
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

                        out_stream_tx
                            .send(Err(Status::internal(err.to_string())))
                            .await
                            .unwrap_or_else(|err| {
                                error!("send download progress error: {:?}", err)
                            });
                    }
                }

                drop(out_stream_tx);
            }
            .in_current_span(),
        );

        // If prefetch flag is true, prefetch the full task.
        if download.prefetch {
            info!("try to prefetch task");
            match task_manager.prefetch_task_started(task_id.as_str()).await {
                Ok(_) => {
                    info!("prefetch task started");
                    let socket_path = self.socket_path.clone();
                    let task_manager_clone = task_manager.clone();
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
                Err(ClientError::InvalidState(_)) => info!("task is already prefetched"),
                Err(err) => {
                    error!("prefetch task started: {}", err);
                }
            }
        }

        Ok(Response::new(ReceiverStream::new(out_stream_rx)))
    }

    /// stat_task gets the status of the task.
    #[instrument(skip_all, fields(host_id, task_id))]
    async fn stat_task(
        &self,
        request: Request<DfdaemonStatTaskRequest>,
    ) -> Result<Response<Task>, Status> {
        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());

        // Collect the stat task metrics.
        collect_stat_task_started_metrics(TaskType::Standard as i32);

        // Get the task from the scheduler.
        let task = self
            .task
            .stat(task_id.as_str(), host_id.as_str())
            .await
            .map_err(|err| {
                // Collect the stat task failure metrics.
                collect_stat_task_failure_metrics(TaskType::Standard as i32);

                error!("stat task: {}", err);
                Status::internal(err.to_string())
            })?;

        Ok(Response::new(task))
    }

    /// delete_task calls the dfdaemon to delete the task.
    #[instrument(skip_all, fields(host_id, task_id))]
    async fn delete_task(
        &self,
        request: Request<DeleteTaskRequest>,
    ) -> Result<Response<()>, Status> {
        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());

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
                Status::internal(err.to_string())
            })?;

        Ok(Response::new(()))
    }

    /// delete_host calls the scheduler to delete the host.
    #[instrument(skip_all, fields(host_id))]
    async fn delete_host(&self, _: Request<()>) -> Result<Response<()>, Status> {
        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Span record the host id.
        Span::current().record("host_id", host_id.as_str());

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

    /// DownloadPersistentCacheTaskStream is the stream of the download persistent cache task response.
    type DownloadPersistentCacheTaskStream =
        ReceiverStream<Result<DownloadPersistentCacheTaskResponse, Status>>;

    /// download_persistent_cache_task downloads the persistent cache task.
    #[instrument(skip_all, fields(host_id, task_id, peer_id))]
    async fn download_persistent_cache_task(
        &self,
        request: Request<DownloadPersistentCacheTaskRequest>,
    ) -> Result<Response<Self::DownloadPersistentCacheTaskStream>, Status> {
        info!("download persistent cache task in download server");

        // Record the start time.
        let start_time = Instant::now();

        // Clone the request.
        let request = request.into_inner();

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

        // Download task started.
        info!("download persistent cache task started: {:?}", request);
        let task = match self
            .persistent_cache_task
            .download_started(task_id.as_str(), host_id.as_str(), request.clone())
            .await
        {
            Err(ClientError::BackendError(err)) => {
                error!("download cache started failed by error: {}", err);
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
                    Err(e) => {
                        error!("serialize error: {}", e);
                        return Err(Status::internal(e.to_string()));
                    }
                }
            }
            Err(err) => {
                error!("download started failed: {}", err);
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
        info!(
            "content length {}, piece length {}",
            task.content_length(),
            task.piece_length()
        );

        // Clone the persistent cache task.
        let task_manager = self.persistent_cache_task.clone();

        // Initialize stream channel.
        let request_clone = request.clone();
        let task_manager_clone = task_manager.clone();
        let task_clone = task.clone();
        let (out_stream_tx, out_stream_rx) = mpsc::channel(10);
        tokio::spawn(
            async move {
                match task_manager_clone
                    .download(
                        task_clone.clone(),
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
                        info!("download persistent cache task succeeded");

                        // Hard link or copy the persistent cache task content to the destination.
                        if let Err(err) = task_manager_clone
                            .hard_link_or_copy(
                                task_clone,
                                Path::new(request_clone.output_path.as_str()),
                            )
                            .await
                        {
                            error!("hard link or copy persistent cache task: {}", err);
                            out_stream_tx
                                .send(Err(Status::internal(err.to_string())))
                                .await
                                .unwrap_or_else(|err| {
                                    error!("send download progress error: {:?}", err)
                                });
                        };
                    }
                    Err(e) => {
                        // Download task failed.
                        task_manager_clone
                            .download_failed(task_clone.id.as_str())
                            .await
                            .unwrap_or_else(|err| {
                                error!("download persistent cache task failed: {}", err)
                            });

                        // Collect download task failure metrics.
                        collect_download_task_failure_metrics(
                            task_type,
                            request.tag.clone().unwrap_or_default().as_str(),
                            request.application.clone().unwrap_or_default().as_str(),
                            task_priority.to_string().as_str(),
                        );

                        error!("download failed: {}", e);
                    }
                }

                drop(out_stream_tx);
            }
            .in_current_span(),
        );

        Ok(Response::new(ReceiverStream::new(out_stream_rx)))
    }

    /// upload_persistent_cache_task uploads the persistent cache task.
    #[instrument(skip_all, fields(host_id, task_id, peer_id))]
    async fn upload_persistent_cache_task(
        &self,
        request: Request<UploadPersistentCacheTaskRequest>,
    ) -> Result<Response<PersistentCacheTask>, Status> {
        // Record the start time.
        let start_time = Instant::now();

        // Clone the request.
        let request = request.into_inner();
        let path = Path::new(request.path.as_str());

        // Calculate the file hash by the blake3 algorithm.
        let digest = calculate_file_hash(Algorithm::Blake3, path).map_err(|err| {
            let error_message = format!("calculate file {} hash: {}", request.path.as_str(), err);
            error!(error_message);
            Status::with_details(Code::Internal, err.to_string(), error_message.into())
        })?;
        info!("calculate file digest: {}", digest);

        // Generate the task id.
        let task_id = self
            .task
            .id_generator
            .persistent_cache_task_id(
                &path.to_path_buf(),
                request.tag.as_deref(),
                request.application.as_deref(),
            )
            .map_err(|err| {
                error!("generate task id: {}", err);
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

        // Collect upload task started metrics.
        collect_upload_task_started_metrics(
            TaskType::PersistentCache as i32,
            request.tag.clone().unwrap_or_default().as_str(),
            request.application.clone().unwrap_or_default().as_str(),
        );

        // Create the persistent cache task to local storage.
        let task = match self
            .persistent_cache_task
            .create_persistent(
                task_id.as_str(),
                host_id.as_str(),
                peer_id.as_str(),
                path,
                digest.to_string().as_str(),
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

    /// stat_persistent_cache_task stats the persistent cache task.
    #[instrument(skip_all, fields(host_id, task_id))]
    async fn stat_persistent_cache_task(
        &self,
        request: Request<StatPersistentCacheTaskRequest>,
    ) -> Result<Response<PersistentCacheTask>, Status> {
        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());

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

    /// delete_persistent_cache_task deletes the persistent cache task.
    #[instrument(skip_all, fields(host_id, task_id))]
    async fn delete_persistent_cache_task(
        &self,
        request: Request<DeletePersistentCacheTaskRequest>,
    ) -> Result<Response<()>, Status> {
        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());

        // Collect the delete persistent cache task started metrics.
        collect_delete_task_started_metrics(TaskType::PersistentCache as i32);
        self.persistent_cache_task.delete(task_id.as_str()).await;
        Ok(Response::new(()))
    }
}

/// DfdaemonDownloadClient is a wrapper of DfdaemonDownloadGRPCClient.
#[derive(Clone)]
pub struct DfdaemonDownloadClient {
    /// client is the grpc client of the dfdaemon.
    pub client: DfdaemonDownloadGRPCClient<Channel>,
}

/// DfdaemonDownloadClient implements the grpc client of the dfdaemon download.
impl DfdaemonDownloadClient {
    /// new_unix creates a new DfdaemonDownloadClient with unix domain socket.
    #[instrument(skip_all)]
    pub async fn new_unix(socket_path: PathBuf) -> ClientResult<Self> {
        // Ignore the uri because it is not used.
        let channel = Endpoint::try_from("http://[::]:50051")
            .unwrap()
            .buffer_size(super::BUFFER_SIZE)
            .connect_timeout(super::CONNECT_TIMEOUT)
            .timeout(super::REQUEST_TIMEOUT)
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
            .map_err(|err| {
                error!("connect failed: {}", err);
                err
            })
            .or_err(ErrorType::ConnectError)?;
        let client = DfdaemonDownloadGRPCClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        Ok(Self { client })
    }

    /// download_task tells the dfdaemon to download the task.
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

    /// stat_task gets the status of the task.
    #[instrument(skip_all)]
    pub async fn stat_task(&self, request: DfdaemonStatTaskRequest) -> ClientResult<Task> {
        let request = Self::make_request(request);
        let response = self.client.clone().stat_task(request).await?;
        Ok(response.into_inner())
    }

    /// delete_task tells the dfdaemon to delete the task.
    #[instrument(skip_all)]
    pub async fn delete_task(&self, request: DeleteTaskRequest) -> ClientResult<()> {
        let request = Self::make_request(request);
        self.client.clone().delete_task(request).await?;
        Ok(())
    }

    /// download_persistent_cache_task downloads the persistent cache task.
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

    /// upload_persistent_cache_task uploads the persistent cache task.
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

    /// stat_persistent_cache_task stats the persistent cache task.
    #[instrument(skip_all)]
    pub async fn stat_persistent_cache_task(
        &self,
        request: StatPersistentCacheTaskRequest,
    ) -> ClientResult<PersistentCacheTask> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::CONNECT_TIMEOUT);

        let response = self
            .client
            .clone()
            .stat_persistent_cache_task(request)
            .await?;
        Ok(response.into_inner())
    }

    /// delete_persistent_cache_task deletes the persistent cache task.
    #[instrument(skip_all)]
    pub async fn delete_persistent_cache_task(
        &self,
        request: DeletePersistentCacheTaskRequest,
    ) -> ClientResult<()> {
        let request = Self::make_request(request);
        let _response = self
            .client
            .clone()
            .delete_persistent_cache_task(request)
            .await?;
        Ok(())
    }

    /// make_request creates a new request with timeout.
    #[instrument(skip_all)]
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}
