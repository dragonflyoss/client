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
    collect_download_task_failure_metrics, collect_download_task_finished_metrics,
    collect_download_task_started_metrics,
};
use crate::shutdown;
use crate::task;
use dragonfly_api::common::v2::Task;
use dragonfly_api::dfdaemon::v2::{
    dfdaemon_download_client::DfdaemonDownloadClient as DfdaemonDownloadGRPCClient,
    dfdaemon_download_server::{
        DfdaemonDownload, DfdaemonDownloadServer as DfdaemonDownloadGRPCServer,
    },
    DeleteTaskRequest, DownloadTaskRequest, DownloadTaskResponse,
    StatTaskRequest as DfdaemonStatTaskRequest, UploadTaskRequest,
};
use dragonfly_api::errordetails::v2::Http;
use dragonfly_api::scheduler::v2::{
    LeaveHostRequest as SchedulerLeaveHostRequest, StatTaskRequest as SchedulerStatTaskRequest,
};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error as ClientError, Result as ClientResult,
};
use dragonfly_client_util::http::{
    get_range, hashmap_to_reqwest_headermap, reqwest_headermap_to_hashmap,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{
    transport::{Channel, Endpoint, Server, Uri},
    Code, Request, Response, Status,
};
use tower::service_fn;
use tracing::{error, info, instrument, Instrument, Span};

// DfdaemonDownloadServer is the grpc unix server of the download.
pub struct DfdaemonDownloadServer {
    // socket_path is the path of the unix domain socket.
    socket_path: PathBuf,

    // service is the grpc service of the dfdaemon.
    service: DfdaemonDownloadGRPCServer<DfdaemonDownloadServerHandler>,

    // shutdown is used to shutdown the grpc server.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the grpc server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

// DfdaemonDownloadServer implements the grpc server of the download.
impl DfdaemonDownloadServer {
    // new creates a new DfdaemonServer.
    pub fn new(
        socket_path: PathBuf,
        task: Arc<task::Task>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        // Initialize the grpc service.
        let service = DfdaemonDownloadGRPCServer::new(DfdaemonDownloadServerHandler {
            socket_path: socket_path.clone(),
            task: task.clone(),
        })
        .max_decoding_message_size(usize::MAX);

        Self {
            socket_path,
            service,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    // run starts the download server with unix domain socket.
    #[instrument(skip_all)]
    pub async fn run(&mut self) {
        // Register the reflection service.
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(dragonfly_api::FILE_DESCRIPTOR_SET)
            .build()
            .unwrap();

        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Initialize health reporter.
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

        // Set the serving status of the download grpc server.
        health_reporter
            .set_serving::<DfdaemonDownloadGRPCServer<DfdaemonDownloadServerHandler>>()
            .await;

        // Start download grpc server with unix domain socket.
        info!(
            "download server listening on {}",
            self.socket_path.display()
        );
        fs::create_dir_all(self.socket_path.parent().unwrap())
            .await
            .unwrap();
        let uds = UnixListener::bind(&self.socket_path).unwrap();
        let uds_stream = UnixListenerStream::new(uds);
        Server::builder()
            .add_service(reflection.clone())
            .add_service(health_service)
            .add_service(self.service.clone())
            .serve_with_incoming_shutdown(uds_stream, async move {
                // Download grpc server shutting down with signals.
                let _ = shutdown.recv().await;
                info!("download grpc server shutting down");
            })
            .await
            .unwrap();

        // Remove the unix domain socket file.
        fs::remove_file(&self.socket_path).await.unwrap();
        info!("remove the unix domain socket file of the download server");
    }
}

// DfdaemonDownloadServerHandler is the handler of the dfdaemon download grpc service.
pub struct DfdaemonDownloadServerHandler {
    // socket_path is the path of the unix domain socket.
    socket_path: PathBuf,

    // task is the task manager.
    task: Arc<task::Task>,
}

// DfdaemonDownloadServerHandler implements the dfdaemon download grpc service.
#[tonic::async_trait]
impl DfdaemonDownload for DfdaemonDownloadServerHandler {
    // DownloadTaskStream is the stream of the download task response.
    type DownloadTaskStream = ReceiverStream<Result<DownloadTaskResponse, Status>>;

    // download_task tells the dfdaemon to download the task.
    #[instrument(skip_all, fields(host_id, task_id, peer_id))]
    async fn download_task(
        &self,
        request: Request<DownloadTaskRequest>,
    ) -> Result<Response<Self::DownloadTaskStream>, Status> {
        info!("download task in download server");

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
                download.digest.as_deref(),
                download.tag.as_deref(),
                download.application.as_deref(),
                download.piece_length,
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
            .download_started(task_id.as_str(), peer_id.as_str(), download.clone())
            .await
        {
            Err(ClientError::HTTP(err)) => {
                error!("download started failed by HTTP error: {}", err);
                match serde_json::to_vec::<Http>(&Http {
                    header: reqwest_headermap_to_hashmap(&err.header),
                    status_code: err.status_code.as_u16() as i32,
                }) {
                    Ok(json) => {
                        return Err(Status::with_details(
                            Code::Internal,
                            err.to_string(),
                            json.into(),
                        ));
                    }
                    Err(e) => {
                        error!("serialize HTTP error: {}", e);
                        return Err(Status::internal(e.to_string()));
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
                    download.r#type.to_string().as_str(),
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
                download.r#type.to_string().as_str(),
                download.tag.clone().unwrap_or_default().as_str(),
                download.application.clone().unwrap_or_default().as_str(),
                download.priority.to_string().as_str(),
            );

            error!("missing content length in the response");
            return Err(Status::internal("missing content length in the response"));
        };

        info!("content length: {}", content_length);

        // Download's range priority is higher than the request header's range.
        // If download protocol is http, use the range of the request header.
        // If download protocol is not http, use the range of the download.
        if download.range.is_none() {
            // Convert the header.
            let request_header = match hashmap_to_reqwest_headermap(&download.request_header) {
                Ok(header) => header,
                Err(e) => {
                    // Download task failed.
                    task_manager
                        .download_failed(task_id.as_str())
                        .await
                        .unwrap_or_else(|err| error!("download task failed: {}", err));

                    // Collect download task failure metrics.
                    collect_download_task_failure_metrics(
                        download.r#type.to_string().as_str(),
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
                        download.r#type.to_string().as_str(),
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
        let task_clone = task.clone();
        let (out_stream_tx, out_stream_rx) = mpsc::channel(1024);
        tokio::spawn(
            async move {
                match task_manager
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
                            download_clone.r#type.to_string().as_str(),
                            download_clone.tag.clone().unwrap_or_default().as_str(),
                            download_clone
                                .application
                                .clone()
                                .unwrap_or_default()
                                .as_str(),
                            download_clone.priority.to_string().as_str(),
                            task_clone.content_length().unwrap_or_default(),
                            download_clone.range.clone(),
                            start_time.elapsed(),
                        );

                        // Download task succeeded.
                        info!("download task succeeded");
                        if download_clone.range.is_none() {
                            if let Err(err) = task_manager.download_finished(task_id.as_str()) {
                                error!("download task finished: {}", err);
                            }
                        }

                        // Check whether the output path is empty. If output path is empty,
                        // should not hard link or copy the task content to the destination.
                        if let Some(output_path) = download_clone.output_path.clone() {
                            // Hard link or copy the task content to the destination.
                            if let Err(err) = task_manager
                                .hard_link_or_copy(
                                    task_clone,
                                    Path::new(output_path.as_str()),
                                    download_clone.range.clone(),
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
                    Err(e) => {
                        // Download task failed.
                        task_manager
                            .download_failed(task_id.as_str())
                            .await
                            .unwrap_or_else(|err| error!("download task failed: {}", err));

                        // Collect download task failure metrics.
                        collect_download_task_failure_metrics(
                            download_clone.r#type.to_string().as_str(),
                            download_clone.tag.clone().unwrap_or_default().as_str(),
                            download_clone
                                .application
                                .clone()
                                .unwrap_or_default()
                                .as_str(),
                            download_clone.priority.to_string().as_str(),
                        );

                        error!("download failed: {}", e);
                    }
                }

                drop(out_stream_tx);
            }
            .in_current_span(),
        );

        // If prefetch flag is true, prefetch the full task.
        if download.prefetch && !task.is_finished() {
            // Prefetch the task if prefetch flag is true.
            let socket_path = self.socket_path.clone();
            tokio::spawn(
                async move {
                    info!("prefetch task started");
                    if let Err(err) = super::prefetch_task(
                        socket_path.clone(),
                        Request::new(DownloadTaskRequest {
                            download: Some(download.clone()),
                        }),
                    )
                    .await
                    {
                        error!("prefetch task failed: {}", err);
                    }
                }
                .in_current_span(),
            );
        }

        Ok(Response::new(ReceiverStream::new(out_stream_rx)))
    }

    // upload_task tells the dfdaemon to upload the task.
    #[instrument(skip_all)]
    async fn upload_task(
        &self,
        request: Request<UploadTaskRequest>,
    ) -> Result<Response<()>, Status> {
        println!("upload_task: {:?}", request);
        Err(Status::unimplemented("not implemented"))
    }

    // stat_task gets the status of the task.
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

        // Get the task from the scheduler.
        let task = self
            .task
            .scheduler_client
            .stat_task(
                task_id.as_str(),
                SchedulerStatTaskRequest {
                    id: task_id.clone(),
                },
            )
            .await
            .map_err(|e| {
                error!("stat task: {}", e);
                Status::internal(e.to_string())
            })?;

        Ok(Response::new(task))
    }

    // delete_task calls the dfdaemon to delete the task.
    #[instrument(skip_all)]
    async fn delete_task(
        &self,
        request: Request<DeleteTaskRequest>,
    ) -> Result<Response<()>, Status> {
        println!("delete_task: {:?}", request);
        Err(Status::unimplemented("not implemented"))
    }

    // leave_host calls the scheduler to leave the host.
    #[instrument(skip_all)]
    async fn leave_host(&self, _: Request<()>) -> Result<Response<()>, Status> {
        self.task
            .scheduler_client
            .leave_host(SchedulerLeaveHostRequest {
                id: self.task.id_generator.host_id(),
            })
            .await
            .map_err(|e| {
                error!("leave host: {}", e);
                Status::internal(e.to_string())
            })?;

        Ok(Response::new(()))
    }
}

// DfdaemonDownloadClient is a wrapper of DfdaemonDownloadGRPCClient.
#[derive(Clone)]
pub struct DfdaemonDownloadClient {
    // client is the grpc client of the dfdaemon.
    pub client: DfdaemonDownloadGRPCClient<Channel>,
}

// DfdaemonDownloadClient implements the grpc client of the dfdaemon download.
impl DfdaemonDownloadClient {
    // new_unix creates a new DfdaemonDownloadClient with unix domain socket.
    pub async fn new_unix(socket_path: PathBuf) -> ClientResult<Self> {
        // Ignore the uri because it is not used.
        let channel = Endpoint::try_from("http://[::]:50051")
            .unwrap()
            .connect_with_connector(service_fn(move |_: Uri| {
                UnixStream::connect(socket_path.clone())
            }))
            .await
            .map_err(|err| {
                error!("connect failed: {}", err);
                err
            })
            .or_err(ErrorType::ConnectError)?;
        let client = DfdaemonDownloadGRPCClient::new(channel).max_decoding_message_size(usize::MAX);
        Ok(Self { client })
    }

    // download_task tells the dfdaemon to download the task.
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

    // upload_task tells the dfdaemon to upload the task.
    #[instrument(skip_all)]
    pub async fn upload_task(&self, request: UploadTaskRequest) -> ClientResult<()> {
        let request = Self::make_request(request);
        self.client.clone().upload_task(request).await?;
        Ok(())
    }

    // stat_task gets the status of the task.
    #[instrument(skip_all)]
    pub async fn stat_task(&self, request: DfdaemonStatTaskRequest) -> ClientResult<Task> {
        let request = Self::make_request(request);
        let response = self.client.clone().stat_task(request).await?;
        Ok(response.into_inner())
    }

    // delete_task tells the dfdaemon to delete the task.
    #[instrument(skip_all)]
    pub async fn delete_task(&self, request: DeleteTaskRequest) -> ClientResult<()> {
        let request = Self::make_request(request);
        self.client.clone().delete_task(request).await?;
        Ok(())
    }

    // make_request creates a new request with timeout.
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}
