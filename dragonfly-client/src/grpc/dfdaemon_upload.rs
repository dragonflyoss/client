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
    collect_download_task_started_metrics, collect_upload_piece_failure_metrics,
    collect_upload_piece_finished_metrics, collect_upload_piece_started_metrics,
};
use crate::shutdown;
use crate::task;
use dragonfly_api::common::v2::Piece;
use dragonfly_api::dfdaemon::v2::{
    dfdaemon_upload_client::DfdaemonUploadClient as DfdaemonUploadGRPCClient,
    dfdaemon_upload_server::{DfdaemonUpload, DfdaemonUploadServer as DfdaemonUploadGRPCServer},
    DownloadPieceRequest, DownloadPieceResponse, DownloadTaskRequest, DownloadTaskResponse,
    SyncPiecesRequest, SyncPiecesResponse,
};
use dragonfly_api::errordetails::v2::Http;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error as ClientError, Result as ClientResult,
};
use dragonfly_client_util::http::{
    get_range, hashmap_to_reqwest_headermap, reqwest_headermap_to_hashmap,
};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    transport::{Channel, Server},
    Code, Request, Response, Status,
};
use tracing::{error, info, instrument, Instrument, Span};

// DfdaemonUploadServer is the grpc server of the upload.
pub struct DfdaemonUploadServer {
    // addr is the address of the grpc server.
    addr: SocketAddr,

    // service is the grpc service of the dfdaemon upload.
    service: DfdaemonUploadGRPCServer<DfdaemonUploadServerHandler>,

    // shutdown is used to shutdown the grpc server.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the grpc server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

// DfdaemonUploadServer implements the grpc server of the upload.
impl DfdaemonUploadServer {
    // new creates a new DfdaemonUploadServer.
    pub fn new(
        config: Arc<Config>,
        addr: SocketAddr,
        task: Arc<task::Task>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        // Initialize the grpc service.
        let service = DfdaemonUploadGRPCServer::new(DfdaemonUploadServerHandler {
            socket_path: config.download.server.socket_path.clone(),
            task,
        })
        .max_decoding_message_size(usize::MAX);

        Self {
            addr,
            service,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    // run starts the upload server.
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

        // Set the serving status of the upload grpc server.
        health_reporter
            .set_serving::<DfdaemonUploadGRPCServer<DfdaemonUploadServerHandler>>()
            .await;

        // Start upload grpc server.
        info!("upload server listening on {}", self.addr);
        Server::builder()
            .add_service(reflection.clone())
            .add_service(health_service)
            .add_service(self.service.clone())
            .serve_with_shutdown(self.addr, async move {
                // Upload grpc server shutting down with signals.
                let _ = shutdown.recv().await;
                info!("upload grpc server shutting down");
            })
            .await
            .unwrap();
    }
}

// DfdaemonUploadServerHandler is the handler of the dfdaemon upload grpc service.
pub struct DfdaemonUploadServerHandler {
    // socket_path is the path of the unix domain socket.
    socket_path: PathBuf,

    // task is the task manager.
    task: Arc<task::Task>,
}

// DfdaemonUploadServerHandler implements the dfdaemon upload grpc service.
#[tonic::async_trait]
impl DfdaemonUpload for DfdaemonUploadServerHandler {
    // SyncPiecesStream is the stream of the sync pieces response.
    type SyncPiecesStream = ReceiverStream<Result<SyncPiecesResponse, Status>>;

    // get_piece_numbers gets the piece numbers.
    #[instrument(skip_all, fields(host_id, task_id))]
    async fn sync_pieces(
        &self,
        request: Request<SyncPiecesRequest>,
    ) -> Result<Response<Self::SyncPiecesStream>, Status> {
        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from tae request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.clone());
        Span::current().record("task_id", task_id.clone());

        // Get the interested piece numbers from the request.
        let mut interested_piece_numbers = request.interested_piece_numbers.clone();

        // Clone the task.
        let task_manager = self.task.clone();

        // Initialize stream channel.
        let (out_stream_tx, out_stream_rx) = mpsc::channel(1024);
        tokio::spawn(
            async move {
                loop {
                    let mut has_started_piece = false;
                    let mut finished_piece_numbers = Vec::new();
                    for interested_piece_number in interested_piece_numbers.iter() {
                        let piece = match task_manager
                            .piece
                            .get(task_id.as_str(), *interested_piece_number)
                        {
                            Ok(Some(piece)) => piece,
                            Ok(None) => continue,
                            Err(err) => {
                                error!("get piece metadata: {}", err);
                                out_stream_tx
                                    .send(Err(Status::internal(err.to_string())))
                                    .await
                                    .unwrap_or_else(|err| {
                                        error!("send piece metadata to stream: {}", err);
                                    });

                                drop(out_stream_tx);
                                return;
                            }
                        };

                        // Send the piece metadata to the stream.
                        if piece.is_finished() {
                            out_stream_tx
                                .send(Ok(SyncPiecesResponse {
                                    number: piece.number,
                                    offset: piece.offset,
                                    length: piece.length,
                                }))
                                .await
                                .unwrap_or_else(|err| {
                                    error!("send finished pieces to stream: {}", err);
                                });
                            info!("send finished piece {}", piece.number);

                            // Add the finished piece number to the finished piece numbers.
                            finished_piece_numbers.push(piece.number);
                            continue;
                        }

                        // Check whether the piece is started.
                        if piece.is_started() {
                            has_started_piece = true;
                        }
                    }

                    // Remove the finished piece numbers from the interested piece numbers.
                    interested_piece_numbers
                        .retain(|number| !finished_piece_numbers.contains(number));

                    // If all the interested pieces are finished, return.
                    if interested_piece_numbers.is_empty() {
                        info!("all the interested pieces are finished");
                        drop(out_stream_tx);
                        return;
                    }

                    // If there is no started piece, return.
                    if !has_started_piece {
                        info!("there is no started piece");
                        drop(out_stream_tx);
                        return;
                    }

                    // Wait for the piece to be finished.
                    tokio::time::sleep(
                        dragonfly_client_storage::DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL,
                    )
                    .await;
                }
            }
            .in_current_span(),
        );

        Ok(Response::new(ReceiverStream::new(out_stream_rx)))
    }

    // sync_pieces syncs the pieces.
    #[instrument(skip_all, fields(host_id, task_id, piece_number))]
    async fn download_piece(
        &self,
        request: Request<DownloadPieceRequest>,
    ) -> Result<Response<DownloadPieceResponse>, Status> {
        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Get the interested piece number from the request.
        let piece_number = request.piece_number;

        // Span record the host id, task id and piece number.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record("piece_number", piece_number);

        // Get the piece metadata from the local storage.
        let piece = self
            .task
            .piece
            .get(task_id.as_str(), piece_number)
            .map_err(|err| {
                error!("get piece metadata from local storage: {}", err);
                Status::internal(err.to_string())
            })?
            .ok_or_else(|| {
                error!("piece metadata not found");
                Status::not_found("piece metadata not found")
            })?;

        // Collect upload piece started metrics.
        collect_upload_piece_started_metrics();

        // Get the piece content from the local storage.
        let mut reader = self
            .task
            .piece
            .upload_from_local_peer_into_async_read(
                task_id.as_str(),
                piece_number,
                piece.length,
                None,
                false,
            )
            .await
            .map_err(|err| {
                // Collect upload piece failure metrics.
                collect_upload_piece_failure_metrics();

                error!("read piece content from local storage: {}", err);
                Status::internal(err.to_string())
            })?;

        // Read the content of the piece.
        let mut content = Vec::new();
        reader.read_to_end(&mut content).await.map_err(|err| {
            // Collect upload piece failure metrics.
            collect_upload_piece_failure_metrics();

            error!("read piece content: {}", err);
            Status::internal(err.to_string())
        })?;

        // Collect upload piece finished metrics.
        collect_upload_piece_finished_metrics();

        // Return the piece.
        Ok(Response::new(DownloadPieceResponse {
            piece: Some(Piece {
                number: piece.number,
                parent_id: piece.parent_id,
                offset: piece.offset,
                length: piece.length,
                digest: piece.digest,
                content: Some(content),
                traffic_type: None,
                cost: None,
                created_at: None,
            }),
        }))
    }

    // DownloadTaskStream is the stream of the download task response.
    type DownloadTaskStream = ReceiverStream<Result<DownloadTaskResponse, Status>>;

    // download_task downloads the task.
    #[instrument(skip_all, fields(host_id, task_id, peer_id))]
    async fn download_task(
        &self,
        request: Request<DownloadTaskRequest>,
    ) -> Result<Response<Self::DownloadTaskStream>, Status> {
        info!("download task in upload server");

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
                                        error!("send download progress error: {:?}", err);
                                    });
                            };
                        }
                    }
                    Err(e) => {
                        // Download task failed.
                        task_manager
                            .download_failed(task_id.as_str())
                            .await
                            .unwrap_or_else(|err| {
                                error!("download task failed: {}", err);
                            });

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
}

// DfdaemonUploadClient is a wrapper of DfdaemonUploadGRPCClient.
#[derive(Clone)]
pub struct DfdaemonUploadClient {
    // client is the grpc client of the dfdaemon upload.
    pub client: DfdaemonUploadGRPCClient<Channel>,
}

// DfdaemonUploadClient implements the dfdaemon upload grpc client.
impl DfdaemonUploadClient {
    // new creates a new DfdaemonUploadClient.
    pub async fn new(addr: String) -> ClientResult<Self> {
        let channel = Channel::from_static(Box::leak(addr.clone().into_boxed_str()))
            .connect_timeout(super::CONNECT_TIMEOUT)
            .connect()
            .await
            .map_err(|err| {
                error!("connect to {} failed: {}", addr, err);
                err
            })
            .or_err(ErrorType::ConnectError)?;
        let client = DfdaemonUploadGRPCClient::new(channel).max_decoding_message_size(usize::MAX);
        Ok(Self { client })
    }

    // get_piece_numbers gets the piece numbers.
    #[instrument(skip_all)]
    pub async fn sync_pieces(
        &self,
        request: SyncPiecesRequest,
    ) -> ClientResult<tonic::Response<tonic::codec::Streaming<SyncPiecesResponse>>> {
        let request = Self::make_request(request);
        let response = self.client.clone().sync_pieces(request).await?;
        Ok(response)
    }

    // sync_pieces syncs the pieces.
    #[instrument(skip_all)]
    pub async fn download_piece(
        &self,
        request: DownloadPieceRequest,
        timeout: Duration,
    ) -> ClientResult<DownloadPieceResponse> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(timeout);

        let response = self.client.clone().download_piece(request).await?;
        Ok(response.into_inner())
    }

    // trigger_download_task triggers the download task.
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

    // make_request creates a new request with timeout.
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}
