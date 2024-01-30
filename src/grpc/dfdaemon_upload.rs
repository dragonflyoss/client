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

use crate::config::dfdaemon::Config;
use crate::grpc::dfdaemon_download::DfdaemonDownloadClient;
use crate::shutdown;
use crate::storage;
use crate::task;
use crate::Result as ClientResult;
use dragonfly_api::common::v2::Piece;
use dragonfly_api::dfdaemon::v2::{
    dfdaemon_upload_client::DfdaemonUploadClient as DfdaemonUploadGRPCClient,
    dfdaemon_upload_server::{DfdaemonUpload, DfdaemonUploadServer as DfdaemonUploadGRPCServer},
    DownloadPieceRequest, DownloadPieceResponse, DownloadTaskRequest, SyncPiecesRequest,
    SyncPiecesResponse, TriggerDownloadTaskRequest,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
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
            config: config.clone(),
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
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

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
                    tokio::time::sleep(storage::DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL).await;
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
                error!("read piece content from local storage: {}", err);
                Status::internal(err.to_string())
            })?;

        // Read the content of the piece.
        let mut content = Vec::new();
        reader.read_to_end(&mut content).await.map_err(|err| {
            error!("read piece content: {}", err);
            Status::internal(err.to_string())
        })?;

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

    // trigger_download_task triggers the download task.
    #[instrument(skip_all, fields(host_id, url))]
    async fn trigger_download_task(
        &self,
        request: Request<TriggerDownloadTaskRequest>,
    ) -> Result<Response<()>, Status> {
        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the download from the request.
        let download = request.download.ok_or_else(|| {
            error!("download not found");
            Status::invalid_argument("download not found")
        })?;

        // Span record the host id and download url.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("url", download.url.as_str());

        // Initialize the dfdaemon download client.
        let dfdaemon_download_client =
            DfdaemonDownloadClient::new_unix(self.config.download.server.socket_path.clone())
                .await
                .map_err(|err| {
                    error!("create dfdaemon download client: {}", err);
                    Status::internal(format!("create dfdaemon download client: {}", err))
                })?;

        // Download task by dfdaemon download client.
        let response = dfdaemon_download_client
            .download_task(DownloadTaskRequest {
                download: Some(download),
            })
            .await
            .map_err(|err| {
                error!("download task: {}", err);
                Status::internal(format!("download task: {}", err))
            })?;

        // Spawn to handle the download task.
        tokio::spawn(
            async move {
                let mut out_stream = response.into_inner();
                loop {
                    match out_stream.message().await {
                        Ok(Some(message)) => info!("download piece finished {:?}", message),
                        Ok(None) => {
                            info!("download task finished");
                            return;
                        }
                        Err(err) => {
                            error!("download piece failed: {}", err);
                            return;
                        }
                    }
                }
            }
            .in_current_span(),
        );

        Ok(Response::new(()))
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
            })?;
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
    pub async fn trigger_download_task(
        &self,
        request: TriggerDownloadTaskRequest,
    ) -> Result<Response<()>, Status> {
        let request = Self::make_request(request);
        let response = self.client.clone().trigger_download_task(request).await?;
        Ok(response)
    }

    // make_request creates a new request with timeout.
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}
