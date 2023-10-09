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

use crate::shutdown;
use crate::task;
use crate::{Error, Result as ClientResult};
use dragonfly_api::common::v2::Task;
use dragonfly_api::dfdaemon::v2::{
    dfdaemon_client::DfdaemonClient as DfdaemonGRPCClient,
    dfdaemon_server::{Dfdaemon, DfdaemonServer as DfdaemonGRPCServer},
    sync_pieces_request, sync_pieces_response, DeleteTaskRequest, DownloadTaskRequest,
    InterestedAllPiecesRequest, InterestedAllPiecesResponse, InterestedPiecesRequest,
    InterestedPiecesResponse, StatTaskRequest as DfdaemonStatTaskRequest, SyncPiecesRequest,
    SyncPiecesResponse, UploadTaskRequest,
};
use dragonfly_api::scheduler::v2::StatTaskRequest as SchedulerStatTaskRequest;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::codec::CompressionEncoding;
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};
use tracing::{error, info};

// DfdaemonServer is the grpc server of the dfdaemon.
pub struct DfdaemonServer {
    // addr is the address of the grpc server.
    addr: SocketAddr,

    // task is the task manager.
    task: Arc<task::Task>,

    // shutdown is used to shutdown the grpc server.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the grpc server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

// TODO Implement security feature for the dfdaemon grpc server.
// DfdaemonServer implements the grpc server of the dfdaemon.
impl DfdaemonServer {
    pub fn new(
        addr: SocketAddr,
        task: Arc<task::Task>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            addr,
            task,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    // run starts the metrics server.
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Register the reflection service.
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(dragonfly_api::FILE_DESCRIPTOR_SET)
            .build()
            .unwrap();

        // Start the grpc server.
        info!("listening on {}", self.addr);
        Server::builder()
            .add_service(reflection)
            .add_service(DfdaemonGRPCServer::new(DfdaemonServerHandler {
                task: self.task.clone(),
            }))
            .serve_with_shutdown(self.addr, async move {
                // Dfdaemon grpc server shutting down with signals.
                let _ = shutdown.recv().await;
                info!("dfdaemon grpc server shutting down");
            })
            .await
            .unwrap();
    }
}

// DfdaemonServerHandler is the handler of the dfdaemon grpc service.
pub struct DfdaemonServerHandler {
    // task is the task manager.
    task: Arc<task::Task>,
}

// TODO Implement the dfdaemon grpc service.
// DfdaemonServerHandler implements the dfdaemon grpc service.
#[tonic::async_trait]
impl Dfdaemon for DfdaemonServerHandler {
    // SyncPiecesStream is the stream of the sync pieces response.
    type SyncPiecesStream =
        Pin<Box<dyn Stream<Item = Result<SyncPiecesResponse, Status>> + Send + 'static>>;

    // sync_pieces syncs the pieces.
    async fn sync_pieces(
        &self,
        request: Request<tonic::Streaming<SyncPiecesRequest>>,
    ) -> Result<Response<Self::SyncPiecesStream>, Status> {
        let mut in_stream = request.into_inner();

        let output_stream = async_stream::try_stream! {
            while let Some(message) = in_stream.next().await {
                match message {
                    Ok(message) => {
                        let task_id = message.task_id.clone();
                        match message.request {
                            Some(sync_pieces_request::Request::InterestedPiecesRequest(
                                InterestedPiecesRequest { piece_numbers },
                            )) => {
                                for piece_number in piece_numbers {
                                    let reader = self.task.download_piece_from_local_peer(&task_id, piece_number).await;
                                    yield SyncPiecesResponse {
                                        task_id: task_id.clone(),
                                        piece_number,
                                        reader: Some(reader),
                                    };

                                }
                            }
                            Some(sync_pieces_request::Request::InterestedAllPiecesRequest(
                                InterestedAllPiecesRequest {},
                            )) => {
                            }
                            None => {
                                error!("sync pieces request is empty");
                                break;
                            }
                        }
                    }
                    Err(e) => {
                    }
                }
            }
        };

        Ok(Response::new(
            Box::pin(output_stream) as Self::SyncPiecesStream
        ))
    }

    // download_task tells the dfdaemon to download the task.
    async fn download_task(
        &self,
        request: Request<DownloadTaskRequest>,
    ) -> Result<Response<()>, Status> {
        println!("download_task: {:?}", request);
        Err(Status::unimplemented("not implemented"))
    }

    // upload_task tells the dfdaemon to upload the task.
    async fn upload_task(
        &self,
        request: Request<UploadTaskRequest>,
    ) -> Result<Response<()>, Status> {
        println!("upload_task: {:?}", request);
        Err(Status::unimplemented("not implemented"))
    }

    // stat_task gets the status of the task.
    async fn stat_task(
        &self,
        request: Request<DfdaemonStatTaskRequest>,
    ) -> Result<Response<Task>, Status> {
        let mut request = tonic::Request::new(SchedulerStatTaskRequest {
            id: request.into_inner().task_id.clone(),
        });
        request.set_timeout(super::REQUEST_TIMEOUT);

        self.task
            .scheduler_client
            .client()
            .map_err(|e| Status::internal(e.to_string()))?
            .stat_task(request)
            .await
    }

    // delete_task tells the dfdaemon to delete the task.
    async fn delete_task(
        &self,
        request: Request<DeleteTaskRequest>,
    ) -> Result<Response<()>, Status> {
        println!("delete_task: {:?}", request);
        Err(Status::unimplemented("not implemented"))
    }
}

// DfdaemonClient is a wrapper of DfdaemonGRPCClient.
#[derive(Clone)]
pub struct DfdaemonClient {
    // client is the grpc client of the dfdaemon.
    pub client: DfdaemonGRPCClient<Channel>,
}

// DfdaemonClient implements the grpc client of the dfdaemon.
impl DfdaemonClient {
    // new creates a new DfdaemonClient.
    pub async fn new(addr: String) -> ClientResult<Self> {
        let channel = Channel::from_static(Box::leak(addr.into_boxed_str()))
            .connect()
            .await?;
        let client = DfdaemonGRPCClient::new(channel)
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(usize::MAX);
        Ok(Self { client })
    }

    // sync_pieces syncs the pieces.
    pub async fn sync_pieces(
        &self,
        request: impl tonic::IntoStreamingRequest<Message = SyncPiecesRequest>,
    ) -> ClientResult<tonic::Response<tonic::codec::Streaming<SyncPiecesResponse>>> {
        let response = self.client.clone().sync_pieces(request).await?;
        Ok(response)
    }

    // download_task tells the dfdaemon to download the task.
    pub async fn download_task(&self, request: DownloadTaskRequest) -> ClientResult<()> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        self.client.clone().download_task(request).await?;
        Ok(())
    }

    // upload_task tells the dfdaemon to upload the task.
    pub async fn upload_task(&self, request: UploadTaskRequest) -> ClientResult<()> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        self.client.clone().upload_task(request).await?;
        Ok(())
    }

    // stat_task gets the status of the task.
    pub async fn stat_task(&self, request: DfdaemonStatTaskRequest) -> ClientResult<Task> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        let response = self.client.clone().stat_task(request).await?;
        Ok(response.into_inner())
    }

    // delete_task tells the dfdaemon to delete the task.
    pub async fn delete_task(&self, request: DeleteTaskRequest) -> ClientResult<()> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        self.client.clone().delete_task(request).await?;
        Ok(())
    }
}
