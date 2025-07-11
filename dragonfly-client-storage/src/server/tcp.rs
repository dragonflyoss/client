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

use crate::metrics::{
    collect_upload_piece_failure_metrics,
    collect_upload_piece_finished_metrics, 
    collect_upload_piece_started_metrics,
    collect_upload_piece_traffic_metrics,
};
use crate::shutdown;
use dragonfly_api::common::v2::Piece;
use dragonfly_api::dfdaemon::v2::{
    DownloadPieceRequest, 
    DownloadPieceResponse,
    DownloadPersistentCachePieceRequest,
    DownloadPersistentCachePieceResponse,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    Error as ClientError, Result as ClientResult,
};
use dragonfly_client_util::{
    net::{get_interface_info, Interface},
    id_generator::IDGenerator,
};
use prost::Message;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream};
use tokio::sync::mpsc;
use tokio::sync::Barrier;
use tracing::{error, info, instrument, Span};
use vortex_protocol::{
    Vortex,
    tlv::Tag,
};
use bytes::Bytes;
use crate::Storage;
use leaky_bucket::RateLimiter;
use std::time::Duration;
use crate::server::{Server, ServerHandler};

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};

const HEADER_SIZE: usize = 6;

/// TCPServer is a TCP-based server for dfdaemon upload service.
pub struct TCPServer {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// addr is the address of the TCP server.
    addr: SocketAddr,

    /// handler is the request handler.
    handler: TCPServerHandler,

    /// shutdown is used to shutdown the TCP server.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the TCP server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

#[tonic::async_trait]
impl Server for TCPServer {
    /// Creates a new TCPServer.
    #[instrument(skip_all)]
    fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        addr: SocketAddr,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        // Initialize the interface info.
        let interface =
            get_interface_info(config.host.ip.unwrap(), config.upload.rate_limit).unwrap();

        let handler = TCPServerHandler {
            interface,
            socket_path: config.download.server.socket_path.clone(),
            id_generator,
            storage,
            config: config.clone(),
        };

        Self {
            config,
            addr,
            handler,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// Starts the TCP upload server.
    #[instrument(skip_all)]
    async fn run(&mut self, tcp_server_started_barrier: Arc<Barrier>) -> ClientResult<()> {
        let listener = TokioTcpListener::bind(self.addr).await.map_err(|err| {
            error!("Failed to bind to {}: {}", self.addr, err);
            ClientError::HostNotFound(self.addr.to_string())
        })?;

        info!("TCP upload server listening on {}", self.addr);

        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Notify that the server is ready
        tcp_server_started_barrier.wait().await;
        info!("TCP upload server is ready");

        loop {
            tokio::select! {
                // Accept new connections
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            // Get socket file descriptor
                            let socket_fd = Self::get_socket_fd(&stream);
                            info!("New connection from {} with socket_fd: {:?}", peer_addr, socket_fd);
                            let handler = self.handler.clone();
                            
                            // Spawn a task to handle the connection
                            tokio::spawn(async move {
                                if let Err(err) = handler.handle_connection(stream).await {
                                    error!("Error handling connection from {}: {}", peer_addr, err);
                                }
                            });
                        }
                        Err(err) => {
                            error!("Failed to accept connection: {}", err);
                        }
                    }
                }
                
                // Handle shutdown signal
                _ = shutdown.recv() => {
                    info!("TCP upload server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

impl TCPServer {

    /// Gets the socket file descriptor from a TcpStream
    #[cfg(unix)]
    fn get_socket_fd(stream: &TokioTcpStream) -> Option<RawFd> {
        Some(stream.as_raw_fd())
    }

    /// Gets the socket handle from a TcpStream (Windows)
    #[cfg(windows)]
    fn get_socket_fd(stream: &TokioTcpStream) -> Option<RawSocket> {
        Some(stream.as_raw_socket())
    }

    /// Gets socket information as a generic identifier
    #[cfg(not(any(unix, windows)))]
    fn get_socket_fd(stream: &TokioTcpStream) -> Option<String> {
        // For other platforms, we can use the peer address as identifier
        stream.peer_addr().ok().map(|addr| addr.to_string())
    }

}

/// TCPServerHandler handles TCP connections and requests.
#[derive(Clone)]
pub struct TCPServerHandler {
    /// interface is the network interface.
    interface: Interface,

    /// socket_path is the path of the unix domain socket.
    socket_path: PathBuf,

    id_generator: Arc<IDGenerator>,

    storage: Arc<Storage>,

    config: Arc<Config>,
}

impl TCPServerHandler {
    /// Handles a single TCP connection.
    async fn handle_connection(&self, mut stream: TokioTcpStream) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            // Read header
            let mut header_buf = [0u8; HEADER_SIZE];
            match stream.read_exact(&mut header_buf).await {
                Ok(_) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                    // Connection closed by client
                    break;
                }
                Err(err) => {
                    error!("Failed to read header: {}", err);
                    return Err(err.into());
                }
            }

            let length = u32::from_be_bytes(header_buf[2..HEADER_SIZE].try_into().expect("Failed to read value length")) as usize;

            // Read request data
            let mut request_data = vec![0u8; length];
            stream.read_exact(&mut request_data).await?;

            request_data.splice(0..0, header_buf.iter().copied());

            let deserialized = Vortex::from_bytes(request_data.into()).expect("Failed to deserialize packet");

            // Process request based on message type
            let response_result = match deserialized {
                Vortex::DownloadPiece(_, download_piece) => {
                    let bytes_back: Bytes = download_piece.into();
                    self.handle_download_piece_request(&bytes_back).await
                }
                _ => {
                    error!("Unsupported message type: {:?}", deserialized.tag());
                    Err("Unsupported message type".to_string())
                }
            };
            // let response_result = match header.message_type {
            //     MessageType::DownloadPieceRequest => {
            //         self.handle_download_piece_request(&request_data).await
            //     }
            //     MessageType::DownloadTaskRequest => {
            //         self.handle_download_task_request(&request_data).await
            //     }
            //     MessageType::StatTaskRequest => {
            //         self.handle_stat_task_request(&request_data).await
            //     }
            //     MessageType::DeleteTaskRequest => {
            //         self.handle_delete_task_request(&request_data).await
            //     }
            //     _ => {
            //         error!("Unsupported message type: {:?}", header.message_type);
            //         Err("Unsupported message type".to_string())
            //     }
            // };

            // Send response
            match response_result {
                Ok(response_data) => {
                    let rsp = Vortex::new(Tag::PieceContent, response_data.into()).expect("Failed to create Vortex packet");
                    stream.write_all(&rsp.to_bytes()).await?;
                    // stream.write_all(&response_data).await?;
                }
                Err(error_message) => {
                    let ersp = Vortex::new(Tag::Error, error_message.into()).expect("Failed to create Vortex packet");
                    // let error_data = error_response.to_bytes();
                    // let error_header = ProtocolHeader::new(MessageType::Error, error_data.len() as u32);
                    // stream.write_all(&error_header.to_bytes()).await?;
                    // stream.write_all(&error_data).await?;
                    stream.write_all(&ersp.to_bytes()).await?;
                }
            }
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl ServerHandler for TCPServerHandler{
    /// Handles download piece request.
    #[instrument(skip_all, fields(host_id, task_id, piece_id))]
    async fn handle_download_piece_request(
        &self,
        request_data: &[u8],
    ) -> Result<Vec<u8>, String> {
        // Decode request
        let request = DownloadPieceRequest::decode(request_data)
            .map_err(|err| format!("Failed to decode request: {}", err))?;

        // Generate the host id.
        let host_id = self.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id.clone();

        // Get the interested piece number from the request.
        let piece_number = request.piece_number;

        // Generate the piece id.
        let piece_id = self.storage.piece_id(task_id.as_str(), piece_number);

        // Span record the host id, task id and piece number.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record("piece_id", piece_id.as_str());
        info!("download piece content in TCP upload server");

        // Get the piece metadata from the local storage.
        let piece = self
            .storage
            .get_piece(piece_id.as_str())
            .map_err(|err| format!("Failed to get piece metadata: {}", err))?
            .ok_or_else(|| "Piece metadata not found".to_string())?;

        // Collect upload piece started metrics.
        collect_upload_piece_started_metrics();
        info!("start upload piece content");

        // Get the piece content from the local storage.
        // let mut reader = self
        //     .task
        //     .piece
        //     .upload_from_local_into_async_read(
        //         piece_id.as_str(),
        //         task_id.as_str(),
        //         piece.length,
        //         None,
        //         false,
        //     )
        //     .await
        //     .map_err(|err| {
        //         // Collect upload piece failure metrics.
        //         collect_upload_piece_failure_metrics();
        //         format!("Failed to get piece content: {}", err)
        //     })?;
        // Span record the piece_id.
        Span::current().record("piece_id", piece_id.as_str());
        Span::current().record("piece_length", piece.length);

        // Acquire the upload rate limiter.
        Arc::new(
        RateLimiter::builder()
            .initial(self.config.upload.rate_limit.as_u64() as usize)
            .refill(self.config.upload.rate_limit.as_u64() as usize)
            .max(self.config.upload.rate_limit.as_u64() as usize)
            .interval(Duration::from_secs(1))
            .fair(false)
            .build(),
        ).acquire(piece.length as usize).await;

        // Upload the piece content.
        let mut reader = self.storage
            .upload_piece(piece_id.as_str(), task_id.as_str(), None)
            .await
            .inspect(|_| {
                collect_upload_piece_traffic_metrics(
                    self.id_generator.task_type(task_id.as_str()) as i32,
                    piece.length,
                );
            })
            .map_err(|err| {
                // Collect upload piece failure metrics.
                collect_upload_piece_failure_metrics();
                format!("Failed to get piece content: {}", err)
            })?;
        // Read the content of the piece.
        let mut content = vec![0; piece.length as usize];
        reader.read_exact(&mut content).await.map_err(|err| {
            // Collect upload piece failure metrics.
            collect_upload_piece_failure_metrics();
            format!("Failed to read piece content: {}", err)
        })?;

        // Collect upload piece finished metrics.
        collect_upload_piece_finished_metrics();
        info!("finished upload piece content");

        // Create response
        let response = DownloadPieceResponse {
            piece: Some(Piece {
                number: piece.number,
                parent_id: piece.parent_id.clone(),
                offset: piece.offset,
                length: piece.length,
                digest: piece.digest.clone(),
                content: Some(content),
                traffic_type: None,
                cost: None,
                created_at: None,
            }),
            digest: Some(piece.calculate_digest()),
        };

        Ok(response.encode_to_vec())
    }

    /// Handles download piece request.
    #[instrument(skip_all, fields(host_id, task_id, piece_id))]
    async fn handle_download_persistent_cache_piece_request(
        &self,
        request_data: &[u8],
    ) -> Result<Vec<u8>, String> {
        // Decode request
        let request = DownloadPersistentCachePieceRequest::decode(request_data)
            .map_err(|err| format!("Failed to decode request: {}", err))?;

        // Generate the host id.
        let host_id = self.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id.clone();

        // Get the interested piece number from the request.
        let piece_number = request.piece_number;

        // Generate the piece id.
        let piece_id = self.storage.piece_id(task_id.as_str(), piece_number);

        // Span record the host id, task id and piece number.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record("piece_id", piece_id.as_str());
        info!("download piece content in TCP upload server");

        // Get the piece metadata from the local storage.
        let piece = self
            .storage
            .get_persistent_cache_piece(piece_id.as_str())
            .map_err(|err| format!("Failed to get persistent cache piece metadata: {}", err))?
            .ok_or_else(|| "Persistent cache piece metadata not found".to_string())?;

        // Collect upload piece started metrics.
        collect_upload_piece_started_metrics();
        info!("start upload persistent cache piece content");

        // Get the piece content from the local storage.
        // let mut reader = self
        //     .task
        //     .piece
        //     .upload_from_local_into_async_read(
        //         piece_id.as_str(),
        //         task_id.as_str(),
        //         piece.length,
        //         None,
        //         false,
        //     )
        //     .await
        //     .map_err(|err| {
        //         // Collect upload piece failure metrics.
        //         collect_upload_piece_failure_metrics();
        //         format!("Failed to get piece content: {}", err)
        //     })?;
        // Span record the piece_id.
        Span::current().record("piece_id", piece_id.as_str());
        Span::current().record("piece_length", piece.length);

        // Acquire the upload rate limiter.
        Arc::new(
        RateLimiter::builder()
            .initial(self.config.upload.rate_limit.as_u64() as usize)
            .refill(self.config.upload.rate_limit.as_u64() as usize)
            .max(self.config.upload.rate_limit.as_u64() as usize)
            .interval(Duration::from_secs(1))
            .fair(false)
            .build(),
        ).acquire(piece.length as usize).await;

        // Upload the piece content.
        let mut reader = self.storage
            .upload_persistent_cache_piece(piece_id.as_str(), task_id.as_str(), None)
            .await
            .inspect(|_| {
                collect_upload_piece_traffic_metrics(
                    self.id_generator.task_type(task_id.as_str()) as i32,
                    piece.length,
                );
            })
            .map_err(|err| {
                // Collect upload piece failure metrics.
                collect_upload_piece_failure_metrics();
                format!("Failed to get persistent cache piece content: {}", err)
            })?;
        // Read the content of the piece.
        let mut content = vec![0; piece.length as usize];
        reader.read_exact(&mut content).await.map_err(|err| {
            // Collect upload piece failure metrics.
            collect_upload_piece_failure_metrics();
            format!("Failed to read persistent cache piece content: {}", err)
        })?;

        // Collect upload piece finished metrics.
        collect_upload_piece_finished_metrics();
        info!("finished persistent cache upload piece content");

        // Create response
        let response = DownloadPersistentCachePieceResponse {
            piece: Some(Piece {
                number: piece.number,
                parent_id: piece.parent_id.clone(),
                offset: piece.offset,
                length: piece.length,
                digest: piece.digest.clone(),
                content: Some(content),
                traffic_type: None,
                cost: None,
                created_at: None,
            }),
            digest: Some(piece.calculate_digest()),
        };

        Ok(response.encode_to_vec())
    }
    // /// Handles download task request (placeholder).
    // async fn handle_download_task_request(
    //     &self,
    //     request_data: &[u8],
    // ) -> Result<(MessageType, Vec<u8>), String> {
    //     // TODO: Implement download task logic
    //     Err("Download task not implemented".to_string())
    // }

    // /// Handles stat task request (placeholder).
    // async fn handle_stat_task_request(
    //     &self,
    //     request_data: &[u8],
    // ) -> Result<(MessageType, Vec<u8>), String> {
    //     // TODO: Implement stat task logic
    //     Err("Stat task not implemented".to_string())
    // }

    // /// Handles delete task request (placeholder).
    // async fn handle_delete_task_request(
    //     &self,
    //     request_data: &[u8],
    // ) -> Result<(MessageType, Vec<u8>), String> {
    //     // TODO: Implement delete task logic
    //     Err("Delete task not implemented".to_string())
    // }
}