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

use crate::Storage;
use bytes::Bytes;
use chrono::Utc;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use dragonfly_client_util::{id_generator::IDGenerator, shutdown};
use leaky_bucket::RateLimiter;
use vortex_protocol::tlv::piece_content::PieceContent;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream};
use tokio::sync::{mpsc, Barrier};
use tracing::{error, info, Span};
use vortex_protocol::{
    Header,
    HEADER_SIZE,
    tlv::{download_piece::DownloadPiece, Tag},
    Vortex,
};

/// TCPServer is a TCP-based server for dfdaemon upload service.
pub struct TCPServer {
    /// addr is the address of the TCP server.
    addr: SocketAddr,

    /// handler is the request handler.
    handler: TCPServerHandler,

    /// shutdown is used to shutdown the TCP server.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the TCP server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

impl TCPServer {
    /// Creates a new TCPServer.
    pub fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        addr: SocketAddr,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        let handler = TCPServerHandler {
            id_generator,
            storage,
            upload_rate_limiter: Arc::new(
                RateLimiter::builder()
                    .initial(config.upload.rate_limit.as_u64() as usize)
                    .refill(config.upload.rate_limit.as_u64() as usize)
                    .max(config.upload.rate_limit.as_u64() as usize)
                    .interval(Duration::from_secs(1))
                    .fair(false)
                    .build(),
            ),
        };

        Self {
            addr,
            handler,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// Starts the TCP upload server.
    pub async fn run(&mut self, tcp_server_started_barrier: Arc<Barrier>) -> ClientResult<()> {
        // Initialize the TCP service.
        let listener = TokioTcpListener::bind(self.addr).await.map_err(|err| {
            error!("Failed to bind to {}: {}", self.addr, err);
            ClientError::HostNotFound(self.addr.to_string())
        })?;

        info!("TCP upload server listening on {}", self.addr);

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
                _ = self.shutdown.recv() => {
                    info!("TCP upload server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Gets the socket file descriptor from a TcpStream
    fn get_socket_fd(stream: &TokioTcpStream) -> RawFd {
        stream.as_raw_fd()
    }
}

/// TCPServerHandler handles TCP connections and requests.
#[derive(Clone)]
pub struct TCPServerHandler {
    /// id_generator is the id generator.
    id_generator: Arc<IDGenerator>,

    /// storage is the local storage.
    storage: Arc<Storage>,

    /// upload_rate_limiter is the rate limiter of the upload speed in bps(bytes per second).
    upload_rate_limiter: Arc<RateLimiter>,
}

impl TCPServerHandler {
    /// Handles a single TCP connection.
    async fn handle_connection(
        &self,
        mut stream: TokioTcpStream,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Read header
        info!("Read header");
        let mut header_buf = [0u8; HEADER_SIZE];
        stream.read_exact(&mut header_buf).await?;

        let length = u32::from_be_bytes(
            header_buf[2..HEADER_SIZE]
                .try_into()
                .expect("Failed to read value length"),
        ) as usize;

        // Read request data
        info!("Read request data");
        let mut request_data = vec![0u8; length];
        stream.read_exact(&mut request_data).await?;

        let mut complete_data = Vec::with_capacity(header_buf.len() + request_data.len());
        complete_data.extend_from_slice(&header_buf);
        complete_data.extend_from_slice(&request_data);

        // Process request based on message type
        match Bytes::from(complete_data).try_into()? {
            Vortex::DownloadPiece(_, download_piece) => {
                match self.handle_piece(&download_piece).await {
                    Ok((piece_content, mut reader)) => {
                        let piece_content_bytes: Bytes = piece_content.into();
                        let header_bytes: Bytes = Header::new(
                            Tag::PieceContent,
                            piece_content_bytes.len() as u32,
                        ).into();
                        stream.write_all(&header_bytes).await?;
                        stream.write_all(&piece_content_bytes).await?;

                        tokio::io::copy(&mut reader, &mut stream).await?;
                    }
                    Err(error_message) => {
                        error!(error_message);
                        let packet = Vortex::new(Tag::Error, error_message.into()).unwrap();
                        let error_response: Bytes = packet.into();
                        stream.write_all(&error_response).await?;
                    }
                }
            }
            _ => {
                let error_message = "Unsupported message type".to_string();
                error!(error_message);
                let packet = Vortex::new(Tag::Error, error_message.into()).unwrap();
                let error_response: Bytes = packet.into();
                stream.write_all(&error_response).await?;
            }
        }

        Ok(())
    }

    /// Handles download piece request.
    async fn handle_piece(&self, request: &DownloadPiece) -> Result<(PieceContent, impl AsyncRead), String> {
        // Generate the host id.
        let host_id = self.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id();

        // Get the interested piece number from the request.
        let piece_number = request.piece_number();

        // Generate the piece id.
        let piece_id = self.storage.piece_id(task_id, piece_number);

        // Span record the host id, task id and piece number.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id);
        Span::current().record("piece_id", piece_id.as_str());
        info!("download piece content in TCP upload server");

        // Get the piece metadata from the local storage.
        let piece = self
            .storage
            .get_piece(piece_id.as_str())
            .map_err(|err| format!("Failed to get piece metadata: {}", err))?
            .ok_or_else(|| "Piece metadata not found".to_string())?;

        info!("start upload piece content");

        // Span record the piece_id.
        Span::current().record("piece_id", piece_id.as_str());
        Span::current().record("piece_length", piece.length);

        // Acquire the upload rate limiter.
        self.upload_rate_limiter
            .acquire(piece.length as usize)
            .await;

        // Upload the piece content.
        let reader = self
            .storage
            .upload_piece(piece_id.as_str(), task_id, None)
            .await
            .map_err(|err| format!("Failed to get piece content: {}", err))?;
        info!("finished upload piece content");

        // Create response
        let piece_content = PieceContent::new(
            piece_number,
            piece.offset,
            piece.length,
            piece.digest,
            piece.parent_id.unwrap_or_default(),
            1,
            Duration::from_secs(30),
            Utc::now().naive_utc(),
        );

        Ok((piece_content, reader))
    }

    /// Handles download piece request.
    async fn handle_persistent_cache_piece(
        &self,
        request: &DownloadPiece,
    ) -> Result<(PieceContent, impl AsyncRead), String> {
        // Generate the host id.
        let host_id = self.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id();

        // Get the interested piece number from the request.
        let piece_number = request.piece_number();

        // Generate the piece id.
        let piece_id = self.storage.piece_id(task_id, piece_number);

        // Span record the host id, task id and piece number.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id);
        Span::current().record("piece_id", piece_id.as_str());
        info!("download piece content in TCP upload server");

        // Get the piece metadata from the local storage.
        let piece = self
            .storage
            .get_persistent_cache_piece(piece_id.as_str())
            .map_err(|err| format!("Failed to get persistent cache piece metadata: {}", err))?
            .ok_or_else(|| "Persistent cache piece metadata not found".to_string())?;

        info!("start upload persistent cache piece content");

        // Span record the piece_id.
        Span::current().record("piece_id", piece_id.as_str());
        Span::current().record("piece_length", piece.length);

        // Acquire the upload rate limiter.
        self.upload_rate_limiter
            .acquire(piece.length as usize)
            .await;

        // Upload the piece content.
        let reader = self
            .storage
            .upload_persistent_cache_piece(piece_id.as_str(), task_id, None)
            .await
            .map_err(|err| format!("Failed to get persistent cache piece content: {}", err))?;
        info!("finished persistent cache upload piece content");

        // Create response
        let piece_content = PieceContent::new(
            piece_number,
            piece.offset,
            piece.length,
            piece.digest,
            piece.parent_id.unwrap_or_default(),
            1,
            Duration::from_secs(30),
            Utc::now().naive_utc(),
        );

        Ok((piece_content, reader))
    }
}
