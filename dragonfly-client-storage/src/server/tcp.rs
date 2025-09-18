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
use bytes::{Bytes, BytesMut};
use chrono::Utc;
use dragonfly_api::common::v2::TrafficType;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use dragonfly_client_util::{id_generator::IDGenerator, shutdown};
use leaky_bucket::RateLimiter;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{copy, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream};
use tokio::sync::mpsc;
use tracing::{error, info, Span};
use vortex_protocol::{
    tlv::{
        download_persistent_cache_piece::DownloadPersistentCachePiece,
        download_piece::DownloadPiece,
        error::{Code, Error},
        persistent_cache_piece_content::PersistentCachePieceContent,
        piece_content::PieceContent,
        Tag,
    },
    Header, Vortex, HEADER_SIZE,
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
            read_buffer_size: config.storage.read_buffer_size,
            write_buffer_size: config.storage.write_buffer_size,
        };

        Self {
            addr,
            handler,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// Starts the storage tcp server.
    pub async fn run(&mut self) -> ClientResult<()> {
        // Initialize the TCP service.
        let listener = TokioTcpListener::bind(self.addr).await.map_err(|err| {
            error!("failed to bind to {}: {}", self.addr, err);
            ClientError::HostNotFound(self.addr.to_string())
        })?;

        info!("storage tcp server listening on {}", self.addr);

        loop {
            tokio::select! {
                // Accept new connections
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            let handler = self.handler.clone();

                            // Spawn a task to handle the connection
                            tokio::spawn(async move {
                                if let Err(err) = handler.handle_connection(stream).await {
                                    error!("error handling connection from {}: {}", peer_addr, err);
                                }
                            });
                        }
                        Err(err) => {
                            error!("failed to accept connection: {}", err);
                        }
                    }
                }

                // Handle shutdown signal
                _ = self.shutdown.recv() => {
                    info!("tcp server shutting down");
                    break;
                }
            }
        }

        Ok(())
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

    /// read_buffer_size is the buffer size for reading piece to stream, default is 128KB.
    pub read_buffer_size: usize,

    /// write_buffer_size is the buffer size for writing piece to stream, default is 128KB.
    pub write_buffer_size: usize,
}

impl TCPServerHandler {
    /// Handles a single TCP connection.
    async fn handle_connection(&self, stream: TokioTcpStream) -> ClientResult<()> {
        let (s_reader, s_writer) = stream.into_split();
        let mut reader = BufReader::with_capacity(self.read_buffer_size, s_reader);
        let mut writer = BufWriter::with_capacity(self.write_buffer_size, s_writer);

        let mut header_bytes = BytesMut::with_capacity(HEADER_SIZE);
        header_bytes.resize(HEADER_SIZE, 0);
        reader.read_exact(&mut header_bytes).await.map_err(|err| {
            error!("failed to receive request header: {}", err);
            ClientError::MpscSend(err.to_string())
        })?;
        let header: Header = header_bytes
            .freeze()
            .try_into()
            .map_err(|_| ClientError::UnexpectedResponse)?;
        info!("received header: {:?}", header);

        match header.tag() {
            Tag::DownloadPiece => {
                // ------------------------------
                // Recieve DownloadPiece request.
                // ------------------------------
                let mut download_piece_bytes = BytesMut::with_capacity(header.length() as usize);
                download_piece_bytes.resize(header.length() as usize, 0);
                reader
                    .read_exact(&mut download_piece_bytes)
                    .await
                    .map_err(|err| {
                        error!("failed to receive DownloadPiece: {}", err);
                        ClientError::MpscSend(err.to_string())
                    })?;

                let download_piece: DownloadPiece = download_piece_bytes
                    .freeze()
                    .try_into()
                    .map_err(|_| ClientError::UnexpectedResponse)?;
                info!("received DownloadPiece request: {:?}", download_piece);

                // ---------------------------
                // Send PieceContent response.
                // ---------------------------
                match self.handle_piece(&download_piece).await {
                    Ok((piece_content, mut content_reader)) => {
                        let piece_content_bytes: Bytes = piece_content.clone().into();
                        let header = Header::new_piece_content(piece_content_bytes.len() as u32);
                        let header_bytes: Bytes = header.clone().into();

                        let mut request =
                            BytesMut::with_capacity(HEADER_SIZE + piece_content_bytes.len());
                        request.extend_from_slice(&header_bytes);
                        request.extend_from_slice(&piece_content_bytes);

                        // Write header and piece_content.
                        writer.write_all(&request).await.map_err(|err| {
                            error!("failed to send PieceContent: {}", err);
                            ClientError::MpscSend(err.to_string())
                        })?;
                        writer.flush().await.map_err(|err| {
                            error!("failed to send PieceContent: {}", err);
                            ClientError::MpscSend(err.to_string())
                        })?;
                        info!("sent PieceContent: {:?}", piece_content);

                        // Write content.
                        copy(&mut content_reader, &mut writer)
                            .await
                            .inspect_err(|err| {
                                error!("copy failed: {}", err);
                            })?;

                        writer.flush().await.inspect_err(|err| {
                            error!("flush failed: {}", err);
                        })?;
                    }
                    Err(err) => {
                        let error_response: Bytes =
                            Vortex::Error(Header::new_error(err.len() as u32), err).into();
                        writer.write_all(&error_response).await.map_err(|err| {
                            error!("failed to send Error: {}", err);
                            ClientError::MpscSend(err.to_string())
                        })?;
                        writer.flush().await.map_err(|err| {
                            error!("failed to send Error: {}", err);
                            ClientError::MpscSend(err.to_string())
                        })?;
                    }
                }

                Ok(())
            }
            Tag::DownloadPersistentCachePiece => {
                // ------------------------------
                // Recieve DownloadPersistentCachePiece request.
                // ------------------------------
                let mut download_persistent_cache_piece_bytes =
                    BytesMut::with_capacity(header.length() as usize);
                download_persistent_cache_piece_bytes.resize(header.length() as usize, 0);
                reader
                    .read_exact(&mut download_persistent_cache_piece_bytes)
                    .await
                    .map_err(|err| {
                        error!("failed to receive DownloadPersistentCachePiece: {}", err);
                        ClientError::MpscSend(err.to_string())
                    })?;

                let download_persistent_cache_piece: DownloadPersistentCachePiece =
                    download_persistent_cache_piece_bytes
                        .freeze()
                        .try_into()
                        .map_err(|_| ClientError::UnexpectedResponse)?;
                info!(
                    "received DownloadPersistentCachePiece request: {:?}",
                    download_persistent_cache_piece
                );

                // ---------------------------
                // Send PersistentCachePieceContent response.
                // ---------------------------
                match self
                    .handle_persistent_cache_piece(&download_persistent_cache_piece)
                    .await
                {
                    Ok((persistent_cache_piece_content, mut content_reader)) => {
                        let persistent_cache_piece_content_bytes: Bytes =
                            persistent_cache_piece_content.clone().into();
                        let header = Header::new_persistent_cache_piece_content(
                            persistent_cache_piece_content_bytes.len() as u32,
                        );
                        let header_bytes: Bytes = header.clone().into();

                        let mut request = BytesMut::with_capacity(
                            HEADER_SIZE + persistent_cache_piece_content_bytes.len(),
                        );
                        request.extend_from_slice(&header_bytes);
                        request.extend_from_slice(&persistent_cache_piece_content_bytes);

                        // Write header and persistent_cache_piece_content.
                        writer.write_all(&request).await.map_err(|err| {
                            error!("failed to send PersistentCachePieceContent: {}", err);
                            ClientError::MpscSend(err.to_string())
                        })?;
                        writer.flush().await.map_err(|err| {
                            error!("failed to send PersistentCachePieceContent: {}", err);
                            ClientError::MpscSend(err.to_string())
                        })?;
                        info!(
                            "sent PersistentCachePieceContent: {:?}",
                            persistent_cache_piece_content
                        );

                        // Write content.
                        copy(&mut content_reader, &mut writer)
                            .await
                            .inspect_err(|err| {
                                error!("copy failed: {}", err);
                            })?;

                        writer.flush().await.inspect_err(|err| {
                            error!("flush failed: {}", err);
                        })?;
                    }
                    Err(err) => {
                        let error_response: Bytes =
                            Vortex::Error(Header::new_error(err.len() as u32), err).into();
                        writer.write_all(&error_response).await.map_err(|err| {
                            error!("failed to send Error: {}", err);
                            ClientError::MpscSend(err.to_string())
                        })?;
                        writer.flush().await.map_err(|err| {
                            error!("failed to send Error: {}", err);
                            ClientError::MpscSend(err.to_string())
                        })?;
                    }
                }

                Ok(())
            }
            _ => Err(ClientError::Unsupported(format!(
                "unsupported tag: {:?}",
                header.tag()
            ))),
        }
    }

    /// Handles download piece request.
    async fn handle_piece(
        &self,
        request: &DownloadPiece,
    ) -> Result<(PieceContent, impl AsyncRead), Error> {
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
        info!("download piece content in storage tcp server");

        // Get the piece metadata from the local storage.
        let piece = self
            .storage
            .get_piece(piece_id.as_str())
            .map_err(|err| {
                Error::new(
                    Code::InvalidArgument,
                    format!("failed to get piece metadata: {}", err),
                )
            })?
            .ok_or_else(|| {
                error!("piece metadata not found");
                Error::new(Code::NotFound, piece_id.clone())
            })?;

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
            .map_err(|err| {
                error!("failed to get piece content: {}", err);
                Error::new(Code::Internal, piece_id)
            })?;
        info!("finished upload piece content");

        // Create response
        let piece_content = PieceContent::new(
            piece_number,
            piece.offset,
            piece.length,
            piece.digest,
            piece.parent_id.unwrap_or_default(),
            TrafficType::RemotePeer as u8,
            Duration::from_secs(30),
            Utc::now().naive_utc(),
        );

        Ok((piece_content, reader))
    }

    /// Handles download piece request.
    async fn handle_persistent_cache_piece(
        &self,
        request: &DownloadPersistentCachePiece,
    ) -> Result<(PersistentCachePieceContent, impl AsyncRead), Error> {
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
        info!("download persistent cache piece content in storage tcp server");

        // Get the piece metadata from the local storage.
        let piece = self
            .storage
            .get_persistent_cache_piece(piece_id.as_str())
            .map_err(|err| {
                Error::new(
                    Code::InvalidArgument,
                    format!("failed to get persistent cache piece metadata: {}", err),
                )
            })?
            .ok_or_else(|| {
                error!("persistent cache piece metadata not found");
                Error::new(Code::NotFound, piece_id.clone())
            })?;

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
            .map_err(|err| {
                error!("failed to get persistent cache piece content: {}", err);
                Error::new(Code::Internal, piece_id)
            })?;
        info!("finished upload persistent cache piece content");

        // Create response
        let persistent_cache_piece_content = PersistentCachePieceContent::new(
            piece_number,
            piece.offset,
            piece.length,
            piece.digest,
            piece.parent_id.unwrap_or_default(),
            TrafficType::RemotePeer as u8,
            Duration::from_secs(30),
            Utc::now().naive_utc(),
        );

        Ok((persistent_cache_piece_content, reader))
    }
}
