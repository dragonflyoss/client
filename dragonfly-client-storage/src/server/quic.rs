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

use dragonfly_client_util::shutdown;
use dragonfly_client_util::tls;
use dragonfly_api::common::v2::Piece;
use dragonfly_api::dfdaemon::v2::{
    DownloadPieceResponse,
    DownloadPersistentCachePieceResponse,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    Error as ClientError, Result as ClientResult,
};
use dragonfly_client_util::id_generator::IDGenerator;
use prost::Message;
use std::io::{self, ErrorKind};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Barrier;
use tracing::{error, info, Span};
use vortex_protocol::{
    Vortex,
    tlv::{Tag, download_piece::DownloadPiece},
};
use crate::Storage;
use leaky_bucket::RateLimiter;
use std::time::Duration;
use bytes::{Bytes, BytesMut};
use quinn::{ConnectError, Endpoint, ReadExactError, ServerConfig, TransportConfig};
use tokio::io::AsyncReadExt;
const HEADER_SIZE: usize = 6;

/// QUICServer is a QUIC-based server for dfdaemon upload service.
pub struct QUICServer {
    /// addr is the address of the QUIC server.
    addr: SocketAddr,

    /// handler is the request handler.
    handler: QUICServerHandler,

    /// shutdown is used to shutdown the QUIC server.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the QUIC server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,

    /// ca_cert_path is the path to the CA certificate file.
    ca_cert_path: PathBuf,

    /// ca_key_path is the path to the CA private key file.
    ca_key_path: PathBuf,
}

impl QUICServer {
    /// Creates a new QUIC server instance.
    pub fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        addr: SocketAddr,
        ca_cert_path: PathBuf,
        ca_key_path: PathBuf,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        let handler = QUICServerHandler {
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
            ca_cert_path,
            ca_key_path,
        }
    }

    /// Starts the QUIC server.
    pub async fn run(&mut self, quic_server_started_barrier: Arc<Barrier>) -> ClientResult<()> {
        let ca_cert = tls::generate_ca_cert_from_pem(&self.ca_cert_path, &self.ca_key_path).unwrap();
        let self_signed_certs = tls::generate_self_signed_certs_by_ca_cert(&ca_cert, "localhost", vec!(String::from("quic")));
        let (tls_certs, tls_key) = self_signed_certs
            .map_err(|err| ClientError::InvalidParameter)?;

        let mut server_config = ServerConfig::with_single_cert(tls_certs, tls_key)
            .map_err(|err| ClientError::InvalidParameter)?;

        let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
        transport_config.max_concurrent_uni_streams(0_u8.into());

        // Create QUIC endpoint
        let endpoint = Endpoint::server(
            server_config,
            self.addr,
        ).map_err(|e| {
            error!("Failed to bind QUIC server to {}: {}", self.addr, e);
            ClientError::HostNotFound(self.addr.to_string())
        })?;

        info!("QUIC upload server listening on {}", self.addr);

        // Notify that the server is ready
        quic_server_started_barrier.wait().await;
        info!("QUIC upload server is ready");

        loop {
            tokio::select! {
                // Accept new connections
                result = endpoint.connect(self.addr, "quic") => {
                    match result {
                        Ok((connection,)) => {
                            let handler = self.handler.clone();
                            let shutdown = self.shutdown.clone();

                            // Handle the connection
                            tokio::spawn(async move {
                                if let Err(err) = handler.handle_connection(connection, shutdown).await {
                                    error!("Error handling QUIC connection: {}", err);
                                }
                            });
                        }
                    }
                }

                // Handle shutdown signal
                _ = self.shutdown.recv() => {
                    info!("QUIC upload server is shutting down");
                    break;
                }
            }
        }

        // Close the server endpoint
        endpoint.close(0u32.into(), b"shutdown");
        Ok(())
    }
}

/// QUICServerHandler processes QUIC connections and streams.
#[derive(Clone)]
pub struct QUICServerHandler {
    /// id_generator is the ID generator.
    id_generator: Arc<IDGenerator>,

    /// storage is the local storage.
    storage: Arc<Storage>,

    /// upload_rate_limiter is the rate limiter for upload speed in bps (bytes per second).
    upload_rate_limiter: Arc<RateLimiter>,
}

impl QUICServerHandler {
    /// Handles a QUIC connection (manages multiple streams).
    async fn handle_connection(
        &self,
        connection: quinn::Connection,
        mut shutdown: shutdown::Shutdown,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let peer_addr = connection.remote_address();

        loop {
            tokio::select! {
                // Accept new bidirectional stream
                stream_result = connection.accept_bi() => {
                    let (send_stream, recv_stream) = match stream_result {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to accept stream from {}: {}", peer_addr, e);
                            break; // Connection closed
                        }
                    };

                    let handler = self.clone();
                    tokio::spawn(async move {
                        if let Err(err) = handler.handle_stream(send_stream, recv_stream).await {
                            error!("Error handling QUIC stream: {}", err);
                        }
                    });
                }

                // Handle shutdown signal
                _ = shutdown.recv() => {
                    info!("Shutting down QUIC connection with {}", peer_addr);
                    connection.close(0u32.into(), b"shutdown");
                    break;
                }

                // Connection closed
                _ = connection.closed() => {
                    info!("QUIC connection with {} closed", peer_addr);
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handles a single QUIC bidirectional stream.
    async fn handle_stream(
        &self,
        mut send_stream: quinn::SendStream,
        mut recv_stream: quinn::RecvStream,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            // Read header 
            let mut header_buf = [0u8; HEADER_SIZE];
            match recv_stream.read_exact(&mut header_buf).await {
                Ok(_) => {}
                Err(e) if e == ReadExactError::FinishedEarly(0) => {
                    // Stream closed by client
                    info!("QUIC stream closed by client");
                    break;
                }
                Err(e) => {
                    error!("Failed to read header from stream: {}", e);
                    return Err(e.into());
                }
            }

            // Parse length from header 
            let length = u32::from_be_bytes(
                header_buf[2..HEADER_SIZE].try_into().expect("failed to read length")
            ) as usize;

            // Read request data
            let mut request_data = vec![0u8; length];
            recv_stream.read_exact(&mut request_data).await?;

            // Deserialize Vortex message 
            let mut complete_data = BytesMut::with_capacity(header_buf.len() + request_data.len());
            complete_data.extend_from_slice(&header_buf);
            complete_data.extend_from_slice(&request_data);

            let deserialized = Vortex::from_bytes(complete_data.freeze())
                .map_err(|e| format!("failed to deserialize packet: {}", e))?;

            // Process request 
            let response_result = match deserialized {
                Vortex::DownloadPiece(_, download_piece) => {
                    self.handle_piece(&download_piece).await
                }
                _ => {
                    error!("Unsupported message type: {:?}", deserialized.tag());
                    Err("unsupported message type".to_string())
                }
            };

            // Send response through QUIC stream
            match response_result {
                Ok(response_data) => {
                    let rsp = Vortex::new(Tag::PieceContent, Bytes::from(response_data))
                        .map_err(|e| format!("failed to create Vortex packet: {}", e))?;
                    send_stream.write_all(&rsp.to_bytes()).await?;
                    send_stream.finish().expect("failed to finish sending response");
                }
                Err(error_message) => {
                    let ersp = Vortex::new(Tag::Error, Bytes::from(error_message))
                        .map_err(|e| format!("failed to create error packet: {}", e))?;
                    send_stream.write_all(&ersp.to_bytes()).await?;
                    send_stream.finish().expect("failed to finish sending error");
                }
            }
        }

        Ok(())
    }

    /// Handles download piece request.
    async fn handle_piece(
        &self,
        request: &DownloadPiece,
    ) -> Result<Vec<u8>, String> {
        // Generate host ID
        let host_id = self.id_generator.host_id();

        // Get task ID from request
        let task_id = request.task_id();

        // Get piece number from request
        let piece_number = request.piece_number();

        // Generate piece ID
        let piece_id = self.storage.piece_id(task_id, piece_number);

        // Record host ID, task ID and piece number in span
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id);
        Span::current().record("piece_id", piece_id.as_str());
        info!("downloading piece content in QUIC upload server");

        // Get piece metadata from local storage
        let piece = self
            .storage
            .get_piece(piece_id.as_str())
            .map_err(|err| format!("failed to get piece metadata: {}", err))?
            .ok_or_else(|| "piece metadata not found".to_string())?;

        info!("starting to upload piece content");

        // Record piece ID and length in span
        Span::current().record("piece_id", piece_id.as_str());
        Span::current().record("piece_length", piece.length);

        // Acquire upload rate limiter
        self.upload_rate_limiter.acquire(piece.length as usize).await;

        // Upload piece content
        let mut reader = self.storage
            .upload_piece(piece_id.as_str(), task_id, None)
            .await
            .map_err(|err| {
                format!("failed to get piece content: {}", err)
            })?;

        // Read piece content
        let mut content = vec![0; piece.length as usize];
        reader.read_exact(&mut content).await.map_err(|err| {
            format!("failed to read piece content: {}", err)
        })?;

        info!("finished uploading piece content");

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

    /// Handles download persistent cache piece request.
    async fn handle_persistent_cache_piece(
        &self,
        request: &DownloadPiece,
    ) -> Result<Vec<u8>, String> {
        // Generate host ID
        let host_id = self.id_generator.host_id();

        // Get task ID from request
        let task_id = request.task_id();

        // Get piece number from request
        let piece_number = request.piece_number();

        // Generate piece ID
        let piece_id = self.storage.piece_id(task_id, piece_number);

        // Record host ID, task ID and piece number in span
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id);
        Span::current().record("piece_id", piece_id.as_str());
        info!("downloading persistent cache piece content in QUIC upload server");

        // Get piece metadata from local storage
        let piece = self
            .storage
            .get_persistent_cache_piece(piece_id.as_str())
            .map_err(|err| format!("failed to get persistent cache piece metadata: {}", err))?
            .ok_or_else(|| "persistent cache piece metadata not found".to_string())?;

        info!("starting to upload persistent cache piece content");

        // Record piece ID and length in span
        Span::current().record("piece_id", piece_id.as_str());
        Span::current().record("piece_length", piece.length);

        // Acquire upload rate limiter
        self.upload_rate_limiter.acquire(piece.length as usize).await;

        // Upload piece content
        let mut reader = self.storage
            .upload_persistent_cache_piece(piece_id.as_str(), task_id, None)
            .await
            .map_err(|err| {
                format!("failed to get persistent cache piece content: {}", err)
            })?;

        // Read piece content
        let mut content = vec![0; piece.length as usize];
        reader.read_exact(&mut content).await.map_err(|err| {
            format!("failed to read persistent cache piece content: {}", err)
        })?;

        info!("finished uploading persistent cache piece content");

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
}
