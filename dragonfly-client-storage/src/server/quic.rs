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
use dragonfly_api::common::v2::Piece;
use dragonfly_api::dfdaemon::v2::{
    DownloadPieceResponse,
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
use quinn::{Endpoint, ServerConfig, Incoming, RecvStream, SendStream};
use tokio::sync::mpsc;
use tokio::sync::Barrier;
use tracing::{error, info, instrument, Span};
use vortex_protocol::{
    Vortex,
    tlv::{Tag, download_piece::DownloadPiece},
};
use crate::Storage;
use leaky_bucket::RateLimiter;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};

const HEADER_SIZE: usize = 6;

/// QUICServer is a QUIC-based server for dfdaemon upload service.
pub struct QUICServer {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// addr is the address of the TCP server.
    addr: SocketAddr,

    /// handler is the request handler.
    handler: QUICServerHandler,

    /// shutdown is used to shutdown the TCP server.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the TCP server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

impl QUICServer {
    /// Creates a new TCPServer.
    #[instrument(skip_all)]
    pub fn new(
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

        let handler = QUICServerHandler {
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

    /// Starts the QUIC upload server.
    #[instrument(skip_all)]
    pub async fn run(&mut self, quic_server_started_barrier: Arc<Barrier>) -> ClientResult<()> {
        // Build QUIC server config (for demo, use insecure config, production use real certs!)
        let mut server_config = ServerConfig::with_single_cert(
            quinn::CertificateChain::from_certs(vec![quinn::Certificate::from_der(&[0u8; 1]).unwrap()]),
            quinn::PrivateKey::from_der(&[0u8; 1]).unwrap(),
        ).unwrap();
        server_config.transport = Arc::new(quinn::TransportConfig::default());

        let (endpoint, mut incoming) = Endpoint::server(server_config, self.addr).map_err(|err| {
            error!("Failed to bind QUIC endpoint to {}: {}", self.addr, err);
            ClientError::HostNotFound(self.addr.to_string())
        })?;

        info!("QUIC upload server listening on {}", self.addr);

        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Notify that the server is ready
        quic_server_started_barrier.wait().await;
        info!("QUIC upload server is ready");

        loop {
            tokio::select! {
                // Accept new connections
                Some(connecting) = incoming.next() => {
                    match connecting.await {
                        Ok(new_conn) => {
                            let handler = self.handler.clone();
                            tokio::spawn(async move {
                                let mut bi_streams = new_conn.bi_streams;
                                while let Some(Ok((send, recv))) = bi_streams.next().await {
                                    if let Err(err) = handler.handle_connection(send, recv).await {
                                        error!("Error handling QUIC stream: {}", err);
                                    }
                                }
                            });
                        }
                        Err(err) => {
                            error!("Failed to accept QUIC connection: {}", err);
                        }
                    }
                }
                // Handle shutdown signal
                _ = shutdown.recv() => {
                    info!("QUIC upload server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Gets the socket file descriptor from a TcpStream
    #[cfg(unix)]
    fn get_socket_fd(stream: &quinn::RecvStream) -> Option<RawFd> {
        // For other platforms, we can use the peer address as identifier
        stream.peer_addr().ok().map(|addr| addr.to_string().into_bytes().into_iter().collect())
    }

    /// Gets the socket handle from a TcpStream (Windows)
    #[cfg(windows)]
    fn get_socket_fd(stream: &quinn::RecvStream) -> Option<RawSocket> {
        // For other platforms, we can use the peer address as identifier
        stream.peer_addr().ok().map(|addr| addr.to_string().into_bytes().into_iter().collect())
    }

    /// Gets socket information as a generic identifier
    #[cfg(not(any(unix, windows)))]
    fn get_socket_fd(stream: &quinn::RecvStream) -> Option<String> {
        // For other platforms, we can use the peer address as identifier
        stream.peer_addr().ok().map(|addr| addr.to_string())
    }

}

/// QUICServerHandler handles QUIC connections and requests.
#[derive(Clone)]
pub struct QUICServerHandler {
    /// interface is the network interface.
    interface: Interface,

    /// socket_path is the path of the unix domain socket.
    socket_path: PathBuf,

    id_generator: Arc<IDGenerator>,

    storage: Arc<Storage>,

    config: Arc<Config>,
}

impl QUICServerHandler {
    /// Handles a single QUIC bi-directional stream.
    async fn handle_connection(&self, mut send: SendStream, mut recv: RecvStream) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            // Read header
            let mut header_buf = [0u8; HEADER_SIZE];
            match recv.read_exact(&mut header_buf).await {
                Ok(_) => {}
                Err(err) => {
                    error!("Failed to read header: {}", err);
                    return Err(err.into());
                }
            }
            info!("QUICServer can receive data");
            let length = u32::from_be_bytes(header_buf[2..HEADER_SIZE].try_into().expect("Failed to read value length")) as usize;

            // Read request data
            let mut request_data = vec![0u8; length];
            recv.read_exact(&mut request_data).await?;

            request_data.splice(0..0, header_buf.iter().copied());

            let deserialized = Vortex::from_bytes(request_data.into()).expect("Failed to deserialize packet");

            // Process request based on message type
            let response_result = match deserialized {
                Vortex::DownloadPiece(_, download_piece) => {
                    self.send_piece(&download_piece).await
                }
                _ => {
                    error!("Unsupported message type: {:?}", deserialized.tag());
                    Err("Unsupported message type".to_string())
                }
            };

            // Send response
            match response_result {
                Ok(response_data) => {
                    let rsp = Vortex::new(Tag::PieceContent, response_data.into()).expect("Failed to create Vortex packet");
                    send.write_all(&rsp.to_bytes()).await?;
                }
                Err(error_message) => {
                    let ersp = Vortex::new(Tag::Error, error_message.into()).expect("Failed to create Vortex packet");
                    send.write_all(&ersp.to_bytes()).await?;
                }
            }
        }

        // Ok(())
    }

    /// Handles download piece request.
    #[instrument(skip_all, fields(host_id, task_id, piece_id))]
    async fn send_piece(
        &self,
        request: &DownloadPiece,
    ) -> Result<Vec<u8>, String> {

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
            .upload_piece(piece_id.as_str(), task_id, None)
            .await
            .map_err(|err| {
                format!("Failed to get piece content: {}", err)
            })?;
        // Read the content of the piece.
        let mut content = vec![0; piece.length as usize];
        reader.read_exact(&mut content).await.map_err(|err| {
            format!("Failed to read piece content: {}", err)
        })?;

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
    async fn send_persistent_cache_piece(
        &self,
        request: &DownloadPiece,
    ) -> Result<Vec<u8>, String> {

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
            .upload_persistent_cache_piece(piece_id.as_str(), task_id, None)
            .await
            .map_err(|err| {
                format!("Failed to get persistent cache piece content: {}", err)
            })?;
        // Read the content of the piece.
        let mut content = vec![0; piece.length as usize];
        reader.read_exact(&mut content).await.map_err(|err| {
            format!("Failed to read persistent cache piece content: {}", err)
        })?;

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
}

