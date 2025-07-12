/*
 *     Copyright 2024 The Dragonfly Authors
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

use crate::quic::types::{QuicMessage, QuicMessagePayload, QuicMessageType, QuicServerConfig};
use dragonfly_api::dfdaemon::v2::{
    DownloadPieceRequest, DownloadPieceResponse, DownloadTaskRequest, DownloadTaskResponse,
    SyncPiecesRequest, SyncPiecesResponse, DownloadPersistentCachePieceRequest, DownloadPersistentCachePieceResponse,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use quinn::{Endpoint, ServerConfig};
use rustls::{Certificate, PrivateKey};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, warn};

/// QUIC server for handling piece download requests
pub struct QuicServer {
    /// Server configuration
    config: QuicServerConfig,
    /// Dragonfly configuration
    dfdaemon_config: Arc<Config>,
    /// Storage instance for accessing pieces
    storage: Arc<crate::Storage>,
    /// Shutdown channel
    shutdown: mpsc::UnboundedReceiver<()>,
}

impl QuicServer {
    /// Create a new QUIC server
    pub fn new(
        config: QuicServerConfig,
        dfdaemon_config: Arc<Config>,
        storage: Arc<crate::Storage>,
        shutdown: mpsc::UnboundedReceiver<()>,
    ) -> Self {
        Self {
            config,
            dfdaemon_config,
            storage,
            shutdown,
        }
    }

    /// Start the QUIC server
    #[instrument(skip_all)]
    pub async fn run(&mut self) -> ClientResult<()> {
        // Create server configuration
        let server_config = Self::create_server_config()?;
        
        // Create endpoint
        let endpoint = Endpoint::server(server_config, self.config.listen_addr.parse()?)?;
        
        info!("QUIC server listening on {}", self.config.listen_addr);
        
        // Accept connections
        while let Some(connection) = endpoint.accept().await {
            let connection = connection.await?;
            let peer_addr = connection.remote_address();
            
            info!("New QUIC connection from {}", peer_addr);
            
            // Handle connection in a separate task
            let config = self.config.clone();
            let dfdaemon_config = self.dfdaemon_config.clone();
            let storage = self.storage.clone();
            
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(connection, config, dfdaemon_config, storage).await {
                    error!("Connection error: {}", e);
                }
            });
        }
        
        Ok(())
    }

    /// Create server configuration
    fn create_server_config() -> ClientResult<ServerConfig> {
        let mut crypto = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth();
        
        // For development, use a self-signed certificate
        let cert = Self::generate_self_signed_cert()?;
        let key = Self::generate_private_key()?;
        
        crypto
            .single_cert(vec![cert], key)
            .map_err(|e| ClientError::InvalidParameter)?;
        
        Ok(ServerConfig::with_crypto(Arc::new(crypto)))
    }

    /// Generate self-signed certificate for development
    fn generate_self_signed_cert() -> ClientResult<Certificate> {
        // In a real implementation, you would load from file or generate properly
        // For now, we'll create a simple certificate using rcgen
        let mut certificate_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]);
        certificate_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        
        let certificate = rcgen::Certificate::from_params(certificate_params)
            .map_err(|_| ClientError::InvalidParameter)?;
        
        let cert_der = certificate.serialize_der()
            .map_err(|_| ClientError::InvalidParameter)?;
        
        Ok(Certificate(cert_der))
    }

    /// Generate private key for development
    fn generate_private_key() -> ClientResult<PrivateKey> {
        // In a real implementation, you would load from file or generate properly
        // For now, we'll create a simple key using rcgen
        let mut certificate_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]);
        certificate_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        
        let certificate = rcgen::Certificate::from_params(certificate_params)
            .map_err(|_| ClientError::InvalidParameter)?;
        
        let key_der = certificate.serialize_private_key_der();
        
        Ok(PrivateKey(key_der))
    }

    /// Handle incoming connection
    async fn handle_connection(
        connection: quinn::Connecting,
        config: QuicServerConfig,
        dfdaemon_config: Arc<Config>,
        storage: Arc<crate::Storage>,
    ) -> ClientResult<()> {
        let connection = connection.await?;
        let peer_addr = connection.remote_address();
        
        info!("Handling QUIC connection from {}", peer_addr);
        
        // Accept bidirectional streams
        while let Ok((send, recv)) = connection.accept_bi().await {
            let config = config.clone();
            let dfdaemon_config = dfdaemon_config.clone();
            let storage = storage.clone();
            
            tokio::spawn(async move {
                if let Err(e) = Self::handle_stream(send, recv, config, dfdaemon_config, storage).await {
                    error!("Stream error: {}", e);
                }
            });
        }
        
        Ok(())
    }

    /// Handle bidirectional stream
    async fn handle_stream(
        mut send: quinn::SendStream,
        mut recv: quinn::RecvStream,
        _config: QuicServerConfig,
        _dfdaemon_config: Arc<Config>,
        storage: Arc<crate::Storage>,
    ) -> ClientResult<()> {
        // Read request
        let mut request_data = Vec::new();
        let mut buffer = [0u8; 4096];
        
        loop {
            match recv.read(&mut buffer).await {
                Ok(Some(bytes_read)) => {
                    request_data.extend_from_slice(&buffer[..bytes_read]);
                }
                Ok(None) => break,
                Err(e) => {
                    error!("Failed to read request: {}", e);
                    return Err(ClientError::NetworkError);
                }
            }
        }
        
        // Deserialize request
        let request_message = QuicMessage::deserialize(&request_data)?;
        
        // Handle request based on message type
        let response_message = match request_message.payload {
            QuicMessagePayload::DownloadPieceRequest(request) => {
                Self::handle_download_piece(request, storage.clone()).await?
            }
            QuicMessagePayload::DownloadTaskRequest(request) => {
                Self::handle_download_task(request, storage.clone()).await?
            }
            QuicMessagePayload::SyncPiecesRequest(request) => {
                Self::handle_sync_pieces(request, storage.clone()).await?
            }
            QuicMessagePayload::DownloadPersistentCachePieceRequest(request) => {
                Self::handle_download_persistent_cache_piece(request, storage.clone()).await?
            }
            QuicMessagePayload::HealthCheck => {
                Self::handle_health_check().await?
            }
            _ => {
                error!("Unknown message type");
                return Err(ClientError::InvalidParameter);
            }
        };
        
        // Serialize and send response
        let response_bytes = response_message.serialize()?;
        send.write_all(&response_bytes).await?;
        send.finish().await?;
        
        Ok(())
    }

    /// Handle download piece request
    async fn handle_download_piece(
        request: DownloadPieceRequest,
        storage: Arc<crate::Storage>,
    ) -> ClientResult<QuicMessage> {
        info!("Handling download piece request: {:?}", request);
        
        // Extract piece information from request
        let piece_id = request.piece_id;
        let task_id = request.task_id;
        
        // Get piece from storage
        if let Some(piece) = storage.get_piece(&piece_id)? {
            // Create piece data for response
            let piece_data = dragonfly_api::common::v2::Piece {
                number: piece.number,
                parent_id: piece.parent_id.unwrap_or_default(),
                offset: piece.offset,
                length: piece.length,
                digest: piece.digest,
                content: piece.content,
                traffic_type: piece.traffic_type,
                cost: piece.cost,
                created_at: piece.created_at.timestamp(),
                updated_at: piece.updated_at.timestamp(),
            };
            
            let response = DownloadPieceResponse {
                piece: Some(piece_data),
            };
            
            let message = QuicMessage::new(
                QuicMessageType::DownloadPieceResponse,
                QuicMessagePayload::DownloadPieceResponse(response),
            );
            
            Ok(message)
        } else {
            // Piece not found
            let response = DownloadPieceResponse {
                piece: None,
            };
            
            let message = QuicMessage::new(
                QuicMessageType::DownloadPieceResponse,
                QuicMessagePayload::DownloadPieceResponse(response),
            );
            
            Ok(message)
        }
    }

    /// Handle download task request
    async fn handle_download_task(
        request: DownloadTaskRequest,
        storage: Arc<crate::Storage>,
    ) -> ClientResult<QuicMessage> {
        info!("Handling download task request: {:?}", request);
        
        // Extract task information from request
        let task_id = request.task_id;
        
        // Get task from storage
        if let Some(task) = storage.get_task(&task_id)? {
            // Create task data for response
            let task_data = dragonfly_api::common::v2::Task {
                id: task.id,
                url: task.url,
                task_type: task.task_type,
                filters: task.filters,
                header: task.header,
                piece_length: task.piece_length,
                content_length: task.content_length,
                piece_count: task.piece_count,
                range: task.range,
                pieces: task.pieces,
                state: task.state,
                peer_count: task.peer_count,
                created_at: task.created_at.timestamp(),
                updated_at: task.updated_at.timestamp(),
            };
            
            let response = DownloadTaskResponse {
                piece: Some(task_data),
            };
            
            let message = QuicMessage::new(
                QuicMessageType::DownloadTaskResponse,
                QuicMessagePayload::DownloadTaskResponse(response),
            );
            
            Ok(message)
        } else {
            // Task not found
            let response = DownloadTaskResponse {
                piece: None,
            };
            
            let message = QuicMessage::new(
                QuicMessageType::DownloadTaskResponse,
                QuicMessagePayload::DownloadTaskResponse(response),
            );
            
            Ok(message)
        }
    }

    /// Handle sync pieces request
    async fn handle_sync_pieces(
        request: SyncPiecesRequest,
        storage: Arc<crate::Storage>,
    ) -> ClientResult<QuicMessage> {
        info!("Handling sync pieces request: {:?}", request);
        
        // Extract task information from request
        let task_id = request.task_id;
        
        // Get pieces from storage
        let pieces = storage.get_pieces(&task_id)?;
        
        // Convert pieces to API format
        let api_pieces: Vec<dragonfly_api::common::v2::Piece> = pieces
            .into_iter()
            .map(|piece| dragonfly_api::common::v2::Piece {
                number: piece.number,
                parent_id: piece.parent_id.unwrap_or_default(),
                offset: piece.offset,
                length: piece.length,
                digest: piece.digest,
                content: piece.content,
                traffic_type: piece.traffic_type,
                cost: piece.cost,
                created_at: piece.created_at.timestamp(),
                updated_at: piece.updated_at.timestamp(),
            })
            .collect();
        
        let response = SyncPiecesResponse {
            pieces: api_pieces,
        };
        
        let message = QuicMessage::new(
            QuicMessageType::SyncPiecesResponse,
            QuicMessagePayload::SyncPiecesResponse(response),
        );
        
        Ok(message)
    }

    /// Handle download persistent cache piece request
    async fn handle_download_persistent_cache_piece(
        request: DownloadPersistentCachePieceRequest,
        storage: Arc<crate::Storage>,
    ) -> ClientResult<QuicMessage> {
        info!("Handling download persistent cache piece request: {:?}", request);
        
        // Extract piece information from request
        let piece_id = request.piece_id;
        let task_id = request.task_id;
        
        // Get persistent cache piece from storage
        if let Some(piece) = storage.get_persistent_cache_piece(&piece_id)? {
            // Create piece data for response
            let piece_data = dragonfly_api::common::v2::Piece {
                number: piece.number,
                parent_id: piece.parent_id.unwrap_or_default(),
                offset: piece.offset,
                length: piece.length,
                digest: piece.digest,
                content: piece.content,
                traffic_type: piece.traffic_type,
                cost: piece.cost,
                created_at: piece.created_at.timestamp(),
                updated_at: piece.updated_at.timestamp(),
            };
            
            let response = DownloadPersistentCachePieceResponse {
                piece: Some(piece_data),
            };
            
            let message = QuicMessage::new(
                QuicMessageType::DownloadPersistentCachePieceResponse,
                QuicMessagePayload::DownloadPersistentCachePieceResponse(response),
            );
            
            Ok(message)
        } else {
            // Piece not found
            let response = DownloadPersistentCachePieceResponse {
                piece: None,
            };
            
            let message = QuicMessage::new(
                QuicMessageType::DownloadPersistentCachePieceResponse,
                QuicMessagePayload::DownloadPersistentCachePieceResponse(response),
            );
            
            Ok(message)
        }
    }

    /// Handle health check
    async fn handle_health_check() -> ClientResult<QuicMessage> {
        info!("Handling health check request");
        
        let response = QuicMessage::new(
            QuicMessageType::HealthCheckResponse,
            QuicMessagePayload::HealthCheckResponse { 
                status: "OK".to_string() 
            },
        );
        
        Ok(response)
    }
}
