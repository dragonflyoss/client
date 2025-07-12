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

use crate::quic::types::{QuicConfig, QuicMessage, QuicMessagePayload, QuicMessageType};
use dragonfly_api::dfdaemon::v2::{
    DownloadPieceRequest, DownloadPieceResponse, DownloadTaskRequest, DownloadTaskResponse,
    SyncPiecesRequest, SyncPiecesResponse, DownloadPersistentCachePieceRequest, DownloadPersistentCachePieceResponse,
};
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use quinn::{ClientConfig, Connection, Endpoint};
use rustls::{Certificate, PrivateKey};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument, warn};

/// QUIC client for downloading pieces
pub struct QuicClient {
    /// QUIC endpoint
    endpoint: Endpoint,
    /// Connection to server
    connection: Arc<Mutex<Option<Connection>>>,
    /// Configuration
    config: QuicConfig,
}

impl QuicClient {
    /// Create a new QUIC client
    pub async fn new(config: QuicConfig) -> ClientResult<Self> {
        // Create client configuration
        let client_config = Self::create_client_config()?;
        
        // Create endpoint
        let endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap())?;
        let endpoint = endpoint.with_default_crypto_config(client_config);
        
        Ok(Self {
            endpoint,
            connection: Arc::new(Mutex::new(None)),
            config,
        })
    }

    /// Create client configuration
    fn create_client_config() -> ClientResult<ClientConfig> {
        let mut crypto = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_native_roots()
            .with_no_client_auth();
        
        // For development, accept invalid certificates
        crypto.dangerous().set_certificate_verifier(Arc::new(
            DangerousCertificateVerifier,
        ));
        
        Ok(ClientConfig::new(Arc::new(crypto)))
    }

    /// Connect to server
    async fn connect(&self) -> ClientResult<Connection> {
        let mut connection_guard = self.connection.lock().await;
        
        if let Some(conn) = connection_guard.as_ref() {
            if conn.connection.stable_id() != 0 {
                return Ok(conn.clone());
            }
        }
        
        // Connect to server
        let connection = self.endpoint
            .connect(self.config.addr.parse()?, "localhost")?
            .await?;
        
        info!("Connected to QUIC server at {}", self.config.addr);
        
        *connection_guard = Some(connection.clone());
        Ok(connection)
    }

    /// Download piece from server
    #[instrument(skip_all)]
    pub async fn download_piece(
        &self,
        request: DownloadPieceRequest,
    ) -> ClientResult<DownloadPieceResponse> {
        let connection = self.connect().await?;
        
        // Create bidirectional stream
        let (mut send, mut recv) = connection.open_bi().await?;
        
        // Create download piece message
        let message = QuicMessage::new(
            QuicMessageType::DownloadPiece,
            QuicMessagePayload::DownloadPieceRequest(request),
        );
        
        // Serialize and send message
        let message_bytes = message.serialize()?;
        send.write_all(&message_bytes).await?;
        send.finish().await?;
        
        // Read response
        let mut response_data = Vec::new();
        let mut buffer = [0u8; 4096];
        
        loop {
            match recv.read(&mut buffer).await {
                Ok(Some(bytes_read)) => {
                    response_data.extend_from_slice(&buffer[..bytes_read]);
                }
                Ok(None) => break,
                Err(e) => {
                    error!("Failed to read response: {}", e);
                    return Err(ClientError::NetworkError);
                }
            }
        }
        
        // Deserialize response
        let response_message = QuicMessage::deserialize(&response_data)?;
        
        match response_message.payload {
            QuicMessagePayload::DownloadPieceResponse(response) => Ok(response),
            _ => Err(ClientError::InvalidParameter),
        }
    }

    /// Download task from server
    #[instrument(skip_all)]
    pub async fn download_task(
        &self,
        request: DownloadTaskRequest,
    ) -> ClientResult<DownloadTaskResponse> {
        let connection = self.connect().await?;
        
        // Create bidirectional stream
        let (mut send, mut recv) = connection.open_bi().await?;
        
        // Create download task message
        let message = QuicMessage::new(
            QuicMessageType::DownloadTask,
            QuicMessagePayload::DownloadTaskRequest(request),
        );
        
        // Serialize and send message
        let message_bytes = message.serialize()?;
        send.write_all(&message_bytes).await?;
        send.finish().await?;
        
        // Read response
        let mut response_data = Vec::new();
        let mut buffer = [0u8; 4096];
        
        loop {
            match recv.read(&mut buffer).await {
                Ok(Some(bytes_read)) => {
                    response_data.extend_from_slice(&buffer[..bytes_read]);
                }
                Ok(None) => break,
                Err(e) => {
                    error!("Failed to read response: {}", e);
                    return Err(ClientError::NetworkError);
                }
            }
        }
        
        // Deserialize response
        let response_message = QuicMessage::deserialize(&response_data)?;
        
        match response_message.payload {
            QuicMessagePayload::DownloadTaskResponse(response) => Ok(response),
            _ => Err(ClientError::InvalidParameter),
        }
    }

    /// Sync pieces with server
    #[instrument(skip_all)]
    pub async fn sync_pieces(
        &self,
        request: SyncPiecesRequest,
    ) -> ClientResult<SyncPiecesResponse> {
        let connection = self.connect().await?;
        
        // Create bidirectional stream
        let (mut send, mut recv) = connection.open_bi().await?;
        
        // Create sync pieces message
        let message = QuicMessage::new(
            QuicMessageType::SyncPieces,
            QuicMessagePayload::SyncPiecesRequest(request),
        );
        
        // Serialize and send message
        let message_bytes = message.serialize()?;
        send.write_all(&message_bytes).await?;
        send.finish().await?;
        
        // Read response
        let mut response_data = Vec::new();
        let mut buffer = [0u8; 4096];
        
        loop {
            match recv.read(&mut buffer).await {
                Ok(Some(bytes_read)) => {
                    response_data.extend_from_slice(&buffer[..bytes_read]);
                }
                Ok(None) => break,
                Err(e) => {
                    error!("Failed to read response: {}", e);
                    return Err(ClientError::NetworkError);
                }
            }
        }
        
        // Deserialize response
        let response_message = QuicMessage::deserialize(&response_data)?;
        
        match response_message.payload {
            QuicMessagePayload::SyncPiecesResponse(response) => Ok(response),
            _ => Err(ClientError::InvalidParameter),
        }
    }

    /// Download persistent cache piece from server
    #[instrument(skip_all)]
    pub async fn download_persistent_cache_piece(
        &self,
        request: DownloadPersistentCachePieceRequest,
    ) -> ClientResult<DownloadPersistentCachePieceResponse> {
        let connection = self.connect().await?;
        
        // Create bidirectional stream
        let (mut send, mut recv) = connection.open_bi().await?;
        
        // Create download persistent cache piece message
        let message = QuicMessage::new(
            QuicMessageType::DownloadPersistentCachePiece,
            QuicMessagePayload::DownloadPersistentCachePieceRequest(request),
        );
        
        // Serialize and send message
        let message_bytes = message.serialize()?;
        send.write_all(&message_bytes).await?;
        send.finish().await?;
        
        // Read response
        let mut response_data = Vec::new();
        let mut buffer = [0u8; 4096];
        
        loop {
            match recv.read(&mut buffer).await {
                Ok(Some(bytes_read)) => {
                    response_data.extend_from_slice(&buffer[..bytes_read]);
                }
                Ok(None) => break,
                Err(e) => {
                    error!("Failed to read response: {}", e);
                    return Err(ClientError::NetworkError);
                }
            }
        }
        
        // Deserialize response
        let response_message = QuicMessage::deserialize(&response_data)?;
        
        match response_message.payload {
            QuicMessagePayload::DownloadPersistentCachePieceResponse(response) => Ok(response),
            _ => Err(ClientError::InvalidParameter),
        }
    }

    /// Health check
    #[instrument(skip_all)]
    pub async fn health_check(&self) -> ClientResult<String> {
        let connection = self.connect().await?;
        
        // Create bidirectional stream
        let (mut send, mut recv) = connection.open_bi().await?;
        
        // Create health check message
        let message = QuicMessage::new(
            QuicMessageType::HealthCheck,
            QuicMessagePayload::HealthCheck,
        );
        
        // Serialize and send message
        let message_bytes = message.serialize()?;
        send.write_all(&message_bytes).await?;
        send.finish().await?;
        
        // Read response
        let mut response_data = Vec::new();
        let mut buffer = [0u8; 4096];
        
        loop {
            match recv.read(&mut buffer).await {
                Ok(Some(bytes_read)) => {
                    response_data.extend_from_slice(&buffer[..bytes_read]);
                }
                Ok(None) => break,
                Err(e) => {
                    error!("Failed to read response: {}", e);
                    return Err(ClientError::NetworkError);
                }
            }
        }
        
        // Deserialize response
        let response_message = QuicMessage::deserialize(&response_data)?;
        
        match response_message.payload {
            QuicMessagePayload::HealthCheckResponse { status } => Ok(status),
            _ => Err(ClientError::InvalidParameter),
        }
    }
}

/// Dangerous certificate verifier for development
struct DangerousCertificateVerifier;

impl rustls::client::danger::ServerCertVerifier for DangerousCertificateVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<(), rustls::Error> {
        Ok(())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<(), rustls::Error> {
        Ok(())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
        ]
    }
} 