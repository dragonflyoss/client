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

use dragonfly_api::dfdaemon::v2::{
    DownloadPieceResponse,
    DownloadPersistentCachePieceResponse,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    Error as ClientError, Result as ClientResult,
};
use prost::Message;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::error;
use vortex_protocol::{
    Vortex,
    tlv::Tag,
    tlv::download_piece::DownloadPiece,
};
use bytes::Bytes;
use std::net::SocketAddr;
use std::any::TypeId;
use quinn::{Endpoint, Connection, ClientConfig, ConnectError, ConnectionError};
use rustls::ClientConfig as RustlsClientConfig;
use rustls::RootCertStore;
use std::time::Duration;
use quinn::crypto::rustls::QuicClientConfig;

const HEADER_SIZE: usize = 6;

/// QUICClient is a QUIC-based client for dfdaemon download service.
#[derive(Clone)]
pub struct QUICClient {
    /// connection is the single long QUIC connection.
    connection: Arc<Mutex<Connection>>,
}

impl QUICClient {
    /// Creates a new QUICClient.
    pub async fn new(
        config: Arc<Config>,
        addr: String,
    ) -> ClientResult<Self> {
        // Validate address format
        Self::validate(addr.as_str())?;

        // Basic TLS configuration using rustls
        let mut root_store = RootCertStore::empty();
        // Add trusted root certificates if needed
        // for cert in trusted_certs {
        //     root_store.add(cert)?;
        // }

        let tls_config = RustlsClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        // Create client config with default transport settings
        let client_config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls_config)?));
        // Create endpoint
        let addr1 = addr.parse::<SocketAddr>().unwrap();
        let mut endpoint = Endpoint::client(addr1)
            .map_err(|err| {
                error!("Failed to create QUIC endpoint: {}", err);
                ClientError::IO(err)
            })?;
        endpoint.set_default_client_config(client_config);

        // Parse server address
        let server_addr: SocketAddr = addr.parse().map_err(|e| {
            error!("Failed to parse server address: {}", e);
            ClientError::InvalidParameter
        })?;

        let connecting = endpoint.connect(server_addr, "localhost")
            .map_err(|err| {
                error!("Connection initialization failed: {}", err);
                ClientError::IO(err.into())
            })?;

        // Establish connection with timeout
        let timeout = config.download.piece_timeout;
        let connection = match tokio::time::timeout(timeout, connecting).await {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                error!("Connection failed: {}", e);
                return Err(ClientError::IO(e.into()));
            }
            Err(_) => {
                error!("Connection timed out");
                return Err(ClientError::SendTimeout);
            }
        };

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Validates the address format.
    fn validate(address: &str) -> ClientResult<()> {
        address.parse::<SocketAddr>().map_err(|_| {
            error!("Invalid address: {}", address);
            ClientError::InvalidParameter
        })?;
        Ok(())
    }

    /// Sends a request and receives a response using the Vortex protocol.
    pub async fn send<R: Message + Default + 'static>(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<R> {
        // Create Vortex request packet
        let value = DownloadPiece::new(task_id.to_string(), number).into();
        let packet = match TypeId::of::<R>() {
            id if id == TypeId::of::<DownloadPieceResponse>() => {
                Vortex::new(Tag::DownloadPiece, value)
                    .map_err(|_| ClientError::ValidationError("Failed to create Vortex packet".to_string()))?
            }
            id if id == TypeId::of::<DownloadPersistentCachePieceResponse>() => {
                Vortex::new(Tag::DownloadPiece, value)
                    .map_err(|_| ClientError::ValidationError("Failed to create Vortex packet".to_string()))?
            }
            _ => return Err(ClientError::Unimplemented),
        };

        let mut connection = self.connection.lock().await;
        let (mut send, mut recv) = connection.open_bi().await.map_err(|e| {
            error!("Failed to open bidirectional stream: {}", e);
            ClientError::IO(e.into())
        })?;

        // Send request data
        send.write_all(&packet.to_bytes()).await.map_err(|e| {
            error!("Failed to send request: {}", e);
            ClientError::MpscSend(e.to_string())
        })?;
        send.finish().map_err(|e| {
            error!("Failed to finish sending stream: {}", e);
            ClientError::IO(e.into())
        })?;

        // Read response header
        let mut header_buf = [0u8; HEADER_SIZE];
        recv.read_exact(&mut header_buf).await.map_err(|e| {
            error!("Failed to read response header: {}", e);
            ClientError::UnexpectedResponse
        })?;

        let length = u32::from_be_bytes(header_buf[2..].try_into().map_err(|_| ClientError::InvalidPieceLength)?) as usize;
        let mut response_data = vec![0u8; length];
        recv.read_exact(&mut response_data).await.map_err(|e| {
            error!("Failed to read response data: {}", e);
            ClientError::UnexpectedResponse
        })?;

        // Parse response
        let mut complete_data = header_buf.to_vec();
        complete_data.extend(response_data);
        let deserialized = Vortex::from_bytes(complete_data.into())
            .map_err(|_| ClientError::ValidationError("Failed to deserialize packet".to_string()))?;

        match deserialized {
            Vortex::PieceContent(_, piece_content) => {
                let bytes: Bytes = piece_content.into();
                R::decode(bytes).map_err(|e| {
                    error!("Failed to decode response: {}", e);
                    ClientError::ValidationError(e.to_string())
                })
            }
            Vortex::Error(_, err) => {
                error!("Invalid error response: {}", err.message());
                let code: u8 = err.code().into();
                Err(ClientError::InvalidState(code.to_string()))
            }
            _ => Err(ClientError::UnexpectedResponse),
        }
    }
}

