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
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use quinn::{ClientConfig, Endpoint, NewConnection, RecvStream, SendStream};
use tokio::time::timeout;
use tracing::{error, info, instrument};
use vortex_protocol::{
    Vortex,
    tlv::Tag,
    tlv::download_piece::DownloadPiece,
};
use bytes::Bytes;
use validator::{Validate, ValidationError};
use std::net::SocketAddr;

const HEADER_SIZE: usize = 6;

/// QuicClient is a Quic-based client for dfdaemon upload service.
#[derive(Clone, Validate)]
pub struct QuicClient {
    /// server_addr is the address of the QUIC server.
    #[validate(custom = "validate_ip_port")]
    addr: String,
    /// timeout is the request timeout.
    timeout: Duration,
    endpoint: Endpoint,
}

/// Validate the address.
fn validate_ip_port(address: &str) -> Result<(), ValidationError> {
    match address.parse::<SocketAddr>() {
        Ok(_) => Ok(()),
        Err(_) => Err(ValidationError::new("invalid_ip_port")),
    }
}

impl QuicClient {
    /// Creates a new QuicClient.
    #[instrument(skip_all)]
    pub async fn new(
        config: Arc<Config>,
        addr: String,
    ) -> ClientResult<Self> {
        // If it is download piece, use the download piece timeout, otherwise use the
        // default request timeout.
        let mut timeout = config.download.piece_timeout;
        if timeout.is_zero() {
            timeout = Duration::from_secs(30);
        }

        // Setup QUIC endpoint (client)
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
            .map_err(|e| ClientError::ValidationError(e.to_string()))?;
        endpoint.set_default_client_config(ClientConfig::with_native_roots());

        let client = Self {
            addr,
            timeout,
            endpoint,
        };

        // Validate the address
        match client.validate() {
            Ok(_) => println!("Valid IP:Port"),
            Err(e) => println!("Validation error: {:?}", e),
        }
        Ok(client)
    }

    /// Downloads piece content from parent.
    #[instrument(skip_all)]
    pub async fn download_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<DownloadPieceResponse> {
        let value: Bytes = DownloadPiece::new(task_id.to_string(), number).into();
        let packet = Vortex::new(Tag::DownloadPiece, value).expect("Failed to create Vortex packet");        
        self.send_and_receive(&packet).await
    }

    /// Downloads piece content from parent.
    #[instrument(skip_all)]
    pub async fn download_persistent_cache_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<DownloadPersistentCachePieceResponse> {
        let value: Bytes = DownloadPiece::new(task_id.to_string(), number).into();
        let packet = Vortex::new(Tag::DownloadPiece, value).expect("Failed to create Vortex packet");        
        self.send_and_receive(&packet).await
    }

    /// Establishes a QUIC connection to the server.
    async fn connect(&self) -> Result<NewConnection, ClientError> {
        let conn = timeout(self.timeout, self.endpoint.connect(self.addr.parse().unwrap(), "dragonfly")).await
            .map_err(|_| ClientError::HostNotFound(self.addr.clone()))?
            .map_err(|err| {
                error!("Failed to connect to {}: {}", self.addr, err);
                ClientError::HostNotFound(self.addr.clone())
            })?;
        Ok(conn)
    }

    /// Sends a request and receives a response over QUIC.
    async fn send_and_receive<R: Message + Default>(
        &self,
        request: &Vortex,
    ) -> ClientResult<R> {
        let conn = self.connect().await?;
        let (mut send, mut recv) = conn.connection.open_bi().await.map_err(|err| {
            error!("Failed to open QUIC stream: {}", err);
            ClientError::MpscSend(err.to_string())
        })?;

        // Send request data
        send.write_all(&request.to_bytes()).await.map_err(|err| {
            error!("Failed to send request: {}", err);
            ClientError::MpscSend(err.to_string())
        })?;
        send.finish().await.map_err(|err| {
            error!("Failed to finish sending: {}", err);
            ClientError::MpscSend(err.to_string())
        })?;

        // Read response header
        let mut header_buf = [0u8; HEADER_SIZE];
        recv.read_exact(&mut header_buf).await.map_err(|err| {
            error!("Failed to read response header: {}", err);
            ClientError::UnexpectedResponse
        })?;
        info!("QuicClient can receive data");
        let length = u32::from_be_bytes(header_buf[2..HEADER_SIZE].try_into().expect("Failed to read value length")) as usize;

        // Read response data
        let mut response_data = vec![0u8; length];
        recv.read_exact(&mut response_data).await.map_err(|err| {
            error!("Failed to read response data: {}", err);
            ClientError::UnexpectedResponse
        })?;

        response_data.splice(0..0, header_buf.iter().copied());

        let deserialized = Vortex::from_bytes(response_data.into()).expect("Failed to deserialize packet");

        match deserialized {
            Vortex::DownloadPiece(_, _) => {
                return Err(ClientError::UnexpectedResponse);
            }
            Vortex::PieceContent(_, piece_content) => {
                let bytes_back: Bytes = piece_content.into();
                return R::decode(&*bytes_back).map_err(|err| {
                    error!("Failed to decode response: {}", err);
                    ClientError::ValidationError(err.to_string())
                })
            }
            Vortex::Reserved(_) => {return Err(ClientError::UnexpectedResponse);}
            Vortex::Close(_) => {return Err(ClientError::UnexpectedResponse);}
            Vortex::Error(_, error) => {
                error!("Invalid error response: {}", error.message());
                let code: u8 = error.code().into();
                return Err(ClientError::InvalidState(code.to_string()));
            }
        }
    }

}
