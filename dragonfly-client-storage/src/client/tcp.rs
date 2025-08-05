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
use tokio::net::TcpStream as TokioTcpStream;
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
use std::any::TypeId;

const HEADER_SIZE: usize = 6;

/// TCPClient is a TCP-based client for dfdaemon upload service.
#[derive(Clone, Validate)]
pub struct TCPClient {
    /// server_addr is the address of the TCP server.
    addr: String,
    /// timeout is the request timeout.
    timeout: Duration,
}

/// Validate the address.
fn validate(address: &str) -> Result<(), ValidationError> {
    match address.parse::<SocketAddr>() {
        Ok(_) => Ok(()),
        Err(_) => Err(ValidationError::new("invalid_ip_port")),
    }
}

impl TCPClient {
    /// Creates a new TCPClient.
    #[instrument(skip_all)]
    pub async fn new(
        config: Arc<Config>,
        addr: String,
    ) -> ClientResult<Self> {
        // Validate address
        match validate(addr.as_str()) {
            Ok(_) => println!("Valid IP:Port"),
            Err(e) => println!("Validation error: {:?}", e),
        }
        
        Ok(Self {
            addr,
            timeout: config.download.piece_timeout,
        })
    }

    /// Establishes a TCP connection to the server.
    async fn connect(&self) -> Result<TokioTcpStream, ClientError> {
        timeout(self.timeout, TokioTcpStream::connect(&self.addr))
            .await
            .map_err(|_| ClientError::HostNotFound(self.addr.clone()))?
            .map_err(|err| {
                error!("Failed to connect to {}: {}", self.addr, err);
                ClientError::HostNotFound(self.addr.clone())
            })
    }

    /// Sends a request and receives a response.
    pub async fn send<R: Message + Default + 'static>(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<R> {
        // New request
        let value: Bytes = DownloadPiece::new(task_id.to_string(), number).into();
        let packet ;
        if TypeId::of::<R>() == TypeId::of::<DownloadPieceResponse>() {
            packet = Vortex::new(Tag::DownloadPiece, value).expect("Failed to create Vortex packet");
        } else if TypeId::of::<R>() == TypeId::of::<DownloadPersistentCachePieceResponse>() {
            packet = Vortex::new(Tag::DownloadPiece, value).expect("Failed to create Vortex packet");
        } else {
            return Err(ClientError::ValidationError(("Unexpected response type").to_string()));
        }
        let mut stream = self.connect().await?;
        
        // Send request data
        stream.write_all(&packet.to_bytes()).await.map_err(|err| {
            error!("Failed to send request: {}", err);
            ClientError::MpscSend(err.to_string())
        })?;
        
        // Read response header
        let mut header_buf = [0u8; HEADER_SIZE];
        stream.read_exact(&mut header_buf).await.map_err(|err| {
            error!("Failed to read response header: {}", err);
            ClientError::UnexpectedResponse
        })?;
        info!("TCPClient can receive data");
        let length = u32::from_be_bytes(header_buf[2..HEADER_SIZE].try_into().expect("Failed to read value length")) as usize;
        
        // Read response data
        let mut response_data = vec![0u8; length];
        stream.read_exact(&mut response_data).await.map_err(|err| {
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
        };
    }

}