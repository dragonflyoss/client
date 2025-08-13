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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream as TokioTcpStream;
use tokio::time;
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

const HEADER_SIZE: usize = 6;

/// TCPClient is a TCP-based client for dfdaemon upload service.
#[derive(Clone)]
pub struct TCPClient {
    /// connection is the single long TCP connection.
    connection: Arc<Mutex<TokioTcpStream>>,
}

impl TCPClient {
    /// Creates a new TCPClient.
    pub async fn new(
        config: Arc<Config>,
        addr: String,
    ) -> ClientResult<Self> {
        // Validate address
        Self::validate(addr.as_str())?;

        // Establishes a TCP connection to the server.
        let timeout = config.download.piece_timeout;
        match time::timeout(timeout, TokioTcpStream::connect(addr.clone())).await {
            Ok(Ok(stream)) => {
                Ok(Self {connection: Arc::new(Mutex::new(stream))})
            }
            Ok(Err(err)) => {
                error!("Failed to connect to {}: {}", addr, err);
                Err(ClientError::IO(err))
            },
            Err(_) => {
                error!("Connection timeout to {}", addr);
                Err(ClientError::SendTimeout)
            }
        }
    }

    /// Validate the address.
    fn validate(address: &str) -> ClientResult<()> {
        match address.parse::<SocketAddr>() {
            Ok(_) => Ok(()),
            Err(_) => {
                error!("invalid address: {}", address);
                Err(ClientError::InvalidParameter)
            }
        }
    }

    /// Sends a request and receives a response.
    pub async fn send<R: Message + Default + 'static>(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<R> {
        // New request
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
            _ => {
                return Err(ClientError::Unimplemented);
            }
        };

        let mut stream = self.connection.lock().await;

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

        let length = u32::from_be_bytes(header_buf[2..HEADER_SIZE]
            .try_into()
            .map_err(|_| ClientError::InvalidPieceLength)?) as usize;

        // Read response data
        let mut response_data = vec![0u8; length];
        stream.read_exact(&mut response_data).await.map_err(|err| {
            error!("Failed to read response data: {}", err);
            ClientError::UnexpectedResponse
        })?;
        drop(stream);

        let mut complete_data = Vec::with_capacity(header_buf.len() + response_data.len());
        complete_data.extend_from_slice(&header_buf);
        complete_data.extend_from_slice(&response_data);

        let deserialized = Vortex::from_bytes(complete_data.into())
            .map_err(|_| ClientError::ValidationError("Failed to deserialize packet".to_string()))?;

        match deserialized {
            Vortex::DownloadPiece(_, _) |
            Vortex::Reserved(_) |
            Vortex::Close(_) => Err(ClientError::UnexpectedResponse),
            Vortex::PieceContent(_, piece_content) => {
                let bytes_back: Bytes = piece_content.into();
                R::decode(bytes_back).map_err(|err| {
                    error!("Failed to decode response: {}", err);
                    ClientError::ValidationError(err.to_string())
                })
            }
            Vortex::Error(_, err) => {
                error!("Invalid error response: {}", err.message());
                let code: u8 = err.code().into();
                Err(ClientError::InvalidState(code.to_string()))
            }
        }
    }
}
