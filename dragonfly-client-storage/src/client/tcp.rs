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
    DownloadPieceRequest, 
    DownloadPieceResponse,
    DownloadPersistentCachePieceRequest,
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
use tracing::{error, instrument};
use url::Url;
use vortex_protocol::{
    Vortex,
    tlv::Tag,
};
use bytes::Bytes;
use crate::client::Client;

const HEADER_SIZE: usize = 6;

/// TCPClient is a TCP-based client for dfdaemon upload service.
#[derive(Clone)]
pub struct TCPClient {
    /// server_addr is the address of the TCP server.
    server_addr: String,
    /// timeout is the request timeout.
    timeout: Duration,
}

#[tonic::async_trait]
impl Client for TCPClient {
    /// Creates a new TCPClient.
    #[instrument(skip_all)]
    async fn new(
        config: Arc<Config>,
        addr: String,
        is_download_piece: bool,
    ) -> ClientResult<Self> {
        // Validate the address
        let _parsed_url = Url::parse(&format!("tcp://{}", addr))?;

        // If it is download piece, use the download piece timeout, otherwise use the
        // default request timeout.
        let timeout = if is_download_piece {
            config.download.piece_timeout
        } else {
            Duration::from_secs(30) // Default timeout
        };

        Ok(Self {
            server_addr: addr,
            timeout,
        })
    }

    
    /// Downloads piece content from parent.
    #[instrument(skip_all)]
    async fn download_piece(
        &self,
        request: DownloadPieceRequest,
        timeout: Duration,
    ) -> ClientResult<DownloadPieceResponse> {
        // let original_timeout = self.timeout;
        
        // Create a temporary client with custom timeout
        let mut temp_client = self.clone();
        temp_client.timeout = timeout;
        
        temp_client.send_request(Tag::DownloadPiece, &request).await
    }

    /// Downloads piece content from parent.
    #[instrument(skip_all)]
    async fn download_persistent_cache_piece(
        &self,
        request: DownloadPersistentCachePieceRequest,
        timeout: Duration,
    ) -> ClientResult<DownloadPersistentCachePieceResponse> {
        // let original_timeout = self.timeout;
        
        // Create a temporary client with custom timeout
        let mut temp_client = self.clone();
        temp_client.timeout = timeout;
        
        temp_client.send_request(Tag::DownloadPiece, &request).await
    }
}

impl TCPClient {
/// Establishes a TCP connection to the server.
    async fn connect(&self) -> Result<TokioTcpStream, ClientError> {
        timeout(self.timeout, TokioTcpStream::connect(&self.server_addr))
            .await
            .map_err(|_| ClientError::HostNotFound(self.server_addr.clone()))?
            .map_err(|err| {
                error!("Failed to connect to {}: {}", self.server_addr, err);
                ClientError::HostNotFound(self.server_addr.clone())
            })
    }

    /// Sends a request and receives a response.
    async fn send_request<T: Message, R: Message + Default>(
        &self,
        message_type: Tag,
        request: &T,
    ) -> ClientResult<R> {
        let mut stream = self.connect().await?;
        
        // Serialize the request
        let request_data = request.encode_to_vec();
        
        // Send request data
        let req = Vortex::new(message_type, request_data.into()).expect("Failed to create Vortex packet");
        stream.write_all(&req.to_bytes()).await.map_err(|err| {
            error!("Failed to send request: {}", err);
            ClientError::MpscSend(err.to_string())
        })?;
        
        // Read response header
        let mut header_buf = [0u8; HEADER_SIZE];
        stream.read_exact(&mut header_buf).await.map_err(|err| {
            error!("Failed to read response header: {}", err);
            ClientError::UnexpectedResponse
        })?;
        
        let length = u32::from_be_bytes(header_buf[2..HEADER_SIZE].try_into().expect("Failed to read value length")) as usize;
        
        // let tag: tlv::Tag = header_buf[1]
        //     .try_into()
        //     .expect("invalid tag value");
        // let response_header = ProtocolHeader::from_bytes(&header_buf).map_err(|err| {
        //     error!("Invalid response header: {}", err);
        //     ClientError::from(ErrorType::ConnectError)
        // })?;
        
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
        // // Check for error response
        // if *deserialized.tag() == tlv::Tag::Error {
        //     let mut error_data = vec![0u8; length];
        //     stream.read_exact(&mut error_data).await.map_err(|err| {
        //         error!("Failed to read error response: {}", err);
        //         ClientError::UnexpectedResponse
        //     })?;
        //     let error = tlv::error::Error::try_from(Bytes::from(error_data)).expect("Failed to read error");
        //     // let error_message = String::from_utf8(error_data)
        //     // .unwrap_or_else(|_| format!("Invalid UTF-8 error data"));
    

        //     error!("Invalid error response: {}", error.message());
            
        //     return Err(ClientError::UnexpectedResponse);
        // }
        
        // Deserialize response
        // R::decode(&*deserialized).map_err(|err| {
        //     error!("Failed to decode response: {}", err);
        //     ClientError::UnexpectedResponse
        // })
    }

}