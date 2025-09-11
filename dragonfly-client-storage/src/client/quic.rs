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

use bytes::{Bytes, BytesMut};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use std::sync::Arc;
use std::net::SocketAddr;
use tokio::io::AsyncRead;
use tracing::{error, info};
use vortex_protocol::{
    Header, HEADER_SIZE,
    tlv::{
        download_persistent_cache_piece::DownloadPersistentCachePiece, download_piece::DownloadPiece,
        error::{Error, Code},
        persistent_cache_piece_content, piece_content,
        Tag,
    },
    Vortex,
};
use quinn::{Endpoint};

/// QUICClient is a QUIC-based client for dfdaemon upload service 。
#[derive(Clone)]
pub struct QUICClient {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// addr is the address of the QUIC server.
    addr: String,
}

impl QUICClient {
    /// Creates a new QUICClient.
    pub fn new(config: Arc<Config>, addr: String) -> Self {
        Self {
            config,
            addr,
        }
    }
    /// Sends a request and receives a response.
    pub async fn send(
        &self,
        number: u32,
        task_id: &str,
        tag: Tag,
    ) -> ClientResult<(impl AsyncRead, u64, String)> {

        // Create Endpoint
        let endpoint = Endpoint::client(self.addr.parse::<SocketAddr>().unwrap()).unwrap();

        // Connect to QUIC server
        let connection = endpoint.connect(
            self.addr.parse::<SocketAddr>().unwrap(),
            &self.config.host.hostname
        ).unwrap().await.map_err(|err| {
            error!("QUIC connect error: {}", err);
            ClientError::IO(std::io::Error::new(std::io::ErrorKind::Other, err))
        })?;

        // Establish unidirectional stream
        let mut send_stream = connection.open_uni().await.map_err(|err| {
            error!("QUIC open_uni error: {}", err);
            ClientError::IO(std::io::Error::new(std::io::ErrorKind::Other, err))
        })?;

        // Construct request
        let request: Bytes = if tag == Tag::DownloadPersistentCachePiece {
            Vortex::DownloadPersistentCachePiece(
            Header::new_download_persistent_cache_piece(),
            DownloadPersistentCachePiece::new(
                task_id.to_string(),
                number,
            ),
            )
            .into()
        } else {
            Vortex::DownloadPiece(
            Header::new_download_piece(),
            DownloadPiece::new(
                task_id.to_string(),
                number,
            ),
            )
            .into()
        };
        send_stream.write_all(&request).await.map_err(|e| {
            error!("QUIC send error: {}", e);
            ClientError::MpscSend(e.to_string())
        })?;
        send_stream.finish().map_err(|e| {
            error!("QUIC finish error: {}", e);
            ClientError::MpscSend(e.to_string())
        })?;
        // Receive unidirectional stream
        let mut recv_stream = connection.accept_uni().await.map_err(|e| {
            error!("QUIC accept_uni error: {}", e);
            ClientError::IO(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;

        // Read header
        let mut header_bytes = BytesMut::with_capacity(HEADER_SIZE);
        header_bytes.resize(HEADER_SIZE, 0);
        if let Err(err) = recv_stream.read_exact(&mut header_bytes).await {
            error!("Failed to read header: {}", err);
            return Err(ClientError::IO(std::io::Error::new(std::io::ErrorKind::Other, err)));
        }
        let header: Header = match header_bytes.freeze().try_into() {
            Ok(header) => header,
            Err(err) => {
                error!("Failed to parse header: {}", err);
                return Err(ClientError::IO(std::io::Error::new(std::io::ErrorKind::Other, err)));
            }
        };

        match header.tag() {
            Tag::PieceContent => {
                let mut metadata_length_bytes = BytesMut::with_capacity(piece_content::METADATA_LENGTH_SIZE);
                metadata_length_bytes.resize(piece_content::METADATA_LENGTH_SIZE, 0);
                if let Err(err) = recv_stream.read_exact(&mut metadata_length_bytes).await {
                    error!("Failed to read piece_content metadata length: {}", err);
                    return Err(ClientError::IO(std::io::Error::new(std::io::ErrorKind::Other, err)));
                }
                let metadata_length = match metadata_length_bytes[..].try_into() {
                    Ok(bytes) => u32::from_be_bytes(bytes) as usize,
                    Err(err) => {
                        error!("Failed to parse piece_content metadata length: {}", err);
                        return Err(ClientError::IO(std::io::Error::new(std::io::ErrorKind::Other, err)));
                    }
                };
                let mut metadata_bytes = BytesMut::with_capacity(metadata_length);
                metadata_bytes.resize(metadata_length, 0);
                if let Err(err) = recv_stream.read_exact(&mut metadata_bytes).await {
                    error!("Failed to read piece_content metadata: {}", err);
                    return Err(ClientError::IO(std::io::Error::new(std::io::ErrorKind::Other, err)));
                }
                let mut piece_content_bytes = BytesMut::with_capacity(piece_content::METADATA_LENGTH_SIZE + metadata_length);
                piece_content_bytes.extend_from_slice(&metadata_length_bytes);
                piece_content_bytes.extend_from_slice(&metadata_bytes);
                let piece_content: piece_content::PieceContent = match piece_content_bytes.freeze().try_into() {
                    Ok(content) => content,
                    Err(err) => {
                        error!("Failed to parse piece_content: {}", err);
                        return Err(ClientError::IO(std::io::Error::new(std::io::ErrorKind::Other, err)));
                    }
                };
                info!("received PieceContent: {:?}", piece_content);
                let metadata = piece_content.metadata();
                Ok((recv_stream, metadata.offset, metadata.digest))
            }
            Tag::PersistentCachePieceContent => {
                let mut metadata_length_bytes = BytesMut::with_capacity(persistent_cache_piece_content::METADATA_LENGTH_SIZE);
                metadata_length_bytes.resize(persistent_cache_piece_content::METADATA_LENGTH_SIZE, 0);
                if let Err(err) = recv_stream.read_exact(&mut metadata_length_bytes).await {
                    error!("Failed to read persistent_cache_piece_content metadata length: {}", err);
                    return Err(ClientError::IO(std::io::Error::new(std::io::ErrorKind::Other, err)));
                }
                let metadata_length = match metadata_length_bytes[..].try_into() {
                    Ok(bytes) => u32::from_be_bytes(bytes) as usize,
                    Err(err) => {
                        error!("Failed to parse persistent_cache_piece_content metadata length: {}", err);
                        return Err(ClientError::IO(std::io::Error::new(std::io::ErrorKind::Other, err)));
                    }
                };
                let mut metadata_bytes = BytesMut::with_capacity(metadata_length);
                metadata_bytes.resize(metadata_length, 0);
                if let Err(err) = recv_stream.read_exact(&mut metadata_bytes).await {
                    error!("Failed to read persistent_cache_piece_content metadata: {}", err);
                    return Err(ClientError::IO(std::io::Error::new(std::io::ErrorKind::Other, err)));
                }
                let mut persistent_cache_piece_content_bytes = BytesMut::with_capacity(persistent_cache_piece_content::METADATA_LENGTH_SIZE + metadata_length);
                persistent_cache_piece_content_bytes.extend_from_slice(&metadata_length_bytes);
                persistent_cache_piece_content_bytes.extend_from_slice(&metadata_bytes);
                let persistent_cache_piece_content: persistent_cache_piece_content::PersistentCachePieceContent = match persistent_cache_piece_content_bytes.freeze().try_into() {
                    Ok(content) => content,
                    Err(err) => {
                        error!("Failed to parse persistent_cache_piece_content: {}", err);
                        return Err(ClientError::IO(std::io::Error::new(std::io::ErrorKind::Other, err)));
                    }
                };
                info!("received PersistentCachePieceContent: {:?}", persistent_cache_piece_content);
                let metadata = persistent_cache_piece_content.metadata();
                Ok((recv_stream, metadata.offset, metadata.digest))
            }
            Tag::Error => {
                let mut error_bytes = BytesMut::with_capacity(header.length() as usize);
                error_bytes.resize(header.length() as usize, 0);
                recv_stream.read_exact(&mut error_bytes).await.unwrap();
                let error: Error = error_bytes.freeze().try_into().unwrap();
                error!("received Error: {}", error.message());
                match error.code() {
                    Code::Unknown => Err(ClientError::Unknown(error.message().to_string())),
                    Code::InvalidArgument => Err(ClientError::InvalidParameter),
                    Code::NotFound => Err(ClientError::PieceNotFound(error.message().to_string())),
                    Code::Internal => Err(ClientError::PieceStateIsFailed(error.message().to_string())),
                    Code::Reserved(_) => Err(ClientError::Unimplemented),
                }
            }
            _ => {
                error!("unexpected tag: {:?}", header.tag());
                Err(ClientError::UnexpectedResponse)
            }
        }
    }
}
