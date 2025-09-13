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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream as TokioTcpStream;
use tokio::time;
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

/// TCPClient is a TCP-based client for dfdaemon upload service.
#[derive(Clone)]
pub struct TCPClient {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// addr is the address of the TCP server.
    addr: String,
}

impl TCPClient {
    /// Creates a new TCPClient.
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
        is_persistent: bool,
    ) -> ClientResult<(impl AsyncRead, u64, String)> {
        // ---------------------------
        // Establishes a TCP connection to the server.
        // ---------------------------
        let (s_reader, s_writer) = time::timeout(
            self.config.download.piece_timeout,
            TokioTcpStream::connect(self.addr.clone())
        ).await
        .map_err(|_| {
            error!("Connection timeout to {}", self.addr);
            ClientError::SendTimeout
        })?
        .map_err(|err| {
            error!("Failed to connect to {}: {}", self.addr, err);
            ClientError::IO(err)
        })?
        .into_split();
        let mut reader = BufReader::with_capacity(self.config.storage.read_buffer_size, s_reader);
        let mut writer = BufWriter::with_capacity(self.config.storage.write_buffer_size, s_writer);

        // ---------------------------
        // Send DownloadPiece response.
        // ---------------------------
        let request: Bytes = if is_persistent {
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
        writer.write_all(&request).await.map_err(|err| {
            error!("Failed to send request: {}", err);
            ClientError::MpscSend(err.to_string())
        })?;
        writer.flush().await.map_err(|err| {
            error!("Failed to send request: {}", err);
            ClientError::MpscSend(err.to_string())
        })?;

        // ---------------------------
        // Receive response header.
        // ---------------------------
        let mut header_bytes = BytesMut::with_capacity(HEADER_SIZE);
        header_bytes.resize(HEADER_SIZE, 0);
        reader.read_exact(&mut header_bytes).await.unwrap();
        let header: Header = header_bytes.freeze().try_into().unwrap();

        match header.tag() {
            Tag::PieceContent => {
                if is_persistent {
                    return Err(ClientError::UnexpectedResponse);
                }

                // ------------------------------
                // Receive PieceContent response.
                // ------------------------------
                let mut metadata_length_bytes = BytesMut::with_capacity(piece_content::METADATA_LENGTH_SIZE);
                metadata_length_bytes.resize(piece_content::METADATA_LENGTH_SIZE, 0);
                reader.read_exact(&mut metadata_length_bytes).await.unwrap();

                let metadata_length =
                    u32::from_be_bytes(metadata_length_bytes[..].try_into().unwrap()) as usize;

                let mut metadata_bytes = BytesMut::with_capacity(metadata_length);
                metadata_bytes.resize(metadata_length, 0);
                reader.read_exact(&mut metadata_bytes).await.unwrap();

                let mut piece_content_bytes =
                    BytesMut::with_capacity(piece_content::METADATA_LENGTH_SIZE + metadata_length);

                piece_content_bytes.extend_from_slice(&metadata_length_bytes);
                piece_content_bytes.extend_from_slice(&metadata_bytes);
                let piece_content: piece_content::PieceContent = piece_content_bytes.freeze().try_into().unwrap();
                info!("received PieceContent: {:?}", piece_content);

                let metadata = piece_content.metadata();
                Ok((reader, metadata.offset, metadata.digest))
            }
            Tag::PersistentCachePieceContent => {
                if !is_persistent {
                    return Err(ClientError::UnexpectedResponse);
                }

                // ------------------------------
                // Receive PersistentCachePieceContent response.
                // ------------------------------
                let mut metadata_length_bytes = BytesMut::with_capacity(persistent_cache_piece_content::METADATA_LENGTH_SIZE);
                metadata_length_bytes.resize(persistent_cache_piece_content::METADATA_LENGTH_SIZE, 0);
                reader.read_exact(&mut metadata_length_bytes).await.unwrap();

                let metadata_length =
                    u32::from_be_bytes(metadata_length_bytes[..].try_into().unwrap()) as usize;

                let mut metadata_bytes = BytesMut::with_capacity(metadata_length);
                metadata_bytes.resize(metadata_length, 0);
                reader.read_exact(&mut metadata_bytes).await.unwrap();

                let mut persistent_cache_piece_content_bytes =
                    BytesMut::with_capacity(persistent_cache_piece_content::METADATA_LENGTH_SIZE + metadata_length);

                persistent_cache_piece_content_bytes.extend_from_slice(&metadata_length_bytes);
                persistent_cache_piece_content_bytes.extend_from_slice(&metadata_bytes);
                let persistent_cache_piece_content: persistent_cache_piece_content::PersistentCachePieceContent = persistent_cache_piece_content_bytes.freeze().try_into().unwrap();
                info!("received PersistentCachePieceContent: {:?}", persistent_cache_piece_content);

                let metadata = persistent_cache_piece_content.metadata();
                Ok((reader, metadata.offset, metadata.digest))
            }
            Tag::Error => {
                // ------------------------------
                // Receive Error response.
                // ------------------------------
                let mut error_bytes = BytesMut::with_capacity(header.length() as usize);
                error_bytes.resize(header.length() as usize, 0);
                reader.read_exact(&mut error_bytes).await.unwrap();
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
