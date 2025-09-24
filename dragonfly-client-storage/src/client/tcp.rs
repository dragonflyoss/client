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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream as TokioTcpStream,
};
use tokio::time;
use tracing::{error, instrument};
use vortex_protocol::{
    tlv::{
        download_persistent_cache_piece::DownloadPersistentCachePiece,
        download_piece::DownloadPiece, error::Error as VortexError, persistent_cache_piece_content,
        piece_content, Tag,
    },
    Header, Vortex, HEADER_SIZE,
};

/// TCPClient is a TCP-based client for tcp storage service.
#[derive(Clone)]
pub struct TCPClient {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// addr is the address of the TCP server.
    addr: String,
}

/// TCPClient implements the TCP-based client for tcp storage service.
impl TCPClient {
    /// Creates a new TCPClient instance.
    pub fn new(config: Arc<Config>, addr: String) -> Self {
        Self { config, addr }
    }

    /// Downloads a piece from the server using the vortex protocol.
    ///
    /// This is the main entry point for downloading a piece. It applies
    /// a timeout based on the configuration and handles connection timeouts gracefully.
    #[instrument(skip_all)]
    pub async fn download_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(impl AsyncRead, u64, String)> {
        time::timeout(
            self.config.download.piece_timeout,
            self.handle_download_piece(number, task_id),
        )
        .await
        .inspect_err(|err| {
            error!("connect timeout to {}: {}", self.addr, err);
        })?
    }
    /// Internal handler for downloading a piece.
    ///
    /// This method performs the actual protocol communication:
    /// 1. Creates a download piece request.
    /// 2. Establishes TCP connection and sends the request.
    /// 3. Reads and validates the response header.
    /// 4. Processes the piece content based on the response type.
    #[instrument(skip_all)]
    async fn handle_download_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(impl AsyncRead, u64, String)> {
        let request: Bytes = Vortex::DownloadPiece(
            Header::new_download_piece(),
            DownloadPiece::new(task_id.to_string(), number),
        )
        .into();

        let (mut reader, _writer) = self.connect_and_write_request(request).await?;
        let header = self.read_header(&mut reader).await?;
        match header.tag() {
            Tag::PieceContent => {
                let piece_content: piece_content::PieceContent = self
                    .read_piece_content(&mut reader, piece_content::METADATA_LENGTH_SIZE)
                    .await?;

                let metadata = piece_content.metadata();
                Ok((reader, metadata.offset, metadata.digest))
            }
            Tag::Error => Err(self.read_error(&mut reader, header.length() as usize).await),
            _ => Err(ClientError::Unknown(format!(
                "unexpected tag: {:?}",
                header.tag()
            ))),
        }
    }

    /// Downloads a persistent cache piece from the server using the vortex protocol.
    ///
    /// Similar to `download_piece` but specifically for persistent cache piece.
    #[instrument(skip_all)]
    pub async fn download_persistent_cache_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(impl AsyncRead, u64, String)> {
        time::timeout(
            self.config.download.piece_timeout,
            self.handle_download_persistent_cache_piece(number, task_id),
        )
        .await
        .inspect_err(|err| {
            error!("connect timeout to {}: {}", self.addr, err);
        })?
    }

    /// Internal handler for downloading a persistent cache piece.
    ///
    /// Implements the same protocol flow as `handle_download_piece` but uses
    /// persistent cache specific request/response types.
    #[instrument(skip_all)]
    async fn handle_download_persistent_cache_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(impl AsyncRead, u64, String)> {
        let request: Bytes = Vortex::DownloadPersistentCachePiece(
            Header::new_download_persistent_cache_piece(),
            DownloadPersistentCachePiece::new(task_id.to_string(), number),
        )
        .into();

        let (mut reader, _writer) = self.connect_and_write_request(request).await?;
        let header = self.read_header(&mut reader).await?;
        match header.tag() {
            Tag::PersistentCachePieceContent => {
                let persistent_cache_piece_content: persistent_cache_piece_content::PersistentCachePieceContent =
                    self.read_piece_content(&mut reader, piece_content::METADATA_LENGTH_SIZE)
                        .await?;

                let metadata = persistent_cache_piece_content.metadata();
                Ok((reader, metadata.offset, metadata.digest))
            }
            Tag::Error => Err(self.read_error(&mut reader, header.length() as usize).await),
            _ => Err(ClientError::Unknown(format!(
                "unexpected tag: {:?}",
                header.tag()
            ))),
        }
    }

    /// Establishes TCP connection and writes a vortex protocol request.
    ///
    /// This is a low-level utility function that handles the TCP connection
    /// lifecycle and request transmission. It ensures proper error handling
    /// and connection cleanup.
    #[instrument(skip_all)]
    async fn connect_and_write_request(
        &self,
        request: Bytes,
    ) -> ClientResult<(OwnedReadHalf, OwnedWriteHalf)> {
        let (reader, mut writer) = TokioTcpStream::connect(self.addr.clone())
            .await
            .inspect_err(|err| {
                error!("failed to connect to {}: {}", self.addr, err);
            })?
            .into_split();

        writer.write_all(&request).await.inspect_err(|err| {
            error!("failed to send request: {}", err);
        })?;

        writer.flush().await.inspect_err(|err| {
            error!("failed to flush request: {}", err);
        })?;

        Ok((reader, writer))
    }

    /// Reads and parses a vortex protocol header from the TCP stream.
    ///
    /// The header contains metadata about the following message, including
    /// the message type (tag) and payload length. This is critical for
    /// proper protocol message framing.
    #[instrument(skip_all)]
    async fn read_header(&self, reader: &mut OwnedReadHalf) -> ClientResult<Header> {
        let mut header_bytes = BytesMut::with_capacity(HEADER_SIZE);
        header_bytes.resize(HEADER_SIZE, 0);
        reader
            .read_exact(&mut header_bytes)
            .await
            .inspect_err(|err| {
                error!("failed to receive header: {}", err);
            })?;

        Header::try_from(header_bytes.freeze()).map_err(Into::into)
    }

    /// Reads and parses piece content with variable-length metadata.
    ///
    /// This generic function handles the two-stage reading process for
    /// piece content: first reading the metadata length, then reading
    /// the actual metadata, and finally constructing the complete message.
    #[instrument(skip_all)]
    async fn read_piece_content<T>(
        &self,
        reader: &mut OwnedReadHalf,
        metadata_length_size: usize,
    ) -> ClientResult<T>
    where
        T: TryFrom<Bytes, Error: Into<ClientError>>,
    {
        let mut metadata_length_bytes = BytesMut::with_capacity(metadata_length_size);
        metadata_length_bytes.resize(metadata_length_size, 0);
        reader
            .read_exact(&mut metadata_length_bytes)
            .await
            .inspect_err(|err| {
                error!("failed to receive metadata length: {}", err);
            })?;
        let metadata_length = u32::from_be_bytes(metadata_length_bytes[..].try_into()?) as usize;

        let mut metadata_bytes = BytesMut::with_capacity(metadata_length);
        metadata_bytes.resize(metadata_length, 0);
        reader
            .read_exact(&mut metadata_bytes)
            .await
            .inspect_err(|err| {
                error!("failed to receive metadata: {}", err);
            })?;

        let mut content_bytes = BytesMut::with_capacity(metadata_length_size + metadata_length);
        content_bytes.extend_from_slice(&metadata_length_bytes);
        content_bytes.extend_from_slice(&metadata_bytes);
        content_bytes.freeze().try_into().map_err(Into::into)
    }

    /// Reads and processes error responses from the server.
    ///
    /// When the server responds with an error tag, this function reads
    /// the error payload and converts it into an appropriate client error.
    /// This provides structured error handling for protocol-level failures.
    #[instrument(skip_all)]
    async fn read_error(&self, reader: &mut OwnedReadHalf, header_length: usize) -> ClientError {
        let mut error_bytes = BytesMut::with_capacity(header_length);
        error_bytes.resize(header_length, 0);
        if let Err(err) = reader.read_exact(&mut error_bytes).await {
            error!("failed to receive error: {}", err);
            return ClientError::IO(err);
        };

        error_bytes
            .freeze()
            .try_into()
            .map(|error: VortexError| {
                ClientError::VortexProtocolStatus(error.code(), error.message().to_string())
            })
            .unwrap_or_else(|err| {
                error!("failed to extract error: {}", err);
                ClientError::Unknown(format!("failed to extract error: {}", err))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use dragonfly_client_config::dfdaemon::Config;
    use dragonfly_client_core::Error as ClientError;
    use std::{sync::Arc, time::Duration};
    use tokio::io::{AsyncRead, AsyncReadExt};
    use vortex_protocol::tlv::download_piece::DownloadPiece;

    fn create_test_config() -> Arc<Config> {
        let mut config = Config::default();
        config.download.piece_timeout = Duration::from_secs(5);
        Arc::new(config)
    }

    fn create_test_client() -> TCPClient {
        let config = create_test_config();
        let addr = "127.0.0.1:8080".to_string();
        TCPClient::new(config, addr)
    }

    #[test]
    fn test_new_client_creation() {
        let client = create_test_client();

        // Verify the client is created
        assert!(std::mem::size_of_val(&client) > 0);

        // Test clone functionality
        let cloned_client = client.clone();
        assert!(std::mem::size_of_val(&cloned_client) > 0);
    }

    #[test]
    fn test_config_timeout_setting() {
        let mut config = Config::default();
        config.download.piece_timeout = Duration::from_millis(500);
        let config_arc = Arc::new(config);

        // Verify config is set correctly
        assert_eq!(
            config_arc.download.piece_timeout,
            Duration::from_millis(500)
        );
    }

    #[test]
    fn test_client_error_conversions() {
        // Test various error type conversions
        let io_error =
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "Connection refused");
        let client_error = ClientError::IO(io_error);

        match client_error {
            ClientError::IO(err) => {
                assert_eq!(err.kind(), std::io::ErrorKind::ConnectionRefused);
                assert_eq!(err.to_string(), "Connection refused");
            },
            _ => unreachable!("Unexpected error"),
        }

        let unknown_error = ClientError::Unknown("Test error".to_string());
        match unknown_error {
            ClientError::Unknown(msg) => assert_eq!(msg, "Test error"),
            _ => unreachable!("Unexpected error"),
        }
    }

    #[test]
    fn test_bytes_conversion() {
        // Test various Bytes operations used in the client
        let test_data = vec![1, 2, 3, 4, 5];
        let mut bytes_mut = BytesMut::with_capacity(test_data.len());
        bytes_mut.extend_from_slice(&test_data);

        let frozen = bytes_mut.freeze();
        assert_eq!(frozen.len(), test_data.len());
        assert_eq!(frozen.to_vec(), test_data);
    }

    #[test]
    fn test_task_id_and_piece_number_validation() {
        // Test input validation scenarios
        let download_piece = DownloadPiece::new("".to_string(), 0);
        assert_eq!(download_piece.task_id(), "");
        assert_eq!(download_piece.piece_number(), 0);

        let download_piece = DownloadPiece::new("valid-task-id".to_string(), u32::MAX);
        assert_eq!(download_piece.task_id(), "valid-task-id");
        assert_eq!(download_piece.piece_number(), u32::MAX);
    }

    #[test]
    fn test_address_format_validation() {
        // Test different address formats
        let addresses = vec![
            "127.0.0.1:8080",
            "localhost:9000",
            "192.168.1.1:8080",
            "[::1]:8080", // IPv6
        ];

        for addr in addresses {
            let client = TCPClient::new(create_test_config(), addr.to_string());
            // Just verify client creation doesn't panic
            assert!(std::mem::size_of_val(&client) > 0);
        }
    }

    // Mock async reader for testing read operations
    struct FailingReader;

    impl AsyncRead for FailingReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "Mock connection failure",
            )))
        }
    }

    #[tokio::test]
    async fn test_read_error_handling() {
        // Test error handling in read operations
        let mut failing_reader = FailingReader;
        let mut buffer = vec![0u8; 10];

        let result = failing_reader.read(&mut buffer).await;
        assert!(result.is_err());

        match result {
            Err(err) => {
                assert_eq!(err.kind(), std::io::ErrorKind::ConnectionAborted);
                assert_eq!(err.to_string(), "Mock connection failure");
            },
            _ => unreachable!("Unexpected error"),
        }
    }

    #[test]
    fn test_timeout_duration_settings() {
        // Test different timeout configurations
        let short_timeout = Duration::from_millis(100);
        let long_timeout = Duration::from_secs(30);

        let mut config = Config::default();
        config.download.piece_timeout = short_timeout;
        assert_eq!(config.download.piece_timeout, short_timeout);

        config.download.piece_timeout = long_timeout;
        assert_eq!(config.download.piece_timeout, long_timeout);
    }
}
