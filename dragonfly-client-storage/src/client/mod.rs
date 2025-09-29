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

pub mod quic;
pub mod tcp;

use bytes::{Bytes, BytesMut};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
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

/// DEFAULT_SEND_BUFFER_SIZE is the default size of the send buffer for network connections.
const DEFAULT_SEND_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// DEFAULT_RECV_BUFFER_SIZE is the default size of the receive buffer for network connections.
const DEFAULT_RECV_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// DEFAULT_KEEPALIVE_INTERVAL is the default interval for sending keepalive messages.
const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// DEFAULT_MAX_IDLE_TIMEOUT is the default maximum idle timeout for connections.
const DEFAULT_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Client defines a generic client interface for storage service protocols.
#[tonic::async_trait]
pub trait Client {
    /// Downloads a piece from the server using the vortex protocol.
    ///
    /// This is the main entry point for downloading a piece. It applies
    /// a timeout based on the configuration and handles connection timeouts gracefully.
    #[instrument(skip_all)]
    async fn download_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        time::timeout(
            self.config().download.piece_timeout,
            self.handle_download_piece(number, task_id),
        )
        .await
        .inspect_err(|err| {
            error!("connect timeout to {}: {}", self.addr(), err);
        })?
    }
    /// Internal handler for downloading a piece.
    ///
    /// This method performs the actual protocol communication:
    /// 1. Creates a download piece request.
    /// 2. Establishes connection and sends the request.
    /// 3. Reads and validates the response header.
    /// 4. Processes the piece content based on the response type.
    #[instrument(skip_all)]
    async fn handle_download_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
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
    async fn download_persistent_cache_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        time::timeout(
            self.config().download.piece_timeout,
            self.handle_download_persistent_cache_piece(number, task_id),
        )
        .await
        .inspect_err(|err| {
            error!("connect timeout to {}: {}", self.addr(), err);
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
    ) -> ClientResult<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
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

    /// Establishes connection and writes a vortex protocol request.
    ///
    /// This is a low-level utility function that handles the connection
    /// lifecycle and request transmission. It ensures proper error handling
    /// and connection cleanup.
    async fn connect_and_write_request(
        &self,
        request: Bytes,
    ) -> ClientResult<(
        Box<dyn AsyncRead + Send + Unpin>,
        Box<dyn AsyncWrite + Send + Unpin>,
    )>;

    /// Reads and parses a vortex protocol header.
    ///
    /// The header contains metadata about the following message, including
    /// the message type (tag) and payload length. This is critical for
    /// proper protocol message framing.
    #[instrument(skip_all)]
    async fn read_header(
        &self,
        reader: &mut Box<dyn AsyncRead + Send + Unpin>,
    ) -> ClientResult<Header> {
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
        reader: &mut Box<dyn AsyncRead + Send + Unpin>,
        metadata_length_size: usize,
    ) -> ClientResult<T>
    where
        T: TryFrom<Bytes>,
        T::Error: Into<ClientError>,
        T: 'static,
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
    async fn read_error(
        &self,
        reader: &mut Box<dyn AsyncRead + Send + Unpin>,
        header_length: usize,
    ) -> ClientError {
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

    /// Access to client configuration.
    fn config(&self) -> &Arc<Config>;

    /// Access to client address.
    fn addr(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::{BufMut, Bytes, BytesMut};
    use dragonfly_client_config::dfdaemon::Download;
    use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
    use std::io::Cursor;
    use tokio::io::duplex;
    use tokio::time::sleep;

    struct Mock {
        config: Arc<Config>,

        addr: String,

        timeout: Duration,
    }

    #[tonic::async_trait]
    impl Client for Mock {
        async fn handle_download_piece(
            &self,
            _number: u32,
            _task_id: &str,
        ) -> ClientResult<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
            sleep(self.timeout).await;

            Ok((Box::new(Cursor::new(b"data")), 0, "hash".to_string()))
        }

        async fn handle_download_persistent_cache_piece(
            &self,
            _number: u32,
            _task_id: &str,
        ) -> ClientResult<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
            sleep(self.timeout).await;

            Ok((Box::new(Cursor::new(b"data")), 0, "hash".to_string()))
        }

        async fn connect_and_write_request(
            &self,
            _request: Bytes,
        ) -> ClientResult<(
            Box<dyn AsyncRead + Send + Unpin>,
            Box<dyn AsyncWrite + Send + Unpin>,
        )> {
            let (reader, writer) = duplex(1);
            Ok((Box::new(reader), Box::new(writer)))
        }

        fn config(&self) -> &Arc<Config> {
            &self.config
        }

        fn addr(&self) -> &str {
            &self.addr
        }
    }

    #[tokio::test]
    async fn test_download_piece() {
        let addr = "127.0.0.1:8080".to_string();
        let config = Arc::new(Config {
            download: Download {
                piece_timeout: Duration::from_secs(2),
                ..Default::default()
            },
            ..Default::default()
        });

        let mut mock = Mock {
            config,
            addr,
            timeout: Duration::from_secs(1),
        };
        let result = mock.download_piece(1, "task").await;
        assert!(result.is_ok());
        if let Ok((mut reader, offset, digest)) = result {
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, b"data");
            assert_eq!(offset, 0);
            assert_eq!(digest, "hash");
        }

        mock.timeout = Duration::from_secs(3);
        let result = mock.download_piece(1, "task").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_download_persistent_cache_piece() {
        let addr = "127.0.0.1:8080".to_string();
        let config = Arc::new(Config {
            download: Download {
                piece_timeout: Duration::from_secs(2),
                ..Default::default()
            },
            ..Default::default()
        });

        let mut mock = Mock {
            config,
            addr,
            timeout: Duration::from_secs(1),
        };
        let result = mock.download_persistent_cache_piece(1, "task").await;
        assert!(result.is_ok());
        if let Ok((mut reader, offset, digest)) = result {
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, b"data");
            assert_eq!(offset, 0);
            assert_eq!(digest, "hash");
        }

        mock.timeout = Duration::from_secs(3);
        let result = mock.download_persistent_cache_piece(1, "task").await;
        assert!(result.is_err());
    }

    mockall::mock! {
        Client {}

        #[tonic::async_trait]
        impl Client for Client {
            async fn connect_and_write_request(
                &self,
                request: Bytes,
            ) -> ClientResult<(Box<dyn AsyncRead + Send + Unpin>, Box<dyn AsyncWrite + Send + Unpin>)>;

            async fn read_header(&self, reader: &mut Box<dyn AsyncRead + Send + Unpin>) -> ClientResult<Header>;

            async fn read_piece_content<T>(
                &self,
                reader: &mut Box<dyn AsyncRead + Send + Unpin>,
                metadata_length_size: usize,
            ) -> ClientResult<T>
            where
                T: TryFrom<Bytes>,
                T::Error: Into<ClientError>,
                T: 'static;

            async fn read_error(&self, reader: &mut Box<dyn AsyncRead + Send + Unpin>, header_length: usize) -> ClientError;

            fn config(&self) -> &Arc<Config>;

            fn addr(&self) -> &str;
        }
    }

    #[tokio::test]
    async fn test_handle_download_piece() {
        let mut mock = MockClient::new();
        mock.expect_connect_and_write_request().returning(|_| {
            let (reader, writer) = duplex(1);
            Ok((Box::new(reader), Box::new(writer)))
        });

        mock.expect_read_header()
            .times(1)
            .returning(|_| Ok(Header::new_piece_content(1024)));
        mock.expect_read_piece_content().returning(|_, _| {
            Ok(piece_content::PieceContent::new(
                42,
                1024,
                2048,
                "a".repeat(32),
                "test_parent_id".to_string(),
                1,
                Duration::from_secs(5),
                chrono::DateTime::from_timestamp(1693152000, 0)
                    .unwrap()
                    .naive_utc(),
            ))
        });
        let result = mock.handle_download_piece(1, "task").await;
        assert!(result.is_ok());
        if let Ok((_, offset, digest)) = result {
            assert_eq!(offset, 1024);
            assert_eq!(digest, "a".repeat(32));
        }

        mock.expect_read_header()
            .times(1)
            .returning(|_| Ok(Header::new_error(1024)));
        mock.expect_read_error()
            .returning(|_, _| ClientError::Unknown("test".to_string()));
        let result = mock.handle_download_piece(1, "task").await;
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(format!("{:?}", err).contains("test"));
        }

        mock.expect_read_header()
            .returning(|_| Ok(Header::new_close()));
        let result = mock.handle_download_piece(1, "task").await;
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(format!("{:?}", err).contains("unexpected tag"));
        }
    }

    #[tokio::test]
    async fn test_handle_download_persistent_cache_piece() {
        let mut mock = MockClient::new();
        mock.expect_connect_and_write_request().returning(|_| {
            let (reader, writer) = duplex(1);
            Ok((Box::new(reader), Box::new(writer)))
        });

        mock.expect_read_header()
            .times(1)
            .returning(|_| Ok(Header::new_persistent_cache_piece_content(1024)));
        mock.expect_read_piece_content().returning(|_, _| {
            Ok(
                persistent_cache_piece_content::PersistentCachePieceContent::new(
                    42,
                    1024,
                    2048,
                    "a".repeat(32),
                    "test_parent_id".to_string(),
                    1,
                    Duration::from_secs(5),
                    chrono::DateTime::from_timestamp(1693152000, 0)
                        .unwrap()
                        .naive_utc(),
                ),
            )
        });
        let result = mock.handle_download_persistent_cache_piece(1, "task").await;
        assert!(result.is_ok());
        if let Ok((_, offset, digest)) = result {
            assert_eq!(offset, 1024);
            assert_eq!(digest, "a".repeat(32));
        }

        mock.expect_read_header()
            .times(1)
            .returning(|_| Ok(Header::new_error(1024)));
        mock.expect_read_error()
            .returning(|_, _| ClientError::Unknown("test".to_string()));
        let result = mock.handle_download_persistent_cache_piece(1, "task").await;
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(format!("{:?}", err).contains("test"));
        }

        mock.expect_read_header()
            .returning(|_| Ok(Header::new_close()));
        let result = mock.handle_download_persistent_cache_piece(1, "task").await;
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(format!("{:?}", err).contains("unexpected tag"));
        }
    }

    #[tokio::test]
    async fn test_read_header() {
        let addr = "127.0.0.1:8080".to_string();
        let config = Arc::new(Config::default());
        let mock = Mock {
            config,
            addr,
            timeout: Duration::from_secs(1),
        };

        let mut reader: Box<dyn AsyncRead + Send + Unpin> = Box::new(Cursor::new(b"HEADER_SIZE"));
        let result = mock.read_header(&mut reader).await;
        assert!(result.is_ok());

        let mut reader: Box<dyn AsyncRead + Send + Unpin> = Box::new(Cursor::new(b""));
        let result = mock.read_header(&mut reader).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_piece_content() {
        let addr = "127.0.0.1:8080".to_string();
        let config = Arc::new(Config::default());
        let mock = Mock {
            config,
            addr,
            timeout: Duration::from_secs(1),
        };

        let piece_content = piece_content::PieceContent::new(
            42,
            1024,
            2048,
            "a".repeat(32),
            "test_parent_id".to_string(),
            1,
            Duration::from_secs(5),
            chrono::DateTime::from_timestamp(1693152000, 0)
                .unwrap()
                .naive_utc(),
        );
        let piece_content_bytes: Bytes = piece_content.into();
        let mut reader: Box<dyn AsyncRead + Send + Unpin> =
            Box::new(Cursor::new(piece_content_bytes));
        let result: ClientResult<piece_content::PieceContent> = mock
            .read_piece_content(&mut reader, piece_content::METADATA_LENGTH_SIZE)
            .await;
        assert!(result.is_ok());
        if let Ok(content) = result {
            assert_eq!(content.metadata().number, 42);
            assert_eq!(content.metadata().offset, 1024);
            assert_eq!(content.metadata().length, 2048);
            assert_eq!(content.metadata().digest, "a".repeat(32));
            assert_eq!(content.metadata().parent_id, "test_parent_id".to_string());
            assert_eq!(content.metadata().traffic_type, 1);
            assert_eq!(content.metadata().cost, Duration::from_secs(5));
        }

        let mut reader: Box<dyn AsyncRead + Send + Unpin> = Box::new(Cursor::new(b""));
        let result: ClientResult<piece_content::PieceContent> = mock
            .read_piece_content(&mut reader, piece_content::METADATA_LENGTH_SIZE)
            .await;
        assert!(result.is_err());

        let mut reader: Box<dyn AsyncRead + Send + Unpin> = Box::new(Cursor::new(b"METADATA"));
        let result: ClientResult<piece_content::PieceContent> = mock
            .read_piece_content(&mut reader, piece_content::METADATA_LENGTH_SIZE)
            .await;
        assert!(result.is_err());

        let data = {
            let mut bytes = BytesMut::new();
            bytes.put_u32(100);
            bytes.put(&vec![0u8; 50][..]);
            bytes.freeze()
        };
        let mut reader: Box<dyn AsyncRead + Send + Unpin> = Box::new(Cursor::new(data));
        let result: ClientResult<piece_content::PieceContent> = mock
            .read_piece_content(&mut reader, piece_content::METADATA_LENGTH_SIZE)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_error() {
        let addr = "127.0.0.1:8080".to_string();
        let config = Arc::new(Config::default());
        let mock = Mock {
            config,
            addr,
            timeout: Duration::from_secs(1),
        };

        let expected_code = vortex_protocol::tlv::error::Code::NotFound;
        let expected_message = "Resource not found".to_string();
        let error = VortexError::new(expected_code, expected_message.clone());
        let bytes: Bytes = error.into();
        let bytes_length = bytes.len();
        let mut reader: Box<dyn AsyncRead + Send + Unpin> = Box::new(Cursor::new(bytes));
        let result = mock.read_error(&mut reader, bytes_length).await;
        assert!(matches!(result,
            ClientError::VortexProtocolStatus(code, ref message)
            if code == expected_code && message == &expected_message
        ));

        let mut reader: Box<dyn AsyncRead + Send + Unpin> = Box::new(Cursor::new(b""));
        let result = mock.read_error(&mut reader, 5).await;
        assert!(matches!(result, ClientError::IO(_)));

        let mut reader: Box<dyn AsyncRead + Send + Unpin> = Box::new(Cursor::new(b""));
        let result = mock.read_error(&mut reader, 0).await;
        assert!(matches!(result, ClientError::Unknown(_)));
    }
}
