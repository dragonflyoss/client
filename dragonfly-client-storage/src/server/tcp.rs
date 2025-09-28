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

use crate::Storage;
use bytes::{Bytes, BytesMut};
use dragonfly_api::common::v2::TrafficType;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use dragonfly_client_metric::{
    collect_upload_piece_failure_metrics, collect_upload_piece_started_metrics,
};
use dragonfly_client_util::{id_generator::IDGenerator, shutdown};
use leaky_bucket::RateLimiter;
use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{copy, AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener, TcpStream,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, Span};
use vortex_protocol::{
    tlv::{
        download_persistent_cache_piece::DownloadPersistentCachePiece,
        download_piece::DownloadPiece,
        error::{Code, Error},
        persistent_cache_piece_content::PersistentCachePieceContent,
        piece_content::PieceContent,
        Tag,
    },
    Header, Vortex, HEADER_SIZE,
};

/// TCPServer is a TCP-based server for dfdaemon upload service.
pub struct TCPServer {
    /// addr is the address of the TCP server.
    addr: SocketAddr,

    /// handler is the request handler.
    handler: TCPServerHandler,

    /// shutdown is used to shutdown the TCP server.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the TCP server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// TCPServer implements the TCP server.
impl TCPServer {
    /// Creates a new TCPServer.
    pub fn new(
        addr: SocketAddr,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        upload_rate_limiter: Arc<RateLimiter>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            addr,
            handler: TCPServerHandler {
                id_generator,
                storage,
                upload_rate_limiter,
            },
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// Starts the storage tcp server.
    pub async fn run(&mut self) -> ClientResult<()> {
        let socket = Socket::new(
            Domain::for_address(self.addr),
            Type::STREAM,
            Some(Protocol::TCP),
        )?;
        socket.set_tcp_nodelay(true)?;
        socket.set_nonblocking(true)?;
        socket.set_send_buffer_size(super::DEFAULT_SEND_BUFFER_SIZE)?;
        socket.set_recv_buffer_size(super::DEFAULT_RECV_BUFFER_SIZE)?;
        socket.set_tcp_keepalive(
            &TcpKeepalive::new().with_interval(super::DEFAULT_KEEPALIVE_INTERVAL),
        )?;
        #[cfg(target_os = "linux")]
        {
            use nix::sys::socket::{setsockopt, sockopt::TcpFastOpenConnect};
            use std::os::fd::AsFd;
            use tracing::{info, warn};

            if let Err(err) = socket.set_tcp_congestion("cubic".as_bytes()) {
                warn!("failed to set tcp congestion: {}", err);
            } else {
                info!("set tcp congestion to cubic");
            }

            if let Err(err) = setsockopt(&socket.as_fd(), TcpFastOpenConnect, &true) {
                warn!("failed to set tcp fast open: {}", err);
            } else {
                info!("set tcp fast open to true");
            }
        }

        socket.bind(&self.addr.into())?;
        socket.listen(1024)?;
        let std_listener: std::net::TcpListener = socket.into();
        let listener = TcpListener::from_std(std_listener).inspect_err(|err| {
            error!("failed to bind tcp server: {}", err);
        })?;
        info!("storage tcp server listening on {}", self.addr);

        loop {
            tokio::select! {
                tcp_accepted = listener.accept() => {
                    let (tcp, remote_address) = tcp_accepted?;
                    debug!("accepted connection from {}", remote_address);

                    let handler = self.handler.clone();
                    tokio::spawn(async move {
                        if let Err(err) = handler.handle(tcp, remote_address.to_string()).await {
                           error!("failed to serve connection from {}: {}", remote_address, err);
                        }
                    });
                },
                _ = self.shutdown.recv() => {
                    info!("tcp server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

/// TCPServerHandler handles TCP connections and requests.
#[derive(Clone)]
pub struct TCPServerHandler {
    /// id_generator is the id generator.
    id_generator: Arc<IDGenerator>,

    /// storage is the local storage.
    storage: Arc<Storage>,

    /// upload_rate_limiter is the rate limiter of the upload speed in bps(bytes per second).
    upload_rate_limiter: Arc<RateLimiter>,
}

/// TCPServerHandler implements the request handler.
impl TCPServerHandler {
    /// Handles a single TCP connection for the Dragonfly P2P protocol.
    ///
    /// This is the main entry point for processing incoming TCP connections.
    /// It reads the protocol header to determine the request type and dispatches
    /// to the appropriate handler. Supports both regular piece downloads and
    /// persistent cache piece downloads with proper request/response framing.
    #[instrument(skip_all, fields(host_id, remote_address, task_id, piece_id))]
    async fn handle(&self, stream: TcpStream, remote_address: String) -> ClientResult<()> {
        let (mut reader, mut writer) = stream.into_split();
        let header = self.read_header(&mut reader).await?;
        match header.tag() {
            Tag::DownloadPiece => {
                let download_piece: DownloadPiece = self
                    .read_download_piece(&mut reader, header.length() as usize)
                    .await?;

                // Generate the host id.
                let host_id = self.id_generator.host_id();

                // Get the task id from the request.
                let task_id = download_piece.task_id();

                // Get the interested piece number from the request.
                let piece_number = download_piece.piece_number();

                // Generate the piece id.
                let piece_id = self.storage.piece_id(task_id, piece_number);

                Span::current().record("host_id", host_id);
                Span::current().record("remote_address", remote_address.as_str());
                Span::current().record("task_id", task_id);
                Span::current().record("piece_id", piece_id.as_str());

                // Collect upload piece started metrics.
                collect_upload_piece_started_metrics();
                info!("start upload piece content");

                match self.handle_piece(piece_id.as_str(), task_id).await {
                    Ok((piece_content, mut content_reader)) => {
                        let piece_content_bytes: Bytes = piece_content.into();

                        let header = Header::new_piece_content(piece_content_bytes.len() as u32);
                        let header_bytes: Bytes = header.into();

                        let mut response =
                            BytesMut::with_capacity(HEADER_SIZE + piece_content_bytes.len());
                        response.extend_from_slice(&header_bytes);
                        response.extend_from_slice(&piece_content_bytes);

                        self.write_response(response.freeze(), &mut writer).await?;
                        self.write_stream(&mut content_reader, &mut writer).await?;
                    }
                    Err(err) => {
                        // Collect upload piece failure metrics.
                        collect_upload_piece_failure_metrics();

                        let error_response: Bytes =
                            Vortex::Error(Header::new_error(err.len() as u32), err).into();
                        self.write_response(error_response, &mut writer).await?;
                    }
                }

                Ok(())
            }
            Tag::DownloadPersistentCachePiece => {
                let download_persistent_cache_piece: DownloadPersistentCachePiece = self
                    .read_download_piece(&mut reader, header.length() as usize)
                    .await?;

                // Generate the host id.
                let host_id = self.id_generator.host_id();

                // Get the task id from the request.
                let task_id = download_persistent_cache_piece.task_id();

                // Get the interested piece number from the request.
                let piece_number = download_persistent_cache_piece.piece_number();

                // Generate the piece id.
                let piece_id = self.storage.piece_id(task_id, piece_number);

                Span::current().record("host_id", host_id);
                Span::current().record("remote_address", remote_address.as_str());
                Span::current().record("task_id", task_id);
                Span::current().record("piece_id", piece_id.as_str());

                // Collect upload piece started metrics.
                collect_upload_piece_started_metrics();
                info!("start upload persistent cache piece content");

                match self
                    .handle_persistent_cache_piece(piece_id.as_str(), task_id)
                    .await
                {
                    Ok((persistent_cache_piece_content, mut content_reader)) => {
                        let persistent_cache_piece_content_bytes: Bytes =
                            persistent_cache_piece_content.into();

                        let header = Header::new_persistent_cache_piece_content(
                            persistent_cache_piece_content_bytes.len() as u32,
                        );
                        let header_bytes: Bytes = header.into();

                        let mut response = BytesMut::with_capacity(
                            HEADER_SIZE + persistent_cache_piece_content_bytes.len(),
                        );
                        response.extend_from_slice(&header_bytes);
                        response.extend_from_slice(&persistent_cache_piece_content_bytes);

                        self.write_response(response.freeze(), &mut writer).await?;
                        self.write_stream(&mut content_reader, &mut writer).await?;
                    }
                    Err(err) => {
                        // Collect upload piece failure metrics.
                        collect_upload_piece_failure_metrics();

                        let error_response: Bytes =
                            Vortex::Error(Header::new_error(err.len() as u32), err).into();
                        self.write_response(error_response, &mut writer).await?;
                    }
                }

                Ok(())
            }
            _ => Err(ClientError::Unsupported(format!(
                "unsupported tag: {:?}",
                header.tag()
            ))),
        }
    }

    /// Handles download piece request and retrieves piece content.
    ///
    /// This function fetches piece metadata from local storage, applies
    /// upload rate limiting, and prepares both the piece metadata and
    /// content stream for transmission. It's the core handler for regular
    /// piece download requests in the P2P network.
    #[instrument(skip_all)]
    async fn handle_piece(
        &self,
        piece_id: &str,
        task_id: &str,
    ) -> Result<(PieceContent, impl AsyncRead), Error> {
        // Get the piece metadata from the local storage.
        let piece = match self.storage.get_piece(piece_id) {
            Ok(Some(piece)) => piece,
            Ok(None) => {
                error!("piece {} not found in local storage", piece_id);
                return Err(Error::new(
                    Code::NotFound,
                    format!("piece {} not found", piece_id),
                ));
            }
            Err(err) => {
                error!("get piece {} from local storage error: {:?}", piece_id, err);
                return Err(Error::new(
                    Code::Internal,
                    format!("failed to get piece: {}", err),
                ));
            }
        };

        // Acquire the upload rate limiter.
        self.upload_rate_limiter
            .acquire(piece.length as usize)
            .await;

        // Upload the piece content.
        let reader = self
            .storage
            .upload_piece(piece_id, task_id, None)
            .await
            .map_err(|err| {
                error!("failed to get piece content: {}", err);
                Error::new(
                    Code::Internal,
                    format!("failed to get piece {} content: {}", piece_id, err),
                )
            })?;

        Ok((
            PieceContent::new(
                piece.number,
                piece.offset,
                piece.length,
                piece.digest.clone(),
                piece.parent_id.clone().unwrap_or_default(),
                TrafficType::RemotePeer as u8,
                piece.cost().unwrap_or_default(),
                piece.created_at,
            ),
            reader,
        ))
    }

    /// Handles download persistent cache piece request and retrieves content.
    ///
    /// Similar to handle_piece but specifically for persistent cache pieces
    /// which have different storage semantics and metadata structure. This
    /// enables efficient serving of frequently accessed content from the
    /// persistent cache layer.
    #[instrument(skip_all)]
    async fn handle_persistent_cache_piece(
        &self,
        piece_id: &str,
        task_id: &str,
    ) -> Result<(PersistentCachePieceContent, impl AsyncRead), Error> {
        // Get the piece metadata from the local storage.
        let piece = match self.storage.get_persistent_cache_piece(piece_id) {
            Ok(Some(piece)) => piece,
            Ok(None) => {
                error!("piece {} not found in local storage", piece_id);
                return Err(Error::new(
                    Code::NotFound,
                    format!("piece {} not found", piece_id),
                ));
            }
            Err(err) => {
                error!("get piece {} from local storage error: {:?}", piece_id, err);
                return Err(Error::new(
                    Code::Internal,
                    format!("failed to get piece: {}", err),
                ));
            }
        };

        // Acquire the upload rate limiter.
        self.upload_rate_limiter
            .acquire(piece.length as usize)
            .await;

        // Upload the piece content.
        let reader = self
            .storage
            .upload_persistent_cache_piece(piece_id, task_id, None)
            .await
            .map_err(|err| {
                error!("failed to get piece content: {}", err);
                Error::new(
                    Code::Internal,
                    format!("failed to get piece {} content: {}", piece_id, err),
                )
            })?;

        Ok((
            PersistentCachePieceContent::new(
                piece.number,
                piece.offset,
                piece.length,
                piece.digest.clone(),
                piece.parent_id.clone().unwrap_or_default(),
                TrafficType::RemotePeer as u8,
                piece.cost().unwrap_or_default(),
                piece.created_at,
            ),
            reader,
        ))
    }

    /// Reads and parses a vortex protocol header from the TCP stream.
    ///
    /// The header contains metadata about the following message, including
    /// the message type (tag) and payload length. This is critical for
    /// proper protocol message framing.
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

    /// Reads and parses a download piece message from the TCP stream.
    ///
    /// This function reads a fixed-length payload based on the header length
    /// and attempts to parse it into the specified type T. The type T must
    /// implement TryFrom<Bytes> for deserialization from the raw bytes.
    pub async fn read_download_piece<T>(
        &self,
        reader: &mut OwnedReadHalf,
        header_length: usize,
    ) -> ClientResult<T>
    where
        T: TryFrom<Bytes, Error: Into<ClientError>>,
    {
        let mut download_piece_bytes = BytesMut::with_capacity(header_length);
        download_piece_bytes.resize(header_length, 0);

        reader
            .read_exact(&mut download_piece_bytes)
            .await
            .inspect_err(|err| {
                error!("failed to receive download piece: {}", err);
            })?;

        download_piece_bytes.freeze().try_into().map_err(Into::into)
    }

    /// Writes a complete response message to the TCP stream.
    ///
    /// This function sends the provided bytes as a response and ensures
    /// all data is flushed to the underlying transport. This is typically
    /// used for sending headers and small payloads in a single operation.
    #[instrument(skip_all)]
    async fn write_response(
        &self,
        request: Bytes,
        writer: &mut OwnedWriteHalf,
    ) -> ClientResult<()> {
        writer.write_all(&request).await.inspect_err(|err| {
            error!("failed to send request: {}", err);
        })?;

        writer.flush().await.inspect_err(|err| {
            error!("failed to flush request: {}", err);
        })?;

        Ok(())
    }

    /// Streams data from a reader directly to the TCP writer.
    ///
    /// This function efficiently copies all data from the provided stream
    /// to the TCP connection using tokio's copy utility. It's designed for
    /// streaming large piece content without loading everything into memory.
    /// The operation is flushed to ensure data delivery.
    #[instrument(skip_all)]
    async fn write_stream<R: AsyncRead + Unpin + ?Sized>(
        &self,
        stream: &mut R,
        writer: &mut OwnedWriteHalf,
    ) -> ClientResult<()> {
        copy(stream, writer).await.inspect_err(|err| {
            error!("copy failed: {}", err);
        })?;

        writer.flush().await.inspect_err(|err| {
            error!("flush failed: {}", err);
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_client_config::dfdaemon::Config;
    use std::time::Duration;
    use tempfile::tempdir;

    async fn create_test_tcp_server_handler() -> TCPServerHandler {
        let config = Arc::new(Config::default());
        let dir = tempdir().unwrap();
        let log_dir = dir.path().join("log");

        let storage = Storage::new(config.clone(), dir.path(), log_dir)
            .await
            .unwrap();
        let storage = Arc::new(storage);

        let id_generator = IDGenerator::new(
            "127.0.0.1".to_string(),
            config.host.hostname.clone(),
            config.seed_peer.enable,
        );
        let id_generator = Arc::new(id_generator);

        let upload_rate_limiter = Arc::new(
            RateLimiter::builder()
                .initial(config.upload.rate_limit.as_u64() as usize)
                .refill(config.upload.rate_limit.as_u64() as usize)
                .max(config.upload.rate_limit.as_u64() as usize)
                .interval(Duration::from_secs(1))
                .fair(false)
                .build(),
        );

        TCPServerHandler {
            id_generator,
            storage,
            upload_rate_limiter,
        }
    }

    #[tokio::test]
    async fn test_read_header_success() {
        let handler = create_test_tcp_server_handler().await;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, _writer) = stream.into_split();
            handler.read_header(&mut reader).await
        });

        let client_stream = TcpStream::connect(addr).await.unwrap();
        let (_reader, mut writer) = client_stream.into_split();

        let header = Header::new(Tag::DownloadPiece, 100);
        let header_bytes: Bytes = header.into();
        writer.write_all(&header_bytes).await.unwrap();
        writer.flush().await.unwrap();

        let result = server_handle.await.unwrap();
        assert!(result.is_ok());
        if let Ok(header) = result {
            assert_eq!(header.tag(), Tag::DownloadPiece);
            assert_eq!(header.length(), 100);
        }
    }

    #[tokio::test]
    async fn test_read_header_insufficient_data() {
        let handler = create_test_tcp_server_handler().await;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, _writer) = stream.into_split();
            handler.read_header(&mut reader).await
        });

        let client_stream = TcpStream::connect(addr).await.unwrap();
        let (_reader, mut writer) = client_stream.into_split();

        let partial_data = vec![0u8; HEADER_SIZE - 1];
        writer.write_all(&partial_data).await.unwrap();
        writer.flush().await.unwrap();
        drop(writer);

        let result = server_handle.await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_header_connection_closed() {
        let handler = create_test_tcp_server_handler().await;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, _writer) = stream.into_split();
            handler.read_header(&mut reader).await
        });

        let client_stream = TcpStream::connect(addr).await.unwrap();
        drop(client_stream);

        let result = server_handle.await.unwrap();
        assert!(result.is_err());
    }

    const HEADER_LENGTH: usize = 68;

    #[tokio::test]
    async fn test_read_download_piece_success() {
        let handler = create_test_tcp_server_handler().await;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, _writer) = stream.into_split();
            handler
                .read_download_piece(&mut reader, HEADER_LENGTH)
                .await
        });

        let client_stream = TcpStream::connect(addr).await.unwrap();
        let (_reader, mut writer) = client_stream.into_split();

        let task_id = "a".repeat(64);
        let piece_number = 42;
        let download_piece = DownloadPiece::new(task_id.clone(), piece_number);
        let bytes: Bytes = download_piece.into();
        writer.write_all(&bytes).await.unwrap();
        writer.flush().await.unwrap();

        let result: ClientResult<DownloadPiece> = server_handle.await.unwrap();
        assert!(result.is_ok());
        if let Ok(download_piece) = result {
            assert_eq!(download_piece.task_id(), task_id);
            assert_eq!(download_piece.piece_number(), piece_number);
        }
    }

    #[tokio::test]
    async fn test_read_download_piece_insufficient_data() {
        let handler = create_test_tcp_server_handler().await;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, _writer) = stream.into_split();
            handler
                .read_download_piece(&mut reader, HEADER_LENGTH)
                .await
        });

        let client_stream = TcpStream::connect(addr).await.unwrap();
        let (_reader, mut writer) = client_stream.into_split();

        let partial_data = vec![0u8; HEADER_LENGTH - 1];
        writer.write_all(&partial_data).await.unwrap();
        writer.flush().await.unwrap();
        drop(writer);

        let result: ClientResult<DownloadPiece> = server_handle.await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_download_piece_connection_closed() {
        let handler = create_test_tcp_server_handler().await;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, _writer) = stream.into_split();
            handler
                .read_download_piece(&mut reader, HEADER_LENGTH)
                .await
        });

        let client_stream = TcpStream::connect(addr).await.unwrap();
        drop(client_stream);

        let result: ClientResult<DownloadPiece> = server_handle.await.unwrap();
        assert!(result.is_err());
    }

    async fn create_test_tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_task = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

        let (server_stream, _) = listener.accept().await.unwrap();
        let client_stream = client_task.await.unwrap();

        (server_stream, client_stream)
    }

    #[tokio::test]
    async fn test_write_response_success() {
        let handler = create_test_tcp_server_handler().await;
        let (server_stream, mut client_stream) = create_test_tcp_pair().await;
        let (_server_reader, mut server_writer) = server_stream.into_split();

        let test_data = Bytes::from("Hello from server!");

        let write_task = tokio::spawn(async move {
            handler
                .write_response(test_data.clone(), &mut server_writer)
                .await
        });

        let mut response_buffer = vec![0u8; 1024];
        let bytes_read = client_stream.read(&mut response_buffer).await.unwrap();

        assert_eq!(&response_buffer[..bytes_read], b"Hello from server!");
        assert!(write_task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_write_response_large_data() {
        let handler = create_test_tcp_server_handler().await;
        let (server_stream, mut client_stream) = create_test_tcp_pair().await;
        let (_server_reader, mut server_writer) = server_stream.into_split();

        let large_data = Bytes::from(vec![42u8; 1024 * 1024]);

        let write_task = tokio::spawn(async move {
            handler
                .write_response(large_data.clone(), &mut server_writer)
                .await
        });

        let mut total_received = 0;
        let mut buffer = vec![0u8; 8192];

        while total_received < 1024 * 1024 {
            match client_stream.read(&mut buffer).await {
                Ok(0) => break,
                Ok(n) => {
                    total_received += n;
                    assert!(buffer[..n].iter().all(|&b| b == 42));
                }
                Err(e) => panic!("Read error: {}", e),
            }
        }

        assert_eq!(total_received, 1024 * 1024);
        assert!(write_task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_write_response_client_slow_read() {
        let handler = create_test_tcp_server_handler().await;
        let (server_stream, mut client_stream) = create_test_tcp_pair().await;
        let (_server_reader, mut server_writer) = server_stream.into_split();

        let test_data = Bytes::from(vec![1u8; 64 * 1024]);

        let write_task =
            tokio::spawn(
                async move { handler.write_response(test_data, &mut server_writer).await },
            );

        tokio::spawn(async move {
            let mut buffer = vec![0u8; 1024];
            let mut total_read = 0;

            while total_read < 64 * 1024 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                match client_stream.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(n) => total_read += n,
                    Err(_) => break,
                }
            }
        });

        let result = tokio::time::timeout(Duration::from_secs(5), write_task).await;
        assert!(result.is_ok());
        assert!(result.unwrap().unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_write_stream_success() {
        let handler = create_test_tcp_server_handler().await;
        let (server_stream, mut client_stream) = create_test_tcp_pair().await;
        let (_server_reader, mut server_writer) = server_stream.into_split();

        let test_data = b"Stream content for testing".to_vec();
        let mut mock_stream = std::io::Cursor::new(test_data.clone());

        let write_task = tokio::spawn(async move {
            handler
                .write_stream(&mut mock_stream, &mut server_writer)
                .await
        });

        let mut response_buffer = vec![0u8; 1024];
        let bytes_read = client_stream.read(&mut response_buffer).await.unwrap();

        assert_eq!(&response_buffer[..bytes_read], test_data.as_slice());
        assert!(write_task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_write_stream_large_stream() {
        let handler = create_test_tcp_server_handler().await;
        let (server_stream, mut client_stream) = create_test_tcp_pair().await;
        let (_server_reader, mut server_writer) = server_stream.into_split();

        let large_data = vec![123u8; 10 * 1024 * 1024];
        let mut mock_stream = std::io::Cursor::new(large_data.clone());

        let write_task = tokio::spawn(async move {
            handler
                .write_stream(&mut mock_stream, &mut server_writer)
                .await
        });

        let mut total_received = 0;
        let mut buffer = vec![0u8; 64 * 1024];

        while total_received < 10 * 1024 * 1024 {
            match tokio::time::timeout(Duration::from_secs(1), client_stream.read(&mut buffer))
                .await
            {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => {
                    total_received += n;
                    assert!(buffer[..n].iter().all(|&b| b == 123));
                }
                Ok(Err(e)) => panic!("Read error: {}", e),
                Err(_) => panic!("Read timeout"),
            }
        }

        assert_eq!(total_received, 10 * 1024 * 1024);
        assert!(write_task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_write_stream_with_slow_source() {
        let handler = create_test_tcp_server_handler().await;
        let (server_stream, mut client_stream) = create_test_tcp_pair().await;
        let (_server_reader, mut server_writer) = server_stream.into_split();

        struct SlowReader {
            data: std::io::Cursor<Vec<u8>>,
            delay: Duration,
        }

        impl AsyncRead for SlowReader {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                cx.waker().wake_by_ref();
                std::thread::sleep(self.delay);

                let mut temp_buf = vec![0u8; std::cmp::min(buf.remaining(), 10)];
                match std::io::Read::read(&mut self.data, &mut temp_buf) {
                    Ok(0) => std::task::Poll::Ready(Ok(())),
                    Ok(n) => {
                        buf.put_slice(&temp_buf[..n]);
                        std::task::Poll::Ready(Ok(()))
                    }
                    Err(e) => std::task::Poll::Ready(Err(e)),
                }
            }
        }

        let test_data = b"Slow stream data".to_vec();
        let mut slow_stream = SlowReader {
            data: std::io::Cursor::new(test_data.clone()),
            delay: Duration::from_millis(5),
        };

        let write_task = tokio::spawn(async move {
            handler
                .write_stream(&mut slow_stream, &mut server_writer)
                .await
        });

        let mut response_buffer = vec![0u8; 1024];
        let bytes_read = tokio::time::timeout(
            Duration::from_secs(2),
            client_stream.read(&mut response_buffer),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(&response_buffer[..bytes_read], test_data.as_slice());
        assert!(write_task.await.unwrap().is_ok());
    }
}
