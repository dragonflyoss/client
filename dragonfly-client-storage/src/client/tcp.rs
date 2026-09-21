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
use futures::{Stream, StreamExt};
use socket2::{SockRef, TcpKeepalive};
use std::collections::VecDeque;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Take};
use tokio::net::TcpStream;
use tokio::time;
use tokio_util::io::poll_read_buf;
use tracing::{debug, error, instrument, Span};
use vortex_protocol::{
    tlv::{
        download_persistent_cache_piece::DownloadPersistentCachePiece,
        download_persistent_piece::DownloadPersistentPiece, download_piece::DownloadPiece,
        error::Error as VortexError, persistent_cache_piece_content, persistent_piece_content,
        piece_content, Tag,
    },
    Header, Vortex, HEADER_SIZE,
};

/// The idle connections to a server with the time they became idle.
type IdleConnections = Arc<Mutex<VecDeque<(TcpStream, Instant)>>>;

/// A TCP-based client for tcp storage service.
#[derive(Clone)]
pub struct TCPClient {
    /// The configuration of the dfdaemon.
    config: Arc<Config>,

    /// The address of the TCP server.
    addr: String,

    /// The idle connections reused by the following piece requests, skipping
    /// the handshake and the slow start of a new connection for every piece.
    idle_connections: IdleConnections,
}

/// Implements the TCP-based client for tcp storage service.
impl TCPClient {
    /// Creates a new TCPClient instance.
    pub fn new(config: Arc<Config>, addr: String) -> Self {
        Self {
            config,
            addr,
            idle_connections: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Streams the piece content of the connection, so the storage writes the
    /// chunks without copying them again, and returns the connection to the
    /// idle connections once the content is read completely.
    fn content_stream(&self, stream: TcpStream, length: u64) -> super::PieceContentStream {
        ContentStream {
            reader: Some(stream.take(length)),
            buf: BytesMut::new(),
            capacity: self.config.storage.write_buffer_size,
            idle_connections: self.idle_connections.clone(),
        }
        .boxed()
    }

    /// Downloads a piece from the server using the vortex protocol.
    ///
    /// This is the main entry point for downloading a piece. It applies
    /// a timeout based on the configuration and handles connection timeouts gracefully.
    #[instrument(skip_all, fields(parent_addr))]
    pub async fn download_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(super::PieceContentStream, u64, String)> {
        Span::current().record("parent_addr", self.addr.as_str());

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
    /// 2. Sends the request on an idle or new connection.
    /// 3. Receives and validates the response header.
    /// 4. Processes the piece content based on the response type.
    #[instrument(skip_all)]
    async fn handle_download_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(super::PieceContentStream, u64, String)> {
        let request: Bytes = Vortex::DownloadPiece(
            Header::new_download_piece(),
            DownloadPiece::new(task_id.to_string(), number),
        )
        .into();

        let (mut stream, header) = self.request(request).await?;
        match header.tag() {
            Tag::PieceContent => {
                let piece_content: piece_content::PieceContent = self
                    .recv_piece_content(&mut stream, piece_content::METADATA_LENGTH_SIZE)
                    .await?;
                debug!("received piece content: {:?}", piece_content.metadata());

                let metadata = piece_content.metadata();
                Ok((
                    self.content_stream(stream, metadata.length),
                    metadata.offset,
                    metadata.digest,
                ))
            }
            Tag::Error => Err(self.recv_error(&mut stream, header.length() as usize).await),
            _ => Err(ClientError::Unknown(format!(
                "unexpected tag: {:?}",
                header.tag()
            ))),
        }
    }

    /// Downloads a persistent piece from the server using the vortex protocol.
    ///
    /// Similar to `download_piece` but specifically for persistent piece.
    #[instrument(skip_all)]
    pub async fn download_persistent_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(super::PieceContentStream, u64, String)> {
        time::timeout(
            self.config.download.piece_timeout,
            self.handle_download_persistent_piece(number, task_id),
        )
        .await
        .inspect_err(|err| {
            error!("connect timeout to {}: {}", self.addr, err);
        })?
    }

    /// Internal handler for downloading a persistent piece.
    ///
    /// Implements the same protocol flow as `handle_download_piece` but uses
    /// persistent specific request/response types.
    #[instrument(skip_all)]
    async fn handle_download_persistent_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(super::PieceContentStream, u64, String)> {
        let request: Bytes = Vortex::DownloadPersistentPiece(
            Header::new_download_persistent_piece(),
            DownloadPersistentPiece::new(task_id.to_string(), number),
        )
        .into();

        let (mut stream, header) = self.request(request).await?;
        match header.tag() {
            Tag::PersistentPieceContent => {
                let persistent_piece_content: persistent_piece_content::PersistentPieceContent =
                    self.recv_piece_content(&mut stream, piece_content::METADATA_LENGTH_SIZE)
                        .await?;
                debug!(
                    "received piece content: {:?}",
                    persistent_piece_content.metadata()
                );

                let metadata = persistent_piece_content.metadata();
                Ok((
                    self.content_stream(stream, metadata.length),
                    metadata.offset,
                    metadata.digest,
                ))
            }
            Tag::Error => Err(self.recv_error(&mut stream, header.length() as usize).await),
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
    ) -> ClientResult<(super::PieceContentStream, u64, String)> {
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
    ) -> ClientResult<(super::PieceContentStream, u64, String)> {
        let request: Bytes = Vortex::DownloadPersistentCachePiece(
            Header::new_download_persistent_cache_piece(),
            DownloadPersistentCachePiece::new(task_id.to_string(), number),
        )
        .into();

        let (mut stream, header) = self.request(request).await?;
        match header.tag() {
            Tag::PersistentCachePieceContent => {
                let persistent_cache_piece_content: persistent_cache_piece_content::PersistentCachePieceContent =
                    self.recv_piece_content(&mut stream, piece_content::METADATA_LENGTH_SIZE)
                        .await?;
                debug!(
                    "received piece content: {:?}",
                    persistent_cache_piece_content.metadata()
                );

                let metadata = persistent_cache_piece_content.metadata();
                Ok((
                    self.content_stream(stream, metadata.length),
                    metadata.offset,
                    metadata.digest,
                ))
            }
            Tag::Error => Err(self.recv_error(&mut stream, header.length() as usize).await),
            _ => Err(ClientError::Unknown(format!(
                "unexpected tag: {:?}",
                header.tag()
            ))),
        }
    }

    /// Sends the request on an idle connection, or on a new one when none is
    /// idle, and receives the response header. An idle connection closed by
    /// the server fails here, so the request is retried once on a new one.
    #[instrument(skip_all)]
    async fn request(&self, request: Bytes) -> ClientResult<(TcpStream, Header)> {
        if let Some(mut stream) = self.idle_connection() {
            match self.round_trip(&mut stream, &request).await {
                Ok(header) => return Ok((stream, header)),
                Err(err) => debug!("idle connection to {} failed: {}", self.addr, err),
            }
        }

        let mut stream = self.connect().await?;
        let header = self.round_trip(&mut stream, &request).await?;
        Ok((stream, header))
    }

    /// Sends the request and receives the response header on the connection.
    async fn round_trip(&self, stream: &mut TcpStream, request: &Bytes) -> ClientResult<Header> {
        self.send_request(stream, request).await?;
        self.recv_header(stream).await
    }

    /// Takes the most recently idle connection, closing the ones idle for
    /// longer than the idle timeout and the ones closed by the server.
    fn idle_connection(&self) -> Option<TcpStream> {
        let mut idle_connections = self.idle_connections.lock().ok()?;
        while idle_connections
            .front()
            .is_some_and(|(_, idle_at)| idle_at.elapsed() >= super::DEFAULT_MAX_IDLE_TIMEOUT)
        {
            idle_connections.pop_front();
        }

        while let Some((stream, _)) = idle_connections.pop_back() {
            if Self::is_open(&stream) {
                return Some(stream);
            }
        }

        None
    }

    /// Returns whether the idle connection is still open. An idle connection
    /// has nothing to read, so a readable byte or EOF means the server closed
    /// it or the connection is out of sync.
    fn is_open(stream: &TcpStream) -> bool {
        let mut buf = [MaybeUninit::<u8>::uninit()];
        matches!(
            SockRef::from(stream).peek(&mut buf),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock
        )
    }

    /// Establishes a TCP connection to the server.
    #[instrument(skip_all)]
    async fn connect(&self) -> ClientResult<TcpStream> {
        let stream = tokio::time::timeout(
            super::DEFAULT_CONNECT_TIMEOUT,
            TcpStream::connect(self.addr.clone()),
        )
        .await?
        .inspect_err(|err| {
            error!("failed to connect to {}: {}", self.addr, err);
        })?;

        let socket = SockRef::from(&stream);
        socket.set_tcp_nodelay(true)?;
        socket.set_nonblocking(true)?;
        socket.set_tcp_keepalive(
            &TcpKeepalive::new()
                .with_interval(super::DEFAULT_KEEPALIVE_INTERVAL)
                .with_time(super::DEFAULT_KEEPALIVE_TIME)
                .with_retries(super::DEFAULT_KEEPALIVE_RETRIES),
        )?;
        #[cfg(target_os = "linux")]
        {
            use dragonfly_client_util::net::set_tcp_fastopen_connect;
            use std::os::unix::io::AsRawFd;
            use tracing::{info, warn};

            if self.config.storage.server.tcp_fastopen {
                if let Err(err) = set_tcp_fastopen_connect(socket.as_raw_fd()) {
                    warn!("failed to enable tcp fastopen: {}", err);
                } else {
                    info!("enabled tcp fastopen");
                }
            }
        }

        Ok(stream)
    }

    /// Sends a vortex protocol request on the connection.
    #[instrument(skip_all)]
    async fn send_request(&self, stream: &mut TcpStream, request: &Bytes) -> ClientResult<()> {
        stream.write_all(request).await.inspect_err(|err| {
            error!("failed to send request: {}", err);
        })?;

        stream.flush().await.inspect_err(|err| {
            error!("failed to flush request: {}", err);
        })?;

        Ok(())
    }

    /// Receives and parses a vortex protocol header from the TCP stream.
    ///
    /// The header contains metadata about the following message, including
    /// the message type (tag) and payload length. This is critical for
    /// proper protocol message framing.
    #[instrument(skip_all)]
    async fn recv_header(&self, stream: &mut TcpStream) -> ClientResult<Header> {
        let mut header_bytes = BytesMut::with_capacity(HEADER_SIZE);
        header_bytes.resize(HEADER_SIZE, 0);
        stream
            .read_exact(&mut header_bytes)
            .await
            .inspect_err(|err| {
                error!("failed to receive header: {}", err);
            })?;

        Header::try_from(header_bytes.freeze()).map_err(Into::into)
    }

    /// Receives and parses piece content with variable-length metadata.
    ///
    /// This generic function handles the two-stage reading process for
    /// piece content: first reading the metadata length, then reading
    /// the actual metadata, and finally constructing the complete message.
    #[instrument(skip_all)]
    async fn recv_piece_content<T>(
        &self,
        stream: &mut TcpStream,
        metadata_length_size: usize,
    ) -> ClientResult<T>
    where
        T: TryFrom<Bytes, Error: Into<ClientError>>,
    {
        let mut metadata_length_bytes = BytesMut::with_capacity(metadata_length_size);
        metadata_length_bytes.resize(metadata_length_size, 0);
        stream
            .read_exact(&mut metadata_length_bytes)
            .await
            .inspect_err(|err| {
                error!("failed to receive metadata length: {}", err);
            })?;
        let metadata_length = u32::from_be_bytes(metadata_length_bytes[..].try_into()?) as usize;

        let mut metadata_bytes = BytesMut::with_capacity(metadata_length);
        metadata_bytes.resize(metadata_length, 0);
        stream
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

    /// Receives and processes error responses from the server.
    ///
    /// When the server responds with an error tag, this function reads
    /// the error payload and converts it into an appropriate client error.
    /// This provides structured error handling for protocol-level failures.
    #[instrument(skip_all)]
    async fn recv_error(&self, stream: &mut TcpStream, header_length: usize) -> ClientError {
        let mut error_bytes = BytesMut::with_capacity(header_length);
        error_bytes.resize(header_length, 0);
        if let Err(err) = stream.read_exact(&mut error_bytes).await {
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
                ClientError::Unknown(format!("failed to extract error: {err}"))
            })
    }
}

/// The stream of the piece content on a connection, which is returned to the
/// idle connections once read completely and closed when dropped before that.
struct ContentStream {
    /// The connection limited to the content length, none once the content is
    /// read completely or the connection failed.
    reader: Option<Take<TcpStream>>,

    /// The buffer the chunks are read into.
    buf: BytesMut,

    /// The capacity of the buffer.
    capacity: usize,

    /// The idle connections the connection is returned to.
    idle_connections: IdleConnections,
}

impl Stream for ContentStream {
    type Item = std::io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let Some(reader) = this.reader.as_mut() else {
            return Poll::Ready(None);
        };

        if this.buf.capacity() == 0 {
            this.buf.reserve(this.capacity);
        }

        let n = match poll_read_buf(Pin::new(&mut *reader), cx, &mut this.buf) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => {
                this.reader = None;
                return Poll::Ready(Some(Err(err)));
            }
            Poll::Ready(Ok(n)) => n,
        };

        if reader.limit() == 0 {
            // The content is read completely, so the connection is idle again.
            let stream = this.reader.take().unwrap().into_inner();
            if let Ok(mut idle_connections) = this.idle_connections.lock() {
                idle_connections.push_back((stream, Instant::now()));
            }
        } else if n == 0 {
            // The server closed the connection before the whole content.
            this.reader = None;
        }

        if n == 0 {
            return Poll::Ready(None);
        }

        Poll::Ready(Some(Ok(this.buf.split().freeze())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use dragonfly_api::common::v2::TrafficType;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::net::TcpListener;

    const CONTENT: &[u8] = b"hello vortex";

    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (stream, server)
    }

    async fn serve(
        listener: TcpListener,
        requests_per_connection: usize,
        accepted: Arc<AtomicUsize>,
    ) {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            accepted.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                for _ in 0..requests_per_connection {
                    let mut header = [0u8; HEADER_SIZE];
                    if stream.read_exact(&mut header).await.is_err() {
                        return;
                    }

                    let header = Header::try_from(Bytes::copy_from_slice(&header)).unwrap();
                    let mut body = vec![0u8; header.length() as usize];
                    stream.read_exact(&mut body).await.unwrap();
                    let request = DownloadPiece::try_from(Bytes::from(body)).unwrap();

                    let content: Bytes = piece_content::PieceContent::new(
                        request.piece_number(),
                        0,
                        CONTENT.len() as u64,
                        "crc32:1".to_string(),
                        "parent".to_string(),
                        TrafficType::RemotePeer as u8,
                        Duration::ZERO,
                        Utc::now().naive_utc(),
                    )
                    .into();
                    let header: Bytes = Header::new_piece_content(content.len() as u32).into();
                    stream.write_all(&header).await.unwrap();
                    stream.write_all(&content).await.unwrap();
                    stream.write_all(CONTENT).await.unwrap();
                }
            });
        }
    }

    async fn download(client: &TCPClient, number: u32) -> Vec<u8> {
        let (mut stream, offset, _) = client
            .download_piece(number, &"0".repeat(64))
            .await
            .unwrap();
        assert_eq!(offset, 0);

        let mut content = Vec::new();
        while let Some(chunk) = stream.next().await {
            content.extend_from_slice(&chunk.unwrap());
        }
        content
    }

    #[tokio::test]
    async fn download_piece_reuses_the_idle_connection_and_retries_a_closed_one() {
        let test_cases = vec![(usize::MAX, 3, 1), (1, 3, 3)];

        for (requests_per_connection, downloads, expected_connections) in test_cases {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let accepted = Arc::new(AtomicUsize::new(0));
            let server = tokio::spawn(serve(listener, requests_per_connection, accepted.clone()));

            let client = TCPClient::new(Arc::new(Config::default()), addr);
            for number in 0..downloads {
                assert_eq!(download(&client, number).await, CONTENT);
            }

            assert_eq!(accepted.load(Ordering::SeqCst), expected_connections);
            server.abort();
        }
    }

    #[tokio::test]
    async fn content_stream_returns_the_connection_once_the_content_is_read() {
        let data = b"hello vortex, and more";
        let test_cases = vec![
            (CONTENT.len(), false, true, 1, true),
            (data.len(), false, true, 1, false),
            (5, true, true, 0, false),
            (5, false, false, 0, false),
        ];

        for (written, close_server, read_all, expected_idle, expected_reusable) in test_cases {
            let client = TCPClient::new(Arc::new(Config::default()), String::new());
            let (stream, mut server) = tcp_pair().await;
            server.write_all(&data[..written]).await.unwrap();
            if close_server {
                drop(server);
            }

            let mut content_stream = client.content_stream(stream, CONTENT.len() as u64);
            let mut received = Vec::new();
            while let Some(chunk) = content_stream.next().await {
                received.extend_from_slice(&chunk.unwrap());
                if !read_all {
                    break;
                }
            }
            drop(content_stream);

            assert_eq!(received, &data[..written.min(CONTENT.len())]);
            assert_eq!(client.idle_connections.lock().unwrap().len(), expected_idle);
            assert_eq!(client.idle_connection().is_some(), expected_reusable);
        }
    }

    #[tokio::test]
    async fn idle_connection_takes_the_latest_open_connection() {
        let expired = crate::client::DEFAULT_MAX_IDLE_TIMEOUT + Duration::from_secs(1);
        let test_cases = vec![
            (vec![(Duration::ZERO, true)], true, 0),
            (vec![(expired, true)], false, 0),
            (vec![(expired, true), (Duration::ZERO, true)], true, 0),
            (vec![(Duration::ZERO, false)], false, 0),
            (
                vec![(Duration::ZERO, true), (Duration::ZERO, false)],
                true,
                0,
            ),
            (
                vec![(Duration::ZERO, false), (Duration::ZERO, true)],
                true,
                1,
            ),
        ];

        for (connections, expected_some, expected_remaining) in test_cases {
            let client = TCPClient::new(Arc::new(Config::default()), String::new());
            let mut servers = Vec::new();
            for (idle_for, open) in connections {
                let (stream, server) = tcp_pair().await;
                if open {
                    servers.push(server);
                } else {
                    drop(server);
                    stream.readable().await.unwrap();
                }

                client
                    .idle_connections
                    .lock()
                    .unwrap()
                    .push_back((stream, Instant::now() - idle_for));
            }

            assert_eq!(client.idle_connection().is_some(), expected_some);
            assert_eq!(
                client.idle_connections.lock().unwrap().len(),
                expected_remaining
            );
        }
    }
}
