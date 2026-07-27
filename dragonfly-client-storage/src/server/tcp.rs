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

use crate::io::RangeReader;
use crate::Storage;
use bytes::{Bytes, BytesMut};
use dragonfly_api::common::v2::TrafficType;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use dragonfly_client_metric::{
    collect_upload_piece_failure_metrics, collect_upload_piece_finished_metrics,
    collect_upload_piece_started_metrics, collect_upload_piece_traffic_metrics,
};
use dragonfly_client_util::{id_generator::IDGenerator, shutdown};
use leaky_bucket::RateLimiter;
use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener, TcpStream,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, Span};
use vortex_protocol::{
    tlv::{
        download_persistent_cache_piece::DownloadPersistentCachePiece,
        download_persistent_piece::DownloadPersistentPiece,
        download_piece::DownloadPiece,
        error::{Code, Error},
        persistent_cache_piece_content::PersistentCachePieceContent,
        persistent_piece_content::PersistentPieceContent,
        piece_content::PieceContent,
        Tag,
    },
    Header, Vortex, HEADER_SIZE,
};

#[cfg(not(target_os = "linux"))]
use tokio::io::copy_buf;

/// A TCP-based server for dfdaemon upload service.
pub struct TCPServer {
    /// The configuration of the dfdaemon.
    #[allow(dead_code)]
    config: Arc<Config>,

    /// The address of the TCP server.
    addr: SocketAddr,

    /// The request handler.
    handler: TCPServerHandler,

    /// Used to shutdown the TCP server.
    shutdown: shutdown::Shutdown,

    /// Used to notify the TCP server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// Implements the TCP server.
impl TCPServer {
    /// Creates a new TCPServer.
    pub fn new(
        config: Arc<Config>,
        addr: SocketAddr,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        upload_bandwidth_limiter: Arc<RateLimiter>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            config,
            addr,
            handler: TCPServerHandler {
                id_generator,
                storage,
                upload_bandwidth_limiter,
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
            &TcpKeepalive::new()
                .with_interval(super::DEFAULT_KEEPALIVE_INTERVAL)
                .with_time(super::DEFAULT_KEEPALIVE_TIME)
                .with_retries(super::DEFAULT_KEEPALIVE_RETRIES),
        )?;
        #[cfg(target_os = "linux")]
        {
            use dragonfly_client_util::net::set_tcp_fastopen;
            use std::os::unix::io::AsRawFd;
            use tracing::{info, warn};

            if let Err(err) = socket.set_tcp_congestion("cubic".as_bytes()) {
                warn!("failed to set tcp congestion: {}", err);
            } else {
                info!("set tcp congestion to cubic");
            }

            if self.config.storage.server.tcp_fastopen {
                if let Err(err) = set_tcp_fastopen(socket.as_raw_fd()) {
                    warn!("failed to enable tcp fastopen: {}", err);
                } else {
                    info!("enabled tcp fastopen");
                }
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

/// Handles TCP connections and requests.
#[derive(Clone)]
pub struct TCPServerHandler {
    /// The id generator.
    id_generator: Arc<IDGenerator>,

    /// The local storage.
    storage: Arc<Storage>,

    /// The rate limiter of the upload speed in bytes per second.
    upload_bandwidth_limiter: Arc<RateLimiter>,
}

/// Implements the request handler.
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
                    Ok((piece_content, content_reader)) => {
                        let piece_length = piece_content.metadata().length;
                        let piece_content_bytes: Bytes = piece_content.into();

                        let header = Header::new_piece_content(piece_content_bytes.len() as u32);
                        let header_bytes: Bytes = header.into();

                        let mut response =
                            BytesMut::with_capacity(HEADER_SIZE + piece_content_bytes.len());
                        response.extend_from_slice(&header_bytes);
                        response.extend_from_slice(&piece_content_bytes);

                        self.write_response(response.freeze(), &mut writer)
                            .await
                            .inspect_err(|err| {
                                error!("failed to send piece content response: {}", err);

                                // Collect upload piece failure metrics.
                                collect_upload_piece_failure_metrics();
                            })?;

                        self.write_stream(content_reader, &mut writer)
                            .await
                            .inspect_err(|err| {
                                error!("failed to send piece content stream: {}", err);

                                // Collect upload piece failure metrics.
                                collect_upload_piece_failure_metrics();
                            })?;

                        // Collect upload piece finished metrics.
                        collect_upload_piece_finished_metrics();

                        // Collect upload piece traffic metrics.
                        collect_upload_piece_traffic_metrics(piece_length)
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
            Tag::DownloadPersistentPiece => {
                let download_persistent_piece: DownloadPersistentPiece = self
                    .read_download_piece(&mut reader, header.length() as usize)
                    .await?;

                // Generate the host id.
                let host_id = self.id_generator.host_id();

                // Get the task id from the request.
                let task_id = download_persistent_piece.task_id();

                // Get the interested piece number from the request.
                let piece_number = download_persistent_piece.piece_number();

                // Generate the piece id.
                let piece_id = self.storage.piece_id(task_id, piece_number);

                Span::current().record("host_id", host_id);
                Span::current().record("remote_address", remote_address.as_str());
                Span::current().record("task_id", task_id);
                Span::current().record("piece_id", piece_id.as_str());

                // Collect upload piece started metrics.
                collect_upload_piece_started_metrics();
                info!("start upload persistent piece content");

                match self
                    .handle_persistent_piece(piece_id.as_str(), task_id)
                    .await
                {
                    Ok((persistent_piece_content, content_reader)) => {
                        let persistent_piece_length = persistent_piece_content.metadata().length;
                        let persistent_piece_content_bytes: Bytes = persistent_piece_content.into();

                        let header = Header::new_persistent_piece_content(
                            persistent_piece_content_bytes.len() as u32,
                        );
                        let header_bytes: Bytes = header.into();

                        let mut response = BytesMut::with_capacity(
                            HEADER_SIZE + persistent_piece_content_bytes.len(),
                        );
                        response.extend_from_slice(&header_bytes);
                        response.extend_from_slice(&persistent_piece_content_bytes);

                        self.write_response(response.freeze(), &mut writer)
                            .await
                            .inspect_err(|err| {
                                error!("failed to send persistent piece content response: {}", err);

                                // Collect upload piece failure metrics.
                                collect_upload_piece_failure_metrics();
                            })?;

                        self.write_stream(content_reader, &mut writer)
                            .await
                            .inspect_err(|err| {
                                error!("failed to send persistent piece content stream: {}", err);

                                // Collect upload piece failure metrics.
                                collect_upload_piece_failure_metrics();
                            })?;

                        // Collect upload piece finished metrics.
                        collect_upload_piece_finished_metrics();

                        // Collect upload piece traffic metrics.
                        collect_upload_piece_traffic_metrics(persistent_piece_length)
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
                    Ok((persistent_cache_piece_content, content_reader)) => {
                        let persistent_cache_piece_length =
                            persistent_cache_piece_content.metadata().length;
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

                        self.write_response(response.freeze(), &mut writer)
                            .await
                            .inspect_err(|err| {
                                error!(
                                    "failed to send persistent cache piece content response: {}",
                                    err
                                );

                                // Collect upload piece failure metrics.
                                collect_upload_piece_failure_metrics();
                            })?;

                        self.write_stream(content_reader, &mut writer)
                            .await
                            .inspect_err(|err| {
                                error!(
                                    "failed to send persistent cache piece content stream: {}",
                                    err
                                );

                                // Collect upload piece failure metrics.
                                collect_upload_piece_failure_metrics();
                            })?;

                        // Collect upload piece finished metrics.
                        collect_upload_piece_finished_metrics();

                        // Collect upload piece traffic metrics.
                        collect_upload_piece_traffic_metrics(persistent_cache_piece_length)
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
    ) -> Result<(PieceContent, RangeReader), Error> {
        // Get the piece metadata from the local storage.
        let piece = match self.storage.get_piece(piece_id) {
            Ok(Some(piece)) => piece,
            Ok(None) => {
                error!("piece {} not found in local storage", piece_id);
                return Err(Error::new(
                    Code::NotFound,
                    format!("piece {piece_id} not found"),
                ));
            }
            Err(err) => {
                error!("get piece {} from local storage error: {:?}", piece_id, err);
                return Err(Error::new(
                    Code::Internal,
                    format!("failed to get piece: {err}"),
                ));
            }
        };

        // Acquire the upload bandwidth limiter.
        self.upload_bandwidth_limiter
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
                    format!("failed to get piece {piece_id} content: {err}"),
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

    /// Handles download persistent piece request and retrieves content.
    ///
    /// Similar to handle_piece but specifically for persistent pieces
    /// which have different storage semantics and metadata structure. This
    /// enables efficient serving of frequently accessed content from the
    /// persistent layer.
    #[instrument(skip_all)]
    async fn handle_persistent_piece(
        &self,
        piece_id: &str,
        task_id: &str,
    ) -> Result<(PersistentPieceContent, RangeReader), Error> {
        // Get the piece metadata from the local storage.
        let piece = match self.storage.get_persistent_piece(piece_id) {
            Ok(Some(piece)) => piece,
            Ok(None) => {
                error!("piece {} not found in local storage", piece_id);
                return Err(Error::new(
                    Code::NotFound,
                    format!("piece {piece_id} not found"),
                ));
            }
            Err(err) => {
                error!("get piece {} from local storage error: {:?}", piece_id, err);
                return Err(Error::new(
                    Code::Internal,
                    format!("failed to get piece: {err}"),
                ));
            }
        };

        // Acquire the upload bandwidth limiter.
        self.upload_bandwidth_limiter
            .acquire(piece.length as usize)
            .await;

        // Upload the piece content.
        let reader = self
            .storage
            .upload_persistent_piece(piece_id, task_id, None)
            .await
            .map_err(|err| {
                error!("failed to get piece content: {}", err);
                Error::new(
                    Code::Internal,
                    format!("failed to get piece {piece_id} content: {err}"),
                )
            })?;

        Ok((
            PersistentPieceContent::new(
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
    ) -> Result<(PersistentCachePieceContent, RangeReader), Error> {
        // Get the piece metadata from the local storage.
        let piece = match self.storage.get_persistent_cache_piece(piece_id) {
            Ok(Some(piece)) => piece,
            Ok(None) => {
                error!("piece {} not found in local storage", piece_id);
                return Err(Error::new(
                    Code::NotFound,
                    format!("piece {piece_id} not found"),
                ));
            }
            Err(err) => {
                error!("get piece {} from local storage error: {:?}", piece_id, err);
                return Err(Error::new(
                    Code::Internal,
                    format!("failed to get piece: {err}"),
                ));
            }
        };

        // Acquire the upload bandwidth limiter.
        self.upload_bandwidth_limiter
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
                    format!("failed to get piece {piece_id} content: {err}"),
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

    /// Streams the remaining range of the piece file directly to the TCP
    /// socket with sendfile, so the piece bytes move from the page cache to
    /// the socket inside the kernel without passing through user space.
    #[cfg(target_os = "linux")]
    #[instrument(skip_all)]
    async fn write_stream(
        &self,
        reader: RangeReader,
        writer: &mut OwnedWriteHalf,
    ) -> ClientResult<()> {
        debug!("start to write stream to tcp writer");
        let (fd, offset, remaining) = reader.into_parts();
        sendfile_range(writer.as_ref(), &fd, offset, remaining)
            .await
            .inspect_err(|err| {
                error!("sendfile failed: {}", err);
            })?;

        writer.flush().await.inspect_err(|err| {
            error!("flush failed: {}", err);
        })?;
        debug!("finished writing stream to tcp writer");

        Ok(())
    }

    /// Streams data from a reader directly to the TCP writer.
    ///
    /// This function efficiently copies all data from the provided stream
    /// to the TCP connection using tokio's copy_buf utility, which writes the
    /// reader's internal buffer directly without an intermediate copy buffer.
    /// It's designed for streaming large piece content without loading
    /// everything into memory. The operation is flushed to ensure data delivery.
    #[cfg(not(target_os = "linux"))]
    #[instrument(skip_all)]
    async fn write_stream(
        &self,
        mut reader: RangeReader,
        writer: &mut OwnedWriteHalf,
    ) -> ClientResult<()> {
        debug!("start to write stream to tcp writer");
        copy_buf(&mut reader, writer).await.inspect_err(|err| {
            error!("copy failed: {}", err);
        })?;

        writer.flush().await.inspect_err(|err| {
            error!("flush failed: {}", err);
        })?;
        debug!("finished writing stream to tcp writer");

        Ok(())
    }
}

/// Sends `remaining` bytes of the file starting at `offset` to the TCP stream
/// with sendfile. The socket is nonblocking, so each sendfile call is driven
/// by the write readiness of the stream and retried when the socket buffer is
/// full. The file offset is passed explicitly to every call, so the file
/// cursor of the shared descriptor never moves. Stops at the end of the file
/// even if the range is longer, matching the RangeReader semantics.
#[cfg(target_os = "linux")]
async fn sendfile_range(
    stream: &TcpStream,
    fd: &std::fs::File,
    mut offset: u64,
    mut remaining: u64,
) -> std::io::Result<()> {
    // The maximum number of bytes of a single sendfile call, limited by the
    // kernel to 0x7ffff000 on Linux.
    const MAX_SENDFILE_COUNT: u64 = 0x7fff_f000;

    while remaining > 0 {
        stream.writable().await?;
        match stream.try_io(tokio::io::Interest::WRITABLE, || {
            let mut off = offset;
            let count = remaining.min(MAX_SENDFILE_COUNT) as usize;
            rustix::fs::sendfile(stream, fd, Some(&mut off), count)
                .map(|n| n as u64)
                .map_err(std::io::Error::from)
        }) {
            Ok(0) => break,
            Ok(n) => {
                offset += n;
                remaining -= n;
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }

    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    fn pattern(length: usize) -> Vec<u8> {
        (0..length).map(|i| (i % 251) as u8).collect()
    }

    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let sender = TcpStream::connect(addr).await.unwrap();
        let (receiver, _) = listener.accept().await.unwrap();
        (sender, receiver)
    }

    #[tokio::test]
    async fn test_sendfile_range() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("task");
        let data = pattern(8 * 1024 * 1024);
        tokio::fs::write(&path, &data).await.unwrap();
        let fd = std::fs::File::open(&path).unwrap();

        let (sender, mut receiver) = tcp_pair().await;
        socket2::SockRef::from(&sender)
            .set_send_buffer_size(16 * 1024)
            .unwrap();

        let length = data.len() as u64;
        let sender_handle = tokio::spawn(async move {
            sendfile_range(&sender, &fd, 0, length).await.unwrap();
            fd
        });

        let mut received = Vec::new();
        receiver.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, data);

        use std::io::Seek as _;
        let fd = sender_handle.await.unwrap();
        assert_eq!((&fd).stream_position().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_sendfile_range_sub_range() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("task");
        let data = pattern(64 * 1024);
        tokio::fs::write(&path, &data).await.unwrap();
        let fd = std::fs::File::open(&path).unwrap();

        let (sender, mut receiver) = tcp_pair().await;
        tokio::spawn(async move {
            sendfile_range(&sender, &fd, 1_000, 50_000).await.unwrap();
        });

        let mut received = Vec::new();
        receiver.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, &data[1_000..51_000]);
    }

    #[tokio::test]
    async fn test_sendfile_range_stops_at_eof() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("task");
        let data = pattern(64 * 1024);
        tokio::fs::write(&path, &data).await.unwrap();
        let fd = std::fs::File::open(&path).unwrap();

        let (sender, mut receiver) = tcp_pair().await;
        let length = data.len() as u64;
        tokio::spawn(async move {
            sendfile_range(&sender, &fd, 0, length + 4096)
                .await
                .unwrap();
        });

        let mut received = Vec::new();
        receiver.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, data);
    }

    #[tokio::test]
    async fn test_sendfile_range_peer_closed() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("task");
        let data = pattern(8 * 1024 * 1024);
        tokio::fs::write(&path, &data).await.unwrap();
        let fd = std::fs::File::open(&path).unwrap();

        let (sender, receiver) = tcp_pair().await;
        socket2::SockRef::from(&sender)
            .set_send_buffer_size(16 * 1024)
            .unwrap();
        drop(receiver);

        assert!(sendfile_range(&sender, &fd, 0, data.len() as u64)
            .await
            .is_err());
    }
}
