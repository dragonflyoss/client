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
use dragonfly_client_util::{
    id_generator::IDGenerator, shutdown, tls::generate_simple_self_signed_certs,
};
use leaky_bucket::RateLimiter;
use quinn::{congestion::BbrConfig, AckFrequencyConfig, Endpoint, ServerConfig, TransportConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{copy, AsyncRead};
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

/// QUICServer is a QUIC-based server for dfdaemon upload service.
pub struct QUICServer {
    /// addr is the address of the QUIC server.
    addr: SocketAddr,

    /// handler is the request handler.
    handler: QUICServerHandler,

    /// shutdown is used to shutdown the QUIC server.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the QUIC server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// QUICServer implements the QUIC server.
impl QUICServer {
    /// Creates a new QUICServer.
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
            handler: QUICServerHandler {
                id_generator,
                storage,
                upload_rate_limiter,
            },
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// Starts the storage quic server.
    pub async fn run(&mut self) -> ClientResult<()> {
        let (certs, key) = generate_simple_self_signed_certs("d7y", vec!["d7y".into()])?;
        let mut server_config = ServerConfig::with_single_cert(certs, key).map_err(|err| {
            ClientError::Unknown(format!("failed to create server config: {}", err))
        })?;

        let mut transport = TransportConfig::default();
        transport.congestion_controller_factory(Arc::new(BbrConfig::default()));
        transport.keep_alive_interval(Some(super::DEFAULT_KEEPALIVE_INTERVAL));
        transport.max_idle_timeout(Some(super::DEFAULT_MAX_IDLE_TIMEOUT.try_into().unwrap()));
        transport.ack_frequency_config(Some(AckFrequencyConfig::default()));
        transport.send_window(super::DEFAULT_SEND_BUFFER_SIZE as u64);
        transport.receive_window((super::DEFAULT_RECV_BUFFER_SIZE as u32).into());
        transport.stream_receive_window((super::DEFAULT_RECV_BUFFER_SIZE as u32).into());
        server_config.transport_config(Arc::new(transport));

        let endpoint = Endpoint::server(server_config, self.addr)?;
        info!("storage quic server listening on {}", self.addr);

        loop {
            tokio::select! {
                Some(quic_accepted) = endpoint.accept() => {
                    let quic = quic_accepted.await?;
                    let remote_address = quic.remote_address();
                    debug!("accepted connection from {}", remote_address);

                    let handler = self.handler.clone();
                    tokio::spawn(async move {
                       if let Err(err) = handler.handle(quic, remote_address).await {
                            error!("failed to handle connection from {}: {}", remote_address, err);
                        }
                    });
                },
                _ = self.shutdown.recv() => {
                    info!("quic server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

/// QUICServerHandler handles QUIC connections and requests.
#[derive(Clone)]
pub struct QUICServerHandler {
    /// id_generator is the id generator.
    id_generator: Arc<IDGenerator>,

    /// storage is the local storage.
    storage: Arc<Storage>,

    /// upload_rate_limiter is the rate limiter of the upload speed in bps(bytes per second).
    upload_rate_limiter: Arc<RateLimiter>,
}

/// QUICServerHandler implements the request handler.
impl QUICServerHandler {
    /// handle handles a single QUIC connection.
    #[instrument(skip_all)]
    async fn handle(
        &self,
        connection: quinn::Connection,
        remote_address: SocketAddr,
    ) -> ClientResult<()> {
        loop {
            match connection.accept_bi().await {
                Ok((send, recv)) => {
                    let handler = self.clone();
                    tokio::spawn(async move {
                        if let Err(err) = handler.handle_stream(recv, send, remote_address).await {
                            error!("failed to handle stream: {}", err);
                        }
                    });
                }
                Err(err) => {
                    // Downgrade common close cases to debug to reduce noisy logs.
                    match err {
                        quinn::ConnectionError::ApplicationClosed(_)
                        | quinn::ConnectionError::LocallyClosed => {
                            debug!("connection closed: {}", err);
                        }
                        _ => {
                            error!("failed to accept bidirectional stream: {}", err);
                        }
                    }
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handles a single QUIC stream for the Dragonfly P2P protocol.
    ///
    /// This is the main entry point for processing incoming QUIC streams.
    /// It reads the protocol header to determine the request type and dispatches
    /// to the appropriate handler. Supports both regular piece downloads and
    /// persistent cache piece downloads with proper request/response framing.
    #[instrument(skip_all, fields(host_id, remote_address, task_id, piece_id))]
    async fn handle_stream(
        &self,
        mut reader: quinn::RecvStream,
        mut writer: quinn::SendStream,
        remote_address: SocketAddr,
    ) -> ClientResult<()> {
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
                Span::current().record("remote_address", remote_address.to_string().as_str());
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

                        if let Err(err) = writer.finish() {
                            error!("failed to finish stream: {}", err);
                        }
                    }
                    Err(err) => {
                        // Collect upload piece failure metrics.
                        collect_upload_piece_failure_metrics();

                        let error_response: Bytes =
                            Vortex::Error(Header::new_error(err.len() as u32), err).into();
                        self.write_response(error_response, &mut writer).await?;

                        if let Err(err) = writer.finish() {
                            error!("failed to finish stream: {}", err);
                        }
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
                Span::current().record("remote_address", remote_address.to_string().as_str());
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

                        if let Err(err) = writer.finish() {
                            error!("failed to finish stream: {}", err);
                        }
                    }
                    Err(err) => {
                        // Collect upload piece failure metrics.
                        collect_upload_piece_failure_metrics();

                        let error_response: Bytes =
                            Vortex::Error(Header::new_error(err.len() as u32), err).into();
                        self.write_response(error_response, &mut writer).await?;

                        if let Err(err) = writer.finish() {
                            error!("failed to finish stream: {}", err);
                        }
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

    /// Reads and parses a vortex protocol header from the QUIC stream.
    ///
    /// The header contains metadata about the following message, including
    /// the message type (tag) and payload length. This is critical for
    /// proper protocol message framing.
    async fn read_header(&self, reader: &mut quinn::RecvStream) -> ClientResult<Header> {
        let mut header_bytes = BytesMut::with_capacity(HEADER_SIZE);
        header_bytes.resize(HEADER_SIZE, 0);
        reader
            .read_exact(&mut header_bytes)
            .await
            .inspect_err(|err| error!("failed to receive header: {}", err))?;

        Header::try_from(header_bytes.freeze()).map_err(Into::into)
    }

    /// Reads and parses a download piece message from the QUIC stream.
    ///
    /// This function reads a fixed-length payload based on the header length
    /// and attempts to parse it into the specified type T. The type T must
    /// implement TryFrom<Bytes> for deserialization from the raw bytes.
    pub async fn read_download_piece<T>(
        &self,
        reader: &mut quinn::RecvStream,
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
            .inspect_err(|err| error!("failed to receive download piece: {}", err))?;

        download_piece_bytes.freeze().try_into().map_err(Into::into)
    }

    /// Writes a complete response message to the QUIC stream.
    ///
    /// This function sends the provided bytes as a response and ensures
    /// all data is flushed to the underlying transport. This is typically
    /// used for sending headers and small payloads in a single operation.
    #[instrument(skip_all)]
    async fn write_response(
        &self,
        request: Bytes,
        writer: &mut quinn::SendStream,
    ) -> ClientResult<()> {
        writer
            .write_all(&request)
            .await
            .inspect_err(|err| error!("failed to send request: {}", err))?;

        Ok(())
    }

    /// Streams data from a reader directly to the QUIC writer.
    ///
    /// This function efficiently copies all data from the provided stream
    /// to the QUIC connection using tokio's copy utility. It's designed for
    /// streaming large piece content without loading everything into memory.
    /// The operation is flushed to ensure data delivery.
    #[instrument(skip_all)]
    async fn write_stream<R: AsyncRead + Unpin + ?Sized>(
        &self,
        stream: &mut R,
        writer: &mut quinn::SendStream,
    ) -> ClientResult<()> {
        copy(stream, writer)
            .await
            .inspect_err(|err| error!("copy failed: {}", err))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::quic::NoVerifier;
    use dragonfly_client_config::dfdaemon::Config;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;

    async fn create_test_quic_server_handler() -> QUICServerHandler {
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

        QUICServerHandler {
            id_generator,
            storage,
            upload_rate_limiter,
        }
    }

    async fn create_test_server_endpoint(addr: SocketAddr) -> Endpoint {
        let (cert, key) =
            generate_simple_self_signed_certs("localhost", vec!["localhost".into()]).unwrap();
        let mut server_config = ServerConfig::with_single_cert(cert, key).unwrap();
        let transport = Arc::new(TransportConfig::default());
        server_config.transport_config(transport);
        let endpoint = Endpoint::server(server_config, addr).unwrap();
        endpoint
    }

    async fn create_test_client_endpoint(addr: SocketAddr) -> Endpoint {
        let client_config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(
                quinn::rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(NoVerifier::new())
                    .with_no_client_auth(),
            )
            .unwrap(),
        ));
        let mut endpoint = Endpoint::client(addr).unwrap();
        endpoint.set_default_client_config(client_config);
        endpoint
    }

    #[tokio::test]
    async fn test_read_header_success() {
        let handler = create_test_quic_server_handler().await;
        let server_addr = "127.0.0.1:4431".parse().unwrap();
        let endpoint = create_test_server_endpoint(server_addr).await;

        let server_handle = tokio::spawn(async move {
            let conn = endpoint.accept().await.unwrap().await.unwrap();
            let (mut _send, mut recv) = conn.accept_bi().await.unwrap();
            handler.read_header(&mut recv).await
        });
        let client_addr = "127.0.0.1:44331".parse().unwrap();
        let client_endpoint = create_test_client_endpoint(client_addr).await;

        let client_conn = client_endpoint
            .connect(server_addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (mut send, _recv) = client_conn.open_bi().await.unwrap();

        let header = Header::new(Tag::DownloadPiece, 100);
        let header_bytes: Bytes = header.into();
        send.write_all(&header_bytes).await.unwrap();
        send.finish().unwrap();

        let result = server_handle.await.unwrap();
        assert!(result.is_ok());
        if let Ok(header) = result {
            assert_eq!(header.tag(), Tag::DownloadPiece);
            assert_eq!(header.length(), 100);
        }
    }

    #[tokio::test]
    async fn test_read_header_insufficient_data() {
        let handler = create_test_quic_server_handler().await;
        let server_addr = "127.0.0.1:4432".parse().unwrap();
        let endpoint = create_test_server_endpoint(server_addr).await;

        let server_handle = tokio::spawn(async move {
            let conn = endpoint.accept().await.unwrap().await.unwrap();
            let (mut _send, mut recv) = conn.accept_bi().await.unwrap();
            handler.read_header(&mut recv).await
        });

        let client_addr = "127.0.0.1:44332".parse().unwrap();
        let client_endpoint = create_test_client_endpoint(client_addr).await;
        let client_conn = client_endpoint
            .connect(server_addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (mut send, _recv) = client_conn.open_bi().await.unwrap();

        let partial_data = vec![0u8; HEADER_SIZE - 1];
        send.write_all(&partial_data).await.unwrap();
        send.finish().unwrap();

        let result = server_handle.await.unwrap();
        assert!(result.is_err());
    }

    const HEADER_LENGTH: usize = 68;

    #[tokio::test]
    async fn test_read_download_piece_success() {
        let handler = create_test_quic_server_handler().await;
        let server_addr = "127.0.0.1:4433".parse().unwrap();
        let endpoint = create_test_server_endpoint(server_addr).await;

        let task_id = "a".repeat(64);
        let piece_number = 42;

        let server_handle = tokio::spawn(async move {
            let conn = endpoint.accept().await.unwrap().await.unwrap();
            let (mut _send, mut recv) = conn.accept_bi().await.unwrap();
            handler
                .read_download_piece::<DownloadPiece>(&mut recv, HEADER_LENGTH)
                .await
        });

        let client_addr = "127.0.0.1:44333".parse().unwrap();
        let client_endpoint = create_test_client_endpoint(client_addr).await;
        let client_conn = client_endpoint
            .connect(server_addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (mut send, _recv) = client_conn.open_bi().await.unwrap();

        let download_piece = DownloadPiece::new(task_id.clone(), piece_number);
        let bytes: Bytes = download_piece.into();
        send.write_all(&bytes).await.unwrap();
        send.finish().unwrap();

        let result: ClientResult<DownloadPiece> = server_handle.await.unwrap();
        assert!(result.is_ok());
        if let Ok(download_piece) = result {
            assert_eq!(download_piece.task_id(), task_id);
            assert_eq!(download_piece.piece_number(), piece_number);
        }
    }

    #[tokio::test]
    async fn test_read_download_piece_insufficient_data() {
        let handler = create_test_quic_server_handler().await;
        let server_addr = "127.0.0.1:4434".parse().unwrap();
        let endpoint = create_test_server_endpoint(server_addr).await;

        let server_handle = tokio::spawn(async move {
            let conn = endpoint.accept().await.unwrap().await.unwrap();
            let (mut _send, mut recv) = conn.accept_bi().await.unwrap();
            handler
                .read_download_piece::<DownloadPiece>(&mut recv, HEADER_LENGTH)
                .await
        });

        let client_addr = "127.0.0.1:44334".parse().unwrap();
        let client_endpoint = create_test_client_endpoint(client_addr).await;
        let client_conn = client_endpoint
            .connect(server_addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (mut send, _recv) = client_conn.open_bi().await.unwrap();

        let partial_data = vec![0u8; HEADER_LENGTH - 1];
        send.write_all(&partial_data).await.unwrap();
        send.finish().unwrap();

        let result = server_handle.await.unwrap();
        assert!(result.is_err());
    }
}
