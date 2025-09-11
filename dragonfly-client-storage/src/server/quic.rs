use crate::Storage;
use bytes::Bytes;
use chrono::Utc;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use dragonfly_client_util::{id_generator::IDGenerator, shutdown};
use leaky_bucket::RateLimiter;
use quinn::{Connection, Endpoint, RecvStream, SendStream, ServerConfig};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio::sync::{mpsc, Barrier};
use tracing::{error, info, Span};
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

// QUICServer replaces the original TCPServer
pub struct QUICServer {
    addr: SocketAddr,
    handler: QUICServerHandler,
    shutdown: shutdown::Shutdown,
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

impl QUICServer {
    pub fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        addr: SocketAddr,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        let handler = QUICServerHandler {
            id_generator,
            storage,
            upload_rate_limiter: Arc::new(
                RateLimiter::builder()
                    .initial(config.upload.rate_limit.as_u64() as usize)
                    .refill(config.upload.rate_limit.as_u64() as usize)
                    .max(config.upload.rate_limit.as_u64() as usize)
                    .interval(Duration::from_secs(1))
                    .fair(false)
                    .build(),
            ),
            read_buffer_size: config.storage.read_buffer_size,
            write_buffer_size: config.storage.write_buffer_size,
        };

        Self {
            addr,
            handler,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }
}

impl QUICServer {
    pub async fn run(&mut self, quic_server_started_barrier: Arc<Barrier>) -> ClientResult<()> {
        // Setup server configuration
        let certs = vec![]; // Replace with your certificate chain in PEM format
        let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(vec![])); // Replace with your private key in PKCS#8 DER format
        let server_config = ServerConfig::with_single_cert(certs, private_key)
            .map_err(|err| {
                error!("Failed to create server config: {}", err);
                ClientError::ValidationError("Invalid server config".to_string())
            })?;
        let endpoint =
            Endpoint::server(server_config.clone(), self.addr).map_err(|err| {
                error!("Failed to bind QUIC endpoint: {}", err);
                ClientError::HostNotFound(self.addr.to_string())
            })?;

        info!("QUIC upload server listening on {}", self.addr);

        // Notify that the server is ready
        quic_server_started_barrier.wait().await;
        info!("QUIC upload server is ready");

        loop {
            tokio::select! {
                // Accept new QUIC connections
                Some(connecting) = endpoint.accept() => {
                    let handler = self.handler.clone();
                    tokio::spawn(async move {
                        match connecting.await {
                            Ok(new_conn) => {
                                info!("New QUIC connection from {:?}", new_conn.remote_address());
                                handler.handle_connection(new_conn).await;
                            }
                            Err(err) => {
                                error!("Failed to establish QUIC connection: {}", err);
                            }
                        }
                    });
                }
                // Handle shutdown signal
                _ = self.shutdown.recv() => {
                    info!("QUIC upload server shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}

// QUICServerHandler replaces the original TCPServerHandler
#[derive(Clone)]
pub struct QUICServerHandler {
    id_generator: Arc<IDGenerator>,
    storage: Arc<Storage>,
    upload_rate_limiter: Arc<RateLimiter>,
    pub read_buffer_size: usize,
    pub write_buffer_size: usize,
}

impl QUICServerHandler {
    // Handle QUIC connection
    async fn handle_connection(&self, new_conn: Connection) {
        loop {
            match new_conn.accept_bi().await {
                Ok((mut send, mut recv)) => {
                    let handler = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handler.handle_stream(&mut send, &mut recv).await {
                            error!("Error handling QUIC stream: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept QUIC stream: {}", e);
                    break;
                }
            }
        }
    }

    // Handle a single QUIC stream
    async fn handle_stream(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Read header
        let mut header_bytes = vec![0u8; HEADER_SIZE];
        recv.read_exact(&mut header_bytes).await?;
        let header: Header = Bytes::from(header_bytes).try_into()?;
        info!("received header: {:?}", header);

        match header.tag() {
            Tag::DownloadPiece => {
                let mut download_piece_bytes = vec![0u8; header.length() as usize];
                recv.read_exact(&mut download_piece_bytes).await?;
                let download_piece: DownloadPiece = Bytes::from(download_piece_bytes).try_into()?;
                info!("received DownloadPiece request: {:?}", download_piece);

                match self.handle_piece(&download_piece).await {
                    Ok((piece_content, mut content_reader)) => {
                        let piece_content_bytes: Bytes = piece_content.clone().into();
                        let header = Header::new_piece_content(piece_content_bytes.len() as u32);
                        let header_bytes: Bytes = header.clone().into();

                        // Send header and piece_content
                        send.write_all(&header_bytes).await?;
                        send.write_all(&piece_content_bytes).await?;
                        info!("sent PieceContent: {:?}", piece_content);

                        // Send content
                        tokio::io::copy(&mut content_reader, send).await?;
                        send.finish()?;
                    }
                    Err(err) => {
                        let error_response: Bytes =
                            Vortex::Error(Header::new_error(err.len() as u32), err).into();
                        send.write_all(&error_response).await?;
                        send.finish()?;
                    }
                }
            }
            Tag::DownloadPersistentCachePiece => {
                let mut download_persistent_cache_piece_bytes = vec![0u8; header.length() as usize];
                recv.read_exact(&mut download_persistent_cache_piece_bytes)
                    .await?;
                let download_persistent_cache_piece: DownloadPersistentCachePiece =
                    Bytes::from(download_persistent_cache_piece_bytes).try_into()?;
                info!(
                    "received DownloadPersistentCachePiece request: {:?}",
                    download_persistent_cache_piece
                );

                match self
                    .handle_persistent_cache_piece(&download_persistent_cache_piece)
                    .await
                {
                    Ok((persistent_cache_piece_content, mut content_reader)) => {
                        let persistent_cache_piece_content_bytes: Bytes =
                            persistent_cache_piece_content.clone().into();
                        let header = Header::new_persistent_cache_piece_content(
                            persistent_cache_piece_content_bytes.len() as u32,
                        );
                        let header_bytes: Bytes = header.clone().into();

                        send.write_all(&header_bytes).await?;
                        send.write_all(&persistent_cache_piece_content_bytes)
                            .await?;
                        info!(
                            "sent PersistentCachePieceContent: {:?}",
                            persistent_cache_piece_content
                        );

                        tokio::io::copy(&mut content_reader, send).await?;
                        send.finish()?;
                    }
                    Err(err) => {
                        let error_response: Bytes =
                            Vortex::Error(Header::new_error(err.len() as u32), err).into();
                        send.write_all(&error_response).await?;
                        send.finish()?;
                    }
                }
            }
            _ => {
                error!("unexpected tag: {:?}", header.tag());
            }
        }
        Ok(())
    }

    /// Handles download piece request.
    async fn handle_piece(
        &self,
        request: &DownloadPiece,
    ) -> Result<(PieceContent, impl AsyncRead), Error> {
        // Generate the host id.
        let host_id = self.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id();

        // Get the interested piece number from the request.
        let piece_number = request.piece_number();

        // Generate the piece id.
        let piece_id = self.storage.piece_id(task_id, piece_number);

        // Span record the host id, task id and piece number.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id);
        Span::current().record("piece_id", piece_id.as_str());
        info!("download piece content in TCP upload server");

        // Get the piece metadata from the local storage.
        let piece = self
            .storage
            .get_piece(piece_id.as_str())
            .map_err(|err| {
                Error::new(
                    Code::InvalidArgument,
                    format!("Failed to get piece metadata: {}", err),
                )
            })?
            .ok_or_else(|| {
                error!("Piece metadata not found");
                Error::new(Code::NotFound, piece_id.clone())
            })?;

        info!("start upload piece content");

        // Span record the piece_id.
        Span::current().record("piece_id", piece_id.as_str());
        Span::current().record("piece_length", piece.length);

        // Acquire the upload rate limiter.
        self.upload_rate_limiter
            .acquire(piece.length as usize)
            .await;

        // Upload the piece content.
        let reader = self
            .storage
            .upload_piece(piece_id.as_str(), task_id, None)
            .await
            .map_err(|err| {
                error!("Failed to get piece content: {}", err);
                Error::new(Code::Internal, piece_id)
            })?;
        info!("finished upload piece content");

        // Create response
        let piece_content = PieceContent::new(
            piece_number,
            piece.offset,
            piece.length,
            piece.digest,
            piece.parent_id.unwrap_or_default(),
            1,
            Duration::from_secs(30),
            Utc::now().naive_utc(),
        );

        Ok((piece_content, reader))
    }

    /// Handles download persistent cache piece request.
    async fn handle_persistent_cache_piece(
        &self,
        request: &DownloadPersistentCachePiece,
    ) -> Result<(PersistentCachePieceContent, impl AsyncRead), Error> {
        // Generate the host id.
        let host_id = self.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id();

        // Get the interested piece number from the request.
        let piece_number = request.piece_number();

        // Generate the piece id.
        let piece_id = self.storage.piece_id(task_id, piece_number);

        // Span record the host id, task id and piece number.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id);
        Span::current().record("piece_id", piece_id.as_str());
        info!("download persistent cache piece content in TCP upload server");

        // Get the piece metadata from the local storage.
        let piece = self
            .storage
            .get_persistent_cache_piece(piece_id.as_str())
            .map_err(|err| {
                Error::new(
                    Code::InvalidArgument,
                    format!("Failed to get persistent cache piece metadata: {}", err),
                )
            })?
            .ok_or_else(|| {
                error!("Persistent cache piece metadata not found");
                Error::new(Code::NotFound, piece_id.clone())
            })?;

        info!("start upload persistent cache piece content");

        // Span record the piece_id.
        Span::current().record("piece_id", piece_id.as_str());
        Span::current().record("piece_length", piece.length);

        // Acquire the upload rate limiter.
        self.upload_rate_limiter
            .acquire(piece.length as usize)
            .await;

        // Upload the piece content.
        let reader = self
            .storage
            .upload_persistent_cache_piece(piece_id.as_str(), task_id, None)
            .await
            .map_err(|err| {
                error!("Failed to get persistent cache piece content: {}", err);
                Error::new(Code::Internal, piece_id)
            })?;
        info!("finished upload persistent cache piece content");

        // Create response
        let persistent_cache_piece_content = PersistentCachePieceContent::new(
            piece_number,
            piece.offset,
            piece.length,
            piece.digest,
            piece.parent_id.unwrap_or_default(),
            1,
            Duration::from_secs(30),
            Utc::now().naive_utc(),
        );

        Ok((persistent_cache_piece_content, reader))
    }
}
