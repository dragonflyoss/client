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
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, Endpoint, RecvStream, SendStream};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::io::AsyncRead;
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

/// QUICClient is a QUIC-based client for quic storage service.
#[derive(Clone)]
pub struct QUICClient {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// addr is the address of the QUIC server.
    addr: String,
}

/// QUICClient implements the QUIC-based client for quic storage service.
impl QUICClient {
    /// Creates a new QUICClient instance.
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
    /// 2. Establishes QUIC connection and sends the request.
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
                self.read_piece_content(&mut reader, persistent_cache_piece_content::METADATA_LENGTH_SIZE)
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

    /// Establishes QUIC connection and writes a vortex protocol request.
    ///
    /// This is a low-level utility function that handles the QUIC connection
    /// lifecycle and request transmission. It ensures proper error handling
    /// and connection cleanup.
    #[instrument(skip_all)]
    async fn connect_and_write_request(
        &self,
        request: Bytes,
    ) -> ClientResult<(RecvStream, SendStream)> {
        let crypto = quinn::rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(NoVerifier::new())
            .with_no_client_auth();

        let client_config =
            ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto).map_err(
                |err| ClientError::Unknown(format!("failed to create quic client config: {}", err)),
            )?));

        let endpoint = {
            let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
            let mut endpoint = Endpoint::client(bind_addr)?;
            endpoint.set_default_client_config(client_config);
            endpoint
        };

        let remote_addr: SocketAddr = self.addr.parse().unwrap();
        let connection = endpoint
            .connect(remote_addr, "quic")?
            .await
            .inspect_err(|err| error!("failed to connect to {}: {}", self.addr, err))?;

        let (mut writer, reader) = connection
            .open_bi()
            .await
            .inspect_err(|err| error!("failed to open bi stream: {}", err))?;

        writer
            .write_all(&request)
            .await
            .inspect_err(|err| error!("failed to send request: {}", err))?;

        Ok((reader, writer))
    }

    /// Reads and parses a vortex protocol header from the QUIC stream.
    ///
    /// The header contains metadata about the following message, including
    /// the message type (tag) and payload length. This is critical for
    /// proper protocol message framing.
    #[instrument(skip_all)]
    async fn read_header(&self, reader: &mut RecvStream) -> ClientResult<Header> {
        let mut header_bytes = BytesMut::with_capacity(HEADER_SIZE);
        header_bytes.resize(HEADER_SIZE, 0);
        reader
            .read_exact(&mut header_bytes)
            .await
            .inspect_err(|err| error!("failed to receive header: {}", err))?;

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
        reader: &mut RecvStream,
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
            .inspect_err(|err| error!("failed to receive metadata length: {}", err))?;
        let metadata_length = u32::from_be_bytes(metadata_length_bytes[..].try_into()?) as usize;

        let mut metadata_bytes = BytesMut::with_capacity(metadata_length);
        metadata_bytes.resize(metadata_length, 0);
        reader
            .read_exact(&mut metadata_bytes)
            .await
            .inspect_err(|err| error!("failed to receive metadata: {}", err))?;

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
    async fn read_error(&self, reader: &mut RecvStream, header_length: usize) -> ClientError {
        let mut error_bytes = BytesMut::with_capacity(header_length);
        error_bytes.resize(header_length, 0);
        if let Err(err) = reader.read_exact(&mut error_bytes).await {
            error!("failed to receive error: {}", err);
            return ClientError::Unknown(err.to_string());
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

/// NoVerifier is a verifier for QUIC Client that does not verify the server certificate.
/// It is used for testing and should not be used in production.
#[derive(Debug)]
struct NoVerifier(Arc<quinn::rustls::crypto::CryptoProvider>);

impl NoVerifier {
    pub fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(
            quinn::rustls::crypto::ring::default_provider(),
        )))
    }
}

impl quinn::rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<quinn::rustls::client::danger::ServerCertVerified, quinn::rustls::Error> {
        Ok(quinn::rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &quinn::rustls::DigitallySignedStruct,
    ) -> Result<quinn::rustls::client::danger::HandshakeSignatureValid, quinn::rustls::Error> {
        quinn::rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &quinn::rustls::DigitallySignedStruct,
    ) -> Result<quinn::rustls::client::danger::HandshakeSignatureValid, quinn::rustls::Error> {
        quinn::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<quinn::rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
