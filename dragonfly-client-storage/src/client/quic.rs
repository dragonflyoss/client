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

use crate::client::Client;
use bytes::Bytes;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error as ClientError, Result as ClientResult,
};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{AckFrequencyConfig, ClientConfig, Endpoint, TransportConfig};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{error, instrument};

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
}

#[tonic::async_trait]
impl Client for QUICClient {
    /// Establishes QUIC connection and writes a vortex protocol request.
    ///
    /// This is a low-level utility function that handles the QUIC connection
    /// lifecycle and request transmission. It ensures proper error handling
    /// and connection cleanup.
    #[instrument(skip_all)]
    async fn connect_and_write_request(
        &self,
        request: Bytes,
    ) -> ClientResult<(
        Box<dyn AsyncRead + Send + Unpin>,
        Box<dyn AsyncWrite + Send + Unpin>,
    )> {
        let mut client_config = ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(
                quinn::rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(NoVerifier::new())
                    .with_no_client_auth(),
            )
            .map_err(|err| {
                ClientError::Unknown(format!("failed to create quic client config: {}", err))
            })?,
        ));

        let mut transport = TransportConfig::default();
        transport.keep_alive_interval(Some(super::DEFAULT_KEEPALIVE_INTERVAL));
        transport.max_idle_timeout(Some(super::DEFAULT_MAX_IDLE_TIMEOUT.try_into().unwrap()));
        transport.ack_frequency_config(Some(AckFrequencyConfig::default()));
        transport.send_window(super::DEFAULT_SEND_BUFFER_SIZE as u64);
        transport.receive_window((super::DEFAULT_RECV_BUFFER_SIZE as u32).into());
        transport.stream_receive_window((super::DEFAULT_RECV_BUFFER_SIZE as u32).into());
        client_config.transport_config(Arc::new(transport));

        // Port is zero to let the OS assign an ephemeral port.
        let mut endpoint =
            Endpoint::client(SocketAddr::new(self.config.storage.server.ip.unwrap(), 0))?;
        endpoint.set_default_client_config(client_config);

        // Connect's server name used for verifying the certificate. Since we used
        // NoVerifier, it can be anything.
        let connection = endpoint
            .connect(self.addr.parse().or_err(ErrorType::ParseError)?, "d7y")?
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

        Ok((Box::new(reader), Box::new(writer)))
    }

    /// Access to client configuration.
    fn config(&self) -> &Arc<Config> {
        &self.config
    }

    /// Access to client address.
    fn addr(&self) -> &str {
        &self.addr
    }
}

/// NoVerifier is a verifier for QUIC Client that does not verify the server certificate.
/// It is used for testing and should not be used in production.
#[derive(Debug)]
struct NoVerifier(Arc<quinn::rustls::crypto::CryptoProvider>);

/// NoVerifier implements a no-op server certificate verifier.
impl NoVerifier {
    /// Creates a new NoVerifier instance.
    pub fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(
            quinn::rustls::crypto::ring::default_provider(),
        )))
    }
}

/// NoVerifier implements the ServerCertVerifier trait to skip certificate verification.
impl quinn::rustls::client::danger::ServerCertVerifier for NoVerifier {
    /// Verifies the server certificate.
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

    /// Verifies a TLS 1.2 signature.
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

    /// Verifies a TLS 1.3 signature.
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

    /// Returns the supported signature schemes.
    fn supported_verify_schemes(&self) -> Vec<quinn::rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
