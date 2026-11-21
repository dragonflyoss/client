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

//! The mutual TLS configuration of the piece transport between peers over tcp
//! and quic, enabled by `storage.server.mtls`.

use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use dragonfly_client_util::tls::{generate_cert_from_pem, load_key_from_pem};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::crypto::{aws_lc_rs, CryptoProvider};
use rustls::server::WebPkiClientVerifier;
use rustls::{CertificateError, DigitallySignedStruct, RootCertStore, SignatureScheme};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The server name the piece downloaders connect with. The peers are dialed
/// by ip, so the name of the certificate of the parent is not checked.
pub const SERVER_NAME: &str = "d7y";

/// Returns the TLS configuration of the storage servers, which present the
/// `upload.server` certificate and require a client certificate issued by its
/// CA, or none if mutual TLS is disabled.
pub fn server_config(config: &Config) -> ClientResult<Option<Arc<rustls::ServerConfig>>> {
    if !config.storage.server.mtls {
        return Ok(None);
    }

    let server = &config.upload.server;
    let (ca_cert, cert, key) = required_paths(
        "upload.server",
        server.ca_cert.as_ref(),
        server.cert.as_ref(),
        server.key.as_ref(),
    )?;

    let provider = provider();
    let client_verifier =
        WebPkiClientVerifier::builder_with_provider(root_store(ca_cert)?, provider.clone())
            .build()
            .map_err(tls_error)?;

    let mut server_config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(tls_error)?
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(generate_cert_from_pem(cert)?, load_key(key)?)
        .map_err(tls_error)?;

    // The connections are long-lived and the session resumption is not
    // used, so no session tickets are sent.
    server_config.send_tls13_tickets = 0;
    Ok(Some(Arc::new(server_config)))
}

/// Returns the TLS configuration of the piece downloaders, which present the
/// `upload.client` certificate and verify the certificate of the parent
/// against its CA, or none if mutual TLS is disabled.
pub fn client_config(config: &Config) -> ClientResult<Option<Arc<rustls::ClientConfig>>> {
    if !config.storage.server.mtls {
        return Ok(None);
    }

    let client = &config.upload.client;
    let (ca_cert, cert, key) = required_paths(
        "upload.client",
        client.ca_cert.as_ref(),
        client.cert.as_ref(),
        client.key.as_ref(),
    )?;

    let provider = provider();
    let server_verifier = CAServerCertVerifier::new(root_store(ca_cert)?, provider.clone())?;
    let client_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(tls_error)?
        .dangerous()
        .with_custom_certificate_verifier(server_verifier)
        .with_client_auth_cert(generate_cert_from_pem(cert)?, load_key(key)?)
        .map_err(tls_error)?;

    Ok(Some(Arc::new(client_config)))
}

/// Returns the crypto provider of the piece transport.
fn provider() -> Arc<CryptoProvider> {
    Arc::new(aws_lc_rs::default_provider())
}

/// Returns the CA cert, cert and key paths, which are all required for mutual
/// TLS.
fn required_paths<'a>(
    section: &str,
    ca_cert: Option<&'a PathBuf>,
    cert: Option<&'a PathBuf>,
    key: Option<&'a PathBuf>,
) -> ClientResult<(&'a PathBuf, &'a PathBuf, &'a PathBuf)> {
    match (ca_cert, cert, key) {
        (Some(ca_cert), Some(cert), Some(key)) => Ok((ca_cert, cert, key)),
        _ => Err(ClientError::Unknown(format!(
            "storage.server.mtls requires {section}.caCert, {section}.cert and {section}.key"
        ))),
    }
}

/// Returns the root cert store holding the CA certificates of the PEM file.
fn root_store(ca_cert: &PathBuf) -> ClientResult<Arc<RootCertStore>> {
    let mut roots = RootCertStore::empty();
    for cert in generate_cert_from_pem(ca_cert)? {
        roots.add(cert).map_err(tls_error)?;
    }

    if roots.is_empty() {
        return Err(ClientError::Unknown(format!(
            "no CA certificate found in {}",
            ca_cert.display()
        )));
    }

    Ok(Arc::new(roots))
}

/// Loads the private key of the PEM file.
fn load_key(key: &Path) -> ClientResult<PrivateKeyDer<'static>> {
    let key_pem = std::fs::read_to_string(key)?;
    load_key_from_pem(&key_pem)
}

/// Converts a rustls error to a client error.
fn tls_error(err: impl std::fmt::Display) -> ClientError {
    ClientError::Unknown(format!("invalid piece transport tls config: {err}"))
}

/// Verifies the certificate of the parent against the CA, including its
/// validity period, chain and signatures, without checking its name. The
/// peers are dialed by ip and every node holding a certificate issued by the
/// CA is a member of the cluster.
#[derive(Debug)]
struct CAServerCertVerifier(Arc<WebPkiServerVerifier>);

/// Implements the CAServerCertVerifier.
impl CAServerCertVerifier {
    /// Creates a new CAServerCertVerifier with the CA certificates.
    fn new(roots: Arc<RootCertStore>, provider: Arc<CryptoProvider>) -> ClientResult<Arc<Self>> {
        let inner = WebPkiServerVerifier::builder_with_provider(roots, provider)
            .build()
            .map_err(tls_error)?;
        Ok(Arc::new(Self(inner)))
    }
}

/// Implements the ServerCertVerifier for CAServerCertVerifier.
impl ServerCertVerifier for CAServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            // The name is checked after the chain, so a name mismatch means
            // the certificate is otherwise valid.
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            result => result,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    #![allow(clippy::type_complexity)]

    use super::*;
    use rcgen::{BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa};
    use std::net::{IpAddr, Ipv4Addr};

    /// Generates a CA certificate.
    fn new_ca() -> Certificate {
        let mut params = CertificateParams::new(vec![]);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        Certificate::from_params(params).unwrap()
    }

    /// Writes a certificate signed by the CA and its key to the directory, and
    /// returns their paths. The certificate is issued for a name the peers
    /// are not dialed with.
    fn issue(
        dir: &Path,
        name: &str,
        ca: &Certificate,
        usage: ExtendedKeyUsagePurpose,
    ) -> (PathBuf, PathBuf) {
        let mut params = CertificateParams::new(vec!["peer.example.com".to_string()]);
        params.extended_key_usages = vec![usage];
        let cert = Certificate::from_params(params).unwrap();

        let cert_path = dir.join(format!("{name}.crt"));
        let key_path = dir.join(format!("{name}.key"));
        std::fs::write(&cert_path, cert.serialize_pem_with_signer(ca).unwrap()).unwrap();
        std::fs::write(&key_path, cert.serialize_private_key_pem()).unwrap();
        (cert_path, key_path)
    }

    /// Writes the CA certificate to the directory and returns its path.
    fn write_ca(dir: &Path, name: &str, ca: &Certificate) -> PathBuf {
        let path = dir.join(format!("{name}.crt"));
        std::fs::write(&path, ca.serialize_pem().unwrap()).unwrap();
        path
    }

    /// The configurations of the nodes of a cluster with mutual TLS.
    pub(crate) struct Cluster {
        /// The configuration of a node with certificates issued by the CA of
        /// the cluster.
        pub member: Config,

        /// The configuration of a node trusting the CA of the cluster, with a
        /// client certificate issued by another CA.
        pub outsider: Config,

        /// The configuration of a node with a valid client certificate, but
        /// trusting another CA for the servers.
        pub distrusting: Config,
    }

    /// Generates the certificates of a cluster with mutual TLS in the directory.
    pub(crate) fn cluster(dir: &Path) -> Cluster {
        let ca = new_ca();
        let other_ca = new_ca();
        let ca_path = write_ca(dir, "ca", &ca);
        let other_ca_path = write_ca(dir, "other-ca", &other_ca);
        let (server_cert, server_key) =
            issue(dir, "server", &ca, ExtendedKeyUsagePurpose::ServerAuth);
        let (client_cert, client_key) =
            issue(dir, "client", &ca, ExtendedKeyUsagePurpose::ClientAuth);
        let (other_client_cert, other_client_key) = issue(
            dir,
            "other-client",
            &other_ca,
            ExtendedKeyUsagePurpose::ClientAuth,
        );

        let mut member = Config::default();
        member.storage.server.ip = Some(IpAddr::V4(Ipv4Addr::LOCALHOST));
        member.storage.server.mtls = true;
        member.upload.server.ca_cert = Some(ca_path.clone());
        member.upload.server.cert = Some(server_cert);
        member.upload.server.key = Some(server_key);
        member.upload.client.ca_cert = Some(ca_path);
        member.upload.client.cert = Some(client_cert.clone());
        member.upload.client.key = Some(client_key.clone());

        let mut outsider = member.clone();
        outsider.upload.client.cert = Some(other_client_cert);
        outsider.upload.client.key = Some(other_client_key);

        let mut distrusting = member.clone();
        distrusting.upload.client.ca_cert = Some(other_ca_path);

        Cluster {
            member,
            outsider,
            distrusting,
        }
    }

    #[test]
    fn configs_are_none_when_mtls_is_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = cluster(dir.path()).member;
        config.storage.server.mtls = false;

        assert!(server_config(&config).unwrap().is_none());
        assert!(client_config(&config).unwrap().is_none());
    }

    #[test]
    fn configs_load_the_certificates() {
        let dir = tempfile::tempdir().unwrap();
        let config = cluster(dir.path()).member;

        assert!(server_config(&config).unwrap().is_some());
        assert!(client_config(&config).unwrap().is_some());
    }

    #[test]
    fn configs_fail_on_missing_or_invalid_certificates() {
        let dir = tempfile::tempdir().unwrap();
        let member = cluster(dir.path()).member;
        let test_cases: Vec<(fn(&mut Config), bool, bool)> = vec![
            (|config| config.upload.server.key = None, false, true),
            (|config| config.upload.client.ca_cert = None, true, false),
            (
                |config| config.upload.server.cert = Some(PathBuf::from("/nonexistent.crt")),
                false,
                true,
            ),
            // A CA file without certificates.
            (
                |config| config.upload.client.ca_cert = config.upload.client.key.clone(),
                true,
                false,
            ),
        ];

        for (mutate, server_ok, client_ok) in test_cases {
            let mut config = member.clone();
            mutate(&mut config);
            assert_eq!(server_config(&config).is_ok(), server_ok);
            assert_eq!(client_config(&config).is_ok(), client_ok);
        }
    }
}
