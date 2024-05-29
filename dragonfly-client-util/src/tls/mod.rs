/*
 *     Copyright 2024 The Dragonfly Authors
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

use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use rcgen::{Certificate, CertificateParams, KeyPair};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use std::path::PathBuf;
use std::sync::Arc;
use std::vec::Vec;
use std::{fs, io};

// NoVerifier is a verifier that does not verify the server certificate.
// It is used for testing and should not be used in production.
#[derive(Debug)]
pub struct NoVerifier(Arc<rustls::crypto::CryptoProvider>);

// Implement the NoVerifier.
impl NoVerifier {
    // new creates a new NoVerifier.
    pub fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

// Implement the ServerCertVerifier trait for NoVerifier.
impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    // verify_server_cert verifies the server certificate.
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    // verify_tls12_signature verifies the TLS 1.2 signature.
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    // verify_tls13_signature verifies the TLS 1.3 signature.
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    // supported_verify_schemes returns the supported signature schemes.
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

// Generate a CA certificate from PEM format files.
// Generate CA by openssl with PEM format files:
// openssl req -x509 -sha256 -days 36500 -nodes -newkey rsa:4096 -keyout ca.key -out ca.crt
pub fn generate_ca_cert_from_pem(
    ca_cert_path: &PathBuf,
    ca_key_path: &PathBuf,
) -> ClientResult<Certificate> {
    // Load CA certificate and key with PEM format.
    let ca_cert_pem = fs::read(ca_cert_path)?;
    let ca_cert_pem = std::str::from_utf8(&ca_cert_pem)?;
    let ca_key_pem = fs::read(ca_key_path)?;
    let ca_key_pem = std::str::from_utf8(&ca_key_pem)?;

    // Parse CA certificate and key.
    let key_pair = KeyPair::from_pem(ca_key_pem).or_err(ErrorType::CertificateError)?;
    let ca_params = CertificateParams::from_ca_cert_pem(ca_cert_pem, key_pair)
        .or_err(ErrorType::CertificateError)?;
    let ca_cert = Certificate::from_params(ca_params).or_err(ErrorType::CertificateError)?;

    Ok(ca_cert)
}

// Generate certificates from PEM format files.
pub fn generate_certs_from_pem(cert_path: &PathBuf) -> ClientResult<Vec<CertificateDer<'static>>> {
    let f = fs::File::open(cert_path)?;
    let mut certs_pem_reader = io::BufReader::new(f);
    let certs = rustls_pemfile::certs(&mut certs_pem_reader).collect::<Result<Vec<_>, _>>()?;
    Ok(certs)
}

// generate_self_signed_certs_by_ca_cert generates a self-signed certificates
// by given subject alternative names with CA certificate.
pub fn generate_self_signed_certs_by_ca_cert(
    ca_cert: &Certificate,
    subject_alt_names: Vec<String>,
) -> ClientResult<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    // Sign certificate with CA certificate by given subject alternative names.
    let params = CertificateParams::new(subject_alt_names);
    let cert = Certificate::from_params(params).or_err(ErrorType::CertificateError)?;
    let cert_pem = cert
        .serialize_pem_with_signer(ca_cert)
        .or_err(ErrorType::CertificateError)?;
    let key_pem = cert.serialize_private_key_pem();

    // Parse certificate.
    let mut cert_pem_reader = io::BufReader::new(cert_pem.as_bytes());
    let certs = rustls_pemfile::certs(&mut cert_pem_reader).collect::<Result<Vec<_>, _>>()?;

    // Parse private key.
    let mut key_pem_reader = io::BufReader::new(key_pem.as_bytes());
    let key = rustls_pemfile::private_key(&mut key_pem_reader)?
        .ok_or_else(|| ClientError::Unknown("failed to load private key".to_string()))?;

    Ok((certs, key))
}

// generate_simple_self_signed_certs generates a simple self-signed certificates
pub fn generate_simple_self_signed_certs(
    subject_alt_names: impl Into<Vec<String>>,
) -> ClientResult<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(subject_alt_names)
        .or_err(ErrorType::CertificateError)?;
    let key = rustls_pki_types::PrivateKeyDer::Pkcs8(cert.serialize_private_key_der().into());
    let certs = vec![cert
        .serialize_der()
        .or_err(ErrorType::CertificateError)?
        .into()];

    Ok((certs, key))
}

// certs_to_raw_certs converts DER format of the certificates to raw certificates.
pub fn certs_to_raw_certs(certs: Vec<CertificateDer<'static>>) -> Vec<Vec<u8>> {
    certs
        .into_iter()
        .map(|cert| cert.as_ref().to_vec())
        .collect()
}

// raw_certs_to_certs converts raw certificates to DER format of certificates.
pub fn raw_certs_to_certs(raw_certs: Vec<Vec<u8>>) -> Vec<CertificateDer<'static>> {
    raw_certs.into_iter().map(|cert| cert.into()).collect()
}
