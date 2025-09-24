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
use lazy_static::lazy_static;
use lru::LruCache;
use rcgen::{Certificate, CertificateParams, KeyPair};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::vec::Vec;
use std::{fs, io};
use tracing::instrument;

/// DEFAULT_CERTS_CACHE_CAPACITY is the default capacity of the certificates cache.
const DEFAULT_CERTS_CACHE_CAPACITY: usize = 1000;

/// CertKeyPair is the type of the certificate and private key pair.
type CertKeyPair = (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>);

lazy_static! {
    /// SELF_SIGNED_CERTS is a map that stores the self-signed certificates to avoid
    /// generating the same certificates multiple times.
    static ref SELF_SIGNED_CERTS: Arc<Mutex<LruCache<String, CertKeyPair>>> =
        Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(DEFAULT_CERTS_CACHE_CAPACITY).unwrap())));

    /// SIMPLE_SELF_SIGNED_CERTS is a map that stores the simple self-signed certificates to avoid
    /// generating the same certificates multiple times.
    static ref SIMPLE_SELF_SIGNED_CERTS: Arc<Mutex<LruCache<String, CertKeyPair>>> =
        Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(DEFAULT_CERTS_CACHE_CAPACITY).unwrap())));
}

/// NoVerifier is a verifier that does not verify the server certificate.
/// It is used for testing and should not be used in production.
#[derive(Debug)]
pub struct NoVerifier(Arc<rustls::crypto::CryptoProvider>);

/// Implement the NoVerifier.
impl NoVerifier {
    /// new creates a new NoVerifier.
    pub fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

/// Implement the ServerCertVerifier trait for NoVerifier.
impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    /// verify_server_cert verifies the server certificate.
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

    /// verify_tls12_signature verifies the TLS 1.2 signature.
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

    /// verify_tls13_signature verifies the TLS 1.3 signature.
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

    /// supported_verify_schemes returns the supported signature schemes.
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// SkipServerVerification is a verifier for QUIC Client that does not verify the server certificate.
/// It is used for testing and should not be used in production.
#[derive(Debug)]
pub struct SkipServerVerification(Arc<quinn::rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    pub fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(
            quinn::rustls::crypto::ring::default_provider(),
        )))
    }
}

impl quinn::rustls::client::danger::ServerCertVerifier for SkipServerVerification {
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

/// Generate a CA certificate from PEM format files.
/// Generate CA by openssl with PEM format files:
/// openssl req -x509 -sha256 -days 36500 -nodes -newkey rsa:4096 -keyout ca.key -out ca.crt
#[instrument(skip_all)]
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

/// Generate certificates from PEM format files.
#[instrument(skip_all)]
pub fn generate_cert_from_pem(cert_path: &PathBuf) -> ClientResult<Vec<CertificateDer<'static>>> {
    let f = fs::File::open(cert_path)?;
    let mut certs_pem_reader = io::BufReader::new(f);
    let certs = rustls_pemfile::certs(&mut certs_pem_reader).collect::<Result<Vec<_>, _>>()?;
    Ok(certs)
}

/// generate_self_signed_certs_by_ca_cert generates a self-signed certificates
/// by given subject alternative names with CA certificate.
#[instrument(skip_all)]
pub fn generate_self_signed_certs_by_ca_cert(
    ca_cert: &Certificate,
    host: &str,
    subject_alt_names: Vec<String>,
) -> ClientResult<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let mut cache = SELF_SIGNED_CERTS.lock().unwrap();
    if let Some((certs, key)) = cache.get(host) {
        return Ok((certs.clone(), key.clone_key()));
    };
    drop(cache);

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

    let mut cache = SELF_SIGNED_CERTS.lock().unwrap();
    cache.push(host.to_string(), (certs.clone(), key.clone_key()));
    Ok((certs, key))
}

/// generate_simple_self_signed_certs generates a simple self-signed certificates
#[instrument(skip_all)]
pub fn generate_simple_self_signed_certs(
    host: &str,
    subject_alt_names: impl Into<Vec<String>>,
) -> ClientResult<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let mut cache = SIMPLE_SELF_SIGNED_CERTS.lock().unwrap();
    if let Some((certs, key)) = cache.get(host) {
        return Ok((certs.clone(), key.clone_key()));
    };
    drop(cache);

    let cert = rcgen::generate_simple_self_signed(subject_alt_names)
        .or_err(ErrorType::CertificateError)?;
    let key = rustls_pki_types::PrivateKeyDer::Pkcs8(cert.serialize_private_key_der().into());
    let certs = vec![cert
        .serialize_der()
        .or_err(ErrorType::CertificateError)?
        .into()];

    let mut cache = SIMPLE_SELF_SIGNED_CERTS.lock().unwrap();
    cache.push(host.to_string(), (certs.clone(), key.clone_key()));
    Ok((certs, key))
}

/// certs_to_raw_certs converts DER format of the certificates to raw certificates.
#[instrument(skip_all)]
pub fn certs_to_raw_certs(certs: Vec<CertificateDer<'static>>) -> Vec<Vec<u8>> {
    certs
        .into_iter()
        .map(|cert| cert.as_ref().to_vec())
        .collect()
}

/// raw_certs_to_certs converts raw certificates to DER format of certificates.
#[instrument(skip_all)]
pub fn raw_certs_to_certs(raw_certs: Vec<Vec<u8>>) -> Vec<CertificateDer<'static>> {
    raw_certs.into_iter().map(|cert| cert.into()).collect()
}

/// load_certs_from_pem loads certificates from PEM format string.
#[instrument(skip_all)]
pub fn load_certs_from_pem(cert_pem: &str) -> ClientResult<Vec<CertificateDer<'static>>> {
    let certs = rustls_pemfile::certs(&mut cert_pem.as_bytes()).collect::<Result<Vec<_>, _>>()?;

    Ok(certs)
}

/// load_key_from_pem loads private key from PEM format string.
#[instrument(skip_all)]
pub fn load_key_from_pem(key_pem: &str) -> ClientResult<PrivateKeyDer<'static>> {
    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())?
        .ok_or_else(|| ClientError::Unknown("failed to load private key".to_string()))?;

    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::client::danger::ServerCertVerifier;
    use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Generate the certificate and private key by script(`scripts/generate_certs.sh`).
    const SERVER_CERT: &str = r#"""
-----BEGIN CERTIFICATE-----
MIIDsDCCApigAwIBAgIUWuckNOpaPERz+QMACyqCqFJwYIYwDQYJKoZIhvcNAQEL
BQAwYjELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0Jl
aWppbmcxEDAOBgNVBAoMB1Rlc3QgQ0ExCzAJBgNVBAsMAklUMRAwDgYDVQQDDAdU
ZXN0IENBMB4XDTI0MTAxMTEyMTEwN1oXDTI2MDIyMzEyMTEwN1owaDELMAkGA1UE
BhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0JlaWppbmcxFDASBgNV
BAoMC1Rlc3QgU2VydmVyMQswCQYDVQQLDAJJVDESMBAGA1UEAwwJbG9jYWxob3N0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAiA9wEge3Jq8qw8Ix9z6t
ss7ttK/49TMddhnQuqoYrFKjYliuvfbRZOU1nBP7+5XSAliPDCRNPS17JSwsXJk2
bstc69fruDpYmthualSTsUYSwJJqzJjy5mlwSPtBsombcSHrUasMce5C4iXJX8Wx
1O8ZCwuI5LUKxLujt+ZWnYfp5lzDcDhgD6wIzcMk67jv2edcWhqGkKmQbbmmK3Ve
DJRa56NCh0F2U1SW0KCXTzoC1YU/bbB4UCfvHouMzCRNTr3VcrfL5aBIn/z/f6Xt
atQkqFa/T1/lOQ0miMqNyBW58NxkPsTaJm2kVZ21hF2Dvo8MU/8Ras0J0aL8sc4n
LwIDAQABo1gwVjAUBgNVHREEDTALgglsb2NhbGhvc3QwHQYDVR0OBBYEFJP+jy8a
tCfnu6nekyZugvq8XT2gMB8GA1UdIwQYMBaAFOwXKq7J6STkwLUWC1xKwq1Psy63
MA0GCSqGSIb3DQEBCwUAA4IBAQCu8nqnuzNn3E9dNC8ptV7ga1zb7cGdL3ZT5W3d
10gmPo3YijWoCj4snattX9zxI8ThAY7uX6jrR0/HRXGJIw5JnlBmykdgyrQYEDzU
FUL0GGabJNxZ+zDV77P+3WdgCx3F7wLQk+x+etMPvYuWC8RMse7W6dB1INyMT/l6
k1rV73KTupSNJrYhqw0RnmNHIctkwiZLLpzLFj91BHjK5ero7VV4s7vnx+gtO/zQ
FnIyiyfYYcSpVMhhaNkeCtWOfgVYU/m4XXn5bwEOhMN6q0JcdBPnT6kd2otLhiIo
/WeyWEUeZ4rQhS7C1i31AYtNtVnnvI7BrsI4czYdcJcj3CM+
-----END CERTIFICATE-----
"""#;

    const SERVER_KEY: &str = r#"""
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCID3ASB7cmryrD
wjH3Pq2yzu20r/j1Mx12GdC6qhisUqNiWK699tFk5TWcE/v7ldICWI8MJE09LXsl
LCxcmTZuy1zr1+u4Olia2G5qVJOxRhLAkmrMmPLmaXBI+0GyiZtxIetRqwxx7kLi
JclfxbHU7xkLC4jktQrEu6O35ladh+nmXMNwOGAPrAjNwyTruO/Z51xaGoaQqZBt
uaYrdV4MlFrno0KHQXZTVJbQoJdPOgLVhT9tsHhQJ+8ei4zMJE1OvdVyt8vloEif
/P9/pe1q1CSoVr9PX+U5DSaIyo3IFbnw3GQ+xNombaRVnbWEXYO+jwxT/xFqzQnR
ovyxzicvAgMBAAECggEABqHVkTfe1p+PBGx34tG/4nQxwIRxLJG31no+jeAdYOLF
AEeulqezbmIroyTMA0uQKWscy0V/gXUi3avHOOktp72Vv9fxy98F/fyBPx3YEvLa
69DMnl0qPl06CvLlTey6km8RKxUrRq9S2NoTydD+m1fC9jCIhvHkrNExIXjtaewU
PvAHJy4ho+hVLo40udmQ4i1gnEWYUtjkr65ujuOAlWrlScHGvOrATbrfcaufPi/S
5A/h8UlfahBstmh3a2tBLZlNl82s5ZKsVM1Oq1Vk9hAX5DP2JBAmuZKgX/xSDdpR
62VUQGqp1WLgble5vR6ZUFo5+Jiw1uxe9jmNUg9mMQKBgQC8giG3DeeU6+rX9LVz
cklF4jioU5LMdYutwXbtuGIWgXeJo8r0fzrgBtBVGRn7anS7YnYA+67h+A8SC6MO
SXvktpHIC3Egge2Q9dRrWA4YCpkIxlOQ5ofCqovvCg9kq9sYqGz6lMr3RrzOWkUW
+0hF1CHCV0+KGFeIvTYVIKSsJwKBgQC4xiTsaShmwJ6HdR59jOmij+ccCPQTt2IO
eGcniY2cHIoX9I7nn7Yah6JbMT0c8j75KA+pfCrK3FpRNrb71cI1iqBHedZXpRaV
eshJztmw3AKtxQPNwRYrKYpY/M0ShAduppELeshZz1kubQU3sD4adrhcGCDXkctb
dP44IpipuQKBgC+W5q4Q65L0ECCe3aQciRUEbGtKVfgaAL5H5h9TeifWXXg5Coa5
DAL8lWG2aZHIKVoZHFNZNqhDeIKEv5BeytFNqfYHtXKQeoorFYpX+47kNgg6EWS2
XjWt2o/pSUOQA0rxUjnckHTmvcmWjnSj0XYXfMJUSndBd+/EXL/ussPnAoGAGE5Q
Wxz2KJYcBHuemCtqLG07nI988/8Ckh66ixPoIeoLLF2KUuPKg7Dl5ZMTk/Q13nar
oMLpqifUZayJ45TZ6EslDGH1lS/tSZqOME9aiY5Xd95bwrwsm17qiQwwOchOZfrZ
R6ZOJqpE8/t5XTr84GRPmiW+ZD0UgCJisqWyaVkCgYEAtupQDst0hmZ0KnJSIZ5U
R6skHABhmwNU5lOPUBIzHVorbAaKDKd4iFbBI5wnBuWxXY0SANl2HYX3gZaPccH4
wzvR3jZ1B4UlEBXl2V+VRbrXyPTN4uUF42AkSGuOsK4O878wW8noX+ZZTk7gydTN
Z+yQ5jhu/fmSBNhqO/8Lp+Y=
-----END PRIVATE KEY-----
"""#;

    #[test]
    fn test_no_verifier() {
        let verifier = NoVerifier::new();

        // Test verify_server_cert
        let result = verifier.verify_server_cert(
            &CertificateDer::from(vec![]),
            &[],
            &ServerName::DnsName("d7y.io".try_into().unwrap()),
            &[],
            UnixTime::now(),
        );
        assert!(result.is_ok());

        // Test supported_verify_schemes
        let schemes = verifier.supported_verify_schemes();
        assert!(!schemes.is_empty());
    }

    #[test]
    fn test_generate_ca_cert_from_pem() {
        let ca_cert_file = NamedTempFile::new().unwrap();
        let ca_key_file = NamedTempFile::new().unwrap();

        ca_cert_file
            .as_file()
            .write_all(SERVER_CERT.as_bytes())
            .unwrap();
        ca_key_file
            .as_file()
            .write_all(SERVER_KEY.as_bytes())
            .unwrap();

        let result = generate_ca_cert_from_pem(
            &ca_cert_file.path().to_path_buf(),
            &ca_key_file.path().to_path_buf(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_generate_cert_from_pem() {
        let cert_file = NamedTempFile::new().unwrap();
        cert_file
            .as_file()
            .write_all(SERVER_CERT.as_bytes())
            .unwrap();

        let result = generate_cert_from_pem(&cert_file.path().to_path_buf());
        assert!(result.is_ok());
        assert!(!result.unwrap().is_empty());
    }

    #[test]
    fn test_generate_self_signed_certs_by_ca_cert() {
        let ca_cert_file = NamedTempFile::new().unwrap();
        let ca_key_file = NamedTempFile::new().unwrap();

        ca_cert_file
            .as_file()
            .write_all(SERVER_CERT.as_bytes())
            .unwrap();
        ca_key_file
            .as_file()
            .write_all(SERVER_KEY.as_bytes())
            .unwrap();

        let ca_cert = generate_ca_cert_from_pem(
            &ca_cert_file.path().to_path_buf(),
            &ca_key_file.path().to_path_buf(),
        )
        .unwrap();
        let host = "example.com";
        let subject_alt_names = vec![host.to_string()];

        let result = generate_self_signed_certs_by_ca_cert(&ca_cert, host, subject_alt_names);
        assert!(result.is_ok());
        let (certs, key) = result.unwrap();
        assert!(!certs.is_empty());
        assert!(matches!(key, PrivateKeyDer::Pkcs8(_)));
    }

    #[test]
    fn test_certs_to_raw_certs() {
        let cert_file = NamedTempFile::new().unwrap();
        cert_file
            .as_file()
            .write_all(SERVER_CERT.as_bytes())
            .unwrap();

        let certs = generate_cert_from_pem(&cert_file.path().to_path_buf()).unwrap();

        let raw_certs = certs_to_raw_certs(certs);
        assert!(!raw_certs.is_empty());
    }

    #[test]
    fn test_raw_certs_to_certs() {
        let cert_file = NamedTempFile::new().unwrap();
        cert_file
            .as_file()
            .write_all(SERVER_CERT.as_bytes())
            .unwrap();

        let certs = generate_cert_from_pem(&cert_file.path().to_path_buf()).unwrap();
        let raw_certs = certs_to_raw_certs(certs);

        let certs = raw_certs_to_certs(raw_certs);
        assert!(!certs.is_empty());
    }

    #[test]
    fn test_load_certs_from_pem() {
        let result = load_certs_from_pem(SERVER_CERT);
        assert!(result.is_ok());
        assert!(!result.unwrap().is_empty());
    }

    #[test]
    fn test_load_key_from_pem() {
        let result = load_key_from_pem(SERVER_KEY);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), PrivateKeyDer::Pkcs8(_)));
    }
}
