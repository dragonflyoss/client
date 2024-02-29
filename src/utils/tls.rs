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

use crate::{Error as ClientError, Result as ClientResult};
use rcgen::{Certificate, CertificateParams, KeyPair};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::path::PathBuf;
use std::vec::Vec;
use std::{fs, io};

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
    let key_pair = KeyPair::from_pem(ca_key_pem)?;
    let ca_params = CertificateParams::from_ca_cert_pem(ca_cert_pem, key_pair)?;
    let ca_cert = Certificate::from_params(ca_params)?;

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
    let cert = Certificate::from_params(params)?;
    let cert_pem = cert.serialize_pem_with_signer(ca_cert)?;
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
    let cert = rcgen::generate_simple_self_signed(subject_alt_names)?;
    let key = rustls_pki_types::PrivateKeyDer::Pkcs8(cert.serialize_private_key_der().into());
    let certs = vec![cert.serialize_der()?.into()];

    Ok((certs, key))
}
