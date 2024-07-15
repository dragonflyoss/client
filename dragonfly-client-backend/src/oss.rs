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

use std::io::{self, ErrorKind};
use std::time::Duration;

use dragonfly_client_core::*;
use dragonfly_client_util::tls::NoVerifier;
use error::BackendError;
use opendal::{raw::HttpClient, Metakey, Operator};
use reqwest::{header::HeaderMap, StatusCode};
use rustls_pki_types::CertificateDer;
use tokio_util::io::StreamReader;
use tracing::info;
use url::Url;

use crate::*;

/// The OSS config that parsed from the url.
#[derive(Debug)]
struct Config {
    url: Url,
    bucket: String,
    endpoint: String,
    version: Option<String>,
}

impl Config {
    fn is_dir(&self) -> bool {
        self.url.path().ends_with('/')
    }

    #[inline]
    /// Returns a key derived from the URL path.
    fn key(&self) -> &str {
        // Get the path part of the URL and trim any leading slashes.
        self.url.path().trim_start_matches('/')
    }

    /// Get url that have the same endpoint with the config's with given path.
    fn get_url(&self, path: &str) -> String {
        let mut url = self.url.clone();
        url.set_path(path);
        url.to_string()
    }
}

impl TryFrom<Url> for Config {
    type Error = Error;

    fn try_from(url: Url) -> std::result::Result<Self, Self::Error> {
        let host_str = url
            .host_str()
            .ok_or_else(|| Error::InvalidURI(url.to_string()))?;

        // Check the url is valid.
        //
        // The scheme of url should be "oss".
        // The path of url should not be empty to access the oss system.
        // The domain name should end with "aliyuncs.com" to access the oss system.
        // There should be at least three "." in the host string to parse the "bucket",
        // "region" and "endpoint".
        if url.scheme() != "oss"
            || url.path().is_empty()
            || !host_str.ends_with("aliyuncs.com")
            || host_str.matches('.').count() < 3
        {
            return Err(Error::InvalidURI(url.to_string()));
        }

        // Parse the bucket and endpoint.
        //
        // OSS use the "Virtual Hosting of Buckets", so the bucket will be the lowest level of
        // the host, the rest of the host string is the endpoint.
        let (bucket, endpoint) = host_str
            .split_once('.')
            .map(|(s1, s2)| (s1.to_string(), format!("https://{}", s2))) // Add scheme for endpoint.
            .ok_or_else(|| Error::InvalidURI(url.to_string()))?;

        // Parse the version if possible.
        let version = url
            .query_pairs()
            .find(|(key, _)| key == "versionId")
            .map(|(_, version)| version.to_string());

        Ok(Self {
            url,
            endpoint,
            bucket,
            version,
        })
    }
}

/// Initialize operator that used to access the OSS service.
fn init_operator(
    header: HeaderMap,
    certs: Option<Vec<CertificateDer<'static>>>,
    timeout: Duration,
    object_storage: Option<ObjectStorage>,
    config: &Config,
) -> Result<Operator> {
    let client_config_builder = match certs.as_ref() {
        Some(client_certs) => {
            let mut root_cert_store = rustls::RootCertStore::empty();
            root_cert_store.add_parsable_certificates(client_certs.to_owned());

            // TLS client config using the custom CA store for lookups.
            rustls::ClientConfig::builder()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth()
        }
        // Default TLS client config with native roots.
        None => rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(NoVerifier::new())
            .with_no_client_auth(),
    };

    let client = reqwest::Client::builder()
        .use_preconfigured_tls(client_config_builder)
        .timeout(timeout)
        .default_headers(header)
        .build()?;

    let ObjectStorage {
        access_key_id,
        access_key_secret,
    } = object_storage.ok_or(Error::InvalidParameter)?;

    let mut operator_builder = opendal::services::Oss::default();

    // Set up operator builder.
    operator_builder
        .access_key_id(&access_key_id)
        .access_key_secret(&access_key_secret)
        .http_client(HttpClient::with(client))
        .root("/")
        .bucket(&config.bucket)
        .endpoint(&config.endpoint);

    Ok(Operator::new(operator_builder)
        .map_err(|err| {
            Error::BackendError(BackendError {
                message: err.to_string(),
                status_code: StatusCode::INTERNAL_SERVER_ERROR,
                header: HeaderMap::default(),
            })
        })?
        .finish())
}

#[derive(Default)]
pub struct OSS;

impl OSS {
    /// Returns OSS that implement `Backend` trait.
    pub fn new() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl Backend for OSS {
    /// Returns the header(mainly is Content-Length) of requested file or directory.
    ///
    /// # Note
    ///
    /// This function will automatically list the entries if the requested url is a
    /// directory.
    ///
    /// If `HeadRequest.recursive` is true, this fucntion will list all the entries.
    ///
    /// # Error
    ///
    /// This function will return error when:
    /// - the `HeadRequest.url` is not valid.
    /// - other network/IO error.
    async fn head(&self, request: HeadRequest) -> Result<HeadResponse> {
        info!(
            "head request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        let url: Url = request
            .url
            .parse()
            .map_err(|_| Error::InvalidURI(request.url))?;

        let config: Config = url.try_into()?;

        let operator = init_operator(
            request.http_header.clone().unwrap_or_default(),
            request.client_certs,
            request.timeout,
            request.object_storage,
            &config,
        )?;

        // Get the entries if url point to a directory.
        let entries = if config.is_dir() {
            Some(
                operator
                    .list_with(config.key())
                    .recursive(request.recursive)
                    .metakey(Metakey::ContentLength | Metakey::Mode | Metakey::Version)
                    .await // Do the list op here.
                    .map_err(|err| {
                        error!(
                            "get request fail {} {}: {}",
                            request.task_id, config.url, err
                        );

                        Error::BackendError(BackendError {
                            message: err.to_string(),
                            status_code: StatusCode::INTERNAL_SERVER_ERROR,
                            header: HeaderMap::default(),
                        })
                    })?
                    .into_iter()
                    .map(|entry| {
                        let path = entry.path();
                        let metadata = entry.metadata();

                        DirEntry {
                            url: config.get_url(path),
                            content_length: metadata.content_length() as usize,
                            is_dir: metadata.is_dir(),
                            version: metadata.version().map(|v| v.into()),
                        }
                    })
                    .collect(),
            )
        } else {
            None
        };

        // Initialize the stat_operator with the key from the config.
        let mut stat_operator = operator.stat_with(config.key());

        // If a version is specified in the config, set the stat_operator to use that version.
        if let Some(ref version) = config.version {
            stat_operator = stat_operator.version(version);
        }

        // Await the result of the stat_operator. If an error occurs, log the error and return a BackendError.
        let stat = stat_operator.await.map_err(|err| {
            error!(
                "get request fail {} {}: {}",
                request.task_id, config.url, err
            );
            // Return a BackendError with the error message, status code, and an empty header map.
            Error::BackendError(BackendError {
                message: err.to_string(),
                status_code: StatusCode::INTERNAL_SERVER_ERROR,
                header: HeaderMap::default(),
            })
        })?;

        info!("head response {} {}", request.task_id, config.url,);

        Ok(HeadResponse {
            success: true,
            content_length: Some(stat.content_length()),
            http_header: request.http_header,
            http_status_code: Some(reqwest::StatusCode::OK),
            error_message: None,
            entries,
        })
    }

    /// Returns content of requested file or directory.
    ///
    /// # Error
    ///
    /// This function will return error when:
    /// - the `GetRequest.url` is not valid.
    /// - the `GetRequest.url` points to a directory.
    /// - the `GetRequest.url` is not existed.
    /// - other network/IO error.
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>> {
        info!(
            "get request {} {}: {:?}",
            request.piece_id, request.url, request.http_header
        );

        let url: Url = request
            .url
            .parse()
            .map_err(|_| Error::InvalidURI(request.url))?;

        let config: Config = url.try_into()?;

        let operator = init_operator(
            request.http_header.clone().unwrap_or_default(),
            request.client_certs,
            request.timeout,
            request.object_storage,
            &config,
        )?;

        // Set up the reader_operator with the key from the config and a chunk size of 8 KB.
        let mut reader_operator = operator.reader_with(config.key()).chunk(8 << 10);

        // If a version is specified in the config, set the reader_operator to use that version.
        if let Some(ref version) = config.version {
            reader_operator = reader_operator.version(version);
        }

        // Await the result of the reader_operator. If an error occurs, log the error and return a BackendError.
        let reader = reader_operator.await.map_err(|err| {
            error!(
                "get request fail {} {}: {}",
                request.piece_id, config.url, err
            );
            // Return a BackendError with the error message, status code, and an empty header map.
            Error::BackendError(BackendError {
                message: err.to_string(),
                status_code: StatusCode::INTERNAL_SERVER_ERROR,
                header: HeaderMap::default(),
            })
        })?;

        // Create a byte stream based on the range specified in the request.
        let stream = match request.range {
            // If a range is specified, create a byte stream for the specified range.
            Some(range) => reader
                .into_bytes_stream(range.start..range.start + range.length)
                .await
                .map_err(|err| io::Error::new(ErrorKind::Other, err))?,
            // If no range is specified, create a byte stream for the entire content.
            None => reader
                .into_bytes_stream(..)
                .await
                .map_err(|err| io::Error::new(ErrorKind::Other, err))?,
        };

        Ok(GetResponse {
            success: true,
            http_header: None,
            http_status_code: Some(reqwest::StatusCode::OK),
            reader: Box::new(StreamReader::new(stream)),
            error_message: None,
        })
    }
}
