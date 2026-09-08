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

//! Object Storage backend implementation for downloading and uploading files from cloud storage services.
//!
//! This module provides support for multiple cloud object storage URL schemes to access files
//! from various cloud providers. It uses the OpenDAL library to provide a unified interface
//! across different object storage services, handling authentication, TLS configuration,
//! and concurrent uploads.
//!
//! # Supported Schemes
//!
//! - `s3://` - Amazon Simple Storage Service (S3)
//! - `gs://` - Google Cloud Storage (GCS)
//! - `abs://` - Azure Blob Storage (ABS)
//! - `oss://` - Aliyun Object Storage Service (OSS)
//! - `obs://` - Huawei Cloud Object Storage Service (OBS)
//! - `cos://` - Tencent Cloud Object Storage Service (COS)
//!
//! # URL Format
//!
//! The URL format is: `<scheme>://<bucket>/<key>`
//!
//! Examples:
//! - `s3://my-bucket/models/` - List entire directory in S3
//! - `s3://my-bucket/models/weights.bin` - Access specific file in S3
//! - `gs://my-bucket/data/train.csv` - Access specific file in GCS
//! - `oss://my-bucket/path/to/file` - Access specific file in OSS
//!
//! # Authentication
//!
//! Each object storage provider requires different credentials:
//! - **S3**: `access_key_id`, `access_key_secret`, and `region` (optionally `endpoint`, `session_token`)
//! - **GCS**: `credential_path` for service account credentials (optionally `endpoint`, `predefined_acl`)
//! - **ABS**: `access_key_id` (account name), `access_key_secret` (account key), and `endpoint`
//! - **OSS**: `access_key_id`, `access_key_secret`, and `endpoint` (optionally `security_token`)
//! - **OBS**: `access_key_id`, `access_key_secret`, and `endpoint`
//! - **COS**: `access_key_id` (secret id), `access_key_secret` (secret key), and `endpoint`
//!
//! # TLS Configuration
//!
//! By default, TLS certificate verification is enabled. To skip certificate verification
//! (e.g., for self-signed certificates), set `insecure_skip_verify` to `true` in the
//! object storage configuration.

use crate::{
    Body, DirEntry, ExistsRequest, GetRequest, GetResponse, PutRequest, PutResponse, StatRequest,
    StatResponse, HTTP2_CONNECTION_WINDOW_SIZE, HTTP2_KEEP_ALIVE_INTERVAL,
    HTTP2_KEEP_ALIVE_TIMEOUT, HTTP2_STREAM_WINDOW_SIZE, KEEP_ALIVE_INTERVAL,
    POOL_MAX_IDLE_PER_HOST,
};
use async_trait::async_trait;
use dragonfly_api::common;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::error::BackendError;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use dragonfly_client_util::tls::NoVerifier;
use futures::StreamExt;
use opendal::{layers::HttpClientLayer, layers::TimeoutLayer, raw::HttpClient, Operator};
use percent_encoding::percent_decode_str;
use std::fmt;
use std::result::Result;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::io::StreamReader;
use tracing::{debug, error, instrument};
use url::Url;

/// Default region for S3 if not specified.
const DEFAULT_REGION: &str = "us-east-1";

/// The scheme of the object storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// The Amazon Simple Storage Service.
    S3,

    /// The Google Cloud Storage Service.
    GCS,

    /// The Azure Blob Storage Service.
    ABS,

    /// The Aliyun Object Storage Service.
    OSS,

    /// The Huawei Cloud Object Storage Service.
    OBS,

    /// The Tencent Cloud Object Storage Service.
    COS,
}

/// Implements the Scheme trait.
impl Scheme {
    /// Returns true if the given string is a supported scheme.
    pub fn is_supported(scheme: &str) -> bool {
        scheme.parse::<Scheme>().is_ok()
    }
}

/// Implements the Display.
impl fmt::Display for Scheme {
    /// Fmt formats the value using the given formatter.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scheme::S3 => write!(f, "s3"),
            Scheme::GCS => write!(f, "gs"),
            Scheme::ABS => write!(f, "abs"),
            Scheme::OSS => write!(f, "oss"),
            Scheme::OBS => write!(f, "obs"),
            Scheme::COS => write!(f, "cos"),
        }
    }
}

/// Implements the FromStr.
impl FromStr for Scheme {
    type Err = String;

    /// FromStr parses a scheme string.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "s3" => Ok(Scheme::S3),
            "gs" => Ok(Scheme::GCS),
            "abs" => Ok(Scheme::ABS),
            "oss" => Ok(Scheme::OSS),
            "obs" => Ok(Scheme::OBS),
            "cos" => Ok(Scheme::COS),
            _ => Err(format!("invalid scheme: {s}")),
        }
    }
}

/// A struct that contains the parsed URL, bucket, and path.
#[derive(Debug)]
pub struct ParsedURL {
    /// The requested URL of the object storage.
    pub url: Url,

    /// The scheme of the object storage.
    pub scheme: Scheme,

    /// The bucket of the object storage.
    pub bucket: String,

    /// The key of the object storage.
    pub key: String,
}

/// Implements the ParsedURL trait.
impl ParsedURL {
    /// Returns true if the URL path is a directory, which means it ends with a slash.
    pub fn is_dir(&self) -> bool {
        self.url.path().ends_with('/')
    }

    /// Make a URL by the entry path when the URL is a directory. The entry path is the path of the
    /// entry in the directory.
    pub fn make_url_by_entry_path(&self, entry_path: &str) -> Url {
        let mut url = self.url.clone();
        url.set_path(entry_path);
        url
    }
}

/// Implements the TryFrom trait for the URL.
///
/// The object storage URL should be in the format of `scheme://<bucket>/<path>`.
impl TryFrom<Url> for ParsedURL {
    type Error = ClientError;

    /// TryFrom parses the URL and returns a ParsedURL.
    fn try_from(url: Url) -> Result<Self, Self::Error> {
        // Get the bucket from the URL host.
        let bucket = url
            .host_str()
            .ok_or_else(|| ClientError::InvalidURI(url.to_string()))?
            .to_string();

        // Get the scheme from the URL scheme.
        let scheme: Scheme = url.scheme().to_string().parse().map_err(|err| {
            error!("parse scheme failed {}: {}", url, err);
            ClientError::InvalidURI(url.to_string())
        })?;

        // Get the key from the URL path.
        let key = url
            .path()
            .strip_prefix('/')
            .ok_or_else(|| ClientError::InvalidURI(url.to_string()))?;
        // Decode the key.
        let decoded_key = percent_decode_str(key).decode_utf8_lossy().to_string();

        Ok(Self {
            url,
            scheme,
            bucket,
            key: decoded_key,
        })
    }
}

/// Make a message for the need fields in the object storage. The fields are the required fields
/// for the object storage, which are different for different object storages. The macro takes a
/// variable and a list of fields, and returns a message that indicates which fields are needed.
macro_rules! make_need_fields_message {
    ($var:ident {$($field:ident),*}) => {{
            let mut need_fields: Vec<&'static str> = vec![];

            $(
                if $var.$field.is_none() {
                    need_fields.push(stringify!($field));
                }
            )*

            format!("need {}", need_fields.join(", "))
       }};
}

/// A struct that implements the backend trait.
pub struct ObjectStorage {
    /// The scheme of the object storage.
    scheme: Scheme,

    /// The configuration of the dfdaemon.
    config: Arc<Config>,

    /// The reqwest client.
    client: reqwest::Client,

    // Danger client is the reqwest dangerous client, which skips certificate verification.
    danger_client: reqwest::Client,
}

/// Implements the ObjectStorage trait.
impl ObjectStorage {
    /// Returns ObjectStorage that implements the Backend trait.
    pub fn new(scheme: Scheme, config: Arc<Config>) -> ClientResult<ObjectStorage> {
        // Initialize the reqwest client.
        let client = reqwest::Client::builder()
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .hickory_dns(config.backend.enable_hickory_dns)
            .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
            .tcp_keepalive(KEEP_ALIVE_INTERVAL)
            .tcp_nodelay(true)
            .http2_adaptive_window(true)
            .http2_initial_stream_window_size(Some(HTTP2_STREAM_WINDOW_SIZE))
            .http2_initial_connection_window_size(Some(HTTP2_CONNECTION_WINDOW_SIZE))
            .http2_keep_alive_timeout(HTTP2_KEEP_ALIVE_TIMEOUT)
            .http2_keep_alive_interval(HTTP2_KEEP_ALIVE_INTERVAL)
            .http2_keep_alive_while_idle(true)
            .build()?;

        // Initialize the reqwest dangerous client.
        let client_config_builder = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(NoVerifier::new())
            .with_no_client_auth();

        let danger_client = reqwest::Client::builder()
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .hickory_dns(config.backend.enable_hickory_dns)
            .use_preconfigured_tls(client_config_builder)
            .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
            .tcp_keepalive(KEEP_ALIVE_INTERVAL)
            .tcp_nodelay(true)
            .http2_adaptive_window(true)
            .http2_initial_stream_window_size(Some(HTTP2_STREAM_WINDOW_SIZE))
            .http2_initial_connection_window_size(Some(HTTP2_CONNECTION_WINDOW_SIZE))
            .http2_keep_alive_timeout(HTTP2_KEEP_ALIVE_TIMEOUT)
            .http2_keep_alive_interval(HTTP2_KEEP_ALIVE_INTERVAL)
            .http2_keep_alive_while_idle(true)
            .build()?;

        Ok(Self {
            scheme,
            config,
            client,
            danger_client,
        })
    }

    /// Operator initializes the operator with the parsed URL and object storage.
    pub fn operator(
        &self,
        parsed_url: &ParsedURL,
        object_storage: Option<common::v2::ObjectStorage>,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // If download backend is object storage, object_storage parameter is required.
        let Some(object_storage) = object_storage else {
            return Err(ClientError::BackendError(Box::new(BackendError {
                message: format!("{} need object_storage parameter", self.scheme),
                status_code: None,
                header: None,
            })));
        };

        match self.scheme {
            Scheme::S3 => self.s3_operator(parsed_url, object_storage, timeout),
            Scheme::GCS => self.gcs_operator(parsed_url, object_storage, timeout),
            Scheme::ABS => self.abs_operator(parsed_url, object_storage, timeout),
            Scheme::OSS => self.oss_operator(parsed_url, object_storage, timeout),
            Scheme::OBS => self.obs_operator(parsed_url, object_storage, timeout),
            Scheme::COS => self.cos_operator(parsed_url, object_storage, timeout),
        }
    }

    /// S3 operator initializes the S3 operator with the parsed URL and object storage.
    pub fn s3_operator(
        &self,
        parsed_url: &ParsedURL,
        object_storage: common::v2::ObjectStorage,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // Initialize the S3 operator with the object storage.
        let mut builder = opendal::services::S3::default();
        builder = builder.bucket(&parsed_url.bucket);

        // Configure the credentials using the access key id and access key secret if provided.
        if let Some(access_key_id) = object_storage.access_key_id.as_deref() {
            builder = builder.access_key_id(access_key_id);
        }

        if let Some(access_key_secret) = object_storage.access_key_secret.as_deref() {
            builder = builder.secret_access_key(access_key_secret);
        }

        // Configure the region if it is provided. If not provided, use the default region.
        builder = builder.region(object_storage.region.as_deref().unwrap_or(DEFAULT_REGION));

        // Configure the endpoint if it is provided.
        if let Some(endpoint) = object_storage.endpoint.as_deref() {
            builder = builder.endpoint(endpoint);
        };

        // Configure the session token if it is provided.
        if let Some(session_token) = object_storage.session_token.as_deref() {
            builder = builder.session_token(session_token);
        }

        // Choose the http client using dangerous client or not by insecure_skip_verify.
        let http_client = match object_storage.insecure_skip_verify {
            Some(true) => self.danger_client.clone(),
            _ => self.client.clone(),
        };

        Ok(Operator::new(builder)?
            .finish()
            .layer(TimeoutLayer::new().with_timeout(timeout))
            .layer(HttpClientLayer::new(HttpClient::with(http_client))))
    }

    /// GCS operator initializes the GCS operator with the parsed URL and object storage.
    pub fn gcs_operator(
        &self,
        parsed_url: &ParsedURL,
        object_storage: common::v2::ObjectStorage,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // Initialize the GCS operator with the object storage.
        let mut builder = opendal::services::Gcs::default();
        builder = builder.bucket(&parsed_url.bucket);

        // Configure the credentials using the local path to the credential file if provided.
        // Otherwise, configure using the Application Default Credentials (ADC).
        if let Some(credential_path) = object_storage.credential_path.as_deref() {
            builder = builder.credential_path(credential_path);
        }

        // Configure the endpoint if it is provided. If the endpoint is not provided, the operator
        // will use the default endpoint of GCS.
        if let Some(endpoint) = object_storage.endpoint.as_deref() {
            builder = builder.endpoint(endpoint);
        }

        // Configure the predefined ACL if it is provided.
        if let Some(predefined_acl) = object_storage.predefined_acl.as_deref() {
            builder = builder.predefined_acl(predefined_acl);
        }

        // Choose the http client using dangerous client or not by insecure_skip_verify.
        let http_client = match object_storage.insecure_skip_verify {
            Some(true) => self.danger_client.clone(),
            _ => self.client.clone(),
        };

        Ok(Operator::new(builder)?
            .finish()
            .layer(TimeoutLayer::new().with_timeout(timeout))
            .layer(HttpClientLayer::new(HttpClient::with(http_client))))
    }

    /// ABS operator initializes the ABS operator with the parsed URL and object storage.
    pub fn abs_operator(
        &self,
        parsed_url: &ParsedURL,
        object_storage: common::v2::ObjectStorage,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        let Some(endpoint) = &object_storage.endpoint else {
            return Err(ClientError::BackendError(Box::new(BackendError {
                message: format!(
                    "{} {}",
                    self.scheme,
                    make_need_fields_message!(object_storage { endpoint })
                ),
                status_code: None,
                header: None,
            })));
        };

        // Initialize the ABS operator with the object storage.
        let mut builder = opendal::services::Azblob::default();
        builder = builder.container(&parsed_url.bucket).endpoint(endpoint);

        // Configure the credentials using the access key id and access key secret if provided.
        if let Some(access_key_id) = object_storage.access_key_id.as_deref() {
            builder = builder.account_name(access_key_id);
        }

        if let Some(access_key_secret) = object_storage.access_key_secret.as_deref() {
            builder = builder.account_key(access_key_secret);
        }

        // Choose the http client using dangerous client or not by insecure_skip_verify.
        let http_client = match object_storage.insecure_skip_verify {
            Some(true) => self.danger_client.clone(),
            _ => self.client.clone(),
        };

        Ok(Operator::new(builder)?
            .finish()
            .layer(TimeoutLayer::new().with_timeout(timeout))
            .layer(HttpClientLayer::new(HttpClient::with(http_client))))
    }

    /// OSS operator initializes the OSS operator with the parsed URL and object storage.
    pub fn oss_operator(
        &self,
        parsed_url: &ParsedURL,
        object_storage: common::v2::ObjectStorage,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        let Some(endpoint) = &object_storage.endpoint else {
            return Err(ClientError::BackendError(Box::new(BackendError {
                message: format!(
                    "{} {}",
                    self.scheme,
                    make_need_fields_message!(object_storage { endpoint })
                ),
                status_code: None,
                header: None,
            })));
        };

        // Initialize the OSS operator with the object storage.
        let mut builder = opendal::services::Oss::default();
        builder = if let Some(security_token) = &object_storage.security_token {
            builder
                .endpoint(endpoint)
                .root("/")
                .bucket(&parsed_url.bucket)
                .security_token(security_token)
        } else {
            builder
                .endpoint(endpoint)
                .root("/")
                .bucket(&parsed_url.bucket)
        };

        // Configure the credentials using the access key id and access key secret if provided.
        if let Some(access_key_id) = object_storage.access_key_id.as_deref() {
            builder = builder.access_key_id(access_key_id);
        }

        if let Some(access_key_secret) = object_storage.access_key_secret.as_deref() {
            builder = builder.access_key_secret(access_key_secret);
        }

        // Choose the http client using dangerous client or not by insecure_skip_verify.
        let http_client = match object_storage.insecure_skip_verify {
            Some(true) => self.danger_client.clone(),
            _ => self.client.clone(),
        };

        Ok(Operator::new(builder)?
            .finish()
            .layer(TimeoutLayer::new().with_timeout(timeout))
            .layer(HttpClientLayer::new(HttpClient::with(http_client))))
    }

    /// OBS operator initializes the OBS operator with the parsed URL and object storage.
    pub fn obs_operator(
        &self,
        parsed_url: &ParsedURL,
        object_storage: common::v2::ObjectStorage,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        let Some(endpoint) = &object_storage.endpoint else {
            return Err(ClientError::BackendError(Box::new(BackendError {
                message: format!(
                    "{} {}",
                    self.scheme,
                    make_need_fields_message!(object_storage { endpoint })
                ),
                status_code: None,
                header: None,
            })));
        };

        // Initialize the OBS operator with the object storage.
        let mut builder = opendal::services::Obs::default();
        builder = builder.endpoint(endpoint).bucket(&parsed_url.bucket);

        // Configure the credentials using the access key id and access key secret if provided.
        if let Some(access_key_id) = object_storage.access_key_id.as_deref() {
            builder = builder.access_key_id(access_key_id);
        }

        if let Some(access_key_secret) = object_storage.access_key_secret.as_deref() {
            builder = builder.secret_access_key(access_key_secret);
        }

        // Choose the http client using dangerous client or not by insecure_skip_verify.
        let http_client = match object_storage.insecure_skip_verify {
            Some(true) => self.danger_client.clone(),
            _ => self.client.clone(),
        };

        Ok(Operator::new(builder)?
            .finish()
            .layer(TimeoutLayer::new().with_timeout(timeout))
            .layer(HttpClientLayer::new(HttpClient::with(http_client))))
    }

    /// COS operator initializes the COS operator with the parsed URL and object storage.
    pub fn cos_operator(
        &self,
        parsed_url: &ParsedURL,
        object_storage: common::v2::ObjectStorage,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        let Some(endpoint) = &object_storage.endpoint else {
            return Err(ClientError::BackendError(Box::new(BackendError {
                message: format!(
                    "{} {}",
                    self.scheme,
                    make_need_fields_message!(object_storage { endpoint })
                ),
                status_code: None,
                header: None,
            })));
        };

        // Initialize the COS operator with the object storage.
        let mut builder = opendal::services::Cos::default();
        builder = builder.endpoint(endpoint).bucket(&parsed_url.bucket);

        // Configure the credentials using the access key id and access key secret if provided.
        if let Some(access_key_id) = object_storage.access_key_id.as_deref() {
            builder = builder.secret_id(access_key_id);
        }

        if let Some(access_key_secret) = object_storage.access_key_secret.as_deref() {
            builder = builder.secret_key(access_key_secret);
        }

        // Choose the http client using dangerous client or not by insecure_skip_verify.
        let http_client = match object_storage.insecure_skip_verify {
            Some(true) => self.danger_client.clone(),
            _ => self.client.clone(),
        };

        Ok(Operator::new(builder)?
            .finish()
            .layer(TimeoutLayer::new().with_timeout(timeout))
            .layer(HttpClientLayer::new(HttpClient::with(http_client))))
    }
}

/// Implements the Backend trait.
#[async_trait]
impl crate::Backend for ObjectStorage {
    /// Returns the scheme of the object storage.
    fn scheme(&self) -> String {
        self.scheme.to_string()
    }

    /// Stat the metadata from the backend.
    #[instrument(skip_all)]
    async fn stat(&self, request: StatRequest) -> ClientResult<StatResponse> {
        debug!(
            "stat request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        // Parse the URL and convert it to a ParsedURL for create the ObjectStorage operator.
        let url: Url = request
            .url
            .parse()
            .map_err(|_| ClientError::InvalidURI(request.url.clone()))?;

        let parsed_url: ParsedURL = url.try_into().inspect_err(|err| {
            error!(
                "parse stat request url failed {} {}: {}",
                request.task_id, request.url, err
            );
        })?;

        // Initialize the operator with the parsed URL, object storage, and timeout.
        let operator = self.operator(&parsed_url, request.object_storage, request.timeout)?;

        // Get the entries if url point to a directory.
        let entries = if parsed_url.is_dir() {
            operator
                .list_with(&parsed_url.key)
                .recursive(true)
                .await // Do the list op here.
                .map_err(|err| {
                    error!(
                        "list request failed {} {}: {}",
                        request.task_id, request.url, err
                    );

                    ClientError::BackendError(Box::new(BackendError {
                        message: err.to_string(),
                        status_code: None,
                        header: None,
                    }))
                })?
                .into_iter()
                .map(|entry| {
                    let metadata = entry.metadata();
                    DirEntry {
                        url: parsed_url.make_url_by_entry_path(entry.path()).to_string(),
                        content_length: metadata.content_length() as usize,
                        is_dir: metadata.is_dir(),
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        // Stat the object to get the response from the ObjectStorage.
        let response = operator.stat_with(&parsed_url.key).await.map_err(|err| {
            error!(
                "stat request failed {} {}: {}",
                request.task_id, request.url, err
            );

            ClientError::BackendError(Box::new(BackendError {
                message: err.to_string(),
                status_code: None,
                header: None,
            }))
        })?;

        debug!(
            "stat response {} {}: {}",
            request.task_id,
            request.url,
            response.content_length()
        );

        Ok(StatResponse {
            success: true,
            content_length: Some(response.content_length()),
            http_header: None,
            http_status_code: None,
            error_message: None,
            entries,
        })
    }

    /// Get the content from the backend.
    #[instrument(skip_all)]
    async fn get(&self, request: GetRequest) -> ClientResult<GetResponse<Body>> {
        debug!(
            "get request {} {}: {:?}",
            request.piece_id, request.url, request.http_header
        );

        // Parse the URL and convert it to a ParsedURL for create the ObjectStorage operator.
        let url: Url = request
            .url
            .parse()
            .map_err(|_| ClientError::InvalidURI(request.url.clone()))?;

        let parsed_url: ParsedURL = url.try_into().inspect_err(|err| {
            error!(
                "parse get request url failed {} {}: {}",
                request.piece_id, request.url, err
            );
        })?;

        // Initialize the operator with the parsed URL, object storage, and timeout.
        let operator_reader = self
            .operator(&parsed_url, request.object_storage, request.timeout)?
            .reader(&parsed_url.key)
            .await
            .map_err(|err| {
                error!(
                    "get request failed {} {}: {}",
                    request.piece_id, request.url, err
                );

                ClientError::BackendError(Box::new(BackendError {
                    message: err.to_string(),
                    status_code: None,
                    header: None,
                }))
            })?;

        let stream = match request.range {
            Some(range) => operator_reader
                .into_bytes_stream(range.start..range.start + range.length)
                .await
                .map_err(|err| {
                    error!(
                        "get request failed {} {}: {}",
                        request.piece_id, request.url, err
                    );

                    ClientError::BackendError(Box::new(BackendError {
                        message: err.to_string(),
                        status_code: None,
                        header: None,
                    }))
                })?,
            None => operator_reader.into_bytes_stream(..).await.map_err(|err| {
                error!(
                    "get request failed {} {}: {}",
                    request.piece_id, request.url, err
                );

                ClientError::BackendError(Box::new(BackendError {
                    message: err.to_string(),
                    status_code: None,
                    header: None,
                }))
            })?,
        };

        Ok(crate::GetResponse {
            success: true,
            http_header: None,
            http_status_code: Some(reqwest::StatusCode::OK),
            reader: StreamReader::new(stream.boxed()),
            error_message: None,
        })
    }

    /// Put the content to the backend.
    #[instrument(skip_all)]
    async fn put(&self, request: PutRequest) -> ClientResult<PutResponse> {
        debug!("put request {:?} {}", request.path, request.url);

        // Parse the URL and convert it to a ParsedURL for create the ObjectStorage operator.
        let url: Url = request
            .url
            .parse()
            .map_err(|_| ClientError::InvalidURI(request.url.clone()))?;

        let parsed_url: ParsedURL = url.try_into().inspect_err(|err| {
            error!(
                "parse put request url failed {:?} {}: {}",
                request.path, request.url, err
            );
        })?;

        // Initialize the object storage operator to write the object.
        let mut object_storage_writer = self
            .operator(&parsed_url, request.object_storage, request.timeout)?
            .writer_with(&parsed_url.key)
            .concurrent(self.config.backend.put_concurrent_chunk_count as usize)
            .chunk(self.config.backend.put_chunk_size.as_u64() as usize)
            .await
            .map_err(|err| {
                error!(
                    "put request failed {:?} {}: {}",
                    request.path, request.url, err
                );

                ClientError::BackendError(Box::new(BackendError {
                    message: err.to_string(),
                    status_code: None,
                    header: None,
                }))
            })?;

        // Initialize the fs operator to read the local file.
        let fs_operator = Operator::new(opendal::services::Fs::default().root("/"))
            .inspect_err(|err| {
                error!("initialize fs operator failed: {}", err);
            })?
            .finish();

        let fs_reader = fs_operator
            .reader_with(&request.path.to_string_lossy())
            .concurrent(self.config.backend.put_concurrent_chunk_count as usize)
            .chunk(self.config.backend.put_chunk_size.as_u64() as usize)
            .await?;

        let content_length = fs_operator
            .stat(&request.path.to_string_lossy())
            .await
            .inspect_err(|err| {
                error!(
                    "stat local file failed {:?} {}: {}",
                    request.path, request.url, err
                );
            })?
            .content_length();

        let mut offset: u64 = 0;
        while offset < content_length {
            let end = std::cmp::min(
                offset + self.config.backend.put_chunk_size.as_u64(),
                content_length,
            );

            let buf = fs_reader.read(offset..end).await.inspect_err(|err| {
                error!(
                    "read local file failed {:?} {}: {}",
                    request.path, request.url, err
                );
            })?;

            object_storage_writer.write(buf).await.inspect_err(|err| {
                error!(
                    "put request failed {:?} {}: {}",
                    request.path, request.url, err
                );
            })?;

            offset = end;
        }

        object_storage_writer.close().await.inspect_err(|err| {
            error!(
                "close put request failed {:?} {}: {}",
                request.path, request.url, err
            );
        })?;

        Ok(crate::PutResponse {
            success: true,
            http_header: None,
            http_status_code: Some(reqwest::StatusCode::OK),
            content_length: Some(content_length),
            error_message: None,
        })
    }

    /// Exists checks whether the file exists in the backend.
    #[instrument(skip_all)]
    async fn exists(&self, request: ExistsRequest) -> ClientResult<bool> {
        debug!(
            "exists request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        // Parse the URL and convert it to a ParsedURL for create the ObjectStorage operator.
        let url: Url = request
            .url
            .parse()
            .map_err(|_| ClientError::InvalidURI(request.url.clone()))?;

        let parsed_url: ParsedURL = url.try_into().inspect_err(|err| {
            error!(
                "parse exists request url failed {} {}: {}",
                request.task_id, request.url, err
            );
        })?;

        // Initialize the operator with the parsed URL, object storage, and timeout.
        let operator = self.operator(&parsed_url, request.object_storage, request.timeout)?;
        Ok(operator.exists(&parsed_url.key).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_api::common::v2::ObjectStorage as ObjectStorageInfo;

    const SCHEMES: [Scheme; 6] = [
        Scheme::S3,
        Scheme::GCS,
        Scheme::ABS,
        Scheme::OSS,
        Scheme::OBS,
        Scheme::COS,
    ];

    #[test]
    fn scheme_is_supported_only_for_object_storage_schemes() {
        let test_cases = vec![
            ("s3", true),
            ("gs", true),
            ("abs", true),
            ("oss", true),
            ("obs", true),
            ("cos", true),
            ("http", false),
            ("https", false),
            ("ftp", false),
            ("hdfs", false),
            ("file", false),
            ("", false),
            ("S3", false),
            ("GCS", false),
        ];

        for (scheme, expected) in test_cases {
            assert_eq!(Scheme::is_supported(scheme), expected);
        }
    }

    #[test]
    fn parsed_url_splits_bucket_and_decoded_key() {
        let test_cases = vec![
            ("test-bucket/file", "file", false),
            ("test-bucket/path/to/dir/", "path/to/dir/", true),
            ("test-bucket/path%20to/file", "path to/file", false),
            ("test-bucket/", "", true),
        ];

        for (location, expected_key, expected_is_dir) in test_cases {
            for scheme in SCHEMES {
                let url = format!("{scheme}://{location}");
                let parsed_url: ParsedURL = url.parse::<Url>().unwrap().try_into().unwrap();
                assert_eq!(parsed_url.scheme, scheme);
                assert_eq!(parsed_url.bucket, "test-bucket");
                assert_eq!(parsed_url.key, expected_key);
                assert_eq!(parsed_url.is_dir(), expected_is_dir);
            }
        }
    }

    #[test]
    fn parsed_url_rejects_malformed_urls() {
        let test_cases = vec![
            "github://test-bucket/file",
            "s3:///file",
            "gs:///file",
            "abs:///file",
            "oss:///file",
            "obs:///file",
            "cos:///file",
            "s3://test-bucket",
        ];

        for url in test_cases {
            let result: Result<ParsedURL, ClientError> = url.parse::<Url>().unwrap().try_into();
            assert!(matches!(result, Err(ClientError::InvalidURI(_))));
        }
    }

    #[test]
    fn make_url_by_entry_path_replaces_the_key() {
        let test_cases = vec![
            ("test-bucket/file", "test-entry", false),
            ("test-bucket/path/to/dir/", "path/to/dir/file", false),
            ("test-bucket/path/to/dir/", "path/to/dir/sub/", true),
        ];

        for (location, entry_path, expected_is_dir) in test_cases {
            for scheme in SCHEMES {
                let url = format!("{scheme}://{location}");
                let parsed_url: ParsedURL = url.parse::<Url>().unwrap().try_into().unwrap();
                let entry_url: ParsedURL = parsed_url
                    .make_url_by_entry_path(entry_path)
                    .try_into()
                    .unwrap();
                assert_eq!(entry_url.scheme, parsed_url.scheme);
                assert_eq!(entry_url.bucket, parsed_url.bucket);
                assert_eq!(entry_url.key, entry_path);
                assert_eq!(entry_url.is_dir(), expected_is_dir);
            }
        }
    }

    #[test]
    fn operator_builds_with_scheme_specific_fields() {
        dragonfly_client_util::tls::install_crypto_provider();
        let credentials = ObjectStorageInfo {
            access_key_id: Some("access-key-id".into()),
            access_key_secret: Some("access-key-secret".into()),
            ..Default::default()
        };
        let endpoint_credentials = ObjectStorageInfo {
            endpoint: Some("test-endpoint.local".into()),
            ..credentials.clone()
        };

        let test_cases = vec![
            (
                Scheme::S3,
                ObjectStorageInfo {
                    region: Some("test-region".into()),
                    ..credentials.clone()
                },
                "s3",
            ),
            (Scheme::S3, credentials.clone(), "s3"),
            (
                Scheme::S3,
                ObjectStorageInfo {
                    region: Some("test-region".into()),
                    ..endpoint_credentials.clone()
                },
                "s3",
            ),
            (
                Scheme::S3,
                ObjectStorageInfo {
                    region: Some("test-region".into()),
                    session_token: Some("session-token".into()),
                    ..credentials.clone()
                },
                "s3",
            ),
            (
                Scheme::S3,
                ObjectStorageInfo {
                    region: Some("test-region".into()),
                    session_token: Some("session-token".into()),
                    ..endpoint_credentials.clone()
                },
                "s3",
            ),
            (Scheme::GCS, ObjectStorageInfo::default(), "gcs"),
            (
                Scheme::GCS,
                ObjectStorageInfo {
                    credential_path: Some("credential-path".into()),
                    ..Default::default()
                },
                "gcs",
            ),
            (
                Scheme::GCS,
                ObjectStorageInfo {
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "gcs",
            ),
            (
                Scheme::GCS,
                ObjectStorageInfo {
                    predefined_acl: Some("predefined-acl".into()),
                    ..Default::default()
                },
                "gcs",
            ),
            (
                Scheme::GCS,
                ObjectStorageInfo {
                    credential_path: Some("credential-path".into()),
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "gcs",
            ),
            (
                Scheme::GCS,
                ObjectStorageInfo {
                    credential_path: Some("credential-path".into()),
                    predefined_acl: Some("predefined-acl".into()),
                    ..Default::default()
                },
                "gcs",
            ),
            (
                Scheme::GCS,
                ObjectStorageInfo {
                    endpoint: Some("test-endpoint.local".into()),
                    predefined_acl: Some("predefined-acl".into()),
                    ..Default::default()
                },
                "gcs",
            ),
            (
                Scheme::GCS,
                ObjectStorageInfo {
                    credential_path: Some("credential-path".into()),
                    endpoint: Some("test-endpoint.local".into()),
                    predefined_acl: Some("predefined-acl".into()),
                    ..Default::default()
                },
                "gcs",
            ),
            (
                Scheme::ABS,
                ObjectStorageInfo {
                    access_key_secret: Some("YWNjZXNzLWtleS1zZWNyZXQK".into()),
                    ..endpoint_credentials.clone()
                },
                "azblob",
            ),
            (Scheme::OSS, endpoint_credentials.clone(), "oss"),
            (
                Scheme::OSS,
                ObjectStorageInfo {
                    security_token: Some("security-token".into()),
                    ..endpoint_credentials.clone()
                },
                "oss",
            ),
            (
                Scheme::OSS,
                ObjectStorageInfo {
                    endpoint: Some("https://oss-cn-beijing.aliyuncs.com".into()),
                    insecure_skip_verify: Some(true),
                    ..credentials.clone()
                },
                "oss",
            ),
            (
                Scheme::OSS,
                ObjectStorageInfo {
                    endpoint: Some("https://oss-cn-beijing.aliyuncs.com".into()),
                    insecure_skip_verify: Some(false),
                    ..credentials.clone()
                },
                "oss",
            ),
            (
                Scheme::OSS,
                ObjectStorageInfo {
                    endpoint: Some("https://oss-cn-beijing.aliyuncs.com".into()),
                    insecure_skip_verify: None,
                    ..credentials.clone()
                },
                "oss",
            ),
            (Scheme::OBS, endpoint_credentials.clone(), "obs"),
            (Scheme::COS, endpoint_credentials.clone(), "cos"),
        ];

        let config = Arc::new(Config::default());
        for (scheme, object_storage, expected_service) in test_cases {
            let parsed_url: ParsedURL = format!("{scheme}://test-bucket/file")
                .parse::<Url>()
                .unwrap()
                .try_into()
                .unwrap();
            let result = ObjectStorage::new(scheme, config.clone())
                .unwrap()
                .operator(
                    &parsed_url,
                    Some(object_storage.clone()),
                    Duration::from_secs(3),
                );
            assert!(result.is_ok());

            let info = result.unwrap().info();
            assert_eq!(info.scheme(), expected_service);
            assert_eq!(info.name(), "test-bucket");
        }
    }

    #[test]
    fn operator_rejects_missing_required_fields() {
        dragonfly_client_util::tls::install_crypto_provider();
        let access_key_id = ObjectStorageInfo {
            access_key_id: Some("access-key-id".into()),
            ..Default::default()
        };
        let access_key_secret = ObjectStorageInfo {
            access_key_secret: Some("access-key-secret".into()),
            ..Default::default()
        };
        let credentials = ObjectStorageInfo {
            access_key_id: Some("access-key-id".into()),
            access_key_secret: Some("access-key-secret".into()),
            ..Default::default()
        };

        let test_cases = vec![
            (
                Scheme::S3,
                None,
                "backend error: s3 need object_storage parameter",
            ),
            (
                Scheme::ABS,
                Some(ObjectStorageInfo::default()),
                "backend error: abs need endpoint",
            ),
            (
                Scheme::ABS,
                Some(access_key_id.clone()),
                "backend error: abs need endpoint",
            ),
            (
                Scheme::ABS,
                Some(access_key_secret.clone()),
                "backend error: abs need endpoint",
            ),
            (
                Scheme::ABS,
                Some(credentials.clone()),
                "backend error: abs need endpoint",
            ),
            (
                Scheme::OSS,
                Some(ObjectStorageInfo::default()),
                "backend error: oss need endpoint",
            ),
            (
                Scheme::OSS,
                Some(access_key_id.clone()),
                "backend error: oss need endpoint",
            ),
            (
                Scheme::OSS,
                Some(access_key_secret.clone()),
                "backend error: oss need endpoint",
            ),
            (
                Scheme::OSS,
                Some(credentials.clone()),
                "backend error: oss need endpoint",
            ),
            (
                Scheme::OBS,
                Some(ObjectStorageInfo::default()),
                "backend error: obs need endpoint",
            ),
            (
                Scheme::OBS,
                Some(access_key_id.clone()),
                "backend error: obs need endpoint",
            ),
            (
                Scheme::OBS,
                Some(access_key_secret.clone()),
                "backend error: obs need endpoint",
            ),
            (
                Scheme::OBS,
                Some(credentials.clone()),
                "backend error: obs need endpoint",
            ),
            (
                Scheme::COS,
                Some(ObjectStorageInfo::default()),
                "backend error: cos need endpoint",
            ),
            (
                Scheme::COS,
                Some(access_key_id.clone()),
                "backend error: cos need endpoint",
            ),
            (
                Scheme::COS,
                Some(access_key_secret.clone()),
                "backend error: cos need endpoint",
            ),
            (
                Scheme::COS,
                Some(credentials.clone()),
                "backend error: cos need endpoint",
            ),
        ];

        let config = Arc::new(Config::default());
        for (scheme, object_storage, expected) in test_cases {
            let parsed_url: ParsedURL = format!("{scheme}://test-bucket/file")
                .parse::<Url>()
                .unwrap()
                .try_into()
                .unwrap();
            let err = ObjectStorage::new(scheme, config.clone())
                .unwrap()
                .operator(&parsed_url, object_storage.clone(), Duration::from_secs(3))
                .unwrap_err();
            assert_eq!(err.to_string(), expected);
        }
    }
}
