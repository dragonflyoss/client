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

use dragonfly_api::common;
use dragonfly_client_core::error::BackendError;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use opendal::{layers::HttpClientLayer, layers::TimeoutLayer, raw::HttpClient, Operator};
use percent_encoding::percent_decode_str;
use std::fmt;
use std::result::Result;
use std::str::FromStr;
use std::time::Duration;
use tokio_util::io::StreamReader;
use tracing::{debug, error};
use url::Url;

/// Scheme is the scheme of the object storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// S3 is the Amazon Simple Storage Service.
    S3,

    /// GCS is the Google Cloud Storage Service.
    GCS,

    /// ABS is the Azure Blob Storage Service.
    ABS,

    /// OSS is the Aliyun Object Storage Service.
    OSS,

    /// OBS is the Huawei Cloud Object Storage Service.
    OBS,

    /// COS is the Tencent Cloud Object Storage Service.
    COS,
}

/// Scheme implements the Display.
impl fmt::Display for Scheme {
    /// fmt formats the value using the given formatter.
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

/// Scheme implements the FromStr.
impl FromStr for Scheme {
    type Err = String;

    /// from_str parses a scheme string.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "s3" => Ok(Scheme::S3),
            "gs" => Ok(Scheme::GCS),
            "abs" => Ok(Scheme::ABS),
            "oss" => Ok(Scheme::OSS),
            "obs" => Ok(Scheme::OBS),
            "cos" => Ok(Scheme::COS),
            _ => Err(format!("invalid scheme: {}", s)),
        }
    }
}

/// ParsedURL is a struct that contains the parsed URL, bucket, and path.
#[derive(Debug)]
pub struct ParsedURL {
    /// url is the requested URL of the object storage.
    pub url: Url,

    /// scheme is the scheme of the object storage.
    pub scheme: Scheme,

    /// bucket is the bucket of the object storage.
    pub bucket: String,

    /// key is the key of the object storage.
    pub key: String,
}

/// ParsedURL implements the ParsedURL trait.
impl ParsedURL {
    /// is_dir returns true if the URL path ends with a slash.
    pub fn is_dir(&self) -> bool {
        self.url.path().ends_with('/')
    }

    /// make_url_by_entry_path makes a URL by the entry path when the URL is a directory.
    pub fn make_url_by_entry_path(&self, entry_path: &str) -> Url {
        let mut url = self.url.clone();
        url.set_path(entry_path);
        url
    }
}

/// ParsedURL implements the TryFrom trait for the URL.
///
/// The object storage URL should be in the format of `scheme://<bucket>/<path>`.
impl TryFrom<Url> for ParsedURL {
    type Error = ClientError;

    /// try_from parses the URL and returns a ParsedURL.
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

/// make_need_fields_message makes a message for the need fields in the object storage.
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

/// ObjectStorage is a struct that implements the backend trait.
pub struct ObjectStorage {
    /// scheme is the scheme of the object storage.
    scheme: Scheme,

    /// client is the reqwest client.
    client: reqwest::Client,
}

/// ObjectStorage implements the ObjectStorage trait.
impl ObjectStorage {
    /// Returns ObjectStorage that implements the Backend trait.
    pub fn new(scheme: Scheme) -> ClientResult<ObjectStorage> {
        // Initialize the reqwest client.
        let client = reqwest::Client::builder()
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .hickory_dns(true)
            .pool_max_idle_per_host(super::POOL_MAX_IDLE_PER_HOST)
            .tcp_keepalive(super::KEEP_ALIVE_INTERVAL)
            .tcp_nodelay(true)
            .http2_adaptive_window(true)
            .http2_initial_stream_window_size(Some(super::HTTP2_STREAM_WINDOW_SIZE))
            .http2_initial_connection_window_size(Some(super::HTTP2_CONNECTION_WINDOW_SIZE))
            .http2_keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT)
            .http2_keep_alive_interval(super::HTTP2_KEEP_ALIVE_INTERVAL)
            .http2_keep_alive_while_idle(true)
            .build()?;

        Ok(Self { scheme, client })
    }

    /// operator initializes the operator with the parsed URL and object storage.
    pub fn operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
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

    /// s3_operator initializes the S3 operator with the parsed URL and object storage.
    pub fn s3_operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
        object_storage: common::v2::ObjectStorage,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // S3 requires the access key id and the secret access key.
        let (Some(access_key_id), Some(access_key_secret), Some(region)) = (
            &object_storage.access_key_id,
            &object_storage.access_key_secret,
            &object_storage.region,
        ) else {
            return Err(ClientError::BackendError(Box::new(BackendError {
                message: format!(
                    "{} {}",
                    self.scheme,
                    make_need_fields_message!(object_storage {
                        access_key_id,
                        access_key_secret,
                        region
                    })
                ),
                status_code: None,
                header: None,
            })));
        };

        // Initialize the S3 operator with the object storage.
        let mut builder = opendal::services::S3::default();
        builder = builder
            .access_key_id(access_key_id)
            .secret_access_key(access_key_secret)
            .bucket(&parsed_url.bucket)
            .region(region);

        // Configure the endpoint if it is provided.
        if let Some(endpoint) = object_storage.endpoint.as_deref() {
            builder = builder.endpoint(endpoint);
        }

        // Configure the session token if it is provided.
        if let Some(session_token) = object_storage.session_token.as_deref() {
            builder = builder.session_token(session_token);
        }

        Ok(Operator::new(builder)?
            .finish()
            .layer(TimeoutLayer::new().with_timeout(timeout))
            .layer(HttpClientLayer::new(HttpClient::with(self.client.clone()))))
    }

    /// gcs_operator initializes the GCS operator with the parsed URL and object storage.
    pub fn gcs_operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
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

        // Configure the endpoint if it is provided.
        if let Some(endpoint) = object_storage.endpoint.as_deref() {
            builder = builder.endpoint(endpoint);
        }

        // Configure the predefined ACL if it is provided.
        if let Some(predefined_acl) = object_storage.predefined_acl.as_deref() {
            builder = builder.predefined_acl(predefined_acl);
        }

        Ok(Operator::new(builder)?
            .finish()
            .layer(TimeoutLayer::new().with_timeout(timeout))
            .layer(HttpClientLayer::new(HttpClient::with(self.client.clone()))))
    }

    /// abs_operator initializes the ABS operator with the parsed URL and object storage.
    pub fn abs_operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
        object_storage: common::v2::ObjectStorage,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // ABS requires the account name and the account key.
        let (Some(access_key_id), Some(access_key_secret), Some(endpoint)) = (
            &object_storage.access_key_id,
            &object_storage.access_key_secret,
            &object_storage.endpoint,
        ) else {
            return Err(ClientError::BackendError(Box::new(BackendError {
                message: format!(
                    "{} {}",
                    self.scheme,
                    make_need_fields_message!(object_storage {
                        access_key_id,
                        access_key_secret,
                        endpoint
                    })
                ),
                status_code: None,
                header: None,
            })));
        };

        // Initialize the ABS operator with the object storage.
        let mut builder = opendal::services::Azblob::default();
        builder = builder
            .account_name(access_key_id)
            .account_key(access_key_secret)
            .container(&parsed_url.bucket)
            .endpoint(endpoint);

        Ok(Operator::new(builder)?
            .finish()
            .layer(TimeoutLayer::new().with_timeout(timeout))
            .layer(HttpClientLayer::new(HttpClient::with(self.client.clone()))))
    }

    /// oss_operator initializes the OSS operator with the parsed URL and object storage.
    pub fn oss_operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
        object_storage: common::v2::ObjectStorage,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // OSS requires the access key id, access key secret, and endpoint.
        let (Some(access_key_id), Some(access_key_secret), Some(endpoint)) = (
            &object_storage.access_key_id,
            &object_storage.access_key_secret,
            &object_storage.endpoint,
        ) else {
            return Err(ClientError::BackendError(Box::new(BackendError {
                message: format!(
                    "{} {}",
                    self.scheme,
                    make_need_fields_message!(object_storage {
                        access_key_id,
                        access_key_secret,
                        endpoint
                    })
                ),
                status_code: None,
                header: None,
            })));
        };

        // Initialize the OSS operator with the object storage.
        let mut builder = opendal::services::Oss::default();
        builder = if let Some(security_token) = &object_storage.security_token {
            builder
                .access_key_id(access_key_id)
                .access_key_secret(access_key_secret)
                .endpoint(endpoint)
                .root("/")
                .bucket(&parsed_url.bucket)
                .security_token(security_token)
        } else {
            builder
                .access_key_id(access_key_id)
                .access_key_secret(access_key_secret)
                .endpoint(endpoint)
                .root("/")
                .bucket(&parsed_url.bucket)
        };

        Ok(Operator::new(builder)?
            .finish()
            .layer(TimeoutLayer::new().with_timeout(timeout))
            .layer(HttpClientLayer::new(HttpClient::with(self.client.clone()))))
    }

    /// obs_operator initializes the OBS operator with the parsed URL and object storage.
    pub fn obs_operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
        object_storage: common::v2::ObjectStorage,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // OBS requires the endpoint, access key id, and access key secret.
        let (Some(access_key_id), Some(access_key_secret), Some(endpoint)) = (
            &object_storage.access_key_id,
            &object_storage.access_key_secret,
            &object_storage.endpoint,
        ) else {
            return Err(ClientError::BackendError(Box::new(BackendError {
                message: format!(
                    "{} {}",
                    self.scheme,
                    make_need_fields_message!(object_storage {
                        access_key_id,
                        access_key_secret,
                        endpoint
                    })
                ),
                status_code: None,
                header: None,
            })));
        };

        // Initialize the OBS operator with the object storage.
        let mut builder = opendal::services::Obs::default();
        builder = builder
            .access_key_id(access_key_id)
            .secret_access_key(access_key_secret)
            .endpoint(endpoint)
            .bucket(&parsed_url.bucket);

        Ok(Operator::new(builder)?
            .finish()
            .layer(TimeoutLayer::new().with_timeout(timeout))
            .layer(HttpClientLayer::new(HttpClient::with(self.client.clone()))))
    }

    /// cos_operator initializes the COS operator with the parsed URL and object storage.
    pub fn cos_operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
        object_storage: common::v2::ObjectStorage,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // COS requires the access key id, the access key secret, and the endpoint.
        let (Some(access_key_id), Some(access_key_secret), Some(endpoint)) = (
            &object_storage.access_key_id,
            &object_storage.access_key_secret,
            &object_storage.endpoint,
        ) else {
            return Err(ClientError::BackendError(Box::new(BackendError {
                message: format!(
                    "{} {}",
                    self.scheme,
                    make_need_fields_message!(object_storage {
                        access_key_id,
                        access_key_secret,
                        endpoint
                    })
                ),
                status_code: None,
                header: None,
            })));
        };

        // Initialize the COS operator with the object storage.
        let mut builder = opendal::services::Cos::default();
        builder = builder
            .secret_id(access_key_id)
            .secret_key(access_key_secret)
            .endpoint(endpoint)
            .bucket(&parsed_url.bucket);

        Ok(Operator::new(builder)?
            .finish()
            .layer(TimeoutLayer::new().with_timeout(timeout))
            .layer(HttpClientLayer::new(HttpClient::with(self.client.clone()))))
    }
}

/// Backend implements the Backend trait.
#[tonic::async_trait]
impl crate::Backend for ObjectStorage {
    /// scheme returns the scheme of the object storage.
    fn scheme(&self) -> String {
        self.scheme.to_string()
    }

    /// head gets the header of the request.

    async fn head(&self, request: super::HeadRequest) -> ClientResult<super::HeadResponse> {
        debug!(
            "head request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        // Parse the URL and convert it to a ParsedURL for create the ObjectStorage operator.
        let url: Url = request
            .url
            .parse()
            .map_err(|_| ClientError::InvalidURI(request.url.clone()))?;
        let parsed_url: super::object_storage::ParsedURL = url.try_into().inspect_err(|err| {
            error!(
                "parse head request url failed {} {}: {}",
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
                    super::DirEntry {
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
            "head response {} {}: {}",
            request.task_id,
            request.url,
            response.content_length()
        );

        Ok(super::HeadResponse {
            success: true,
            content_length: Some(response.content_length()),
            http_header: None,
            http_status_code: None,
            error_message: None,
            entries,
        })
    }

    /// get returns content of requested file.

    async fn get(
        &self,
        request: super::GetRequest,
    ) -> ClientResult<super::GetResponse<super::Body>> {
        debug!(
            "get request {} {}: {:?}",
            request.piece_id, request.url, request.http_header
        );

        // Parse the URL and convert it to a ParsedURL for create the ObjectStorage operator.
        let url: Url = request
            .url
            .parse()
            .map_err(|_| ClientError::InvalidURI(request.url.clone()))?;
        let parsed_url: super::object_storage::ParsedURL = url.try_into().inspect_err(|err| {
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
            reader: Box::new(StreamReader::new(stream)),
            error_message: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_api::common::v2::ObjectStorage as ObjectStorageInfo;

    #[test]
    fn should_get_parsed_url() {
        let file_key = "test-bucket/file";
        let dir_key = "test-bucket/path/to/dir/";
        let schemes = vec![
            Scheme::OBS,
            Scheme::S3,
            Scheme::ABS,
            Scheme::OSS,
            Scheme::COS,
            Scheme::GCS,
        ];

        // Test each scheme for both file and directory URLs.
        for scheme in schemes {
            let file_url = format!("{}://{}", scheme, file_key);
            let url: Url = file_url.parse().unwrap();
            let parsed_url: ParsedURL = url.try_into().unwrap();

            assert!(!parsed_url.is_dir());
            assert_eq!(parsed_url.bucket, "test-bucket");
            assert_eq!(parsed_url.key, "file");
            assert_eq!(parsed_url.scheme, scheme);

            let dir_url = format!("{}://{}", scheme, dir_key);
            let url: Url = dir_url.parse().unwrap();
            let parsed_url: ParsedURL = url.try_into().unwrap();

            assert!(parsed_url.is_dir());
            assert_eq!(parsed_url.bucket, "test-bucket");
            assert_eq!(parsed_url.key, "path/to/dir/");
            assert_eq!(parsed_url.scheme, scheme);
        }
    }

    #[test]
    fn should_get_url_with_the_same_prefix() {
        let file_key = "test-bucket/file";
        let schemes = vec![
            Scheme::OBS,
            Scheme::S3,
            Scheme::ABS,
            Scheme::OSS,
            Scheme::COS,
            Scheme::GCS,
        ];

        // Test each scheme for both file and directory URLs.
        for scheme in schemes {
            let file_url = format!("{}://{}", scheme, file_key);
            let url: Url = file_url.parse().unwrap();
            let parsed_url: ParsedURL = url.try_into().unwrap();

            let new_url = parsed_url.make_url_by_entry_path("test-entry");
            let new_parsed_url: ParsedURL = new_url.try_into().unwrap();

            assert_eq!(parsed_url.bucket, new_parsed_url.bucket);
            assert_eq!(parsed_url.scheme, new_parsed_url.scheme);
            assert_eq!(new_parsed_url.key, "test-entry");
        }
    }

    #[test]
    fn should_return_error_when_scheme_not_valid() {
        let url: Url = "github://test-bucket/file".parse().unwrap();
        let result = TryInto::<ParsedURL>::try_into(url);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ClientError::InvalidURI(..)));
    }

    #[test]
    fn should_return_error_when_bucket_not_valid() {
        let schemes = vec![
            Scheme::OBS,
            Scheme::S3,
            Scheme::ABS,
            Scheme::OSS,
            Scheme::COS,
            Scheme::GCS,
        ];

        for scheme in schemes {
            let url: Url = format!("{}:///file", scheme).parse().unwrap();
            let result = TryInto::<ParsedURL>::try_into(url);

            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), ClientError::InvalidURI(..)));
        }
    }

    #[test]
    fn should_get_operator() {
        let test_cases = vec![
            (
                Scheme::S3,
                ObjectStorageInfo {
                    region: Some("test-region".into()),
                    access_key_id: Some("access-key-id".into()),
                    access_key_secret: Some("access-key-secret".into()),
                    ..Default::default()
                },
            ),
            (Scheme::GCS, ObjectStorageInfo::default()),
            (
                Scheme::ABS,
                ObjectStorageInfo {
                    endpoint: Some("test-endpoint.local".into()),
                    access_key_id: Some("access-key-id".into()),
                    access_key_secret: Some("YWNjZXNzLWtleS1zZWNyZXQK".into()),
                    ..Default::default()
                },
            ),
            (
                Scheme::OSS,
                ObjectStorageInfo {
                    endpoint: Some("test-endpoint.local".into()),
                    access_key_id: Some("access-key-id".into()),
                    access_key_secret: Some("access-key-secret".into()),
                    ..Default::default()
                },
            ),
            (
                Scheme::OBS,
                ObjectStorageInfo {
                    endpoint: Some("test-endpoint.local".into()),
                    access_key_id: Some("access-key-id".into()),
                    access_key_secret: Some("access-key-secret".into()),
                    ..Default::default()
                },
            ),
            (
                Scheme::COS,
                ObjectStorageInfo {
                    endpoint: Some("test-endpoint.local".into()),
                    access_key_id: Some("access-key-id".into()),
                    access_key_secret: Some("access-key-secret".into()),
                    ..Default::default()
                },
            ),
        ];

        for (scheme, object_storage) in test_cases {
            let url: Url = format!("{}://test-bucket/file", scheme).parse().unwrap();
            let parsed_url: ParsedURL = url.try_into().unwrap();

            let result = ObjectStorage::new(scheme).unwrap().operator(
                &parsed_url,
                Some(object_storage),
                Duration::from_secs(3),
            );

            assert!(
                result.is_ok(),
                "can not get {} operator, due to: {}",
                scheme,
                result.unwrap_err()
            );
        }
    }

    #[test]
    fn should_get_s3_operator_with_extra_info() {
        let test_cases = vec![
            ObjectStorageInfo {
                access_key_id: Some("access_key_id".into()),
                access_key_secret: Some("access_key_secret".into()),
                region: Some("test-region".into()),

                endpoint: Some("test-endpoint.local".into()),
                ..Default::default()
            },
            ObjectStorageInfo {
                access_key_id: Some("access_key_id".into()),
                access_key_secret: Some("access_key_secret".into()),
                region: Some("test-region".into()),

                session_token: Some("session_token".into()),
                ..Default::default()
            },
            ObjectStorageInfo {
                access_key_id: Some("access_key_id".into()),
                access_key_secret: Some("access_key_secret".into()),
                region: Some("test-region".into()),

                endpoint: Some("test-endpoint.local".into()),
                session_token: Some("session_token".into()),
                ..Default::default()
            },
        ];

        for object_storage in test_cases {
            let url: Url = "s3://test-bucket/file".parse().unwrap();
            let parsed_url: ParsedURL = url.try_into().unwrap();

            let result = ObjectStorage::new(Scheme::S3).unwrap().operator(
                &parsed_url,
                Some(object_storage),
                Duration::from_secs(3),
            );

            assert!(result.is_ok());
            assert_eq!(result.unwrap().info().scheme().to_string(), "s3");
        }
    }

    #[test]
    fn should_get_gcs_operator_with_extra_info() {
        let test_cases = vec![
            ObjectStorageInfo {
                credential_path: Some("credential_path".into()),
                ..Default::default()
            },
            ObjectStorageInfo {
                endpoint: Some("test-endpoint".into()),
                ..Default::default()
            },
            ObjectStorageInfo {
                predefined_acl: Some("predefine_acl".into()),
                ..Default::default()
            },
            ObjectStorageInfo {
                credential_path: Some("credential_path".into()),
                endpoint: Some("test-endpoint".into()),
                ..Default::default()
            },
            ObjectStorageInfo {
                credential_path: Some("credential_path".into()),
                predefined_acl: Some("predefine_acl".into()),
                ..Default::default()
            },
            ObjectStorageInfo {
                endpoint: Some("test-endpoint".into()),
                predefined_acl: Some("predefine_acl".into()),
                ..Default::default()
            },
            ObjectStorageInfo {
                credential_path: Some("credential_path".into()),
                endpoint: Some("test-endpoint".into()),
                predefined_acl: Some("predefine_acl".into()),
                ..Default::default()
            },
        ];

        for object_storage in test_cases {
            let url: Url = "gs://test-bucket/file".parse().unwrap();
            let parsed_url: ParsedURL = url.try_into().unwrap();

            let result = ObjectStorage::new(Scheme::GCS).unwrap().operator(
                &parsed_url,
                Some(object_storage),
                Duration::from_secs(3),
            );

            assert!(result.is_ok());
            assert_eq!(result.unwrap().info().scheme().to_string(), "gcs");
        }
    }

    #[test]
    fn should_return_error_when_lacks_of_info() {
        let url: Url = "s3://test-bucket/file".parse().unwrap();
        let parsed_url: ParsedURL = url.try_into().unwrap();

        let result = ObjectStorage::new(Scheme::S3).unwrap().operator(
            &parsed_url,
            None,
            Duration::from_secs(3),
        );

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "backend error: s3 need object_storage parameter"
        )
    }

    #[test]
    fn should_return_error_when_s3_lacks_of_info() {
        let test_cases = vec![
            (
                ObjectStorageInfo::default(),
                "backend error: s3 need access_key_id, access_key_secret, region",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    ..Default::default()
                },
                "backend error: s3 need access_key_secret, region",
            ),
            (
                ObjectStorageInfo {
                    access_key_secret: Some("access_key_secret".into()),
                    ..Default::default()
                },
                "backend error: s3 need access_key_id, region",
            ),
            (
                ObjectStorageInfo {
                    region: Some("test-region".into()),
                    ..Default::default()
                },
                "backend error: s3 need access_key_id, access_key_secret",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    access_key_secret: Some("access_key_secret".into()),
                    ..Default::default()
                },
                "backend error: s3 need region",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    region: Some("test-region".into()),
                    ..Default::default()
                },
                "backend error: s3 need access_key_secret",
            ),
            (
                ObjectStorageInfo {
                    access_key_secret: Some("access_key_secret".into()),
                    region: Some("test-region".into()),
                    ..Default::default()
                },
                "backend error: s3 need access_key_id",
            ),
        ];

        for (object_storage, error_message) in test_cases {
            let url: Url = "s3://test-bucket/file".parse().unwrap();
            let parsed_url: ParsedURL = url.try_into().unwrap();

            let result = ObjectStorage::new(Scheme::S3).unwrap().operator(
                &parsed_url,
                Some(object_storage),
                Duration::from_secs(3),
            );

            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), error_message);
        }
    }

    #[test]
    fn should_return_error_when_abs_lacks_of_info() {
        let test_cases = vec![
            (
                ObjectStorageInfo::default(),
                "backend error: abs need access_key_id, access_key_secret, endpoint",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    ..Default::default()
                },
                "backend error: abs need access_key_secret, endpoint",
            ),
            (
                ObjectStorageInfo {
                    access_key_secret: Some("access_key_secret".into()),
                    ..Default::default()
                },
                "backend error: abs need access_key_id, endpoint",
            ),
            (
                ObjectStorageInfo {
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "backend error: abs need access_key_id, access_key_secret",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    access_key_secret: Some("access_key_secret".into()),
                    ..Default::default()
                },
                "backend error: abs need endpoint",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "backend error: abs need access_key_secret",
            ),
            (
                ObjectStorageInfo {
                    access_key_secret: Some("access_key_secret".into()),
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "backend error: abs need access_key_id",
            ),
        ];

        for (object_storage, error_message) in test_cases {
            let url: Url = "abs://test-bucket/file".parse().unwrap();
            let parsed_url: ParsedURL = url.try_into().unwrap();

            let result = ObjectStorage::new(Scheme::ABS).unwrap().operator(
                &parsed_url,
                Some(object_storage),
                Duration::from_secs(3),
            );

            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), error_message);
        }
    }

    #[test]
    fn should_return_error_when_oss_lacks_of_info() {
        let test_cases = vec![
            (
                ObjectStorageInfo::default(),
                "backend error: oss need access_key_id, access_key_secret, endpoint",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    ..Default::default()
                },
                "backend error: oss need access_key_secret, endpoint",
            ),
            (
                ObjectStorageInfo {
                    access_key_secret: Some("access_key_secret".into()),
                    ..Default::default()
                },
                "backend error: oss need access_key_id, endpoint",
            ),
            (
                ObjectStorageInfo {
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "backend error: oss need access_key_id, access_key_secret",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    access_key_secret: Some("access_key_secret".into()),
                    ..Default::default()
                },
                "backend error: oss need endpoint",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "backend error: oss need access_key_secret",
            ),
            (
                ObjectStorageInfo {
                    access_key_secret: Some("access_key_secret".into()),
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "backend error: oss need access_key_id",
            ),
        ];

        for (object_storage, error_message) in test_cases {
            let url: Url = "oss://test-bucket/file".parse().unwrap();
            let parsed_url: ParsedURL = url.try_into().unwrap();

            let result = ObjectStorage::new(Scheme::OSS).unwrap().operator(
                &parsed_url,
                Some(object_storage),
                Duration::from_secs(3),
            );

            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), error_message);
        }
    }

    #[test]
    fn should_return_error_when_obs_lacks_of_info() {
        let test_cases = vec![
            (
                ObjectStorageInfo::default(),
                "backend error: obs need access_key_id, access_key_secret, endpoint",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    ..Default::default()
                },
                "backend error: obs need access_key_secret, endpoint",
            ),
            (
                ObjectStorageInfo {
                    access_key_secret: Some("access_key_secret".into()),
                    ..Default::default()
                },
                "backend error: obs need access_key_id, endpoint",
            ),
            (
                ObjectStorageInfo {
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "backend error: obs need access_key_id, access_key_secret",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    access_key_secret: Some("access_key_secret".into()),
                    ..Default::default()
                },
                "backend error: obs need endpoint",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "backend error: obs need access_key_secret",
            ),
            (
                ObjectStorageInfo {
                    access_key_secret: Some("access_key_secret".into()),
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "backend error: obs need access_key_id",
            ),
        ];

        for (object_storage, error_message) in test_cases {
            let url: Url = "obs://test-bucket/file".parse().unwrap();
            let parsed_url: ParsedURL = url.try_into().unwrap();

            let result = ObjectStorage::new(Scheme::OBS).unwrap().operator(
                &parsed_url,
                Some(object_storage),
                Duration::from_secs(3),
            );

            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), error_message);
        }
    }

    #[test]
    fn should_return_error_when_cos_lacks_of_info() {
        let test_cases = vec![
            (
                ObjectStorageInfo::default(),
                "backend error: cos need access_key_id, access_key_secret, endpoint",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    ..Default::default()
                },
                "backend error: cos need access_key_secret, endpoint",
            ),
            (
                ObjectStorageInfo {
                    access_key_secret: Some("access_key_secret".into()),
                    ..Default::default()
                },
                "backend error: cos need access_key_id, endpoint",
            ),
            (
                ObjectStorageInfo {
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "backend error: cos need access_key_id, access_key_secret",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    access_key_secret: Some("access_key_secret".into()),
                    ..Default::default()
                },
                "backend error: cos need endpoint",
            ),
            (
                ObjectStorageInfo {
                    access_key_id: Some("access_key_id".into()),
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "backend error: cos need access_key_secret",
            ),
            (
                ObjectStorageInfo {
                    access_key_secret: Some("access_key_secret".into()),
                    endpoint: Some("test-endpoint.local".into()),
                    ..Default::default()
                },
                "backend error: cos need access_key_id",
            ),
        ];

        for (object_storage, error_message) in test_cases {
            let url: Url = "cos://test-bucket/file".parse().unwrap();
            let parsed_url: ParsedURL = url.try_into().unwrap();

            let result = ObjectStorage::new(Scheme::COS).unwrap().operator(
                &parsed_url,
                Some(object_storage),
                Duration::from_secs(3),
            );

            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), error_message);
        }
    }
}
