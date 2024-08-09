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
use opendal::{raw::HttpClient, Metakey, Operator};
use percent_encoding::percent_decode_str;
use std::fmt;
use std::result::Result;
use std::str::FromStr;
use std::time::Duration;
use tokio_util::io::StreamReader;
use tracing::{error, info};
use url::Url;

// Scheme is the scheme of the object storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    // S3 is the Amazon Simple Storage Service.
    S3,

    // GCS is the Google Cloud Storage Service.
    GCS,

    // ABS is the Azure Blob Storage Service.
    ABS,

    // OSS is the Aliyun Object Storage Service.
    OSS,

    // OBS is the Huawei Cloud Object Storage Service.
    OBS,

    // COS is the Tencent Cloud Object Storage Service.
    COS,
}

// Scheme implements the Display.
impl fmt::Display for Scheme {
    // fmt formats the value using the given formatter.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scheme::S3 => write!(f, "s3"),
            Scheme::GCS => write!(f, "gcs"),
            Scheme::ABS => write!(f, "abs"),
            Scheme::OSS => write!(f, "oss"),
            Scheme::OBS => write!(f, "obs"),
            Scheme::COS => write!(f, "cos"),
        }
    }
}

// Scheme implements the FromStr.
impl FromStr for Scheme {
    type Err = String;

    // from_str parses an scheme string.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "s3" => Ok(Scheme::S3),
            "gcs" => Ok(Scheme::GCS),
            "abs" => Ok(Scheme::ABS),
            "oss" => Ok(Scheme::OSS),
            "obs" => Ok(Scheme::OBS),
            "cos" => Ok(Scheme::COS),
            _ => Err(format!("invalid scheme: {}", s)),
        }
    }
}

// ParsedURL is a struct that contains the parsed URL, bucket, and path.
#[derive(Debug)]
pub struct ParsedURL {
    // url is the requested URL of the object storage.
    pub url: Url,

    // scheme is the scheme of the object storage.
    pub scheme: Scheme,

    // bucket is the bucket of the object storage.
    pub bucket: String,

    // key is the key of the object storage.
    pub key: String,
}

// ParsedURL implements the ParsedURL trait.
impl ParsedURL {
    // is_dir returns true if the URL path ends with a slash.
    pub fn is_dir(&self) -> bool {
        self.url.path().ends_with('/')
    }

    // make_url_by_entry_path makes a URL by the entry path when the URL is a directory.
    pub fn make_url_by_entry_path(&self, entry_path: &str) -> Url {
        let mut url = self.url.clone();
        url.set_path(entry_path);
        url
    }
}

// ParsedURL implements the TryFrom trait for the URL.
//
// The object storage URL should be in the format of `scheme://<bucket>/<path>`.
impl TryFrom<Url> for ParsedURL {
    type Error = ClientError;

    // try_from parses the URL and returns a ParsedURL.
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

// ObjectStorage is a struct that implements the backend trait.
pub struct ObjectStorage {
    // scheme is the scheme of the object storage.
    scheme: Scheme,
}

// ObjectStorage implements the ObjectStorage trait.
impl ObjectStorage {
    /// Returns ObjectStorage that implements the Backend trait.
    pub fn new(scheme: Scheme) -> ObjectStorage {
        Self { scheme }
    }

    // operator initializes the operator with the parsed URL and object storage.
    pub fn operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
        object_storage: Option<common::v2::ObjectStorage>,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        match self.scheme {
            Scheme::S3 => self.s3_operator(parsed_url, object_storage, timeout),
            Scheme::GCS => self.gcs_operator(parsed_url, object_storage, timeout),
            Scheme::ABS => self.abs_operator(parsed_url, object_storage, timeout),
            Scheme::OSS => self.oss_operator(parsed_url, object_storage, timeout),
            Scheme::OBS => self.obs_operator(parsed_url, object_storage, timeout),
            Scheme::COS => self.cos_operator(parsed_url, object_storage, timeout),
        }
    }

    // s3_operator initializes the S3 operator with the parsed URL and object storage.
    pub fn s3_operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
        object_storage: Option<common::v2::ObjectStorage>,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // Check if the object storage is provided.
        let Some(object_storage) = object_storage else {
            error!("need access_key_id and access_key_secret");
            return Err(ClientError::BackendError(BackendError {
                message: "need access_key_id and access_key_secret".to_string(),
                status_code: None,
                header: None,
            }));
        };

        // Create a reqwest http client.
        let client = reqwest::Client::builder().timeout(timeout).build()?;

        // Initialize the S3 operator with the object storage.
        let mut builder = opendal::services::S3::default();
        builder = builder
            .access_key_id(&object_storage.access_key_id)
            .secret_access_key(&object_storage.access_key_secret)
            .http_client(HttpClient::with(client))
            .bucket(&parsed_url.bucket);

        // Configure the region and endpoint if they are provided.
        if let Some(region) = object_storage.region.as_deref() {
            builder = builder.region(region);
        }

        // Configure the endpoint if it is provided.
        if let Some(endpoint) = object_storage.endpoint.as_deref() {
            builder = builder.endpoint(endpoint);
        }

        // Configure the session token if it is provided.
        if let Some(session_token) = object_storage.session_token.as_deref() {
            builder = builder.session_token(session_token);
        }

        Ok(Operator::new(builder)?.finish())
    }

    // gcs_operator initializes the GCS operator with the parsed URL and object storage.
    pub fn gcs_operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
        object_storage: Option<common::v2::ObjectStorage>,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // Check if the object storage is provided.
        let Some(object_storage) = object_storage else {
            error!("need credential");
            return Err(ClientError::BackendError(BackendError {
                message: "need credential".to_string(),
                status_code: None,
                header: None,
            }));
        };

        // Create a reqwest http client.
        let client = reqwest::Client::builder().timeout(timeout).build()?;

        // Initialize the GCS operator with the object storage.
        let mut builder = opendal::services::Gcs::default();
        builder = builder
            .http_client(HttpClient::with(client))
            .bucket(&parsed_url.bucket);

        // Configure the region and endpoint if they are provided.
        if let Some(credential) = object_storage.credential.as_deref() {
            builder = builder.credential(credential);
        } else {
            error!("need credential");
            return Err(ClientError::BackendError(BackendError {
                message: "need credential".to_string(),
                status_code: None,
                header: None,
            }));
        }

        // Configure the predefined ACL if it is provided.
        if let Some(predefined_acl) = object_storage.predefined_acl.as_deref() {
            builder = builder.predefined_acl(predefined_acl);
        }

        Ok(Operator::new(builder)?.finish())
    }

    // abs_operator initializes the ABS operator with the parsed URL and object storage.
    pub fn abs_operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
        object_storage: Option<common::v2::ObjectStorage>,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // Check if the object storage is provided.
        let Some(object_storage) = object_storage else {
            error!("need access_key_id and access_key_secret");
            return Err(ClientError::BackendError(BackendError {
                message: "need access_key_id and access_key_secret".to_string(),
                status_code: None,
                header: None,
            }));
        };

        // Create a reqwest http client.
        let client = reqwest::Client::builder().timeout(timeout).build()?;

        // Initialize the ABS operator with the object storage.
        let mut builder = opendal::services::Azblob::default();
        builder = builder
            .account_name(&object_storage.access_key_id)
            .account_key(&object_storage.access_key_secret)
            .http_client(HttpClient::with(client))
            .container(&parsed_url.bucket);

        // Configure the endpoint if it is provided.
        if let Some(endpoint) = object_storage.endpoint.as_deref() {
            builder = builder.endpoint(endpoint);
        }

        Ok(Operator::new(builder)?.finish())
    }

    // oss_operator initializes the OSS operator with the parsed URL and object storage.
    pub fn oss_operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
        object_storage: Option<common::v2::ObjectStorage>,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // Check if the object storage is provided.
        let Some(object_storage) = object_storage else {
            error!("need endpoint, access_key_id and access_key_secret");
            return Err(ClientError::BackendError(BackendError {
                message: "need endpoint, access_key_id and access_key_secret".to_string(),
                status_code: None,
                header: None,
            }));
        };

        #[allow(unused_mut)]
        let mut client_builder = reqwest::Client::builder().timeout(timeout);

        // Due to opendal must set bucket and endpoint to build the url, so ip address can not
        // be used.
        // Concequently, use `ClientBuilder::resolve` to resolve a host to mock server ip address.
        #[cfg(test)]
        {
            client_builder = client_builder.resolve(
                "test-bucket.test-endpoint.local",
                std::net::SocketAddr::new([127, 0, 0, 1].into(), 8080),
            );
        }

        // Create a reqwest http client.
        let client = client_builder.build()?;

        // Initialize the OSS operator with the object storage.
        let mut builder = opendal::services::Oss::default();
        builder = builder
            .access_key_id(&object_storage.access_key_id)
            .access_key_secret(&object_storage.access_key_secret)
            .http_client(HttpClient::with(client))
            .root("/")
            .bucket(&parsed_url.bucket);

        // Configure the endpoint if provided.
        if let Some(endpoint) = object_storage.endpoint {
            builder = builder.endpoint(&endpoint);
        } else {
            error!("need endpoint");
            return Err(ClientError::BackendError(BackendError {
                message: "need endpoint".to_string(),
                status_code: None,
                header: None,
            }));
        }

        Ok(Operator::new(builder)?.finish())
    }

    // obs_operator initializes the OBS operator with the parsed URL and object storage.
    pub fn obs_operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
        object_storage: Option<common::v2::ObjectStorage>,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // Check if the object storage is provided.
        let Some(object_storage) = object_storage else {
            error!("need endpoint, access_key_id and access_key_secret");
            return Err(ClientError::BackendError(BackendError {
                message: "need endpoint, access_key_id and access_key_secret".to_string(),
                status_code: None,
                header: None,
            }));
        };

        // Create a reqwest http client.
        let client = reqwest::Client::builder().timeout(timeout).build()?;

        // Initialize the OBS operator with the object storage.
        let mut builder = opendal::services::Obs::default();
        builder = builder
            .access_key_id(&object_storage.access_key_id)
            .secret_access_key(&object_storage.access_key_secret)
            .http_client(HttpClient::with(client))
            .bucket(&parsed_url.bucket);

        // Configure the endpoint if provided.
        if let Some(endpoint) = object_storage.endpoint {
            builder = builder.endpoint(&endpoint);
        }

        Ok(Operator::new(builder)?.finish())
    }

    // cos_operator initializes the COS operator with the parsed URL and object storage.
    pub fn cos_operator(
        &self,
        parsed_url: &super::object_storage::ParsedURL,
        object_storage: Option<common::v2::ObjectStorage>,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // Check if the object storage is provided.
        let Some(object_storage) = object_storage else {
            error!("need endpoint, access_key_id and access_key_secret");
            return Err(ClientError::BackendError(BackendError {
                message: "need endpoint, access_key_id and access_key_secret".to_string(),
                status_code: None,
                header: None,
            }));
        };

        // Create a reqwest http client.
        let client = reqwest::Client::builder().timeout(timeout).build()?;

        // Initialize the COS operator with the object storage.
        let mut builder = opendal::services::Cos::default();
        builder = builder
            .secret_id(&object_storage.access_key_id)
            .secret_key(&object_storage.access_key_secret)
            .http_client(HttpClient::with(client))
            .bucket(&parsed_url.bucket);

        // Configure the endpoint if provided.
        if let Some(endpoint) = object_storage.endpoint {
            builder = builder.endpoint(&endpoint);
        }

        Ok(Operator::new(builder)?.finish())
    }
}

// Backend implements the Backend trait.
#[tonic::async_trait]
impl crate::Backend for ObjectStorage {
    //head gets the header of the request.
    async fn head(&self, request: super::HeadRequest) -> ClientResult<super::HeadResponse> {
        info!(
            "head request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        // Parse the URL and convert it to a ParsedURL for create the ObjectStorage operator.
        let url: Url = request
            .url
            .parse()
            .map_err(|_| ClientError::InvalidURI(request.url.clone()))?;
        let parsed_url: super::object_storage::ParsedURL = url.try_into().map_err(|err| {
            error!(
                "parse head request url failed {} {}: {}",
                request.task_id, request.url, err
            );
            err
        })?;

        // Initialize the operator with the parsed URL, object storage, and timeout.
        let operator = self.operator(&parsed_url, request.object_storage, request.timeout)?;

        // Get the entries if url point to a directory.
        let entries = if parsed_url.is_dir() {
            operator
                .list_with(&parsed_url.key)
                .recursive(true)
                .metakey(Metakey::ContentLength | Metakey::Mode)
                .await // Do the list op here.
                .map_err(|err| {
                    error!(
                        "list request failed {} {}: {}",
                        request.task_id, request.url, err
                    );
                    ClientError::BackendError(BackendError {
                        message: err.to_string(),
                        status_code: None,
                        header: None,
                    })
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
            ClientError::BackendError(BackendError {
                message: err.to_string(),
                status_code: None,
                header: None,
            })
        })?;

        info!(
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

    // Returns content of requested file.
    async fn get(
        &self,
        request: super::GetRequest,
    ) -> ClientResult<super::GetResponse<super::Body>> {
        info!(
            "get request {} {}: {:?}",
            request.piece_id, request.url, request.http_header
        );

        // Parse the URL and convert it to a ParsedURL for create the ObjectStorage operator.
        let url: Url = request
            .url
            .parse()
            .map_err(|_| ClientError::InvalidURI(request.url.clone()))?;
        let parsed_url: super::object_storage::ParsedURL = url.try_into().map_err(|err| {
            error!(
                "parse head request url failed {} {}: {}",
                request.piece_id, request.url, err
            );
            err
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
                ClientError::BackendError(BackendError {
                    message: err.to_string(),
                    status_code: None,
                    header: None,
                })
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
                    ClientError::BackendError(BackendError {
                        message: err.to_string(),
                        status_code: None,
                        header: None,
                    })
                })?,
            None => operator_reader.into_bytes_stream(..).await.map_err(|err| {
                error!(
                    "get request failed {} {}: {}",
                    request.piece_id, request.url, err
                );
                ClientError::BackendError(BackendError {
                    message: err.to_string(),
                    status_code: None,
                    header: None,
                })
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
    use std::net::TcpListener;

    use crate::{Backend, DirEntry, GetRequest, HeadRequest};

    use super::*;
    use common::v2::Range;
    use dragonfly_client_core::Error;
    use wiremock::{matchers::*, Mock, ResponseTemplate};

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

            // Assert that the file URL is parsed correctly.
            assert!(!parsed_url.is_dir());
            assert_eq!(parsed_url.bucket, "test-bucket");
            assert_eq!(parsed_url.key, "file");
            assert_eq!(parsed_url.scheme, scheme);

            let dir_url = format!("{}://{}", scheme, dir_key);

            let url: Url = dir_url.parse().unwrap();
            let parsed_url: ParsedURL = url.try_into().unwrap();

            // Assert that the directory URL is parsed correctly.
            assert!(parsed_url.is_dir());
            assert_eq!(parsed_url.bucket, "test-bucket");
            assert_eq!(parsed_url.key, "path/to/dir/");
            assert_eq!(parsed_url.scheme, scheme);
        }
    }

    #[test]
    fn should_return_error_when_scheme_not_valid() {
        let url: Url = "github://test-bucket/file".parse().unwrap();

        let result = TryInto::<ParsedURL>::try_into(url);

        // Assert that an invalid scheme returns an error.
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidURI(..)));
    }

    #[test]
    fn should_return_error_when_bucket_not_valid() {
        let url: Url = "oss:///file".parse().unwrap();

        let result = TryInto::<ParsedURL>::try_into(url);

        // Assert that an invalid bucket returns an error.
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidURI(..)));
    }

    #[test]
    fn should_get_oss_operator() {
        let url: Url = "oss://test-bucket/file".parse().unwrap();
        let parsed_url: ParsedURL = url.try_into().unwrap();

        let object_storage = dragonfly_api::common::v2::ObjectStorage {
            endpoint: Some("test-endpoint.local".into()),
            access_key_id: "access-key-id".into(),
            access_key_secret: "access-key-secret".into(),
            ..Default::default()
        };

        let result = ObjectStorage::new(Scheme::OSS).oss_operator(
            &parsed_url,
            Some(object_storage),
            Duration::from_secs(3),
        );

        // Assert that the OSS operator is successfully created.
        assert!(result.is_ok());
    }

    #[test]
    fn should_return_error_when_oss_aksk_not_provided() {
        let url: Url = "oss://test-bucket/file".parse().unwrap();
        let parsed_url: ParsedURL = url.try_into().unwrap();

        let result =
            ObjectStorage::new(Scheme::OSS).oss_operator(&parsed_url, None, Duration::from_secs(3));

        // Assert that missing access keys return an error.
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::BackendError(..)));
    }

    #[test]
    fn should_return_error_when_oss_endpoint_not_provided() {
        let url: Url = "oss://test-bucket/file".parse().unwrap();
        let parsed_url: ParsedURL = url.try_into().unwrap();

        let object_storage = dragonfly_api::common::v2::ObjectStorage {
            access_key_id: "access-key-id".into(),
            access_key_secret: "access-key-secret".into(),
            ..Default::default()
        };

        let result = ObjectStorage::new(Scheme::OSS).oss_operator(
            &parsed_url,
            Some(object_storage),
            Duration::from_secs(3),
        );

        // Assert that missing endpoint returns an error.
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::BackendError(..)));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn should_get_oss_head_response_of_file() {
        let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
        let server = wiremock::MockServer::builder()
            .listener(listener)
            .start()
            .await;

        // Mock the response.
        Mock::given(method("HEAD"))
            .and(path("/test.txt"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Server", "AliyunOSS")
                    .insert_header("Content-Type", "text/plain")
                    .insert_header("Content-Length", "24")
                    .insert_header("Connection", "keep-alive")
                    .insert_header("Accept-Ranges", "bytes")
                    .insert_header("Last-Modified", "Sun, 07 Jul 2024 04:12:50 GMT")
                    .insert_header("Content-MD5", "a9OkUn+nmTdHwZ/EuShY6A=="),
            )
            .mount(&server)
            .await;

        let url = "oss://test-bucket/test.txt";

        let object_storage = build_oss_info();
        let backend = ObjectStorage::new(Scheme::OSS);
        let head_response = backend
            .head(build_head_request(url, object_storage))
            .await
            .unwrap();

        assert!(head_response.success);
        assert_eq!(head_response.content_length, Some(24));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn should_get_oss_head_response_of_dir() {
        let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
        let server = wiremock::MockServer::builder()
            .listener(listener)
            .start()
            .await;

        // Mock the response.
        Mock::given(method("GET"))
            .and(path("/"))
            .and(query_param("list-type", "2"))
            .and(query_param("delimiter", ""))
            .and(query_param("prefix", "test_dir_recursive"))
            .and(query_param("max-keys", "1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Server", "AliyunOSS")
                    .insert_header("Content-Type", "application/xml")
                    .insert_header("Content-Length", "598")
                    .insert_header("Connection", "keep-alive")
                    .set_body_string(include_str!(
                        "../test-assets/should_get_oss_head_response_of_dir_stat.xml"
                    )),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/"))
            .and(query_param("list-type", "2"))
            .and(query_param("delimiter", ""))
            .and(query_param("prefix", "test_dir_recursive/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Server", "AliyunOSS")
                    .insert_header("Content-Type", "application/xml")
                    .insert_header("Content-Length", "2613")
                    .insert_header("Connection", "keep-alive")
                    .set_body_string(include_str!(
                        "../test-assets/should_get_oss_head_response_of_dir_list.xml"
                    )),
            )
            .mount(&server)
            .await;

        let entries = vec![
            DirEntry {
                url: "oss://test-bucket/test_dir_recursive/dir1/".into(),
                content_length: 0,
                is_dir: true,
            },
            DirEntry {
                url: "oss://test-bucket/test_dir_recursive/dir1/aaa.txt".into(),
                content_length: 16,
                is_dir: false,
            },
            DirEntry {
                url: "oss://test-bucket/test_dir_recursive/dir1/ccc.txt".into(),
                content_length: 16,
                is_dir: false,
            },
            DirEntry {
                url: "oss://test-bucket/test_dir_recursive/dir2/".into(),
                content_length: 0,
                is_dir: true,
            },
            DirEntry {
                url: "oss://test-bucket/test_dir_recursive/dir2/bbb.txt".into(),
                content_length: 16,
                is_dir: false,
            },
            DirEntry {
                url: "oss://test-bucket/test_dir_recursive/dir2/ddd.txt".into(),
                content_length: 16,
                is_dir: false,
            },
            DirEntry {
                url: "oss://test-bucket/test_dir_recursive/dir2/eee.txt".into(),
                content_length: 16,
                is_dir: false,
            },
            DirEntry {
                url: "oss://test-bucket/test_dir_recursive/test.txt".into(),
                content_length: 14,
                is_dir: false,
            },
        ];
        let url = "oss://test-bucket/test_dir_recursive/";

        let object_storage = build_oss_info();
        let backend = ObjectStorage::new(Scheme::OSS);
        let head_response = backend
            .head(build_head_request(url, object_storage))
            .await
            .unwrap();

        assert!(head_response.success);
        assert_eq!(head_response.content_length, Some(0));
        assert_eq!(head_response.entries, entries);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn should_return_error_response_when_head_inexists() {
        let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
        let server = wiremock::MockServer::builder()
            .listener(listener)
            .start()
            .await;

        // Mock the response.
        Mock::given(method("HEAD"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let file_url = "oss://test-bucket/test.txt";
        let dir_url = "oss://test-bucket/path/to/dir/";
        let backend = ObjectStorage::new(Scheme::OSS);

        let file_head_result = backend
            .head(build_head_request(file_url, build_oss_info()))
            .await;
        assert!(file_head_result.is_err());
        assert!(matches!(
            file_head_result.unwrap_err(),
            Error::BackendError(..)
        ));

        let dir_head_result = backend
            .head(build_head_request(dir_url, build_oss_info()))
            .await;
        assert!(dir_head_result.is_err());
        assert!(matches!(
            dir_head_result.unwrap_err(),
            Error::BackendError(..)
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn should_get_oss_get_response() {
        let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
        let server = wiremock::MockServer::builder()
            .listener(listener)
            .start()
            .await;

        // Mock the response.
        Mock::given(method("HEAD"))
            .and(path("/test.txt"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Server", "AliyunOSS")
                    .insert_header("Content-Type", "text/plain")
                    .insert_header("Content-Length", "24")
                    .insert_header("Connection", "keep-alive")
                    .insert_header("Accept-Ranges", "bytes")
                    .insert_header("Last-Modified", "Sun, 07 Jul 2024 04:12:50 GMT")
                    .insert_header("x-oss-hash-crc64ecma", "11089307625450993923")
                    .insert_header("Content-MD5", "a9OkUn+nmTdHwZ/EuShY6A=="),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/test.txt"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Server", "AliyunOSS")
                    .insert_header("Content-Type", "text/plain")
                    .insert_header("Content-Length", "24")
                    .insert_header("Connection", "keep-alive")
                    .insert_header("Content-Range", "bytes 0-23/24")
                    .insert_header("Accept-Ranges", "bytes")
                    .insert_header("Last-Modified", "Sun, 07 Jul 2024 04:12:50 GMT")
                    .insert_header("x-oss-hash-crc64ecma", "11089307625450993923")
                    .insert_header("Content-MD5", "a9OkUn+nmTdHwZ/EuShY6A==")
                    .set_body_string("Hello, world!\nVersion 2."),
            )
            .mount(&server)
            .await;

        let url = "oss://test-bucket/test.txt";

        let object_storage = build_oss_info();
        let backend = ObjectStorage::new(Scheme::OSS);
        let mut get_response = backend
            .get(build_get_request(url, None, object_storage))
            .await
            .unwrap();

        assert!(get_response.success);
        assert_eq!(
            get_response.text().await.unwrap(),
            "Hello, world!\nVersion 2.".to_string()
        )
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn should_get_oss_get_response_with_range() {
        let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
        let server = wiremock::MockServer::builder()
            .listener(listener)
            .start()
            .await;

        // Mock the response.
        Mock::given(method("GET"))
            .and(path("/test.txt"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Server", "AliyunOSS")
                    .insert_header("Content-Type", "text/plain")
                    .insert_header("Content-Length", "6")
                    .insert_header("Connection", "keep-alive")
                    .insert_header("Content-Range", "bytes 1-6/24")
                    .insert_header("Accept-Ranges", "bytes")
                    .insert_header("Last-Modified", "Sun, 07 Jul 2024 04:12:50 GMT")
                    .insert_header("x-oss-hash-crc64ecma", "11089307625450993923")
                    .set_body_string("ello, "),
            )
            .mount(&server)
            .await;

        let url = "oss://test-bucket/test.txt";

        let object_storage = build_oss_info();
        let backend = ObjectStorage::new(Scheme::OSS);
        let mut get_response = backend
            .get(build_get_request(
                url,
                Some(Range {
                    start: 1,
                    length: 6,
                }),
                object_storage,
            ))
            .await
            .unwrap();

        assert!(get_response.success);
        assert_eq!(get_response.text().await.unwrap(), "ello, ".to_string())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn should_return_error_response_when_get_inexists() {
        let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
        let server = wiremock::MockServer::builder()
            .listener(listener)
            .start()
            .await;

        // Mock the response.
        Mock::given(method("HEAD"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let url = "oss://test-bucket/test.txt";
        let backend = ObjectStorage::new(Scheme::OSS);

        let result = backend
            .get(build_get_request(url, None, build_oss_info()))
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn should_get_oss_head_response_of_file_with_non_ascii() {
        let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
        let server = wiremock::MockServer::builder()
            .listener(listener)
            .start()
            .await;

        // Mock the response.
        Mock::given(method("HEAD"))
            .and(path(
                "/test_dir_non_recursive/%F0%9F%8E%89%E5%9B%BE%E7%89%87%F0%9F%8E%89.jpg",
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Server", "AliyunOSS")
                    .insert_header("Content-Type", "image/jpeg")
                    .insert_header("Content-Length", "30137")
                    .insert_header("Connection", "keep-alive")
                    .insert_header("Accept-Ranges", "bytes")
                    .insert_header("Last-Modified", "Fri, 02 Aug 2024 07:01:37 GMT")
                    .insert_header("x-oss-hash-crc64ecma", "9171375736106722013")
                    .insert_header("Content-MD5", "8nHHAiFN3uTYSG0pH4fW7g=="),
            )
            .mount(&server)
            .await;

        let url = "oss://test-bucket/test_dir_non_recursive/🎉图片🎉.jpg";

        let object_storage = build_oss_info();
        let backend = ObjectStorage::new(Scheme::OSS);
        let head_response = backend
            .head(build_head_request(url, object_storage))
            .await
            .unwrap();

        assert!(head_response.success);
        assert_eq!(head_response.content_length, Some(30137));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn should_get_oss_get_response_of_file_with_non_ascii() {
        let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
        let server = wiremock::MockServer::builder()
            .listener(listener)
            .start()
            .await;

        // Mock the response.
        Mock::given(method("HEAD"))
            .and(path(
                "/test_dir_non_recursive/%F0%9F%8E%89%E5%9B%BE%E7%89%87%F0%9F%8E%89.txt",
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Server", "AliyunOSS")
                    .insert_header("Content-Type", "text/plain")
                    .insert_header("Content-Length", "3")
                    .insert_header("Connection", "keep-alive")
                    .insert_header("Accept-Ranges", "bytes")
                    .insert_header("Last-Modified", "Fri, 02 Aug 2024 07:01:37 GMT"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/test_dir_non_recursive/%F0%9F%8E%89%E5%9B%BE%E7%89%87%F0%9F%8E%89.txt",
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Server", "AliyunOSS")
                    .insert_header("Content-Type", "text/plain")
                    .insert_header("Content-Length", "3")
                    .insert_header("Connection", "keep-alive")
                    .insert_header("Content-Range", "bytes 0-23/24")
                    .insert_header("Accept-Ranges", "bytes")
                    .insert_header("Last-Modified", "Fri, 02 Aug 2024 07:01:37 GMT")
                    .set_body_string("111"),
            )
            .mount(&server)
            .await;
        let url = "oss://test-bucket/test_dir_non_recursive/🎉图片🎉.txt";

        let object_storage = build_oss_info();
        let backend = ObjectStorage::new(Scheme::OSS);
        let mut get_response = backend
            .get(build_get_request(url, None, object_storage))
            .await
            .unwrap();

        assert!(get_response.success);
        assert_eq!(get_response.text().await.unwrap(), "111");
    }

    /// Returns the object storage info of oss for test.
    fn build_oss_info() -> dragonfly_api::common::v2::ObjectStorage {
        dragonfly_api::common::v2::ObjectStorage {
            access_key_id: "test-access-key-id".into(),
            access_key_secret: "test-access-key-secret".into(),
            endpoint: Some("http://test-endpoint.local:8080".into()),
            ..Default::default()
        }
    }

    /// Returns the head request uesd for test.
    fn build_head_request(
        url: &str,
        object_storage: dragonfly_api::common::v2::ObjectStorage,
    ) -> HeadRequest {
        HeadRequest {
            task_id: "test-task".to_string(),
            url: url.into(),
            http_header: None,
            timeout: Duration::from_secs(3),
            client_certs: None,
            object_storage: Some(object_storage),
        }
    }

    /// Returns the get request uesd for test.
    fn build_get_request(
        url: &str,
        range: Option<Range>,
        object_storage: dragonfly_api::common::v2::ObjectStorage,
    ) -> GetRequest {
        GetRequest {
            piece_id: "test-task".into(),
            url: url.into(),
            range,
            http_header: None,
            timeout: Duration::from_secs(3),
            client_certs: None,
            object_storage: Some(object_storage),
        }
    }
}
