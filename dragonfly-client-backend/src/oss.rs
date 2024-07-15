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

use std::time::Duration;

use dragonfly_client_core::*;
use error::BackendError;
use opendal::{raw::HttpClient, Metakey, Operator};
use tokio_util::io::StreamReader;
use tracing::info;
use url::Url;

use crate::*;

// ParsedURL is a struct that contains the parsed URL, bucket, and endpoint.
#[derive(Debug)]
pub struct ParsedURL {
    // url is the requested URL of the OSS.
    url: Url,

    // bucket is the bucket of the OSS.
    bucket: String,

    // endpoint is the endpoint of the OSS.
    endpoint: String,
}

// ParsedURL implements the ParsedURL trait.
impl ParsedURL {
    // is_dir returns true if the URL path ends with a slash.
    fn is_dir(&self) -> bool {
        self.url.path().ends_with('/')
    }

    // key returns the OSS key of the URL.
    fn key(&self) -> &str {
        // Get the path part of the URL and trim any leading slashes.
        self.url.path().trim_start_matches('/')
    }

    // make_url_by_entry_path makes a URL by the entry path when the URL is a directory.
    fn make_url_by_entry_path(&self, entry_path: &str) -> Url {
        let mut url = self.url.clone();
        url.set_path(entry_path);
        url
    }
}

// ParsedURL implements the TryFrom trait for the URL.
impl TryFrom<Url> for ParsedURL {
    type Error = Error;

    // try_from parses the URL and returns a ParsedURL.
    fn try_from(url: Url) -> std::result::Result<Self, Self::Error> {
        let host = url
            .host_str()
            .ok_or_else(|| Error::InvalidURI(url.to_string()))?;

        // Check the url is valid.
        //
        // The path of url should not be empty to access the OSS system.
        // The domain name should end with "aliyuncs.com" to access the OSS system.
        if url.path().is_empty() || !host.ends_with("aliyuncs.com") {
            return Err(Error::InvalidURI(url.to_string()));
        }

        // Parse the bucket and endpoint.
        //
        // OSS use the "Virtual Hosting of Buckets", so the bucket will be the lowest level of
        // the host, the rest of the host string is the endpoint.
        let parts: Vec<&str> = host.splitn(2, '.').collect();
        let bucket = parts[0].to_string();
        let endpoint = parts[1..].join(".");

        Ok(Self {
            url,
            bucket,
            endpoint,
        })
    }
}

// OSS is a struct that implements the Backend trait.
#[derive(Default)]
pub struct OSS;

// OSS implements the OSS trait.
impl OSS {
    /// Returns OSS that implement `Backend` trait.
    pub fn new() -> Self {
        Self
    }

    // operator initializes the operator with the parsed URL and object storage.
    pub fn operator(
        &self,
        parsed_url: &ParsedURL,
        object_storage: Option<ObjectStorage>,
        timeout: Duration,
    ) -> Result<Operator> {
        let Some(ObjectStorage {
            access_key_id,
            access_key_secret,
            session_token: None,
        }) = object_storage
        else {
            error!("need access_key_id and access_key_secret");
            return Err(Error::BackendError(BackendError {
                message: "need access_key_id and access_key_secret".to_string(),
                status_code: None,
                header: None,
            }));
        };

        // Create a reqwest http client.
        let client = reqwest::Client::builder().timeout(timeout).build()?;

        // Set up operator builder.
        let mut builder = opendal::services::Oss::default();
        builder
            .access_key_id(&access_key_id)
            .access_key_secret(&access_key_secret)
            .http_client(HttpClient::with(client))
            .root("/")
            .bucket(&parsed_url.bucket)
            .endpoint(&parsed_url.endpoint);

        Ok(Operator::new(builder)?.finish())
    }
}

// Backend implements the Backend trait.
#[tonic::async_trait]
impl Backend for OSS {
    // Returns the header(mainly is Content-Length) of requested file or directory.
    // This function will automatically list the entries if the requested url is a
    // directory.
    async fn head(&self, request: HeadRequest) -> Result<HeadResponse> {
        info!(
            "head request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        // Parse the URL and convert it to a ParsedURL for create the OSS operator.
        let url: Url = request
            .url
            .parse()
            .map_err(|_| Error::InvalidURI(request.url.clone()))?;
        let parsed_url: ParsedURL = url.try_into().map_err(|err| {
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
            Some(
                operator
                    .list_with(parsed_url.key())
                    .recursive(true)
                    .metakey(Metakey::ContentLength | Metakey::Mode)
                    .await // Do the list op here.
                    .map_err(|err| {
                        error!(
                            "list request failed {} {}: {}",
                            request.task_id, request.url, err
                        );
                        Error::BackendError(BackendError {
                            message: err.to_string(),
                            status_code: None,
                            header: None,
                        })
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
                    .collect(),
            )
        } else {
            None
        };

        // Stat the object to get the response from the OSS.
        let response = operator.stat_with(parsed_url.key()).await.map_err(|err| {
            error!(
                "stat request failed {} {}: {}",
                request.task_id, request.url, err
            );
            Error::BackendError(BackendError {
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

        Ok(HeadResponse {
            success: true,
            content_length: Some(response.content_length()),
            http_header: None,
            http_status_code: None,
            error_message: None,
            entries,
        })
    }

    // Returns content of requested file.
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>> {
        info!(
            "get request {} {}: {:?}",
            request.piece_id, request.url, request.http_header
        );

        // Parse the URL and convert it to a ParsedURL for create the OSS operator.
        let url: Url = request
            .url
            .parse()
            .map_err(|_| Error::InvalidURI(request.url.clone()))?;
        let parsed_url: ParsedURL = url.try_into().map_err(|err| {
            error!(
                "parse head request url failed {} {}: {}",
                request.piece_id, request.url, err
            );
            err
        })?;

        // Initialize the operator with the parsed URL, object storage, and timeout.
        let operator_reader = self
            .operator(&parsed_url, request.object_storage, request.timeout)?
            .reader(parsed_url.key())
            .await
            .map_err(|err| {
                error!(
                    "get request failed {} {}: {}",
                    request.piece_id, request.url, err
                );
                Error::BackendError(BackendError {
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
                    Error::BackendError(BackendError {
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
                Error::BackendError(BackendError {
                    message: err.to_string(),
                    status_code: None,
                    header: None,
                })
            })?,
        };

        Ok(GetResponse {
            success: true,
            http_header: None,
            http_status_code: None,
            reader: Box::new(StreamReader::new(stream)),
            error_message: None,
        })
    }
}
