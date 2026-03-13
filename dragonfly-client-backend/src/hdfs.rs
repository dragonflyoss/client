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

//! HDFS backend implementation for downloading and accessing files from Hadoop Distributed File System.
//!
//! This module provides support for the `hdfs://` URL scheme to access files from
//! HDFS clusters. It uses the WebHDFS REST API through the OpenDAL library to handle
//! file operations including stat, get, exists, and directory listing.
//!
//! # URL Format
//!
//! The URL format is: `hdfs://<namenode_host>[:<port>]/<path>`
//!
//! Examples:
//! - `hdfs://namenode:9870/data/` - List entire directory
//! - `hdfs://namenode:9870/data/model.bin` - Access specific file
//! - `hdfs://namenode/data/model.bin` - Access file using default port (9870)
//!
//! # Authentication
//!
//! For secured HDFS clusters, use the `--hdfs-delegation-token` flag to provide
//! a delegation token for authentication.

use crate::{
    Backend, Body, DirEntry, ExistsRequest, GetRequest, GetResponse, PutRequest, PutResponse,
    StatRequest, StatResponse,
};
use dragonfly_api::common;
use dragonfly_client_core::error::BackendError;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use opendal::{layers::TimeoutLayer, Operator};
use percent_encoding::percent_decode_str;
use std::time::Duration;
use tokio_util::io::StreamReader;
use tracing::{debug, error, instrument};
use url::Url;

/// SCHEME is the scheme of the HDFS.
pub const SCHEME: &str = "hdfs";

/// DEFAULT_NAMENODE_PORT is the default port of the HDFS namenode.
const DEFAULT_NAMENODE_PORT: u16 = 9870;

/// Hdfs is a struct that implements the Backend trait.
#[derive(Default)]
pub struct Hdfs {
    /// Scheme is the scheme of the HDFS.
    scheme: String,
}

/// Hdfs implements the Backend trait.
impl Hdfs {
    /// Create a new Hdfs instance.
    pub fn new() -> Self {
        Self {
            scheme: SCHEME.to_string(),
        }
    }

    /// Operator initializes the operator with the parsed URL and HDFS config.
    pub fn operator(
        &self,
        url: Url,
        config: Option<common::v2::Hdfs>,
        timeout: Duration,
    ) -> ClientResult<Operator> {
        // Get the host and port from the URL.
        let host = url
            .host_str()
            .ok_or_else(|| ClientError::InvalidURI(url.to_string()))?
            .to_string();
        let port = url.port().unwrap_or(DEFAULT_NAMENODE_PORT);

        // Initialize the HDFS operator.
        let mut builder = opendal::services::Webhdfs::default();
        builder = builder
            .root("/")
            // Host can be an IP address or a hostname, so we need to handle both cases.
            .endpoint(&format!("http://{}:{}", host, port));

        // If HDFS config is not None, set the config for builder.
        if let Some(config) = config {
            if let Some(delegation_token) = &config.delegation_token {
                builder = builder.delegation(delegation_token);
            }
        }

        Ok(Operator::new(builder)?
            .finish()
            .layer(TimeoutLayer::new().with_timeout(timeout)))
    }
}

/// Implement the Backend trait for Hdfs.
#[tonic::async_trait]
impl Backend for Hdfs {
    /// Scheme returns the scheme of the HDFS backend.
    fn scheme(&self) -> String {
        self.scheme.clone()
    }

    /// Stat the metadata from the backend.
    #[instrument(skip_all)]
    async fn stat(&self, request: StatRequest) -> ClientResult<StatResponse> {
        debug!(
            "stat request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        // Parse the URL.
        let url = Url::parse(request.url.as_ref())
            .map_err(|_| ClientError::InvalidURI(request.url.clone()))?;
        let decoded_path = percent_decode_str(url.path())
            .decode_utf8_lossy()
            .to_string();

        // Initialize the operator with the parsed URL and HDFS config.
        let operator = self.operator(url.clone(), request.hdfs, request.timeout)?;

        // Get the entries if url point to a directory.
        let entries = if url.path().ends_with('/') {
            operator
                .list_with(&decoded_path)
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
                    let mut url = url.clone();
                    url.set_path(entry.path());
                    DirEntry {
                        url: url.to_string(),
                        content_length: metadata.content_length() as usize,
                        is_dir: metadata.is_dir(),
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        // Stat the path to get the response from HDFS operator.
        let response = operator.stat_with(&decoded_path).await.map_err(|err| {
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

        // Parse the URL.
        let url = Url::parse(request.url.as_ref())
            .map_err(|_| ClientError::InvalidURI(request.url.clone()))?;
        let decoded_path = percent_decode_str(url.path())
            .decode_utf8_lossy()
            .to_string();

        // Initialize the operator with the parsed URL and HDFS config.
        let operator_reader = self
            .operator(url.clone(), request.hdfs, request.timeout)?
            .reader(decoded_path.as_ref())
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

    /// Put the content to the backend.
    #[instrument(skip_all)]
    async fn put(&self, _request: PutRequest) -> ClientResult<PutResponse> {
        unimplemented!()
    }

    /// Exists checks whether the file exists in the backend.
    #[instrument(skip_all)]
    async fn exists(&self, request: ExistsRequest) -> ClientResult<bool> {
        debug!(
            "exist request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        // Parse the URL.
        let url = Url::parse(request.url.as_ref())
            .map_err(|_| ClientError::InvalidURI(request.url.clone()))?;
        let decoded_path = percent_decode_str(url.path())
            .decode_utf8_lossy()
            .to_string();

        // Initialize the operator with the parsed URL and HDFS config.
        let operator = self.operator(url.clone(), request.hdfs, request.timeout)?;
        Ok(operator.exists(&decoded_path).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn should_get_operator() {
        let url: Url = Url::parse("hdfs://127.0.0.1:9870/file").unwrap();
        let operator = Hdfs::new().operator(url, None, Duration::from_secs(10));

        assert!(
            operator.is_ok(),
            "can not get hdfs operator, due to: {}",
            operator.unwrap_err()
        );
    }

    #[test]
    fn should_return_error_when_url_not_valid() {
        let url: Url = Url::parse("hdfs:/127.0.0.1:9870/file").unwrap();
        let result = Hdfs::new().operator(url, None, Duration::from_secs(10));

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ClientError::InvalidURI(..)));
    }
}
