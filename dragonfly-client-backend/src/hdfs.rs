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
use opendal::{layers::TimeoutLayer, Operator};
use percent_encoding::percent_decode_str;
use std::time::Duration;
use tokio_util::io::StreamReader;
use tracing::{debug, error, instrument};
use url::Url;

/// HDFS_SCHEME is the scheme of the HDFS.
pub const HDFS_SCHEME: &str = "hdfs";

/// DEFAULT_NAMENODE_PORT is the default port of the HDFS namenode.
const DEFAULT_NAMENODE_PORT: u16 = 9870;

/// Hdfs is a struct that implements the Backend trait.
#[derive(Default)]
pub struct Hdfs {
    /// scheme is the scheme of the HDFS.
    scheme: String,
}

/// Hdfs implements the Backend trait.
impl Hdfs {
    /// new returns a new HDFS backend.
    pub fn new() -> Self {
        Self {
            scheme: HDFS_SCHEME.to_string(),
        }
    }

    /// operator initializes the operator with the parsed URL and HDFS config.
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
impl super::Backend for Hdfs {
    /// scheme returns the scheme of the HDFS backend.
    fn scheme(&self) -> String {
        self.scheme.clone()
    }

    /// stat gets the metadata from the backend.
    #[instrument(skip_all)]
    async fn stat(&self, request: super::StatRequest) -> ClientResult<super::StatResponse> {
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
                    super::DirEntry {
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

        Ok(super::StatResponse {
            success: true,
            content_length: Some(response.content_length()),
            http_header: None,
            http_status_code: None,
            error_message: None,
            entries,
        })
    }

    /// get gets the content from the backend.
    #[instrument(skip_all)]
    async fn get(
        &self,
        request: super::GetRequest,
    ) -> ClientResult<super::GetResponse<super::Body>> {
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

    /// put puts the content to the backend.
    #[instrument(skip_all)]
    async fn put(&self, _request: super::PutRequest) -> ClientResult<super::PutResponse> {
        unimplemented!()
    }

    /// exists checks whether the file exists in the backend.
    #[instrument(skip_all)]
    async fn exists(&self, request: super::ExistsRequest) -> ClientResult<bool> {
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
