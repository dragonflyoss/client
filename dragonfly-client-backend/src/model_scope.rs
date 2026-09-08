/*
 *     Copyright 2026 The Dragonfly Authors
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

//! ModelScope backend implementation for downloading models and datasets.
//!
//! This module provides support for the `modelscope://` URL scheme to download files from
//! ModelScope Hub repositories. It handles file downloads through the ModelScope HTTP API.
//!
//! # URL Format
//!
//! The URL format is: `modelscope://[<repo_type>/]<owner>/<repo>[/<path>]`
//!
//! Examples:
//! - `modelscope://deepseek-ai/DeepSeek-R1` - Download entire repository
//! - `modelscope://deepseek-ai/DeepSeek-R1/config.json` - Download specific file
//! - `modelscope://datasets/owner/dataset-name` - Download from a dataset repository
//!
//! # Authentication
//!
//! For private repositories or to increase rate limits, use the `--ms-token` flag.

use crate::{
    empty_body, Backend, Body, DirEntry, ExistsRequest, GetRequest, GetResponse, PutRequest,
    PutResponse, StatRequest, StatResponse, DEFAULT_USER_AGENT, KEEP_ALIVE_INTERVAL,
    POOL_MAX_IDLE_PER_HOST,
};
use async_trait::async_trait;
use dragonfly_api::common::v2::Range;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    error::{BackendError, ErrorType, OrErr},
    Error, Result,
};
use dragonfly_client_util::{http::validate_ranged_response, tls::NoVerifier};
use futures::{StreamExt, TryStreamExt};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_LENGTH, RANGE, USER_AGENT};
use reqwest::Client;
use serde::Deserialize;
use std::error::Error as _;
use std::io::Error as IOError;
use std::sync::Arc;
use tokio_util::io::StreamReader;
use tracing::{debug, error, instrument};
use url::Url;

/// The URL scheme for ModelScope backend.
pub const SCHEME: &str = "modelscope";

/// The base URL for ModelScope Hub.
const MODEL_SCOPE_BASE_URL: &str = "https://modelscope.cn";

/// Represents the top-level response from the ModelScope API.
#[derive(Debug, Deserialize)]
struct Response<T> {
    /// The status code of the API response, where 200 indicates success.
    #[serde(rename = "Code")]
    code: i32,

    /// The actual data payload of the response, which varies based on the API endpoint.
    #[serde(rename = "Data")]
    data: T,

    /// An optional message providing additional information about the response, such as error
    /// details.
    #[serde(rename = "Message", default)]
    message: Option<String>,
}

/// File list represents the data field in a file list API response.
#[derive(Debug, Deserialize)]
struct FileList {
    /// A list of file entries returned by the API, which may be empty if no files are found or if
    #[serde(rename = "Files")]
    files: Option<Vec<File>>,
}

/// Represents a file entry returned by the ModelScope API.
#[derive(Debug, Deserialize)]
struct File {
    /// The relative path within the repository.
    #[serde(rename = "Path")]
    path: String,

    /// The entry type: "blob" for files, "tree" for directories.
    #[serde(rename = "Type")]
    entry_type: String,

    /// The file size in bytes.
    #[serde(rename = "Size")]
    size: Option<u64>,
}

/// The type of a ModelScope repository.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RepositoryType {
    /// A model repository. This is the default when no type prefix is specified,
    /// or when explicitly prefixed with `models/`.
    Model,

    /// A dataset repository, prefixed with `datasets/`.
    Dataset,
}

/// Implements methods for getting string representations and API paths.
impl RepositoryType {
    /// Returns the canonical string identifier (e.g., `"models"`, `"datasets"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            RepositoryType::Model => "models",
            RepositoryType::Dataset => "datasets",
        }
    }
}

/// A parsed representation of a ModelScope URL.
///
/// Format: `modelscope://[<repository_type>/]<owner>/<repository>[/<path>]`
#[derive(Debug, Clone)]
pub struct ParsedURL {
    /// The original, unparsed URL.
    pub url: Url,

    /// The repository identifier in `<owner>/<repository>` format (e.g., `"deepseek-ai/DeepSeek-R1"`).
    pub repository_id: String,

    /// The type of repository: model or dataset.
    pub repository_type: RepositoryType,

    /// An optional file path within the repository (e.g., `"path/to/config.json"`).
    pub file_path: Option<String>,
}

/// Parses a ModelScope URL into its constituent components.
///
/// URL Format: modelscope://[<repository_type>/]<owner>/<repository>[/<path>]
/// - repository_type  Optional. One of "models" (default) or "datasets".
/// - owner/repository Required. For example, "deepseek-ai/DeepSeek-R1".
/// - path             Optional file path within the repository.
impl TryFrom<Url> for ParsedURL {
    type Error = Error;

    /// Parses the URL and returns a ParsedURL.
    fn try_from(url: Url) -> std::result::Result<Self, Self::Error> {
        let host = url
            .host_str()
            .ok_or_else(|| Error::InvalidURI(url.to_string()))?;
        let raw_path = format!("{}{}", host, url.path().trim_end_matches('/'));
        let segments: Vec<&str> = raw_path.trim_matches('/').split('/').collect();
        let (repository_type, offset) = match segments.first() {
            Some(&"datasets") => (RepositoryType::Dataset, 1),
            Some(&"models") => (RepositoryType::Model, 1),
            _ => (RepositoryType::Model, 0),
        };

        // After stripping the optional type prefix, at least two segments
        // (owner and repository name) must remain.
        let remaining = &segments[offset..];
        if remaining.len() < 2 {
            return Err(Error::InvalidParameter);
        }

        let repository_id = format!("{}/{}", remaining[0], remaining[1]);
        let file_path = if remaining.len() > 2 {
            Some(remaining[2..].join("/"))
        } else {
            None
        };

        Ok(ParsedURL {
            url,
            repository_type,
            repository_id,
            file_path,
        })
    }
}

/// Implements TryFrom for &str.
impl TryFrom<&str> for ParsedURL {
    type Error = Error;

    /// Try to parse a string URL into a ParsedURL struct.
    fn try_from(url: &str) -> std::result::Result<Self, Self::Error> {
        let parsed_url = Url::parse(url).or_err(ErrorType::ParseError)?;
        ParsedURL::try_from(parsed_url)
    }
}

/// The ModelScope backend implementation.
pub struct ModelScope {
    /// The scheme of the ModelScope backend.
    scheme: String,

    /// HTTP client for making requests.
    client: Client,
}

/// Implements the ModelScope interface.
impl ModelScope {
    /// Create a new ModelScope backend.
    pub fn new(config: Arc<Config>) -> Result<Self> {
        // Default TLS client config with no validation.
        let client_config_builder = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(NoVerifier::new())
            .with_no_client_auth();

        let client = reqwest::Client::builder()
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .hickory_dns(config.backend.enable_hickory_dns)
            .use_preconfigured_tls(client_config_builder)
            .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
            .tcp_keepalive(KEEP_ALIVE_INTERVAL)
            .tcp_nodelay(true)
            .build()?;

        Ok(Self {
            scheme: SCHEME.to_string(),
            client,
        })
    }

    /// Resolves the base URLs from gRPC ModelScope options.
    fn resolve_base_urls(base_url: Option<&str>) -> Result<(Url, Url)> {
        let base_url = Url::parse(base_url.unwrap_or(MODEL_SCOPE_BASE_URL))?;
        let api_base_url = base_url.join("/api/v1/")?;
        Ok((base_url, api_base_url))
    }

    /// Builds the download URL for a file based on the repository type and path.
    ///
    /// Format: `{base_url}/{repo_type}/{repo_id}/resolve/{revision}/{file_path}`
    fn build_download_url(
        parsed_url: &ParsedURL,
        file_path: &str,
        revision: &str,
        base_url: &Url,
    ) -> Result<Url> {
        let path = format!(
            "{}/{}/resolve/{}/{}",
            parsed_url.repository_type.as_str(),
            parsed_url.repository_id,
            revision,
            file_path
        );

        Ok(base_url.join(&path)?)
    }

    /// Builds the API URL for listing files in the repository.
    ///
    /// Format: `{api_base_url}/{repo_type}/{repo_id}/repo/files?Revision={revision}&Recursive=true`
    fn build_file_list_url(
        parsed_url: &ParsedURL,
        revision: &str,
        api_base_url: &Url,
    ) -> Result<Url> {
        let path = format!(
            "{}/{}/repo/files?Revision={}&Recursive=true",
            parsed_url.repository_type.as_str(),
            parsed_url.repository_id,
            revision
        );

        Ok(api_base_url.join(&path)?)
    }

    /// Builds a `modelscope://` URL for a file so downstream downloads continue to
    /// use the ModelScope backend (preserving auth and URL semantics).
    fn build_model_scope_url(parsed_url: &ParsedURL, filename: &str) -> Result<Url> {
        let url = match parsed_url.repository_type {
            RepositoryType::Model => {
                format!("{}://{}/{}", SCHEME, parsed_url.repository_id, filename)
            }
            RepositoryType::Dataset => format!(
                "{}://datasets/{}/{}",
                SCHEME, parsed_url.repository_id, filename
            ),
        };

        Ok(Url::parse(&url)?)
    }

    /// Build the request headers for ModelScope API requests, including authentication if a
    /// token is provided by the `--ms-token` CLI flag.
    fn build_request_headers(token: Option<String>, range: Option<Range>) -> Result<HeaderMap> {
        let mut request_header = HeaderMap::new();

        // Add Range header if present in the request.
        if let Some(range) = &range {
            request_header.insert(
                RANGE,
                format!("bytes={}-{}", range.start, range.start + range.length - 1).parse()?,
            );
        };

        // Make the user agent if not specified in header.
        request_header
            .entry(USER_AGENT)
            .or_insert(HeaderValue::from_static(DEFAULT_USER_AGENT));

        // Add the Authorization header for ModelScope API authentication.
        if let Some(token) = token {
            request_header.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
            );
        }

        Ok(request_header)
    }
}

/// Backend implementation for ModelScope.
#[async_trait]
impl Backend for ModelScope {
    /// Returns the scheme of the backend.
    fn scheme(&self) -> String {
        self.scheme.clone()
    }

    /// Stat the metadata from the backend.
    #[instrument(skip_all)]
    async fn stat(&self, request: StatRequest) -> Result<StatResponse> {
        debug!(
            "stat request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        // Build request headers, including authentication if provided ModelScope token.
        let request_header = Self::build_request_headers(
            request.model_scope.as_ref().and_then(|ms| ms.token.clone()),
            None,
        )?;

        // Get the ModelScope information from the request, request must contain ModelScope
        // information for stat request, otherwise return error.
        let model_scope = request.model_scope.as_ref().ok_or_else(|| {
            error!(
                "stat request {} {}: missing ModelScope information",
                request.task_id, request.url
            );

            Error::InvalidParameter
        })?;

        let parsed_url = ParsedURL::try_from(request.url.as_str())?;
        let (base_url, api_base_url) = Self::resolve_base_urls(model_scope.base_url.as_deref())?;
        match &parsed_url.file_path {
            Some(file_path) => {
                let download_url = Self::build_download_url(
                    &parsed_url,
                    file_path,
                    &model_scope.revision,
                    &base_url,
                )?;

                let response = self
                    .client
                    .get(download_url.as_str())
                    .headers(request_header)
                    .timeout(request.timeout)
                    .send()
                    .await
                    .map_err(|err| {
                        error!(
                            "stat request failed {} {}: {}",
                            request.task_id, download_url, err
                        );

                        Error::BackendError(Box::new(BackendError {
                            message: err.to_string(),
                            status_code: None,
                            header: None,
                        }))
                    })?;

                let response_status_code = response.status();
                let response_header = response.headers().clone();
                let content_length = match response_header.get(CONTENT_LENGTH) {
                    Some(content_length) => content_length.to_str()?.parse::<u64>().ok(),
                    None => response.content_length(),
                };

                if !response.status().is_success() {
                    error!(
                        "stat request failed {} {}: {}",
                        request.task_id, download_url, response_status_code
                    );

                    return Err(Error::BackendError(Box::new(BackendError {
                        message: response_status_code.to_string(),
                        status_code: Some(response_status_code),
                        header: Some(response_header),
                    })));
                }

                debug!(
                    "stat response {} {}: {:?} {:?} {:?}",
                    request.task_id,
                    download_url,
                    response_status_code,
                    content_length,
                    response_header
                );

                drop(response);
                Ok(StatResponse {
                    success: response_status_code.is_success(),
                    content_length,
                    http_header: Some(response_header),
                    http_status_code: Some(response_status_code),
                    error_message: Some(response_status_code.to_string()),
                    entries: Vec::new(),
                })
            }
            None => {
                let file_list_url =
                    Self::build_file_list_url(&parsed_url, &model_scope.revision, &api_base_url)?;

                let response = self
                    .client
                    .get(file_list_url.as_str())
                    .headers(request_header)
                    .timeout(request.timeout)
                    .send()
                    .await
                    .map_err(|err| {
                        error!(
                            "stat request failed {} {}: {}",
                            request.task_id, file_list_url, err
                        );

                        Error::BackendError(Box::new(BackendError {
                            message: err.to_string(),
                            status_code: None,
                            header: None,
                        }))
                    })?;

                let response_status_code = response.status();
                let response_header = response.headers().clone();
                let content_length = match response_header.get(CONTENT_LENGTH) {
                    Some(content_length) => content_length.to_str()?.parse::<u64>().ok(),
                    None => response.content_length(),
                };

                if !response.status().is_success() {
                    error!(
                        "stat request failed {} {}: {}",
                        request.task_id, file_list_url, response_status_code
                    );

                    return Err(Error::BackendError(Box::new(BackendError {
                        message: response_status_code.to_string(),
                        status_code: Some(response_status_code),
                        header: Some(response_header),
                    })));
                }

                let text = response.text().await.map_err(|err| {
                    error!(
                        "stat request failed {} {}: {}",
                        request.task_id, file_list_url, err
                    );

                    Error::BackendError(Box::new(BackendError {
                        message: err.to_string(),
                        status_code: None,
                        header: None,
                    }))
                })?;

                let response: Response<FileList> = serde_json::from_str(&text).map_err(|err| {
                    error!(
                        "stat request failed {} {}: {}",
                        request.task_id, file_list_url, err
                    );

                    Error::BackendError(Box::new(BackendError {
                        message: err.to_string(),
                        status_code: None,
                        header: None,
                    }))
                })?;

                if response.code != 200 {
                    return Err(Error::BackendError(Box::new(BackendError {
                        status_code: None,
                        header: None,
                        message: format!(
                            "ModelScope API error: code={}, message={}",
                            response.code,
                            response.message.unwrap_or_default()
                        ),
                    })));
                }

                let entries = response
                    .data
                    .files
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|file: &File| file.entry_type != "tree")
                    .map(|file: File| -> Result<DirEntry> {
                        // Return modelscope:// URLs so downstream downloads continue to use the
                        // ModelScope backend (preserving auth headers and URL semantics).
                        let ms_url = Self::build_model_scope_url(&parsed_url, &file.path)?;
                        let content_length = file.size.unwrap_or(0);
                        Ok(DirEntry {
                            url: ms_url.to_string(),
                            content_length: content_length as usize,
                            is_dir: false,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                debug!(
                    "stat response {} {}: {:?} {:?} {:?}",
                    request.task_id,
                    file_list_url,
                    response_status_code,
                    content_length,
                    response_header
                );

                Ok(StatResponse {
                    success: response_status_code.is_success(),
                    content_length,
                    http_header: Some(response_header),
                    http_status_code: Some(response_status_code),
                    error_message: Some(response_status_code.to_string()),
                    entries,
                })
            }
        }
    }

    /// Get the content from the backend.
    #[instrument(skip_all)]
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>> {
        debug!(
            "get request {} {} {}: {:?}",
            request.task_id, request.piece_id, request.url, request.http_header
        );

        // Build request headers, including authentication if provided ModelScope token.
        let request_header = Self::build_request_headers(
            request.model_scope.as_ref().and_then(|ms| ms.token.clone()),
            request.range,
        )?;

        // Get the ModelScope information from the request, request must contain ModelScope
        // information for get request, otherwise return error.
        let model_scope = request.model_scope.as_ref().ok_or_else(|| {
            error!(
                "get request {} {}: missing ModelScope information",
                request.task_id, request.url
            );

            Error::InvalidParameter
        })?;

        // Parse the URL and build the download URL for the specified file.
        let parsed_url = ParsedURL::try_from(request.url.as_str())?;
        let Some(file_path) = &parsed_url.file_path else {
            error!(
                "get request {} {}: URL must specify a file path",
                request.task_id, request.url
            );
            return Err(Error::InvalidParameter);
        };

        let (base_url, _) = Self::resolve_base_urls(model_scope.base_url.as_deref())?;
        let download_url =
            Self::build_download_url(&parsed_url, file_path, &model_scope.revision, &base_url)?;
        let response = match self
            .client
            .get(download_url.as_str())
            .headers(request_header)
            .timeout(request.timeout)
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                error!(
                    "get request failed {} {} {}: {}",
                    request.task_id, request.piece_id, download_url, err
                );
                return Ok(GetResponse {
                    success: false,
                    http_header: None,
                    http_status_code: None,
                    reader: empty_body(),
                    error_message: Some(err.to_string()),
                });
            }
        };

        let response_header = response.headers().clone();
        let response_status_code = response.status();
        if let Err(err) =
            validate_ranged_response(request.range, response_status_code, &response_header)
        {
            error!(
                "get request failed {} {} {}: {}",
                request.task_id, request.piece_id, download_url, err
            );

            return Ok(GetResponse {
                success: false,
                http_header: Some(response_header),
                http_status_code: Some(response_status_code),
                reader: empty_body(),
                error_message: Some(err.to_string()),
            });
        }

        let response_reader = StreamReader::new(
            response
                .bytes_stream()
                .map_err(move |err| {
                    let mut chain = err.to_string();
                    let mut source = err.source();
                    while let Some(err) = source {
                        chain.push_str(": ");
                        chain.push_str(&err.to_string());
                        source = err.source();
                    }

                    IOError::other(chain)
                })
                .boxed(),
        );

        debug!(
            "get response {} {}: {:?} {:?}",
            request.task_id, request.piece_id, response_status_code, response_header,
        );

        Ok(GetResponse {
            success: response_status_code.is_success(),
            http_header: Some(response_header),
            http_status_code: Some(response_status_code),
            reader: response_reader,
            error_message: Some(response_status_code.to_string()),
        })
    }

    /// Put the content to the backend.
    #[instrument(skip_all)]
    async fn put(&self, _request: PutRequest) -> Result<PutResponse> {
        unimplemented!()
    }

    /// Exists checks whether the file exists in the backend.
    #[instrument(skip_all)]
    async fn exists(&self, request: ExistsRequest) -> Result<bool> {
        debug!(
            "exists request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        // Build request headers, including authentication if provided ModelScope token.
        let request_header = Self::build_request_headers(
            request.model_scope.as_ref().and_then(|ms| ms.token.clone()),
            None,
        )?;

        // Get the ModelScope information from the request, request must contain ModelScope
        // information for exists request, otherwise return error.
        let model_scope = request.model_scope.as_ref().ok_or_else(|| {
            error!(
                "exists request {} {}: missing ModelScope information",
                request.task_id, request.url
            );

            Error::InvalidParameter
        })?;

        let parsed_url = ParsedURL::try_from(request.url.as_str())?;
        let (base_url, api_base_url) = Self::resolve_base_urls(model_scope.base_url.as_deref())?;
        match &parsed_url.file_path {
            Some(file_path) => {
                let download_url = Self::build_download_url(
                    &parsed_url,
                    file_path,
                    &model_scope.revision,
                    &base_url,
                )?;
                let response = self
                    .client
                    .head(download_url.as_str())
                    .headers(request_header)
                    .timeout(request.timeout)
                    .send()
                    .await
                    .inspect_err(|err| {
                        error!(
                            "exists request failed {} {}: {}",
                            request.task_id, request.url, err
                        );
                    })?;

                let response_status_code = response.status();
                debug!(
                    "exists response {} {}: {:?} {:?}",
                    request.task_id,
                    request.url,
                    response_status_code,
                    response.headers()
                );

                Ok(response_status_code.is_success())
            }
            None => {
                let file_list_url =
                    Self::build_file_list_url(&parsed_url, &model_scope.revision, &api_base_url)?;
                let response = self
                    .client
                    .get(file_list_url.as_str())
                    .headers(request_header)
                    .timeout(request.timeout)
                    .send()
                    .await
                    .inspect_err(|err| {
                        error!(
                            "exists request failed {} {}: {}",
                            request.task_id, request.url, err
                        );
                    })?;

                let response_status_code = response.status();
                debug!(
                    "exists response {} {}: {:?} {:?}",
                    request.task_id,
                    request.url,
                    response_status_code,
                    response.headers()
                );

                Ok(response_status_code.is_success())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::type_complexity)]

    use super::*;
    use dragonfly_api::common::v2::ModelScope as ModelScopeOptions;
    use reqwest::StatusCode;
    use std::time::Duration;
    use wiremock::{
        matchers::{header, method, path, query_param},
        Mock, MockServer, ResponseTemplate,
    };

    #[test]
    fn parse_url_extracts_type_id_and_path() {
        let test_cases = vec![
            (
                "modelscope://deepseek-ai/DeepSeek-R1",
                RepositoryType::Model,
                "deepseek-ai/DeepSeek-R1",
                None,
            ),
            (
                "modelscope://deepseek-ai/DeepSeek-R1/",
                RepositoryType::Model,
                "deepseek-ai/DeepSeek-R1",
                None,
            ),
            (
                "modelscope://deepseek-ai/DeepSeek-R1/config.json",
                RepositoryType::Model,
                "deepseek-ai/DeepSeek-R1",
                Some("config.json"),
            ),
            (
                "modelscope://deepseek-ai/DeepSeek-R1/models/v1/model.bin",
                RepositoryType::Model,
                "deepseek-ai/DeepSeek-R1",
                Some("models/v1/model.bin"),
            ),
            (
                "modelscope://models/deepseek-ai/DeepSeek-R1/config.json",
                RepositoryType::Model,
                "deepseek-ai/DeepSeek-R1",
                Some("config.json"),
            ),
            (
                "modelscope://datasets/owner/my-dataset",
                RepositoryType::Dataset,
                "owner/my-dataset",
                None,
            ),
            (
                "modelscope://datasets/owner/my-dataset/train.json",
                RepositoryType::Dataset,
                "owner/my-dataset",
                Some("train.json"),
            ),
        ];

        for (url, expected_type, expected_id, expected_path) in test_cases {
            let parsed_url = ParsedURL::try_from(url).unwrap();
            assert_eq!(parsed_url.repository_type, expected_type);
            assert_eq!(parsed_url.repository_id, expected_id);
            assert_eq!(parsed_url.file_path.as_deref(), expected_path);
        }
    }

    #[test]
    fn parse_url_rejects_missing_owner_or_repository() {
        let test_cases = vec!["modelscope://deepseek-ai", "modelscope://datasets/owner"];

        for url in test_cases {
            let result = ParsedURL::try_from(url);
            assert!(matches!(result, Err(Error::InvalidParameter)));
        }
    }

    #[test]
    fn repository_type_as_str_returns_route_segment() {
        let test_cases = vec![
            (RepositoryType::Model, "models"),
            (RepositoryType::Dataset, "datasets"),
        ];

        for (repository_type, expected) in test_cases {
            assert_eq!(repository_type.as_str(), expected);
        }
    }

    #[test]
    fn resolve_base_urls_defaults_to_hub_and_joins_api_path() {
        let test_cases = vec![
            (
                None,
                "https://modelscope.cn/",
                "https://modelscope.cn/api/v1/",
            ),
            (
                Some("https://modelscope-mirror.example.com/"),
                "https://modelscope-mirror.example.com/",
                "https://modelscope-mirror.example.com/api/v1/",
            ),
        ];

        for (base_url, expected_base_url, expected_api_base_url) in test_cases {
            let (resolved_base_url, api_base_url) =
                ModelScope::resolve_base_urls(base_url).unwrap();
            assert_eq!(resolved_base_url.as_str(), expected_base_url);
            assert_eq!(api_base_url.as_str(), expected_api_base_url);
        }
    }

    #[test]
    fn build_download_url_routes_by_repository_type() {
        let base_url = Url::parse(MODEL_SCOPE_BASE_URL).unwrap();

        let test_cases = vec![
            (
                "modelscope://deepseek-ai/DeepSeek-R1/config.json",
                "config.json",
                "master",
                "https://modelscope.cn/models/deepseek-ai/DeepSeek-R1/resolve/master/config.json",
            ),
            (
                "modelscope://deepseek-ai/DeepSeek-R1/config.json",
                "config.json",
                "v1.0",
                "https://modelscope.cn/models/deepseek-ai/DeepSeek-R1/resolve/v1.0/config.json",
            ),
            (
                "modelscope://datasets/owner/my-dataset/train.json",
                "train.json",
                "master",
                "https://modelscope.cn/datasets/owner/my-dataset/resolve/master/train.json",
            ),
        ];

        for (url, file_path, revision, expected) in test_cases {
            let parsed_url = ParsedURL::try_from(url).unwrap();
            let download_url =
                ModelScope::build_download_url(&parsed_url, file_path, revision, &base_url)
                    .unwrap();
            assert_eq!(download_url.as_str(), expected);
        }
    }

    #[test]
    fn build_file_list_url_routes_by_repository_type() {
        let api_base_url = Url::parse("https://modelscope.cn/api/v1/").unwrap();

        let test_cases = vec![
            (
                "modelscope://deepseek-ai/DeepSeek-R1",
                "master",
                "https://modelscope.cn/api/v1/models/deepseek-ai/DeepSeek-R1/repo/files?Revision=master&Recursive=true",
            ),
            (
                "modelscope://datasets/owner/my-dataset",
                "v1.0",
                "https://modelscope.cn/api/v1/datasets/owner/my-dataset/repo/files?Revision=v1.0&Recursive=true",
            ),
        ];

        for (url, revision, expected) in test_cases {
            let parsed_url = ParsedURL::try_from(url).unwrap();
            let file_list_url =
                ModelScope::build_file_list_url(&parsed_url, revision, &api_base_url).unwrap();
            assert_eq!(file_list_url.as_str(), expected);
        }
    }

    #[test]
    fn build_model_scope_url_routes_by_repository_type() {
        let test_cases = vec![
            (
                "modelscope://deepseek-ai/DeepSeek-R1",
                "config.json",
                "modelscope://deepseek-ai/DeepSeek-R1/config.json",
            ),
            (
                "modelscope://deepseek-ai/DeepSeek-R1",
                "models/v1/model.bin",
                "modelscope://deepseek-ai/DeepSeek-R1/models/v1/model.bin",
            ),
            (
                "modelscope://datasets/owner/my-dataset",
                "train.json",
                "modelscope://datasets/owner/my-dataset/train.json",
            ),
        ];

        for (url, filename, expected) in test_cases {
            let parsed_url = ParsedURL::try_from(url).unwrap();
            let model_scope_url = ModelScope::build_model_scope_url(&parsed_url, filename).unwrap();
            assert_eq!(model_scope_url.as_str(), expected);
        }
    }

    #[test]
    fn build_request_headers_sets_user_agent_token_and_range() {
        let test_cases = vec![
            (None, None, None, None),
            (Some("test-token"), None, Some("Bearer test-token"), None),
            (
                None,
                Some(Range {
                    start: 0,
                    length: 1024,
                }),
                None,
                Some("bytes=0-1023"),
            ),
            (
                Some("my-secret-token"),
                Some(Range {
                    start: 100,
                    length: 200,
                }),
                Some("Bearer my-secret-token"),
                Some("bytes=100-299"),
            ),
        ];

        for (token, range, expected_authorization, expected_range) in test_cases {
            let request_header =
                ModelScope::build_request_headers(token.map(str::to_string), range).unwrap();
            assert_eq!(request_header.get(USER_AGENT).unwrap(), DEFAULT_USER_AGENT);
            assert_eq!(
                request_header
                    .get(AUTHORIZATION)
                    .and_then(|value| value.to_str().ok()),
                expected_authorization
            );
            assert_eq!(
                request_header
                    .get(RANGE)
                    .and_then(|value| value.to_str().ok()),
                expected_range
            );
        }
    }

    #[tokio::test]
    async fn stat_maps_file_and_repository_responses() {
        let test_cases: Vec<(&str, Mock, fn(Result<StatResponse>))> = vec![
            (
                "modelscope://owner/repo/config.json",
                Mock::given(method("GET"))
                    .and(path("/models/owner/repo/resolve/master/config.json"))
                    .and(header("authorization", "Bearer secret"))
                    .respond_with(ResponseTemplate::new(200).set_body_string("file content")),
                |result| {
                    let response = result.unwrap();
                    assert!(response.success);
                    assert_eq!(response.content_length, Some(12));
                    assert!(response.entries.is_empty());
                },
            ),
            (
                "modelscope://owner/repo/config.json",
                Mock::given(method("GET"))
                    .and(path("/models/owner/repo/resolve/master/config.json"))
                    .respond_with(ResponseTemplate::new(404)),
                |result| {
                    assert!(
                        matches!(&result, Err(Error::BackendError(err)) if err.status_code == Some(StatusCode::NOT_FOUND))
                    );
                },
            ),
            (
                "modelscope://owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/v1/models/owner/repo/repo/files"))
                    .and(query_param("Revision", "master"))
                    .and(query_param("Recursive", "true"))
                    .and(header("authorization", "Bearer secret"))
                    .and(header("user-agent", DEFAULT_USER_AGENT))
                    .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "Code": 200,
                        "Data": {
                            "Files": [
                                {"Path": "nested/config file.json", "Type": "blob", "Size": 12},
                                {"Path": "model.bin", "Type": "blob", "Size": 4096},
                                {"Path": "README.md", "Type": "blob"},
                                {"Path": "nested", "Type": "tree", "Size": 0}
                            ]
                        }
                    }))),
                |result| {
                    let response = result.unwrap();
                    assert!(response.success);
                    assert_eq!(response.entries.len(), 3);
                    assert_eq!(
                        response.entries[0],
                        DirEntry {
                            url: "modelscope://owner/repo/nested/config%20file.json".to_string(),
                            content_length: 12,
                            is_dir: false,
                        }
                    );
                    assert_eq!(response.entries[1].content_length, 4096);
                    assert_eq!(response.entries[2].content_length, 0);
                },
            ),
            (
                "modelscope://datasets/owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/v1/datasets/owner/repo/repo/files"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "Code": 200,
                        "Data": {
                            "Files": [
                                {"Path": "nested/train.json", "Type": "blob"},
                                {"Path": "README.md", "Type": "blob"}
                            ]
                        }
                    }))),
                |result| {
                    let response = result.unwrap();
                    assert!(response.success);
                    assert_eq!(response.entries.len(), 2);
                    assert_eq!(
                        response.entries[0],
                        DirEntry {
                            url: "modelscope://datasets/owner/repo/nested/train.json".to_string(),
                            content_length: 0,
                            is_dir: false,
                        }
                    );
                },
            ),
            (
                "modelscope://owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/v1/models/owner/repo/repo/files"))
                    .respond_with(
                        ResponseTemplate::new(200).set_body_json(
                            serde_json::json!({"Code": 200, "Data": {"Files": null}}),
                        ),
                    ),
                |result| {
                    let response = result.unwrap();
                    assert!(response.success);
                    assert!(response.entries.is_empty());
                },
            ),
            (
                "modelscope://owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/v1/models/owner/repo/repo/files"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "Code": 404,
                        "Message": "repo not found",
                        "Data": {}
                    }))),
                |result| {
                    assert!(
                        matches!(&result, Err(Error::BackendError(err)) if err.status_code.is_none() && err.message.contains("code=404, message=repo not found"))
                    );
                },
            ),
            (
                "modelscope://owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/v1/models/owner/repo/repo/files"))
                    .respond_with(ResponseTemplate::new(401)),
                |result| {
                    assert!(
                        matches!(&result, Err(Error::BackendError(err)) if err.status_code == Some(StatusCode::UNAUTHORIZED))
                    );
                },
            ),
            (
                "modelscope://owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/v1/models/owner/repo/repo/files"))
                    .respond_with(ResponseTemplate::new(200).set_body_string("not json")),
                |result| {
                    assert!(
                        matches!(&result, Err(Error::BackendError(err)) if err.status_code.is_none())
                    );
                },
            ),
        ];

        let backend = ModelScope::new(Arc::new(Config::default())).unwrap();
        for (url, mock, expect) in test_cases {
            let server = MockServer::start().await;
            mock.mount(&server).await;
            expect(
                backend
                    .stat(StatRequest {
                        task_id: "task".to_string(),
                        url: url.to_string(),
                        http_header: None,
                        timeout: Duration::from_secs(5),
                        client_cert: None,
                        object_storage: None,
                        hdfs: None,
                        hugging_face: None,
                        model_scope: Some(ModelScopeOptions {
                            revision: "master".to_string(),
                            token: Some("secret".to_string()),
                            base_url: Some(server.uri()),
                        }),
                        open_csg: None,
                    })
                    .await,
            );
        }
    }

    #[tokio::test]
    async fn get_streams_body_and_validates_ranged_responses() {
        let test_cases: Vec<(Option<Range>, Mock, fn(&GetResponse<Body>, &str))> = vec![
            (
                None,
                Mock::given(method("GET"))
                    .and(path("/models/owner/repo/resolve/master/config.json"))
                    .respond_with(ResponseTemplate::new(200).set_body_string("full content")),
                |response, text| {
                    assert!(response.success);
                    assert_eq!(response.http_status_code, Some(StatusCode::OK));
                    assert_eq!(text, "full content");
                },
            ),
            (
                Some(Range {
                    start: 10,
                    length: 20,
                }),
                Mock::given(method("GET"))
                    .and(path("/models/owner/repo/resolve/master/config.json"))
                    .and(header("range", "bytes=10-29"))
                    .respond_with(
                        ResponseTemplate::new(206)
                            .insert_header("content-range", "bytes 10-29/100")
                            .set_body_string("partial content"),
                    ),
                |response, text| {
                    assert!(response.success);
                    assert_eq!(response.http_status_code, Some(StatusCode::PARTIAL_CONTENT));
                    assert_eq!(text, "partial content");
                },
            ),
            (
                Some(Range {
                    start: 10,
                    length: 20,
                }),
                Mock::given(method("GET"))
                    .and(path("/models/owner/repo/resolve/master/config.json"))
                    .and(header("range", "bytes=10-29"))
                    .respond_with(ResponseTemplate::new(200).set_body_string("full body content")),
                |response, text| {
                    assert!(!response.success);
                    assert_eq!(response.http_status_code, Some(StatusCode::OK));
                    assert!(response
                        .error_message
                        .as_deref()
                        .unwrap()
                        .contains("expected 206 Partial Content"));
                    assert_eq!(text, "");
                },
            ),
            (
                None,
                Mock::given(method("GET"))
                    .and(path("/models/owner/repo/resolve/master/config.json"))
                    .respond_with(ResponseTemplate::new(404)),
                |response, text| {
                    assert!(!response.success);
                    assert_eq!(response.http_status_code, Some(StatusCode::NOT_FOUND));
                    assert_eq!(text, "");
                },
            ),
        ];

        let backend = ModelScope::new(Arc::new(Config::default())).unwrap();
        for (range, mock, expect) in test_cases {
            let server = MockServer::start().await;
            mock.mount(&server).await;
            let mut response = backend
                .get(GetRequest {
                    task_id: "task".to_string(),
                    piece_id: "piece".to_string(),
                    url: "modelscope://owner/repo/config.json".to_string(),
                    range,
                    http_header: None,
                    timeout: Duration::from_secs(5),
                    client_cert: None,
                    object_storage: None,
                    hdfs: None,
                    hugging_face: None,
                    model_scope: Some(ModelScopeOptions {
                        revision: "master".to_string(),
                        token: None,
                        base_url: Some(server.uri()),
                    }),
                    open_csg: None,
                })
                .await
                .unwrap();
            let text = response.text().await.unwrap();
            expect(&response, &text);
        }
    }

    #[tokio::test]
    async fn get_follows_redirect_without_forwarding_token() {
        let server = MockServer::start().await;
        let object_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models/owner/repo/resolve/master/config.json"))
            .and(header("range", "bytes=10-29"))
            .respond_with(ResponseTemplate::new(302).insert_header(
                "location",
                format!("{}/objects/config.json", object_server.uri()),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/objects/config.json"))
            .and(header("range", "bytes=10-29"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("content-range", "bytes 10-29/100")
                    .set_body_string("redirected cdn data!"),
            )
            .mount(&object_server)
            .await;

        let mut response = ModelScope::new(Arc::new(Config::default()))
            .unwrap()
            .get(GetRequest {
                task_id: "task".to_string(),
                piece_id: "piece".to_string(),
                url: "modelscope://owner/repo/config.json".to_string(),
                range: Some(Range {
                    start: 10,
                    length: 20,
                }),
                http_header: None,
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: Some(ModelScopeOptions {
                    revision: "master".to_string(),
                    token: Some("secret".to_string()),
                    base_url: Some(server.uri()),
                }),
                open_csg: None,
            })
            .await
            .unwrap();

        assert!(response.success);
        assert_eq!(response.text().await.unwrap(), "redirected cdn data!");

        let object_requests = object_server.received_requests().await.unwrap();
        assert_eq!(object_requests.len(), 1);
        assert!(object_requests[0].headers.get("authorization").is_none());
    }

    #[tokio::test]
    async fn get_rejects_missing_options_or_file_path() {
        let test_cases = vec![
            ("modelscope://owner/repo/config.json", None),
            (
                "modelscope://owner/repo",
                Some(ModelScopeOptions::default()),
            ),
        ];

        let backend = ModelScope::new(Arc::new(Config::default())).unwrap();
        for (url, model_scope) in test_cases {
            let result = backend
                .get(GetRequest {
                    task_id: "task".to_string(),
                    piece_id: "piece".to_string(),
                    url: url.to_string(),
                    range: None,
                    http_header: None,
                    timeout: Duration::from_secs(5),
                    client_cert: None,
                    object_storage: None,
                    hdfs: None,
                    hugging_face: None,
                    model_scope,
                    open_csg: None,
                })
                .await;
            assert!(matches!(result, Err(Error::InvalidParameter)));
        }
    }

    #[tokio::test]
    async fn exists_reports_file_and_repository_presence() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/models/owner/repo/resolve/master/config.json"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v1/models/owner/repo/repo/files"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let backend = ModelScope::new(Arc::new(Config::default())).unwrap();

        let test_cases = vec![
            ("modelscope://owner/repo/config.json", true),
            ("modelscope://owner/repo", true),
            ("modelscope://owner/repo/missing.json", false),
            ("modelscope://owner/missing", false),
        ];

        for (url, expected) in test_cases {
            let exists = backend
                .exists(ExistsRequest {
                    task_id: "task".to_string(),
                    url: url.to_string(),
                    http_header: None,
                    timeout: Duration::from_secs(5),
                    client_cert: None,
                    object_storage: None,
                    hdfs: None,
                    hugging_face: None,
                    model_scope: Some(ModelScopeOptions {
                        revision: "master".to_string(),
                        token: None,
                        base_url: Some(server.uri()),
                    }),
                    open_csg: None,
                })
                .await
                .unwrap();
            assert_eq!(exists, expected);
        }
    }
}
