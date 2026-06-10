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
    Backend, Body, DirEntry, ExistsRequest, GetRequest, GetResponse, PutRequest, PutResponse,
    StatRequest, StatResponse, DEFAULT_USER_AGENT, KEEP_ALIVE_INTERVAL, POOL_MAX_IDLE_PER_HOST,
};
use async_trait::async_trait;
use dragonfly_api::common::v2::Range;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    error::{BackendError, ErrorType, OrErr},
    Error, Result,
};
use dragonfly_client_util::tls::NoVerifier;
use futures::TryStreamExt;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_LENGTH, RANGE, USER_AGENT};
use reqwest::Client;
use serde::Deserialize;
use std::error::Error as _;
use std::io::Error as IOError;
use std::sync::Arc;
use tokio_util::io::StreamReader;
use tracing::{debug, error};
use url::Url;

/// SCHEME is the URL scheme for ModelScope backend.
pub const SCHEME: &str = "modelscope";

/// MODEL_SCOPE_BASE_URL is the base URL for ModelScope Hub.
const MODEL_SCOPE_BASE_URL: &str = "https://modelscope.cn";

/// Response represents the top-level response from the ModelScope API.
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

/// File represents a file entry returned by the ModelScope API.
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

/// RepositoryType implements methods for getting string representations and API paths.
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

    /// try_from parses the URL and returns a ParsedURL.
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

/// ParsedURL implements TryFrom for &str.
impl TryFrom<&str> for ParsedURL {
    type Error = Error;

    /// Try to parse a string URL into a ParsedURL struct.
    fn try_from(url: &str) -> std::result::Result<Self, Self::Error> {
        let parsed_url = Url::parse(url).or_err(ErrorType::ParseError)?;
        ParsedURL::try_from(parsed_url)
    }
}

/// ModelScope is the ModelScope backend implementation.
pub struct ModelScope {
    /// Scheme is the scheme of the ModelScope backend.
    scheme: String,

    /// HTTP client for making requests.
    client: Client,
}

/// ModelScope implements the ModelScope interface.
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
    /// Scheme returns the scheme of the backend.
    fn scheme(&self) -> String {
        self.scheme.clone()
    }

    /// Stat the metadata from the backend.
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
                            "stat request failed {} {}: {}",
                            request.task_id, download_url, err
                        );

                        return Ok(StatResponse {
                            success: false,
                            content_length: None,
                            http_header: None,
                            http_status_code: None,
                            entries: Vec::new(),
                            error_message: Some(err.to_string()),
                        });
                    }
                };

                let response_status_code = response.status();
                let response_header = response.headers().clone();
                let content_length = match response_header.get(CONTENT_LENGTH) {
                    Some(content_length) => content_length.to_str()?.parse::<u64>().ok(),
                    None => response.content_length(),
                };

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
                let response = match self
                    .client
                    .get(file_list_url.as_str())
                    .headers(request_header)
                    .timeout(request.timeout)
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(err) => {
                        error!(
                            "stat request failed {} {}: {}",
                            request.task_id, file_list_url, err
                        );

                        return Ok(StatResponse {
                            success: false,
                            content_length: None,
                            http_header: None,
                            http_status_code: None,
                            entries: Vec::new(),
                            error_message: Some(err.to_string()),
                        });
                    }
                };

                let response_status_code = response.status();
                let response_header = response.headers().clone();
                let content_length = match response_header.get(CONTENT_LENGTH) {
                    Some(content_length) => content_length.to_str()?.parse::<u64>().ok(),
                    None => response.content_length(),
                };

                if !response.status().is_success() {
                    return Ok(StatResponse {
                        success: false,
                        content_length: None,
                        http_header: Some(response_header),
                        http_status_code: response_status_code.into(),
                        error_message: Some(response_status_code.to_string()),
                        entries: Vec::new(),
                    });
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
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>> {
        debug!(
            "get request {} {} {}: {:?}",
            request.task_id, request.piece_id, request.url, request.http_header
        );

        // Build request headers, including authentication if provided ModelScope token.
        let request_header = Self::build_request_headers(
            request.model_scope.as_ref().and_then(|ms| ms.token.clone()),
            None,
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
                    reader: Box::new(tokio::io::empty()),
                    error_message: Some(err.to_string()),
                });
            }
        };

        let response_header = response.headers().clone();
        let response_status_code = response.status();
        let response_reader = Box::new(StreamReader::new(response.bytes_stream().map_err(
            move |err| {
                let mut chain = err.to_string();
                let mut source = err.source();
                while let Some(err) = source {
                    chain.push_str(": ");
                    chain.push_str(&err.to_string());
                    source = err.source();
                }

                IOError::new(ErrorKind::Other, chain)
            },
        )));

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
    async fn put(&self, _request: PutRequest) -> Result<PutResponse> {
        unimplemented!()
    }

    /// Exists checks whether the file exists in the backend.
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
    use super::*;
    use crate::DEFAULT_USER_AGENT;

    #[test]
    fn test_parse_url_simple() {
        let parsed_url = ParsedURL::try_from("modelscope://deepseek-ai/DeepSeek-R1").unwrap();
        assert_eq!(parsed_url.repository_id, "deepseek-ai/DeepSeek-R1");
        assert_eq!(parsed_url.repository_type, RepositoryType::Model);
        assert!(parsed_url.file_path.is_none());
    }

    #[test]
    fn test_parse_url_with_file() {
        let parsed_url =
            ParsedURL::try_from("modelscope://deepseek-ai/DeepSeek-R1/config.json").unwrap();
        assert_eq!(parsed_url.repository_id, "deepseek-ai/DeepSeek-R1");
        assert_eq!(parsed_url.repository_type, RepositoryType::Model);
        assert_eq!(parsed_url.file_path, Some("config.json".to_string()));
    }

    #[test]
    fn test_parse_url_with_nested_path() {
        let parsed_url =
            ParsedURL::try_from("modelscope://deepseek-ai/DeepSeek-R1/models/v1/model.bin")
                .unwrap();
        assert_eq!(parsed_url.repository_id, "deepseek-ai/DeepSeek-R1");
        assert_eq!(parsed_url.repository_type, RepositoryType::Model);
        assert_eq!(
            parsed_url.file_path,
            Some("models/v1/model.bin".to_string())
        );
    }

    #[test]
    fn test_parse_url_dataset() {
        let parsed_url = ParsedURL::try_from("modelscope://datasets/owner/my-dataset").unwrap();
        assert_eq!(parsed_url.repository_id, "owner/my-dataset");
        assert_eq!(parsed_url.repository_type, RepositoryType::Dataset);
        assert!(parsed_url.file_path.is_none());
    }

    #[test]
    fn test_parse_url_dataset_with_path() {
        let parsed_url =
            ParsedURL::try_from("modelscope://datasets/owner/my-dataset/train.json").unwrap();
        assert_eq!(parsed_url.repository_id, "owner/my-dataset");
        assert_eq!(parsed_url.repository_type, RepositoryType::Dataset);
        assert_eq!(parsed_url.file_path, Some("train.json".to_string()));
    }

    #[test]
    fn test_parse_url_explicit_model_type() {
        let parsed_url =
            ParsedURL::try_from("modelscope://models/deepseek-ai/DeepSeek-R1/config.json").unwrap();
        assert_eq!(parsed_url.repository_id, "deepseek-ai/DeepSeek-R1");
        assert_eq!(parsed_url.repository_type, RepositoryType::Model);
        assert_eq!(parsed_url.file_path, Some("config.json".to_string()));
    }

    #[test]
    fn test_parse_url_missing_repo() {
        let result = ParsedURL::try_from("modelscope://deepseek-ai");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_url_trailing_slash() {
        let parsed_url = ParsedURL::try_from("modelscope://deepseek-ai/DeepSeek-R1/").unwrap();
        assert_eq!(parsed_url.repository_id, "deepseek-ai/DeepSeek-R1");
        assert_eq!(parsed_url.repository_type, RepositoryType::Model);
        assert!(parsed_url.file_path.is_none());
    }

    #[test]
    fn test_build_download_url_model() {
        let parsed_url =
            ParsedURL::try_from("modelscope://deepseek-ai/DeepSeek-R1/config.json").unwrap();
        let url = ModelScope::build_download_url(
            &parsed_url,
            "config.json",
            "master",
            &Url::parse(MODEL_SCOPE_BASE_URL).unwrap(),
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://modelscope.cn/models/deepseek-ai/DeepSeek-R1/resolve/master/config.json"
        );
    }

    #[test]
    fn test_build_download_url_dataset() {
        let parsed_url =
            ParsedURL::try_from("modelscope://datasets/owner/my-dataset/train.json").unwrap();
        let url = ModelScope::build_download_url(
            &parsed_url,
            "train.json",
            "master",
            &Url::parse(MODEL_SCOPE_BASE_URL).unwrap(),
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://modelscope.cn/datasets/owner/my-dataset/resolve/master/train.json"
        );
    }

    #[test]
    fn test_build_download_url_with_revision() {
        let parsed_url =
            ParsedURL::try_from("modelscope://deepseek-ai/DeepSeek-R1/config.json").unwrap();
        let url = ModelScope::build_download_url(
            &parsed_url,
            "config.json",
            "v1.0",
            &Url::parse(MODEL_SCOPE_BASE_URL).unwrap(),
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://modelscope.cn/models/deepseek-ai/DeepSeek-R1/resolve/v1.0/config.json"
        );
    }

    #[test]
    fn test_build_file_list_url_model() {
        let parsed_url = ParsedURL::try_from("modelscope://deepseek-ai/DeepSeek-R1").unwrap();
        let url = ModelScope::build_file_list_url(
            &parsed_url,
            "master",
            &Url::parse("https://modelscope.cn/api/v1/").unwrap(),
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://modelscope.cn/api/v1/models/deepseek-ai/DeepSeek-R1/repo/files?Revision=master&Recursive=true"
        );
    }

    #[test]
    fn test_build_file_list_url_dataset() {
        let parsed_url = ParsedURL::try_from("modelscope://datasets/owner/my-dataset").unwrap();
        let url = ModelScope::build_file_list_url(
            &parsed_url,
            "master",
            &Url::parse("https://modelscope.cn/api/v1/").unwrap(),
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://modelscope.cn/api/v1/datasets/owner/my-dataset/repo/files?Revision=master&Recursive=true"
        );
    }

    #[test]
    fn test_resolve_base_urls() {
        let (base_url, api_base_url) = ModelScope::resolve_base_urls(None).unwrap();
        assert_eq!(base_url.as_str(), "https://modelscope.cn/");
        assert_eq!(api_base_url.as_str(), "https://modelscope.cn/api/v1/");

        let (base_url, api_base_url) =
            ModelScope::resolve_base_urls(Some("https://modelscope-mirror.example.com/")).unwrap();
        assert_eq!(base_url.as_str(), "https://modelscope-mirror.example.com/");
        assert_eq!(
            api_base_url.as_str(),
            "https://modelscope-mirror.example.com/api/v1/"
        );
    }

    #[test]
    fn test_build_model_scope_url_model() {
        let parsed_url = ParsedURL::try_from("modelscope://deepseek-ai/DeepSeek-R1").unwrap();
        let url = ModelScope::build_model_scope_url(&parsed_url, "config.json").unwrap();
        assert_eq!(
            url.as_str(),
            "modelscope://deepseek-ai/DeepSeek-R1/config.json"
        );
    }

    #[test]
    fn test_build_model_scope_url_dataset() {
        let parsed_url = ParsedURL::try_from("modelscope://datasets/owner/my-dataset").unwrap();
        let url = ModelScope::build_model_scope_url(&parsed_url, "train.json").unwrap();
        assert_eq!(
            url.as_str(),
            "modelscope://datasets/owner/my-dataset/train.json"
        );
    }

    #[test]
    fn test_build_model_scope_url_nested_file() {
        let parsed_url = ParsedURL::try_from("modelscope://deepseek-ai/DeepSeek-R1").unwrap();
        let url = ModelScope::build_model_scope_url(&parsed_url, "models/v1/model.bin").unwrap();
        assert_eq!(
            url.as_str(),
            "modelscope://deepseek-ai/DeepSeek-R1/models/v1/model.bin"
        );
    }

    #[test]
    fn test_build_headers_default_user_agent() {
        let request_header = ModelScope::build_request_headers(None, None).unwrap();
        assert_eq!(
            request_header.get(USER_AGENT).unwrap(),
            HeaderValue::from_static(DEFAULT_USER_AGENT)
        );
    }

    #[test]
    fn test_build_headers_with_token() {
        let request_headers =
            ModelScope::build_request_headers(Some("test-token".to_string()), None).unwrap();
        assert_eq!(
            request_headers.get(reqwest::header::AUTHORIZATION).unwrap(),
            "Bearer test-token"
        );
        assert_eq!(
            request_headers.get(USER_AGENT).unwrap(),
            HeaderValue::from_static(DEFAULT_USER_AGENT)
        );
    }

    #[test]
    fn test_build_headers_without_token() {
        let request_headers = ModelScope::build_request_headers(None, None).unwrap();
        assert!(request_headers
            .get(reqwest::header::AUTHORIZATION)
            .is_none());
        assert_eq!(
            request_headers.get(USER_AGENT).unwrap(),
            HeaderValue::from_static(DEFAULT_USER_AGENT)
        );
    }

    #[test]
    fn test_build_headers_with_range() {
        let range = Range {
            start: 100,
            length: 200,
        };
        let request_headers = ModelScope::build_request_headers(None, Some(range)).unwrap();
        assert_eq!(
            request_headers.get(reqwest::header::RANGE).unwrap(),
            "bytes=100-299"
        );
    }

    #[test]
    fn test_build_headers_with_token_and_range() {
        let range = Range {
            start: 0,
            length: 1024,
        };
        let request_headers =
            ModelScope::build_request_headers(Some("my-secret-token".to_string()), Some(range))
                .unwrap();
        assert_eq!(
            request_headers.get(reqwest::header::AUTHORIZATION).unwrap(),
            "Bearer my-secret-token"
        );
        assert_eq!(
            request_headers.get(reqwest::header::RANGE).unwrap(),
            "bytes=0-1023"
        );
        assert_eq!(
            request_headers.get(USER_AGENT).unwrap(),
            HeaderValue::from_static(DEFAULT_USER_AGENT)
        );
    }

    #[test]
    fn test_repository_type_as_str() {
        assert_eq!(RepositoryType::Model.as_str(), "models");
        assert_eq!(RepositoryType::Dataset.as_str(), "datasets");
    }
}
