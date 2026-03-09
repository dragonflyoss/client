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

//! Hugging Face backend implementation for downloading models and datasets.
//!
//! This module provides support for the `hf://` URL scheme to download files from
//! Hugging Face Hub repositories. It handles both regular files and Git LFS files
//! (large model files) through the Hugging Face HTTP API.
//!
//! # URL Format
//!
//! The URL format is: `hf://<repo_id>[/<path>][@<revision>]`
//!
//! Examples:
//! - `hf://deepseek-ai/DeepSeek-OCR` - Download entire repository
//! - `hf://deepseek-ai/DeepSeek-OCR/model.safetensors` - Download specific file
//! - `hf://deepseek-ai/DeepSeek-OCR@main` - Download from specific revision
//!
//! # Authentication
//!
//! For private repositories or to increase rate limits, use the `--hf-token` flag.

use crate::{
    Backend, Body, DirEntry, ExistsRequest, GetRequest, GetResponse, PutRequest, PutResponse,
    StatRequest, StatResponse, DEFAULT_USER_AGENT, KEEP_ALIVE_INTERVAL, POOL_MAX_IDLE_PER_HOST,
};
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
use std::io::{Error as IOError, ErrorKind};
use std::sync::Arc;
use tokio_util::io::StreamReader;
use tracing::{debug, error};
use url::Url;

/// HUGGING_FACE_SCHEME is the URL scheme for Hugging Face backend.
pub const HUGGING_FACE_SCHEME: &str = "hf";

/// HUGGING_FACE_BASE_URL is the base URL for Hugging Face Hub.
const HUGGING_FACE_BASE_URL: &str = "https://huggingface.co";

/// HUGGING_FACE_API_BASE_URL is the API base URL for Hugging Face API.
const HUGGING_FACE_API_BASE_URL: &str = "https://huggingface.co/api";

/// Repository represents the Hugging Face repository information returned by the API.
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "camelCase")]
#[allow(dead_code)]
struct Repository {
    #[serde(rename = "_id")]
    id: String,
    model_id: Option<String>,
    private: bool,
    siblings: Option<Vec<Sibling>>,
}

/// Sibling represents a file or directory in the Hugging Face repository.
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Sibling {
    rfilename: String,
    size: Option<u64>,
    lfs: Option<Lfs>,
}

/// Lfs represents Git LFS metadata for large files in the Hugging Face repository.
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "camelCase")]
#[allow(dead_code)]
struct Lfs {
    size: u64,
    sha256: Option<String>,
    pointer_size: Option<u64>,
}

/// A parsed representation of a Hugging Face URL.
///
/// Format: `hf://[<repository_type>/]<owner>/<repository>[/<path>][@<revision>]`
#[derive(Debug, Clone)]
pub struct ParsedURL {
    /// The original, unparsed URL.
    pub url: Url,

    /// The repository identifier in `<owner>/<repository>` format (e.g., `"deepseek-ai/DeepSeek-OCR"`).
    pub repository_id: String,

    /// The type of repository: model, dataset, or space.
    pub repository_type: RepositoryType,

    /// An optional file path within the repository (e.g., `"path/to/weights.bin"`).
    pub file_path: Option<String>,
}

/// The type of a Hugging Face repository.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RepositoryType {
    /// A model repository. This is the default when no type prefix is specified,
    /// or when explicitly prefixed with `models/`.
    Model,

    /// A dataset repository, prefixed with `datasets/`.
    Dataset,

    /// A space repository, prefixed with `spaces/`.
    Space,
}

/// RepositoryType implements methods for getting string representations and API paths.
impl RepositoryType {
    /// Returns the canonical string identifier (e.g., `"models"`, `"datasets"`, `"spaces"`).
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            RepositoryType::Model => "models",
            RepositoryType::Dataset => "datasets",
            RepositoryType::Space => "spaces",
        }
    }
}

/// Parses a Hugging Face URL into its constituent components.
///
/// URL Format: hf://[<repository_type>/]<owner>/<repository>[/<path>][@<revision>]
/// - repository_type  Optional. One of "models" (default), "datasets", or "spaces".
/// - owner/repository Required. For example, "meta-llama/Llama-2-7b".
/// - path             Optional file path within the repository.
/// - revision         Optional git ref after "@".
///
/// Examples:
///   - URL: https://huggingface.co/meta-llama/Llama-2-7b => Type: Model, Repository ID: meta-llama/Llama-2-7b, Path: None, Revision: main
///   - URL: https://huggingface.co/datasets/squad/data/train.json@v1.0 => Type: Dataset, Repository ID: squad/data, Path: train.json, Revision: v1.0
///   - URL: https://huggingface.co/models/owner/repo/path/to/file@dev => Type: Model, Repository ID: owner/repo, Path: path/to/file, Revision: dev
///
/// Errors:
///   Returns Error::InvalidURI if the URL has no host, or Error::InvalidParameter
///   if the path does not contain at least an owner and repository name.
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
            Some(&"spaces") => (RepositoryType::Space, 1),
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

/// HuggingFace is the Hugging Face backend implementation.
pub struct HuggingFace {
    /// Scheme is the scheme of the Hugging Face backend.
    scheme: String,

    /// HTTP client for making requests.
    client: Client,
}

/// HuggingFace implements the hugging face interface.
impl HuggingFace {
    /// Create a new HuggingFace backend.
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
            scheme: HUGGING_FACE_SCHEME.to_string(),
            client,
        })
    }

    /// Builds the download URL for a file based on the repository type and path.
    fn build_download_url(parsed_url: &ParsedURL, file_path: &str, revision: &str) -> String {
        match parsed_url.repository_type {
            RepositoryType::Model => {
                format!(
                    "{}/{}/resolve/{}/{}",
                    HUGGING_FACE_BASE_URL, parsed_url.repository_id, revision, file_path
                )
            }
            RepositoryType::Dataset => {
                format!(
                    "{}/datasets/{}/resolve/{}/{}",
                    HUGGING_FACE_BASE_URL, parsed_url.repository_id, revision, file_path
                )
            }
            RepositoryType::Space => {
                format!(
                    "{}/spaces/{}/resolve/{}/{}",
                    HUGGING_FACE_BASE_URL, parsed_url.repository_id, revision, file_path
                )
            }
        }
    }

    /// Builds the API URL for fetching repository information based on the repository type and ID.
    fn build_repository_url(parsed_url: &ParsedURL) -> String {
        format!(
            "{}/{}/{}",
            HUGGING_FACE_API_BASE_URL,
            parsed_url.repository_type.as_str(),
            parsed_url.repository_id
        )
    }

    /// Builds the API URL for fetching repository information at a specific revision.
    fn build_repository_revision_url(parsed_url: &ParsedURL, revision: &str) -> String {
        format!(
            "{}/{}/{}?revision={}",
            HUGGING_FACE_API_BASE_URL,
            parsed_url.repository_type.as_str(),
            parsed_url.repository_id,
            revision
        )
    }

    /// Builds an `hf://` URL for a file so downstream downloads continue to
    /// use the HF backend (preserving auth and URL semantics).
    fn build_hf_url(parsed_url: &ParsedURL, filename: &str) -> String {
        match parsed_url.repository_type {
            RepositoryType::Model => format!(
                "{}://{}/{}",
                HUGGING_FACE_SCHEME, parsed_url.repository_id, filename
            ),
            RepositoryType::Dataset => format!(
                "{}://datasets/{}/{}",
                HUGGING_FACE_SCHEME, parsed_url.repository_id, filename
            ),
            RepositoryType::Space => format!(
                "{}://spaces/{}/{}",
                HUGGING_FACE_SCHEME, parsed_url.repository_id, filename
            ),
        }
    }

    /// Build the request headers for Hugging Face API requests, including authentication if a
    /// token is provided by the `--hf-token` CLI flag.
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

        // Add the Authorization header for Hugging Face API authentication.
        if let Some(token) = token {
            request_header.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
            );
        }

        Ok(request_header)
    }
}

/// Backend implementation for Hugging Face.
#[tonic::async_trait]
impl Backend for HuggingFace {
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

        // Build request headers, including authentication if provided hugging face token.
        let request_header = Self::build_request_headers(
            request
                .hugging_face
                .as_ref()
                .and_then(|hf| hf.token.clone()),
            None,
        )?;

        // Get the revision from the request, request must contain revision for stat request,
        // otherwise return error.
        let revision = request
            .hugging_face
            .ok_or_else(|| {
                error!(
                    "stat request {} {}: missing Hugging Face information",
                    request.task_id, request.url
                );

                Error::InvalidParameter
            })?
            .revision;

        let parsed_url = ParsedURL::try_from(request.url.as_str())?;
        match &parsed_url.file_path {
            Some(file_path) => {
                let download_url = Self::build_download_url(&parsed_url, file_path, &revision);
                let response = match self
                    .client
                    .head(&download_url)
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
                            error_message: None,
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
                let repository_revision_url =
                    Self::build_repository_revision_url(&parsed_url, &revision);
                let response = match self
                    .client
                    .get(&repository_revision_url)
                    .headers(request_header)
                    .timeout(request.timeout)
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(err) => {
                        error!(
                            "stat request failed {} {}: {}",
                            request.task_id, repository_revision_url, err
                        );

                        return Ok(StatResponse {
                            success: false,
                            content_length: None,
                            http_header: None,
                            http_status_code: None,
                            entries: Vec::new(),
                            error_message: None,
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
                        request.task_id, repository_revision_url, err
                    );

                    Error::BackendError(Box::new(BackendError {
                        message: err.to_string(),
                        status_code: None,
                        header: None,
                    }))
                })?;

                let repository: Repository = serde_json::from_str(&text).map_err(|err| {
                    error!(
                        "stat request failed {} {}: {}",
                        request.task_id, repository_revision_url, err
                    );

                    Error::BackendError(Box::new(BackendError {
                        message: err.to_string(),
                        status_code: None,
                        header: None,
                    }))
                })?;

                let entries = repository
                    .siblings
                    .unwrap_or_default()
                    .into_iter()
                    .map(|sibling| {
                        // Return hf:// URLs so downstream downloads continue to use the HF
                        // backend (preserving auth headers and URL semantics).
                        let hf_url = Self::build_hf_url(&parsed_url, &sibling.rfilename);
                        let content_length = sibling
                            .lfs
                            .as_ref()
                            .map(|lfs| lfs.size)
                            .or(sibling.size)
                            .unwrap_or(0);

                        DirEntry {
                            url: hf_url,
                            content_length: content_length as usize,
                            is_dir: false,
                        }
                    })
                    .collect();

                debug!(
                    "stat response {} {}: {:?} {:?} {:?}",
                    request.task_id,
                    repository_revision_url,
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

        // Build request headers, including authentication if provided hugging face token.
        let request_header = Self::build_request_headers(
            request
                .hugging_face
                .as_ref()
                .and_then(|hf| hf.token.clone()),
            None,
        )?;

        // Get the revision from the request, request must contain revision for stat request,
        // otherwise return error.
        let revision = request
            .hugging_face
            .ok_or_else(|| {
                error!(
                    "get request {} {}: missing Hugging Face information",
                    request.task_id, request.url
                );

                Error::InvalidParameter
            })?
            .revision;

        // Parse the URL and build the download URL for the specified file.
        let parsed_url = ParsedURL::try_from(request.url.as_str())?;
        let Some(file_path) = &parsed_url.file_path else {
            error!(
                "get request {} {}: URL must specify a file path",
                request.task_id, request.url
            );

            return Err(Error::InvalidParameter);
        };
        let download_url = Self::build_download_url(&parsed_url, file_path, &revision);
        let response = match self
            .client
            .get(&download_url)
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
        let response_reader = Box::new(StreamReader::new(
            response
                .bytes_stream()
                .map_err(|err| IOError::new(ErrorKind::Other, err)),
        ));

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

        // Build request headers, including authentication if provided hugging face token.
        let request_header = Self::build_request_headers(
            request
                .hugging_face
                .as_ref()
                .and_then(|hf| hf.token.clone()),
            None,
        )?;

        // Get the revision from the request, request must contain revision for stat request,
        // otherwise return error.
        let revision = request
            .hugging_face
            .ok_or_else(|| {
                error!(
                    "stat request {} {}: missing Hugging Face information",
                    request.task_id, request.url
                );

                Error::InvalidParameter
            })?
            .revision;

        let parsed_url = ParsedURL::try_from(request.url.as_str())?;
        match &parsed_url.file_path {
            Some(file_path) => {
                let download_url = Self::build_download_url(&parsed_url, file_path, &revision);
                let response = self
                    .client
                    .head(&download_url)
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
                let repository_url = Self::build_repository_url(&parsed_url);
                let response = self
                    .client
                    .head(&repository_url)
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
        let parsed_url = ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR").unwrap();
        assert_eq!(parsed_url.repository_id, "deepseek-ai/DeepSeek-OCR");
        assert_eq!(parsed_url.repository_type, RepositoryType::Model);
        assert!(parsed_url.file_path.is_none());
    }

    #[test]
    fn test_parse_url_with_file() {
        let parsed_url =
            ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR/model.safetensors").unwrap();
        assert_eq!(parsed_url.repository_id, "deepseek-ai/DeepSeek-OCR");
        assert_eq!(parsed_url.repository_type, RepositoryType::Model);
        assert_eq!(parsed_url.file_path, Some("model.safetensors".to_string()));
    }

    #[test]
    fn test_parse_url_with_revision() {
        let parsed_url = ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR").unwrap();
        assert_eq!(parsed_url.repository_id, "deepseek-ai/DeepSeek-OCR");
        assert!(parsed_url.file_path.is_none());
    }

    #[test]
    fn test_parse_url_with_nested_path() {
        let parsed_url =
            ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR/models/v1/model.bin").unwrap();
        assert_eq!(parsed_url.repository_id, "deepseek-ai/DeepSeek-OCR");
        assert_eq!(parsed_url.repository_type, RepositoryType::Model);
        assert_eq!(
            parsed_url.file_path,
            Some("models/v1/model.bin".to_string())
        );
    }

    #[test]
    fn test_parse_url_dataset() {
        let parsed_url = ParsedURL::try_from("hf://datasets/huggingface/squad").unwrap();
        assert_eq!(parsed_url.repository_id, "huggingface/squad");
        assert_eq!(parsed_url.repository_type, RepositoryType::Dataset);
        assert!(parsed_url.file_path.is_none());
    }

    #[test]
    fn test_parse_url_dataset_with_path() {
        let parsed_url = ParsedURL::try_from("hf://datasets/huggingface/squad/train.json").unwrap();
        assert_eq!(parsed_url.repository_id, "huggingface/squad");
        assert_eq!(parsed_url.repository_type, RepositoryType::Dataset);
        assert_eq!(parsed_url.file_path, Some("train.json".to_string()));
    }

    #[test]
    fn test_parse_url_space() {
        let parsed_url = ParsedURL::try_from("hf://spaces/huggingface/transformers-demo").unwrap();
        assert_eq!(parsed_url.repository_id, "huggingface/transformers-demo");
        assert_eq!(parsed_url.repository_type, RepositoryType::Space);
        assert!(parsed_url.file_path.is_none());
    }

    #[test]
    fn test_parse_url_explicit_model_type() {
        let parsed_url =
            ParsedURL::try_from("hf://models/deepseek-ai/DeepSeek-OCR/model.safetensors").unwrap();
        assert_eq!(parsed_url.repository_id, "deepseek-ai/DeepSeek-OCR");
        assert_eq!(parsed_url.repository_type, RepositoryType::Model);
        assert_eq!(parsed_url.file_path, Some("model.safetensors".to_string()));
    }

    #[test]
    fn test_parse_url_missing_repo() {
        let result = ParsedURL::try_from("hf://deepseek-ai");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_download_url_model() {
        let parsed_url =
            ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR/model.safetensors").unwrap();
        let url = HuggingFace::build_download_url(&parsed_url, "model.safetensors", "main");
        assert_eq!(
            url,
            "https://huggingface.co/deepseek-ai/DeepSeek-OCR/resolve/main/model.safetensors"
        );
    }

    #[test]
    fn test_build_download_url_dataset() {
        let parsed_url = ParsedURL::try_from("hf://datasets/huggingface/squad/train.json").unwrap();
        let url = HuggingFace::build_download_url(&parsed_url, "train.json", "main");
        assert_eq!(
            url,
            "https://huggingface.co/datasets/huggingface/squad/resolve/main/train.json"
        );
    }

    #[test]
    fn test_build_api_url_model() {
        let parsed_url = ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR").unwrap();
        let url = HuggingFace::build_repository_url(&parsed_url);
        assert_eq!(
            url,
            "https://huggingface.co/api/models/deepseek-ai/DeepSeek-OCR"
        );
    }

    #[test]
    fn test_build_api_url_dataset() {
        let parsed_url = ParsedURL::try_from("hf://datasets/huggingface/squad").unwrap();
        let url = HuggingFace::build_repository_url(&parsed_url);
        assert_eq!(url, "https://huggingface.co/api/datasets/huggingface/squad");
    }

    #[test]
    fn test_build_hf_url_model() {
        let parsed_url = ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR").unwrap();
        let url = HuggingFace::build_hf_url(&parsed_url, "model.safetensors");
        assert_eq!(url, "hf://deepseek-ai/DeepSeek-OCR/model.safetensors");
    }

    #[test]
    fn test_build_hf_url_dataset() {
        let parsed_url = ParsedURL::try_from("hf://datasets/huggingface/squad").unwrap();
        let url = HuggingFace::build_hf_url(&parsed_url, "train.json");
        assert_eq!(url, "hf://datasets/huggingface/squad/train.json");
    }

    #[test]
    fn test_build_headers_default_user_agent() {
        let request_header = HuggingFace::build_request_headers(None, None).unwrap();
        assert_eq!(
            request_header.get(USER_AGENT).unwrap(),
            HeaderValue::from_static(DEFAULT_USER_AGENT)
        );
    }

    #[test]
    fn test_build_headers_preserves_request_headers() {
        let request_headers =
            HuggingFace::build_request_headers(Some("test-token".to_string()), None).unwrap();
        assert_eq!(
            request_headers.get(reqwest::header::AUTHORIZATION).unwrap(),
            "Bearer test-token"
        );
        assert_eq!(
            request_headers.get(USER_AGENT).unwrap(),
            HeaderValue::from_static(DEFAULT_USER_AGENT)
        );
    }
}
