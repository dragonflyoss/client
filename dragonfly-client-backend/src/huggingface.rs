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
    StatRequest, StatResponse, KEEP_ALIVE_INTERVAL, POOL_MAX_IDLE_PER_HOST,
};
use dragonfly_client_core::{
    error::{BackendError, ErrorType, OrErr},
    Error, Result,
};
use futures::StreamExt;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use tokio_util::io::StreamReader;
use tracing::{debug, error, info};
use url::Url;

/// HUGGING_FACE_SCHEME is the URL scheme for Hugging Face backend.
pub const HUGGING_FACE_SCHEME: &str = "hf";

/// HUGGING_FACE_BASE_URL is the base URL for Hugging Face Hub.
const HUGGING_FACE_BASE_URL: &str = "https://huggingface.co";

/// HUGGING_FACE_API_BASE_URL is the API base URL for Hugging Face API.
const HUGGING_FACE_API_BASE_URL: &str = "https://huggingface.co/api";

/// DEFAULT_HUGGING_FACE_REVISION is the default revision (branch) to use for Hugging Face
/// repositories if not specified in the URL.
const DEFAULT_HUGGING_FACE_REVISION: &str = "main";

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

/// ParsedURL represents a parsed Hugging Face URL.
///
/// The Hugging Face URL should be in the format of `hf://[<repository_type>/]<owner>/<repository>[/<path>][@<revision>]`.
#[derive(Debug, Clone)]
pub struct ParsedURL {
    /// The original URL.
    pub url: Url,

    /// The repository ID (e.g., "deepseek-ai/DeepSeek-OCR")
    pub repository_id: String,

    /// The repository type (models, datasets, spaces)
    pub repository_type: RepositoryType,

    /// The file path within the repository (optional)
    pub path: Option<String>,

    /// The revision (branch, tag, or commit hash)
    pub revision: String,
}

/// RepositoryType represents the type of Hugging Face repository.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RepositoryType {
    /// Model repository (default with no prefix, or explicitly prefixed with "models/").
    Model,

    // Dataset repository (prefixed with "datasets/").
    Dataset,

    // Space repository (prefixed with "spaces/").
    Space,
}

/// RepositoryType implements methods for getting string representations and API paths.
impl RepositoryType {
    /// Returns the string representation of the repository type for API paths and URL construction.
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            RepositoryType::Model => "models",
            RepositoryType::Dataset => "datasets",
            RepositoryType::Space => "spaces",
        }
    }

    /// Returns the API path segment for the repository type.
    pub fn api_path(&self) -> &'static str {
        match self {
            RepositoryType::Model => "models",
            RepositoryType::Dataset => "datasets",
            RepositoryType::Space => "spaces",
        }
    }
}

/// ParsedURL implements the TryFrom trait for the URL.
impl TryFrom<Url> for ParsedURL {
    type Error = Error;

    /// try_from parses the URL and returns a ParsedURL.
    fn try_from(url: Url) -> std::result::Result<Self, Self::Error> {
        let host = url
            .host_str()
            .ok_or_else(|| Error::InvalidURI(url.to_string()))?
            .to_string();
        let path = url.path();

        // Combine host and path to get the full path
        let full_path = if path.is_empty() || path == "/" {
            host.to_string()
        } else {
            format!("{}{}", host, path)
        };

        // Check for revision in the URL (after @)
        let (path_part, revision) = if let Some(at_pos) = full_path.rfind('@') {
            let (p, r) = full_path.split_at(at_pos);
            (p.to_string(), r[1..].to_string())
        } else {
            (full_path, DEFAULT_HUGGING_FACE_REVISION.to_string())
        };

        // Parse the path to extract repository_type, repository_id, and file path
        let parts: Vec<&str> = path_part.trim_matches('/').split('/').collect();

        if parts.is_empty() {
            return Err(Error::InvalidParameter);
        }

        // Check if first part is a repository type
        let (repository_type, repository_id_start) = match parts[0] {
            "datasets" => (RepositoryType::Dataset, 1),
            "spaces" => (RepositoryType::Space, 1),
            "models" => (RepositoryType::Model, 1),
            _ => (RepositoryType::Model, 0), // Default to model
        };

        // Need at least owner/repository (two segments after the optional repository type prefix)
        if parts.len() < repository_id_start + 2 {
            return Err(Error::InvalidParameter);
        }

        let repository_id = format!(
            "{}/{}",
            parts[repository_id_start],
            parts[repository_id_start + 1]
        );
        let file_path = if parts.len() > repository_id_start + 2 {
            Some(parts[repository_id_start + 2..].join("/"))
        } else {
            None
        };

        Ok(ParsedURL {
            url,
            repository_id,
            repository_type,
            path: file_path,
            revision,
        })
    }
}

/// ParsedURL implements TryFrom for &str.
impl TryFrom<&str> for ParsedURL {
    type Error = Error;

    fn try_from(url: &str) -> std::result::Result<Self, Self::Error> {
        let parsed_url = Url::parse(url).or_err(ErrorType::ParseError)?;
        ParsedURL::try_from(parsed_url)
    }
}

/// HuggingFace is the Hugging Face backend implementation.
pub struct HuggingFace {
    /// HTTP client for making requests.
    client: Client,
}

impl HuggingFace {
    /// new creates a new HuggingFace backend.
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
            .tcp_keepalive(KEEP_ALIVE_INTERVAL)
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(3600))
            .build()
            .or_err(ErrorType::ConnectError)?;

        Ok(Self { client })
    }

    /// build_download_url builds the download URL for a file.
    fn build_download_url(parsed: &ParsedURL, filename: &str) -> String {
        match parsed.repository_type {
            RepositoryType::Model => {
                format!(
                    "{}/{}/resolve/{}/{}",
                    HUGGING_FACE_BASE_URL, parsed.repository_id, parsed.revision, filename
                )
            }
            RepositoryType::Dataset => {
                format!(
                    "{}/datasets/{}/resolve/{}/{}",
                    HUGGING_FACE_BASE_URL, parsed.repository_id, parsed.revision, filename
                )
            }
            RepositoryType::Space => {
                format!(
                    "{}/spaces/{}/resolve/{}/{}",
                    HUGGING_FACE_BASE_URL, parsed.repository_id, parsed.revision, filename
                )
            }
        }
    }

    /// build_api_url builds the API URL for repository information.
    fn build_api_url(parsed: &ParsedURL) -> String {
        format!(
            "{}/{}/{}",
            HUGGING_FACE_API_BASE_URL,
            parsed.repository_type.api_path(),
            parsed.repository_id
        )
    }

    /// build_hf_url builds an hf:// URL for a file so downstream downloads continue to
    /// use the HF backend (preserving auth and URL semantics).
    fn build_hf_url(parsed: &ParsedURL, filename: &str) -> String {
        let type_prefix = match parsed.repository_type {
            RepositoryType::Model => "",
            RepositoryType::Dataset => "datasets/",
            RepositoryType::Space => "spaces/",
        };
        format!(
            "{}://{}{}/{}@{}",
            HUGGING_FACE_SCHEME, type_prefix, parsed.repository_id, filename, parsed.revision
        )
    }

    /// build_headers builds request headers by merging base headers with request-provided headers.
    /// Authentication headers (e.g., Authorization: Bearer <token>) are expected to be
    /// provided via the request's http_header, which is populated from the --hf-token CLI flag.
    fn build_headers(request_header: &Option<HeaderMap>) -> HeaderMap {
        let mut headers = HeaderMap::new();

        // Set the default user agent, matching the HTTP backend's versioned pattern.
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static(super::DEFAULT_USER_AGENT),
        );

        // Merge request-provided headers (including Authorization from --hf-token).
        // This may override the default User-Agent if the caller provides one.
        if let Some(ref req_headers) = request_header {
            for (key, value) in req_headers.iter() {
                headers.insert(key, value.clone());
            }
        }

        headers
    }

    /// get_repo_info fetches repository information from the Hugging Face API.
    async fn get_repository(
        &self,
        parsed: &ParsedURL,
        request_header: &Option<HeaderMap>,
    ) -> Result<Repository> {
        let api_url = Self::build_api_url(parsed);
        debug!("fetching repository info from: {}", api_url);

        let response = self
            .client
            .get(&api_url)
            .headers(Self::build_headers(request_header))
            .send()
            .await
            .or_err(ErrorType::ConnectError)?;

        if !response.status().is_success() {
            error!(
                "failed to fetch repository info: {} - {}",
                response.status(),
                api_url
            );
            return Err(Error::BackendError(Box::new(BackendError {
                status_code: Some(response.status()),
                header: None,
                message: format!("Failed to fetch repository info: {}", response.status()),
            })));
        }

        let text = response.text().await.or_err(ErrorType::ConnectError)?;
        let repository: Repository = serde_json::from_str(&text).or_err(ErrorType::ParseError)?;
        Ok(repository)
    }

    /// list_files lists all files in the repository.
    async fn list_files(
        &self,
        parsed: &ParsedURL,
        request_header: &Option<HeaderMap>,
    ) -> Result<Vec<DirEntry>> {
        let api_url = format!(
            "{}/{}/{}?revision={}",
            HUGGING_FACE_API_BASE_URL,
            parsed.repository_type.api_path(),
            parsed.repository_id,
            parsed.revision
        );

        debug!("listing files from: {}", api_url);

        let response = self
            .client
            .get(&api_url)
            .headers(Self::build_headers(request_header))
            .send()
            .await
            .or_err(ErrorType::ConnectError)?;

        if !response.status().is_success() {
            return Err(Error::BackendError(Box::new(BackendError {
                status_code: Some(response.status()),
                header: None,
                message: format!("Failed to list files: {}", response.status()),
            })));
        }

        let text = response.text().await.or_err(ErrorType::ConnectError)?;
        let repository: Repository = serde_json::from_str(&text).or_err(ErrorType::ParseError)?;

        let entries = repository
            .siblings
            .unwrap_or_default()
            .into_iter()
            .filter_map(|sibling| {
                // Filter by path prefix if specified
                if let Some(ref prefix) = parsed.path {
                    if !sibling.rfilename.starts_with(prefix) {
                        return None;
                    }
                }

                // Return hf:// URLs so downstream downloads continue to use the HF
                // backend (preserving auth headers and URL semantics).
                let hf_url = Self::build_hf_url(parsed, &sibling.rfilename);
                let size = sibling
                    .lfs
                    .as_ref()
                    .map(|lfs| lfs.size)
                    .or(sibling.size)
                    .unwrap_or(0);

                Some(DirEntry {
                    url: hf_url,
                    content_length: size as usize,
                    is_dir: false,
                })
            })
            .collect();

        Ok(entries)
    }
}

impl Default for HuggingFace {
    fn default() -> Self {
        Self::new().expect("failed to create HuggingFace backend")
    }
}

#[tonic::async_trait]
impl Backend for HuggingFace {
    /// scheme returns the scheme of the backend.
    fn scheme(&self) -> String {
        HUGGING_FACE_SCHEME.to_string()
    }

    /// stat gets the metadata from the backend.
    async fn stat(&self, request: StatRequest) -> Result<StatResponse> {
        let parsed = ParsedURL::try_from(request.url.as_str())?;
        info!(
            "stat huggingface repo: {} path: {:?}",
            parsed.repository_id, parsed.path
        );

        // If a specific file is requested, get its info
        if let Some(ref path) = parsed.path {
            let download_url = Self::build_download_url(&parsed, path);
            debug!("checking file: {}", download_url);

            let response = self
                .client
                .head(&download_url)
                .headers(Self::build_headers(&request.http_header))
                .timeout(request.timeout)
                .send()
                .await
                .or_err(ErrorType::ConnectError)?;

            let content_length = response
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());

            return Ok(StatResponse {
                success: response.status().is_success(),
                content_length,
                http_header: Some(response.headers().clone()),
                http_status_code: Some(response.status()),
                entries: vec![],
                error_message: if !response.status().is_success() {
                    Some(format!("HTTP error: {}", response.status()))
                } else {
                    None
                },
            });
        }

        // List all files in the repository
        let entries = self.list_files(&parsed, &request.http_header).await?;

        Ok(StatResponse {
            success: true,
            content_length: None,
            http_header: None,
            http_status_code: Some(reqwest::StatusCode::OK),
            entries,
            error_message: None,
        })
    }

    /// get gets the content from the backend.
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>> {
        let parsed = ParsedURL::try_from(request.url.as_str())?;

        let filename = parsed.path.as_ref().ok_or_else(|| {
            error!("file path is required for download");
            Error::InvalidParameter
        })?;

        let download_url = Self::build_download_url(&parsed, filename);
        info!("downloading from huggingface: {}", download_url);

        let mut req = self
            .client
            .get(&download_url)
            .headers(Self::build_headers(&request.http_header))
            .timeout(request.timeout);

        // Add range header if specified
        if let Some(ref range) = request.range {
            let range_value = format!("bytes={}-{}", range.start, range.start + range.length - 1);
            req = req.header(reqwest::header::RANGE, range_value);
        }

        let response = req.send().await.or_err(ErrorType::ConnectError)?;

        let status = response.status();
        let headers = response.headers().clone();

        if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
            let error_text = response.text().await.unwrap_or_default();
            return Ok(GetResponse {
                success: false,
                http_header: Some(headers),
                http_status_code: Some(status),
                reader: Box::new(std::io::Cursor::new(Vec::new())),
                error_message: Some(error_text),
            });
        }

        let stream = response.bytes_stream();
        let reader =
            StreamReader::new(stream.map(|result| {
                result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            }));

        Ok(GetResponse {
            success: true,
            http_header: Some(headers),
            http_status_code: Some(status),
            reader: Box::new(reader),
            error_message: None,
        })
    }

    /// put puts the content to the backend.
    async fn put(&self, _request: PutRequest) -> Result<PutResponse> {
        // Hugging Face upload is not supported through this backend.
        // Use the Hugging Face CLI or API directly for uploads.
        Err(Error::Unsupported(
            "upload to Hugging Face is not supported".to_string(),
        ))
    }

    /// exists checks whether the file exists in the backend.
    async fn exists(&self, request: ExistsRequest) -> Result<bool> {
        let parsed = ParsedURL::try_from(request.url.as_str())?;
        let filename = match parsed.path {
            Some(ref path) => path,
            None => {
                // Check if repository exists.
                let repository = self.get_repository(&parsed, &request.http_header).await;
                return Ok(repository.is_ok());
            }
        };

        let download_url = Self::build_download_url(&parsed, filename);
        let response = self
            .client
            .head(&download_url)
            .headers(Self::build_headers(&request.http_header))
            .timeout(request.timeout)
            .send()
            .await
            .or_err(ErrorType::ConnectError)?;

        Ok(response.status().is_success())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_USER_AGENT;

    #[test]
    fn test_parse_url_simple() {
        let parsed = ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR").unwrap();
        assert_eq!(parsed.repository_id, "deepseek-ai/DeepSeek-OCR");
        assert_eq!(parsed.repository_type, RepositoryType::Model);
        assert!(parsed.path.is_none());
        assert_eq!(parsed.revision, "main");
    }

    #[test]
    fn test_parse_url_with_file() {
        let parsed =
            ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR/model.safetensors").unwrap();
        assert_eq!(parsed.repository_id, "deepseek-ai/DeepSeek-OCR");
        assert_eq!(parsed.repository_type, RepositoryType::Model);
        assert_eq!(parsed.path, Some("model.safetensors".to_string()));
        assert_eq!(parsed.revision, "main");
    }

    #[test]
    fn test_parse_url_with_revision() {
        let parsed = ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR@v1.0").unwrap();
        assert_eq!(parsed.repository_id, "deepseek-ai/DeepSeek-OCR");
        assert_eq!(parsed.revision, "v1.0");
        assert!(parsed.path.is_none());
    }

    #[test]
    fn test_parse_url_with_nested_path() {
        let parsed =
            ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR/models/v1/model.bin").unwrap();
        assert_eq!(parsed.repository_id, "deepseek-ai/DeepSeek-OCR");
        assert_eq!(parsed.repository_type, RepositoryType::Model);
        assert_eq!(parsed.path, Some("models/v1/model.bin".to_string()));
    }

    #[test]
    fn test_parse_url_dataset() {
        let parsed = ParsedURL::try_from("hf://datasets/huggingface/squad").unwrap();
        assert_eq!(parsed.repository_id, "huggingface/squad");
        assert_eq!(parsed.repository_type, RepositoryType::Dataset);
        assert!(parsed.path.is_none());
    }

    #[test]
    fn test_parse_url_dataset_with_path() {
        let parsed = ParsedURL::try_from("hf://datasets/huggingface/squad/train.json").unwrap();
        assert_eq!(parsed.repository_id, "huggingface/squad");
        assert_eq!(parsed.repository_type, RepositoryType::Dataset);
        assert_eq!(parsed.path, Some("train.json".to_string()));
    }

    #[test]
    fn test_parse_url_space() {
        let parsed = ParsedURL::try_from("hf://spaces/huggingface/transformers-demo").unwrap();
        assert_eq!(parsed.repository_id, "huggingface/transformers-demo");
        assert_eq!(parsed.repository_type, RepositoryType::Space);
        assert!(parsed.path.is_none());
    }

    #[test]
    fn test_parse_url_explicit_model_type() {
        let parsed =
            ParsedURL::try_from("hf://models/deepseek-ai/DeepSeek-OCR/model.safetensors").unwrap();
        assert_eq!(parsed.repository_id, "deepseek-ai/DeepSeek-OCR");
        assert_eq!(parsed.repository_type, RepositoryType::Model);
        assert_eq!(parsed.path, Some("model.safetensors".to_string()));
    }

    #[test]
    fn test_parse_url_invalid_scheme() {
        let result = ParsedURL::try_from("http://deepseek-ai/DeepSeek-OCR");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_url_missing_repo() {
        let result = ParsedURL::try_from("hf://deepseek-ai");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_download_url_model() {
        let parsed =
            ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR/model.safetensors").unwrap();
        let url = HuggingFace::build_download_url(&parsed, "model.safetensors");
        assert_eq!(
            url,
            "https://huggingface.co/deepseek-ai/DeepSeek-OCR/resolve/main/model.safetensors"
        );
    }

    #[test]
    fn test_build_download_url_dataset() {
        let parsed = ParsedURL::try_from("hf://datasets/huggingface/squad/train.json").unwrap();
        let url = HuggingFace::build_download_url(&parsed, "train.json");
        assert_eq!(
            url,
            "https://huggingface.co/datasets/huggingface/squad/resolve/main/train.json"
        );
    }

    #[test]
    fn test_build_api_url_model() {
        let parsed = ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR").unwrap();
        let url = HuggingFace::build_api_url(&parsed);
        assert_eq!(
            url,
            "https://huggingface.co/api/models/deepseek-ai/DeepSeek-OCR"
        );
    }

    #[test]
    fn test_build_api_url_dataset() {
        let parsed = ParsedURL::try_from("hf://datasets/huggingface/squad").unwrap();
        let url = HuggingFace::build_api_url(&parsed);
        assert_eq!(url, "https://huggingface.co/api/datasets/huggingface/squad");
    }

    #[test]
    fn test_build_hf_url_model() {
        let parsed = ParsedURL::try_from("hf://deepseek-ai/DeepSeek-OCR").unwrap();
        let url = HuggingFace::build_hf_url(&parsed, "model.safetensors");
        assert_eq!(url, "hf://deepseek-ai/DeepSeek-OCR/model.safetensors@main");
    }

    #[test]
    fn test_build_hf_url_dataset() {
        let parsed = ParsedURL::try_from("hf://datasets/huggingface/squad").unwrap();
        let url = HuggingFace::build_hf_url(&parsed, "train.json");
        assert_eq!(url, "hf://datasets/huggingface/squad/train.json@main");
    }

    #[test]
    fn test_build_headers_default_user_agent() {
        let headers = HuggingFace::build_headers(&None);
        assert_eq!(
            headers.get(USER_AGENT).unwrap(),
            HeaderValue::from_static(DEFAULT_USER_AGENT)
        );
    }

    #[test]
    fn test_build_headers_preserves_request_headers() {
        let mut req_headers = HeaderMap::new();
        req_headers.insert(
            reqwest::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer test-token"),
        );
        let headers = HuggingFace::build_headers(&Some(req_headers));
        assert_eq!(
            headers.get(reqwest::header::AUTHORIZATION).unwrap(),
            "Bearer test-token"
        );
        assert_eq!(
            headers.get(USER_AGENT).unwrap(),
            HeaderValue::from_static(DEFAULT_USER_AGENT)
        );
    }

    #[test]
    fn test_build_headers_user_supplied_ua_overrides() {
        let mut req_headers = HeaderMap::new();
        req_headers.insert(USER_AGENT, HeaderValue::from_static("custom-agent/2.0"));
        let headers = HuggingFace::build_headers(&Some(req_headers));
        assert_eq!(
            headers.get(USER_AGENT).unwrap(),
            HeaderValue::from_static("custom-agent/2.0")
        );
    }
}
