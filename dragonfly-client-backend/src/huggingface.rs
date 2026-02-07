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
//! For private repositories or to increase rate limits, set the `HF_TOKEN` environment
//! variable or use the `--hf-token` flag.

use crate::{
    Backend, Body, DirEntry, ExistsRequest, GetRequest, GetResponse, PutRequest, PutResponse,
    StatRequest, StatResponse, KEEP_ALIVE_INTERVAL, POOL_MAX_IDLE_PER_HOST,
};
use dragonfly_client_core::{
    error::{BackendError, ErrorType, OrErr},
    Error, Result,
};
use futures::StreamExt;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, USER_AGENT};
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use tokio_util::io::StreamReader;
use tracing::{debug, error, info};
use url::Url;

/// HUGGINGFACE_SCHEME is the scheme of the Hugging Face backend.
pub const HUGGINGFACE_SCHEME: &str = "hf";

/// HUGGINGFACE_API_BASE is the base URL for Hugging Face API.
const HUGGINGFACE_API_BASE: &str = "https://huggingface.co/api";

/// HUGGINGFACE_DOWNLOAD_BASE is the base URL for downloading files from Hugging Face.
const HUGGINGFACE_DOWNLOAD_BASE: &str = "https://huggingface.co";

/// DEFAULT_REVISION is the default revision (branch) to use.
const DEFAULT_REVISION: &str = "main";

/// HF_TOKEN_ENV is the environment variable for Hugging Face token.
const HF_TOKEN_ENV: &str = "HF_TOKEN";

/// RepoInfo represents the repository information from Hugging Face API.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct RepoInfo {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "modelId", default)]
    model_id: Option<String>,
    #[serde(rename = "private")]
    is_private: bool,
    siblings: Option<Vec<RepoFile>>,
}

/// RepoFile represents a file in the repository.
#[derive(Debug, Deserialize)]
struct RepoFile {
    #[serde(rename = "rfilename")]
    filename: String,
    size: Option<u64>,
    #[serde(rename = "lfs")]
    lfs_info: Option<LfsInfo>,
}

/// LfsInfo represents Git LFS information for a file.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct LfsInfo {
    size: u64,
    #[serde(rename = "sha256")]
    sha256: Option<String>,
    #[serde(rename = "pointerSize")]
    pointer_size: Option<u64>,
}

/// ParsedHfUrl represents a parsed Hugging Face URL.
#[derive(Debug, Clone)]
struct ParsedHfUrl {
    /// The repository ID (e.g., "deepseek-ai/DeepSeek-OCR")
    repo_id: String,
    /// The repository type (models, datasets, spaces)
    repo_type: RepoType,
    /// The file path within the repository (optional)
    path: Option<String>,
    /// The revision (branch, tag, or commit hash)
    revision: String,
}

/// RepoType represents the type of Hugging Face repository.
#[derive(Debug, Clone, Copy, PartialEq)]
enum RepoType {
    Model,
    Dataset,
    Space,
}

impl RepoType {
    #[allow(dead_code)]
    fn as_str(&self) -> &'static str {
        match self {
            RepoType::Model => "models",
            RepoType::Dataset => "datasets",
            RepoType::Space => "spaces",
        }
    }

    fn api_path(&self) -> &'static str {
        match self {
            RepoType::Model => "models",
            RepoType::Dataset => "datasets",
            RepoType::Space => "spaces",
        }
    }
}

/// HuggingFace is the Hugging Face backend implementation.
pub struct HuggingFace {
    /// HTTP client for making requests.
    client: Client,
    /// Optional authentication token.
    token: Option<String>,
}

impl HuggingFace {
    /// new creates a new HuggingFace backend.
    pub fn new() -> Result<Self> {
        let token = std::env::var(HF_TOKEN_ENV).ok();

        let client = Client::builder()
            .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
            .tcp_keepalive(KEEP_ALIVE_INTERVAL)
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(3600))
            .build()
            .or_err(ErrorType::ConnectError)?;

        Ok(Self { client, token })
    }

    /// new_with_token creates a new HuggingFace backend with a specific token.
    pub fn new_with_token(token: Option<String>) -> Result<Self> {
        let token = token.or_else(|| std::env::var(HF_TOKEN_ENV).ok());

        let client = Client::builder()
            .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
            .tcp_keepalive(KEEP_ALIVE_INTERVAL)
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(3600))
            .build()
            .or_err(ErrorType::ConnectError)?;

        Ok(Self { client, token })
    }

    /// parse_url parses a Hugging Face URL into its components.
    ///
    /// URL format: hf://[<repo_type>/]<repo_id>[/<path>][@<revision>]
    ///
    /// Examples:
    /// - hf://deepseek-ai/DeepSeek-OCR
    /// - hf://deepseek-ai/DeepSeek-OCR/model.safetensors
    /// - hf://deepseek-ai/DeepSeek-OCR@main
    /// - hf://datasets/squad/train.json
    fn parse_url(url: &str) -> Result<ParsedHfUrl> {
        let url = Url::parse(url).or_err(ErrorType::ParseError)?;

        if url.scheme() != HUGGINGFACE_SCHEME {
            return Err(Error::InvalidParameter);
        }

        let host = url.host_str().ok_or(Error::InvalidParameter)?;
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
            (full_path, DEFAULT_REVISION.to_string())
        };

        // Parse the path to extract repo_type, repo_id, and file path
        let parts: Vec<&str> = path_part.trim_matches('/').split('/').collect();

        if parts.is_empty() {
            return Err(Error::InvalidParameter);
        }

        // Check if first part is a repo type
        let (repo_type, repo_id_start) = match parts[0] {
            "datasets" => (RepoType::Dataset, 1),
            "spaces" => (RepoType::Space, 1),
            "models" => (RepoType::Model, 1),
            _ => (RepoType::Model, 0), // Default to model
        };

        // Need at least owner/repo
        if parts.len() < repo_id_start + 2 {
            // Could be just owner/repo without explicit type
            if parts.len() >= 2 {
                let repo_id = format!("{}/{}", parts[0], parts[1]);
                let file_path = if parts.len() > 2 {
                    Some(parts[2..].join("/"))
                } else {
                    None
                };
                return Ok(ParsedHfUrl {
                    repo_id,
                    repo_type: RepoType::Model,
                    path: file_path,
                    revision,
                });
            }
            return Err(Error::InvalidParameter);
        }

        let repo_id = format!("{}/{}", parts[repo_id_start], parts[repo_id_start + 1]);
        let file_path = if parts.len() > repo_id_start + 2 {
            Some(parts[repo_id_start + 2..].join("/"))
        } else {
            None
        };

        Ok(ParsedHfUrl {
            repo_id,
            repo_type,
            path: file_path,
            revision,
        })
    }

    /// build_download_url builds the download URL for a file.
    fn build_download_url(parsed: &ParsedHfUrl, filename: &str) -> String {
        match parsed.repo_type {
            RepoType::Model => {
                format!(
                    "{}/{}/resolve/{}/{}",
                    HUGGINGFACE_DOWNLOAD_BASE, parsed.repo_id, parsed.revision, filename
                )
            }
            RepoType::Dataset => {
                format!(
                    "{}/datasets/{}/resolve/{}/{}",
                    HUGGINGFACE_DOWNLOAD_BASE, parsed.repo_id, parsed.revision, filename
                )
            }
            RepoType::Space => {
                format!(
                    "{}/spaces/{}/resolve/{}/{}",
                    HUGGINGFACE_DOWNLOAD_BASE, parsed.repo_id, parsed.revision, filename
                )
            }
        }
    }

    /// build_api_url builds the API URL for repository information.
    fn build_api_url(parsed: &ParsedHfUrl) -> String {
        format!(
            "{}/{}/{}",
            HUGGINGFACE_API_BASE,
            parsed.repo_type.api_path(),
            parsed.repo_id
        )
    }

    /// get_auth_headers returns the authentication headers if a token is available.
    fn get_auth_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("dragonfly-client/1.0"));

        if let Some(ref token) = self.token {
            if let Ok(value) = HeaderValue::from_str(&format!("Bearer {}", token)) {
                headers.insert(AUTHORIZATION, value);
            }
        }

        headers
    }

    /// get_repo_info fetches repository information from the Hugging Face API.
    async fn get_repo_info(&self, parsed: &ParsedHfUrl) -> Result<RepoInfo> {
        let api_url = Self::build_api_url(parsed);
        debug!("fetching repo info from: {}", api_url);

        let response = self
            .client
            .get(&api_url)
            .headers(self.get_auth_headers())
            .send()
            .await
            .or_err(ErrorType::ConnectError)?;

        if !response.status().is_success() {
            error!(
                "failed to fetch repo info: {} - {}",
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
        let repo_info: RepoInfo = serde_json::from_str(&text).or_err(ErrorType::ParseError)?;
        Ok(repo_info)
    }

    /// list_files lists all files in the repository.
    async fn list_files(&self, parsed: &ParsedHfUrl) -> Result<Vec<DirEntry>> {
        let api_url = format!(
            "{}/{}/{}?revision={}",
            HUGGINGFACE_API_BASE,
            parsed.repo_type.api_path(),
            parsed.repo_id,
            parsed.revision
        );

        debug!("listing files from: {}", api_url);

        let response = self
            .client
            .get(&api_url)
            .headers(self.get_auth_headers())
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
        let repo_info: RepoInfo = serde_json::from_str(&text).or_err(ErrorType::ParseError)?;

        let entries = repo_info
            .siblings
            .unwrap_or_default()
            .into_iter()
            .filter_map(|file| {
                // Filter by path prefix if specified
                if let Some(ref prefix) = parsed.path {
                    if !file.filename.starts_with(prefix) {
                        return None;
                    }
                }

                let download_url = Self::build_download_url(parsed, &file.filename);
                let size = file
                    .lfs_info
                    .as_ref()
                    .map(|lfs| lfs.size)
                    .or(file.size)
                    .unwrap_or(0);

                Some(DirEntry {
                    url: download_url,
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
        HUGGINGFACE_SCHEME.to_string()
    }

    /// stat gets the metadata from the backend.
    async fn stat(&self, request: StatRequest) -> Result<StatResponse> {
        let parsed = Self::parse_url(&request.url)?;
        info!(
            "stat huggingface repo: {} path: {:?}",
            parsed.repo_id, parsed.path
        );

        // If a specific file is requested, get its info
        if let Some(ref path) = parsed.path {
            let download_url = Self::build_download_url(&parsed, path);
            debug!("checking file: {}", download_url);

            let response = self
                .client
                .head(&download_url)
                .headers(self.get_auth_headers())
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
        let entries = self.list_files(&parsed).await?;

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
        let parsed = Self::parse_url(&request.url)?;

        let filename = parsed.path.as_ref().ok_or_else(|| {
            error!("file path is required for download");
            Error::InvalidParameter
        })?;

        let download_url = Self::build_download_url(&parsed, filename);
        info!("downloading from huggingface: {}", download_url);

        let mut req = self
            .client
            .get(&download_url)
            .headers(self.get_auth_headers())
            .timeout(request.timeout);

        // Add range header if specified
        if let Some(ref range) = request.range {
            let range_value = format!("bytes={}-{}", range.start, range.start + range.length - 1);
            req = req.header(reqwest::header::RANGE, range_value);
        }

        // Add custom headers
        if let Some(ref headers) = request.http_header {
            for (key, value) in headers.iter() {
                req = req.header(key, value);
            }
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
        let parsed = Self::parse_url(&request.url)?;

        let filename = match parsed.path {
            Some(ref path) => path,
            None => {
                // Check if repository exists
                let repo_info = self.get_repo_info(&parsed).await;
                return Ok(repo_info.is_ok());
            }
        };

        let download_url = Self::build_download_url(&parsed, filename);

        let response = self
            .client
            .head(&download_url)
            .headers(self.get_auth_headers())
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

    #[test]
    fn test_parse_url_simple() {
        let parsed = HuggingFace::parse_url("hf://deepseek-ai/DeepSeek-OCR").unwrap();
        assert_eq!(parsed.repo_id, "deepseek-ai/DeepSeek-OCR");
        assert_eq!(parsed.repo_type, RepoType::Model);
        assert!(parsed.path.is_none());
        assert_eq!(parsed.revision, "main");
    }

    #[test]
    fn test_parse_url_with_file() {
        let parsed =
            HuggingFace::parse_url("hf://deepseek-ai/DeepSeek-OCR/model.safetensors").unwrap();
        assert_eq!(parsed.repo_id, "deepseek-ai/DeepSeek-OCR");
        assert_eq!(parsed.path, Some("model.safetensors".to_string()));
        assert_eq!(parsed.revision, "main");
    }

    #[test]
    fn test_parse_url_with_revision() {
        let parsed = HuggingFace::parse_url("hf://deepseek-ai/DeepSeek-OCR@v1.0").unwrap();
        assert_eq!(parsed.repo_id, "deepseek-ai/DeepSeek-OCR");
        assert_eq!(parsed.revision, "v1.0");
    }

    #[test]
    fn test_parse_url_dataset() {
        let parsed = HuggingFace::parse_url("hf://datasets/squad/train.json").unwrap();
        assert_eq!(parsed.repo_id, "squad/train.json");
        assert_eq!(parsed.repo_type, RepoType::Dataset);
    }

    #[test]
    fn test_parse_url_with_nested_path() {
        let parsed =
            HuggingFace::parse_url("hf://deepseek-ai/DeepSeek-OCR/models/v1/model.bin").unwrap();
        assert_eq!(parsed.repo_id, "deepseek-ai/DeepSeek-OCR");
        assert_eq!(parsed.path, Some("models/v1/model.bin".to_string()));
    }

    #[test]
    fn test_build_download_url() {
        let parsed = ParsedHfUrl {
            repo_id: "deepseek-ai/DeepSeek-OCR".to_string(),
            repo_type: RepoType::Model,
            path: Some("model.safetensors".to_string()),
            revision: "main".to_string(),
        };

        let url = HuggingFace::build_download_url(&parsed, "model.safetensors");
        assert_eq!(
            url,
            "https://huggingface.co/deepseek-ai/DeepSeek-OCR/resolve/main/model.safetensors"
        );
    }

    #[test]
    fn test_build_api_url() {
        let parsed = ParsedHfUrl {
            repo_id: "deepseek-ai/DeepSeek-OCR".to_string(),
            repo_type: RepoType::Model,
            path: None,
            revision: "main".to_string(),
        };

        let url = HuggingFace::build_api_url(&parsed);
        assert_eq!(
            url,
            "https://huggingface.co/api/models/deepseek-ai/DeepSeek-OCR"
        );
    }
}
