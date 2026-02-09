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
//! The URL format is: `modelscope://[<repo_type>/]<owner>/<repo>[/<path>][@<revision>]`
//!
//! Examples:
//! - `modelscope://inclusionAI/Ling-1T` - Download entire repository
//! - `modelscope://inclusionAI/Ling-1T/config.json` - Download specific file
//! - `modelscope://inclusionAI/Ling-1T@v1.0` - Download from specific revision
//! - `modelscope://datasets/owner/dataset-name` - Download from a dataset repository
//!
//! # Authentication
//!
//! For private repositories or to increase rate limits, use the `--ms-token` flag.

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

/// MODELSCOPE_SCHEME is the scheme of the ModelScope backend.
pub const MODELSCOPE_SCHEME: &str = "modelscope";

/// DEFAULT_USER_AGENT is the default user agent for ModelScope requests.
const DEFAULT_USER_AGENT: &str = concat!("dragonfly", "/", env!("CARGO_PKG_VERSION"));

/// MODELSCOPE_API_BASE is the base URL for ModelScope API.
const MODELSCOPE_API_BASE: &str = "https://modelscope.cn/api/v1";

/// DEFAULT_REVISION is the default revision (branch) to use.
const DEFAULT_REVISION: &str = "master";

/// ApiResponse represents the top-level response from the ModelScope API.
#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    #[serde(rename = "Code")]
    code: i32,
    #[serde(rename = "Data")]
    data: T,
    #[serde(rename = "Message", default)]
    message: Option<String>,
}

/// FileListData represents the data field in a file list API response.
#[derive(Debug, Deserialize)]
struct FileListData {
    #[serde(rename = "Files")]
    files: Option<Vec<RepoFile>>,
}

/// RepoFile represents a file entry returned by the ModelScope API.
#[derive(Debug, Deserialize)]
struct RepoFile {
    /// The file name.
    #[serde(rename = "Name")]
    name: String,
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

/// RepoType represents the type of ModelScope repository.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RepoType {
    Model,
    Dataset,
}

impl RepoType {
    /// api_segment returns the API path segment for the repo type.
    pub fn api_segment(&self) -> &'static str {
        match self {
            RepoType::Model => "models",
            RepoType::Dataset => "datasets",
        }
    }
}

/// ParsedModelScopeUrl represents a parsed ModelScope URL.
///
/// The ModelScope URL should be in the format of
/// `modelscope://[<repo_type>/]<owner>/<repo>[/<path>][@<revision>]`.
#[derive(Debug, Clone)]
pub struct ParsedModelScopeUrl {
    /// The original URL.
    pub url: Url,
    /// The repository ID (e.g., "inclusionAI/Ling-1T")
    pub repo_id: String,
    /// The repository type (models, datasets)
    pub repo_type: RepoType,
    /// The file path within the repository (optional)
    pub path: Option<String>,
    /// The revision (branch, tag, or commit hash)
    pub revision: String,
}

/// ParsedModelScopeUrl implements the TryFrom trait for the URL.
impl TryFrom<Url> for ParsedModelScopeUrl {
    type Error = Error;

    /// try_from parses the URL and returns a ParsedModelScopeUrl.
    fn try_from(url: Url) -> std::result::Result<Self, Self::Error> {
        if url.scheme() != MODELSCOPE_SCHEME {
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
            "models" => (RepoType::Model, 1),
            _ => (RepoType::Model, 0), // Default to model
        };

        // Need at least owner/repo (two segments after the optional repo type prefix)
        if parts.len() < repo_id_start + 2 {
            return Err(Error::InvalidParameter);
        }

        let repo_id = format!("{}/{}", parts[repo_id_start], parts[repo_id_start + 1]);
        let file_path = if parts.len() > repo_id_start + 2 {
            Some(parts[repo_id_start + 2..].join("/"))
        } else {
            None
        };

        Ok(ParsedModelScopeUrl {
            url,
            repo_id,
            repo_type,
            path: file_path,
            revision,
        })
    }
}

/// ParsedModelScopeUrl implements TryFrom for &str.
impl TryFrom<&str> for ParsedModelScopeUrl {
    type Error = Error;

    fn try_from(url: &str) -> std::result::Result<Self, Self::Error> {
        let parsed_url = Url::parse(url).or_err(ErrorType::ParseError)?;
        ParsedModelScopeUrl::try_from(parsed_url)
    }
}

/// ModelScope is the ModelScope backend implementation.
pub struct ModelScope {
    /// HTTP client for making requests.
    client: Client,
}

impl ModelScope {
    /// new creates a new ModelScope backend.
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

    /// build_download_url builds the file download URL for ModelScope.
    ///
    /// Format: `{MODELSCOPE_API_BASE}/{repo_type}/{repo_id}/repo?Revision={revision}&FilePath={file_path}`
    fn build_download_url(parsed: &ParsedModelScopeUrl, filename: &str) -> String {
        format!(
            "{}/{}/{}/repo?Revision={}&FilePath={}",
            MODELSCOPE_API_BASE,
            parsed.repo_type.api_segment(),
            parsed.repo_id,
            parsed.revision,
            filename
        )
    }

    /// build_file_list_url builds the URL to list repository files.
    ///
    /// Format: `{MODELSCOPE_API_BASE}/{repo_type}/{repo_id}/repo/files?Revision={revision}&Recursive=true`
    fn build_file_list_url(parsed: &ParsedModelScopeUrl) -> String {
        format!(
            "{}/{}/{}/repo/files?Revision={}&Recursive=true",
            MODELSCOPE_API_BASE,
            parsed.repo_type.api_segment(),
            parsed.repo_id,
            parsed.revision
        )
    }

    /// build_modelscope_url builds a modelscope:// URL for a file so downstream downloads
    /// continue to use the ModelScope backend (preserving auth and URL semantics).
    fn build_modelscope_url(parsed: &ParsedModelScopeUrl, filename: &str) -> String {
        let type_prefix = match parsed.repo_type {
            RepoType::Model => "",
            RepoType::Dataset => "datasets/",
        };
        format!(
            "{}://{}{}/{}@{}",
            MODELSCOPE_SCHEME, type_prefix, parsed.repo_id, filename, parsed.revision
        )
    }

    /// build_headers builds request headers by merging base headers with request-provided headers.
    /// Authentication headers (e.g., Authorization: Bearer <token>) are expected to be
    /// provided via the request's http_header, which is populated from the --ms-token CLI flag.
    fn build_headers(request_header: &Option<HeaderMap>) -> HeaderMap {
        let mut headers = HeaderMap::new();

        // Set the default user agent, matching the HTTP backend's versioned pattern.
        headers.insert(USER_AGENT, HeaderValue::from_static(DEFAULT_USER_AGENT));

        // Merge request-provided headers (including Authorization from --ms-token).
        // This may override the default User-Agent if the caller provides one.
        if let Some(ref req_headers) = request_header {
            for (key, value) in req_headers.iter() {
                headers.insert(key, value.clone());
            }
        }

        headers
    }

    /// list_files lists all files in the repository.
    async fn list_files(
        &self,
        parsed: &ParsedModelScopeUrl,
        request_header: &Option<HeaderMap>,
    ) -> Result<Vec<DirEntry>> {
        let api_url = Self::build_file_list_url(parsed);
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
        let api_response: ApiResponse<FileListData> =
            serde_json::from_str(&text).or_err(ErrorType::ParseError)?;

        if api_response.code != 200 {
            return Err(Error::BackendError(Box::new(BackendError {
                status_code: None,
                header: None,
                message: format!(
                    "ModelScope API error: code={}, message={}",
                    api_response.code,
                    api_response.message.unwrap_or_default()
                ),
            })));
        }

        let entries = api_response
            .data
            .files
            .unwrap_or_default()
            .into_iter()
            .filter_map(|file| {
                // Skip directories
                if file.entry_type == "tree" {
                    return None;
                }

                // Skip .gitignore and .gitattributes
                if file.name == ".gitignore" || file.name == ".gitattributes" {
                    return None;
                }

                // Filter by path prefix if specified
                if let Some(ref prefix) = parsed.path {
                    if !file.path.starts_with(prefix) {
                        return None;
                    }
                }

                // Return modelscope:// URLs so downstream downloads continue to use
                // this backend (preserving auth headers and URL semantics).
                let ms_url = Self::build_modelscope_url(parsed, &file.path);
                let size = file.size.unwrap_or(0);

                Some(DirEntry {
                    url: ms_url,
                    content_length: size as usize,
                    is_dir: false,
                })
            })
            .collect();

        Ok(entries)
    }
}

impl Default for ModelScope {
    fn default() -> Self {
        Self::new().expect("failed to create ModelScope backend")
    }
}

#[tonic::async_trait]
impl Backend for ModelScope {
    /// scheme returns the scheme of the backend.
    fn scheme(&self) -> String {
        MODELSCOPE_SCHEME.to_string()
    }

    /// stat gets the metadata from the backend.
    async fn stat(&self, request: StatRequest) -> Result<StatResponse> {
        let parsed = ParsedModelScopeUrl::try_from(request.url.as_str())?;
        info!(
            "stat modelscope repo: {} path: {:?}",
            parsed.repo_id, parsed.path
        );

        // If a specific file is requested, get its info via HEAD request
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
        let parsed = ParsedModelScopeUrl::try_from(request.url.as_str())?;

        let filename = parsed.path.as_ref().ok_or_else(|| {
            error!("file path is required for download");
            Error::InvalidParameter
        })?;

        let download_url = Self::build_download_url(&parsed, filename);
        info!("downloading from modelscope: {}", download_url);

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
        // ModelScope upload is not supported through this backend.
        // Use the ModelScope CLI or API directly for uploads.
        Err(Error::Unsupported(
            "upload to ModelScope is not supported".to_string(),
        ))
    }

    /// exists checks whether the file exists in the backend.
    async fn exists(&self, request: ExistsRequest) -> Result<bool> {
        let parsed = ParsedModelScopeUrl::try_from(request.url.as_str())?;

        let filename = match parsed.path {
            Some(ref path) => path,
            None => {
                // Check if repository exists by trying to list files
                let list_url = Self::build_file_list_url(&parsed);
                let response = self
                    .client
                    .get(&list_url)
                    .headers(Self::build_headers(&request.http_header))
                    .timeout(request.timeout)
                    .send()
                    .await
                    .or_err(ErrorType::ConnectError)?;
                return Ok(response.status().is_success());
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

    #[test]
    fn test_parse_url_simple() {
        let parsed = ParsedModelScopeUrl::try_from("modelscope://inclusionAI/Ling-1T").unwrap();
        assert_eq!(parsed.repo_id, "inclusionAI/Ling-1T");
        assert_eq!(parsed.repo_type, RepoType::Model);
        assert!(parsed.path.is_none());
        assert_eq!(parsed.revision, "master");
    }

    #[test]
    fn test_parse_url_with_file() {
        let parsed =
            ParsedModelScopeUrl::try_from("modelscope://inclusionAI/Ling-1T/config.json").unwrap();
        assert_eq!(parsed.repo_id, "inclusionAI/Ling-1T");
        assert_eq!(parsed.repo_type, RepoType::Model);
        assert_eq!(parsed.path, Some("config.json".to_string()));
        assert_eq!(parsed.revision, "master");
    }

    #[test]
    fn test_parse_url_with_revision() {
        let parsed =
            ParsedModelScopeUrl::try_from("modelscope://inclusionAI/Ling-1T@v1.0").unwrap();
        assert_eq!(parsed.repo_id, "inclusionAI/Ling-1T");
        assert_eq!(parsed.revision, "v1.0");
        assert!(parsed.path.is_none());
    }

    #[test]
    fn test_parse_url_with_nested_path() {
        let parsed =
            ParsedModelScopeUrl::try_from("modelscope://inclusionAI/Ling-1T/models/v1/model.bin")
                .unwrap();
        assert_eq!(parsed.repo_id, "inclusionAI/Ling-1T");
        assert_eq!(parsed.repo_type, RepoType::Model);
        assert_eq!(parsed.path, Some("models/v1/model.bin".to_string()));
    }

    #[test]
    fn test_parse_url_dataset() {
        let parsed = ParsedModelScopeUrl::try_from("modelscope://datasets/damo/squad-zh").unwrap();
        assert_eq!(parsed.repo_id, "damo/squad-zh");
        assert_eq!(parsed.repo_type, RepoType::Dataset);
        assert!(parsed.path.is_none());
    }

    #[test]
    fn test_parse_url_dataset_with_path() {
        let parsed =
            ParsedModelScopeUrl::try_from("modelscope://datasets/damo/squad-zh/train.json")
                .unwrap();
        assert_eq!(parsed.repo_id, "damo/squad-zh");
        assert_eq!(parsed.repo_type, RepoType::Dataset);
        assert_eq!(parsed.path, Some("train.json".to_string()));
    }

    #[test]
    fn test_parse_url_explicit_model_type() {
        let parsed =
            ParsedModelScopeUrl::try_from("modelscope://models/inclusionAI/Ling-1T/config.json")
                .unwrap();
        assert_eq!(parsed.repo_id, "inclusionAI/Ling-1T");
        assert_eq!(parsed.repo_type, RepoType::Model);
        assert_eq!(parsed.path, Some("config.json".to_string()));
    }

    #[test]
    fn test_parse_url_invalid_scheme() {
        let result = ParsedModelScopeUrl::try_from("http://inclusionAI/Ling-1T");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_url_missing_repo() {
        let result = ParsedModelScopeUrl::try_from("modelscope://inclusionAI");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_download_url_model() {
        let parsed =
            ParsedModelScopeUrl::try_from("modelscope://inclusionAI/Ling-1T/config.json").unwrap();
        let url = ModelScope::build_download_url(&parsed, "config.json");
        assert_eq!(
            url,
            "https://modelscope.cn/api/v1/models/inclusionAI/Ling-1T/repo?Revision=master&FilePath=config.json"
        );
    }

    #[test]
    fn test_build_download_url_dataset() {
        let parsed =
            ParsedModelScopeUrl::try_from("modelscope://datasets/damo/squad-zh/train.json")
                .unwrap();
        let url = ModelScope::build_download_url(&parsed, "train.json");
        assert_eq!(
            url,
            "https://modelscope.cn/api/v1/datasets/damo/squad-zh/repo?Revision=master&FilePath=train.json"
        );
    }

    #[test]
    fn test_build_file_list_url_model() {
        let parsed = ParsedModelScopeUrl::try_from("modelscope://inclusionAI/Ling-1T").unwrap();
        let url = ModelScope::build_file_list_url(&parsed);
        assert_eq!(
            url,
            "https://modelscope.cn/api/v1/models/inclusionAI/Ling-1T/repo/files?Revision=master&Recursive=true"
        );
    }

    #[test]
    fn test_build_file_list_url_dataset() {
        let parsed = ParsedModelScopeUrl::try_from("modelscope://datasets/damo/squad-zh").unwrap();
        let url = ModelScope::build_file_list_url(&parsed);
        assert_eq!(
            url,
            "https://modelscope.cn/api/v1/datasets/damo/squad-zh/repo/files?Revision=master&Recursive=true"
        );
    }

    #[test]
    fn test_build_modelscope_url_model() {
        let parsed = ParsedModelScopeUrl::try_from("modelscope://inclusionAI/Ling-1T").unwrap();
        let url = ModelScope::build_modelscope_url(&parsed, "config.json");
        assert_eq!(url, "modelscope://inclusionAI/Ling-1T/config.json@master");
    }

    #[test]
    fn test_build_modelscope_url_dataset() {
        let parsed = ParsedModelScopeUrl::try_from("modelscope://datasets/damo/squad-zh").unwrap();
        let url = ModelScope::build_modelscope_url(&parsed, "train.json");
        assert_eq!(url, "modelscope://datasets/damo/squad-zh/train.json@master");
    }

    #[test]
    fn test_build_headers_default_user_agent() {
        let headers = ModelScope::build_headers(&None);
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
        let headers = ModelScope::build_headers(&Some(req_headers));
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
        let headers = ModelScope::build_headers(&Some(req_headers));
        assert_eq!(
            headers.get(USER_AGENT).unwrap(),
            HeaderValue::from_static("custom-agent/2.0")
        );
    }
}
