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
//! The URL format is: `hf://<repo_id>[/<path>]`
//!
//! Examples:
//! - `hf://deepseek-ai/DeepSeek-OCR` - Download entire repository
//! - `hf://deepseek-ai/DeepSeek-OCR/model.safetensors` - Download specific file
//!
//! # Authentication
//!
//! For private repositories or to increase rate limits, use the `--hf-token` flag.

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

/// The URL scheme for Hugging Face backend.
pub const SCHEME: &str = "hf";

/// The base URL for Hugging Face Hub.
const HUGGING_FACE_BASE_URL: &str = "https://huggingface.co";

/// Represents the Hugging Face repository information returned by the API.
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

/// Represents a file or directory in the Hugging Face repository.
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Sibling {
    rfilename: String,
    size: Option<u64>,
    lfs: Option<Lfs>,
}

/// Represents Git LFS metadata for large files in the Hugging Face repository.
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
/// Format: `hf://[<repository_type>/]<owner>/<repository>[/<path>]`
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

/// Implements methods for getting string representations and API paths.
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
/// URL Format: hf://[<repository_type>/]<owner>/<repository>[/<path>]
/// - repository_type  Optional. One of "models" (default), "datasets", or "spaces".
/// - owner/repository Required. For example, "meta-llama/Llama-2-7b".
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

/// Implements TryFrom for &str.
impl TryFrom<&str> for ParsedURL {
    type Error = Error;

    /// Try to parse a string URL into a ParsedURL struct.
    fn try_from(url: &str) -> std::result::Result<Self, Self::Error> {
        let parsed_url = Url::parse(url).or_err(ErrorType::ParseError)?;
        ParsedURL::try_from(parsed_url)
    }
}

/// The Hugging Face backend implementation.
pub struct HuggingFace {
    /// The scheme of the Hugging Face backend.
    scheme: String,

    /// HTTP client for making requests.
    client: Client,
}

/// Implements the hugging face interface.
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
            scheme: SCHEME.to_string(),
            client,
        })
    }

    /// Resolves the base URLs from gRPC Hugging Face options.
    fn resolve_base_urls(base_url: Option<&str>) -> Result<(Url, Url)> {
        let base_url = Url::parse(base_url.unwrap_or(HUGGING_FACE_BASE_URL))?;
        let api_base_url = base_url.join("/api/")?;
        Ok((base_url, api_base_url))
    }

    /// Builds the download URL for a file based on the repository type and path.
    fn build_download_url(
        parsed_url: &ParsedURL,
        file_path: &str,
        revision: &str,
        base_url: &Url,
    ) -> Result<Url> {
        let path = match parsed_url.repository_type {
            RepositoryType::Model => {
                format!(
                    "{}/resolve/{}/{}",
                    parsed_url.repository_id, revision, file_path
                )
            }
            RepositoryType::Dataset => {
                format!(
                    "datasets/{}/resolve/{}/{}",
                    parsed_url.repository_id, revision, file_path
                )
            }
            RepositoryType::Space => {
                format!(
                    "spaces/{}/resolve/{}/{}",
                    parsed_url.repository_id, revision, file_path
                )
            }
        };

        Ok(base_url.join(&path)?)
    }

    /// Builds the API URL for fetching repository information based on the repository type and ID.
    fn build_repository_url(parsed_url: &ParsedURL, api_base_url: &Url) -> Result<Url> {
        let path = format!(
            "{}/{}",
            parsed_url.repository_type.as_str(),
            parsed_url.repository_id
        );

        Ok(api_base_url.join(&path)?)
    }

    /// Builds the API URL for fetching repository information at a specific revision.
    fn build_repository_revision_url(
        parsed_url: &ParsedURL,
        revision: &str,
        api_base_url: &Url,
    ) -> Result<Url> {
        let path = format!(
            "{}/{}?revision={}",
            parsed_url.repository_type.as_str(),
            parsed_url.repository_id,
            revision
        );

        Ok(api_base_url.join(&path)?)
    }

    /// Builds an `hf://` URL for a file so downstream downloads continue to
    /// use the HF backend (preserving auth and URL semantics).
    fn build_hf_url(parsed_url: &ParsedURL, filename: &str) -> Result<Url> {
        let url = match parsed_url.repository_type {
            RepositoryType::Model => {
                format!("{}://{}/{}", SCHEME, parsed_url.repository_id, filename)
            }
            RepositoryType::Dataset => {
                format!(
                    "{}://datasets/{}/{}",
                    SCHEME, parsed_url.repository_id, filename
                )
            }
            RepositoryType::Space => {
                format!(
                    "{}://spaces/{}/{}",
                    SCHEME, parsed_url.repository_id, filename
                )
            }
        };

        Ok(Url::parse(&url)?)
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
                HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
            );
        }

        Ok(request_header)
    }
}

/// Backend implementation for Hugging Face.
#[async_trait]
impl Backend for HuggingFace {
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

        // Build request headers, including authentication if provided hugging face token.
        let request_header = Self::build_request_headers(
            request
                .hugging_face
                .as_ref()
                .and_then(|hf| hf.token.clone()),
            None,
        )?;

        // Get the Hugging Face information from the request, request must contain Hugging Face
        // information for stat request, otherwise return error.
        let hugging_face = request.hugging_face.as_ref().ok_or_else(|| {
            error!(
                "stat request {} {}: missing Hugging Face information",
                request.task_id, request.url
            );

            Error::InvalidParameter
        })?;

        let parsed_url = ParsedURL::try_from(request.url.as_str())?;
        let (base_url, api_base_url) = Self::resolve_base_urls(hugging_face.base_url.as_deref())?;
        match &parsed_url.file_path {
            Some(file_path) => {
                let download_url = Self::build_download_url(
                    &parsed_url,
                    file_path,
                    &hugging_face.revision,
                    &base_url,
                )?;

                let response = self
                    .client
                    .head(download_url.as_str())
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
                let repository_revision_url = Self::build_repository_revision_url(
                    &parsed_url,
                    &hugging_face.revision,
                    &api_base_url,
                )?;

                let response = self
                    .client
                    .get(repository_revision_url.as_str())
                    .headers(request_header)
                    .timeout(request.timeout)
                    .send()
                    .await
                    .map_err(|err| {
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

                let response_status_code = response.status();
                let response_header = response.headers().clone();
                let content_length = match response_header.get(CONTENT_LENGTH) {
                    Some(content_length) => content_length.to_str()?.parse::<u64>().ok(),
                    None => response.content_length(),
                };

                if !response.status().is_success() {
                    error!(
                        "stat request failed {} {}: {}",
                        request.task_id, repository_revision_url, response_status_code
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

                let entries: Vec<DirEntry> = repository
                    .siblings
                    .unwrap_or_default()
                    .into_iter()
                    .map(|sibling: Sibling| -> Result<DirEntry> {
                        // Return hf:// URLs so downstream downloads continue to use the HF
                        // backend (preserving auth headers and URL semantics).
                        let hf_url: Url = Self::build_hf_url(&parsed_url, &sibling.rfilename)?;
                        let content_length: u64 = sibling
                            .lfs
                            .as_ref()
                            .map(|lfs: &Lfs| lfs.size)
                            .or(sibling.size)
                            .unwrap_or(0);

                        Ok(DirEntry {
                            url: hf_url.to_string(),
                            content_length: content_length as usize,
                            is_dir: false,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

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
    #[instrument(skip_all)]
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
            request.range,
        )?;

        // Get the Hugging Face information from the request, request must contain Hugging Face
        // information for get request, otherwise return error.
        let hugging_face = request.hugging_face.as_ref().ok_or_else(|| {
            error!(
                "get request {} {}: missing Hugging Face information",
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

        let (base_url, _) = Self::resolve_base_urls(hugging_face.base_url.as_deref())?;
        let download_url =
            Self::build_download_url(&parsed_url, file_path, &hugging_face.revision, &base_url)?;
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

        // Build request headers, including authentication if provided hugging face token.
        let request_header = Self::build_request_headers(
            request
                .hugging_face
                .as_ref()
                .and_then(|hf| hf.token.clone()),
            None,
        )?;

        // Get the Hugging Face information from the request, request must contain Hugging Face
        // information for exists request, otherwise return error.
        let hugging_face = request.hugging_face.as_ref().ok_or_else(|| {
            error!(
                "exists request {} {}: missing Hugging Face information",
                request.task_id, request.url
            );

            Error::InvalidParameter
        })?;

        let parsed_url = ParsedURL::try_from(request.url.as_str())?;
        let (base_url, api_base_url) = Self::resolve_base_urls(hugging_face.base_url.as_deref())?;
        match &parsed_url.file_path {
            Some(file_path) => {
                let download_url = Self::build_download_url(
                    &parsed_url,
                    file_path,
                    &hugging_face.revision,
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
                let repository_url = Self::build_repository_url(&parsed_url, &api_base_url)?;
                let response = self
                    .client
                    .head(repository_url.as_str())
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
    use dragonfly_api::common::v2::HuggingFace as HuggingFaceOptions;
    use reqwest::StatusCode;
    use std::time::Duration;
    use wiremock::{
        matchers::{header, method, path, query_param},
        Mock, MockServer, ResponseTemplate,
    };

    type ExpectStat = fn(Result<StatResponse>);
    type ExpectGet = fn(&GetResponse<Body>, &str);

    fn backend() -> HuggingFace {
        HuggingFace::new(Arc::new(Config::default())).unwrap()
    }

    fn options(server: &MockServer, token: Option<&str>) -> Option<HuggingFaceOptions> {
        Some(HuggingFaceOptions {
            revision: "main".to_string(),
            token: token.map(str::to_string),
            base_url: Some(server.uri()),
        })
    }

    fn stat_request(url: &str, hugging_face: Option<HuggingFaceOptions>) -> StatRequest {
        StatRequest {
            task_id: "task".to_string(),
            url: url.to_string(),
            http_header: None,
            timeout: Duration::from_secs(5),
            client_cert: None,
            object_storage: None,
            hdfs: None,
            hugging_face,
            model_scope: None,
            open_csg: None,
        }
    }

    fn get_request(
        url: &str,
        range: Option<Range>,
        hugging_face: Option<HuggingFaceOptions>,
    ) -> GetRequest {
        GetRequest {
            task_id: "task".to_string(),
            piece_id: "piece".to_string(),
            url: url.to_string(),
            range,
            http_header: None,
            timeout: Duration::from_secs(5),
            client_cert: None,
            object_storage: None,
            hdfs: None,
            hugging_face,
            model_scope: None,
            open_csg: None,
        }
    }

    fn exists_request(url: &str, hugging_face: Option<HuggingFaceOptions>) -> ExistsRequest {
        ExistsRequest {
            task_id: "task".to_string(),
            url: url.to_string(),
            http_header: None,
            timeout: Duration::from_secs(5),
            client_cert: None,
            object_storage: None,
            hdfs: None,
            hugging_face,
            model_scope: None,
            open_csg: None,
        }
    }

    #[test]
    fn parse_url_extracts_type_id_and_path() {
        let test_cases = vec![
            (
                "hf://deepseek-ai/DeepSeek-OCR",
                RepositoryType::Model,
                "deepseek-ai/DeepSeek-OCR",
                None,
            ),
            (
                "hf://deepseek-ai/DeepSeek-OCR/",
                RepositoryType::Model,
                "deepseek-ai/DeepSeek-OCR",
                None,
            ),
            (
                "hf://deepseek-ai/DeepSeek-OCR/model.safetensors",
                RepositoryType::Model,
                "deepseek-ai/DeepSeek-OCR",
                Some("model.safetensors"),
            ),
            (
                "hf://deepseek-ai/DeepSeek-OCR/models/v1/model.bin",
                RepositoryType::Model,
                "deepseek-ai/DeepSeek-OCR",
                Some("models/v1/model.bin"),
            ),
            (
                "hf://models/deepseek-ai/DeepSeek-OCR/model.safetensors",
                RepositoryType::Model,
                "deepseek-ai/DeepSeek-OCR",
                Some("model.safetensors"),
            ),
            (
                "hf://datasets/huggingface/squad",
                RepositoryType::Dataset,
                "huggingface/squad",
                None,
            ),
            (
                "hf://datasets/huggingface/squad/train.json",
                RepositoryType::Dataset,
                "huggingface/squad",
                Some("train.json"),
            ),
            (
                "hf://spaces/huggingface/transformers-demo",
                RepositoryType::Space,
                "huggingface/transformers-demo",
                None,
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
        let test_cases = vec!["hf://deepseek-ai", "hf://datasets/huggingface"];

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
            (RepositoryType::Space, "spaces"),
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
                "https://huggingface.co/",
                "https://huggingface.co/api/",
            ),
            (
                Some("https://hf-mirror.com/"),
                "https://hf-mirror.com/",
                "https://hf-mirror.com/api/",
            ),
        ];

        for (base_url, expected_base_url, expected_api_base_url) in test_cases {
            let (resolved_base_url, api_base_url) =
                HuggingFace::resolve_base_urls(base_url).unwrap();
            assert_eq!(resolved_base_url.as_str(), expected_base_url);
            assert_eq!(api_base_url.as_str(), expected_api_base_url);
        }
    }

    #[test]
    fn build_download_url_routes_by_repository_type() {
        let base_url = Url::parse(HUGGING_FACE_BASE_URL).unwrap();

        let test_cases = vec![
            (
                "hf://deepseek-ai/DeepSeek-OCR/model.safetensors",
                "model.safetensors",
                "main",
                "https://huggingface.co/deepseek-ai/DeepSeek-OCR/resolve/main/model.safetensors",
            ),
            (
                "hf://datasets/huggingface/squad/train.json",
                "train.json",
                "main",
                "https://huggingface.co/datasets/huggingface/squad/resolve/main/train.json",
            ),
            (
                "hf://spaces/huggingface/transformers-demo/app.py",
                "app.py",
                "v1.0",
                "https://huggingface.co/spaces/huggingface/transformers-demo/resolve/v1.0/app.py",
            ),
        ];

        for (url, file_path, revision, expected) in test_cases {
            let parsed_url = ParsedURL::try_from(url).unwrap();
            let download_url =
                HuggingFace::build_download_url(&parsed_url, file_path, revision, &base_url)
                    .unwrap();
            assert_eq!(download_url.as_str(), expected);
        }
    }

    #[test]
    fn build_repository_url_routes_by_repository_type() {
        let api_base_url = Url::parse("https://huggingface.co/api/").unwrap();

        let test_cases = vec![
            (
                "hf://deepseek-ai/DeepSeek-OCR",
                "https://huggingface.co/api/models/deepseek-ai/DeepSeek-OCR",
            ),
            (
                "hf://datasets/huggingface/squad",
                "https://huggingface.co/api/datasets/huggingface/squad",
            ),
            (
                "hf://spaces/huggingface/transformers-demo",
                "https://huggingface.co/api/spaces/huggingface/transformers-demo",
            ),
        ];

        for (url, expected) in test_cases {
            let parsed_url = ParsedURL::try_from(url).unwrap();
            let repository_url =
                HuggingFace::build_repository_url(&parsed_url, &api_base_url).unwrap();
            assert_eq!(repository_url.as_str(), expected);
        }
    }

    #[test]
    fn build_repository_revision_url_routes_by_repository_type() {
        let api_base_url = Url::parse("https://huggingface.co/api/").unwrap();

        let test_cases = vec![
            (
                "hf://deepseek-ai/DeepSeek-OCR",
                "main",
                "https://huggingface.co/api/models/deepseek-ai/DeepSeek-OCR?revision=main",
            ),
            (
                "hf://datasets/huggingface/squad",
                "v1.0",
                "https://huggingface.co/api/datasets/huggingface/squad?revision=v1.0",
            ),
            (
                "hf://spaces/huggingface/transformers-demo",
                "main",
                "https://huggingface.co/api/spaces/huggingface/transformers-demo?revision=main",
            ),
        ];

        for (url, revision, expected) in test_cases {
            let parsed_url = ParsedURL::try_from(url).unwrap();
            let repository_revision_url =
                HuggingFace::build_repository_revision_url(&parsed_url, revision, &api_base_url)
                    .unwrap();
            assert_eq!(repository_revision_url.as_str(), expected);
        }
    }

    #[test]
    fn build_hf_url_routes_by_repository_type() {
        let test_cases = vec![
            (
                "hf://deepseek-ai/DeepSeek-OCR",
                "model.safetensors",
                "hf://deepseek-ai/DeepSeek-OCR/model.safetensors",
            ),
            (
                "hf://deepseek-ai/DeepSeek-OCR",
                "models/v1/model.bin",
                "hf://deepseek-ai/DeepSeek-OCR/models/v1/model.bin",
            ),
            (
                "hf://datasets/huggingface/squad",
                "train.json",
                "hf://datasets/huggingface/squad/train.json",
            ),
            (
                "hf://spaces/huggingface/transformers-demo",
                "app.py",
                "hf://spaces/huggingface/transformers-demo/app.py",
            ),
        ];

        for (url, filename, expected) in test_cases {
            let parsed_url = ParsedURL::try_from(url).unwrap();
            let hf_url = HuggingFace::build_hf_url(&parsed_url, filename).unwrap();
            assert_eq!(hf_url.as_str(), expected);
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
                HuggingFace::build_request_headers(token.map(str::to_string), range).unwrap();
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
        let test_cases: Vec<(&str, Mock, ExpectStat)> = vec![
            (
                "hf://owner/repo/model.bin",
                Mock::given(method("HEAD"))
                    .and(path("/owner/repo/resolve/main/model.bin"))
                    .and(header("authorization", "Bearer secret"))
                    .respond_with(
                        ResponseTemplate::new(200).insert_header("content-length", "4096"),
                    ),
                |result| {
                    let response = result.unwrap();
                    assert!(response.success);
                    assert_eq!(response.content_length, Some(4096));
                    assert!(response.entries.is_empty());
                },
            ),
            (
                "hf://owner/repo/model.bin",
                Mock::given(method("HEAD"))
                    .and(path("/owner/repo/resolve/main/model.bin"))
                    .respond_with(ResponseTemplate::new(404)),
                |result| {
                    assert!(
                        matches!(&result, Err(Error::BackendError(err)) if err.status_code == Some(StatusCode::NOT_FOUND))
                    );
                },
            ),
            (
                "hf://owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/models/owner/repo"))
                    .and(query_param("revision", "main"))
                    .and(header("authorization", "Bearer secret"))
                    .and(header("user-agent", DEFAULT_USER_AGENT))
                    .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "siblings": [
                            {"rfilename": "nested/config file.json", "size": 12},
                            {"rfilename": "model.bin", "size": 128, "lfs": {"size": 4096}},
                            {"rfilename": "README.md"}
                        ]
                    }))),
                |result| {
                    let response = result.unwrap();
                    assert!(response.success);
                    assert_eq!(response.entries.len(), 3);
                    assert_eq!(
                        response.entries[0],
                        DirEntry {
                            url: "hf://owner/repo/nested/config%20file.json".to_string(),
                            content_length: 12,
                            is_dir: false,
                        }
                    );
                    assert_eq!(response.entries[1].content_length, 4096);
                    assert_eq!(response.entries[2].content_length, 0);
                },
            ),
            (
                "hf://datasets/owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/datasets/owner/repo"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "siblings": [
                            {"rfilename": "nested/train.json"},
                            {"rfilename": "README.md"}
                        ]
                    }))),
                |result| {
                    let response = result.unwrap();
                    assert!(response.success);
                    assert_eq!(response.entries.len(), 2);
                    assert_eq!(
                        response.entries[0],
                        DirEntry {
                            url: "hf://datasets/owner/repo/nested/train.json".to_string(),
                            content_length: 0,
                            is_dir: false,
                        }
                    );
                },
            ),
            (
                "hf://owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/models/owner/repo"))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .set_body_json(serde_json::json!({"siblings": null})),
                    ),
                |result| {
                    let response = result.unwrap();
                    assert!(response.success);
                    assert!(response.entries.is_empty());
                },
            ),
            (
                "hf://owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/models/owner/repo"))
                    .respond_with(ResponseTemplate::new(401)),
                |result| {
                    assert!(
                        matches!(&result, Err(Error::BackendError(err)) if err.status_code == Some(StatusCode::UNAUTHORIZED))
                    );
                },
            ),
            (
                "hf://owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/models/owner/repo"))
                    .respond_with(ResponseTemplate::new(200).set_body_string("not json")),
                |result| {
                    assert!(
                        matches!(&result, Err(Error::BackendError(err)) if err.status_code.is_none())
                    );
                },
            ),
        ];

        let backend = backend();
        for (url, mock, expect) in test_cases {
            let server = MockServer::start().await;
            mock.mount(&server).await;
            expect(
                backend
                    .stat(stat_request(url, options(&server, Some("secret"))))
                    .await,
            );
        }
    }

    #[tokio::test]
    async fn get_streams_body_and_validates_ranged_responses() {
        let test_cases: Vec<(Option<Range>, Mock, ExpectGet)> = vec![
            (
                None,
                Mock::given(method("GET"))
                    .and(path("/owner/repo/resolve/main/model.bin"))
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
                    .and(path("/owner/repo/resolve/main/model.bin"))
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
                    .and(path("/owner/repo/resolve/main/model.bin"))
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
                    .and(path("/owner/repo/resolve/main/model.bin"))
                    .respond_with(ResponseTemplate::new(404)),
                |response, text| {
                    assert!(!response.success);
                    assert_eq!(response.http_status_code, Some(StatusCode::NOT_FOUND));
                    assert_eq!(text, "");
                },
            ),
        ];

        let backend = backend();
        for (range, mock, expect) in test_cases {
            let server = MockServer::start().await;
            mock.mount(&server).await;
            let mut response = backend
                .get(get_request(
                    "hf://owner/repo/model.bin",
                    range,
                    options(&server, None),
                ))
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
            .and(path("/owner/repo/resolve/main/model.bin"))
            .and(header("range", "bytes=10-29"))
            .respond_with(ResponseTemplate::new(302).insert_header(
                "location",
                format!("{}/objects/model.bin", object_server.uri()),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/objects/model.bin"))
            .and(header("range", "bytes=10-29"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("content-range", "bytes 10-29/100")
                    .set_body_string("redirected lfs data!"),
            )
            .mount(&object_server)
            .await;

        let mut response = backend()
            .get(get_request(
                "hf://owner/repo/model.bin",
                Some(Range {
                    start: 10,
                    length: 20,
                }),
                options(&server, Some("secret")),
            ))
            .await
            .unwrap();

        assert!(response.success);
        assert_eq!(response.text().await.unwrap(), "redirected lfs data!");

        let object_requests = object_server.received_requests().await.unwrap();
        assert_eq!(object_requests.len(), 1);
        assert!(object_requests[0].headers.get("authorization").is_none());
    }

    #[tokio::test]
    async fn get_rejects_missing_options_or_file_path() {
        let test_cases = vec![
            ("hf://owner/repo/model.bin", None),
            ("hf://owner/repo", Some(HuggingFaceOptions::default())),
        ];

        let backend = backend();
        for (url, hugging_face) in test_cases {
            let result = backend.get(get_request(url, None, hugging_face)).await;
            assert!(matches!(result, Err(Error::InvalidParameter)));
        }
    }

    #[tokio::test]
    async fn exists_reports_file_and_repository_presence() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/owner/repo/resolve/main/model.bin"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/api/models/owner/repo"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let backend = backend();

        let test_cases = vec![
            ("hf://owner/repo/model.bin", true),
            ("hf://owner/repo", true),
            ("hf://owner/repo/missing.bin", false),
            ("hf://owner/missing", false),
        ];

        for (url, expected) in test_cases {
            let exists = backend
                .exists(exists_request(url, options(&server, None)))
                .await
                .unwrap();
            assert_eq!(exists, expected);
        }
    }
}
