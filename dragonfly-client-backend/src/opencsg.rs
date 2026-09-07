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

//! OpenCSG backend implementation for downloading models and datasets.
//!
//! This module provides support for the `opencsg://` URL scheme to download files
//! from OpenCSG Hub repositories through its Hugging Face compatible SDK API. It
//! handles both regular files and Git LFS files.
//!
//! # URL Format
//!
//! The URL format is: `opencsg://[<repository_type>/]<owner>/<repository>[/<path>]`
//!
//! Examples:
//! - `opencsg://OpenCSG/csg-wukong-1B` - Download entire repository
//! - `opencsg://OpenCSG/csg-wukong-1B/model.safetensors` - Download specific file
//! - `opencsg://datasets/OpenCSG/chinese-fineweb-edu` - Download a dataset repository
//!
//! # Authentication
//!
//! For private repositories, use the `--csg-token` flag.

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

/// The URL scheme for OpenCSG backend.
pub const SCHEME: &str = "opencsg";

/// The base URL for OpenCSG Hub, the `/csg/` path prefix routes to the SDK API.
const OPEN_CSG_BASE_URL: &str = "https://hub.opencsg.com/csg/";

/// Represents the OpenCSG repository information returned by the API.
#[derive(Default, Debug, Deserialize)]
#[serde(default)]
struct Repository {
    siblings: Option<Vec<Sibling>>,
}

/// Represents a file or directory in the OpenCSG repository.
#[derive(Default, Debug, Deserialize)]
#[serde(default)]
struct Sibling {
    rfilename: String,
    size: Option<u64>,
    lfs: Option<Lfs>,
    r#type: Option<String>,
}

/// Represents Git LFS metadata for large files in the OpenCSG repository.
#[derive(Default, Debug, Deserialize)]
#[serde(default)]
struct Lfs {
    size: Option<u64>,
}

/// A parsed representation of an OpenCSG URL.
///
/// Format: `opencsg://[<repository_type>/]<owner>/<repository>[/<path>]`
#[derive(Debug, Clone)]
pub struct ParsedURL {
    /// The original, unparsed URL.
    pub url: Url,

    /// The repository identifier in `<owner>/<repository>` format (e.g., `"OpenCSG/csg-wukong-1B"`).
    pub repository_id: String,

    /// The type of repository: model, dataset, space, code, mcp, or skill.
    pub repository_type: RepositoryType,

    /// An optional file path within the repository (e.g., `"path/to/weights.bin"`).
    pub file_path: Option<String>,
}

/// The type of an OpenCSG repository.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepositoryType {
    /// A model repository. This is the default when no type prefix is specified,
    /// or when explicitly prefixed with `models/`.
    Model,

    /// A dataset repository, prefixed with `datasets/`.
    Dataset,

    /// A space repository, prefixed with `spaces/`.
    Space,

    /// A code repository, prefixed with `codes/`.
    Code,

    /// An MCP server repository, prefixed with `mcps/`.
    Mcp,

    /// A skill repository, prefixed with `skills/`.
    Skill,
}

/// Implements methods for getting string representations and API paths.
impl RepositoryType {
    /// Returns the canonical route segment (e.g., `"models"`, `"datasets"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            RepositoryType::Model => "models",
            RepositoryType::Dataset => "datasets",
            RepositoryType::Space => "spaces",
            RepositoryType::Code => "codes",
            RepositoryType::Mcp => "mcps",
            RepositoryType::Skill => "skills",
        }
    }
}

/// Parses an OpenCSG URL into its constituent components.
///
/// URL Format: opencsg://[<repository_type>/]<owner>/<repository>[/<path>]
/// - repository_type  Optional. One of "models" (default), "datasets", "spaces",
///   "codes", "mcps", or "skills".
/// - owner/repository Required. For example, "OpenCSG/csg-wukong-1B".
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
            Some(&"codes") => (RepositoryType::Code, 1),
            Some(&"mcps") => (RepositoryType::Mcp, 1),
            Some(&"skills") => (RepositoryType::Skill, 1),
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

/// The OpenCSG backend implementation.
pub struct OpenCsg {
    /// The scheme of the OpenCSG backend.
    scheme: String,

    /// HTTP client for making requests.
    client: Client,
}

/// Implements the OpenCSG interface.
impl OpenCsg {
    /// Create a new OpenCsg backend.
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

    /// Resolves the base URLs from gRPC OpenCSG options. The API is nested under
    /// the endpoint's path prefix (e.g., `/csg/api/`), so the join is relative.
    fn resolve_base_urls(base_url: Option<&str>) -> Result<(Url, Url)> {
        let base_url = Url::parse(base_url.unwrap_or(OPEN_CSG_BASE_URL))?;
        let api_base_url = base_url.join("api/")?;
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
            RepositoryType::Code => {
                format!(
                    "codes/{}/resolve/{}/{}",
                    parsed_url.repository_id, revision, file_path
                )
            }
            RepositoryType::Mcp => {
                format!(
                    "mcps/{}/resolve/{}/{}",
                    parsed_url.repository_id, revision, file_path
                )
            }
            RepositoryType::Skill => {
                format!(
                    "skills/{}/resolve/{}/{}",
                    parsed_url.repository_id, revision, file_path
                )
            }
        };

        Ok(base_url.join(&path)?)
    }

    /// Builds the API URL for fetching repository information at a specific revision.
    fn build_repository_revision_url(
        parsed_url: &ParsedURL,
        revision: &str,
        api_base_url: &Url,
    ) -> Result<Url> {
        let path = match parsed_url.repository_type {
            RepositoryType::Model => {
                format!(
                    "models/{}/revision/{}?blobs=true",
                    parsed_url.repository_id, revision
                )
            }
            RepositoryType::Dataset => {
                format!(
                    "datasets/{}/revision/{}",
                    parsed_url.repository_id, revision
                )
            }
            RepositoryType::Space => {
                format!("spaces/{}/revision/{}", parsed_url.repository_id, revision)
            }
            RepositoryType::Code => {
                format!("codes/{}/revision/{}", parsed_url.repository_id, revision)
            }
            RepositoryType::Mcp => {
                format!("mcps/{}/revision/{}", parsed_url.repository_id, revision)
            }
            RepositoryType::Skill => {
                format!("skills/{}/revision/{}", parsed_url.repository_id, revision)
            }
        };

        Ok(api_base_url.join(&path)?)
    }

    /// Builds an `opencsg://` URL for a file so downstream downloads continue to
    /// use the OpenCSG backend (preserving auth and URL semantics).
    fn build_opencsg_url(parsed_url: &ParsedURL, filename: &str) -> Result<Url> {
        let url = match parsed_url.repository_type {
            RepositoryType::Model => {
                format!(
                    "{}://models/{}/{}",
                    SCHEME, parsed_url.repository_id, filename
                )
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
            RepositoryType::Code => {
                format!(
                    "{}://codes/{}/{}",
                    SCHEME, parsed_url.repository_id, filename
                )
            }
            RepositoryType::Mcp => {
                format!(
                    "{}://mcps/{}/{}",
                    SCHEME, parsed_url.repository_id, filename
                )
            }
            RepositoryType::Skill => {
                format!(
                    "{}://skills/{}/{}",
                    SCHEME, parsed_url.repository_id, filename
                )
            }
        };

        Ok(Url::parse(&url)?)
    }

    /// Build the request headers for OpenCSG API requests, including authentication if a
    /// token is provided by the `--csg-token` CLI flag.
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

        // Add the Authorization header for OpenCSG API authentication.
        if let Some(token) = token {
            request_header.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
            );
        }

        Ok(request_header)
    }
}

/// Backend implementation for OpenCSG.
#[async_trait]
impl Backend for OpenCsg {
    /// Returns the scheme of the backend.
    fn scheme(&self) -> String {
        self.scheme.clone()
    }

    /// Stat the file or repository information.
    #[instrument(skip_all)]
    async fn stat(&self, request: StatRequest) -> Result<StatResponse> {
        debug!(
            "stat request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        // Build request headers, including authentication if provided OpenCSG token.
        let request_header = Self::build_request_headers(
            request.open_csg.as_ref().and_then(|csg| csg.token.clone()),
            None,
        )?;

        // Get the OpenCSG information from the request, request must contain OpenCSG
        // information for stat request, otherwise return error.
        let open_csg = request.open_csg.as_ref().ok_or_else(|| {
            error!(
                "stat request {} {}: missing OpenCSG information",
                request.task_id, request.url
            );

            Error::InvalidParameter
        })?;

        let parsed_url = ParsedURL::try_from(request.url.as_str())?;
        let (base_url, api_base_url) = Self::resolve_base_urls(open_csg.base_url.as_deref())?;
        match &parsed_url.file_path {
            Some(file_path) => {
                let download_url = Self::build_download_url(
                    &parsed_url,
                    file_path,
                    &open_csg.revision,
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
                    &open_csg.revision,
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
                    // OpenCSG lists files recursively, skip directory placeholders.
                    .filter(|sibling: &Sibling| sibling.r#type.as_deref() != Some("tree"))
                    .map(|sibling: Sibling| -> Result<DirEntry> {
                        // Return opencsg:// URLs so downstream downloads continue to use the
                        // OpenCSG backend (preserving auth and URL semantics).
                        let opencsg_url: Url =
                            Self::build_opencsg_url(&parsed_url, &sibling.rfilename)?;
                        let content_length: u64 = sibling
                            .lfs
                            .and_then(|lfs: Lfs| lfs.size)
                            .or(sibling.size)
                            .unwrap_or(0);

                        Ok(DirEntry {
                            url: opencsg_url.to_string(),
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

        // Build request headers, including authentication if provided OpenCSG token.
        let request_header = Self::build_request_headers(
            request.open_csg.as_ref().and_then(|csg| csg.token.clone()),
            request.range,
        )?;

        // Get the OpenCSG information from the request, request must contain OpenCSG
        // information for get request, otherwise return error.
        let open_csg = request.open_csg.as_ref().ok_or_else(|| {
            error!(
                "get request {} {}: missing OpenCSG information",
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

        let (base_url, _) = Self::resolve_base_urls(open_csg.base_url.as_deref())?;
        let download_url =
            Self::build_download_url(&parsed_url, file_path, &open_csg.revision, &base_url)?;
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

    /// Exists checks whether the file or the repository exists in the backend.
    #[instrument(skip_all)]
    async fn exists(&self, request: ExistsRequest) -> Result<bool> {
        debug!(
            "exists request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        // Build request headers, including authentication if provided OpenCSG token.
        let request_header = Self::build_request_headers(
            request.open_csg.as_ref().and_then(|csg| csg.token.clone()),
            None,
        )?;

        // Get the OpenCSG information from the request, request must contain OpenCSG
        // information for exists request, otherwise return error.
        let open_csg = request.open_csg.as_ref().ok_or_else(|| {
            error!(
                "exists request {} {}: missing OpenCSG information",
                request.task_id, request.url
            );

            Error::InvalidParameter
        })?;

        let parsed_url = ParsedURL::try_from(request.url.as_str())?;
        let (base_url, api_base_url) = Self::resolve_base_urls(open_csg.base_url.as_deref())?;
        match &parsed_url.file_path {
            Some(file_path) => {
                let download_url = Self::build_download_url(
                    &parsed_url,
                    file_path,
                    &open_csg.revision,
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
                let repository_revision_url = Self::build_repository_revision_url(
                    &parsed_url,
                    &open_csg.revision,
                    &api_base_url,
                )?;

                let response = self
                    .client
                    .head(repository_revision_url.as_str())
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
    use dragonfly_api::common::v2::OpenCsg as OpenCsgOptions;
    use reqwest::StatusCode;
    use std::time::Duration;
    use wiremock::{
        matchers::{header, method, path, query_param},
        Mock, MockServer, ResponseTemplate,
    };

    type ExpectStat = fn(Result<StatResponse>);
    type ExpectGet = fn(&GetResponse<Body>, &str);

    fn backend() -> OpenCsg {
        OpenCsg::new(Arc::new(Config::default())).unwrap()
    }

    fn options(server: &MockServer, token: Option<&str>) -> Option<OpenCsgOptions> {
        Some(OpenCsgOptions {
            revision: "main".to_string(),
            token: token.map(str::to_string),
            base_url: Some(server.uri()),
        })
    }

    fn stat_request(url: &str, open_csg: Option<OpenCsgOptions>) -> StatRequest {
        StatRequest {
            task_id: "task".to_string(),
            url: url.to_string(),
            http_header: None,
            timeout: Duration::from_secs(5),
            client_cert: None,
            object_storage: None,
            hdfs: None,
            hugging_face: None,
            model_scope: None,
            open_csg,
        }
    }

    fn get_request(
        url: &str,
        range: Option<Range>,
        open_csg: Option<OpenCsgOptions>,
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
            hugging_face: None,
            model_scope: None,
            open_csg,
        }
    }

    fn exists_request(url: &str, open_csg: Option<OpenCsgOptions>) -> ExistsRequest {
        ExistsRequest {
            task_id: "task".to_string(),
            url: url.to_string(),
            http_header: None,
            timeout: Duration::from_secs(5),
            client_cert: None,
            object_storage: None,
            hdfs: None,
            hugging_face: None,
            model_scope: None,
            open_csg,
        }
    }

    #[test]
    fn parse_url_extracts_type_id_and_path() {
        let test_cases = vec![
            (
                "opencsg://OpenCSG/csg-wukong-1B",
                RepositoryType::Model,
                "OpenCSG/csg-wukong-1B",
                None,
            ),
            (
                "opencsg://OpenCSG/csg-wukong-1B/",
                RepositoryType::Model,
                "OpenCSG/csg-wukong-1B",
                None,
            ),
            (
                "opencsg://OpenCSG/csg-wukong-1B/model.safetensors",
                RepositoryType::Model,
                "OpenCSG/csg-wukong-1B",
                Some("model.safetensors"),
            ),
            (
                "opencsg://OpenCSG/csg-wukong-1B/models/v1/model.bin",
                RepositoryType::Model,
                "OpenCSG/csg-wukong-1B",
                Some("models/v1/model.bin"),
            ),
            (
                "opencsg://models/OpenCSG/csg-wukong-1B/model.safetensors",
                RepositoryType::Model,
                "OpenCSG/csg-wukong-1B",
                Some("model.safetensors"),
            ),
            (
                "opencsg://datasets/OpenCSG/chinese-fineweb-edu",
                RepositoryType::Dataset,
                "OpenCSG/chinese-fineweb-edu",
                None,
            ),
            (
                "opencsg://datasets/OpenCSG/chinese-fineweb-edu/train.json",
                RepositoryType::Dataset,
                "OpenCSG/chinese-fineweb-edu",
                Some("train.json"),
            ),
            (
                "opencsg://spaces/owner/repo",
                RepositoryType::Space,
                "owner/repo",
                None,
            ),
            (
                "opencsg://codes/owner/repo",
                RepositoryType::Code,
                "owner/repo",
                None,
            ),
            (
                "opencsg://mcps/owner/repo",
                RepositoryType::Mcp,
                "owner/repo",
                None,
            ),
            (
                "opencsg://skills/owner/repo",
                RepositoryType::Skill,
                "owner/repo",
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
        let test_cases = vec!["opencsg://owner", "opencsg://datasets/owner"];

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
            (RepositoryType::Code, "codes"),
            (RepositoryType::Mcp, "mcps"),
            (RepositoryType::Skill, "skills"),
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
                "https://hub.opencsg.com/csg/",
                "https://hub.opencsg.com/csg/api/",
            ),
            (
                Some("https://hub-mirror.example.com/csg/"),
                "https://hub-mirror.example.com/csg/",
                "https://hub-mirror.example.com/csg/api/",
            ),
        ];

        for (base_url, expected_base_url, expected_api_base_url) in test_cases {
            let (resolved_base_url, api_base_url) = OpenCsg::resolve_base_urls(base_url).unwrap();
            assert_eq!(resolved_base_url.as_str(), expected_base_url);
            assert_eq!(api_base_url.as_str(), expected_api_base_url);
        }
    }

    #[test]
    fn build_download_url_routes_by_repository_type() {
        let base_url = Url::parse(OPEN_CSG_BASE_URL).unwrap();

        let test_cases = vec![
            (
                "opencsg://OpenCSG/csg-wukong-1B/model.safetensors",
                "model.safetensors",
                "main",
                "https://hub.opencsg.com/csg/OpenCSG/csg-wukong-1B/resolve/main/model.safetensors",
            ),
            (
                "opencsg://datasets/OpenCSG/chinese-fineweb-edu/train.json",
                "train.json",
                "main",
                "https://hub.opencsg.com/csg/datasets/OpenCSG/chinese-fineweb-edu/resolve/main/train.json",
            ),
            (
                "opencsg://spaces/owner/repo/app.py",
                "app.py",
                "v1.0",
                "https://hub.opencsg.com/csg/spaces/owner/repo/resolve/v1.0/app.py",
            ),
            (
                "opencsg://codes/owner/repo/main.rs",
                "main.rs",
                "main",
                "https://hub.opencsg.com/csg/codes/owner/repo/resolve/main/main.rs",
            ),
            (
                "opencsg://mcps/owner/repo/server.json",
                "server.json",
                "main",
                "https://hub.opencsg.com/csg/mcps/owner/repo/resolve/main/server.json",
            ),
            (
                "opencsg://skills/owner/repo/SKILL.md",
                "SKILL.md",
                "main",
                "https://hub.opencsg.com/csg/skills/owner/repo/resolve/main/SKILL.md",
            ),
        ];

        for (url, file_path, revision, expected) in test_cases {
            let parsed_url = ParsedURL::try_from(url).unwrap();
            let download_url =
                OpenCsg::build_download_url(&parsed_url, file_path, revision, &base_url).unwrap();
            assert_eq!(download_url.as_str(), expected);
        }
    }

    #[test]
    fn build_repository_revision_url_routes_by_repository_type() {
        let api_base_url = Url::parse("https://hub.opencsg.com/csg/api/").unwrap();

        let test_cases = vec![
            (
                "opencsg://OpenCSG/csg-wukong-1B",
                "main",
                "https://hub.opencsg.com/csg/api/models/OpenCSG/csg-wukong-1B/revision/main?blobs=true",
            ),
            (
                "opencsg://datasets/OpenCSG/chinese-fineweb-edu",
                "main",
                "https://hub.opencsg.com/csg/api/datasets/OpenCSG/chinese-fineweb-edu/revision/main",
            ),
            (
                "opencsg://spaces/owner/repo",
                "v1.0",
                "https://hub.opencsg.com/csg/api/spaces/owner/repo/revision/v1.0",
            ),
            (
                "opencsg://codes/owner/repo",
                "main",
                "https://hub.opencsg.com/csg/api/codes/owner/repo/revision/main",
            ),
            (
                "opencsg://mcps/owner/repo",
                "main",
                "https://hub.opencsg.com/csg/api/mcps/owner/repo/revision/main",
            ),
            (
                "opencsg://skills/owner/repo",
                "main",
                "https://hub.opencsg.com/csg/api/skills/owner/repo/revision/main",
            ),
        ];

        for (url, revision, expected) in test_cases {
            let parsed_url = ParsedURL::try_from(url).unwrap();
            let repository_revision_url =
                OpenCsg::build_repository_revision_url(&parsed_url, revision, &api_base_url)
                    .unwrap();
            assert_eq!(repository_revision_url.as_str(), expected);
        }
    }

    #[test]
    fn build_opencsg_url_routes_by_repository_type() {
        let test_cases = vec![
            (
                "opencsg://OpenCSG/csg-wukong-1B",
                "model.safetensors",
                "opencsg://models/OpenCSG/csg-wukong-1B/model.safetensors",
            ),
            (
                "opencsg://OpenCSG/csg-wukong-1B",
                "models/v1/model.bin",
                "opencsg://models/OpenCSG/csg-wukong-1B/models/v1/model.bin",
            ),
            (
                "opencsg://datasets/OpenCSG/chinese-fineweb-edu",
                "train.json",
                "opencsg://datasets/OpenCSG/chinese-fineweb-edu/train.json",
            ),
            (
                "opencsg://spaces/owner/repo",
                "app.py",
                "opencsg://spaces/owner/repo/app.py",
            ),
            (
                "opencsg://codes/owner/repo",
                "main.rs",
                "opencsg://codes/owner/repo/main.rs",
            ),
            (
                "opencsg://mcps/owner/repo",
                "server.json",
                "opencsg://mcps/owner/repo/server.json",
            ),
            (
                "opencsg://skills/owner/repo",
                "SKILL.md",
                "opencsg://skills/owner/repo/SKILL.md",
            ),
        ];

        for (url, filename, expected) in test_cases {
            let parsed_url = ParsedURL::try_from(url).unwrap();
            let opencsg_url = OpenCsg::build_opencsg_url(&parsed_url, filename).unwrap();
            assert_eq!(opencsg_url.as_str(), expected);
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
                OpenCsg::build_request_headers(token.map(str::to_string), range).unwrap();
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
                "opencsg://owner/repo/model.bin",
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
                "opencsg://owner/repo/model.bin",
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
                "opencsg://owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/models/owner/repo/revision/main"))
                    .and(query_param("blobs", "true"))
                    .and(header("authorization", "Bearer secret"))
                    .and(header("user-agent", DEFAULT_USER_AGENT))
                    .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "siblings": [
                            {"rfilename": "nested/config file.json", "size": 12},
                            {"rfilename": "model.bin", "size": 128, "lfs": {"size": 4096}},
                            {"rfilename": "README.md"},
                            {"rfilename": "ignored", "type": "tree"}
                        ]
                    }))),
                |result| {
                    let response = result.unwrap();
                    assert!(response.success);
                    assert_eq!(response.entries.len(), 3);
                    assert_eq!(
                        response.entries[0],
                        DirEntry {
                            url: "opencsg://models/owner/repo/nested/config%20file.json"
                                .to_string(),
                            content_length: 12,
                            is_dir: false,
                        }
                    );
                    assert_eq!(response.entries[1].content_length, 4096);
                    assert_eq!(response.entries[2].content_length, 0);
                },
            ),
            (
                "opencsg://datasets/owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/datasets/owner/repo/revision/main"))
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
                            url: "opencsg://datasets/owner/repo/nested/train.json".to_string(),
                            content_length: 0,
                            is_dir: false,
                        }
                    );
                },
            ),
            (
                "opencsg://owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/models/owner/repo/revision/main"))
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
                "opencsg://owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/models/owner/repo/revision/main"))
                    .respond_with(ResponseTemplate::new(401)),
                |result| {
                    assert!(
                        matches!(&result, Err(Error::BackendError(err)) if err.status_code == Some(StatusCode::UNAUTHORIZED))
                    );
                },
            ),
            (
                "opencsg://owner/repo",
                Mock::given(method("GET"))
                    .and(path("/api/models/owner/repo/revision/main"))
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
                            .set_body_string("partial content here"),
                    ),
                |response, text| {
                    assert!(response.success);
                    assert_eq!(response.http_status_code, Some(StatusCode::PARTIAL_CONTENT));
                    assert_eq!(text, "partial content here");
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
                    "opencsg://owner/repo/model.bin",
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
                "opencsg://owner/repo/model.bin",
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
            ("opencsg://owner/repo/model.bin", None),
            ("opencsg://owner/repo", Some(OpenCsgOptions::default())),
        ];

        let backend = backend();
        for (url, open_csg) in test_cases {
            let result = backend.get(get_request(url, None, open_csg)).await;
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
            .and(path("/api/models/owner/repo/revision/main"))
            .and(query_param("blobs", "true"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let backend = backend();

        let test_cases = vec![
            ("opencsg://owner/repo/model.bin", true),
            ("opencsg://owner/repo", true),
            ("opencsg://owner/repo/missing.bin", false),
            ("opencsg://owner/missing", false),
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
