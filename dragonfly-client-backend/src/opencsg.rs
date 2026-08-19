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
use percent_encoding::percent_decode_str;
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

    /// Parses a canonical route segment into a repository type.
    fn from_segment(segment: &str) -> Option<Self> {
        match segment {
            "models" => Some(RepositoryType::Model),
            "datasets" => Some(RepositoryType::Dataset),
            "spaces" => Some(RepositoryType::Space),
            "codes" => Some(RepositoryType::Code),
            "mcps" => Some(RepositoryType::Mcp),
            "skills" => Some(RepositoryType::Skill),
            _ => None,
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
        if url.scheme() != SCHEME {
            return Err(Error::InvalidURI(url.to_string()));
        }

        let host = url
            .host_str()
            .ok_or_else(|| Error::InvalidURI(url.to_string()))?;
        let raw_path = format!("{}{}", host, url.path().trim_end_matches('/'));

        // Decode each segment so it is re-encoded canonically when building
        // provider URLs, and reject decoded separators and NUL bytes.
        let segments: Vec<String> = raw_path
            .trim_matches('/')
            .split('/')
            .map(|segment| {
                percent_decode_str(segment)
                    .decode_utf8()
                    .map(|segment| segment.into_owned())
                    .map_err(|_| Error::InvalidURI(url.to_string()))
            })
            .collect::<Result<_>>()?;
        if segments
            .iter()
            .any(|segment| segment.contains(['/', '\\', '\0']))
        {
            return Err(Error::InvalidURI(url.to_string()));
        }

        let (repository_type, offset) = match segments.first() {
            Some(segment) => match RepositoryType::from_segment(segment) {
                Some(repository_type) => (repository_type, 1),
                None => (RepositoryType::Model, 0),
            },
            None => return Err(Error::InvalidParameter),
        };

        // After stripping the optional type prefix, at least two non-empty
        // segments (owner and repository name) must remain.
        let remaining = &segments[offset..];
        if remaining.len() < 2 || remaining.iter().any(|segment| segment.is_empty()) {
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

    /// Resolves the base URLs from gRPC OpenCSG options, keeping any path prefix
    /// (e.g., `/csg/`) from custom endpoints.
    fn resolve_base_urls(base_url: Option<&str>) -> Result<(Url, Url)> {
        let mut base_url = Url::parse(base_url.unwrap_or(OPEN_CSG_BASE_URL))?;
        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }

        let api_base_url = base_url.join("api/")?;
        Ok((base_url, api_base_url))
    }

    /// Appends already-decoded path segments so `Url` performs canonical escaping.
    fn append_segments(
        mut base_url: Url,
        segments: impl IntoIterator<Item = String>,
    ) -> Result<Url> {
        {
            let mut path_segments = base_url
                .path_segments_mut()
                .map_err(|_| Error::InvalidParameter)?;
            path_segments.pop_if_empty();
            for segment in segments {
                path_segments.push(&segment);
            }
        }

        Ok(base_url)
    }

    /// Splits a repository-relative path and rejects components unsafe for local
    /// output paths.
    fn repository_path_segments(path: &str) -> Result<Vec<String>> {
        let segments: Vec<&str> = path.split('/').collect();
        if segments.iter().any(|segment| {
            segment.is_empty() || matches!(*segment, "." | "..") || segment.contains(['\\', '\0'])
        }) {
            return Err(Error::InvalidParameter);
        }

        Ok(segments.into_iter().map(str::to_string).collect())
    }

    /// Builds the download URL for a file based on the repository type and path.
    fn build_download_url(
        parsed_url: &ParsedURL,
        file_path: &str,
        revision: &str,
        base_url: &Url,
    ) -> Result<Url> {
        let mut segments = vec![];
        if parsed_url.repository_type != RepositoryType::Model {
            segments.push(parsed_url.repository_type.as_str().to_string());
        }

        segments.extend(parsed_url.repository_id.split('/').map(str::to_string));
        segments.push("resolve".to_string());
        segments.push(revision.to_string());
        segments.extend(Self::repository_path_segments(file_path)?);
        Self::append_segments(base_url.clone(), segments)
    }

    /// Builds the API URL for fetching repository information at a specific revision.
    fn build_repository_revision_url(
        parsed_url: &ParsedURL,
        revision: &str,
        api_base_url: &Url,
    ) -> Result<Url> {
        let mut segments = vec![parsed_url.repository_type.as_str().to_string()];
        segments.extend(parsed_url.repository_id.split('/').map(str::to_string));
        segments.push("revision".to_string());
        segments.push(revision.to_string());

        let mut url = Self::append_segments(api_base_url.clone(), segments)?;
        if parsed_url.repository_type == RepositoryType::Model {
            url.query_pairs_mut().append_pair("blobs", "true");
        }

        Ok(url)
    }

    /// Builds an `opencsg://` URL for a file so downstream downloads continue to
    /// use the OpenCSG backend (preserving auth and URL semantics).
    fn build_opencsg_url(parsed_url: &ParsedURL, filename: &str) -> Result<Url> {
        let (owner, repository) = parsed_url
            .repository_id
            .split_once('/')
            .ok_or(Error::InvalidParameter)?;
        let (authority, mut segments) = match parsed_url.repository_type {
            RepositoryType::Model => (owner, vec![repository.to_string()]),
            repository_type => (
                repository_type.as_str(),
                vec![owner.to_string(), repository.to_string()],
            ),
        };

        segments.extend(Self::repository_path_segments(filename)?);
        Self::append_segments(Url::parse(&format!("{SCHEME}://{authority}"))?, segments)
    }

    /// Build the request headers for OpenCSG API requests, including authentication if a
    /// token is provided by the `--csg-token` CLI flag.
    fn build_request_headers(token: Option<String>, range: Option<Range>) -> Result<HeaderMap> {
        let mut request_header = HeaderMap::new();

        // Add Range header if present in the request.
        if let Some(range) = &range {
            if range.length == 0 {
                return Err(Error::InvalidParameter);
            }

            let end = range
                .start
                .checked_add(range.length - 1)
                .ok_or(Error::InvalidParameter)?;
            request_header.insert(RANGE, format!("bytes={}-{}", range.start, end).parse()?);
        };

        // Make the user agent if not specified in header.
        request_header
            .entry(USER_AGENT)
            .or_insert(HeaderValue::from_static(DEFAULT_USER_AGENT));

        // Add the Authorization header for OpenCSG API authentication.
        if let Some(token) = token {
            request_header.insert(AUTHORIZATION, format!("Bearer {token}").parse()?);
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

        // Get the OpenCSG information from the request, request must contain OpenCSG
        // information for stat request, otherwise return error.
        let open_csg = request.open_csg.as_ref().ok_or_else(|| {
            error!(
                "stat request {} {}: missing OpenCSG information",
                request.task_id, request.url
            );

            Error::InvalidParameter
        })?;

        // Build request headers, including authentication if provided OpenCSG token.
        let request_header = Self::build_request_headers(open_csg.token.clone(), None)?;

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

                let response = match self
                    .client
                    .head(download_url.as_str())
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

                // A failed listing must not be mistaken for an empty repository, so
                // listing failures are returned as backend errors.
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
                if !response_status_code.is_success() {
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

                let content_length = match response_header.get(CONTENT_LENGTH) {
                    Some(content_length) => content_length.to_str()?.parse::<u64>().ok(),
                    None => response.content_length(),
                };

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
                    .filter(|sibling| {
                        !sibling.rfilename.is_empty() && sibling.r#type.as_deref() != Some("tree")
                    })
                    .map(|sibling| -> Result<DirEntry> {
                        // Return opencsg:// URLs so downstream downloads continue to use
                        // the OpenCSG backend (preserving auth and URL semantics).
                        let opencsg_url = Self::build_opencsg_url(&parsed_url, &sibling.rfilename)?;
                        let content_length = sibling
                            .lfs
                            .and_then(|lfs| lfs.size)
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
                    success: true,
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

        // Get the OpenCSG information from the request, request must contain OpenCSG
        // information for get request, otherwise return error.
        let open_csg = request.open_csg.as_ref().ok_or_else(|| {
            error!(
                "get request {} {}: missing OpenCSG information",
                request.task_id, request.url
            );

            Error::InvalidParameter
        })?;

        // Build request headers, including authentication if provided OpenCSG token.
        let request_header = Self::build_request_headers(open_csg.token.clone(), request.range)?;

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

        // Get the OpenCSG information from the request, request must contain OpenCSG
        // information for exists request, otherwise return error.
        let open_csg = request.open_csg.as_ref().ok_or_else(|| {
            error!(
                "exists request {} {}: missing OpenCSG information",
                request.task_id, request.url
            );

            Error::InvalidParameter
        })?;

        // Build request headers, including authentication if provided OpenCSG token.
        let request_header = Self::build_request_headers(open_csg.token.clone(), None)?;

        let parsed_url = ParsedURL::try_from(request.url.as_str())?;
        let (base_url, api_base_url) = Self::resolve_base_urls(open_csg.base_url.as_deref())?;
        let url = match &parsed_url.file_path {
            Some(file_path) => {
                Self::build_download_url(&parsed_url, file_path, &open_csg.revision, &base_url)?
            }
            None => {
                Self::build_repository_revision_url(&parsed_url, &open_csg.revision, &api_base_url)?
            }
        };

        let response = self
            .client
            .head(url.as_str())
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

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_api::common::v2::OpenCsg as OpenCsgOptions;
    use std::time::Duration;
    use wiremock::{
        matchers::{header, method, path, query_param},
        Mock, MockServer, ResponseTemplate,
    };

    /// Creates OpenCSG request options targeting a test server.
    fn options(server: &MockServer) -> OpenCsgOptions {
        OpenCsgOptions {
            token: Some("secret".to_string()),
            revision: "main".to_string(),
            base_url: Some(server.uri()),
        }
    }

    /// Verifies all OpenCSG repository kinds are parsed from provider URLs.
    #[test]
    fn parses_model_and_extended_repository_types() {
        let model = ParsedURL::try_from("opencsg://owner/repo/a/b.bin").unwrap();
        assert_eq!(model.repository_type, RepositoryType::Model);
        assert_eq!(model.repository_id, "owner/repo");
        assert_eq!(model.file_path.as_deref(), Some("a/b.bin"));
        for (segment, repository_type) in [
            ("datasets", RepositoryType::Dataset),
            ("spaces", RepositoryType::Space),
            ("codes", RepositoryType::Code),
            ("mcps", RepositoryType::Mcp),
            ("skills", RepositoryType::Skill),
        ] {
            let parsed_url =
                ParsedURL::try_from(format!("opencsg://{segment}/owner/repo").as_str()).unwrap();
            assert_eq!(parsed_url.repository_type, repository_type);
        }
    }

    /// Verifies resolve and revision URLs retain custom endpoint path prefixes.
    #[test]
    fn builds_urls_and_preserves_csg_prefix() {
        let parsed_url = ParsedURL::try_from("opencsg://owner/repo/model%20file.bin").unwrap();
        let (base_url, api_base_url) =
            OpenCsg::resolve_base_urls(Some("https://example.test/private/csg")).unwrap();
        let download_url =
            OpenCsg::build_download_url(&parsed_url, "model file.bin", "main", &base_url).unwrap();
        assert_eq!(
            download_url.as_str(),
            "https://example.test/private/csg/owner/repo/resolve/main/model%20file.bin"
        );
        let repository_revision_url =
            OpenCsg::build_repository_revision_url(&parsed_url, "main", &api_base_url).unwrap();
        assert_eq!(
            repository_revision_url.as_str(),
            "https://example.test/private/csg/api/models/owner/repo/revision/main?blobs=true"
        );

        let dataset = ParsedURL::try_from("opencsg://datasets/owner/repo/data.json").unwrap();
        let download_url =
            OpenCsg::build_download_url(&dataset, "data.json", "dev", &base_url).unwrap();
        assert_eq!(
            download_url.as_str(),
            "https://example.test/private/csg/datasets/owner/repo/resolve/dev/data.json"
        );
        let repository_revision_url =
            OpenCsg::build_repository_revision_url(&dataset, "dev", &api_base_url).unwrap();
        assert_eq!(
            repository_revision_url.as_str(),
            "https://example.test/private/csg/api/datasets/owner/repo/revision/dev"
        );
        assert_eq!(
            OpenCsg::build_opencsg_url(&dataset, "nested/data.json")
                .unwrap()
                .as_str(),
            "opencsg://datasets/owner/repo/nested/data.json"
        );

        let explicit_model = ParsedURL::try_from("opencsg://models/owner/repo").unwrap();
        assert_eq!(
            OpenCsg::build_opencsg_url(&explicit_model, "model.bin")
                .unwrap()
                .as_str(),
            "opencsg://owner/repo/model.bin"
        );
    }

    /// Verifies malformed URLs, unsafe paths and invalid ranges are rejected.
    #[test]
    fn rejects_invalid_urls_and_empty_ranges() {
        assert!(ParsedURL::try_from("hf://owner/repo").is_err());
        assert!(ParsedURL::try_from("opencsg://owner").is_err());
        assert!(OpenCsg::build_request_headers(
            None,
            Some(Range {
                start: 0,
                length: 0
            })
        )
        .is_err());
        assert!(OpenCsg::repository_path_segments("../secret").is_err());
        assert!(OpenCsg::repository_path_segments("dir\\secret").is_err());
        assert!(OpenCsg::repository_path_segments("dir/secret\0file").is_err());
    }

    /// Verifies an empty repository represented by JSON null is accepted.
    #[test]
    fn accepts_null_siblings_for_empty_repository() {
        let repository: Repository = serde_json::from_str(r#"{"siblings":null}"#).unwrap();
        assert!(repository.siblings.unwrap_or_default().is_empty());
    }

    /// Verifies recursive model metadata, authentication, sizes and child URLs.
    #[tokio::test]
    async fn stats_model_repository() {
        let server = MockServer::start().await;
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
            })))
            .mount(&server)
            .await;

        let backend = OpenCsg::new(Arc::new(Config::default())).unwrap();
        let response = backend
            .stat(StatRequest {
                task_id: "task".to_string(),
                url: "opencsg://owner/repo".to_string(),
                http_header: None,
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
                open_csg: Some(options(&server)),
            })
            .await
            .unwrap();

        assert!(response.success);
        assert_eq!(response.entries.len(), 3);
        assert_eq!(
            response.entries[0],
            DirEntry {
                url: "opencsg://owner/repo/nested/config%20file.json".to_string(),
                content_length: 12,
                is_dir: false,
            }
        );
        assert_eq!(response.entries[1].content_length, 4096);
        assert_eq!(response.entries[2].content_length, 0);
    }

    /// Verifies dataset metadata uses the typed route and tolerates omitted sizes.
    #[tokio::test]
    async fn stats_dataset_repository_without_sizes() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/datasets/owner/repo/revision/main"))
            .and(header("authorization", "Bearer secret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "siblings": [
                    {"rfilename": "nested/train.json"},
                    {"rfilename": "README.md"}
                ]
            })))
            .mount(&server)
            .await;

        let backend = OpenCsg::new(Arc::new(Config::default())).unwrap();
        let response = backend
            .stat(StatRequest {
                task_id: "task".to_string(),
                url: "opencsg://datasets/owner/repo".to_string(),
                http_header: None,
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
                open_csg: Some(options(&server)),
            })
            .await
            .unwrap();

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
    }

    /// Verifies a failed repository listing returns a backend error instead of an
    /// empty repository.
    #[tokio::test]
    async fn stats_repository_with_error_status() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/models/owner/repo/revision/main"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let backend = OpenCsg::new(Arc::new(Config::default())).unwrap();
        let err = backend
            .stat(StatRequest {
                task_id: "task".to_string(),
                url: "opencsg://owner/repo".to_string(),
                http_header: None,
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
                open_csg: Some(options(&server)),
            })
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::BackendError(err) if err.status_code == Some(reqwest::StatusCode::UNAUTHORIZED)
        ));
    }

    /// Verifies file HEAD uses the resolve route and reports its length.
    #[tokio::test]
    async fn stats_file_with_head() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/owner/repo/resolve/main/model.bin"))
            .and(header("authorization", "Bearer secret"))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "4096"))
            .mount(&server)
            .await;

        let backend = OpenCsg::new(Arc::new(Config::default())).unwrap();
        let response = backend
            .stat(StatRequest {
                task_id: "task".to_string(),
                url: "opencsg://owner/repo/model.bin".to_string(),
                http_header: None,
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
                open_csg: Some(options(&server)),
            })
            .await
            .unwrap();

        assert!(response.success);
        assert_eq!(response.content_length, Some(4096));
    }

    /// Verifies ranged content is streamed only from a valid 206 response.
    #[tokio::test]
    async fn gets_ranged_file() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/owner/repo/resolve/main/model.bin"))
            .and(header("range", "bytes=10-29"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("content-range", "bytes 10-29/100")
                    .set_body_string("partial content here"),
            )
            .mount(&server)
            .await;

        let backend = OpenCsg::new(Arc::new(Config::default())).unwrap();
        let mut response = backend
            .get(GetRequest {
                task_id: "task".to_string(),
                piece_id: "piece".to_string(),
                url: "opencsg://owner/repo/model.bin".to_string(),
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
                model_scope: None,
                open_csg: Some(options(&server)),
            })
            .await
            .unwrap();

        assert!(response.success);
        assert_eq!(response.text().await.unwrap(), "partial content here");
    }

    /// Verifies LFS redirects retain the Range request and stream the object response.
    #[tokio::test]
    async fn follows_lfs_redirect() {
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

        let backend = OpenCsg::new(Arc::new(Config::default())).unwrap();
        let mut response = backend
            .get(GetRequest {
                task_id: "task".to_string(),
                piece_id: "piece".to_string(),
                url: "opencsg://owner/repo/model.bin".to_string(),
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
                model_scope: None,
                open_csg: Some(options(&server)),
            })
            .await
            .unwrap();

        assert!(response.success);
        assert_eq!(response.text().await.unwrap(), "redirected lfs data!");
        let object_requests = object_server.received_requests().await.unwrap();
        assert_eq!(object_requests.len(), 1);
        assert!(object_requests[0].headers.get("authorization").is_none());
    }

    /// Verifies existence checks select the resolve route for files and the
    /// revision route for repositories.
    #[tokio::test]
    async fn checks_file_and_repository_existence() {
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

        let backend = OpenCsg::new(Arc::new(Config::default())).unwrap();
        for (url, expected) in [
            ("opencsg://owner/repo/model.bin", true),
            ("opencsg://owner/repo", true),
            ("opencsg://owner/repo/missing.bin", false),
        ] {
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
                    model_scope: None,
                    open_csg: Some(options(&server)),
                })
                .await
                .unwrap();
            assert_eq!(exists, expected);
        }
    }
}
