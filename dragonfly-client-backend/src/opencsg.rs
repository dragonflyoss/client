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

//! OpenCSG repository backend.
//!
//! OpenCSG exposes Hugging Face compatible SDK routes below `/csg`. Repository
//! metadata is used for recursive downloads and file resolve routes are used
//! for ranged piece downloads. Authentication is deliberately kept in the
//! request options, so it never becomes part of a task URL.

use crate::{
    empty_body, Backend, Body, DirEntry, ExistsRequest, GetRequest, GetResponse, PutRequest,
    PutResponse, StatRequest, StatResponse, DEFAULT_USER_AGENT, KEEP_ALIVE_INTERVAL,
    POOL_MAX_IDLE_PER_HOST,
};
use async_trait::async_trait;
use dragonfly_api::common::v2::{OpenCsg as OpenCsgOptions, Range};
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
use tracing::{error, instrument};
use url::Url;

/// The URL scheme for OpenCSG repositories.
pub const SCHEME: &str = "opencsg";

/// The default OpenCSG SDK endpoint. The `/csg/` prefix is significant.
const OPEN_CSG_BASE_URL: &str = "https://hub.opencsg.com/csg/";

/// OpenCSG repository kinds exposed by the SDK mapping routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepositoryType {
    /// A model repository, the default kind.
    Model,
    /// A dataset repository.
    Dataset,
    /// A space repository.
    Space,
    /// A code repository.
    Code,
    /// An MCP server repository.
    Mcp,
    /// A skill repository.
    Skill,
}

impl RepositoryType {
    /// Returns the route segment used by OpenCSG.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Model => "models",
            Self::Dataset => "datasets",
            Self::Space => "spaces",
            Self::Code => "codes",
            Self::Mcp => "mcps",
            Self::Skill => "skills",
        }
    }

    /// Parses a canonical repository type route segment.
    fn from_segment(segment: &str) -> Option<Self> {
        match segment {
            "models" => Some(Self::Model),
            "datasets" => Some(Self::Dataset),
            "spaces" => Some(Self::Space),
            "codes" => Some(Self::Code),
            "mcps" => Some(Self::Mcp),
            "skills" => Some(Self::Skill),
            _ => None,
        }
    }
}

/// Parsed representation of an `opencsg://` URL.
#[derive(Debug, Clone)]
pub struct ParsedURL {
    /// Original URL.
    pub url: Url,
    /// Repository identifier in `owner/name` form.
    pub repository_id: String,
    /// OpenCSG repository kind.
    pub repository_type: RepositoryType,
    /// Optional repository-relative file path.
    pub file_path: Option<String>,
}

impl TryFrom<Url> for ParsedURL {
    type Error = Error;

    /// Parses `opencsg://[type/]owner/repository[/path]`.
    fn try_from(url: Url) -> std::result::Result<Self, Self::Error> {
        if url.scheme() != SCHEME {
            return Err(Error::InvalidURI(url.to_string()));
        }
        let host = url
            .host_str()
            .ok_or_else(|| Error::InvalidURI(url.to_string()))?;
        let raw_path = format!("{}{}", host, url.path().trim_end_matches('/'));
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
        if segments.iter().any(|segment| {
            segment.contains('/') || segment.contains('\\') || segment.contains('\0')
        }) {
            return Err(Error::InvalidURI(url.to_string()));
        }
        let (repository_type, offset) = match segments.first() {
            Some(segment) => match RepositoryType::from_segment(segment) {
                Some(kind) => (kind, 1),
                None => (RepositoryType::Model, 0),
            },
            None => return Err(Error::InvalidParameter),
        };
        let remaining = &segments[offset..];
        if remaining.len() < 2 || remaining.iter().any(|segment| segment.is_empty()) {
            return Err(Error::InvalidParameter);
        }
        Ok(Self {
            url,
            repository_id: format!("{}/{}", remaining[0], remaining[1]),
            repository_type,
            file_path: (remaining.len() > 2).then(|| remaining[2..].join("/")),
        })
    }
}

impl TryFrom<&str> for ParsedURL {
    type Error = Error;

    /// Parses a string URL.
    fn try_from(url: &str) -> std::result::Result<Self, Self::Error> {
        ParsedURL::try_from(Url::parse(url).or_err(ErrorType::ParseError)?)
    }
}

/// Repository metadata returned by OpenCSG SDK-compatible APIs.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Metadata {
    /// Files contained in the requested repository revision.
    siblings: Option<Vec<Sibling>>,
}

/// A repository metadata entry.
#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Sibling {
    /// Repository-relative file name.
    #[serde(rename = "rfilename")]
    filename: String,
    /// File size when provided by the metadata endpoint.
    size: Option<u64>,
    /// Git LFS metadata when the entry is backed by LFS.
    lfs: Option<Lfs>,
    /// Entry kind when returned by a compatible endpoint.
    #[serde(rename = "type")]
    entry_type: Option<String>,
}

/// Relevant Git LFS metadata for a repository entry.
#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Lfs {
    /// Downloadable LFS object size.
    size: Option<u64>,
}

/// OpenCSG backend implementation.
pub struct OpenCsg {
    /// Registered backend scheme.
    scheme: String,
    /// HTTP client used for metadata and resolve requests.
    client: Client,
}

impl OpenCsg {
    /// Creates an OpenCSG backend using dfdaemon's common TLS and DNS settings.
    pub fn new(config: Arc<Config>) -> Result<Self> {
        let client_config_builder = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(NoVerifier::new())
            .with_no_client_auth();
        let client = Client::builder()
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

    /// Normalizes a base URL while retaining any user-supplied path prefix.
    fn resolve_base_urls(base_url: Option<&str>) -> Result<(Url, Url)> {
        let mut base = Url::parse(base_url.unwrap_or(OPEN_CSG_BASE_URL))?;
        if !base.path().ends_with('/') {
            let path = format!("{}/", base.path());
            base.set_path(&path);
        }
        let api = base.join("api/")?;
        Ok((base, api))
    }

    /// Appends already-decoded path segments so `Url` performs canonical escaping.
    fn append_segments(mut base: Url, segments: impl IntoIterator<Item = String>) -> Result<Url> {
        let base_string = base.to_string();
        {
            let mut path = base
                .path_segments_mut()
                .map_err(|_| Error::InvalidURI(base_string))?;
            path.pop_if_empty();
            for segment in segments {
                path.push(&segment);
            }
        }
        Ok(base)
    }

    /// Splits a repository-relative path and rejects components unsafe for local output paths.
    fn repository_path_segments(path: &str) -> Result<Vec<String>> {
        let segments: Vec<&str> = path.split('/').collect();
        if segments.is_empty()
            || segments
                .iter()
                .any(|segment| segment.is_empty() || matches!(*segment, "." | ".."))
            || segments.iter().any(|segment| segment.contains('\\'))
            || segments.iter().any(|segment| segment.contains('\0'))
        {
            return Err(Error::InvalidParameter);
        }
        Ok(segments.into_iter().map(str::to_string).collect())
    }

    /// Builds a provider resolve URL for a file.
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

    /// Builds the revision metadata URL used for recursive repository listing.
    fn build_metadata_url(parsed_url: &ParsedURL, revision: &str, api_base: &Url) -> Result<Url> {
        let mut segments = vec![parsed_url.repository_type.as_str().to_string()];
        segments.extend(parsed_url.repository_id.split('/').map(str::to_string));
        segments.push("revision".to_string());
        segments.push(revision.to_string());
        let mut url = Self::append_segments(api_base.clone(), segments)?;
        if parsed_url.repository_type == RepositoryType::Model {
            url.query_pairs_mut().append_pair("blobs", "true");
        }
        Ok(url)
    }

    /// Builds an `opencsg://` child URL that keeps downloads on this backend.
    fn build_opencsg_url(parsed_url: &ParsedURL, filename: &str) -> Result<Url> {
        let repository_segments: Vec<&str> = parsed_url.repository_id.split('/').collect();
        let owner = repository_segments.first().ok_or(Error::InvalidParameter)?;
        let repository = repository_segments.get(1).ok_or(Error::InvalidParameter)?;
        let explicit_model = parsed_url.repository_type == RepositoryType::Model
            && parsed_url.url.host_str() == Some(RepositoryType::Model.as_str());
        let (authority, mut path) = if explicit_model {
            (
                RepositoryType::Model.as_str(),
                vec![(*owner).to_string(), (*repository).to_string()],
            )
        } else if parsed_url.repository_type == RepositoryType::Model {
            (*owner, vec![(*repository).to_string()])
        } else {
            (
                parsed_url.repository_type.as_str(),
                vec![(*owner).to_string(), (*repository).to_string()],
            )
        };
        let mut url = Url::parse(&format!("{SCHEME}://{authority}"))?;
        path.extend(Self::repository_path_segments(filename)?);
        let url_string = url.to_string();
        {
            let mut path_segments = url
                .path_segments_mut()
                .map_err(|_| Error::InvalidURI(url_string))?;
            for segment in path {
                path_segments.push(&segment);
            }
        }
        Ok(url)
    }

    /// Builds Authorization, User-Agent and optional single-range headers.
    fn build_request_headers(token: Option<String>, range: Option<Range>) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        if let Some(range) = range {
            if range.length == 0 {
                return Err(Error::InvalidParameter);
            }
            let end = range
                .start
                .checked_add(range.length - 1)
                .ok_or(Error::InvalidParameter)?;
            headers.insert(RANGE, format!("bytes={}-{end}", range.start).parse()?);
        }
        headers
            .entry(USER_AGENT)
            .or_insert(HeaderValue::from_static(DEFAULT_USER_AGENT));
        if let Some(token) = token {
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|_| Error::InvalidParameter)?,
            );
        }
        Ok(headers)
    }

    /// Wraps response decoding failures as backend errors.
    fn backend_error(message: impl Into<String>) -> Error {
        Error::BackendError(Box::new(BackendError {
            message: message.into(),
            status_code: None,
            header: None,
        }))
    }

    /// Validates options that are required by every OpenCSG request.
    fn validate_options(options: &OpenCsgOptions) -> Result<()> {
        if options.revision.trim().is_empty() {
            return Err(Error::InvalidParameter);
        }
        Ok(())
    }
}

#[async_trait]
impl Backend for OpenCsg {
    /// Returns the OpenCSG URL scheme.
    fn scheme(&self) -> String {
        self.scheme.clone()
    }

    /// Stats a file with HEAD or lists repository metadata with GET.
    #[instrument(skip_all)]
    async fn stat(&self, request: StatRequest) -> Result<StatResponse> {
        let options = request.open_csg.as_ref().ok_or_else(|| {
            error!(
                "stat request {} {}: missing OpenCSG information",
                request.task_id, request.url
            );
            Error::InvalidParameter
        })?;
        Self::validate_options(options)?;
        let headers = Self::build_request_headers(options.token.clone(), None)?;
        let parsed = ParsedURL::try_from(request.url.as_str())?;
        let (base, api) = Self::resolve_base_urls(options.base_url.as_deref())?;
        let response = if let Some(file_path) = parsed.file_path.as_deref() {
            let url = Self::build_download_url(&parsed, file_path, &options.revision, &base)?;
            self.client
                .head(url)
                .headers(headers)
                .timeout(request.timeout)
                .send()
                .await
        } else {
            let url = Self::build_metadata_url(&parsed, &options.revision, &api)?;
            self.client
                .get(url)
                .headers(headers)
                .timeout(request.timeout)
                .send()
                .await
        };
        let response = match response {
            Ok(response) => response,
            Err(err) => {
                return Ok(StatResponse {
                    success: false,
                    content_length: None,
                    http_header: None,
                    http_status_code: None,
                    entries: vec![],
                    error_message: Some(err.to_string()),
                });
            }
        };
        let status = response.status();
        let response_headers = response.headers().clone();
        let content_length = response_headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .or(response.content_length());
        if parsed.file_path.is_some() {
            return Ok(StatResponse {
                success: status.is_success(),
                content_length,
                http_header: Some(response_headers),
                http_status_code: Some(status),
                entries: vec![],
                error_message: Some(status.to_string()),
            });
        }
        if !status.is_success() {
            return Ok(StatResponse {
                success: false,
                content_length: None,
                http_header: Some(response_headers),
                http_status_code: Some(status),
                entries: vec![],
                error_message: Some(status.to_string()),
            });
        }
        let body = response
            .text()
            .await
            .map_err(|err| Self::backend_error(err.to_string()))?;
        let metadata: Metadata =
            serde_json::from_str(&body).map_err(|err| Self::backend_error(err.to_string()))?;
        let entries = metadata
            .siblings
            .unwrap_or_default()
            .into_iter()
            .filter(|sibling| {
                !sibling.filename.is_empty() && sibling.entry_type.as_deref() != Some("tree")
            })
            .map(|sibling| {
                Ok(DirEntry {
                    url: Self::build_opencsg_url(&parsed, &sibling.filename)?.to_string(),
                    content_length: sibling
                        .lfs
                        .and_then(|lfs| lfs.size)
                        .or(sibling.size)
                        .unwrap_or_default() as usize,
                    is_dir: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(StatResponse {
            success: true,
            content_length,
            http_header: Some(response_headers),
            http_status_code: Some(status),
            entries,
            error_message: Some(status.to_string()),
        })
    }

    /// Downloads a file through a single-range GET and streams the response.
    #[instrument(skip_all)]
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>> {
        let options = request.open_csg.as_ref().ok_or_else(|| {
            error!(
                "get request {} {}: missing OpenCSG information",
                request.task_id, request.url
            );
            Error::InvalidParameter
        })?;
        Self::validate_options(options)?;
        let parsed = ParsedURL::try_from(request.url.as_str())?;
        let file_path = parsed.file_path.as_deref().ok_or(Error::InvalidParameter)?;
        let (base, _) = Self::resolve_base_urls(options.base_url.as_deref())?;
        let url = Self::build_download_url(&parsed, file_path, &options.revision, &base)?;
        let headers = Self::build_request_headers(options.token.clone(), request.range)?;
        let response = match self
            .client
            .get(url)
            .headers(headers)
            .timeout(request.timeout)
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                return Ok(GetResponse {
                    success: false,
                    http_header: None,
                    http_status_code: None,
                    reader: empty_body(),
                    error_message: Some(err.to_string()),
                });
            }
        };
        let response_headers = response.headers().clone();
        let status = response.status();
        if request.range.is_some()
            && status.is_success()
            && status != reqwest::StatusCode::PARTIAL_CONTENT
        {
            return Ok(GetResponse {
                success: false,
                http_header: Some(response_headers),
                http_status_code: Some(status),
                reader: empty_body(),
                error_message: Some(format!(
                    "expected 206 Partial Content for ranged OpenCSG request, got {status}"
                )),
            });
        }
        if let Err(err) = validate_ranged_response(request.range, status, &response_headers) {
            return Ok(GetResponse {
                success: false,
                http_header: Some(response_headers),
                http_status_code: Some(status),
                reader: empty_body(),
                error_message: Some(err.to_string()),
            });
        }
        let reader = StreamReader::new(
            response
                .bytes_stream()
                .map_err(|err| {
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
        Ok(GetResponse {
            success: status.is_success(),
            http_header: Some(response_headers),
            http_status_code: Some(status),
            reader,
            error_message: Some(status.to_string()),
        })
    }

    /// OpenCSG upload is outside the download protocol.
    #[instrument(skip_all)]
    async fn put(&self, _request: PutRequest) -> Result<PutResponse> {
        unimplemented!()
    }

    /// Checks file existence with HEAD, or repository existence with metadata GET.
    #[instrument(skip_all)]
    async fn exists(&self, request: ExistsRequest) -> Result<bool> {
        let options = request.open_csg.as_ref().ok_or(Error::InvalidParameter)?;
        Self::validate_options(options)?;
        let headers = Self::build_request_headers(options.token.clone(), None)?;
        let parsed = ParsedURL::try_from(request.url.as_str())?;
        let (base, api) = Self::resolve_base_urls(options.base_url.as_deref())?;
        let response = if let Some(file_path) = parsed.file_path.as_deref() {
            let url = Self::build_download_url(&parsed, file_path, &options.revision, &base)?;
            self.client
                .head(url)
                .headers(headers)
                .timeout(request.timeout)
                .send()
                .await
        } else {
            let url = Self::build_metadata_url(&parsed, &options.revision, &api)?;
            self.client
                .get(url)
                .headers(headers)
                .timeout(request.timeout)
                .send()
                .await
        }?;
        Ok(response.status().is_success())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            let parsed =
                ParsedURL::try_from(format!("opencsg://{segment}/owner/repo").as_str()).unwrap();
            assert_eq!(parsed.repository_type, repository_type);
        }
    }

    /// Verifies resolve and metadata URLs retain custom endpoint path prefixes.
    #[test]
    fn builds_urls_and_preserves_csg_prefix() {
        let parsed = ParsedURL::try_from("opencsg://owner/repo/model%20file.bin").unwrap();
        let (base, api) =
            OpenCsg::resolve_base_urls(Some("https://example.test/private/csg")).unwrap();
        let download =
            OpenCsg::build_download_url(&parsed, "model file.bin", "main", &base).unwrap();
        assert_eq!(
            download.as_str(),
            "https://example.test/private/csg/owner/repo/resolve/main/model%20file.bin"
        );
        let metadata = OpenCsg::build_metadata_url(&parsed, "main", &api).unwrap();
        assert_eq!(
            metadata.as_str(),
            "https://example.test/private/csg/api/models/owner/repo/revision/main?blobs=true"
        );

        let dataset = ParsedURL::try_from("opencsg://datasets/owner/repo/data.json").unwrap();
        let download = OpenCsg::build_download_url(&dataset, "data.json", "dev", &base).unwrap();
        assert_eq!(
            download.as_str(),
            "https://example.test/private/csg/datasets/owner/repo/resolve/dev/data.json"
        );
        let metadata = OpenCsg::build_metadata_url(&dataset, "dev", &api).unwrap();
        assert_eq!(
            metadata.as_str(),
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
            "opencsg://models/owner/repo/model.bin"
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
        let metadata: Metadata = serde_json::from_str(r#"{"siblings":null}"#).unwrap();
        assert!(metadata.siblings.unwrap_or_default().is_empty());
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

    /// Verifies a successful full-body response cannot satisfy a Range request.
    #[tokio::test]
    async fn rejects_full_body_for_range_from_zero() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/owner/repo/resolve/main/model.bin"))
            .and(header("range", "bytes=0-19"))
            .respond_with(ResponseTemplate::new(200).set_body_string("full body"))
            .mount(&server)
            .await;

        let backend = OpenCsg::new(Arc::new(Config::default())).unwrap();
        let response = backend
            .get(GetRequest {
                task_id: "task".to_string(),
                piece_id: "piece".to_string(),
                url: "opencsg://owner/repo/model.bin".to_string(),
                range: Some(Range {
                    start: 0,
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

        assert!(!response.success);
        assert_eq!(response.http_status_code, Some(reqwest::StatusCode::OK));
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

    /// Verifies existence checks select HEAD for files and metadata GET for repositories.
    #[tokio::test]
    async fn checks_file_and_repository_existence() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/owner/repo/resolve/main/model.bin"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
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
