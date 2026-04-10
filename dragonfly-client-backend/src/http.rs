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

//! HTTP/HTTPS backend implementation for downloading files from web servers.
//!
//! This module provides support for the `http://` and `https://` URL schemes to download
//! files from any HTTP-compatible server. It handles connection pooling, automatic retries
//! with exponential backoff, TLS certificate management, and 307 Temporary Redirect caching
//! to optimize performance for object storage services.
//!
//! # URL Format
//!
//! The URL format is standard HTTP/HTTPS:
//! - `http://<host>[:<port>]/<path>`
//! - `https://<host>[:<port>]/<path>`
//!
//! Examples:
//! - `http://example.com/data/model.bin` - Download via HTTP
//! - `https://cdn.example.com/releases/v1.0/archive.tar.gz` - Download via HTTPS
//! - `https://s3.amazonaws.com/bucket/key` - Download from object storage
//!
//! # Features
//!
//! - **Connection Pooling**: Maintains a pool of HTTP clients for concurrent downloads.
//! - **Automatic Retries**: Uses exponential backoff for transient failures.
//! - **307 Redirect Caching**: Caches temporary redirects to reduce round trips to origin servers.
//! - **Custom Headers**: Supports custom request headers configured in dfdaemon config.
//! - **TLS Support**: Handles custom CA certificates and TLS verification.
//!
//! # Authentication
//!
//! Authentication is handled through custom HTTP headers configured in the dfdaemon
//! configuration file or passed directly in the request headers.

use crate::{
    Backend, Body, ExistsRequest, GetRequest, GetResponse, PutRequest, PutResponse, StatRequest,
    StatResponse, DEFAULT_USER_AGENT, KEEP_ALIVE_INTERVAL, MAX_RETRY_TIMES, POOL_MAX_IDLE_PER_HOST,
};
use async_trait::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};
use dragonfly_api::common::v2::Range;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use dragonfly_client_util::tls::NoVerifier;
use futures::TryStreamExt;
use http::header::{
    HeaderName, HeaderValue, CONTENT_LENGTH, LOCATION, RANGE, TRANSFER_ENCODING, USER_AGENT,
};
use lru::LruCache;
use reqwest::header::HeaderMap;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use reqwest_tracing::TracingMiddleware;
use rustls_pki_types::CertificateDer;
use std::collections::HashMap;
use std::io::{Error as IOError, ErrorKind};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_util::io::StreamReader;
use tracing::{debug, error, info, instrument};
use url::Url;

/// HTTP_SCHEME is the HTTP scheme.
pub const HTTP_SCHEME: &str = "http";

/// HTTPS_SCHEME is the HTTPS scheme.
pub const HTTPS_SCHEME: &str = "https";

/// USER_AGENT_HEADER is the user agent header.
pub const USER_AGENT_HEADER: &str = "user-agent";

/// TemporaryRedirectEntry stores a temporary redirect entry with its creation time.
#[derive(Clone, Debug)]
struct TemporaryRedirectEntry {
    /// URL is the redirect Location URL.
    url: String,

    /// Created at is the time when the entry was created.
    created_at: Instant,
}

/// HTTP is the HTTP backend.
pub struct HTTP {
    /// Scheme is the scheme of the HTTP backend.
    scheme: String,

    /// Clients is a pool of reqwest clients (each has its own connection pool).
    clients: Arc<DashMap<usize, ClientWithMiddleware>>,

    /// Request headers is the custom request headers configurated in the dfdaemon config,
    /// which will insert to the each request if original header is not already set.
    request_header: Option<HashMap<String, String>>,

    /// Temporary redirects stores 307 redirect url with TTL (LRU eviction).
    temporary_redirects: Arc<Mutex<LruCache<String, TemporaryRedirectEntry>>>,

    /// Caching 307 redirects can avoid extra round trips to the origin server for subsequent
    /// requests.
    enable_cache_temporary_redirect: bool,

    /// Cache TTL for temporary redirects. If a cached redirect is older than this duration, it
    /// will be considered expired and removed from the cache.
    cache_temporary_redirect_ttl: Duration,

    /// Enable hickory DNS resolver for reqwest client. It can be enabled to improve DNS resolution
    /// performance
    enable_hickory_dns: bool,
}

/// HTTP implements the http interface.
impl HTTP {
    /// MAX_CONNECTIONS_PER_ADDRESS is the maximum number of connections per address.
    const MAX_CONNECTIONS_PER_ADDRESS: usize = 32;

    /// DEFAULT_CACHE_TEMPORARY_REDIRECT_CAPACITY is the default capacity for temporary redirect cache.
    const DEFAULT_CACHE_TEMPORARY_REDIRECT_CAPACITY: usize = 1000;

    /// Create a new HTTP backend.
    pub fn new(
        scheme: &str,
        request_header: Option<HashMap<String, String>>,
        enable_cache_temporary_redirect: bool,
        cache_temporary_redirect_ttl: Duration,
        enable_hickory_dns: bool,
    ) -> Result<HTTP> {
        // Disable automatic compression to prevent double-decompression issues.
        //
        // Problem scenario:
        // 1. Origin server supports gzip and returns "content-encoding: gzip" header.
        // 2. Backend decompresses the response and stores uncompressed content to disk.
        // 3. When user's client downloads via dfdaemon proxy, the original "content-encoding: gzip".
        //    header is forwarded to it.
        // 4. User's client attempts to decompress the already-decompressed content, causing errors.
        //
        // Solution: Disable all compression formats (gzip, brotli, zstd, deflate) to ensure
        // we receive and store uncompressed content, eliminating the double-decompression issue.
        let make_reqwest_client = || -> Result<ClientWithMiddleware> {
            // Default TLS client config with no validation.
            let client_config_builder = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(NoVerifier::new())
                .with_no_client_auth();

            let client = reqwest::Client::builder()
                // Disable automatic compression to prevent double-decompression issues.
                .no_gzip()
                .no_brotli()
                .no_zstd()
                .no_deflate()
                .http1_only()
                .hickory_dns(enable_hickory_dns)
                .use_preconfigured_tls(client_config_builder)
                .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
                .tcp_keepalive(KEEP_ALIVE_INTERVAL)
                .tcp_nodelay(true)
                .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                    if enable_cache_temporary_redirect
                        && attempt.status() == reqwest::StatusCode::TEMPORARY_REDIRECT
                    {
                        attempt.stop()
                    } else {
                        attempt.follow()
                    }
                })) // Disable automatic redirects when status is 307.
                .build()?;

            let retry_policy =
                ExponentialBackoff::builder().build_with_max_retries(MAX_RETRY_TIMES);
            let client = ClientBuilder::new(client)
                .with(TracingMiddleware::default())
                .with(RetryTransientMiddleware::new_with_policy(retry_policy))
                .build();

            Ok(client)
        };

        let clients = DashMap::with_capacity(Self::MAX_CONNECTIONS_PER_ADDRESS);
        for i in 0..Self::MAX_CONNECTIONS_PER_ADDRESS {
            let client = make_reqwest_client()?;
            clients.insert(i, client);
        }

        Ok(Self {
            scheme: scheme.to_string(),
            clients: Arc::new(clients),
            request_header,
            temporary_redirects: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(Self::DEFAULT_CACHE_TEMPORARY_REDIRECT_CAPACITY).unwrap(),
            ))),
            enable_cache_temporary_redirect,
            cache_temporary_redirect_ttl,
            enable_hickory_dns,
        })
    }

    /// Client returns a new reqwest client.
    fn client(
        &self,
        client_cert: Option<Vec<CertificateDer<'static>>>,
        enable_hickory_dns: bool,
    ) -> Result<ClientWithMiddleware> {
        match client_cert.as_ref() {
            Some(client_cert) => {
                let mut root_cert_store = rustls::RootCertStore::empty();
                root_cert_store.add_parsable_certificates(client_cert.to_owned());

                // TLS client config using the custom CA store for lookups.
                let client_config_builder = rustls::ClientConfig::builder()
                    .with_root_certificates(root_cert_store)
                    .with_no_client_auth();

                // Disable automatic compression to prevent double-decompression issues.
                //
                // Problem scenario:
                // 1. Origin server supports gzip and returns "content-encoding: gzip" header.
                // 2. Backend decompresses the response and stores uncompressed content to disk.
                // 3. When user's client downloads via dfdaemon proxy, the original "content-encoding: gzip".
                //    header is forwarded to it.
                // 4. User's client attempts to decompress the already-decompressed content, causing errors.
                //
                // Solution: Disable all compression formats (gzip, brotli, zstd, deflate) to ensure
                // we receive and store uncompressed content, eliminating the double-decompression issue.
                let client = reqwest::Client::builder()
                    .no_gzip()
                    .no_brotli()
                    .no_zstd()
                    .no_deflate()
                    .http1_only()
                    .hickory_dns(enable_hickory_dns)
                    .use_preconfigured_tls(client_config_builder)
                    .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
                    .tcp_keepalive(KEEP_ALIVE_INTERVAL)
                    .tcp_nodelay(true)
                    .redirect(reqwest::redirect::Policy::custom({
                        let enable_cache_temporary_redirect = self.enable_cache_temporary_redirect;
                        move |attempt| {
                            if enable_cache_temporary_redirect
                                && attempt.status() == reqwest::StatusCode::TEMPORARY_REDIRECT
                            {
                                attempt.stop()
                            } else {
                                attempt.follow()
                            }
                        }
                    })) // Disable automatic redirects when status is 307.
                    .build()?;

                let retry_policy =
                    ExponentialBackoff::builder().build_with_max_retries(MAX_RETRY_TIMES);
                let client = ClientBuilder::new(client)
                    .with(TracingMiddleware::default())
                    .with(RetryTransientMiddleware::new_with_policy(retry_policy))
                    .build();

                Ok(client)
            }
            // Default TLS client config with no validation.
            None => match self
                .clients
                .entry(fastrand::usize(..Self::MAX_CONNECTIONS_PER_ADDRESS))
            {
                Entry::Occupied(o) => Ok(o.get().clone()),
                Entry::Vacant(_) => Err(Error::Unknown("reqwest client not found".to_string())),
            },
        }
    }

    // Make custom request headers to the request header map.
    fn make_request_headers(
        &self,
        request_header: &mut HeaderMap,
        range: Option<Range>,
    ) -> Result<()> {
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

        // Make custom request headers if provided and not defined in original request header.
        if let Some(custom_headers) = &self.request_header {
            for (key, value) in custom_headers {
                let header_key: HeaderName = key.parse()?;
                request_header.entry(header_key).or_insert(value.parse()?);
            }
        }

        Ok(())
    }

    /// Get the cached temporary redirect URL if exists and not expired.
    async fn get_temporary_redirect_url(&self, url: &str) -> Option<String> {
        let mut temporary_redirects = self.temporary_redirects.lock().await;
        if let Some(entry) = temporary_redirects.get(url) {
            if entry.created_at + self.cache_temporary_redirect_ttl > Instant::now() {
                debug!(
                    "found cached temporary redirect for {} -> {}",
                    url, entry.url
                );

                return Some(entry.url.clone());
            } else {
                debug!("cached temporary redirect for {} expired", url);
                temporary_redirects.pop(url);
            }
        }

        None
    }

    /// Store the temporary redirect URL in the cache.
    async fn store_temporary_redirect_url(&self, original_url: &str, target_url: &str) {
        if !self.enable_cache_temporary_redirect {
            return;
        }

        // Only cache absolute URLs. A relative path (e.g. "/new/path") cannot be used
        // as a standalone redirect target in subsequent requests, so skip caching it.
        match Url::parse(target_url) {
            Ok(parsed) if parsed.has_host() => {}
            _ => {
                debug!(
                    "skipping cache for relative redirect {} -> {}",
                    original_url, target_url
                );
                return;
            }
        }

        debug!(
            "caching temporary redirect {} -> {}",
            original_url, target_url
        );

        let mut temporary_redirects = self.temporary_redirects.lock().await;
        temporary_redirects.put(
            original_url.to_string(),
            TemporaryRedirectEntry {
                url: target_url.to_string(),
                created_at: Instant::now(),
            },
        );
    }
}

/// Backend implements the Backend trait.
#[async_trait]
impl Backend for HTTP {
    /// Scheme returns the scheme of the HTTP backend.
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

        // The header of the request is required.
        let mut request_header = request
            .http_header
            .ok_or(Error::InvalidParameter)
            .inspect_err(|_err| {
                error!("request header is missing");
            })?;

        // Make the custom request headers.
        self.make_request_headers(&mut request_header, None)?;

        // Check if we have a cached temporary redirect for this URL.
        let (request_url, request_header) =
            match self.get_temporary_redirect_url(&request.url).await {
                Some(redirect_url) => {
                    let mut redirect_headers = request_header.clone();
                    remove_sensitive_headers(
                        &mut redirect_headers,
                        &redirect_url.parse()?,
                        &request.url.parse()?,
                    );

                    (redirect_url, redirect_headers)
                }
                None => (request.url.clone(), request_header),
            };

        // The signature in the signed URL generated by the object storage client will include
        // the request method. Therefore, the signed URL of the GET method cannot be requested
        // through the HEAD method. Use GET request to replace of HEAD request
        // to get header and status code.
        let response = match self
            .client(request.client_cert.clone(), self.enable_hickory_dns)?
            .get(&request_url)
            .headers(request_header.clone())
            .timeout(request.timeout)
            .send()
            .await
        {
            Ok(response) if response.status() == reqwest::StatusCode::TEMPORARY_REDIRECT => {
                if let Some(location) = response.headers().get(LOCATION) {
                    let location_str = location.to_str().or_err(ErrorType::ParseError)?;

                    // Resolve relative Location URLs against the request URL base so
                    // that the redirect can always be followed with an absolute URL.
                    let base_url = Url::parse(&request.url).or_err(ErrorType::ParseError)?;
                    let redirect_url_parsed =
                        base_url.join(location_str).or_err(ErrorType::ParseError)?;
                    let redirect_url = redirect_url_parsed.as_str();

                    debug!(
                        "stat request got 307 Temporary Redirect, following redirect {} -> {}",
                        request.url, redirect_url
                    );

                    // Pass the raw Location string to the cache so that relative paths
                    // (e.g. "/signed") are not stored — only absolute URLs are cached.
                    self.store_temporary_redirect_url(&request.url, location_str)
                        .await;

                    // Strips sensitive headers when following a cross-origin redirect.
                    let mut redirect_headers = request_header.clone();
                    remove_sensitive_headers(
                        &mut redirect_headers,
                        &redirect_url_parsed,
                        &base_url,
                    );

                    match self
                        .client(request.client_cert.clone(), self.enable_hickory_dns)?
                        .get(redirect_url)
                        .headers(redirect_headers)
                        .timeout(request.timeout)
                        .send()
                        .await
                    {
                        Ok(response) => response,
                        Err(err) => {
                            error!(
                                "stat request failed {} {}: {}",
                                request.task_id, redirect_url, err
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
                    }
                } else {
                    error!(
                        "stat request got 307 Temporary Redirect without Location header {} {}",
                        request.task_id, request_url
                    );

                    return Ok(StatResponse {
                        success: false,
                        content_length: None,
                        http_header: None,
                        http_status_code: None,
                        entries: Vec::new(),
                        error_message: Some(
                            "got 307 Temporary Redirect without Location header".to_string(),
                        ),
                    });
                }
            }
            Ok(response)
                if response.headers().get(TRANSFER_ENCODING).is_some()
                    && response.headers().get(CONTENT_LENGTH).is_none() =>
            {
                // If the response has Transfer-Encoding header but no Content-Length header,
                // retry with HEAD request to get the correct Content-Length.
                info!(
                    "stat request got Transfer-Encoding header, retrying with HEAD {} {}",
                    request.task_id, request.url,
                );

                match self
                    .client(request.client_cert.clone(), self.enable_hickory_dns)?
                    .head(&request_url)
                    .headers(request_header.clone())
                    .timeout(request.timeout)
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(err) => {
                        error!(
                            "stat request failed with HEAD {} {}: {}",
                            request.task_id, request_url, err
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
                }
            }
            Ok(response) => response,
            Err(err) => {
                error!(
                    "stat request failed with GET {} {}: {}",
                    request.task_id, request_url, err
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
            request.task_id, request_url, response_status_code, content_length, response_header
        );

        // Drop the response body to avoid reading it.
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

    /// Get the content from the backend.
    #[instrument(skip_all)]
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>> {
        debug!(
            "get request {} {} {}: {:?}",
            request.task_id, request.piece_id, request.url, request.http_header
        );

        // The header of the request is required.
        let mut request_header = request
            .http_header
            .ok_or(Error::InvalidParameter)
            .inspect_err(|_err| {
                error!("request header is missing");
            })?;

        // Make the custom request headers.
        self.make_request_headers(&mut request_header, request.range)?;

        // Check if we have a cached temporary redirect for this URL.
        let (request_url, request_header) =
            match self.get_temporary_redirect_url(&request.url).await {
                Some(redirect_url) => {
                    let mut redirect_headers = request_header.clone();
                    remove_sensitive_headers(
                        &mut redirect_headers,
                        &redirect_url.parse()?,
                        &request.url.parse()?,
                    );

                    (redirect_url, redirect_headers)
                }
                None => (request.url.clone(), request_header),
            };

        let mut response = match self
            .client(request.client_cert.clone(), self.enable_hickory_dns)?
            .get(&request_url)
            .headers(request_header.clone())
            .timeout(request.timeout)
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                error!(
                    "get request failed {} {} {}: {}",
                    request.task_id, request.piece_id, request_url, err
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

        // If the response is a 307 Temporary Redirect, follow the redirect manually.
        if response.status() == reqwest::StatusCode::TEMPORARY_REDIRECT {
            if let Some(location) = response.headers().get(LOCATION) {
                let location_str = location.to_str().or_err(ErrorType::ParseError)?;

                // Resolve relative Location URLs against the request URL base so
                // that the redirect can always be followed with an absolute URL.
                let base_url = Url::parse(&request.url).or_err(ErrorType::ParseError)?;
                let redirect_url_parsed =
                    base_url.join(location_str).or_err(ErrorType::ParseError)?;
                let redirect_url = redirect_url_parsed.as_str();

                debug!(
                    "get request got 307 Temporary Redirect, following redirect {} -> {}",
                    request.url, redirect_url
                );

                // Pass the raw Location string to the cache so that relative paths
                // (e.g. "/signed") are not stored — only absolute URLs are cached.
                self.store_temporary_redirect_url(&request.url, location_str)
                    .await;

                // Strips sensitive headers when following a cross-origin redirect.
                let mut redirect_headers = request_header.clone();
                remove_sensitive_headers(
                    &mut redirect_headers,
                    &redirect_url_parsed,
                    &base_url,
                );

                response = match self
                    .client(request.client_cert.clone(), self.enable_hickory_dns)?
                    .get(redirect_url)
                    .headers(redirect_headers)
                    .timeout(request.timeout)
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(err) => {
                        error!(
                            "get request failed {} {} {}: {}",
                            request.task_id, request.piece_id, redirect_url, err
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
            }
        }

        let response_header = response.headers().clone();
        let response_status_code = response.status();

        // Non-redirect response or redirect without Location header
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

        // The header of the request is required.
        let mut request_header = request
            .http_header
            .ok_or(Error::InvalidParameter)
            .inspect_err(|_err| {
                error!("request header is missing");
            })?;

        // Make the custom request headers.
        self.make_request_headers(&mut request_header, None)?;

        // The signature in the signed URL generated by the object storage client will include
        // the request method. Therefore, the signed URL of the GET method cannot be requested
        // through the HEAD method. Use GET request to replace of HEAD request
        // to get header and status code.
        let response = match self
            .client(request.client_cert.clone(), self.enable_hickory_dns)?
            .get(&request.url)
            .headers(request_header.clone())
            // Add Range header to ensure Content-Length is returned in response headers.
            // Some servers (especially when using Transfer-Encoding: chunked,
            // refer to https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Transfer-Encoding.) may not
            // include Content-Length in HEAD requests. Using "bytes=0-" requests the
            // entire file starting from byte 0, forcing the server to include file size
            // information in the response headers.
            .header(RANGE, "bytes=0-")
            .timeout(request.timeout)
            .send()
            .await
        {
            Ok(response) if response.status() == reqwest::StatusCode::RANGE_NOT_SATISFIABLE => {
                // For zero-byte files, some servers return 416 Range Not Satisfiable.
                // Retry with a GET request without the Range header to retrieve headers.
                info!(
                    "exists request got 416 Range Not Satisfiable, retrying with HEAD {} {}",
                    request.task_id, request.url
                );

                self.client(request.client_cert.clone(), self.enable_hickory_dns)?
                    .get(&request.url)
                    .headers(request_header.clone())
                    .timeout(request.timeout)
                    .send()
                    .await
                    .inspect_err(|err| {
                        error!(
                            "exists request failed {} {}: {}",
                            request.task_id, request.url, err
                        );
                    })?
            }
            Ok(response) => response,
            Err(err) => {
                error!(
                    "exists request failed {} {}: {}",
                    request.task_id, request.url, err
                );

                return Err(Error::ReqwestMiddlewareError(err));
            }
        };

        let response_status_code = response.status();
        debug!(
            "exists response {} {}: {:?} {:?}",
            request.task_id,
            request.url,
            response_status_code,
            response.headers()
        );

        // Drop the response body to avoid reading it.
        drop(response);
        Ok(response_status_code.is_success())
    }
}

/// Strips sensitive headers when following a cross-origin redirect.
///
/// This replicates the behavior of reqwest's internal `remove_sensitive_headers`:
/// https://github.com/seanmonstar/reqwest/blob/v0.12.15/src/redirect.rs#L134
///
/// Per RFC 9110 §15.4, user agents SHOULD strip credentials when redirecting
/// to a different origin.
fn remove_sensitive_headers(headers: &mut HeaderMap, next: &Url, previous: &Url) {
    if next.host() != previous.host()
        || next.port_or_known_default() != previous.port_or_known_default()
    {
        headers.remove(reqwest::header::AUTHORIZATION);
        headers.remove(reqwest::header::COOKIE);
        headers.remove("cookie2");
        headers.remove(reqwest::header::PROXY_AUTHORIZATION);
        headers.remove(reqwest::header::WWW_AUTHENTICATE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        http::{HTTP, HTTPS_SCHEME, HTTP_SCHEME},
        Backend, ExistsRequest, GetRequest, StatRequest, DEFAULT_USER_AGENT,
    };
    use dragonfly_client_util::tls::{load_certs_from_pem, load_key_from_pem};
    use http::header::{HeaderValue, USER_AGENT};
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use reqwest::{header::HeaderMap, StatusCode};
    use std::collections::HashMap;
    use std::{sync::Arc, time::Duration};
    use tokio::net::TcpListener;
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::TlsAcceptor;
    use wiremock::{
        matchers::{method, path},
        Mock, ResponseTemplate,
    };

    // Generate the certificate and private key by script(`scripts/generate_certs.sh`).
    const SERVER_CERT: &str = r#"""
-----BEGIN CERTIFICATE-----
MIIDsjCCApqgAwIBAgIUCGVh9Btth+ucS6niZsWZb+q6m6UwDQYJKoZIhvcNAQEL
BQAwYjELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0Jl
aWppbmcxEDAOBgNVBAoMB1Rlc3QgQ0ExCzAJBgNVBAsMAklUMRAwDgYDVQQDDAdU
ZXN0IENBMCAXDTI2MDMwMzAyNTU0MloYDzIxMjYwMjA3MDI1NTQyWjBoMQswCQYD
VQQGEwJDTjEQMA4GA1UECAwHQmVpamluZzEQMA4GA1UEBwwHQmVpamluZzEUMBIG
A1UECgwLVGVzdCBTZXJ2ZXIxCzAJBgNVBAsMAklUMRIwEAYDVQQDDAlsb2NhbGhv
c3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC0yUjumwCpg3E1a6s0
CXCruDZfYnggL4McjOOh9buznUN8S2k9as+/+RWYOUecwzayHPUbvpp3Fluaxo9v
YzWSG+TQTf8IXugoECaETsw0nArhjXyOBwhXsA3N6GaAXGSQfqXHNG+IuA0AoX/H
2HiS/QynQXh41BLRZRxlPRYpcUnmWDDk9R82IYpeFx0mGuVzOTh/uiOH2hkL3pEq
hzauEiiK5R26Nr3zPMfKYbIrxCzNLPnk4IiBxdJhhV2c5Eq5XsgNTKcnCOEiScki
Wb+h1tYrEqPi0sdf0JSVd/kL1qyJaSKWK/WJK3TPvpjgnNXBzMOo4wIOA0Aa11OR
ZkSbAgMBAAGjWDBWMBQGA1UdEQQNMAuCCWxvY2FsaG9zdDAdBgNVHQ4EFgQU+qu/
f2ma5LrwFTe4Q8ja9TCCGJwwHwYDVR0jBBgwFoAUSG2Qa0ZPJS8oNv+TDI3N8YOX
TaAwDQYJKoZIhvcNAQELBQADggEBAJWrcf4LOrs95N++0C48HnV0D+3FgcakW7zb
VgJj1ixcCWRbOrnwcjbxVc5OgNY51hq+ixfvLICb0/joYuR/gKWtl8m+ziFzXU3x
3k6G1iS7gFRj/DS4cYH/qwfFEAMxBNREIqZA8DwVsCuuj0isgPRIwSF9o4ZwfzbC
k6ISsAPxnU/rVx+dc25uEqGb+ys6OlO56zTosMSA4Nj95UmZcBS6WbTFbU3IRbvT
N8vGgI5iEEJskRO3Q1JxupHx79J5Zwuz9jmdkVFFgXP9QDOO5JoRnwKb+mvLtxB8
FpStz4dDsu3BN02H1rHDKporN2SMqYEEu45waQHAEA8zfAll2A0=
-----END CERTIFICATE-----
"""#;

    const SERVER_KEY: &str = r#"""
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC0yUjumwCpg3E1
a6s0CXCruDZfYnggL4McjOOh9buznUN8S2k9as+/+RWYOUecwzayHPUbvpp3Flua
xo9vYzWSG+TQTf8IXugoECaETsw0nArhjXyOBwhXsA3N6GaAXGSQfqXHNG+IuA0A
oX/H2HiS/QynQXh41BLRZRxlPRYpcUnmWDDk9R82IYpeFx0mGuVzOTh/uiOH2hkL
3pEqhzauEiiK5R26Nr3zPMfKYbIrxCzNLPnk4IiBxdJhhV2c5Eq5XsgNTKcnCOEi
SckiWb+h1tYrEqPi0sdf0JSVd/kL1qyJaSKWK/WJK3TPvpjgnNXBzMOo4wIOA0Aa
11ORZkSbAgMBAAECggEAOrjs+zAW8XjUA3WjKSZt1iFia+44tb+pF1N+NyPyIcAR
5SQ7nWr96031oTnt1HImaIl2Zloto0P8YlRfz98KThjIZI8JKYdmYmkIkc5kjywm
bqg+DoYjRBRYD4uPC9+2/KZeo8uY9PBPrOZIcroSRDB09TkTcC/2otR0ej/y3Ge3
LahzIyBIJ4wL5CErEOwjsXzUt7jO+WN7hFXRj0ezuZCJB6prt4viu2D6AmKAoPZY
naae3pqcVvnmQiTAI+KhOQuG5VzMWwDw8iu/QXCbYmN8k2LdF5TlgRsKFPyMXVHk
TYpc9DoGFVfq+T+EujBgMDVtVtZY43CTErCmyHQjlQKBgQDr+YrVMwiDdG3buUFM
q5bYBV29SmtcDbkKtYemhMBr+JL7B4meF1VsgvRPOs0376vQizBowB/39LlOxN4v
a5Qad1DtshwSZcXJsq5ZqQAumRjpsT7Ux4Kj2qqI+sx2fGqDAvgT1Hna3Aq9Y+8z
TJlkfigvhMxzlA9qiHRKSY58TwKBgQDEIMtaMmc7hZ5OmPDh1jdkclGSkVppbsJc
FJotqQzojcvfFY5c/whsPCkdazCN/NPZJvGTOjNVeqDhSuzkC7L90c7WmXaWPIqX
feKyB11YQp4m4wxUqQgaWzzwtUUA6UnbZm7QnK4ytiWsX5eMkcgK079B5iu8wqe1
55TJly2j9QKBgFZX3MDeB4NyGrCHPKl9L5ijfgVBMb9hFhAhFB2N/YqETeOkgmpi
R1OJJzPGZEjPXaLVC0WI5ymnVhbIWjQnvO1iMy6GOVdR/ekrhDgyamqigkcgH8lj
px2laTjt69p+88o0T+mRmXTHhvZ9lozCvm3S64lXoie4SVvFyidUetppAoGBAJue
rdwOvEzFU/xnbFK1p9QixUj33nZj9QIdMsziIyTvRgHn18NAdU10WudF4wv2vZ3D
QdGhT5QWrkq1Kcw04Dx32pf6wtaoiQt1TogWQeHDUjvm0iTmzlAjbvJL0snLUdgt
qeYLPElur+vbGaPnFIRKyaofWTr4dRxn+W4Pb551AoGBAOGO/1Ah4u6c+x9zPeva
VmCY9ufTi5Cp5CPEZRN1Dt48cEUMvIV3pOlwl/JUw9B5yJaKJTEffPo9MgEvGUoD
J7lEIkQHhDJUQaoN8WHlvRv6WBYadialvB5///diQBdNiOukbOSUVoCOR66NyM0k
ghc1mLbKHOuFh6/EslueNpOh
-----END PRIVATE KEY-----
"""#;

    const CA_CERT: &str = r#"""
-----BEGIN CERTIFICATE-----
MIIDpzCCAo+gAwIBAgIUe7Ya+eWGODaIaLGKFiHBzWR8I0IwDQYJKoZIhvcNAQEL
BQAwYjELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0Jl
aWppbmcxEDAOBgNVBAoMB1Rlc3QgQ0ExCzAJBgNVBAsMAklUMRAwDgYDVQQDDAdU
ZXN0IENBMCAXDTI2MDMwMzAyNTU0MloYDzIxMjYwMjA3MDI1NTQyWjBiMQswCQYD
VQQGEwJDTjEQMA4GA1UECAwHQmVpamluZzEQMA4GA1UEBwwHQmVpamluZzEQMA4G
A1UECgwHVGVzdCBDQTELMAkGA1UECwwCSVQxEDAOBgNVBAMMB1Rlc3QgQ0EwggEi
MA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCo3eryzhplBRTTRy+u4KLa3oyX
1cach8hmdPgrGLWDZkJexhMMgd8rky4PRypz2u0DedxKrQqod7e+ea91nblPXk9r
Yboqo75sTqtfPWIgsuEj6L552lzB/9Gkm56Bd+THIrzUNO6G76BCiUR0PEX+SbJb
znuiNTwgAmjXVW6P3sjETk6Sf1ePEUHSxjdff54SXq4jlryRZ3EEWErmPHT6twRg
3bGgTgXiakgYdsuTDE0L+F15TKKeqm90HCfCtzv8GjqT8YlNXl8ZQG+42321kiqP
z4Vg+K34i5atiysDJ2d1RBu1p2chnNI+pUd7p/hOSJqC00ZBw57sLcgBwgZlAgMB
AAGjUzBRMB0GA1UdDgQWBBRIbZBrRk8lLyg2/5MMjc3xg5dNoDAfBgNVHSMEGDAW
gBRIbZBrRk8lLyg2/5MMjc3xg5dNoDAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3
DQEBCwUAA4IBAQAiTbh2EHoy/MDfK0QBVDMIUaZW10dhrRnUGztiJ2AK1+Y6MOok
C8ovPXCeHt3mLlptnlnt04FIpOaQzuz8jo+rJu0tEdh1cJy7T7vxWZxhnQ3u0dr3
MewqYzM3i05/EmWn1PSzKMu4+fAc/cEyzdqULF7sb+nBcRdLuoLJkCkSz26ccROp
zEr1F+LxuslKmIu62w5PwvjQ9Hq2oH2GnhmjRXcX87lNsBOZrW0RmcLHTTqp/YJT
1TKCwXgwX6eCN103B1WO3IkPJZ8PgwEoEFXmE1NlPLGqqqd9J4wvfg0CiSRSM1j1
+4LNKz9FP28tWLMNZK1fmkU8Iv6UpASrGwAo
-----END CERTIFICATE-----
"""#;

    const WRONG_CA_CERT: &str = r#"""
-----BEGIN CERTIFICATE-----
MIIDqzCCApOgAwIBAgIUA2hjJ1hZToi943msnaCZtUq5rNkwDQYJKoZIhvcNAQEL
BQAwZDELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0Jl
aWppbmcxETAPBgNVBAoMCFdyb25nIENBMQswCQYDVQQLDAJJVDERMA8GA1UEAwwI
V3JvbmcgQ0EwIBcNMjYwMzAzMDI1NTQyWhgPMjEyNjAyMDcwMjU1NDJaMGQxCzAJ
BgNVBAYTAkNOMRAwDgYDVQQIDAdCZWlqaW5nMRAwDgYDVQQHDAdCZWlqaW5nMREw
DwYDVQQKDAhXcm9uZyBDQTELMAkGA1UECwwCSVQxETAPBgNVBAMMCFdyb25nIENB
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxYrk93GSr1ix4hVDG0Mq
WcDEdTH6l58GR+6GVRlYJdou30T3FRj+07t15Vy79haX4+H4vK82Tj/DtV+oSm9+
sHmt3LsnrPJgIowpXlH3SYMR9eA/YrPOd2Ei/R4hO4TuBmFhecBHseii97Lnv10g
JeFxLT2LZkrvC3mhrYvdt7s5EQgYh3H9oUci45QO+xKWfPQuuZbsTdCplWrORIr5
qvfw2xr401tsva9Z/Oy+bbMyyvk1qdBg0KFU/HYQRQqKOS8twya//LuBUFcBPvrU
Tv4XVdbGnyEPlJSegG+/XRmaOyUkoTTo893/X3oaM8y0quENl/9OXQ2X9c6Q4xIS
QQIDAQABo1MwUTAdBgNVHQ4EFgQUIhyb66/4ZO7E2xRCBnTtKwmkIjQwHwYDVR0j
BBgwFoAUIhyb66/4ZO7E2xRCBnTtKwmkIjQwDwYDVR0TAQH/BAUwAwEB/zANBgkq
hkiG9w0BAQsFAAOCAQEAR/6CYeE6YPupEIELSbAOO6HsFSWU0DC1AhayknAViM2g
gzICmCITXlPf4Pz+2eXA0vucjAftG0XUhwkfaUHR3/ZC4gy9Ya927+/8LZg71yiP
Bp8CslMEXUyIn+Buj0IDNgoX2fVug7E5hGSFjSkg5DCz1aExUbnPWEY16dQjFT9f
ctSOG3MLu/SFbXIDt4pIenLrcnq4x1GntvVwChFZymSCXrOsrv+ujRhL1rzfU0g6
pOjHJ3diEXrIXhgELj2gWQLptmlxmivtQXKC/4BuFUkeVzxd1f33Stdf56HH3KAk
LJ8gCHKBOJy9dW62DcRWw6zzlTtt9y18/Btx0Hpawg==
-----END CERTIFICATE-----
"""#;

    /// Start a https server with given public key and private key.
    async fn start_https_server(cert_pem: &str, key_pem: &str) -> String {
        let server_certs = load_certs_from_pem(cert_pem).unwrap();
        let server_key = load_key_from_pem(key_pem).unwrap();

        // Setup the server.
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(server_certs, server_key.clone_key())
            .unwrap();

        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let stream = acceptor.accept(stream).await.unwrap();

                    // Always return 200 OK with OK as its body for any requests.
                    let service = hyper::service::service_fn(|_| async {
                        Ok::<_, hyper::Error>(hyper::Response::new("OK".to_string()))
                    });

                    hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                });
            }
        });

        format!("https://localhost:{}", addr.port())
    }

    #[tokio::test]
    async fn should_stat_response() {
        let server = wiremock::MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/stat"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/html; charset=UTF-8"),
            )
            .mount(&server)
            .await;

        let resp = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600), true)
            .unwrap()
            .stat(StatRequest {
                task_id: "test".to_string(),
                url: format!("{}/stat", server.uri()),
                http_header: Some(HeaderMap::new()),
                timeout: std::time::Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK))
    }

    #[tokio::test]
    async fn should_return_error_response_when_stat_notexists() {
        let server = wiremock::MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/stat"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/html; charset=UTF-8"),
            )
            .mount(&server)
            .await;

        let resp = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600), true)
            .unwrap()
            .stat(StatRequest {
                task_id: "test".to_string(),
                url: format!("{}/stat", server.uri()),
                http_header: None,
                timeout: std::time::Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await;

        assert!(resp.is_err());
    }

    #[tokio::test]
    async fn should_get_response() {
        let server = wiremock::MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/get"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/html; charset=UTF-8")
                    .set_body_string("OK"),
            )
            .mount(&server)
            .await;

        let mut resp = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600), true)
            .unwrap()
            .get(GetRequest {
                task_id: "test".to_string(),
                piece_id: "test".to_string(),
                url: format!("{}/get", server.uri()),
                range: None,
                http_header: Some(HeaderMap::new()),
                timeout: std::time::Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK));
        assert_eq!(resp.text().await.unwrap(), "OK");
    }

    #[tokio::test]
    async fn should_stat_response_with_self_signed_cert() {
        let server_addr = start_https_server(SERVER_CERT, SERVER_KEY).await;
        let resp = HTTP::new(HTTPS_SCHEME, None, true, Duration::from_secs(600), true)
            .unwrap()
            .stat(StatRequest {
                task_id: "test".to_string(),
                url: server_addr,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: Some(load_certs_from_pem(CA_CERT).unwrap()),
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK));
    }

    #[tokio::test]
    async fn should_return_error_response_when_stat_with_wrong_cert() {
        let server_addr = start_https_server(SERVER_CERT, SERVER_KEY).await;
        let resp = HTTP::new(HTTPS_SCHEME, None, true, Duration::from_secs(600), true)
            .unwrap()
            .stat(StatRequest {
                task_id: "test".to_string(),
                url: server_addr,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: Some(load_certs_from_pem(WRONG_CA_CERT).unwrap()),
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await;

        assert!(!resp.unwrap().success);
    }

    #[tokio::test]
    async fn should_get_response_with_self_signed_cert() {
        let server_addr = start_https_server(SERVER_CERT, SERVER_KEY).await;
        let mut resp = HTTP::new(HTTPS_SCHEME, None, true, Duration::from_secs(600), true)
            .unwrap()
            .get(GetRequest {
                task_id: "test".to_string(),
                piece_id: "test".to_string(),
                url: server_addr,
                range: None,
                http_header: Some(HeaderMap::new()),
                timeout: std::time::Duration::from_secs(5),
                client_cert: Some(load_certs_from_pem(CA_CERT).unwrap()),
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK));
        assert_eq!(resp.text().await.unwrap(), "OK");
    }

    #[tokio::test]
    async fn should_return_error_response_when_get_with_wrong_cert() {
        let server_addr = start_https_server(SERVER_CERT, SERVER_KEY).await;
        let resp = HTTP::new(HTTPS_SCHEME, None, true, Duration::from_secs(600), true)
            .unwrap()
            .get(GetRequest {
                task_id: "test".to_string(),
                piece_id: "test".to_string(),
                url: server_addr,
                range: None,
                http_header: Some(HeaderMap::new()),
                timeout: std::time::Duration::from_secs(5),
                client_cert: Some(load_certs_from_pem(WRONG_CA_CERT).unwrap()),
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await;

        assert!(!resp.unwrap().success);
    }

    #[tokio::test]
    async fn should_stat_response_with_no_verifier() {
        let server_addr = start_https_server(SERVER_CERT, SERVER_KEY).await;
        let resp = HTTP::new(HTTPS_SCHEME, None, true, Duration::from_secs(600), true)
            .unwrap()
            .stat(StatRequest {
                task_id: "test".to_string(),
                url: server_addr,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK));
    }

    #[tokio::test]
    async fn should_get_response_with_no_verifier() {
        let server_addr = start_https_server(SERVER_CERT, SERVER_KEY).await;
        let http_backend = HTTP::new(HTTPS_SCHEME, None, true, Duration::from_secs(600), true);
        let mut resp = http_backend
            .unwrap()
            .get(GetRequest {
                task_id: "test".to_string(),
                piece_id: "test".to_string(),
                url: server_addr,
                range: None,
                http_header: Some(HeaderMap::new()),
                timeout: std::time::Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK));
        assert_eq!(resp.text().await.unwrap(), "OK");
    }

    #[tokio::test]
    async fn should_exists_response() {
        let server = wiremock::MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/exists"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/html; charset=UTF-8"),
            )
            .mount(&server)
            .await;

        let resp = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600), true)
            .unwrap()
            .exists(ExistsRequest {
                task_id: "test".to_string(),
                url: format!("{}/exists", server.uri()),
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();

        assert!(resp);
    }

    #[tokio::test]
    async fn should_return_false_when_notexists() {
        let server = wiremock::MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/exists"))
            .respond_with(
                ResponseTemplate::new(404)
                    .insert_header("Content-Type", "text/html; charset=UTF-8"),
            )
            .mount(&server)
            .await;

        let resp = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600), true)
            .unwrap()
            .exists(ExistsRequest {
                task_id: "test".to_string(),
                url: format!("{}/exists", server.uri()),
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();

        assert!(!resp);
    }

    #[tokio::test]
    async fn should_return_error_when_exists_header_missing() {
        let server = wiremock::MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/exists"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/html; charset=UTF-8"),
            )
            .mount(&server)
            .await;

        let resp = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600), true)
            .unwrap()
            .exists(ExistsRequest {
                task_id: "test".to_string(),
                url: format!("{}/exists", server.uri()),
                http_header: None,
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await;

        assert!(resp.is_err());
    }

    #[test]
    fn should_make_request_headers() {
        // Apply default user-agent when not specified.
        let http = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600), true).unwrap();
        let mut headers = HeaderMap::new();
        http.make_request_headers(&mut headers, None).unwrap();
        assert_eq!(
            headers.get(USER_AGENT).unwrap(),
            HeaderValue::from_static(DEFAULT_USER_AGENT)
        );

        // Should not override existing user-agent.
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("custom-agent/1.0"));
        http.make_request_headers(&mut headers, None).unwrap();
        assert_eq!(
            headers.get(USER_AGENT).unwrap(),
            HeaderValue::from_static("custom-agent/1.0")
        );

        // Apply range header when specified.
        let mut headers = HeaderMap::new();
        http.make_request_headers(
            &mut headers,
            Some(Range {
                start: 1,
                length: 100,
            }),
        )
        .unwrap();
        assert_eq!(
            headers.get(RANGE).unwrap(),
            HeaderValue::from_static("bytes=1-100")
        );

        // Apply custom request headers.
        let mut custom_headers = HashMap::new();
        custom_headers.insert("X-Custom-Header".to_string(), "custom-value".to_string());
        custom_headers.insert("Authorization".to_string(), "Bearer token123".to_string());

        let http = HTTP::new(
            HTTP_SCHEME,
            Some(custom_headers),
            true,
            Duration::from_secs(600),
            true,
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        http.make_request_headers(&mut headers, None).unwrap();
        assert_eq!(
            headers.get("X-Custom-Header").unwrap(),
            HeaderValue::from_static("custom-value")
        );
        assert_eq!(
            headers.get("Authorization").unwrap(),
            HeaderValue::from_static("Bearer token123")
        );
        assert_eq!(
            headers.get(USER_AGENT).unwrap(),
            HeaderValue::from_static(DEFAULT_USER_AGENT)
        );

        // Should not override existing custom headers.
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Custom-Header",
            HeaderValue::from_static("original-value"),
        );
        headers.insert("Authorization", HeaderValue::from_static("Bearer original"));
        http.make_request_headers(&mut headers, None).unwrap();
        assert_eq!(
            headers.get("X-Custom-Header").unwrap(),
            HeaderValue::from_static("original-value")
        );
        assert_eq!(
            headers.get("Authorization").unwrap(),
            HeaderValue::from_static("Bearer original")
        );

        // Return error for invalid header name.
        let mut custom_headers = HashMap::new();
        custom_headers.insert("Invalid Header Name".to_string(), "value".to_string());
        let http = HTTP::new(
            HTTP_SCHEME,
            Some(custom_headers),
            true,
            Duration::from_secs(600),
            true,
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        assert!(http.make_request_headers(&mut headers, None).is_err());

        // Return error for invalid header value.
        let mut custom_headers = HashMap::new();
        custom_headers.insert(
            "X-Custom-Header".to_string(),
            "value\nwith\nnewlines".to_string(),
        );
        let http = HTTP::new(
            HTTP_SCHEME,
            Some(custom_headers),
            true,
            Duration::from_secs(600),
            true,
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        assert!(http.make_request_headers(&mut headers, None).is_err());
    }

    #[tokio::test]
    async fn should_cache_307_redirect_with_default_ttl() {
        let server = wiremock::MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/target"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("target content")
                    .insert_header("Content-Type", "text/plain"),
            )
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/redirect"))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("Location", format!("{}/target", server.uri())),
            )
            .expect(1)
            .mount(&server)
            .await;

        // First request - should store redirect url.
        let backend = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600), true).unwrap();
        let mut response = backend
            .get(GetRequest {
                task_id: "025a7b4c4615f86617acb34c7ec3404a0a475c2cfaf847ecead944c0bae6277d"
                    .to_string(),
                piece_id: "1".to_string(),
                url: format!("{}/redirect", server.uri()),
                range: None,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();
        assert_eq!(response.http_status_code, Some(StatusCode::OK));
        assert_eq!(response.text().await.unwrap(), "target content");

        // Second request - should use cached redirect with default TTL.
        let mut response = backend
            .get(GetRequest {
                task_id: "025a7b4c4615f86617acb34c7ec3404a0a475c2cfaf847ecead944c0bae6277d"
                    .to_string(),
                piece_id: "1".to_string(),
                url: format!("{}/redirect", server.uri()),
                range: None,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();
        assert_eq!(response.http_status_code, Some(StatusCode::OK));
        assert_eq!(response.text().await.unwrap(), "target content");
    }

    #[tokio::test]
    async fn should_expire_cached_redirect_after_ttl() {
        let server = wiremock::MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/target"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("target content")
                    .insert_header("Content-Type", "text/plain"),
            )
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/redirect"))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("Location", format!("{}/target", server.uri())),
            )
            .expect(2)
            .mount(&server)
            .await;

        // Use a very short TTL for this test (1 second).
        let backend = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(1), true).unwrap();

        // First request - should store redirect url.
        let mut response = backend
            .get(GetRequest {
                task_id: "test".to_string(),
                piece_id: "1".to_string(),
                url: format!("{}/redirect", server.uri()),
                range: None,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();
        assert_eq!(response.http_status_code, Some(StatusCode::OK));
        assert_eq!(response.text().await.unwrap(), "target content");

        // Wait for cache to expire.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Second request after TTL expiry - should store redirect url again.
        let mut response = backend
            .get(GetRequest {
                task_id: "test".to_string(),
                piece_id: "1".to_string(),
                url: format!("{}/redirect", server.uri()),
                range: None,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();
        assert_eq!(response.http_status_code, Some(StatusCode::OK));
        assert_eq!(response.text().await.unwrap(), "target content");
    }

    #[tokio::test]
    async fn should_not_cache_relative_307_redirect_location() {
        let server = wiremock::MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/target"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("target content")
                    .insert_header("Content-Type", "text/plain"),
            )
            .mount(&server)
            .await;

        // Return a 307 with a relative Location path each time it is called.
        Mock::given(method("GET"))
            .and(path("/redirect"))
            .respond_with(
                ResponseTemplate::new(307).insert_header("Location", "/target"),
            )
            .expect(2)
            .mount(&server)
            .await;

        let backend = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600), true).unwrap();

        // First request - relative Location should NOT be cached.
        let mut response = backend
            .get(GetRequest {
                task_id: "test".to_string(),
                piece_id: "1".to_string(),
                url: format!("{}/redirect", server.uri()),
                range: None,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();
        assert_eq!(response.http_status_code, Some(StatusCode::OK));
        assert_eq!(response.text().await.unwrap(), "target content");

        // Second request - because the relative URL was not cached, the origin server
        // must be contacted again (wiremock expects exactly 2 calls to /redirect).
        let mut response = backend
            .get(GetRequest {
                task_id: "test".to_string(),
                piece_id: "1".to_string(),
                url: format!("{}/redirect", server.uri()),
                range: None,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
                hugging_face: None,
                model_scope: None,
            })
            .await
            .unwrap();
        assert_eq!(response.http_status_code, Some(StatusCode::OK));
        assert_eq!(response.text().await.unwrap(), "target content");
    }
}
