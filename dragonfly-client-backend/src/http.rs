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

use dashmap::{mapref::entry::Entry, DashMap};
use dragonfly_api::common::v2::Range;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use dragonfly_client_util::tls::NoVerifier;
use futures::TryStreamExt;
use http::header::{HeaderName, HeaderValue, RANGE, USER_AGENT};
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

/// HTTP_SCHEME is the HTTP scheme.
pub const HTTP_SCHEME: &str = "http";

/// HTTPS_SCHEME is the HTTPS scheme.
pub const HTTPS_SCHEME: &str = "https";

/// USER_AGENT_HEADER is the user agent header.
pub const USER_AGENT_HEADER: &str = "user-agent";

/// DEFAULT_USER_AGENT is the default user agent.
pub const DEFAULT_USER_AGENT: &str = concat!("dragonfly", "/", env!("CARGO_PKG_VERSION"));

/// TemporaryRedirectEntry stores a temporary redirect entry with its creation time.
#[derive(Clone, Debug)]
struct TemporaryRedirectEntry {
    /// url is the redirect Location URL.
    url: String,

    /// created_at is the time when the entry was created.
    created_at: Instant,
}

/// HTTP is the HTTP backend.
pub struct HTTP {
    /// scheme is the scheme of the HTTP backend.
    scheme: String,

    /// clients is a pool of reqwest clients (each has its own connection pool).
    clients: Arc<DashMap<usize, ClientWithMiddleware>>,

    /// request_header is the custom request headers configurated in the dfdaemon config,
    /// which will insert to the each request if original header is not already set.
    request_header: Option<HashMap<String, String>>,

    /// temporary_redirects stores 307 redirect url with TTL (LRU eviction).
    temporary_redirects: Arc<Mutex<LruCache<String, TemporaryRedirectEntry>>>,

    /// enable_cache_temporary_redirect indicates whether to enable caching 307 redirects.
    enable_cache_temporary_redirect: bool,

    /// cache_temporary_redirect_ttl is the TTL for cached 307 redirects.
    cache_temporary_redirect_ttl: Duration,
}

/// HTTP implements the http interface.
impl HTTP {
    /// MAX_CONNECTIONS_PER_ADDRESS is the maximum number of connections per address.
    const MAX_CONNECTIONS_PER_ADDRESS: usize = 32;

    /// DEFAULT_CACHE_TEMPORARY_REDIRECT_CAPACITY is the default capacity for temporary redirect cache.
    const DEFAULT_CACHE_TEMPORARY_REDIRECT_CAPACITY: usize = 1000;

    /// new returns a new HTTP.
    pub fn new(
        scheme: &str,
        request_header: Option<HashMap<String, String>>,
        enable_cache_temporary_redirect: bool,
        cache_temporary_redirect_ttl: Duration,
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
                .hickory_dns(true)
                .use_preconfigured_tls(client_config_builder)
                .pool_max_idle_per_host(super::POOL_MAX_IDLE_PER_HOST)
                .tcp_keepalive(super::KEEP_ALIVE_INTERVAL)
                .tcp_nodelay(true)
                .http2_adaptive_window(false)
                .http2_initial_stream_window_size(Some(super::HTTP2_STREAM_WINDOW_SIZE))
                .http2_initial_connection_window_size(Some(super::HTTP2_CONNECTION_WINDOW_SIZE))
                .http2_keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT)
                .http2_keep_alive_interval(super::HTTP2_KEEP_ALIVE_INTERVAL)
                .http2_keep_alive_while_idle(true)
                .http2_max_frame_size(Some(super::HTTP2_MAX_FRAME_SIZE))
                .redirect(reqwest::redirect::Policy::custom(|attempt| {
                    if attempt.status() == reqwest::StatusCode::TEMPORARY_REDIRECT {
                        attempt.stop()
                    } else {
                        attempt.follow()
                    }
                })) // Disable automatic redirects when status is 307.
                .build()?;

            let retry_policy =
                ExponentialBackoff::builder().build_with_max_retries(super::MAX_RETRY_TIMES);
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
        })
    }

    /// client returns a new reqwest client.
    fn client(
        &self,
        client_cert: Option<Vec<CertificateDer<'static>>>,
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
                    .hickory_dns(true)
                    .use_preconfigured_tls(client_config_builder)
                    .tcp_nodelay(true)
                    .http2_adaptive_window(true)
                    .http2_initial_stream_window_size(Some(super::HTTP2_STREAM_WINDOW_SIZE))
                    .http2_initial_connection_window_size(Some(super::HTTP2_CONNECTION_WINDOW_SIZE))
                    .http2_keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT)
                    .http2_keep_alive_interval(super::HTTP2_KEEP_ALIVE_INTERVAL)
                    .http2_keep_alive_while_idle(true)
                    .redirect(reqwest::redirect::Policy::custom(|attempt| {
                        if attempt.status() == reqwest::StatusCode::TEMPORARY_REDIRECT {
                            attempt.stop()
                        } else {
                            attempt.follow()
                        }
                    })) // Disable automatic redirects when status is 307.
                    .build()?;

                let retry_policy =
                    ExponentialBackoff::builder().build_with_max_retries(super::MAX_RETRY_TIMES);
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

    /// get_temporary_redirect_url gets the cached temporary redirect URL if exists
    /// and not expired.
    async fn get_temporary_redirect_url(&self, url: &str) -> String {
        let mut temporary_redirects = self.temporary_redirects.lock().await;
        if let Some(entry) = temporary_redirects.get(url) {
            if entry.created_at + self.cache_temporary_redirect_ttl > Instant::now() {
                debug!(
                    "found cached temporary redirect for {} -> {}",
                    url, entry.url
                );

                return entry.url.clone();
            } else {
                debug!("cached temporary redirect for {} expired", url);
                temporary_redirects.pop(url);
            }
        }

        url.to_string()
    }

    /// store_temporary_redirect_url stores the temporary redirect URL in the cache.
    async fn store_temporary_redirect_url(&self, original_url: &str, target_url: &str) {
        if !self.enable_cache_temporary_redirect {
            return;
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
}

/// Backend implements the Backend trait.
#[tonic::async_trait]
impl super::Backend for HTTP {
    /// scheme returns the scheme of the HTTP backend.
    fn scheme(&self) -> String {
        self.scheme.clone()
    }

    /// head gets the header of the request.
    #[instrument(skip_all)]
    async fn head(&self, request: super::HeadRequest) -> Result<super::HeadResponse> {
        debug!(
            "head request {} {}: {:?}",
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
            .client(request.client_cert.clone())?
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
                    "head request got 416 Range Not Satisfiable, retrying with HEAD {} {}",
                    request.task_id, request.url
                );

                match self
                    .client(request.client_cert.clone())?
                    .get(&request.url)
                    .headers(request_header.clone())
                    .timeout(request.timeout)
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(err) => {
                        error!(
                            "head request failed {} {}: {}",
                            request.task_id, request.url, err
                        );

                        return Ok(super::HeadResponse {
                            success: false,
                            content_length: None,
                            http_header: None,
                            http_status_code: None,
                            entries: Vec::new(),
                            error_message: None,
                        });
                    }
                }
            }
            Ok(response) => response,
            Err(err) => {
                error!(
                    "head request failed {} {}: {}",
                    request.task_id, request.url, err
                );

                return Ok(super::HeadResponse {
                    success: false,
                    content_length: None,
                    http_header: None,
                    http_status_code: None,
                    entries: Vec::new(),
                    error_message: None,
                });
            }
        };

        let response_header = response.headers().clone();
        let response_status_code = response.status();
        let response_content_length = response.content_length();
        debug!(
            "head response {} {}: {:?} {:?} {:?}",
            request.task_id,
            request.url,
            response_status_code,
            response_content_length,
            response_header
        );

        // Drop the response body to avoid reading it.
        drop(response);
        Ok(super::HeadResponse {
            success: response_status_code.is_success(),
            content_length: response_content_length,
            http_header: Some(response_header),
            http_status_code: Some(response_status_code),
            error_message: Some(response_status_code.to_string()),
            entries: Vec::new(),
        })
    }

    /// get gets the content of the request.
    #[instrument(skip_all)]
    async fn get(&self, request: super::GetRequest) -> Result<super::GetResponse<super::Body>> {
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
        let target_url = self.get_temporary_redirect_url(&request.url).await;
        let mut response = match self
            .client(request.client_cert.clone())?
            .get(&target_url)
            .headers(request_header.clone())
            .timeout(request.timeout)
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                error!(
                    "get request failed {} {} {}: {}",
                    request.task_id, request.piece_id, target_url, err
                );

                return Ok(super::GetResponse {
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
            if let Some(location) = response.headers().get(reqwest::header::LOCATION) {
                let target_url = location.to_str().or_err(ErrorType::ParseError)?;
                self.store_temporary_redirect_url(&request.url, target_url)
                    .await;

                response = match self
                    .client(request.client_cert.clone())?
                    .get(target_url)
                    .headers(request_header.clone())
                    .timeout(request.timeout)
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(err) => {
                        error!(
                            "get request failed {} {} {}: {}",
                            request.task_id, request.piece_id, target_url, err
                        );

                        return Ok(super::GetResponse {
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

        Ok(super::GetResponse {
            success: response_status_code.is_success(),
            http_header: Some(response_header),
            http_status_code: Some(response_status_code),
            reader: response_reader,
            error_message: Some(response_status_code.to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        http::{HTTP, HTTPS_SCHEME, HTTP_SCHEME},
        Backend, GetRequest, HeadRequest,
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
MIIDsDCCApigAwIBAgIUWuckNOpaPERz+QMACyqCqFJwYIYwDQYJKoZIhvcNAQEL
BQAwYjELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0Jl
aWppbmcxEDAOBgNVBAoMB1Rlc3QgQ0ExCzAJBgNVBAsMAklUMRAwDgYDVQQDDAdU
ZXN0IENBMB4XDTI0MTAxMTEyMTEwN1oXDTI2MDIyMzEyMTEwN1owaDELMAkGA1UE
BhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0JlaWppbmcxFDASBgNV
BAoMC1Rlc3QgU2VydmVyMQswCQYDVQQLDAJJVDESMBAGA1UEAwwJbG9jYWxob3N0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAiA9wEge3Jq8qw8Ix9z6t
ss7ttK/49TMddhnQuqoYrFKjYliuvfbRZOU1nBP7+5XSAliPDCRNPS17JSwsXJk2
bstc69fruDpYmthualSTsUYSwJJqzJjy5mlwSPtBsombcSHrUasMce5C4iXJX8Wx
1O8ZCwuI5LUKxLujt+ZWnYfp5lzDcDhgD6wIzcMk67jv2edcWhqGkKmQbbmmK3Ve
DJRa56NCh0F2U1SW0KCXTzoC1YU/bbB4UCfvHouMzCRNTr3VcrfL5aBIn/z/f6Xt
atQkqFa/T1/lOQ0miMqNyBW58NxkPsTaJm2kVZ21hF2Dvo8MU/8Ras0J0aL8sc4n
LwIDAQABo1gwVjAUBgNVHREEDTALgglsb2NhbGhvc3QwHQYDVR0OBBYEFJP+jy8a
tCfnu6nekyZugvq8XT2gMB8GA1UdIwQYMBaAFOwXKq7J6STkwLUWC1xKwq1Psy63
MA0GCSqGSIb3DQEBCwUAA4IBAQCu8nqnuzNn3E9dNC8ptV7ga1zb7cGdL3ZT5W3d
10gmPo3YijWoCj4snattX9zxI8ThAY7uX6jrR0/HRXGJIw5JnlBmykdgyrQYEDzU
FUL0GGabJNxZ+zDV77P+3WdgCx3F7wLQk+x+etMPvYuWC8RMse7W6dB1INyMT/l6
k1rV73KTupSNJrYhqw0RnmNHIctkwiZLLpzLFj91BHjK5ero7VV4s7vnx+gtO/zQ
FnIyiyfYYcSpVMhhaNkeCtWOfgVYU/m4XXn5bwEOhMN6q0JcdBPnT6kd2otLhiIo
/WeyWEUeZ4rQhS7C1i31AYtNtVnnvI7BrsI4czYdcJcj3CM+
-----END CERTIFICATE-----
"""#;

    const SERVER_KEY: &str = r#"""
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCID3ASB7cmryrD
wjH3Pq2yzu20r/j1Mx12GdC6qhisUqNiWK699tFk5TWcE/v7ldICWI8MJE09LXsl
LCxcmTZuy1zr1+u4Olia2G5qVJOxRhLAkmrMmPLmaXBI+0GyiZtxIetRqwxx7kLi
JclfxbHU7xkLC4jktQrEu6O35ladh+nmXMNwOGAPrAjNwyTruO/Z51xaGoaQqZBt
uaYrdV4MlFrno0KHQXZTVJbQoJdPOgLVhT9tsHhQJ+8ei4zMJE1OvdVyt8vloEif
/P9/pe1q1CSoVr9PX+U5DSaIyo3IFbnw3GQ+xNombaRVnbWEXYO+jwxT/xFqzQnR
ovyxzicvAgMBAAECggEABqHVkTfe1p+PBGx34tG/4nQxwIRxLJG31no+jeAdYOLF
AEeulqezbmIroyTMA0uQKWscy0V/gXUi3avHOOktp72Vv9fxy98F/fyBPx3YEvLa
69DMnl0qPl06CvLlTey6km8RKxUrRq9S2NoTydD+m1fC9jCIhvHkrNExIXjtaewU
PvAHJy4ho+hVLo40udmQ4i1gnEWYUtjkr65ujuOAlWrlScHGvOrATbrfcaufPi/S
5A/h8UlfahBstmh3a2tBLZlNl82s5ZKsVM1Oq1Vk9hAX5DP2JBAmuZKgX/xSDdpR
62VUQGqp1WLgble5vR6ZUFo5+Jiw1uxe9jmNUg9mMQKBgQC8giG3DeeU6+rX9LVz
cklF4jioU5LMdYutwXbtuGIWgXeJo8r0fzrgBtBVGRn7anS7YnYA+67h+A8SC6MO
SXvktpHIC3Egge2Q9dRrWA4YCpkIxlOQ5ofCqovvCg9kq9sYqGz6lMr3RrzOWkUW
+0hF1CHCV0+KGFeIvTYVIKSsJwKBgQC4xiTsaShmwJ6HdR59jOmij+ccCPQTt2IO
eGcniY2cHIoX9I7nn7Yah6JbMT0c8j75KA+pfCrK3FpRNrb71cI1iqBHedZXpRaV
eshJztmw3AKtxQPNwRYrKYpY/M0ShAduppELeshZz1kubQU3sD4adrhcGCDXkctb
dP44IpipuQKBgC+W5q4Q65L0ECCe3aQciRUEbGtKVfgaAL5H5h9TeifWXXg5Coa5
DAL8lWG2aZHIKVoZHFNZNqhDeIKEv5BeytFNqfYHtXKQeoorFYpX+47kNgg6EWS2
XjWt2o/pSUOQA0rxUjnckHTmvcmWjnSj0XYXfMJUSndBd+/EXL/ussPnAoGAGE5Q
Wxz2KJYcBHuemCtqLG07nI988/8Ckh66ixPoIeoLLF2KUuPKg7Dl5ZMTk/Q13nar
oMLpqifUZayJ45TZ6EslDGH1lS/tSZqOME9aiY5Xd95bwrwsm17qiQwwOchOZfrZ
R6ZOJqpE8/t5XTr84GRPmiW+ZD0UgCJisqWyaVkCgYEAtupQDst0hmZ0KnJSIZ5U
R6skHABhmwNU5lOPUBIzHVorbAaKDKd4iFbBI5wnBuWxXY0SANl2HYX3gZaPccH4
wzvR3jZ1B4UlEBXl2V+VRbrXyPTN4uUF42AkSGuOsK4O878wW8noX+ZZTk7gydTN
Z+yQ5jhu/fmSBNhqO/8Lp+Y=
-----END PRIVATE KEY-----
"""#;

    const CA_CERT: &str = r#"""
-----BEGIN CERTIFICATE-----
MIIDpTCCAo2gAwIBAgIULqNbOr0fRj05VwIKlYdDt8HwxsUwDQYJKoZIhvcNAQEL
BQAwYjELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0Jl
aWppbmcxEDAOBgNVBAoMB1Rlc3QgQ0ExCzAJBgNVBAsMAklUMRAwDgYDVQQDDAdU
ZXN0IENBMB4XDTI0MTAxMTEyMTEwNloXDTI3MDgwMTEyMTEwNlowYjELMAkGA1UE
BhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0JlaWppbmcxEDAOBgNV
BAoMB1Rlc3QgQ0ExCzAJBgNVBAsMAklUMRAwDgYDVQQDDAdUZXN0IENBMIIBIjAN
BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvDQCTmptzEmjwAkk6vsnEbch0Gt+
Xp3bEEE1YhW89Jy6/bmclEINXsoRxpgkx4XnW0bcoDcqWBES82sFsQtEFWkP0Q3S
8CQtpymDIuSj63xSVJWG8/cobzwztJfVQjBJwfmdnamXcjtqGHaGo3RjaHurSBTT
Tft+gUvCuzFAblK+liQuQWRMq7JBwONgVzoMYoWSi+JJpEUcy/T+oznn9jNAW8Do
FnXi1xvbRv6JiGOsYH1t869j5R8BkpjyGlZ6RYfPhiKtTg4K/ufnkkKteHzGZfcV
HW2tqXyIkUl4j/+041nYtnyUuOZgLs2sJ33PER7GwVgi3sWG8AsNolRHUQIDAQAB
o1MwUTAdBgNVHQ4EFgQU7BcqrsnpJOTAtRYLXErCrU+zLrcwHwYDVR0jBBgwFoAU
7BcqrsnpJOTAtRYLXErCrU+zLrcwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0B
AQsFAAOCAQEADFoewfDAIqf8OAhFFcTYiTTu16sbTzZTzRfxSa0R0oOmSl8338If
71q8Yx65gFlu7FMiVRaVASzupwDhtLpqr6oVxLlmNW4fM0Bb+2CbmRuwhlm6ymBo
NXtRh5AkWAxHOp124Rmrr3WB9r+zvZ2kxuWPvN/cOq4H4VAp/F0cBtKPRDw/W0IQ
hDvG4OanBOKLE9Q7VH2kHXb6fJ4imKIztYcU4hOenKdUhfkCIBiIFgntUcEAaEpU
FnJ4fV4c4aJ+9D3VyPlrdiBqIPI0Wms9YqqG2b8EDid561Jj7paIR2wLn0/Gq61b
ePv3eLH0ZmBhSyl4+q/V56Z1TdZU46QZlg==
-----END CERTIFICATE-----
"""#;

    const WRONG_CA_CERT: &str = r#"""
-----BEGIN CERTIFICATE-----
MIIDqTCCApGgAwIBAgIUW+6n+025VMqvZd4wm+Xdfzu4o38wDQYJKoZIhvcNAQEL
BQAwZDELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0Jl
aWppbmcxETAPBgNVBAoMCFdyb25nIENBMQswCQYDVQQLDAJJVDERMA8GA1UEAwwI
V3JvbmcgQ0EwHhcNMjQxMDExMTIxMTA2WhcNMjcwODAxMTIxMTA2WjBkMQswCQYD
VQQGEwJDTjEQMA4GA1UECAwHQmVpamluZzEQMA4GA1UEBwwHQmVpamluZzERMA8G
A1UECgwIV3JvbmcgQ0ExCzAJBgNVBAsMAklUMREwDwYDVQQDDAhXcm9uZyBDQTCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALThl83CHSlT+xHONWqjOlsG
z+qeYcdZRxVJZQWJ9DrfTBcE64fqXnRIMesZbZNGi0d4XyfiJDB8AxVRAD/lVHQi
WR8LHglV/Hd7NjYG3bMQSkRHf5oleKjm1KDLvvnoD25YhqZsVDSCe+V4JkPc6xun
SGU/WJluyzy0j49KJXjKJTzpkFsvYF91s8oYMCjwVMuYxcZLA7OCUgb9phlfZBND
S9Dc5HI99O+0Uxfvfa/nRp85n2WpEJWQruGaazHFP/k842iR6zXIFclySE7n+1IG
SBLJqZ4IYfS0NisTEozD/LcuEJ87/PZ7ag0zFhu7MpnD55JeJP8cq8pISHj8gJcC
AwEAAaNTMFEwHQYDVR0OBBYEFLmV6Oqgwc1kIrv4JKLzn5qpKbvAMB8GA1UdIwQY
MBaAFLmV6Oqgwc1kIrv4JKLzn5qpKbvAMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZI
hvcNAQELBQADggEBAEJ+DbjdAZdJltIkHeIwFx9S4VnhA+Dw5+EBY03XzYo3HB/i
qSQTvYz4laZppierxuR8Z5O6DPOxNJ4pXhXDcn2e2TzlBq+P0fUE9z2w+QBQyTEl
6J2W5ce6dh9ke601pSMedLFDiARDGLkRDsIuEh91i62o+O3gNRkD/OWvjHAorQTf
BOP2lbcTYGg6wMPOUMBHg73E/pyXVXeN9x1qN7dCWN4zDwInII7iUA6BQ0zECJAD
sYhAYqHktkJsl0K4gJVanpnUhAC+SMD3+LRdjwMBp4mk+q3p2FMJMkACK3ffpn9j
TrIVG3cErZoBC6zqBs/Ibe9q3gdHGqS3QLAKy/k=
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
    async fn should_get_head_response() {
        let server = wiremock::MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/head"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/html; charset=UTF-8"),
            )
            .mount(&server)
            .await;

        let resp = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600))
            .unwrap()
            .head(HeadRequest {
                task_id: "test".to_string(),
                url: format!("{}/head", server.uri()),
                http_header: Some(HeaderMap::new()),
                timeout: std::time::Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK))
    }

    #[tokio::test]
    async fn should_return_error_response_when_head_notexists() {
        let server = wiremock::MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/head"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/html; charset=UTF-8"),
            )
            .mount(&server)
            .await;

        let resp = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600))
            .unwrap()
            .head(HeadRequest {
                task_id: "test".to_string(),
                url: format!("{}/head", server.uri()),
                http_header: None,
                timeout: std::time::Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
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

        let mut resp = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600))
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
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK));
        assert_eq!(resp.text().await.unwrap(), "OK");
    }

    #[tokio::test]
    async fn should_get_head_response_with_self_signed_cert() {
        let server_addr = start_https_server(SERVER_CERT, SERVER_KEY).await;
        let resp = HTTP::new(HTTPS_SCHEME, None, true, Duration::from_secs(600))
            .unwrap()
            .head(HeadRequest {
                task_id: "test".to_string(),
                url: server_addr,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: Some(load_certs_from_pem(CA_CERT).unwrap()),
                object_storage: None,
                hdfs: None,
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK));
    }

    #[tokio::test]
    async fn should_return_error_response_when_head_with_wrong_cert() {
        let server_addr = start_https_server(SERVER_CERT, SERVER_KEY).await;
        let resp = HTTP::new(HTTPS_SCHEME, None, true, Duration::from_secs(600))
            .unwrap()
            .head(HeadRequest {
                task_id: "test".to_string(),
                url: server_addr,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: Some(load_certs_from_pem(WRONG_CA_CERT).unwrap()),
                object_storage: None,
                hdfs: None,
            })
            .await;

        assert!(!resp.unwrap().success);
    }

    #[tokio::test]
    async fn should_get_response_with_self_signed_cert() {
        let server_addr = start_https_server(SERVER_CERT, SERVER_KEY).await;
        let mut resp = HTTP::new(HTTPS_SCHEME, None, true, Duration::from_secs(600))
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
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK));
        assert_eq!(resp.text().await.unwrap(), "OK");
    }

    #[tokio::test]
    async fn should_return_error_response_when_get_with_wrong_cert() {
        let server_addr = start_https_server(SERVER_CERT, SERVER_KEY).await;
        let resp = HTTP::new(HTTPS_SCHEME, None, true, Duration::from_secs(600))
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
            })
            .await;

        assert!(!resp.unwrap().success);
    }

    #[tokio::test]
    async fn should_get_head_response_with_no_verifier() {
        let server_addr = start_https_server(SERVER_CERT, SERVER_KEY).await;
        let resp = HTTP::new(HTTPS_SCHEME, None, true, Duration::from_secs(600))
            .unwrap()
            .head(HeadRequest {
                task_id: "test".to_string(),
                url: server_addr,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK));
    }

    #[tokio::test]
    async fn should_get_response_with_no_verifier() {
        let server_addr = start_https_server(SERVER_CERT, SERVER_KEY).await;
        let http_backend = HTTP::new(HTTPS_SCHEME, None, true, Duration::from_secs(600));
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
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK));
        assert_eq!(resp.text().await.unwrap(), "OK");
    }

    #[test]
    fn should_make_request_headers() {
        // Apply default user-agent when not specified.
        let http = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600)).unwrap();
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
        let backend = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(600)).unwrap();
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
                    .set_body_string("content")
                    .insert_header("Content-Type", "text/plain"),
            )
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/short-ttl"))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("Location", format!("{}/target", server.uri())),
            )
            .expect(2)
            .mount(&server)
            .await;

        // Use a very short TTL for this test (1 second).
        let backend = HTTP::new(HTTP_SCHEME, None, true, Duration::from_secs(1)).unwrap();

        // First request - should store redirect url.
        let mut response = backend
            .get(GetRequest {
                task_id: "test".to_string(),
                piece_id: "p1".to_string(),
                url: format!("{}/short-ttl", server.uri()),
                range: None,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
            })
            .await
            .unwrap();
        assert_eq!(response.http_status_code, Some(StatusCode::OK));
        assert_eq!(response.text().await.unwrap(), "content");

        // Wait for cache to expire.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Second request after TTL expiry - should store redirect url again.
        let mut response = backend
            .get(GetRequest {
                task_id: "test".to_string(),
                piece_id: "p2".to_string(),
                url: format!("{}/short-ttl", server.uri()),
                range: None,
                http_header: Some(HeaderMap::new()),
                timeout: Duration::from_secs(5),
                client_cert: None,
                object_storage: None,
                hdfs: None,
            })
            .await
            .unwrap();
        assert_eq!(response.http_status_code, Some(StatusCode::OK));
        assert_eq!(response.text().await.unwrap(), "content");
    }
}
