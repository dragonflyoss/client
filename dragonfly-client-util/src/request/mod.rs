/*
 *     Copyright 2025 The Dragonfly Authors
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

mod errors;

use crate::http::headermap_to_hashmap;
use crate::id_generator::{IDGenerator, TaskIDParameter};
use crate::selector::{SeedPeerSelector, Selector};
use bytes::BytesMut;
use dashmap::DashMap;
use dragonfly_client_core::error::DFError;
use errors::{BackendError, DfdaemonError, Error, ProxyError};
use futures::TryStreamExt;
use hostname;
use local_ip_address::local_ip;
use reqwest::{header::HeaderMap, redirect::Policy, Client};
use rustls_pki_types::CertificateDer;
use std::io::{Error as IOError, ErrorKind};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio::time::Instant;
use tokio_util::io::StreamReader;
use tonic::transport::Endpoint;
use tracing::error;

/// POOL_MAX_IDLE_PER_HOST is the max idle connections per host.
const POOL_MAX_IDLE_PER_HOST: usize = 1024;

/// KEEP_ALIVE_INTERVAL is the keep alive interval for TCP connection.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// DEFAULT_RETRY_TIMES is the default number of times to retry a request.
const DEFAULT_RETRY_TIMES: u8 = 1;

/// RETRY_TIMES_RANGE is the range of retry times.
const RETRY_TIMES_RANGE: RangeInclusive<u8> = 1..=3;

/// DEFAULT_CLIENT_POOL_IDLE_TIMEOUT is the default idle timeout(30 minutes) for clients in the pool.
const DEFAULT_CLIENT_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// DEFAULT_CLIENT_POOL_CLEANUP_INTERVAL is the default cleanup interval(10 minutes) for the client pool.
const DEFAULT_CLIENT_POOL_CLEANUP_INTERVAL: Duration = Duration::from_secs(10 * 60);

pub type Result<T> = std::result::Result<T, Error>;

pub type Body = Box<dyn AsyncRead + Send + Unpin>;

#[tonic::async_trait]
pub trait Request {
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>>;
    async fn get_into(&self, request: GetRequest, buf: &mut BytesMut) -> Result<GetResponse>;
}

pub struct Proxy {
    /// seed_peer_selector is the selector service for selecting seed peers.
    seed_peer_selector: Arc<SeedPeerSelector>,

    /// retry_times is the number of times to retry a request.
    retry_times: u8,

    /// client_pool is the pool of clients.
    client_pool: ClientPool,

    /// id_generator is the task id generator.
    id_generator: Arc<IDGenerator>,
}

pub struct GetRequest {
    /// url is the url of the request.
    pub url: String,

    /// header is the headers of the request.
    pub header: Option<HeaderMap>,

    /// Task piece length.
    pub piece_length: Option<u64>,

    /// URL tag identifies different task for same url.
    pub tag: Option<String>,

    /// Application of task identifies different task for same url.
    pub application: Option<String>,

    /// Filtered query params to generate the task id.
    /// When filter is ["Signature", "Expires", "ns"], for example:
    /// http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io and http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io
    /// will generate the same task id.
    /// Default value includes the filtered query params of s3, gcs, oss, obs, cos.
    pub filtered_query_params: Vec<String>,

    /// content_for_calculating_task_id is the content used to calculate the task id.
    /// If content_for_calculating_task_id is set, use its value to calculate the task ID.
    /// Otherwise, calculate the task ID based on url, piece_length, tag, application, and filtered_query_params.
    pub content_for_calculating_task_id: Option<String>,

    /// Refer to https://github.com/dragonflyoss/api/blob/main/proto/common.proto#L67
    pub priority: Option<i32>,

    /// timeout is the timeout of the request.
    pub timeout: Duration,

    /// client_cert is the client certificates for the request.
    pub client_cert: Option<Vec<CertificateDer<'static>>>,

    /// disable_proxy is the flag to disable the request pass via proxy.
    pub disable_proxy: bool,
}

pub struct GetResponse<R = Body>
where
    R: AsyncRead + Unpin,
{
    /// success is the success of the response.
    pub success: bool,

    /// header is the headers of the response.
    pub header: Option<HeaderMap>,

    /// status_code is the status code of the response.
    pub status_code: Option<reqwest::StatusCode>,

    /// body is the content of the response.
    pub reader: Option<R>,
}

struct ClientEntry {
    /// client is the reqwest client.
    client: Client,

    /// last_used is the last time the client was used.
    last_used: Instant,
}

struct ClientPool {
    /// clients is the map of clients, keyed by the proxy server address.
    clients: Arc<DashMap<String, ClientEntry>>,
}

impl ClientPool {
    // new creates a new client pool with a background cleanup task.
    fn new(idle_timeout: Duration, cleanup_interval: Duration) -> Self {
        let clients = Arc::new(DashMap::new());
        let clients_clone = clients.clone();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(cleanup_interval).await;
                clients_clone.retain(|_, client: &mut ClientEntry| {
                    client.last_used.elapsed() < idle_timeout
                });
            }
        });

        Self { clients }
    }

    // get_or_create gets a client from the pool or creates a new one.
    fn get_or_create(&self, proxy_addr: &str) -> Result<Client> {
        if let Some(mut client_entry) = self.clients.get_mut(proxy_addr) {
            let client_ref = &mut *client_entry;
            client_ref.last_used = Instant::now();
            return Ok(client_ref.client.clone());
        }

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
        let mut client_builder = Client::builder()
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .redirect(Policy::none())
            .hickory_dns(true)
            .danger_accept_invalid_certs(true)
            .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
            .tcp_keepalive(KEEP_ALIVE_INTERVAL);

        // Add the proxy configuration if provided.
        if !proxy_addr.is_empty() {
            let proxy = reqwest::Proxy::all(proxy_addr)
                .map_err(|err| DFError::Unknown(format!("failed to create proxy: {:?}", err)))?;
            client_builder = client_builder.proxy(proxy);
        }

        let client = client_builder
            .build()
            .map_err(|err| DFError::Unknown(format!("failed to build client: {:?}", err)))?;

        self.clients.insert(
            proxy_addr.to_string(),
            ClientEntry {
                client: client.clone(),
                last_used: Instant::now(),
            },
        );

        Ok(client)
    }
}

impl Proxy {
    /// new creates a new proxy.
    pub async fn new(
        scheduler_service_endpoint: String,
        seed_peers_healthcheck_interval: Option<Duration>,
        retry_times: Option<u8>,
    ) -> Result<Self> {
        // Validate scheduler_service_endpoint.
        if scheduler_service_endpoint.is_empty() {
            return Err(DFError::ValidationError(
                "scheduler_service_endpoint is empty".to_string(),
            )
            .into());
        }

        // Use default retry_times if not provided.
        let retry_times = retry_times.unwrap_or(DEFAULT_RETRY_TIMES);

        // Validate retry_times.
        if !RETRY_TIMES_RANGE.contains(&retry_times) {
            return Err(DFError::ValidationError(format!(
                "retry_times is not in range {:?}",
                RETRY_TIMES_RANGE
            ))
            .into());
        }

        // Create the scheduler channel.
        let scheduler_channel = Endpoint::from_shared(scheduler_service_endpoint.clone())?
            .connect()
            .await?;

        // Create seed peer selector.
        let seed_peer_selector =
            match SeedPeerSelector::new(scheduler_channel, seed_peers_healthcheck_interval) {
                Ok(selector) => Arc::new(selector),
                Err(err) => {
                    return Err(DFError::Unknown(format!(
                        "failed to create seed peer selector: {:?}",
                        err
                    ))
                    .into())
                }
            };

        let seed_peer_selector_clone = Arc::clone(&seed_peer_selector);
        tokio::spawn(async move {
            // Run the selector service in the background to refresh the seed peers periodically.
            seed_peer_selector_clone.run().await;
        });

        // Get local IP address and hostname.
        let local_ip = match local_ip() {
            Ok(ip) => ip.to_string(),
            Err(err) => {
                return Err(
                    DFError::Unknown(format!("failed to get local IP address: {:?}", err)).into(),
                )
            }
        };

        let is_seed_peer = true;
        let hostname = hostname::get().unwrap().to_string_lossy().to_string();
        let id_generator = IDGenerator::new(local_ip, hostname, is_seed_peer);

        let proxy = Self {
            seed_peer_selector,
            retry_times,
            client_pool: ClientPool::new(
                DEFAULT_CLIENT_POOL_IDLE_TIMEOUT,
                DEFAULT_CLIENT_POOL_CLEANUP_INTERVAL,
            ),
            id_generator: Arc::new(id_generator),
        };

        Ok(proxy)
    }
}

#[tonic::async_trait]
impl Request for Proxy {
    /// Performs a GET request, handling custom errors and returning a streaming response.
    async fn get(&self, request: GetRequest) -> Result<GetResponse> {
        // 1. Delegate the request sending and header-based error handling to the helper.
        let response = self.dispatch(&request).await?;

        // 2. Process the successful response body by creating a stream.
        let status = response.status();
        let headers = response.headers().clone();
        Ok(GetResponse {
            success: status.is_success(),
            header: Some(headers),
            status_code: Some(status),
            reader: status.is_success().then(|| {
                Box::new(StreamReader::new(
                    response
                        .bytes_stream()
                        .map_err(|err| IOError::new(ErrorKind::Other, err)),
                )) as _
            }),
        })
    }

    /// Performs a GET request, handling custom errors and collecting the response into a buffer.
    async fn get_into(&self, request: GetRequest, buf: &mut BytesMut) -> Result<GetResponse> {
        // 1. Delegate the request sending and header-based error handling to the helper.
        let response = self.dispatch(&request).await?;

        // 2. Process the successful response body by buffering it.
        let status = response.status();
        let headers = response.headers().clone();

        if status.is_success() {
            let bytes = response.bytes().await.map_err(|err| {
                DFError::Unknown(format!("failed to read response bytes: {:?}", err))
            })?;
            buf.extend_from_slice(&bytes);
        }

        Ok(GetResponse {
            success: status.is_success(),
            header: Some(headers),
            status_code: Some(status),
            reader: None,
        })
    }
}

impl Proxy {
    /// Creates a reqwest client with proxy configuration for the given request.
    async fn create_client(&self, request: &GetRequest) -> Result<Client> {
        // If the request is disabled via proxy, get or create a client without proxy.
        if request.disable_proxy {
            match self.client_pool.get_or_create("") {
                Ok(client) => return Ok(client),
                Err(err) => {
                    return Err(
                        DFError::Unknown(format!("failed to create client: {:?}", err)).into(),
                    )
                }
            };
        };

        // The request should be processed by the proxy, so generate the task ID to select a seed peer as proxy server.
        let task_id = self
            .id_generator
            .task_id(TaskIDParameter::URLBased {
                url: request.url.clone(),
                piece_length: request.piece_length,
                tag: request.tag.clone(),
                application: request.application.clone(),
                filtered_query_params: request.filtered_query_params.clone(),
            })
            .map_err(|err| DFError::Unknown(format!("failed to generate task ID: {:?}", err)))?;

        // Calculate total attempts (initial try + retries).
        let replicas = (self.retry_times + 1) as u32;

        // Select seed peers.
        let seed_peers = self
            .seed_peer_selector
            .select(task_id, replicas)
            .await
            .map_err(|err| DFError::Unknown(format!("failed to select seed peers: {:?}", err)))?;

        // Try each peer until success or exhaustion.
        for peer in seed_peers.iter() {
            let proxy_addr = format!("http://{}:{}", peer.ip, peer.proxy_port);
            match self.client_pool.get_or_create(&proxy_addr) {
                Ok(client) => return Ok(client),
                Err(err) => {
                    error!(
                        "failed to build client for peer {}:{}: {}",
                        peer.ip, peer.port, err
                    );
                    continue;
                }
            }
        }

        Err(DFError::Unknown("no valid seed client available".to_string()).into())
    }

    /// Private helper to dispatch requests and handle response headers.
    async fn dispatch(&self, request: &GetRequest) -> Result<reqwest::Response> {
        // Create client and send the request.
        let client = self.create_client(request).await?;
        let headers = request.header.clone().unwrap_or_default();
        let response = client
            .get(&request.url)
            .headers(headers)
            .timeout(request.timeout)
            .send()
            .await
            .map_err(|err| {
                error!("failed to send request: {}", err);
                DFError::Unknown(format!("failed to send request: {:?}", err))
            })?;

        // Check for custom Dragonfly error headers.
        if let Some(error_type) = response
            .headers()
            .get("X-Dragonfly-Error-Type")
            .and_then(|value| value.to_str().ok())
        {
            // If a known error type is found, consume the response body
            // to get the message and return a specific error.
            let status = response.status();
            let headers = response.headers().clone();

            return match error_type {
                "backend" => Err(Error::BackendError(BackendError {
                    message: response.text().await.ok(),
                    header: headermap_to_hashmap(&headers),
                    status_code: Some(status),
                })),
                "proxy" => Err(Error::ProxyError(ProxyError {
                    message: response.text().await.ok(),
                    header: headermap_to_hashmap(&headers),
                    status_code: Some(status),
                })),
                "dfdaemon" => Err(Error::DfdaemonError(DfdaemonError {
                    message: response.text().await.ok(),
                })),
                // If the header value is unknown, fall through and treat it as a success.
                _ => Ok(response),
            };
        }

        // No custom error header, return the response for further processing.
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use dragonfly_client_core::error::DFError;
    use mockito::ServerGuard;
    use reqwest::StatusCode;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    // Mock scheduler service for testing.
    async fn setup_mock_scheduler() -> ServerGuard {
        let server = mockito::Server::new_async().await;
        server
    }

    fn create_test_request() -> GetRequest {
        GetRequest {
            url: "http://example.com/test".to_string(),
            header: None,
            piece_length: Some(1024),
            tag: Some("test".to_string()),
            application: Some("test-app".to_string()),
            filtered_query_params: vec!["signature".to_string()],
            content_for_calculating_task_id: None,
            priority: Some(1),
            timeout: Duration::from_secs(30),
            client_cert: None,
            disable_proxy: false,
        }
    }

    #[tokio::test]
    async fn test_proxy_new_success() {
        let mock_server = setup_mock_scheduler().await;
        let scheduler_endpoint = mock_server.url();

        let result = Proxy::new(scheduler_endpoint, Some(Duration::from_secs(60)), Some(2)).await;

        assert!(result.is_ok());
        let proxy = result.unwrap();
        assert_eq!(proxy.retry_times, 2);
    }

    #[tokio::test]
    async fn test_proxy_new_empty_endpoint() {
        let result = Proxy::new("".to_string(), None, None).await;

        assert!(result.is_err());
        if let Err(Error::Base(DFError::ValidationError(msg))) = result {
            assert_eq!(msg, "scheduler_service_endpoint is empty");
        } else {
            panic!("Expected ValidationError");
        }
    }

    #[tokio::test]
    async fn test_proxy_new_invalid_retry_times() {
        let mock_server = setup_mock_scheduler().await;
        let scheduler_endpoint = mock_server.url();

        let result = Proxy::new(scheduler_endpoint, None, Some(5)).await;

        assert!(result.is_err());
        if let Err(Error::Base(DFError::ValidationError(msg))) = result {
            assert!(msg.contains("retry_times is not in range"));
        } else {
            panic!("Expected ValidationError for retry_times");
        }
    }

    #[tokio::test]
    async fn test_client_pool_get_or_create() {
        let pool = ClientPool::new(Duration::from_secs(60), Duration::from_secs(10));

        // Initially, the pool is empty.
        assert_eq!(pool.clients.len(), 0);

        // Get a client for the first time. A new client should be created.
        let _client1 = pool.get_or_create("http://proxy1.com").unwrap();
        assert_eq!(pool.clients.len(), 1);

        // Get a client for the same proxy again. It should be reused, so the count remains 1.
        let _client2 = pool.get_or_create("http://proxy1.com").unwrap();
        assert_eq!(pool.clients.len(), 1);

        // Get a client for a different proxy. A new client should be created, so the count becomes 2.
        let _client3 = pool.get_or_create("http://proxy2.com").unwrap();
        assert_eq!(pool.clients.len(), 2);
    }

    #[tokio::test]
    async fn test_client_pool_cleanup() {
        let pool = ClientPool::new(Duration::from_millis(10), Duration::from_millis(20));

        // Create a client.
        pool.get_or_create("http://proxy1.com").unwrap();
        assert_eq!(pool.clients.len(), 1);

        // Wait for cleanup.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Client should be cleaned up.
        assert_eq!(pool.clients.len(), 0);
    }

    #[tokio::test]
    async fn test_get_request_success() {
        let mut mock_server = mockito::Server::new_async().await;

        // Mock successful response.
        let mock = mock_server
            .mock("GET", "/test")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_body("test response")
            .create_async()
            .await;

        let proxy = create_test_proxy().await;
        let mut request = create_test_request();
        request.url = format!("{}/test", mock_server.url());
        // Disable proxy for direct connection.
        request.disable_proxy = true;

        let response = proxy.get(request).await.unwrap();

        assert!(response.success);
        assert!(response.reader.is_some());
        assert_eq!(response.status_code.unwrap(), StatusCode::OK);

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_into_success() {
        let mut mock_server = mockito::Server::new_async().await;

        let mock = mock_server
            .mock("GET", "/test")
            .with_status(200)
            .with_body("test response")
            .create_async()
            .await;

        let proxy = create_test_proxy().await;
        let mut request = create_test_request();
        request.url = format!("{}/test", mock_server.url());
        request.disable_proxy = true;

        let mut buf = BytesMut::new();
        let response = proxy.get_into(request, &mut buf).await.unwrap();

        assert!(response.success);
        assert_eq!(buf, b"test response"[..]);
        assert_eq!(response.status_code.unwrap(), StatusCode::OK);

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_backend_error() {
        let mut mock_server = mockito::Server::new_async().await;

        let mock = mock_server
            .mock("GET", "/test")
            .with_status(500)
            .with_header("X-Dragonfly-Error-Type", "backend")
            .with_body("Backend error occurred")
            .create_async()
            .await;

        let proxy = create_test_proxy().await;
        let mut request = create_test_request();
        request.url = format!("{}/test", mock_server.url());
        request.disable_proxy = true;

        let result = proxy.get(request).await;

        assert!(result.is_err());
        if let Err(Error::BackendError(err)) = result {
            assert_eq!(err.message, Some("Backend error occurred".to_string()));
            assert_eq!(err.status_code, Some(StatusCode::INTERNAL_SERVER_ERROR));
        } else {
            panic!("Expected BackendError");
        }

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_proxy_error() {
        let mut mock_server = mockito::Server::new_async().await;

        let mock = mock_server
            .mock("GET", "/test")
            .with_status(502)
            .with_header("X-Dragonfly-Error-Type", "proxy")
            .with_body("Proxy error occurred")
            .create_async()
            .await;

        let proxy = create_test_proxy().await;
        let mut request = create_test_request();
        request.url = format!("{}/test", mock_server.url());
        request.disable_proxy = true;

        let result = proxy.get(request).await;

        assert!(result.is_err());
        if let Err(Error::ProxyError(err)) = result {
            assert_eq!(err.message, Some("Proxy error occurred".to_string()));
            assert_eq!(err.status_code, Some(StatusCode::BAD_GATEWAY));
        } else {
            panic!("Expected ProxyError");
        }

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_dfdaemon_error() {
        let mut mock_server = mockito::Server::new_async().await;

        let mock = mock_server
            .mock("GET", "/test")
            .with_status(503)
            .with_header("X-Dragonfly-Error-Type", "dfdaemon")
            .with_body("Dfdaemon error occurred")
            .create_async()
            .await;

        let proxy = create_test_proxy().await;
        let mut request = create_test_request();
        request.url = format!("{}/test", mock_server.url());
        request.disable_proxy = true;

        let result = proxy.get(request).await;

        assert!(result.is_err());
        if let Err(Error::DfdaemonError(err)) = result {
            assert_eq!(err.message, Some("Dfdaemon error occurred".to_string()));
        } else {
            panic!("Expected DfdaemonError");
        }

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_unknown_error_type() {
        let mut mock_server = mockito::Server::new_async().await;

        let mock = mock_server
            .mock("GET", "/test")
            .with_status(500)
            .with_header("X-Dragonfly-Error-Type", "unknown")
            .with_body("Unknown error")
            .create_async()
            .await;

        let proxy = create_test_proxy().await;
        let mut request = create_test_request();
        request.url = format!("{}/test", mock_server.url());
        request.disable_proxy = true;

        let response = proxy.get(request).await.unwrap();

        // Should treat as normal response since error type is unknown.
        assert!(!response.success);
        assert_eq!(
            response.status_code.unwrap(),
            StatusCode::INTERNAL_SERVER_ERROR
        );

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_create_client_disabled_proxy() {
        let proxy = create_test_proxy().await;
        let mut request = create_test_request();
        request.disable_proxy = true;

        let result = proxy.create_client(&request).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_client_with_proxy() {
        let proxy = create_test_proxy().await;
        let request = create_test_request();

        // This might fail due to no actual seed peers, but we test the code path.
        let _ = proxy.create_client(&request).await;
    }

    #[tokio::test]
    async fn test_get_request_timeout() {
        let proxy = create_test_proxy().await;
        let mut request = create_test_request();
        request.url = "http://1.2.3.4:12345/timeout".to_string();
        request.timeout = Duration::from_millis(100);
        request.disable_proxy = true;

        let result = proxy.get(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_request_with_headers() {
        let mut mock_server = mockito::Server::new_async().await;

        let mock = mock_server
            .mock("GET", "/test")
            .match_header("Authorization", "Bearer token")
            .with_status(200)
            .with_body("authorized")
            .create_async()
            .await;

        let proxy = create_test_proxy().await;
        let mut request = create_test_request();
        request.url = format!("{}/test", mock_server.url());
        request.disable_proxy = true;

        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Bearer token".parse().unwrap());
        request.header = Some(headers);

        let response = proxy.get(request).await.unwrap();
        assert!(response.success);

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_response_stream_reading() {
        let mut mock_server = mockito::Server::new_async().await;

        let mock = mock_server
            .mock("GET", "/stream")
            .with_status(200)
            .with_body("streaming data")
            .create_async()
            .await;

        let proxy = create_test_proxy().await;
        let mut request = create_test_request();
        request.url = format!("{}/stream", mock_server.url());
        request.disable_proxy = true;

        let response = proxy.get(request).await.unwrap();
        assert!(response.success);

        if let Some(mut reader) = response.reader {
            let mut buf = vec![0u8; 1024];
            let n = reader.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"streaming data");
        } else {
            panic!("Expected reader to be present");
        }

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_request_various_status_codes() {
        let test_cases = vec![
            (200, true),
            (201, true),
            (400, false),
            (404, false),
            (500, false),
        ];

        for (status_code, expected_success) in test_cases {
            let mut mock_server = mockito::Server::new_async().await;

            let mock = mock_server
                .mock("GET", "/test")
                .with_status(status_code)
                .with_body("response")
                .create_async()
                .await;

            let proxy = create_test_proxy().await;
            let mut request = create_test_request();
            request.url = format!("{}/test", mock_server.url());
            request.disable_proxy = true;

            let response = proxy.get(request).await.unwrap();
            assert_eq!(response.success, expected_success);
            assert_eq!(response.status_code.unwrap().as_u16(), status_code as u16);

            mock.assert_async().await;
        }
    }

    // Helper function to create a test proxy.
    async fn create_test_proxy() -> Proxy {
        let mock_server = setup_mock_scheduler().await;
        let scheduler_endpoint = mock_server.url();

        Proxy::new(scheduler_endpoint, None, None).await.unwrap()
    }

    #[tokio::test]
    async fn test_get_response_without_reader_for_error_status() {
        let mut mock_server = mockito::Server::new_async().await;

        let mock = mock_server
            .mock("GET", "/test")
            .with_status(404)
            .with_body("Not found")
            .create_async()
            .await;

        let proxy = create_test_proxy().await;
        let mut request = create_test_request();
        request.url = format!("{}/test", mock_server.url());
        request.disable_proxy = true;

        let response = proxy.get(request).await.unwrap();

        assert!(!response.success);
        assert!(response.reader.is_none());
        assert_eq!(response.status_code.unwrap(), StatusCode::NOT_FOUND);

        mock.assert_async().await;
    }
}
