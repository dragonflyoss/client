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

pub mod errors;
mod selector;

use crate::http::{headermap_to_hashmap, query_params::default_proxy_rule_filtered_query_params};
use crate::id_generator::{IDGenerator, TaskIDParameter};
use crate::pool::{Builder as PoolBuilder, Entry, Factory, Pool};
use bytes::BytesMut;
use dragonfly_api::scheduler::v2::scheduler_client::SchedulerClient;
use errors::{BackendError, DfdaemonError, Error, ProxyError};
use futures::TryStreamExt;
use hostname;
use local_ip_address::local_ip;
use reqwest::{header::HeaderMap, header::HeaderValue, Client};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_tracing::TracingMiddleware;
use rustix::path::Arg;
use rustls_pki_types::CertificateDer;
use selector::{SeedPeerSelector, Selector};
use std::io::{Error as IOError, ErrorKind};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;
use tonic::transport::Endpoint;
use tracing::{debug, error};

/// POOL_MAX_IDLE_PER_HOST is the max idle connections per host.
const POOL_MAX_IDLE_PER_HOST: usize = 1024;

/// KEEP_ALIVE_INTERVAL is the keep alive interval for TCP connection.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// DEFAULT_CLIENT_POOL_IDLE_TIMEOUT is the default idle timeout(30 minutes) for clients in the pool.
const DEFAULT_CLIENT_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// DEFAULT_CLIENT_POOL_CAPACITY is the default capacity of the client pool.
const DEFAULT_CLIENT_POOL_CAPACITY: usize = 128;

/// DEFAULT_SCHEDULER_REQUEST_TIMEOUT is the default timeout(5 seconds) for requests to the
/// scheduler service.
const DEFAULT_SCHEDULER_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Result is a specialized Result type for the proxy module.
pub type Result<T> = std::result::Result<T, Error>;

/// Body is the type alias for the response body reader.
pub type Body = Box<dyn AsyncRead + Send + Unpin>;

/// Defines the interface for sending requests via the Dragonfly.
///
/// This trait enables interaction with remote servers through the Dragonfly, providing methods
/// for performing GET requests with flexible response handling. It is designed for clients that
/// need to communicate with Dragonfly seed client efficiently, supporting both streaming and buffered
/// response processing. The trait shields the complex request logic between the client and the
/// Dragonfly seed client's proxy, abstracting the underlying communication details to simplify
/// client implementation and usage.
#[tonic::async_trait]
pub trait Request {
    /// Sends an GET request to a remote server via the Dragonfly and returns a response
    /// with a streaming body.
    ///
    /// This method is designed for scenarios where the response body is expected to be processed as a
    /// stream, allowing efficient handling of large or continuous data. The response includes metadata
    /// such as status codes and headers, along with a streaming `Body` for accessing the response content.
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>>;

    /// Sends an GET request to a remote server via the Dragonfly and writes the response
    /// body directly into the provided buffer.
    ///
    /// This method is optimized for scenarios where the response body needs to be stored directly in
    /// memory, avoiding the overhead of streaming for smaller or fixed-size responses. The provided
    /// `BytesMut` buffer is used to store the response content, and the response metadata (e.g., status
    /// and headers) is returned separately.
    async fn get_into(&self, request: GetRequest, buf: &mut BytesMut) -> Result<GetResponse>;
}

/// GetRequest represents a GET request to be sent via the Dragonfly.
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
}

/// GetResponse represents a GET response received via the Dragonfly.
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

/// Factory for creating HTTPClient instances.
#[derive(Debug, Clone, Default)]
struct HTTPClientFactory {}

/// HTTPClientFactory implements Factory for creating reqwest::Client instances with proxy support.
#[tonic::async_trait]
impl Factory<String, ClientWithMiddleware> for HTTPClientFactory {
    type Error = Error;

    /// Creates a new reqwest::Client configured to use the specified proxy address.
    async fn make_client(&self, proxy_addr: &String) -> Result<ClientWithMiddleware> {
        // TODO(chlins): Support client certificates and set `danger_accept_invalid_certs`
        // based on the certificates.
        let client = Client::builder()
            .hickory_dns(true)
            .danger_accept_invalid_certs(true)
            .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
            .tcp_keepalive(KEEP_ALIVE_INTERVAL)
            .proxy(reqwest::Proxy::all(proxy_addr).map_err(|err| {
                Error::Internal(format!("failed to set proxy {}: {}", proxy_addr, err))
            })?)
            .build()
            .map_err(|err| Error::Internal(format!("failed to build reqwest client: {}", err)))?;

        Ok(ClientBuilder::new(client)
            .with(TracingMiddleware::default())
            .build())
    }
}

/// Builder is the builder for Proxy.
pub struct Builder {
    /// scheduler_endpoint is the endpoint of the scheduler service.
    scheduler_endpoint: String,

    /// scheduler_request_timeout is the timeout of the request to the scheduler service.
    scheduler_request_timeout: Duration,

    /// health_check_interval is the interval of health check for selector(seed peers).
    health_check_interval: Duration,

    /// max_retries is the number of times to retry a request.
    max_retries: u8,
}

/// Builder implements Default trait.
impl Default for Builder {
    /// default returns a default Builder.
    fn default() -> Self {
        Self {
            scheduler_endpoint: "".to_string(),
            scheduler_request_timeout: DEFAULT_SCHEDULER_REQUEST_TIMEOUT,
            health_check_interval: Duration::from_secs(60),
            max_retries: 1,
        }
    }
}

/// Builder implements the builder pattern for Proxy.
impl Builder {
    /// Sets the scheduler endpoint.
    pub fn scheduler_endpoint(mut self, endpoint: String) -> Self {
        self.scheduler_endpoint = endpoint;
        self
    }

    /// Sets the scheduler request timeout.
    pub fn scheduler_request_timeout(mut self, timeout: Duration) -> Self {
        self.scheduler_request_timeout = timeout;
        self
    }

    /// Sets the health check interval.
    pub fn health_check_interval(mut self, interval: Duration) -> Self {
        self.health_check_interval = interval;
        self
    }

    /// Sets the maximum number of retries.
    pub fn max_retries(mut self, retries: u8) -> Self {
        self.max_retries = retries;
        self
    }

    /// Builds and returns a Proxy instance.
    pub async fn build(self) -> Result<Proxy> {
        // Validate input parameters.
        self.validate()?;

        // Create the scheduler channel.
        let scheduler_channel = Endpoint::from_shared(self.scheduler_endpoint.to_string())
            .map_err(|err| Error::InvalidArgument(err.to_string()))?
            .connect_timeout(self.scheduler_request_timeout)
            .timeout(self.scheduler_request_timeout)
            .connect()
            .await
            .map_err(|err| {
                Error::Internal(format!(
                    "failed to connect to scheduler {}: {}",
                    self.scheduler_endpoint, err
                ))
            })?;

        // Create scheduler client.
        let scheduler_client = SchedulerClient::new(scheduler_channel);

        // Create seed peer selector.
        let seed_peer_selector = Arc::new(
            SeedPeerSelector::new(scheduler_client, self.health_check_interval)
                .await
                .map_err(|err| {
                    Error::Internal(format!("failed to create seed peer selector: {}", err))
                })?,
        );

        let seed_peer_selector_clone = seed_peer_selector.clone();
        tokio::spawn(async move {
            // Run the selector service in the background to refresh the seed peers periodically.
            seed_peer_selector_clone.run().await;
        });

        // Get local IP address and hostname.
        let local_ip = local_ip().unwrap().to_string();
        let hostname = hostname::get().unwrap().to_string_lossy().to_string();
        let id_generator = IDGenerator::new(local_ip, hostname, true);
        let proxy = Proxy {
            seed_peer_selector,
            max_retries: self.max_retries,
            client_pool: PoolBuilder::new(HTTPClientFactory::default())
                .capacity(DEFAULT_CLIENT_POOL_CAPACITY)
                .idle_timeout(DEFAULT_CLIENT_POOL_IDLE_TIMEOUT)
                .build(),
            id_generator: Arc::new(id_generator),
        };

        Ok(proxy)
    }

    /// validate validates the input parameters.
    fn validate(&self) -> Result<()> {
        if let Err(err) = url::Url::parse(&self.scheduler_endpoint) {
            return Err(Error::InvalidArgument(err.to_string()));
        };

        if self.scheduler_request_timeout.as_millis() < 100 {
            return Err(Error::InvalidArgument(
                "scheduler request timeout must be at least 100 milliseconds".to_string(),
            ));
        }

        if self.health_check_interval.as_secs() < 1 || self.health_check_interval.as_secs() > 600 {
            return Err(Error::InvalidArgument(
                "health check interval must be between 1 and 600 seconds".to_string(),
            ));
        }

        if self.max_retries > 10 {
            return Err(Error::InvalidArgument(
                "max retries must be between 0 and 10".to_string(),
            ));
        }

        Ok(())
    }
}

/// Proxy is the HTTP proxy client that sends requests via Dragonfly.
pub struct Proxy {
    /// seed_peer_selector is the selector service for selecting seed peers.
    seed_peer_selector: Arc<SeedPeerSelector>,

    /// max_retries is the number of times to retry a request.
    max_retries: u8,

    /// client_pool is the pool of clients.
    client_pool: Pool<String, String, ClientWithMiddleware, HTTPClientFactory>,

    /// id_generator is the task id generator.
    id_generator: Arc<IDGenerator>,
}

/// Proxy implements the proxy client that sends requests via Dragonfly.
impl Proxy {
    /// builder returns a new Builder for Proxy.
    pub fn builder() -> Builder {
        Builder::default()
    }
}

/// Implements the interface for sending requests via the Dragonfly.
///
/// This struct enables interaction with remote servers through the Dragonfly, providing methods
/// for performing GET requests with flexible response handling. It is designed for clients that
/// need to communicate with Dragonfly seed client efficiently, supporting both streaming and buffered
/// response processing. The trait shields the complex request logic between the client and the
/// Dragonfly seed client's proxy, abstracting the underlying communication details to simplify
/// client implementation and usage.
#[tonic::async_trait]
impl Request for Proxy {
    /// Sends an GET request to a remote server via the Dragonfly and returns a response
    /// with a streaming body.
    ///
    /// This method is designed for scenarios where the response body is expected to be processed as a
    /// stream, allowing efficient handling of large or continuous data. The response includes metadata
    /// such as status codes and headers, along with a streaming `Body` for accessing the response content.
    async fn get(&self, request: GetRequest) -> Result<GetResponse> {
        let response = self.try_send(&request).await?;
        let header = response.headers().clone();
        let status_code = response.status();
        let reader = Box::new(StreamReader::new(
            response
                .bytes_stream()
                .map_err(|err| IOError::new(ErrorKind::Other, err)),
        ));

        Ok(GetResponse {
            success: status_code.is_success(),
            header: Some(header),
            status_code: Some(status_code),
            reader: Some(reader),
        })
    }

    /// Sends an GET request to a remote server via the Dragonfly and writes the response
    /// body directly into the provided buffer.
    ///
    /// This method is optimized for scenarios where the response body needs to be stored directly in
    /// memory, avoiding the overhead of streaming for smaller or fixed-size responses. The provided
    /// `BytesMut` buffer is used to store the response content, and the response metadata (e.g., status
    /// and headers) is returned separately.
    async fn get_into(&self, request: GetRequest, buf: &mut BytesMut) -> Result<GetResponse> {
        let get_into = async {
            let response = self.try_send(&request).await?;
            let status = response.status();
            let headers = response.headers().clone();

            if status.is_success() {
                let bytes = response.bytes().await.map_err(|err| {
                    Error::Internal(format!("failed to read response body: {}", err))
                })?;

                buf.extend_from_slice(&bytes);
            }

            Ok(GetResponse {
                success: status.is_success(),
                header: Some(headers),
                status_code: Some(status),
                reader: None,
            })
        };

        // Apply timeout which will properly cancel the operation when timeout is reached.
        tokio::time::timeout(request.timeout, get_into)
            .await
            .map_err(|err| Error::RequestTimeout(err.to_string()))?
    }
}

/// Proxy implements proxy request logic.
impl Proxy {
    /// Creates reqwest clients with proxy configuration for the given request.
    async fn client_entries(
        &self,
        request: &GetRequest,
    ) -> Result<Vec<Entry<ClientWithMiddleware>>> {
        let filtered_query_params = if request.filtered_query_params.is_empty() {
            default_proxy_rule_filtered_query_params()
        } else {
            request.filtered_query_params.clone()
        };

        // Generate task id for selecting seed peer.
        let task_id = self
            .id_generator
            .task_id(match request.content_for_calculating_task_id.as_ref() {
                Some(content) => TaskIDParameter::Content(content.clone()),
                None => TaskIDParameter::URLBased {
                    url: request.url.clone(),
                    piece_length: request.piece_length,
                    tag: request.tag.clone(),
                    application: request.application.clone(),
                    filtered_query_params,
                },
            })
            .map_err(|err| Error::Internal(format!("failed to generate task id: {}", err)))?;

        // Select seed peers for downloading.
        let seed_peers = self
            .seed_peer_selector
            .select(task_id.clone(), self.max_retries as u32)
            .await
            .map_err(|err| {
                Error::Internal(format!(
                    "failed to select seed peers from scheduler: {}",
                    err
                ))
            })?;

        debug!("task {} selected seed peers: {:?}", task_id, seed_peers);

        let mut client_entries = Vec::with_capacity(seed_peers.len());
        for peer in seed_peers.iter() {
            // TODO(chlins): Support client https scheme.
            let addr = format!("http://{}:{}", peer.ip, peer.proxy_port);
            let client_entry = self.client_pool.entry(&addr, &addr).await?;
            client_entries.push(client_entry);
        }

        Ok(client_entries)
    }

    /// Private helper to process requests and handle response headers with retries.
    async fn try_send(&self, request: &GetRequest) -> Result<reqwest::Response> {
        // Create client and send the request.
        let entries = self.client_entries(request).await?;
        if entries.is_empty() {
            return Err(Error::Internal(
                "no available client entries to send request".to_string(),
            ));
        }

        for (index, entry) in entries.iter().enumerate() {
            match self.send(entry, request).await {
                Ok(response) => return Ok(response),
                Err(err) => {
                    error!(
                        "failed to send request to client entry {:?}: {:?}",
                        entry.client, err
                    );

                    // If this is the last entry, return the error.
                    if index == entries.len() - 1 {
                        return Err(err);
                    }
                }
            }
        }

        Err(Error::Internal(
            "failed to send request to any client entry".to_string(),
        ))
    }

    /// Send a request to the specified URL via client entry with the given headers.
    async fn send(
        &self,
        entry: &Entry<ClientWithMiddleware>,
        request: &GetRequest,
    ) -> Result<reqwest::Response> {
        let headers = self.make_request_headers(request)?;
        let response = entry
            .client
            .get(&request.url)
            .headers(headers.clone())
            .timeout(request.timeout)
            .send()
            .await
            .map_err(|err| Error::Internal(err.to_string()))?;

        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }

        let response_headers = response.headers().clone();
        let header_map = headermap_to_hashmap(&response_headers);
        let message = response.text().await.ok();
        let error_type = response_headers
            .get("X-Dragonfly-Error-Type")
            .and_then(|v| v.to_str().ok());

        match error_type {
            Some("backend") => Err(Error::BackendError(BackendError {
                message,
                header: header_map,
                status_code: Some(status),
            })),
            Some("proxy") => Err(Error::ProxyError(ProxyError {
                message,
                header: header_map,
                status_code: Some(status),
            })),
            Some("dfdaemon") => Err(Error::DfdaemonError(DfdaemonError { message })),
            Some(other) => Err(Error::ProxyError(ProxyError {
                message: Some(format!("unknown error type from proxy: {}", other)),
                header: header_map,
                status_code: Some(status),
            })),
            None => Err(Error::ProxyError(ProxyError {
                message: Some(format!("unexpected status code from proxy: {}", status)),
                header: header_map,
                status_code: Some(status),
            })),
        }
    }

    /// make_request_headers applies p2p related headers to the request headers.
    fn make_request_headers(&self, request: &GetRequest) -> Result<HeaderMap> {
        let mut headers = request.header.clone().unwrap_or_default();

        if let Some(piece_length) = request.piece_length {
            headers.insert(
                "X-Dragonfly-Piece-Length",
                piece_length.to_string().parse().map_err(|err| {
                    Error::InvalidArgument(format!("invalid piece length: {}", err))
                })?,
            );
        }

        if let Some(tag) = request.tag.clone() {
            headers.insert(
                "X-Dragonfly-Tag",
                tag.to_string()
                    .parse()
                    .map_err(|err| Error::InvalidArgument(format!("invalid tag: {}", err)))?,
            );
        }

        if let Some(application) = request.application.clone() {
            headers.insert(
                "X-Dragonfly-Application",
                application.to_string().parse().map_err(|err| {
                    Error::InvalidArgument(format!("invalid application: {}", err))
                })?,
            );
        }

        if let Some(content_for_calculating_task_id) =
            request.content_for_calculating_task_id.clone()
        {
            headers.insert(
                "X-Dragonfly-Content-For-Calculating-Task-ID",
                content_for_calculating_task_id
                    .to_string()
                    .parse()
                    .map_err(|err| {
                        Error::InvalidArgument(format!(
                            "invalid content for calculating task id: {}",
                            err
                        ))
                    })?,
            );
        }

        if let Some(priority) = request.priority {
            headers.insert(
                "X-Dragonfly-Priority",
                priority
                    .to_string()
                    .parse()
                    .map_err(|err| Error::InvalidArgument(format!("invalid priority: {}", err)))?,
            );
        }

        if !request.filtered_query_params.is_empty() {
            let value = request.filtered_query_params.join(",");
            headers.insert(
                "X-Dragonfly-Filtered-Query-Params",
                value.parse().map_err(|err| {
                    Error::InvalidArgument(format!("invalid filtered query params: {}", err))
                })?,
            );
        }

        headers.insert("X-Dragonfly-Use-P2P", HeaderValue::from_static("true"));
        Ok(headers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_api::scheduler::v2::ListHostsResponse;
    use mocktail::prelude::*;
    use std::time::Duration;

    // Mock scheduler service for testing.
    async fn setup_mock_scheduler() -> Result<mocktail::server::MockServer> {
        let mut mocks = MockSet::new();
        mocks.mock(|when, then| {
            when.path("/scheduler.v2.Scheduler/ListHosts");
            then.pb(ListHostsResponse { hosts: vec![] });
        });

        let server = MockServer::new_grpc("scheduler.v2.Scheduler").with_mocks(mocks);
        server.start().await.map_err(|err| {
            Error::Internal(format!("failed to start mock scheduler server: {}", err))
        })?;

        Ok(server)
    }

    #[tokio::test]
    async fn test_proxy_new_success() {
        let mock_server = setup_mock_scheduler().await.unwrap();
        let scheduler_endpoint = format!("http://0.0.0.0:{}", mock_server.port().unwrap());
        let result = Proxy::builder()
            .scheduler_endpoint(scheduler_endpoint)
            .build()
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().max_retries, 1);
    }

    #[tokio::test]
    async fn test_proxy_new_empty_endpoint() {
        let result = Proxy::builder()
            .scheduler_endpoint("".to_string())
            .build()
            .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::InvalidArgument(_))));
    }

    #[tokio::test]
    async fn test_proxy_new_invalid_retry_times() {
        let mock_server = setup_mock_scheduler().await.unwrap();

        let scheduler_endpoint = format!("http://0.0.0.0:{}", mock_server.port().unwrap());
        let result = Proxy::builder()
            .scheduler_endpoint(scheduler_endpoint)
            .max_retries(11)
            .build()
            .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::InvalidArgument(_))));
    }

    #[tokio::test]
    async fn test_proxy_new_invalid_health_check_interval() {
        let mock_server = setup_mock_scheduler().await.unwrap();

        let scheduler_endpoint = format!("http://0.0.0.0:{}", mock_server.port().unwrap());
        let result = Proxy::builder()
            .scheduler_endpoint(scheduler_endpoint)
            .max_retries(11)
            .build()
            .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::InvalidArgument(_))));
    }

    #[tokio::test]
    async fn test_client_pool_get_or_create() {
        let pool = PoolBuilder::new(HTTPClientFactory {})
            .capacity(10)
            .idle_timeout(Duration::from_secs(600))
            .build();

        // Initially, the pool is empty.
        assert_eq!(pool.size().await, 0);

        // Get a client for the first time. A new client should be created.
        let addr = "http://proxy1.com".to_string();
        let _ = pool.entry(&addr, &addr).await.unwrap();
        assert_eq!(pool.size().await, 1);

        // Get a client for the same proxy again. It should be reused, so the count remains 1.
        let _ = pool.entry(&addr, &addr).await.unwrap();
        assert_eq!(pool.size().await, 1);

        // Get a client for a different proxy. A new client should be created, so the count becomes 2.
        let addr = "http://proxy2.com".to_string();
        let _ = pool.entry(&addr, &addr).await.unwrap();
        assert_eq!(pool.size().await, 2);
    }

    #[tokio::test]
    async fn test_client_pool_cleanup() {
        let pool = PoolBuilder::new(HTTPClientFactory {})
            .capacity(10)
            .idle_timeout(Duration::from_millis(10))
            .build();

        // Create a client.
        let addr = "http://proxy1.com".to_string();
        let _ = pool.entry(&addr, &addr).await.unwrap();
        assert_eq!(pool.size().await, 1);

        // Wait for cleanup above client.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Create another client.
        let addr = "http://proxy2.com".to_string();
        let _ = pool.entry(&addr, &addr).await.unwrap();

        // Still should be 1 because the proxy1 client should have been cleaned up.
        assert_eq!(pool.size().await, 1);
    }
}
