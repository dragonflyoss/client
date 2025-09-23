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
use crate::pool::{Builder as PoolBuilder, Entry, Factory, Pool};
use crate::selector::{SeedPeerSelector, Selector};
use bytes::BytesMut;
use dragonfly_api::scheduler::v2::scheduler_client::SchedulerClient;
use dragonfly_client_core::error::DFError;
use errors::{BackendError, DfdaemonError, Error, ProxyError};
use futures::TryStreamExt;
use hostname;
use local_ip_address::local_ip;
use reqwest::{header::HeaderMap, header::HeaderValue, redirect::Policy, Client};
use rustix::path::Arg;
use rustls_pki_types::CertificateDer;
use std::io::{Error as IOError, ErrorKind};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;
use tonic::transport::Endpoint;
use tracing::{debug, error, instrument};

/// POOL_MAX_IDLE_PER_HOST is the max idle connections per host.
const POOL_MAX_IDLE_PER_HOST: usize = 1024;

/// KEEP_ALIVE_INTERVAL is the keep alive interval for TCP connection.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// DEFAULT_CLIENT_POOL_IDLE_TIMEOUT is the default idle timeout(30 minutes) for clients in the pool.
const DEFAULT_CLIENT_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// DEFAULT_CLIENT_POOL_CAPACITY is the default capacity of the client pool.
const DEFAULT_CLIENT_POOL_CAPACITY: usize = 128;

pub type Result<T> = std::result::Result<T, Error>;

pub type Body = Box<dyn AsyncRead + Send + Unpin>;

#[tonic::async_trait]
pub trait Request {
    /// get sends the get request to the remote server and returns the response reader.
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>>;

    /// get_into sends the get request to the remote server and writes the response to the input buffer.
    async fn get_into(&self, request: GetRequest, buf: &mut BytesMut) -> Result<GetResponse>;
}

pub struct Proxy {
    /// seed_peer_selector is the selector service for selecting seed peers.
    seed_peer_selector: Arc<SeedPeerSelector>,

    /// max_retries is the number of times to retry a request.
    max_retries: u8,

    /// client_pool is the pool of clients.
    client_pool: Pool<String, Client, HTTPClientFactory>,

    /// id_generator is the task id generator.
    id_generator: Arc<IDGenerator>,
}

pub struct Builder {
    /// scheduler_endpoint is the endpoint of the scheduler service.
    scheduler_endpoint: String,

    /// health_check_interval is the interval of health check for selector(seed peers).
    health_check_interval: Duration,

    /// max_retries is the number of times to retry a request.
    max_retries: u8,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            scheduler_endpoint: "".to_string(),
            health_check_interval: Duration::from_secs(60),
            max_retries: 1,
        }
    }
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

/// Factory for creating HTTPClient instances.
struct HTTPClientFactory {}

#[tonic::async_trait]
impl Factory<String, Client> for HTTPClientFactory {
    type Error = Error;
    async fn make_client(&self, proxy_addr: &String) -> Result<Client> {
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
            let proxy = reqwest::Proxy::all(proxy_addr)?;
            client_builder = client_builder.proxy(proxy);
        }

        let client = client_builder.build()?;

        Ok(client)
    }
}

impl Proxy {
    pub fn builder() -> Builder {
        Builder::default()
    }
}

impl Builder {
    /// Sets the scheduler endpoint.
    pub fn scheduler_endpoint(mut self, endpoint: String) -> Self {
        self.scheduler_endpoint = endpoint;
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

    pub async fn build(self) -> Result<Proxy> {
        // Validate input parameters.
        Self::validate(
            &self.scheduler_endpoint,
            self.health_check_interval,
            self.max_retries,
        )?;

        // Create the scheduler channel.
        let scheduler_channel = Endpoint::from_shared(self.scheduler_endpoint.to_string())?
            .connect()
            .await?;

        // Create scheduler client.
        let scheduler_client = SchedulerClient::new(scheduler_channel);

        // Create seed peer selector.
        let seed_peer_selector =
            Arc::new(SeedPeerSelector::new(scheduler_client, self.health_check_interval).await?);
        let seed_peer_selector_clone = Arc::clone(&seed_peer_selector);
        tokio::spawn(async move {
            // Run the selector service in the background to refresh the seed peers periodically.
            seed_peer_selector_clone.run().await;
        });

        // Get local IP address and hostname.
        let local_ip = local_ip().unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let hostname = hostname::get().unwrap().to_string_lossy().to_string();
        let id_generator = IDGenerator::new(local_ip.to_string(), hostname, true);

        let proxy = Proxy {
            seed_peer_selector,
            max_retries: self.max_retries,
            client_pool: PoolBuilder::new(HTTPClientFactory {})
                .capacity(DEFAULT_CLIENT_POOL_CAPACITY)
                .idle_timeout(DEFAULT_CLIENT_POOL_IDLE_TIMEOUT)
                .build(),
            id_generator: Arc::new(id_generator),
        };

        Ok(proxy)
    }

    fn validate(
        scheduler_endpoint: &str,
        health_check_duration: Duration,
        max_retries: u8,
    ) -> Result<()> {
        if let Err(err) = url::Url::parse(scheduler_endpoint) {
            return Err(
                DFError::ValidationError(format!("invalid scheduler endpoint: {}", err)).into(),
            );
        };

        if health_check_duration.as_secs() < 1 || health_check_duration.as_secs() > 600 {
            return Err(DFError::ValidationError(
                "health check duration must be between 1 and 600 seconds".to_string(),
            )
            .into());
        }

        if max_retries > 10 {
            return Err(DFError::ValidationError(
                "max retries must be between 0 and 10".to_string(),
            )
            .into());
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl Request for Proxy {
    /// Performs a GET request, handling custom errors and returning a streaming response.
    #[instrument(skip_all)]
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

    /// Performs a GET request, handling custom errors and collecting the response into a buffer.
    #[instrument(skip_all)]
    async fn get_into(&self, request: GetRequest, buf: &mut BytesMut) -> Result<GetResponse> {
        let operation = async {
            let response = self.try_send(&request).await?;
            let status = response.status();
            let headers = response.headers().clone();

            if status.is_success() {
                let bytes = response.bytes().await?;
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
        tokio::time::timeout(request.timeout, operation)
            .await
            .map_err(|_| {
                DFError::Unknown(format!("request timed out after {:?}", request.timeout))
            })?
    }
}

impl Proxy {
    /// Creates reqwest clients with proxy configuration for the given request.
    #[instrument(skip_all)]
    async fn client_entries(&self, request: &GetRequest) -> Result<Vec<Entry<Client>>> {
        // The request should be processed by the proxy, so generate the task ID to select a seed peer as proxy server.
        let parameter = match request.content_for_calculating_task_id.as_ref() {
            Some(content) => TaskIDParameter::Content(content.clone()),
            None => TaskIDParameter::URLBased {
                url: request.url.clone(),
                piece_length: request.piece_length,
                tag: request.tag.clone(),
                application: request.application.clone(),
                filtered_query_params: request.filtered_query_params.clone(),
            },
        };
        let task_id = self.id_generator.task_id(parameter)?;

        // Select seed peers.
        let seed_peers = self
            .seed_peer_selector
            .select(task_id, self.max_retries as u32)
            .await
            .map_err(|err| DFError::Unknown(format!("failed to select seed peers: {:?}", err)))?;

        debug!("selected seed peers: {:?}", seed_peers);

        let mut client_entries = Vec::new();
        for peer in seed_peers.iter() {
            let proxy_addr = format!("http://{}:{}", peer.ip, peer.proxy_port);
            let client_entry = self.client_pool.entry(&proxy_addr).await?;
            client_entries.push(client_entry);
        }

        Ok(client_entries)
    }

    /// Private helper to process requests and handle response headers with retries.
    #[instrument(skip_all)]
    async fn try_send(&self, request: &GetRequest) -> Result<reqwest::Response> {
        // Create client and send the request.
        let entries = self.client_entries(request).await?;
        if entries.is_empty() {
            return Err(DFError::Unknown(
                "no available client entries to send request".to_string(),
            )
            .into());
        }

        for (index, entry) in entries.iter().enumerate() {
            match self.send(entry, request).await {
                Ok(response) => return Ok(response),
                Err(err) => {
                    error!(
                        "failed to send request to client entry {:?}: {:?}",
                        entry.client, err
                    );
                    if index == entries.len() - 1 {
                        return Err(err);
                    }
                }
            }
        }

        Err(DFError::Unknown("all retries failed".to_string()).into())
    }

    /// Send a request to the specified URL via client entry with the given headers.
    #[instrument(skip_all)]
    async fn send(&self, entry: &Entry<Client>, request: &GetRequest) -> Result<reqwest::Response> {
        let headers = make_request_headers(request)?;
        let response = entry
            .client
            .get(&request.url)
            .headers(headers.clone())
            .timeout(request.timeout)
            .send()
            .await?;

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
                // Other error case we handle it as unknown error.
                _ => Err(DFError::Unknown(format!("unknown error type: {}", error_type)).into()),
            };
        }

        if !response.status().is_success() {
            return Err(
                DFError::Unknown(format!("unexpected status code: {}", response.status())).into(),
            );
        }

        Ok(response)
    }
}

/// make_request_headers applies p2p related headers to the request headers.
fn make_request_headers(request: &GetRequest) -> Result<HeaderMap> {
    let mut headers = request.header.clone().unwrap_or_default();

    // Apply the p2p related headers to the request headers.
    if let Some(piece_length) = request.piece_length {
        headers.insert(
            "X-Dragonfly-Piece-Length",
            piece_length.to_string().parse()?,
        );
    }

    if let Some(tag) = request.tag.clone() {
        headers.insert("X-Dragonfly-Tag", tag.to_string().parse()?);
    }

    if let Some(application) = request.application.clone() {
        headers.insert("X-Dragonfly-Application", application.to_string().parse()?);
    }

    if let Some(content_for_calculating_task_id) = request.content_for_calculating_task_id.clone() {
        headers.insert(
            "X-Dragonfly-Content-For-Calculating-Task-ID",
            content_for_calculating_task_id.to_string().parse()?,
        );
    }

    if let Some(priority) = request.priority {
        headers.insert("X-Dragonfly-Priority", priority.to_string().parse()?);
    }

    if !request.filtered_query_params.is_empty() {
        let value = request.filtered_query_params.join(",");
        headers.insert("X-Dragonfly-Filtered-Query-Params", value.parse()?);
    }

    // Always use p2p for sdk scenarios.
    headers.insert("X-Dragonfly-Use-P2P", HeaderValue::from_static("true"));

    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_api::scheduler::v2::ListHostsResponse;
    use dragonfly_client_core::error::DFError;
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
        match server.start().await {
            Ok(_) => Ok(server),
            Err(err) => Err(DFError::Unknown(format!(
                "failed to start mock scheduler server: {}",
                err,
            ))
            .into()),
        }
    }

    #[tokio::test]
    async fn test_proxy_new_success() -> Result<()> {
        let mock_server = setup_mock_scheduler().await?;

        let scheduler_endpoint = format!("http://0.0.0.0:{}", mock_server.port().unwrap());
        let result = Proxy::builder()
            .scheduler_endpoint(scheduler_endpoint)
            .build()
            .await;

        assert!(result.is_ok());
        let proxy = result.unwrap();
        assert_eq!(proxy.max_retries, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_proxy_new_empty_endpoint() {
        let result = Proxy::builder()
            .scheduler_endpoint("".to_string())
            .build()
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(Error::Base(DFError::ValidationError(_)))
        ));
    }

    #[tokio::test]
    async fn test_proxy_new_invalid_retry_times() -> Result<()> {
        let mock_server = setup_mock_scheduler().await?;

        let scheduler_endpoint = format!("http://0.0.0.0:{}", mock_server.port().unwrap());
        let result = Proxy::builder()
            .scheduler_endpoint(scheduler_endpoint)
            .max_retries(11)
            .build()
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(Error::Base(DFError::ValidationError(_)))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_proxy_new_invalid_health_check_interval() -> Result<()> {
        let mock_server = setup_mock_scheduler().await?;

        let scheduler_endpoint = format!("http://0.0.0.0:{}", mock_server.port().unwrap());
        let result = Proxy::builder()
            .scheduler_endpoint(scheduler_endpoint)
            .max_retries(11)
            .build()
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(Error::Base(DFError::ValidationError(_)))
        ));

        Ok(())
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
        let _ = pool.entry(&"http://proxy1.com".to_string()).await.unwrap();
        assert_eq!(pool.size().await, 1);

        // Get a client for the same proxy again. It should be reused, so the count remains 1.
        let _ = pool.entry(&"http://proxy1.com".to_string()).await.unwrap();
        assert_eq!(pool.size().await, 1);

        // Get a client for a different proxy. A new client should be created, so the count becomes 2.
        let _ = pool.entry(&"http://proxy2.com".to_string()).await.unwrap();
        assert_eq!(pool.size().await, 2);
    }

    #[tokio::test]
    async fn test_client_pool_cleanup() {
        let pool = PoolBuilder::new(HTTPClientFactory {})
            .capacity(10)
            .idle_timeout(Duration::from_millis(10))
            .build();

        // Create a client.
        let _ = pool.entry(&"http://proxy1.com".to_string()).await.unwrap();
        assert_eq!(pool.size().await, 1);

        // Wait for cleanup above client.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Create another client.
        let _ = pool.entry(&"http://proxy2.com".to_string()).await.unwrap();
        // Still should be 1 because the proxy1 client should have been cleaned up.
        assert_eq!(pool.size().await, 1);
    }
}
