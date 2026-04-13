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

use crate::digest::is_blob_url;
use crate::http::{headermap_to_hashmap, query_params::default_proxy_rule_filtered_query_params};
use crate::id_generator::{IDGenerator, TaskIDParameter};
use crate::net::format_url;
use crate::net::preferred_local_ip;
use crate::pool::{Builder as PoolBuilder, Entry, Factory, Pool};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use bytes::BytesMut;
use dragonfly_api::scheduler::v2::scheduler_client::SchedulerClient;
use errors::{BackendError, DfdaemonError, Error, ProxyError};
use futures::TryStreamExt;
use hostname;
use oci_client::client::ClientConfig;
use oci_client::manifest::ImageIndexEntry;
use oci_client::secrets::RegistryAuth;
use oci_client::{Client as OciClient, Reference, RegistryOperation};
use oci_spec::image::{Arch, Os};
use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION},
    Client,
};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_tracing::TracingMiddleware;
use rustix::path::Arg;
use rustls_pki_types::CertificateDer;
use selector::{SeedPeerSelector, Selector};
use std::io::{Error as IOError, ErrorKind};
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;
use tonic::transport::Endpoint;
use tracing::{debug, error, warn};

pub mod errors;
mod selector;

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
#[async_trait]
pub trait Request {
    /// Sends an GET request to a remote server via the Dragonfly and returns a response
    /// with a streaming body.
    ///
    /// This method is designed for scenarios where the response body is expected to be processed as a
    /// stream, allowing efficient handling of large or continuous data. The response includes metadata
    /// such as status codes and headers, along with a streaming `Body` for accessing the response content.
    async fn get(&self, request: &GetRequest) -> Result<GetResponse<Body>>;

    /// Sends an GET request to a remote server via the Dragonfly and writes the response
    /// body directly into the provided buffer.
    ///
    /// This method is optimized for scenarios where the response body needs to be stored directly in
    /// memory, avoiding the overhead of streaming for smaller or fixed-size responses. The provided
    /// `BytesMut` buffer is used to store the response content, and the response metadata (e.g., status
    /// and headers) is returned separately.
    async fn get_into(&self, request: &GetRequest, buf: &mut BytesMut) -> Result<GetResponse>;

    /// Preheats an OCI image by downloading all its blobs via the Dragonfly.
    ///
    /// This method is designed for scenarios where OCI image content needs to be pre-cached in
    /// the seed client before actual consumption, ensuring faster subsequent access across the
    /// cluster. It parses the image reference, authenticates with the OCI registry, resolves
    /// the image manifest (including multi-platform image indexes), and downloads each blob
    /// (config and layers) through the seed client.
    async fn preheat(&self, request: &PreheatRequest) -> Result<()>;
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

    /// Content for calculating task id. This is used when the task ID cannot be calculated based
    /// on URL and other parameters, such as when the URL contains dynamic query parameters that
    /// cannot be filtered out.
    pub content_for_calculating_task_id: Option<String>,

    /// Enable task id based blob digest. It indicates whether to use the blob digest for task ID calculation
    /// when downloading from OCI registries. When enabled for OCI blob URLs (e.g., /v2/<name>/blobs/sha256:<digest>),
    /// the task ID is derived from the blob digest rather than the full URL. This enables deduplication across
    /// registries - the same blob from different registries shares one task ID, eliminating redundant downloads
    /// and storage, default is true.
    pub enable_task_id_based_blob_digest: bool,

    /// Refer to https://github.com/dragonflyoss/api/blob/main/proto/common.proto#L67
    pub priority: Option<i32>,

    /// timeout is the timeout of the request, default is 300s.
    pub timeout: Duration,

    /// Client cert is the client certificates for the request.
    pub client_cert: Option<Vec<CertificateDer<'static>>>,
}

/// Default implementation for GetRequest.
impl Default for GetRequest {
    /// Default returns a default GetRequest with empty url and default values for other fields.
    fn default() -> Self {
        Self {
            url: String::new(),
            header: None,
            piece_length: None,
            tag: None,
            application: None,
            filtered_query_params: default_proxy_rule_filtered_query_params(),
            content_for_calculating_task_id: None,
            enable_task_id_based_blob_digest: true,
            priority: None,
            timeout: Duration::from_secs(300),
            client_cert: None,
        }
    }
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

/// PreheatRequest represents a request to preheat an OCI image through the
/// Dragonfly seed client. The preheat downloads all blobs (config and layers)
/// of the specified image via the Dragonfly proxy, effectively caching them
/// in the P2P network for faster downloading.
pub struct PreheatRequest {
    /// Image is the OCI image reference (e.g., "docker.io/library/nginx:latest").
    pub image: String,

    /// Username for registry authentication. If not provided, anonymous access is used.
    pub username: Option<String>,

    /// Password for registry authentication. If not provided, anonymous access is used.
    pub password: Option<String>,

    /// Platform specifies the target platform in the format "os/arch"
    /// (e.g., "linux/amd64", "linux/arm64"). This is used to select the correct
    /// manifest from a multi-platform image index, default is current platform.
    pub platform: Option<String>,

    /// Piece length is the optional piece length for the Dragonfly task.
    pub piece_length: Option<u64>,

    /// Tag identifies different tasks for the same URL.
    pub tag: Option<String>,

    /// Application identifies different tasks for the same URL.
    pub application: Option<String>,

    /// Filtered query params to generate the task id.
    /// When filter is ["Signature", "Expires", "ns"], for example:
    /// http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io and http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io
    /// will generate the same task id.
    /// Default value includes the filtered query params of s3, gcs, oss, obs, cos.
    pub filtered_query_params: Vec<String>,

    /// Content for calculating task id. This is used when the task ID cannot be calculated based
    /// on URL and other parameters, such as when the URL contains dynamic query parameters that
    /// cannot be filtered out.
    pub content_for_calculating_task_id: Option<String>,

    /// Enable task id based blob digest. It indicates whether to use the blob digest for task ID calculation
    /// when downloading from OCI registries. When enabled for OCI blob URLs (e.g., /v2/<name>/blobs/sha256:<digest>),
    /// the task ID is derived from the blob digest rather than the full URL. This enables deduplication across
    /// registries - the same blob from different registries shares one task ID, eliminating redundant downloads
    /// and storage, default is true.
    pub enable_task_id_based_blob_digest: bool,

    /// Refer to https://github.com/dragonflyoss/api/blob/main/proto/common.proto#L67
    pub priority: Option<i32>,

    /// Timeout is the timeout for each blob download request, default is 300s.
    pub timeout: Duration,

    /// Client cert is the optional client certificates for the request.
    pub client_cert: Option<Vec<CertificateDer<'static>>>,
}

/// Default implementation for PreheatRequest.
impl Default for PreheatRequest {
    /// Default returns a default PreheatRequest with empty image and default values for other
    /// fields.
    fn default() -> Self {
        Self {
            image: String::new(),
            username: None,
            password: None,
            platform: None,
            piece_length: None,
            tag: None,
            application: None,
            filtered_query_params: default_proxy_rule_filtered_query_params(),
            content_for_calculating_task_id: None,
            enable_task_id_based_blob_digest: true,
            priority: None,
            timeout: Duration::from_secs(300),
            client_cert: None,
        }
    }
}

/// Factory for creating HTTPClient instances.
#[derive(Debug, Clone, Default)]
struct HTTPClientFactory {}

/// HTTPClientFactory implements Factory for creating reqwest::Client instances with proxy support.
#[async_trait]
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
        // In IPv6-only environments, IPv4 detection may fail, so we use a best-effort IPv4->IPv6 fallback.
        let local_ip = preferred_local_ip().unwrap().to_string();
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
#[async_trait]
impl Request for Proxy {
    /// Sends an GET request to a remote server via the Dragonfly and returns a response
    /// with a streaming body.
    ///
    /// This method is designed for scenarios where the response body is expected to be processed as a
    /// stream, allowing efficient handling of large or continuous data. The response includes metadata
    /// such as status codes and headers, along with a streaming `Body` for accessing the response content.
    async fn get(&self, request: &GetRequest) -> Result<GetResponse> {
        let response = self.try_send(request).await?;
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
    async fn get_into(&self, request: &GetRequest, buf: &mut BytesMut) -> Result<GetResponse> {
        let get_into = async {
            let response = self.try_send(request).await?;
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

    /// Preheats an OCI image by downloading all its blobs via the Dragonfly.
    ///
    /// This method is designed for scenarios where OCI image content needs to be pre-cached in
    /// the seed client before actual consumption, ensuring faster subsequent access across the
    /// cluster. It parses the image reference, authenticates with the OCI registry, resolves
    /// the image manifest (including multi-platform image indexes), and downloads each blob
    /// (config and layers) through the seed client.
    async fn preheat(&self, request: &PreheatRequest) -> Result<()> {
        let oci_client = Self::oci_client(request.platform.clone())?;

        // Parse image reference.
        let reference: Reference = request
            .image
            .parse()
            .map_err(|err| Error::InvalidArgument(format!("invalid image reference: {}", err)))?;

        // Create registry authentication.
        let auth = match (&request.username, &request.password) {
            (Some(username), Some(password)) => {
                RegistryAuth::Basic(username.clone(), password.clone())
            }
            _ => RegistryAuth::Anonymous,
        };

        // Pull image manifest. This handles multi-platform image index manifests
        // by selecting the platform-specific manifest using our resolver.
        let (manifest, digest) = oci_client
            .pull_image_manifest(&reference, &auth)
            .await
            .map_err(|err| Error::Internal(format!("failed to pull image manifest: {}", err)))?;
        debug!(
            "pulled manifest for image {} with digest {}, layers: {}",
            request.image,
            digest,
            manifest.layers.len()
        );

        // Authenticate with the registry and get a bearer token if available.
        let token = oci_client
            .auth(&reference, &auth, RegistryOperation::Pull)
            .await
            .map_err(|err| {
                Error::Internal(format!("failed to authenticate with registry: {}", err))
            })?;

        // Build authorization header for blob downloads through the Dragonfly.
        let mut header = HeaderMap::new();
        if let Some(authorization) = Self::make_registry_authorization_header(&auth, token)? {
            header.insert(AUTHORIZATION, authorization);
        }

        let registry = reference.resolve_registry();
        let repository = reference.repository();
        for digest in std::iter::once(&manifest.config.digest)
            .chain(manifest.layers.iter().map(|layer| &layer.digest))
        {
            let url = Self::build_blob_url(registry, repository, digest);
            let get_request = GetRequest {
                url: url.clone(),
                header: Some(header.clone()),
                piece_length: request.piece_length,
                tag: request.tag.clone(),
                application: request.application.clone(),
                filtered_query_params: request.filtered_query_params.clone(),
                content_for_calculating_task_id: request.content_for_calculating_task_id.clone(),
                enable_task_id_based_blob_digest: request.enable_task_id_based_blob_digest,
                priority: request.priority,
                timeout: request.timeout,
                client_cert: request.client_cert.clone(),
            };

            let response = self.get(&get_request).await?;
            match response.reader {
                Some(mut reader) => {
                    tokio::io::copy(&mut reader, &mut tokio::io::sink())
                        .await
                        .map_err(|err| {
                            Error::Internal(format!("failed to read blob {}: {}", digest, err))
                        })?;
                }
                None => {
                    warn!(
                        "no response body for blob {}, download may not have completed",
                        digest
                    );
                }
            }

            debug!("preheated blob: {}", url);
        }

        debug!("preheat completed for image: {}", request.image);
        Ok(())
    }
}

/// Proxy implements proxy request logic.
impl Proxy {
    /// Creates reqwest clients with proxy configuration for the given request.
    async fn client_entries(
        &self,
        request: &GetRequest,
    ) -> Result<Vec<Entry<ClientWithMiddleware>>> {
        let filtered_query_params =
            Self::effective_filtered_query_params(&request.filtered_query_params);

        // Generate task id for selecting seed peer.
        let task_id = self
            .id_generator
            .task_id(
                if let Some(content) = request.content_for_calculating_task_id.clone() {
                    TaskIDParameter::Content(content)
                } else if request.enable_task_id_based_blob_digest && is_blob_url(&request.url) {
                    TaskIDParameter::BlobDigestBased(request.url.clone())
                } else {
                    TaskIDParameter::URLBased {
                        url: request.url.clone(),
                        piece_length: request.piece_length,
                        tag: request.tag.clone(),
                        application: request.application.clone(),
                        filtered_query_params,
                        revision: None,
                    }
                },
            )
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
            let addr = format_url(
                "http",
                IpAddr::from_str(&peer.ip).map_err(|err| Error::Internal(err.to_string()))?,
                peer.proxy_port as u16,
            );
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

    /// Make request headers applies p2p related headers to the request headers.
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

        headers.insert(
            "X-Dragonfly-Enable-Task-ID-Based-Blob-Digest",
            request
                .enable_task_id_based_blob_digest
                .to_string()
                .parse()
                .map_err(|err| {
                    Error::InvalidArgument(format!(
                        "invalid enable task id based blob digest: {}",
                        err
                    ))
                })?,
        );

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

    /// Helper function to check if a URL is an OCI blob URL (e.g., /v2/<name>/blobs/sha256:
    /// <digest>).
    fn build_blob_url(registry: &str, repository: &str, digest: &str) -> String {
        format!("https://{}/v2/{}/blobs/{}", registry, repository, digest)
    }

    /// Returns the configured query-param filters, defaulting to the standard proxy rule filters
    /// when the caller leaves the list empty.
    fn effective_filtered_query_params(filtered_query_params: &[String]) -> Vec<String> {
        if filtered_query_params.is_empty() {
            default_proxy_rule_filtered_query_params()
        } else {
            filtered_query_params.to_vec()
        }
    }

    /// Builds the Authorization header for OCI blob downloads.
    ///
    /// Prefer a bearer token when the registry returns one. If no token is returned, fall back to
    /// basic auth when credentials were provided; otherwise send no Authorization header.
    fn make_registry_authorization_header(
        auth: &RegistryAuth,
        bearer_token: Option<String>,
    ) -> Result<Option<HeaderValue>> {
        if let Some(token) = bearer_token {
            return HeaderValue::from_str(&format!("Bearer {}", token))
                .map(Some)
                .map_err(|err| Error::Internal(format!("invalid auth token: {}", err)));
        }

        match auth {
            RegistryAuth::Basic(username, password) => HeaderValue::from_str(&format!(
                "Basic {}",
                BASE64_STANDARD.encode(format!("{}:{}", username, password))
            ))
            .map(Some)
            .map_err(|err| Error::Internal(format!("invalid basic auth header: {}", err))),
            RegistryAuth::Bearer(token) => HeaderValue::from_str(&format!("Bearer {}", token))
                .map(Some)
                .map_err(|err| Error::Internal(format!("invalid auth token: {}", err))),
            RegistryAuth::Anonymous => Ok(None),
        }
    }

    /// Builds an OCI client with a platform resolver that matches the requested os/arch.
    fn oci_client(platform: Option<String>) -> Result<OciClient> {
        let mut oci_config = ClientConfig::default();
        if let Some(platform) = platform {
            let (os, arch) = platform
                .split_once('/')
                .map(|(os, arch)| (Os::from(os), Arch::from(arch)))
                .ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "invalid platform format '{}', expected 'os/arch' (e.g., 'linux/amd64')",
                        platform
                    ))
                })?;

            oci_config.platform_resolver = Some(Box::new(move |manifests: &[ImageIndexEntry]| {
                manifests
                    .iter()
                    .find(|entry| {
                        entry.platform.as_ref().is_some_and(|platform| {
                            platform.os == os && platform.architecture == arch
                        })
                    })
                    .map(|entry| entry.digest.clone())
            }))
        };

        Ok(OciClient::new(oci_config))
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

    #[test]
    fn test_effective_filtered_query_params_defaults_when_empty() {
        let mut actual = Proxy::effective_filtered_query_params(&[]);
        let mut expected = default_proxy_rule_filtered_query_params();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_effective_filtered_query_params_preserves_explicit_values() {
        let filters = vec!["Signature".to_string(), "Expires".to_string()];
        assert_eq!(Proxy::effective_filtered_query_params(&filters), filters);
    }

    #[test]
    fn test_make_registry_authorization_header_prefers_bearer_token() {
        let header = Proxy::make_registry_authorization_header(
            &RegistryAuth::Basic("user".to_string(), "password".to_string()),
            Some("token".to_string()),
        )
        .unwrap()
        .unwrap();

        assert_eq!(header.to_str().unwrap(), "Bearer token");
    }

    #[test]
    fn test_make_registry_authorization_header_uses_basic_auth_without_token() {
        let header = Proxy::make_registry_authorization_header(
            &RegistryAuth::Basic("user".to_string(), "password".to_string()),
            None,
        )
        .unwrap()
        .unwrap();

        assert_eq!(header.to_str().unwrap(), "Basic dXNlcjpwYXNzd29yZA==");
    }

    #[test]
    fn test_make_registry_authorization_header_uses_existing_bearer_auth_without_token() {
        let header = Proxy::make_registry_authorization_header(
            &RegistryAuth::Bearer("token".to_string()),
            None,
        )
        .unwrap()
        .unwrap();

        assert_eq!(header.to_str().unwrap(), "Bearer token");
    }

    #[test]
    fn test_make_registry_authorization_header_allows_anonymous_without_token() {
        let header =
            Proxy::make_registry_authorization_header(&RegistryAuth::Anonymous, None).unwrap();

        assert!(header.is_none());
    }
}
