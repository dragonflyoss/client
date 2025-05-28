/*
 *     Copyright 2023 The Dragonfly Authors
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

use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use hyper_util::rt::TokioIo;
use std::path::PathBuf;
use tokio::net::UnixStream;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::ClientTlsConfig;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic_health::pb::{
    health_client::HealthClient as HealthGRPCClient, HealthCheckRequest, HealthCheckResponse,
};
use tower::service_fn;
use tracing::{error, instrument};

use super::interceptor::InjectTracingInterceptor;

/// HealthClient is a wrapper of HealthGRPCClient.
#[derive(Clone)]
pub struct HealthClient {
    /// client is the grpc client of the certificate.
    client: HealthGRPCClient<InterceptedService<Channel, InjectTracingInterceptor>>,
}

/// HealthClient implements the grpc client of the health.
impl HealthClient {
    /// new creates a new HealthClient.
    pub async fn new(addr: &str, client_tls_config: Option<ClientTlsConfig>) -> Result<Self> {
        let channel = match client_tls_config {
            Some(client_tls_config) => Channel::from_shared(addr.to_string())
                .map_err(|_| Error::InvalidURI(addr.into()))?
                .tls_config(client_tls_config)?
                .connect_timeout(super::CONNECT_TIMEOUT)
                .timeout(super::REQUEST_TIMEOUT)
                .tcp_keepalive(Some(super::TCP_KEEPALIVE))
                .http2_keep_alive_interval(super::HTTP2_KEEP_ALIVE_INTERVAL)
                .keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT)
                .connect()
                .await
                .inspect_err(|err| {
                    error!("connect to {} failed: {}", addr, err);
                })
                .or_err(ErrorType::ConnectError)?,
            None => Channel::from_shared(addr.to_string())
                .map_err(|_| Error::InvalidURI(addr.into()))?
                .connect_timeout(super::CONNECT_TIMEOUT)
                .timeout(super::REQUEST_TIMEOUT)
                .tcp_keepalive(Some(super::TCP_KEEPALIVE))
                .http2_keep_alive_interval(super::HTTP2_KEEP_ALIVE_INTERVAL)
                .keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT)
                .connect()
                .await
                .inspect_err(|err| {
                    error!("connect to {} failed: {}", addr, err);
                })
                .or_err(ErrorType::ConnectError)?,
        };

        let client = HealthGRPCClient::with_interceptor(channel, InjectTracingInterceptor)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        Ok(Self { client })
    }

    /// new_unix creates a new HealthClient with unix domain socket.
    pub async fn new_unix(socket_path: PathBuf) -> Result<Self> {
        // Ignore the uri because it is not used.
        let channel = Endpoint::try_from("http://[::]:50051")
            .unwrap()
            .connect_with_connector(service_fn(move |_: Uri| {
                let socket_path = socket_path.clone();
                async move {
                    Ok::<_, std::io::Error>(TokioIo::new(
                        UnixStream::connect(socket_path.clone()).await?,
                    ))
                }
            }))
            .await
            .inspect_err(|err| {
                error!("connect failed: {}", err);
            })
            .or_err(ErrorType::ConnectError)?;

        let client = HealthGRPCClient::with_interceptor(channel, InjectTracingInterceptor)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        Ok(Self { client })
    }

    /// check checks the health of the grpc service without service name.
    #[instrument(skip_all)]
    pub async fn check(&self) -> Result<HealthCheckResponse> {
        let request = Self::make_request(HealthCheckRequest {
            service: "".to_string(),
        });
        let response = self.client.clone().check(request).await?;
        Ok(response.into_inner())
    }

    /// check_service checks the health of the grpc service with service name.
    #[instrument(skip_all)]
    pub async fn check_service(&self, service: String) -> Result<HealthCheckResponse> {
        let request = Self::make_request(HealthCheckRequest { service });
        let response = self.client.clone().check(request).await?;
        Ok(response.into_inner())
    }

    /// check_dfdaemon_download checks the health of the dfdaemon download service.
    #[instrument(skip_all)]
    pub async fn check_dfdaemon_download(&self) -> Result<HealthCheckResponse> {
        self.check_service("dfdaemon.v2.DfdaemonDownload".to_string())
            .await
    }

    /// check_dfdaemon_upload checks the health of the dfdaemon upload service.
    #[instrument(skip_all)]
    pub async fn check_dfdaemon_upload(&self) -> Result<HealthCheckResponse> {
        self.check_service("dfdaemon.v2.DfdaemonUpload".to_string())
            .await
    }

    /// make_request creates a new request with timeout.
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}
