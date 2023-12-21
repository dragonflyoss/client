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

use crate::shutdown;
use crate::{Error, Result};
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tonic::transport::{Channel, Endpoint, Server, Uri};
use tonic_health::pb::{
    health_client::HealthClient as HealthGRPCClient,
    health_server::{Health, HealthServer as HealthGRPCServer},
    HealthCheckRequest, HealthCheckResponse,
};
use tower::service_fn;
use tracing::{info, instrument};

// HealthServer is the grpc server of the health.
pub struct HealthServer<T: Health> {
    // addr is the address of the grpc server.
    addr: SocketAddr,

    // service is the grpc service of the health.
    service: HealthGRPCServer<T>,

    // shutdown is used to shutdown the grpc server.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the grpc server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

// HealthServer implements the grpc server of the health.
impl<T: Health> HealthServer<T> {
    // new creates a new HealthServer.
    #[instrument(skip_all)]
    pub fn new(
        addr: SocketAddr,
        service: HealthGRPCServer<T>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            addr,
            service,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    // run starts the health server.
    #[instrument(skip_all)]
    pub async fn run(&self) {
        // Register the reflection service.
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(dragonfly_api::FILE_DESCRIPTOR_SET)
            .build()
            .unwrap();

        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Start health grpc server.
        info!("health server listening on {}", self.addr);
        Server::builder()
            .add_service(reflection.clone())
            .add_service(self.service.clone())
            .serve_with_shutdown(self.addr, async move {
                // Health grpc server shutting down with signals.
                let _ = shutdown.recv().await;
                info!("health grpc server shutting down");
            })
            .await
            .unwrap();
    }
}

// HealthClient is a wrapper of HealthGRPCClient.
#[derive(Clone)]
pub struct HealthClient {
    // client is the grpc client of the certificate.
    client: HealthGRPCClient<Channel>,
}

// HealthClient implements the grpc client of the health.
impl HealthClient {
    // new creates a new HealthClient.
    pub async fn new(addr: &str) -> Result<Self> {
        let channel = Channel::from_shared(addr.to_string())
            .map_err(|_| Error::InvalidURI(addr.into()))?
            .connect_timeout(super::CONNECT_TIMEOUT)
            .connect()
            .await?;
        let client = HealthGRPCClient::new(channel);
        Ok(Self { client })
    }

    // new_unix creates a new HealthClient with unix domain socket.
    #[instrument(skip_all)]
    pub async fn new_unix(socket_path: PathBuf) -> Result<Self> {
        // Ignore the uri because it is not used.
        let channel = Endpoint::try_from("http://[::]:50051")
            .unwrap()
            .connect_with_connector(service_fn(move |_: Uri| {
                UnixStream::connect(socket_path.clone())
            }))
            .await?;
        let client = HealthGRPCClient::new(channel).max_decoding_message_size(usize::MAX);
        Ok(Self { client })
    }

    // check checks the health of the grpc service without service name.
    #[instrument(skip_all)]
    pub async fn check(&self) -> Result<HealthCheckResponse> {
        let request = Self::make_request(HealthCheckRequest {
            service: "".to_string(),
        });
        let response = self.client.clone().check(request).await?;
        Ok(response.into_inner())
    }

    // check_service checks the health of the grpc service with service name.
    pub async fn check_service(&self, service: String) -> Result<HealthCheckResponse> {
        let request = Self::make_request(HealthCheckRequest { service });
        let response = self.client.clone().check(request).await?;
        Ok(response.into_inner())
    }

    // check_dfdaemon checks the health of the dfdaemon.
    #[instrument(skip_all)]
    pub async fn check_dfdaemon(&self) -> Result<HealthCheckResponse> {
        let services = vec![
            "dfdaemon.v2.DfdaemonDownload".to_string(),
            "dfdaemon.v2.DfdaemonUpload".to_string(),
        ];

        let mut join_set = JoinSet::new();
        for service in services {
            let client = self.clone();
            async fn check_service(
                client: HealthClient,
                service: String,
            ) -> Result<HealthCheckResponse> {
                client.check_service(service).await
            }

            join_set.spawn(check_service(client, service));
        }

        // Wait for all tasks to finish.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok(check) => info!("check: {:?}", check),
                Err(err) => return Err(err.into()),
            };
        }

        Ok(HealthCheckResponse {
            status: tonic_health::ServingStatus::Serving as i32,
        })
    }

    // check_dfdaemon_download checks the health of the dfdaemon download service.
    #[instrument(skip_all)]
    pub async fn check_dfdaemon_download(&self) -> Result<HealthCheckResponse> {
        self.check_service("dfdaemon.v2.DfdaemonDownload".to_string())
            .await
    }

    // check_dfdaemon_upload checks the health of the dfdaemon upload service.
    #[instrument(skip_all)]
    pub async fn check_dfdaemon_upload(&self) -> Result<HealthCheckResponse> {
        self.check_service("dfdaemon.v2.DfdaemonUpload".to_string())
            .await
    }

    // make_request creates a new request with timeout.
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}
