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

use crate::{Error, Result};
use tonic::transport::Channel;
use tonic_health::pb::{
    health_client::HealthClient as HealthGRPCClient, HealthCheckRequest, HealthCheckResponse,
};

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

    // check checks the health of the server.
    pub async fn check(&self, request: HealthCheckRequest) -> Result<HealthCheckResponse> {
        let request = Self::make_request(request);
        let response = self.client.clone().check(request).await?;
        Ok(response.into_inner())
    }

    // make_request creates a new request with timeout.
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}

#[cfg(test)]
mod test {
    use std::{net::SocketAddr, time::Duration};

    use tokio::task::JoinHandle;
    use tonic::transport::Server;
    use tonic_health::{pb::HealthCheckRequest, ServingStatus};

    use crate::grpc::health::HealthClient;

    struct MockHealthServer {
        addr: String,
        handle: JoinHandle<()>,
    }

    impl Drop for MockHealthServer {
        fn drop(&mut self) {
            self.handle.abort()
        }
    }

    async fn spawn_mock_app() -> MockHealthServer {
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_service_status("test", ServingStatus::Serving)
            .await;

        let addr = SocketAddr::from(([127, 0, 0, 1], 8081));
        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(health_service)
                .serve(addr)
                .await
                .expect("failed to start server");
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        return MockHealthServer {
            handle,
            addr: addr.to_string(),
        };
    }

    #[tokio::test]
    async fn test_get_avaliable_scheduler() {
        let app = spawn_mock_app().await;
        let health_client = HealthClient::new(&format!("http://{}", app.addr))
            .await
            .expect("failed to create health client");
        let Ok(res) = health_client
            .check(HealthCheckRequest {
                service: String::new(),
            })
            .await
        else {
            panic!("failed to check health");
        };
        assert_eq!(res.status, ServingStatus::Serving as i32);
    }
}
