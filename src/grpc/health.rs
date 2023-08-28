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

use crate::Result;
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
    pub async fn new(addr: String) -> Result<Self> {
        let channel = Channel::from_static(Box::leak(addr.into_boxed_str()))
            .connect()
            .await?;
        let client = HealthGRPCClient::new(channel);
        Ok(Self { client })
    }

    // check checks the health of the server.
    pub async fn check(&mut self, request: HealthCheckRequest) -> Result<HealthCheckResponse> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        let response = self.client.check(request).await?;
        Ok(response.into_inner())
    }
}
