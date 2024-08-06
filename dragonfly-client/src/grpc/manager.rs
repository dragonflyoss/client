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

use crate::grpc::health::HealthClient;
use dragonfly_api::manager::v2::{
    manager_client::ManagerClient as ManagerGRPCClient, DeleteSeedPeerRequest,
    ListSchedulersRequest, ListSchedulersResponse, SeedPeer, UpdateSeedPeerRequest,
};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use tonic::transport::Channel;
use tonic_health::pb::health_check_response::ServingStatus;
use tracing::{error, info, instrument, warn};

// ManagerClient is a wrapper of ManagerGRPCClient.
#[derive(Clone)]
pub struct ManagerClient {
    // client is the grpc client of the manager.
    pub client: ManagerGRPCClient<Channel>,
}

// ManagerClient implements the grpc client of the manager.
impl ManagerClient {
    // new creates a new ManagerClient.
    pub async fn new(addrs: Vec<String>) -> Result<Self> {
        // Find the available manager address.
        let mut available_addr = String::new();
        for addr in addrs {
            let health_client = match HealthClient::new(addr.as_str()).await {
                Ok(client) => client,
                Err(err) => {
                    warn!("create {} health client failed: {}", addr, err);
                    continue;
                }
            };

            match health_client.check().await {
                Ok(resp) => {
                    if resp.status == ServingStatus::Serving as i32 {
                        info!("use manager address: {}", addr);
                        available_addr = addr;
                    }
                }
                Err(err) => {
                    warn!("check manager health failed: {}", err);
                    continue;
                }
            }
        }

        // Return error if no available address found.
        if available_addr.is_empty() {
            return Err(Error::AvailableManagerNotFound);
        }

        // Initialize the manager client by the available address.
        let channel = Channel::from_shared(available_addr.clone())
            .map_err(|_| Error::InvalidURI(available_addr.clone()))?
            .buffer_size(super::BUFFER_SIZE)
            .connect_timeout(super::CONNECT_TIMEOUT)
            .timeout(super::REQUEST_TIMEOUT)
            .tcp_keepalive(Some(super::TCP_KEEPALIVE))
            .http2_keep_alive_interval(super::HTTP2_KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT)
            .connect()
            .await
            .map_err(|err| {
                error!("connect to {} failed: {}", available_addr.to_string(), err);
                err
            })
            .or_err(ErrorType::ConnectError)?;
        let client = ManagerGRPCClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        Ok(Self { client })
    }

    // list_schedulers lists all schedulers that best match the client.
    #[instrument(skip_all)]
    pub async fn list_schedulers(
        &self,
        request: ListSchedulersRequest,
    ) -> Result<ListSchedulersResponse> {
        let request = Self::make_request(request);
        let response = self.client.clone().list_schedulers(request).await?;
        Ok(response.into_inner())
    }

    // update_seed_peer updates the seed peer information.
    #[instrument(skip_all)]
    pub async fn update_seed_peer(&self, request: UpdateSeedPeerRequest) -> Result<SeedPeer> {
        let request = Self::make_request(request);
        let response = self.client.clone().update_seed_peer(request).await?;
        Ok(response.into_inner())
    }

    // delete_seed_peer deletes the seed peer information.
    #[instrument(skip_all)]
    pub async fn delete_seed_peer(&self, request: DeleteSeedPeerRequest) -> Result<()> {
        let request = Self::make_request(request);
        self.client.clone().delete_seed_peer(request).await?;
        Ok(())
    }

    // make_request creates a new request with timeout.
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}

#[cfg(test)]
mod tests {
    use super::ManagerClient;

    #[tokio::test]
    async fn invalid_uri_should_fail() {
        let addrs = vec!["htt:/xxx".to_string()];
        let result = ManagerClient::new(addrs).await;
        assert!(result.is_err());
        match result {
            Err(e) => assert_eq!(e.to_string(), "available manager not found"),
            _ => panic!("unexpected error"),
        }
    }
}
