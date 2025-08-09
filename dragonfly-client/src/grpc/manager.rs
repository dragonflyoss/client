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
    RequestEncryptionKeyRequest,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use std::sync::Arc;
use tonic::{service::interceptor::InterceptedService, transport::Channel};
use tonic_health::pb::health_check_response::ServingStatus;
use tracing::{error, instrument};
use url::Url;

use super::interceptor::InjectTracingInterceptor;

/// ManagerClient is a wrapper of ManagerGRPCClient.
#[derive(Clone)]
pub struct ManagerClient {
    /// client is the grpc client of the manager.
    pub client: ManagerGRPCClient<InterceptedService<Channel, InjectTracingInterceptor>>,
}

/// ManagerClient implements the grpc client of the manager.
impl ManagerClient {
    /// new creates a new ManagerClient.
    pub async fn new(config: Arc<Config>, addr: String) -> Result<Self> {
        let domain_name = Url::parse(addr.as_str())?
            .host_str()
            .ok_or_else(|| {
                error!("invalid address: {}", addr);
                Error::InvalidParameter
            })?
            .to_string();

        let client_tls_config = config
            .manager
            .load_client_tls_config(domain_name.as_str())
            .await?;

        let health_client = HealthClient::new(addr.as_str(), client_tls_config.clone()).await?;
        match health_client.check().await {
            Ok(resp) => {
                if resp.status != ServingStatus::Serving as i32 {
                    return Err(Error::AvailableManagerNotFound);
                }
            }
            Err(err) => return Err(err),
        }

        let channel = match client_tls_config {
            Some(client_tls_config) => Channel::from_shared(addr.clone())
                .map_err(|_| Error::InvalidURI(addr.clone()))?
                .tls_config(client_tls_config)?
                .buffer_size(super::BUFFER_SIZE)
                .connect_timeout(super::CONNECT_TIMEOUT)
                .timeout(super::REQUEST_TIMEOUT)
                .tcp_keepalive(Some(super::TCP_KEEPALIVE))
                .http2_keep_alive_interval(super::HTTP2_KEEP_ALIVE_INTERVAL)
                .keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT)
                .connect()
                .await
                .inspect_err(|err| {
                    error!("connect to {} failed: {}", addr.to_string(), err);
                })
                .or_err(ErrorType::ConnectError)?,
            None => Channel::from_shared(addr.clone())
                .map_err(|_| Error::InvalidURI(addr.clone()))?
                .buffer_size(super::BUFFER_SIZE)
                .connect_timeout(super::CONNECT_TIMEOUT)
                .timeout(super::REQUEST_TIMEOUT)
                .tcp_keepalive(Some(super::TCP_KEEPALIVE))
                .http2_keep_alive_interval(super::HTTP2_KEEP_ALIVE_INTERVAL)
                .keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT)
                .connect()
                .await
                .inspect_err(|err| {
                    error!("connect to {} failed: {}", addr.to_string(), err);
                })
                .or_err(ErrorType::ConnectError)?,
        };

        let client = ManagerGRPCClient::with_interceptor(channel, InjectTracingInterceptor)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        Ok(Self { client })
    }

    /// list_schedulers lists all schedulers that best match the client.
    #[instrument(skip_all)]
    pub async fn list_schedulers(
        &self,
        request: ListSchedulersRequest,
    ) -> Result<ListSchedulersResponse> {
        let request = Self::make_request(request);
        let response = self.client.clone().list_schedulers(request).await?;
        Ok(response.into_inner())
    }

    /// update_seed_peer updates the seed peer information.
    #[instrument(skip_all)]
    pub async fn update_seed_peer(&self, request: UpdateSeedPeerRequest) -> Result<SeedPeer> {
        let request = Self::make_request(request);
        let response = self.client.clone().update_seed_peer(request).await?;
        Ok(response.into_inner())
    }

    /// delete_seed_peer deletes the seed peer information.
    #[instrument(skip_all)]
    pub async fn delete_seed_peer(&self, request: DeleteSeedPeerRequest) -> Result<()> {
        let request = Self::make_request(request);
        self.client.clone().delete_seed_peer(request).await?;
        Ok(())
    }

    /// request_encryption_key request a key from manager
    pub async fn request_encryption_key(&self, request: RequestEncryptionKeyRequest) -> Result<Vec<u8>> {
        let request = Self::make_request(request);
        let response = self.client
            .clone()
            .request_encryption_key(request)
            .await?;
        Ok(response.into_inner().encryption_key)
    }

    /// make_request creates a new request with timeout.
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}

#[cfg(test)]
mod tests {
    use super::ManagerClient;
    use dragonfly_client_config::dfdaemon::Config;
    use std::sync::Arc;

    #[tokio::test]
    async fn invalid_uri_should_fail() {
        let addr = "htt:/xxx".to_string();
        let result = ManagerClient::new(Arc::new(Config::default()), addr).await;
        assert!(result.is_err());
        match result {
            Err(e) => assert_eq!(e.to_string(), "invalid parameter"),
            _ => panic!("unexpected error"),
        }
    }
}
