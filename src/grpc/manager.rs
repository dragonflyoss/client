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

use crate::{Result, Error};
use dragonfly_api::manager::v2::{
    manager_client::ManagerClient as ManagerGRPCClient, DeleteSeedPeerRequest,
    GetObjectStorageRequest, ListSchedulersRequest, ListSchedulersResponse, ObjectStorage,
    SeedPeer, UpdateSeedPeerRequest,
};
use tonic::transport::Channel;

// ManagerClient is a wrapper of ManagerGRPCClient.
#[derive(Clone)]
pub struct ManagerClient {
    // client is the grpc client of the manager.
    pub client: ManagerGRPCClient<Channel>,
}

// ManagerClient implements the grpc client of the manager.
impl ManagerClient {
    // new creates a new ManagerClient.
    pub async fn new(addr: &str) -> Result<Self> {
        let channel = Channel::from_shared(addr.to_string())
            .map_err(|_| Error::InvalidURI(addr.into()))?
            .connect()
            .await?;
        let client = ManagerGRPCClient::new(channel);
        Ok(Self { client })
    }

    // list_schedulers lists all schedulers that best match the client.
    pub async fn list_schedulers(
        &self,
        request: ListSchedulersRequest,
    ) -> Result<ListSchedulersResponse> {
        let request = Self::make_request(request);
        let response = self.client.clone().list_schedulers(request).await?;
        Ok(response.into_inner())
    }

    // get_object_storage provides the object storage information.
    pub async fn get_object_storage(
        &self,
        request: GetObjectStorageRequest,
    ) -> Result<ObjectStorage> {
        let request = Self::make_request(request);
        let response = self.client.clone().get_object_storage(request).await?;
        Ok(response.into_inner())
    }

    // update_seed_peer updates the seed peer information.
    pub async fn update_seed_peer(&self, request: UpdateSeedPeerRequest) -> Result<SeedPeer> {
        let request = Self::make_request(request);
        let response = self.client.clone().update_seed_peer(request).await?;
        Ok(response.into_inner())
    }

    // delete_seed_peer deletes the seed peer information.
    pub async fn delete_seed_peer(&self, request: DeleteSeedPeerRequest) -> Result<()> {
        let request = Self::make_request(request);
        self.client.clone().delete_seed_peer(request).await?;
        Ok(())
    }

    fn make_request<T>(req: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(req);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}

#[cfg(test)]
mod tests {
    use super::ManagerClient;

    #[tokio::test]
    async fn invalid_uri_should_fail() {
        let result = ManagerClient::new("htt:/xxx").await;
        assert!(result.is_err());
        match result {
            Err(e) => assert_eq!(e.to_string(), "invalid uri htt:/xxx"),
            _ => panic!("unexpected error"),
        }
    }

}