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

use dragonfly_api::common::Task;
use dragonfly_api::dfdaemon::{
    dfdaemon_client::DfdaemonClient as DfdaemonGRPCClient, DeleteTaskRequest, DownloadTaskRequest,
    StatTaskRequest, UploadTaskRequest,
};
use std::net::SocketAddr;
use tonic::transport::Channel;

// DfdaemonClient is a wrapper of DfdaemonGRPCClient.
pub struct DfdaemonClient {
    // client is the grpc client of the dfdaemon.
    pub client: DfdaemonGRPCClient<Channel>,
}

// DfdaemonClient implements the grpc client of the dfdaemon.
impl DfdaemonClient {
    // new creates a new DfdaemonClient.
    pub async fn new(addr: &SocketAddr) -> super::Result<Self> {
        let conn = tonic::transport::Endpoint::new(addr.to_string())?
            .connect()
            .await?;
        let client = DfdaemonGRPCClient::new(conn);
        Ok(Self { client })
    }

    // download_task tells the dfdaemon to download the task.
    pub async fn download_task(&mut self, request: DownloadTaskRequest) -> super::Result<()> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        self.client.download_task(request).await?;
        Ok(())
    }

    // upload_task tells the dfdaemon to upload the task.
    pub async fn upload_task(&mut self, request: UploadTaskRequest) -> super::Result<()> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        self.client.upload_task(request).await?;
        Ok(())
    }

    // stat_task gets the status of the task.
    pub async fn stat_task(&mut self, request: StatTaskRequest) -> super::Result<Task> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        let response = self.client.stat_task(request).await?;
        Ok(response.into_inner())
    }

    // delete_task tells the dfdaemon to delete the task.
    pub async fn delete_task(&mut self, request: DeleteTaskRequest) -> super::Result<()> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        self.client.delete_task(request).await?;
        Ok(())
    }
}
