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

// use crate::dynconfig::Dynconfig;
use crate::Result;
use dragonfly_api::common::v2::{Peer, Task};
// use dragonfly_api::manager::v2::Scheduler;
use dragonfly_api::scheduler::v2::{
    scheduler_client::SchedulerClient as SchedulerGRPCClient, AnnounceHostRequest,
    ExchangePeerRequest, ExchangePeerResponse, LeaveHostRequest, LeavePeerRequest, StatPeerRequest,
    StatTaskRequest,
};
// use std::sync::Arc;
use tonic::transport::Channel;

// SchedulerClient is a wrapper of SchedulerGRPCClient.
#[derive(Clone)]
pub struct SchedulerClient {
    // dynconfig is the dynamic configuration of the dfdaemon.
    // dynconfig: Arc<Dynconfig>,

    // client is the grpc client of the scehduler.
    pub client: SchedulerGRPCClient<Channel>,
}

// SchedulerClient implements the grpc client of the scheduler.
impl SchedulerClient {
    // new creates a new SchedulerClient.
    pub async fn new() -> Result<Self> {
        let channel = Channel::from_static("http://127.0.0.1:8002")
            .connect()
            .await?;
        let client = SchedulerGRPCClient::new(channel);
        Ok(Self { client })
    }

    // stat_peer gets the status of the peer.
    pub async fn stat_peer(&self, request: StatPeerRequest) -> Result<Peer> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        let response = self.client.clone().stat_peer(request).await?;
        Ok(response.into_inner())
    }

    // leave_peer tells the scheduler that the peer is leaving.
    pub async fn leave_peer(&self, request: LeavePeerRequest) -> Result<()> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        self.client.clone().leave_peer(request).await?;
        Ok(())
    }

    // exchange_peer exchanges the peer with the scheduler.
    pub async fn exchange_peer(
        &self,
        request: ExchangePeerRequest,
    ) -> Result<ExchangePeerResponse> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        let response = self.client.clone().exchange_peer(request).await?;
        Ok(response.into_inner())
    }

    // stat_task gets the status of the task.
    pub async fn stat_task(&self, request: StatTaskRequest) -> Result<Task> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        let response = self.client.clone().stat_task(request).await?;
        Ok(response.into_inner())
    }

    // announce_host announces the host to the scheduler.
    pub async fn announce_host(&self, request: AnnounceHostRequest) -> Result<()> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        self.client.clone().announce_host(request).await?;
        Ok(())
    }

    // leave_host tells the scheduler that the host is leaving.
    pub async fn leave_host(&self, request: LeaveHostRequest) -> Result<()> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);

        self.client.clone().leave_host(request).await?;
        Ok(())
    }

    // get_available_schedulers gets the available schedulers.
    // async fn get_available_schedulers(&self) -> Vec<Scheduler> {
    // let data = self.dynconfig.data.read().await;
    // data.available_schedulers.clone()
    // }
}
