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
use crate::dynconfig::Dynconfig;
use dragonfly_api::common::v2::{Peer, Task};
use dragonfly_api::manager::v2::Scheduler;
use dragonfly_api::scheduler::v2::{
    scheduler_client::SchedulerClient as SchedulerGRPCClient, AnnounceHostRequest,
    AnnouncePeerRequest, AnnouncePeerResponse, ExchangePeerRequest, ExchangePeerResponse,
    LeaveHostRequest, LeavePeerRequest, StatPeerRequest, StatTaskRequest,
};
use dragonfly_client_core::{Error, Result};
use hashring::HashRing;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tonic::transport::Channel;
use tracing::{error, info, instrument, Instrument};

#[derive(Debug, Copy, Clone, Hash, PartialEq)]
struct VNode {
    addr: SocketAddr,
}

impl ToString for VNode {
    fn to_string(&self) -> String {
        format!("{}", self.addr)
    }
}

// SchedulerClient is a wrapper of SchedulerGRPCClient.
#[derive(Clone)]
pub struct SchedulerClient {
    // dynconfig is the dynamic configuration of the dfdaemon.
    dynconfig: Arc<Dynconfig>,

    // available_schedulers is the available schedulers.
    available_schedulers: Arc<RwLock<Vec<Scheduler>>>,

    // available_scheduler_addrs is the addresses of available schedulers.
    available_scheduler_addrs: Arc<RwLock<Vec<SocketAddr>>>,

    // hashring is the hashring of the scheduler.
    hashring: Arc<RwLock<HashRing<VNode>>>,
}

// SchedulerClient implements the grpc client of the scheduler.
impl SchedulerClient {
    // new creates a new SchedulerClient.
    pub async fn new(dynconfig: Arc<Dynconfig>) -> Result<Self> {
        let client = Self {
            dynconfig,
            available_schedulers: Arc::new(RwLock::new(Vec::new())),
            available_scheduler_addrs: Arc::new(RwLock::new(Vec::new())),
            hashring: Arc::new(RwLock::new(HashRing::new())),
        };

        client.refresh_available_scheduler_addrs().await?;
        Ok(client)
    }

    // announce_peer announces the peer to the scheduler.
    #[instrument(skip(self, request))]
    pub async fn announce_peer(
        &self,
        task_id: &str,
        request: impl tonic::IntoStreamingRequest<Message = AnnouncePeerRequest>,
    ) -> Result<tonic::Response<tonic::codec::Streaming<AnnouncePeerResponse>>> {
        let response = self
            .client(task_id.to_string())
            .await?
            .announce_peer(request)
            .await?;
        Ok(response)
    }

    // stat_peer gets the status of the peer.
    #[instrument(skip(self))]
    pub async fn stat_peer(&self, task_id: &str, request: StatPeerRequest) -> Result<Peer> {
        let request = Self::make_request(request);
        let response = self
            .client(task_id.to_string())
            .await?
            .stat_peer(request)
            .await?;
        Ok(response.into_inner())
    }

    // leave_peer tells the scheduler that the peer is leaving.
    #[instrument(skip(self))]
    pub async fn leave_peer(&self, task_id: &str, request: LeavePeerRequest) -> Result<()> {
        let request = Self::make_request(request);
        self.client(task_id.to_string())
            .await?
            .leave_peer(request)
            .await?;
        Ok(())
    }

    // exchange_peer exchanges the peer with the scheduler.
    #[instrument(skip(self))]
    pub async fn exchange_peer(
        &self,
        task_id: &str,
        request: ExchangePeerRequest,
    ) -> Result<ExchangePeerResponse> {
        let request = Self::make_request(request);
        let response = self
            .client(task_id.to_string())
            .await?
            .exchange_peer(request)
            .await?;
        Ok(response.into_inner())
    }

    // stat_task gets the status of the task.
    #[instrument(skip(self))]
    pub async fn stat_task(&self, task_id: &str, request: StatTaskRequest) -> Result<Task> {
        let request = Self::make_request(request);
        let response = self
            .client(task_id.to_string())
            .await?
            .stat_task(request)
            .await?;
        Ok(response.into_inner())
    }

    // init_announce_host announces the host to the scheduler.
    #[instrument(skip(self))]
    pub async fn init_announce_host(&self, request: AnnounceHostRequest) -> Result<()> {
        let mut join_set = JoinSet::new();
        let available_scheduler_addrs = self.available_scheduler_addrs.read().await;
        for available_scheduler_addr in available_scheduler_addrs.iter() {
            let request = Self::make_request(request.clone());
            async fn announce_host(
                addr: SocketAddr,
                request: tonic::Request<AnnounceHostRequest>,
            ) -> Result<()> {
                info!("announce host to {:?}", addr);

                // Connect to the scheduler.
                let channel = Channel::from_shared(format!("http://{}", addr))
                    .map_err(|_| Error::InvalidURI(addr.to_string()))?
                    .connect_timeout(super::CONNECT_TIMEOUT)
                    .connect()
                    .await
                    .map_err(|err| {
                        error!("connect to {} failed: {}", addr.to_string(), err);
                        err
                    })?;

                let mut client = SchedulerGRPCClient::new(channel);
                client.announce_host(request).await?;
                Ok(())
            }

            join_set.spawn(announce_host(*available_scheduler_addr, request).in_current_span());
        }

        while let Some(message) = join_set.join_next().await {
            if let Err(err) = message? {
                error!("failed to init announce host: {}", err);
                return Err(err);
            }
        }

        Ok(())
    }

    // announce_host announces the host to the scheduler.
    #[instrument(skip(self))]
    pub async fn announce_host(&self, request: AnnounceHostRequest) -> Result<()> {
        // Update scheduler addresses of the client.
        self.update_available_scheduler_addrs().await?;

        // Announce the host to the scheduler.
        let mut join_set = JoinSet::new();
        let available_scheduler_addrs = self.available_scheduler_addrs.read().await;
        for available_scheduler_addr in available_scheduler_addrs.iter() {
            let request = Self::make_request(request.clone());
            async fn announce_host(
                addr: SocketAddr,
                request: tonic::Request<AnnounceHostRequest>,
            ) -> Result<()> {
                info!("announce host to {}", addr);

                // Connect to the scheduler.
                let channel = Channel::from_shared(format!("http://{}", addr))
                    .map_err(|_| Error::InvalidURI(addr.to_string()))?
                    .connect_timeout(super::CONNECT_TIMEOUT)
                    .connect()
                    .await
                    .map_err(|err| {
                        error!("connect to {} failed: {}", addr.to_string(), err);
                        err
                    })?;

                let mut client = SchedulerGRPCClient::new(channel);
                client.announce_host(request).await?;
                Ok(())
            }

            join_set.spawn(announce_host(*available_scheduler_addr, request).in_current_span());
        }

        while let Some(message) = join_set.join_next().await {
            if let Err(err) = message? {
                error!("failed to announce host: {}", err);
            }
        }

        Ok(())
    }

    // leave_host tells the scheduler that the host is leaving.
    #[instrument(skip(self))]
    pub async fn leave_host(&self, request: LeaveHostRequest) -> Result<()> {
        // Update scheduler addresses of the client.
        self.update_available_scheduler_addrs().await?;

        // Leave the host from the scheduler.
        let mut join_set = JoinSet::new();
        let available_scheduler_addrs = self.available_scheduler_addrs.read().await;
        for available_scheduler_addr in available_scheduler_addrs.iter() {
            let request = Self::make_request(request.clone());
            async fn leave_host(
                addr: SocketAddr,
                request: tonic::Request<LeaveHostRequest>,
            ) -> Result<()> {
                info!("leave host from {}", addr);

                // Connect to the scheduler.
                let channel = Channel::from_shared(format!("http://{}", addr))
                    .map_err(|_| Error::InvalidURI(addr.to_string()))?
                    .connect_timeout(super::CONNECT_TIMEOUT)
                    .connect()
                    .await
                    .map_err(|err| {
                        error!("connect to {} failed: {}", addr.to_string(), err);
                        err
                    })?;

                let mut client = SchedulerGRPCClient::new(channel);
                client.leave_host(request).await?;
                Ok(())
            }

            join_set.spawn(leave_host(*available_scheduler_addr, request).in_current_span());
        }

        while let Some(message) = join_set.join_next().await {
            if let Err(err) = message? {
                error!("failed to leave host: {}", err);
            }
        }

        Ok(())
    }

    // client gets the grpc client of the scheduler.
    #[instrument(skip(self))]
    async fn client(&self, key: String) -> Result<SchedulerGRPCClient<Channel>> {
        // Update scheduler addresses of the client.
        self.update_available_scheduler_addrs().await?;

        // Get the scheduler address from the hashring.
        let addr = self.hashring.read().await;
        let addr = addr
            .get(&key[0..5].to_string())
            .ok_or_else(|| Error::HashRing(key.clone()))?;
        info!("{} picked {:?}", key, addr);

        let channel = match Channel::from_shared(format!("http://{}", addr.to_string()))
            .map_err(|_| Error::InvalidURI(addr.to_string()))?
            .connect_timeout(super::CONNECT_TIMEOUT)
            .connect()
            .await
        {
            Ok(channel) => channel,
            Err(err) => {
                error!("connect to {} failed: {}", addr.to_string(), err);
                if let Err(err) = self.refresh_available_scheduler_addrs().await {
                    error!("failed to refresh scheduler client: {}", err);
                };

                return Err(Error::TonicTransport(err));
            }
        };

        Ok(SchedulerGRPCClient::new(channel))
    }

    // update_available_scheduler_addrs updates the addresses of available schedulers.
    #[instrument(skip(self))]
    async fn update_available_scheduler_addrs(&self) -> Result<()> {
        // Get the endpoints of available schedulers.
        let data = self.dynconfig.data.read().await;

        // Check if the available schedulers is empty.
        if data.available_schedulers.is_empty() {
            return Err(Error::AvailableSchedulersNotFound());
        }

        // Get the available schedulers.
        let available_schedulers = self.available_schedulers.read().await;

        // Check if the available schedulers is not changed.
        if data.available_schedulers.len() == available_schedulers.len()
            && data
                .available_schedulers
                .iter()
                .zip(available_schedulers.iter())
                .all(|(a, b)| a == b)
        {
            info!(
                "available schedulers is not changed: {:?}",
                data.available_schedulers
            );
            return Ok(());
        }
        drop(available_schedulers);

        let mut new_available_schedulers = Vec::new();
        let mut new_available_scheduler_addrs = Vec::new();
        let mut new_hashring = HashRing::new();
        for available_scheduler in data.available_schedulers.iter() {
            let ip = match IpAddr::from_str(&available_scheduler.ip) {
                Ok(ip) => ip,
                Err(err) => {
                    error!("failed to parse ip: {}", err);
                    continue;
                }
            };

            // Add the scheduler to the available schedulers.
            new_available_schedulers.push(available_scheduler.clone());

            // Add the scheduler address to the addresses of available schedulers.
            new_available_scheduler_addrs
                .push(SocketAddr::new(ip, available_scheduler.port as u16));

            // Add the scheduler to the hashring.
            new_hashring.add(VNode {
                addr: SocketAddr::new(ip, available_scheduler.port as u16),
            });
        }

        // Update the available schedulers.
        let mut available_schedulers = self.available_schedulers.write().await;
        *available_schedulers = new_available_schedulers;

        // Update the addresses of available schedulers.
        let mut available_scheduler_addrs = self.available_scheduler_addrs.write().await;
        *available_scheduler_addrs = new_available_scheduler_addrs;

        // Update the hashring.
        let mut hashring = self.hashring.write().await;
        *hashring = new_hashring;
        info!(
            "refresh available scheduler addresses: {:?}",
            available_scheduler_addrs
        );
        Ok(())
    }

    // refresh_available_scheduler_addrs refreshes addresses of available schedulers.
    #[instrument(skip(self))]
    async fn refresh_available_scheduler_addrs(&self) -> Result<()> {
        // Refresh the dynamic configuration.
        self.dynconfig.refresh().await?;

        // Update scheduler addresses of the client.
        self.update_available_scheduler_addrs().await
    }

    // make_request creates a new request with timeout.
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}
