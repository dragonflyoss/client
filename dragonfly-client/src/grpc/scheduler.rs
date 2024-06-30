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
use crate::grpc::health::HealthClient;
use dragonfly_api::common::v2::{Peer, Task};
use dragonfly_api::manager::v2::Scheduler;
use dragonfly_api::scheduler::v2::{
    scheduler_client::SchedulerClient as SchedulerGRPCClient, AnnounceHostRequest,
    AnnouncePeerRequest, AnnouncePeerResponse, DeleteHostRequest, DeletePeerRequest,
    DeleteTaskRequest, StatPeerRequest, StatTaskRequest,
};
use dragonfly_client_core::error::{ErrorType, ExternalError, OrErr};
use dragonfly_client_core::{Error, Result};
use futures_util::__private::async_await;
use hashring::HashRing;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tonic::transport::Channel;
use tracing::{error, info, instrument, Instrument};

// VNode is the virtual node of the hashring.
#[derive(Debug, Copy, Clone, Hash, PartialEq)]
struct VNode {
    // addr is the address of the virtual node.
    addr: SocketAddr,
}

// VNode implements the Display trait.
impl std::fmt::Display for VNode {
    // fmt formats the virtual node.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
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

    // new add
    unavailable_schedulers: Arc<RwLock<HashMap<SocketAddr, Instant>>>,
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
            unavailable_schedulers: Arc::new(Mutex::new(HashMap::new())),
        };

        client.refresh_available_scheduler_addrs().await?;
        Ok(client)
    }

    // announce_peer announces the peer to the scheduler.
    #[instrument(skip_all)]
    pub async fn announce_peer(
        &self,
        task_id: &str,
        peer_id: &str,
        request: impl tonic::IntoStreamingRequest<Message = AnnouncePeerRequest>,
    ) -> Result<tonic::Response<tonic::codec::Streaming<AnnouncePeerResponse>>> {
        let response = self
            .client(task_id, Some(peer_id))
            .await?
            .announce_peer(request)
            .await?;
        Ok(response)
    }

    // stat_peer gets the status of the peer.
    #[instrument(skip(self))]
    pub async fn stat_peer(&self, task_id: &str, request: StatPeerRequest) -> Result<Peer> {
        let request = Self::make_request(request);
        let response = self.client(task_id, None).await?.stat_peer(request).await?;
        Ok(response.into_inner())
    }

    // delete_peer tells the scheduler that the peer is deleting.
    #[instrument(skip(self))]
    pub async fn delete_peer(&self, task_id: &str, request: DeletePeerRequest) -> Result<()> {
        let request = Self::make_request(request);
        self.client(task_id, None)
            .await?
            .delete_peer(request)
            .await?;
        Ok(())
    }

    // stat_task gets the status of the task.
    #[instrument(skip(self))]
    pub async fn stat_task(&self, task_id: &str, request: StatTaskRequest) -> Result<Task> {
        let request = Self::make_request(request);
        let response = self.client(task_id, None).await?.stat_task(request).await?;
        Ok(response.into_inner())
    }

    // delete_task tells the scheduler that the task is deleting.
    #[instrument(skip(self))]
    pub async fn delete_task(&self, task_id: &str, request: DeleteTaskRequest) -> Result<()> {
        let request = Self::make_request(request);
        self.client(task_id, None)
            .await?
            .delete_task(request)
            .await?;
        Ok(())
    }

    // init_announce_host announces the host to the scheduler.
    #[instrument(skip(self))]
    pub async fn init_announce_host(&self, request: AnnounceHostRequest) -> Result<()> {
        let mut join_set = JoinSet::new();
        let available_scheduler_addrs = self.available_scheduler_addrs.read().await;
        let available_scheduler_addrs_clone = available_scheduler_addrs.clone();
        drop(available_scheduler_addrs);

        for available_scheduler_addr in available_scheduler_addrs_clone.iter() {
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
                    })
                    .or_err(ErrorType::ConnectError)?;

                let mut client = SchedulerGRPCClient::new(channel);
                client.announce_host(request).await?;
                Ok(())
            }

            join_set.spawn(announce_host(*available_scheduler_addr, request).in_current_span());
        }

        while let Some(message) = join_set
            .join_next()
            .await
            .transpose()
            .or_err(ErrorType::AsyncRuntimeError)?
        {
            if let Err(err) = message {
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
        let available_scheduler_addrs_clone = available_scheduler_addrs.clone();
        drop(available_scheduler_addrs);

        for available_scheduler_addr in available_scheduler_addrs_clone.iter() {
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
                    })
                    .or_err(ErrorType::ConnectError)?;

                let mut client = SchedulerGRPCClient::new(channel);
                client.announce_host(request).await?;
                Ok(())
            }

            join_set.spawn(announce_host(*available_scheduler_addr, request).in_current_span());
        }

        while let Some(message) = join_set
            .join_next()
            .await
            .transpose()
            .or_err(ErrorType::AsyncRuntimeError)?
        {
            if let Err(err) = message {
                error!("failed to announce host: {}", err);
            }
        }

        Ok(())
    }

    // delete_host tells the scheduler that the host is deleting.
    #[instrument(skip(self))]
    pub async fn delete_host(&self, request: DeleteHostRequest) -> Result<()> {
        // Update scheduler addresses of the client.
        self.update_available_scheduler_addrs().await?;

        // Delete the host from the scheduler.
        let mut join_set = JoinSet::new();
        let available_scheduler_addrs = self.available_scheduler_addrs.read().await;
        let available_scheduler_addrs_clone = available_scheduler_addrs.clone();
        drop(available_scheduler_addrs);

        for available_scheduler_addr in available_scheduler_addrs_clone.iter() {
            let request = Self::make_request(request.clone());
            async fn delete_host(
                addr: SocketAddr,
                request: tonic::Request<DeleteHostRequest>,
            ) -> Result<()> {
                info!("delete host from {}", addr);

                // Connect to the scheduler.
                let channel = Channel::from_shared(format!("http://{}", addr))
                    .map_err(|_| Error::InvalidURI(addr.to_string()))?
                    .connect_timeout(super::CONNECT_TIMEOUT)
                    .connect()
                    .await
                    .map_err(|err| {
                        error!("connect to {} failed: {}", addr.to_string(), err);
                        err
                    })
                    .or_err(ErrorType::ConnectError)?;

                let mut client = SchedulerGRPCClient::new(channel);
                client.delete_host(request).await?;
                Ok(())
            }

            join_set.spawn(delete_host(*available_scheduler_addr, request).in_current_span());
        }

        while let Some(message) = join_set
            .join_next()
            .await
            .transpose()
            .or_err(ErrorType::AsyncRuntimeError)?
        {
            if let Err(err) = message {
                error!("failed to delete host: {}", err);
            }
        }

        Ok(())
    }

    // // client gets the grpc client of the scheduler.
    // #[instrument(skip(self))]
    // async fn client(
    //     &self,
    //     task_id: &str,
    //     peer_id: Option<&str>,
    // ) -> Result<SchedulerGRPCClient<Channel>> {
    //     // Update scheduler addresses of the client.
    //     self.update_available_scheduler_addrs().await?;

    //     // Get the scheduler address from the hashring.
    //     let addrs = self.hashring.read().await;
    //     let addr = *addrs
    //         .get(&task_id[0..5].to_string())
    //         .ok_or_else(|| Error::HashRing(task_id.to_string()))?;
    //     drop(addrs);
    //     info!("picked {:?}", addr);

    //     let channel = match Channel::from_shared(format!("http://{}", addr))
    //         .map_err(|_| Error::InvalidURI(addr.to_string()))?
    //         .connect_timeout(super::CONNECT_TIMEOUT)
    //         .connect()
    //         .await
    //     {
    //         Ok(channel) => channel,
    //         Err(err) => {
    //             error!("connect to {} failed: {}", addr.to_string(), err);
    //             if let Err(err) = self.refresh_available_scheduler_addrs().await {
    //                 error!("failed to refresh scheduler client: {}", err);
    //             };

    //             return Err(ExternalError::new(ErrorType::ConnectError)
    //                 .with_cause(Box::new(err))
    //                 .into());
    //         }
    //     };

    //     Ok(SchedulerGRPCClient::new(channel))
    // }

    // client gets the grpc client of the scheduler.
    #[instrument(skip(self))]
    async fn client(
        &self,
        task_id: &str,
        peer_id: Option<&str>,
    ) -> Result<SchedulerGRPCClient<Channel>> {
        let mut last_err = None;
        // Update scheduler addresses of the client.
        self.update_available_scheduler_addrs().await?;

        // First try to get the address from the task_id
        let addrs = self.hashring.read().await;
        if let Some(&addr) = *addrs.get(&task_id[0..5].to_string()) {
            drop(addrs);
            info!("first picked {:?} according to task_id", addr);

            match self.try_client(&addr).await {
                Ok(client) => return Ok(client),
                Err(err) => {
                    error!("connect to {} failed: {}", addr.to_string(), err);
                    last_err = Some(err);

                    let mut unavailable_schedulers = self.unavailable_schedulers.lock().await;
                    if !unavailable_schedulers.contains_key(&addr) {
                        unavailable_schedulers.insert(addr, std::time::Instant::now());
                    }
                }
            }
        } else {
            drop(addrs);
        }

        let hashring = self.hashring.read().await;
        let scheduler_keys: Vec<&VNode> = hashring.nodes().collect();
        drop(hashring);

        // Minimize lock holding time to reduce conflicts
        let config_data = self.dynconfig.data.read().await;
        let cooldown_duration = Duration::from_secs(config_data.cooldown_duration_secs);
        let max_attempts = config_data.max_attempts;
        let refresh_threshold = config_data.refresh_threshold;
        drop(config_data);

        // Initialize retry attempts
        let mut attempts = 0;

        // Use finer-grained locks in the loop
        for scheduler_key in scheduler_keys.iter().cycle().take(scheduler_keys.len()) {
            let scheduler_addr = scheduler_key.addr;

            {
                let unavailable_schedulers = self.unavailable_schedulers.lock().await;
                if let Some(&instant) = unavailable_schedulers.get(&scheduler_addr) {
                    if instant.elapsed() < cooldown_duration {
                        continue;
                    }
                }
            }

            match self.try_client(&scheduler_addr).await {
                Ok(client) => {
                    let mut unavailable_schedulers = self.unavailable_schedulers.lock().await;
                    if unavailable_schedulers.contains_key(&scheduler_addr) {
                        unavailable_schedulers.remove(&scheduler_addr);
                    }
                    return Ok(client);
                }
                Err(err) => {
                    error!("Scheduler {} is not available: {}", scheduler_addr, err);
                    last_err = Some(err);

                    let mut unavailable_schedulers = self.unavailable_schedulers.lock().await;
                    if !unavailable_schedulers.contains_key(&scheduler_addr) {
                        unavailable_schedulers.insert(scheduler_addr, std::time::Instant::now());
                    }

                    // Increment retry attempts
                    attempts += 1;

                    // Refresh scheduler addresses when reaching the refresh threshold
                    if attempts >= refresh_threshold {
                        self.refresh_available_scheduler_addrs().await?;
                    }

                    // Return error when reaching the maximum retry attempts
                    if attempts >= max_attempts {
                        return Err(last_err.unwrap_or(Error::SchedulerUnavailable));
                    }
                }
            }
        }

        // After polling once, refresh available scheduler addresses regardless of finding a usable scheduler or not
        self.refresh_available_scheduler_addrs().await;
        Err(last_err.unwrap_or(Error::SchedulerUnavailable))
    }

    async fn try_client(
        &self,
        scheduler_addr: &SocketAddr,
    ) -> Result<SchedulerGRPCClient<Channel>> {
        info!("try to connect {:?}", scheduler_addr);

        // 创建健康检查客户端
        let health_client = match HealthClient::new(&format!("http://{}", scheduler_addr)).await {
            Ok(client) => client,
            Err(err) => {
                error!(
                    "create health client for scheduler {} failed: {}",
                    scheduler_addr, err
                );
                return Err(Error::ConnectError);
            }
        };

        match health_client.check().await {
            Ok(resp) => {
                if resp.status != ServingStatus::Serving as i32 {
                    error!("scheduler {} is not serving", scheduler_addr);
                    return Err(Error::SchedulerUnavailable);
                }
            }
            Err(err) => {
                error!("check scheduler health failed: {}", err);
                return Err(Error::ConnectError);
            }
        }

        let channel = Channel::from_shared(format!("http://{}", scheduler_addr))
            .map_err(|_| Error::InvalidURI(scheduler_addr.to_string()))?
            .connect_timeout(super::CONNECT_TIMEOUT)
            .connect()
            .await
            .map_err(|err| {
                error!("connect to {} failed: {}", scheduler_addr, err);
                err
            })
            .or_err(ErrorType::ConnectError)?;

        Ok(SchedulerGRPCClient::new(channel))
    }

    // update_available_scheduler_addrs updates the addresses of available schedulers.
    #[instrument(skip(self))]
    async fn update_available_scheduler_addrs(&self) -> Result<()> {
        // Get the endpoints of available schedulers.
        let data = self.dynconfig.data.read().await;
        let data_available_schedulers_clone = data.available_schedulers.clone();
        drop(data);

        // Check if the available schedulers is empty.
        if data_available_schedulers_clone.is_empty() {
            return Err(Error::AvailableSchedulersNotFound);
        }

        // Get the available schedulers.
        let available_schedulers = self.available_schedulers.read().await;
        let available_schedulers_clone = available_schedulers.clone();
        drop(available_schedulers);

        // Check if the available schedulers is not changed.
        if data_available_schedulers_clone.len() == available_schedulers_clone.len()
            && data_available_schedulers_clone
                .iter()
                .zip(available_schedulers_clone.iter())
                .all(|(a, b)| a == b)
        {
            info!(
                "available schedulers is not changed: {:?}",
                data_available_schedulers_clone
                    .iter()
                    .map(|s| s.ip.clone())
                    .collect::<Vec<String>>()
            );
            return Ok(());
        }

        let mut new_available_schedulers = Vec::new();
        let mut new_available_scheduler_addrs = Vec::new();
        let mut new_hashring = HashRing::new();
        for available_scheduler in data_available_schedulers_clone.iter() {
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
        drop(available_schedulers);

        // Update the addresses of available schedulers.
        let mut available_scheduler_addrs = self.available_scheduler_addrs.write().await;
        *available_scheduler_addrs = new_available_scheduler_addrs;
        drop(available_scheduler_addrs);

        // Update the hashring.
        let mut hashring = self.hashring.write().await;
        *hashring = new_hashring;
        drop(hashring);

        let available_scheduler_addrs = self.available_scheduler_addrs.read().await;
        info!(
            "refresh available scheduler addresses: {:?}",
            available_scheduler_addrs
                .iter()
                .map(|s| s.ip().to_string())
                .collect::<Vec<String>>(),
        );
        Ok(())
    }

    // refresh_available_scheduler_addrs refreshes addresses of available schedulers.
    #[instrument(skip(self))]
    async fn refresh_available_scheduler_addrs(&self) -> Result<()> {
        // Refresh the dynamic configuration.
        // refresh中有getavail进行健康检查
        self.dynconfig.refresh().await?;

        // 上述已经进行了健康检查，为什么还需要进行可用性检测呢，1. 上述更新的是self.dynconfig.data中的可用调度器集合
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
