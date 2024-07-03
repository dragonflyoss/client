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
use dragonfly_api::common::v2::{CachePeer, CacheTask, Peer, Task};
use dragonfly_api::manager::v2::Scheduler;
use dragonfly_api::scheduler::v2::{
    scheduler_client::SchedulerClient as SchedulerGRPCClient, AnnounceCachePeerRequest,
    AnnounceCachePeerResponse, AnnounceHostRequest, AnnouncePeerRequest, AnnouncePeerResponse,
    DeleteCachePeerRequest, DeleteCacheTaskRequest, DeleteHostRequest, DeletePeerRequest,
    DeleteTaskRequest, StatCachePeerRequest, StatCacheTaskRequest, StatPeerRequest,
    StatTaskRequest, UploadCacheTaskRequest,
};
use dragonfly_client_core::error::{ErrorType, ExternalError, OrErr};
use dragonfly_client_core::{Error, Result};
use hashring::HashRing;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tonic::transport::Channel;
use tracing::{error, info, instrument, Instrument};

use tonic_health::pb::health_check_response::ServingStatus;

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
    #[allow(dead_code)]
    unavailable_schedulers: Arc<RwLock<HashMap<SocketAddr, Instant>>>,

    // new add: cooldown_duration is the scheduler's cooldown time (in seconds)
    #[allow(dead_code)]
    cooldown_duration_secs: u64,

    // new add: Maximum retry attempts
    #[allow(dead_code)]
    max_attempts: u32,

    // new add: Refresh threshold
    #[allow(dead_code)]
    refresh_threshold: u32,
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
            unavailable_schedulers: Arc::new(RwLock::new(HashMap::new())),
            cooldown_duration_secs: 60,
            max_attempts: 5,
            refresh_threshold: 10,
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

                let mut client = SchedulerGRPCClient::new(channel)
                    .max_decoding_message_size(usize::MAX)
                    .max_encoding_message_size(usize::MAX);
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

                let mut client = SchedulerGRPCClient::new(channel)
                    .max_decoding_message_size(usize::MAX)
                    .max_encoding_message_size(usize::MAX);
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

                let mut client = SchedulerGRPCClient::new(channel)
                    .max_decoding_message_size(usize::MAX)
                    .max_encoding_message_size(usize::MAX);
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

    // announce_cache_peer announces the cache peer to the scheduler.
    #[instrument(skip_all)]
    pub async fn announce_cache_peer(
        &self,
        task_id: &str,
        peer_id: &str,
        request: impl tonic::IntoStreamingRequest<Message = AnnounceCachePeerRequest>,
    ) -> Result<tonic::Response<tonic::codec::Streaming<AnnounceCachePeerResponse>>> {
        let response = self
            .client(task_id, Some(peer_id))
            .await?
            .announce_cache_peer(request)
            .await?;
        Ok(response)
    }

    // stat_cache_peer gets the status of the cache peer.
    #[instrument(skip(self))]
    pub async fn stat_cache_peer(
        &self,
        task_id: &str,
        request: StatCachePeerRequest,
    ) -> Result<CachePeer> {
        let request = Self::make_request(request);
        let response = self
            .client(task_id, None)
            .await?
            .stat_cache_peer(request)
            .await?;
        Ok(response.into_inner())
    }

    // delete_cache_peer tells the scheduler that the cache peer is deleting.
    #[instrument(skip(self))]
    pub async fn delete_cache_peer(
        &self,
        task_id: &str,
        request: DeleteCachePeerRequest,
    ) -> Result<()> {
        let request = Self::make_request(request);
        self.client(task_id, None)
            .await?
            .delete_cache_peer(request)
            .await?;
        Ok(())
    }

    // upload_cache_task uploads the metadata of the cache task.
    #[instrument(skip(self))]
    pub async fn upload_cache_task(
        &self,
        task_id: &str,
        request: UploadCacheTaskRequest,
    ) -> Result<CacheTask> {
        let request = Self::make_request(request);
        let response = self
            .client(task_id, None)
            .await?
            .upload_cache_task(request)
            .await?;
        Ok(response.into_inner())
    }

    // stat_cache_task gets the status of the cache task.
    #[instrument(skip(self))]
    pub async fn stat_cache_task(
        &self,
        task_id: &str,
        request: StatCacheTaskRequest,
    ) -> Result<CacheTask> {
        let request = Self::make_request(request);
        let response = self
            .client(task_id, None)
            .await?
            .stat_cache_task(request)
            .await?;
        Ok(response.into_inner())
    }

    // delete_cache_task tells the scheduler that the cache task is deleting.
    #[instrument(skip(self))]
    pub async fn delete_cache_task(
        &self,
        task_id: &str,
        request: DeleteCacheTaskRequest,
    ) -> Result<()> {
        let request = Self::make_request(request);
        self.client(task_id, None)
            .await?
            .delete_cache_task(request)
            .await?;
        Ok(())
    }

    // client gets the grpc client of the scheduler.
    #[instrument(skip(self))]
    async fn client(
        &self,
        task_id: &str,
        peer_id: Option<&str>,
    ) -> Result<SchedulerGRPCClient<Channel>> {
        // Update scheduler addresses of the client.
        self.update_available_scheduler_addrs().await?;

        // Get the scheduler address from the hashring.
        let addrs = self.hashring.read().await;
        let addr = *addrs
            .get(&task_id[0..5].to_string())
            .ok_or_else(|| Error::HashRing(task_id.to_string()))?;
        drop(addrs);
        info!("picked {:?}", addr);

        let channel = match Channel::from_shared(format!("http://{}", addr))
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

                return Err(ExternalError::new(ErrorType::ConnectError)
                    .with_cause(Box::new(err))
                    .into());
            }
        };

        Ok(SchedulerGRPCClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX))
    }

    #[instrument(skip(self))]
    async fn new_client(
        &self,
        task_id: &str,
        peer_id: Option<&str>,
    ) -> Result<SchedulerGRPCClient<Channel>> {
        // Update scheduler addresses of the client.
        self.update_available_scheduler_addrs().await?;

        // First try to get the address from the task_id
        let addrs = self.hashring.read().await;
        let addr = *addrs
            .get(&task_id[0..5].to_string())
            .ok_or_else(|| Error::HashRing(task_id.to_string()))?;
        drop(addrs);
        info!(
            "first try to picked {:?} the address from the task_id",
            addr
        );

        match Channel::from_shared(format!("http://{}", addr))
            .map_err(|_| Error::InvalidURI(addr.to_string()))?
            .connect_timeout(super::CONNECT_TIMEOUT)
            .connect()
            .await
        {
            Ok(channel) => {
                let mut unavailable_schedulers = self.unavailable_schedulers.write().await;
                if unavailable_schedulers.contains_key(&addr.addr) {
                    unavailable_schedulers.remove(&addr.addr);
                }
                return Ok(SchedulerGRPCClient::new(channel)
                    .max_decoding_message_size(usize::MAX)
                    .max_encoding_message_size(usize::MAX));
            }

            Err(err) => {
                error!("connect to {} failed: {}", addr.to_string(), err);

                let mut unavailable_schedulers = self.unavailable_schedulers.write().await;
                unavailable_schedulers
                    .entry(addr.addr)
                    .or_insert_with(std::time::Instant::now);
            }
        }

        // Minimize lock holding time to reduce conflicts
        // let config_data = self.dynconfig.data.read().await;
        let cooldown_duration = Duration::from_secs(self.cooldown_duration_secs);
        let max_attempts = self.max_attempts;
        let refresh_threshold = self.refresh_threshold;

        // Initialize retry attempts
        let mut attempts = 0;

        // Use finer-grained locks in the loop
        let hashring = self.hashring.read().await;
        for node in hashring.clone().into_iter() {
            let scheduler_addr = node.addr;

            let unavailable_schedulers = self.unavailable_schedulers.write().await;
            if let Some(&instant) = unavailable_schedulers.get(&scheduler_addr) {
                if instant.elapsed() < cooldown_duration {
                    continue;
                }
            }

            match self.try_client(&scheduler_addr).await {
                Ok(channel) => {
                    let mut unavailable_schedulers = self.unavailable_schedulers.write().await;
                    if unavailable_schedulers.contains_key(&scheduler_addr) {
                        unavailable_schedulers.remove(&scheduler_addr);
                    }
                    return Ok(SchedulerGRPCClient::new(channel)
                        .max_decoding_message_size(usize::MAX)
                        .max_encoding_message_size(usize::MAX));
                }
                Err(err) => {
                    error!("Scheduler {} is not available: {}", scheduler_addr, err);

                    let mut unavailable_schedulers = self.unavailable_schedulers.write().await;
                    unavailable_schedulers
                        .entry(scheduler_addr)
                        .or_insert_with(std::time::Instant::now);

                    // Increment retry attempts
                    attempts += 1;

                    // Refresh scheduler addresses when reaching the refresh threshold
                    if attempts >= refresh_threshold {
                        self.refresh_available_scheduler_addrs().await?;
                    }

                    // Return error when reaching the maximum retry attempts
                    if attempts >= max_attempts {
                        return Err(Error::ExceededMaxAttempts);
                    }
                }
            }
        }

        return Err(Error::AvailableSchedulersNotFound);
    }

    async fn try_client(&self, scheduler_addr: &SocketAddr) -> Result<Channel> {
        info!("try to connect {:?}", scheduler_addr);
        let real_addr = format!("http://{}", scheduler_addr);
        let health_client = match HealthClient::new(&real_addr).await {
            Ok(client) => client,
            Err(err) => {
                error!("create {} health client failed: {}", real_addr, err);
                error!("check scheduler health failed: {}", err);
                return Err(ExternalError::new(ErrorType::ConnectError)
                    .with_cause(Box::new(err))
                    .into());
            }
        };

        match health_client.check().await {
            Ok(resp) => {
                if resp.status != ServingStatus::Serving as i32 {
                    error!("scheduler {} is not serving", scheduler_addr);
                    return Err(Error::SchedulerisNotServing);
                }
            }
            Err(err) => {
                error!("check scheduler health failed: {}", err);
                return Err(ExternalError::new(ErrorType::ConnectError)
                    .with_cause(Box::new(err))
                    .into());
            }
        }

        let channel = match Channel::from_shared(format!("http://{}", scheduler_addr))
            .map_err(|_| Error::InvalidURI(scheduler_addr.to_string()))?
            .connect_timeout(super::CONNECT_TIMEOUT)
            .connect()
            .await
        {
            Ok(channel) => channel,
            Err(err) => {
                error!("connect to {} failed: {}", scheduler_addr.to_string(), err);

                return Err(ExternalError::new(ErrorType::ConnectError)
                    .with_cause(Box::new(err))
                    .into());
            }
        };

        Ok(channel)
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
