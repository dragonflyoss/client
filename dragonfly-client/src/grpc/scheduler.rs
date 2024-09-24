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
    StatTaskRequest, UploadCacheTaskFailedRequest, UploadCacheTaskFinishedRequest,
    UploadCacheTaskStartedRequest,
};
use dragonfly_client_core::error::{ErrorType, ExternalError, OrErr};
use dragonfly_client_core::{Error, Result};
use hashring::HashRing;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tonic::transport::Channel;
use tonic_health::pb::health_check_response::ServingStatus;
use tracing::{error, info, instrument, Instrument};

/// VNode is the virtual node of the hashring.
#[derive(Debug, Copy, Clone, Hash, PartialEq)]
struct VNode {
    /// addr is the address of the virtual node.
    addr: SocketAddr,
}

/// VNode implements the Display trait.
impl std::fmt::Display for VNode {
    /// fmt formats the virtual node.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}

/// SchedulerClient is a wrapper of SchedulerGRPCClient.
#[derive(Clone)]
pub struct SchedulerClient {
    /// dynconfig is the dynamic configuration of the dfdaemon.
    dynconfig: Arc<Dynconfig>,

    /// available_schedulers is the available schedulers.
    available_schedulers: Arc<RwLock<Vec<Scheduler>>>,

    /// available_scheduler_addrs is the addresses of available schedulers.
    available_scheduler_addrs: Arc<RwLock<Vec<SocketAddr>>>,

    /// hashring is the hashring of the scheduler.
    hashring: Arc<RwLock<HashRing<VNode>>>,

    // unavailable_scheduler_addrs is a map of unavailable scheduler addrs and the time they were marked as unavailable.
    unavailable_scheduler_addrs: Arc<RwLock<HashMap<SocketAddr, Instant>>>,
}

/// SchedulerClient implements the grpc client of the scheduler.
impl SchedulerClient {
    /// new creates a new SchedulerClient.
    #[instrument(skip_all)]
    pub async fn new(dynconfig: Arc<Dynconfig>) -> Result<Self> {
        let client = Self {
            dynconfig,
            available_schedulers: Arc::new(RwLock::new(Vec::new())),
            available_scheduler_addrs: Arc::new(RwLock::new(Vec::new())),
            hashring: Arc::new(RwLock::new(HashRing::new())),
            unavailable_scheduler_addrs: Arc::new(RwLock::new(HashMap::new())),
        };

        client.refresh_available_scheduler_addrs().await?;
        Ok(client)
    }

    /// announce_peer announces the peer to the scheduler.
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

    /// stat_peer gets the status of the peer.
    #[instrument(skip(self))]
    pub async fn stat_peer(&self, request: StatPeerRequest) -> Result<Peer> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        let response = self
            .client(task_id.as_str(), None)
            .await?
            .stat_peer(request)
            .await?;
        Ok(response.into_inner())
    }

    /// delete_peer tells the scheduler that the peer is deleting.
    #[instrument(skip(self))]
    pub async fn delete_peer(&self, request: DeletePeerRequest) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .delete_peer(request)
            .await?;
        Ok(())
    }

    /// stat_task gets the status of the task.
    #[instrument(skip(self))]
    pub async fn stat_task(&self, request: StatTaskRequest) -> Result<Task> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        let response = self
            .client(task_id.as_str(), None)
            .await?
            .stat_task(request)
            .await?;
        Ok(response.into_inner())
    }

    /// delete_task tells the scheduler that the task is deleting.
    #[instrument(skip(self))]
    pub async fn delete_task(&self, request: DeleteTaskRequest) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .delete_task(request)
            .await?;
        Ok(())
    }

    /// announce_host announces the host to the scheduler.
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
                    .buffer_size(super::BUFFER_SIZE)
                    .connect_timeout(super::CONNECT_TIMEOUT)
                    .timeout(super::REQUEST_TIMEOUT)
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

    /// init_announce_host announces the host to the scheduler.
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
                    .buffer_size(super::BUFFER_SIZE)
                    .connect_timeout(super::CONNECT_TIMEOUT)
                    .timeout(super::REQUEST_TIMEOUT)
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

    /// delete_host tells the scheduler that the host is deleting.
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
                    .buffer_size(super::BUFFER_SIZE)
                    .connect_timeout(super::CONNECT_TIMEOUT)
                    .timeout(super::REQUEST_TIMEOUT)
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

    /// announce_cache_peer announces the cache peer to the scheduler.
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

    /// stat_cache_peer gets the status of the cache peer.
    #[instrument(skip(self))]
    pub async fn stat_cache_peer(&self, request: StatCachePeerRequest) -> Result<CachePeer> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        let response = self
            .client(task_id.as_str(), None)
            .await?
            .stat_cache_peer(request)
            .await?;
        Ok(response.into_inner())
    }

    /// delete_cache_peer tells the scheduler that the cache peer is deleting.
    #[instrument(skip(self))]
    pub async fn delete_cache_peer(&self, request: DeleteCachePeerRequest) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .delete_cache_peer(request)
            .await?;
        Ok(())
    }

    /// upload_cache_task_started uploads the metadata of the cache task started.
    #[instrument(skip(self))]
    pub async fn upload_cache_task_started(
        &self,
        request: UploadCacheTaskStartedRequest,
    ) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .upload_cache_task_started(request)
            .await?;
        Ok(())
    }

    /// upload_cache_task_finished uploads the metadata of the cache task finished.
    #[instrument(skip_all)]
    pub async fn upload_cache_task_finished(
        &self,
        request: UploadCacheTaskFinishedRequest,
    ) -> Result<CacheTask> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        let response = self
            .client(task_id.as_str(), None)
            .await?
            .upload_cache_task_finished(request)
            .await?;
        Ok(response.into_inner())
    }

    /// upload_cache_task_failed uploads the metadata of the cache task failed.
    #[instrument(skip_all)]
    pub async fn upload_cache_task_failed(
        &self,
        request: UploadCacheTaskFailedRequest,
    ) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .upload_cache_task_failed(request)
            .await?;
        Ok(())
    }

    /// stat_cache_task gets the status of the cache task.
    #[instrument(skip(self))]
    pub async fn stat_cache_task(&self, request: StatCacheTaskRequest) -> Result<CacheTask> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        let response = self
            .client(task_id.as_str(), None)
            .await?
            .stat_cache_task(request)
            .await?;
        Ok(response.into_inner())
    }

    /// delete_cache_task tells the scheduler that the cache task is deleting.
    #[instrument(skip(self))]
    pub async fn delete_cache_task(&self, request: DeleteCacheTaskRequest) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .delete_cache_task(request)
            .await?;
        Ok(())
    }

    /// client gets the grpc client of the scheduler.
    #[instrument(skip(self))]
    async fn client(
        &self,
        task_id: &str,
        peer_id: Option<&str>,
    ) -> Result<SchedulerGRPCClient<Channel>> {
        // Update scheduler addresses of the client.
        self.update_available_scheduler_addrs().await?;

        // First try to get the address from the task_id.
        let addrs = self.hashring.read().await;
        let addr = *addrs
            .get(&task_id[0..5].to_string())
            .ok_or_else(|| Error::HashRing(task_id.to_string()))?;
        drop(addrs);
        info!("first picked {:?}", addr);

        match Channel::from_shared(format!("http://{}", addr))
            .map_err(|_| Error::InvalidURI(addr.to_string()))?
            .buffer_size(super::BUFFER_SIZE)
            .connect_timeout(super::CONNECT_TIMEOUT)
            .timeout(super::REQUEST_TIMEOUT)
            .tcp_keepalive(Some(super::TCP_KEEPALIVE))
            .http2_keep_alive_interval(super::HTTP2_KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT)
            .connect()
            .await
        {
            Ok(channel) => {
                let mut unavailable_scheduler_addrs =
                    self.unavailable_scheduler_addrs.write().await;
                unavailable_scheduler_addrs.remove(&addr.addr);
                drop(unavailable_scheduler_addrs);
                return Ok(SchedulerGRPCClient::new(channel)
                    .max_decoding_message_size(usize::MAX)
                    .max_encoding_message_size(usize::MAX));
            }

            Err(err) => {
                error!("connect to {} failed: {}", addr.to_string(), err);
                let mut unavailable_scheduler_addrs =
                    self.unavailable_scheduler_addrs.write().await;
                unavailable_scheduler_addrs
                    .entry(addr.addr)
                    .or_insert_with(Instant::now);
                drop(unavailable_scheduler_addrs);
            }
        }

        // Read the shooting configuration items.
        let config = self.dynconfig.get_config().await;
        let cooldown_interval = config.scheduler.cooldown_interval;
        let max_attempts = config.scheduler.max_attempts;
        let refresh_threshold = config.scheduler.refresh_threshold;
        let mut attempts = 1;
        let mut refresh_count = 1;

        // Traverse through available scheduler collections.
        let hashring = self.hashring.read().await;
        for vnode in hashring.clone().into_iter() {
            let scheduler_addr = vnode.addr;
            let unavailable_scheduler_addrs = self.unavailable_scheduler_addrs.read().await;
            if let Some(&instant) = unavailable_scheduler_addrs.get(&scheduler_addr) {
                if instant.elapsed() < cooldown_interval {
                    continue;
                }
            }
            drop(unavailable_scheduler_addrs);
            info!("try to picked {:?}", scheduler_addr);
            match self.check_scheduler(&scheduler_addr).await {
                Ok(channel) => {
                    let mut unavailable_scheduler_addrs =
                        self.unavailable_scheduler_addrs.write().await;
                    unavailable_scheduler_addrs.remove(&scheduler_addr);
                    drop(unavailable_scheduler_addrs);
                    info!("finally picked {:?}", scheduler_addr);
                    return Ok(SchedulerGRPCClient::new(channel)
                        .max_decoding_message_size(usize::MAX)
                        .max_encoding_message_size(usize::MAX));
                }
                Err(err) => {
                    error!("scheduler {} is not available: {}", scheduler_addr, err);
                    let mut unavailable_scheduler_addrs =
                        self.unavailable_scheduler_addrs.write().await;
                    unavailable_scheduler_addrs
                        .entry(scheduler_addr)
                        .or_insert_with(Instant::now);
                    drop(unavailable_scheduler_addrs);

                    attempts += 1;
                    refresh_count += 1;

                    if refresh_count >= refresh_threshold {
                        if let Err(err) = self.refresh_available_scheduler_addrs().await {
                            error!("failed to refresh scheduler client: {}", err);
                        };
                        refresh_count = 0;
                    }

                    if attempts >= max_attempts {
                        return Err(Error::ExceededMaxAttempts);
                    }
                }
            }
        }

        Err(Error::AvailableSchedulersNotFound)
    }

    // Check the health of the scheduler.
    async fn check_scheduler(&self, scheduler_addr: &SocketAddr) -> Result<Channel> {
        let addr = format!("http://{}:{}", scheduler_addr.ip(), scheduler_addr.port());
        let health_client = match HealthClient::new(&addr).await {
            Ok(client) => client,
            Err(err) => {
                error!(
                    "create health client for scheduler {}:{} failed: {}",
                    scheduler_addr.ip(),
                    scheduler_addr.port(),
                    err
                );
                return Err(ExternalError::new(ErrorType::ConnectError)
                    .with_cause(Box::new(err))
                    .into());
            }
        };

        match health_client.check().await {
            Ok(resp) => {
                if resp.status == ServingStatus::Serving as i32 {
                    return Channel::from_shared(format!("http://{}", scheduler_addr))
                        .map_err(|_| Error::InvalidURI(scheduler_addr.to_string()))?
                        .connect_timeout(super::CONNECT_TIMEOUT)
                        .connect()
                        .await
                        .map_err(|err| {
                            error!("connect to {} failed: {}", scheduler_addr.to_string(), err);
                            ExternalError::new(ErrorType::ConnectError)
                                .with_cause(Box::new(err))
                                .into()
                        });
                }

                Err(Error::SchedulerNotServing)
            }
            Err(err) => {
                error!("check scheduler health failed: {}", err);
                Err(ExternalError::new(ErrorType::ConnectError)
                    .with_cause(Box::new(err))
                    .into())
            }
        }
    }

    /// update_available_scheduler_addrs updates the addresses of available schedulers.
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

        // Clean the unavailable scheduler addrs.
        let mut unavailable_scheduler_addrs = self.unavailable_scheduler_addrs.write().await;
        unavailable_scheduler_addrs.clear();
        drop(unavailable_scheduler_addrs);

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

    /// refresh_available_scheduler_addrs refreshes addresses of available schedulers.
    #[instrument(skip(self))]
    async fn refresh_available_scheduler_addrs(&self) -> Result<()> {
        // Refresh the dynamic configuration.
        self.dynconfig.refresh().await?;

        // Update scheduler addresses of the client.
        self.update_available_scheduler_addrs().await
    }

    /// make_request creates a new request with timeout.
    #[instrument(skip_all)]
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}

// test
#[cfg(test)]
mod tests {
    use super::*;
    use mockall::mock;
    use mockall::predicate;
    use mockall::predicate::always;
    use tokio::time::Duration;
    use tonic::transport::Channel;

    // Only mock the methods used in the client function.
    mock! {
        pub SchedulerClient {
            async fn check_scheduler(&self, scheduler_addr: &SocketAddr) -> Result<Channel>;
            async fn update_available_scheduler_addrs(&self) -> Result<()>;
            async fn refresh_available_scheduler_addrs(&self) -> Result<()>;
        }
    }

    mock! {
        pub Dynconfig {
            pub async fn get_config(&self) -> Arc<SchedulerConfig>;
        }
    }

    #[derive(Clone)]
    pub struct SchedulerConfig {
        pub cooldown_interval: Duration,
        pub max_attempts: u32,
        pub refresh_threshold: u32,
    }

    // Reuse the Client logic in the Scheduler, retaining only the structural fields necessary for testing.
    #[derive(Clone)]
    pub struct SchedulerClient {
        // available_scheduler_addrs is the addresses of available schedulers.
        available_scheduler_addrs: Arc<RwLock<Vec<SocketAddr>>>,

        // hashring is the hashring of the scheduler.
        hashring: Arc<RwLock<HashRing<VNode>>>,

        // unavailable_scheduler_addrs is a map of unavailable scheduler addrs and the time they were marked as unavailable.
        unavailable_scheduler_addrs: Arc<RwLock<HashMap<SocketAddr, Instant>>>,
    }

    impl SchedulerClient {
        // client gets the grpc client of the scheduler.
        async fn client(
            &self,
            task_id: &str,
            _peer_id: Option<&str>,
            mock_scheduler_client: &MockSchedulerClient,
            mock_dynconfig: &MockDynconfig,
        ) -> Result<SchedulerGRPCClient<Channel>> {
            // Update scheduler addresses of the client.
            mock_scheduler_client
                .update_available_scheduler_addrs()
                .await?;

            // First try to get the address from the task_id.
            let addrs = self.hashring.read().await;
            let addr = *addrs
                .get(&task_id[0..5].to_string())
                .ok_or_else(|| Error::HashRing(task_id.to_string()))?;
            drop(addrs);

            match Channel::from_shared(format!("http://{}", addr))
                .map_err(|_| Error::InvalidURI(addr.to_string()))?
                .connect_timeout(super::super::CONNECT_TIMEOUT)
                .connect()
                .await
            {
                Ok(channel) => {
                    let mut unavailable_scheduler_addrs =
                        self.unavailable_scheduler_addrs.write().await;
                    unavailable_scheduler_addrs.remove(&addr.addr);
                    drop(unavailable_scheduler_addrs);
                    return Ok(SchedulerGRPCClient::new(channel)
                        .max_decoding_message_size(usize::MAX)
                        .max_encoding_message_size(usize::MAX));
                }

                Err(err) => {
                    error!("connect to {} failed: {}", addr.to_string(), err);
                    let mut unavailable_scheduler_addrs =
                        self.unavailable_scheduler_addrs.write().await;
                    unavailable_scheduler_addrs
                        .entry(addr.addr)
                        .or_insert_with(Instant::now);
                    drop(unavailable_scheduler_addrs);
                }
            }

            // Read the shooting configuration items.
            let config = mock_dynconfig.get_config().await;
            let cooldown_interval = config.cooldown_interval;
            let max_attempts = config.max_attempts;
            let refresh_threshold = config.refresh_threshold;
            let mut attempts = 1;
            let mut refresh_count = 1;

            // Traverse through available scheduler collections.
            let hashring = self.hashring.read().await;
            for vnode in hashring.clone().into_iter() {
                let scheduler_addr = vnode.addr;
                let unavailable_scheduler_addrs = self.unavailable_scheduler_addrs.read().await;
                if let Some(&instant) = unavailable_scheduler_addrs.get(&scheduler_addr) {
                    if instant.elapsed() < cooldown_interval {
                        continue;
                    }
                }
                drop(unavailable_scheduler_addrs);
                match mock_scheduler_client.check_scheduler(&scheduler_addr).await {
                    Ok(channel) => {
                        let mut unavailable_scheduler_addrs =
                            self.unavailable_scheduler_addrs.write().await;
                        unavailable_scheduler_addrs.remove(&scheduler_addr);
                        drop(unavailable_scheduler_addrs);
                        return Ok(SchedulerGRPCClient::new(channel)
                            .max_decoding_message_size(usize::MAX)
                            .max_encoding_message_size(usize::MAX));
                    }
                    Err(err) => {
                        error!("scheduler {} is not available: {}", scheduler_addr, err);
                        let mut unavailable_scheduler_addrs =
                            self.unavailable_scheduler_addrs.write().await;
                        unavailable_scheduler_addrs
                            .entry(scheduler_addr)
                            .or_insert_with(Instant::now);
                        drop(unavailable_scheduler_addrs);

                        attempts += 1;
                        refresh_count += 1;

                        if refresh_count >= refresh_threshold {
                            if let Err(err) = mock_scheduler_client
                                .refresh_available_scheduler_addrs()
                                .await
                            {
                                error!("failed to refresh scheduler client: {}", err);
                            };
                            refresh_count = 0;
                        }

                        if attempts >= max_attempts {
                            return Err(Error::ExceededMaxAttempts);
                        }
                    }
                }
            }
            Err(Error::AvailableSchedulersNotFound)
        }
    }

    async fn setup() -> (SchedulerClient, MockSchedulerClient, MockDynconfig) {
        // Mock the SchedulerClient::check_scheduler().
        let mock_scheduler_client = MockSchedulerClient::new();

        // Mock the Dynconfig::get_config().
        let mock_dynconfig = MockDynconfig::new();

        let available_scheduler_addrs = vec![
            "192.168.1.1:8080".parse().unwrap(),
            "192.168.2.2:8080".parse().unwrap(),
            "192.168.2.3:8080".parse().unwrap(),
            "192.168.2.4:8080".parse().unwrap(),
            "192.168.2.5:8080".parse().unwrap(),
            "192.168.2.6:8080".parse().unwrap(),
            "192.168.2.7:8080".parse().unwrap(),
            "192.168.2.8:8080".parse().unwrap(),
            "192.168.2.9:8080".parse().unwrap(),
            "192.168.2.10:8080".parse().unwrap(),
            "127.0.0.1:8080".parse().unwrap(),
        ];
        let mut ring: HashRing<VNode> = HashRing::new();
        let nodes: Vec<VNode> = available_scheduler_addrs
            .iter()
            .map(|&addr| VNode { addr })
            .collect();
        for node in nodes {
            ring.add(node);
        }
        let unavailable_scheduler_addrs: HashMap<SocketAddr, Instant> = HashMap::new();

        let scheduler_client = SchedulerClient {
            available_scheduler_addrs: Arc::new(RwLock::new(available_scheduler_addrs)),
            hashring: Arc::new(RwLock::new(ring)),
            unavailable_scheduler_addrs: Arc::new(RwLock::new(unavailable_scheduler_addrs)),
        };

        (scheduler_client, mock_scheduler_client, mock_dynconfig)
    }

    #[tokio::test]
    async fn test_client_exceeds_max_attempts() {
        let (scheduler_client, mut mock_scheduler_client, mut mock_dynconfig) = setup().await;

        mock_scheduler_client
            .expect_check_scheduler()
            .with(predicate::eq(
                "127.0.0.1:8080".parse::<SocketAddr>().unwrap(),
            ))
            .returning(|_| {
                let channel = Channel::from_static("").connect_lazy();
                Ok(channel)
            });

        mock_scheduler_client
            .expect_check_scheduler()
            .with(predicate::function(|addr: &SocketAddr| {
                addr.ip().to_string().starts_with("192.168")
            }))
            .returning(|_| Err(Error::SchedulerNotServing));

        mock_dynconfig.expect_get_config().returning(|| {
            Arc::new(SchedulerConfig {
                cooldown_interval: Duration::from_secs(60),
                max_attempts: 3,
                refresh_threshold: 10,
            })
        });

        // Mock the SchedulerClient::update_available_scheduler_addrs().
        mock_scheduler_client
            .expect_update_available_scheduler_addrs()
            .returning(|| Ok(()));

        // Mock the SchedulerClient::refresh_available_scheduler_addrs().
        mock_scheduler_client
            .expect_refresh_available_scheduler_addrs()
            .times(0)
            .returning(|| Ok(()));

        let task_id = "task12345";
        let peer_id = Some("peer12345");
        match scheduler_client
            .client(task_id, peer_id, &mock_scheduler_client, &mock_dynconfig)
            .await
        {
            Ok(_) => {
                panic!("Client should not connect successfully.");
            }
            Err(e) => match e {
                Error::ExceededMaxAttempts => {}
                _ => {
                    panic!("Unexpected error type.");
                }
            },
        }
    }

    #[tokio::test]
    async fn test_available_schedulers_not_found() {
        let (scheduler_client, mut mock_scheduler_client, mut mock_dynconfig) = setup().await;

        let mut call_count = 0;
        mock_scheduler_client
            .expect_check_scheduler()
            .with(always())
            .returning_st(move |_| {
                call_count += 1;
                if call_count <= 12 {
                    Err(Error::SchedulerNotServing)
                } else {
                    Ok(Channel::from_static("http://localhost:8080").connect_lazy())
                }
            });

        mock_dynconfig.expect_get_config().returning(|| {
            Arc::new(SchedulerConfig {
                cooldown_interval: Duration::from_secs(60),
                max_attempts: 100,
                refresh_threshold: 10,
            })
        });

        // Mock the SchedulerClient::update_available_scheduler_addrs().
        mock_scheduler_client
            .expect_update_available_scheduler_addrs()
            .returning(|| Ok(()));

        // Mock the SchedulerClient::refresh_available_scheduler_addrs().
        mock_scheduler_client
            .expect_refresh_available_scheduler_addrs()
            .returning(|| Ok(()));

        let task_id = "task12345";
        let peer_id = Some("peer12345");

        // Step 1: Simulate a situation where all schedulers are unavailable.
        match scheduler_client
            .client(task_id, peer_id, &mock_scheduler_client, &mock_dynconfig)
            .await
        {
            Ok(_) => {
                panic!("Client should not connect successfully.");
            }
            Err(e) => match e {
                Error::AvailableSchedulersNotFound => {}
                _ => {
                    panic!("Unexpected error type.");
                }
            },
        }

        // Step 2: Verify if unavailable scheduler_addrs have stored content.
        let available_addrs = scheduler_client.available_scheduler_addrs.read().await;
        let unavailable_addrs = scheduler_client.unavailable_scheduler_addrs.read().await;
        for addr in available_addrs.iter() {
            assert!(
                unavailable_addrs.contains_key(addr),
                "Address {:?} is not in unavailable_scheduler_addrs",
                addr
            );
        }
        drop(available_addrs);
        drop(unavailable_addrs);

        // Step: 3: Simulate a update operation to clear unavailable scheduler_addrs.
        let mut unavailable_addrs = scheduler_client.unavailable_scheduler_addrs.write().await;
        unavailable_addrs.clear();
        drop(unavailable_addrs);

        // Step: 4: Verify that the scheduler is available.
        match scheduler_client
            .client(task_id, peer_id, &mock_scheduler_client, &mock_dynconfig)
            .await
        {
            Ok(_) => {}
            Err(_e) => {
                panic!("Client should connect successfully.")
            }
        }
    }

    #[tokio::test]
    async fn test_available_schedulers_concurrent() {
        let (scheduler_client, mut mock_scheduler_client, mut mock_dynconfig) = setup().await;

        mock_dynconfig.expect_get_config().returning(|| {
            Arc::new(SchedulerConfig {
                cooldown_interval: Duration::from_secs(60),
                max_attempts: 100,
                refresh_threshold: 10,
            })
        });

        mock_scheduler_client
            .expect_update_available_scheduler_addrs()
            .returning(|| Ok(()));

        mock_scheduler_client
            .expect_refresh_available_scheduler_addrs()
            .returning(|| Ok(()));

        let mut call_count = 0;
        mock_scheduler_client
            .expect_check_scheduler()
            .with(always())
            .times(1100)
            .returning_st(move |_| {
                call_count += 1;
                if call_count <= 1000 {
                    Err(Error::SchedulerNotServing)
                } else {
                    Ok(Channel::from_static("http://localhost:8080").connect_lazy())
                }
            });

        let mock_dynconfig = Arc::new(mock_dynconfig);
        let scheduler_client = Arc::new(scheduler_client);
        let mock_scheduler_client = Arc::new(mock_scheduler_client);

        let task_id = "12345";
        let peer_id = Some("peer12345");

        let handles: Vec<_> = (0..100)
            .map(|_| {
                let scheduler_client = Arc::clone(&scheduler_client);
                let mock_scheduler_client = Arc::clone(&mock_scheduler_client);
                let mock_dynconfig = Arc::clone(&mock_dynconfig);
                let task_id = task_id.to_string();
                let peer_id = peer_id.map(|id| id.to_string());

                tokio::spawn(async move {
                    // Step 1: Simulate a situation where all schedulers are unavailable.
                    match scheduler_client
                        .client(
                            &task_id,
                            peer_id.as_deref(),
                            &mock_scheduler_client,
                            &mock_dynconfig,
                        )
                        .await
                    {
                        Ok(_) => {
                            panic!("Client should not connect successfully.");
                        }
                        Err(e) => match e {
                            Error::AvailableSchedulersNotFound => {}
                            _ => {
                                panic!("Unexpected error type.");
                            }
                        },
                    }

                    // Step 2: Verify if unavailable scheduler_addrs have stored content.
                    let available_addrs = scheduler_client.available_scheduler_addrs.read().await;
                    let unavailable_addrs =
                        scheduler_client.unavailable_scheduler_addrs.read().await;
                    for addr in available_addrs.iter() {
                        assert!(
                            unavailable_addrs.contains_key(addr),
                            "Address {:?} is not in unavailable_scheduler_addrs.",
                            addr
                        );
                    }
                    drop(available_addrs);
                    drop(unavailable_addrs);

                    // Step 3: Simulate an update operation to clear unavailable scheduler_addrs.
                    let mut unavailable_addrs =
                        scheduler_client.unavailable_scheduler_addrs.write().await;
                    unavailable_addrs.clear();
                    drop(unavailable_addrs);

                    // Step 4: Verify that all tasks can connect to the scheduler.
                    match scheduler_client
                        .client(
                            &task_id,
                            peer_id.as_deref(),
                            &mock_scheduler_client,
                            &mock_dynconfig,
                        )
                        .await
                    {
                        Ok(_) => {}
                        Err(_e) => {
                            panic!("Client should connect successfully.")
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            if let Err(_e) = handle.await {
                panic!("One of the tasks panicked.");
            }
        }
    }
}
