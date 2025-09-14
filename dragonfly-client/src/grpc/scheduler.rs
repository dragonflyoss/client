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

use crate::dynconfig::Dynconfig;
use dragonfly_api::common::v2::{
    CachePeer, CacheTask, Peer, PersistentCachePeer, PersistentCacheTask, Task,
};
use dragonfly_api::manager::v2::Scheduler;
use dragonfly_api::scheduler::v2::{
    scheduler_client::SchedulerClient as SchedulerGRPCClient, AnnounceCachePeerRequest,
    AnnounceCachePeerResponse, AnnounceHostRequest, AnnouncePeerRequest, AnnouncePeerResponse,
    AnnouncePersistentCachePeerRequest, AnnouncePersistentCachePeerResponse,
    DeleteCachePeerRequest, DeleteCacheTaskRequest, DeleteHostRequest, DeletePeerRequest,
    DeletePersistentCachePeerRequest, DeletePersistentCacheTaskRequest, DeleteTaskRequest,
    StatCachePeerRequest, StatCacheTaskRequest, StatPeerRequest, StatPersistentCachePeerRequest,
    StatPersistentCacheTaskRequest, StatTaskRequest, UploadPersistentCacheTaskFailedRequest,
    UploadPersistentCacheTaskFinishedRequest, UploadPersistentCacheTaskStartedRequest,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::{Error, Result};
use hashring::HashRing;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tracing::{error, info, instrument, Instrument};
use url::Url;

use super::interceptor::InjectTracingInterceptor;

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
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// dynconfig is the dynamic configuration of the dfdaemon.
    dynconfig: Arc<Dynconfig>,

    /// available_schedulers is the available schedulers.
    available_schedulers: Arc<RwLock<Vec<Scheduler>>>,

    /// available_scheduler_addrs is the addresses of available schedulers.
    available_scheduler_addrs: Arc<RwLock<Vec<SocketAddr>>>,

    /// hashring is the hashring of the scheduler.
    hashring: Arc<RwLock<HashRing<VNode>>>,
}

/// SchedulerClient implements the grpc client of the scheduler.
impl SchedulerClient {
    /// new creates a new SchedulerClient.
    pub async fn new(config: Arc<Config>, dynconfig: Arc<Dynconfig>) -> Result<Self> {
        let client = Self {
            config,
            dynconfig,
            available_schedulers: Arc::new(RwLock::new(Vec::new())),
            available_scheduler_addrs: Arc::new(RwLock::new(Vec::new())),
            hashring: Arc::new(RwLock::new(HashRing::new())),
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
                    .inspect_err(|err| {
                        error!("connect to {} failed: {}", addr.to_string(), err);
                    })
                    .or_err(ErrorType::ConnectError)?;

                let mut client =
                    SchedulerGRPCClient::with_interceptor(channel, InjectTracingInterceptor)
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
                    .inspect_err(|err| {
                        error!("connect to {} failed: {}", addr.to_string(), err);
                    })
                    .or_err(ErrorType::ConnectError)?;

                let mut client =
                    SchedulerGRPCClient::with_interceptor(channel, InjectTracingInterceptor)
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
                    .inspect_err(|err| {
                        error!("connect to {} failed: {}", addr.to_string(), err);
                    })
                    .or_err(ErrorType::ConnectError)?;

                let mut client =
                    SchedulerGRPCClient::with_interceptor(channel, InjectTracingInterceptor)
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

    /// announce_persistent_cache_peer announces the persistent cache peer to the scheduler.
    #[instrument(skip_all)]
    pub async fn announce_persistent_cache_peer(
        &self,
        task_id: &str,
        peer_id: &str,
        request: impl tonic::IntoStreamingRequest<Message = AnnouncePersistentCachePeerRequest>,
    ) -> Result<tonic::Response<tonic::codec::Streaming<AnnouncePersistentCachePeerResponse>>> {
        let response = self
            .client(task_id, Some(peer_id))
            .await?
            .announce_persistent_cache_peer(request)
            .await?;
        Ok(response)
    }

    /// stat_persistent_cache_peer gets the status of the persistent cache peer.
    #[instrument(skip(self))]
    pub async fn stat_persistent_cache_peer(
        &self,
        request: StatPersistentCachePeerRequest,
    ) -> Result<PersistentCachePeer> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        let response = self
            .client(task_id.as_str(), None)
            .await?
            .stat_persistent_cache_peer(request)
            .await?;
        Ok(response.into_inner())
    }

    /// delete_persistent_cache_peer tells the scheduler that the persistent cache peer is deleting.
    #[instrument(skip(self))]
    pub async fn delete_persistent_cache_peer(
        &self,
        request: DeletePersistentCachePeerRequest,
    ) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .delete_persistent_cache_peer(request)
            .await?;
        Ok(())
    }

    /// upload_persistent_cache_task_started uploads the metadata of the persistent cache task started.
    #[instrument(skip(self))]
    pub async fn upload_persistent_cache_task_started(
        &self,
        request: UploadPersistentCacheTaskStartedRequest,
    ) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .upload_persistent_cache_task_started(request)
            .await?;
        Ok(())
    }

    /// upload_persistent_cache_task_finished uploads the metadata of the persistent cache task finished.
    #[instrument(skip_all)]
    pub async fn upload_persistent_cache_task_finished(
        &self,
        request: UploadPersistentCacheTaskFinishedRequest,
    ) -> Result<PersistentCacheTask> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        let response = self
            .client(task_id.as_str(), None)
            .await?
            .upload_persistent_cache_task_finished(request)
            .await?;
        Ok(response.into_inner())
    }

    /// upload_persistent_cache_task_failed uploads the metadata of the persistent cache task failed.
    #[instrument(skip_all)]
    pub async fn upload_persistent_cache_task_failed(
        &self,
        request: UploadPersistentCacheTaskFailedRequest,
    ) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .upload_persistent_cache_task_failed(request)
            .await?;
        Ok(())
    }

    /// stat_persistent_cache_task gets the status of the persistent cache task.
    #[instrument(skip(self))]
    pub async fn stat_persistent_cache_task(
        &self,
        request: StatPersistentCacheTaskRequest,
    ) -> Result<PersistentCacheTask> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        let response = self
            .client(task_id.as_str(), None)
            .await?
            .stat_persistent_cache_task(request)
            .await?;
        Ok(response.into_inner())
    }

    /// delete_persistent_cache_task tells the scheduler that the persistent cache task is deleting.
    #[instrument(skip(self))]
    pub async fn delete_persistent_cache_task(
        &self,
        request: DeletePersistentCacheTaskRequest,
    ) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .delete_persistent_cache_task(request)
            .await?;
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
    ) -> Result<SchedulerGRPCClient<InterceptedService<Channel, InjectTracingInterceptor>>> {
        // Update scheduler addresses of the client.
        self.update_available_scheduler_addrs().await?;

        // Get the scheduler address from the hashring.
        let addrs = self.hashring.read().await;
        let addr = *addrs
            .get(&task_id[0..5].to_string())
            .ok_or_else(|| Error::HashRing(task_id.to_string()))?;
        drop(addrs);
        info!("picked {:?}", addr);

        let addr = format!("http://{}", addr);
        let domain_name = Url::parse(addr.as_str())?
            .host_str()
            .ok_or(Error::InvalidParameter)
            .inspect_err(|_err| {
                error!("invalid address: {}", addr);
            })?
            .to_string();

        let channel = match self
            .config
            .scheduler
            .load_client_tls_config(domain_name.as_str())
            .await?
        {
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

        Ok(
            SchedulerGRPCClient::with_interceptor(channel, InjectTracingInterceptor)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
        )
    }

    /// update_available_scheduler_addrs updates the addresses of available schedulers.
    #[instrument(skip(self))]
    async fn update_available_scheduler_addrs(&self) -> Result<()> {
        // Get the endpoints of available schedulers.
        let data_available_schedulers_clone = {
            let data = self.dynconfig.data.read().await;
            data.available_schedulers.clone()
        };

        // Check if the available schedulers is empty.
        if data_available_schedulers_clone.is_empty() {
            return Err(Error::AvailableSchedulersNotFound);
        }

        // Get the available schedulers.
        let available_schedulers_clone = {
            let available_schedulers = self.available_schedulers.read().await;
            available_schedulers.clone()
        };

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
            let socket_addr = SocketAddr::new(ip, available_scheduler.port as u16);
            new_available_scheduler_addrs.push(socket_addr);

            // Add the scheduler to the hashring.
            new_hashring.add(VNode { addr: socket_addr });
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

    /// refresh_available_scheduler_addrs refreshes addresses of available schedulers.
    #[instrument(skip(self))]
    async fn refresh_available_scheduler_addrs(&self) -> Result<()> {
        // Refresh the dynamic configuration.
        self.dynconfig.refresh().await?;

        // Update scheduler addresses of the client.
        self.update_available_scheduler_addrs().await
    }

    /// make_request creates a new request with timeout.
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}
