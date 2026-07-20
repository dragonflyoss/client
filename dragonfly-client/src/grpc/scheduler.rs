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
    Peer, PersistentCachePeer, PersistentCacheTask, PersistentPeer, PersistentTask, Task,
};
use dragonfly_api::manager::v2::Scheduler;
use dragonfly_api::scheduler::v2::{
    scheduler_client::SchedulerClient as SchedulerGRPCClient, AnnounceHostRequest,
    AnnouncePeerRequest, AnnouncePeerResponse, AnnouncePersistentCachePeerRequest,
    AnnouncePersistentCachePeerResponse, AnnouncePersistentPeerRequest,
    AnnouncePersistentPeerResponse, DeleteHostRequest, DeletePeerRequest,
    DeletePersistentCachePeerRequest, DeletePersistentCacheTaskRequest,
    DeletePersistentPeerRequest, DeletePersistentTaskRequest, DeleteTaskRequest, StatPeerRequest,
    StatPersistentCachePeerRequest, StatPersistentCacheTaskRequest, StatPersistentPeerRequest,
    StatPersistentTaskRequest, StatTaskRequest, UploadPersistentCacheTaskFailedRequest,
    UploadPersistentCacheTaskFinishedRequest, UploadPersistentCacheTaskStartedRequest,
    UploadPersistentTaskFailedRequest, UploadPersistentTaskFinishedRequest,
    UploadPersistentTaskStartedRequest,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::{Error, Result};
use hashring::HashRing;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tracing::{debug, error, info, instrument, Instrument};
use url::Url;

use super::interceptor::InjectTracingInterceptor;

/// The number of connections per scheduler address. Requests are balanced over
/// a pool of connections to avoid the head-of-line blocking and congestion
/// window limit of a single TCP connection, and the concurrent stream limit
/// that a server or an intermediate proxy may advertise.
const CONNECTION_POOL_SIZE: usize = 10;

/// Virtual node of the hashring.
#[derive(Debug, Copy, Clone, Hash, PartialEq)]
struct VNode {
    /// Address of the virtual node.
    addr: SocketAddr,
}

/// Implements the Display trait.
impl std::fmt::Display for VNode {
    /// Formats the virtual node.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}

/// Wrapper around the gRPC scheduler client.
#[derive(Clone)]
pub struct SchedulerClient {
    /// Configuration of the dfdaemon.
    config: Arc<Config>,

    /// Dynamic configuration of the dfdaemon.
    dynconfig: Arc<Dynconfig>,

    /// Available schedulers.
    available_schedulers: Arc<RwLock<Vec<Scheduler>>>,

    /// Addresses of available schedulers.
    available_scheduler_addrs: Arc<RwLock<Vec<SocketAddr>>>,

    /// Hashring of the scheduler.
    hashring: Arc<RwLock<HashRing<VNode>>>,

    /// Channels of the available schedulers, keyed by the scheduler address.
    channels: Arc<RwLock<HashMap<SocketAddr, Channel>>>,
}

/// Implements the grpc client of the scheduler.
impl SchedulerClient {
    /// Creates a new scheduler client.
    pub async fn new(config: Arc<Config>, dynconfig: Arc<Dynconfig>) -> Result<Self> {
        let client = Self {
            config,
            dynconfig,
            available_schedulers: Arc::new(RwLock::new(Vec::new())),
            available_scheduler_addrs: Arc::new(RwLock::new(Vec::new())),
            hashring: Arc::new(RwLock::new(HashRing::new())),
            channels: Arc::new(RwLock::new(HashMap::new())),
        };

        client.refresh_available_scheduler_addrs().await?;
        Ok(client)
    }

    /// Announces the peer to the scheduler.
    #[instrument(level = "debug", skip_all)]
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

    /// Gets the status of the peer.
    #[instrument(level = "debug", skip(self))]
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

    /// Tells the scheduler that the peer is deleting.
    #[instrument(level = "debug", skip(self))]
    pub async fn delete_peer(&self, request: DeletePeerRequest) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .delete_peer(request)
            .await?;
        Ok(())
    }

    /// Gets the status of the task.
    #[instrument(level = "debug", skip(self))]
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

    /// Tells the scheduler that the task is deleting.
    #[instrument(level = "debug", skip(self))]
    pub async fn delete_task(&self, request: DeleteTaskRequest) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .delete_task(request)
            .await?;
        Ok(())
    }

    /// Announces the host to the scheduler.
    #[instrument(level = "debug", skip(self))]
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
                scheduler_client: SchedulerClient,
                addr: SocketAddr,
                request: tonic::Request<AnnounceHostRequest>,
            ) -> Result<()> {
                debug!("announce host to {}", addr);

                // Reuse the cached channel of the scheduler.
                let channel = scheduler_client.channel(addr).await?;
                let mut client =
                    SchedulerGRPCClient::with_interceptor(channel, InjectTracingInterceptor)
                        .max_decoding_message_size(usize::MAX)
                        .max_encoding_message_size(usize::MAX);
                client.announce_host(request).await?;
                Ok(())
            }

            join_set.spawn(
                announce_host(self.clone(), *available_scheduler_addr, request).in_current_span(),
            );
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

    /// Announces the host to the scheduler.
    #[instrument(level = "debug", skip(self))]
    pub async fn init_announce_host(&self, request: AnnounceHostRequest) -> Result<()> {
        let mut join_set = JoinSet::new();
        let available_scheduler_addrs = self.available_scheduler_addrs.read().await;
        let available_scheduler_addrs_clone = available_scheduler_addrs.clone();
        drop(available_scheduler_addrs);

        for available_scheduler_addr in available_scheduler_addrs_clone.iter() {
            let request = Self::make_request(request.clone());
            async fn announce_host(
                scheduler_client: SchedulerClient,
                addr: SocketAddr,
                request: tonic::Request<AnnounceHostRequest>,
            ) -> Result<()> {
                info!("announce host to {:?}", addr);

                // Reuse the cached channel of the scheduler.
                let channel = scheduler_client.channel(addr).await?;
                let mut client =
                    SchedulerGRPCClient::with_interceptor(channel, InjectTracingInterceptor)
                        .max_decoding_message_size(usize::MAX)
                        .max_encoding_message_size(usize::MAX);
                client.announce_host(request).await?;
                Ok(())
            }

            join_set.spawn(
                announce_host(self.clone(), *available_scheduler_addr, request).in_current_span(),
            );
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

    /// Tells the scheduler that the host is deleting.
    #[instrument(level = "debug", skip(self))]
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
                scheduler_client: SchedulerClient,
                addr: SocketAddr,
                request: tonic::Request<DeleteHostRequest>,
            ) -> Result<()> {
                info!("delete host from {}", addr);

                // Reuse the cached channel of the scheduler.
                let channel = scheduler_client.channel(addr).await?;
                let mut client =
                    SchedulerGRPCClient::with_interceptor(channel, InjectTracingInterceptor)
                        .max_decoding_message_size(usize::MAX)
                        .max_encoding_message_size(usize::MAX);
                client.delete_host(request).await?;
                Ok(())
            }

            join_set.spawn(
                delete_host(self.clone(), *available_scheduler_addr, request).in_current_span(),
            );
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

    /// Announces the persistent peer to the scheduler.
    #[instrument(level = "debug", skip_all)]
    pub async fn announce_persistent_peer(
        &self,
        task_id: &str,
        peer_id: &str,
        request: impl tonic::IntoStreamingRequest<Message = AnnouncePersistentPeerRequest>,
    ) -> Result<tonic::Response<tonic::codec::Streaming<AnnouncePersistentPeerResponse>>> {
        let response = self
            .client(task_id, Some(peer_id))
            .await?
            .announce_persistent_peer(request)
            .await?;
        Ok(response)
    }

    /// Gets the status of the persistent peer.
    #[instrument(level = "debug", skip(self))]
    pub async fn stat_persistent_peer(
        &self,
        request: StatPersistentPeerRequest,
    ) -> Result<PersistentPeer> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        let response = self
            .client(task_id.as_str(), None)
            .await?
            .stat_persistent_peer(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Tells the scheduler that the persistent peer is deleting.
    #[instrument(level = "debug", skip(self))]
    pub async fn delete_persistent_peer(&self, request: DeletePersistentPeerRequest) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .delete_persistent_peer(request)
            .await?;
        Ok(())
    }

    /// Uploads the metadata of the persistent task started.
    #[instrument(level = "debug", skip(self))]
    pub async fn upload_persistent_task_started(
        &self,
        request: UploadPersistentTaskStartedRequest,
    ) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .upload_persistent_task_started(request)
            .await?;
        Ok(())
    }

    /// Uploads the metadata of the persistent task finished.
    #[instrument(level = "debug", skip_all)]
    pub async fn upload_persistent_task_finished(
        &self,
        request: UploadPersistentTaskFinishedRequest,
    ) -> Result<PersistentTask> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        let response = self
            .client(task_id.as_str(), None)
            .await?
            .upload_persistent_task_finished(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Uploads the metadata of the persistent task failed.
    #[instrument(level = "debug", skip_all)]
    pub async fn upload_persistent_task_failed(
        &self,
        request: UploadPersistentTaskFailedRequest,
    ) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .upload_persistent_task_failed(request)
            .await?;
        Ok(())
    }

    /// Gets the status of the persistent task.
    #[instrument(level = "debug", skip(self))]
    pub async fn stat_persistent_task(
        &self,
        request: StatPersistentTaskRequest,
    ) -> Result<PersistentTask> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        let response = self
            .client(task_id.as_str(), None)
            .await?
            .stat_persistent_task(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Tells the scheduler that the persistent task is deleting.
    #[instrument(level = "debug", skip(self))]
    pub async fn delete_persistent_task(&self, request: DeletePersistentTaskRequest) -> Result<()> {
        let task_id = request.task_id.clone();
        let request = Self::make_request(request);
        self.client(task_id.as_str(), None)
            .await?
            .delete_persistent_task(request)
            .await?;
        Ok(())
    }

    /// Announces the persistent cache peer to the scheduler.
    #[instrument(level = "debug", skip_all)]
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

    /// Gets the status of the persistent cache peer.
    #[instrument(level = "debug", skip(self))]
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

    /// Tells the scheduler that the persistent cache peer is deleting.
    #[instrument(level = "debug", skip(self))]
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

    /// Uploads the metadata of the persistent cache task started.
    #[instrument(level = "debug", skip(self))]
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

    /// Uploads the metadata of the persistent cache task finished.
    #[instrument(level = "debug", skip_all)]
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

    /// Uploads the metadata of the persistent cache task failed.
    #[instrument(level = "debug", skip_all)]
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

    /// Gets the status of the persistent cache task.
    #[instrument(level = "debug", skip(self))]
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

    /// Tells the scheduler that the persistent cache task is deleting.
    #[instrument(level = "debug", skip(self))]
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

    /// Gets the grpc client of the scheduler.
    #[instrument(level = "debug", skip(self))]
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
        debug!("picked {:?}", addr);

        let channel = self.channel(addr.addr).await?;
        Ok(
            SchedulerGRPCClient::with_interceptor(channel, InjectTracingInterceptor)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
        )
    }

    /// Returns the cached grpc channel of the scheduler address, connecting and
    /// caching it if it is absent. The channel balances requests over a pool of
    /// connections and reconnects on demand, so reusing it avoids a connection
    /// handshake for every task. Concurrent misses may build the channel more
    /// than once, the last inserted channel wins and the others are closed when
    /// their requests finish.
    async fn channel(&self, socket_addr: SocketAddr) -> Result<Channel> {
        if let Some(channel) = self.channels.read().await.get(&socket_addr) {
            return Ok(channel.clone());
        }

        let addr = format!("http://{socket_addr}");
        let domain_name = Url::parse(addr.as_str())?
            .host_str()
            .ok_or(Error::InvalidParameter)
            .inspect_err(|_err| {
                error!("invalid address: {}", addr);
            })?
            .to_string();

        let endpoint = match self
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
                .keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT),
            None => Channel::from_shared(addr.clone())
                .map_err(|_| Error::InvalidURI(addr.clone()))?
                .buffer_size(super::BUFFER_SIZE)
                .connect_timeout(super::CONNECT_TIMEOUT)
                .timeout(super::REQUEST_TIMEOUT)
                .tcp_keepalive(Some(super::TCP_KEEPALIVE))
                .http2_keep_alive_interval(super::HTTP2_KEEP_ALIVE_INTERVAL)
                .keep_alive_timeout(super::HTTP2_KEEP_ALIVE_TIMEOUT),
        };

        // Balance the requests over a pool of connections, since the scheduler
        // limits the concurrent streams of a single connection. The connections
        // are established on demand.
        let channel = Channel::balance_list((0..CONNECTION_POOL_SIZE).map(|_| endpoint.clone()));

        self.channels
            .write()
            .await
            .insert(socket_addr, channel.clone());
        Ok(channel)
    }

    /// Updates the addresses of available schedulers.
    #[instrument(level = "debug", skip(self))]
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
            debug!(
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

        // Remove the channels of the schedulers that are no longer available.
        let mut channels = self.channels.write().await;
        channels.retain(|addr, _| new_available_scheduler_addrs.contains(addr));
        drop(channels);

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

    /// Refreshes addresses of available schedulers.
    #[instrument(level = "debug", skip(self))]
    async fn refresh_available_scheduler_addrs(&self) -> Result<()> {
        // Refresh the dynamic configuration.
        self.dynconfig.refresh().await?;

        // Update scheduler addresses of the client.
        self.update_available_scheduler_addrs().await
    }

    /// Creates a new request with timeout.
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}
