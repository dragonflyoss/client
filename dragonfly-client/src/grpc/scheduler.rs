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
use dragonfly_hashring::VNodeHashRing;
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

/// Available schedulers keyed by hostname. The fields are replaced together on
/// every refresh, so a lookup never observes a hostname without an address.
struct AvailableSchedulers {
    /// Addresses of the schedulers keyed by hostname.
    addrs: HashMap<String, SocketAddr>,

    /// Hashring of the scheduler hostnames.
    hashring: VNodeHashRing,
}

/// Implements the available schedulers.
impl AvailableSchedulers {
    /// Creates the available schedulers from the addresses keyed by hostname.
    ///
    /// The hashring is keyed by hostname rather than address, so that a scheduler
    /// keeps its position when its ip changes, refer to
    /// https://github.com/grpc/proposal/blob/master/A76-ring-hash-improvements.md.
    fn new(addrs: HashMap<String, SocketAddr>) -> Self {
        let mut hashring = VNodeHashRing::new();
        for hostname in addrs.keys() {
            hashring.add(hostname.clone());
        }

        Self { addrs, hashring }
    }

    /// Returns the address of the scheduler responsible for the task, and `None`
    /// when no scheduler is available.
    fn addr(&self, task_id: &str) -> Option<SocketAddr> {
        self.hashring
            .get(task_id)
            .and_then(|vnode| self.addrs.get(vnode.name()))
            .copied()
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
    available_schedulers: Arc<RwLock<AvailableSchedulers>>,

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
            available_schedulers: Arc::new(RwLock::new(AvailableSchedulers::new(HashMap::new()))),
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
        for available_scheduler_addr in self.available_scheduler_addrs().await {
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
                announce_host(self.clone(), available_scheduler_addr, request).in_current_span(),
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
        for available_scheduler_addr in self.available_scheduler_addrs().await {
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
                announce_host(self.clone(), available_scheduler_addr, request).in_current_span(),
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
        for available_scheduler_addr in self.available_scheduler_addrs().await {
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
                delete_host(self.clone(), available_scheduler_addr, request).in_current_span(),
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
        let addr = self
            .available_schedulers
            .read()
            .await
            .addr(task_id)
            .ok_or_else(|| Error::HashRing(task_id.to_string()))?;
        info!("picked {}", addr);

        let channel = self.channel(addr).await?;
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

        let scheme = self.config.scheduler.scheme();
        let addr = format!("{scheme}://{socket_addr}");
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

    /// Returns the addresses of the available schedulers.
    async fn available_scheduler_addrs(&self) -> Vec<SocketAddr> {
        self.available_schedulers
            .read()
            .await
            .addrs
            .values()
            .copied()
            .collect()
    }

    /// Returns the addresses of the schedulers keyed by hostname. A hostname
    /// registered from several addresses keeps the most recently registered one,
    /// and a static scheduler without a hostname is keyed by its address.
    fn scheduler_addrs(schedulers: &[Scheduler]) -> HashMap<String, SocketAddr> {
        let mut schedulers: Vec<&Scheduler> = schedulers.iter().collect();
        schedulers.sort_by_key(|scheduler| scheduler.id);

        let mut addrs = HashMap::with_capacity(schedulers.len());
        for scheduler in schedulers {
            let ip = match IpAddr::from_str(&scheduler.ip) {
                Ok(ip) => ip,
                Err(err) => {
                    error!("failed to parse ip: {}", err);
                    continue;
                }
            };

            let addr = SocketAddr::new(ip, scheduler.port as u16);
            let hostname = if scheduler.hostname.is_empty() {
                addr.to_string()
            } else {
                scheduler.hostname.clone()
            };

            if let Some(stale_addr) = addrs.insert(hostname.clone(), addr) {
                debug!(
                    "scheduler {} registered from {} and {}, using {}",
                    hostname, stale_addr, addr, addr
                );
            }
        }

        addrs
    }

    /// Updates the addresses of available schedulers.
    #[instrument(level = "debug", skip(self))]
    async fn update_available_scheduler_addrs(&self) -> Result<()> {
        // Get the addresses of available schedulers.
        let addrs = {
            let data = self.dynconfig.data.read().await;
            if data.available_schedulers.is_empty() {
                return Err(Error::AvailableSchedulersNotFound);
            }

            Self::scheduler_addrs(&data.available_schedulers)
        };

        // Check if the available schedulers is not changed.
        if self.available_schedulers.read().await.addrs == addrs {
            debug!("available schedulers is not changed: {:?}", addrs);
            return Ok(());
        }

        // Remove the channels of the schedulers that are no longer available.
        self.channels
            .write()
            .await
            .retain(|addr, _| addrs.values().any(|available_addr| available_addr == addr));

        info!("refresh available scheduler addresses: {:?}", addrs);
        *self.available_schedulers.write().await = AvailableSchedulers::new(addrs);
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

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_hashring::DEFAULT_VNODES_PER_NODE;

    #[test]
    fn available_schedulers_keys_hashring_by_hostname() {
        let test_cases = vec![
            (
                vec![
                    Scheduler {
                        id: 1,
                        hostname: "scheduler-0".to_string(),
                        ip: "10.0.0.1".to_string(),
                        port: 8002,
                        ..Default::default()
                    },
                    Scheduler {
                        id: 2,
                        hostname: "scheduler-1".to_string(),
                        ip: "10.0.0.2".to_string(),
                        port: 8002,
                        ..Default::default()
                    },
                ],
                vec![
                    ("scheduler-0", "10.0.0.1:8002"),
                    ("scheduler-1", "10.0.0.2:8002"),
                ],
            ),
            (
                vec![
                    Scheduler {
                        id: 1,
                        hostname: "scheduler-0".to_string(),
                        ip: "10.0.0.1".to_string(),
                        port: 8002,
                        ..Default::default()
                    },
                    Scheduler {
                        id: 3,
                        hostname: "scheduler-0".to_string(),
                        ip: "10.0.0.3".to_string(),
                        port: 8002,
                        ..Default::default()
                    },
                ],
                vec![("scheduler-0", "10.0.0.3:8002")],
            ),
            (
                vec![
                    Scheduler {
                        id: 1,
                        hostname: "scheduler-0".to_string(),
                        ip: "10.0.0.1".to_string(),
                        port: 8002,
                        ..Default::default()
                    },
                    Scheduler {
                        id: 2,
                        hostname: "scheduler-1".to_string(),
                        ip: "invalid".to_string(),
                        port: 8002,
                        ..Default::default()
                    },
                ],
                vec![("scheduler-0", "10.0.0.1:8002")],
            ),
            (
                vec![
                    Scheduler {
                        ip: "10.0.0.1".to_string(),
                        port: 8002,
                        ..Default::default()
                    },
                    Scheduler {
                        ip: "10.0.0.2".to_string(),
                        port: 8002,
                        ..Default::default()
                    },
                ],
                vec![
                    ("10.0.0.1:8002", "10.0.0.1:8002"),
                    ("10.0.0.2:8002", "10.0.0.2:8002"),
                ],
            ),
        ];

        for (schedulers, expected) in test_cases {
            let available_schedulers =
                AvailableSchedulers::new(SchedulerClient::scheduler_addrs(&schedulers));
            let expected: HashMap<String, SocketAddr> = expected
                .into_iter()
                .map(|(name, addr)| (name.to_string(), addr.parse().unwrap()))
                .collect();

            assert_eq!(available_schedulers.addrs, expected);
            assert_eq!(
                available_schedulers.hashring.len(),
                expected.len() * DEFAULT_VNODES_PER_NODE
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn available_schedulers_are_replaced_together() {
        let scheduler = |id: u64, hostname: &str, ip: &str| Scheduler {
            id,
            hostname: hostname.to_string(),
            ip: ip.to_string(),
            port: 8002,
            ..Default::default()
        };
        let addrs = [
            SchedulerClient::scheduler_addrs(&[
                scheduler(1, "scheduler-0", "10.0.0.1"),
                scheduler(2, "scheduler-1", "10.0.0.2"),
            ]),
            SchedulerClient::scheduler_addrs(&[
                scheduler(3, "scheduler-2", "10.0.0.3"),
                scheduler(4, "scheduler-3", "10.0.0.4"),
                scheduler(5, "scheduler-4", "10.0.0.5"),
            ]),
            HashMap::new(),
        ];

        let available_schedulers = Arc::new(RwLock::new(AvailableSchedulers::new(HashMap::new())));
        let mut join_set = JoinSet::new();
        for i in 0..4 {
            let available_schedulers = available_schedulers.clone();
            let addrs = addrs.clone();
            join_set.spawn(async move {
                for j in 0..200 {
                    *available_schedulers.write().await =
                        AvailableSchedulers::new(addrs[(i + j) % addrs.len()].clone());
                    tokio::task::yield_now().await;
                }
            });
        }

        for i in 0..4 {
            let available_schedulers = available_schedulers.clone();
            let addrs = addrs.clone();
            join_set.spawn(async move {
                for j in 0..1000 {
                    let available_schedulers = available_schedulers.read().await;
                    let task_id = format!("task-{i}-{j}");
                    match available_schedulers.addr(&task_id) {
                        Some(addr) => {
                            assert!(addrs.iter().any(|addrs| addrs.values().any(|a| *a == addr)))
                        }
                        None => assert!(available_schedulers.hashring.is_empty()),
                    }
                    drop(available_schedulers);
                    tokio::task::yield_now().await;
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            result.unwrap();
        }
    }

    #[test]
    fn available_schedulers_keep_hashring_positions_when_ip_changes() {
        let before = AvailableSchedulers::new(SchedulerClient::scheduler_addrs(&[
            Scheduler {
                id: 1,
                hostname: "scheduler-0".to_string(),
                ip: "10.0.0.1".to_string(),
                port: 8002,
                ..Default::default()
            },
            Scheduler {
                id: 2,
                hostname: "scheduler-1".to_string(),
                ip: "10.0.0.2".to_string(),
                port: 8002,
                ..Default::default()
            },
            Scheduler {
                id: 3,
                hostname: "scheduler-2".to_string(),
                ip: "10.0.0.3".to_string(),
                port: 8002,
                ..Default::default()
            },
        ]));

        let after = AvailableSchedulers::new(SchedulerClient::scheduler_addrs(&[
            Scheduler {
                id: 1,
                hostname: "scheduler-0".to_string(),
                ip: "10.0.0.1".to_string(),
                port: 8002,
                ..Default::default()
            },
            Scheduler {
                id: 3,
                hostname: "scheduler-2".to_string(),
                ip: "10.0.0.3".to_string(),
                port: 8002,
                ..Default::default()
            },
            Scheduler {
                id: 4,
                hostname: "scheduler-1".to_string(),
                ip: "10.0.0.4".to_string(),
                port: 8002,
                ..Default::default()
            },
        ]));

        assert_ne!(before.addrs, after.addrs);
        for task_id in (0..1000).map(|i| format!("task-{i}")) {
            assert_eq!(
                before.hashring.get(&task_id).unwrap().name(),
                after.hashring.get(&task_id).unwrap().name()
            );
        }
    }
}
