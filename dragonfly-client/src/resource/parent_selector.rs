/*
 *     Copyright 2025 The Dragonfly Authors
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

use crate::grpc::dfdaemon_upload::DfdaemonUploadClient;
use crate::resource::piece_collector::CollectedParent;
use dashmap::DashMap;
use dragonfly_api::common::v2::{Host, Peer, PersistentCachePeer};
use dragonfly_api::dfdaemon::v2::SyncHostRequest;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Result;
use dragonfly_client_util::id_generator::IDGenerator;
use dragonfly_client_util::shutdown::{self, Shutdown};
use rand::distr::weighted::WeightedIndex;
use rand::distr::Distribution;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, warn, Instrument};

/// Manages a single persistent connection to a parent peer.
///
/// This structure tracks the gRPC client, reference count of active requests,
/// and shutdown signaling for background synchronization tasks.
struct Connection {
    /// Number of active requests currently using this connection.
    /// Used for reference counting to determine when cleanup is safe.
    active_requests: Arc<AtomicUsize>,

    /// Shutdown signal to stop the background host sync task.
    shutdown: Shutdown,
}

/// Implements lifecycle management for parent peer connections.
impl Connection {
    /// Creates a new connection wrapper with zero active requests.
    pub fn new() -> Self {
        Self {
            active_requests: Arc::new(AtomicUsize::new(0)),
            shutdown: Shutdown::new(),
        }
    }

    /// Returns the current number of active requests using this connection.
    pub fn active_requests(&self) -> usize {
        self.active_requests.load(Ordering::SeqCst)
    }

    /// Increments the active request counter.
    pub fn increment_request(&self) {
        self.active_requests.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrements the active request counter.
    pub fn decrement_request(&self) {
        self.active_requests.fetch_sub(1, Ordering::SeqCst);
    }

    /// Triggers shutdown of the background host synchronization task.
    pub fn shutdown(&self) {
        self.shutdown.trigger();
    }
}

/// ParentSelector is the download parent selector configuration for dfdaemon. It will synchronize
/// the host info in real-time from the parents and then select the parents for downloading.
///
/// The workflow diagram is as follows:
///
///```text
///                              +----------+
///              ----------------|  Parent  |---------------
///              |               +----------+              |
///          Host Load Quality                     Piece Metadata
/// +------------|-----------------------------------------|------------+
/// |            |                                         |            |
/// |            |                 Peer                    |            |
/// |            v                                         v            |
/// |  +------------------+  Select Best Parent   +------------------+  |
/// |  |  ParentSelector  | ------------------->  |  PieceCollector  |  |
/// |  +------------------+                       +------------------+  |
/// |                                                      |            |
/// |                                             Download Piece From   |
/// |                                                  Best Parent      |
/// |                                                      |            |
/// |                                                      v            |
/// |                                                +------------+     |
/// |                                                |  Download  |     |
/// |                                                +------------+     |
/// +-------------------------------------------------------------------+
/// ```
pub struct ParentSelector {
    /// Config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// Generator for host and peer identifiers.
    id_generator: Arc<IDGenerator>,

    /// Maps parent host IDs to their current bandwidth weights.
    weights: Arc<DashMap<String, u32>>,

    /// Active connections indexed by parent host ID and each connection tracks usage and manages its sync task.
    connections: Arc<DashMap<String, Connection>>,

    /// Global shutdown signal for the entire daemon.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the garbage collector is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// Implements parent peer selection and connection management logic.
impl ParentSelector {
    /// Creates a new parent selector instance.
    #[instrument(skip_all)]
    pub fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> ParentSelector {
        Self {
            config,
            id_generator,
            weights: Arc::new(DashMap::new()),
            connections: Arc::new(DashMap::new()),
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// Selects the best parent from a list of candidates based on their load quality weights.
    ///
    /// This function performs weighted random selection where parents with higher weights
    /// (better idle bandwidth) have a higher probability of being selected. If weight
    /// calculation fails, falls back to uniform random selection.
    pub fn select(&self, parents: Vec<CollectedParent>) -> CollectedParent {
        let weights: Vec<u32> = parents
            .iter()
            .map(|parent| {
                let Some(parent_host) = parent.host.as_ref() else {
                    warn!(
                        "parent {} has no host info, defaulting weight to 0",
                        parent.id
                    );

                    return 0;
                };
                let parent_host_id = parent_host.id.clone();

                self.weights
                    .get(&parent_host_id)
                    .map(|w| *w)
                    .unwrap_or_else(|| {
                        warn!(
                            "no weight info for parent {} {}, defaulting weight to 0",
                            parent.id, parent_host_id
                        );

                        0
                    })
            })
            .collect();

        match WeightedIndex::new(weights) {
            Ok(dist) => {
                let mut rng = rand::rng();
                let index = dist.sample(&mut rng);
                let selected_parent = &parents[index];
                debug!("selected parent {}", selected_parent.id);

                selected_parent.clone()
            }
            Err(_) => parents[fastrand::usize(..parents.len())].clone(),
        }
    }

    /// Registers multiple parents for host information synchronization.
    ///
    /// For each parent, this function:
    /// - Creates a new gRPC connection if one doesn't exist.
    /// - Spawns a background task to continuously sync host metrics (bandwidth, load).
    /// - Updates the connection's request counter.
    pub async fn register(&self, parents: &[Peer]) -> Result<()> {
        let dfdaemon_shutdown = self.shutdown.clone();
        let mut join_set = JoinSet::new();
        for parent in parents {
            info!("register parent {}", parent.id);

            let Some(parent_host) = parent.host.as_ref() else {
                warn!("parent {} has no host info, skipping", parent.id);
                continue;
            };
            let parent_host_id = parent_host.id.clone();

            match self.connections.entry(parent_host_id.clone()) {
                dashmap::mapref::entry::Entry::Occupied(entry) => {
                    entry.get().increment_request();
                    continue;
                }
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    let dfdaemon_upload_client = DfdaemonUploadClient::new(
                        self.config.clone(),
                        format!("http://{}:{}", parent_host.ip, parent_host.port),
                        false,
                    )
                    .await?;

                    let connection = Connection::new();
                    connection.increment_request();
                    let shutdown = connection.shutdown.clone();
                    entry.insert(connection);

                    let weights = self.weights.clone();
                    let host_id = self.id_generator.host_id();
                    let peer_id = self.id_generator.peer_id();
                    let dfdaemon_shutdown_clone = dfdaemon_shutdown.clone();
                    join_set.spawn(
                        Self::sync_host(
                            host_id,
                            peer_id,
                            parent_host_id.clone(),
                            weights,
                            dfdaemon_upload_client,
                            shutdown,
                            dfdaemon_shutdown_clone,
                        )
                        .in_current_span(),
                    );
                }
            }
        }

        tokio::spawn(async move {
            while let Some(message) = join_set.join_next().await {
                match message {
                    Ok(Ok(_)) => debug!("sync host info completed"),
                    Ok(Err(err)) => error!("sync host info failed: {}", err),
                    Err(err) => error!("task join error: {}", err),
                }
            }
        });

        Ok(())
    }

    /// Unregisters multiple parents and cleans up their connections.
    ///
    /// Decrements the request counter for each parent's connection. When a connection's
    /// active request count reaches zero, it:
    /// - Triggers connection shutdown.
    /// - Removes the weight entry.
    /// - Removes the connection from the pool.
    pub fn unregister(&self, parents: &[Peer]) {
        for parent in parents {
            info!("unregister parent {}", parent.id);

            let Some(parent_host) = parent.host.as_ref() else {
                warn!("parent {} has no host info, skipping", parent.id);
                continue;
            };
            let parent_host_id = parent_host.id.clone();

            if let Some(connection) = self.connections.get(&parent_host_id) {
                connection.decrement_request();
                if connection.active_requests() == 0 {
                    info!("cleaning up parent {} connection", parent_host_id);
                    connection.shutdown();

                    // Explicitly drop the reference to avoid holding the borrow
                    // from self.connections.get() while trying to call remove().
                    drop(connection);
                    self.weights.remove(&parent_host_id);
                    self.connections.remove(&parent_host_id);
                }
            }
        }
    }

    /// Continuously synchronizes host metrics from a parent peer.
    ///
    /// This is a long-running background task that:
    /// - Establishes a streaming gRPC connection to the parent.
    /// - Receives periodic host status updates (CPU, bandwidth, etc.).
    /// - Updates the parent's weight based on idle TX bandwidth.
    /// - Runs until shutdown signal or connection failure.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn sync_host(
        host_id: String,
        peer_id: String,
        parent_host_id: String,
        weights: Arc<DashMap<String, u32>>,
        dfdaemon_upload_client: DfdaemonUploadClient,
        mut shutdown: Shutdown,
        mut dfdaemon_shutdown: Shutdown,
    ) -> Result<()> {
        info!("sync host info from parent {}", parent_host_id);
        let response = dfdaemon_upload_client
            .sync_host(SyncHostRequest { host_id, peer_id })
            .await
            .inspect_err(|err| {
                error!(
                    "sync host info from parent {} failed: {}",
                    parent_host_id, err
                );
            })?;

        let out_stream = response.into_inner();
        tokio::pin!(out_stream);
        loop {
            tokio::select! {
                result = out_stream.try_next() => {
                    match result.inspect_err(|err| {
                        error!("sync host info from parent {} failed: {}", parent_host_id, err);
                    })? {
                        Some(message) => {
                            let idle_tx_bandwidth = Self::get_idle_tx_bandwidth(&message);

                            info!("update host {} idle TX bandwidth to {}", parent_host_id, idle_tx_bandwidth);
                            weights.insert(parent_host_id.clone(), idle_tx_bandwidth as u32);
                        }
                        None => break,
                    }
                }
                _ = shutdown.recv() => {
                    info!("sync host info from parent {} shutting down", parent_host_id);
                    break;
                }
                _ = dfdaemon_shutdown.recv() => {
                    info!("parent selector shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Calculates the idle transmission bandwidth of a host.
    fn get_idle_tx_bandwidth(host: &Host) -> u64 {
        let network = match &host.network {
            Some(network) => network,
            None => return 0,
        };

        debug!("host {} network info: {:?}", host.id, network);
        let Some(tx_bandwidth) = network.tx_bandwidth else {
            return 0;
        };

        if tx_bandwidth < network.max_tx_bandwidth {
            network.max_tx_bandwidth - tx_bandwidth
        } else {
            0
        }
    }
}

/// PersistentCacheParentSelector is the download persistent cache parent selector configuration for dfdaemon. It will synchronize
/// the host info in real-time from the parents and then select the parents for downloading.
///
/// The workflow diagram is as follows:
///
///```text
///                              +----------+
///              ----------------|  Parent  |---------------
///              |               +----------+              |
///          Host Load Quality                     Piece Metadata
/// +------------|-----------------------------------------|------------+
/// |            |                                         |            |
/// |            |                 Peer                    |            |
/// |            v                                         v            |
/// |  +------------------+  Select Best Parent   +------------------+  |
/// |  |  ParentSelector  | ------------------->  |  PieceCollector  |  |
/// |  +------------------+                       +------------------+  |
/// |                                                      |            |
/// |                                             Download Piece From   |
/// |                                                  Best Parent      |
/// |                                                      |            |
/// |                                                      v            |
/// |                                                +------------+     |
/// |                                                |  Download  |     |
/// |                                                +------------+     |
/// +-------------------------------------------------------------------+
/// ```
pub struct PersistentCacheParentSelector {
    /// Config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// Generator for host and peer identifiers.
    id_generator: Arc<IDGenerator>,

    /// Maps parent host IDs to their current bandwidth weights.
    weights: Arc<DashMap<String, u32>>,

    /// Active connections indexed by parent host ID and each connection tracks usage and manages its sync task.
    connections: Arc<DashMap<String, Connection>>,

    /// Global shutdown signal for the entire daemon.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the garbage collector is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// Implements persistent cache parent peer selection and connection management logic.
impl PersistentCacheParentSelector {
    /// Creates a new persistent cache parent selector instance.
    #[instrument(skip_all)]
    pub fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> PersistentCacheParentSelector {
        Self {
            config,
            id_generator,
            weights: Arc::new(DashMap::new()),
            connections: Arc::new(DashMap::new()),
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// Selects the best persistent cache parent from a list of candidates based on their load quality weights.
    ///
    /// This function performs weighted random selection where parents with higher weights
    /// (better idle bandwidth) have a higher probability of being selected. If weight
    /// calculation fails, falls back to uniform random selection.
    pub fn select(&self, parents: Vec<CollectedParent>) -> CollectedParent {
        let weights: Vec<u32> = parents
            .iter()
            .map(|parent| {
                let Some(parent_host) = parent.host.as_ref() else {
                    warn!(
                        "persistent cache parent {} has no host info, defaulting weight to 0",
                        parent.id
                    );

                    return 0;
                };
                let parent_host_id = parent_host.id.clone();

                self.weights
                    .get(&parent_host_id)
                    .map(|w| *w)
                    .unwrap_or_else(|| {
                        warn!(
                            "no weight info for persistent cache parent {} {}, defaulting weight to 0",
                            parent.id, parent_host_id
                        );

                        0
                    })
            })
            .collect();

        match WeightedIndex::new(weights) {
            Ok(dist) => {
                let mut rng = rand::rng();
                let index = dist.sample(&mut rng);
                let selected_parent = &parents[index];
                debug!("selected persistent cache parent {}", selected_parent.id);

                selected_parent.clone()
            }
            Err(_) => parents[fastrand::usize(..parents.len())].clone(),
        }
    }

    /// Registers multiple persistent cache parents for host information synchronization.
    ///
    /// For each parent, this function:
    /// - Creates a new gRPC connection if one doesn't exist.
    /// - Spawns a background task to continuously sync host metrics (bandwidth, load).
    /// - Updates the connection's request counter.
    pub async fn register(&self, parents: &[PersistentCachePeer]) -> Result<()> {
        let dfdaemon_shutdown = self.shutdown.clone();
        let mut join_set = JoinSet::new();
        for parent in parents {
            info!("register persistent cache parent {}", parent.id);

            let Some(parent_host) = parent.host.as_ref() else {
                warn!(
                    "persistent cache parent {} has no host info, skipping",
                    parent.id
                );
                continue;
            };
            let parent_host_id = parent_host.id.clone();

            match self.connections.entry(parent_host_id.clone()) {
                dashmap::mapref::entry::Entry::Occupied(entry) => {
                    entry.get().increment_request();
                    continue;
                }
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    let dfdaemon_upload_client = DfdaemonUploadClient::new(
                        self.config.clone(),
                        format!("http://{}:{}", parent_host.ip, parent_host.port),
                        false,
                    )
                    .await?;

                    let connection = Connection::new();
                    connection.increment_request();
                    let shutdown = connection.shutdown.clone();
                    entry.insert(connection);

                    let weights = self.weights.clone();
                    let host_id = self.id_generator.host_id();
                    let peer_id = self.id_generator.peer_id();
                    let dfdaemon_shutdown_clone = dfdaemon_shutdown.clone();
                    join_set.spawn(
                        Self::sync_host(
                            host_id,
                            peer_id,
                            parent_host_id.clone(),
                            weights,
                            dfdaemon_upload_client,
                            shutdown,
                            dfdaemon_shutdown_clone,
                        )
                        .in_current_span(),
                    );
                }
            }
        }

        tokio::spawn(async move {
            while let Some(message) = join_set.join_next().await {
                match message {
                    Ok(Ok(_)) => debug!("sync host info completed"),
                    Ok(Err(err)) => error!("sync host info failed: {}", err),
                    Err(err) => error!("task join error: {}", err),
                }
            }
        });

        Ok(())
    }

    /// Unregisters multiple persistent cache parents and cleans up their connections.
    ///
    /// Decrements the request counter for each parent's connection. When a connection's
    /// active request count reaches zero, it:
    /// - Triggers connection shutdown.
    /// - Removes the weight entry.
    /// - Removes the connection from the pool.
    pub fn unregister(&self, parents: &[PersistentCachePeer]) {
        for parent in parents {
            info!("unregister persistent cache parent {}", parent.id);

            let Some(parent_host) = parent.host.as_ref() else {
                warn!(
                    "persistent cache parent {} has no host info, skipping",
                    parent.id
                );
                continue;
            };
            let parent_host_id = parent_host.id.clone();

            if let Some(connection) = self.connections.get(&parent_host_id) {
                connection.decrement_request();
                if connection.active_requests() == 0 {
                    info!("cleaning up parent {} connection", parent_host_id);
                    connection.shutdown();

                    // Explicitly drop the reference to avoid holding the borrow
                    // from self.connections.get() while trying to call remove().
                    drop(connection);
                    self.weights.remove(&parent_host_id);
                    self.connections.remove(&parent_host_id);
                }
            }
        }
    }

    /// Continuously synchronizes host metrics from a persistent cache parent peer.
    ///
    /// This is a long-running background task that:
    /// - Establishes a streaming gRPC connection to the parent.
    /// - Receives periodic host status updates (CPU, bandwidth, etc.).
    /// - Updates the parent's weight based on idle TX bandwidth.
    /// - Runs until shutdown signal or connection failure.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn sync_host(
        host_id: String,
        peer_id: String,
        parent_host_id: String,
        weights: Arc<DashMap<String, u32>>,
        dfdaemon_upload_client: DfdaemonUploadClient,
        mut shutdown: Shutdown,
        mut dfdaemon_shutdown: Shutdown,
    ) -> Result<()> {
        info!(
            "sync host info from persistent cache parent {}",
            parent_host_id
        );
        let response = dfdaemon_upload_client
            .sync_host(SyncHostRequest { host_id, peer_id })
            .await
            .inspect_err(|err| {
                error!(
                    "sync host info from persistent cache parent {} failed: {}",
                    parent_host_id, err
                );
            })?;

        let out_stream = response.into_inner();
        tokio::pin!(out_stream);
        loop {
            tokio::select! {
                result = out_stream.try_next() => {
                    match result.inspect_err(|err| {
                        error!("sync host info from persistent cache parent {} failed: {}", parent_host_id, err);
                    })? {
                        Some(message) => {
                            let idle_tx_bandwidth = Self::get_idle_tx_bandwidth(&message);

                            info!("update host {} idle TX bandwidth to {}", parent_host_id, idle_tx_bandwidth);
                            weights.insert(parent_host_id.clone(), idle_tx_bandwidth as u32);
                        }
                        None => break,
                    }
                }
                _ = shutdown.recv() => {
                    info!("sync host info from persistent cache parent {} shutting down", parent_host_id);
                    break;
                }
                _ = dfdaemon_shutdown.recv() => {
                    info!("persistent cache parent selector shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Calculates the idle transmission bandwidth of a host.
    fn get_idle_tx_bandwidth(host: &Host) -> u64 {
        let network = match &host.network {
            Some(network) => network,
            None => return 0,
        };

        debug!("host {} network info: {:?}", host.id, network);
        let Some(tx_bandwidth) = network.tx_bandwidth else {
            return 0;
        };

        if tx_bandwidth < network.max_tx_bandwidth {
            network.max_tx_bandwidth - tx_bandwidth
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_api::common::v2::{CacheTask, Host, PersistentCacheTask, Task};
    use dragonfly_api::dfdaemon::v2::dfdaemon_upload_server::{
        DfdaemonUpload, DfdaemonUploadServer as DfdaemonUploadGRPCServer,
    };
    use dragonfly_api::dfdaemon::v2::*;
    use dragonfly_client_config::dfdaemon::Config;
    use dragonfly_client_util::shutdown::Shutdown;
    use std::io::ErrorKind;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::TcpListenerStream;
    use tokio_stream::Stream;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    type GrpcResult<T> = std::result::Result<T, Status>;
    type BoxStream<T> = Pin<Box<dyn Stream<Item = GrpcResult<T>> + Send>>;

    struct MockUploadService {
        hosts: Vec<Host>,
    }

    #[tonic::async_trait]
    impl DfdaemonUpload for MockUploadService {
        type DownloadTaskStream = BoxStream<DownloadTaskResponse>;
        type DownloadCacheTaskStream = BoxStream<DownloadCacheTaskResponse>;
        type SyncCachePiecesStream = BoxStream<SyncCachePiecesResponse>;
        type SyncPiecesStream = BoxStream<SyncPiecesResponse>;
        type SyncHostStream = BoxStream<Host>;
        type DownloadPersistentCacheTaskStream = BoxStream<DownloadPersistentCacheTaskResponse>;
        type SyncPersistentCachePiecesStream = BoxStream<SyncPersistentCachePiecesResponse>;

        async fn download_task(
            &self,
            _request: Request<DownloadTaskRequest>,
        ) -> GrpcResult<Response<Self::DownloadTaskStream>> {
            Err(Status::unimplemented("download_task"))
        }

        async fn stat_task(
            &self,
            _request: Request<StatTaskRequest>,
        ) -> GrpcResult<Response<Task>> {
            Err(Status::unimplemented("stat_task"))
        }

        async fn list_task_entries(
            &self,
            _request: Request<ListTaskEntriesRequest>,
        ) -> GrpcResult<Response<ListTaskEntriesResponse>> {
            Err(Status::unimplemented("list_task_entries"))
        }

        async fn delete_task(
            &self,
            _request: Request<DeleteTaskRequest>,
        ) -> GrpcResult<Response<()>> {
            Err(Status::unimplemented("delete_task"))
        }

        async fn sync_pieces(
            &self,
            _request: Request<SyncPiecesRequest>,
        ) -> GrpcResult<Response<Self::SyncPiecesStream>> {
            Err(Status::unimplemented("sync_pieces"))
        }

        async fn download_piece(
            &self,
            _request: Request<DownloadPieceRequest>,
        ) -> GrpcResult<Response<DownloadPieceResponse>> {
            Err(Status::unimplemented("download_piece"))
        }

        async fn download_cache_task(
            &self,
            _request: Request<DownloadCacheTaskRequest>,
        ) -> GrpcResult<Response<Self::DownloadCacheTaskStream>> {
            Err(Status::unimplemented("download_cache_task"))
        }

        async fn stat_cache_task(
            &self,
            _request: Request<StatCacheTaskRequest>,
        ) -> GrpcResult<Response<CacheTask>> {
            Err(Status::unimplemented("stat_cache_task"))
        }

        async fn delete_cache_task(
            &self,
            _request: Request<DeleteCacheTaskRequest>,
        ) -> GrpcResult<Response<()>> {
            Err(Status::unimplemented("delete_cache_task"))
        }

        async fn sync_cache_pieces(
            &self,
            _request: Request<SyncCachePiecesRequest>,
        ) -> GrpcResult<Response<Self::SyncCachePiecesStream>> {
            Err(Status::unimplemented("sync_cache_pieces"))
        }

        async fn download_cache_piece(
            &self,
            _request: Request<DownloadCachePieceRequest>,
        ) -> GrpcResult<Response<DownloadCachePieceResponse>> {
            Err(Status::unimplemented("download_cache_piece"))
        }

        async fn sync_host(
            &self,
            _request: Request<SyncHostRequest>,
        ) -> GrpcResult<Response<Self::SyncHostStream>> {
            let stream = tokio_stream::iter(self.hosts.clone().into_iter().map(Ok));
            Ok(Response::new(Box::pin(stream) as Self::SyncHostStream))
        }

        async fn download_persistent_cache_task(
            &self,
            _request: Request<DownloadPersistentCacheTaskRequest>,
        ) -> GrpcResult<Response<Self::DownloadPersistentCacheTaskStream>> {
            Err(Status::unimplemented("download_persistent_cache_task"))
        }

        async fn update_persistent_cache_task(
            &self,
            _request: Request<UpdatePersistentCacheTaskRequest>,
        ) -> GrpcResult<Response<()>> {
            Err(Status::unimplemented("update_persistent_cache_task"))
        }

        async fn stat_persistent_cache_task(
            &self,
            _request: Request<StatPersistentCacheTaskRequest>,
        ) -> GrpcResult<Response<PersistentCacheTask>> {
            Err(Status::unimplemented("stat_persistent_cache_task"))
        }

        async fn delete_persistent_cache_task(
            &self,
            _request: Request<DeletePersistentCacheTaskRequest>,
        ) -> GrpcResult<Response<()>> {
            Err(Status::unimplemented("delete_persistent_cache_task"))
        }

        async fn sync_persistent_cache_pieces(
            &self,
            _request: Request<SyncPersistentCachePiecesRequest>,
        ) -> GrpcResult<Response<Self::SyncPersistentCachePiecesStream>> {
            Err(Status::unimplemented("sync_persistent_cache_pieces"))
        }

        async fn download_persistent_cache_piece(
            &self,
            _request: Request<DownloadPersistentCachePieceRequest>,
        ) -> GrpcResult<Response<DownloadPersistentCachePieceResponse>> {
            Err(Status::unimplemented("download_persistent_cache_piece"))
        }

        async fn exchange_ib_verbs_queue_pair_endpoint(
            &self,
            _request: Request<ExchangeIbVerbsQueuePairEndpointRequest>,
        ) -> GrpcResult<Response<ExchangeIbVerbsQueuePairEndpointResponse>> {
            Err(Status::unimplemented(
                "exchange_ib_verbs_queue_pair_endpoint",
            ))
        }
    }

    #[test]
    fn test_get_idle_upload_rate() {
        struct TestCase {
            name: &'static str,
            host: Host,
            expected: u64,
        }

        let test_cases = vec![
            TestCase {
                name: "no network",
                host: Host {
                    network: None,
                    ..Default::default()
                },
                expected: 0,
            },
            TestCase {
                name: "tx_bandwidth none",
                host: Host {
                    network: Some(dragonfly_api::common::v2::Network {
                        tx_bandwidth: None,
                        max_tx_bandwidth: 100,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                expected: 100,
            },
            TestCase {
                name: "idle bandwidth",
                host: Host {
                    network: Some(dragonfly_api::common::v2::Network {
                        tx_bandwidth: Some(50),
                        max_tx_bandwidth: 100,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                expected: 50,
            },
            TestCase {
                name: "no idle bandwidth",
                host: Host {
                    network: Some(dragonfly_api::common::v2::Network {
                        tx_bandwidth: Some(100),
                        max_tx_bandwidth: 100,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                expected: 0,
            },
            TestCase {
                name: "tx greater than max",
                host: Host {
                    network: Some(dragonfly_api::common::v2::Network {
                        tx_bandwidth: Some(150),
                        max_tx_bandwidth: 100,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                expected: 0,
            },
        ];

        for test_case in test_cases {
            assert_eq!(
                ParentSelector::get_idle_tx_bandwidth(&test_case.host),
                test_case.expected,
                "Failed for test case: {}",
                test_case.name
            );
        }
    }
}
