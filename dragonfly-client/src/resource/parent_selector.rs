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
use tracing::{debug, error, instrument, warn, Instrument};

/// Manages a single connection to a parent peer.
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
    weights: Arc<DashMap<String, u64>>,

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
    #[instrument(skip_all)]
    pub fn select(&self, parents: Vec<CollectedParent>) -> CollectedParent {
        let weights: Vec<u64> = parents
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
                        debug!(
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
    #[instrument(skip_all)]
    pub async fn register(&self, parents: &[Peer]) -> Result<()> {
        let dfdaemon_shutdown = self.shutdown.clone();
        let mut join_set = JoinSet::new();
        for parent in parents {
            debug!("register parent {}", parent.id);

            let Some(parent_host) = parent.host.as_ref() else {
                error!("parent {} has no host info, skipping", parent.id);
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
    #[instrument(skip_all)]
    pub fn unregister(&self, parents: &[Peer]) {
        for parent in parents {
            debug!("unregister parent {}", parent.id);

            let Some(parent_host) = parent.host.as_ref() else {
                warn!("parent {} has no host info, skipping", parent.id);
                continue;
            };
            let parent_host_id = parent_host.id.clone();

            if let Some(connection) = self.connections.get(&parent_host_id) {
                connection.decrement_request();
                if connection.active_requests() == 0 {
                    debug!("cleaning up parent {} connection", parent_host_id);
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
        weights: Arc<DashMap<String, u64>>,
        dfdaemon_upload_client: DfdaemonUploadClient,
        mut shutdown: Shutdown,
        mut dfdaemon_shutdown: Shutdown,
    ) -> Result<()> {
        debug!("sync host info from parent {}", parent_host_id);
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

                            debug!("update host {} idle TX bandwidth to {}", parent_host_id, idle_tx_bandwidth);
                            weights.insert(parent_host_id.clone(), idle_tx_bandwidth);
                        }
                        None => break,
                    }
                }
                _ = shutdown.recv() => {
                    debug!("sync host info from parent {} shutting down", parent_host_id);
                    break;
                }
                _ = dfdaemon_shutdown.recv() => {
                    debug!("parent selector shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Calculates the idle transmission bandwidth of a host.
    #[instrument(skip_all)]
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

/// PersistentParentSelector is the download persistent parent selector configuration for dfdaemon. It will synchronize
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
pub struct PersistentParentSelector {
    /// Config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// Generator for host and peer identifiers.
    id_generator: Arc<IDGenerator>,

    /// Maps parent host IDs to their current bandwidth weights.
    weights: Arc<DashMap<String, u64>>,

    /// Active connections indexed by parent host ID and each connection tracks usage and manages its sync task.
    connections: Arc<DashMap<String, Connection>>,

    /// Global shutdown signal for the entire daemon.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the garbage collector is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// Implements persistent parent peer selection and connection management logic.
impl PersistentParentSelector {
    /// Creates a new persistent parent selector instance.
    #[instrument(skip_all)]
    pub fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> PersistentParentSelector {
        Self {
            config,
            id_generator,
            weights: Arc::new(DashMap::new()),
            connections: Arc::new(DashMap::new()),
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// Selects the best persistent parent from a list of candidates based on their load quality weights.
    ///
    /// This function performs weighted random selection where parents with higher weights
    /// (better idle bandwidth) have a higher probability of being selected. If weight
    /// calculation fails, falls back to uniform random selection.
    #[instrument(skip_all)]
    pub fn select(&self, parents: Vec<CollectedParent>) -> CollectedParent {
        let weights: Vec<u64> = parents
            .iter()
            .map(|parent| {
                let Some(parent_host) = parent.host.as_ref() else {
                    warn!(
                        "persistent parent {} has no host info, defaulting weight to 0",
                        parent.id
                    );

                    return 0;
                };
                let parent_host_id = parent_host.id.clone();

                self.weights
                    .get(&parent_host_id)
                    .map(|w| *w)
                    .unwrap_or_else(|| {
                        debug!(
                            "no weight info for persistent parent {} {}, defaulting weight to 0",
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
                debug!("selected persistent parent {}", selected_parent.id);

                selected_parent.clone()
            }
            Err(_) => parents[fastrand::usize(..parents.len())].clone(),
        }
    }

    /// Registers multiple persistent parents for host information synchronization.
    ///
    /// For each parent, this function:
    /// - Creates a new gRPC connection if one doesn't exist.
    /// - Spawns a background task to continuously sync host metrics (bandwidth, load).
    /// - Updates the connection's request counter.
    #[instrument(skip_all)]
    pub async fn register(&self, parents: &[PersistentPeer]) -> Result<()> {
        let dfdaemon_shutdown = self.shutdown.clone();
        let mut join_set = JoinSet::new();
        for parent in parents {
            debug!("register persistent parent {}", parent.id);

            let Some(parent_host) = parent.host.as_ref() else {
                warn!("persistent parent {} has no host info, skipping", parent.id);
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

    /// Unregisters multiple persistent parents and cleans up their connections.
    ///
    /// Decrements the request counter for each parent's connection. When a connection's
    /// active request count reaches zero, it:
    /// - Triggers connection shutdown.
    /// - Removes the weight entry.
    /// - Removes the connection from the pool.
    #[instrument(skip_all)]
    pub fn unregister(&self, parents: &[PersistentPeer]) {
        for parent in parents {
            debug!("unregister persistent parent {}", parent.id);

            let Some(parent_host) = parent.host.as_ref() else {
                warn!("persistent parent {} has no host info, skipping", parent.id);
                continue;
            };
            let parent_host_id = parent_host.id.clone();

            if let Some(connection) = self.connections.get(&parent_host_id) {
                connection.decrement_request();
                if connection.active_requests() == 0 {
                    debug!("cleaning up parent {} connection", parent_host_id);
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

    /// Continuously synchronizes host metrics from a persistent parent peer.
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
        weights: Arc<DashMap<String, u64>>,
        dfdaemon_upload_client: DfdaemonUploadClient,
        mut shutdown: Shutdown,
        mut dfdaemon_shutdown: Shutdown,
    ) -> Result<()> {
        debug!("sync host info from persistent parent {}", parent_host_id);
        let response = dfdaemon_upload_client
            .sync_host(SyncHostRequest { host_id, peer_id })
            .await
            .inspect_err(|err| {
                error!(
                    "sync host info from persistent parent {} failed: {}",
                    parent_host_id, err
                );
            })?;

        let out_stream = response.into_inner();
        tokio::pin!(out_stream);
        loop {
            tokio::select! {
                result = out_stream.try_next() => {
                    match result.inspect_err(|err| {
                        error!("sync host info from persistent parent {} failed: {}", parent_host_id, err);
                    })? {
                        Some(message) => {
                            let idle_tx_bandwidth = Self::get_idle_tx_bandwidth(&message);

                            debug!("update host {} idle TX bandwidth to {}", parent_host_id, idle_tx_bandwidth);
                            weights.insert(parent_host_id.clone(), idle_tx_bandwidth);
                        }
                        None => break,
                    }
                }
                _ = shutdown.recv() => {
                    debug!("sync host info from persistent parent {} shutting down", parent_host_id);
                    break;
                }
                _ = dfdaemon_shutdown.recv() => {
                    debug!("persistent parent selector shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Calculates the idle transmission bandwidth of a host.
    #[instrument(skip_all)]
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
    weights: Arc<DashMap<String, u64>>,

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
    #[instrument(skip_all)]
    pub fn select(&self, parents: Vec<CollectedParent>) -> CollectedParent {
        let weights: Vec<u64> = parents
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
                        debug!(
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
    #[instrument(skip_all)]
    pub async fn register(&self, parents: &[PersistentCachePeer]) -> Result<()> {
        let dfdaemon_shutdown = self.shutdown.clone();
        let mut join_set = JoinSet::new();
        for parent in parents {
            debug!("register persistent cache parent {}", parent.id);

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
    #[instrument(skip_all)]
    pub fn unregister(&self, parents: &[PersistentCachePeer]) {
        for parent in parents {
            debug!("unregister persistent cache parent {}", parent.id);

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
                    debug!("cleaning up parent {} connection", parent_host_id);
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
        weights: Arc<DashMap<String, u64>>,
        dfdaemon_upload_client: DfdaemonUploadClient,
        mut shutdown: Shutdown,
        mut dfdaemon_shutdown: Shutdown,
    ) -> Result<()> {
        debug!(
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

                            debug!("update host {} idle TX bandwidth to {}", parent_host_id, idle_tx_bandwidth);
                            weights.insert(parent_host_id.clone(), idle_tx_bandwidth);
                        }
                        None => break,
                    }
                }
                _ = shutdown.recv() => {
                    debug!("sync host info from persistent cache parent {} shutting down", parent_host_id);
                    break;
                }
                _ = dfdaemon_shutdown.recv() => {
                    debug!("persistent cache parent selector shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Calculates the idle transmission bandwidth of a host.
    #[instrument(skip_all)]
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
