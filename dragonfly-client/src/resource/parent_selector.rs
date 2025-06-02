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
use crate::shutdown::{self, Shutdown};
use dashmap::DashMap;
use dragonfly_api::common::v2::{Host, Peer};
use dragonfly_api::dfdaemon::v2::SyncHostRequest;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Error;
use dragonfly_client_core::Result;
use dragonfly_client_util::id_generator::IDGenerator;
use rand::distr::weighted::WeightedIndex;
use rand::distr::Distribution;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Barrier, Mutex};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, warn, Instrument};

/// DEFAULT_PARENT_CONNECTION_CLEANUP_DELAY is the default delay before closing parent connections.
const DEFAULT_PARENT_CONNECTION_CLEANUP_DELAY: Duration = Duration::from_secs(5);

/// HostInfo combines host information and its bandwidth weight.
#[derive(Clone, Debug)]
struct HostInfo {
    /// host contains the host information.
    host: Host,
    /// weight represents the cached bandwidth weight for this host.
    weight: u32,
}

impl HostInfo {
    /// new creates a new HostInfo with the given host and weight.
    pub fn new(host: Host, weight: u32) -> Self {
        Self { host, weight }
    }
}

/// ParentConnection manages a single parent connection with reference counting.
#[derive(Clone)]
struct ParentConnection {
    /// parent_id is the id of the parent.
    parent_id: String,

    /// client is the dfdaemon upload client for this parent.
    client: DfdaemonUploadClient,

    /// active_connection tracks how many download tasks are using this connection.
    active_connection: Arc<AtomicUsize>,

    /// shutdown is used to signal the monitoring task to stop.
    shutdown: Shutdown,

    /// cleanup_scheduled indicates if a delayed cleanup is scheduled.
    cleanup_scheduled: Arc<AtomicBool>,
}

impl ParentConnection {
    /// new creates a new ParentConnection.
    pub fn new(parent_id: String, client: DfdaemonUploadClient) -> Self {
        Self {
            parent_id,
            client,
            active_connection: Arc::new(AtomicUsize::new(0)),
            shutdown: Shutdown::new(),
            cleanup_scheduled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// acquire increments the reference count and cancels any scheduled cleanup.
    pub fn acquire(&self) -> ConnectionGuard {
        self.active_connection.fetch_add(1, Ordering::SeqCst);
        self.cleanup_scheduled.store(false, Ordering::SeqCst);
        debug!(
            "acquired connection to parent {}, ref_count: {}",
            self.parent_id,
            self.active_connection.load(Ordering::SeqCst)
        );
        ConnectionGuard::new(self.active_connection.clone(), self.parent_id.clone())
    }

    /// schedule_cleanup schedules cleanup when reference count reaches zero.
    pub fn schedule_cleanup(&self) -> bool {
        if self.active_connection.load(Ordering::SeqCst) == 0 {
            self.cleanup_scheduled.store(true, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    /// should_cleanup returns true if this connection should be cleaned up.
    pub fn should_cleanup(&self) -> bool {
        self.active_connection.load(Ordering::SeqCst) == 0
            && self.cleanup_scheduled.load(Ordering::SeqCst)
    }

    /// shutdown triggers shutdown of the monitoring task.
    pub fn shutdown(&self) {
        self.shutdown.trigger();
    }
}

/// ConnectionGuard automatically manages reference counting for parent connections.
pub struct ConnectionGuard {
    reference_count: Arc<AtomicUsize>,
    parent_id: String,
}

impl ConnectionGuard {
    fn new(reference_count: Arc<AtomicUsize>, parent_id: String) -> Self {
        Self {
            reference_count,
            parent_id,
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let old_count = self.reference_count.fetch_sub(1, Ordering::SeqCst);
        debug!(
            "released connection to parent {}, ref_count: {}",
            self.parent_id,
            old_count - 1
        );
    }
}

/// ParentSelector manages parent connections and selects optimal parents.
pub struct ParentSelector {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// sync_interval represents the time interval between two refreshing probability operations.
    sync_interval: Duration,

    /// id_generator is a IDGenerator.
    id_generator: Arc<IDGenerator>,

    /// hosts_info stores the latest host information and bandwidth weights for different parents.
    hosts_info: Arc<DashMap<String, HostInfo>>,

    /// connections stores parent connections with reference counting.
    connections: Arc<DashMap<String, ParentConnection>>,

    /// join_set manages all parent monitoring tasks.
    join_set: Arc<Mutex<JoinSet<Result<()>>>>,

    /// shutdown is used to shutdown the parent selector.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the parent selector is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// ParentSelector implements the parent selector.
impl ParentSelector {
    /// new returns a ParentSelector.
    #[instrument(skip_all)]
    pub fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Result<ParentSelector> {
        let config = config.clone();
        let sync_interval = config.download.parent_selector.sync_interval;
        let hosts_info = Arc::new(DashMap::new());
        let id_generator = id_generator.clone();
        let join_set = Arc::new(Mutex::new(JoinSet::new()));

        let selector = ParentSelector {
            config,
            sync_interval,
            id_generator,
            hosts_info,
            connections: Arc::new(DashMap::new()),
            join_set,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        };

        Ok(selector)
    }

    /// run starts the main ParentSelector loop that manages task lifecycle.
    #[instrument(skip_all)]
    pub async fn run(&self, grpc_server_started_barrier: Arc<Barrier>) -> Result<()> {
        if !self.config.download.parent_selector.enable {
            info!("parent selector is disabled");
            return Ok(());
        }

        let mut shutdown = self.shutdown.clone();

        // When the grpc server is started, notify the barrier. If the shutdown signal is received
        // before barrier is waited successfully, the server will shutdown immediately.
        tokio::select! {
            // Wait for starting the parent selector
            _ = grpc_server_started_barrier.wait() => {
                info!("parent selector is ready to start");
            }
            _ = shutdown.recv() => {
                // Parent selector shutting down with signals.
                info!("parent selector shutting down");
                return Ok(());
            }
        }

        let cleanup_interval = DEFAULT_PARENT_CONNECTION_CLEANUP_DELAY;
        let mut cleanup_timer = tokio::time::interval(cleanup_interval);

        loop {
            tokio::select! {
                // Handle completed tasks from joinset
                result = async {
                    let mut join_set = self.join_set.lock().await;
                    join_set.join_next().await
                } => {
                    match result {
                        Some(Ok(Ok(()))) => {
                            debug!("parent monitoring task completed successfully");
                        }
                        Some(Ok(Err(err))) => {
                            error!("parent monitoring task failed: {}", err);
                        }
                        Some(Err(err)) => {
                            error!("parent monitoring task join error: {}", err);
                        }
                        None => {
                            // No tasks in joinset, continue monitoring
                        }
                    }
                }

                // Periodic cleanup of unused connections
                _ = cleanup_timer.tick() => {
                    self.cleanup_idle_connections().await;
                }
            }
        }
    }

    /// cleanup_idle_connections removes and aborts tasks for idle connections.
    async fn cleanup_idle_connections(&self) {
        // Collect connections that should be cleaned up
        let to_cleanup: Vec<String> = self
            .connections
            .iter()
            .filter_map(|entry| {
                if entry.value().should_cleanup() {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();

        // Remove and shutdown connections
        for parent_id in to_cleanup {
            // First get the connection to trigger shutdown
            if let Some(connection) = self.connections.get(&parent_id) {
                connection.shutdown();
            }
            // Then remove it from the map
            self.connections.remove(&parent_id);
            info!("cleaned up unused connection to parent {}", parent_id);
        }

        // Clean up completed tasks in joinset
        let active_task_count = {
            let mut join_set = self.join_set.lock().await;

            // Poll for completed tasks without waiting
            while let Some(result) = join_set.try_join_next() {
                match result {
                    Ok(Ok(())) => {
                        debug!("cleaned up completed monitoring task");
                    }
                    Ok(Err(err)) => {
                        error!("cleaned up failed monitoring task: {}", err);
                    }
                    Err(err) => {
                        error!("task join error during cleanup: {}", err);
                    }
                }
            }

            join_set.len()
        };

        if active_task_count > 0 {
            debug!("active monitoring tasks: {}", active_task_count);
        }
    }

    /// sync_host is a sub thread to sync host info from the parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn sync_host(
        host_id: String,
        peer_id: String,
        parent: CollectedParent,
        hosts_info: Arc<DashMap<String, HostInfo>>,
        timeout: Duration,
        client: DfdaemonUploadClient,
        mut shutdown: Shutdown,
    ) -> Result<()> {
        let _ = parent.host.clone().ok_or_else(|| {
            error!("peer {:?} host is empty", parent);
            Error::InvalidPeer(parent.id.clone())
        })?;

        let response = client
            .sync_host(SyncHostRequest { host_id, peer_id })
            .await
            .inspect_err(|err| {
                error!("sync host info from parent {} failed: {}", parent.id, err);
            })?;

        let out_stream = response.into_inner().timeout(timeout);
        tokio::pin!(out_stream);

        // Process the stream until it ends naturally, times out, or shutdown is triggered.
        loop {
            tokio::select! {
                result = out_stream.next() => {
                    match result {
                        Some(Ok(message_result)) => {
                            match message_result {
                                Ok(message) => {
                                    info!("received host info from parent {}", parent.id);

                                    // Calculate weight from host information
                                    let weight = if let Ok(rate) = Self::available_rate(&message) {
                                        (rate / 1024.0).max(1.0) as u32
                                    } else {
                                        1
                                    };

                                    // Update the parent's host info with calculated weight.
                                    hosts_info.insert(parent.id.clone(), HostInfo::new(message, weight));
                                }
                                Err(err) => {
                                    error!(
                                        "failed to process message from parent {}: {}",
                                        parent.id, err
                                    );
                                    break;
                                }
                            }
                        }
                        Some(Err(err)) => {
                            error!("timeout or stream error from parent {}: {}", parent.id, err);
                            break;
                        }
                        None => {
                            break;
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!("shutdown signal received for parent {}", parent.id);
                    break;
                }
            }
        }

        info!(
            "parent selector connection to parent {} has been closed",
            parent.id
        );
        Ok(())
    }

    /// available_rate returns the available upload rate of a host.
    fn available_rate(host: &Host) -> Result<f64> {
        let network = match &host.network {
            Some(network) => network,
            None => return Ok(0f64),
        };

        if network.upload_rate_limit > 0 {
            let available_rate = if network.upload_rate < network.upload_rate_limit {
                network.upload_rate_limit - network.upload_rate
            } else {
                0
            };

            return Ok(available_rate as f64);
        }

        Ok(network.upload_rate as f64)
    }

    /// select_parent selects the best parent for the task based on bandwidth.
    pub fn select_parent(&self, parents: Vec<CollectedParent>) -> Result<CollectedParent> {
        if parents.is_empty() {
            return Err(Error::Unknown("empty parents".to_string()));
        }

        let weights: Vec<u32> = parents
            .iter()
            .map(|parent| {
                self.hosts_info
                    .get(&parent.id)
                    .map(|h| h.weight)
                    .unwrap_or(1)
            })
            .collect();

        match WeightedIndex::new(weights) {
            Ok(dist) => {
                let mut rng = rand::rng();
                let index = dist.sample(&mut rng);
                let selected_parent = &parents[index];

                if let Some(host_info) = self.hosts_info.get(&selected_parent.id) {
                    info!(
                        "selected parent {} with weight {}， host: {}:{}",
                        selected_parent.id,
                        host_info.weight,
                        host_info.host.ip,
                        host_info.host.port
                    );
                } else {
                    info!("selected parent {}", selected_parent.id);
                }

                Ok(parents[index].clone())
            }
            Err(_) => {
                // Fallback to last parent.
                parents
                    .first()
                    .cloned()
                    .ok_or_else(|| Error::Unknown("no parents available".to_string()))
            }
        }
    }

    /// unregister_parent removes the parent's host info and schedules connection cleanup if no longer in use.
    pub async fn unregister_parent(&self, parent_id: &str) -> Result<()> {
        self.hosts_info.remove(parent_id);

        if let Some(connection) = self.connections.get(parent_id) {
            if connection.schedule_cleanup() {
                info!("scheduled cleanup for parent {}", parent_id);
            } else {
                info!(
                    "removed parent {} host info (connection still in use)",
                    parent_id
                );
            }
        } else {
            info!("removed parent {} host info", parent_id);
        }

        Ok(())
    }

    /// unregister_all_parents removes all parents' host info and schedules cleanup.
    pub async fn unregister_all_parents(&self, parents: Vec<Peer>) -> Result<()> {
        for parent in parents {
            self.unregister_parent(&parent.id).await?;
        }
        Ok(())
    }

    /// get_connection returns a connection guard for the given parent, creating the connection if needed.
    pub async fn get_connection(
        &self,
        parent: &CollectedParent,
    ) -> Result<(ConnectionGuard, DfdaemonUploadClient)> {
        // Try to get existing connection
        if let Some(connection) = self.connections.get(&parent.id) {
            let guard = connection.acquire();
            let client = connection.client.clone();
            return Ok((guard, client));
        }

        // Create new connection
        let host = parent
            .host
            .as_ref()
            .ok_or_else(|| Error::InvalidPeer(parent.id.clone()))?;

        info!("creating new connection to parent {}", parent.id);
        let client = DfdaemonUploadClient::new(
            self.config.clone(),
            format!("http://{}:{}", host.ip, host.port),
            false,
        )
        .await?;

        let connection = ParentConnection::new(parent.id.clone(), client.clone());
        let guard = connection.acquire();

        // Insert the connection using entry API
        match self.connections.entry(parent.id.clone()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(connection);
                Ok((guard, client))
            }
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                // Another task created the connection, use the existing one
                debug!("using existing connection to parent {}", parent.id);
                let existing_connection = entry.get();
                let guard = existing_connection.acquire();
                let client = existing_connection.client.clone();
                Ok((guard, client))
            }
        }
    }

    /// register_parent starts monitoring a parent.
    pub async fn register_parent(&self, parent: &CollectedParent) -> Result<ConnectionGuard> {
        // Skip if parent already has host info and monitoring task
        if self.hosts_info.get(&parent.id).is_some() {
            let (guard, _) = self.get_connection(parent).await?;
            return Ok(guard);
        }

        // Get or create connection
        let (guard, client) = self.get_connection(parent).await?;

        // Start monitoring task for this parent
        let parent = parent.clone();
        let hosts_info = self.hosts_info.clone();
        let timeout = self.sync_interval;
        let host_id = self.id_generator.host_id();
        let peer_id = self.id_generator.peer_id();
        let shutdown = self
            .connections
            .get(&parent.id)
            .map(|conn| conn.shutdown.clone())
            .unwrap_or_default();

        let mut join_set = self.join_set.lock().await;
        join_set.spawn(
            async move {
                debug!("started sync host info for parent {}", parent.id);

                let result = Self::sync_host(
                    host_id,
                    peer_id,
                    parent.clone(),
                    hosts_info,
                    timeout,
                    client,
                    shutdown,
                )
                .await;

                if let Err(ref err) = result {
                    error!("sync host info for parent {} failed: {}", parent.id, err);
                }

                debug!("sync host info for parent {} ended", parent.id);
                result
            }
            .in_current_span(),
        );

        Ok(guard)
    }

    /// register_parents registers multiple parents.
    pub async fn register_parents(&self, parents: &[CollectedParent]) -> Result<()> {
        if parents.is_empty() {
            return Ok(());
        }

        if parents
            .iter()
            .filter(|p| !self.hosts_info.contains_key(&p.id))
            .count()
            + self.connections.len()
            > self.config.download.parent_selector.capacity
        {
            return Err(Error::Unknown("parent selector is full".to_string()));
        }

        for parent in parents {
            self.register_parent(parent).await?;
        }
        Ok(())
    }
}
