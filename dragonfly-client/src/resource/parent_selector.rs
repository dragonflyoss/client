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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, warn, Instrument};

/// Connection manages a single parent connection.
#[derive(Clone)]
struct Connection {
    /// client is the dfdaemon upload client for this parent.
    client: DfdaemonUploadClient,

    /// connection_guard tracks how many download tasks are using this connection.
    connection_guard: ConnectionGuard,

    /// shutdown is used to signal the sync host to stop.
    shutdown: Shutdown,
}

impl Connection {
    /// new creates a new Connection.
    pub fn new(client: DfdaemonUploadClient) -> Self {
        Self {
            client,
            connection_guard: ConnectionGuard::new(Arc::new(AtomicUsize::new(0))),
            shutdown: Shutdown::new(),
        }
    }

    /// connection_guard increments the reference count.
    pub fn connection_guard(&self) {
        self.connection_guard.acquire();
    }

    /// active_count returns the number of active requests.
    pub fn active_requests(&self) -> usize {
        self.connection_guard.active_requests()
    }

    /// release_request decrements the reference count.
    pub fn release_request(&self) {
        drop(self.connection_guard.clone());
    }

    /// shutdown triggers shutdown of the sync host.
    pub fn shutdown(&self) {
        self.shutdown.trigger();
    }
}

/// ConnectionGuard automatically manages reference counting for parent connections.
pub struct ConnectionGuard {
    active_requests: Arc<AtomicUsize>,
}

impl ConnectionGuard {
    fn new(active_connections: Arc<AtomicUsize>) -> Self {
        Self {
            active_requests: active_connections,
        }
    }

    fn active_requests(&self) -> usize {
        self.active_requests.load(Ordering::SeqCst)
    }

    fn acquire(&self) {
        self.active_requests.fetch_add(1, Ordering::SeqCst);
    }
}

impl Clone for ConnectionGuard {
    fn clone(&self) -> Self {
        Self {
            active_requests: self.active_requests.clone(),
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.active_requests.fetch_sub(1, Ordering::SeqCst);
    }
}

/// ParentSelector manages parent connections and selects optimal parents.
pub struct ParentSelector {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// sync_interval represents the time interval between two refreshing probability operations.
    sync_interval: Duration,

    /// timeout is the timeout for the sync host.
    timeout: Duration,

    /// host_id is the id of the host.
    host_id: String,

    /// peer_id is the id of the peer.
    peer_id: String,

    /// wetghts stores the latest host information and bandwidth weights for different parents.
    wetghts: Arc<DashMap<String, u32>>,

    /// connections stores parent connections with reference counting.
    connections: Arc<DashMap<String, Connection>>,

    /// shutdown is used to shutdown the garbage collector.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the garbage collector is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// ParentSelector implements the parent selector.
impl ParentSelector {
    /// new returns a ParentSelector.
    #[instrument(skip_all)]
    pub fn new(
        config: Arc<Config>,
        host_id: String,
        peer_id: String,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> ParentSelector {
        let config = config.clone();
        let sync_interval = config.download.parent_selector.sync_interval;
        let timeout = config.download.parent_selector.timeout;
        let wetghts = Arc::new(DashMap::new());

        Self {
            config,
            sync_interval,
            timeout,
            host_id,
            peer_id,
            wetghts,
            connections: Arc::new(DashMap::new()),
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// sync_host is a sub thread to sync host info from the parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn sync_host(
        host_id: String,
        peer_id: String,
        parent: CollectedParent,
        weights: Arc<DashMap<String, u32>>,
        timeout: Duration,
        sync_interval: Duration,
        client: DfdaemonUploadClient,
        mut shutdown: Shutdown,
        mut dfdaemon_shutdown: Shutdown,
    ) -> Result<()> {
        let remote_host_id = Self::get_host_id(&parent.host);

        let response = client
            .sync_host(SyncHostRequest { host_id, peer_id })
            .await
            .inspect_err(|err| {
                error!("sync host from host {} failed: {}", remote_host_id, err);
            })?;

        let out_stream = response.into_inner().timeout(timeout);
        tokio::pin!(out_stream);

        loop {
            tokio::select! {
                result = out_stream.try_next() => {
                    match result.inspect_err(|err| {
                        error!("sync host from host {} failed: {}", remote_host_id, err);
                    })? {
                        Some(message) => {
                            debug!("sync host from host {} received message", remote_host_id);
                            let message = message?;

                            // Calculate weight from host information.
                            let weight = Self::get_idle_upload_rate(&message) as u32;

                            // Update the parent's host info with calculated weight.
                            weights.insert(remote_host_id.clone(), weight);
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(sync_interval) => {
                    // Continue the loop after sync_interval
                }
                _ = shutdown.recv() => {
                    debug!("parent selector: shutdown signal received for host {}", remote_host_id);
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

    /// select_parent selects the best parent for the task based on bandwidth.
    pub fn select_parent(&self, parents: Vec<CollectedParent>) -> Option<CollectedParent> {
        let remote_hosts: Vec<String> = parents
            .iter()
            .map(|parent| Self::get_host_id(&parent.host))
            .collect();
        let weights: Vec<u32> = remote_hosts
            .iter()
            .map(|remote_host| self.wetghts.get(remote_host).map(|w| *w).unwrap_or(0))
            .collect();

        match WeightedIndex::new(weights) {
            Ok(dist) => {
                let mut rng = rand::rng();
                let index = dist.sample(&mut rng);
                let selected_parent = &parents[index];
                debug!("selected parent {}", selected_parent.id);

                Some(selected_parent.clone())
            }
            Err(_) => parents.get(fastrand::usize(..parents.len())).cloned(),
        }
    }

    /// unregister_parents removes the weights of the given parents and triggers shutdown.
    pub async fn unregister_parents(&self, parents: Vec<Peer>) {
        for parent in parents {
            let host_id = Self::get_host_id(&parent.host);
            if let Some(connection) = self.connections.get(&host_id) {
                connection.release_request();
                if connection.active_requests() == 0 {
                    info!("shutting down sync host for parent {}", host_id);
                    connection.shutdown();
                }
            }
        }
    }

    /// get_connection returns a connection guard for the given parent, creating the connection if needed.
    pub async fn get_connection(&self, parent: &CollectedParent) -> Result<DfdaemonUploadClient> {
        let remote_host_id = Self::get_host_id(&parent.host);

        // Try to get existing connection
        if let Some(connection) = self.connections.get(&remote_host_id) {
            connection.connection_guard();
            return Ok(connection.client.clone());
        }

        let host = parent
            .host
            .as_ref()
            .ok_or_else(|| Error::InvalidPeer(parent.id.clone()))?;

        let client = DfdaemonUploadClient::new(
            self.config.clone(),
            format!("http://{}:{}", host.ip, host.port),
            false,
        )
        .await?;

        let connection = Connection::new(client.clone());
        connection.connection_guard();

        match self.connections.entry(remote_host_id.clone()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(connection);
                Ok(client)
            }
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let existing_connection = entry.get();
                existing_connection.connection_guard();
                let client = existing_connection.client.clone();
                Ok(client)
            }
        }
    }

    /// register_parents registers multiple parents.
    pub async fn register_parents(&self, parents: &[CollectedParent]) -> Result<()> {
        let dfdaemon_shutdown = self.shutdown.clone();

        let mut join_set = JoinSet::<Result<()>>::new();

        for parent in parents {
            let remote_host_id = Self::get_host_id(&parent.host);

            // Check if connection already has active requests
            if let Some(connection) = self.connections.get(&remote_host_id) {
                if connection.active_requests() > 0 {
                    debug!("sync host already running for parent {}", parent.id);
                    continue;
                }
            }

            // Get or create connection for the sync host.
            let client = self.get_connection(parent).await?;

            // Start sync host for this parent.
            let parent = parent.clone();
            let weights = self.wetghts.clone();
            let connections = self.connections.clone();
            let timeout = self.timeout;
            let sync_interval = self.sync_interval;
            let host_id = self.host_id.clone();
            let peer_id = self.peer_id.clone();
            let shutdown = self
                .connections
                .get(&remote_host_id)
                .map(|conn| conn.shutdown.clone())
                .unwrap_or_default();
            let dfdaemon_shutdown_clone = dfdaemon_shutdown.clone();

            join_set.spawn(
                async move {
                    info!("started sync host for parent {}", parent.id);

                    if let Err(err) = Self::sync_host(
                        host_id,
                        peer_id,
                        parent.clone(),
                        weights.clone(),
                        timeout,
                        sync_interval,
                        client,
                        shutdown,
                        dfdaemon_shutdown_clone,
                    )
                    .await
                    {
                        error!("sync host for parent {} failed: {}", parent.id, err);
                        return Err(err);
                    }

                    // Check if connection should be cleaned up.
                    if let Some(connection) = connections.get(&remote_host_id) {
                        if connection.active_requests() == 0 {
                            weights.remove(&remote_host_id);
                            connections.remove(&remote_host_id);
                        }
                    }

                    Ok(())
                }
                .in_current_span(),
            );
        }

        // Spawn a task to manage this JoinSet.
        tokio::spawn(async move {
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok(_)) => debug!("parent sync host completed successfully"),
                    Ok(Err(_)) => debug!("parent sync host failed"),
                    Err(err) => debug!("parent sync host join error: {}", err),
                }
            }
        });

        Ok(())
    }

    /// get_idle_upload_rate returns the available upload rate of a host.
    fn get_idle_upload_rate(host: &Host) -> f64 {
        let network = match &host.network {
            Some(network) => network,
            None => return 0f64,
        };

        let idle_upload_rate = if network.upload_rate < network.upload_rate_limit {
            network.upload_rate_limit - network.upload_rate
        } else {
            0
        };

        idle_upload_rate as f64
    }

    /// get_host_id extracts the host id from parent id.
    fn get_host_id(parent: &Option<Host>) -> String {
        let Some(host) = parent.as_ref() else {
            error!("parent is not found");
            return String::new();
        };

        let id_generator = IDGenerator::new(host.ip.clone(), host.hostname.clone(), false);
        id_generator.host_id()
    }
}
