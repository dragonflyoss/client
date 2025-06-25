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
use crate::shutdown::Shutdown;
use dashmap::DashMap;
use dragonfly_api::common::v2::{Host, Peer};
use dragonfly_api::dfdaemon::v2::SyncHostRequest;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Error;
use dragonfly_client_core::Result;
use rand::distr::weighted::WeightedIndex;
use rand::distr::Distribution;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, warn, Instrument};

/// Connection manages a single parent connection.
#[derive(Clone)]
struct Connection {
    /// client is the dfdaemon upload client for this parent.
    client: DfdaemonUploadClient,

    /// active_requests tracks how many download tasks are using this connection.
    active_requests: Arc<AtomicUsize>,

    /// shutdown is used to signal the sync host to stop.
    shutdown: Shutdown,
}

impl Connection {
    /// new creates a new Connection.
    pub fn new(client: DfdaemonUploadClient) -> Self {
        Self {
            client,
            active_requests: Arc::new(AtomicUsize::new(0)),
            shutdown: Shutdown::new(),
        }
    }

    /// connection_guard increments the reference count.
    pub fn connection_guard(&self) -> ConnectionGuard {
        ConnectionGuard::new(self.active_requests.clone())
    }

    /// active_count returns the number of active requests.
    pub fn active_count(&self) -> usize {
        self.active_requests.load(Ordering::SeqCst)
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
        active_connections.fetch_add(1, Ordering::SeqCst);
        Self {
            active_requests: active_connections,
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

    /// host_id is the id of the host.
    host_id: String,

    /// peer_id is the id of the peer.
    peer_id: String,

    /// wetghts stores the latest host information and bandwidth weights for different parents.
    wetghts: Arc<DashMap<String, u32>>,

    /// connections stores parent connections with reference counting.
    connections: Arc<DashMap<String, Connection>>,
}

/// ParentSelector implements the parent selector.
impl ParentSelector {
    /// new returns a ParentSelector.
    #[instrument(skip_all)]
    pub fn new(config: Arc<Config>, host_id: String, peer_id: String) -> ParentSelector {
        let config = config.clone();
        let sync_interval = config.download.parent_selector.sync_interval;
        let wetghts = Arc::new(DashMap::new());

        Self {
            config,
            sync_interval,
            host_id,
            peer_id,
            wetghts,
            connections: Arc::new(DashMap::new()),
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
        client: DfdaemonUploadClient,
        mut shutdown: Shutdown,
    ) -> Result<()> {
        let remote_host_id = Self::get_host_id(&parent.id);

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
                            let message = message?;

                            // Calculate weight from host information.
                            let weight = Self::get_idle_upload_rate(&message) as u32;

                            // Update the parent's host info with calculated weight.
                            weights.insert(remote_host_id.clone(), weight);
                        }
                        None => break,
                    }
                }
                _ = shutdown.recv() => {
                    debug!("parent selector: shutdown signal received for host {}", remote_host_id);
                    break;
                }
            }
        }

        Ok(())
    }

    /// select_parent selects the best parent for the task based on bandwidth.
    pub fn select_parent(&self, parents: Vec<CollectedParent>) -> Option<CollectedParent> {
        if parents.is_empty() {
            return None;
        }

        let remote_hosts: Vec<String> = parents
            .iter()
            .map(|parent| Self::get_host_id(&parent.id))
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
            let host_id = Self::get_host_id(&parent.id);
            if let Some(connection) = self.connections.get(&host_id) {
                connection.active_requests.fetch_sub(1, Ordering::SeqCst);
                if connection.active_count() == 0 {
                    connection.shutdown();
                }
            }
        }
    }

    /// get_connection returns a connection guard for the given parent, creating the connection if needed.
    pub async fn get_connection(
        &self,
        parent: &CollectedParent,
    ) -> Result<(ConnectionGuard, DfdaemonUploadClient)> {
        let remote_host_id = Self::get_host_id(&parent.id);

        // Try to get existing connection
        if let Some(connection) = self.connections.get(&remote_host_id) {
            let guard = connection.connection_guard();
            let client = connection.client.clone();
            return Ok((guard, client));
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
        let guard = connection.connection_guard();

        match self.connections.entry(remote_host_id.clone()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(connection);
                Ok((guard, client))
            }
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let existing_connection = entry.get();
                let guard = existing_connection.connection_guard();
                let client = existing_connection.client.clone();
                Ok((guard, client))
            }
        }
    }

    /// register_parents registers multiple parents.
    pub async fn register_parents(&self, parents: &[CollectedParent]) -> Result<()> {
        if parents.is_empty() {
            return Ok(());
        }

        let mut join_set = JoinSet::new();

        for parent in parents {
            let remote_host_id = Self::get_host_id(&parent.id);

            // Check if connection already has active requests
            if let Some(connection) = self.connections.get(&remote_host_id) {
                if connection.active_count() > 0 {
                    info!("sync host already running for parent {}", parent.id);
                    continue;
                }
            }

            // Get or create connection for the sync host.
            let (guard, client) = self.get_connection(parent).await?;

            // Start sync host for this parent.
            let parent = parent.clone();
            let weights = self.wetghts.clone();
            let connections = self.connections.clone();
            let timeout = self.sync_interval;
            let host_id = self.host_id.clone();
            let peer_id = self.peer_id.clone();
            let shutdown = self
                .connections
                .get(&remote_host_id)
                .map(|conn| conn.shutdown.clone())
                .unwrap_or_default();

            join_set.spawn(
                async move {
                    info!("started sync host for parent {}", parent.id);

                    let result = Self::sync_host(
                        host_id,
                        peer_id,
                        parent.clone(),
                        weights.clone(),
                        timeout,
                        client,
                        shutdown,
                    )
                    .await;

                    if let Err(ref err) = result {
                        error!("sync host for parent {} failed: {}", parent.id, err);
                    }

                    drop(guard);

                    // Check if connection should be cleaned up.
                    if let Some(connection) = connections.get(&remote_host_id) {
                        if connection.active_count() == 0 {
                            weights.remove(&remote_host_id);
                            connections.remove(&remote_host_id);
                        }
                    }

                    result
                }
                .in_current_span(),
            );
        }

        // Spawn a task to manage this JoinSet.
        tokio::spawn(async move {
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok(())) => {
                        debug!("parent sync host completed successfully");
                    }
                    Ok(Err(err)) => {
                        debug!("parent sync host failed: {}", err);
                    }
                    Err(err) => {
                        debug!("parent sync host join error: {}", err);
                    }
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
    fn get_host_id(parent_id: &str) -> String {
        let parts: Vec<&str> = parent_id.split('-').collect();
        if parts.len() >= 2 {
            if parent_id.ends_with("-seed") {
                format!("{}-{}-seed", parts[0], parts[1])
            } else {
                format!("{}-{}", parts[0], parts[1])
            }
        } else {
            parent_id.to_string()
        }
    }
}
