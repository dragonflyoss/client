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
use dragonfly_client_util::id_generator::IDGenerator;
use rand::distr::weighted::WeightedIndex;
use rand::distr::Distribution;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, warn, Instrument};

/// ParentConnection manages a single parent connection.
#[derive(Clone)]
struct ParentConnection {
    /// client is the dfdaemon upload client for this parent.
    client: DfdaemonUploadClient,

    /// active_connections tracks how many download tasks are using this connection.
    active_connections: Arc<AtomicUsize>,

    /// shutdown is used to signal the sync host to stop.
    shutdown: Shutdown,
}

impl ParentConnection {
    /// new creates a new ParentConnection.
    pub fn new(client: DfdaemonUploadClient) -> Self {
        Self {
            client,
            active_connections: Arc::new(AtomicUsize::new(0)),
            shutdown: Shutdown::new(),
        }
    }

    /// acquire increments the reference count.
    pub fn acquire(&self) -> ConnectionGuard {
        ConnectionGuard::new(self.active_connections.clone())
    }

    /// shutdown triggers shutdown of the sync host.
    pub fn shutdown(&self) {
        self.shutdown.trigger();
    }
}

/// ConnectionGuard automatically manages reference counting for parent connections.
pub struct ConnectionGuard {
    active_connections: Arc<AtomicUsize>,
}

impl ConnectionGuard {
    fn new(active_connections: Arc<AtomicUsize>) -> Self {
        active_connections.fetch_add(1, Ordering::SeqCst);
        Self { active_connections }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::SeqCst);
    }
}

/// ParentSelector manages parent connections and selects optimal parents.
pub struct ParentSelector {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// capacity is the maximum number of parents that can be tracked.
    capacity: usize,

    /// sync_interval represents the time interval between two refreshing probability operations.
    sync_interval: Duration,

    /// id_generator is a IDGenerator.
    id_generator: Arc<IDGenerator>,

    /// wetghts stores the latest host information and bandwidth weights for different parents.
    wetghts: Arc<DashMap<String, u32>>,

    /// connections stores parent connections with reference counting.
    connections: Arc<DashMap<String, ParentConnection>>,
}

/// ParentSelector implements the parent selector.
impl ParentSelector {
    /// new returns a ParentSelector.
    #[instrument(skip_all)]
    pub fn new(config: Arc<Config>, id_generator: Arc<IDGenerator>) -> Result<ParentSelector> {
        let config = config.clone();
        let capacity = config.download.parent_selector.capacity;
        let sync_interval = config.download.parent_selector.sync_interval;
        let wetghts = Arc::new(DashMap::new());
        let id_generator = id_generator.clone();

        let selector = ParentSelector {
            config,
            capacity,
            sync_interval,
            id_generator,
            wetghts,
            connections: Arc::new(DashMap::new()),
        };

        Ok(selector)
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
        let _ = parent.host.clone().ok_or_else(|| {
            error!("peer {:?} host is empty", parent);
            Error::InvalidPeer(parent.id.clone())
        })?;

        let response = client
            .sync_host(SyncHostRequest { host_id, peer_id })
            .await
            .inspect_err(|err| {
                error!("sync host from parent {} failed: {}", parent.id, err);
            })?;

        let out_stream = response.into_inner().timeout(timeout);
        tokio::pin!(out_stream);

        loop {
            tokio::select! {
                result = out_stream.try_next() => {
                    match result.inspect_err(|err| {
                        error!("sync host from parent {} failed: {}", parent.id, err);
                    })? {
                        Some(message) => {
                            let message = message?;
                            info!("parent selector: received host info from parent {}", parent.id);

                            // Calculate weight from host information.
                            let weight = if let Ok(rate) = Self::available_rate(&message) {
                                (rate / 1024.0).max(1.0) as u32
                            } else {
                                1
                            };

                            // Update the parent's host info with calculated weight.
                            weights.insert(parent.id.clone(), weight);
                        }
                        None => break,
                    }
                }
                _ = shutdown.recv() => {
                    debug!("parent selector: shutdown signal received for parent {}", parent.id);
                    break;
                }
            }
        }

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
            .map(|parent| self.wetghts.get(&parent.id).map(|w| *w).unwrap_or(1))
            .collect();

        match WeightedIndex::new(weights) {
            Ok(dist) => {
                let mut rng = rand::rng();
                let index = dist.sample(&mut rng);
                let selected_parent = &parents[index];
                debug!("selected parent {}", selected_parent.id);

                Ok(selected_parent.clone())
            }
            Err(_) => parents
                .first()
                .cloned()
                .ok_or_else(|| Error::Unknown("no parents available".to_string())),
        }
    }

    /// unregister_parents removes the weights of the given parents and triggers shutdown.
    pub async fn unregister_parents(&self, parents: Vec<Peer>) -> Result<()> {
        for parent in parents {
            self.wetghts.remove(&parent.id);

            if let Some(connection) = self.connections.get(&parent.id) {
                connection.shutdown();
            }
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

        let connection = ParentConnection::new(client.clone());
        let guard = connection.acquire();

        match self.connections.entry(parent.id.clone()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(connection);
                Ok((guard, client))
            }
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                debug!("using existing connection to parent {}", parent.id);
                let existing_connection = entry.get();
                let guard = existing_connection.acquire();
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

        let size = parents
            .iter()
            .filter(|p| !self.wetghts.contains_key(&p.id))
            .count()
            + self.connections.len();

        if size > self.capacity {
            return Err(Error::Unknown(format!(
                "capacity is exceeded, size: {}, capacity: {}",
                size, self.capacity
            )));
        }

        let mut join_set = JoinSet::new();

        for parent in parents {
            debug!("registering parent {}", parent.id);

            // Get or create connection for the sync host
            let (guard, client) = self.get_connection(parent).await?;

            // Start sync host for this parent
            let parent = parent.clone();
            let weights = self.wetghts.clone();
            let connections = self.connections.clone();
            let timeout = self.sync_interval;
            let host_id = self.id_generator.host_id();
            let peer_id = self.id_generator.peer_id();
            let shutdown = self
                .connections
                .get(&parent.id)
                .map(|conn| conn.shutdown.clone())
                .unwrap_or_default();

            join_set.spawn(
                async move {
                    debug!("started sync host for parent {}", parent.id);

                    let result = Self::sync_host(
                        host_id,
                        peer_id,
                        parent.clone(),
                        weights,
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
                    if let Some(connection) = connections.get(&parent.id) {
                        if connection.active_connections.load(Ordering::SeqCst) == 0 {
                            debug!("cleaning up unused connection to parent {}", parent.id);
                            connections.remove(&parent.id);
                        }
                    }

                    result
                }
                .in_current_span(),
            );
        }

        // Spawn a task to manage this JoinSet
        tokio::spawn(async move {
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok(())) => {
                        debug!("parent sync host completed successfully");
                    }
                    Ok(Err(err)) => {
                        error!("parent sync host failed: {}", err);
                    }
                    Err(err) => {
                        error!("parent sync host join error: {}", err);
                    }
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_api::common::v2::{Host, Network};
    use dragonfly_client_util::id_generator::IDGenerator;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_new() {
        let test_cases = vec![
            (
                "default config",
                5,
                Duration::from_millis(100),
                5,
                Duration::from_millis(100),
            ),
            (
                "custom config",
                10,
                Duration::from_secs(1),
                10,
                Duration::from_secs(1),
            ),
            (
                "zero capacity",
                0,
                Duration::from_millis(50),
                0,
                Duration::from_millis(50),
            ),
        ];

        for (_, capacity, sync_interval, expected_capacity, expected_sync_interval) in test_cases {
            let mut config = Config::default();
            config.download.parent_selector.capacity = capacity;
            config.download.parent_selector.sync_interval = sync_interval;
            config.download.parent_selector.enable = true;
            let config = Arc::new(config);
            let id_generator = Arc::new(IDGenerator::new(
                "127.0.0.1".to_string(),
                "test-host".to_string(),
                false,
            ));

            let selector = ParentSelector::new(config, id_generator).unwrap();

            assert_eq!(selector.capacity, expected_capacity);
            assert_eq!(selector.sync_interval, expected_sync_interval);
            assert_eq!(selector.wetghts.len(), 0);
            assert_eq!(selector.connections.len(), 0);
        }
    }

    #[test]
    fn test_available_rate() {
        let test_cases = vec![
            (
                "with upload rate limit",
                Host {
                    network: Some(Network {
                        upload_rate: 1000,
                        upload_rate_limit: 2000,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                1000.0, // 2000 - 1000
            ),
            (
                "without upload rate limit",
                Host {
                    network: Some(Network {
                        upload_rate: 1500,
                        upload_rate_limit: 0,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                1500.0,
            ),
            (
                "upload rate exceeds limit",
                Host {
                    network: Some(Network {
                        upload_rate: 2500,
                        upload_rate_limit: 2000,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                0.0,
            ),
            (
                "no network information",
                Host {
                    network: None,
                    ..Default::default()
                },
                0.0,
            ),
        ];

        for (_, host, expected_rate) in test_cases {
            let rate = ParentSelector::available_rate(&host).unwrap();
            assert_eq!(rate, expected_rate);
        }
    }

    #[test]
    fn test_select_parent() {
        let mut config = Config::default();
        config.download.parent_selector.capacity = 5;
        config.download.parent_selector.sync_interval = Duration::from_millis(100);
        config.download.parent_selector.enable = true;
        let config = Arc::new(config);
        let id_generator = Arc::new(IDGenerator::new(
            "127.0.0.1".to_string(),
            "test-host".to_string(),
            false,
        ));
        let selector = ParentSelector::new(config, id_generator).unwrap();

        let test_cases = vec![
            ("empty list", vec![], vec![], false, vec![]),
            (
                "single parent",
                vec![CollectedParent {
                    id: "parent1".to_string(),
                    host: Some(Host {
                        id: "host-parent1".to_string(),
                        ip: "127.0.0.1".to_string(),
                        port: 8080,
                        ..Default::default()
                    }),
                }],
                vec![],
                true,
                vec!["parent1".to_string()],
            ),
            (
                "multiple parents with weights",
                vec![
                    CollectedParent {
                        id: "parent1".to_string(),
                        host: Some(Host {
                            id: "host-parent1".to_string(),
                            ip: "127.0.0.1".to_string(),
                            port: 8080,
                            ..Default::default()
                        }),
                    },
                    CollectedParent {
                        id: "parent2".to_string(),
                        host: Some(Host {
                            id: "host-parent2".to_string(),
                            ip: "127.0.0.1".to_string(),
                            port: 8081,
                            ..Default::default()
                        }),
                    },
                ],
                vec![("parent1".to_string(), 10), ("parent2".to_string(), 20)],
                true,
                vec!["parent1".to_string(), "parent2".to_string()],
            ),
            (
                "multiple parents without weights",
                vec![
                    CollectedParent {
                        id: "parent3".to_string(),
                        host: Some(Host {
                            id: "host-parent3".to_string(),
                            ip: "127.0.0.1".to_string(),
                            port: 8082,
                            ..Default::default()
                        }),
                    },
                    CollectedParent {
                        id: "parent4".to_string(),
                        host: Some(Host {
                            id: "host-parent4".to_string(),
                            ip: "127.0.0.1".to_string(),
                            port: 8083,
                            ..Default::default()
                        }),
                    },
                ],
                vec![],
                true,
                vec!["parent3".to_string(), "parent4".to_string()],
            ),
        ];

        for (name, parents, weights, should_succeed, expected_ids) in test_cases {
            // Set up weights for this test case
            selector.wetghts.clear();
            for (id, weight) in &weights {
                selector.wetghts.insert(id.clone(), *weight);
            }

            let result = selector.select_parent(parents);

            if should_succeed {
                assert!(result.is_ok());
                let selected = result.unwrap();
                assert!(expected_ids.contains(&selected.id));
            } else {
                assert!(result.is_err());
                if name == "empty list" {
                    assert!(result.unwrap_err().to_string().contains("empty parents"));
                }
            }
        }
    }
}
