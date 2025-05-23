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
use dragonfly_api::common::v2::Host;
use dragonfly_api::dfdaemon::v2::SyncHostRequest;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Error;
use dragonfly_client_core::Result;
use dragonfly_client_storage::cache::LruCache;
use dragonfly_client_util::id_generator::IDGenerator;
use rand::distr::weighted::WeightedIndex;
use rand::distr::Distribution;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_stream::StreamExt;
use tracing::{error, info, instrument, warn, Instrument};

/// ParentSync manages the synchronization of host information from a parent.
#[derive(Clone)]
struct ParentSync {
    /// parent_id is the id of the parent this controller syncs with.
    parent_id: String,

    /// shutdown used to stop the synchronization.
    shutdown: Shutdown,
}

/// ParentSelector represents a parent selector.
#[allow(dead_code)]
pub struct ParentSelector {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// sync_interval represents the time interval between two refreshing probability operations.
    sync_interval: Duration,

    /// hosts is a lru cache to store host sync controllers.
    hosts: Arc<Mutex<LruCache<String, ParentSync>>>,

    /// id_generator is a IDGenerator.
    id_generator: Arc<IDGenerator>,

    /// hosts_info is the latest host info of different parents.
    hosts_info: Arc<DashMap<String, Host>>,

    /// hosts_weights stores the cached bandwidth weight for each parent.
    hosts_weights: Arc<DashMap<String, u32>>,
}

/// TaskParentSelector implements the task parent selector.
#[allow(dead_code)]
impl ParentSelector {
    /// new returns a ParentSelector.
    #[instrument(skip_all)]
    pub fn new(config: Arc<Config>, id_generator: Arc<IDGenerator>) -> Result<ParentSelector> {
        let config = config.clone();
        let sync_interval = config.download.parent_selector.sync_interval;
        let parent_cache = LruCache::new(config.download.parent_selector.capacity);
        let id_generator = id_generator.clone();
        let hosts_info = Arc::new(DashMap::new());
        let hosts_weights = Arc::new(DashMap::new());

        Ok(ParentSelector {
            config,
            sync_interval,
            hosts: Arc::new(Mutex::new(parent_cache)),
            id_generator,
            hosts_info,
            hosts_weights,
        })
    }

    /// run registers the given parents and starts monitoring their bandwidth.
    #[instrument(skip_all)]
    pub async fn run(&self, parents: &[CollectedParent]) -> Result<()> {
        if !self.config.download.parent_selector.enable {
            info!("parent registration is disabled in config");
            return Ok(());
        }

        if parents.is_empty() {
            return Err(Error::Unknown(
                "no parents provided for registration".to_string(),
            ));
        }

        // Get current host_id to avoid connecting to self
        let current_host_id = self.id_generator.host_id();

        info!("register {} parents", parents.len());

        let cache = self.hosts.clone();
        let mut cache = cache
            .lock()
            .map_err(|_| Error::Unknown("failed to acquire cache lock".to_string()))?;
        let config = self.config.clone();
        let hosts_info = self.hosts_info.clone();
        let bandwidth_scores = self.hosts_weights.clone();

        for parent in parents {
            // Skip self to avoid self-connection
            if parent.id == current_host_id {
                info!("skipping self parent {}", parent.id);
                continue;
            }

            if cache.get(&parent.id).is_some() {
                info!("parent {} is already registered", parent.id);
                continue;
            }

            let shutdown = Shutdown::new();
            let parent_sync = ParentSync {
                parent_id: parent.id.clone(),
                shutdown,
            };

            if let Some(evicted) = cache.put(parent.id.clone(), parent_sync.clone()) {
                evicted.shutdown.trigger();
                hosts_info.remove(&evicted.parent_id);

                // Remove bandwidth score for evicted parent
                bandwidth_scores.remove(&evicted.parent_id);

                info!("evicted parent {} due to cache capacity", evicted.parent_id);
            }

            if let Some(host) = &parent.host {
                info!(
                    "registered parent {} at {}:{}",
                    parent.id, host.ip, host.port
                );
            } else {
                warn!("registered parent {} without host information", parent.id);
            }

            let config = config.clone();
            let host_id = self.id_generator.host_id();
            let peer_id = self.id_generator.peer_id();
            let parent = parent.clone();
            let shutdown = parent_sync.shutdown.clone();
            let hosts_info = hosts_info.clone();
            let bandwidth_scores = bandwidth_scores.clone();
            let timeout = self.sync_interval;

            tokio::spawn(
                async move {
                    Self::sync_host(
                        config,
                        host_id,
                        peer_id,
                        parent.clone(),
                        hosts_info,
                        bandwidth_scores,
                        shutdown,
                        timeout,
                    )
                    .await
                    .unwrap_or_else(|err| {
                        error!("failed to monitor parent {}: {}", parent.id, err);
                    });
                }
                .in_current_span(),
            );
        }

        info!("successfully registered all parents");
        Ok(())
    }

    /// sync_host is a sub thread to sync host info from the parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn sync_host(
        config: Arc<Config>,
        host_id: String,
        peer_id: String,
        parent: CollectedParent,
        hosts_info: Arc<DashMap<String, Host>>,
        bandwidth_scores: Arc<DashMap<String, u32>>,
        shutdown: Shutdown,
        timeout: Duration,
    ) -> Result<()> {
        info!("sync host info from parent {}", parent.id);

        let host = parent.host.clone().ok_or_else(|| {
            error!("peer {:?} host is empty", parent);
            Error::InvalidPeer(parent.id.clone())
        })?;

        // Create a dfdaemon client.
        let dfdaemon_upload_client = DfdaemonUploadClient::new(
            config.clone(),
            format!("http://{}:{}", host.ip, host.port),
            false,
        )
        .await
        .inspect_err(|err| {
            error!(
                "create dfdaemon upload client from parent {} failed: {}",
                parent.id, err
            );
        })?;

        info!("create dfdaemon upload client for host {}:", host.ip);

        let response = dfdaemon_upload_client
            .sync_host(SyncHostRequest { host_id, peer_id })
            .await
            .inspect_err(|err| {
                error!("sync host info from parent {} failed: {}", parent.id, err);
            })?;

        let hosts_info = hosts_info.clone();
        let out_stream = response.into_inner().timeout(timeout);
        tokio::pin!(out_stream);

        info!("receive host info from parent {}", parent.id);
        while let Some(result) = out_stream.next().await {
            match result {
                Ok(message_result) => {
                    match message_result {
                        Ok(message) => {
                            if shutdown.is_shutdown() {
                                break;
                            }

                            // Deal with message.
                            // Update the parent's host info if exists.
                            hosts_info.insert(parent.id.clone(), message.clone());
                            info!(
                                "sync host info from parent {} message: {:?}",
                                parent.id, message
                            );

                            // Calculate and update bandwidth score after each sync
                            if let Ok(rate) = Self::available_rate(&message) {
                                // Convert bandwidth to weight score (u32)
                                // Using 1 as minimum weight to ensure all parents have a chance
                                info!("rate: {:?}", rate);
                                let weight = (rate / 1024.0).max(1.0) as u32;

                                // Update the bandwidth score in cache
                                bandwidth_scores.insert(parent.id.clone(), weight);

                                info!(
                                    "updated bandwidth score for parent {}: {}",
                                    parent.id, weight
                                );
                            }
                        },
                        Err(err) => {
                            error!("failed to process message from parent {}: {}", parent.id, err);
                            break;
                        }
                    }
                },
                Err(err) => {
                    error!("timeout or stream error from parent {}: {}", parent.id, err);
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
            error!("empty parents");
            return Err(Error::Unknown("empty parents".to_string()));
        }

        let weights: Vec<u32> = parents
            .iter()
            .map(|parent| self.hosts_weights.get(&parent.id).map(|w| *w).unwrap_or(1))
            .collect();

        info!("weights: {:?}", weights);

        match WeightedIndex::new(weights) {
            Ok(dist) => {
                let mut rng = rand::rng();
                let index = dist.sample(&mut rng);

                Ok(parents[index].clone())
            }
            Err(_) => {
                // Fallback to last parent.
                parents
                    .last()
                    .cloned()
                    .ok_or_else(|| Error::Unknown("no parents available".to_string()))
            }
        }
    }
}
