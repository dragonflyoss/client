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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, warn, Instrument};

/// ParentSync manages the synchronization of host information from a parent.
#[derive(Clone)]
struct ParentSync {
    /// parent_id is the id of the parent this controller syncs with.
    parent_id: String,

    /// shutdown used to stop the synchronization.
    shutdown: Shutdown,

    /// ref_count tracks the number of tasks using this parent.
    ref_count: Arc<AtomicU32>,

    /// cleanup_timeout is the timeout before releasing the connection when ref_count reaches 0.
    cleanup_timeout: Duration,

    /// cleanup_in_progress marks if this parent is already being cleaned up.
    cleanup_in_progress: Arc<AtomicBool>,
}

impl ParentSync {
    /// new creates a new ParentSync.
    pub fn new(parent_id: String, cleanup_timeout: Duration) -> Self {
        Self {
            parent_id,
            shutdown: Shutdown::new(),
            ref_count: Arc::new(AtomicU32::new(0)),
            cleanup_timeout,
            cleanup_in_progress: Arc::new(AtomicBool::new(false)),
        }
    }

    /// increment_ref increments the reference count for this parent.
    pub async fn increment_ref(&self) -> u32 {
        let count = self.ref_count.fetch_add(1, Ordering::SeqCst) + 1;

        if count == 1 {
            if self.cleanup_in_progress.load(Ordering::SeqCst) {
                self.cleanup_in_progress.store(false, Ordering::SeqCst);
            }
        }

        count
    }

    /// decrement_ref decrements the reference count and schedules cleanup if needed.
    pub async fn decrement_ref(&self) -> u32 {
        let count = self.ref_count.fetch_sub(1, Ordering::SeqCst) - 1;

        if count == 0 {
            if self
                .cleanup_in_progress
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                let shutdown = self.shutdown.clone();
                let cleanup_timeout = self.cleanup_timeout;
                let ref_count = self.ref_count.clone();
                let cleanup_in_progress = self.cleanup_in_progress.clone();

                tokio::spawn(async move {
                    sleep(cleanup_timeout).await;

                    if ref_count.load(Ordering::SeqCst) == 0 {
                        shutdown.trigger();
                    } else {
                        cleanup_in_progress.store(false, Ordering::SeqCst);
                    }
                });
            } else {
                debug!(
                    "parent {} cleanup already scheduled by another thread",
                    self.parent_id
                );
            }
        }

        count
    }
}

/// ParentSelector represents a parent selector.
#[allow(dead_code)]
pub struct ParentSelector {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// active_connections tracks the current number of active connections.
    active_connections: Arc<AtomicUsize>,

    /// sync_interval represents the time interval between two refreshing probability operations.
    sync_interval: Duration,

    /// hosts stores parent sync controllers.
    hosts: Arc<DashMap<String, ParentSync>>,

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
        let active_connections = Arc::new(AtomicUsize::new(0));
        let sync_interval = config.download.parent_selector.sync_interval;
        let hosts = Arc::new(DashMap::new());
        let id_generator = id_generator.clone();
        let hosts_info = Arc::new(DashMap::new());
        let hosts_weights = Arc::new(DashMap::new());

        Ok(ParentSelector {
            config,
            active_connections,
            sync_interval,
            hosts,
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

        // Check if the number of parents exceeds available capacity
        let current_connections = self.active_connections.load(Ordering::SeqCst);
        let capacity = self.config.download.parent_selector.capacity;
        let available_slots = capacity.saturating_sub(current_connections);
        
        if parents.len() > available_slots {
            return Ok(());
        }

        let hosts_info = self.hosts_info.clone();
        let hosts_weights = self.hosts_weights.clone();

        for parent in parents {
            // Check capacity limit before adding new connections
            if self.is_full() {
                continue;
            }

            let parent_sync = {
                if self.hosts.get(&parent.id).is_some() {
                    if let Err(err) = self.register_parent(&parent.id).await {
                        warn!(
                            "failed to register task for existing parent {}: {}",
                            parent.id, err
                        );
                    }
                    continue;
                }

                if self.is_full() {
                    continue;
                }

                // Create new parent sync
                let parent_sync = ParentSync::new(parent.id.clone(), self.sync_interval);

                // Insert the new parent and increment counter atomically
                self.hosts.insert(parent.id.clone(), parent_sync.clone());
                self.active_connections.fetch_add(1, Ordering::SeqCst);

                // Register this parent to initialize its reference count
                if let Err(err) = self.register_parent(&parent.id).await {
                    warn!("failed to register task for parent {}: {}", parent.id, err);
                } else {
                    info!("registered and initialized parent {}", parent.id);
                }

                parent_sync
            };

            let config = self.config.clone();
            let host_id = self.id_generator.host_id();
            let peer_id = self.id_generator.peer_id();
            let parent = parent.clone();
            let shutdown = parent_sync.shutdown.clone();
            let hosts_info = hosts_info.clone();
            let hosts_weights = hosts_weights.clone();
            let timeout = self.sync_interval;

            tokio::spawn(
                async move {
                    Self::sync_host(
                        config,
                        host_id,
                        peer_id,
                        parent.clone(),
                        hosts_info,
                        hosts_weights,
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

        info!(
            "successfully registered parents, active connections: {}/{}",
            self.active_connections.load(Ordering::SeqCst),
            self.config.download.parent_selector.capacity
        );
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
        hosts_weights: Arc<DashMap<String, u32>>,
        mut shutdown: Shutdown,
        timeout: Duration,
    ) -> Result<()> {
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

        let response = dfdaemon_upload_client
            .sync_host(SyncHostRequest { host_id, peer_id })
            .await
            .inspect_err(|err| {
                error!("sync host info from parent {} failed: {}", parent.id, err);
            })?;

        let hosts_info = hosts_info.clone();
        let out_stream = response.into_inner().timeout(timeout);
        tokio::pin!(out_stream);

        loop {
            tokio::select! {
                result = out_stream.next() => {
                    match result {
                        Some(Ok(message_result)) => {
                            match message_result {
                                Ok(message) => {
                                    // Update the parent's host info if exists.
                                    hosts_info.insert(parent.id.clone(), message.clone());

                                    if let Ok(rate) = Self::available_rate(&message) {
                                        let weight = (rate / 1024.0).max(1.0) as u32;

                                        hosts_weights.insert(parent.id.clone(), weight);
                                    }
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
                    break;
                }
            }
        }

        info!("parent selector connection to parent {} has been closed", parent.id);

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
            .map(|parent| self.hosts_weights.get(&parent.id).map(|w| *w).unwrap_or(1))
            .collect();

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

    /// register_parent increments the reference count for the given parent.
    pub async fn register_parent(&self, parent_id: &str) -> Result<()> {
        if let Some(parent_sync) = self.hosts.get(parent_id) {
            parent_sync.increment_ref().await;
            Ok(())
        } else {
            Err(Error::Unknown(format!("parent {} not found", parent_id)))
        }
    }

    /// unregister_parent decrements the reference count for the given parent.
    pub async fn unregister_parent(&self, parent_id: &str) -> Result<()> {
        if let Some(parent_sync) = self.hosts.get(parent_id) {
            let count = parent_sync.decrement_ref().await;

            if count == 0 {
                if parent_sync
                    .cleanup_in_progress
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    // Double-check the ref_count after acquiring the cleanup flag
                    if parent_sync.ref_count.load(Ordering::SeqCst) == 0 {
                        if let Some((_, parent_sync)) = self.hosts.remove(parent_id) {
                            parent_sync.shutdown.trigger();
                            self.hosts_info.remove(parent_id);
                            self.hosts_weights.remove(parent_id);
                            
                            // Decrement the active connection counter
                            self.active_connections.fetch_sub(1, Ordering::SeqCst);
                            
                            info!(
                                "cleaned up inactive parent {}, active connections: {}/{}",
                                parent_id,
                                self.active_connections.load(Ordering::SeqCst),
                                self.config.download.parent_selector.capacity
                            );
                        }
                    } else {
                        parent_sync
                            .cleanup_in_progress
                            .store(false, Ordering::SeqCst);
                    }
                } else {
                    debug!("parent {} cleanup already in progress", parent_id);
                }
            }

            Ok(())
        } else {
            Err(Error::Unknown(format!("parent {} not found", parent_id)))
        }
    }

    pub async fn unregister_all_parents(&self, parents: Vec<Peer>) -> Result<()> {
        for parent in parents {
            self.unregister_parent(&parent.id).await?;
        }
        Ok(())
    }

    /// get_connection_count returns the current number of active connections.
    pub fn get_connection_count(&self) -> usize {
        self.active_connections.load(Ordering::SeqCst)
    }

    /// get_capacity returns the maximum capacity of connections.
    pub fn get_capacity(&self) -> usize {
        self.config.download.parent_selector.capacity
    }

    /// is_full returns true if the selector is at maximum capacity.
    pub fn is_full(&self) -> bool {
        self.active_connections.load(Ordering::SeqCst) >= self.get_capacity()
    }
}
