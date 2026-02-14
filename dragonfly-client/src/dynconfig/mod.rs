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

use crate::grpc::health::HealthClient;
use crate::grpc::manager::ManagerClient;
use dragonfly_api::manager::v2::{
    ListSchedulersRequest, ListSchedulersResponse, Scheduler, SourceType,
};
use dragonfly_client_config::{dfdaemon::Config, CARGO_PKG_VERSION, GIT_COMMIT_SHORT_HASH};
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::net::format_url;
use dragonfly_client_util::shutdown;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic_health::pb::health_check_response::ServingStatus;
use tracing::{debug, error, info, instrument};
use url::Url;

/// Block list configuration for scheduler cluster clients.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterClientConfig {
    /// Block list rules applied to client requests.
    pub block_list: Option<SchedulerClusterConfigBlockList>,
}

/// Block list configuration for scheduler cluster seed clients.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterSeedClientConfig {
    /// Block list rules applied to seed client requests.
    pub block_list: Option<SchedulerClusterConfigBlockList>,
}

/// Categorized block lists by task type.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterConfigBlockList {
    /// Block list for regular tasks.
    pub task: Option<SchedulerClusterConfigTaskBlockList>,

    /// Block list for persistent tasks.
    pub persistent_task: Option<SchedulerClusterConfigPersistentTaskBlockList>,

    /// Block list for persistent cache tasks.
    pub persistent_cache_task: Option<SchedulerClusterConfigPersistentCacheTaskBlockList>,
}

/// Block list scoped to regular task operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterConfigTaskBlockList {
    /// Rules applied to download gRPC requests.
    pub download: Option<SchedulerClusterConfigDownloadBlockList>,
}

/// Block list scoped to persistent task operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterConfigPersistentTaskBlockList {
    /// Rules applied to download gRPC requests.
    pub download: Option<SchedulerClusterConfigDownloadBlockList>,

    /// Rules applied to upload gRPC requests.
    pub upload: Option<SchedulerClusterConfigUploadBlockList>,
}

/// Block list scoped to persistent cache task operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterConfigPersistentCacheTaskBlockList {
    /// Rules applied to download gRPC requests.
    pub download: Option<SchedulerClusterConfigDownloadBlockList>,

    /// Rules applied to upload gRPC requests.
    pub upload: Option<SchedulerClusterConfigUploadBlockList>,
}

/// Block list criteria for download gRPC requests.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterConfigDownloadBlockList {
    /// Blocked application names.
    pub applications: Option<Vec<String>>,

    /// Blocked URL regex patterns.
    #[serde(with = "serde_regex")]
    pub urls: Vec<Regex>,

    /// Blocked tags.
    pub tags: Option<Vec<String>>,

    /// Blocked priorities.
    pub priorities: Option<Vec<i32>>,
}

/// Block list criteria for upload gRPC requests.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterConfigUploadBlockList {
    /// Blocked application names.
    pub applications: Option<Vec<String>>,

    /// Blocked URL regex patterns.
    #[serde(with = "serde_regex")]
    pub urls: Vec<Regex>,

    /// Blocked tags.
    pub tags: Option<Vec<String>>,
}

/// Dynamic configuration state of the dfdaemon.
#[derive(Default)]
pub struct Data {
    /// All known schedulers returned by the manager.
    pub schedulers: ListSchedulersResponse,

    /// Schedulers currently available for use.
    pub available_schedulers: Vec<Scheduler>,

    /// ID of the currently active scheduler cluster, if any.
    pub available_scheduler_cluster_id: Option<u64>,

    /// Client-specific block list configuration from the scheduler cluster.
    pub client_config: Option<SchedulerClusterClientConfig>,

    /// Seed-client-specific block list configuration from the scheduler cluster.
    pub seed_client_config: Option<SchedulerClusterSeedClientConfig>,
}

/// Manages dynamic configuration for the dfdaemon, periodically
/// refreshing state from the manager service.
pub struct Dynconfig {
    /// Current dynamic configuration, protected by a read-write lock.
    pub data: RwLock<Data>,

    /// Static dfdaemon configuration.
    config: Arc<Config>,

    /// gRPC client used to communicate with the manager.
    manager_client: Arc<ManagerClient>,

    /// Mutex guarding concurrent refresh operations.
    mutex: Mutex<()>,

    /// Handle to signal graceful shutdown.
    shutdown: shutdown::Shutdown,

    /// Sender held to detect when all shutdown work is complete.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// Dynconfig is the implementation of Dynconfig.
impl Dynconfig {
    // Create a new Dynconfig instance.
    pub async fn new(
        config: Arc<Config>,
        manager_client: Arc<ManagerClient>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Result<Self> {
        // Create a new Dynconfig.
        let dc = Dynconfig {
            config,
            data: RwLock::new(Data::default()),
            manager_client,
            mutex: Mutex::new(()),
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        };

        // Initialize the dynamic configuration.
        dc.refresh().await?;
        Ok(dc)
    }

    /// Run starts the dynconfig server.
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Start the refresh loop.
        let mut interval = tokio::time::interval(self.config.dynconfig.refresh_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match self.refresh().await {
                        Err(err) => error!("refresh dynconfig failed: {}", err),
                        Ok(_) => debug!("refresh dynconfig success"),
                    }
                }
                _ = shutdown.recv() => {
                    // Dynconfig server shutting down with signals.
                    info!("dynconfig server shutting down");
                    return
                }
            }
        }
    }

    /// Refresh refreshes the dynamic configuration of the dfdaemon.
    #[instrument(skip_all)]
    pub async fn refresh(&self) -> Result<()> {
        // Only one refresh can be running at a time.
        let Ok(_guard) = self.mutex.try_lock() else {
            debug!("refresh is already running");
            return Ok(());
        };

        let schedulers = self.list_schedulers().await?;
        let available_schedulers = self
            .get_available_schedulers(&schedulers.schedulers)
            .await?;

        let Some(available_scheduler) = available_schedulers.first() else {
            return Err(Error::AvailableSchedulersNotFound);
        };

        let scheduler_cluster_id = available_scheduler.scheduler_cluster_id;
        let Some(scheduler_cluster) = &available_scheduler.scheduler_cluster else {
            return Err(Error::AvailableSchedulersNotFound);
        };

        // Deserialize the client configs from the scheduler cluster.
        let client_config = match serde_json::from_slice::<SchedulerClusterClientConfig>(
            &scheduler_cluster.client_config,
        ) {
            Ok(config) => Some(config),
            Err(err) => {
                error!("failed to deserialize client config: {}", err);
                None
            }
        };

        // Deserialize the seed client configs from the scheduler cluster.
        let seed_client_config = match serde_json::from_slice::<SchedulerClusterSeedClientConfig>(
            &scheduler_cluster.seed_client_config,
        ) {
            Ok(config) => Some(config),
            Err(err) => {
                error!("failed to deserialize seed client config: {}", err);
                None
            }
        };

        let mut data = self.data.write().await;
        data.schedulers = schedulers;
        data.available_schedulers = available_schedulers;
        data.available_scheduler_cluster_id = Some(scheduler_cluster_id);
        data.client_config = client_config;
        data.seed_client_config = seed_client_config;
        Ok(())
    }

    /// List schedulers from the manager service based on the source type (peer or seed peer).
    #[instrument(skip_all)]
    async fn list_schedulers(&self) -> Result<ListSchedulersResponse> {
        // Get the source type.
        let source_type = if self.config.seed_peer.enable {
            SourceType::SeedPeerSource.into()
        } else {
            SourceType::PeerSource.into()
        };

        // Get the schedulers from the manager.
        self.manager_client
            .list_schedulers(ListSchedulersRequest {
                source_type,
                hostname: self.config.host.hostname.clone(),
                ip: self.config.host.ip.unwrap().to_string(),
                idc: self.config.host.idc.clone(),
                location: self.config.host.location.clone(),
                version: CARGO_PKG_VERSION.to_string(),
                commit: GIT_COMMIT_SHORT_HASH.to_string(),
                scheduler_cluster_id: self.config.host.scheduler_cluster_id.unwrap_or(0),
            })
            .await
    }

    /// Gets the available schedulers by checking the health of each scheduler and filtering out
    /// the unhealthy ones. If scheduler_cluster_id is specified, only returns the schedulers of
    /// the specified scheduler cluster.
    #[instrument(skip_all)]
    async fn get_available_schedulers(&self, schedulers: &[Scheduler]) -> Result<Vec<Scheduler>> {
        let mut available_schedulers: Vec<Scheduler> = Vec::new();
        let mut available_scheduler_cluster_id: Option<u64> = None;
        for scheduler in schedulers {
            // If scheduler_cluster_id is specified, only return the schedulers
            // of the specified scheduler cluster.
            if let Some(scheduler_cluster_id) = available_scheduler_cluster_id {
                if scheduler.scheduler_cluster_id != scheduler_cluster_id {
                    continue;
                }
            }

            let addr = format_url(
                "http",
                IpAddr::from_str(&scheduler.ip)?,
                scheduler.port as u16,
            );
            let domain_name = Url::parse(addr.as_str())?
                .host_str()
                .ok_or(Error::InvalidParameter)
                .inspect_err(|_err| {
                    error!("invalid address: {}", addr);
                })?
                .to_string();

            // Check the health of the scheduler.
            let health_client = match HealthClient::new(
                &addr,
                self.config
                    .scheduler
                    .load_client_tls_config(domain_name.as_str())
                    .await?,
            )
            .await
            {
                Ok(client) => client,
                Err(err) => {
                    error!(
                        "create health client for scheduler {}:{} failed: {}",
                        scheduler.ip, scheduler.port, err
                    );
                    continue;
                }
            };

            match health_client.check().await {
                Ok(resp) => {
                    if resp.status == ServingStatus::Serving as i32 {
                        available_schedulers.push(scheduler.clone());
                        available_scheduler_cluster_id = Some(scheduler.scheduler_cluster_id);
                    }
                }
                Err(err) => {
                    error!("check scheduler health failed: {}", err);
                    continue;
                }
            }
        }

        Ok(available_schedulers)
    }
}
