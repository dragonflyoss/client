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

use crate::grpc::manager::ManagerClient;
use block_list::BlockList;
use dragonfly_api::manager::v2::{ListSchedulersResponse, Scheduler};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Result;
use dragonfly_client_util::shutdown;
use local::Local;
use regex::Regex;
use remote::Remote;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, instrument};

pub mod block_list;
pub mod local;
pub mod remote;

/// Block list configuration for scheduler cluster clients.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterClientConfig {
    /// Block list rules applied to client requests.
    #[serde(alias = "blockList")]
    pub block_list: Option<SchedulerClusterConfigBlockList>,
}

/// Block list configuration for scheduler cluster seed clients.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterSeedClientConfig {
    /// Block list rules applied to seed client requests.
    #[serde(alias = "blockList")]
    pub block_list: Option<SchedulerClusterConfigBlockList>,
}

/// Categorized block lists by task type.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterConfigBlockList {
    /// Block list for regular tasks.
    pub task: Option<SchedulerClusterConfigTaskBlockList>,

    /// Block list for persistent tasks.
    #[serde(alias = "persistentTask")]
    pub persistent_task: Option<SchedulerClusterConfigPersistentTaskBlockList>,

    /// Block list for persistent cache tasks.
    #[serde(alias = "persistentCacheTask")]
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
    /// All known schedulers returned by the backend.
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

/// Backend of the dynamic configuration. When the manager is configured, the
/// dynamic configuration is fetched from the manager, otherwise it is loaded
/// from the local dynconfig file.
enum Backend {
    /// Fetches the dynamic configuration from the manager.
    Remote(Remote),

    /// Loads the dynamic configuration from the local dynconfig file.
    Local(Local),
}

/// Manages dynamic configuration for the dfdaemon, periodically
/// refreshing state from the configured backend.
pub struct Dynconfig {
    /// Current dynamic configuration, protected by a read-write lock.
    pub data: Arc<RwLock<Data>>,

    /// Block list to check whether tasks are blocked, backed by the dynamic configuration data.
    pub block_list: Arc<BlockList>,

    /// Static dfdaemon configuration.
    config: Arc<Config>,

    /// Backend of the dynamic configuration.
    backend: Backend,

    /// Mutex guarding concurrent refresh operations.
    mutex: Mutex<()>,

    /// Handle to signal graceful shutdown.
    shutdown: shutdown::Shutdown,

    /// Sender held to detect when all shutdown work is complete.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// The implementation of Dynconfig.
impl Dynconfig {
    /// Creates a new Dynconfig instance.
    ///
    /// The backend is selected based on the configuration: if the manager
    /// address is configured, the dynamic configuration is fetched from the
    /// manager; otherwise, it is loaded from the local dynconfig file.
    pub async fn new(
        config: Arc<Config>,
        dynconfig_path: PathBuf,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Result<Self> {
        // Create a new Dynconfig.
        let data = Arc::new(RwLock::new(Data::default()));
        let dc = Dynconfig {
            block_list: Arc::new(BlockList::new(config.clone(), data.clone())),
            data,
            config: config.clone(),
            backend: Self::backend(config, dynconfig_path.clone()).await?,
            mutex: Mutex::new(()),
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        };

        // Initialize the dynamic configuration.
        dc.refresh().await?;
        Ok(dc)
    }

    /// Creates a new backend for the dynamic configuration based on the provided configuration.
    async fn backend(config: Arc<Config>, dynconfig_path: PathBuf) -> Result<Backend> {
        match config.manager {
            Some(ref manager) => {
                let manager_client = ManagerClient::new(config.clone(), manager.addr.clone())
                    .await
                    .inspect_err(|err| {
                        error!("initialize manager client failed: {}", err);
                    })?;

                info!(
                    "refresh dynamic configuration from manager {}",
                    manager.addr
                );
                Ok(Backend::Remote(Remote::new(
                    config.clone(),
                    Arc::new(manager_client),
                )))
            }
            None => {
                info!(
                    "manager is not configured, load dynamic configuration from {}",
                    dynconfig_path.display()
                );

                let local = Local::new(config.clone(), dynconfig_path);
                local.generate_default().await?;
                Ok(Backend::Local(local))
            }
        }
    }

    /// Run starts the dynconfig server.
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Start the refresh loop.
        let mut interval = tokio::time::interval(self.refresh_interval().await);

        // Start the refresh loop. The refresh interval is re-evaluated on each
        // iteration, since the local backend can change it on refresh.
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

        let data = match &self.backend {
            Backend::Remote(remote) => remote.refresh().await?,
            Backend::Local(local) => local.refresh().await?,
        };

        *self.data.write().await = data;
        Ok(())
    }

    /// Returns the interval to refresh the dynamic configuration.
    async fn refresh_interval(&self) -> Duration {
        match &self.backend {
            Backend::Remote(_) => self.config.dynconfig.refresh_interval,
            Backend::Local(local) => local.refresh_interval().await,
        }
    }
}
