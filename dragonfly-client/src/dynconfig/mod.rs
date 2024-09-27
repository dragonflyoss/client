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
use crate::shutdown;
use dragonfly_api::manager::v2::{
    ListSchedulersRequest, ListSchedulersResponse, Scheduler, SourceType,
};
use dragonfly_client_config::{dfdaemon::Config, CARGO_PKG_VERSION, GIT_HASH};
use dragonfly_client_core::{Error, Result};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic_health::pb::health_check_response::ServingStatus;
use tracing::{error, info, instrument};

/// Data is the dynamic configuration of the dfdaemon.
#[derive(Default)]
pub struct Data {
    /// schedulers is the schedulers of the dfdaemon.
    pub schedulers: ListSchedulersResponse,

    /// available_schedulers is the available schedulers of the dfdaemon.
    pub available_schedulers: Vec<Scheduler>,

    /// available_scheduler_cluster_id is the id of the available scheduler cluster of the dfdaemon.
    pub available_scheduler_cluster_id: Option<u64>,
}

/// Dynconfig supports dynamic configuration of the client.
pub struct Dynconfig {
    /// data is the dynamic configuration of the dfdaemon.
    pub data: RwLock<Data>,

    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// manager_client is the grpc client of the manager.
    manager_client: Arc<ManagerClient>,

    /// mutex is used to protect refresh.
    mutex: Mutex<()>,

    /// shutdown is used to shutdown the dynconfig.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the dynconfig is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// Dynconfig is the implementation of Dynconfig.
impl Dynconfig {
    /// new creates a new Dynconfig.
    #[instrument(skip_all)]
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

    /// run starts the dynconfig server.
    #[instrument(skip_all)]
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Start the refresh loop.
        let mut interval = tokio::time::interval(self.config.dynconfig.refresh_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(err) = self.refresh().await {
                        error!("refresh dynconfig failed: {}", err);
                    };
                }
                _ = shutdown.recv() => {
                    // Dynconfig server shutting down with signals.
                    info!("dynconfig server shutting down");
                    return
                }
            }
        }
    }

    /// get the config.
    pub async fn get_config(&self) -> Arc<Config> {
        self.config.clone()
    }

    /// refresh refreshes the dynamic configuration of the dfdaemon.
    pub async fn refresh(&self) -> Result<()> {
        // Only one refresh can be running at a time.
        let Ok(_guard) = self.mutex.try_lock() else {
            info!("refresh is already running");
            return Ok(());
        };

        // refresh the schedulers.
        let schedulers = self.list_schedulers().await?;

        // Get the available schedulers.
        let available_schedulers = self
            .get_available_schedulers(&schedulers.schedulers)
            .await?;

        // If no available schedulers, return error.
        if available_schedulers.is_empty() {
            return Err(Error::AvailableSchedulersNotFound);
        }

        // Get the data with write lock.
        let mut data = self.data.write().await;
        data.schedulers = schedulers;
        data.available_schedulers = available_schedulers;
        if let Some(available_scheduler) = data.available_schedulers.first() {
            data.available_scheduler_cluster_id = Some(available_scheduler.scheduler_cluster_id);
        }
        Ok(())
    }

    /// list_schedulers lists the schedulers from the manager.
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
                commit: GIT_HASH.unwrap_or_default().to_string(),
            })
            .await
    }

    /// get_available_schedulers gets the available schedulers.
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

            // Check the health of the scheduler.
            let health_client =
                match HealthClient::new(&format!("http://{}:{}", scheduler.ip, scheduler.port))
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
