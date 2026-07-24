/*
 *     Copyright 2026 The Dragonfly Authors
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

use super::{Data, SchedulerClusterClientConfig, SchedulerClusterSeedClientConfig};
use crate::grpc::health::HealthClient;
use crate::grpc::manager::ManagerClient;
use dragonfly_api::manager::v2::{
    ListSchedulersRequest, ListSchedulersResponse, Scheduler, SourceType,
};
use dragonfly_client_config::{dfdaemon::Config, CARGO_PKG_VERSION, GIT_COMMIT_SHORT_HASH};
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::net::format_url;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use tonic_health::pb::health_check_response::ServingStatus;
use tracing::{error, instrument};
use url::Url;

/// Remote backend of the dynamic configuration, fetching it from the manager.
pub struct Remote {
    /// Static dfdaemon configuration.
    config: Arc<Config>,

    /// gRPC client used to communicate with the manager.
    manager_client: Arc<ManagerClient>,
}

/// The implementation of Remote.
impl Remote {
    /// Creates a new remote backend.
    pub fn new(config: Arc<Config>, manager_client: Arc<ManagerClient>) -> Self {
        Self {
            config,
            manager_client,
        }
    }

    /// Refreshes the dynamic configuration from the manager.
    #[instrument(skip_all)]
    pub async fn refresh(&self) -> Result<Data> {
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

        Ok(Data {
            schedulers,
            available_schedulers,
            available_scheduler_cluster_id: Some(scheduler_cluster_id),
            client_config,
            seed_client_config,
        })
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
