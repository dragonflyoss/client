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
use tokio::task::JoinSet;
use tonic_health::pb::health_check_response::ServingStatus;
use tracing::{error, instrument, Instrument};
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
        let available_schedulers =
            Self::get_available_schedulers(&self.config, &schedulers.schedulers).await;

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
    /// Checks the health of the schedulers concurrently and returns the serving
    /// ones in the order of the manager, limited to the scheduler cluster of the
    /// first serving scheduler.
    #[instrument(skip_all)]
    async fn get_available_schedulers(
        config: &Arc<Config>,
        schedulers: &[Scheduler],
    ) -> Vec<Scheduler> {
        let mut join_set = JoinSet::new();
        for (i, scheduler) in schedulers.iter().enumerate() {
            let config = config.clone();
            let scheduler = scheduler.clone();
            join_set.spawn(
                async move {
                    (
                        i,
                        Self::check_health(&config, &scheduler)
                            .await
                            .then_some(scheduler),
                    )
                }
                .in_current_span(),
            );
        }

        let mut healthy = vec![None; schedulers.len()];
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((i, scheduler)) => healthy[i] = scheduler,
                Err(err) => error!("check scheduler health failed: {}", err),
            }
        }

        let mut available_schedulers = Vec::new();
        let mut available_scheduler_cluster_id = None;
        for scheduler in healthy.into_iter().flatten() {
            if available_scheduler_cluster_id.is_some_and(|id| scheduler.scheduler_cluster_id != id)
            {
                continue;
            }

            available_scheduler_cluster_id = Some(scheduler.scheduler_cluster_id);
            available_schedulers.push(scheduler);
        }

        available_schedulers
    }

    /// Returns whether the scheduler is serving, a scheduler that cannot be
    /// addressed or checked is logged and treated as not serving.
    async fn check_health(config: &Config, scheduler: &Scheduler) -> bool {
        let ip = match IpAddr::from_str(&scheduler.ip) {
            Ok(ip) => ip,
            Err(err) => {
                error!(
                    "invalid ip {} of scheduler {}: {}",
                    scheduler.ip, scheduler.hostname, err
                );
                return false;
            }
        };

        let addr = format_url(config.scheduler.scheme(), ip, scheduler.port as u16);
        let Some(domain_name) = Url::parse(&addr)
            .ok()
            .and_then(|url| url.host_str().map(str::to_string))
        else {
            error!("invalid address: {}", addr);
            return false;
        };

        let client_tls_config = match config.scheduler.load_client_tls_config(&domain_name).await {
            Ok(client_tls_config) => client_tls_config,
            Err(err) => {
                error!(
                    "load client tls config for scheduler {} failed: {}",
                    addr, err
                );
                return false;
            }
        };

        let health_client = match HealthClient::new(&addr, client_tls_config).await {
            Ok(health_client) => health_client,
            Err(err) => {
                error!(
                    "create health client for scheduler {} failed: {}",
                    addr, err
                );
                return false;
            }
        };

        match health_client.check().await {
            Ok(resp) if resp.status == ServingStatus::Serving as i32 => true,
            Ok(resp) => {
                error!("scheduler {} is not serving: {}", addr, resp.status);
                false
            }
            Err(err) => {
                error!("check scheduler {} health failed: {}", addr, err);
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio_stream::wrappers::TcpListenerStream;

    async fn spawn_health_server() -> SocketAddr {
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_service_status("", tonic_health::ServingStatus::Serving)
            .await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(health_service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        addr
    }

    fn scheduler(hostname: &str, ip: &str, port: u16, scheduler_cluster_id: u64) -> Scheduler {
        Scheduler {
            hostname: hostname.to_string(),
            ip: ip.to_string(),
            port: port as i32,
            scheduler_cluster_id,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn get_available_schedulers_keeps_the_cluster_of_the_first_serving_scheduler() {
        let config = Arc::new(Config::default());
        let serving_a = spawn_health_server().await;
        let serving_b = spawn_health_server().await;
        let serving_c = spawn_health_server().await;

        let test_cases = vec![
            (
                vec![
                    scheduler("scheduler-a", "127.0.0.1", serving_a.port(), 1),
                    scheduler("scheduler-b", "127.0.0.1", serving_b.port(), 2),
                    scheduler("scheduler-c", "127.0.0.1", serving_c.port(), 1),
                ],
                vec!["scheduler-a", "scheduler-c"],
            ),
            (
                vec![
                    scheduler("scheduler-dead", "127.0.0.1", 1, 1),
                    scheduler("scheduler-b", "127.0.0.1", serving_b.port(), 2),
                    scheduler("scheduler-a", "127.0.0.1", serving_a.port(), 1),
                ],
                vec!["scheduler-b"],
            ),
            (
                vec![
                    scheduler("scheduler-invalid", "invalid", serving_a.port(), 1),
                    scheduler("scheduler-a", "127.0.0.1", serving_a.port(), 1),
                ],
                vec!["scheduler-a"],
            ),
            (vec![scheduler("scheduler-dead", "127.0.0.1", 1, 1)], vec![]),
            (vec![], vec![]),
        ];

        for (schedulers, expected) in test_cases {
            let available_schedulers = Remote::get_available_schedulers(&config, &schedulers).await;
            let hostnames: Vec<&str> = available_schedulers
                .iter()
                .map(|scheduler| scheduler.hostname.as_str())
                .collect();
            assert_eq!(hostnames, expected);
        }
    }
}
