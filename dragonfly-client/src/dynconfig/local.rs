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
use dragonfly_api::manager::v2::{ListSchedulersResponse, Scheduler as ManagerScheduler};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use dragonfly_client_util::net::format_url;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::net::lookup_host;
use tokio::sync::RwLock;
use tonic_health::pb::health_check_response::ServingStatus;
use tracing::{error, info, instrument};
use url::Url;

use dragonfly_client_config::dfdaemon::{
    default_local_dynconfig_refresh_interval, default_local_dynconfig_scheduler_addr,
    Config as DfdaemonConfig,
};

/// The scheduler configuration for the local dynamic configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Scheduler {
    /// The address of the scheduler headless service with port (e.g.
    /// `scheduler-headless.default.svc:8002`), resolved via DNS to the list
    /// of scheduler IPs.
    #[serde(default = "default_local_dynconfig_scheduler_addr")]
    pub addr: String,

    /// The static list of scheduler addresses with port (e.g.
    /// `192.168.1.10:8002`). When set, it takes precedence over `addr`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub addrs: Option<Vec<String>>,
}

/// Implement Default for Scheduler.
impl Default for Scheduler {
    fn default() -> Self {
        Scheduler {
            addr: default_local_dynconfig_scheduler_addr(),
            addrs: None,
        }
    }
}

/// The local dynamic configuration for dfdaemon, loaded from the dynconfig
/// file (typically mounted as a Kubernetes ConfigMap).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
    /// The interval to refresh the local dynamic configuration.
    #[serde(
        default = "default_local_dynconfig_refresh_interval",
        with = "humantime_serde"
    )]
    pub refresh_interval: Duration,

    /// The scheduler configuration for scheduler discovery.
    pub scheduler: Scheduler,

    /// Block list configuration for clients running as normal peers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_config: Option<SchedulerClusterClientConfig>,

    /// Block list configuration for clients running as seed peers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed_client_config: Option<SchedulerClusterSeedClientConfig>,
}

/// Implement Default for Config.
impl Default for Config {
    fn default() -> Self {
        Config {
            refresh_interval: default_local_dynconfig_refresh_interval(),
            scheduler: Scheduler::default(),
            client_config: None,
            seed_client_config: None,
        }
    }
}

/// Local backend of the dynamic configuration, loading it from the local
/// dynconfig file and discovering schedulers via DNS.
pub struct Local {
    /// Static dfdaemon configuration.
    config: Arc<DfdaemonConfig>,

    /// Path of the local dynconfig file.
    path: PathBuf,

    /// The interval to refresh the local dynamic configuration, updated from
    /// the file on each refresh.
    refresh_interval: RwLock<Duration>,
}

/// The implementation of Local.
impl Local {
    /// Creates a new local backend.
    pub fn new(config: Arc<DfdaemonConfig>, path: PathBuf) -> Self {
        Self {
            config,
            path,
            refresh_interval: RwLock::new(default_local_dynconfig_refresh_interval()),
        }
    }

    /// Returns the interval to refresh the local dynamic configuration.
    pub async fn refresh_interval(&self) -> Duration {
        *self.refresh_interval.read().await
    }

    /// Generates the default dynconfig file if it does not exist.
    #[instrument(skip_all)]
    pub async fn generate_default(&self) -> Result<()> {
        if fs::try_exists(&self.path).await? {
            info!(
                "dynconfig {} already exists, skipping generation",
                self.path.display()
            );

            return Ok(());
        }

        let config = Config::default();
        let content = serde_yaml::to_string(&config).or_err(ErrorType::SerializeError)?;
        fs::write(&self.path, content).await.inspect_err(|err| {
            error!("write dynconfig {} failed: {}", self.path.display(), err);
        })?;

        info!("generated default dynconfig {}", self.path.display());
        Ok(())
    }

    /// Refreshes the dynamic configuration from the local dynconfig file.
    #[instrument(skip_all)]
    pub async fn refresh(&self) -> Result<Data> {
        // Load the local dynconfig file.
        let content = fs::read_to_string(&self.path).await.inspect_err(|err| {
            error!("read dynconfig {} failed: {}", self.path.display(), err);
        })?;
        let config: Config = serde_yaml::from_str(&content).or_err(ErrorType::ConfigError)?;

        // Update the refresh interval from the file.
        *self.refresh_interval.write().await = config.refresh_interval;

        // Discover the schedulers from the static address list or by DNS.
        let schedulers = match config.scheduler.addrs.as_deref() {
            Some(addrs) if !addrs.is_empty() => Self::parse_schedulers(addrs)?,
            _ => {
                if config.scheduler.addr.is_empty() {
                    error!("scheduler addr is not specified in dynconfig");
                    return Err(Error::InvalidParameter);
                } else {
                    self.resolve_schedulers(&config.scheduler.addr).await?
                }
            }
        };

        // Filter out the unhealthy schedulers by health check.
        let available_schedulers = self.get_available_schedulers(&schedulers).await?;
        if available_schedulers.is_empty() {
            return Err(Error::AvailableSchedulersNotFound);
        }

        Ok(Data {
            schedulers: ListSchedulersResponse { schedulers },
            available_schedulers,
            available_scheduler_cluster_id: None,
            client_config: config.client_config,
            seed_client_config: config.seed_client_config,
        })
    }

    /// Gets the available schedulers by checking the health of each scheduler
    /// and filtering out the unhealthy ones.
    #[instrument(skip_all)]
    async fn get_available_schedulers(
        &self,
        schedulers: &[ManagerScheduler],
    ) -> Result<Vec<ManagerScheduler>> {
        let mut available_schedulers: Vec<ManagerScheduler> = Vec::new();
        for scheduler in schedulers {
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

    /// Resolves the scheduler address to the list of schedulers via DNS. The
    /// resolved addresses are sorted to keep the scheduler selection stable
    /// across refreshes.
    #[instrument(skip_all)]
    async fn resolve_schedulers(&self, addr: &str) -> Result<Vec<ManagerScheduler>> {
        let mut socket_addrs: Vec<SocketAddr> = lookup_host(addr)
            .await
            .inspect_err(|err| {
                error!("resolve scheduler address {} failed: {}", addr, err);
            })?
            .collect();
        if socket_addrs.is_empty() {
            return Err(Error::AvailableSchedulersNotFound);
        }

        socket_addrs.sort();
        socket_addrs.dedup();
        Ok(socket_addrs
            .into_iter()
            .map(|socket_addr| ManagerScheduler {
                ip: socket_addr.ip().to_string(),
                port: socket_addr.port() as i32,
                ..Default::default()
            })
            .collect())
    }

    /// Parses the static scheduler addresses (ip:port) to the list of
    /// schedulers. The addresses are sorted to keep the scheduler selection
    /// stable across refreshes.
    #[instrument(skip_all)]
    fn parse_schedulers(addrs: &[String]) -> Result<Vec<ManagerScheduler>> {
        let mut socket_addrs = Vec::with_capacity(addrs.len());
        for addr in addrs {
            let socket_addr = addr.parse::<SocketAddr>().inspect_err(|err| {
                error!("parse scheduler address {} failed: {}", addr, err);
            })?;

            socket_addrs.push(socket_addr);
        }

        socket_addrs.sort();
        socket_addrs.dedup();
        Ok(socket_addrs
            .into_iter()
            .map(|socket_addr| ManagerScheduler {
                ip: socket_addr.ip().to_string(),
                port: socket_addr.port() as i32,
                ..Default::default()
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::wrappers::TcpListenerStream;

    /// Creates a new local backend with the default dfdaemon configuration.
    fn new_local(path: PathBuf) -> Local {
        Local::new(Arc::new(DfdaemonConfig::default()), path)
    }

    /// Spawns a grpc health server with serving status on a random local port.
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

    #[test]
    fn deserialize_local_config_correctly() {
        let yaml = r#"
refreshInterval: 10s
scheduler:
  addr: 'scheduler-headless.default.svc:8002'
clientConfig:
  blockList:
    task:
      download:
        applications: ['blocked-app']
        urls: []
        tags: []
        priorities: []
    persistentTask:
      upload:
        applications: []
        urls: []
        tags: []
      download:
        applications: []
        urls: []
        tags: []
        priorities: []
    persistentCacheTask:
      upload:
        applications: []
        urls: []
        tags: []
      download:
        applications: []
        urls: []
        tags: []
        priorities: []
seedClientConfig:
  blockList:
    task:
      download:
        applications: []
        urls: []
        tags: []
        priorities: []
"#;

        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.refresh_interval, Duration::from_secs(10));
        assert_eq!(config.scheduler.addr, "scheduler-headless.default.svc:8002");

        let block_list = config.client_config.unwrap().block_list.unwrap();
        assert_eq!(
            block_list
                .task
                .unwrap()
                .download
                .unwrap()
                .applications
                .unwrap(),
            vec!["blocked-app".to_string()]
        );
        assert!(block_list.persistent_task.is_some());
        assert!(block_list.persistent_cache_task.is_some());
        assert!(config.seed_client_config.unwrap().block_list.is_some());
    }

    #[test]
    fn deserialize_local_config_with_defaults() {
        let yaml = r#"
scheduler:
  addr: 'scheduler-headless.default.svc:8002'
"#;

        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            config.refresh_interval,
            default_local_dynconfig_refresh_interval()
        );
        assert!(config.client_config.is_none());
        assert!(config.seed_client_config.is_none());
    }

    #[tokio::test]
    async fn refresh_should_resolve_schedulers() {
        let health_addr = spawn_health_server().await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");
        tokio::fs::write(
            &path,
            format!(
                "refreshInterval: 10s\nscheduler:\n  addr: 'localhost:{}'\n",
                health_addr.port()
            ),
        )
        .await
        .unwrap();

        let local = new_local(path);
        let data = local.refresh().await.unwrap();
        assert!(!data.available_schedulers.is_empty());
        assert!(data
            .available_schedulers
            .iter()
            .all(|scheduler| scheduler.port == health_addr.port() as i32));
        assert!(data.available_scheduler_cluster_id.is_none());
        assert_eq!(local.refresh_interval().await, Duration::from_secs(10));
    }

    #[tokio::test]
    async fn generate_default_should_create_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");

        let local = new_local(path.clone());
        local.generate_default().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let config: Config = serde_yaml::from_str(&content).unwrap();
        assert_eq!(
            config.scheduler.addr,
            default_local_dynconfig_scheduler_addr()
        );
        assert_eq!(
            config.refresh_interval,
            default_local_dynconfig_refresh_interval()
        );
    }

    #[tokio::test]
    async fn generate_default_should_keep_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");
        let existing = "scheduler:\n  addr: 'scheduler-headless.default.svc:8002'\n";
        tokio::fs::write(&path, existing).await.unwrap();

        let local = new_local(path.clone());
        local.generate_default().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(content, existing);
    }

    #[tokio::test]
    async fn refresh_should_use_static_scheduler_addrs() {
        let health_addr_a = spawn_health_server().await;
        let health_addr_b = spawn_health_server().await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");
        tokio::fs::write(
            &path,
            format!(
                "scheduler:\n  addrs:\n    - '{}'\n    - '{}'\n    - '{}'\n",
                health_addr_b, health_addr_a, health_addr_a
            ),
        )
        .await
        .unwrap();

        let local = new_local(path);
        let data = local.refresh().await.unwrap();
        let mut expected_ports = vec![health_addr_a.port() as i32, health_addr_b.port() as i32];
        expected_ports.sort();

        let ports: Vec<i32> = data
            .available_schedulers
            .iter()
            .map(|scheduler| scheduler.port)
            .collect();
        assert_eq!(ports, expected_ports);
        assert_eq!(data.schedulers.schedulers.len(), 2);
    }

    #[tokio::test]
    async fn refresh_should_filter_unhealthy_schedulers() {
        let health_addr = spawn_health_server().await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");
        tokio::fs::write(
            &path,
            format!(
                "scheduler:\n  addrs:\n    - '{}'\n    - '127.0.0.1:1'\n",
                health_addr
            ),
        )
        .await
        .unwrap();

        let local = new_local(path);
        let data = local.refresh().await.unwrap();
        assert_eq!(data.schedulers.schedulers.len(), 2);
        assert_eq!(data.available_schedulers.len(), 1);
        assert_eq!(data.available_schedulers[0].port, health_addr.port() as i32);
    }

    #[tokio::test]
    async fn refresh_should_fail_when_schedulers_unhealthy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");
        tokio::fs::write(&path, "scheduler:\n  addrs:\n    - '127.0.0.1:1'\n")
            .await
            .unwrap();

        let local = new_local(path);
        assert!(local.refresh().await.is_err());
    }

    #[tokio::test]
    async fn refresh_should_prefer_static_scheduler_addrs_over_addr() {
        let health_addr = spawn_health_server().await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");
        tokio::fs::write(
            &path,
            format!(
                "scheduler:\n  addr: 'localhost:1'\n  addrs:\n    - '{}'\n",
                health_addr
            ),
        )
        .await
        .unwrap();

        let local = new_local(path);
        let data = local.refresh().await.unwrap();
        assert_eq!(data.available_schedulers.len(), 1);
        assert_eq!(data.available_schedulers[0].ip, "127.0.0.1");
        assert_eq!(data.available_schedulers[0].port, health_addr.port() as i32);
    }

    #[tokio::test]
    async fn refresh_should_fail_when_static_scheduler_addr_is_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");
        tokio::fs::write(
            &path,
            r#"
scheduler:
  addrs:
    - '192.168.1.10'
"#,
        )
        .await
        .unwrap();

        let local = new_local(path);
        assert!(local.refresh().await.is_err());
    }

    #[tokio::test]
    async fn refresh_should_fail_when_file_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let local = new_local(dir.path().join("dynconfig.yaml"));
        assert!(local.refresh().await.is_err());
    }

    #[tokio::test]
    async fn refresh_should_fail_when_scheduler_addr_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");
        tokio::fs::write(&path, "scheduler:\n  addr: ''")
            .await
            .unwrap();

        let local = new_local(path);
        assert!(local.refresh().await.is_err());
    }
}
