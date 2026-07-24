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
use dragonfly_api::manager::v2::{ListSchedulersResponse, Scheduler};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs;
use tokio::net::lookup_host;
use tokio::sync::RwLock;
use tracing::{error, info, instrument};

use dragonfly_client_config::dfdaemon::{
    default_local_dynconfig_refresh_interval, default_local_dynconfig_scheduler_addr,
};

/// The scheduler configuration for the local dynamic configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SchedulerConfig {
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

/// Implement Default for SchedulerConfig.
impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            addr: default_local_dynconfig_scheduler_addr(),
            addrs: None,
        }
    }
}

/// The local dynamic configuration for dfdaemon, loaded from the dynconfig
/// file (typically mounted as a Kubernetes ConfigMap).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct LocalConfig {
    /// The interval to refresh the local dynamic configuration.
    #[serde(
        default = "default_local_dynconfig_refresh_interval",
        with = "humantime_serde"
    )]
    pub refresh_interval: Duration,

    /// The scheduler configuration for scheduler discovery.
    pub scheduler: SchedulerConfig,

    /// Client-specific block list configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_config: Option<SchedulerClusterClientConfig>,

    /// Seed-client-specific block list configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed_client_config: Option<SchedulerClusterSeedClientConfig>,
}

/// Implement Default for LocalConfig.
impl Default for LocalConfig {
    fn default() -> Self {
        LocalConfig {
            refresh_interval: default_local_dynconfig_refresh_interval(),
            scheduler: SchedulerConfig::default(),
            client_config: None,
            seed_client_config: None,
        }
    }
}

/// Local backend of the dynamic configuration, loading it from the local
/// dynconfig file and discovering schedulers via DNS.
pub struct Local {
    /// Path of the local dynconfig file.
    path: PathBuf,

    /// The interval to refresh the local dynamic configuration, updated from
    /// the file on each refresh.
    refresh_interval: RwLock<Duration>,
}

/// The implementation of Local.
impl Local {
    /// Creates a new local backend.
    pub fn new(path: PathBuf) -> Self {
        Self {
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
            return Ok(());
        }

        let config = LocalConfig::default();
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
        let config: LocalConfig = serde_yaml::from_str(&content).or_err(ErrorType::ConfigError)?;

        // Update the refresh interval from the file.
        *self.refresh_interval.write().await = config.refresh_interval;

        // Discover the available schedulers from the static address list or by DNS.
        let available_schedulers = match config.scheduler.addrs.as_deref() {
            Some(addrs) if !addrs.is_empty() => Self::parse_schedulers(addrs)?,
            _ => self.resolve_schedulers(&config.scheduler.addr).await?,
        };
        Ok(Data {
            schedulers: ListSchedulersResponse {
                schedulers: available_schedulers.clone(),
            },
            available_schedulers,
            available_scheduler_cluster_id: None,
            client_config: config.client_config,
            seed_client_config: config.seed_client_config,
        })
    }

    /// Resolves the scheduler address to the list of schedulers via DNS. The
    /// resolved addresses are sorted to keep the scheduler selection stable
    /// across refreshes.
    #[instrument(skip_all)]
    async fn resolve_schedulers(&self, addr: &str) -> Result<Vec<Scheduler>> {
        if addr.is_empty() {
            error!("scheduler addr is not specified in dynconfig");
            return Err(Error::InvalidParameter);
        }

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
            .map(|socket_addr| Scheduler {
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
    fn parse_schedulers(addrs: &[String]) -> Result<Vec<Scheduler>> {
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
            .map(|socket_addr| Scheduler {
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

        let config: LocalConfig = serde_yaml::from_str(yaml).unwrap();
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

        let config: LocalConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            config.refresh_interval,
            default_local_dynconfig_refresh_interval()
        );
        assert!(config.client_config.is_none());
        assert!(config.seed_client_config.is_none());
    }

    #[tokio::test]
    async fn refresh_should_resolve_schedulers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");
        tokio::fs::write(
            &path,
            r#"
refreshInterval: 10s
scheduler:
  addr: 'localhost:8002'
"#,
        )
        .await
        .unwrap();

        let local = Local::new(path);
        let data = local.refresh().await.unwrap();
        assert!(!data.available_schedulers.is_empty());
        assert!(data
            .available_schedulers
            .iter()
            .all(|scheduler| scheduler.port == 8002));
        assert!(data.available_scheduler_cluster_id.is_none());
        assert_eq!(local.refresh_interval().await, Duration::from_secs(10));
    }

    #[tokio::test]
    async fn generate_default_should_create_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");

        let local = Local::new(path.clone());
        local.generate_default().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let config: LocalConfig = serde_yaml::from_str(&content).unwrap();
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

        let local = Local::new(path.clone());
        local.generate_default().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(content, existing);
    }

    #[tokio::test]
    async fn refresh_should_use_static_scheduler_addrs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");
        tokio::fs::write(
            &path,
            r#"
scheduler:
  addrs:
    - '192.168.1.11:8002'
    - '192.168.1.10:8002'
    - '192.168.1.10:8002'
"#,
        )
        .await
        .unwrap();

        let local = Local::new(path);
        let data = local.refresh().await.unwrap();
        let addrs: Vec<String> = data
            .available_schedulers
            .iter()
            .map(|scheduler| format!("{}:{}", scheduler.ip, scheduler.port))
            .collect();
        assert_eq!(addrs, vec!["192.168.1.10:8002", "192.168.1.11:8002"]);
    }

    #[tokio::test]
    async fn refresh_should_prefer_static_scheduler_addrs_over_addr() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");
        tokio::fs::write(
            &path,
            r#"
scheduler:
  addr: 'localhost:8002'
  addrs:
    - '192.168.1.10:8003'
"#,
        )
        .await
        .unwrap();

        let local = Local::new(path);
        let data = local.refresh().await.unwrap();
        assert_eq!(data.available_schedulers.len(), 1);
        assert_eq!(data.available_schedulers[0].ip, "192.168.1.10");
        assert_eq!(data.available_schedulers[0].port, 8003);
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

        let local = Local::new(path);
        assert!(local.refresh().await.is_err());
    }

    #[tokio::test]
    async fn refresh_should_fail_when_file_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let local = Local::new(dir.path().join("dynconfig.yaml"));
        assert!(local.refresh().await.is_err());
    }

    #[tokio::test]
    async fn refresh_should_fail_when_scheduler_addr_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dynconfig.yaml");
        tokio::fs::write(&path, "scheduler:\n  addr: ''")
            .await
            .unwrap();

        let local = Local::new(path);
        assert!(local.refresh().await.is_err());
    }
}
