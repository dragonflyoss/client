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

use local_ip_address::{local_ip, local_ipv6};
use serde::Deserialize;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;
use std::time::Duration;
use std::{fmt, fs};
use tracing::info;
use validator::Validate;

// NAME is the name of dfdaemon.
pub const NAME: &str = "dfdaemon";

// DEFAULT_GRPC_SERVER_PORT is the default port of the grpc server.
const DEFAULT_GRPC_SERVER_PORT: u16 = 65000;

// DEFAULT_PROXY_SERVER_PORT is the default port of the proxy server.
// const DEFAULT_PROXY_SERVER_PORT: u16 = 65001;

// DEFAULT_OBJECT_STORAGE_SERVER_PORT is the default port of the object storage server.
const DEFAULT_OBJECT_STORAGE_SERVER_PORT: u16 = 65002;

// DEFAULT_METRICS_SERVER_PORT is the default port of the metrics server.
const DEFAULT_METRICS_SERVER_PORT: u16 = 8000;

// DEFAULT_HEALTH_SERVER_PORT is the default port of the health server.
const DEFAULT_HEALTH_SERVER_PORT: u16 = 40901;

// default_dfdaemon_config_path is the default config path for dfdaemon.
pub fn default_dfdaemon_config_path() -> PathBuf {
    super::default_config_dir().join("dfdaemon.yaml")
}

// default_dfdaemon_log_dir is the default log directory for dfdaemon.
pub fn default_dfdaemon_log_dir() -> PathBuf {
    super::default_log_dir().join(NAME)
}

// default_dfdaemon_plugin_dir is the default plugin directory for dfdaemon.
pub fn default_dfdaemon_plugin_dir() -> PathBuf {
    super::default_plugin_dir().join(NAME)
}

// default_dfdaemon_cache_dir is the default cache directory for dfdaemon.
pub fn default_dfdaemon_cache_dir() -> PathBuf {
    super::default_cache_dir().join(NAME)
}

// default_dfdaemon_unix_socket_path is the default unix socket path for dfdaemon GRPC service.
pub fn default_dfdaemon_unix_socket_path() -> PathBuf {
    super::default_root_dir().join("dfdaemon.sock")
}

// default_dfdaemon_lock_path is the default file lock path for dfdaemon service.
pub fn default_dfdaemon_lock_path() -> PathBuf {
    super::default_lock_dir().join("dfdaemon.lock")
}

// default_scheduler_announce_interval is the default interval to announce peer to the scheduler.
pub fn default_scheduler_announce_interval() -> Duration {
    Duration::from_secs(30)
}

// default_scheduler_schedule_timeout is the default timeout for scheduling.
pub fn default_scheduler_schedule_timeout() -> Duration {
    Duration::from_secs(300)
}

// default_dynconfig_refresh_interval is the default interval to
// refresh dynamic configuration from manager.
pub fn default_dynconfig_refresh_interval() -> Duration {
    Duration::from_secs(600)
}

// default_seed_peer_keepalive_interval is the default interval to keepalive with manager.
pub fn default_seed_peer_keepalive_interval() -> Duration {
    Duration::from_secs(15)
}

// Error is the error for Config.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // IO is the error for IO operation.
    #[error(transparent)]
    IO(#[from] std::io::Error),

    // YAML is the error for serde_yaml.
    #[error(transparent)]
    YAML(#[from] serde_yaml::Error),
}

// Result is the result for Config.
pub type Result<T> = std::result::Result<T, Error>;

// Host is the host configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Host {
    // idc is the idc of the host.
    pub idc: Option<String>,

    // location is the location of the host.
    pub location: Option<String>,

    // hostname is the hostname of the host.
    pub hostname: String,

    // ip is the advertise ip of the host.
    pub ip: Option<IpAddr>,
}

// Host implements default value for Host.
impl Default for Host {
    fn default() -> Self {
        Self {
            idc: None,
            location: None,
            hostname: hostname::get().unwrap().to_string_lossy().to_string(),
            ip: None,
        }
    }
}

// Server is the server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Server {
    // data_dir is the directory to store task's metadata and content.
    pub data_dir: PathBuf,

    // plugin_dir is the directory to store plugins.
    pub plugin_dir: PathBuf,

    // cache_dir is the directory to store cache files.
    pub cache_dir: PathBuf,

    // root_dir is the root directory for dfdaemon.
    pub root_dir: PathBuf,

    // lock_path is the file lock path for dfdaemon service.
    pub lock_dir: PathBuf,

    // ip is the listen ip of the grpc server.
    pub ip: Option<IpAddr>,

    // port is the port to the grpc server.
    pub port: u16,
}

// Server implements default value for Server.
impl Default for Server {
    fn default() -> Self {
        Self {
            data_dir: super::default_data_dir(),
            plugin_dir: default_dfdaemon_plugin_dir(),
            cache_dir: default_dfdaemon_cache_dir(),
            root_dir: super::default_root_dir(),
            lock_dir: super::default_lock_dir(),
            ip: None,
            port: DEFAULT_GRPC_SERVER_PORT,
        }
    }
}

// Manager is the manager configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Manager {
    // addr is manager address.
    pub addr: Option<String>,
}

// Scheduler is the scheduler configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Scheduler {
    // announce_interval is the interval to announce peer to the scheduler.
    // Announcer will provide the scheduler with peer information for scheduling,
    // peer information includes cpu, memory, etc.
    pub announce_interval: Duration,

    // schedule_timeout is the timeout for scheduling. If the scheduling timesout, dfdaemon will back-to-source
    // download if enable_back_to_source is true, otherwise dfdaemon will return download failed.
    pub schedule_timeout: Duration,

    // enable_back_to_source indicates whether enable back-to-source download, when the scheduling failed.
    pub enable_back_to_source: bool,
}

// Scheduler implements default value for Scheduler.
impl Default for Scheduler {
    fn default() -> Self {
        Self {
            announce_interval: default_scheduler_announce_interval(),
            schedule_timeout: default_scheduler_schedule_timeout(),
            enable_back_to_source: true,
        }
    }
}

// HostType is the type of the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum HostType {
    // Normal indicates the peer is normal peer.
    #[serde(rename = "normal")]
    Normal,

    // Super indicates the peer is super seed peer.
    #[serde(rename = "super")]
    Super,

    // Strong indicates the peer is strong seed peer.
    #[serde(rename = "strong")]
    Strong,

    // Weak indicates the peer is weak seed peer.
    #[serde(rename = "weak")]
    Weak,
}

// HostType implements Display.
impl fmt::Display for HostType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HostType::Normal => write!(f, "normal"),
            HostType::Super => write!(f, "super"),
            HostType::Strong => write!(f, "strong"),
            HostType::Weak => write!(f, "weak"),
        }
    }
}

// SeedPeer is the seed peer configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SeedPeer {
    // enable indicates whether enable seed peer.
    pub enable: bool,

    // kind is the type of seed peer.
    #[serde(rename = "type")]
    pub kind: HostType,

    // cluster_id is the cluster id of the seed peer cluster.
    pub cluster_id: u64,

    // keepalive_interval is the interval to keep alive with manager.
    pub keepalive_interval: Duration,
}

// SeedPeer implements default value for SeedPeer.
impl Default for SeedPeer {
    fn default() -> Self {
        Self {
            enable: false,
            kind: HostType::Super,
            cluster_id: 1,
            keepalive_interval: default_seed_peer_keepalive_interval(),
        }
    }
}

// Dynconfig is the dynconfig configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Dynconfig {
    // refresh_interval is the interval to refresh dynamic configuration from manager.
    pub refresh_interval: Duration,
}

// Dynconfig implements default value for Dynconfig.
impl Default for Dynconfig {
    fn default() -> Self {
        Self {
            refresh_interval: default_dynconfig_refresh_interval(),
        }
    }
}

// Storage is the storage configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Storage {}

// Proxy is the proxy configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Proxy {
    // enable indicates whether enable proxy.
    pub enable: bool,
}

// Security is the security configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Security {
    // enable indicates whether enable security.
    pub enable: bool,
}

// ObjectStorage is the object storage configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ObjectStorage {
    // enable indicates whether enable object storage.
    pub enable: bool,

    // ip is the listen ip of the object storage server.
    pub ip: Option<IpAddr>,

    // port is the port to the object storage server.
    pub port: u16,
}

// ObjectStorage implements default value for ObjectStorage.
impl Default for ObjectStorage {
    fn default() -> Self {
        Self {
            enable: false,
            ip: None,
            port: DEFAULT_OBJECT_STORAGE_SERVER_PORT,
        }
    }
}

// Network is the network configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Network {
    // enable_ipv6 indicates whether enable ipv6.
    pub enable_ipv6: bool,
}

// Metrics is the metrics configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Metrics {
    // ip is the listen ip of the metrics server.
    pub ip: Option<IpAddr>,

    // port is the port to the metrics server.
    pub port: u16,
}

// Metrics implements default value for Metrics.
impl Default for Metrics {
    fn default() -> Self {
        Self {
            ip: None,
            port: DEFAULT_METRICS_SERVER_PORT,
        }
    }
}

// Tracing is the tracing configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Tracing {
    // addr is the address to report tracing log.
    pub addr: Option<String>,
}

// Tracing is the tracing configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Health {
    // ip is the listen ip of the health server.
    pub ip: Option<IpAddr>,

    // port is the port to the health server.
    pub port: u16,
}

// Tracing implements default value for Tracing.
impl Default for Health {
    fn default() -> Self {
        Self {
            ip: None,
            port: DEFAULT_HEALTH_SERVER_PORT,
        }
    }
}

// Config is the configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
    // host is the host configuration for dfdaemon.
    pub host: Host,

    // server is the server configuration for dfdaemon.
    pub server: Server,

    // manager is the manager configuration for dfdaemon.
    pub manager: Manager,

    // scheduler is the scheduler configuration for dfdaemon.
    pub scheduler: Scheduler,

    // seed_peer is the seed peer configuration for dfdaemon.
    pub seed_peer: SeedPeer,

    // dynconfig is the dynconfig configuration for dfdaemon.
    pub dynconfig: Dynconfig,

    // storage is the storage configuration for dfdaemon.
    pub storage: Storage,

    // proxy is the proxy configuration for dfdaemon.
    pub proxy: Proxy,

    // security is the security configuration for dfdaemon.
    pub security: Security,

    // object_storage is the object storage configuration for dfdaemon.
    pub object_storage: ObjectStorage,

    // metrics is the metrics configuration for dfdaemon.
    pub metrics: Metrics,

    // tracing is the tracing configuration for dfdaemon.
    pub tracing: Tracing,

    // health is the health configuration for dfdaemon.
    pub health: Health,

    // network is the network configuration for dfdaemon.
    pub network: Network,
}

// Config implements the config operation of dfdaemon.
impl Config {
    // load loads configuration from file.
    pub fn load(path: &PathBuf) -> Result<Config> {
        if path.exists() {
            let content = fs::read_to_string(path)?;
            let mut config: Config = serde_yaml::from_str(&content)?;
            info!("load config from {}", path.display());

            // Convert configuration.
            config.convert();
            Ok(config)
        } else {
            let mut config = Self::default();
            info!(
                "config file {} not found, use default config",
                path.display()
            );

            // Convert configuration.
            config.convert();
            Ok(config)
        }
    }

    // convert converts the configuration.
    fn convert(&mut self) {
        // Convert advertise ip.
        if self.host.ip.is_none() {
            self.host.ip = if self.network.enable_ipv6 {
                Some(local_ipv6().unwrap())
            } else {
                Some(local_ip().unwrap())
            }
        }

        // Convert grpc server listen ip.
        if self.server.ip.is_none() {
            self.server.ip = if self.network.enable_ipv6 {
                Some(Ipv6Addr::UNSPECIFIED.into())
            } else {
                Some(Ipv4Addr::UNSPECIFIED.into())
            }
        }

        // Convert object storage server listen ip.
        if self.object_storage.ip.is_none() {
            self.object_storage.ip = if self.network.enable_ipv6 {
                Some(Ipv6Addr::UNSPECIFIED.into())
            } else {
                Some(Ipv4Addr::UNSPECIFIED.into())
            }
        }

        // Convert metrics server listen ip.
        if self.metrics.ip.is_none() {
            self.metrics.ip = if self.network.enable_ipv6 {
                Some(Ipv6Addr::UNSPECIFIED.into())
            } else {
                Some(Ipv4Addr::UNSPECIFIED.into())
            }
        }

        // Convert health server listen ip.
        if self.health.ip.is_none() {
            self.health.ip = if self.network.enable_ipv6 {
                Some(Ipv6Addr::UNSPECIFIED.into())
            } else {
                Some(Ipv4Addr::UNSPECIFIED.into())
            }
        }
    }
}
