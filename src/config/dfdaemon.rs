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

use crate::config::{
    default_cache_dir, default_config_dir, default_data_dir, default_lock_dir, default_log_dir,
    default_plugin_dir, default_root_dir,
};
use local_ip_address::{local_ip, local_ipv6};
use serde::Deserialize;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;
use tracing::info;
use validator::Validate;

// NAME is the name of dfdaemon.
pub const NAME: &str = "dfdaemon";

// default_dfdaemon_config_path is the default config path for dfdaemon.
pub fn default_dfdaemon_config_path() -> PathBuf {
    default_config_dir().join("dfdaemon.yaml")
}

// default_dfdaemon_log_dir is the default log directory for dfdaemon.
pub fn default_dfdaemon_log_dir() -> PathBuf {
    default_log_dir().join(NAME)
}

// default_dfdaemon_plugin_dir is the default plugin directory for dfdaemon.
pub fn default_dfdaemon_plugin_dir() -> PathBuf {
    default_plugin_dir().join(NAME)
}

// default_dfdaemon_cache_dir is the default cache directory for dfdaemon.
pub fn default_dfdaemon_cache_dir() -> PathBuf {
    default_cache_dir().join(NAME)
}

// default_dfdaemon_unix_socket_path is the default unix socket path for dfdaemon GRPC service.
pub fn default_dfdaemon_unix_socket_path() -> PathBuf {
    default_root_dir().join("dfdaemon.sock")
}

// default_dfdaemon_lock_path is the default file lock path for dfdaemon service.
pub fn default_dfdaemon_lock_path() -> PathBuf {
    default_lock_dir().join("dfdaemon.lock")
}

// default_tracing_addr is the default address to report tracing log.
pub fn default_tracing_addr() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 14268))
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
    pub ip: IpAddr,
}

// Host implements default value for Host.
impl Default for Host {
    fn default() -> Self {
        Host {
            idc: None,
            location: None,
            hostname: hostname::get().unwrap().to_string_lossy().to_string(),
            ip: local_ip().unwrap(),
        }
    }
}

// Manager is the manager configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Manager {
    // addrs is manager addresses.
    pub addrs: Vec<SocketAddr>,
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
        Scheduler {
            announce_interval: default_scheduler_announce_interval(),
            schedule_timeout: default_scheduler_schedule_timeout(),
            enable_back_to_source: true,
        }
    }
}

// SeedPeerType is the type of seed peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum SeedPeerType {
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

// SeedPeer is the seed peer configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SeedPeer {
    // enable indicates whether enable seed peer.
    pub enable: bool,

    // kind is the type of seed peer.
    #[serde(rename = "type")]
    pub kind: SeedPeerType,

    // cluster_id is the cluster id of the seed peer cluster.
    pub cluster_id: u32,

    // keepalive_interval is the interval to keep alive with manager.
    pub keepalive_interval: Duration,
}

// SeedPeer implements default value for SeedPeer.
impl Default for SeedPeer {
    fn default() -> Self {
        SeedPeer {
            enable: false,
            kind: SeedPeerType::Super,
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
        Dynconfig {
            refresh_interval: default_dynconfig_refresh_interval(),
        }
    }
}

// Downloader is the downloader configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Downloader {}

// Uploader is the uploader configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Uploader {}

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
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ObjectStorage {
    // enable indicates whether enable object storage.
    pub enable: bool,
}

// Network is the network configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Network {
    // enable_ipv6 indicates whether enable ipv6.
    pub enable_ipv6: bool,
}

// Metrics is the metrics configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Metrics {
    // enable indicates whether enable metrics.
    pub enable: bool,
}

// Tracing is the tracing configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Tracing {
    // enable indicates whether enable tracing.
    pub enable: bool,

    // addr is the address to report tracing log.
    pub addr: SocketAddr,
}

// Tracing implements default value for Tracing.
impl Default for Tracing {
    fn default() -> Self {
        Tracing {
            enable: false,
            addr: default_tracing_addr(),
        }
    }
}

// Config is the configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
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

    // host is the host configuration for dfdaemon.
    pub host: Host,

    // manager is the manager configuration for dfdaemon.
    pub manager: Manager,

    // scheduler is the scheduler configuration for dfdaemon.
    pub scheduler: Scheduler,

    // seed_peer is the seed peer configuration for dfdaemon.
    pub seed_peer: SeedPeer,

    // dynconfig is the dynconfig configuration for dfdaemon.
    pub dynconfig: Dynconfig,

    // downloader is the downloader configuration for dfdaemon.
    pub downloader: Downloader,

    // uploader is the uploader configuration for dfdaemon.
    pub uploader: Uploader,

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

    // network is the network configuration for dfdaemon.
    pub network: Network,
}

// Default implements default value for Config.
impl Default for Config {
    fn default() -> Self {
        Config {
            data_dir: default_data_dir(),
            plugin_dir: default_dfdaemon_plugin_dir(),
            cache_dir: default_dfdaemon_cache_dir(),
            root_dir: default_root_dir(),
            lock_dir: default_lock_dir(),
            host: Host::default(),
            manager: Manager::default(),
            scheduler: Scheduler::default(),
            seed_peer: SeedPeer::default(),
            dynconfig: Dynconfig::default(),
            downloader: Downloader::default(),
            uploader: Uploader::default(),
            storage: Storage::default(),
            proxy: Proxy::default(),
            security: Security::default(),
            object_storage: ObjectStorage::default(),
            network: Network::default(),
            metrics: Metrics::default(),
            tracing: Tracing::default(),
        }
    }
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
            info!(
                "config file {} not found, use default config",
                path.display()
            );
            Ok(Self::default())
        }
    }

    // convert converts the configuration.
    fn convert(&mut self) {
        // Convert IP address.
        if self.network.enable_ipv6 {
            self.host.ip = local_ipv6().unwrap()
        }
    }
}
