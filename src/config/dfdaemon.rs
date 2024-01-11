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
use crate::Result;
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

// default_dfdaemon_config_path is the default config path for dfdaemon.
#[inline]
pub fn default_dfdaemon_config_path() -> PathBuf {
    super::default_config_dir().join("dfdaemon.yaml")
}

// default_dfdaemon_log_dir is the default log directory for dfdaemon.
#[inline]
pub fn default_dfdaemon_log_dir() -> PathBuf {
    super::default_log_dir().join(NAME)
}

// default_download_unix_socket_path is the default unix socket path for download GRPC service.
pub fn default_download_unix_socket_path() -> PathBuf {
    super::default_root_dir().join("dfdaemon.sock")
}

// default_dfdaemon_lock_path is the default file lock path for dfdaemon service.
pub fn default_dfdaemon_lock_path() -> PathBuf {
    super::default_lock_dir().join("dfdaemon.lock")
}

// default_host_hostname is the default hostname of the host.
#[inline]
fn default_host_hostname() -> String {
    hostname::get().unwrap().to_string_lossy().to_string()
}

// default_dfdaemon_plugin_dir is the default plugin directory for dfdaemon.
#[inline]
fn default_dfdaemon_plugin_dir() -> PathBuf {
    super::default_plugin_dir().join(NAME)
}

// default_dfdaemon_cache_dir is the default cache directory for dfdaemon.
#[inline]
fn default_dfdaemon_cache_dir() -> PathBuf {
    super::default_cache_dir().join(NAME)
}

// default_upload_grpc_server_port is the default port of the upload grpc server.
#[inline]
fn default_upload_grpc_server_port() -> u16 {
    4000
}

// default_upload_rate_limit is the default rate limit of the upload speed in bps(bytes per second).
#[inline]
fn default_upload_rate_limit() -> u64 {
    // Default rate limit is 10Gbps.
    10_000_000_000
}

// default_metrics_server_port is the default port of the metrics server.
#[inline]
fn default_metrics_server_port() -> u16 {
    4001
}

// default_download_rate_limit is the default rate limit of the download speed in bps(bytes per second).
#[inline]
fn default_download_rate_limit() -> u64 {
    // Default rate limit is 10Gbps.
    10_000_000_000
}

// default_download_piece_timeout is the default timeout for downloading a piece from source.
#[inline]
fn default_download_piece_timeout() -> Duration {
    Duration::from_secs(30)
}

// default_download_concurrent_piece_count is the default number of concurrent pieces to download.
#[inline]
fn default_download_concurrent_piece_count() -> u32 {
    10
}

// default_download_max_schedule_count is the default max count of schedule.
#[inline]
fn default_download_max_schedule_count() -> u32 {
    5
}

// default_scheduler_announce_interval is the default interval to announce peer to the scheduler.
#[inline]
fn default_scheduler_announce_interval() -> Duration {
    Duration::from_secs(300)
}

// default_scheduler_schedule_timeout is the default timeout for scheduling.
#[inline]
fn default_scheduler_schedule_timeout() -> Duration {
    Duration::from_secs(30)
}

// default_scheduler_enable_back_to_source indicates whether enable back-to-source download, when the scheduling failed.
#[inline]
fn default_scheduler_enable_back_to_source() -> bool {
    true
}

// default_dynconfig_refresh_interval is the default interval to refresh dynamic configuration from manager.
#[inline]
fn default_dynconfig_refresh_interval() -> Duration {
    Duration::from_secs(300)
}

// default_seed_peer_cluster_id is the default cluster id of seed peer.
#[inline]
fn default_seed_peer_cluster_id() -> u64 {
    1
}

// default_seed_peer_keepalive_interval is the default interval to keepalive with manager.
#[inline]
fn default_seed_peer_keepalive_interval() -> Duration {
    Duration::from_secs(15)
}

// default_gc_interval is the default interval to do gc.
#[inline]
fn default_gc_interval() -> Duration {
    Duration::from_secs(900)
}

// default_gc_policy_task_ttl is the default ttl of the task.
#[inline]
fn default_gc_policy_task_ttl() -> Duration {
    Duration::from_secs(21_600)
}

// default_gc_policy_dist_high_threshold_percent is the default high threshold percent of the disk usage.
#[inline]
fn default_gc_policy_dist_high_threshold_percent() -> u8 {
    80
}

// default_gc_policy_dist_low_threshold_percent is the default low threshold percent of the disk usage.
#[inline]
fn default_gc_policy_dist_low_threshold_percent() -> u8 {
    60
}

// Host is the host configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Host {
    // idc is the idc of the host.
    pub idc: Option<String>,

    // location is the location of the host.
    pub location: Option<String>,

    // hostname is the hostname of the host.
    #[serde(default = "default_host_hostname")]
    pub hostname: String,

    // ip is the advertise ip of the host.
    pub ip: Option<IpAddr>,
}

// Host implements Default.
impl Default for Host {
    fn default() -> Self {
        Host {
            idc: None,
            location: None,
            hostname: default_host_hostname(),
            ip: None,
        }
    }
}

// Server is the server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Server {
    // plugin_dir is the directory to store plugins.
    #[serde(default = "default_dfdaemon_plugin_dir")]
    pub plugin_dir: PathBuf,

    // cache_dir is the directory to store cache files.
    #[serde(default = "default_dfdaemon_cache_dir")]
    pub cache_dir: PathBuf,
}

// Server implements Default.
impl Default for Server {
    fn default() -> Self {
        Server {
            plugin_dir: default_dfdaemon_plugin_dir(),
            cache_dir: default_dfdaemon_cache_dir(),
        }
    }
}

// DownloadServer is the download server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct DownloadServer {
    // socket_path is the unix socket path for dfdaemon GRPC service.
    #[serde(default = "default_download_unix_socket_path")]
    pub socket_path: PathBuf,
}

// DownloadServer implements Default.
impl Default for DownloadServer {
    fn default() -> Self {
        DownloadServer {
            socket_path: default_download_unix_socket_path(),
        }
    }
}

// Download is the download configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Download {
    // server is the download server configuration for dfdaemon.
    pub server: DownloadServer,

    // rate_limit is the rate limit of the download speed in bps(bytes per second).
    #[serde(default = "default_download_rate_limit")]
    pub rate_limit: u64,

    // piece_timeout is the timeout for downloading a piece from source.
    #[serde(default = "default_download_piece_timeout", with = "humantime_serde")]
    pub piece_timeout: Duration,

    // concurrent_piece_count is the number of concurrent pieces to download.
    #[serde(default = "default_download_concurrent_piece_count")]
    #[validate(range(min = 1))]
    pub concurrent_piece_count: u32,
}

// Download implements Default.
impl Default for Download {
    fn default() -> Self {
        Download {
            server: DownloadServer::default(),
            rate_limit: default_download_rate_limit(),
            piece_timeout: default_download_piece_timeout(),
            concurrent_piece_count: default_download_concurrent_piece_count(),
        }
    }
}

// UploadServer is the upload server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct UploadServer {
    // ip is the listen ip of the grpc server.
    pub ip: Option<IpAddr>,

    // port is the port to the grpc server.
    #[serde(default = "default_upload_grpc_server_port")]
    pub port: u16,
}

// UploadServer implements Default.
impl Default for UploadServer {
    fn default() -> Self {
        UploadServer {
            ip: None,
            port: default_upload_grpc_server_port(),
        }
    }
}

// Upload is the upload configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Upload {
    // server is the upload server configuration for dfdaemon.
    pub server: UploadServer,

    // rate_limit is the rate limit of the upload speed in bps(bytes per second).
    #[serde(default = "default_upload_rate_limit")]
    pub rate_limit: u64,
}

// Upload implements Default.
impl Default for Upload {
    fn default() -> Self {
        Upload {
            server: UploadServer::default(),
            rate_limit: default_upload_rate_limit(),
        }
    }
}

// Manager is the manager configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Manager {
    // addrs is manager addresses.
    #[validate(length(min = 1))]
    pub addrs: Vec<String>,
}

// Scheduler is the scheduler configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Scheduler {
    // announce_interval is the interval to announce peer to the scheduler.
    // Announcer will provide the scheduler with peer information for scheduling,
    // peer information includes cpu, memory, etc.
    #[serde(
        default = "default_scheduler_announce_interval",
        with = "humantime_serde"
    )]
    pub announce_interval: Duration,

    // schedule_timeout is the timeout for scheduling. If the scheduling timesout, dfdaemon will back-to-source
    // download if enable_back_to_source is true, otherwise dfdaemon will return download failed.
    #[serde(
        default = "default_scheduler_schedule_timeout",
        with = "humantime_serde"
    )]
    pub schedule_timeout: Duration,

    // max_schedule_count is the max count of schedule.
    #[serde(default = "default_download_max_schedule_count")]
    #[validate(range(min = 1))]
    pub max_schedule_count: u32,

    // enable_back_to_source indicates whether enable back-to-source download, when the scheduling failed.
    #[serde(default = "default_scheduler_enable_back_to_source")]
    pub enable_back_to_source: bool,
}

// Scheduler implements Default.
impl Default for Scheduler {
    fn default() -> Self {
        Scheduler {
            announce_interval: default_scheduler_announce_interval(),
            schedule_timeout: default_scheduler_schedule_timeout(),
            max_schedule_count: default_download_max_schedule_count(),
            enable_back_to_source: default_scheduler_enable_back_to_source(),
        }
    }
}

// HostType is the type of the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
pub enum HostType {
    // Normal indicates the peer is normal peer.
    #[serde(rename = "normal")]
    Normal,

    // Super indicates the peer is super seed peer.
    #[default]
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
    #[serde(default, rename = "type")]
    pub kind: HostType,

    // cluster_id is the cluster id of the seed peer cluster.
    #[serde(default = "default_seed_peer_cluster_id", rename = "clusterID")]
    #[validate(range(min = 1))]
    pub cluster_id: u64,

    // keepalive_interval is the interval to keep alive with manager.
    #[serde(
        default = "default_seed_peer_keepalive_interval",
        with = "humantime_serde"
    )]
    pub keepalive_interval: Duration,
}

// SeedPeer implements Default.
impl Default for SeedPeer {
    fn default() -> Self {
        SeedPeer {
            enable: false,
            kind: HostType::Normal,
            cluster_id: default_seed_peer_cluster_id(),
            keepalive_interval: default_seed_peer_keepalive_interval(),
        }
    }
}

// Dynconfig is the dynconfig configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Dynconfig {
    // refresh_interval is the interval to refresh dynamic configuration from manager.
    #[serde(
        default = "default_dynconfig_refresh_interval",
        with = "humantime_serde"
    )]
    pub refresh_interval: Duration,
}

// Dynconfig implements Default.
impl Default for Dynconfig {
    fn default() -> Self {
        Dynconfig {
            refresh_interval: default_dynconfig_refresh_interval(),
        }
    }
}

// Storage is the storage configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Storage {
    // dir is the directory to store task's metadata and content.
    #[serde(default = "super::default_storage_dir")]
    pub dir: PathBuf,
}

// Storage implements Default.
impl Default for Storage {
    fn default() -> Self {
        Storage {
            dir: super::default_storage_dir(),
        }
    }
}

// Policy is the policy configuration for gc.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Policy {
    // task_ttl is the ttl of the task.
    #[serde(
        default = "default_gc_policy_task_ttl",
        rename = "taskTTL",
        with = "humantime_serde"
    )]
    pub task_ttl: Duration,

    // dist_high_threshold_percent is the high threshold percent of the disk usage.
    // If the disk usage is greater than the threshold, dfdaemon will do gc.
    #[serde(default = "default_gc_policy_dist_high_threshold_percent")]
    #[validate(range(min = 1, max = 99))]
    pub dist_high_threshold_percent: u8,

    // dist_low_threshold_percent is the low threshold percent of the disk usage.
    // If the disk usage is less than the threshold, dfdaemon will stop gc.
    #[serde(default = "default_gc_policy_dist_low_threshold_percent")]
    #[validate(range(min = 1, max = 99))]
    pub dist_low_threshold_percent: u8,
}

// Policy implements Default.
impl Default for Policy {
    fn default() -> Self {
        Policy {
            task_ttl: default_gc_policy_task_ttl(),
            dist_high_threshold_percent: default_gc_policy_dist_high_threshold_percent(),
            dist_low_threshold_percent: default_gc_policy_dist_low_threshold_percent(),
        }
    }
}

// GC is the gc configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct GC {
    // interval is the interval to do gc.
    #[serde(default = "default_gc_interval", with = "humantime_serde")]
    pub interval: Duration,

    // policy is the gc policy.
    pub policy: Policy,
}

// GC implements Default.
impl Default for GC {
    fn default() -> Self {
        GC {
            interval: default_gc_interval(),
            policy: Policy::default(),
        }
    }
}

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
    #[serde(default = "default_metrics_server_port")]
    pub port: u16,
}

// Metrics implements Default.
impl Default for Metrics {
    fn default() -> Self {
        Metrics {
            ip: None,
            port: default_metrics_server_port(),
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

// Config is the configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
    // host is the host configuration for dfdaemon.
    #[validate]
    pub host: Host,

    // server is the server configuration for dfdaemon.
    #[validate]
    pub server: Server,

    // download is the download configuration for dfdaemon.
    #[validate]
    pub download: Download,

    // upload is the upload configuration for dfdaemon.
    #[validate]
    pub upload: Upload,

    // manager is the manager configuration for dfdaemon.
    #[validate]
    pub manager: Manager,

    // scheduler is the scheduler configuration for dfdaemon.
    #[validate]
    pub scheduler: Scheduler,

    // seed_peer is the seed peer configuration for dfdaemon.
    #[validate]
    pub seed_peer: SeedPeer,

    // dynconfig is the dynconfig configuration for dfdaemon.
    #[validate]
    pub dynconfig: Dynconfig,

    // storage is the storage configuration for dfdaemon.
    #[validate]
    pub storage: Storage,

    // gc is the gc configuration for dfdaemon.
    #[validate]
    pub gc: GC,

    // proxy is the proxy configuration for dfdaemon.
    #[validate]
    pub proxy: Proxy,

    // security is the security configuration for dfdaemon.
    #[validate]
    pub security: Security,

    // metrics is the metrics configuration for dfdaemon.
    #[validate]
    pub metrics: Metrics,

    // tracing is the tracing configuration for dfdaemon.
    #[validate]
    pub tracing: Tracing,

    // network is the network configuration for dfdaemon.
    #[validate]
    pub network: Network,
}

// Config implements the config operation of dfdaemon.
impl Config {
    // load loads configuration from file.
    pub fn load(path: &PathBuf) -> Result<Config> {
        // Load configuration from file.
        let content = fs::read_to_string(path)?;
        let mut config: Config = serde_yaml::from_str(&content)?;
        info!("load config from {}", path.display());

        // Convert configuration.
        config.convert();

        // Validate configuration.
        config.validate()?;
        Ok(config)
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

        // Convert upload grpc server listen ip.
        if self.upload.server.ip.is_none() {
            self.upload.server.ip = if self.network.enable_ipv6 {
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
    }
}
