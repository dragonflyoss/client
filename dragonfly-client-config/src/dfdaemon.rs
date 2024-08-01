/*
 *     Copyright 2024 The Dragonfly Authors
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

use bytesize::ByteSize;
use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::Result;
use local_ip_address::{local_ip, local_ipv6};
use regex::Regex;
use serde::Deserialize;
use std::collections::HashSet;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs;
use tracing::info;
use validator::Validate;

// NAME is the name of dfdaemon.
pub const NAME: &str = "dfdaemon";

// default_dfdaemon_config_path is the default config path for dfdaemon.
#[inline]
pub fn default_dfdaemon_config_path() -> PathBuf {
    crate::default_config_dir().join("dfdaemon.yaml")
}

// default_dfdaemon_log_dir is the default log directory for dfdaemon.
#[inline]
pub fn default_dfdaemon_log_dir() -> PathBuf {
    crate::default_log_dir().join(NAME)
}

// default_download_unix_socket_path is the default unix socket path for download GRPC service.
pub fn default_download_unix_socket_path() -> PathBuf {
    crate::default_root_dir().join("dfdaemon.sock")
}

// default_host_hostname is the default hostname of the host.
#[inline]
fn default_host_hostname() -> String {
    hostname::get().unwrap().to_string_lossy().to_string()
}

// default_dfdaemon_plugin_dir is the default plugin directory for dfdaemon.
#[inline]
fn default_dfdaemon_plugin_dir() -> PathBuf {
    crate::default_plugin_dir().join(NAME)
}

// default_dfdaemon_cache_dir is the default cache directory for dfdaemon.
#[inline]
fn default_dfdaemon_cache_dir() -> PathBuf {
    crate::default_cache_dir().join(NAME)
}

// default_upload_grpc_server_port is the default port of the upload grpc server.
#[inline]
fn default_upload_grpc_server_port() -> u16 {
    4000
}

// default_upload_rate_limit is the default rate limit of the upload speed in GiB/Mib/Kib per second.
#[inline]
fn default_upload_rate_limit() -> ByteSize {
    // Default rate limit is 10GiB/s.
    ByteSize::gib(10)
}

// default_health_server_port is the default port of the health server.
#[inline]
fn default_health_server_port() -> u16 {
    4003
}

// default_metrics_server_port is the default port of the metrics server.
#[inline]
fn default_metrics_server_port() -> u16 {
    4002
}

// default_stats_server_port is the default port of the stats server.
#[inline]
fn default_stats_server_port() -> u16 {
    4004
}

// default_download_rate_limit is the default rate limit of the download speed in GiB/Mib/Kib per second.
#[inline]
fn default_download_rate_limit() -> ByteSize {
    // Default rate limit is 10GiB/s.
    ByteSize::gib(10)
}

// default_download_piece_timeout is the default timeout for downloading a piece from source.
#[inline]
fn default_download_piece_timeout() -> Duration {
    Duration::from_secs(60)
}

// default_download_concurrent_piece_count is the default number of concurrent pieces to download.
#[inline]
fn default_download_concurrent_piece_count() -> u32 {
    16
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

// default_dynconfig_refresh_interval is the default interval to refresh dynamic configuration from manager.
#[inline]
fn default_dynconfig_refresh_interval() -> Duration {
    Duration::from_secs(300)
}

// default_storage_keep is the default keep of the task's metadata and content when the dfdaemon restarts.
#[inline]
fn default_storage_keep() -> bool {
    false
}

// default_storage_write_buffer_size is the default buffer size for writing piece to disk, default is 128KB.
#[inline]
fn default_storage_write_buffer_size() -> usize {
    128 * 1024
}

// default_storage_read_buffer_size is the default buffer size for reading piece from disk, default is 128KB.
#[inline]
fn default_storage_read_buffer_size() -> usize {
    128 * 1024
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

// default_proxy_server_port is the default port of the proxy server.
#[inline]
pub fn default_proxy_server_port() -> u16 {
    4001
}

// default_proxy_read_buffer_size is the default buffer size for reading piece, default is 32KB.
#[inline]
pub fn default_proxy_read_buffer_size() -> usize {
    32 * 1024
}

// default_s3_filtered_query_params is the default filtered query params with s3 protocol to generate the task id.
#[inline]
fn s3_filtered_query_params() -> Vec<String> {
    vec![
        "X-Amz-Algorithm".to_string(),
        "X-Amz-Credential".to_string(),
        "X-Amz-Date".to_string(),
        "X-Amz-Expires".to_string(),
        "X-Amz-SignedHeaders".to_string(),
        "X-Amz-Signature".to_string(),
        "X-Amz-Security-Token".to_string(),
        "X-Amz-User-Agent".to_string(),
    ]
}

// gcs_filtered_query_params is the filtered query params with gcs protocol to generate the task id.
#[inline]
fn gcs_filtered_query_params() -> Vec<String> {
    vec![
        "X-Goog-Algorithm".to_string(),
        "X-Goog-Credential".to_string(),
        "X-Goog-Date".to_string(),
        "X-Goog-Expires".to_string(),
        "X-Goog-SignedHeaders".to_string(),
        "X-Goog-Signature".to_string(),
    ]
}

// oss_filtered_query_params is the filtered query params with oss protocol to generate the task id.
#[inline]
fn oss_filtered_query_params() -> Vec<String> {
    vec![
        "OSSAccessKeyId".to_string(),
        "Expires".to_string(),
        "Signature".to_string(),
        "SecurityToken".to_string(),
    ]
}

// obs_filtered_query_params is the filtered query params with obs protocol to generate the task id.
#[inline]
fn obs_filtered_query_params() -> Vec<String> {
    vec![
        "AccessKeyId".to_string(),
        "Signature".to_string(),
        "Expires".to_string(),
        "X-Obs-Date".to_string(),
        "X-Obs-Security-Token".to_string(),
    ]
}

// cos_filtered_query_params is the filtered query params with cos protocol to generate the task id.
#[inline]
fn cos_filtered_query_params() -> Vec<String> {
    vec![
        "q-sign-algorithm".to_string(),
        "q-ak".to_string(),
        "q-sign-time".to_string(),
        "q-key-time".to_string(),
        "q-header-list".to_string(),
        "q-url-param-list".to_string(),
        "q-signature".to_string(),
        "x-cos-security-token".to_string(),
    ]
}

// default_proxy_rule_filtered_query_params is the default filtered query params to generate the task id.
#[inline]
fn default_proxy_rule_filtered_query_params() -> Vec<String> {
    let mut visited = HashSet::new();
    for query_param in s3_filtered_query_params() {
        visited.insert(query_param);
    }

    for query_param in gcs_filtered_query_params() {
        visited.insert(query_param);
    }

    for query_param in oss_filtered_query_params() {
        visited.insert(query_param);
    }

    for query_param in obs_filtered_query_params() {
        visited.insert(query_param);
    }

    for query_param in cos_filtered_query_params() {
        visited.insert(query_param);
    }

    visited.into_iter().collect()
}

// default_proxy_registry_mirror_addr is the default registry mirror address.
#[inline]
fn default_proxy_registry_mirror_addr() -> String {
    "https://index.docker.io".to_string()
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

    // rate_limit is the rate limit of the download speed in GiB/Mib/Kib per second.
    #[serde(with = "bytesize_serde", default = "default_download_rate_limit")]
    pub rate_limit: ByteSize,

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

    // rate_limit is the rate limit of the upload speed in GiB/Mib/Kib per second.
    #[serde(with = "bytesize_serde", default = "default_upload_rate_limit")]
    pub rate_limit: ByteSize,
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

    // schedule_timeout is the timeout for scheduling. If the scheduling timeout, dfdaemon will back-to-source
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
}

// Scheduler implements Default.
impl Default for Scheduler {
    fn default() -> Self {
        Scheduler {
            announce_interval: default_scheduler_announce_interval(),
            schedule_timeout: default_scheduler_schedule_timeout(),
            max_schedule_count: default_download_max_schedule_count(),
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
    #[serde(default = "crate::default_storage_dir")]
    pub dir: PathBuf,

    // keep indicates whether keep the task's metadata and content when the dfdaemon restarts.
    #[serde(default = "default_storage_keep")]
    pub keep: bool,

    // write_buffer_size is the buffer size for writing piece to disk, default is 4KB.
    #[serde(default = "default_storage_write_buffer_size")]
    pub write_buffer_size: usize,

    // read_buffer_size is the buffer size for reading piece from disk, default is 4KB.
    #[serde(default = "default_storage_read_buffer_size")]
    pub read_buffer_size: usize,
}

// Storage implements Default.
impl Default for Storage {
    fn default() -> Self {
        Storage {
            dir: crate::default_storage_dir(),
            keep: default_storage_keep(),
            write_buffer_size: default_storage_write_buffer_size(),
            read_buffer_size: default_storage_read_buffer_size(),
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

// ProxyServer is the proxy server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ProxyServer {
    // ip is the listen ip of the proxy server.
    pub ip: Option<IpAddr>,

    // port is the port to the proxy server.
    #[serde(default = "default_proxy_server_port")]
    pub port: u16,

    // ca_cert is the root CA cert path with PEM format for the proxy server to generate the server cert.
    //
    // If ca_cert is empty, proxy will generate a smaple CA cert by rcgen::generate_simple_self_signed.
    // When client requests via the proxy, the client should not verify the server cert and set
    // insecure to true.
    //
    // If ca_cert is not empty, proxy will sign the server cert with the CA cert. If openssl is installed,
    // you can use openssl to generate the root CA cert and make the system trust the root CA cert.
    // Then set the ca_cert and ca_key to the root CA cert and key path. Dfdaemon generates the server cert
    // and key, and signs the server cert with the root CA cert. When client requests via the proxy,
    // the proxy can intercept the request by the server cert.
    pub ca_cert: Option<PathBuf>,

    // ca_key is the root CA key path with PEM format for the proxy server to generate the server cert.
    //
    // If ca_key is empty, proxy will generate a smaple CA key by rcgen::generate_simple_self_signed.
    // When client requests via the proxy, the client should not verify the server cert and set
    // insecure to true.
    //
    // If ca_key is not empty, proxy will sign the server cert with the CA cert. If openssl is installed,
    // you can use openssl to generate the root CA cert and make the system trust the root CA cert.
    // Then set the ca_cert and ca_key to the root CA cert and key path. Dfdaemon generates the server cert
    // and key, and signs the server cert with the root CA cert. When client requests via the proxy,
    // the proxy can intercept the request by the server cert.
    pub ca_key: Option<PathBuf>,
}

// ProxyServer implements Default.
impl Default for ProxyServer {
    fn default() -> Self {
        Self {
            ip: None,
            port: default_proxy_server_port(),
            ca_cert: None,
            ca_key: None,
        }
    }
}

// Rule is the proxy rule configuration.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Rule {
    // regex is the regex of the request url.
    #[serde(with = "serde_regex")]
    pub regex: Regex,

    // use_tls indicates whether use tls for the proxy backend.
    #[serde(rename = "useTLS")]
    pub use_tls: bool,

    // redirect is the redirect url.
    pub redirect: Option<String>,

    // filtered_query_params is the filtered query params to generate the task id.
    // When filter is ["Signature", "Expires", "ns"], for example:
    // http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io and http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io
    // will generate the same task id.
    // Default value includes the filtered query params of s3, gcs, oss, obs, cos.
    #[serde(default = "default_proxy_rule_filtered_query_params")]
    pub filtered_query_params: Vec<String>,
}

// Rule implements Default.
impl Default for Rule {
    fn default() -> Self {
        Self {
            regex: Regex::new(r".*").unwrap(),
            use_tls: false,
            redirect: None,
            filtered_query_params: default_proxy_rule_filtered_query_params(),
        }
    }
}

// RegistryMirror is the registry mirror configuration.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RegistryMirror {
    // addr is the default address of the registry mirror. Proxy will start a registry mirror service for the
    // client to pull the image. The client can use the default address of the registry mirror in
    // configuration to pull the image. The `X-Dragonfly-Registry` header can instead of the default address
    // of registry mirror.
    #[serde(default = "default_proxy_registry_mirror_addr")]
    pub addr: String,

    // certs is the client certs path with PEM format for the registry.
    // If registry use self-signed cert, the client should set the
    // cert for the registry mirror.
    pub certs: Option<PathBuf>,
}

// RegistryMirror implements Default.
impl Default for RegistryMirror {
    fn default() -> Self {
        Self {
            addr: default_proxy_registry_mirror_addr(),
            certs: None,
        }
    }
}

// Proxy is the proxy configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Proxy {
    // server is the proxy server configuration for dfdaemon.
    pub server: ProxyServer,

    // rules is the proxy rules.
    pub rules: Option<Vec<Rule>>,

    // registry_mirror is implementation of the registry mirror in the proxy.
    pub registry_mirror: RegistryMirror,

    // disable_back_to_source indicates whether disable to download back-to-source
    // when download failed.
    pub disable_back_to_source: bool,

    // prefetch pre-downloads full of the task when download with range request.
    pub prefetch: bool,

    // read_buffer_size is the buffer size for reading piece from disk, default is 1KB.
    #[serde(default = "default_proxy_read_buffer_size")]
    pub read_buffer_size: usize,
}

// Proxy implements Default.
impl Default for Proxy {
    fn default() -> Self {
        Self {
            server: ProxyServer::default(),
            rules: None,
            registry_mirror: RegistryMirror::default(),
            disable_back_to_source: false,
            prefetch: false,
            read_buffer_size: default_proxy_read_buffer_size(),
        }
    }
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

// HealthServer is the health server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct HealthServer {
    // ip is the listen ip of the health server.
    pub ip: Option<IpAddr>,

    // port is the port to the health server.
    #[serde(default = "default_health_server_port")]
    pub port: u16,
}

// HealthServer implements Default.
impl Default for HealthServer {
    fn default() -> Self {
        Self {
            ip: None,
            port: default_health_server_port(),
        }
    }
}

// Health is the health configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Health {
    // server is the health server configuration for dfdaemon.
    pub server: HealthServer,
}

// MetricsServer is the metrics server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct MetricsServer {
    // ip is the listen ip of the metrics server.
    pub ip: Option<IpAddr>,

    // port is the port to the metrics server.
    #[serde(default = "default_metrics_server_port")]
    pub port: u16,
}

// MetricsServer implements Default.
impl Default for MetricsServer {
    fn default() -> Self {
        Self {
            ip: None,
            port: default_metrics_server_port(),
        }
    }
}

// Metrics is the metrics configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Metrics {
    // server is the metrics server configuration for dfdaemon.
    pub server: MetricsServer,
}

// StatsServer is the stats server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct StatsServer {
    // ip is the listen ip of the stats server.
    pub ip: Option<IpAddr>,

    // port is the port to the stats server.
    #[serde(default = "default_stats_server_port")]
    pub port: u16,
}

// StatsServer implements Default.
impl Default for StatsServer {
    fn default() -> Self {
        Self {
            ip: None,
            port: default_stats_server_port(),
        }
    }
}

// Stats is the stats configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Stats {
    // server is the stats server configuration for dfdaemon.
    pub server: StatsServer,
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

    // health is the health configuration for dfdaemon.
    #[validate]
    pub health: Health,

    // metrics is the metrics configuration for dfdaemon.
    #[validate]
    pub metrics: Metrics,

    // stats is the stats configuration for dfdaemon.
    #[validate]
    pub stats: Stats,

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
    pub async fn load(path: &PathBuf) -> Result<Config> {
        // Load configuration from file.
        let content = fs::read_to_string(path).await?;
        let mut config: Config = serde_yaml::from_str(&content).or_err(ErrorType::ConfigError)?;
        info!("load config from {}", path.display());

        // Convert configuration.
        config.convert();

        // Validate configuration.
        config.validate().or_err(ErrorType::ValidationError)?;
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
        if self.health.server.ip.is_none() {
            self.health.server.ip = if self.network.enable_ipv6 {
                Some(Ipv6Addr::UNSPECIFIED.into())
            } else {
                Some(Ipv4Addr::UNSPECIFIED.into())
            }
        }

        // Convert metrics server listen ip.
        if self.metrics.server.ip.is_none() {
            self.metrics.server.ip = if self.network.enable_ipv6 {
                Some(Ipv6Addr::UNSPECIFIED.into())
            } else {
                Some(Ipv4Addr::UNSPECIFIED.into())
            }
        }

        // Convert stats server listen ip.
        if self.stats.server.ip.is_none() {
            self.stats.server.ip = if self.network.enable_ipv6 {
                Some(Ipv6Addr::UNSPECIFIED.into())
            } else {
                Some(Ipv4Addr::UNSPECIFIED.into())
            }
        }

        // Convert proxy server listen ip.
        if self.proxy.server.ip.is_none() {
            self.proxy.server.ip = if self.network.enable_ipv6 {
                Some(Ipv6Addr::UNSPECIFIED.into())
            } else {
                Some(Ipv4Addr::UNSPECIFIED.into())
            }
        }
    }
}
