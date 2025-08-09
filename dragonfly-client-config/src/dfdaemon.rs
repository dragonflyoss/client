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
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Result,
};
use dragonfly_client_util::{
    http::basic_auth,
    tls::{generate_ca_cert_from_pem, generate_cert_from_pem},
};
use local_ip_address::{local_ip, local_ipv6};
use rcgen::Certificate;
use regex::Regex;
use rustls_pki_types::CertificateDer;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs;
use tonic::transport::{
    Certificate as TonicCertificate, ClientTlsConfig, Identity, ServerTlsConfig,
};
use tracing::{error, instrument};
use validator::Validate;

/// NAME is the name of dfdaemon.
pub const NAME: &str = "dfdaemon";

/// default_dfdaemon_config_path is the default config path for dfdaemon.
#[inline]
pub fn default_dfdaemon_config_path() -> PathBuf {
    crate::default_config_dir().join("dfdaemon.yaml")
}

/// default_dfdaemon_log_dir is the default log directory for dfdaemon.
#[inline]
pub fn default_dfdaemon_log_dir() -> PathBuf {
    crate::default_log_dir().join(NAME)
}

/// default_download_unix_socket_path is the default unix socket path for download gRPC service.
pub fn default_download_unix_socket_path() -> PathBuf {
    crate::default_root_dir().join("dfdaemon.sock")
}

/// default_download_request_rate_limit is the default rate limit of the download request in the
/// download grpc server, default is 4000 req/s.
pub fn default_download_request_rate_limit() -> u64 {
    4000
}

/// default_parent_selector_sync_interval is the default interval to sync host information.
#[inline]
fn default_parent_selector_sync_interval() -> Duration {
    Duration::from_secs(3)
}

/// default_parent_selector_capacity is the default capacity of the parent selector's gRPC connections.
#[inline]
pub fn default_parent_selector_capacity() -> usize {
    20
}

/// default_host_hostname is the default hostname of the host.
#[inline]
fn default_host_hostname() -> String {
    hostname::get().unwrap().to_string_lossy().to_string()
}

/// default_dfdaemon_plugin_dir is the default plugin directory for dfdaemon.
#[inline]
fn default_dfdaemon_plugin_dir() -> PathBuf {
    crate::default_plugin_dir().join(NAME)
}

/// default_dfdaemon_cache_dir is the default cache directory for dfdaemon.
#[inline]
fn default_dfdaemon_cache_dir() -> PathBuf {
    crate::default_cache_dir().join(NAME)
}

/// default_upload_grpc_server_port is the default port of the upload gRPC server.
#[inline]
fn default_upload_grpc_server_port() -> u16 {
    4000
}

/// default_upload_request_rate_limit is the default rate limit of the upload request in the
/// upload grpc server, default is 4000 req/s.
pub fn default_upload_request_rate_limit() -> u64 {
    4000
}

/// default_upload_rate_limit is the default rate limit of the upload speed in GiB/Mib/Kib per second.
#[inline]
fn default_upload_rate_limit() -> ByteSize {
    // Default rate limit is 10GiB/s.
    ByteSize::gib(10)
}

/// default_health_server_port is the default port of the health server.
#[inline]
fn default_health_server_port() -> u16 {
    4003
}

/// default_metrics_server_port is the default port of the metrics server.
#[inline]
fn default_metrics_server_port() -> u16 {
    4002
}

/// default_stats_server_port is the default port of the stats server.
#[inline]
fn default_stats_server_port() -> u16 {
    4004
}

/// default_download_rate_limit is the default rate limit of the download speed in GiB/Mib/Kib per second.
#[inline]
fn default_download_rate_limit() -> ByteSize {
    // Default rate limit is 10GiB/s.
    ByteSize::gib(10)
}

/// default_download_piece_timeout is the default timeout for downloading a piece from source.
#[inline]
fn default_download_piece_timeout() -> Duration {
    Duration::from_secs(120)
}

/// default_collected_download_piece_timeout is the default timeout for collecting one piece from the parent in the stream.
#[inline]
fn default_collected_download_piece_timeout() -> Duration {
    Duration::from_secs(10)
}

/// default_download_concurrent_piece_count is the default number of concurrent pieces to download.
#[inline]
fn default_download_concurrent_piece_count() -> u32 {
    8
}

/// default_download_max_schedule_count is the default max count of schedule.
#[inline]
fn default_download_max_schedule_count() -> u32 {
    5
}

/// default_tracing_path is the default tracing path for dfdaemon.
#[inline]
fn default_tracing_path() -> Option<PathBuf> {
    Some(PathBuf::from("/v1/traces"))
}

/// default_scheduler_announce_interval is the default interval to announce peer to the scheduler.
#[inline]
fn default_scheduler_announce_interval() -> Duration {
    Duration::from_secs(300)
}

/// default_scheduler_schedule_timeout is the default timeout for scheduling.
#[inline]
fn default_scheduler_schedule_timeout() -> Duration {
    Duration::from_secs(3 * 60 * 60)
}

/// default_dynconfig_refresh_interval is the default interval to refresh dynamic configuration from manager.
#[inline]
fn default_dynconfig_refresh_interval() -> Duration {
    Duration::from_secs(300)
}

/// default_storage_server_protocol is the default protocol of the storage server.
#[inline]
fn default_storage_server_protocol() -> String {
    "grpc".to_string()
}

/// default_storage_keep is the default keep of the task's metadata and content when the dfdaemon restarts.
#[inline]
fn default_storage_keep() -> bool {
    false
}

/// default_storage_write_piece_timeout is the default timeout for writing a piece to storage(e.g., disk
/// or cache).
#[inline]
fn default_storage_write_piece_timeout() -> Duration {
    Duration::from_secs(90)
}

/// default_storage_write_buffer_size is the default buffer size for writing piece to disk, default is 4MB.
#[inline]
fn default_storage_write_buffer_size() -> usize {
    4 * 1024 * 1024
}

/// default_storage_read_buffer_size is the default buffer size for reading piece from disk, default is 4MB.
#[inline]
fn default_storage_read_buffer_size() -> usize {
    4 * 1024 * 1024
}

/// default_storage_cache_capacity is the default cache capacity for the storage server, default is
/// 64MiB.
#[inline]
fn default_storage_cache_capacity() -> ByteSize {
    ByteSize::mib(64)
}

/// default_gc_interval is the default interval to do gc.
#[inline]
fn default_gc_interval() -> Duration {
    Duration::from_secs(900)
}

/// default_gc_policy_task_ttl is the default ttl of the task.
#[inline]
fn default_gc_policy_task_ttl() -> Duration {
    Duration::from_secs(21_600)
}

/// default_gc_policy_dist_threshold is the default threshold of the disk usage to do gc.
#[inline]
fn default_gc_policy_dist_threshold() -> ByteSize {
    ByteSize::default()
}

/// default_gc_policy_dist_high_threshold_percent is the default high threshold percent of the disk usage.
#[inline]
fn default_gc_policy_dist_high_threshold_percent() -> u8 {
    80
}

/// default_gc_policy_dist_low_threshold_percent is the default low threshold percent of the disk usage.
#[inline]
fn default_gc_policy_dist_low_threshold_percent() -> u8 {
    60
}

/// default_proxy_server_port is the default port of the proxy server.
#[inline]
pub fn default_proxy_server_port() -> u16 {
    4001
}

/// default_proxy_read_buffer_size is the default buffer size for reading piece, default is 4MB.
#[inline]
pub fn default_proxy_read_buffer_size() -> usize {
    4 * 1024 * 1024
}

/// default_prefetch_rate_limit is the default rate limit of the prefetch speed in GiB/Mib/Kib per second. The prefetch request
/// has lower priority so limit the rate to avoid occupying the bandwidth impact other download tasks.
#[inline]
fn default_prefetch_rate_limit() -> ByteSize {
    // Default rate limit is 2GiB/s.
    ByteSize::gib(2)
}

/// default_s3_filtered_query_params is the default filtered query params with s3 protocol to generate the task id.
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

/// gcs_filtered_query_params is the filtered query params with gcs protocol to generate the task id.
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

/// oss_filtered_query_params is the filtered query params with oss protocol to generate the task id.
#[inline]
fn oss_filtered_query_params() -> Vec<String> {
    vec![
        "OSSAccessKeyId".to_string(),
        "Expires".to_string(),
        "Signature".to_string(),
        "SecurityToken".to_string(),
    ]
}

/// obs_filtered_query_params is the filtered query params with obs protocol to generate the task id.
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

/// cos_filtered_query_params is the filtered query params with cos protocol to generate the task id.
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

/// containerd_filtered_query_params is the filtered query params with containerd to generate the task id.
#[inline]
fn containerd_filtered_query_params() -> Vec<String> {
    vec!["ns".to_string()]
}

/// default_proxy_rule_filtered_query_params is the default filtered query params to generate the task id.
#[inline]
pub fn default_proxy_rule_filtered_query_params() -> Vec<String> {
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

    for query_param in containerd_filtered_query_params() {
        visited.insert(query_param);
    }

    visited.into_iter().collect()
}

/// default_proxy_registry_mirror_addr is the default registry mirror address.
#[inline]
fn default_proxy_registry_mirror_addr() -> String {
    "https://index.docker.io".to_string()
}

/// Host is the host configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Host {
    /// idc is the idc of the host.
    pub idc: Option<String>,

    /// location is the location of the host.
    pub location: Option<String>,

    /// hostname is the hostname of the host.
    #[serde(default = "default_host_hostname")]
    pub hostname: String,

    /// ip is the advertise ip of the host.
    pub ip: Option<IpAddr>,

    /// scheduler_cluster_id is the ID of the cluster to which the scheduler belongs.
    /// NOTE: This field is used to identify the cluster to which the scheduler belongs.
    /// If this flag is set, the idc, location, hostname and ip will be ignored when listing schedulers.
    #[serde(rename = "schedulerClusterID")]
    pub scheduler_cluster_id: Option<u64>,
}

/// Host implements Default.
impl Default for Host {
    fn default() -> Self {
        Host {
            idc: None,
            location: None,
            hostname: default_host_hostname(),
            ip: None,
            scheduler_cluster_id: None,
        }
    }
}

/// Server is the server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Server {
    /// plugin_dir is the directory to store plugins.
    #[serde(default = "default_dfdaemon_plugin_dir")]
    pub plugin_dir: PathBuf,

    /// cache_dir is the directory to store cache files.
    #[serde(default = "default_dfdaemon_cache_dir")]
    pub cache_dir: PathBuf,
}

/// Server implements Default.
impl Default for Server {
    fn default() -> Self {
        Server {
            plugin_dir: default_dfdaemon_plugin_dir(),
            cache_dir: default_dfdaemon_cache_dir(),
        }
    }
}

/// DownloadServer is the download server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct DownloadServer {
    /// socket_path is the unix socket path for dfdaemon gRPC service.
    #[serde(default = "default_download_unix_socket_path")]
    pub socket_path: PathBuf,

    /// request_rate_limit is the rate limit of the download request in the download grpc server,
    /// default is 4000 req/s.
    #[serde(default = "default_download_request_rate_limit")]
    pub request_rate_limit: u64,
}

/// DownloadServer implements Default.
impl Default for DownloadServer {
    fn default() -> Self {
        DownloadServer {
            socket_path: default_download_unix_socket_path(),
            request_rate_limit: default_download_request_rate_limit(),
        }
    }
}

/// Download is the download configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Download {
    /// server is the download server configuration for dfdaemon.
    pub server: DownloadServer,

    /// parent_selector is the download parent selector configuration for dfdaemon.
    pub parent_selector: ParentSelector,

    /// rate_limit is the rate limit of the download speed in GiB/Mib/Kib per second.
    #[serde(with = "bytesize_serde", default = "default_download_rate_limit")]
    pub rate_limit: ByteSize,

    /// piece_timeout is the timeout for downloading a piece from source.
    #[serde(default = "default_download_piece_timeout", with = "humantime_serde")]
    pub piece_timeout: Duration,

    /// collected_piece_timeout is the timeout for collecting one piece from the parent in the
    /// stream.
    #[serde(
        default = "default_collected_download_piece_timeout",
        with = "humantime_serde"
    )]
    pub collected_piece_timeout: Duration,

    /// concurrent_piece_count is the number of concurrent pieces to download.
    #[serde(default = "default_download_concurrent_piece_count")]
    #[validate(range(min = 1))]
    pub concurrent_piece_count: u32,
}

/// Download implements Default.
impl Default for Download {
    fn default() -> Self {
        Download {
            server: DownloadServer::default(),
            parent_selector: ParentSelector::default(),
            rate_limit: default_download_rate_limit(),
            piece_timeout: default_download_piece_timeout(),
            collected_piece_timeout: default_collected_download_piece_timeout(),
            concurrent_piece_count: default_download_concurrent_piece_count(),
        }
    }
}

/// UploadServer is the upload server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct UploadServer {
    /// ip is the listen ip of the gRPC server.
    pub ip: Option<IpAddr>,

    /// port is the port to the gRPC server.
    #[serde(default = "default_upload_grpc_server_port")]
    pub port: u16,

    /// ca_cert is the root CA cert path with PEM format for the upload server, and it is used
    /// for mutual TLS.
    pub ca_cert: Option<PathBuf>,

    /// cert is the server cert path with PEM format for the upload server and it is used for
    /// mutual TLS.
    pub cert: Option<PathBuf>,

    /// key is the server key path with PEM format for the upload server and it is used for
    /// mutual TLS.
    pub key: Option<PathBuf>,

    /// request_rate_limit is the rate limit of the upload request in the upload grpc server,
    /// default is 4000 req/s.
    #[serde(default = "default_upload_request_rate_limit")]
    pub request_rate_limit: u64,
}

/// UploadServer implements Default.
impl Default for UploadServer {
    fn default() -> Self {
        UploadServer {
            ip: None,
            port: default_upload_grpc_server_port(),
            ca_cert: None,
            cert: None,
            key: None,
            request_rate_limit: default_upload_request_rate_limit(),
        }
    }
}

/// UploadServer is the implementation of UploadServer.
impl UploadServer {
    /// load_server_tls_config loads the server tls config.
    pub async fn load_server_tls_config(&self) -> Result<Option<ServerTlsConfig>> {
        if let (Some(ca_cert_path), Some(server_cert_path), Some(server_key_path)) =
            (self.ca_cert.clone(), self.cert.clone(), self.key.clone())
        {
            let server_cert = fs::read(&server_cert_path).await?;
            let server_key = fs::read(&server_key_path).await?;
            let server_identity = Identity::from_pem(server_cert, server_key);

            let ca_cert = fs::read(&ca_cert_path).await?;
            let ca_cert = TonicCertificate::from_pem(ca_cert);

            return Ok(Some(
                ServerTlsConfig::new()
                    .identity(server_identity)
                    .client_ca_root(ca_cert),
            ));
        }

        Ok(None)
    }
}

/// UploadClient is the upload client configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct UploadClient {
    /// ca_cert is the root CA cert path with PEM format for the upload client, and it is used
    /// for mutual TLS.
    pub ca_cert: Option<PathBuf>,

    /// cert is the client cert path with PEM format for the upload client and it is used for
    /// mutual TLS.
    pub cert: Option<PathBuf>,

    /// key is the client key path with PEM format for the upload client and it is used for
    /// mutual TLS.
    pub key: Option<PathBuf>,
}

/// UploadClient is the implementation of UploadClient.
impl UploadClient {
    /// load_client_tls_config loads the client tls config.
    pub async fn load_client_tls_config(
        &self,
        domain_name: &str,
    ) -> Result<Option<ClientTlsConfig>> {
        if let (Some(ca_cert_path), Some(client_cert_path), Some(client_key_path)) =
            (self.ca_cert.clone(), self.cert.clone(), self.key.clone())
        {
            let client_cert = fs::read(&client_cert_path).await?;
            let client_key = fs::read(&client_key_path).await?;
            let client_identity = Identity::from_pem(client_cert, client_key);

            let ca_cert = fs::read(&ca_cert_path).await?;
            let ca_cert = TonicCertificate::from_pem(ca_cert);

            // TODO(gaius): Use trust_anchor to skip the verify of hostname.
            return Ok(Some(
                ClientTlsConfig::new()
                    .domain_name(domain_name)
                    .ca_certificate(ca_cert)
                    .identity(client_identity),
            ));
        }

        Ok(None)
    }
}

/// ParentSelector is the download parent selector configuration for dfdaemon. It will synchronize
/// the host info in real-time from the parents and then select the parents for downloading.
///
/// The workflow diagram is as follows:
///
///```text
///                              +----------+
///              ----------------|  Parent  |---------------
///              |               +----------+              |
///          Host Info                                Piece Metadata
/// +------------|-----------------------------------------|------------+
/// |            |                                         |            |
/// |            |                 Peer                    |            |
/// |            v                                         v            |
/// |  +------------------+                       +------------------+  |
/// |  |  ParentSelector  | ---Optimal Parent---> |  PieceCollector  |  |
/// |  +------------------+                       +------------------+  |
/// |                                                      |            |
/// |                                                 Piece Metadata    |
/// |                                                      |            |
/// |                                                      v            |
/// |                                                +------------+     |
/// |                                                |  Download  |     |
/// |                                                +------------+     |
/// +-------------------------------------------------------------------+
/// ```
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ParentSelector {
    /// enable indicates whether enable parent selector for downloading.
    ///
    /// If `enable` is true, the `ParentSelector`'s sync loop will start. It will periodically fetch
    /// host information from parents and use this information to calculate scores for selecting the
    /// parents for downloading.
    pub enable: bool,

    /// sync_interval is the interval to sync parents' host info by gRPC streaming.
    #[serde(
        default = "default_parent_selector_sync_interval",
        with = "humantime_serde"
    )]
    pub sync_interval: Duration,

    /// capacity is the maximum number of gRPC connections that `DfdaemonUpload.SyncHost` maintains
    /// in the `ParentSelector`, the default value is 20.
    #[serde(default = "default_parent_selector_capacity")]
    pub capacity: usize,
}

/// Upload is the upload configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Upload {
    /// server is the upload server configuration for dfdaemon.
    pub server: UploadServer,

    /// client is the upload client configuration for dfdaemon.
    pub client: UploadClient,

    /// disable_shared indicates whether disable to share data for other peers.
    pub disable_shared: bool,

    /// rate_limit is the rate limit of the upload speed in GiB/Mib/Kib per second.
    #[serde(with = "bytesize_serde", default = "default_upload_rate_limit")]
    pub rate_limit: ByteSize,
}

/// Upload implements Default.
impl Default for Upload {
    fn default() -> Self {
        Upload {
            server: UploadServer::default(),
            client: UploadClient::default(),
            disable_shared: false,
            rate_limit: default_upload_rate_limit(),
        }
    }
}

/// Manager is the manager configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Manager {
    /// addr is the manager address.
    pub addr: String,

    /// ca_cert is the root CA cert path with PEM format for the manager, and it is used
    /// for mutual TLS.
    pub ca_cert: Option<PathBuf>,

    /// cert is the client cert path with PEM format for the manager and it is used for
    /// mutual TLS.
    pub cert: Option<PathBuf>,

    /// key is the client key path with PEM format for the manager and it is used for
    /// mutual TLS.
    pub key: Option<PathBuf>,
}

/// Manager is the implementation of Manager.
impl Manager {
    /// load_client_tls_config loads the client tls config.
    pub async fn load_client_tls_config(
        &self,
        domain_name: &str,
    ) -> Result<Option<ClientTlsConfig>> {
        if let (Some(ca_cert_path), Some(client_cert_path), Some(client_key_path)) =
            (self.ca_cert.clone(), self.cert.clone(), self.key.clone())
        {
            let client_cert = fs::read(&client_cert_path).await?;
            let client_key = fs::read(&client_key_path).await?;
            let client_identity = Identity::from_pem(client_cert, client_key);

            let ca_cert = fs::read(&ca_cert_path).await?;
            let ca_cert = TonicCertificate::from_pem(ca_cert);

            // TODO(gaius): Use trust_anchor to skip the verify of hostname.
            return Ok(Some(
                ClientTlsConfig::new()
                    .domain_name(domain_name)
                    .ca_certificate(ca_cert)
                    .identity(client_identity),
            ));
        }

        Ok(None)
    }
}

/// Scheduler is the scheduler configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Scheduler {
    /// announce_interval is the interval to announce peer to the scheduler.
    /// Announcer will provide the scheduler with peer information for scheduling,
    /// peer information includes cpu, memory, etc.
    #[serde(
        default = "default_scheduler_announce_interval",
        with = "humantime_serde"
    )]
    pub announce_interval: Duration,

    /// schedule_timeout is timeout for the scheduler to respond to a scheduling request from dfdaemon, default is 3 hours.
    ///
    /// If the scheduler's response time for a scheduling decision exceeds this timeout,
    /// dfdaemon will encounter a `TokioStreamElapsed(Elapsed(()))` error.
    ///
    /// Behavior upon timeout:
    ///   - If `enable_back_to_source` is `true`, dfdaemon will attempt to download directly
    ///     from the source.
    ///   - Otherwise (if `enable_back_to_source` is `false`), dfdaemon will report a download failure.
    ///
    /// **Important Considerations Regarding Timeout Triggers**:
    /// This timeout isn't solely for the scheduler's direct response. It can also be triggered
    /// if the overall duration of the client's interaction with the scheduler for a task
    /// (e.g., client downloading initial pieces and reporting their status back to the scheduler)
    /// exceeds `schedule_timeout`. During such client-side processing and reporting,
    /// the scheduler might be awaiting these updates before sending its comprehensive
    /// scheduling response, and this entire period is subject to the `schedule_timeout`.
    ///
    /// **Configuration Guidance**:
    /// To prevent premature timeouts, `schedule_timeout` should be configured to a value
    /// greater than the maximum expected time for the *entire scheduling interaction*.
    /// This includes:
    ///   1. The scheduler's own processing and response time.
    ///   2. The time taken by the client to download any initial pieces and download all pieces finished,
    ///      as this communication is part of the scheduling phase.
    ///
    /// Setting this value too low can lead to `TokioStreamElapsed` errors even if the
    /// network and scheduler are functioning correctly but the combined interaction time
    /// is longer than the configured timeout.
    #[serde(
        default = "default_scheduler_schedule_timeout",
        with = "humantime_serde"
    )]
    pub schedule_timeout: Duration,

    /// max_schedule_count is the max count of schedule.
    #[serde(default = "default_download_max_schedule_count")]
    #[validate(range(min = 1))]
    pub max_schedule_count: u32,

    /// ca_cert is the root CA cert path with PEM format for the scheduler, and it is used
    /// for mutual TLS.
    pub ca_cert: Option<PathBuf>,

    /// cert is the client cert path with PEM format for the scheduler and it is used for
    /// mutual TLS.
    pub cert: Option<PathBuf>,

    /// key is the client key path with PEM format for the scheduler and it is used for
    /// mutual TLS.
    pub key: Option<PathBuf>,
}

/// Scheduler implements Default.
impl Default for Scheduler {
    fn default() -> Self {
        Scheduler {
            announce_interval: default_scheduler_announce_interval(),
            schedule_timeout: default_scheduler_schedule_timeout(),
            max_schedule_count: default_download_max_schedule_count(),
            ca_cert: None,
            cert: None,
            key: None,
        }
    }
}

/// Scheduler is the implementation of Scheduler.
impl Scheduler {
    /// load_client_tls_config loads the client tls config.
    pub async fn load_client_tls_config(
        &self,
        domain_name: &str,
    ) -> Result<Option<ClientTlsConfig>> {
        if let (Some(ca_cert_path), Some(client_cert_path), Some(client_key_path)) =
            (self.ca_cert.clone(), self.cert.clone(), self.key.clone())
        {
            let client_cert = fs::read(&client_cert_path).await?;
            let client_key = fs::read(&client_key_path).await?;
            let client_identity = Identity::from_pem(client_cert, client_key);

            let ca_cert = fs::read(&ca_cert_path).await?;
            let ca_cert = TonicCertificate::from_pem(ca_cert);

            // TODO(gaius): Use trust_anchor to skip the verify of hostname.
            return Ok(Some(
                ClientTlsConfig::new()
                    .domain_name(domain_name)
                    .ca_certificate(ca_cert)
                    .identity(client_identity),
            ));
        }

        Ok(None)
    }
}

/// HostType is the type of the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum HostType {
    /// Normal indicates the peer is normal peer.
    #[serde(rename = "normal")]
    Normal,

    /// Super indicates the peer is super seed peer.
    #[default]
    #[serde(rename = "super")]
    Super,

    /// Strong indicates the peer is strong seed peer.
    #[serde(rename = "strong")]
    Strong,

    /// Weak indicates the peer is weak seed peer.
    #[serde(rename = "weak")]
    Weak,
}

/// HostType implements Display.
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

/// SeedPeer is the seed peer configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SeedPeer {
    /// enable indicates whether enable seed peer.
    pub enable: bool,

    /// kind is the type of seed peer.
    #[serde(default, rename = "type")]
    pub kind: HostType,
}

/// SeedPeer implements Default.
impl Default for SeedPeer {
    fn default() -> Self {
        SeedPeer {
            enable: false,
            kind: HostType::Normal,
        }
    }
}

/// Dynconfig is the dynconfig configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Dynconfig {
    /// refresh_interval is the interval to refresh dynamic configuration from manager.
    #[serde(
        default = "default_dynconfig_refresh_interval",
        with = "humantime_serde"
    )]
    pub refresh_interval: Duration,
}

/// Dynconfig implements Default.
impl Default for Dynconfig {
    fn default() -> Self {
        Dynconfig {
            refresh_interval: default_dynconfig_refresh_interval(),
        }
    }
}

/// StorageServer is the storage server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct StorageServer {
    /// protocol is the protocol of the storage server. The protocol used for downloading pieces
    /// between different peers, now only support gRPC.
    ///
    /// gRPC Protocol: The storage server will start a gRPC service in the DfdaemonUploadServer,
    /// refer to https://github.com/dragonflyoss/api/blob/main/proto/dfdaemon.proto#L185.
    #[serde(default = "default_storage_server_protocol")]
    pub protocol: String,
}

/// Storage implements Default.
impl Default for StorageServer {
    fn default() -> Self {
        StorageServer {
            protocol: default_storage_server_protocol(),
        }
    }
}

/// Encryption is the storage encryption configuration for dfdaemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Encryption {
    pub enable: bool,
}

fn default_storage_encryption() -> Encryption {
    Encryption { 
        enable: false,
    }
}

/// Storage is the storage configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Storage {
    /// server is the storage server configuration for dfdaemon.
    pub server: StorageServer,

    /// dir is the directory to store task's metadata and content.
    #[serde(default = "crate::default_storage_dir")]
    pub dir: PathBuf,

    /// keep indicates whether keep the task's metadata and content when the dfdaemon restarts.
    #[serde(default = "default_storage_keep")]
    pub keep: bool,

    /// write_piece_timeout is the timeout for writing a piece to storage(e.g., disk
    /// or cache).
    #[serde(
        default = "default_storage_write_piece_timeout",
        with = "humantime_serde"
    )]
    pub write_piece_timeout: Duration,

    /// write_buffer_size is the buffer size for writing piece to disk, default is 128KB.
    #[serde(default = "default_storage_write_buffer_size")]
    pub write_buffer_size: usize,

    /// read_buffer_size is the buffer size for reading piece from disk, default is 128KB.
    #[serde(default = "default_storage_read_buffer_size")]
    pub read_buffer_size: usize,

    /// cache_capacity is the cache capacity for downloading, default is 100.
    ///
    /// Cache storage:
    /// 1. Users can preheat task by caching to memory (via CacheTask) or to disk (via Task).
    ///    For more details, refer to https://github.com/dragonflyoss/api/blob/main/proto/dfdaemon.proto#L174.
    /// 2. If the download hits the memory cache, it will be faster than reading from the disk, because there is no
    ///    page cache for the first read.
    ///
    ///```text
    ///                    +--------+
    ///                    │ Source │
    ///                    +--------+
    ///                       ^ ^                Preheat
    ///                       │ │                   |
    /// +-----------------+   │ │    +----------------------------+
    /// │   Other Peers   │   │ │    │  Peer        |             │
    /// │                 │   │ │    │              v             │
    /// │  +----------+   │   │ │    │        +----------+        │
    /// │  │  Cache   |<--|----------|<-Miss--|  Cache   |--Hit-->|<----Download CacheTask
    /// │  +----------+   │     │    │        +----------+        │
    /// │                 │     │    │                            │
    /// │  +----------+   │     │    │        +----------+        │
    /// │  │   Disk   |<--|----------|<-Miss--|   Disk   |--Hit-->|<----Download Task
    /// │  +----------+   │          │        +----------+        │
    /// │                 │          │              ^             │
    /// │                 │          │              |             │
    /// +-----------------+          +----------------------------+
    ///                                             |
    ///                                          Preheat
    ///```   
    #[serde(with = "bytesize_serde", default = "default_storage_cache_capacity")]
    pub cache_capacity: ByteSize,

    /// encrtption is the encryption configuration for the storage.
    #[serde(default = "default_storage_encryption")] 
    pub encryption: Encryption,
}

/// Storage implements Default.
impl Default for Storage {
    fn default() -> Self {
        Storage {
            server: StorageServer::default(),
            dir: crate::default_storage_dir(),
            keep: default_storage_keep(),
            write_piece_timeout: default_storage_write_piece_timeout(),
            write_buffer_size: default_storage_write_buffer_size(),
            read_buffer_size: default_storage_read_buffer_size(),
            cache_capacity: default_storage_cache_capacity(),
            encryption: default_storage_encryption(),
        }
    }
}

/// Policy is the policy configuration for gc.
#[derive(Debug, Clone, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Policy {
    /// task_ttl is the ttl of the task.
    #[serde(
        default = "default_gc_policy_task_ttl",
        rename = "taskTTL",
        with = "humantime_serde"
    )]
    pub task_ttl: Duration,

    /// dist_threshold optionally defines a specific disk capacity to be used as the base for
    /// calculating GC trigger points with `dist_high_threshold_percent` and `dist_low_threshold_percent`.
    ///
    /// - If a value is provided (e.g., "500GB"), the percentage-based thresholds (`dist_high_threshold_percent`,
    ///   `dist_low_threshold_percent`) are applied relative to this specified capacity.
    /// - If not provided or set to 0 (the default behavior), these percentage-based thresholds are applied
    ///   relative to the total actual disk space.
    ///
    /// This allows dfdaemon to effectively manage a logical portion of the disk for its cache,
    /// rather than always considering the entire disk volume.
    #[serde(with = "bytesize_serde", default = "default_gc_policy_dist_threshold")]
    pub dist_threshold: ByteSize,

    /// dist_high_threshold_percent is the high threshold percent of the disk usage.
    /// If the disk usage is greater than the threshold, dfdaemon will do gc.
    #[serde(default = "default_gc_policy_dist_high_threshold_percent")]
    #[validate(range(min = 1, max = 99))]
    pub dist_high_threshold_percent: u8,

    /// dist_low_threshold_percent is the low threshold percent of the disk usage.
    /// If the disk usage is less than the threshold, dfdaemon will stop gc.
    #[serde(default = "default_gc_policy_dist_low_threshold_percent")]
    #[validate(range(min = 1, max = 99))]
    pub dist_low_threshold_percent: u8,
}

/// Policy implements Default.
impl Default for Policy {
    fn default() -> Self {
        Policy {
            dist_threshold: default_gc_policy_dist_threshold(),
            task_ttl: default_gc_policy_task_ttl(),
            dist_high_threshold_percent: default_gc_policy_dist_high_threshold_percent(),
            dist_low_threshold_percent: default_gc_policy_dist_low_threshold_percent(),
        }
    }
}

/// GC is the gc configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct GC {
    /// interval is the interval to do gc.
    #[serde(default = "default_gc_interval", with = "humantime_serde")]
    pub interval: Duration,

    /// policy is the gc policy.
    pub policy: Policy,
}

/// GC implements Default.
impl Default for GC {
    fn default() -> Self {
        GC {
            interval: default_gc_interval(),
            policy: Policy::default(),
        }
    }
}

/// BasicAuth is the basic auth configuration for HTTP proxy in dfdaemon.
#[derive(Default, Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct BasicAuth {
    /// username is the username of the basic auth.
    #[validate(length(min = 1, max = 20))]
    pub username: String,

    /// passwork is the passwork of the basic auth.
    #[validate(length(min = 1, max = 20))]
    pub password: String,
}

impl BasicAuth {
    /// credentials loads the credentials.
    pub fn credentials(&self) -> basic_auth::Credentials {
        basic_auth::Credentials::new(&self.username, &self.password)
    }
}

/// ProxyServer is the proxy server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ProxyServer {
    /// ip is the listen ip of the proxy server.
    pub ip: Option<IpAddr>,

    /// port is the port to the proxy server.
    #[serde(default = "default_proxy_server_port")]
    pub port: u16,

    /// ca_cert is the root CA cert path with PEM format for the proxy server to generate the server cert.
    ///
    /// If ca_cert is empty, proxy will generate a smaple CA cert by rcgen::generate_simple_self_signed.
    /// When client requests via the proxy, the client should not verify the server cert and set
    /// insecure to true.
    ///
    /// If ca_cert is not empty, proxy will sign the server cert with the CA cert. If openssl is installed,
    /// you can use openssl to generate the root CA cert and make the system trust the root CA cert.
    /// Then set the ca_cert and ca_key to the root CA cert and key path. Dfdaemon generates the server cert
    /// and key, and signs the server cert with the root CA cert. When client requests via the proxy,
    /// the proxy can intercept the request by the server cert.
    pub ca_cert: Option<PathBuf>,

    /// ca_key is the root CA key path with PEM format for the proxy server to generate the server cert.
    ///
    /// If ca_key is empty, proxy will generate a smaple CA key by rcgen::generate_simple_self_signed.
    /// When client requests via the proxy, the client should not verify the server cert and set
    /// insecure to true.
    ///
    /// If ca_key is not empty, proxy will sign the server cert with the CA cert. If openssl is installed,
    /// you can use openssl to generate the root CA cert and make the system trust the root CA cert.
    /// Then set the ca_cert and ca_key to the root CA cert and key path. Dfdaemon generates the server cert
    /// and key, and signs the server cert with the root CA cert. When client requests via the proxy,
    /// the proxy can intercept the request by the server cert.
    pub ca_key: Option<PathBuf>,

    /// basic_auth is the basic auth configuration for HTTP proxy in dfdaemon. If basic_auth is not
    /// empty, the proxy will use the basic auth to authenticate the client by Authorization
    /// header. The value of the Authorization header is "Basic base64(username:password)", refer
    /// to https://en.wikipedia.org/wiki/Basic_access_authentication.
    pub basic_auth: Option<BasicAuth>,
}

/// ProxyServer implements Default.
impl Default for ProxyServer {
    fn default() -> Self {
        Self {
            ip: None,
            port: default_proxy_server_port(),
            ca_cert: None,
            ca_key: None,
            basic_auth: None,
        }
    }
}

/// ProxyServer is the implementation of ProxyServer.
impl ProxyServer {
    /// load_cert loads the cert.
    pub fn load_cert(&self) -> Result<Option<Certificate>> {
        if let (Some(server_ca_cert_path), Some(server_ca_key_path)) =
            (self.ca_cert.clone(), self.ca_key.clone())
        {
            match generate_ca_cert_from_pem(&server_ca_cert_path, &server_ca_key_path) {
                Ok(server_ca_cert) => return Ok(Some(server_ca_cert)),
                Err(err) => {
                    error!("generate ca cert and key from pem failed: {}", err);
                    return Err(err);
                }
            }
        }

        Ok(None)
    }
}

/// Rule is the proxy rule configuration.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Rule {
    /// regex is the regex of the request url.
    #[serde(with = "serde_regex")]
    pub regex: Regex,

    /// use_tls indicates whether use tls for the proxy backend.
    #[serde(rename = "useTLS")]
    pub use_tls: bool,

    /// redirect is the redirect url.
    pub redirect: Option<String>,

    /// filtered_query_params is the filtered query params to generate the task id.
    /// When filter is ["Signature", "Expires", "ns"], for example:
    /// http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io and http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io
    /// will generate the same task id.
    /// Default value includes the filtered query params of s3, gcs, oss, obs, cos.
    #[serde(default = "default_proxy_rule_filtered_query_params")]
    pub filtered_query_params: Vec<String>,
}

/// Rule implements Default.
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

/// RegistryMirror is the registry mirror configuration.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RegistryMirror {
    /// addr is the default address of the registry mirror. Proxy will start a registry mirror service for the
    /// client to pull the image. The client can use the default address of the registry mirror in
    /// configuration to pull the image. The `X-Dragonfly-Registry` header can instead of the default address
    /// of registry mirror.
    #[serde(default = "default_proxy_registry_mirror_addr")]
    pub addr: String,

    /// cert is the client cert path with PEM format for the registry.
    /// If registry use self-signed cert, the client should set the
    /// cert for the registry mirror.
    pub cert: Option<PathBuf>,
}

/// RegistryMirror implements Default.
impl Default for RegistryMirror {
    fn default() -> Self {
        Self {
            addr: default_proxy_registry_mirror_addr(),
            cert: None,
        }
    }
}

/// RegistryMirror is the implementation of RegistryMirror.
impl RegistryMirror {
    /// load_cert_ders loads the cert ders.
    pub fn load_cert_der(&self) -> Result<Option<Vec<CertificateDer<'static>>>> {
        if let Some(cert_path) = self.cert.clone() {
            match generate_cert_from_pem(&cert_path) {
                Ok(cert) => return Ok(Some(cert)),
                Err(err) => {
                    error!("generate cert from pems failed: {}", err);
                    return Err(err);
                }
            }
        };

        Ok(None)
    }
}

/// Proxy is the proxy configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Proxy {
    /// server is the proxy server configuration for dfdaemon.
    pub server: ProxyServer,

    /// rules is the proxy rules.
    pub rules: Option<Vec<Rule>>,

    /// registry_mirror is implementation of the registry mirror in the proxy.
    pub registry_mirror: RegistryMirror,

    /// disable_back_to_source indicates whether disable to download back-to-source
    /// when download failed.
    pub disable_back_to_source: bool,

    /// prefetch pre-downloads full of the task when download with range request.
    pub prefetch: bool,

    /// prefetch_rate_limit is the rate limit of the prefetch speed in GiB/Mib/Kib per second. The prefetch request
    /// has lower priority so limit the rate to avoid occupying the bandwidth impact other download tasks.
    #[serde(with = "bytesize_serde", default = "default_prefetch_rate_limit")]
    pub prefetch_rate_limit: ByteSize,

    /// read_buffer_size is the buffer size for reading piece from disk, default is 1KB.
    #[serde(default = "default_proxy_read_buffer_size")]
    pub read_buffer_size: usize,
}

/// Proxy implements Default.
impl Default for Proxy {
    fn default() -> Self {
        Self {
            server: ProxyServer::default(),
            rules: None,
            registry_mirror: RegistryMirror::default(),
            disable_back_to_source: false,
            prefetch: false,
            prefetch_rate_limit: default_prefetch_rate_limit(),
            read_buffer_size: default_proxy_read_buffer_size(),
        }
    }
}

/// Security is the security configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Security {
    /// enable indicates whether enable security.
    pub enable: bool,
}

/// Network is the network configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Network {
    /// enable_ipv6 indicates whether enable ipv6.
    pub enable_ipv6: bool,
}

/// HealthServer is the health server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct HealthServer {
    /// ip is the listen ip of the health server.
    pub ip: Option<IpAddr>,

    /// port is the port to the health server.
    #[serde(default = "default_health_server_port")]
    pub port: u16,
}

/// HealthServer implements Default.
impl Default for HealthServer {
    fn default() -> Self {
        Self {
            ip: None,
            port: default_health_server_port(),
        }
    }
}

/// Health is the health configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Health {
    /// server is the health server configuration for dfdaemon.
    pub server: HealthServer,
}

/// MetricsServer is the metrics server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct MetricsServer {
    /// ip is the listen ip of the metrics server.
    pub ip: Option<IpAddr>,

    /// port is the port to the metrics server.
    #[serde(default = "default_metrics_server_port")]
    pub port: u16,
}

/// MetricsServer implements Default.
impl Default for MetricsServer {
    fn default() -> Self {
        Self {
            ip: None,
            port: default_metrics_server_port(),
        }
    }
}

/// Metrics is the metrics configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Metrics {
    /// server is the metrics server configuration for dfdaemon.
    pub server: MetricsServer,
}

/// StatsServer is the stats server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct StatsServer {
    /// ip is the listen ip of the stats server.
    pub ip: Option<IpAddr>,

    /// port is the port to the stats server.
    #[serde(default = "default_stats_server_port")]
    pub port: u16,
}

/// StatsServer implements Default.
impl Default for StatsServer {
    fn default() -> Self {
        Self {
            ip: None,
            port: default_stats_server_port(),
        }
    }
}

/// Stats is the stats configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Stats {
    /// server is the stats server configuration for dfdaemon.
    pub server: StatsServer,
}

/// Tracing is the tracing configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Tracing {
    /// Protocol specifies the communication protocol for the tracing server.
    /// Supported values: "http", "https", "grpc" (default: None).
    /// This determines how tracing logs are transmitted to the server.
    pub protocol: Option<String>,

    /// endpoint is the endpoint to report tracing log, example: "localhost:4317".
    pub endpoint: Option<String>,

    /// path is the path to report tracing log, example: "/v1/traces" if the protocol is "http" or
    /// "https".
    #[serde(default = "default_tracing_path")]
    pub path: Option<PathBuf>,

    /// headers is the headers to report tracing log.
    #[serde(with = "http_serde::header_map")]
    pub headers: reqwest::header::HeaderMap,
}

/// Tracing implements Default.
impl Default for Tracing {
    fn default() -> Self {
        Self {
            protocol: None,
            endpoint: None,
            path: default_tracing_path(),
            headers: reqwest::header::HeaderMap::new(),
        }
    }
}

/// Config is the configuration for dfdaemon.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
    /// host is the host configuration for dfdaemon.
    #[validate]
    pub host: Host,

    /// server is the server configuration for dfdaemon.
    #[validate]
    pub server: Server,

    /// download is the download configuration for dfdaemon.
    #[validate]
    pub download: Download,

    /// upload is the upload configuration for dfdaemon.
    #[validate]
    pub upload: Upload,

    /// manager is the manager configuration for dfdaemon.
    #[validate]
    pub manager: Manager,

    /// scheduler is the scheduler configuration for dfdaemon.
    #[validate]
    pub scheduler: Scheduler,

    /// seed_peer is the seed peer configuration for dfdaemon.
    #[validate]
    pub seed_peer: SeedPeer,

    /// dynconfig is the dynconfig configuration for dfdaemon.
    #[validate]
    pub dynconfig: Dynconfig,

    /// storage is the storage configuration for dfdaemon.
    #[validate]
    pub storage: Storage,

    /// gc is the gc configuration for dfdaemon.
    #[validate]
    pub gc: GC,

    /// proxy is the proxy configuration for dfdaemon.
    #[validate]
    pub proxy: Proxy,

    /// security is the security configuration for dfdaemon.
    #[validate]
    pub security: Security,

    /// health is the health configuration for dfdaemon.
    #[validate]
    pub health: Health,

    /// metrics is the metrics configuration for dfdaemon.
    #[validate]
    pub metrics: Metrics,

    /// stats is the stats configuration for dfdaemon.
    #[validate]
    pub stats: Stats,

    /// tracing is the tracing configuration for dfdaemon.
    #[validate]
    pub tracing: Tracing,

    /// network is the network configuration for dfdaemon.
    #[validate]
    pub network: Network,
}

/// Config implements the config operation of dfdaemon.
impl Config {
    /// load loads configuration from file.
    #[instrument(skip_all)]
    pub async fn load(path: &PathBuf) -> Result<Config> {
        // Load configuration from file.
        let content = fs::read_to_string(path).await?;
        let mut config: Config = serde_yaml::from_str(&content).or_err(ErrorType::ConfigError)?;

        // Convert configuration.
        config.convert();

        // Validate configuration.
        config.validate().or_err(ErrorType::ValidationError)?;
        Ok(config)
    }

    /// convert converts the configuration.
    #[instrument(skip_all)]
    fn convert(&mut self) {
        // Convert advertise ip.
        if self.host.ip.is_none() {
            self.host.ip = if self.network.enable_ipv6 {
                Some(local_ipv6().unwrap())
            } else {
                Some(local_ip().unwrap())
            }
        }

        // Convert upload gRPC server listen ip.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;
    use tokio::fs;

    #[test]
    fn default_proxy_rule_filtered_query_params_contains_all_params() {
        let mut expected = HashSet::new();
        expected.extend(s3_filtered_query_params());
        expected.extend(gcs_filtered_query_params());
        expected.extend(oss_filtered_query_params());
        expected.extend(obs_filtered_query_params());
        expected.extend(cos_filtered_query_params());
        expected.extend(containerd_filtered_query_params());

        let actual = default_proxy_rule_filtered_query_params();
        let actual_set: HashSet<_> = actual.into_iter().collect();

        assert_eq!(actual_set, expected);
    }

    #[test]
    fn default_proxy_rule_removes_duplicates() {
        let params: Vec<String> = default_proxy_rule_filtered_query_params();
        let param_count = params.len();

        let unique_params: HashSet<_> = params.into_iter().collect();
        assert_eq!(unique_params.len(), param_count);
    }

    #[test]
    fn default_proxy_rule_filtered_query_params_contains_key_properties() {
        let params = default_proxy_rule_filtered_query_params();
        let param_set: HashSet<_> = params.into_iter().collect();

        assert!(param_set.contains("X-Amz-Signature"));
        assert!(param_set.contains("X-Goog-Signature"));
        assert!(param_set.contains("OSSAccessKeyId"));
        assert!(param_set.contains("X-Obs-Security-Token"));
        assert!(param_set.contains("q-sign-algorithm"));
        assert!(param_set.contains("ns"));
    }

    #[test]
    fn deserialize_server_correctly() {
        let json_data = r#"
        {
            "pluginDir": "/custom/plugin/dir",
            "cacheDir": "/custom/cache/dir"
        }"#;

        let server: Server = serde_json::from_str(json_data).unwrap();

        assert_eq!(server.plugin_dir, PathBuf::from("/custom/plugin/dir"));
        assert_eq!(server.cache_dir, PathBuf::from("/custom/cache/dir"));
    }

    #[test]
    fn deserialize_download_correctly() {
        let json_data = r#"
        {
            "server": {
                "socketPath": "/var/run/dragonfly/dfdaemon.sock",
                "requestRateLimit": 4000
            },
            "rateLimit": "50GiB",
            "pieceTimeout": "30s",
            "concurrentPieceCount": 10
        }"#;

        let download: Download = serde_json::from_str(json_data).unwrap();

        assert_eq!(
            download.server.socket_path,
            PathBuf::from("/var/run/dragonfly/dfdaemon.sock")
        );
        assert_eq!(download.server.request_rate_limit, 4000);

        assert_eq!(download.rate_limit, ByteSize::gib(50));
        assert_eq!(download.piece_timeout, Duration::from_secs(30));
        assert_eq!(download.concurrent_piece_count, 10);
    }

    #[test]
    fn deserialize_upload_correctly() {
        let json_data = r#"
        {
            "server": {
                "port": 4000,
                "ip": "127.0.0.1",
                "caCert": "/etc/ssl/certs/ca.crt",
                "cert": "/etc/ssl/certs/server.crt",
                "key": "/etc/ssl/private/server.pem"
            },
            "client": {
                "caCert": "/etc/ssl/certs/ca.crt",
                "cert": "/etc/ssl/certs/client.crt",
                "key": "/etc/ssl/private/client.pem"
            },
            "disableShared": false,
            "rateLimit": "10GiB"
        }"#;

        let upload: Upload = serde_json::from_str(json_data).unwrap();

        assert_eq!(upload.server.port, 4000);
        assert_eq!(
            upload.server.ip,
            Some("127.0.0.1".parse::<IpAddr>().unwrap())
        );
        assert_eq!(
            upload.server.ca_cert,
            Some(PathBuf::from("/etc/ssl/certs/ca.crt"))
        );
        assert_eq!(
            upload.server.cert,
            Some(PathBuf::from("/etc/ssl/certs/server.crt"))
        );
        assert_eq!(
            upload.server.key,
            Some(PathBuf::from("/etc/ssl/private/server.pem"))
        );

        assert_eq!(
            upload.client.ca_cert,
            Some(PathBuf::from("/etc/ssl/certs/ca.crt"))
        );
        assert_eq!(
            upload.client.cert,
            Some(PathBuf::from("/etc/ssl/certs/client.crt"))
        );
        assert_eq!(
            upload.client.key,
            Some(PathBuf::from("/etc/ssl/private/client.pem"))
        );

        assert!(!upload.disable_shared);
        assert_eq!(upload.rate_limit, ByteSize::gib(10));
    }

    #[test]
    fn upload_server_default() {
        let server = UploadServer::default();

        assert!(server.ip.is_none());
        assert_eq!(server.port, default_upload_grpc_server_port());
        assert!(server.ca_cert.is_none());
        assert!(server.cert.is_none());
        assert!(server.key.is_none());
        assert_eq!(
            server.request_rate_limit,
            default_upload_request_rate_limit()
        );
    }

    #[tokio::test]
    async fn upload_load_server_tls_config_success() {
        let (ca_file, cert_file, key_file) = create_temp_certs().await;

        let server = UploadServer {
            ca_cert: Some(ca_file.path().to_path_buf()),
            cert: Some(cert_file.path().to_path_buf()),
            key: Some(key_file.path().to_path_buf()),
            ..Default::default()
        };

        let tls_config = server.load_server_tls_config().await.unwrap();
        assert!(tls_config.is_some());
    }

    #[tokio::test]
    async fn load_server_tls_config_missing_certs() {
        let server = UploadServer {
            ca_cert: Some(PathBuf::from("/invalid/path")),
            cert: None,
            key: None,
            ..Default::default()
        };

        let tls_config = server.load_server_tls_config().await.unwrap();
        assert!(tls_config.is_none());
    }

    #[test]
    fn upload_client_default() {
        let client = UploadClient::default();
        assert!(client.ca_cert.is_none());
        assert!(client.cert.is_none());
        assert!(client.key.is_none());
    }

    #[tokio::test]
    async fn upload_client_load_tls_config_success() {
        let (ca_file, cert_file, key_file) = create_temp_certs().await;

        let client = UploadClient {
            ca_cert: Some(ca_file.path().to_path_buf()),
            cert: Some(cert_file.path().to_path_buf()),
            key: Some(key_file.path().to_path_buf()),
        };

        let tls_config = client.load_client_tls_config("example.com").await.unwrap();
        assert!(tls_config.is_some());

        let cfg_string = format!("{:?}", tls_config.unwrap());
        assert!(
            cfg_string.contains("example.com"),
            "Domain name not found in TLS config"
        );
    }

    #[tokio::test]
    async fn upload_server_load_tls_config_invalid_path() {
        let server = UploadServer {
            ca_cert: Some(PathBuf::from("/invalid/ca.crt")),
            cert: Some(PathBuf::from("/invalid/server.crt")),
            key: Some(PathBuf::from("/invalid/server.key")),
            ..Default::default()
        };

        let result = server.load_server_tls_config().await;
        assert!(result.is_err());
    }

    async fn create_temp_certs() -> (NamedTempFile, NamedTempFile, NamedTempFile) {
        let ca = NamedTempFile::new().unwrap();
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();

        fs::write(ca.path(), "-----BEGIN CERT-----\n...\n-----END CERT-----\n")
            .await
            .unwrap();
        fs::write(
            cert.path(),
            "-----BEGIN CERT-----\n...\n-----END CERT-----\n",
        )
        .await
        .unwrap();
        fs::write(
            key.path(),
            "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
        )
        .await
        .unwrap();

        (ca, cert, key)
    }

    #[tokio::test]
    async fn manager_load_client_tls_config_success() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let ca_path = temp_dir.path().join("ca.crt");
        let cert_path = temp_dir.path().join("client.crt");
        let key_path = temp_dir.path().join("client.key");

        fs::write(&ca_path, "CA cert content").await.unwrap();
        fs::write(&cert_path, "Client cert content").await.unwrap();
        fs::write(&key_path, "Client key content").await.unwrap();

        let manager = Manager {
            addr: "http://example.com".to_string(),
            ca_cert: Some(ca_path),
            cert: Some(cert_path),
            key: Some(key_path),
        };

        let result = manager.load_client_tls_config("example.com").await;
        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.is_some());
    }

    #[test]
    fn deserialize_optional_fields_correctly() {
        let yaml = r#"
addr: http://another-service:8080
"#;

        let manager: Manager = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(manager.addr, "http://another-service:8080");
        assert!(manager.ca_cert.is_none());
        assert!(manager.cert.is_none());
        assert!(manager.key.is_none());
    }

    #[test]
    fn deserialize_manager_correctly() {
        let yaml = r#"
addr: http://manager-service:65003
caCert: /etc/ssl/certs/ca.crt
cert: /etc/ssl/certs/client.crt
key: /etc/ssl/private/client.pem
"#;
        let manager: Manager = serde_yaml::from_str(yaml).expect("Failed to deserialize");

        assert_eq!(manager.addr, "http://manager-service:65003");
        assert_eq!(
            manager.ca_cert,
            Some(PathBuf::from("/etc/ssl/certs/ca.crt"))
        );
        assert_eq!(
            manager.cert,
            Some(PathBuf::from("/etc/ssl/certs/client.crt"))
        );
        assert_eq!(
            manager.key,
            Some(PathBuf::from("/etc/ssl/private/client.pem"))
        );
    }

    #[test]
    fn default_host_type_correctly() {
        // Test whether the Display implementation is correct.
        assert_eq!(HostType::Normal.to_string(), "normal");
        assert_eq!(HostType::Super.to_string(), "super");
        assert_eq!(HostType::Strong.to_string(), "strong");
        assert_eq!(HostType::Weak.to_string(), "weak");

        // Test if the default value is HostType::Super.
        let default_host_type: HostType = Default::default();
        assert_eq!(default_host_type, HostType::Super);
    }

    #[test]
    fn serialize_host_type_correctly() {
        let normal: HostType = serde_json::from_str("\"normal\"").unwrap();
        let super_seed: HostType = serde_json::from_str("\"super\"").unwrap();
        let strong_seed: HostType = serde_json::from_str("\"strong\"").unwrap();
        let weak_seed: HostType = serde_json::from_str("\"weak\"").unwrap();

        assert_eq!(normal, HostType::Normal);
        assert_eq!(super_seed, HostType::Super);
        assert_eq!(strong_seed, HostType::Strong);
        assert_eq!(weak_seed, HostType::Weak);
    }

    #[test]
    fn serialize_host_type() {
        let normal_json = serde_json::to_string(&HostType::Normal).unwrap();
        let super_json = serde_json::to_string(&HostType::Super).unwrap();
        let strong_json = serde_json::to_string(&HostType::Strong).unwrap();
        let weak_json = serde_json::to_string(&HostType::Weak).unwrap();

        assert_eq!(normal_json, "\"normal\"");
        assert_eq!(super_json, "\"super\"");
        assert_eq!(strong_json, "\"strong\"");
        assert_eq!(weak_json, "\"weak\"");
    }

    #[test]
    fn default_seed_peer() {
        let default_seed_peer = SeedPeer::default();
        assert!(!default_seed_peer.enable);
        assert_eq!(default_seed_peer.kind, HostType::Normal);
    }

    #[test]
    fn validate_seed_peer() {
        let valid_seed_peer = SeedPeer {
            enable: true,
            kind: HostType::Weak,
        };

        assert!(valid_seed_peer.validate().is_ok());
    }

    #[test]
    fn deserialize_seed_peer_correctly() {
        let json_data = r#"
        {
            "enable": true,
            "type": "super",
            "clusterID": 2,
            "keepaliveInterval": "60s"
        }"#;

        let seed_peer: SeedPeer = serde_json::from_str(json_data).unwrap();

        assert!(seed_peer.enable);
        assert_eq!(seed_peer.kind, HostType::Super);
    }

    #[test]
    fn default_dynconfig() {
        let default_dynconfig = Dynconfig::default();
        assert_eq!(default_dynconfig.refresh_interval, Duration::from_secs(300));
    }

    #[test]
    fn deserialize_dynconfig_correctly() {
        let json_data = r#"
        {
            "refreshInterval": "5m"
        }"#;

        let dynconfig: Dynconfig = serde_json::from_str(json_data).unwrap();

        assert_eq!(dynconfig.refresh_interval, Duration::from_secs(300));
    }

    #[test]
    fn deserialize_storage_correctly() {
        let json_data = r#"
        {
            "server": {
                "protocol": "http"
            },
            "dir": "/tmp/storage",
            "keep": true,
            "writePieceTimeout": "20s",
            "writeBufferSize": 8388608,
            "readBufferSize": 8388608,
            "cacheCapacity": "256MB"
        }"#;

        let storage: Storage = serde_json::from_str(json_data).unwrap();

        assert_eq!(storage.server.protocol, "http".to_string());
        assert_eq!(storage.dir, PathBuf::from("/tmp/storage"));
        assert!(storage.keep);
        assert_eq!(storage.write_piece_timeout, Duration::from_secs(20));
        assert_eq!(storage.write_buffer_size, 8 * 1024 * 1024);
        assert_eq!(storage.read_buffer_size, 8 * 1024 * 1024);
        assert_eq!(storage.cache_capacity, ByteSize::mb(256));
    }

    #[test]
    fn validate_policy() {
        let valid_policy = Policy {
            task_ttl: Duration::from_secs(12 * 3600),
            dist_threshold: ByteSize::mb(100),
            dist_high_threshold_percent: 90,
            dist_low_threshold_percent: 70,
        };
        assert!(valid_policy.validate().is_ok());

        let invalid_policy = Policy {
            task_ttl: Duration::from_secs(12 * 3600),
            dist_threshold: ByteSize::mb(100),
            dist_high_threshold_percent: 100,
            dist_low_threshold_percent: 70,
        };
        assert!(invalid_policy.validate().is_err());
    }

    #[test]
    fn deserialize_gc_correctly() {
        let json_data = r#"
        {
            "interval": "1h",
            "policy": {
                "taskTTL": "12h",
                "distHighThresholdPercent": 90,
                "distLowThresholdPercent": 70
            }
        }"#;

        let gc: GC = serde_json::from_str(json_data).unwrap();

        assert_eq!(gc.interval, Duration::from_secs(3600));
        assert_eq!(gc.policy.task_ttl, Duration::from_secs(12 * 3600));
        assert_eq!(gc.policy.dist_high_threshold_percent, 90);
        assert_eq!(gc.policy.dist_low_threshold_percent, 70);
    }

    #[test]
    fn deserialize_proxy_correctly() {
        let json_data = r#"
        {
            "server": {
                "port": 8080,
                "caCert": "/path/to/ca_cert.pem",
                "caKey": "/path/to/ca_key.pem",
                "basicAuth": {
                    "username": "admin",
                    "password": "password"
                }
            },
            "rules": [
                {
                    "regex": "^https?://example\\.com/.*$",
                    "useTLS": true,
                    "redirect": "https://mirror.example.com",
                    "filteredQueryParams": ["Signature", "Expires"]
                }
            ],
            "registryMirror": {
                "addr": "https://mirror.example.com",
                "cert": "/path/to/cert.pem"
            },
            "disableBackToSource": true,
            "prefetch": true,
            "prefetchRateLimit": "1GiB",
            "readBufferSize": 8388608
        }"#;

        let proxy: Proxy = serde_json::from_str(json_data).unwrap();

        assert_eq!(proxy.server.port, 8080);
        assert_eq!(
            proxy.server.ca_cert,
            Some(PathBuf::from("/path/to/ca_cert.pem"))
        );
        assert_eq!(
            proxy.server.ca_key,
            Some(PathBuf::from("/path/to/ca_key.pem"))
        );
        assert_eq!(
            proxy.server.basic_auth.as_ref().unwrap().username,
            "admin".to_string()
        );
        assert_eq!(
            proxy.server.basic_auth.as_ref().unwrap().password,
            "password".to_string()
        );

        let rule = &proxy.rules.as_ref().unwrap()[0];
        assert_eq!(rule.regex.as_str(), "^https?://example\\.com/.*$");
        assert!(rule.use_tls);
        assert_eq!(
            rule.redirect,
            Some("https://mirror.example.com".to_string())
        );
        assert_eq!(rule.filtered_query_params, vec!["Signature", "Expires"]);

        assert_eq!(proxy.registry_mirror.addr, "https://mirror.example.com");
        assert_eq!(
            proxy.registry_mirror.cert,
            Some(PathBuf::from("/path/to/cert.pem"))
        );

        assert!(proxy.disable_back_to_source);
        assert!(proxy.prefetch);
        assert_eq!(proxy.prefetch_rate_limit, ByteSize::gib(1));
        assert_eq!(proxy.read_buffer_size, 8 * 1024 * 1024);
    }

    #[test]
    fn deserialize_tracing_correctly() {
        let json_data = r#"
        {
            "protocol": "http",
            "endpoint": "tracing.example.com",
            "path": "/v1/traces",
            "headers": {
                "X-Custom-Header": "value"
            }
        }"#;

        let tracing: Tracing = serde_json::from_str(json_data).unwrap();
        assert_eq!(tracing.protocol, Some("http".to_string()));
        assert_eq!(tracing.endpoint, Some("tracing.example.com".to_string()));
        assert_eq!(tracing.path, Some(PathBuf::from("/v1/traces")));
        assert!(tracing.headers.contains_key("X-Custom-Header"));
    }

    #[test]
    fn deserialize_metrics_correctly() {
        let json_data = r#"
        {
            "server": {
                "port": 4002,
                "ip": "127.0.0.1"
            }
        }"#;

        let metrics: Metrics = serde_json::from_str(json_data).unwrap();

        assert_eq!(metrics.server.port, 4002);
        assert_eq!(
            metrics.server.ip,
            Some("127.0.0.1".parse::<IpAddr>().unwrap())
        );
    }

    #[test]
    fn deserialize_storage_encryption_yaml() {
        let yaml = 
r#"
encryption:
  enable: true
"#;

        println!("{}", yaml);

        let storage: Storage = serde_yaml::from_str(yaml).expect("Failed to deserialize");

        println!("{:#?}", storage);

        assert_eq!(storage.encryption.enable, true);
    }

    #[test]
    fn deserialize_storage_encryption_default() {
        let yaml = r#""#;
        let storage: Storage = serde_yaml::from_str(yaml).expect("Failed to deserialize");
        assert_eq!(storage.encryption.enable, false);
    }
}
