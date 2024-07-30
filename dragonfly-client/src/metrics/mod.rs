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

use crate::shutdown;
use chrono::DateTime;
use dragonfly_api::common::v2::{Range, TrafficType};
use lazy_static::lazy_static;
use prometheus::{
    exponential_buckets, gather, Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec,
    Opts, Registry, TextEncoder,
};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use warp::{Filter, Rejection, Reply};

// DOWNLOAD_TASK_LEVEL1_DURATION_THRESHOLD is the threshold of download task level1 duration for
// recording slow download task.
const DOWNLOAD_TASK_LEVEL1_DURATION_THRESHOLD: Duration = Duration::from_millis(500);

// UPLOAD_TASK_LEVEL1_DURATION_THRESHOLD is the threshold of upload task level1 duration for
// recording slow upload task.
const UPLOAD_TASK_LEVEL1_DURATION_THRESHOLD: Duration = Duration::from_millis(500);

lazy_static! {
    // REGISTRY is used to register all metrics.
    pub static ref REGISTRY: Registry = Registry::new();

    // VERSION_GAUGE is used to record the version info of the service.
    pub static ref VERSION_GAUGE: IntGaugeVec =
        IntGaugeVec::new(
            Opts::new("version", "Version info of the service.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["git_version", "git_commit", "platform", "build_time"]
        ).expect("metric can be created");

    // UPLOAD_TASK_COUNT is used to count the number of upload tasks.
    pub static ref UPLOAD_TASK_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("upload_task_total", "Counter of the number of the upload task.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["type", "tag", "app"]
        ).expect("metric can be created");

    // UPLOAD_TASK_FAILURE_COUNT is used to count the failed number of upload tasks.
    pub static ref UPLOAD_TASK_FAILURE_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("upload_task_failure_total", "Counter of the number of failed of the upload task.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["type", "tag", "app"]
        ).expect("metric can be created");

    // CONCURRENT_UPLOAD_TASK_GAUGE is used to gauge the number of concurrent upload tasks.
    pub static ref CONCURRENT_UPLOAD_TASK_GAUGE: IntGaugeVec =
        IntGaugeVec::new(
            Opts::new("concurrent_upload_task_total", "Gauge of the number of concurrent of the upload task.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["type", "tag", "app"]
        ).expect("metric can be created");

    // UPLOAD_TASK_DURATION is used to record the upload task duration.
    pub static ref UPLOAD_TASK_DURATION: HistogramVec =
        HistogramVec::new(
            HistogramOpts::new("upload_task_duration_milliseconds", "Histogram of the upload task duration.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME).buckets(exponential_buckets(1.0, 2.0, 24).unwrap()),
            &["task_type", "task_size_level"]
        ).expect("metric can be created");

    // DOWNLOAD_TASK_COUNT is used to count the number of download tasks.
    pub static ref DOWNLOAD_TASK_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("download_task_total", "Counter of the number of the download task.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["type", "tag", "app", "priority"]
        ).expect("metric can be created");

    // DOWNLOAD_TASK_FAILURE_COUNT is used to count the failed number of download tasks.
    pub static ref DOWNLOAD_TASK_FAILURE_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("download_task_failure_total", "Counter of the number of failed of the download task.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["type", "tag", "app", "priority"]
        ).expect("metric can be created");

    // PREFETCH_TASK_COUNT is used to count the number of prefetch tasks.
    pub static ref PREFETCH_TASK_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("prefetch_task_total", "Counter of the number of the prefetch task.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["type", "tag", "app", "priority"]
        ).expect("metric can be created");

    // PREFETCH_TASK_FAILURE_COUNT is used to count the failed number of prefetch tasks.
    pub static ref PREFETCH_TASK_FAILURE_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("prefetch_task_failure_total", "Counter of the number of failed of the prefetch task.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["type", "tag", "app", "priority"]
        ).expect("metric can be created");

    // CONCURRENT_DOWNLOAD_TASK_GAUGE is used to gauge the number of concurrent download tasks.
    pub static ref CONCURRENT_DOWNLOAD_TASK_GAUGE: IntGaugeVec =
        IntGaugeVec::new(
            Opts::new("concurrent_download_task_total", "Gauge of the number of concurrent of the download task.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["type", "tag", "app", "priority"]
        ).expect("metric can be created");

    // CONCURRENT_UPLOAD_PIECE_GAUGE is used to gauge the number of concurrent upload pieces.
    pub static ref CONCURRENT_UPLOAD_PIECE_GAUGE: IntGaugeVec =
        IntGaugeVec::new(
            Opts::new("concurrent_upload_piece_total", "Gauge of the number of concurrent of the upload piece.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &[]
        ).expect("metric can be created");

    // DOWNLOAD_TRAFFIC is used to count the download traffic.
    pub static ref DOWNLOAD_TRAFFIC: IntCounterVec =
        IntCounterVec::new(
            Opts::new("download_traffic", "Counter of the number of the download traffic.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["type", "task_type"]
        ).expect("metric can be created");

    // UPLOAD_TRAFFIC is used to count the upload traffic.
    pub static ref UPLOAD_TRAFFIC: IntCounterVec =
        IntCounterVec::new(
            Opts::new("upload_traffic", "Counter of the number of the upload traffic.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["task_type"]
        ).expect("metric can be created");

    // DOWNLOAD_TASK_DURATION is used to record the download task duration.
    pub static ref DOWNLOAD_TASK_DURATION: HistogramVec =
        HistogramVec::new(
            HistogramOpts::new("download_task_duration_milliseconds", "Histogram of the download task duration.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME).buckets(exponential_buckets(1.0, 2.0, 24).unwrap()),
            &["task_type", "task_size_level"]
        ).expect("metric can be created");

    // PROXY_REQUSET_COUNT is used to count the number of proxy requset.
    pub static ref PROXY_REQUSET_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("proxy_request_total", "Counter of the number of the proxy request.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &[]
        ).expect("metric can be created");

    // PROXY_REQUSET_FAILURE_COUNT is used to count the failed number of proxy request.
    pub static ref PROXY_REQUSET_FAILURE_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("proxy_request_failure_total", "Counter of the number of failed of the proxy request.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &[]
        ).expect("metric can be created");

    // STAT_TASK_COUNT is used to count the number of stat tasks.
    pub static ref STAT_TASK_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("stat_task_total", "Counter of the number of the stat task.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["type"]
        ).expect("metric can be created");

    // STAT_TASK_FAILURE_COUNT is used to count the failed number of stat tasks.
    pub static ref STAT_TASK_FAILURE_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("stat_task_failure_total", "Counter of the number of failed of the stat task.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["type"]
        ).expect("metric can be created");

    // DELETE_TASK_COUNT is used to count the number of delete tasks.
    pub static ref DELETE_TASK_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("delete_task_total", "Counter of the number of the delete task.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["type"]
        ).expect("metric can be created");

    // DELETE_TASK_FAILURE_COUNT is used to count the failed number of delete tasks.
    pub static ref DELETE_TASK_FAILURE_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("delete_task_failure_total", "Counter of the number of failed of the delete task.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &["type"]
        ).expect("metric can be created");

    // DELETE_HOST_COUNT is used to count the number of delete host.
    pub static ref DELETE_HOST_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("delete_host_total", "Counter of the number of the delete host.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &[]
        ).expect("metric can be created");

    // DELETE_HOST_FAILURE_COUNT is used to count the failed number of delete host.
    pub static ref DELETE_HOST_FAILURE_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("delete_host_failure_total", "Counter of the number of failed of the delete host.").namespace(dragonfly_client_config::SERVICE_NAME).subsystem(dragonfly_client_config::NAME),
            &[]
        ).expect("metric can be created");
}

// TaskSize represents the size of the task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskSize {
    // Level0 represents unknown size.
    Level0,

    // Level0 represents size range is from 0 to 1M.
    Level1,

    // Level1 represents size range is from 1M to 4M.
    Level2,

    // Level2 represents size range is from 4M to 8M.
    Level3,

    // Level3 represents size range is from 8M to 16M.
    Level4,

    // Level4 represents size range is from 16M to 32M.
    Level5,

    // Level5 represents size range is from 32M to 64M.
    Level6,

    // Level6 represents size range is from 64M to 128M.
    Level7,

    // Level7 represents size range is from 128M to 256M.
    Level8,

    // Level8 represents size range is from 256M to 512M.
    Level9,

    // Level9 represents size range is from 512M to 1G.
    Level10,

    // Level10 represents size range is from 1G to 4G.
    Level11,

    // Level11 represents size range is from 4G to 8G.
    Level12,

    // Level12 represents size range is from 8G to 16G.
    Level13,

    // Level13 represents size range is from 16G to 32G.
    Level14,

    // Level14 represents size range is from 32G to 64G.
    Level15,

    // Level15 represents size range is from 64G to 128G.
    Level16,

    // Level16 represents size range is from 128G to 256G.
    Level17,

    // Level17 represents size range is from 256G to 512G.
    Level18,

    // Level18 represents size range is from 512G to 1T.
    Level19,

    // Level20 represents size is greater than 1T.
    Level20,
}

// TaskSize implements the Display trait.
impl std::fmt::Display for TaskSize {
    // fmt formats the TaskSize.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TaskSize::Level0 => write!(f, "0"),
            TaskSize::Level1 => write!(f, "1"),
            TaskSize::Level2 => write!(f, "2"),
            TaskSize::Level3 => write!(f, "3"),
            TaskSize::Level4 => write!(f, "4"),
            TaskSize::Level5 => write!(f, "5"),
            TaskSize::Level6 => write!(f, "6"),
            TaskSize::Level7 => write!(f, "7"),
            TaskSize::Level8 => write!(f, "8"),
            TaskSize::Level9 => write!(f, "9"),
            TaskSize::Level10 => write!(f, "10"),
            TaskSize::Level11 => write!(f, "11"),
            TaskSize::Level12 => write!(f, "12"),
            TaskSize::Level13 => write!(f, "13"),
            TaskSize::Level14 => write!(f, "14"),
            TaskSize::Level15 => write!(f, "15"),
            TaskSize::Level16 => write!(f, "16"),
            TaskSize::Level17 => write!(f, "17"),
            TaskSize::Level18 => write!(f, "18"),
            TaskSize::Level19 => write!(f, "19"),
            TaskSize::Level20 => write!(f, "20"),
        }
    }
}

// TaskSize implements the TaskSize.
impl TaskSize {
    // calculate_size_level calculates the size level according to the size.
    pub fn calculate_size_level(size: u64) -> Self {
        match size {
            0 => TaskSize::Level0,
            size if size < 1024 * 1024 => TaskSize::Level1,
            size if size < 4 * 1024 * 1024 => TaskSize::Level2,
            size if size < 8 * 1024 * 1024 => TaskSize::Level3,
            size if size < 16 * 1024 * 1024 => TaskSize::Level4,
            size if size < 32 * 1024 * 1024 => TaskSize::Level5,
            size if size < 64 * 1024 * 1024 => TaskSize::Level6,
            size if size < 128 * 1024 * 1024 => TaskSize::Level7,
            size if size < 256 * 1024 * 1024 => TaskSize::Level8,
            size if size < 512 * 1024 * 1024 => TaskSize::Level9,
            size if size < 1024 * 1024 * 1024 => TaskSize::Level10,
            size if size < 4 * 1024 * 1024 * 1024 => TaskSize::Level11,
            size if size < 8 * 1024 * 1024 * 1024 => TaskSize::Level12,
            size if size < 16 * 1024 * 1024 * 1024 => TaskSize::Level13,
            size if size < 32 * 1024 * 1024 * 1024 => TaskSize::Level14,
            size if size < 64 * 1024 * 1024 * 1024 => TaskSize::Level15,
            size if size < 128 * 1024 * 1024 * 1024 => TaskSize::Level16,
            size if size < 256 * 1024 * 1024 * 1024 => TaskSize::Level17,
            size if size < 512 * 1024 * 1024 * 1024 => TaskSize::Level18,
            size if size < 1024 * 1024 * 1024 * 1024 => TaskSize::Level19,
            _ => TaskSize::Level20,
        }
    }
}

// collect_upload_task_started_metrics collects the upload task started metrics.
pub fn collect_upload_task_started_metrics(typ: i32, tag: &str, app: &str) {
    UPLOAD_TASK_COUNT
        .with_label_values(&[typ.to_string().as_str(), tag, app])
        .inc();

    CONCURRENT_UPLOAD_TASK_GAUGE
        .with_label_values(&[typ.to_string().as_str(), tag, app])
        .inc();
}

// collect_upload_task_finished_metrics collects the upload task finished metrics.
pub fn collect_upload_task_finished_metrics(
    typ: i32,
    tag: &str,
    app: &str,
    content_length: u64,
    cost: Duration,
) {
    let task_size = TaskSize::calculate_size_level(content_length);

    // Collect the slow upload Level1 task for analysis.
    if task_size == TaskSize::Level1 && cost > UPLOAD_TASK_LEVEL1_DURATION_THRESHOLD {
        warn!(
            "upload task cost is too long: {}ms {}bytes",
            cost.as_millis(),
            content_length,
        );
    }

    UPLOAD_TASK_DURATION
        .with_label_values(&[typ.to_string().as_str(), task_size.to_string().as_str()])
        .observe(cost.as_millis() as f64);

    CONCURRENT_UPLOAD_TASK_GAUGE
        .with_label_values(&[typ.to_string().as_str(), tag, app])
        .dec();
}

// collect_upload_task_failure_metrics collects the upload task failure metrics.
pub fn collect_upload_task_failure_metrics(typ: i32, tag: &str, app: &str) {
    UPLOAD_TASK_FAILURE_COUNT
        .with_label_values(&[typ.to_string().as_str(), tag, app])
        .inc();

    CONCURRENT_UPLOAD_TASK_GAUGE
        .with_label_values(&[typ.to_string().as_str(), tag, app])
        .dec();
}

// collect_download_task_started_metrics collects the download task started metrics.
pub fn collect_download_task_started_metrics(typ: i32, tag: &str, app: &str, priority: &str) {
    DOWNLOAD_TASK_COUNT
        .with_label_values(&[typ.to_string().as_str(), tag, app, priority])
        .inc();

    CONCURRENT_DOWNLOAD_TASK_GAUGE
        .with_label_values(&[typ.to_string().as_str(), tag, app, priority])
        .inc();
}

// collect_download_task_finished_metrics collects the download task finished metrics.
pub fn collect_download_task_finished_metrics(
    typ: i32,
    tag: &str,
    app: &str,
    priority: &str,
    content_length: u64,
    range: Option<Range>,
    cost: Duration,
) {
    let size = match range {
        Some(range) => range.length,
        None => content_length,
    };

    let task_size = TaskSize::calculate_size_level(size);

    // Nydus will request the small range of the file, so the download task duration
    // should be short. Collect the slow download Level1 task for analysis.
    if task_size == TaskSize::Level1 && cost > DOWNLOAD_TASK_LEVEL1_DURATION_THRESHOLD {
        warn!(
            "download task cost is too long: {}ms {}bytes",
            cost.as_millis(),
            size,
        );
    }

    DOWNLOAD_TASK_DURATION
        .with_label_values(&[typ.to_string().as_str(), task_size.to_string().as_str()])
        .observe(cost.as_millis() as f64);

    CONCURRENT_DOWNLOAD_TASK_GAUGE
        .with_label_values(&[typ.to_string().as_str(), tag, app, priority])
        .dec();
}

// collect_download_task_failure_metrics collects the download task failure metrics.
pub fn collect_download_task_failure_metrics(typ: i32, tag: &str, app: &str, priority: &str) {
    DOWNLOAD_TASK_FAILURE_COUNT
        .with_label_values(&[typ.to_string().as_str(), tag, app, priority])
        .inc();

    CONCURRENT_DOWNLOAD_TASK_GAUGE
        .with_label_values(&[typ.to_string().as_str(), tag, app, priority])
        .dec();
}

// collect_prefetch_task_started_metrics collects the prefetch task started metrics.
pub fn collect_prefetch_task_started_metrics(typ: i32, tag: &str, app: &str, priority: &str) {
    PREFETCH_TASK_COUNT
        .with_label_values(&[typ.to_string().as_str(), tag, app, priority])
        .inc();
}

// collect_prefetch_task_failure_metrics collects the prefetch task failure metrics.
pub fn collect_prefetch_task_failure_metrics(typ: i32, tag: &str, app: &str, priority: &str) {
    PREFETCH_TASK_FAILURE_COUNT
        .with_label_values(&[typ.to_string().as_str(), tag, app, priority])
        .inc();
}

// collect_download_piece_traffic_metrics collects the download piece traffic metrics.
pub fn collect_download_piece_traffic_metrics(typ: &TrafficType, task_type: i32, length: u64) {
    DOWNLOAD_TRAFFIC
        .with_label_values(&[typ.as_str_name(), task_type.to_string().as_str()])
        .inc_by(length);
}

// collect_upload_piece_started_metrics collects the upload piece started metrics.
pub fn collect_upload_piece_started_metrics() {
    CONCURRENT_UPLOAD_PIECE_GAUGE.with_label_values(&[]).inc();
}

// collect_upload_piece_finished_metrics collects the upload piece finished metrics.
pub fn collect_upload_piece_finished_metrics() {
    CONCURRENT_UPLOAD_PIECE_GAUGE.with_label_values(&[]).dec();
}

// collect_upload_piece_traffic_metrics collects the upload piece traffic metrics.
pub fn collect_upload_piece_traffic_metrics(task_type: i32, length: u64) {
    UPLOAD_TRAFFIC
        .with_label_values(&[task_type.to_string().as_str()])
        .inc_by(length);
}

// collect_upload_piece_failure_metrics collects the upload piece failure metrics.
pub fn collect_upload_piece_failure_metrics() {
    CONCURRENT_UPLOAD_PIECE_GAUGE.with_label_values(&[]).dec();
}

// collect_proxy_request_started_metrics collects the proxy request started metrics.
pub fn collect_proxy_request_started_metrics() {
    PROXY_REQUSET_COUNT.with_label_values(&[]).inc();
}

// collect_proxy_request_failure_metrics collects the proxy request failure metrics.
pub fn collect_proxy_request_failure_metrics() {
    PROXY_REQUSET_FAILURE_COUNT.with_label_values(&[]).inc();
}

// collect_stat_task_started_metrics collects the stat task started metrics.
pub fn collect_stat_task_started_metrics(typ: i32) {
    STAT_TASK_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

// collect_stat_task_failure_metrics collects the stat task failure metrics.
pub fn collect_stat_task_failure_metrics(typ: i32) {
    STAT_TASK_FAILURE_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

// collect_delete_task_started_metrics collects the delete task started metrics.
pub fn collect_delete_task_started_metrics(typ: i32) {
    DELETE_TASK_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

// collect_delete_task_failure_metrics collects the delete task failure metrics.
pub fn collect_delete_task_failure_metrics(typ: i32) {
    DELETE_TASK_FAILURE_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

// collect_delete_host_started_metrics collects the delete host started metrics.
pub fn collect_delete_host_started_metrics() {
    DELETE_HOST_COUNT.with_label_values(&[]).inc();
}

// collect_delete_host_failure_metrics collects the delete host failure metrics.
pub fn collect_delete_host_failure_metrics() {
    DELETE_HOST_FAILURE_COUNT.with_label_values(&[]).inc();
}

// Metrics is the metrics server.
#[derive(Debug)]
pub struct Metrics {
    // addr is the address of the metrics server.
    addr: SocketAddr,

    // shutdown is used to shutdown the metrics server.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the metrics server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

// Metrics implements the metrics server.
impl Metrics {
    // new creates a new Metrics.
    pub fn new(
        addr: SocketAddr,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            addr,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    // run starts the metrics server.
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Register custom metrics.
        self.register_custom_metrics();

        // VERSION_GAUGE sets the version info of the service.
        let build_timestamp = env!("BUILD_TIMESTAMP");
        let build_date_time =
            DateTime::from_timestamp(build_timestamp.parse::<i64>().unwrap(), 0).unwrap();
        VERSION_GAUGE
            .get_metric_with_label_values(&[
                env!("GIT_VERSION"),
                env!("GIT_COMMIT"),
                env!("BUILD_PLATFORM"),
                build_date_time.to_string().as_str(),
            ])
            .unwrap()
            .set(1);

        // Create the metrics route.
        let metrics_route = warp::path!("metrics")
            .and(warp::get())
            .and(warp::path::end())
            .and_then(Self::metrics_handler);

        // Start the metrics server and wait for it to finish.
        info!("metrics server listening on {}", self.addr);
        tokio::select! {
            _ = warp::serve(metrics_route).run(self.addr) => {
                // Metrics server ended.
                info!("metrics server ended");
            }
            _ = shutdown.recv() => {
                // Metrics server shutting down with signals.
                info!("metrics server shutting down");
            }
        }
    }

    // register_custom_metrics registers all custom metrics.
    fn register_custom_metrics(&self) {
        REGISTRY
            .register(Box::new(VERSION_GAUGE.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(DOWNLOAD_TASK_COUNT.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(DOWNLOAD_TASK_FAILURE_COUNT.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(PREFETCH_TASK_COUNT.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(PREFETCH_TASK_FAILURE_COUNT.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(CONCURRENT_DOWNLOAD_TASK_GAUGE.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(CONCURRENT_UPLOAD_PIECE_GAUGE.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(DOWNLOAD_TRAFFIC.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(UPLOAD_TRAFFIC.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(DOWNLOAD_TASK_DURATION.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(PROXY_REQUSET_COUNT.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(PROXY_REQUSET_FAILURE_COUNT.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(STAT_TASK_COUNT.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(STAT_TASK_FAILURE_COUNT.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(DELETE_TASK_COUNT.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(DELETE_TASK_FAILURE_COUNT.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(DELETE_HOST_COUNT.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(DELETE_HOST_FAILURE_COUNT.clone()))
            .expect("metric can be registered");
    }

    // metrics_handler handles the metrics request.
    async fn metrics_handler() -> Result<impl Reply, Rejection> {
        let encoder = TextEncoder::new();

        // Encode custom metrics.
        let mut buf = Vec::new();
        if let Err(err) = encoder.encode(&REGISTRY.gather(), &mut buf) {
            error!("could not encode custom metrics: {}", err);
        };

        let mut res = match String::from_utf8(buf.clone()) {
            Ok(v) => v,
            Err(err) => {
                error!("custom metrics could not be from_utf8'd: {}", err);
                String::default()
            }
        };
        buf.clear();

        // Encode prometheus metrics.
        let mut buf = Vec::new();
        if let Err(err) = encoder.encode(&gather(), &mut buf) {
            error!("could not encode prometheus metrics: {}", err);
        };

        let res_custom = match String::from_utf8(buf.clone()) {
            Ok(v) => v,
            Err(err) => {
                error!("prometheus metrics could not be from_utf8'd: {}", err);
                String::default()
            }
        };
        buf.clear();

        res.push_str(&res_custom);
        Ok(res)
    }
}
