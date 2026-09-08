/*
 *     Copyright 2025 The Dragonfly Authors
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

use bytes::Bytes;
use dragonfly_api::common::v2::{Range, TrafficType};
use dragonfly_client_config::{
    dfdaemon::Config, BUILD_PLATFORM, CARGO_PKG_VERSION, GIT_COMMIT_DATE, GIT_COMMIT_SHORT_HASH,
};
use dragonfly_client_util::shutdown;
use http_body_util::Full;
use hyper::server::conn::http1::Builder as ServerBuilder;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prometheus::{
    exponential_buckets, gather, Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec,
    Opts, Registry, TextEncoder,
};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{error, info, instrument, warn};

/// The threshold of small (Level0–Level2, under 4MiB) download task
/// duration for recording slow download task.
const DOWNLOAD_SMALL_TASK_DURATION_THRESHOLD: Duration = Duration::from_millis(500);

/// The threshold of small (Level0 & Level1, under 1MiB) upload task
/// duration for recording slow upload task.
const UPLOAD_SMALL_TASK_DURATION_THRESHOLD: Duration = Duration::from_millis(500);

/// Used to register all metrics.
pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

/// Used to record the version info of the service.
pub static VERSION_GAUGE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new("version", "Version info of the service.")
            .namespace(dragonfly_client_config::SERVICE_NAME)
            .subsystem(dragonfly_client_config::NAME),
        &["git_version", "git_commit", "platform", "build_time"],
    )
    .expect("metric can be created")
});

/// Used to count the number of upload tasks.
pub static UPLOAD_TASK_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "upload_task_total",
            "Counter of the number of the upload task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type", "tag", "app"],
    )
    .expect("metric can be created")
});

/// Used to count the failed number of upload tasks.
pub static UPLOAD_TASK_FAILURE_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "upload_task_failure_total",
            "Counter of the number of failed of the upload task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type", "tag", "app"],
    )
    .expect("metric can be created")
});

/// Used to gauge the number of concurrent upload tasks.
pub static CONCURRENT_UPLOAD_TASK_GAUGE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "concurrent_upload_task_total",
            "Gauge of the number of concurrent of the upload task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type", "tag", "app"],
    )
    .expect("metric can be created")
});

/// Used to record the upload task duration.
pub static UPLOAD_TASK_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "upload_task_duration_milliseconds",
            "Histogram of the upload task duration.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME)
        .buckets(exponential_buckets(1.0, 2.0, 24).unwrap()),
        &["task_type", "task_size_level"],
    )
    .expect("metric can be created")
});

/// Used to count the number of download tasks.
pub static DOWNLOAD_TASK_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "download_task_total",
            "Counter of the number of the download task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type", "tag", "app", "priority"],
    )
    .expect("metric can be created")
});

/// Used to count the failed number of download tasks.
pub static DOWNLOAD_TASK_FAILURE_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "download_task_failure_total",
            "Counter of the number of failed of the download task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type", "tag", "app", "priority"],
    )
    .expect("metric can be created")
});

/// Used to count the number of prefetch tasks.
pub static PREFETCH_TASK_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "prefetch_task_total",
            "Counter of the number of the prefetch task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type", "tag", "app", "priority"],
    )
    .expect("metric can be created")
});

/// Used to count the failed number of prefetch tasks.
pub static PREFETCH_TASK_FAILURE_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "prefetch_task_failure_total",
            "Counter of the number of failed of the prefetch task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type", "tag", "app", "priority"],
    )
    .expect("metric can be created")
});

/// Used to gauge the number of concurrent download tasks.
pub static CONCURRENT_DOWNLOAD_TASK_GAUGE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "concurrent_download_task_total",
            "Gauge of the number of concurrent of the download task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type", "tag", "app", "priority"],
    )
    .expect("metric can be created")
});

/// Used to gauge the number of concurrent upload pieces.
pub static CONCURRENT_UPLOAD_PIECE_GAUGE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "concurrent_upload_piece_total",
            "Gauge of the number of concurrent of the upload piece.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &[],
    )
    .expect("metric can be created")
});

/// Used to count the download traffic.
pub static DOWNLOAD_TRAFFIC: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "download_traffic",
            "Counter of the number of the download traffic.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to record the download piece duration.
pub static DOWNLOAD_PIECE_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "download_piece_duration_milliseconds",
            "Histogram of the download piece duration.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME)
        .buckets(exponential_buckets(1.0, 2.0, 24).unwrap()),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the upload traffic.
pub static UPLOAD_TRAFFIC: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "upload_traffic",
            "Counter of the number of the upload traffic.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &[],
    )
    .expect("metric can be created")
});

/// Used to record the download task duration.
pub static DOWNLOAD_TASK_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "download_task_duration_milliseconds",
            "Histogram of the download task duration.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME)
        .buckets(exponential_buckets(1.0, 2.0, 24).unwrap()),
        &["task_type", "task_size_level"],
    )
    .expect("metric can be created")
});

/// Used to count the number of backend requset.
pub static BACKEND_REQUEST_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "backend_request_total",
            "Counter of the number of the backend request.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["scheme", "method"],
    )
    .expect("metric can be created")
});

/// Used to count the failed number of backend request.
pub static BACKEND_REQUEST_FAILURE_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "backend_request_failure_total",
            "Counter of the number of failed of the backend request.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["scheme", "method"],
    )
    .expect("metric can be created")
});

/// Used to record the backend request duration.
pub static BACKEND_REQUEST_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "backend_request_duration_milliseconds",
            "Histogram of the backend request duration.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME)
        .buckets(exponential_buckets(1.0, 2.0, 24).unwrap()),
        &["scheme", "method"],
    )
    .expect("metric can be created")
});

/// Used to count the number of proxy requset.
pub static PROXY_REQUEST_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "proxy_request_total",
            "Counter of the number of the proxy request.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &[],
    )
    .expect("metric can be created")
});

/// Used to count the failed number of proxy request.
pub static PROXY_REQUEST_FAILURE_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "proxy_request_failure_total",
            "Counter of the number of failed of the proxy request.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &[],
    )
    .expect("metric can be created")
});

/// Used to count the number of proxy requset via dfdaemon.
pub static PROXY_REQUEST_VIA_DFDAEMON_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "proxy_request_via_dfdaemon_total",
            "Counter of the number of the proxy request via dfdaemon.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &[],
    )
    .expect("metric can be created")
});

/// Used to count the number of update tasks.
pub static UPDATE_TASK_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "update_task_total",
            "Counter of the number of the update task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the failed number of update tasks.
pub static UPDATE_TASK_FAILURE_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "update_task_failure_total",
            "Counter of the number of failed of the update task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the number of stat tasks.
pub static STAT_TASK_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("stat_task_total", "Counter of the number of the stat task.")
            .namespace(dragonfly_client_config::SERVICE_NAME)
            .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the failed number of stat tasks.
pub static STAT_TASK_FAILURE_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "stat_task_failure_total",
            "Counter of the number of failed of the stat task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the number of stat tasks.
pub static STAT_LOCAL_TASK_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "stat_local_task_total",
            "Counter of the number of the stat local task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the failed number of stat tasks.
pub static STAT_LOCAL_TASK_FAILURE_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "stat_local_task_failure_total",
            "Counter of the number of failed of the stat local task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the number of list tasks.
pub static LIST_LOCAL_TASKS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "list_local_tasks_total",
            "Counter of the number of the list tasks.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the failed number of list tasks.
pub static LIST_LOCAL_TASKS_FAILURE_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "list_tasks_failure_total",
            "Counter of the number of failed of the list tasks.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the number of list task entries.
pub static LIST_TASK_ENTRIES_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "list_task_entries_total",
            "Counter of the number of the list task entries.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the failed number of list task entries.
pub static LIST_TASK_ENTRIES_FAILURE_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "list_task_entries_failure_total",
            "Counter of the number of failed of the list task entries.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the number of delete tasks.
pub static DELETE_TASK_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "delete_task_total",
            "Counter of the number of the delete task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the failed number of delete tasks.
pub static DELETE_TASK_FAILURE_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "delete_task_failure_total",
            "Counter of the number of failed of the delete task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the number of delete local tasks.
pub static DELETE_LOCAL_TASK_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "delete_local_task_total",
            "Counter of the number of the delete local task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the failed number of delete local tasks.
pub static DELETE_LOCAL_TASK_FAILURE_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "delete_local_task_failure_total",
            "Counter of the number of failed of the delete local task.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count the number of delete host.
pub static DELETE_HOST_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "delete_host_total",
            "Counter of the number of the delete host.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &[],
    )
    .expect("metric can be created")
});

/// Used to count the failed number of delete host.
pub static DELETE_HOST_FAILURE_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "delete_host_failure_total",
            "Counter of the number of failed of the delete host.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &[],
    )
    .expect("metric can be created")
});

/// Used to count of the disk space.
pub static DISK_SPACE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new("disk_space_total", "Gauge of the disk space in bytes")
            .namespace(dragonfly_client_config::SERVICE_NAME)
            .subsystem(dragonfly_client_config::NAME),
        &[],
    )
    .expect("metric can be created")
});

/// Used to count of the disk usage space.
pub static DISK_USAGE_SPACE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "disk_usage_space_total",
            "Gauge of the disk usage space in bytes",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &[],
    )
    .expect("metric can be created")
});

/// Used to count of the download task blocked.
pub static DOWNLOAD_TASK_BLOCKED_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "download_task_blocked_total",
            "Counter of the number of download task blocked.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Used to count of the upload task blocked.
pub static UPLOAD_TASK_BLOCKED_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "upload_task_blocked_total",
            "Counter of the number of upload task blocked.",
        )
        .namespace(dragonfly_client_config::SERVICE_NAME)
        .subsystem(dragonfly_client_config::NAME),
        &["type"],
    )
    .expect("metric can be created")
});

/// Registers all custom metrics.
fn register_custom_metrics() {
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
        .register(Box::new(DOWNLOAD_PIECE_DURATION.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(UPLOAD_TRAFFIC.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(DOWNLOAD_TASK_DURATION.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(BACKEND_REQUEST_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(BACKEND_REQUEST_FAILURE_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(BACKEND_REQUEST_DURATION.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(PROXY_REQUEST_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(PROXY_REQUEST_FAILURE_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(PROXY_REQUEST_VIA_DFDAEMON_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(UPDATE_TASK_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(UPDATE_TASK_FAILURE_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(STAT_TASK_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(STAT_TASK_FAILURE_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(LIST_LOCAL_TASKS_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(LIST_LOCAL_TASKS_FAILURE_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(LIST_TASK_ENTRIES_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(LIST_TASK_ENTRIES_FAILURE_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(DELETE_TASK_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(DELETE_TASK_FAILURE_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(DELETE_LOCAL_TASK_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(DELETE_LOCAL_TASK_FAILURE_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(DELETE_HOST_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(DELETE_HOST_FAILURE_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(DISK_SPACE.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(DISK_USAGE_SPACE.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(DOWNLOAD_TASK_BLOCKED_COUNT.clone()))
        .expect("metric can be registered");

    REGISTRY
        .register(Box::new(UPLOAD_TASK_BLOCKED_COUNT.clone()))
        .expect("metric can be registered");
}

/// Resets all custom metrics.
fn reset_custom_metrics() {
    VERSION_GAUGE.reset();
    DOWNLOAD_TASK_COUNT.reset();
    DOWNLOAD_TASK_FAILURE_COUNT.reset();
    PREFETCH_TASK_COUNT.reset();
    PREFETCH_TASK_FAILURE_COUNT.reset();
    CONCURRENT_DOWNLOAD_TASK_GAUGE.reset();
    CONCURRENT_UPLOAD_PIECE_GAUGE.reset();
    DOWNLOAD_TRAFFIC.reset();
    DOWNLOAD_PIECE_DURATION.reset();
    UPLOAD_TRAFFIC.reset();
    DOWNLOAD_TASK_DURATION.reset();
    BACKEND_REQUEST_COUNT.reset();
    BACKEND_REQUEST_FAILURE_COUNT.reset();
    BACKEND_REQUEST_DURATION.reset();
    PROXY_REQUEST_COUNT.reset();
    PROXY_REQUEST_FAILURE_COUNT.reset();
    PROXY_REQUEST_VIA_DFDAEMON_COUNT.reset();
    UPDATE_TASK_COUNT.reset();
    UPDATE_TASK_FAILURE_COUNT.reset();
    STAT_TASK_COUNT.reset();
    STAT_TASK_FAILURE_COUNT.reset();
    LIST_LOCAL_TASKS_COUNT.reset();
    LIST_LOCAL_TASKS_FAILURE_COUNT.reset();
    LIST_TASK_ENTRIES_COUNT.reset();
    LIST_TASK_ENTRIES_FAILURE_COUNT.reset();
    DELETE_TASK_COUNT.reset();
    DELETE_TASK_FAILURE_COUNT.reset();
    DELETE_LOCAL_TASK_COUNT.reset();
    DELETE_LOCAL_TASK_FAILURE_COUNT.reset();
    DELETE_HOST_COUNT.reset();
    DELETE_HOST_FAILURE_COUNT.reset();
    DISK_SPACE.reset();
    DISK_USAGE_SPACE.reset();
}

/// Represents the size of the task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskSize {
    /// Represents unknown size.
    Level0,

    /// Represents size range is from 0 to 1M.
    Level1,

    /// Represents size range is from 1M to 4M.
    Level2,

    /// Represents size range is from 4M to 8M.
    Level3,

    /// Represents size range is from 8M to 16M.
    Level4,

    /// Represents size range is from 16M to 32M.
    Level5,

    /// Represents size range is from 32M to 64M.
    Level6,

    /// Represents size range is from 64M to 128M.
    Level7,

    /// Represents size range is from 128M to 256M.
    Level8,

    /// Represents size range is from 256M to 512M.
    Level9,

    /// Represents size range is from 512M to 1G.
    Level10,

    /// Represents size range is from 1G to 4G.
    Level11,

    /// Represents size range is from 4G to 8G.
    Level12,

    /// Represents size range is from 8G to 16G.
    Level13,

    /// Represents size range is from 16G to 32G.
    Level14,

    /// Represents size range is from 32G to 64G.
    Level15,

    /// Represents size range is from 64G to 128G.
    Level16,

    /// Represents size range is from 128G to 256G.
    Level17,

    /// Represents size range is from 256G to 512G.
    Level18,

    /// Represents size range is from 512G to 1T.
    Level19,

    /// Represents size is greater than 1T.
    Level20,
}

/// Implements the Display trait.
impl std::fmt::Display for TaskSize {
    /// fmt formats the TaskSize.
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

/// Implements the TaskSize.
impl TaskSize {
    /// Calculates the size level according to the size.
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

/// Collects the upload task started metrics.
pub fn collect_upload_task_started_metrics(typ: i32, tag: &str, app: &str) {
    let typ = typ.to_string();

    UPLOAD_TASK_COUNT.with_label_values(&[&typ, tag, app]).inc();

    CONCURRENT_UPLOAD_TASK_GAUGE
        .with_label_values(&[&typ, tag, app])
        .inc();
}

/// Collects the upload task finished metrics.
#[instrument(skip_all)]
pub fn collect_upload_task_finished_metrics(
    typ: i32,
    tag: &str,
    app: &str,
    content_length: u64,
    cost: Duration,
) {
    let task_size = TaskSize::calculate_size_level(content_length);

    // Collect the slow upload Level0, Level1 & Level2 task for analysis.
    if matches!(
        task_size,
        TaskSize::Level0 | TaskSize::Level1 | TaskSize::Level2
    ) && cost > UPLOAD_SMALL_TASK_DURATION_THRESHOLD
    {
        warn!(
            "upload task, cost: {:?}, size: {} bytes",
            cost, content_length,
        );
    }

    let typ = typ.to_string();
    let task_size = task_size.to_string();

    UPLOAD_TASK_DURATION
        .with_label_values(&[&typ, &task_size])
        .observe(cost.as_millis() as f64);

    CONCURRENT_UPLOAD_TASK_GAUGE
        .with_label_values(&[&typ, tag, app])
        .dec();
}

/// Collects the upload task failure metrics.
pub fn collect_upload_task_failure_metrics(typ: i32, tag: &str, app: &str) {
    let typ = typ.to_string();

    UPLOAD_TASK_FAILURE_COUNT
        .with_label_values(&[&typ, tag, app])
        .inc();

    CONCURRENT_UPLOAD_TASK_GAUGE
        .with_label_values(&[&typ, tag, app])
        .dec();
}

/// Collects the download task started metrics.
pub fn collect_download_task_started_metrics(typ: i32, tag: &str, app: &str, priority: &str) {
    let typ = typ.to_string();

    DOWNLOAD_TASK_COUNT
        .with_label_values(&[&typ, tag, app, priority])
        .inc();

    CONCURRENT_DOWNLOAD_TASK_GAUGE
        .with_label_values(&[&typ, tag, app, priority])
        .inc();
}

/// Collects the download task finished metrics.
#[instrument(skip_all)]
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
    // should be short. Collect the slow download Level0 & Level1 task for analysis.
    if matches!(
        task_size,
        TaskSize::Level0 | TaskSize::Level1 | TaskSize::Level2
    ) && cost > DOWNLOAD_SMALL_TASK_DURATION_THRESHOLD
    {
        warn!("download task, cost: {:?}, size: {} bytes", cost, size);
    }

    let typ = typ.to_string();
    let task_size = task_size.to_string();

    DOWNLOAD_TASK_DURATION
        .with_label_values(&[&typ, &task_size])
        .observe(cost.as_millis() as f64);

    CONCURRENT_DOWNLOAD_TASK_GAUGE
        .with_label_values(&[&typ, tag, app, priority])
        .dec();
}

/// Collects the download task failure metrics.
pub fn collect_download_task_failure_metrics(typ: i32, tag: &str, app: &str, priority: &str) {
    let typ = typ.to_string();

    DOWNLOAD_TASK_FAILURE_COUNT
        .with_label_values(&[&typ, tag, app, priority])
        .inc();

    CONCURRENT_DOWNLOAD_TASK_GAUGE
        .with_label_values(&[&typ, tag, app, priority])
        .dec();
}

/// Collects the prefetch task started metrics.
pub fn collect_prefetch_task_started_metrics(typ: i32, tag: &str, app: &str, priority: &str) {
    PREFETCH_TASK_COUNT
        .with_label_values(&[typ.to_string().as_str(), tag, app, priority])
        .inc();
}

/// Collects the prefetch task failure metrics.
pub fn collect_prefetch_task_failure_metrics(typ: i32, tag: &str, app: &str, priority: &str) {
    PREFETCH_TASK_FAILURE_COUNT
        .with_label_values(&[typ.to_string().as_str(), tag, app, priority])
        .inc();
}

/// Collects the download piece traffic metrics.
pub fn collect_download_piece_traffic_metrics(typ: &TrafficType, length: u64) {
    DOWNLOAD_TRAFFIC
        .with_label_values(&[typ.as_str_name()])
        .inc_by(length);
}

/// Collects the download piece duration metrics.
pub fn collect_download_piece_duration_metrics(typ: &TrafficType, cost: Duration) {
    DOWNLOAD_PIECE_DURATION
        .with_label_values(&[typ.as_str_name()])
        .observe(cost.as_millis() as f64);
}

/// Collects the upload piece started metrics.
pub fn collect_upload_piece_started_metrics() {
    CONCURRENT_UPLOAD_PIECE_GAUGE.with_label_values(&[]).inc();
}

/// Collects the upload piece finished metrics.
pub fn collect_upload_piece_finished_metrics() {
    CONCURRENT_UPLOAD_PIECE_GAUGE.with_label_values(&[]).dec();
}

/// Collects the upload piece traffic metrics.
pub fn collect_upload_piece_traffic_metrics(length: u64) {
    UPLOAD_TRAFFIC.with_label_values(&[]).inc_by(length);
}

/// Collects the upload piece failure metrics.
pub fn collect_upload_piece_failure_metrics() {
    CONCURRENT_UPLOAD_PIECE_GAUGE.with_label_values(&[]).dec();
}

/// Collects the backend request started metrics.
pub fn collect_backend_request_started_metrics(scheme: &str, method: &str) {
    BACKEND_REQUEST_COUNT
        .with_label_values(&[scheme, method])
        .inc();
}

/// Collects the backend request failure metrics.
pub fn collect_backend_request_failure_metrics(scheme: &str, method: &str) {
    BACKEND_REQUEST_FAILURE_COUNT
        .with_label_values(&[scheme, method])
        .inc();
}

/// Collects the backend request finished metrics.
pub fn collect_backend_request_finished_metrics(scheme: &str, method: &str, cost: Duration) {
    BACKEND_REQUEST_DURATION
        .with_label_values(&[scheme, method])
        .observe(cost.as_millis() as f64);
}

/// Collects the proxy request started metrics.
pub fn collect_proxy_request_started_metrics() {
    PROXY_REQUEST_COUNT.with_label_values(&[]).inc();
}

/// Collects the proxy request failure metrics.
pub fn collect_proxy_request_failure_metrics() {
    PROXY_REQUEST_FAILURE_COUNT.with_label_values(&[]).inc();
}

/// Collects the proxy request via dfdaemon metrics.
pub fn collect_proxy_request_via_dfdaemon_metrics() {
    PROXY_REQUEST_VIA_DFDAEMON_COUNT
        .with_label_values(&[])
        .inc();
}

/// Collects the update task started metrics.
pub fn collect_update_task_started_metrics(typ: i32) {
    UPDATE_TASK_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the update task failure metrics.
pub fn collect_update_task_failure_metrics(typ: i32) {
    UPDATE_TASK_FAILURE_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the stat task started metrics.
pub fn collect_stat_task_started_metrics(typ: i32) {
    STAT_TASK_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the stat task failure metrics.
pub fn collect_stat_task_failure_metrics(typ: i32) {
    STAT_TASK_FAILURE_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the stat local task started metrics.
pub fn collect_stat_local_task_started_metrics(typ: i32) {
    STAT_LOCAL_TASK_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the stat local task failure metrics.
pub fn collect_stat_local_task_failure_metrics(typ: i32) {
    STAT_LOCAL_TASK_FAILURE_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the list task entries started metrics.
pub fn collect_list_local_tasks_started_metrics(typ: i32) {
    LIST_LOCAL_TASKS_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the list task entries failure metrics.
pub fn collect_list_local_tasks_failure_metrics(typ: i32) {
    LIST_LOCAL_TASKS_FAILURE_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the list task entries started metrics.
pub fn collect_list_task_entries_started_metrics(typ: i32) {
    LIST_TASK_ENTRIES_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the list task entries failure metrics.
pub fn collect_list_task_entries_failure_metrics(typ: i32) {
    LIST_TASK_ENTRIES_FAILURE_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the delete task started metrics.
pub fn collect_delete_task_started_metrics(typ: i32) {
    DELETE_TASK_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the delete task failure metrics.
pub fn collect_delete_task_failure_metrics(typ: i32) {
    DELETE_TASK_FAILURE_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the delete local task started metrics.
pub fn collect_delete_local_task_started_metrics(typ: i32) {
    DELETE_LOCAL_TASK_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the delete local task failure metrics.
pub fn collect_delete_local_task_failure_metrics(typ: i32) {
    DELETE_LOCAL_TASK_FAILURE_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the delete host started metrics.
pub fn collect_delete_host_started_metrics() {
    DELETE_HOST_COUNT.with_label_values(&[]).inc();
}

/// Collects the delete host failure metrics.
pub fn collect_delete_host_failure_metrics() {
    DELETE_HOST_FAILURE_COUNT.with_label_values(&[]).inc();
}

/// Collects the disk metrics.
pub fn collect_disk_metrics(path: &Path) {
    // Collect disk space metrics.
    let stats = match fs2::statvfs(path) {
        Ok(stats) => stats,
        Err(err) => {
            error!("failed to get disk space: {}", err);
            return;
        }
    };

    let total_space = stats.total_space();
    let available_space = stats.available_space();
    let usage_space = total_space - available_space;
    DISK_SPACE.with_label_values(&[]).set(total_space as i64);
    DISK_USAGE_SPACE
        .with_label_values(&[])
        .set(usage_space as i64);
}

/// Collects the download task blocked metrics.
pub fn collect_download_task_blocked_metrics(typ: i32) {
    DOWNLOAD_TASK_BLOCKED_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// Collects the upload task blocked metrics.
pub fn collect_upload_task_blocked_metrics(typ: i32) {
    UPLOAD_TASK_BLOCKED_COUNT
        .with_label_values(&[typ.to_string().as_str()])
        .inc();
}

/// The metrics server.
#[derive(Debug)]
pub struct Metrics {
    /// The configuration of the dfdaemon.
    config: Arc<Config>,

    /// Used to shutdown the metrics server.
    shutdown: shutdown::Shutdown,

    /// Used to notify the metrics server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// Implements the metrics server.
impl Metrics {
    /// Creates a new Metrics.
    pub fn new(
        config: Arc<Config>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            config,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// Starts the metrics server.
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Register custom metrics.
        register_custom_metrics();

        // VERSION_GAUGE sets the version info of the service.
        VERSION_GAUGE
            .get_metric_with_label_values(&[
                CARGO_PKG_VERSION,
                GIT_COMMIT_SHORT_HASH,
                BUILD_PLATFORM,
                GIT_COMMIT_DATE,
            ])
            .unwrap()
            .set(1);

        // Clone the config.
        let config = self.config.clone();

        // Create the metrics server address.
        let addr = SocketAddr::new(
            self.config.metrics.server.ip.unwrap(),
            self.config.metrics.server.port,
        );

        // Start the metrics server and wait for it to finish.
        info!("metrics server listening on {}", addr);
        let listener = TcpListener::bind(addr).await.unwrap();
        loop {
            tokio::select! {
                tcp_accepted = listener.accept() => {
                    let (tcp, remote_address) = match tcp_accepted {
                        Ok(tcp_accepted) => tcp_accepted,
                        Err(err) => {
                            error!("failed to accept connection: {}", err);
                            continue;
                        }
                    };

                    let io = TokioIo::new(tcp);
                    let config = config.clone();
                    tokio::spawn(async move {
                        if let Err(err) = ServerBuilder::new()
                            .serve_connection(
                                io,
                                service_fn(move |request| Self::handler(config.clone(), request)),
                            )
                            .await
                        {
                            error!("failed to serve connection from {}: {}", remote_address, err);
                        }
                    });
                }
                _ = shutdown.recv() => {
                    // Metrics server shutting down with signals.
                    info!("metrics server shutting down");
                    return;
                }
            }
        }
    }

    /// Handles the metrics request.
    #[instrument(skip_all)]
    async fn handler<T>(
        config: Arc<Config>,
        request: Request<T>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        match (request.method(), request.uri().path()) {
            (&Method::GET, "/metrics") => Ok(Response::builder()
                .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                .body(Full::new(Bytes::from(
                    Self::get_metrics_handler(config).await,
                )))
                .unwrap()),
            (&Method::DELETE, "/metrics") => {
                Self::delete_metrics_handler().await;
                Ok(Response::new(Full::default()))
            }
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::default())
                .unwrap()),
        }
    }

    /// Handles the metrics request of getting.
    #[instrument(skip_all)]
    async fn get_metrics_handler(config: Arc<Config>) -> String {
        // Collect the disk space metrics.
        collect_disk_metrics(config.storage.dir.as_path());

        // Encode custom metrics.
        let encoder = TextEncoder::new();
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
        res
    }

    /// Handles the metrics request of deleting.
    #[instrument(skip_all)]
    async fn delete_metrics_handler() {
        reset_custom_metrics();
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::type_complexity)]

    use super::*;
    use dragonfly_client_config::dfdaemon::Storage;

    #[test]
    fn calculate_size_level_maps_bytes_to_levels() {
        let test_cases = vec![
            (0, TaskSize::Level0),
            (1, TaskSize::Level1),
            (512 * 1024, TaskSize::Level1),
            (1024 * 1024 - 1, TaskSize::Level1),
            (1024 * 1024, TaskSize::Level2),
            (2 * 1024 * 1024, TaskSize::Level2),
            (4 * 1024 * 1024 - 1, TaskSize::Level2),
            (4 * 1024 * 1024, TaskSize::Level3),
            (6 * 1024 * 1024, TaskSize::Level3),
            (8 * 1024 * 1024, TaskSize::Level4),
            (16 * 1024 * 1024, TaskSize::Level5),
            (32 * 1024 * 1024, TaskSize::Level6),
            (64 * 1024 * 1024, TaskSize::Level7),
            (128 * 1024 * 1024, TaskSize::Level8),
            (256 * 1024 * 1024, TaskSize::Level9),
            (512 * 1024 * 1024, TaskSize::Level10),
            (1024 * 1024 * 1024, TaskSize::Level11),
            (2 * 1024 * 1024 * 1024, TaskSize::Level11),
            (4 * 1024 * 1024 * 1024, TaskSize::Level12),
            (8 * 1024 * 1024 * 1024, TaskSize::Level13),
            (16 * 1024 * 1024 * 1024, TaskSize::Level14),
            (32 * 1024 * 1024 * 1024, TaskSize::Level15),
            (64 * 1024 * 1024 * 1024, TaskSize::Level16),
            (128 * 1024 * 1024 * 1024, TaskSize::Level17),
            (256 * 1024 * 1024 * 1024, TaskSize::Level18),
            (512 * 1024 * 1024 * 1024, TaskSize::Level19),
            (1024 * 1024 * 1024 * 1024, TaskSize::Level20),
            (2 * 1024 * 1024 * 1024 * 1024, TaskSize::Level20),
        ];

        for (size, expected) in test_cases {
            assert_eq!(TaskSize::calculate_size_level(size), expected);
        }
    }

    #[test]
    fn display_formats_level_as_its_number() {
        let test_cases = vec![
            (TaskSize::Level0, "0"),
            (TaskSize::Level1, "1"),
            (TaskSize::Level2, "2"),
            (TaskSize::Level3, "3"),
            (TaskSize::Level4, "4"),
            (TaskSize::Level5, "5"),
            (TaskSize::Level6, "6"),
            (TaskSize::Level7, "7"),
            (TaskSize::Level8, "8"),
            (TaskSize::Level9, "9"),
            (TaskSize::Level10, "10"),
            (TaskSize::Level11, "11"),
            (TaskSize::Level12, "12"),
            (TaskSize::Level13, "13"),
            (TaskSize::Level14, "14"),
            (TaskSize::Level15, "15"),
            (TaskSize::Level16, "16"),
            (TaskSize::Level17, "17"),
            (TaskSize::Level18, "18"),
            (TaskSize::Level19, "19"),
            (TaskSize::Level20, "20"),
        ];

        for (level, expected) in test_cases {
            assert_eq!(level.to_string(), expected);
        }
    }

    #[test]
    fn upload_task_metrics_follow_started_finished_and_failure() {
        let (tag, app) = ("upload-task-tag", "upload-task-app");
        let labels = ["1", tag, app];
        let samples_before = UPLOAD_TASK_DURATION
            .with_label_values(&["1", "1"])
            .get_sample_count();
        let count_before = UPLOAD_TASK_COUNT.with_label_values(&labels).get();
        let concurrent_before = CONCURRENT_UPLOAD_TASK_GAUGE
            .with_label_values(&labels)
            .get();
        collect_upload_task_started_metrics(1, tag, app);
        assert_eq!(
            UPLOAD_TASK_COUNT.with_label_values(&labels).get(),
            count_before + 1
        );
        assert_eq!(
            CONCURRENT_UPLOAD_TASK_GAUGE
                .with_label_values(&labels)
                .get(),
            concurrent_before + 1
        );

        collect_upload_task_finished_metrics(1, tag, app, 1024, Duration::from_millis(100));
        assert_eq!(
            CONCURRENT_UPLOAD_TASK_GAUGE
                .with_label_values(&labels)
                .get(),
            concurrent_before
        );

        let failure_before = UPLOAD_TASK_FAILURE_COUNT.with_label_values(&labels).get();
        collect_upload_task_started_metrics(1, tag, app);
        collect_upload_task_failure_metrics(1, tag, app);
        assert_eq!(
            UPLOAD_TASK_FAILURE_COUNT.with_label_values(&labels).get(),
            failure_before + 1
        );
        assert_eq!(
            CONCURRENT_UPLOAD_TASK_GAUGE
                .with_label_values(&labels)
                .get(),
            concurrent_before
        );
        assert_eq!(
            UPLOAD_TASK_DURATION
                .with_label_values(&["1", "1"])
                .get_sample_count(),
            samples_before + 1
        );
    }

    #[test]
    fn upload_task_finished_observes_duration_by_size_level() {
        let (tag, app) = ("upload-duration-tag", "upload-duration-app");

        let test_cases = vec![
            (2, 512 * 1024, Duration::from_millis(1000), "1"),
            (3, 8 * 1024 * 1024, Duration::from_millis(100), "4"),
        ];

        for (typ, content_length, cost, expected_level) in test_cases {
            let typ_label = typ.to_string();
            let labels = [typ_label.as_str(), expected_level];
            let samples_before = UPLOAD_TASK_DURATION
                .with_label_values(&labels)
                .get_sample_count();
            collect_upload_task_started_metrics(typ, tag, app);
            collect_upload_task_finished_metrics(typ, tag, app, content_length, cost);
            assert_eq!(
                UPLOAD_TASK_DURATION
                    .with_label_values(&labels)
                    .get_sample_count(),
                samples_before + 1
            );
        }
    }

    #[test]
    fn download_task_metrics_follow_started_finished_and_failure() {
        let (tag, app, priority) = ("download-task-tag", "download-task-app", "5");
        let labels = ["1", tag, app, priority];
        let samples_before = DOWNLOAD_TASK_DURATION
            .with_label_values(&["1", "2"])
            .get_sample_count();
        let count_before = DOWNLOAD_TASK_COUNT.with_label_values(&labels).get();
        let concurrent_before = CONCURRENT_DOWNLOAD_TASK_GAUGE
            .with_label_values(&labels)
            .get();
        collect_download_task_started_metrics(1, tag, app, priority);
        assert_eq!(
            DOWNLOAD_TASK_COUNT.with_label_values(&labels).get(),
            count_before + 1
        );
        assert_eq!(
            CONCURRENT_DOWNLOAD_TASK_GAUGE
                .with_label_values(&labels)
                .get(),
            concurrent_before + 1
        );

        collect_download_task_finished_metrics(
            1,
            tag,
            app,
            priority,
            1024 * 1024,
            None,
            Duration::from_millis(200),
        );
        assert_eq!(
            CONCURRENT_DOWNLOAD_TASK_GAUGE
                .with_label_values(&labels)
                .get(),
            concurrent_before
        );

        let failure_before = DOWNLOAD_TASK_FAILURE_COUNT.with_label_values(&labels).get();
        collect_download_task_started_metrics(1, tag, app, priority);
        collect_download_task_failure_metrics(1, tag, app, priority);
        assert_eq!(
            DOWNLOAD_TASK_FAILURE_COUNT.with_label_values(&labels).get(),
            failure_before + 1
        );
        assert_eq!(
            CONCURRENT_DOWNLOAD_TASK_GAUGE
                .with_label_values(&labels)
                .get(),
            concurrent_before
        );
        assert_eq!(
            DOWNLOAD_TASK_DURATION
                .with_label_values(&["1", "2"])
                .get_sample_count(),
            samples_before + 1
        );
    }

    #[test]
    fn download_task_finished_observes_duration_by_range_or_content_length() {
        let (tag, app, priority) = ("download-duration-tag", "download-duration-app", "5");

        let test_cases = vec![
            (2, 512 * 1024, None, Duration::from_millis(600), "1"),
            (
                3,
                5 * 1024 * 1024,
                Some(Range {
                    start: 0,
                    length: 1024,
                }),
                Duration::from_millis(50),
                "1",
            ),
            (4, 5 * 1024 * 1024, None, Duration::from_millis(50), "3"),
        ];

        for (typ, content_length, range, cost, expected_level) in test_cases {
            let typ_label = typ.to_string();
            let labels = [typ_label.as_str(), expected_level];
            let samples_before = DOWNLOAD_TASK_DURATION
                .with_label_values(&labels)
                .get_sample_count();
            collect_download_task_started_metrics(typ, tag, app, priority);
            collect_download_task_finished_metrics(
                typ,
                tag,
                app,
                priority,
                content_length,
                range,
                cost,
            );
            assert_eq!(
                DOWNLOAD_TASK_DURATION
                    .with_label_values(&labels)
                    .get_sample_count(),
                samples_before + 1
            );
        }
    }

    #[test]
    fn prefetch_task_metrics_count_started_and_failure() {
        let (tag, app, priority) = ("prefetch-tag", "prefetch-app", "5");
        let labels = ["3", tag, app, priority];
        let count_before = PREFETCH_TASK_COUNT.with_label_values(&labels).get();
        collect_prefetch_task_started_metrics(3, tag, app, priority);
        assert_eq!(
            PREFETCH_TASK_COUNT.with_label_values(&labels).get(),
            count_before + 1
        );

        let failure_before = PREFETCH_TASK_FAILURE_COUNT.with_label_values(&labels).get();
        collect_prefetch_task_failure_metrics(3, tag, app, priority);
        assert_eq!(
            PREFETCH_TASK_FAILURE_COUNT.with_label_values(&labels).get(),
            failure_before + 1
        );
    }

    #[test]
    fn upload_piece_metrics_move_gauge_and_traffic() {
        let gauge_before = CONCURRENT_UPLOAD_PIECE_GAUGE.with_label_values(&[]).get();
        collect_upload_piece_started_metrics();
        assert_eq!(
            CONCURRENT_UPLOAD_PIECE_GAUGE.with_label_values(&[]).get(),
            gauge_before + 1
        );

        collect_upload_piece_finished_metrics();
        assert_eq!(
            CONCURRENT_UPLOAD_PIECE_GAUGE.with_label_values(&[]).get(),
            gauge_before
        );

        let traffic_before = UPLOAD_TRAFFIC.with_label_values(&[]).get();
        collect_upload_piece_traffic_metrics(1024);
        assert_eq!(
            UPLOAD_TRAFFIC.with_label_values(&[]).get(),
            traffic_before + 1024
        );

        collect_upload_piece_started_metrics();
        collect_upload_piece_failure_metrics();
        assert_eq!(
            CONCURRENT_UPLOAD_PIECE_GAUGE.with_label_values(&[]).get(),
            gauge_before
        );
    }

    #[test]
    fn download_piece_traffic_adds_length_for_the_traffic_type() {
        let labels = [TrafficType::RemotePeer.as_str_name()];
        let traffic_before = DOWNLOAD_TRAFFIC.with_label_values(&labels).get();
        collect_download_piece_traffic_metrics(&TrafficType::RemotePeer, 2048);
        assert_eq!(
            DOWNLOAD_TRAFFIC.with_label_values(&labels).get(),
            traffic_before + 2048
        );
    }

    #[test]
    fn download_piece_duration_observes_a_sample_for_the_traffic_type() {
        let labels = [TrafficType::RemotePeer.as_str_name()];
        let samples_before = DOWNLOAD_PIECE_DURATION
            .with_label_values(&labels)
            .get_sample_count();
        collect_download_piece_duration_metrics(
            &TrafficType::RemotePeer,
            Duration::from_millis(42),
        );
        assert_eq!(
            DOWNLOAD_PIECE_DURATION
                .with_label_values(&labels)
                .get_sample_count(),
            samples_before + 1
        );
    }

    #[test]
    fn backend_request_metrics_count_and_time_by_scheme_and_method() {
        let count_before = BACKEND_REQUEST_COUNT
            .with_label_values(&["http", "GET"])
            .get();
        collect_backend_request_started_metrics("http", "GET");
        assert_eq!(
            BACKEND_REQUEST_COUNT
                .with_label_values(&["http", "GET"])
                .get(),
            count_before + 1
        );

        let failure_before = BACKEND_REQUEST_FAILURE_COUNT
            .with_label_values(&["http", "GET"])
            .get();
        collect_backend_request_failure_metrics("http", "GET");
        assert_eq!(
            BACKEND_REQUEST_FAILURE_COUNT
                .with_label_values(&["http", "GET"])
                .get(),
            failure_before + 1
        );

        let samples_before = BACKEND_REQUEST_DURATION
            .with_label_values(&["http", "POST"])
            .get_sample_count();
        collect_backend_request_finished_metrics("http", "POST", Duration::from_millis(150));
        assert_eq!(
            BACKEND_REQUEST_DURATION
                .with_label_values(&["http", "POST"])
                .get_sample_count(),
            samples_before + 1
        );
    }

    #[test]
    fn typed_counters_increment_under_the_type_label() {
        let test_cases: Vec<(fn(i32), &IntCounterVec)> = vec![
            (collect_update_task_started_metrics, &UPDATE_TASK_COUNT),
            (
                collect_update_task_failure_metrics,
                &UPDATE_TASK_FAILURE_COUNT,
            ),
            (collect_stat_task_started_metrics, &STAT_TASK_COUNT),
            (collect_stat_task_failure_metrics, &STAT_TASK_FAILURE_COUNT),
            (
                collect_stat_local_task_started_metrics,
                &STAT_LOCAL_TASK_COUNT,
            ),
            (
                collect_stat_local_task_failure_metrics,
                &STAT_LOCAL_TASK_FAILURE_COUNT,
            ),
            (
                collect_list_local_tasks_started_metrics,
                &LIST_LOCAL_TASKS_COUNT,
            ),
            (
                collect_list_local_tasks_failure_metrics,
                &LIST_LOCAL_TASKS_FAILURE_COUNT,
            ),
            (
                collect_list_task_entries_started_metrics,
                &LIST_TASK_ENTRIES_COUNT,
            ),
            (
                collect_list_task_entries_failure_metrics,
                &LIST_TASK_ENTRIES_FAILURE_COUNT,
            ),
            (collect_delete_task_started_metrics, &DELETE_TASK_COUNT),
            (
                collect_delete_task_failure_metrics,
                &DELETE_TASK_FAILURE_COUNT,
            ),
            (
                collect_delete_local_task_started_metrics,
                &DELETE_LOCAL_TASK_COUNT,
            ),
            (
                collect_delete_local_task_failure_metrics,
                &DELETE_LOCAL_TASK_FAILURE_COUNT,
            ),
            (
                collect_download_task_blocked_metrics,
                &DOWNLOAD_TASK_BLOCKED_COUNT,
            ),
            (
                collect_upload_task_blocked_metrics,
                &UPLOAD_TASK_BLOCKED_COUNT,
            ),
        ];

        for (collect, metric) in test_cases {
            let count_before = metric.with_label_values(&["1"]).get();
            collect(1);
            assert_eq!(metric.with_label_values(&["1"]).get(), count_before + 1);
        }
    }

    #[test]
    fn unlabeled_counters_increment_on_each_call() {
        let test_cases: Vec<(fn(), &IntCounterVec)> = vec![
            (collect_proxy_request_started_metrics, &PROXY_REQUEST_COUNT),
            (
                collect_proxy_request_failure_metrics,
                &PROXY_REQUEST_FAILURE_COUNT,
            ),
            (
                collect_proxy_request_via_dfdaemon_metrics,
                &PROXY_REQUEST_VIA_DFDAEMON_COUNT,
            ),
            (collect_delete_host_started_metrics, &DELETE_HOST_COUNT),
            (
                collect_delete_host_failure_metrics,
                &DELETE_HOST_FAILURE_COUNT,
            ),
        ];

        for (collect, metric) in test_cases {
            let count_before = metric.with_label_values(&[]).get();
            collect();
            assert_eq!(metric.with_label_values(&[]).get(), count_before + 1);
        }
    }

    #[test]
    fn collect_disk_metrics_sets_space_gauges_only_for_an_existing_path() {
        collect_disk_metrics(&std::env::temp_dir());
        let total_space = DISK_SPACE.with_label_values(&[]).get();
        let usage_space = DISK_USAGE_SPACE.with_label_values(&[]).get();
        assert!(total_space > 0);
        assert!((0..=total_space).contains(&usage_space));

        collect_disk_metrics(Path::new("/nonexistent/dragonfly-client-metric"));
        assert_eq!(DISK_SPACE.with_label_values(&[]).get(), total_space);
        assert_eq!(DISK_USAGE_SPACE.with_label_values(&[]).get(), usage_space);
    }

    #[tokio::test]
    async fn handler_serves_get_metrics_and_rejects_other_routes() {
        let config = Arc::new(Config {
            storage: Storage {
                dir: "/nonexistent/dragonfly-client-metric".into(),
                ..Default::default()
            },
            ..Default::default()
        });

        let test_cases = vec![
            (
                Method::GET,
                "/metrics",
                StatusCode::OK,
                Some("text/plain; charset=utf-8"),
            ),
            (Method::POST, "/metrics", StatusCode::NOT_FOUND, None),
            (Method::GET, "/healthz", StatusCode::NOT_FOUND, None),
        ];

        for (method, path, expected_status, expected_content_type) in test_cases {
            let request = Request::builder()
                .method(&method)
                .uri(path)
                .body(())
                .unwrap();
            let response = Metrics::handler(config.clone(), request).await.unwrap();
            let content_type = response
                .headers()
                .get(hyper::header::CONTENT_TYPE)
                .map(|value| value.to_str().unwrap());
            assert_eq!(response.status(), expected_status);
            assert_eq!(content_type, expected_content_type);
        }
    }
}
