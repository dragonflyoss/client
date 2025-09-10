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

use crate::metrics::{
    collect_prefetch_task_failure_metrics, collect_prefetch_task_started_metrics,
};
use dragonfly_api::dfdaemon::v2::{DownloadCacheTaskRequest, DownloadTaskRequest};
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use std::path::PathBuf;
use std::time::Duration;
use tonic::Request;
use tracing::{error, info, instrument, Instrument};

pub mod dfdaemon_download;
pub mod dfdaemon_upload;
pub mod health;
pub mod interceptor;
pub mod manager;
pub mod scheduler;

/// CONNECT_TIMEOUT is the timeout for GRPC connection.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// REQUEST_TIMEOUT is the timeout for GRPC requests, default is 10 second.
/// Note: This timeout is used for the whole request, including wait for scheduler
/// scheduling, refer to https://d7y.io/docs/next/reference/configuration/scheduler/.
/// Scheduler'configure `scheduler.retryInterval`, `scheduler.retryBackToSourceLimit` and `scheduler.retryLimit`
/// is used for the scheduler to schedule the task.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// TCP_KEEPALIVE is the keepalive duration for TCP connection.
pub const TCP_KEEPALIVE: Duration = Duration::from_secs(3600);

/// HTTP2_KEEP_ALIVE_INTERVAL is the interval for HTTP2 keep alive.
pub const HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(300);

/// HTTP2_KEEP_ALIVE_TIMEOUT is the timeout for HTTP2 keep alive.
pub const HTTP2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// MAX_FRAME_SIZE is the max frame size for GRPC, default is 4MB.
pub const MAX_FRAME_SIZE: u32 = 4 * 1024 * 1024;

/// INITIAL_WINDOW_SIZE is the initial window size for GRPC, default is 512KB.
pub const INITIAL_WINDOW_SIZE: u32 = 512 * 1024;

/// BUFFER_SIZE is the buffer size for GRPC, default is 64KB.
pub const BUFFER_SIZE: usize = 64 * 1024;

/// prefetch_task prefetches the task if prefetch flag is true.
#[instrument(skip_all)]
pub async fn prefetch_task(
    socket_path: PathBuf,
    request: Request<DownloadTaskRequest>,
) -> ClientResult<()> {
    // Initialize the dfdaemon download client.
    let dfdaemon_download_client =
        dfdaemon_download::DfdaemonDownloadClient::new_unix(socket_path.clone()).await?;

    // Make the prefetch request.
    let mut request = request.into_inner();
    let Some(download) = request.download.as_mut() else {
        error!("request download is missing");
        return Err(ClientError::InvalidParameter);
    };

    // Remove the range flag for download full task.
    download.range = None;

    // Remove the prefetch flag for prevent the infinite loop.
    download.prefetch = false;

    // Mark the is_prefetch flag as true to represents it is a prefetch request.
    download.is_prefetch = true;

    // Remove the range header for download full task.
    download
        .request_header
        .remove(reqwest::header::RANGE.as_str());

    // Get the fields from the download task.
    let task_type = download.r#type;
    let tag = download.tag.clone();
    let application = download.application.clone();
    let priority = download.priority;

    // Download task by dfdaemon download client.
    let response = dfdaemon_download_client
        .download_task(request)
        .await
        .inspect_err(|err| {
            error!("prefetch task failed: {}", err);
        })?;

    // Collect the prefetch task started metrics.
    collect_prefetch_task_started_metrics(
        task_type,
        tag.clone().unwrap_or_default().as_str(),
        application.clone().unwrap_or_default().as_str(),
        priority.to_string().as_str(),
    );

    // Spawn to handle the download task.
    tokio::spawn(
        async move {
            let mut out_stream = response.into_inner();
            loop {
                match out_stream.message().await {
                    Ok(Some(_)) => info!("prefetch piece finished"),
                    Ok(None) => {
                        info!("prefetch task finished");
                        return;
                    }
                    Err(err) => {
                        // Collect the prefetch task failure metrics.
                        collect_prefetch_task_failure_metrics(
                            task_type,
                            tag.clone().unwrap_or_default().as_str(),
                            application.clone().unwrap_or_default().as_str(),
                            priority.to_string().as_str(),
                        );

                        error!("prefetch piece failed: {}", err);
                        return;
                    }
                }
            }
        }
        .in_current_span(),
    );

    Ok(())
}

/// prefetch_cache_task prefetches the cache task if prefetch flag is true.
#[instrument(skip_all)]
pub async fn prefetch_cache_task(
    socket_path: PathBuf,
    request: Request<DownloadCacheTaskRequest>,
) -> ClientResult<()> {
    // Initialize the dfdaemon download client.
    let dfdaemon_download_client =
        dfdaemon_download::DfdaemonDownloadClient::new_unix(socket_path.clone()).await?;

    // Make the prefetch request.
    let mut request = request.into_inner();

    // Remove the range flag for download full task.
    request.range = None;

    // Remove the prefetch flag for prevent the infinite loop.
    request.prefetch = false;

    // Mark the is_prefetch flag as true to represents it is a prefetch request.
    request.is_prefetch = true;

    // Remove the range header for download full cache task.
    request
        .request_header
        .remove(reqwest::header::RANGE.as_str());

    // Get the fields from the download cache task.
    let task_type = request.r#type;
    let tag = request.tag.clone();
    let application = request.application.clone();
    let priority = request.priority;

    // Download cache task by dfdaemon download client.
    let response = dfdaemon_download_client
        .download_cache_task(request)
        .await
        .inspect_err(|err| {
            error!("prefetch cache task failed: {}", err);
        })?;

    // Collect the prefetch task started metrics.
    collect_prefetch_task_started_metrics(
        task_type,
        tag.clone().unwrap_or_default().as_str(),
        application.clone().unwrap_or_default().as_str(),
        priority.to_string().as_str(),
    );

    // Spawn to handle the download cache task.
    tokio::spawn(
        async move {
            let mut out_stream = response.into_inner();
            loop {
                match out_stream.message().await {
                    Ok(Some(_)) => info!("prefetch piece finished"),
                    Ok(None) => {
                        info!("prefetch cache task finished");
                        return;
                    }
                    Err(err) => {
                        // Collect the prefetch task failure metrics.
                        collect_prefetch_task_failure_metrics(
                            task_type,
                            tag.clone().unwrap_or_default().as_str(),
                            application.clone().unwrap_or_default().as_str(),
                            priority.to_string().as_str(),
                        );

                        error!("prefetch cache piece failed: {}", err);
                        return;
                    }
                }
            }
        }
        .in_current_span(),
    );

    Ok(())
}
