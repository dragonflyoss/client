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

use crate::dynconfig::block_list::DownloadBlockListCheckParams;
use crate::dynconfig::Dynconfig;
use crate::grpc::{DOWNLOAD_STREAM_BUFFER_SIZE, REQUEST_TIMEOUT};
use crate::resource::task::Task;
use dragonfly_api::common::v2::{Range, TaskType};
use dragonfly_api::dfdaemon::v2::{DownloadTaskRequest, DownloadTaskResponse};
use dragonfly_api::errordetails::v2::Backend;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use dragonfly_client_metric::{
    collect_download_task_blocked_metrics, collect_download_task_failure_metrics,
    collect_download_task_finished_metrics, collect_download_task_started_metrics,
    collect_prefetch_task_failure_metrics, collect_prefetch_task_started_metrics,
};
use dragonfly_client_util::{
    digest::is_blob_url,
    http::{
        headermap_to_hashmap, parse_range_header, signature_bound_range_cache_key_from_hashmap,
        signature_bound_range_from_hashmap,
    },
    id_generator::TaskIDParameter,
    types::redacted::RedactedDownload,
};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tonic::Status;
use tracing::{debug, error, info, instrument, warn, Instrument, Span};

/// DownloadTaskStream is the stream of the download task responses.
type DownloadTaskStream = mpsc::Receiver<Result<DownloadTaskResponse, Status>>;

/// Download the task by the task manager directly. It is similar to the
/// download_task of the dfdaemon download gRPC server, but removes the gRPC-related
/// content to avoid the gRPC overhead in the proxy.
#[instrument(skip_all, fields(host_id, task_id, peer_id, content_length))]
pub async fn download(
    config: Arc<Config>,
    task_manager: Arc<Task>,
    dynconfig: Arc<Dynconfig>,
    request: DownloadTaskRequest,
) -> ClientResult<DownloadTaskStream> {
    // Record the start time.
    let start_time = Instant::now();

    // Check whether the download is empty.
    let mut download = request.download.ok_or_else(|| {
        error!("missing download");
        ClientError::InvalidParameter
    })?;

    // Check whether rejected by blocklist policy.
    let check_params = DownloadBlockListCheckParams {
        application: download.application.clone(),
        url: Some(download.url.clone()),
        tag: download.tag.clone(),
        priority: Some(download.priority),
    };
    if dynconfig
        .block_list
        .is_task_download_blocked(&check_params)
        .await
    {
        warn!("download rejected by blocklist policy: {:?}", check_params);
        collect_download_task_blocked_metrics(TaskType::Standard as i32);
        return Err(ClientError::PermissionDenied);
    }

    // If concurrent_piece_count is not set in the request, use the default value in the config.
    download.concurrent_piece_count = Some(config.download.concurrent_piece_count);

    // A signature-bound Range cannot be removed for a full-object prefetch or
    // rewritten as a Dragonfly piece range without invalidating AWS SigV4.
    let signature_bound_range =
        signature_bound_range_from_hashmap(&download.request_header, &download.url)
            .map(str::to_owned);
    let signature_bound_cache_key =
        signature_bound_range_cache_key_from_hashmap(&download.request_header, &download.url);
    if signature_bound_range.is_some() {
        download.prefetch = false;
        // Parse the authoritative signed header after stat determines the full
        // object length. Do not allow a separate protobuf range to diverge.
        download.range = None;
    }

    // Generate the task id.
    let task_id = task_manager
        .id_generator
        .task_id(
            if let Some(content) = download.content_for_calculating_task_id.clone() {
                TaskIDParameter::Content(content)
            } else if download.enable_task_id_based_blob_digest && is_blob_url(&download.url) {
                TaskIDParameter::BlobDigestBased(download.url.clone())
            } else {
                let revision = download
                    .hugging_face
                    .as_ref()
                    .map(|hf| hf.revision.clone())
                    .or_else(|| download.model_scope.as_ref().map(|ms| ms.revision.clone()));

                TaskIDParameter::URLBased {
                    url: download.url.clone(),
                    piece_length: download.piece_length,
                    tag: download.tag.clone(),
                    application: download.application.clone(),
                    filtered_query_params: download.filtered_query_params.clone(),
                    revision,
                }
            },
        )
        .inspect_err(|err| {
            error!("generate task id: {}", err);
        })?;
    let task_id = match signature_bound_cache_key.as_deref() {
        Some(cache_key) => task_manager.id_generator.range_task_id(&task_id, cache_key),
        None => task_id,
    };

    // Generate the host id.
    let host_id = task_manager.id_generator.host_id();

    // Generate the peer id.
    let peer_id = task_manager.id_generator.peer_id();

    // Span record the host id, task id and peer id.
    Span::current().record("host_id", host_id.as_str());
    Span::current().record("task_id", task_id.as_str());
    Span::current().record("peer_id", peer_id.as_str());

    // Download task started.
    info!("download task started: {:?}", RedactedDownload(&download));
    let task = match task_manager
        .download_started(task_id.as_str(), download.clone())
        .await
    {
        Err(err @ ClientError::BackendError(_)) => {
            error!("download started failed by error: {}", err);
            task_manager
                .download_failed(task_id.as_str())
                .await
                .unwrap_or_else(|err| error!("download task failed: {}", err));

            return Err(err);
        }
        Err(err) => {
            error!("download started failed: {}", err);
            return Err(err);
        }
        Ok(task) => {
            // Collect download task started metrics.
            collect_download_task_started_metrics(
                download.r#type,
                download.tag.clone().unwrap_or_default().as_str(),
                download.application.clone().unwrap_or_default().as_str(),
                download.priority.to_string().as_str(),
            );

            task
        }
    };
    Span::current().record("content_length", task.content_length().unwrap_or_default());

    // Update the actual content length, actual piece length and actual
    // piece count of the download.
    download.actual_content_length = task.content_length();
    download.actual_piece_length = task.piece_length();
    download.actual_piece_count = task.piece_count();

    // Download's range priority is higher than the request header's range.
    // If download protocol is http, use the range of the request header.
    // If download protocol is not http, use the range of the download.
    if let Some(range_header) = signature_bound_range.as_deref() {
        let content_range = Task::signature_bound_content_range(&task, range_header)?;
        download.range = Some(Range {
            start: 0,
            length: content_range.range.length,
        });
    } else if download.range.is_none() {
        // Look up the range header directly instead of converting the whole
        // request header hashmap into a HeaderMap.
        let range_header = download
            .request_header
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(reqwest::header::RANGE.as_str()))
            .map(|(_, value)| value.as_str());

        download.range = match range_header {
            Some(range_header) => {
                match parse_range_header(range_header, task.content_length().unwrap_or_default()) {
                    Ok(range) => Some(range),
                    Err(err) => {
                        // Download task failed.
                        task_manager
                            .download_failed(task_id.as_str())
                            .await
                            .unwrap_or_else(|err| error!("download task failed: {}", err));

                        // Collect download task failure metrics.
                        collect_download_task_failure_metrics(
                            download.r#type,
                            download.tag.clone().unwrap_or_default().as_str(),
                            download.application.clone().unwrap_or_default().as_str(),
                            download.priority.to_string().as_str(),
                        );

                        error!("get range failed: {}", err);
                        return Err(err);
                    }
                }
            }
            None => None,
        };
    }

    // Initialize stream channel.
    let download_clone = download.clone();
    let task_manager_clone = task_manager.clone();
    let task_clone = task.clone();
    let (out_stream_tx, out_stream_rx) = mpsc::channel(DOWNLOAD_STREAM_BUFFER_SIZE);

    // Define the error handler to send the error to the stream.
    async fn handle_error(
        out_stream_tx: &Sender<Result<DownloadTaskResponse, Status>>,
        err: impl std::error::Error,
    ) {
        out_stream_tx
            .send_timeout(Err(Status::internal(err.to_string())), REQUEST_TIMEOUT)
            .await
            .unwrap_or_else(|err| error!("send download progress error: {:?}", err));
    }

    // Define the backend error handler to send the error to the stream.
    async fn handle_backend_error(
        out_stream_tx: &Sender<Result<DownloadTaskResponse, Status>>,
        err: Status,
    ) {
        out_stream_tx
            .send_timeout(Err(err), REQUEST_TIMEOUT)
            .await
            .unwrap_or_else(|err| error!("send download progress error: {:?}", err));
    }

    tokio::spawn(
        async move {
            match task_manager_clone
                .download(
                    &task_clone,
                    host_id.as_str(),
                    peer_id.as_str(),
                    download_clone.clone(),
                    out_stream_tx.clone(),
                )
                .await
            {
                Ok(_) => {
                    // Collect download task finished metrics.
                    collect_download_task_finished_metrics(
                        download_clone.r#type,
                        download_clone.tag.clone().unwrap_or_default().as_str(),
                        download_clone
                            .application
                            .clone()
                            .unwrap_or_default()
                            .as_str(),
                        download_clone.priority.to_string().as_str(),
                        task_clone.content_length().unwrap_or_default(),
                        download_clone.range,
                        start_time.elapsed(),
                    );

                    // Download task succeeded.
                    debug!("download task succeeded");
                    if let Err(err) = task_manager_clone.download_finished(task_clone.id.as_str()) {
                        error!("download task finished: {}", err);
                        handle_error(&out_stream_tx, err).await;
                        return;
                    }
                }
                Err(ClientError::BackendError(err)) => {
                    // Collect download task failure metrics.
                    collect_download_task_failure_metrics(
                        download_clone.r#type,
                        download_clone.tag.clone().unwrap_or_default().as_str(),
                        download_clone
                            .application
                            .clone()
                            .unwrap_or_default()
                            .as_str(),
                        download_clone.priority.to_string().as_str(),
                    );

                    task_manager_clone
                        .download_failed(task_clone.id.as_str())
                        .await
                        .unwrap_or_else(|err| error!("download task failed: {}", err));

                    match serde_json::to_vec::<Backend>(&Backend {
                        message: err.message.clone(),
                        header: headermap_to_hashmap(&err.header.clone().unwrap_or_default()),
                        status_code: err.status_code.map(|code| code.as_u16() as i32),
                    }) {
                        Ok(json) => {
                            handle_backend_error(
                                &out_stream_tx,
                                Status::with_details(
                                    tonic::Code::Internal,
                                    err.to_string(),
                                    json.into(),
                                ),
                            )
                            .await;
                        }
                        Err(err) => {
                            error!("serialize error: {}", err);
                            handle_error(&out_stream_tx, err).await;
                        }
                    }
                }
                Err(err) => {
                    error!("download failed: {}", err);

                    // Collect download task failure metrics.
                    collect_download_task_failure_metrics(
                        download_clone.r#type,
                        download_clone.tag.clone().unwrap_or_default().as_str(),
                        download_clone
                            .application
                            .clone()
                            .unwrap_or_default()
                            .as_str(),
                        download_clone.priority.to_string().as_str(),
                    );

                    // Download task failed.
                    task_manager_clone
                        .download_failed(task_clone.id.as_str())
                        .await
                        .unwrap_or_else(|err| error!("download task failed: {}", err));

                    handle_error(&out_stream_tx, err).await;
                }
            }

            drop(out_stream_tx);
        }
        .in_current_span(),
    );

    Ok(out_stream_rx)
}

///  Prefetch the full task by the task manager directly. It is similar to the
/// prefetch_task of the dfdaemon gRPC module, but downloads the task by the task manager
/// instead of the dfdaemon download gRPC client.
#[instrument(skip_all)]
pub async fn prefetch(
    config: Arc<Config>,
    task_manager: Arc<Task>,
    dynconfig: Arc<Dynconfig>,
    mut request: DownloadTaskRequest,
) -> ClientResult<()> {
    // Make the prefetch request.
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

    // Download the task by the task manager.
    info!("prefetch task started");
    let mut out_stream_rx = self::download(config, task_manager, dynconfig, request)
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
            while let Some(message) = out_stream_rx.recv().await {
                match message {
                    Ok(_) => debug!("prefetch piece finished"),
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

            info!("prefetch task finished");
        }
        .in_current_span(),
    );

    Ok(())
}
