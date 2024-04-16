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

use dragonfly_api::dfdaemon::v2::DownloadTaskRequest;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use std::path::PathBuf;
use std::time::Duration;
use tonic::Request;
use tracing::{error, info, Instrument};

pub mod dfdaemon_download;
pub mod dfdaemon_upload;
pub mod health;
pub mod manager;
pub mod scheduler;
pub mod security;

// CONNECT_TIMEOUT is the timeout for GRPC connection.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(4);

// REQUEST_TIMEOUT is the timeout for GRPC requests.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

// prefetch_task prefetches the task if prefetch flag is true.
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
        return Err(ClientError::InvalidParameter);
    };
    // Remove the range flag for download full task.
    download.range = None;
    // Remove the prefetch flag for prevent the infinite loop.
    download.prefetch = false;
    // Remove the range header for download full task.
    download
        .request_header
        .remove(reqwest::header::RANGE.as_str());

    // Download task by dfdaemon download client.
    let response = dfdaemon_download_client.download_task(request).await?;

    // Spawn to handle the download task.
    tokio::spawn(
        async move {
            let mut out_stream = response.into_inner();
            loop {
                match out_stream.message().await {
                    Ok(Some(message)) => info!("prefetch piece finished {:?}", message),
                    Ok(None) => {
                        info!("prefetch task finished");
                        return;
                    }
                    Err(err) => {
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
