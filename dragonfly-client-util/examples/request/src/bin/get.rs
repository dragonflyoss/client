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

use dragonfly_client_util::request::{GetRequest, Proxy, Request};
use std::time::Duration;
use tokio::io::AsyncReadExt;

/// This example demonstrates how to use the get method of the Dragonfly request module
/// to download a file via the Dragonfly P2P network.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let scheduler_endpoint = std::env::var("DRAGONFLY_SCHEDULER_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8002".to_string());

    let proxy = Proxy::builder()
        .scheduler_endpoint(scheduler_endpoint)
        .scheduler_request_timeout(Duration::from_secs(5))
        .health_check_interval(Duration::from_secs(60))
        .max_retries(3)
        .build()
        .await
        .map_err(|err| anyhow::anyhow!("failed to build proxy: {}", err))?;

    let request = GetRequest {
        url: "https://example.com/path/to/file".to_string(),
        header: None,
        piece_length: None,
        tag: None,
        application: None,
        filtered_query_params: Vec::new(),
        content_for_calculating_task_id: None,
        enable_task_id_based_blob_digest: false,
        priority: None,
        timeout: Duration::from_secs(300),
        client_cert: None,
    };

    let response = proxy
        .get(&request)
        .await
        .map_err(|err| anyhow::anyhow!("get request failed: {}", err))?;

    if let Some(mut reader) = response.reader {
        let mut body = Vec::new();
        reader.read_to_end(&mut body).await?;
        println!("{} downloaded, size: {} bytes", &request.url, body.len());
    }

    Ok(())
}
